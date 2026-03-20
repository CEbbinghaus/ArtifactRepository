// Hash uses OnceLock for hex cache but Hash/Eq only use the [u8; 64] bytes
#![allow(clippy::mutable_key_type)]

use std::collections::HashMap;
use std::io::Write;

use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression as ZipCompression, ZipEntryBuilder};
use axum::{
    body::{Body, Bytes},
    debug_handler,
    extract::{Path as AxumPath, Query, Request, State},
    http::{HeaderMap, Response, StatusCode},
    routing::{get, post, put},
    Router,
};
use common::{
    Hash, Header, Mode, ObjectType,
    archive::{Archive, ArchiveHeaderEntry, Compression, RawEntryData, HEADER, SUPPLEMENTAL_HEADER, write_compressed_body},
    collect_tree_metadata,
    object_body::{Index, Object as _, Tree},
    read_header_and_body,
    store::{Store, StoreObject},
};
use serde::Deserialize;

use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

use futures::{StreamExt, TryStreamExt};

const STREAM_CHUNK_SIZE: usize = 64 * 1024;
const STREAM_CHANNEL_CAPACITY: usize = 32;

/// A `Write` adapter that sends chunks through a bounded mpsc channel.
/// Provides backpressure: when the channel is full, writes block until
/// the receiver consumes data.
struct ChannelWriter {
    tx: tokio::sync::mpsc::Sender<Result<Vec<u8>, std::io::Error>>,
    buf: Vec<u8>,
    chunk_size: usize,
}

impl ChannelWriter {
    fn new(tx: tokio::sync::mpsc::Sender<Result<Vec<u8>, std::io::Error>>) -> Self {
        Self {
            tx,
            buf: Vec::with_capacity(STREAM_CHUNK_SIZE),
            chunk_size: STREAM_CHUNK_SIZE,
        }
    }

    fn send_buf(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            let chunk = std::mem::replace(&mut self.buf, Vec::with_capacity(self.chunk_size));
            self.tx
                .blocking_send(Ok(chunk))
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "receiver dropped"))?;
        }
        Ok(())
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(data);
        if self.buf.len() >= self.chunk_size {
            self.send_buf()?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.send_buf()
    }
}

impl Drop for ChannelWriter {
    fn drop(&mut self) {
        let _ = self.send_buf();
    }
}

#[derive(Clone)]
struct ServerState {
    store: Store,
}

enum ServerError {
    NotFound(String),
    AlreadyExists(String),
    BadRequest(String),
    Internal(String),
}

impl axum::response::IntoResponse for ServerError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match &self {
            ServerError::NotFound(m) => (StatusCode::NOT_FOUND, m.clone()),
            ServerError::AlreadyExists(m) => (StatusCode::OK, m.clone()),
            ServerError::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            ServerError::Internal(m) => {
                tracing::error!(error = %m, "internal server error");
                (StatusCode::INTERNAL_SERVER_ERROR, m.clone())
            }
        };
        (status, msg).into_response()
    }
}

#[debug_handler]
async fn put_object(
    AxumPath(object_hash): AxumPath<Hash>,
    State(ServerState { store }): State<ServerState>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<StatusCode, ServerError> {
    tracing::debug!(hash = %object_hash, "PUT /object - storing object");
    match store.exists(&object_hash).await {
        Ok(false) => Ok(()),
        Ok(true) => Err(ServerError::AlreadyExists("object already exists".into())),
        Err(err) => Err(ServerError::Internal(err.to_string())),
    }?;

    let Some(object_type) = headers.get("Object-Type").and_then(|v| v.to_str().ok()) else {
        return Err(ServerError::BadRequest("missing Object-Type header".into()));
    };

    let Some(object_type) = ObjectType::from_str(object_type) else {
        return Err(ServerError::BadRequest("invalid Object-Type header".into()));
    };

    let Some(object_size) = headers.get("Object-Size").and_then(|v| v.to_str().ok()) else {
        return Err(ServerError::BadRequest("missing Object-Size header".into()));
    };

    let Some(object_size): Option<u64> = object_size.parse().ok() else {
        return Err(ServerError::BadRequest("invalid Object-Size header".into()));
    };
    let header = Header::new(object_type, object_size);
    let object_size_val = object_size;
    let object_type_str = object_type.to_str().to_string();
    let data_stream = request.into_body().into_data_stream();

    let buffered_reader = data_stream.map(|result| {
        result.map_err(std::io::Error::other)
    }).into_async_read();

    let store_object = StoreObject::new_with_header(header, buffered_reader);

    store
        .put_object(
            &object_hash,
            store_object,
        )
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;

    tracing::info!(hash = %object_hash, object_type = %object_type_str, size = object_size_val, "object stored");

    Ok(StatusCode::CREATED)
}

#[debug_handler]
async fn get_object(
    AxumPath(object_hash): AxumPath<Hash>,
    State(ServerState { store }): State<ServerState>,
) -> Result<Response<Body>, ServerError> {
    tracing::debug!(hash = %object_hash, "GET /object - retrieving object");

    let object = store
        .get_object(&object_hash)
        .await
        .map_err(|_| ServerError::NotFound("no object".into()))?;
    let Header { object_type, size } = object.header;

    let reader_stream = ReaderStream::new(object.compat());
    let mut response = Response::new(Body::from_stream(reader_stream));

    let headers = response.headers_mut();
    headers.insert("Object-Type", object_type.to_str().parse().unwrap());
    headers.insert("Object-Size", size.to_string().parse().unwrap());

    Ok(response)
}

#[derive(Deserialize)]
struct CompressionQuery {
    compression: Option<String>,
}

async fn read_index_from_store(store: &Store, index_hash: &Hash) -> Result<Index, ServerError> {
    let raw = store
        .get_raw_bytes(index_hash)
        .await
        .map_err(|_| ServerError::NotFound("index not found".into()))?;

    let (header, body) = read_header_and_body(&raw)
        .ok_or_else(|| ServerError::Internal("invalid index header".into()))?;

    if header.object_type != ObjectType::Index {
        return Err(ServerError::BadRequest("object is not an index".into()));
    }

    Index::from_data(body).map_err(|err| ServerError::Internal(err.to_string()))
}

/// Walk a tree collecting (Hash, Header) pairs and the ordered list of hashes.
/// Only tree bodies are read to discover children; blob bodies are NOT loaded.
#[allow(clippy::mutable_key_type)]
async fn collect_entry_metadata(
    store: &Store,
    tree_hash: &Hash,
) -> Result<(Vec<Hash>, HashMap<Hash, Header>), ServerError> {
    let mut meta: HashMap<Hash, Header> = HashMap::new();
    let mut order: Vec<Hash> = Vec::new();
    let mut stack = vec![tree_hash.clone()];
    let mut blob_hashes: Vec<Hash> = Vec::new();

    // Phase 1: Walk trees to discover all hashes
    while let Some(current_hash) = stack.pop() {
        if meta.contains_key(&current_hash) {
            continue;
        }

        let raw = store.get_raw_bytes(&current_hash).await
            .map_err(|e| ServerError::Internal(e.to_string()))?;
        let (header, body) = read_header_and_body(&raw)
            .ok_or_else(|| ServerError::Internal("invalid object header".into()))?;

        if header.object_type == ObjectType::Tree {
            let tree = Tree::from_data(body)
                .map_err(|e| ServerError::Internal(e.to_string()))?;
            for entry in &tree.contents {
                if !meta.contains_key(&entry.hash) {
                    if entry.mode == Mode::Tree {
                        stack.push(entry.hash.clone());
                    } else {
                        blob_hashes.push(entry.hash.clone());
                    }
                }
            }
            meta.insert(current_hash.clone(), Header::new(ObjectType::Tree, body.len() as u64));
            order.push(current_hash);
        } else {
            meta.insert(current_hash.clone(), header);
            order.push(current_hash);
        }
    }

    // Phase 2: Read blob headers concurrently
    let blob_results: Vec<Result<(Hash, Header), ServerError>> = futures::stream::iter(blob_hashes)
        .map(|hash| {
            let store = store.clone();
            async move {
                if let Some(header) = store.get_raw_bytes(&hash).await
                    .ok()
                    .and_then(|raw| read_header_and_body(&raw).map(|(h, _)| h))
                {
                    Ok((hash, header))
                } else {
                    // Fallback: use get_object for header only
                    let obj = store.get_object(&hash).await
                        .map_err(|e| ServerError::Internal(e.to_string()))?;
                    Ok((hash, obj.header))
                }
            }
        })
        .buffer_unordered(256)
        .collect()
        .await;

    for result in blob_results {
        let (hash, header) = result?;
        if let std::collections::hash_map::Entry::Vacant(e) = meta.entry(hash.clone()) {
            order.push(hash);
            e.insert(header);
        }
    }

    Ok((order, meta))
}

/// Write an archive to `writer` by streaming each entry from the store on-demand.
/// Only one entry's data is in memory at a time.
#[allow(clippy::too_many_arguments)]
fn write_streaming_archive(
    store: Store,
    magic: [u8; 4],
    compression: Compression,
    index_hash: Hash,
    index: Index,
    entry_order: Vec<Hash>,
    entry_meta: HashMap<Hash, Header>,
    writer: &mut impl Write,
) -> anyhow::Result<()> {
    use common::object_body::Object as _;

    // 1. File header (uncompressed) — same layout as Archive::to_data
    writer.write_all(&magic)?;
    writer.write_all(&(compression as u16).to_be_bytes())?;
    writer.write_all(&index_hash.hash)?;
    writer.write_all(&index.to_data())?;
    writer.write_all(&[0])?;

    // 2. Build entry header table from metadata
    let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::with_capacity(entry_order.len());
    let mut offset: u64 = 0;
    for hash in &entry_order {
        let header = &entry_meta[hash];
        let prefix_len = header.to_string().len() as u64;
        let length = prefix_len + header.size;
        header_entries.push(ArchiveHeaderEntry {
            hash: hash.clone(),
            index: offset,
            length,
        });
        offset += length;
    }

    // 3. Write body (entry table + entry data), fetching from store on-demand
    let write_body = |w: &mut dyn Write| -> anyhow::Result<()> {
        w.write_all(&(header_entries.len() as u64).to_be_bytes())?;
        for entry in &header_entries {
            w.write_all(&entry.hash.hash)?;
            w.write_all(&entry.index.to_be_bytes())?;
            w.write_all(&entry.length.to_be_bytes())?;
        }
        for hash in &entry_order {
            let header = &entry_meta[hash];
            let prefix = header.to_string();
            w.write_all(prefix.as_bytes())?;

            // Use direct I/O when available for maximum throughput,
            // otherwise fall back to async via block_on
            let raw = if store.has_direct_io() {
                store.get_bytes_direct_blocking(hash)?
            } else {
                tokio::runtime::Handle::current().block_on(async {
                    store.get_raw_bytes(hash).await
                })?
            };
            let (_, body) = read_header_and_body(&raw)
                .ok_or_else(|| anyhow::anyhow!("invalid object header for {}", hash))?;
            w.write_all(body)?;
        }
        w.flush()?;
        Ok(())
    };

    // 4. Apply compression using the Archive crate's same compression stack
    // We use the common crate's compression by constructing a minimal Archive
    // and delegating to its to_data. But since we need on-demand fetching,
    // we write the compressed body ourselves using the same encoders.
    write_compressed_body(compression, writer, write_body)?;

    Ok(())
}

#[debug_handler]
async fn get_archive(
    AxumPath(index_hash): AxumPath<Hash>,
    Query(query): Query<CompressionQuery>,
    State(ServerState { store }): State<ServerState>,
) -> Result<Response<Body>, ServerError> {
    let compression = match query.compression.as_deref() {
        Some(s) => s
            .parse::<Compression>()
            .map_err(|_| ServerError::BadRequest("invalid compression type".into()))?,
        None => Compression::Zstd,
    };

    tracing::info!(hash = %index_hash, compression = ?compression, "GET /archive - streaming archive");

    let index = read_index_from_store(&store, &index_hash).await?;

    // Collect only metadata (hashes + headers), not body data
    let (entry_order, entry_meta) = collect_entry_metadata(&store, &index.tree).await?;

    let (tx, rx) = tokio::sync::mpsc::channel(STREAM_CHANNEL_CAPACITY);
    let ih = index_hash.clone();

    tokio::task::spawn_blocking(move || {
        let mut writer = ChannelWriter::new(tx);
        #[allow(clippy::mutable_key_type)]
        if let Err(e) = write_streaming_archive(
            store, HEADER, compression, ih, index, entry_order, entry_meta, &mut writer,
        ) {
            tracing::error!(error = %e, "archive streaming failed");
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(stream);

    let short_hash = &index_hash.as_str()[..12];
    let mut response = Response::new(body);
    let headers = response.headers_mut();
    headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
    headers.insert(
        "Content-Disposition",
        format!("attachment; filename=\"{}.arx\"", short_hash)
            .parse()
            .unwrap(),
    );

    Ok(response)
}

#[derive(Deserialize)]
struct SupplementalRequest {
    hashes: Vec<Hash>,
}

#[debug_handler]
async fn get_supplemental(
    AxumPath(index_hash): AxumPath<Hash>,
    Query(query): Query<CompressionQuery>,
    State(ServerState { store }): State<ServerState>,
    axum::Json(request): axum::Json<SupplementalRequest>,
) -> Result<Response<Body>, ServerError> {
    tracing::info!(hash = %index_hash, requested_hashes = request.hashes.len(), "POST /supplemental - streaming");

    let compression = match query.compression.as_deref() {
        Some(s) => s
            .parse::<Compression>()
            .map_err(|_| ServerError::BadRequest("invalid compression type".into()))?,
        None => Compression::Zstd,
    };

    let index = read_index_from_store(&store, &index_hash).await?;

    // Collect metadata only (no body data)
    let (all_order, all_meta) = collect_entry_metadata(&store, &index.tree).await?;

    // Also get the index object's header
    let index_obj = store.get_object(&index_hash).await
        .map_err(|e| ServerError::Internal(e.to_string()))?;
    let index_header = index_obj.header;

    // Build the set of hashes to include:
    // - All requested hashes
    // - All tree-type objects (for directory structure)
    // - The index object itself
    #[allow(clippy::mutable_key_type)]
    let mut included: std::collections::HashSet<Hash> = std::collections::HashSet::new();
    included.insert(index_hash.clone());
    for (hash, header) in &all_meta {
        if header.object_type == ObjectType::Tree {
            included.insert(hash.clone());
        }
    }
    for hash in &request.hashes {
        included.insert(hash.clone());
    }

    // Filter to only included entries
    #[allow(clippy::mutable_key_type)]
    let mut entry_meta: HashMap<Hash, Header> = HashMap::new();
    let mut entry_order: Vec<Hash> = Vec::new();

    // Add index object first
    entry_meta.insert(index_hash.clone(), index_header);
    entry_order.push(index_hash.clone());

    for hash in &all_order {
        if included.contains(hash) && !entry_meta.contains_key(hash) {
            entry_meta.insert(hash.clone(), all_meta[hash]);
            entry_order.push(hash.clone());
        }
    }

    tracing::debug!(entries = entry_order.len(), "supplemental entries selected");

    let (tx, rx) = tokio::sync::mpsc::channel(STREAM_CHANNEL_CAPACITY);
    let ih = index_hash.clone();

    tokio::task::spawn_blocking(move || {
        let mut writer = ChannelWriter::new(tx);
        #[allow(clippy::mutable_key_type)]
        if let Err(e) = write_streaming_archive(
            store, SUPPLEMENTAL_HEADER, compression, ih, index, entry_order, entry_meta, &mut writer,
        ) {
            tracing::error!(error = %e, "supplemental streaming failed");
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(stream);

    let short_hash = &index_hash.as_str()[..12];
    let mut response = Response::new(body);
    let headers = response.headers_mut();
    headers.insert("Content-Type", "application/octet-stream".parse().unwrap());
    headers.insert(
        "Content-Disposition",
        format!("attachment; filename=\"{}.sar\"", short_hash)
            .parse()
            .unwrap(),
    );

    Ok(response)
}

/// Recursively walk a tree and write each blob entry directly to the zip writer,
/// streaming from the store without collecting all file data first.
fn stream_zip_tree<'a, W: tokio::io::AsyncWrite + Unpin + Send + 'a>(
    store: &'a Store,
    tree_hash: &'a Hash,
    prefix: &'a str,
    zip_writer: &'a mut ZipFileWriter<W>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let raw = store.get_raw_bytes(tree_hash).await?;
        let (_, body) = read_header_and_body(&raw)
            .ok_or_else(|| anyhow::anyhow!("invalid tree header"))?;
        let tree = Tree::from_data(body)?;

        for entry in &tree.contents {
            let entry_path = if prefix.is_empty() {
                entry.path.clone()
            } else {
                format!("{}/{}", prefix, entry.path)
            };

            match entry.mode {
                Mode::Tree => {
                    stream_zip_tree(store, &entry.hash, &entry_path, zip_writer).await?;
                }
                _ => {
                    let raw = store.get_raw_bytes(&entry.hash).await?;
                    let (_, data) = read_header_and_body(&raw)
                        .ok_or_else(|| anyhow::anyhow!("invalid blob header"))?;

                    let zip_entry = ZipEntryBuilder::new(
                        entry_path.into(),
                        ZipCompression::Deflate,
                    );
                    let mut entry_writer = zip_writer.write_entry_stream(zip_entry).await?;
                    futures::AsyncWriteExt::write_all(&mut entry_writer, data).await?;
                    entry_writer.close().await?;
                }
            }
        }

        Ok(())
    })
}

#[debug_handler]
async fn get_zip(
    AxumPath(index_hash): AxumPath<Hash>,
    State(ServerState { store }): State<ServerState>,
) -> Result<Response<Body>, ServerError> {
    tracing::info!(hash = %index_hash, "GET /zip - streaming zip");

    let index = read_index_from_store(&store, &index_hash).await?;
    let tree_hash = index.tree.clone();
    let short_hash = index_hash.as_str()[..12].to_string();

    // DuplexStream: write zip to one end, stream from the other
    let (read_half, write_half) = tokio::io::duplex(STREAM_CHUNK_SIZE);

    tokio::spawn(async move {
        let mut zip_writer = ZipFileWriter::with_tokio(write_half);
        if let Err(e) = stream_zip_tree(&store, &tree_hash, "", &mut zip_writer).await {
            tracing::error!(error = %e, "zip streaming failed");
            return;
        }
        if let Err(e) = zip_writer.close().await {
            tracing::error!(error = %e, "zip close failed");
        }
    });

    let stream = ReaderStream::new(read_half);
    let body = Body::from_stream(stream);

    let mut response = Response::new(body);
    let headers = response.headers_mut();
    headers.insert("Content-Type", "application/zip".parse().unwrap());
    headers.insert(
        "Content-Disposition",
        format!("attachment; filename=\"{}.zip\"", short_hash)
            .parse()
            .unwrap(),
    );

    Ok(response)
}

#[debug_handler]
async fn upload_archive(
    State(ServerState { store }): State<ServerState>,
    body: Bytes,
) -> Result<String, ServerError> {
    tracing::info!(bytes = body.len(), "POST /upload - receiving archive");

    let archive = Archive::<RawEntryData>::from_data(&mut std::io::Cursor::new(&body))
        .map_err(|err| ServerError::BadRequest(format!("invalid archive: {}", err)))?;

    let entries: Vec<_> = archive.body.header.iter().zip(archive.body.entries.iter())
        .map(|(header_entry, raw)| (header_entry.hash.clone(), raw.0.clone()))
        .collect();

    let total = entries.len();

    // Use direct blocking I/O when available for maximum throughput
    if store.has_direct_io() {
        let store_clone = store.clone();
        let (added, skipped) = tokio::task::spawn_blocking(move || {
            let mut added = 0usize;
            let mut skipped = 0usize;
            for (hash, raw_data) in &entries {
                if store_clone.exists_direct_blocking(hash).unwrap_or(false) {
                    skipped += 1;
                    continue;
                }
                store_clone.put_bytes_direct_blocking(hash, raw_data)
                    .map_err(|e| ServerError::Internal(e.to_string()))?;
                added += 1;
            }
            Ok::<_, ServerError>((added, skipped))
        }).await.map_err(|e| ServerError::Internal(e.to_string()))??;

        tracing::info!(added, skipped, total, "POST /upload - complete (direct I/O)");
        Ok(format!("Added {} objects, skipped {}", added, skipped))
    } else {
        // Async path with exists check for non-fs backends
        let results: Vec<Result<bool, ServerError>> = futures::stream::iter(entries)
            .map(|(hash, raw_data)| {
                let store = store.clone();
                async move {
                    if store.exists(&hash).await.unwrap_or(false) {
                        return Ok(false);
                    }
                    let (header, body_bytes) = read_header_and_body(&raw_data)
                        .ok_or_else(|| ServerError::BadRequest("invalid entry data".into()))?;
                    store.put_object_bytes(&hash, header, body_bytes.to_vec()).await
                        .map_err(|err| ServerError::Internal(err.to_string()))?;
                    Ok(true)
                }
            })
            .buffer_unordered(64)
            .collect()
            .await;

        let mut added = 0;
        let mut skipped = 0;
        for result in results {
            if result? { added += 1; } else { skipped += 1; }
        }

        tracing::info!(added, skipped, total, "POST /upload - complete");
        Ok(format!("Added {} objects, skipped {}", added, skipped))
    }
}

#[derive(serde::Deserialize)]
struct MissingRequest {
    hashes: Vec<Hash>,
}

#[derive(serde::Serialize)]
struct MissingResponse {
    missing: Vec<Hash>,
}

async fn check_missing(
    State(ServerState { store }): State<ServerState>,
    axum::Json(request): axum::Json<MissingRequest>,
) -> Result<axum::Json<MissingResponse>, ServerError> {
    tracing::debug!(requested = request.hashes.len(), "POST /missing - checking hashes");

    let missing = if store.has_direct_io() {
        // Single blocking task for all exists checks — much faster than concurrent async
        let store_clone = store.clone();
        tokio::task::spawn_blocking(move || {
            request.hashes.into_iter()
                .filter(|hash| !store_clone.exists_direct_blocking(hash).unwrap_or(false))
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|e| ServerError::Internal(e.to_string()))?
    } else {
        // Concurrent async for non-fs backends
        futures::stream::iter(request.hashes)
            .map(|hash| {
                let store = store.clone();
                async move {
                    match store.exists(&hash).await {
                        Ok(true) => None,
                        _ => Some(hash),
                    }
                }
            })
            .buffer_unordered(256)
            .filter_map(|x| async { x })
            .collect()
            .await
    };

    tracing::debug!(missing = missing.len(), "POST /missing - result");

    Ok(axum::Json(MissingResponse { missing }))
}

#[debug_handler]
async fn get_metadata(
    AxumPath(index_hash): AxumPath<Hash>,
    State(ServerState { store }): State<ServerState>,
) -> Result<axum::Json<common::TreeMetadata>, ServerError> {
    tracing::debug!(hash = %index_hash, "GET /metadata - collecting metadata");

    let meta = collect_tree_metadata(&store, &index_hash)
        .await
        .map_err(|err| {
            let msg = err.to_string();
            if msg.contains("not found") || msg.contains("NotFound") {
                ServerError::NotFound("index not found".into())
            } else {
                ServerError::Internal(msg)
            }
        })?;

    Ok(axum::Json(meta))
}

/// Build the application router with the given store.
pub fn create_router(store: Store) -> Router {
    Router::new()
        .route("/object/{object_id}", put(put_object))
        .route("/object/{object_id}", get(get_object))
        .route("/archive/{index_hash}", get(get_archive))
        .route("/supplemental/{index_hash}", post(get_supplemental))
        .route("/zip/{index_hash}", get(get_zip))
        .route("/metadata/{index_hash}", get(get_metadata))
        .route("/upload", post(upload_archive))
        .route("/missing", post(check_missing))
        .with_state(ServerState { store })
}
