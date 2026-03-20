use std::collections::HashMap;
use std::io::Write;

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
    archive::{Archive, ArchiveBody, ArchiveEntryData, ArchiveHeaderEntry, Compression, RawEntryData, HEADER, SUPPLEMENTAL_HEADER},
    collect_tree_metadata,
    object_body::{Index, Object as _, Tree},
    read_header_and_body, read_object_into_headers,
    store::{Store, StoreObject},
};
use serde::Deserialize;

use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

use futures::{AsyncReadExt, StreamExt, TryStreamExt};

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

/// Spawn a blocking task that writes the archive into a channel,
/// returning a streaming Body.
fn stream_archive<T: ArchiveEntryData + Send + 'static>(
    archive: Archive<T>,
) -> Body {
    let (tx, rx) = tokio::sync::mpsc::channel(STREAM_CHANNEL_CAPACITY);

    tokio::task::spawn_blocking(move || {
        let mut writer = ChannelWriter::new(tx);
        if let Err(e) = archive.to_data(&mut writer) {
            tracing::error!(error = %e, "archive streaming serialization failed");
        }
        // Drop flushes remaining bytes
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Body::from_stream(stream)
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

    match store.exists(&object_hash).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(ServerError::NotFound("no object".into())),
        Err(err) => Err(ServerError::Internal(err.to_string())),
    }?;

    let object = store
        .get_object(&object_hash)
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;
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
    match store.exists(index_hash).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(ServerError::NotFound("index not found".into())),
        Err(err) => Err(ServerError::Internal(err.to_string())),
    }?;

    let mut store_obj = store
        .get_object(index_hash)
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;

    if store_obj.header.object_type != ObjectType::Index {
        return Err(ServerError::BadRequest("object is not an index".into()));
    }

    let mut body = Vec::new();
    store_obj
        .read_to_end(&mut body)
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;

    Index::from_data(&body).map_err(|err| ServerError::Internal(err.to_string()))
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

    #[allow(clippy::mutable_key_type)]
    let mut headers: HashMap<Hash, (Header, Vec<u8>)> = HashMap::new();
    read_object_into_headers(&store, &mut headers, &index.tree)
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;

    let mut offset: u64 = 0;
    let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::new();

    for (hash, (header, data)) in &headers {
        let prefix_length = header.to_string().len() as u64;
        let length = prefix_length + data.len() as u64;

        header_entries.push(ArchiveHeaderEntry {
            hash: hash.clone(),
            index: offset,
            length,
        });

        offset += length;
    }

    let archive = Archive {
        header: HEADER,
        compression,
        hash: index_hash.clone(),
        index,
        body: ArchiveBody {
            header: header_entries,
            entries: headers
                .into_iter()
                .map(|(_hash, (header, data))| {
                    let prefix = header.to_string();
                    let mut full_data = Vec::with_capacity(prefix.len() + data.len());
                    full_data.extend_from_slice(prefix.as_bytes());
                    full_data.extend(data);
                    RawEntryData(full_data)
                })
                .collect(),
        },
    };

    let body = stream_archive(archive);

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
    tracing::info!(hash = %index_hash, requested_hashes = request.hashes.len(), "POST /supplemental - building");

    let compression = match query.compression.as_deref() {
        Some(s) => s
            .parse::<Compression>()
            .map_err(|_| ServerError::BadRequest("invalid compression type".into()))?,
        None => Compression::Zstd,
    };

    let index = read_index_from_store(&store, &index_hash).await?;

    #[allow(clippy::mutable_key_type)]
    let mut headers: HashMap<Hash, (Header, Vec<u8>)> = HashMap::new();
    read_object_into_headers(&store, &mut headers, &index.tree)
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;

    // Also include the index object itself
    let mut index_obj = store
        .get_object(&index_hash)
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;
    let index_header = index_obj.header;
    let mut index_body = Vec::new();
    index_obj
        .read_to_end(&mut index_body)
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;
    headers.insert(index_hash.clone(), (index_header, index_body));

    // Build the set of hashes to include:
    // - All requested hashes
    // - All tree-type objects (for directory structure)
    // - The index object itself
    #[allow(clippy::mutable_key_type)]
    let mut included: std::collections::HashSet<Hash> = std::collections::HashSet::new();
    included.insert(index_hash.clone());
    for (hash, (header, _)) in &headers {
        if header.object_type == ObjectType::Tree {
            included.insert(hash.clone());
        }
    }
    for hash in &request.hashes {
        included.insert(hash.clone());
    }

    // Build header entries and raw entries in a single pass for consistent ordering
    let mut offset: u64 = 0;
    let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::new();
    let mut raw_entries: Vec<RawEntryData> = Vec::new();

    for (hash, (header, data)) in headers {
        if !included.contains(&hash) {
            continue;
        }
        let prefix = header.to_string();
        let length = prefix.len() as u64 + data.len() as u64;

        header_entries.push(ArchiveHeaderEntry {
            hash,
            index: offset,
            length,
        });

        let mut full_data = Vec::with_capacity(prefix.len() + data.len());
        full_data.extend_from_slice(prefix.as_bytes());
        full_data.extend(data);
        raw_entries.push(RawEntryData(full_data));

        offset += length;
    }

    tracing::debug!(entries = header_entries.len(), "supplemental archive built");

    let archive = Archive {
        header: SUPPLEMENTAL_HEADER,
        compression,
        hash: index_hash.clone(),
        index,
        body: ArchiveBody {
            header: header_entries,
            entries: raw_entries,
        },
    };

    let body = stream_archive(archive);

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

#[allow(clippy::type_complexity)]
fn collect_files<'a>(
    store: &'a Store,
    tree_hash: &'a Hash,
    prefix: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<(String, Vec<u8>)>>> + Send + 'a>>
{
    Box::pin(async move {
        let mut store_obj = store.get_object(tree_hash).await?;
        let mut body = Vec::new();
        store_obj.read_to_end(&mut body).await?;
        let tree = Tree::from_data(&body)?;

        let mut files: Vec<(String, Vec<u8>)> = Vec::new();

        for entry in &tree.contents {
            let entry_path = if prefix.is_empty() {
                entry.path.clone()
            } else {
                format!("{}/{}", prefix, entry.path)
            };

            match entry.mode {
                Mode::Tree => {
                    let sub_files = collect_files(store, &entry.hash, &entry_path).await?;
                    files.extend(sub_files);
                }
                _ => {
                    let mut blob_obj = store.get_object(&entry.hash).await?;
                    let mut data = Vec::new();
                    blob_obj.read_to_end(&mut data).await?;
                    files.push((entry_path, data));
                }
            }
        }

        Ok(files)
    })
}

#[debug_handler]
async fn get_zip(
    AxumPath(index_hash): AxumPath<Hash>,
    State(ServerState { store }): State<ServerState>,
) -> Result<Response<Body>, ServerError> {
    tracing::info!(hash = %index_hash, "GET /zip - building zip");

    let index = read_index_from_store(&store, &index_hash).await?;

    let files = collect_files(&store, &index.tree, "")
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;

    // Zip requires Write+Seek, so we build in memory then stream the result
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(STREAM_CHANNEL_CAPACITY);
    let short_hash = index_hash.as_str()[..12].to_string();

    tokio::task::spawn_blocking(move || {
        let mut buf = Vec::new();
        {
            let cursor = std::io::Cursor::new(&mut buf);
            let mut zip_writer = zip::ZipWriter::new(cursor);
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);

            for (path, data) in &files {
                if let Err(e) = zip_writer.start_file(path, options) {
                    tracing::error!(error = %e, "zip start_file failed");
                    return;
                }
                if let Err(e) = zip_writer.write_all(data) {
                    tracing::error!(error = %e, "zip write failed");
                    return;
                }
            }

            if let Err(e) = zip_writer.finish() {
                tracing::error!(error = %e, "zip finish failed");
                return;
            }
        }

        // Stream the completed zip in chunks
        for chunk in buf.chunks(STREAM_CHUNK_SIZE) {
            if tx.blocking_send(Ok(chunk.to_vec())).is_err() {
                return;
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
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

    let mut added: usize = 0;
    let mut skipped: usize = 0;

    for (header_entry, raw) in archive.body.header.iter().zip(archive.body.entries.iter()) {
        let hash = &header_entry.hash;

        match store.exists(hash).await {
            Ok(true) => {
                skipped += 1;
                continue;
            }
            Ok(false) => {}
            Err(err) => return Err(ServerError::Internal(err.to_string())),
        }

        let (header, body_bytes) = read_header_and_body(&raw.0)
            .ok_or_else(|| ServerError::BadRequest("invalid entry data: could not parse header".into()))?;

        store
            .put_object_bytes(hash, header, body_bytes.to_vec())
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))?;

        added += 1;
    }

    tracing::info!(added = added, skipped = skipped, "POST /upload - complete");

    Ok(format!("Added {} objects, skipped {}", added, skipped))
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

    let mut missing = Vec::new();
    for hash in request.hashes {
        match store.exists(&hash).await {
            Ok(true) => {}
            _ => missing.push(hash),
        }
    }

    tracing::debug!(missing = missing.len(), "POST /missing - result");

    Ok(axum::Json(MissingResponse { missing }))
}

#[debug_handler]
async fn get_metadata(
    AxumPath(index_hash): AxumPath<Hash>,
    State(ServerState { store }): State<ServerState>,
) -> Result<axum::Json<common::TreeMetadata>, ServerError> {
    tracing::debug!(hash = %index_hash, "GET /metadata - collecting metadata");

    match store.exists(&index_hash).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(ServerError::NotFound("index not found".into())),
        Err(err) => Err(ServerError::Internal(err.to_string())),
    }?;

    let meta = collect_tree_metadata(&store, &index_hash)
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;

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
