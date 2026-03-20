use std::collections::HashMap;
use std::io::{Cursor, Write};

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
    archive::{Archive, ArchiveBody, ArchiveHeaderEntry, Compression, RawEntryData, HEADER, SUPPLEMENTAL_HEADER},
    collect_index_metadata,
    object_body::{Index, Object as _, Tree},
    read_header_and_body, read_object_into_headers,
    store::{Store, StoreObject},
};
use serde::Deserialize;

use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

use futures::{AsyncReadExt, StreamExt, TryStreamExt};

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
        let (status, msg) = match self {
            ServerError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ServerError::AlreadyExists(m) => (StatusCode::OK, m),
            ServerError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ServerError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
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

    Ok(StatusCode::CREATED)
}

#[debug_handler]
async fn get_object(
    AxumPath(object_hash): AxumPath<Hash>,
    State(ServerState { store }): State<ServerState>,
) -> Result<Response<Body>, ServerError> {
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

    let mut buf = Vec::new();
    archive
        .to_data(&mut Cursor::new(&mut buf))
        .map_err(|err| ServerError::Internal(err.to_string()))?;

    let short_hash = &index_hash.as_str()[..12];
    let mut response = Response::new(Body::from(buf));
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

    let mut buf = Vec::new();
    archive
        .to_data(&mut Cursor::new(&mut buf))
        .map_err(|err| ServerError::Internal(err.to_string()))?;

    let short_hash = &index_hash.as_str()[..12];
    let mut response = Response::new(Body::from(buf));
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
    let index = read_index_from_store(&store, &index_hash).await?;

    let files = collect_files(&store, &index.tree, "")
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;

    let mut buf = Vec::new();
    {
        let cursor = Cursor::new(&mut buf);
        let mut zip_writer = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);

        for (path, data) in &files {
            zip_writer
                .start_file(path, options)
                .map_err(|err| ServerError::Internal(err.to_string()))?;
            zip_writer
                .write_all(data)
                .map_err(|err| ServerError::Internal(err.to_string()))?;
        }

        zip_writer
            .finish()
            .map_err(|err| ServerError::Internal(err.to_string()))?;
    }

    let short_hash = &index_hash.as_str()[..12];
    let mut response = Response::new(Body::from(buf));
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
    let mut missing = Vec::new();
    for hash in request.hashes {
        match store.exists(&hash).await {
            Ok(true) => {}
            _ => missing.push(hash),
        }
    }
    Ok(axum::Json(MissingResponse { missing }))
}

#[debug_handler]
async fn get_metadata(
    AxumPath(index_hash): AxumPath<Hash>,
    State(ServerState { store }): State<ServerState>,
) -> Result<axum::Json<serde_json::Value>, ServerError> {
    match store.exists(&index_hash).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(ServerError::NotFound("index not found".into())),
        Err(err) => Err(ServerError::Internal(err.to_string())),
    }?;

    let meta = collect_index_metadata(&store, &index_hash)
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;

    let json = serde_json::json!({
        "index_hash": meta.index_hash,
        "tree_hash": meta.tree_hash,
        "timestamp": meta.timestamp.to_rfc3339(),
        "metadata": meta.metadata,
        "files": meta.files.iter().map(|f| {
            serde_json::json!({
                "path": f.path,
                "hash": f.hash,
                "size": f.size,
                "mode": f.mode.as_str(),
            })
        }).collect::<Vec<_>>(),
        "objects": meta.objects.iter().map(|o| {
            serde_json::json!({
                "hash": o.hash,
                "type": o.object_type.to_str(),
                "size": o.size,
            })
        }).collect::<Vec<_>>(),
    });

    Ok(axum::Json(json))
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
