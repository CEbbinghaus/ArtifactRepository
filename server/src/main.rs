use axum::{
    body::Body,
    debug_handler,
    extract::{DefaultBodyLimit, Path as AxumPath, Request, State},
    http::{HeaderMap, HeaderValue, Response, StatusCode},
    routing::{get, put},
    Router,
};
use clap::Parser;
use common::{
    archive::{Archive, ArchiveBody, ArchiveHeaderEntry, StoreEntryData, HEADER},
    object_body::{Index, Object},
    read_object_into_headers,
    store::{Store, StoreObject},
    Hash, Header, ObjectType,
};
use futures::{AsyncReadExt, TryStreamExt};
use std::{collections::HashMap, fs::create_dir, path::PathBuf};
use tower_http::{compression::CompressionLayer, trace::TraceLayer};

use crate::config::{Config, StoreConfig};
use crate::logging::configure_tracing;

mod config;
mod logging;

// lazy_static! {
//     static ref INDEXES: RwLock<HashSet<Hash>> = Default::default();
//     static ref TREES: RwLock<HashSet<Hash>> = Default::default();
//     static ref BLOBS: RwLock<HashSet<Hash>> = Default::default();
// }

// async fn read_cache(store: &Store) {
//     let mut indexes = INDEXES.try_write().unwrap();
//     let mut trees = TREES.try_write().unwrap();
//     let mut blobs = BLOBS.try_write().unwrap();

//     store.

//     let mut total_size: u128 = 0;

//     for entry in fs::read_dir(path).unwrap().filter_map(|x| x.ok()) {
//         let Ok(metadata) = entry.metadata() else {
//             continue;
//         };

//         if metadata.is_file() {
//             continue;
//         }

//         let prefix = entry.file_name();

//         for entry in fs::read_dir(entry.path()).unwrap().filter_map(|x| x.ok()) {
//             let Ok(metadata) = entry.metadata() else {
//                 continue;
//             };

//             if !metadata.is_file() {
//                 continue;
//             }

//            let Ok(file) = File::open(entry.path()).await else {
//                continue;
//            };
//            let mut reader = BufReader::new(file).compat();

//             let Ok(file) = File::open(entry.path()).await else {
//                 continue;
//             };
//             let mut reader = BufReader::new(file);

//             let Ok(Header { object_type, size }) = Header::read_from_async(&mut reader).await else {
//                 panic!("Corrupt file {:?}", entry.path());
//             };

//             total_size += size as u128;

//             match object_type {
//                 common::ObjectType::Blob => &mut blobs,
//                 common::ObjectType::Tree => &mut trees,
//                 common::ObjectType::Index => &mut indexes,
//             }
//             .insert(hash);
//         }
//     }

//     println!("Loaded {} blobs", blobs.len());
//     println!("Loaded {} trees", trees.len());
//     println!("Loaded {} indexes", indexes.len());
//     println!("Total Size: {} bytes", total_size);

//     indexes.iter().for_each(|i| println!("Index {i}"));
// }

#[derive(Clone)]
struct ServerState {
    store: Store,
    config: Config,
}

#[allow(dead_code)]
enum ErrorResult {
    HashDoesntMatch,
    LengthDoesntMatch,
    InternalError(String),
}

impl ErrorResult {
    #[allow(dead_code)]
    fn get_response(&self) -> (StatusCode, String) {
        match self {
            ErrorResult::HashDoesntMatch => (
                StatusCode::BAD_REQUEST,
                "Hash provided does not match the hash of the content".into(),
            ),
            ErrorResult::LengthDoesntMatch => (
                StatusCode::BAD_REQUEST,
                "Length provided does not match the body length".into(),
            ),
            ErrorResult::InternalError(value) => (StatusCode::INTERNAL_SERVER_ERROR, value.clone()),
        }
    }
}

impl From<std::io::Error> for ErrorResult {
    fn from(value: std::io::Error) -> Self {
        ErrorResult::InternalError(value.to_string())
    }
}

// async fn write_body_to_file(
//     path: &PathBuf,
//     hash: &Hash,
//     object_type: ObjectType,
//     object_size: u64,
//     mut body: BodyDataStream,
// ) -> Result<(), ErrorResult> {
//     assert!(
//         !path.exists(),
//         "Race condition. Someone else has created this file before us"
//     );

//     let header = Header::new(object_type, object_size);

// let file = File::create(path).await?;
// let mut writer = BufWriter::new(file).compat_write();
// let mut hasher = Sha512::new();

//     header.write_to(&mut hasher)?;
//     header.write_to_async(&mut writer).await?;

// let mut writer = writer.into_inner();

// let mut length: u64 = 0;

//     while let Some(chunk) = body.next().await {
//         let chunk = match chunk {
//             Ok(v) => v,
//             Err(err) => return Err(ErrorResult::InternalError(err.to_string())),
//         };

//         hasher.write_all(&chunk)?;
//         writer.write_all(&chunk).await?;
//         length += chunk.len() as u64;
//     }

//     if object_size != length {
//         writer.flush().await?;
//         remove_file(path)?;
//         return Err(ErrorResult::LengthDoesntMatch);
//     }

//     let new_hash = Hash::from(hasher);

//     // the hashes don't match
//     if *hash != new_hash {
//         writer.flush().await?;
//         remove_file(path)?;
//         return Err(ErrorResult::HashDoesntMatch);
//     }

//     Ok(())
// }

#[debug_handler]
async fn put_object(
    AxumPath(object_hash): AxumPath<Hash>,
    State(ServerState { store, .. }): State<ServerState>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<StatusCode, (StatusCode, String)> {
    match store.exists(&object_hash).await {
        Ok(false) => Ok(()),
        Ok(true) => Err((StatusCode::OK, "Object already exists".into())),
        Err(err) => Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }?;

    let Some(object_type) = headers.get("Object-Type").and_then(|v| v.to_str().ok()) else {
        return Err((StatusCode::BAD_REQUEST, "Missing Object-Type Header".into()));
    };

    let Some(object_type) = ObjectType::from_str(object_type) else {
        return Err((StatusCode::BAD_REQUEST, "Invalid Object-Type Header".into()));
    };

    let Some(object_size) = headers.get("Object-Size").and_then(|v| v.to_str().ok()) else {
        return Err((StatusCode::BAD_REQUEST, "Missing Object-Size Header".into()));
    };

    let Some(object_size): Option<u64> = object_size.parse().ok() else {
        return Err((StatusCode::BAD_REQUEST, "Invalid Object-Size Header".into()));
    };
    let header = Header::new(object_type, object_size);
    let data_stream = request.into_body().into_data_stream();

    let buffered_reader = data_stream.map_err(std::io::Error::other).into_async_read();

    let store_object = StoreObject::new_with_header(header, buffered_reader);

    // Content-addressable writes are idempotent. A concurrent PUT for the
    // same hash can race past the exists() check above; opendal's Fs
    // backend then errors with "writer got too much data" for the loser.
    // Treat any put_object failure as success if the object now exists.
    if let Err(err) = store.put_object(&object_hash, store_object).await {
        if !store.exists(&object_hash).await.unwrap_or(false) {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()));
        }
        return Ok(StatusCode::OK);
    }

    Ok(StatusCode::CREATED)
}

#[debug_handler]
async fn get_object(
    AxumPath(object_hash): AxumPath<Hash>,
    State(ServerState { store, .. }): State<ServerState>,
) -> Result<Response<Body>, (StatusCode, String)> {
    match store.exists(&object_hash).await {
        Ok(true) => Ok(()),
        Ok(false) => Err((StatusCode::NO_CONTENT, "no object".into())),
        Err(err) => Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }?;

    let object = store
        .get_object(&object_hash)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    let Header { object_type, size } = object.header;

    let reader_stream = futures::stream::unfold(object, |mut reader| async move {
        let mut buffer = vec![0u8; 8192];
        match reader.read(&mut buffer).await {
            Ok(0) => None,
            Ok(n) => {
                buffer.truncate(n);
                Some((Ok(buffer), reader))
            }
            Err(e) => Some((Err(e), reader)),
        }
    });
    let mut response = Response::new(Body::from_stream(reader_stream));

    let headers = response.headers_mut();
    headers.insert("Object-Type", object_type.to_str().parse().unwrap());
    headers.insert("Object-Size", size.to_string().parse().unwrap());

    Ok(response)
}

#[debug_handler]
async fn get_bundle(
    AxumPath(index_hash): AxumPath<Hash>,
    State(ServerState { store, config }): State<ServerState>,
) -> Result<Response<Body>, (StatusCode, String)> {
    match store.exists(&index_hash).await {
        Ok(true) => Ok(()),
        Ok(false) => Err((StatusCode::NO_CONTENT, "no object".into())),
        Err(err) => Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }?;

    let mut object = store
        .get_object(&index_hash)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    if object.header.object_type != ObjectType::Index {
        return Err((
            StatusCode::BAD_REQUEST,
            "Object requested is not an index".into(),
        ));
    }

    let mut index_data = Vec::new();

    object
        .read_to_end(&mut index_data)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    let index = Index::from_data(&index_data);

    let mut headers = HashMap::new();

    println!("Reading objects for index {}", index_hash);
    read_object_into_headers(&store, &mut headers, &index.tree)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    println!("Finished reading {} objects from index", headers.len());

    let mut i = 0;
    let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::new();

    for (hash, header) in &headers {
        let prefix_length = header.to_string().len() as u64;
        let total_length = header.size + prefix_length;

        header_entries.push(ArchiveHeaderEntry {
            hash: hash.clone(),
            index: i,
            length: total_length,
        });

        i += total_length;
    }

    let archive = Archive {
        header: HEADER,
        compression: config.archive.compression_format,
        hash: index_hash.clone(),
        index,
        body: ArchiveBody {
            header: header_entries,
            entries: headers
                .keys()
                .map(|hash| StoreEntryData {
                    store: store.clone(),
                    hash: hash.clone(),
                })
                .collect(),
        },
    };

    let mut body = Vec::new();

    archive
        .to_data(config.archive.compression_level, &mut body)
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    let mut response = Response::new(Body::from(body));
    let headers = response.headers_mut();
    headers.insert(
        "Content-Type",
        HeaderValue::from_str("application/arc").unwrap(),
    );
    headers.insert(
        "Content-Disposition",
        HeaderValue::from_str(&format!(
            "Content-Disposition: attachment; filename=\"{index_hash}.ar\""
        ))
        .unwrap(),
    );

    Ok(response)
}

#[derive(Parser)]
#[clap(version, about, long_about = None)]
pub struct Cli {
    /// Increase verbosity, and can be used multiple times
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Decrease verbosity, and can be used multiple times
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub quiet: u8,

    #[arg(long, action = clap::ArgAction::SetTrue, conflicts_with = "json")]
    pub plain: Option<bool>,

    #[arg(long, action = clap::ArgAction::SetTrue, conflicts_with = "plain")]
    pub json: Option<bool>,

    /// Path to a TOML configuration file. CLI flags take precedence over values in the file.
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Path to the object store directory (filesystem backend). Overrides the
    /// config file. Required unless --config specifies a `[store]` section.
    pub store: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    let config = Config::load(args.config.as_deref(), &args)?;

    configure_tracing(&config.logging);

    let bind = config.server.bind;

    let store_root: PathBuf = match &config.store {
        Some(StoreConfig::Fs { root }) => PathBuf::from(root),
        None => anyhow::bail!(
            "no store configured: pass a store path as a positional arg, set [store] in --config, or set ARXSRV_STORE_BACKEND / ARXSRV_STORE_ROOT"
        ),
    };

    if !store_root.exists() {
        create_dir(&store_root).expect("Cache directory to exist");
    }

    let store = opendal::services::Fs::default().root(store_root.to_str().expect("valid path"));

    // read_cache(&store).await;

    let comression_layer: CompressionLayer = CompressionLayer::new()
        .br(true)
        .deflate(true)
        .gzip(true)
        .zstd(true);

    // build our application with a single route
    let app = Router::new()
        .route("/object/{object_id}", put(put_object))
        .route("/object/{object_id}", get(get_object))
        .route("/bundle/{index_id}", get(get_bundle))
        .with_state(ServerState {
            store: Store::from_builder(store).expect("S"),
            config,
        })
        .layer(comression_layer)
        .layer(TraceLayer::new_for_http())
        .layer(DefaultBodyLimit::disable())
        .route("/", get(|| async { "Hello, World!" }));

    let listener = tokio::net::TcpListener::bind(bind).await?;

    tracing::info!("Listening at http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
