use axum::{
    body::Body,
    debug_handler,
    extract::{Path as AxumPath, Request, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{get, put},
    Router,
};
use clap::Parser;
use common::{
    store::{Store, StoreObject},
    Hash, Header, ObjectType,
};
use futures::TryStreamExt;
use opendal::Operator;
use std::{fs::create_dir, path::PathBuf};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;
use tower_http::compression::CompressionLayer;

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
}

enum ErrorResult {
    HashDoesntMatch,
    LengthDoesntMatch,
    InternalError(String),
}

impl ErrorResult {
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
    State(ServerState { store }): State<ServerState>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<StatusCode, (StatusCode, String)> {
    match store.exists(&object_hash).await {
        Ok(true) => Ok(()),
        Ok(false) => Err((StatusCode::OK, "Object already exists".into())),
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

    store
        .put_object(&object_hash, store_object)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    Ok(StatusCode::CREATED)
}

#[debug_handler]
async fn get_object(
    AxumPath(object_hash): AxumPath<Hash>,
    State(ServerState { store }): State<ServerState>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let store_object = store.get_object(&object_hash).await.map_err(|_| {
        (
            StatusCode::NO_CONTENT,
            "No object with this hash exists".into(),
        )
    })?;

    let header = store_object.header();
    let object_type = header.object_type;
    let size = header.size;

    let reader_stream = ReaderStream::new(store_object.compat());
    let mut response = Response::new(Body::from_stream(reader_stream));

    let headers = response.headers_mut();
    headers.insert("Object-Type", object_type.to_str().parse().unwrap());
    headers.insert("Object-Size", size.to_string().parse().unwrap());

    Ok(response)
}

// #[debug_handler]
// async fn get_bundle(
//     AxumPath(index_hash): AxumPath<Hash>,
//     State(ServerState { store }): State<ServerState>,
// ) -> Result<Response<Body>, (StatusCode, String)> {
//     // let object_path = index_hash.get_path(&cache_path);

//     let index_object = store.get_object(&index_hash).await;

//     let Ok(index_object) = index_object else {
//         return Err((
//             StatusCode::NO_CONTENT,
//             "No object with this hash exists".into(),
//         ));
//     };

//     let header = index_object.header();
//     if header.object_type != ObjectType::Index {
//         return Err((
//             StatusCode::BAD_REQUEST,
//             "Object requested is not an index".into(),
//         ));
//     }

//     let index = Index::from_data(&index_object.to_data());

//     let mut headers = HashMap::new();

//     let _ = read_object_into_headers(&cache_path, &mut headers, &index.tree);

//     //TODO: Surely there is an algorithm to more efficiently lay out this data
//     let mut i = 0;
//     let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::new();

//     for (hash, header) in &headers {
//         let prefix_length = header.to_string().len() as u64;
//         let total_length = header.size + prefix_length;

//         header_entries.push(ArchiveHeaderEntry {
//             hash: hash.clone(),
//             index: i,
//             length: total_length,
//         });

//         i += total_length;
//     }

//     let archive = Archive {
//         header: HEADER,
//         compression: common::archive::Compression::None,
//         hash: index_hash.clone(),
//         index: index,
//         body: ArchiveBody {
//             header: header_entries,
//             entries: headers
//                 .into_iter()
//                 .map(|(hash, _header)| FileEntryData(hash.get_path(&cache_path)))
//                 .collect(),
//         },
//     };

//     let mut body = Vec::new();

//     archive
//         .to_data(&mut body)
//         .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

//     let mut response = Response::new(body.into());
//     let headers = response.headers_mut();
//     headers.insert(
//         "Content-Type",
//         HeaderValue::from_str("application/arc").unwrap(),
//     );
//     headers.insert(
//         "Content-Disposition",
//         HeaderValue::from_str(&format!(
//             "Content-Disposition: attachment; filename=\"{index_hash}.ar\""
//         ))
//         .unwrap(),
//     );

//     return Ok(response);
// }

#[derive(Parser)]
#[clap(version, about, long_about = None)]
pub struct Cli {
    /// Increase verbosity, and can be used multiple times
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    #[arg(short, long, default_value_t = 1287)]
    pub port: u16,

    pub store: PathBuf,
}

#[tokio::main]
async fn main() {
    let Cli { store, port, .. } = Cli::parse();

    if !store.exists() {
        create_dir(&store).expect("Cache directory to exist");
    }

    let store = opendal::services::Fs::default()
        .root(store.to_str().expect("Path to be valid utf8 string"));

    let operator = Operator::new(store).unwrap().finish();

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
        // .route("/bundle/{index_id}", get(get_bundle))
        .with_state(ServerState {
            store: Store::new(operator),
        })
        .layer(comression_layer)
        .route("/", get(|| async { "Hello, World!" }));

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap();

    println!("Listening at http://{}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
