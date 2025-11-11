use axum::{
    body::{Body, BodyDataStream},
    debug_handler,
    extract::{Path as AxumPath, Request, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{get, put},
    Router,
};
use clap::Parser;
use common::{Hash, Header, ObjectType};
use lazy_static::lazy_static;
use sha2::{Digest, Sha512};
use std::{
    collections::HashSet,
    fs::{self, create_dir, remove_file},
    io::Write,
    path::{Path, PathBuf},
    sync::RwLock,
};
use tokio::{fs::File, io::{AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter}};
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;

lazy_static! {
    static ref INDEXES: RwLock<HashSet<Hash>> = Default::default();
    static ref TREES: RwLock<HashSet<Hash>> = Default::default();
    static ref BLOBS: RwLock<HashSet<Hash>> = Default::default();
}

async fn read_cache<P: AsRef<Path>>(path: P) {
    let mut indexes = INDEXES.try_write().unwrap();
    let mut trees = TREES.try_write().unwrap();
    let mut blobs = BLOBS.try_write().unwrap();

    let mut total_size: u128 = 0;

    for entry in fs::read_dir(path).unwrap().filter_map(|x| x.ok()) {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };

        if metadata.is_file() {
            continue;
        }

        let prefix = entry.file_name();

        for entry in fs::read_dir(entry.path()).unwrap().filter_map(|x| x.ok()) {
            let Ok(metadata) = entry.metadata() else {
                continue;
            };

            if !metadata.is_file() {
                continue;
            }

            let name = format!(
                "{}{}",
                prefix.to_string_lossy(),
                entry.file_name().to_string_lossy()
            );
            let hash = Hash::from(&name);

            let Ok(file) = File::open(entry.path()).await else {
                continue;
            };
            let mut reader = BufReader::new(file);
            
            let Ok(Header { object_type, size }) = Header::read_from_async(&mut reader).await else {
                panic!("Corrupt file {:?}", entry.path());
            };

            total_size += size as u128;

            match object_type {
                common::ObjectType::Blob => &mut blobs,
                common::ObjectType::Tree => &mut trees,
                common::ObjectType::Index => &mut indexes,
            }
            .insert(hash);
        }
    }

    println!("Loaded {} blobs", blobs.len());
    println!("Loaded {} trees", trees.len());
    println!("Loaded {} indexes", indexes.len());
    println!("Total Size: {} bytes", total_size);

    indexes.iter().for_each(|i| println!("Index {i}"));
}

#[derive(Clone)]
struct ServerState {
    cache_path: PathBuf,
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

async fn write_body_to_file(
    path: &PathBuf,
    hash: &Hash,
    object_type: ObjectType,
    object_size: u64,
    mut body: BodyDataStream,
) -> Result<(), ErrorResult> {
    assert!(
        !path.exists(),
        "Race condition. Someone else has created this file before us"
    );

    let header = Header::new(object_type, object_size);

    let file = File::create(path).await?;
    let mut writer = BufWriter::new(file);
    let mut hasher = Sha512::new();

    header.write_to(&mut hasher)?;
    header.write_to_async(&mut writer).await?;

    let mut length: u64 = 0;

    while let Some(chunk) = body.next().await {
        let chunk = match chunk {
            Ok(v) => v,
            Err(err) => return Err(ErrorResult::InternalError(err.to_string())),
        };

        hasher.write_all(&chunk)?;
        writer.write_all(&chunk).await?;
        length += chunk.len() as u64;
    }

    if object_size != length {
        writer.flush().await?;
        remove_file(path)?;
        return Err(ErrorResult::LengthDoesntMatch);
    }

    let new_hash = Hash::from(hasher);

    // the hashes don't match
    if *hash != new_hash {
        writer.flush().await?;
        remove_file(path)?;
        return Err(ErrorResult::HashDoesntMatch);
    }

    Ok(())
}

#[debug_handler]
async fn put_object(
    AxumPath(object_id): AxumPath<String>,
    State(state): State<ServerState>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<StatusCode, (StatusCode, String)> {
    let Some(hash) = Hash::from_string(&object_id) else {
        return Err((StatusCode::BAD_REQUEST, "Invalid Sha512 hash".into()));
    };

    let object_path = hash.get_path(&state.cache_path);

    if object_path.exists() {
        return Err((StatusCode::OK, "Object already exists".into()));
    }

    let Some(parent) = object_path.parent() else {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, "Impossible state".into()));
    };

    create_dir(parent).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

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

    let data_stream = request.into_body().into_data_stream();

    if let Err(err) =
        write_body_to_file(&object_path, &hash, object_type, object_size, data_stream).await
    {
        return Err(err.get_response());
    }

    Ok(StatusCode::CREATED)
}

#[debug_handler]
async fn get_object(
    AxumPath(object_id): AxumPath<String>,
    State(state): State<ServerState>,
) -> Result<Response<Body>, (StatusCode, String)> {
    let Some(hash) = Hash::from_string(&object_id) else {
        return Err((StatusCode::BAD_REQUEST, "Invalid Sha512 hash".into()));
    };

    let object_path = hash.get_path(&state.cache_path);

    if !object_path.exists() {
        return Err((
            StatusCode::NO_CONTENT,
            "No object with this hash exists".into(),
        ));
    }

    let file = tokio::fs::File::open(object_path).await.map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    let mut reader = BufReader::new(file);
    let Header { object_type, size } = Header::read_from_async(&mut reader).await.map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
    
    reader.rewind().await.map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, format!("Unable to read file: \"{err}\"")))?;
    
    let reader = ReaderStream::new(reader);
    let s = Body::from_stream(reader);
    let mut response = Response::new(s);

    let headers = response.headers_mut();
    headers.insert("Object-Type", object_type.to_str().parse().unwrap());
    headers.insert("Object-Size", size.to_string().parse().unwrap());

    return Ok(response);
}

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

    let Cli { store, port, ..} = Cli::parse();

    if !store.exists() {
        create_dir(&store).expect("Cache directory to exist");
    }

    read_cache(&store).await;

    // build our application with a single route
    let app = Router::new()
        .route("/object/{object_id}", put(put_object))
        .route("/object/{object_id}", get(get_object))
        .with_state(ServerState {
            cache_path: store,
        })
        .route("/", get(|| async { "Hello, World!" }));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await.unwrap();

    println!("Listening at http://{}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
