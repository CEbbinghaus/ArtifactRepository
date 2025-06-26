use common::{get_object_prefix, read_header_from_file, read_header_from_slice, Hash, ObjectType};
use lazy_static::lazy_static;
use sha2::{Digest, Sha512};
use tokio_stream::StreamExt;
use tokio_util::{bytes::{Buf, BytesMut}, codec::{Decoder, FramedRead}};
use std::{
    collections::HashSet, fs::{self, remove_file, File}, io::{BufReader, BufWriter, Write}, path::{Path, PathBuf}, sync::RwLock
};
use axum::{
    body::{Body, BodyDataStream}, debug_handler, extract::{Path as AxumPath, Request, State}, http::{HeaderMap, StatusCode}, response::Response, routing::{get, put}, Router
};

lazy_static! {
    static ref INDEXES: RwLock<HashSet<Hash>> = Default::default();
    static ref TREES: RwLock<HashSet<Hash>> = Default::default();
    static ref BLOBS: RwLock<HashSet<Hash>> = Default::default();
}

fn read_cache<P: AsRef<Path>>(path: P) {
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

            let Ok(file) = File::open(entry.path()) else {
                continue;
            };
            let mut reader = BufReader::new(file);

            let Some((object_type, size)) = read_header_from_file(&mut reader) else {
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
    cache_path: PathBuf
}

enum ErrorResult {
    HashDoesntMatch,
    LengthDoesntMatch,
    InternalError(String),
}

impl ErrorResult {
    fn get_response(&self) -> (StatusCode, String) {
        match self {
            ErrorResult::HashDoesntMatch => (StatusCode::BAD_REQUEST, "Hash provided does not match the hash of the content".into()),
            ErrorResult::LengthDoesntMatch => (StatusCode::BAD_REQUEST, "Length provided does not match the body length".into()),
            ErrorResult::InternalError(value) => (StatusCode::INTERNAL_SERVER_ERROR, value.clone()),
        }
    }
}

impl From<std::io::Error> for ErrorResult {
    fn from(value: std::io::Error) -> Self {
        ErrorResult::InternalError(value.to_string())
    }
}

async fn read_body_to_file(path: &PathBuf, hash: &Hash, object_type: ObjectType, object_size: u64, body: BodyDataStream) -> Result<(), ErrorResult> {
    assert!(!path.exists(), "Race condition. Someone else has somehow created this file before us");

    let mut body = body;

    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    let prefix = get_object_prefix(object_type, object_size);

    writer.write_all(prefix.as_bytes())?;
    

    let mut hasher = Sha512::new();
    hasher.write_all(prefix.as_bytes())?;

    let mut length: u64 = 0; 

    while let Some(chunk) = body.next().await {
        let chunk = match chunk {
            Ok(v) => v,
            Err(err) => return Err(ErrorResult::InternalError(err.to_string()))
        };

        hasher.write_all(&chunk)?;
        writer.write_all(&chunk)?;
        length += chunk.len() as u64;
    }

    if object_size != length {
        writer.flush()?;
        remove_file(path)?;
        return Err(ErrorResult::LengthDoesntMatch);
    }

    let new_hash = Hash::from(hasher);

    // the hashes don't match
    if *hash != new_hash {
        writer.flush()?;
        remove_file(path)?;
        return Err(ErrorResult::HashDoesntMatch);
    }

    Ok(())
}

#[debug_handler]
async fn put_object(AxumPath(object_id): AxumPath<String>, State(state): State<ServerState>, headers: HeaderMap, request: Request<Body>) -> Result<StatusCode, (StatusCode, String)> {
    let Some(hash) = Hash::from_string(&object_id) else {
        return Err((StatusCode::BAD_REQUEST, "Invalid Sha512 hash".into()));
    };

    let object_path = hash.get_path(&state.cache_path);

    if object_path.exists() {
        return Err((StatusCode::OK, "Object already exists".into()));
    }

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

    if let Err(err) = read_body_to_file(&object_path, &hash, object_type, object_size, data_stream).await {
        return Err(err.get_response());
    }

    Ok(StatusCode::CREATED)
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Default)]
pub struct TestCodec(Option<(ObjectType, u64)>);

impl TestCodec {
    /// Creates a new `BytesCodec` for shipping around raw bytes.
    pub fn new() -> TestCodec {
        TestCodec(None)
    }
}

impl Decoder for TestCodec {
    type Item = BytesMut;
    type Error = tokio::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<BytesMut>, tokio::io::Error> {
        if self.0 == None {
            if buf.is_empty() {
                return Err(tokio::io::Error::new(std::io::ErrorKind::Other, "No data was avaliable to read the object header from"));
            }
            
            let Some(index) = buf.iter().position(|v| *v == 0) else {
                return Err(tokio::io::Error::new(std::io::ErrorKind::Other, "First file slice did not contain object header"));
            };
            
            let Some(value) = read_header_from_slice(&buf[..index]) else {
                return Err(tokio::io::Error::new(std::io::ErrorKind::Other, "Invalid object header in start of file"));
            };

            self.0 = Some(value);

            buf.advance(index + 1);
            // stupid hack :(
            return Err(tokio::io::Error::new(std::io::ErrorKind::Other, "Not an error"));
        }

        if buf.is_empty() {
            return Ok(None);
        }

        let len = buf.len();
        Ok(Some(buf.split_to(len)))
    }
}


#[debug_handler]
async fn get_object(AxumPath(object_id): AxumPath<String>, State(state): State<ServerState>) -> Result<Response<Body>, (StatusCode, String)> {
    let Some(hash) = Hash::from_string(&object_id) else {
        return Err((StatusCode::BAD_REQUEST, "Invalid Sha512 hash".into()));
    };

    let object_path = hash.get_path(&state.cache_path);

    if !object_path.exists() {
        return Err((StatusCode::NO_CONTENT, "No object with this hash exists".into()));
    }

    let file = match tokio::fs::File::open(object_path).await  {
        Ok(v) => v,
        Err(err) => return Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
    };

    let mut stream = FramedRead::new(file, TestCodec::new());

    // We call .next() on the stream to try and read out object header from the start of the file
    stream.next().await;

    let Some((object_type, object_size)) = stream.decoder().0 else {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, "Unable to read object header from file".into()));
    };

    // we then have to call it again since reading the header required us return an Err() value
    // since None will just continue reading. This however sets the read state machine to errored
    // which requires another read to next() to reset and clear the None
    // after which it finally becomes available for reading again.
    // more info here: https://github.com/tokio-rs/tokio/blob/master/tokio-util/src/codec/framed_impl.rs#L129-L159
    stream.next().await;
    
    let s = Body::from_stream(stream);
    let mut response = Response::new(s);

    let headers = response.headers_mut();
    headers.insert("Object-Type", object_type.to_str().parse().unwrap());
    headers.insert("Object-Size", object_size.to_string().parse().unwrap());

    return Ok(response);
}


#[tokio::main]
async fn main() {
    let cache_dir = "/home/cebbinghaus/Projects/ArtifactRepository/cache";

    let cache_dir = PathBuf::from(cache_dir);

    read_cache(&cache_dir);

    // build our application with a single route
    let app = Router::new()
        .route("/object/{object_id}", put(put_object))
        .route("/object/{object_id}", get(get_object))
        .with_state(ServerState {
            cache_path: cache_dir
        })
        .route("/", get(|| async { "Hello, World!" }));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
