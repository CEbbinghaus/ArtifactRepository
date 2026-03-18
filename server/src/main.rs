use axum::{
    body::Body,
    debug_handler,
    extract::{Path as AxumPath, Request, State},
    http::{HeaderMap, Response, StatusCode},
    routing::{get, put},
    Router,
};
use clap::Parser;
use common::{
    Hash, Header, ObjectType, store::{Store, StoreObject}
};

use std::{
    fs::create_dir,
    path::PathBuf,
};
use tower_http::compression::CompressionLayer;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;

use futures::{StreamExt, TryStreamExt};

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
        Ok(true) => Err(ServerError::AlreadyExists("Object already exists".into())),
        Err(err) => Err(ServerError::Internal(err.to_string())),
    }?;

    let Some(object_type) = headers.get("Object-Type").and_then(|v| v.to_str().ok()) else {
        return Err(ServerError::BadRequest("Missing Object-Type Header".into()));
    };

    let Some(object_type) = ObjectType::from_str(object_type) else {
        return Err(ServerError::BadRequest("Invalid Object-Type Header".into()));
    };

    let Some(object_size) = headers.get("Object-Size").and_then(|v| v.to_str().ok()) else {
        return Err(ServerError::BadRequest("Missing Object-Size Header".into()));
    };

    let Some(object_size): Option<u64> = object_size.parse().ok() else {
        return Err(ServerError::BadRequest("Invalid Object-Size Header".into()));
    };
    let header = Header::new(object_type, object_size);
    let data_stream = request.into_body().into_data_stream();

    let buffered_reader = data_stream.map(|result| {
        result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
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
async fn main() -> anyhow::Result<()> {
    let Cli { store, port, .. } = Cli::parse();

    if !store.exists() {
        create_dir(&store).expect("Failed to create store directory");
    }

    let store = opendal::services::Fs::default().root(store.to_str().expect("valid path"));

    let compression_layer: CompressionLayer = CompressionLayer::new()
        .br(true)
        .deflate(true)
        .gzip(true)
        .zstd(true);

    let app = Router::new()
        .route("/object/{object_id}", put(put_object))
        .route("/object/{object_id}", get(get_object))
        .with_state(ServerState {
            store: Store::from_builder(store).expect("Failed to create store"),
        })
        .layer(compression_layer);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;

    println!("Listening at http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}
