use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::compute_hash;
use common::store::Store;
use common::{Mode, BLOB_KEY, INDEX_KEY, TREE_KEY};
use common::object_body::{Index, Object as _, Tree, TreeEntry};
use http_body_util::BodyExt;
use server::create_router;
use std::collections::HashMap;
use tempfile::TempDir;
use tower::ServiceExt;

fn make_store(dir: &TempDir) -> Store {
    let builder = opendal::services::Fs::default().root(dir.path().to_str().unwrap());
    Store::from_builder(builder).expect("failed to create store")
}

fn put_request(hash_str: &str, body: &[u8], object_type: &str, object_size: u64) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(format!("/object/{}", hash_str))
        .header("Object-Type", object_type)
        .header("Object-Size", object_size.to_string())
        .body(Body::from(body.to_vec()))
        .unwrap()
}

fn get_request(hash_str: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(format!("/object/{}", hash_str))
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn put_get_round_trip() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let data = b"hello world";
    let hash = compute_hash("blob", data);
    let hash_str = hash.as_str();

    // PUT the object
    let resp = app
        .clone()
        .oneshot(put_request(hash_str, data, "blob", data.len() as u64))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // GET the object
    let resp = app
        .oneshot(get_request(hash_str))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("Object-Type").unwrap(), "blob");
    assert_eq!(
        resp.headers().get("Object-Size").unwrap(),
        &data.len().to_string()
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], data);
}

#[tokio::test]
async fn put_idempotent() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let data = b"idempotent data";
    let hash = compute_hash("blob", data);
    let hash_str = hash.as_str();

    // First PUT → 201 Created
    let resp = app
        .clone()
        .oneshot(put_request(hash_str, data, "blob", data.len() as u64))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Second PUT → 200 OK (already exists)
    let resp = app
        .oneshot(put_request(hash_str, data, "blob", data.len() as u64))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn get_missing_object() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let hash = compute_hash("blob", b"does not exist");
    let hash_str = hash.as_str();

    let resp = app.oneshot(get_request(hash_str)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn put_missing_object_type_header() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let data = b"no type";
    let hash = compute_hash("blob", data);
    let hash_str = hash.as_str();

    let req = Request::builder()
        .method("PUT")
        .uri(format!("/object/{}", hash_str))
        .header("Object-Size", data.len().to_string())
        .body(Body::from(data.to_vec()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn put_missing_object_size_header() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let data = b"no size";
    let hash = compute_hash("blob", data);
    let hash_str = hash.as_str();

    let req = Request::builder()
        .method("PUT")
        .uri(format!("/object/{}", hash_str))
        .header("Object-Type", "blob")
        .body(Body::from(data.to_vec()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn put_invalid_object_type() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let data = b"bad type";
    let hash = compute_hash("blob", data);
    let hash_str = hash.as_str();

    let req = Request::builder()
        .method("PUT")
        .uri(format!("/object/{}", hash_str))
        .header("Object-Type", "invalid")
        .header("Object-Size", data.len().to_string())
        .body(Body::from(data.to_vec()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn multiple_object_types() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let cases: &[(&str, &[u8])] = &[
        ("blob", b"blob content"),
        ("tree", b"tree content"),
        ("indx", b"index content"),
    ];

    for &(type_key, data) in cases {
        let hash = compute_hash(type_key, data);
        let hash_str = hash.as_str();

        // PUT
        let resp = app
            .clone()
            .oneshot(put_request(hash_str, data, type_key, data.len() as u64))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED, "PUT failed for {}", type_key);

        // GET
        let resp = app
            .clone()
            .oneshot(get_request(hash_str))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "GET failed for {}", type_key);
        assert_eq!(
            resp.headers().get("Object-Type").unwrap(),
            type_key,
            "Object-Type mismatch for {}",
            type_key
        );
        assert_eq!(
            resp.headers().get("Object-Size").unwrap(),
            &data.len().to_string(),
            "Object-Size mismatch for {}",
            type_key
        );

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], data, "Body mismatch for {}", type_key);
    }
}

/// Helper: PUT a blob, tree, and index into the store via the HTTP API.
/// Returns (blob_data, index_hash_str).
async fn put_blob_tree_index(app: &axum::Router) -> (Vec<u8>, String) {
    let blob_data = b"hello archive world";

    // 1. PUT blob
    let blob_hash = compute_hash(BLOB_KEY, blob_data);
    let resp = app
        .clone()
        .oneshot(put_request(blob_hash.as_str(), blob_data, "blob", blob_data.len() as u64))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 2. Build + PUT tree
    let tree = Tree {
        contents: vec![TreeEntry {
            mode: Mode::Normal,
            path: "file.txt".to_string(),
            hash: blob_hash,
        }],
    };
    let tree_data = tree.to_data();
    let tree_hash = compute_hash(TREE_KEY, &tree_data);
    let resp = app
        .clone()
        .oneshot(put_request(tree_hash.as_str(), &tree_data, "tree", tree_data.len() as u64))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 3. Build + PUT index
    let index = Index {
        tree: tree_hash,
        timestamp: chrono::Utc::now(),
        metadata: HashMap::new(),
    };
    let index_data = index.to_data();
    let index_hash = compute_hash(INDEX_KEY, &index_data);
    let resp = app
        .clone()
        .oneshot(put_request(index_hash.as_str(), &index_data, "indx", index_data.len() as u64))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    (blob_data.to_vec(), index_hash.as_str().to_string())
}

#[tokio::test]
async fn get_archive_round_trip() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let (_blob_data, index_hash_str) = put_blob_tree_index(&app).await;

    let req = Request::builder()
        .method("GET")
        .uri(format!("/archive/{}", index_hash_str))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("Content-Type").unwrap(),
        "application/octet-stream"
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    // .arx magic bytes: 'a', 'r', 'x', 'a'
    assert!(body.len() >= 4, "Archive response too short");
    assert_eq!(&body[..4], b"arxa", "Archive must start with .arx magic bytes");
}

#[tokio::test]
async fn get_archive_missing_index() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let fake_hash = compute_hash(INDEX_KEY, b"nonexistent");
    let req = Request::builder()
        .method("GET")
        .uri(format!("/archive/{}", fake_hash.as_str()))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_zip_round_trip() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let (_blob_data, index_hash_str) = put_blob_tree_index(&app).await;

    let req = Request::builder()
        .method("GET")
        .uri(format!("/zip/{}", index_hash_str))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("Content-Type").unwrap(),
        "application/zip"
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    // ZIP magic bytes: PK\x03\x04
    assert!(body.len() >= 4, "ZIP response too short");
    assert_eq!(&body[..4], b"PK\x03\x04", "ZIP must start with PK magic bytes");
}

#[tokio::test]
async fn get_zip_missing_index() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let fake_hash = compute_hash(INDEX_KEY, b"nonexistent");
    let req = Request::builder()
        .method("GET")
        .uri(format!("/zip/{}", fake_hash.as_str()))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
