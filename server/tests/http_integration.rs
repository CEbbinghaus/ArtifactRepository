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

// --- Metadata endpoint tests ---

#[tokio::test]
async fn get_metadata_returns_correct_json() {
    let dir = TempDir::new().unwrap();
    let store = make_store(&dir);
    let app = create_router(store.clone());

    // Create a blob
    let blob_data = b"hello world";
    let blob_hash = compute_hash(BLOB_KEY, blob_data);
    let blob_header = common::Header::new(common::ObjectType::Blob, blob_data.len() as u64);
    store
        .put_object_bytes(&blob_hash, blob_header, blob_data.to_vec())
        .await
        .unwrap();

    // Create a tree referencing the blob
    let tree = Tree {
        contents: vec![TreeEntry {
            mode: Mode::Normal,
            path: "hello.txt".into(),
            hash: blob_hash.clone(),
        }],
    };
    let tree_data = tree.to_data();
    let tree_hash = compute_hash(TREE_KEY, &tree_data);
    let tree_header = common::Header::new(common::ObjectType::Tree, tree_data.len() as u64);
    store
        .put_object_bytes(&tree_hash, tree_header, tree_data)
        .await
        .unwrap();

    // Create an index referencing the tree
    let index = Index {
        tree: tree_hash.clone(),
        timestamp: chrono::Utc::now(),
        metadata: HashMap::new(),
    };
    let index_data = index.to_data();
    let index_hash = compute_hash(INDEX_KEY, &index_data);
    let index_header = common::Header::new(common::ObjectType::Index, index_data.len() as u64);
    store
        .put_object_bytes(&index_hash, index_header, index_data)
        .await
        .unwrap();

    // GET /metadata/{index_hash}
    let req = Request::builder()
        .method("GET")
        .uri(format!("/metadata/{}", index_hash.as_str()))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["index"]["hash"], index_hash.as_str());
    assert_eq!(json["index"]["tree"], tree_hash.as_str());
    assert!(json["index"]["timestamp"].is_string());
    assert!(json["index"]["metadata"].is_object());

    // Objects is a hash-keyed map
    let objects = json["objects"].as_object().unwrap();

    // Root tree keyed by its hash
    let root = &objects[tree_hash.as_str()];
    assert_eq!(root["type"], "tree");
    let entries = root["entries"].as_object().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries["hello.txt"]["hash"], blob_hash.as_str());
    assert_eq!(entries["hello.txt"]["mode"], "100644");

    // Blob keyed by its hash
    let blob = &objects[blob_hash.as_str()];
    assert_eq!(blob["type"], "blob");
    assert_eq!(blob["size"], blob_data.len() as u64);

    // Should not have old flat fields
    assert!(json.get("tree").is_none());
    assert!(json.get("files").is_none());
}

#[tokio::test]
async fn get_metadata_missing_index_returns_404() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let fake_hash = compute_hash(INDEX_KEY, b"nonexistent");
    let req = Request::builder()
        .method("GET")
        .uri(format!("/metadata/{}", fake_hash.as_str()))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// --- Upload endpoint tests ---

use common::archive::{
    Archive, ArchiveBody, ArchiveHeaderEntry, Compression, RawEntryData, HEADER,
    SUPPLEMENTAL_HEADER,
};
use serde_json;
use sha2::{Digest, Sha512};

/// Build a serialized archive from raw entry data, using the given magic header.
fn build_test_archive(
    magic: [u8; 4],
    entries: Vec<(common::Hash, Vec<u8>)>,
) -> Vec<u8> {
    use common::object_body::Object as _;
    use chrono::Utc;

    let tree_hash = compute_hash(TREE_KEY, b"placeholder");
    let index = common::object_body::Index {
        tree: tree_hash,
        timestamp: Utc::now(),
        metadata: HashMap::new(),
    };
    let index_hash = compute_hash(INDEX_KEY, &index.to_data());

    let mut header_entries = Vec::new();
    let mut raw_entries = Vec::new();
    let mut offset: u64 = 0;

    for (hash, data) in &entries {
        // Archive stores raw SHA512 of data in header for integrity verification
        let mut hasher = Sha512::new();
        hasher.update(data);
        let archive_hash = common::Hash::from(hasher);

        header_entries.push(ArchiveHeaderEntry {
            hash: archive_hash.clone(),
            index: offset,
            length: data.len() as u64,
        });
        raw_entries.push(RawEntryData(data.clone()));
        offset += data.len() as u64;

        // Sanity: the content-addressable hash should equal the raw SHA512 of the entry data
        assert_eq!(hash.as_str(), archive_hash.as_str());
    }

    let archive = Archive {
        header: magic,
        compression: Compression::None,
        hash: index_hash,
        index,
        body: ArchiveBody {
            header: header_entries,
            entries: raw_entries,
        },
    };

    let mut buf = Vec::new();
    archive.to_data(&mut std::io::Cursor::new(&mut buf)).unwrap();
    buf
}

/// Build the full stored representation for an object: "type size\0body"
fn make_entry_data(object_type: &str, body: &[u8]) -> Vec<u8> {
    let header_str = format!("{} {}\0", object_type, body.len());
    let mut data = Vec::with_capacity(header_str.len() + body.len());
    data.extend_from_slice(header_str.as_bytes());
    data.extend_from_slice(body);
    data
}

#[tokio::test]
async fn upload_arx_and_verify_objects() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let blob_body = b"hello upload";
    let entry_data = make_entry_data("blob", blob_body);
    let content_hash = compute_hash(BLOB_KEY, blob_body);

    let archive_bytes = build_test_archive(HEADER, vec![(content_hash.clone(), entry_data)]);

    // POST the archive
    let req = Request::builder()
        .method("POST")
        .uri("/upload")
        .body(Body::from(archive_bytes))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp_body = resp.into_body().collect().await.unwrap().to_bytes();
    let resp_text = std::str::from_utf8(&resp_body).unwrap();
    assert!(resp_text.contains("Added 1"), "expected 1 added, got: {}", resp_text);

    // GET the object to verify it was stored
    let resp = app.oneshot(get_request(content_hash.as_str())).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("Object-Type").unwrap(), "blob");

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], blob_body);
}

#[tokio::test]
async fn upload_sar_and_verify_objects() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let blob_body = b"supplemental data";
    let entry_data = make_entry_data("blob", blob_body);
    let content_hash = compute_hash(BLOB_KEY, blob_body);

    let archive_bytes =
        build_test_archive(SUPPLEMENTAL_HEADER, vec![(content_hash.clone(), entry_data)]);

    // POST the archive with supplemental header
    let req = Request::builder()
        .method("POST")
        .uri("/upload")
        .body(Body::from(archive_bytes))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // GET the object to verify it was stored
    let resp = app.oneshot(get_request(content_hash.as_str())).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], blob_body);
}

#[tokio::test]
async fn upload_skips_existing_objects() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let blob_body = b"already here";
    let content_hash = compute_hash(BLOB_KEY, blob_body);

    // PUT the object first
    let resp = app
        .clone()
        .oneshot(put_request(
            content_hash.as_str(),
            blob_body,
            "blob",
            blob_body.len() as u64,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Build archive containing the same object
    let entry_data = make_entry_data("blob", blob_body);
    let archive_bytes = build_test_archive(HEADER, vec![(content_hash.clone(), entry_data)]);

    let req = Request::builder()
        .method("POST")
        .uri("/upload")
        .body(Body::from(archive_bytes))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp_body = resp.into_body().collect().await.unwrap().to_bytes();
    let resp_text = std::str::from_utf8(&resp_body).unwrap();
    assert!(resp_text.contains("skipped 1"), "expected 1 skipped, got: {}", resp_text);

    // Verify the object still exists
    let resp = app.oneshot(get_request(content_hash.as_str())).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], blob_body);
}

// --- Missing endpoint tests ---

#[tokio::test]
async fn check_missing_returns_missing_hashes() {
    let dir = TempDir::new().unwrap();
    let store = make_store(&dir);

    let data_a = b"test data a";
    let hash_a = compute_hash(BLOB_KEY, data_a);
    let header_a = common::Header::new(common::ObjectType::Blob, data_a.len() as u64);
    store.put_object_bytes(&hash_a, header_a, data_a.to_vec()).await.unwrap();

    let hash_b = compute_hash(BLOB_KEY, b"test data b");
    let hash_c = compute_hash(BLOB_KEY, b"test data c");

    let app = create_router(store);

    let body = serde_json::json!({"hashes": [hash_a.as_str(), hash_b.as_str(), hash_c.as_str()]});
    let req = Request::builder()
        .method("POST")
        .uri("/missing")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp_body = resp.into_body().collect().await.unwrap().to_bytes();
    let result: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
    let missing = result["missing"].as_array().unwrap();

    assert_eq!(missing.len(), 2);
    let missing_strs: Vec<&str> = missing.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(missing_strs.contains(&hash_b.as_str()));
    assert!(missing_strs.contains(&hash_c.as_str()));
    assert!(!missing_strs.contains(&hash_a.as_str()));
}

#[tokio::test]
async fn check_missing_empty_request() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let body = serde_json::json!({"hashes": []});
    let req = Request::builder()
        .method("POST")
        .uri("/missing")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp_body = resp.into_body().collect().await.unwrap().to_bytes();
    let result: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
    let missing = result["missing"].as_array().unwrap();
    assert!(missing.is_empty());
}

#[tokio::test]
async fn check_missing_all_present() {
    let dir = TempDir::new().unwrap();
    let store = make_store(&dir);

    let data_a = b"present data a";
    let hash_a = compute_hash(BLOB_KEY, data_a);
    let header_a = common::Header::new(common::ObjectType::Blob, data_a.len() as u64);
    store.put_object_bytes(&hash_a, header_a, data_a.to_vec()).await.unwrap();

    let data_b = b"present data b";
    let hash_b = compute_hash(BLOB_KEY, data_b);
    let header_b = common::Header::new(common::ObjectType::Blob, data_b.len() as u64);
    store.put_object_bytes(&hash_b, header_b, data_b.to_vec()).await.unwrap();

    let app = create_router(store);

    let body = serde_json::json!({"hashes": [hash_a.as_str(), hash_b.as_str()]});
    let req = Request::builder()
        .method("POST")
        .uri("/missing")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp_body = resp.into_body().collect().await.unwrap().to_bytes();
    let result: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
    let missing = result["missing"].as_array().unwrap();
    assert!(missing.is_empty());
}

// --- Supplemental endpoint tests ---

#[tokio::test]
async fn get_supplemental_returns_subset() {
    let dir = TempDir::new().unwrap();
    let store = make_store(&dir);
    let app = create_router(store.clone());

    // Create two blobs
    let blob1_data = b"first blob";
    let blob1_hash = compute_hash(BLOB_KEY, blob1_data);
    let blob1_header = common::Header::new(common::ObjectType::Blob, blob1_data.len() as u64);
    store
        .put_object_bytes(&blob1_hash, blob1_header, blob1_data.to_vec())
        .await
        .unwrap();

    let blob2_data = b"second blob";
    let blob2_hash = compute_hash(BLOB_KEY, blob2_data);
    let blob2_header = common::Header::new(common::ObjectType::Blob, blob2_data.len() as u64);
    store
        .put_object_bytes(&blob2_hash, blob2_header, blob2_data.to_vec())
        .await
        .unwrap();

    // Create tree referencing both blobs
    let tree = Tree {
        contents: vec![
            TreeEntry {
                mode: Mode::Normal,
                path: "a.txt".into(),
                hash: blob1_hash.clone(),
            },
            TreeEntry {
                mode: Mode::Normal,
                path: "b.txt".into(),
                hash: blob2_hash.clone(),
            },
        ],
    };
    let tree_data = tree.to_data();
    let tree_hash = compute_hash(TREE_KEY, &tree_data);
    let tree_header = common::Header::new(common::ObjectType::Tree, tree_data.len() as u64);
    store
        .put_object_bytes(&tree_hash, tree_header, tree_data)
        .await
        .unwrap();

    // Create index
    let index = Index {
        tree: tree_hash.clone(),
        timestamp: chrono::Utc::now(),
        metadata: HashMap::new(),
    };
    let index_data = index.to_data();
    let index_hash = compute_hash(INDEX_KEY, &index_data);
    let index_header = common::Header::new(common::ObjectType::Index, index_data.len() as u64);
    store
        .put_object_bytes(&index_hash, index_header, index_data)
        .await
        .unwrap();

    // Request supplemental with only blob1
    let body = serde_json::json!({ "hashes": [blob1_hash.as_str()] });
    let req = Request::builder()
        .method("POST")
        .uri(format!("/supplemental/{}", index_hash.as_str()))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("Content-Type").unwrap(),
        "application/octet-stream"
    );
    let content_disp = resp.headers().get("Content-Disposition").unwrap().to_str().unwrap();
    assert!(content_disp.contains(".sar"), "filename should have .sar extension");

    let resp_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    // Verify supplemental magic header
    assert!(resp_bytes.len() >= 4);
    assert_eq!(&resp_bytes[..4], b"arxs");

    // Deserialize and verify entries
    let archive = Archive::<RawEntryData>::from_data(&mut std::io::Cursor::new(&resp_bytes[..])).unwrap();
    // Should contain: blob1 + tree + index = 3 entries
    assert_eq!(archive.body.header.len(), 3, "expected 3 entries (1 blob + 1 tree + 1 index)");

    // Collect hashes in the archive
    let archive_hashes: Vec<String> = archive.body.header.iter().map(|e| e.hash.as_str().to_string()).collect();
    assert!(archive_hashes.contains(&blob1_hash.as_str().to_string()), "should contain requested blob");
    assert!(!archive_hashes.contains(&blob2_hash.as_str().to_string()), "should NOT contain unrequested blob");
    assert!(archive_hashes.contains(&tree_hash.as_str().to_string()), "should contain tree");
    assert!(archive_hashes.contains(&index_hash.as_str().to_string()), "should contain index");
}

#[tokio::test]
async fn get_supplemental_always_includes_trees() {
    let dir = TempDir::new().unwrap();
    let store = make_store(&dir);
    let app = create_router(store.clone());

    // Create a blob
    let blob_data = b"nested blob";
    let blob_hash = compute_hash(BLOB_KEY, blob_data);
    let blob_header = common::Header::new(common::ObjectType::Blob, blob_data.len() as u64);
    store
        .put_object_bytes(&blob_hash, blob_header, blob_data.to_vec())
        .await
        .unwrap();

    // Create inner tree
    let inner_tree = Tree {
        contents: vec![TreeEntry {
            mode: Mode::Normal,
            path: "file.txt".into(),
            hash: blob_hash.clone(),
        }],
    };
    let inner_tree_data = inner_tree.to_data();
    let inner_tree_hash = compute_hash(TREE_KEY, &inner_tree_data);
    let inner_tree_header =
        common::Header::new(common::ObjectType::Tree, inner_tree_data.len() as u64);
    store
        .put_object_bytes(&inner_tree_hash, inner_tree_header, inner_tree_data)
        .await
        .unwrap();

    // Create outer tree referencing inner tree
    let outer_tree = Tree {
        contents: vec![TreeEntry {
            mode: Mode::Tree,
            path: "subdir".into(),
            hash: inner_tree_hash.clone(),
        }],
    };
    let outer_tree_data = outer_tree.to_data();
    let outer_tree_hash = compute_hash(TREE_KEY, &outer_tree_data);
    let outer_tree_header =
        common::Header::new(common::ObjectType::Tree, outer_tree_data.len() as u64);
    store
        .put_object_bytes(&outer_tree_hash, outer_tree_header, outer_tree_data)
        .await
        .unwrap();

    // Create index
    let index = Index {
        tree: outer_tree_hash.clone(),
        timestamp: chrono::Utc::now(),
        metadata: HashMap::new(),
    };
    let index_data = index.to_data();
    let index_hash = compute_hash(INDEX_KEY, &index_data);
    let index_header = common::Header::new(common::ObjectType::Index, index_data.len() as u64);
    store
        .put_object_bytes(&index_hash, index_header, index_data)
        .await
        .unwrap();

    // Request supplemental with NO blob hashes
    let body = serde_json::json!({ "hashes": [] });
    let req = Request::builder()
        .method("POST")
        .uri(format!("/supplemental/{}", index_hash.as_str()))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let archive = Archive::<RawEntryData>::from_data(&mut std::io::Cursor::new(&resp_bytes[..])).unwrap();

    // Should contain: inner_tree + outer_tree + index = 3 entries (no blob)
    assert_eq!(archive.body.header.len(), 3, "expected 3 entries (2 trees + 1 index)");

    let archive_hashes: Vec<String> = archive.body.header.iter().map(|e| e.hash.as_str().to_string()).collect();
    assert!(archive_hashes.contains(&inner_tree_hash.as_str().to_string()), "should contain inner tree");
    assert!(archive_hashes.contains(&outer_tree_hash.as_str().to_string()), "should contain outer tree");
    assert!(archive_hashes.contains(&index_hash.as_str().to_string()), "should contain index");
    assert!(!archive_hashes.contains(&blob_hash.as_str().to_string()), "should NOT contain blob");
}

#[tokio::test]
async fn get_supplemental_missing_index_returns_error() {
    let dir = TempDir::new().unwrap();
    let app = create_router(make_store(&dir));

    let fake_hash = compute_hash(INDEX_KEY, b"nonexistent");
    let body = serde_json::json!({ "hashes": [] });
    let req = Request::builder()
        .method("POST")
        .uri(format!("/supplemental/{}", fake_hash.as_str()))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
