use std::collections::{BTreeMap, HashMap};

use futures::AsyncReadExt;
use serde::Serialize;

use crate::object_body::{self, Object as _};
use crate::store::Store;
use crate::{Hash, Header, Mode, ObjectType};

/// Information about a single object in the tree.
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub hash: Hash,
    pub object_type: ObjectType,
    pub size: u64,
}

/// Information about a file (blob) with its path in the tree.
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub hash: Hash,
    pub size: u64,
    pub mode: Mode,
}

/// Result of walking an index's tree.
#[derive(Debug)]
pub struct IndexMetadata {
    pub index_hash: Hash,
    pub tree_hash: Hash,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub metadata: HashMap<String, String>,
    pub files: Vec<FileInfo>,
    pub objects: Vec<ObjectInfo>,
}

/// Walk an index's tree in the store and collect all referenced objects.
pub async fn collect_index_metadata(
    store: &Store,
    index_hash: &Hash,
) -> anyhow::Result<IndexMetadata> {
    tracing::info!(index_hash = %index_hash, "collecting index metadata");
    // 1. Read and parse the Index object
    let mut index_obj = store.get_object(index_hash).await?;
    let index_header = index_obj.header;

    if index_header.object_type != ObjectType::Index {
        anyhow::bail!(
            "expected Index object, got {:?}",
            index_header.object_type
        );
    }

    let mut index_body = Vec::new();
    index_obj.read_to_end(&mut index_body).await?;
    let index = object_body::Index::from_data(&index_body)?;

    let mut objects = vec![ObjectInfo {
        hash: index_hash.clone(),
        object_type: ObjectType::Index,
        size: index_header.size,
    }];
    let mut files = Vec::new();

    // 2. Recursively walk the tree
    walk_tree(store, &index.tree, "", &mut objects, &mut files).await?;

    tracing::info!(files = files.len(), objects = objects.len(), "index metadata collected");

    Ok(IndexMetadata {
        index_hash: index_hash.clone(),
        tree_hash: index.tree,
        timestamp: index.timestamp,
        metadata: index.metadata,
        files,
        objects,
    })
}

async fn walk_tree(
    store: &Store,
    tree_hash: &Hash,
    prefix: &str,
    objects: &mut Vec<ObjectInfo>,
    files: &mut Vec<FileInfo>,
) -> anyhow::Result<()> {
    let mut tree_obj = store.get_object(tree_hash).await?;
    let tree_header = tree_obj.header;

    if tree_header.object_type != ObjectType::Tree {
        anyhow::bail!(
            "expected Tree object, got {:?}",
            tree_header.object_type
        );
    }

    objects.push(ObjectInfo {
        hash: tree_hash.clone(),
        object_type: ObjectType::Tree,
        size: tree_header.size,
    });

    let mut tree_body = Vec::new();
    tree_obj.read_to_end(&mut tree_body).await?;
    let tree = object_body::Tree::from_data(&tree_body)?;

    for entry in &tree.contents {
        let current_path = if prefix.is_empty() {
            entry.path.clone()
        } else {
            format!("{}/{}", prefix, entry.path)
        };
        tracing::trace!(hash = %entry.hash, path = %current_path, "walking tree entry");
        match entry.mode {
            Mode::Tree => {
                Box::pin(walk_tree(store, &entry.hash, &current_path, objects, files)).await?;
            }
            _ => {
                // Blob entry — record the object and the file
                let blob_header = read_header(store, &entry.hash).await?;
                objects.push(ObjectInfo {
                    hash: entry.hash.clone(),
                    object_type: ObjectType::Blob,
                    size: blob_header.size,
                });
                files.push(FileInfo {
                    path: current_path,
                    hash: entry.hash.clone(),
                    size: blob_header.size,
                    mode: entry.mode,
                });
            }
        }
    }

    Ok(())
}

/// Read just the header of an object (discarding the body).
async fn read_header(store: &Store, hash: &Hash) -> anyhow::Result<Header> {
    let obj = store.get_object(hash).await?;
    Ok(obj.header)
}

/// Content-addressable metadata: index info + flat hash-keyed object map.
/// Trees reference children by hash, enabling O(1) lookup and easy merging.
#[derive(Debug, Clone, Serialize)]
pub struct TreeMetadata {
    pub index: IndexInfo,
    pub objects: HashMap<Hash, MetadataObject>,
}

/// Index object metadata.
#[derive(Debug, Clone, Serialize)]
pub struct IndexInfo {
    pub hash: Hash,
    pub tree: Hash,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub metadata: HashMap<String, String>,
}

/// A directory entry referencing a child by hash.
#[derive(Debug, Clone, Serialize)]
pub struct TreeDirEntry {
    pub mode: String,
    pub hash: Hash,
}

/// An object in the metadata map — either a tree (directory) or blob (file).
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum MetadataObject {
    #[serde(rename = "tree")]
    Tree {
        entries: BTreeMap<String, TreeDirEntry>,
    },
    #[serde(rename = "blob")]
    Blob {
        size: u64,
    },
}

/// Walk an index's tree in the store and build a flat hash-keyed object map.
pub async fn collect_tree_metadata(
    store: &Store,
    index_hash: &Hash,
) -> anyhow::Result<TreeMetadata> {
    tracing::info!(index_hash = %index_hash, "collecting tree metadata");

    let mut index_obj = store.get_object(index_hash).await?;
    let index_header = index_obj.header;

    if index_header.object_type != ObjectType::Index {
        anyhow::bail!(
            "expected Index object, got {:?}",
            index_header.object_type
        );
    }

    let mut index_body = Vec::new();
    index_obj.read_to_end(&mut index_body).await?;
    let index = object_body::Index::from_data(&index_body)?;

    let mut objects = HashMap::new();
    collect_objects(store, &index.tree, &mut objects).await?;

    Ok(TreeMetadata {
        index: IndexInfo {
            hash: index_hash.clone(),
            tree: index.tree,
            timestamp: index.timestamp,
            metadata: index.metadata,
        },
        objects,
    })
}

#[allow(clippy::mutable_key_type)]
async fn collect_objects(
    store: &Store,
    tree_hash: &Hash,
    objects: &mut HashMap<Hash, MetadataObject>,
) -> anyhow::Result<()> {
    if objects.contains_key(tree_hash) {
        return Ok(());
    }

    let mut tree_obj = store.get_object(tree_hash).await?;

    if tree_obj.header.object_type != ObjectType::Tree {
        anyhow::bail!(
            "expected Tree object, got {:?}",
            tree_obj.header.object_type
        );
    }

    let mut tree_body = Vec::new();
    tree_obj.read_to_end(&mut tree_body).await?;
    let tree = object_body::Tree::from_data(&tree_body)?;

    let mut entries = BTreeMap::new();
    for entry in &tree.contents {
        entries.insert(
            entry.path.clone(),
            TreeDirEntry {
                mode: entry.mode.as_str().to_string(),
                hash: entry.hash.clone(),
            },
        );

        match entry.mode {
            Mode::Tree => {
                Box::pin(collect_objects(store, &entry.hash, objects)).await?;
            }
            _ => {
                if !objects.contains_key(&entry.hash) {
                    let blob_header = read_header(store, &entry.hash).await?;
                    objects.insert(
                        entry.hash.clone(),
                        MetadataObject::Blob { size: blob_header.size },
                    );
                }
            }
        }
    }

    objects.insert(tree_hash.clone(), MetadataObject::Tree { entries });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compute_hash, Header};
    use chrono::{TimeZone, Utc};

    fn make_store(dir: &std::path::Path) -> Store {
        let builder = opendal::services::Fs::default().root(dir.to_str().unwrap());
        Store::from_builder(builder).unwrap()
    }

    /// Helper: store a blob and return its hash.
    async fn put_blob(store: &Store, content: &[u8]) -> Hash {
        let header = Header::new(ObjectType::Blob, content.len() as u64);
        let hash = compute_hash("blob", content);
        store
            .put_object_bytes(&hash, header, content.to_vec())
            .await
            .unwrap();
        hash
    }

    /// Helper: store a tree and return its hash.
    async fn put_tree(store: &Store, tree: &object_body::Tree) -> Hash {
        let data = tree.to_data();
        let header = Header::new(ObjectType::Tree, data.len() as u64);
        let hash = compute_hash("tree", &data);
        store
            .put_object_bytes(&hash, header, data)
            .await
            .unwrap();
        hash
    }

    /// Helper: store an index and return its hash.
    async fn put_index(store: &Store, index: &object_body::Index) -> Hash {
        let data = index.to_data();
        let header = Header::new(ObjectType::Index, data.len() as u64);
        let hash = compute_hash("indx", &data);
        store
            .put_object_bytes(&hash, header, data)
            .await
            .unwrap();
        hash
    }

    #[tokio::test]
    async fn collect_flat_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let blob1_hash = put_blob(&store, b"hello").await;
        let blob2_hash = put_blob(&store, b"world!").await;

        let tree = object_body::Tree {
            contents: vec![
                object_body::TreeEntry {
                    mode: Mode::Normal,
                    path: "a.txt".to_string(),
                    hash: blob1_hash.clone(),
                },
                object_body::TreeEntry {
                    mode: Mode::Executable,
                    path: "run.sh".to_string(),
                    hash: blob2_hash.clone(),
                },
            ],
        };
        let tree_hash = put_tree(&store, &tree).await;

        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let index = object_body::Index {
            tree: tree_hash.clone(),
            timestamp: ts,
            metadata: HashMap::new(),
        };
        let index_hash = put_index(&store, &index).await;

        let meta = collect_index_metadata(&store, &index_hash).await.unwrap();

        assert_eq!(meta.index_hash, index_hash);
        assert_eq!(meta.tree_hash, tree_hash);
        assert_eq!(meta.timestamp, ts);
        assert!(meta.metadata.is_empty());

        // 2 files
        assert_eq!(meta.files.len(), 2);
        assert!(meta.files.iter().any(|f| f.path == "a.txt" && f.hash == blob1_hash));
        assert!(meta.files.iter().any(|f| f.path == "run.sh" && f.hash == blob2_hash));

        // Objects: 1 index + 1 tree + 2 blobs = 4
        assert_eq!(meta.objects.len(), 4);
        assert!(meta.objects.iter().any(|o| o.hash == index_hash && o.object_type == ObjectType::Index));
        assert!(meta.objects.iter().any(|o| o.hash == tree_hash && o.object_type == ObjectType::Tree));
        assert!(meta.objects.iter().any(|o| o.hash == blob1_hash && o.object_type == ObjectType::Blob));
        assert!(meta.objects.iter().any(|o| o.hash == blob2_hash && o.object_type == ObjectType::Blob));
    }

    #[tokio::test]
    async fn collect_nested_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let blob_hash = put_blob(&store, b"nested content").await;

        let subtree = object_body::Tree {
            contents: vec![object_body::TreeEntry {
                mode: Mode::Normal,
                path: "deep.txt".to_string(),
                hash: blob_hash.clone(),
            }],
        };
        let subtree_hash = put_tree(&store, &subtree).await;

        let root_tree = object_body::Tree {
            contents: vec![object_body::TreeEntry {
                mode: Mode::Tree,
                path: "subdir".to_string(),
                hash: subtree_hash.clone(),
            }],
        };
        let root_tree_hash = put_tree(&store, &root_tree).await;

        let ts = Utc.with_ymd_and_hms(2025, 6, 15, 10, 0, 0).unwrap();
        let index = object_body::Index {
            tree: root_tree_hash.clone(),
            timestamp: ts,
            metadata: HashMap::new(),
        };
        let index_hash = put_index(&store, &index).await;

        let meta = collect_index_metadata(&store, &index_hash).await.unwrap();

        // File should have full path
        assert_eq!(meta.files.len(), 1);
        assert_eq!(meta.files[0].path, "subdir/deep.txt");
        assert_eq!(meta.files[0].hash, blob_hash);

        // Objects: index + root_tree + subtree + blob = 4
        assert_eq!(meta.objects.len(), 4);
        assert!(meta.objects.iter().any(|o| o.hash == subtree_hash && o.object_type == ObjectType::Tree));
    }

    #[tokio::test]
    async fn collect_with_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let blob_hash = put_blob(&store, b"data").await;
        let tree = object_body::Tree {
            contents: vec![object_body::TreeEntry {
                mode: Mode::Normal,
                path: "file.bin".to_string(),
                hash: blob_hash.clone(),
            }],
        };
        let tree_hash = put_tree(&store, &tree).await;

        let ts = Utc.with_ymd_and_hms(2025, 3, 1, 12, 0, 0).unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("version".to_string(), "1.0".to_string());
        metadata.insert("author".to_string(), "test".to_string());
        let index = object_body::Index {
            tree: tree_hash.clone(),
            timestamp: ts,
            metadata,
        };
        let index_hash = put_index(&store, &index).await;

        let meta = collect_index_metadata(&store, &index_hash).await.unwrap();

        assert_eq!(meta.metadata.get("version").unwrap(), "1.0");
        assert_eq!(meta.metadata.get("author").unwrap(), "test");
    }

    #[tokio::test]
    async fn collect_empty_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let tree = object_body::Tree {
            contents: vec![],
        };
        let tree_hash = put_tree(&store, &tree).await;

        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let index = object_body::Index {
            tree: tree_hash.clone(),
            timestamp: ts,
            metadata: HashMap::new(),
        };
        let index_hash = put_index(&store, &index).await;

        let meta = collect_index_metadata(&store, &index_hash).await.unwrap();

        assert!(meta.files.is_empty());
        // Objects: index + empty tree = 2
        assert_eq!(meta.objects.len(), 2);
    }

    #[tokio::test]
    async fn collect_records_correct_sizes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let content = b"exactly 20 bytes!!!";
        let blob_hash = put_blob(&store, content).await;
        let tree = object_body::Tree {
            contents: vec![object_body::TreeEntry {
                mode: Mode::Normal,
                path: "sized.txt".to_string(),
                hash: blob_hash.clone(),
            }],
        };
        let tree_hash = put_tree(&store, &tree).await;

        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let index = object_body::Index {
            tree: tree_hash.clone(),
            timestamp: ts,
            metadata: HashMap::new(),
        };
        let index_hash = put_index(&store, &index).await;

        let meta = collect_index_metadata(&store, &index_hash).await.unwrap();

        let blob_obj = meta.objects.iter().find(|o| o.hash == blob_hash).unwrap();
        assert_eq!(blob_obj.size, content.len() as u64);

        let file = &meta.files[0];
        assert_eq!(file.size, content.len() as u64);
    }

    #[tokio::test]
    async fn rejects_non_index_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let blob_hash = put_blob(&store, b"not an index").await;

        let result = collect_index_metadata(&store, &blob_hash).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn tree_metadata_flat() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let blob1_hash = put_blob(&store, b"hello").await;
        let blob2_hash = put_blob(&store, b"world!").await;

        let tree = object_body::Tree {
            contents: vec![
                object_body::TreeEntry {
                    mode: Mode::Normal,
                    path: "a.txt".to_string(),
                    hash: blob1_hash.clone(),
                },
                object_body::TreeEntry {
                    mode: Mode::Executable,
                    path: "run.sh".to_string(),
                    hash: blob2_hash.clone(),
                },
            ],
        };
        let tree_hash = put_tree(&store, &tree).await;

        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let index = object_body::Index {
            tree: tree_hash.clone(),
            timestamp: ts,
            metadata: HashMap::new(),
        };
        let index_hash = put_index(&store, &index).await;

        let meta = collect_tree_metadata(&store, &index_hash).await.unwrap();

        assert_eq!(meta.index.hash, index_hash);
        assert_eq!(meta.index.tree, tree_hash);
        assert_eq!(meta.index.timestamp, ts);
        assert!(meta.index.metadata.is_empty());

        // 3 objects: 1 tree + 2 blobs
        assert_eq!(meta.objects.len(), 3);

        // Root tree has 2 entries
        match &meta.objects[&tree_hash] {
            MetadataObject::Tree { entries } => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries["a.txt"].mode, "100644");
                assert_eq!(entries["a.txt"].hash, blob1_hash);
                assert_eq!(entries["run.sh"].mode, "100755");
                assert_eq!(entries["run.sh"].hash, blob2_hash);
            }
            _ => panic!("expected Tree"),
        }

        // Blobs are O(1) lookup by hash
        match &meta.objects[&blob1_hash] {
            MetadataObject::Blob { size } => assert_eq!(*size, 5),
            _ => panic!("expected Blob"),
        }
        match &meta.objects[&blob2_hash] {
            MetadataObject::Blob { size } => assert_eq!(*size, 6),
            _ => panic!("expected Blob"),
        }
    }

    #[tokio::test]
    async fn tree_metadata_nested() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let blob_hash = put_blob(&store, b"nested content").await;

        let subtree = object_body::Tree {
            contents: vec![object_body::TreeEntry {
                mode: Mode::Normal,
                path: "deep.txt".to_string(),
                hash: blob_hash.clone(),
            }],
        };
        let subtree_hash = put_tree(&store, &subtree).await;

        let root_blob_hash = put_blob(&store, b"root file").await;
        let root_tree = object_body::Tree {
            contents: vec![
                object_body::TreeEntry {
                    mode: Mode::Normal,
                    path: "root.txt".to_string(),
                    hash: root_blob_hash.clone(),
                },
                object_body::TreeEntry {
                    mode: Mode::Tree,
                    path: "subdir".to_string(),
                    hash: subtree_hash.clone(),
                },
            ],
        };
        let root_tree_hash = put_tree(&store, &root_tree).await;

        let ts = Utc.with_ymd_and_hms(2025, 6, 15, 10, 0, 0).unwrap();
        let index = object_body::Index {
            tree: root_tree_hash.clone(),
            timestamp: ts,
            metadata: HashMap::new(),
        };
        let index_hash = put_index(&store, &index).await;

        let meta = collect_tree_metadata(&store, &index_hash).await.unwrap();

        // 4 objects: 2 trees + 2 blobs
        assert_eq!(meta.objects.len(), 4);

        // Root tree references subdir by hash
        match &meta.objects[&root_tree_hash] {
            MetadataObject::Tree { entries } => {
                assert_eq!(entries["subdir"].hash, subtree_hash);
                assert_eq!(entries["subdir"].mode, "040000");
                assert_eq!(entries["root.txt"].hash, root_blob_hash);
            }
            _ => panic!("expected Tree"),
        }

        // Subtree also in map, O(1) lookup
        match &meta.objects[&subtree_hash] {
            MetadataObject::Tree { entries } => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries["deep.txt"].hash, blob_hash);
            }
            _ => panic!("expected Tree"),
        }

        match &meta.objects[&blob_hash] {
            MetadataObject::Blob { size } => assert_eq!(*size, 14),
            _ => panic!("expected Blob"),
        }
    }

    #[tokio::test]
    async fn tree_metadata_json_shape() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let blob_hash = put_blob(&store, b"hello").await;
        let subtree = object_body::Tree {
            contents: vec![object_body::TreeEntry {
                mode: Mode::Normal,
                path: "nested.txt".to_string(),
                hash: blob_hash.clone(),
            }],
        };
        let subtree_hash = put_tree(&store, &subtree).await;

        let root_tree = object_body::Tree {
            contents: vec![
                object_body::TreeEntry {
                    mode: Mode::Normal,
                    path: "file.txt".to_string(),
                    hash: blob_hash.clone(),
                },
                object_body::TreeEntry {
                    mode: Mode::Tree,
                    path: "subdir".to_string(),
                    hash: subtree_hash.clone(),
                },
            ],
        };
        let root_tree_hash = put_tree(&store, &root_tree).await;

        let ts = Utc.with_ymd_and_hms(2025, 1, 15, 12, 30, 45).unwrap();
        let mut metadata = HashMap::new();
        metadata.insert("version".to_string(), "1.0".to_string());
        let index = object_body::Index {
            tree: root_tree_hash.clone(),
            timestamp: ts,
            metadata,
        };
        let index_hash = put_index(&store, &index).await;

        let meta = collect_tree_metadata(&store, &index_hash).await.unwrap();
        let json: serde_json::Value = serde_json::to_value(&meta).unwrap();

        // Index is its own object
        let idx = &json["index"];
        assert!(idx["hash"].is_string());
        assert!(idx["tree"].is_string());
        assert!(idx["timestamp"].is_string());
        assert_eq!(idx["metadata"]["version"], "1.0");

        // Objects is a hash-keyed map
        let objects = json["objects"].as_object().unwrap();

        // Root tree keyed by its hash
        let root = &objects[root_tree_hash.as_str()];
        assert_eq!(root["type"], "tree");
        let entries = root["entries"].as_object().unwrap();
        assert_eq!(entries["file.txt"]["hash"], blob_hash.as_str());
        assert_eq!(entries["file.txt"]["mode"], "100644");
        assert_eq!(entries["subdir"]["hash"], subtree_hash.as_str());
        assert_eq!(entries["subdir"]["mode"], "040000");

        // Subtree keyed by its hash
        let sub = &objects[subtree_hash.as_str()];
        assert_eq!(sub["type"], "tree");
        let sub_entries = sub["entries"].as_object().unwrap();
        assert_eq!(sub_entries["nested.txt"]["hash"], blob_hash.as_str());

        // Blob keyed by its hash
        let blob = &objects[blob_hash.as_str()];
        assert_eq!(blob["type"], "blob");
        assert_eq!(blob["size"], 5);

        // Old fields gone
        assert!(json.get("tree").is_none());
        assert!(json.get("files").is_none());
    }

    #[tokio::test]
    async fn tree_metadata_empty_tree() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let tree = object_body::Tree { contents: vec![] };
        let tree_hash = put_tree(&store, &tree).await;

        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let index = object_body::Index {
            tree: tree_hash.clone(),
            timestamp: ts,
            metadata: HashMap::new(),
        };
        let index_hash = put_index(&store, &index).await;

        let meta = collect_tree_metadata(&store, &index_hash).await.unwrap();
        // Just the empty root tree
        assert_eq!(meta.objects.len(), 1);
        match &meta.objects[&tree_hash] {
            MetadataObject::Tree { entries } => assert!(entries.is_empty()),
            _ => panic!("expected Tree"),
        }
    }

    #[tokio::test]
    async fn tree_metadata_deduplicates_shared_blobs() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        // Same blob referenced twice under different names
        let blob_hash = put_blob(&store, b"shared").await;
        let tree = object_body::Tree {
            contents: vec![
                object_body::TreeEntry {
                    mode: Mode::Normal,
                    path: "copy1.txt".to_string(),
                    hash: blob_hash.clone(),
                },
                object_body::TreeEntry {
                    mode: Mode::Normal,
                    path: "copy2.txt".to_string(),
                    hash: blob_hash.clone(),
                },
            ],
        };
        let tree_hash = put_tree(&store, &tree).await;

        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let index = object_body::Index {
            tree: tree_hash.clone(),
            timestamp: ts,
            metadata: HashMap::new(),
        };
        let index_hash = put_index(&store, &index).await;

        let meta = collect_tree_metadata(&store, &index_hash).await.unwrap();
        // 2 objects: 1 tree + 1 blob (deduplicated)
        assert_eq!(meta.objects.len(), 2);
    }

    #[tokio::test]
    async fn tree_metadata_rejects_non_index() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let blob_hash = put_blob(&store, b"not an index").await;
        let result = collect_tree_metadata(&store, &blob_hash).await;
        assert!(result.is_err());
    }
}
