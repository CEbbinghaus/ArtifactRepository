use std::collections::HashMap;

use futures::AsyncReadExt;

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
}
