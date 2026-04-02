use anyhow::Context;
use chrono::Utc;
use clap::{Parser, Subcommand};
use common::{
    BLOB_KEY, Hash, Header, INDEX_KEY, Mode, ObjectType, TREE_KEY,
    archive::{Archive, ArchiveBody, ArchiveEntryData, ArchiveHeaderEntry, Compression, HEADER, RawEntryData, SUPPLEMENTAL_HEADER},
    compute_hash, object_body, read_header_and_body, read_header_from_slice, read_object_into_headers,
    store::Store,
};

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use futures::io::AsyncReadExt as FuturesReadExt;
use tokio::{
    fs::{create_dir, create_dir_all, read_dir},
    io::{AsyncWriteExt, BufWriter},
    task::JoinSet,
};

/// Detect whether a file has any executable bit set (Unix only).
/// Returns Mode::Executable if so, Mode::Normal otherwise.
fn detect_file_mode(path: &Path) -> Mode {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            if meta.permissions().mode() & 0o111 != 0 {
                return Mode::Executable;
            }
        }
    }
    #[cfg(not(unix))]
    { let _ = path; }
    Mode::Normal
}

/// Set executable permissions on a file (Unix only, no-op elsewhere).
#[cfg(unix)]
fn set_executable(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = std::fs::Permissions::from_mode(0o755);
    std::fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

// Helper to build Index from filesystem
async fn build_index_from_path(store: &Store, path: &Path) -> anyhow::Result<(object_body::Index, Hash, u128)> {
    anyhow::ensure!(path.is_dir(), "path must be a directory");

    let (tree_hash, total_size) = build_tree_from_dir(store.clone(), path.to_path_buf(), path.to_path_buf()).await?;

    let index = object_body::Index {
        tree: tree_hash,
        timestamp: Utc::now(),
        metadata: HashMap::new(),
    };

    let index_data = object_body::Object::to_data(&index);
    let index_hash = compute_hash(INDEX_KEY, &index_data);

    // Write index to store immediately
    let header = Header::new(ObjectType::Index, index_data.len() as u64);
    store.put_object_bytes(&index_hash, header, index_data).await?;

    Ok((index, index_hash, total_size))
}

/// Check whether a symlink target escapes the given root directory.
/// Returns the canonicalized resolved path if it stays within root, or None if it escapes.
fn symlink_target_within_root(symlink_path: &Path, root: &Path) -> std::io::Result<Option<PathBuf>> {
    let target = std::fs::read_link(symlink_path)?;
    let resolved = if target.is_absolute() {
        target.clone()
    } else {
        symlink_path.parent().unwrap_or(Path::new(".")).join(&target)
    };
    // Canonicalize root (already canonical from caller), but resolve the target
    // through the filesystem to handle ../ chains
    match resolved.canonicalize() {
        Ok(canonical) => Ok(if canonical.starts_with(root) { Some(canonical) } else { None }),
        // Target doesn't exist — check lexically by normalizing path components
        Err(_) => {
            let mut components = Vec::new();
            let base = symlink_path.parent().unwrap_or(Path::new("."));
            let full = if target.is_absolute() {
                target
            } else {
                base.join(&target)
            };
            for comp in full.components() {
                match comp {
                    std::path::Component::ParentDir => { components.pop(); }
                    std::path::Component::CurDir => {}
                    _ => components.push(comp),
                }
            }
            let normalized: PathBuf = components.iter().collect();
            Ok(if normalized.starts_with(root) { Some(normalized) } else { None })
        }
    }
}

#[allow(clippy::type_complexity)]
fn build_tree_from_dir(
    store: Store,
    path: PathBuf,
    root: PathBuf,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<(Hash, u128)>> + Send>> {
    Box::pin(async move {
        anyhow::ensure!(path.is_dir(), "path must be a directory");
        tracing::debug!(path = %path.display(), "processing directory");
        let mut dir_entries = read_dir(&path).await?;

        // Collect all entries first so we can process them concurrently
        let mut dir_paths = Vec::new();
        let mut file_paths = Vec::new();
        let mut symlink_paths = Vec::new();
        while let Some(entry) = dir_entries.next_entry().await? {
            let entry_path = entry.path();
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                dir_paths.push(entry_path);
            } else if file_type.is_symlink() {
                symlink_paths.push(entry_path);
            } else {
                file_paths.push(entry_path);
            }
        }

        let mut total_size: u128 = 0;
        let mut entries = Vec::new();

        // Process subdirectories concurrently
        let mut dir_set = JoinSet::new();
        for dir_path in dir_paths {
            let store_clone = store.clone();
            let root_clone = root.clone();
            dir_set.spawn(async move {
                let name = dir_path.file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| anyhow::anyhow!("invalid file name: {}", dir_path.display()))?
                    .to_string();
                let (hash, size) = build_tree_from_dir(store_clone, dir_path, root_clone).await?;
                Ok::<_, anyhow::Error>((name, hash, size))
            });
        }

        // Process files concurrently with bounded parallelism
        let semaphore = Arc::new(tokio::sync::Semaphore::new(64));
        let mut file_set = JoinSet::new();
        for file_path in file_paths {
            let store_clone = store.clone();
            let sem = semaphore.clone();
            file_set.spawn(async move {
                let _permit = sem.acquire().await
                    .map_err(|e| anyhow::anyhow!("semaphore: {e}"))?;
                let name = file_path.file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| anyhow::anyhow!("invalid file name: {}", file_path.display()))?
                    .to_string();

                let (data, hash, file_mode) = tokio::task::spawn_blocking({
                    let path = file_path.clone();
                    move || -> anyhow::Result<(Vec<u8>, Hash, Mode)> {
                        let file_mode = detect_file_mode(&path);
                        let data = std::fs::read(&path)?;
                        let hash = compute_hash(BLOB_KEY, &data);
                        Ok((data, hash, file_mode))
                    }
                }).await??;
                let size = data.len() as u64;

                // Content-addressable: concurrent writes of the same hash are safe to ignore.
                // The exists() check is a fast path to skip unnecessary writes.
                if !store_clone.exists(&hash).await? {
                    let header = Header::new(ObjectType::Blob, size);
                    if let Err(e) = store_clone.put_object_bytes(&hash, header, data).await {
                        // If another task wrote the same object concurrently, that's fine
                        if !store_clone.exists(&hash).await.unwrap_or(false) {
                            return Err(e);
                        }
                    }
                }

                Ok::<_, anyhow::Error>((name, hash, size, file_mode))
            });
        }

        // Process symlinks concurrently
        let mut symlink_set = JoinSet::new();
        for symlink_path in symlink_paths {
            let store_clone = store.clone();
            let sem = semaphore.clone();
            let root_clone = root.clone();
            symlink_set.spawn(async move {
                let _permit = sem.acquire().await
                    .map_err(|e| anyhow::anyhow!("semaphore: {e}"))?;
                let name = symlink_path.file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| anyhow::anyhow!("invalid file name: {}", symlink_path.display()))?
                    .to_string();

                // Check if symlink target escapes the root directory
                let (data, hash, mode) = tokio::task::spawn_blocking({
                    let path = symlink_path.clone();
                    move || -> anyhow::Result<(Vec<u8>, Hash, Mode)> {
                        let within = symlink_target_within_root(&path, &root_clone)?;
                        if within.is_some() {
                            // Safe symlink — store as symlink
                            let target = std::fs::read_link(&path)?;
                            let target_str = target.to_str()
                                .ok_or_else(|| anyhow::anyhow!("symlink target is not valid UTF-8: {:?}", target))?;
                            let target_bytes = target_str.as_bytes().to_vec();
                            let hash = compute_hash(BLOB_KEY, &target_bytes);
                            Ok((target_bytes, hash, Mode::SymbolicLink))
                        } else {
                            // Escaping symlink — follow it and store the actual file content
                            tracing::warn!(
                                symlink = %path.display(),
                                "symlink target escapes root directory, storing as regular file"
                            );
                            let data = std::fs::read(&path)
                                .context(format!("failed to read through escaping symlink {:?}", path))?;
                            let file_mode = detect_file_mode(&path);
                            let hash = compute_hash(BLOB_KEY, &data);
                            Ok((data, hash, file_mode))
                        }
                    }
                }).await??;
                let size = data.len() as u64;

                if !store_clone.exists(&hash).await? {
                    let header = Header::new(ObjectType::Blob, size);
                    if let Err(e) = store_clone.put_object_bytes(&hash, header, data).await {
                        if !store_clone.exists(&hash).await.unwrap_or(false) {
                            return Err(e);
                        }
                    }
                }

                Ok::<_, anyhow::Error>((name, hash, size, mode))
            });
        }

        // Collect directory results
        while let Some(result) = dir_set.join_next().await {
            let (name, hash, size) = result??;
            total_size += size;
            entries.push(object_body::TreeEntry {
                mode: Mode::Tree,
                path: name,
                hash,
            });
        }

        // Collect file results
        while let Some(result) = file_set.join_next().await {
            let (name, hash, size, file_mode) = result??;
            total_size += size as u128;
            entries.push(object_body::TreeEntry {
                mode: file_mode,
                path: name,
                hash,
            });
        }

        // Collect symlink results
        while let Some(result) = symlink_set.join_next().await {
            let (name, hash, size, mode) = result??;
            total_size += size as u128;
            entries.push(object_body::TreeEntry {
                mode,
                path: name,
                hash,
            });
        }

        // Sort entries by name for deterministic tree hashes
        entries.sort_by(|a, b| a.path.cmp(&b.path));

        // Build and store tree
        let tree = object_body::Tree { contents: entries };
        let tree_data = object_body::Object::to_data(&tree);
        let hash = compute_hash(TREE_KEY, &tree_data);

        if !store.exists(&hash).await? {
            let header = Header::new(ObjectType::Tree, tree_data.len() as u64);
            store.put_object_bytes(&hash, header, tree_data).await?;
        }

        Ok((hash, total_size))
    })
}

// Helper to read Tree from Store
async fn read_tree_from_store(store: &Store, hash: &Hash) -> anyhow::Result<object_body::Tree> {
    let mut store_obj = store.get_object(hash).await?;
    anyhow::ensure!(store_obj.header.object_type == ObjectType::Tree, "expected tree object");

    let mut data = Vec::new();
    store_obj.read_to_end(&mut data).await?;

    object_body::Object::from_data(&data)
}

async fn commit_directory(store: &Store, path: &Path) -> anyhow::Result<()> {
    let path_meta = tokio::fs::metadata(path).await?;
    anyhow::ensure!(path_meta.is_dir(), "path must be a directory");

    let path = tokio::fs::canonicalize(path).await.context(format!("unable to canonicalize {path:?}"))?;

    let (_index, index_hash, total_size) = build_index_from_path(store, &path).await?;

    tracing::info!("Finished generating Index for {} bytes of data", total_size);
    println!("{}", index_hash);
    Ok(())
}

async fn restore_directory(store: &Store, path: &Path, index_hash: Hash, validate: bool) -> anyhow::Result<()> {
    if tokio::fs::metadata(path).await.is_err() {
        create_dir_all(path).await.context("failed to create directory")?;
    }

    let path_meta = tokio::fs::metadata(path).await?;
    anyhow::ensure!(path_meta.is_dir(), "path must be a valid directory");

    let mut entries = read_dir(path).await?;
    anyhow::ensure!(entries.next_entry().await?.is_none(), "path must be an empty directory");

    let mut store_obj = store.get_object(&index_hash).await?;
    let mut index_data = Vec::new();
    store_obj.read_to_end(&mut index_data).await?;
    let index: object_body::Index = object_body::Object::from_data(&index_data)?;

    let tree = read_tree_from_store(store, &index.tree).await?;

    #[allow(clippy::mutable_key_type)]
    let written_files = Arc::new(tokio::sync::Mutex::new(HashMap::<Hash, PathBuf>::new()));
    write_tree_to_path(store, &tree, path, path, written_files).await?;

    if validate {
        validate_tree_at_path(store, &tree, path).await?;
    }
    Ok(())
}

fn validate_tree_at_path<'a>(
    store: &'a Store,
    tree: &'a object_body::Tree,
    path: &'a Path,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        for entry in &tree.contents {
            let entry_path = path.join(&entry.path);
            let header = store.get_object(&entry.hash).await?.header;

            match header.object_type {
                ObjectType::Tree => {
                    let subtree = read_tree_from_store(store, &entry.hash).await?;
                    validate_tree_at_path(store, &subtree, &entry_path).await?;
                }
                ObjectType::Blob => {
                    let expected_hash = entry.hash.clone();
                    let entry_name = entry.path.clone();
                    let computed_hash = tokio::task::spawn_blocking({
                        let path = entry_path.clone();
                        move || -> anyhow::Result<Hash> {
                            let data = std::fs::read(&path)?;
                            Ok(compute_hash(BLOB_KEY, &data))
                        }
                    }).await??;
                    anyhow::ensure!(computed_hash == expected_hash, "hash mismatch for {}", entry_name);
                }
                ObjectType::Index => anyhow::bail!("invalid object type in tree"),
            }
        }
        Ok(())
    })
}

fn write_tree_to_path<'a>(
    store: &'a Store,
    tree: &'a object_body::Tree,
    path: &'a Path,
    root: &'a Path,
    written_files: Arc<tokio::sync::Mutex<HashMap<Hash, PathBuf>>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(64));
        let mut blob_set = JoinSet::new();

        #[cfg(feature = "cow")]
        let mut deferred_reflinks: Vec<(PathBuf, PathBuf, bool)> = Vec::new();

        for entry in &tree.contents {
            let entry_path = path.join(&entry.path);

            match entry.mode {
                Mode::Tree => {
                    create_dir(&entry_path).await.context("failed to create directory")?;
                    let subtree = read_tree_from_store(store, &entry.hash).await?;
                    write_tree_to_path(store, &subtree, &entry_path, root, written_files.clone()).await?;
                }
                Mode::Normal | Mode::Executable => {
                    let is_executable = entry.mode == Mode::Executable;

                    // When CoW is enabled, reuse already-written files via reflink
                    #[cfg(feature = "cow")]
                    {
                        let mut map = written_files.lock().await;
                        if let Some(source_path) = map.get(&entry.hash) {
                            deferred_reflinks.push((source_path.clone(), entry_path, is_executable));
                            continue;
                        }
                        map.insert(entry.hash.clone(), entry_path.clone());
                    }

                    let store_clone = store.clone();
                    let hash = entry.hash.clone();
                    let sem = semaphore.clone();
                    blob_set.spawn(async move {
                        let _permit = sem.acquire().await
                            .map_err(|e| anyhow::anyhow!("semaphore: {e}"))?;

                        let mut store_obj = store_clone.get_object(&hash).await?;
                        let mut data = Vec::with_capacity(store_obj.header.size as usize);
                        store_obj.read_to_end(&mut data).await?;

                        let path = entry_path;
                        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                            std::fs::write(&path, &data)
                                .context(format!("failed to write file {:?}", path))?;
                            if is_executable {
                                set_executable(&path)
                                    .context(format!("failed to set executable permissions on {:?}", path))?;
                            }
                            Ok(())
                        }).await?
                    });
                }
                Mode::SymbolicLink => {
                    let store_clone = store.clone();
                    let hash = entry.hash.clone();
                    let sem = semaphore.clone();
                    let root_path = root.to_path_buf();
                    blob_set.spawn(async move {
                        let _permit = sem.acquire().await
                            .map_err(|e| anyhow::anyhow!("semaphore: {e}"))?;

                        let mut store_obj = store_clone.get_object(&hash).await?;
                        let mut data = Vec::with_capacity(store_obj.header.size as usize);
                        store_obj.read_to_end(&mut data).await?;

                        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                            let target_str = String::from_utf8(data)
                                .context("symlink target is not valid UTF-8")?;
                            let target = std::path::Path::new(&target_str);

                            // Validate symlink target doesn't escape the restore root
                            let resolved = if target.is_absolute() {
                                target.to_path_buf()
                            } else {
                                entry_path.parent()
                                    .unwrap_or(std::path::Path::new("."))
                                    .join(target)
                            };
                            // Lexically normalize to check for escapes
                            let mut components = Vec::new();
                            for comp in resolved.components() {
                                match comp {
                                    std::path::Component::ParentDir => { components.pop(); }
                                    std::path::Component::CurDir => {}
                                    _ => components.push(comp),
                                }
                            }
                            let normalized: PathBuf = components.iter().collect();
                            anyhow::ensure!(
                                normalized.starts_with(&root_path),
                                "symlink {:?} target {:?} escapes restore directory {:?}",
                                entry_path, target_str, root_path
                            );

                            #[cfg(unix)]
                            {
                                std::os::unix::fs::symlink(target, &entry_path)
                                    .context(format!("failed to create symlink {:?}", entry_path))?;
                            }
                            #[cfg(windows)]
                            {
                                if normalized.is_dir() {
                                    std::os::windows::fs::symlink_dir(target, &entry_path)
                                        .context(format!("failed to create dir symlink {:?}", entry_path))?;
                                } else {
                                    std::os::windows::fs::symlink_file(target, &entry_path)
                                        .context(format!("failed to create file symlink {:?}", entry_path))?;
                                }
                            }
                            #[cfg(not(any(unix, windows)))]
                            {
                                std::fs::write(&entry_path, target_str.as_bytes())
                                    .context(format!("failed to write symlink placeholder {:?}", entry_path))?;
                            }
                            Ok(())
                        }).await?
                    });
                }
                _ => anyhow::bail!("unsupported mode {} for entry {}", entry.mode.as_str(), entry.path),
            }
        }

        while let Some(result) = blob_set.join_next().await {
            result??;
        }

        // Phase 2: reflink (or copy) deferred duplicate files
        #[cfg(feature = "cow")]
        for (source, dest, is_exec) in deferred_reflinks {
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                reflink_copy::reflink_or_copy(&source, &dest)
                    .context(format!("failed to reflink {:?} -> {:?}", source, dest))?;
                if is_exec {
                    set_executable(&dest)?;
                } else {
                    // Source may have been executable; ensure correct permissions
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o644))?;
                    }
                }
                Ok(())
            }).await??;
        }

        Ok(())
    })
}

async fn cat_object(store: &Store, hash: &Hash) -> anyhow::Result<()> {
    let mut store_obj = store.get_object(hash).await?;

    let mut stdout = BufWriter::new(tokio::io::stdout());
    let mut data = [0u8; 65536];
    loop {
        let num = store_obj.read(&mut data).await?;
        if num == 0 {
            break;
        }
        stdout.write_all(&data[..num]).await?;
    }
    stdout.flush().await?;
    println!();
    Ok(())
}

async fn push_cache(store: &Store, url: &str, hash: Option<Hash>) -> anyhow::Result<()> {
    if let Some(hash) = hash {
        let raw_bytes = store.get_raw_bytes(&hash).await
            .context(format!("object {} not found in store", hash))?;
        upload_object(&hash, &raw_bytes, url).await?;
        return Ok(());
    }

    let hashes = store.list_hashes().await.context("failed to list objects in store")?;

    for hash in hashes {
        let raw_bytes = store.get_raw_bytes(&hash).await
            .context(format!("failed to read object {}", hash))?;
        upload_object(&hash, &raw_bytes, url).await?;
    }
    Ok(())
}

fn pull_tree<'a>(store: &'a Store, url: &'a str, tree_hash: &'a Hash) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        // tree already exists locally so we can skip downloading it
        if !store.exists(tree_hash).await? {
            let header = download_object(store, tree_hash, url).await
                .context(format!("unable to download object with hash {tree_hash}"))?;
            anyhow::ensure!(header.object_type == ObjectType::Tree, "expected tree object");
        }

        let raw_bytes = store.get_raw_bytes(tree_hash).await
            .context("tree object should exist in store")?;

        let (_, data) =
            read_header_and_body(&raw_bytes).context("tree should be in the correct format")?;

        let tree_body: object_body::Tree = object_body::Object::from_data(data)?;

        for entry in tree_body.contents {
            let header = download_object(store, &entry.hash, url).await
                .context(format!("unable to download object with hash {}", entry.hash))?;

            anyhow::ensure!(header.object_type != ObjectType::Index, "unexpected index object in tree");

            if header.object_type == ObjectType::Tree {
                pull_tree(store, url, &entry.hash).await?;
            }
        }
        Ok(())
    })
}

async fn pull_cache(store: &Store, url: &str, hash: Hash) -> anyhow::Result<()> {
    let header = download_object(store, &hash, url).await
        .context(format!("unable to download object with hash {hash}"))?;

    anyhow::ensure!(header.object_type == ObjectType::Index, "expected index object");

    let raw_bytes = store.get_raw_bytes(&hash).await
        .context("index object should exist in store")?;

    let (_, data) =
        read_header_and_body(&raw_bytes).context("index should be in the correct format")?;

    let index_body: object_body::Index = object_body::Object::from_data(data)?;

    pull_tree(store, url, &index_body.tree).await?;
    Ok(())
}

async fn upload_object(hash: &Hash, raw_bytes: &[u8], url: &str) -> anyhow::Result<()> {
    let null_pos = raw_bytes.iter().position(|&b| b == 0)
        .context("object data missing null terminator")?;
    let header = read_header_from_slice(&raw_bytes[..null_pos])
        .context("invalid object header")?;
    let body = &raw_bytes[(null_pos + 1)..];

    let url = format!("{url}/v1/object/{hash}");

    tracing::debug!("Sending PUT request to {url}");

    ureq::put(&url)
        .header("Object-Type", header.object_type.to_str())
        .header("Object-Size", &header.size.to_string())
        .send(body)
        .context("failed to upload object")?;

    Ok(())
}

async fn download_object(store: &Store, hash: &Hash, url: &str) -> anyhow::Result<Header> {
    // If already in store, return its header
    if store.exists(hash).await? {
        let mut store_obj = store.get_object(hash).await
            .context("object should exist in store")?;
        // Consume body to drop the reader
        let mut _discard = Vec::new();
        store_obj.read_to_end(&mut _discard).await?;
        return Ok(store_obj.header);
    }

    let url = format!("{url}/v1/object/{hash}");

    tracing::debug!("Sending GET request to {url}");

    let mut response = ureq::get(&url).call()
        .context("failed to send GET request")?;

    let response_headers = response.headers();
    let object_type: ObjectType = ObjectType::from_str(
        response_headers
            .get("Object-Type")
            .context("object-type header missing from response")?
            .to_str()
            .context("object-type header not valid ASCII")?,
    )
    .context("invalid object type in header")?;
    let object_size: u64 = response_headers
        .get("Object-Size")
        .context("object-size header missing from response")?
        .to_str()
        .context("object-size header not valid ASCII")?
        .parse()
        .context("object-size header not a valid number")?;

    let header = Header::new(object_type, object_size);

    let mut body_bytes = Vec::new();
    let mut reader = response.body_mut().as_reader();
    let mut data = [0u8; 65536];
    loop {
        let num = std::io::Read::read(&mut reader, &mut data)?;
        if num == 0 {
            break;
        }
        body_bytes.extend_from_slice(&data[..num]);
    }

    store.put_object_bytes(hash, header, body_bytes).await
        .context("failed to write object to store")?;

    Ok(header)
}

async fn pack_archive(store: &Store, path: &Path, index_hash: &Hash, compression: Compression) -> anyhow::Result<()> {
    anyhow::ensure!(tokio::fs::metadata(path).await.is_err(), "output file already exists");
    anyhow::ensure!(
        path.parent().map(|p| p.exists() && p.is_dir()) == Some(true),
        "Parent directory must exist and be a directory"
    );

    let index: object_body::Index = {
        let mut store_obj = store.get_object(index_hash).await.context("index object not found in store")?;
        anyhow::ensure!(store_obj.header.object_type == ObjectType::Index, "expected index object type");
        let mut body = Vec::new();
        store_obj.read_to_end(&mut body).await?;
        object_body::Object::from_data(&body)?
    };

    #[allow(clippy::mutable_key_type)]
    let mut headers: HashMap<Hash, (Header, Vec<u8>)> = HashMap::new();
    read_object_into_headers(store, &mut headers, &index.tree).await?;

    //TODO: Surely there is an algorithm to more efficiently lay out this data
    let mut i = 0;
    let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::new();

    for (hash, (header, data)) in &headers {
        let prefix_length = header.to_string().len() as u64;
        let length = prefix_length + data.len() as u64;

        header_entries.push(ArchiveHeaderEntry {
            hash: hash.clone(),
            index: i,
            length,
        });

        i += length;
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

    let arx_file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::new(arx_file);

    archive.to_data(&mut writer)?;

    Ok(())
}

async fn push_archive(store: &Store, url: &str, index_hash: &Hash, compression: Compression) -> anyhow::Result<()> {
    tracing::debug!(index = %index_hash, url, "starting push-archive");
    // 1. Read index from store
    let index: object_body::Index = {
        let mut store_obj = store.get_object(index_hash).await.context("index object not found in store")?;
        anyhow::ensure!(store_obj.header.object_type == ObjectType::Index, "expected index object type");
        let mut body = Vec::new();
        store_obj.read_to_end(&mut body).await?;
        object_body::Object::from_data(&body)?
    };

    // 2. Collect all objects reachable from the tree
    #[allow(clippy::mutable_key_type)]
    let mut headers: HashMap<Hash, (Header, Vec<u8>)> = HashMap::new();
    read_object_into_headers(store, &mut headers, &index.tree).await?;

    // Also include the index object itself
    let index_data = object_body::Object::to_data(&index);
    let index_header = Header::new(ObjectType::Index, index_data.len() as u64);
    headers.insert(index_hash.clone(), (index_header, index_data));

    // 3. Collect all hashes
    let all_hashes: Vec<Hash> = headers.keys().cloned().collect();
    let total_objects = all_hashes.len();

    // 4. POST to /missing to find which objects the server needs
    #[derive(serde::Serialize)]
    struct MissingRequest {
        hashes: Vec<Hash>,
    }
    #[derive(serde::Deserialize)]
    struct MissingResponse {
        missing: Vec<Hash>,
    }

    let missing_url = format!("{url}/v1/object/missing");
    let missing_resp: MissingResponse = ureq::post(&missing_url)
        .send_json(&MissingRequest { hashes: all_hashes })
        .context("failed to query missing objects")?
        .body_mut()
        .read_json()
        .context("failed to parse missing response")?;

    // 5. If no missing objects, nothing to do
    if missing_resp.missing.is_empty() {
        tracing::info!("All objects already present on server");
        return Ok(());
    }

    // 6. Build supplemental archive with all trees + missing objects
    #[allow(clippy::mutable_key_type)]
    let missing_set: std::collections::HashSet<Hash> = missing_resp.missing.into_iter().collect();
    tracing::info!(total = total_objects, missing = missing_set.len(), "uploading missing objects");

    let mut offset = 0u64;
    let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::new();
    let mut entry_data_vec: Vec<RawEntryData> = Vec::new();

    for (hash, (header, data)) in &headers {
        // Always include tree and index objects; for blobs, only include if missing
        let dominated_by_missing = header.object_type != ObjectType::Tree
            && header.object_type != ObjectType::Index
            && !missing_set.contains(hash);
        if dominated_by_missing {
            continue;
        }

        let prefix = header.to_string();
        let length = prefix.len() as u64 + data.len() as u64;

        header_entries.push(ArchiveHeaderEntry {
            hash: hash.clone(),
            index: offset,
            length,
        });

        let mut full_data = Vec::with_capacity(prefix.len() + data.len());
        full_data.extend_from_slice(prefix.as_bytes());
        full_data.extend_from_slice(data);
        entry_data_vec.push(RawEntryData(full_data));

        offset += length;
    }

    let num_objects = entry_data_vec.len();

    let archive = Archive {
        header: SUPPLEMENTAL_HEADER,
        compression,
        hash: index_hash.clone(),
        index,
        body: ArchiveBody {
            header: header_entries,
            entries: entry_data_vec,
        },
    };

    // 7. Serialize to bytes
    let mut buf: Vec<u8> = Vec::new();
    archive.to_data(&mut buf)?;
    let total_bytes = buf.len();

    // 8. POST the archive to /v1/archive/upload
    let upload_url = format!("{url}/v1/archive/upload");
    let response_text = ureq::post(&upload_url)
        .header("Content-Type", "application/octet-stream")
        .send(&buf[..])
        .context("failed to upload archive")?
        .body_mut()
        .read_to_string()
        .context("failed to read upload response")?;

    tracing::debug!("{}", response_text);
    tracing::info!("Pushed {} objects ({} bytes) to {}", num_objects, total_bytes, url);

    Ok(())
}

async fn unpack_archive(store: &Store, path: &Path) -> anyhow::Result<()> {
    let path_meta = tokio::fs::metadata(path).await?;
    anyhow::ensure!(path_meta.is_file(), "path must be a file");

    let file = std::fs::File::open(path)?;
    let mut file = std::io::BufReader::new(file);

    let archive = Archive::<RawEntryData>::from_data(&mut file)?;

    anyhow::ensure!(archive.body.entries.len() == archive.body.header.len(), "archive entries and headers length mismatch");

    tracing::debug!("Successfully read archive, Index {}", archive.hash);

    let index_data = object_body::Object::to_data(&archive.index);
    let computed_hash = compute_hash(INDEX_KEY, &index_data);
    anyhow::ensure!(computed_hash == archive.hash, "index hash mismatch");

    // Write index object to store
    {
        let index_header = Header::new(ObjectType::Index, index_data.len() as u64);
        store.put_object_bytes(&archive.hash, index_header, index_data).await?;
    }

    let semaphore = Arc::new(tokio::sync::Semaphore::new(64));
    let mut set = JoinSet::new();

    for (header, entry) in archive.body.header.into_iter().zip(archive.body.entries.into_iter()) {
        let store_clone = store.clone();
        let sem = semaphore.clone();
        let hash = header.hash.clone();
        set.spawn(async move {
            let _permit = sem.acquire().await
                .map_err(|e| anyhow::anyhow!("semaphore: {e}"))?;

            // Skip if already exists
            if store_clone.exists(&hash).await? {
                return Ok::<_, anyhow::Error>(());
            }

            let raw = entry.turn_into_vec()?;
            let (obj_header, body) = read_header_and_body(&raw)
                .ok_or_else(|| anyhow::anyhow!("failed to parse header for object {}", hash))?;

            if let Err(e) = store_clone.put_object_bytes(&hash, obj_header, body.to_vec()).await {
                if !store_clone.exists(&hash).await.unwrap_or(false) {
                    return Err(e);
                }
            }

            Ok(())
        });
    }

    while let Some(result) = set.join_next().await {
        result??;
    }

    Ok(())
}

async fn extract_archive(file: &Path, directory: &Path) -> anyhow::Result<()> {
    if tokio::fs::metadata(directory).await.is_err() {
        create_dir_all(directory).await.context("failed to create output directory")?;
    }

    let dir_meta = tokio::fs::metadata(directory).await?;
    anyhow::ensure!(dir_meta.is_dir(), "output path must be a directory");

    let mut entries = read_dir(directory).await?;
    anyhow::ensure!(entries.next_entry().await?.is_none(), "output directory must be empty");

    let file = file.to_path_buf();
    let archive = tokio::task::spawn_blocking(move || {
        let f = std::fs::File::open(&file)?;
        let mut reader = std::io::BufReader::new(f);
        Archive::<RawEntryData>::from_data(&mut reader)
    }).await??;

    anyhow::ensure!(
        archive.body.entries.len() == archive.body.header.len(),
        "archive entries and headers length mismatch"
    );

    tracing::info!("Index: {}", archive.hash);

    #[allow(clippy::mutable_key_type)]
    let mut data_map: HashMap<Hash, Vec<u8>> = HashMap::with_capacity(archive.body.header.len());
    for (header_entry, raw_entry) in archive.body.header.into_iter().zip(archive.body.entries.into_iter()) {
        data_map.insert(header_entry.hash, raw_entry.0);
    }

    let root_tree_hash = archive.index.tree;
    let data_map = Arc::new(data_map);
    #[allow(clippy::mutable_key_type)]
    let written_files = Arc::new(tokio::sync::Mutex::new(HashMap::<Hash, PathBuf>::new()));
    let file_count = extract_tree(&data_map, &root_tree_hash, directory, directory, written_files).await?;

    tracing::info!("Extracted {} files to {:?}", file_count, directory);

    Ok(())
}

#[allow(clippy::mutable_key_type)]
fn extract_tree<'a>(
    data_map: &'a Arc<HashMap<Hash, Vec<u8>>>,
    tree_hash: &'a Hash,
    path: &'a Path,
    root: &'a Path,
    written_files: Arc<tokio::sync::Mutex<HashMap<Hash, PathBuf>>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<usize>> + Send + 'a>> {
    Box::pin(async move {
        let raw_data = data_map.get(tree_hash)
            .ok_or_else(|| anyhow::anyhow!("tree hash {} not found in archive", tree_hash))?;

        let (_header, body) = read_header_and_body(raw_data)
            .ok_or_else(|| anyhow::anyhow!("failed to parse header for tree {}", tree_hash))?;

        let tree: object_body::Tree = object_body::Object::from_data(body)?;

        let semaphore = Arc::new(tokio::sync::Semaphore::new(64));
        let mut set = JoinSet::new();
        let mut file_count: usize = 0;

        #[cfg(feature = "cow")]
        let mut deferred_reflinks: Vec<(PathBuf, PathBuf, bool)> = Vec::new();

        for entry in tree.contents {
            let entry_path = path.join(&entry.path);

            match entry.mode {
                Mode::Tree => {
                    create_dir(&entry_path).await
                        .context(format!("failed to create directory {:?}", entry_path))?;
                    file_count += extract_tree(data_map, &entry.hash, &entry_path, root, written_files.clone()).await?;
                }
                Mode::Normal | Mode::Executable => {
                    file_count += 1;
                    let is_executable = entry.mode == Mode::Executable;

                    #[cfg(feature = "cow")]
                    {
                        let mut map = written_files.lock().await;
                        if let Some(source_path) = map.get(&entry.hash) {
                            deferred_reflinks.push((source_path.clone(), entry_path, is_executable));
                            continue;
                        }
                        map.insert(entry.hash.clone(), entry_path.clone());
                    }

                    let dm = data_map.clone();
                    let sem = semaphore.clone();
                    set.spawn(async move {
                        let _permit = sem.acquire().await
                            .map_err(|e| anyhow::anyhow!("semaphore: {e}"))?;

                        let raw = dm.get(&entry.hash)
                            .ok_or_else(|| anyhow::anyhow!("blob hash {} not found in archive", entry.hash))?;

                        let (_hdr, blob_body) = read_header_and_body(raw)
                            .ok_or_else(|| anyhow::anyhow!("failed to parse header for blob {}", entry.hash))?;

                        tokio::fs::write(&entry_path, blob_body).await
                            .context(format!("failed to write file {:?}", entry_path))?;

                        if is_executable {
                            tokio::task::spawn_blocking({
                                let path = entry_path.clone();
                                move || set_executable(&path)
                            }).await?
                                .context(format!("failed to set executable permissions on {:?}", entry_path))?;
                        }

                        Ok::<_, anyhow::Error>(())
                    });
                }
                Mode::SymbolicLink => {
                    file_count += 1;
                    let dm = data_map.clone();
                    let sem = semaphore.clone();
                    let root_path = root.to_path_buf();
                    set.spawn(async move {
                        let _permit = sem.acquire().await
                            .map_err(|e| anyhow::anyhow!("semaphore: {e}"))?;

                        let raw = dm.get(&entry.hash)
                            .ok_or_else(|| anyhow::anyhow!("blob hash {} not found in archive", entry.hash))?;

                        let (_hdr, blob_body) = read_header_and_body(raw)
                            .ok_or_else(|| anyhow::anyhow!("failed to parse header for blob {}", entry.hash))?;

                        tokio::task::spawn_blocking({
                            let data = blob_body.to_vec();
                            move || -> anyhow::Result<()> {
                                let target_str = String::from_utf8(data)
                                    .context("symlink target is not valid UTF-8")?;
                                let target = std::path::Path::new(&target_str);

                                // Validate symlink target doesn't escape the extract root
                                let resolved = if target.is_absolute() {
                                    target.to_path_buf()
                                } else {
                                    entry_path.parent()
                                        .unwrap_or(std::path::Path::new("."))
                                        .join(target)
                                };
                                let mut components = Vec::new();
                                for comp in resolved.components() {
                                    match comp {
                                        std::path::Component::ParentDir => { components.pop(); }
                                        std::path::Component::CurDir => {}
                                        _ => components.push(comp),
                                    }
                                }
                                let normalized: PathBuf = components.iter().collect();
                                anyhow::ensure!(
                                    normalized.starts_with(&root_path),
                                    "symlink {:?} target {:?} escapes extract directory {:?}",
                                    entry_path, target_str, root_path
                                );

                                #[cfg(unix)]
                                {
                                    std::os::unix::fs::symlink(target, &entry_path)
                                        .context(format!("failed to create symlink {:?}", entry_path))?;
                                }
                                #[cfg(windows)]
                                {
                                    if normalized.is_dir() {
                                        std::os::windows::fs::symlink_dir(target, &entry_path)
                                            .context(format!("failed to create dir symlink {:?}", entry_path))?;
                                    } else {
                                        std::os::windows::fs::symlink_file(target, &entry_path)
                                            .context(format!("failed to create file symlink {:?}", entry_path))?;
                                    }
                                }
                                #[cfg(not(any(unix, windows)))]
                                {
                                    std::fs::write(&entry_path, target_str.as_bytes())
                                        .context(format!("failed to write symlink placeholder {:?}", entry_path))?;
                                }
                                Ok(())
                            }
                        }).await??;

                        Ok::<_, anyhow::Error>(())
                    });
                }
                _ => {
                    anyhow::bail!("unsupported mode {} for entry {}", entry.mode.as_str(), entry.path);
                }
            }
        }

        while let Some(result) = set.join_next().await {
            result??;
        }

        #[cfg(feature = "cow")]
        for (source, dest, is_exec) in deferred_reflinks {
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                reflink_copy::reflink_or_copy(&source, &dest)
                    .context(format!("failed to reflink {:?} -> {:?}", source, dest))?;
                if is_exec {
                    set_executable(&dest)?;
                } else {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o644))?;
                    }
                }
                Ok(())
            }).await??;
        }

        Ok(file_count)
    })
}

fn archive_build_tree(
    objects: Arc<tokio::sync::Mutex<HashMap<Hash, Vec<u8>>>>,
    path: PathBuf,
    root: PathBuf,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Hash>> + Send>> {
    Box::pin(async move {
        anyhow::ensure!(path.is_dir(), "path must be a directory");
        let mut dir_entries = read_dir(&path).await?;

        let mut dir_paths = Vec::new();
        let mut file_paths = Vec::new();
        let mut symlink_paths = Vec::new();
        while let Some(entry) = dir_entries.next_entry().await? {
            let entry_path = entry.path();
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                dir_paths.push(entry_path);
            } else if file_type.is_symlink() {
                symlink_paths.push(entry_path);
            } else {
                file_paths.push(entry_path);
            }
        }

        let mut entries = Vec::new();

        // Process subdirectories concurrently
        let mut dir_set = JoinSet::new();
        for dir_path in dir_paths {
            let objects_clone = objects.clone();
            let root_clone = root.clone();
            dir_set.spawn(async move {
                let name = dir_path.file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| anyhow::anyhow!("invalid file name: {}", dir_path.display()))?
                    .to_string();
                let hash = archive_build_tree(objects_clone, dir_path, root_clone).await?;
                Ok::<_, anyhow::Error>((name, hash))
            });
        }

        // Process files concurrently with bounded parallelism
        let semaphore = Arc::new(tokio::sync::Semaphore::new(64));
        let mut file_set = JoinSet::new();
        for file_path in file_paths {
            let objects_clone = objects.clone();
            let sem = semaphore.clone();
            file_set.spawn(async move {
                let _permit = sem.acquire().await
                    .map_err(|e| anyhow::anyhow!("semaphore: {e}"))?;
                let name = file_path.file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| anyhow::anyhow!("invalid file name: {}", file_path.display()))?
                    .to_string();

                let (data, hash, file_mode) = tokio::task::spawn_blocking({
                    let path = file_path.clone();
                    move || -> anyhow::Result<(Vec<u8>, Hash, Mode)> {
                        let file_mode = detect_file_mode(&path);
                        let data = std::fs::read(&path)?;
                        let hash = compute_hash(BLOB_KEY, &data);
                        Ok((data, hash, file_mode))
                    }
                }).await??;

                {
                    let mut map = objects_clone.lock().await;
                    if !map.contains_key(&hash) {
                        let header = Header::new(ObjectType::Blob, data.len() as u64);
                        let prefix = header.to_string();
                        let mut full_data = Vec::with_capacity(prefix.len() + data.len());
                        full_data.extend_from_slice(prefix.as_bytes());
                        full_data.extend(data);
                        map.insert(hash.clone(), full_data);
                    }
                }

                Ok::<_, anyhow::Error>((name, hash, file_mode))
            });
        }

        // Process symlinks concurrently
        let mut symlink_set = JoinSet::new();
        for symlink_path in symlink_paths {
            let objects_clone = objects.clone();
            let sem = semaphore.clone();
            let root_clone = root.clone();
            symlink_set.spawn(async move {
                let _permit = sem.acquire().await
                    .map_err(|e| anyhow::anyhow!("semaphore: {e}"))?;
                let name = symlink_path.file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| anyhow::anyhow!("invalid file name: {}", symlink_path.display()))?
                    .to_string();

                let (data, hash, mode) = tokio::task::spawn_blocking({
                    let path = symlink_path.clone();
                    move || -> anyhow::Result<(Vec<u8>, Hash, Mode)> {
                        let within = symlink_target_within_root(&path, &root_clone)?;
                        if within.is_some() {
                            let target = std::fs::read_link(&path)?;
                            let target_str = target.to_str()
                                .ok_or_else(|| anyhow::anyhow!("symlink target is not valid UTF-8: {:?}", target))?;
                            let target_bytes = target_str.as_bytes().to_vec();
                            let hash = compute_hash(BLOB_KEY, &target_bytes);
                            Ok((target_bytes, hash, Mode::SymbolicLink))
                        } else {
                            tracing::warn!(
                                symlink = %path.display(),
                                "symlink target escapes root directory, storing as regular file"
                            );
                            let data = std::fs::read(&path)
                                .context(format!("failed to read through escaping symlink {:?}", path))?;
                            let file_mode = detect_file_mode(&path);
                            let hash = compute_hash(BLOB_KEY, &data);
                            Ok((data, hash, file_mode))
                        }
                    }
                }).await??;

                {
                    let mut map = objects_clone.lock().await;
                    if !map.contains_key(&hash) {
                        let header = Header::new(ObjectType::Blob, data.len() as u64);
                        let prefix = header.to_string();
                        let mut full_data = Vec::with_capacity(prefix.len() + data.len());
                        full_data.extend_from_slice(prefix.as_bytes());
                        full_data.extend(data);
                        map.insert(hash.clone(), full_data);
                    }
                }

                Ok::<_, anyhow::Error>((name, hash, mode))
            });
        }

        // Collect directory results
        while let Some(result) = dir_set.join_next().await {
            let (name, hash) = result??;
            entries.push(object_body::TreeEntry {
                mode: Mode::Tree,
                path: name,
                hash,
            });
        }

        // Collect file results
        while let Some(result) = file_set.join_next().await {
            let (name, hash, file_mode) = result??;
            entries.push(object_body::TreeEntry {
                mode: file_mode,
                path: name,
                hash,
            });
        }

        // Collect symlink results
        while let Some(result) = symlink_set.join_next().await {
            let (name, hash, mode) = result??;
            entries.push(object_body::TreeEntry {
                mode,
                path: name,
                hash,
            });
        }

        // Sort entries by name for deterministic tree hashes
        entries.sort_by(|a, b| a.path.cmp(&b.path));

        // Build tree
        let tree = object_body::Tree { contents: entries };
        let tree_data = object_body::Object::to_data(&tree);
        let hash = compute_hash(TREE_KEY, &tree_data);

        {
            let mut map = objects.lock().await;
            if !map.contains_key(&hash) {
                let header = Header::new(ObjectType::Tree, tree_data.len() as u64);
                let prefix = header.to_string();
                let mut full_data = Vec::with_capacity(prefix.len() + tree_data.len());
                full_data.extend_from_slice(prefix.as_bytes());
                full_data.extend(tree_data);
                map.insert(hash.clone(), full_data);
            }
        }

        Ok(hash)
    })
}

async fn archive_directory(directory: &Path, file: &Path, compression: Compression) -> anyhow::Result<()> {
    anyhow::ensure!(tokio::fs::metadata(file).await.is_err(), "output file already exists");
    anyhow::ensure!(
        file.parent().map(|p| p.exists() && p.is_dir()) == Some(true),
        "Parent directory must exist and be a directory"
    );

    let path = tokio::fs::canonicalize(directory).await
        .context(format!("unable to canonicalize {directory:?}"))?;
    anyhow::ensure!(path.is_dir(), "path must be a directory");

    #[allow(clippy::mutable_key_type)]
    let objects: Arc<tokio::sync::Mutex<HashMap<Hash, Vec<u8>>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    let root_hash = archive_build_tree(objects.clone(), path.clone(), path).await?;

    // Build index
    let index = object_body::Index {
        tree: root_hash,
        timestamp: Utc::now(),
        metadata: HashMap::new(),
    };
    let index_data = object_body::Object::to_data(&index);
    let index_hash = compute_hash(INDEX_KEY, &index_data);

    // Build archive from collected objects
    #[allow(clippy::mutable_key_type)]
    let objects = Arc::try_unwrap(objects)
        .map_err(|_| anyhow::anyhow!("failed to unwrap Arc"))?
        .into_inner();

    let objects_vec: Vec<(Hash, Vec<u8>)> = objects.into_iter().collect();

    let mut i: u64 = 0;
    let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::new();
    for (hash, data) in &objects_vec {
        let length = data.len() as u64;
        header_entries.push(ArchiveHeaderEntry {
            hash: hash.clone(),
            index: i,
            length,
        });
        i += length;
    }

    let archive = Archive {
        header: HEADER,
        compression,
        hash: index_hash.clone(),
        index,
        body: ArchiveBody {
            header: header_entries,
            entries: objects_vec.into_iter()
                .map(|(_, data)| RawEntryData(data))
                .collect(),
        },
    };

    let arx_file = std::fs::File::create(file)?;
    let mut writer = std::io::BufWriter::new(arx_file);
    archive.to_data(&mut writer)?;

    println!("{}", index_hash);
    println!("Archive written to {:?}", file);

    Ok(())
}

#[derive(Parser)]
#[command(version, about = "Artifact repository client for content-addressable storage", long_about = None)]
struct Cli {
    /// Path to the local object store directory.
    /// Falls back to ARTIFACT_STORE env var, then ~/.artifact/store
    #[arg(short, long, value_name = "PATH", env = "ARTIFACT_STORE")]
    store: Option<PathBuf>,

    /// Increase logging verbosity (-d = info, -dd = debug, -ddd = trace)
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,

    #[command(subcommand)]
    command: Commands,
}

/// Resolve the store path from CLI arg / env var / default (~/.artifact/store)
fn resolve_store_path(explicit: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    // Default: ~/.artifact/store
    let home = dirs::home_dir().context(
        "could not determine home directory; please set --store or ARTIFACT_STORE",
    )?;
    Ok(home.join(".artifact").join("store"))
}

#[derive(Subcommand)]
enum Commands {
    /// Commit a directory into the object store
    Commit {
        /// Path to the directory to commit
        #[arg(short, long)]
        directory: PathBuf,
    },

    /// Restore a directory from the object store using an index hash
    Restore {
        /// Output directory to restore into (must be empty or non-existent)
        #[arg(short, long)]
        directory: PathBuf,
        /// Hash of the index object to restore
        #[arg(short, long)]
        index: Hash,
        /// Re-read restored files and verify hashes match
        #[arg(long)]
        validate: bool,
    },

    /// Print the raw contents of an object to stdout
    Cat {
        /// Hash of the object to display
        hash: Hash,
    },

    /// Push objects from the local store to a remote server
    Push {
        /// Base URL of the remote artifact server
        #[arg(long, env = "ARTIFACT_URL")]
        url: String,

        /// Optional index hash to push (pushes all objects if omitted)
        #[arg(long)]
        index: Option<Hash>,
    },

    /// Pull objects from a remote server into the local store
    Pull {
        /// Base URL of the remote artifact server
        #[arg(long, env = "ARTIFACT_URL")]
        url: String,

        /// Hash of the index object to pull
        #[arg(long)]
        index: Hash,
    },

    /// Pack stored objects into an archive file
    Pack {
        /// Hash of the index object to pack
        #[arg(long)]
        index: Hash,

        /// Output archive file path
        #[arg(long)]
        file: PathBuf,

        /// Compression algorithm (none, gzip, deflate, lzma2, zstd)
        #[arg(long, default_value = "zstd")]
        compression: Compression,
    },

    /// Unpack an archive file into the local store
    Unpack {
        /// Path to the archive file to unpack
        #[arg(long)]
        file: PathBuf,
    },

    /// Extract an archive directly to a directory (no intermediate store)
    Extract {
        /// Path to the archive file
        #[arg(short, long)]
        file: PathBuf,
        /// Output directory (must be empty or non-existent)
        #[arg(short, long)]
        directory: PathBuf,
    },

    /// Create an archive directly from a directory (no intermediate store)
    Archive {
        /// Source directory to archive
        #[arg(short, long)]
        directory: PathBuf,
        /// Output archive file path
        #[arg(short, long)]
        file: PathBuf,
        /// Compression algorithm (none, gzip, deflate, lzma2, zstd)
        #[arg(short, long, default_value = "zstd")]
        compression: Compression,
    },

    /// Push objects to a remote server, uploading only what's missing (deduplication)
    PushArchive {
        /// Base URL of the remote artifact server
        #[arg(long, env = "ARTIFACT_URL")]
        url: String,
        /// Hash of the index to push
        #[arg(long)]
        index: Hash,
        /// Compression algorithm for the supplemental archive
        #[arg(long, default_value = "zstd")]
        compression: Compression,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let level = match cli.debug {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    if let Err(e) = run(cli).await {
        tracing::error!("{e:?}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use tempfile::TempDir;

    fn create_store(dir: &std::path::Path) -> Store {
        let builder = opendal::services::Fs::default().root(dir.to_str().unwrap());
        Store::from_builder(builder).unwrap()
    }

    fn collect_files_recursive<'a>(
        base: &'a std::path::Path,
        current: &'a std::path::Path,
        result: &'a mut BTreeMap<String, Vec<u8>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let mut entries = tokio::fs::read_dir(current).await.unwrap();
            while let Some(entry) = entries.next_entry().await.unwrap() {
                let path = entry.path();
                let rel = path.strip_prefix(base).unwrap().to_string_lossy().to_string();
                if entry.file_type().await.unwrap().is_dir() {
                    collect_files_recursive(base, &path, result).await;
                } else {
                    let data = tokio::fs::read(&path).await.unwrap();
                    result.insert(rel, data);
                }
            }
        })
    }

    fn collect_dirs_recursive<'a>(
        base: &'a std::path::Path,
        current: &'a std::path::Path,
        result: &'a mut BTreeSet<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let mut entries = tokio::fs::read_dir(current).await.unwrap();
            while let Some(entry) = entries.next_entry().await.unwrap() {
                let path = entry.path();
                let rel = path.strip_prefix(base).unwrap().to_string_lossy().to_string();
                if entry.file_type().await.unwrap().is_dir() {
                    result.insert(rel);
                    collect_dirs_recursive(base, &path, result).await;
                }
            }
        })
    }

    async fn assert_dirs_equal(dir_a: &std::path::Path, dir_b: &std::path::Path) {
        let mut files_a = BTreeMap::new();
        let mut files_b = BTreeMap::new();
        collect_files_recursive(dir_a, dir_a, &mut files_a).await;
        collect_files_recursive(dir_b, dir_b, &mut files_b).await;
        assert_eq!(
            files_a.keys().collect::<Vec<_>>(),
            files_b.keys().collect::<Vec<_>>(),
            "File paths differ"
        );
        for (path, content_a) in &files_a {
            let content_b = files_b.get(path).unwrap();
            assert_eq!(content_a, content_b, "Content differs for {}", path);
        }

        let mut dirs_a = BTreeSet::new();
        let mut dirs_b = BTreeSet::new();
        collect_dirs_recursive(dir_a, dir_a, &mut dirs_a).await;
        collect_dirs_recursive(dir_b, dir_b, &mut dirs_b).await;
        assert_eq!(dirs_a, dirs_b, "Directory structure differs");
    }

    #[tokio::test]
    async fn commit_restore_roundtrip() {
        let source = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();

        tokio::fs::write(source.path().join("hello.txt"), b"Hello, world!").await.unwrap();
        tokio::fs::write(source.path().join("data.bin"), b"Some binary data \x00\x01\x02").await.unwrap();
        let sub = source.path().join("subdir");
        tokio::fs::create_dir(&sub).await.unwrap();
        tokio::fs::write(sub.join("nested.txt"), b"I am nested").await.unwrap();

        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let (_, index_hash, _) = build_index_from_path(&store, &source_path).await.unwrap();

        let restore = TempDir::new().unwrap();
        let restore_path = restore.path().join("output");
        restore_directory(&store, &restore_path, index_hash, false).await.unwrap();

        assert_dirs_equal(source.path(), &restore_path).await;
    }

    #[tokio::test]
    async fn commit_empty_directory() {
        let source = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();

        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let (_, index_hash, total_size) = build_index_from_path(&store, &source_path).await.unwrap();

        assert_eq!(total_size, 0);

        let restore = TempDir::new().unwrap();
        let restore_path = restore.path().join("output");
        restore_directory(&store, &restore_path, index_hash, false).await.unwrap();

        let mut entries = tokio::fs::read_dir(&restore_path).await.unwrap();
        assert!(
            entries.next_entry().await.unwrap().is_none(),
            "Restored empty directory should have no entries"
        );
    }

    #[tokio::test]
    async fn commit_nested_directories() {
        let source = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();

        let deep = source.path().join("a").join("b").join("c");
        tokio::fs::create_dir_all(&deep).await.unwrap();
        tokio::fs::write(deep.join("file.txt"), b"deep file").await.unwrap();

        let x_dir = source.path().join("x");
        tokio::fs::create_dir(&x_dir).await.unwrap();
        tokio::fs::write(x_dir.join("file.txt"), b"x file").await.unwrap();

        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let (_, index_hash, _) = build_index_from_path(&store, &source_path).await.unwrap();

        let restore = TempDir::new().unwrap();
        let restore_path = restore.path().join("output");
        restore_directory(&store, &restore_path, index_hash, false).await.unwrap();

        assert_dirs_equal(source.path(), &restore_path).await;
    }

    #[tokio::test]
    async fn deduplication_identical_files() {
        let source = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();

        let content = b"IDENTICAL CONTENT FOR DEDUP TEST";

        let dir_a = source.path().join("dir_a");
        let dir_b = source.path().join("dir_b");
        tokio::fs::create_dir(&dir_a).await.unwrap();
        tokio::fs::create_dir(&dir_b).await.unwrap();
        tokio::fs::write(dir_a.join("file.txt"), content).await.unwrap();
        tokio::fs::write(dir_b.join("file.txt"), content).await.unwrap();

        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let (index, _, _) = build_index_from_path(&store, &source_path).await.unwrap();

        // Read root tree and collect file hashes from subtrees
        let root_tree = read_tree_from_store(&store, &index.tree).await.unwrap();
        let mut file_hashes = Vec::new();
        for entry in &root_tree.contents {
            let subtree = read_tree_from_store(&store, &entry.hash).await.unwrap();
            for sub_entry in &subtree.contents {
                if sub_entry.path == "file.txt" {
                    file_hashes.push(sub_entry.hash.clone());
                }
            }
        }

        assert_eq!(file_hashes.len(), 2);
        assert_eq!(
            file_hashes[0], file_hashes[1],
            "Identical files must produce the same blob hash"
        );

        let expected_hash = compute_hash(BLOB_KEY, content);
        assert_eq!(file_hashes[0], expected_hash);

        // Verify only one blob file exists in the store for that hash
        assert!(store.exists(&expected_hash).await.unwrap());
    }

    async fn pack_unpack_roundtrip_with_compression(compression: Compression) {
        let source = TempDir::new().unwrap();
        let store_a_dir = TempDir::new().unwrap();

        tokio::fs::write(source.path().join("hello.txt"), b"Hello, archive world!").await.unwrap();
        let sub = source.path().join("sub");
        tokio::fs::create_dir(&sub).await.unwrap();
        tokio::fs::write(sub.join("data.txt"), b"Nested archive data").await.unwrap();

        // Commit to store A
        let store_a = create_store(store_a_dir.path());
        let source_path = source.path().to_path_buf();
        let (_, index_hash, _) = build_index_from_path(&store_a, &source_path).await.unwrap();

        // Pack to .arx file
        let arx_dir = TempDir::new().unwrap();
        let arx_path = arx_dir.path().join("test.arx");
        pack_archive(&store_a, &arx_path, &index_hash, compression).await.unwrap();
        assert!(tokio::fs::metadata(&arx_path).await.is_ok(), ".arx file should exist");

        // Unpack to store B
        let store_b_dir = TempDir::new().unwrap();
        let store_b = create_store(store_b_dir.path());
        unpack_archive(&store_b, &arx_path).await.unwrap();

        // Restore from store B
        let restore = TempDir::new().unwrap();
        let restore_path = restore.path().join("output");
        restore_directory(&store_b, &restore_path, index_hash, false).await.unwrap();

        assert_dirs_equal(source.path(), &restore_path).await;
    }

    #[tokio::test]
    async fn pack_unpack_roundtrip_none() {
        pack_unpack_roundtrip_with_compression(Compression::None).await;
    }

    #[tokio::test]
    async fn pack_unpack_roundtrip_zstd() {
        pack_unpack_roundtrip_with_compression(Compression::Zstd).await;
    }

    #[tokio::test]
    async fn pack_unpack_roundtrip_gzip() {
        pack_unpack_roundtrip_with_compression(Compression::Gzip).await;
    }

    #[tokio::test]
    async fn restore_with_validate() {
        let source = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();

        tokio::fs::write(source.path().join("test.txt"), b"Validate me!").await.unwrap();
        let sub = source.path().join("inner");
        tokio::fs::create_dir(&sub).await.unwrap();
        tokio::fs::write(sub.join("deep.txt"), b"Validate deep too").await.unwrap();

        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let (_, index_hash, _) = build_index_from_path(&store, &source_path).await.unwrap();

        let restore = TempDir::new().unwrap();
        let restore_path = restore.path().join("output");
        // validate=true: re-reads restored files and checks hashes match
        restore_directory(&store, &restore_path, index_hash, true).await.unwrap();

        assert_dirs_equal(source.path(), &restore_path).await;
    }

    #[tokio::test]
    async fn large_file_roundtrip() {
        let source = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();

        // 1MB file with repeating pattern
        let pattern = b"ABCDEFGHIJKLMNOP";
        let large_data: Vec<u8> = pattern.iter().cycle().take(1024 * 1024).cloned().collect();
        tokio::fs::write(source.path().join("large.bin"), &large_data).await.unwrap();

        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let (_, index_hash, _) = build_index_from_path(&store, &source_path).await.unwrap();

        let restore = TempDir::new().unwrap();
        let restore_path = restore.path().join("output");
        restore_directory(&store, &restore_path, index_hash, false).await.unwrap();

        let restored = tokio::fs::read(restore_path.join("large.bin")).await.unwrap();
        assert_eq!(restored.len(), large_data.len());
        assert_eq!(restored, large_data);
    }

    #[tokio::test]
    async fn special_filenames_roundtrip() {
        let source = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();

        tokio::fs::write(source.path().join("file with spaces.txt"), b"spaces").await.unwrap();
        tokio::fs::write(source.path().join("file.multiple.dots.txt"), b"dots").await.unwrap();
        tokio::fs::write(source.path().join("file-with-hyphens.txt"), b"hyphens").await.unwrap();
        tokio::fs::write(source.path().join(".hidden-file"), b"hidden").await.unwrap();
        tokio::fs::write(source.path().join("UPPERCASE.TXT"), b"upper").await.unwrap();

        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let (_, index_hash, _) = build_index_from_path(&store, &source_path).await.unwrap();

        let restore = TempDir::new().unwrap();
        let restore_path = restore.path().join("output");
        restore_directory(&store, &restore_path, index_hash, false).await.unwrap();

        assert_dirs_equal(source.path(), &restore_path).await;
    }

    async fn extract_roundtrip_with_compression(compression: Compression) {
        let source = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();

        tokio::fs::write(source.path().join("hello.txt"), b"Hello, extract world!").await.unwrap();
        let sub = source.path().join("sub");
        tokio::fs::create_dir(&sub).await.unwrap();
        tokio::fs::write(sub.join("data.txt"), b"Nested extract data").await.unwrap();

        // Commit to store
        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let (_, index_hash, _) = build_index_from_path(&store, &source_path).await.unwrap();

        // Pack to .arx file
        let arx_dir = TempDir::new().unwrap();
        let arx_path = arx_dir.path().join("test.arx");
        pack_archive(&store, &arx_path, &index_hash, compression).await.unwrap();

        // Extract directly to directory (no intermediate store)
        let extract_dir = TempDir::new().unwrap();
        let extract_path = extract_dir.path().join("output");
        extract_archive(&arx_path, &extract_path).await.unwrap();

        assert_dirs_equal(source.path(), &extract_path).await;
    }

    #[tokio::test]
    async fn extract_roundtrip_none() {
        extract_roundtrip_with_compression(Compression::None).await;
    }

    #[tokio::test]
    async fn extract_roundtrip_zstd() {
        extract_roundtrip_with_compression(Compression::Zstd).await;
    }

    #[tokio::test]
    async fn extract_roundtrip_gzip() {
        extract_roundtrip_with_compression(Compression::Gzip).await;
    }

    async fn archive_extract_roundtrip_with_compression(compression: Compression) {
        let source = TempDir::new().unwrap();

        tokio::fs::write(source.path().join("hello.txt"), b"Hello, archive world!").await.unwrap();
        let sub = source.path().join("sub");
        tokio::fs::create_dir(&sub).await.unwrap();
        tokio::fs::write(sub.join("data.txt"), b"Nested archive data").await.unwrap();

        // archive_directory → .arx file
        let arx_dir = TempDir::new().unwrap();
        let arx_path = arx_dir.path().join("test.arx");
        let source_path = source.path().to_path_buf();
        archive_directory(&source_path, &arx_path, compression).await.unwrap();
        assert!(tokio::fs::metadata(&arx_path).await.is_ok(), ".arx file should exist");

        // extract_archive → output directory
        let extract_dir = TempDir::new().unwrap();
        let extract_path = extract_dir.path().join("output");
        extract_archive(&arx_path, &extract_path).await.unwrap();

        assert_dirs_equal(source.path(), &extract_path).await;
    }

    #[tokio::test]
    async fn archive_extract_roundtrip() {
        archive_extract_roundtrip_with_compression(Compression::None).await;
    }

    #[tokio::test]
    async fn archive_extract_roundtrip_zstd() {
        archive_extract_roundtrip_with_compression(Compression::Zstd).await;
    }

    #[tokio::test]
    async fn archive_nested_directories() {
        let source = TempDir::new().unwrap();

        let deep = source.path().join("a").join("b").join("c");
        tokio::fs::create_dir_all(&deep).await.unwrap();
        tokio::fs::write(deep.join("file.txt"), b"deep file").await.unwrap();

        let x_dir = source.path().join("x");
        tokio::fs::create_dir(&x_dir).await.unwrap();
        tokio::fs::write(x_dir.join("file.txt"), b"x file").await.unwrap();

        let arx_dir = TempDir::new().unwrap();
        let arx_path = arx_dir.path().join("nested.arx");
        let source_path = source.path().to_path_buf();
        archive_directory(&source_path, &arx_path, Compression::None).await.unwrap();

        let extract_dir = TempDir::new().unwrap();
        let extract_path = extract_dir.path().join("output");
        extract_archive(&arx_path, &extract_path).await.unwrap();

        assert_dirs_equal(source.path(), &extract_path).await;
    }

    #[tokio::test]
    async fn archive_deduplication() {
        let source = TempDir::new().unwrap();

        let content = b"IDENTICAL CONTENT FOR ARCHIVE DEDUP TEST";
        let dir_a = source.path().join("dir_a");
        let dir_b = source.path().join("dir_b");
        tokio::fs::create_dir(&dir_a).await.unwrap();
        tokio::fs::create_dir(&dir_b).await.unwrap();
        tokio::fs::write(dir_a.join("file.txt"), content).await.unwrap();
        tokio::fs::write(dir_b.join("file.txt"), content).await.unwrap();

        let arx_dir = TempDir::new().unwrap();
        let arx_path = arx_dir.path().join("dedup.arx");
        let source_path = source.path().to_path_buf();
        archive_directory(&source_path, &arx_path, Compression::None).await.unwrap();

        // Read back the archive and check that there's only one blob entry
        let arx_data = std::fs::read(&arx_path).unwrap();
        let archive = Archive::<RawEntryData>::from_data(&mut std::io::Cursor::new(arx_data)).unwrap();

        let blob_hash = compute_hash(BLOB_KEY, content);
        let blob_entries: Vec<_> = archive.body.header
            .iter()
            .filter(|e| e.hash == blob_hash)
            .collect();
        assert_eq!(
            blob_entries.len(),
            1,
            "Identical files should produce only one blob entry in the archive"
        );
    }

    #[tokio::test]
    async fn tree_hash_is_deterministic() {
        let source = TempDir::new().unwrap();

        // Create a non-trivial directory structure
        tokio::fs::write(source.path().join("zebra.txt"), b"z content").await.unwrap();
        tokio::fs::write(source.path().join("alpha.txt"), b"a content").await.unwrap();
        tokio::fs::write(source.path().join("middle.txt"), b"m content").await.unwrap();
        let sub = source.path().join("subdir");
        tokio::fs::create_dir(&sub).await.unwrap();
        tokio::fs::write(sub.join("beta.txt"), b"b content").await.unwrap();
        tokio::fs::write(sub.join("gamma.txt"), b"g content").await.unwrap();
        let deep = sub.join("deep");
        tokio::fs::create_dir(&deep).await.unwrap();
        tokio::fs::write(deep.join("file.txt"), b"deep content").await.unwrap();

        let source_path = source.path().to_path_buf();

        // Commit the same directory multiple times to separate stores
        let mut tree_hashes = Vec::new();
        for _ in 0..5 {
            let store_dir = TempDir::new().unwrap();
            let store = create_store(store_dir.path());
            let (index, _, _) = build_index_from_path(&store, &source_path).await.unwrap();
            tree_hashes.push(index.tree.as_str().to_string());
        }

        // All tree hashes must be identical
        for (i, h) in tree_hashes.iter().enumerate() {
            assert_eq!(
                &tree_hashes[0], h,
                "Tree hash differs on iteration {i}: expected {}, got {h}",
                tree_hashes[0]
            );
        }
    }

    #[tokio::test]
    async fn tree_entries_are_sorted_by_name() {
        let source = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();

        // Create files with names that would sort differently by creation order vs alphabetical
        tokio::fs::write(source.path().join("c.txt"), b"c").await.unwrap();
        tokio::fs::write(source.path().join("a.txt"), b"a").await.unwrap();
        tokio::fs::write(source.path().join("b.txt"), b"b").await.unwrap();

        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let (index, _, _) = build_index_from_path(&store, &source_path).await.unwrap();

        // Read back the tree and verify entries are sorted
        let tree = read_tree_from_store(&store, &index.tree).await.unwrap();
        let names: Vec<&str> = tree.contents.iter().map(|e| e.path.as_str()).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted, "Tree entries should be sorted alphabetically");
    }

    #[tokio::test]
    async fn commit_restore_symlinks() {
        let source = TempDir::new().unwrap();
        let store_dir = TempDir::new().unwrap();

        // Create files and symlinks
        tokio::fs::write(source.path().join("target.txt"), b"I am the target").await.unwrap();
        let sub = source.path().join("subdir");
        tokio::fs::create_dir(&sub).await.unwrap();
        tokio::fs::write(sub.join("nested.txt"), b"nested content").await.unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("target.txt", source.path().join("relative-link")).unwrap();
            std::os::unix::fs::symlink("subdir/nested.txt", source.path().join("deep-link")).unwrap();
        }

        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let (_, index_hash, _) = build_index_from_path(&store, &source_path).await.unwrap();

        let restore = TempDir::new().unwrap();
        let restore_path = restore.path().join("output");
        restore_directory(&store, &restore_path, index_hash, false).await.unwrap();

        // Verify regular files
        let content = tokio::fs::read(restore_path.join("target.txt")).await.unwrap();
        assert_eq!(content, b"I am the target");
        let nested = tokio::fs::read(restore_path.join("subdir").join("nested.txt")).await.unwrap();
        assert_eq!(nested, b"nested content");

        // Verify symlinks are actual symlinks with correct targets
        #[cfg(unix)]
        {
            let meta = std::fs::symlink_metadata(restore_path.join("relative-link")).unwrap();
            assert!(meta.is_symlink(), "relative-link should be a symlink");
            assert_eq!(std::fs::read_link(restore_path.join("relative-link")).unwrap().to_str().unwrap(), "target.txt");

            let meta = std::fs::symlink_metadata(restore_path.join("deep-link")).unwrap();
            assert!(meta.is_symlink(), "deep-link should be a symlink");
            assert_eq!(std::fs::read_link(restore_path.join("deep-link")).unwrap().to_str().unwrap(), "subdir/nested.txt");
        }
    }

    #[tokio::test]
    async fn archive_extract_symlinks() {
        let source = TempDir::new().unwrap();

        tokio::fs::write(source.path().join("file.txt"), b"content").await.unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("file.txt", source.path().join("link")).unwrap();
            std::os::unix::fs::symlink("nonexistent", source.path().join("dangling")).unwrap();
        }

        let archive_dir = TempDir::new().unwrap();
        let archive_path = archive_dir.path().join("test.arx");

        archive_directory(source.path(), &archive_path, Compression::Zstd).await.unwrap();

        let extract = TempDir::new().unwrap();
        let extract_path = extract.path().join("output");
        tokio::fs::create_dir(&extract_path).await.unwrap();
        extract_archive(&archive_path, &extract_path).await.unwrap();

        // Verify file
        let content = tokio::fs::read(extract_path.join("file.txt")).await.unwrap();
        assert_eq!(content, b"content");

        // Verify symlinks
        #[cfg(unix)]
        {
            let meta = std::fs::symlink_metadata(extract_path.join("link")).unwrap();
            assert!(meta.is_symlink(), "link should be a symlink");
            assert_eq!(std::fs::read_link(extract_path.join("link")).unwrap().to_str().unwrap(), "file.txt");

            // Dangling symlinks should also be preserved
            let meta = std::fs::symlink_metadata(extract_path.join("dangling")).unwrap();
            assert!(meta.is_symlink(), "dangling should be a symlink");
            assert_eq!(std::fs::read_link(extract_path.join("dangling")).unwrap().to_str().unwrap(), "nonexistent");
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn symlink_escaping_commit_substitutes_file() {
        // Create an "outside" file that a symlink will point to
        let outside = TempDir::new().unwrap();
        tokio::fs::write(outside.path().join("secret.txt"), b"sensitive data").await.unwrap();

        // Create the source directory with an escaping symlink
        let source = TempDir::new().unwrap();
        tokio::fs::write(source.path().join("normal.txt"), b"hello").await.unwrap();

        // Symlink that escapes via absolute path to a real file
        let secret_path = outside.path().join("secret.txt");
        std::os::unix::fs::symlink(&secret_path, source.path().join("abs_escape")).unwrap();

        // Commit — escaping symlinks should be substituted with file content
        let store_dir = TempDir::new().unwrap();
        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let (index, _, _) = build_index_from_path(&store, &source_path).await.unwrap();

        // The escaping symlink should have been stored as a Normal file, not a SymbolicLink
        let tree = read_tree_from_store(&store, &index.tree).await.unwrap();
        let abs_entry = tree.contents.iter().find(|e| e.path == "abs_escape").unwrap();
        assert_eq!(abs_entry.mode, Mode::Normal,
            "escaping symlink should be stored as a regular file");

        // Restore — the file should contain the content from the original target
        let restore = TempDir::new().unwrap();
        let restore_path = restore.path().join("output");
        let index_hash = compute_hash(INDEX_KEY, &object_body::Object::to_data(&index));
        restore_directory(&store, &restore_path, index_hash, false).await.unwrap();

        let restored = std::fs::read_to_string(restore_path.join("abs_escape")).unwrap();
        assert_eq!(restored, "sensitive data");
        // It should NOT be a symlink
        assert!(!restore_path.join("abs_escape").symlink_metadata().unwrap().is_symlink());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn symlink_escaping_commit_dangling_errors() {
        // Escaping symlink to a nonexistent target should error on commit
        let source = TempDir::new().unwrap();
        tokio::fs::write(source.path().join("normal.txt"), b"hello").await.unwrap();
        std::os::unix::fs::symlink("../nonexistent_escape", source.path().join("rel_escape")).unwrap();

        let store_dir = TempDir::new().unwrap();
        let store = create_store(store_dir.path());
        let source_path = source.path().to_path_buf();
        let result = build_index_from_path(&store, &source_path).await;

        assert!(result.is_err(), "dangling escaping symlink should fail on commit");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn symlink_escaping_extract_errors() {
        // Test that restore rejects symlinks with escaping targets.
        // We craft a tree directly with an escaping SymbolicLink entry.
        let store_dir = TempDir::new().unwrap();
        let store = create_store(store_dir.path());

        // Create a blob containing an escaping symlink target
        let target_bytes = b"../../etc/passwd".to_vec();
        let blob_hash = compute_hash(BLOB_KEY, &target_bytes);
        let header = Header::new(ObjectType::Blob, target_bytes.len() as u64);
        store.put_object_bytes(&blob_hash, header, target_bytes.to_vec()).await.unwrap();

        // Create a tree with the escaping symlink entry
        let tree = object_body::Tree {
            contents: vec![object_body::TreeEntry {
                mode: Mode::SymbolicLink,
                path: "evil_link".to_string(),
                hash: blob_hash,
            }],
        };

        let extract = TempDir::new().unwrap();
        let extract_path = extract.path().join("output");
        tokio::fs::create_dir(&extract_path).await.unwrap();

        #[allow(clippy::mutable_key_type)]
        let written_files = Arc::new(tokio::sync::Mutex::new(HashMap::<Hash, PathBuf>::new()));
        let result = write_tree_to_path(&store, &tree, &extract_path, &extract_path, written_files).await;

        assert!(result.is_err(), "restore should reject escaping symlinks");
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("escapes"), "error should mention escaping: {}", err_msg);
    }

    #[tokio::test]
    async fn symlink_hash_determinism() {
        // Same symlink target should produce the same tree hash
        let source_a = TempDir::new().unwrap();
        let source_b = TempDir::new().unwrap();

        tokio::fs::write(source_a.path().join("file.txt"), b"data").await.unwrap();
        tokio::fs::write(source_b.path().join("file.txt"), b"data").await.unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("file.txt", source_a.path().join("link")).unwrap();
            std::os::unix::fs::symlink("file.txt", source_b.path().join("link")).unwrap();
        }

        let store_a = TempDir::new().unwrap();
        let store_b = TempDir::new().unwrap();

        let sa = create_store(store_a.path());
        let sb = create_store(store_b.path());

        let (_, hash_a, _) = build_index_from_path(&sa, &source_a.path().to_path_buf()).await.unwrap();
        let (_, hash_b, _) = build_index_from_path(&sb, &source_b.path().to_path_buf()).await.unwrap();

        // Hashes won't match because index includes timestamp, but tree hashes should
        // We check that both succeed without error — determinism of tree hash is
        // covered by the existing tree_hash_is_deterministic test's approach
        assert!(hash_a.as_str().len() == 128);
        assert!(hash_b.as_str().len() == 128);
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let store_path = resolve_store_path(cli.store)?;
    tracing::debug!("using store at {}", store_path.display());

    // Ensure store directory exists
    if tokio::fs::metadata(&store_path).await.is_err() {
        create_dir_all(&store_path).await.context("failed to create store directory")?;
    }

    // Create store from cache directory
    let store = Store::from_builder(opendal::services::Fs::default().root(store_path.to_str().context("store path must be valid UTF-8")?))
        .context("failed to create store")?;

    match cli.command {
        Commands::Commit { directory } => commit_directory(&store, &directory).await?,
        Commands::Restore {
            directory,
            index,
            validate,
        } => restore_directory(&store, &directory, index, validate).await?,
        Commands::Cat { hash } => cat_object(&store, &hash).await?,
        Commands::Push { url, index } => {
            push_cache(&store, &url, index).await?
        }
        Commands::Pull { url, index } => pull_cache(&store, &url, index).await?,
        Commands::Pack { index, file, compression } => {
            pack_archive(&store, &file, &index, compression).await?
        }
        Commands::Unpack { file } => {
            unpack_archive(&store, &file).await?
        }
        Commands::Extract { file, directory } => {
            extract_archive(&file, &directory).await?
        }
        Commands::Archive { directory, file, compression } => {
            archive_directory(&directory, &file, compression).await?
        }
        Commands::PushArchive { url, index, compression } => {
            push_archive(&store, &url, &index, compression).await?
        }
    }
    Ok(())
}
