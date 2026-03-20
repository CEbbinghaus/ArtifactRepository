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
    path::PathBuf,
    sync::Arc,
};

use futures::io::AsyncReadExt as FuturesReadExt;
use tokio::{
    fs::{File, create_dir, create_dir_all, read_dir},
    io::{AsyncReadExt, AsyncWriteExt, AsyncBufReadExt, BufReader, BufWriter},
    task::JoinSet,
};

// Helper to build Index from filesystem
#[allow(clippy::ptr_arg)]
async fn build_index_from_path(store: &Store, path: &PathBuf) -> anyhow::Result<(object_body::Index, Hash, u128)> {
    anyhow::ensure!(path.is_dir(), "path must be a directory");

    let (tree_hash, total_size) = build_tree_from_dir(store.clone(), path.clone()).await?;

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

#[allow(clippy::type_complexity)]
fn build_tree_from_dir(
    store: Store,
    path: PathBuf,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<(Hash, u128)>> + Send>> {
    Box::pin(async move {
        anyhow::ensure!(path.is_dir(), "path must be a directory");
        let mut dir_entries = read_dir(&path).await?;

        // Collect all entries first so we can process them concurrently
        let mut dir_paths = Vec::new();
        let mut file_paths = Vec::new();
        while let Some(entry) = dir_entries.next_entry().await? {
            let entry_path = entry.path();
            if entry.file_type().await?.is_dir() {
                dir_paths.push(entry_path);
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
            dir_set.spawn(async move {
                let name = dir_path.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
                let (hash, size) = build_tree_from_dir(store_clone, dir_path).await?;
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
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();

                let (data, hash) = tokio::task::spawn_blocking({
                    let path = file_path.clone();
                    move || -> anyhow::Result<(Vec<u8>, Hash)> {
                        let data = std::fs::read(&path)?;
                        let hash = compute_hash(BLOB_KEY, &data);
                        Ok((data, hash))
                    }
                }).await??;
                let size = data.len() as u64;

                if !store_clone.exists(&hash).await.unwrap_or(false) {
                    let header = Header::new(ObjectType::Blob, size);
                    store_clone.put_object_bytes(&hash, header, data).await?;
                }

                Ok::<_, anyhow::Error>((name, hash, size))
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
            let (name, hash, size) = result??;
            total_size += size as u128;
            entries.push(object_body::TreeEntry {
                mode: Mode::Normal,
                path: name,
                hash,
            });
        }

        // Build and store tree
        let tree = object_body::Tree { contents: entries };
        let tree_data = object_body::Object::to_data(&tree);
        let hash = compute_hash(TREE_KEY, &tree_data);

        if !store.exists(&hash).await.unwrap_or(false) {
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

async fn commit_directory(store: &Store, path: &PathBuf) -> anyhow::Result<()> {
    let path_meta = tokio::fs::metadata(path).await?;
    anyhow::ensure!(path_meta.is_dir(), "path must be a directory");

    let path = tokio::fs::canonicalize(path).await.context(format!("unable to canonicalize {path:?}"))?;

    let (_index, index_hash, total_size) = build_index_from_path(store, &path).await?;

    println!("Finished generating Index for {} bytes of data", total_size);
    println!("{}", index_hash);
    Ok(())
}

async fn restore_directory(store: &Store, path: &PathBuf, index_hash: Hash, validate: bool) -> anyhow::Result<()> {
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

    write_tree_to_path(store, &tree, path).await?;

    if validate {
        validate_tree_at_path(store, &tree, path).await?;
    }
    Ok(())
}

#[allow(clippy::ptr_arg)]
fn validate_tree_at_path<'a>(
    store: &'a Store,
    tree: &'a object_body::Tree,
    path: &'a PathBuf,
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

#[allow(clippy::ptr_arg)]
fn write_tree_to_path<'a>(
    store: &'a Store,
    tree: &'a object_body::Tree,
    path: &'a PathBuf,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(64));
        let mut blob_set = JoinSet::new();

        for entry in &tree.contents {
            let entry_path = path.join(&entry.path);
            let header = store.get_object(&entry.hash).await?.header;

            match header.object_type {
                ObjectType::Tree => {
                    create_dir(&entry_path).await.context("failed to create directory")?;
                    let subtree = read_tree_from_store(store, &entry.hash).await?;
                    write_tree_to_path(store, &subtree, &entry_path).await?;
                }
                ObjectType::Blob => {
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
                            Ok(())
                        }).await?
                    });
                }
                ObjectType::Index => anyhow::bail!("invalid object type in tree"),
            }
        }

        while let Some(result) = blob_set.join_next().await {
            result??;
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

async fn push_cache(cache: &PathBuf, url: &String, hash: Option<Hash>) -> anyhow::Result<()> {
    if let Some(hash) = hash {
        let file = hash.get_path(cache);
        upload_object(&hash, &file, url).await?;
        return Ok(());
    }

    let mut entries = read_dir(cache).await?;

    while let Some(entry) = entries.next_entry().await? {
        let metadata = match entry.metadata().await {
            Ok(m) => m,
            Err(_) => continue,
        };

        if metadata.is_file() {
            continue;
        }

        let prefix = entry.file_name();

        let mut sub_entries = read_dir(entry.path()).await?;

        while let Some(sub_entry) = sub_entries.next_entry().await? {
            let metadata = match sub_entry.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };

            if !metadata.is_file() {
                continue;
            }

            let name = format!(
                "{}{}",
                prefix.to_string_lossy(),
                sub_entry.file_name().to_string_lossy()
            );
            let hash = Hash::try_from(name).context("invalid hash")?;

            let sub_path = sub_entry.path();
            upload_object(&hash, &sub_path, url).await?;
        }
    }
    Ok(())
}

fn pull_tree<'a>(cache: &'a PathBuf, url: &'a String, tree_hash: &'a Hash) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let tree_path = tree_hash.get_path(cache);

        // tree already exists locally so we can skip downloading it
        if tokio::fs::metadata(&tree_path).await.is_err() {
            let header = download_object(tree_hash, &tree_path, url).await
                .context(format!("unable to download object with hash {tree_hash}"))?;
            anyhow::ensure!(header.object_type == ObjectType::Tree, "expected tree object");
        }

        let mut file = File::open(tree_path).await.context("tree file should exist")?;
        let mut index_data = Vec::new();
        let _ = file
            .read_to_end(&mut index_data)
            .await
            .context("file should be readable")?;

        let (_, data) =
            read_header_and_body(&index_data).context("tree should be in the correct format")?;

        let index_body: object_body::Tree = object_body::Object::from_data(data)?;

        for entry in index_body.contents {
            let obj_path = entry.hash.get_path(cache);
            let header = download_object(&entry.hash, &obj_path, url).await
                .context(format!("unable to download object with hash {}", entry.hash))?;

            anyhow::ensure!(header.object_type != ObjectType::Index, "unexpected index object in tree");

            if header.object_type == ObjectType::Tree {
                pull_tree(cache, url, &entry.hash).await?;
            }
        }
        Ok(())
    })
}

async fn pull_cache(cache: &PathBuf, url: &String, hash: Hash) -> anyhow::Result<()> {
    let index_path = hash.get_path(cache);
    let header = download_object(&hash, &index_path, url).await
        .context(format!("unable to download object with hash {hash}"))?;

    anyhow::ensure!(header.object_type == ObjectType::Index, "expected index object");

    let mut file = File::open(index_path).await.context("index file should exist")?;
    let mut index_data = Vec::new();
    let _ = file
        .read_to_end(&mut index_data)
        .await
        .context("file should be readable")?;

    let (_, data) =
        read_header_and_body(&index_data).context("index should be in the correct format")?;

    let index_body: object_body::Index = object_body::Object::from_data(data)?;

    pull_tree(cache, url, &index_body.tree).await?;
    Ok(())
}

#[allow(clippy::ptr_arg)]
async fn upload_object(hash: &Hash, file: &PathBuf, url: &String) -> anyhow::Result<()> {
    let file_data = tokio::task::spawn_blocking({
        let file = file.clone();
        move || std::fs::read(&file)
    }).await?.context("file should exist")?;

    let header = read_header_from_slice(
        &file_data[..file_data.iter().position(|&b| b == 0).unwrap_or(file_data.len())]
    ).context("file should be a valid object")?;

    let url = format!("{url}/object/{hash}");

    println!("Sending put request to {url}");

    // Get the body after the header (skip past the null terminator)
    let header_end = file_data.iter().position(|&b| b == 0).unwrap_or(0) + 1;
    let body = &file_data[header_end..];

    ureq::put(&url)
        .header("Object-Type", header.object_type.to_str())
        .header("Object-Size", &header.size.to_string())
        .send(body)
        .context("failed to upload object")?;

    Ok(())
}

async fn download_object(hash: &Hash, file: &PathBuf, url: &String) -> anyhow::Result<Header> {
    let url = format!("{url}/object/{hash}");

    let dir = file.parent().context("path should not be at root")?;
    create_dir_all(dir).await.context("failed to create directory")?;

    if tokio::fs::metadata(file).await.is_ok() {
        let file_handle = File::open(file).await.context("file should exist")?;
        let mut reader = BufReader::new(file_handle);

        let mut buffer = Vec::new();
        reader
            .read_until(0, &mut buffer)
            .await
            .context("header should exist within file")?;

        // subtract one to get rid of the null byte
        return read_header_from_slice(&buffer[..buffer.len() - 1])
            .context("invalid header in file");
    }

    println!("Sending get request to {url}");

    let mut response = ureq::get(&url).call()
        .context("failed to send GET request")?;

    let file_handle = File::create(file).await.context("failed to create file")?;
    let mut writer = BufWriter::new(file_handle);

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

    writer.write_all(header.to_string().as_bytes()).await?;

    let mut data = [0u8; 65536];
    let mut reader = response.body_mut().as_reader();
    loop {
        let num = std::io::Read::read(&mut reader, &mut data)?;
        if num == 0 {
            break;
        }

        writer.write_all(&data[..num]).await?;
    }

    writer.flush().await?;

    Ok(header)
}

async fn pack_archive(store: &Store, path: &PathBuf, index_hash: &Hash, compression: Compression) -> anyhow::Result<()> {
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

    // 3. Collect all hashes (including the index hash)
    let all_hashes: Vec<Hash> = std::iter::once(index_hash.clone())
        .chain(headers.keys().cloned())
        .collect();

    // 4. POST to /missing to find which objects the server needs
    #[derive(serde::Serialize)]
    struct MissingRequest {
        hashes: Vec<Hash>,
    }
    #[derive(serde::Deserialize)]
    struct MissingResponse {
        missing: Vec<Hash>,
    }

    let missing_url = format!("{url}/missing");
    let missing_resp: MissingResponse = ureq::post(&missing_url)
        .send_json(&MissingRequest { hashes: all_hashes })
        .context("failed to query missing objects")?
        .body_mut()
        .read_json()
        .context("failed to parse missing response")?;

    // 5. If no missing objects, nothing to do
    if missing_resp.missing.is_empty() {
        println!("All objects already present on server");
        return Ok(());
    }

    // 6. Build supplemental archive with all trees + missing objects
    #[allow(clippy::mutable_key_type)]
    let missing_set: std::collections::HashSet<Hash> = missing_resp.missing.into_iter().collect();

    let mut offset = 0u64;
    let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::new();
    let mut entry_data_vec: Vec<RawEntryData> = Vec::new();

    for (hash, (header, data)) in &headers {
        // Always include tree objects; for others, only include if missing
        if header.object_type != ObjectType::Tree && !missing_set.contains(hash) {
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

    // 8. POST the archive to /upload
    let upload_url = format!("{url}/upload");
    let response_text = ureq::post(&upload_url)
        .header("Content-Type", "application/octet-stream")
        .send(&buf[..])
        .context("failed to upload archive")?
        .body_mut()
        .read_to_string()
        .context("failed to read upload response")?;

    println!("{}", response_text);
    println!("Pushed {} objects ({} bytes) to {}", num_objects, total_bytes, url);

    Ok(())
}

async fn unpack_archive(cache: &PathBuf, path: &PathBuf) -> anyhow::Result<()> {
    let path_meta = tokio::fs::metadata(path).await?;
    anyhow::ensure!(path_meta.is_file(), "path must be a file");

    let file = std::fs::File::open(path)?;
    let mut file = std::io::BufReader::new(file);

    let archive = Archive::<RawEntryData>::from_data(&mut file)?;

    anyhow::ensure!(archive.body.entries.len() == archive.body.header.len(), "archive entries and headers length mismatch");

    println!("Successfully read archive, Index {}", archive.hash);

    let index_data = object_body::Object::to_data(&archive.index);
    let computed_hash = compute_hash(INDEX_KEY, &index_data);
    anyhow::ensure!(computed_hash == archive.hash, "index hash mismatch");

    let index_file_path = archive.hash.get_path(cache);
    let _ = create_dir_all(index_file_path.parent().context("path should not be at root")?).await;

    {
        let index_header = Header::new(ObjectType::Index, index_data.len() as u64);
        let mut index_file = File::create(index_file_path).await?;
        index_file.write_all(index_header.to_string().as_bytes()).await?;
        index_file.write_all(&index_data).await?;
    }

    // Pre-create all needed parent directories
    let mut dirs_to_create = std::collections::HashSet::new();
    for header in &archive.body.header {
        let object_path = header.hash.get_path(cache);
        if let Some(parent) = object_path.parent() {
            dirs_to_create.insert(parent.to_path_buf());
        }
    }
    for dir in dirs_to_create {
        let _ = create_dir_all(&dir).await;
    }

    let semaphore = Arc::new(tokio::sync::Semaphore::new(64));
    let mut set = JoinSet::new();

    for (header, entry) in archive.body.header.into_iter().zip(archive.body.entries.into_iter()) {
        let object_path = header.hash.get_path(cache);
        let sem = semaphore.clone();
        set.spawn(async move {
            let _permit = sem.acquire().await
                .map_err(|e| anyhow::anyhow!("semaphore: {e}"))?;

            // Skip if already exists (cheap check)
            if tokio::fs::metadata(&object_path).await.is_ok() {
                return Ok::<_, anyhow::Error>(());
            }

            let data = entry.turn_into_vec();
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                std::fs::write(&object_path, &data)?;
                Ok(())
            }).await??;

            Ok(())
        });
    }

    while let Some(result) = set.join_next().await {
        result??;
    }

    Ok(())
}

#[allow(clippy::ptr_arg)]
async fn extract_archive(file: &PathBuf, directory: &PathBuf) -> anyhow::Result<()> {
    if tokio::fs::metadata(directory).await.is_err() {
        create_dir_all(directory).await.context("failed to create output directory")?;
    }

    let dir_meta = tokio::fs::metadata(directory).await?;
    anyhow::ensure!(dir_meta.is_dir(), "output path must be a directory");

    let mut entries = read_dir(directory).await?;
    anyhow::ensure!(entries.next_entry().await?.is_none(), "output directory must be empty");

    let file = file.clone();
    let archive = tokio::task::spawn_blocking(move || {
        let f = std::fs::File::open(&file)?;
        let mut reader = std::io::BufReader::new(f);
        Archive::<RawEntryData>::from_data(&mut reader)
    }).await??;

    anyhow::ensure!(
        archive.body.entries.len() == archive.body.header.len(),
        "archive entries and headers length mismatch"
    );

    println!("Index: {}", archive.hash);

    #[allow(clippy::mutable_key_type)]
    let mut data_map: HashMap<Hash, Vec<u8>> = HashMap::with_capacity(archive.body.header.len());
    for (header_entry, raw_entry) in archive.body.header.into_iter().zip(archive.body.entries.into_iter()) {
        data_map.insert(header_entry.hash, raw_entry.0);
    }

    let root_tree_hash = archive.index.tree;
    let data_map = Arc::new(data_map);
    let file_count = extract_tree(&data_map, &root_tree_hash, directory).await?;

    println!("Extracted {} files to {:?}", file_count, directory);

    Ok(())
}

#[allow(clippy::ptr_arg, clippy::mutable_key_type)]
fn extract_tree<'a>(
    data_map: &'a Arc<HashMap<Hash, Vec<u8>>>,
    tree_hash: &'a Hash,
    path: &'a PathBuf,
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

        for entry in tree.contents {
            let entry_path = path.join(&entry.path);

            match entry.mode {
                Mode::Tree => {
                    create_dir(&entry_path).await
                        .context(format!("failed to create directory {:?}", entry_path))?;
                    file_count += extract_tree(data_map, &entry.hash, &entry_path).await?;
                }
                Mode::Normal | Mode::Executable => {
                    file_count += 1;
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

        Ok(file_count)
    })
}

fn archive_build_tree(
    objects: Arc<tokio::sync::Mutex<HashMap<Hash, Vec<u8>>>>,
    path: PathBuf,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Hash>> + Send>> {
    Box::pin(async move {
        anyhow::ensure!(path.is_dir(), "path must be a directory");
        let mut dir_entries = read_dir(&path).await?;

        let mut dir_paths = Vec::new();
        let mut file_paths = Vec::new();
        while let Some(entry) = dir_entries.next_entry().await? {
            let entry_path = entry.path();
            if entry.file_type().await?.is_dir() {
                dir_paths.push(entry_path);
            } else {
                file_paths.push(entry_path);
            }
        }

        let mut entries = Vec::new();

        // Process subdirectories concurrently
        let mut dir_set = JoinSet::new();
        for dir_path in dir_paths {
            let objects_clone = objects.clone();
            dir_set.spawn(async move {
                let name = dir_path.file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
                let hash = archive_build_tree(objects_clone, dir_path).await?;
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
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();

                let (data, hash) = tokio::task::spawn_blocking({
                    let path = file_path.clone();
                    move || -> anyhow::Result<(Vec<u8>, Hash)> {
                        let data = std::fs::read(&path)?;
                        let hash = compute_hash(BLOB_KEY, &data);
                        Ok((data, hash))
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

                Ok::<_, anyhow::Error>((name, hash))
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
            let (name, hash) = result??;
            entries.push(object_body::TreeEntry {
                mode: Mode::Normal,
                path: name,
                hash,
            });
        }

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

async fn archive_directory(directory: &PathBuf, file: &PathBuf, compression: Compression) -> anyhow::Result<()> {
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

    let root_hash = archive_build_tree(objects.clone(), path).await?;

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
    /// Path to the local object store directory
    #[arg(short, long, value_name = "Store")]
    store: PathBuf,

    /// Increase debug verbosity (can be repeated)
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,

    #[command(subcommand)]
    command: Commands,
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
        #[arg(long)]
        hash: Hash,
    },

    /// Push objects from the local store to a remote server
    Push {
        /// Base URL of the remote artifact server
        #[arg(long)]
        url: String,

        /// Optional index hash to push (pushes all objects if omitted)
        #[arg(long)]
        index: Option<Hash>,
    },

    /// Pull objects from a remote server into the local store
    Pull {
        /// Base URL of the remote artifact server
        #[arg(long)]
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
        #[arg(long)]
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
        #[arg(short, long)]
        compression: Compression,
    },

    /// Push objects to a remote server, uploading only what's missing (deduplication)
    PushArchive {
        /// Base URL of the remote artifact server
        #[arg(long)]
        url: String,
        /// Hash of the index to push
        #[arg(long)]
        index: Hash,
        /// Compression algorithm for the supplemental archive (default: zstd)
        #[arg(long, default_value = "zstd")]
        compression: Compression,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("Error: {e:?}");
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

    /// Load unpacked cache (2-char prefix layout) into an opendal Store.
    async fn load_cache_into_store(
        cache_dir: &std::path::Path,
        store: &Store,
    ) -> anyhow::Result<()> {
        let mut dirs = tokio::fs::read_dir(cache_dir).await?;
        while let Some(dir_entry) = dirs.next_entry().await? {
            if !dir_entry.file_type().await?.is_dir() {
                continue;
            }
            let dir_name = dir_entry.file_name().to_string_lossy().to_string();
            if dir_name.len() != 2 {
                continue;
            }
            let mut files = tokio::fs::read_dir(dir_entry.path()).await?;
            while let Some(file_entry) = files.next_entry().await? {
                if !file_entry.file_type().await?.is_file() {
                    continue;
                }
                let file_name = file_entry.file_name().to_string_lossy().to_string();
                let hex = format!("{}{}", dir_name, file_name);
                let hash = Hash::try_from(hex.as_str())?;

                let raw_data = tokio::fs::read(file_entry.path()).await?;
                let (header, body) = read_header_and_body(&raw_data)
                    .ok_or_else(|| anyhow::anyhow!("Failed to parse header from cache file"))?;

                store.put_object_bytes(&hash, header, body.to_vec()).await?;
            }
        }
        Ok(())
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

        // Unpack to cache B (file-based 2-char prefix layout)
        let cache_b = TempDir::new().unwrap();
        let cache_b_path = cache_b.path().to_path_buf();
        unpack_archive(&cache_b_path, &arx_path).await.unwrap();

        // Load cache B objects into a new opendal Store
        let store_b_dir = TempDir::new().unwrap();
        let store_b = create_store(store_b_dir.path());
        load_cache_into_store(cache_b.path(), &store_b).await.unwrap();

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
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    // Ensure store directory exists
    if tokio::fs::metadata(&cli.store).await.is_err() {
        create_dir_all(&cli.store).await.context("failed to create store directory")?;
    }

    // Create store from cache directory
    let store = Store::from_builder(opendal::services::Fs::default().root(cli.store.to_str().context("store path must be valid UTF-8")?))
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
            push_cache(&cli.store, &url, index).await?
        }
        Commands::Pull { url, index } => pull_cache(&cli.store, &url, index).await?,
        Commands::Pack { index, file, compression } => {
            pack_archive(&store, &file, &index, compression).await?
        }
        Commands::Unpack { file } => {
            unpack_archive(&cli.store, &file).await?
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
