use anyhow::Context;
use chrono::Utc;
use clap::{Parser, Subcommand};
use common::{
    BLOB_KEY, Hash, Header, INDEX_KEY, Mode, ObjectType, TREE_KEY,
    archive::{Archive, ArchiveBody, ArchiveEntryData, ArchiveHeaderEntry, Compression, HEADER, RawEntryData},
    compute_hash, object_body, read_header_and_body, read_header_from_slice, read_object_into_headers,
    store::{Store, StoreObject},
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
use opendal;

// Helper to build Index from filesystem
async fn build_index_from_path(store: &Store, path: &PathBuf) -> anyhow::Result<(object_body::Index, Hash, u128)> {
    anyhow::ensure!(path.is_dir(), "Path must be a directory");

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
    let reader = futures::io::Cursor::new(index_data);
    let store_obj = StoreObject::new_with_header(header, reader);
    store.put_object(&index_hash, store_obj).await?;

    Ok((index, index_hash, total_size))
}

fn build_tree_from_dir(
    store: Store,
    path: PathBuf,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<(Hash, u128)>> + Send>> {
    Box::pin(async move {
        anyhow::ensure!(path.is_dir(), "Path must be a directory");
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
                    let reader = futures::io::Cursor::new(data);
                    let store_obj = StoreObject::new_with_header(header, reader);
                    store_clone.put_object(&hash, store_obj).await?;
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
            let reader = futures::io::Cursor::new(tree_data);
            let store_obj = StoreObject::new_with_header(header, reader);
            store.put_object(&hash, store_obj).await?;
        }

        Ok((hash, total_size))
    })
}

// Helper to read Tree from Store
async fn read_tree_from_store(store: &Store, hash: &Hash) -> anyhow::Result<object_body::Tree> {
    let mut store_obj = store.get_object(hash).await?;
    anyhow::ensure!(store_obj.header.object_type == ObjectType::Tree, "Expected Tree object");

    let mut data = Vec::new();
    store_obj.read_to_end(&mut data).await?;

    object_body::Object::from_data(&data)
}

async fn commit_directory(store: &Store, path: &PathBuf) -> anyhow::Result<()> {
    let path_meta = tokio::fs::metadata(path).await?;
    anyhow::ensure!(path_meta.is_dir(), "Path must be a directory");

    let path = tokio::fs::canonicalize(path).await.context(format!("unable to canonicalize {path:?}"))?;

    let (_index, index_hash, total_size) = build_index_from_path(store, &path).await?;

    println!("Finished generating Index for {} bytes of data", total_size);
    println!("{}", index_hash);
    Ok(())
}

async fn restore_directory(store: &Store, path: &PathBuf, index_hash: Hash, validate: bool) -> anyhow::Result<()> {
    if tokio::fs::metadata(path).await.is_err() {
        create_dir_all(path).await.context("Failed to create directory")?;
    }

    let path_meta = tokio::fs::metadata(path).await?;
    anyhow::ensure!(path_meta.is_dir(), "Path provided must be a valid directory");

    let mut entries = read_dir(path).await?;
    anyhow::ensure!(entries.next_entry().await?.is_none(), "Path provided must be an empty directory");

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
                    anyhow::ensure!(computed_hash == expected_hash, "Hash mismatch for {}", entry_name);
                }
                ObjectType::Index => anyhow::bail!("Invalid ObjectType in tree"),
            }
        }
        Ok(())
    })
}

fn write_tree_to_path<'a>(
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
                    create_dir(&entry_path).await.context("Failed to create directory")?;
                    let subtree = read_tree_from_store(store, &entry.hash).await?;
                    write_tree_to_path(store, &subtree, &entry_path).await?;
                }
                ObjectType::Blob => {
                    let file = File::create(&entry_path).await.context("Failed to create file")?;
                    let mut writer = BufWriter::new(file);
                    let mut store_obj = store.get_object(&entry.hash).await?;

                    let mut data = [0u8; 65536];
                    loop {
                        let num = store_obj.read(&mut data).await?;
                        if num == 0 {
                            break;
                        }
                        writer.write_all(&data[..num]).await?;
                    }
                    writer.flush().await?;
                }
                ObjectType::Index => anyhow::bail!("Invalid ObjectType in tree"),
            }
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
            let hash = Hash::try_from(name).context("Invalid hash")?;

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
                .context(format!("Unable to download object with hash {tree_hash}"))?;
            anyhow::ensure!(header.object_type == ObjectType::Tree, "Expected Tree object");
        }

        let mut file = File::open(tree_path).await.context("Tree file should exist")?;
        let mut index_data = Vec::new();
        let _ = file
            .read_to_end(&mut index_data)
            .await
            .context("File should be readable")?;

        let (_, data) =
            read_header_and_body(&index_data).context("Tree should be in the correct format")?;

        let index_body: object_body::Tree = object_body::Object::from_data(data)?;

        for entry in index_body.contents {
            let obj_path = entry.hash.get_path(cache);
            let header = download_object(&entry.hash, &obj_path, url).await
                .context(format!("Unable to download object with hash {}", entry.hash))?;

            anyhow::ensure!(header.object_type != ObjectType::Index, "Unexpected Index object in tree");

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
        .context(format!("Unable to download object with hash {hash}"))?;

    anyhow::ensure!(header.object_type == ObjectType::Index, "Expected Index object");

    let mut file = File::open(index_path).await.context("Index file should exist")?;
    let mut index_data = Vec::new();
    let _ = file
        .read_to_end(&mut index_data)
        .await
        .context("File should be readable")?;

    let (_, data) =
        read_header_and_body(&index_data).context("Index should be in the correct format")?;

    let index_body: object_body::Index = object_body::Object::from_data(data)?;

    pull_tree(cache, url, &index_body.tree).await?;
    Ok(())
}

async fn upload_object(hash: &Hash, file: &PathBuf, url: &String) -> anyhow::Result<()> {
    let file_data = tokio::task::spawn_blocking({
        let file = file.clone();
        move || std::fs::read(&file)
    }).await?.context("File should exist")?;

    let header = read_header_from_slice(
        &file_data[..file_data.iter().position(|&b| b == 0).unwrap_or(file_data.len())]
    ).context("File should be a valid object")?;

    let url = format!("{url}/object/{hash}");

    println!("Sending put request to {url}");

    // Get the body after the header (skip past the null terminator)
    let header_end = file_data.iter().position(|&b| b == 0).unwrap_or(0) + 1;
    let body = &file_data[header_end..];

    ureq::put(&url)
        .header("Object-Type", header.object_type.to_str())
        .header("Object-Size", &header.size.to_string())
        .send(body)
        .context("Failed to upload object")?;

    Ok(())
}

async fn download_object(hash: &Hash, file: &PathBuf, url: &String) -> anyhow::Result<Header> {
    let url = format!("{url}/object/{hash}");

    let dir = file.parent().context("Path should not be at root")?;
    create_dir_all(dir).await.context("Failed to create directory")?;

    if tokio::fs::metadata(file).await.is_ok() {
        let file_handle = File::open(file).await.context("File should exist")?;
        let mut reader = BufReader::new(file_handle);

        let mut buffer = Vec::new();
        reader
            .read_until(0, &mut buffer)
            .await
            .context("Header should exist within file")?;

        // subtract one to get rid of the null byte
        return read_header_from_slice(&buffer[..buffer.len() - 1])
            .context("Invalid header in file");
    }

    println!("Sending get request to {url}");

    let mut response = ureq::get(&url).call()
        .context("Failed to send GET request")?;

    let file_handle = File::create(file).await.context("Failed to create file")?;
    let mut writer = BufWriter::new(file_handle);

    let response_headers = response.headers();
    let object_type: ObjectType = ObjectType::from_str(
        response_headers
            .get("Object-Type")
            .context("Object-Type header missing from response")?
            .to_str()
            .context("Object-Type header not valid ASCII")?,
    )
    .context("Invalid ObjectType in header")?;
    let object_size: u64 = response_headers
        .get("Object-Size")
        .context("Object-Size header missing from response")?
        .to_str()
        .context("Object-Size header not valid ASCII")?
        .parse()
        .context("Object-Size header not a valid number")?;

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
    anyhow::ensure!(tokio::fs::metadata(path).await.is_err(), "Output file already exists");
    anyhow::ensure!(
        path.parent().map(|p| p.exists() && p.is_dir()) == Some(true),
        "Parent directory must exist and be a directory"
    );

    let index: object_body::Index = {
        let mut store_obj = store.get_object(index_hash).await.context("Index object not found in store")?;
        anyhow::ensure!(store_obj.header.object_type == ObjectType::Index, "Expected Index object type");
        let mut body = Vec::new();
        store_obj.read_to_end(&mut body).await?;
        object_body::Object::from_data(&body)?
    };

    let mut headers: HashMap<Hash, (Header, Vec<u8>)> = HashMap::new();
    read_object_into_headers(store, &mut headers, &index.tree).await?;

    //TODO: Surely there is an algorithm to more efficiently lay out this data
    let mut i = 0;
    let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::new();

    for (hash, (_header, data)) in &headers {
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
            entries: headers
                .into_iter()
                .map(|(_hash, (_header, data))| RawEntryData(data))
                .collect(),
        },
    };

    let arx_file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::new(arx_file);

    archive.to_data(&mut writer)?;

    Ok(())
}

async fn unpack_archive(cache: &PathBuf, path: &PathBuf) -> anyhow::Result<()> {
    let path_meta = tokio::fs::metadata(path).await?;
    anyhow::ensure!(path_meta.is_file(), "Path must be a file");

    let file = std::fs::File::open(path)?;
    let mut file = std::io::BufReader::new(file);

    let archive = Archive::<RawEntryData>::from_data(&mut file)?;

    anyhow::ensure!(archive.body.entries.len() == archive.body.header.len(), "Archive entries and headers length mismatch");

    println!("Successfully read archive, Index {}", archive.hash);

    let index_data = object_body::Object::to_data(&archive.index);
    let computed_hash = compute_hash(INDEX_KEY, &index_data);
    anyhow::ensure!(computed_hash == archive.hash, "Index hash mismatch");

    let index_file_path = archive.hash.get_path(cache);
    let _ = create_dir_all(index_file_path.parent().context("Path should not be at root")?).await;

    {
        let index_header = Header::new(ObjectType::Index, index_data.len() as u64);
        let mut index_file = File::create(index_file_path).await?;
        index_file.write_all(index_header.to_string().as_bytes()).await?;
        index_file.write_all(&index_data).await?;
    }

    for (header, entry) in archive.body.header.into_iter().zip(archive.body.entries.into_iter()) {
        let object_path = header.hash.get_path(cache);
        let _ = create_dir_all(object_path.parent().context("Path should not be at root")?).await;

        let mut file = File::create(object_path).await?;
        let data = entry.turn_into_vec();
        file.write_all(&data).await?;
    }

    Ok(())
}


#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "Store")]
    store: PathBuf,

    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Commit {
        #[arg(short, long)]
        directory: PathBuf,
    },

    Restore {
        #[arg(short, long)]
        directory: PathBuf,
        #[arg(short, long)]
        index: Hash,
        #[arg(long)]
        validate: bool,
    },

    Cat {
        #[arg(long)]
        hash: Hash,
    },

    Push {
        #[arg(long)]
        url: String,

        #[arg(long)]
        index: Option<Hash>,
    },

    Pull {
        #[arg(long)]
        url: String,

        #[arg(long)]
        index: Hash,
    },

    Pack {
        #[arg(long)]
        index: Hash,

        #[arg(long)]
        file: PathBuf,

        #[arg(long)]
        compression: Compression,
    },

    Unpack {
        #[arg(long)]
        file: PathBuf,
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

async fn run(cli: Cli) -> anyhow::Result<()> {
    // Ensure store directory exists
    if tokio::fs::metadata(&cli.store).await.is_err() {
        create_dir_all(&cli.store).await.context("Failed to create store directory")?;
    }

    // Create store from cache directory
    let store = Store::from_builder(opendal::services::Fs::default().root(cli.store.to_str().context("Store path must be valid UTF-8")?))
        .context("Failed to create store")?;

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
    }
    Ok(())
}
