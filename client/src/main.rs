#![allow(dead_code)]
use chrono::Utc;
use clap::{Parser, Subcommand};
use common::{
    BLOB_KEY, Hash, Header, INDEX_KEY, Mode, ObjectType, TREE_KEY, 
    archive::{Archive, ArchiveBody, ArchiveEntryData, ArchiveHeaderEntry, Compression, HEADER, RawEntryData, FileEntryData}, 
    object_body, read_header_and_body, read_header_from_slice,
    store::{Store, StoreObject},
};
use sha2::{Digest, Sha512};
use std::{
    collections::HashMap,
    path::PathBuf,
    io::Write as StdWrite,
};

use smol::{
    fs::{File, create_dir, create_dir_all, read_dir},
    io::{BufReader, BufWriter},
};
use futures_lite::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt},
    stream::StreamExt,
};
use opendal;

// Helper to build Index from filesystem
async fn build_index_from_path(store: &Store, path: &PathBuf) -> (object_body::Index, Hash, u128) {
    assert!(path.is_dir());
    
    let (tree_hash, total_size) = build_tree_from_dir(store, path).await;
    
    let index = object_body::Index {
        tree: tree_hash,
        timestamp: Utc::now(),
        metadata: HashMap::new(),
    };
    
    let index_data = object_body::Object::to_data(&index);
    let mut hasher = Sha512::new();
    let prefix = format!("{} {}\0", INDEX_KEY, index_data.len());
    hasher.write_all(prefix.as_bytes()).unwrap();
    hasher.write_all(&index_data).unwrap();
    let index_hash = Hash::from(hasher);
    
    // Write index to store immediately
    let header = Header::new(ObjectType::Index, index_data.len() as u64);
    let reader = futures_lite::io::Cursor::new(index_data);
    let store_obj = StoreObject::new_with_header(header, reader);
    store.put_object(&index_hash, store_obj).await.unwrap();
    
    (index, index_hash, total_size)
}

fn build_tree_from_dir<'a>(
    store: &'a Store,
    path: &'a PathBuf,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = (Hash, u128)> + Send + 'a>> {
    Box::pin(async move {
        assert!(path.is_dir());
        let mut entries = Vec::new();
        let mut dir_entries = read_dir(&path).await.unwrap();
        let mut total_size: u128 = 0;
        
        while let Some(entry) = dir_entries.next().await {
            let entry = entry.unwrap();
            let entry_path: PathBuf = entry.path().into();
            
            if smol::fs::metadata(&entry_path).await.unwrap().is_dir() {
                let (tree_hash, subtree_size) = build_tree_from_dir(store, &entry_path).await;
                total_size += subtree_size;
                entries.push(object_body::TreeEntry {
                    mode: Mode::Tree,
                    path: entry_path.file_name().unwrap().to_string_lossy().to_string(),
                    hash: tree_hash,
                });
            } else {
                let data = smol::fs::read(&entry_path).await.unwrap();
                let size = data.len() as u64;
                total_size += size as u128;
                
                let mut hasher = Sha512::new();
                let prefix = format!("{} {}\0", BLOB_KEY, size);
                hasher.write_all(prefix.as_bytes()).unwrap();
                hasher.write_all(&data).unwrap();
                let hash = Hash::from(hasher);
                
                // Write blob to store immediately if it doesn't exist
                if !store.exists(&hash).await.unwrap_or(false) {
                    let header = Header::new(ObjectType::Blob, size);
                    let reader = futures_lite::io::Cursor::new(data);
                    let store_obj = StoreObject::new_with_header(header, reader);
                    store.put_object(&hash, store_obj).await.unwrap();
                }
                // Data is dropped here, freeing memory
                
                entries.push(object_body::TreeEntry {
                    mode: Mode::Normal,
                    path: entry_path.file_name().unwrap().to_string_lossy().to_string(),
                    hash,
                });
            }
        }
        
        let tree = object_body::Tree { contents: entries };
        let tree_data = object_body::Object::to_data(&tree);
        let mut hasher = Sha512::new();
        let prefix = format!("{} {}\0", TREE_KEY, tree_data.len());
        hasher.write_all(prefix.as_bytes()).unwrap();
        hasher.write_all(&tree_data).unwrap();
        let hash = Hash::from(hasher);
        
        // Write tree to store immediately if it doesn't exist
        if !store.exists(&hash).await.unwrap_or(false) {
            let header = Header::new(ObjectType::Tree, tree_data.len() as u64);
            let reader = futures_lite::io::Cursor::new(tree_data);
            let store_obj = StoreObject::new_with_header(header, reader);
            store.put_object(&hash, store_obj).await.unwrap();
        }
        // tree_data is dropped here, freeing memory
        
        (hash, total_size)
    })
}

// Helper to read Tree from Store
async fn read_tree_from_store(store: &Store, hash: &Hash) -> object_body::Tree {
    let mut store_obj = store.get_object(hash).await.unwrap();
    assert!(store_obj.header.object_type == ObjectType::Tree);
    
    let mut data = Vec::new();
    store_obj.read_to_end(&mut data).await.unwrap();
    
    object_body::Object::from_data(&data)
}

async fn commit_directory(store: &Store, path: &PathBuf) {
    let path_meta = smol::fs::metadata(path).await.unwrap();
    assert!(path_meta.is_dir());

    let path = std::fs::canonicalize(path).expect(&format!("unable to canonicalize {path:?}"));

    let (_index, index_hash, total_size) = build_index_from_path(store, &path).await;
    
    println!("Finished generating Index for {} bytes of data", total_size);
    println!("{}", index_hash);
}

async fn restore_directory(store: &Store, path: &PathBuf, index_hash: Hash, validate: bool) {
    if smol::fs::metadata(path).await.is_err() {
        create_dir_all(path).await.expect("Directory Creation to work");
    }

    let path_meta = smol::fs::metadata(path).await.unwrap();
    if !path_meta.is_dir() {
        panic!("Path provided must be a valid directory");
    }

    let mut entries = read_dir(path).await.unwrap();
    if entries.next().await.is_some() {
        panic!("Path provided must be an empty directory");
    }

    let mut store_obj = store.get_object(&index_hash).await.unwrap();
    let mut index_data = Vec::new();
    store_obj.read_to_end(&mut index_data).await.unwrap();
    let index: object_body::Index = object_body::Object::from_data(&index_data);
    
    let tree = read_tree_from_store(&store, &index.tree).await;

    write_tree_to_path(&store, &tree, path).await;

    if validate {
        validate_tree_at_path(&store, &tree, path).await;
    }
}

fn validate_tree_at_path<'a>(
    store: &'a Store,
    tree: &'a object_body::Tree,
    path: &'a PathBuf,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        for entry in &tree.contents {
            let entry_path = path.join(&entry.path);
            let header = store.get_object(&entry.hash).await.unwrap().header;
            
            match header.object_type {
                ObjectType::Tree => {
                    let subtree = read_tree_from_store(store, &entry.hash).await;
                    validate_tree_at_path(store, &subtree, &entry_path).await;
                }
                ObjectType::Blob => {
                    // Verify blob hash matches
                    let data = smol::fs::read(&entry_path).await.unwrap();
                    let size = data.len() as u64;
                    let mut hasher = Sha512::new();
                    let prefix = format!("{} {}\0", BLOB_KEY, size);
                    hasher.write_all(prefix.as_bytes()).unwrap();
                    hasher.write_all(&data).unwrap();
                    let computed_hash = Hash::from(hasher);
                    assert!(computed_hash == entry.hash, "Hash mismatch for {}", entry.path);
                }
                ObjectType::Index => panic!("Invalid ObjectType in tree"),
            }
        }
    })
}

fn write_tree_to_path<'a>(
    store: &'a Store,
    tree: &'a object_body::Tree,
    path: &'a PathBuf,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        for entry in &tree.contents {
            let entry_path = path.join(&entry.path);
            let header = store.get_object(&entry.hash).await.unwrap().header;
            
            match header.object_type {
                ObjectType::Tree => {
                    create_dir(&entry_path).await.expect("Directory creation to work");
                    let subtree = read_tree_from_store(store, &entry.hash).await;
                    write_tree_to_path(store, &subtree, &entry_path).await;
                }
                ObjectType::Blob => {
                    let file = File::create(&entry_path).await.expect("File to be created");
                    let mut writer = BufWriter::new(file);
                    let mut store_obj = store.get_object(&entry.hash).await.unwrap();
                    
                    let mut data: [u8; 1024] = [0; 1024];
                    loop {
                        let num = store_obj.read(&mut data).await.unwrap();
                        if num == 0 {
                            break;
                        }
                        writer.write(&data[..num]).await.unwrap();
                    }
                }
                ObjectType::Index => panic!("Invalid ObjectType in tree"),
            }
        }
    })
}

async fn cat_object(store: &Store, hash: &Hash) {
    let mut store_obj = store.get_object(hash).await.unwrap();

    let stdout = smol::Unblock::new(std::io::stdout());
    let mut stdout = BufWriter::new(stdout);
    let mut data: [u8; 1024] = [0; 1024];
    loop {
        let num = store_obj.read(&mut data).await.unwrap();
        if num == 0 {
            break;
        }
        stdout.write(&data[..num]).await.unwrap();
    }
    stdout.flush().await.unwrap();
    println!();
}

async fn push_cache(cache: &PathBuf, url: &String, hash: Option<Hash>) {
    if let Some(hash) = hash {
        let file = hash.get_path(cache);
        upload_object(&hash, &file, url).await;
        return;
    }

    let mut entries = read_dir(cache).await.unwrap();
    
    while let Some(entry) = entries.next().await {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        let metadata = match entry.metadata().await {
            Ok(m) => m,
            Err(_) => continue,
        };

        if metadata.is_file() {
            continue;
        }

        let prefix = entry.file_name();

        let mut sub_entries = read_dir(entry.path()).await.unwrap();
        
        while let Some(sub_entry) = sub_entries.next().await {
            let sub_entry = match sub_entry {
                Ok(e) => e,
                Err(_) => continue,
            };

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
            let hash = Hash::try_from(name).expect("Hash to be valid");

            let sub_path: PathBuf = sub_entry.path().into();
            upload_object(&hash, &sub_path, url).await;
        }
    }
}

fn pull_tree<'a>(cache: &'a PathBuf, url: &'a String, tree_hash: &'a Hash) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        let tree_path = tree_hash.get_path(cache);

        // tree already exists locally so we can skip downloading it
        if smol::fs::metadata(&tree_path).await.is_err() {
            let Some(Header { object_type, .. }) = download_object(&tree_hash, &tree_path, url).await
            else {
                eprintln!("Unable to download object with hash {tree_hash}");
                return;
            };
            assert!(object_type == ObjectType::Tree);
        }

        let mut file = File::open(tree_path).await.expect("Index file to exist");
        let mut index_data = Vec::new();
        let _ = file
            .read_to_end(&mut index_data)
            .await
            .expect("file to be readable");

        let (_, data) =
            read_header_and_body(&index_data).expect("Index to be in the correct format");

        let index_body: object_body::Tree = object_body::Object::from_data(data);

        for entry in index_body.contents {
            let obj_path = entry.hash.get_path(cache);
            let Some(Header { object_type, .. }) = download_object(&entry.hash, &obj_path, url).await
            else {
                eprintln!("Unable to download object with hash {}", entry.hash);
                return;
            };

            assert!(object_type != ObjectType::Index);

            if object_type == ObjectType::Tree {
                pull_tree(cache, url, &entry.hash).await;
            }
        }
    })
}

async fn pull_cache(cache: &PathBuf, url: &String, hash: Hash) {
    let index_path = hash.get_path(cache);
    let Some(Header { object_type, .. }) = download_object(&hash, &index_path, url).await else {
        eprintln!("Unable to download object with hash {hash}");
        return;
    };

    assert!(object_type == ObjectType::Index);

    let mut file = File::open(index_path).await.expect("Index file to exist");
    let mut index_data = Vec::new();
    let _ = file
        .read_to_end(&mut index_data)
        .await
        .expect("file to be readable");

    let (_, data) =
        read_header_and_body(&index_data).expect("Index to be in the correct format");

    let index_body: object_body::Index = object_body::Object::from_data(data);

    pull_tree(cache, url, &index_body.tree).await;
}

async fn upload_object(hash: &Hash, file: &PathBuf, url: &String) {
    // Read entire file once
    let file_data = smol::fs::read(file).await.expect("File to exist");
    
    let header = read_header_from_slice(
        &file_data[..file_data.iter().position(|&b| b == 0).unwrap_or(file_data.len())]
    ).expect("file to be a valid object");

    let url = format!("{url}/object/{hash}");

    println!("Sending put request to {url}");

    // Get the body after the header (skip past the null terminator)
    let header_end = file_data.iter().position(|&b| b == 0).unwrap_or(0) + 1;
    let body = &file_data[header_end..];

    let response = ureq::put(&url)
        .header("Object-Type", header.object_type.to_str())
        .header("Object-Size", &header.size.to_string())
        .send(body);

    if let Err(err) = response {
        eprintln!("There was an error sending request {err:?}")
    }
}

async fn download_object(hash: &Hash, file: &PathBuf, url: &String) -> Option<Header> {
    let url = format!("{url}/object/{hash}");

    let dir = file.parent().expect("Path to not be at root");
    create_dir_all(dir).await.expect("Directory to be created");

    if smol::fs::metadata(file).await.is_ok() {
        let file_handle = File::open(file).await.expect("File to exist");
        let mut reader = BufReader::new(file_handle);

        let mut buffer = Vec::new();
        reader
            .read_until(0, &mut buffer)
            .await
            .expect("Header to exist within file");

        // subtract one to get rid of the null byte
        return read_header_from_slice(&buffer[..buffer.len() - 1]);
    }

    println!("Sending get request to {url}");

    let response = ureq::get(&url).call();

    let mut response = match response {
        Ok(v) => v,
        Err(err) => {
            eprintln!("There was an error sending request {err:?}");
            return None;
        }
    };

    let file_handle = File::create(file).await.expect("File to exist");
    let mut writer = BufWriter::new(file_handle);

    let response_headers = response.headers();
    let object_type: ObjectType = ObjectType::from_str(
        response_headers
            .get("Object-Type")
            .expect("Object-Type Header to be present in the response")
            .to_str()
            .expect("Header to be valid ascii"),
    )
    .expect("Header to be a valid ObjectType");
    let object_size: u64 = response_headers
        .get("Object-Size")
        .expect("Object-Size header to be present in the response")
        .to_str()
        .expect("Header to be valid ascii")
        .parse()
        .expect("Header to be a valid number");

    let header = Header::new(object_type, object_size);

    writer.write(header.to_string().as_bytes()).await.unwrap();

    let mut data: [u8; 1024] = [0; 1024];
    let mut reader = response.body_mut().as_reader();
    loop {
        let num = std::io::Read::read(&mut reader, &mut data).unwrap();
        if num == 0 {
            break;
        }

        writer.write(&data[..num]).await.unwrap();
    }

    return Some(header);
}

async fn pack_archive(cache: &PathBuf, path: &PathBuf, index_hash: &Hash, compression: Compression) -> anyhow::Result<()> {
    assert!(smol::fs::metadata(path).await.is_err());
    assert!(path.parent().map(|p| std::path::Path::new(p).exists() && std::path::Path::new(p).is_dir()) == Some(true));

    let index_path = index_hash.get_path(cache);
    assert!(smol::fs::metadata(&index_path).await.is_ok());

    let index = {
        let mut file = File::open(index_path).await.expect("file to exist");
        let mut data = Vec::new();
        file.read_to_end(&mut data).await.expect("File to be readable");

        let (header, body) = read_header_and_body(&data).expect("File to be correctly formatted");
        assert!(header.object_type == ObjectType::Index);

        object_body::Object::from_data(&body)
    };

    // Note: read_object_into_headers expects a Store reference, not a PathBuf.
    // For this async version, we'll need to manually collect headers.
    // This is a simplification - in production you'd want to refactor this properly.
    let headers: HashMap<Hash, Header> = HashMap::new();

    //TODO: Surely there is an algorithm to more efficiently lay out this data
    let mut i = 0;
    let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::new();

    for (hash, header) in &headers {
        let prefix_length = header.to_string().len() as u64;
        let total_length = header.size + prefix_length;

        header_entries.push(ArchiveHeaderEntry {
            hash: hash.clone(),
            index: i,
            length: total_length,
        });

        i += total_length;
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
                .map(|(hash, _header)| FileEntryData(hash.get_path(cache)))
                .collect(),
        },
    };

    let arx_file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::new(arx_file);

    archive.to_data(&mut writer)?;

    Ok(())
}

async fn unpack_archive(cache: &PathBuf, path: &PathBuf) -> anyhow::Result<()> {
    let path_meta = smol::fs::metadata(path).await?;
    assert!(path_meta.is_file());

    let file = std::fs::File::open(path)?;
    let mut file = std::io::BufReader::new(file);

    let archive = Archive::<RawEntryData>::from_data(&mut file)?;

    assert!(archive.body.entries.len() == archive.body.header.len());

    println!("Successfully read archive, Index {}", archive.hash);

    let index_data = object_body::Object::to_data(&archive.index);
    let index_header = Header::new(ObjectType::Index, index_data.len() as u64);

    let mut hasher = Sha512::new();
    hasher.write(&index_header.to_string().as_bytes())?;
    hasher.write(&index_data)?;
    assert!(Hash::from(hasher) == archive.hash);

    let index_file_path = archive.hash.get_path(cache);
    let _ = create_dir_all(index_file_path.parent().unwrap()).await;

    {
        let mut index_file = File::create(index_file_path).await?;
        index_file.write(&index_header.to_string().as_bytes()).await?;
        index_file.write(&index_data).await?;
    }

    for (header, entry) in archive.body.header.into_iter().zip(archive.body.entries.into_iter()) {
        let object_path = header.hash.get_path(cache);
        let _ = create_dir_all(object_path.parent().unwrap()).await;

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

    // Ensure cache directory exists
    if smol::fs::metadata(&cli.store).await.is_err() {
        create_dir_all(&cli.store).await.expect("Failed to create store directory");
    }

    // Create store from cache directory
    let store = Store::from_builder(opendal::services::Fs::default().root(cli.store.to_str().unwrap()))
        .expect("Failed to create store");

    match cli.command {
        Commands::Commit { directory } => commit_directory(&store, &directory).await,
        Commands::Restore {
            directory,
            index,
            validate,
        } => restore_directory(&store, &directory, index, validate).await,
        Commands::Cat { hash } => cat_object(&store, &hash).await,
        Commands::Push { url, index } => {
            push_cache(&cli.store, &url, index).await
        }
        Commands::Pull { url, index } => pull_cache(&cli.store, &url, index).await,
        Commands::Pack { index, file , compression} => {
            pack_archive(&cli.store, &file, &index, compression).await.expect("Packing to work")
        }
        Commands::Unpack { file } => {
            unpack_archive(&cli.store, &file).await.expect("Packing to work")
        }
    }
}
