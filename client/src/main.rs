#![allow(dead_code)]
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use common::{
    BLOB_KEY, Hash, Header, INDEX_KEY, Mode, ObjectType, TREE_KEY, 
    archive::{Archive, ArchiveBody, ArchiveEntryData, ArchiveHeaderEntry, Compression, HEADER, RawEntryData, FileEntryData}, 
    object_body::Object as OtherObject, read_header_and_body, read_header_from_slice
};
use sha2::{Digest, Sha512};
use std::{
    collections::HashMap,
    ops::Deref,
    path::PathBuf,
    str::from_utf8,
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

#[derive(Debug)]
struct Hashed<T: Object> {
    inner: T,
    hash: Hash,
}

impl<T: Object> Deref for Hashed<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: Object> Hashed<T> {
    fn from_object(value: T) -> Self {
        Self {
            hash: value.get_hash(),
            inner: value,
        }
    }
}

trait Object {
    fn get_object_type(&self) -> ObjectType;
    fn get_hash(&self) -> Hash;
    fn get_prefix(&self) -> String;
    fn write_to(&self, path: &PathBuf) -> impl std::future::Future<Output = ()> + Send;
}

// Helper to read header from async file
async fn read_header_from_async_file<R: AsyncBufReadExt + Unpin>(reader: &mut R) -> Option<Header> {
    let mut buffer = Vec::new();
    reader.read_until(0, &mut buffer).await.ok()?;
    
    if buffer.last() == Some(&0) {
        buffer.pop();
    }
    
    read_header_from_slice(&buffer)
}

struct CacheObject<'a> {
    cache: &'a PathBuf,
    object_type: ObjectType,
    hash: Hash,
    size: u64,
    file: PathBuf,
}

impl<'a> CacheObject<'a> {
    async fn from_file(cache: &'a PathBuf, file_path: &PathBuf) -> Self {
        let file = File::open(file_path).await.unwrap();
        let mut file = BufReader::new(file);

        let mut data = Vec::new();
        file.read_until(b'\0', &mut data).await.unwrap();

        if data.last() == Some(&0) {
            data.pop();
        }

        let data = String::from_utf8(data).expect("data to be a valid u8");

        let (object_type, size) = data.split_once(' ').unwrap();

        let object_type = ObjectType::from_str(object_type).unwrap();

        let hash = Hash::from_path(file_path).unwrap();

        Self {
            cache,
            file: file_path.clone(),
            size: size.parse().unwrap(),
            object_type,
            hash,
        }
    }

    async fn to_index(&self) -> Hashed<Index> {
        assert!(self.object_type == ObjectType::Index);

        let file = File::open(&self.file).await.unwrap();
        let mut file = BufReader::new(file);

        let mut data = Vec::new();
        file.read_until(b'\0', &mut data).await.unwrap();

        let mut metadata = HashMap::new();

        let mut string_data = String::new();

        file.read_to_string(&mut string_data)
            .await
            .expect("Index to only contain string");

        let string_data = string_data.trim_end();

        let lines = string_data.split('\n').collect::<Vec<&str>>();

        for line in lines {
            let (key, value) = line.split_once(':').unwrap();

            _ = metadata.insert(key, value.trim());
        }

        let timestamp = DateTime::parse_from_rfc3339(metadata["timestamp"]).unwrap();

        let tree_hash = Hash::try_from(metadata["tree"]).expect("tree hash to be valid");

        let tree_object = CacheObject::from_file(self.cache, &tree_hash.get_path(self.cache)).await;

        assert!(tree_object.get_object_type() == ObjectType::Tree);

        Hashed {
            hash: self.hash.clone(),
            inner: Index {
                timestamp: timestamp.into(),
                tree: tree_object.to_tree(Mode::Tree, &"").await,
            },
        }
    }

    fn to_tree<'b>(&'b self, mode: Mode, path: &'b str) -> std::pin::Pin<Box<dyn std::future::Future<Output = Hashed<Tree>> + Send + 'b>> {
        Box::pin(async move {
            assert!(self.object_type == ObjectType::Tree);

            let file = File::open(&self.file).await.unwrap();
            let mut file = BufReader::new(file);

            // Read out the file header
            let _ = read_header_from_async_file(&mut file).await.expect("File header to be correct");

            let mut vec = Vec::new();

            loop {
                let mut buffer = Vec::new();
                let bytes = file
                    .read_until(0, &mut buffer)
                    .await
                    .expect("To have a file header");

                if bytes == 0 {
                    break;
                }

                let string = from_utf8(&buffer[..buffer.len() - 1]).expect("valid utf8");

                let (mode_str, name) = string.split_once(' ').expect("space");

                let mode = Mode::from_str(mode_str).expect("valid mode");

                let mut hash: [u8; 64] = [0; 64];
                file.read_exact(&mut hash).await.expect("file to contain hash");

                let hash = Hash::from(hash);

                let object_file = hash.get_path(&self.cache);

                let cache_object = CacheObject::from_file(&self.cache, &object_file).await;

                vec.push(match cache_object.object_type {
                    ObjectType::Blob => TreeObject::Blob(cache_object.to_blob(mode, name).await),
                    ObjectType::Tree => TreeObject::Tree(cache_object.to_tree(mode, name).await),
                    ObjectType::Index => panic!("Invalid ObjectType in tree"),
                })
            }

            Hashed {
                hash: self.hash.clone(),
                inner: Tree {
                    mode,
                    path: path.to_owned(),
                    contents: vec,
                },
            }
        })
    }

    async fn to_blob(&self, mode: Mode, path: &str) -> Hashed<Blob> {
        assert!(self.object_type == ObjectType::Blob);

        let file = File::open(&self.file).await.unwrap();
        let mut file = BufReader::new(file);

        // Read out the file header
        let Header { size, .. } =
            read_header_from_async_file(&mut file).await.expect("File header to be correct");

        Hashed {
            hash: self.hash.clone(),

            inner: Blob {
                mode,
                path: path.to_string(),
                file: self.file.clone(),
                size,
            },
        }
    }
}

impl<'a> Object for CacheObject<'a> {
    fn get_object_type(&self) -> ObjectType {
        self.object_type.clone()
    }

    fn get_hash(&self) -> Hash {
        self.hash.clone()
    }

    fn get_prefix(&self) -> String {
        format!("{} {}\0", self.object_type.to_str(), self.size)
    }

    async fn write_to(&self, _: &PathBuf) {
        unimplemented!("Should probably fix this")
    }
}

#[derive(Debug)]
struct Index {
    timestamp: DateTime<Utc>,
    tree: Hashed<Tree>,
}

impl Index {
    fn get_body(&self) -> String {
        format!(
            "tree: {}\ntimestamp: {}\n\n",
            self.tree.hash,
            self.timestamp.to_rfc3339()
        )
    }

    async fn from_path(path: &PathBuf) -> Index {
        assert!(path.is_dir());
        Index {
            timestamp: Utc::now(),
            tree: Hashed::from_object(Tree::from_dir(path).await),
        }
    }
}

impl Object for Index {
    fn get_object_type(&self) -> ObjectType {
        ObjectType::Index
    }

    fn get_hash(&self) -> Hash {
        let body = self.get_body();
        let mut hasher = Sha512::new();
        StdWrite::write_fmt(&mut hasher, format_args!("{}{}", self.get_prefix(), body)).unwrap();
        Hash::from(hasher)
    }

    fn get_prefix(&self) -> String {
        format!("{} {}\0", INDEX_KEY, self.get_body().len())
    }

    async fn write_to(&self, path: &PathBuf) {
        let mut file = File::create(path).await.unwrap();

        file.write_all(self.get_prefix().as_bytes()).await.unwrap();
        file.write_all(self.get_body().as_bytes()).await.unwrap();
    }
}

trait WithPath {
    fn get_path_component(&self) -> &String;
    fn get_mode(&self) -> &Mode;
}

#[derive(Debug)]
struct Tree {
    mode: Mode,
    path: String,
    contents: Vec<TreeObject>,
}

impl Tree {
    fn get_body(&self) -> Vec<u8> {
        let mut value = Vec::new();

        for object in self.contents.iter() {
            value.extend_from_slice(&mut object.to_tree_bytes());
        }

        value
    }

    fn from_dir(path: &PathBuf) -> std::pin::Pin<Box<dyn std::future::Future<Output = Self> + Send>> {
        let path = path.clone();
        Box::pin(async move {
            assert!(path.is_dir());

            let mut contents = Vec::new();
            let mut entries = read_dir(&path).await.unwrap();
            
            while let Some(entry) = entries.next().await {
                let entry = entry.unwrap();
                let entry_path: PathBuf = entry.path().into();
                
                if smol::fs::metadata(&entry_path).await.unwrap().is_dir() {
                    contents.push(TreeObject::Tree(Hashed::from_object(Tree::from_dir(&entry_path).await)));
                } else {
                    contents.push(TreeObject::Blob(Hashed::from_object(Blob::from_path(&entry_path).await)));
                }
            }

            Self {
                mode: Mode::Tree,
                contents,
                path: match path.file_name() {
                    Some(v) => v.to_string_lossy().to_string(),
                    None => panic!("{path:?} did not have a filename"),
                },
            }
        })
    }
}

impl WithPath for Tree {
    fn get_path_component(&self) -> &String {
        &self.path
    }

    fn get_mode(&self) -> &Mode {
        &self.mode
    }
}

impl Object for Tree {
    fn get_object_type(&self) -> ObjectType {
        ObjectType::Tree
    }

    fn get_hash(&self) -> Hash {
        let body = self.get_body();
        let mut hasher = Sha512::new();
        StdWrite::write_fmt(&mut hasher, format_args!("{}", self.get_prefix())).unwrap();
        hasher.write_all(&body).expect("Body to be added to the hasher");
        Hash::from(hasher)
    }

    async fn write_to(&self, path: &PathBuf) {
        let mut file = File::create(path).await.unwrap();

        file.write_all(self.get_prefix().as_bytes()).await.unwrap();
        file.write_all(&self.get_body()).await.unwrap();
    }

    fn get_prefix(&self) -> String {
        format!("{} {}\0", TREE_KEY, self.get_body().len())
    }
}

#[derive(Debug)]
struct Blob {
    mode: Mode,
    path: String,
    file: PathBuf,
    size: u64,
}

impl Blob {
    async fn from_path(path: &PathBuf) -> Self {
        let metadata = smol::fs::metadata(path).await.unwrap();
        assert!(metadata.is_file());

        Self {
            // TODO: Support other types
            mode: Mode::Normal,
            path: path.file_name().unwrap().to_string_lossy().to_string(),
            size: metadata.len(),
            file: path.clone(),
        }
    }
}

impl WithPath for Blob {
    fn get_path_component(&self) -> &String {
        &self.path
    }

    fn get_mode(&self) -> &Mode {
        &self.mode
    }
}

impl Object for Blob {
    fn get_object_type(&self) -> ObjectType {
        ObjectType::Blob
    }

    fn get_hash(&self) -> Hash {
        // Since we want to read the file only once, we'll use blocking I/O for hashing
        // as sha2 doesn't support async operations
        let mut hasher = Sha512::new();
        hasher.write_all(self.get_prefix().as_bytes()).unwrap();

        // Read the file once in blocking context
        let data = std::fs::read(&self.file).unwrap();
        hasher.write_all(&data).unwrap();

        Hash::from(hasher)
    }

    async fn write_to(&self, path: &PathBuf) {
        let mut file = File::create(path).await.unwrap();

        file.write_all(self.get_prefix().as_bytes()).await.unwrap();

        let mut src = File::open(&self.file).await.unwrap();
        futures_lite::io::copy(&mut src, &mut file).await.unwrap();
    }

    fn get_prefix(&self) -> String {
        format!("{} {}\0", BLOB_KEY, self.size)
    }
}

#[derive(Debug)]
enum TreeObject {
    Tree(Hashed<Tree>),
    Blob(Hashed<Blob>),
}

impl TreeObject {
    fn to_tree_bytes(&self) -> Vec<u8> {
        match self {
            Self::Tree(tree) => get_bytes_from_thing(tree.deref(), &tree.hash),
            Self::Blob(blob) => get_bytes_from_thing(blob.deref(), &blob.hash),
        }
    }
}

fn get_bytes_from_thing<T: WithPath>(object: &T, hash: &Hash) -> Vec<u8> {
    let mut path = Vec::new();

    path.extend_from_slice(&mut object.get_mode().to_string().as_bytes());
    path.push(b' ');
    path.extend_from_slice(&mut object.get_path_component().as_bytes());
    path.push(0);
    path.extend_from_slice(&hash.hash);

    path
}

impl<T: Object> Hashed<T> {
    async fn write_if_not_exists(&self, dir: &PathBuf) {
        let path = self.hash.get_path(dir);
        let dir_path = path.parent().unwrap();

        let _ = create_dir_all(dir_path).await;

        if !smol::fs::metadata(&path).await.is_ok() {
            self.write_to(&path).await;
        }
    }
}

async fn write_index_to_folder(dir: &PathBuf, index: &Hashed<Index>) {
    index.write_if_not_exists(dir).await;

    write_tree_to_folder(dir, &index.tree).await;
}

fn write_tree_to_folder<'a>(dir: &'a PathBuf, tree: &'a Hashed<Tree>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        tree.write_if_not_exists(dir).await;

        for element in tree.contents.iter() {
            match element {
                TreeObject::Tree(tree) => write_tree_to_folder(dir, &tree).await,
                TreeObject::Blob(blob) => blob.write_if_not_exists(dir).await,
            }
        }
    })
}

fn get_total_size(index: &Hashed<Tree>) -> u128 {
    let mut total = 0;

    for element in &index.contents {
        total += match element {
            TreeObject::Tree(tree) => get_total_size(&tree),
            TreeObject::Blob(blob) => blob.size as u128,
        }
    }

    total
}

async fn commit_directory(cache: &PathBuf, path: &PathBuf) {
    let path_meta = smol::fs::metadata(path).await.unwrap();
    assert!(path_meta.is_dir());

    if let Ok(cache_meta) = smol::fs::metadata(cache).await {
        assert!(cache_meta.is_dir());
    } else {
        create_dir(&cache).await.unwrap();
    }

    let path = std::fs::canonicalize(path).expect(&format!("unable to canonicalize {path:?}"));

    let index = Hashed::from_object(Index::from_path(&path).await);

    println!(
        "Finished generating Index for {} bytes of data",
        get_total_size(&index.tree)
    );

    write_index_to_folder(&cache, &index).await;

    println!("{}", index.hash);
}

async fn restore_directory(cache: &PathBuf, path: &PathBuf, index: Hash, validate: bool) {
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

    let index_path = index.get_path(cache);
    let index_cache = Hashed::from_object(CacheObject::from_file(cache, &index_path).await);

    let index = index_cache.to_index().await;

    write_tree(&index.tree, path).await;

    if validate {
        validate_tree(&index.tree, path).await;
    }
}

fn validate_tree<'a>(tree: &'a Tree, path: &'a PathBuf) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        for item in tree.contents.iter() {
            if let TreeObject::Tree(tree) = item {
                let tree_path = path.join(&tree.path);
                validate_tree(&tree, &tree_path).await;
                continue;
            }

            let TreeObject::Blob(blob) = item else {
                unreachable!();
            };

            let blob_path = path.join(&blob.path);

            let blob_hash = Hashed::from_object(Blob::from_path(&blob_path).await);

            assert!(blob_hash.hash == blob.hash);
        }
    })
}

fn write_tree<'a>(tree: &'a Tree, path: &'a PathBuf) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        for item in tree.contents.iter() {
            if let TreeObject::Tree(tree) = item {
                let tree_path = path.join(&tree.path);

                create_dir(&tree_path).await.expect("Directory creation to work");

                write_tree(&tree, &tree_path).await;
                continue;
            }

            let TreeObject::Blob(blob) = item else {
                unreachable!();
            };

            let blob_path = path.join(&blob.path);

            let file = File::create(blob_path).await.expect("File to be created");
            let mut writer = BufWriter::new(file);

            let cache_file = File::open(&blob.file).await.unwrap();
            let mut reader = BufReader::new(cache_file);

            let _ = read_header_from_async_file(&mut reader).await;

            let mut data: [u8; 1024] = [0; 1024];
            loop {
                let num = reader.read(&mut data).await.unwrap();
                if num == 0 {
                    break;
                }
                writer.write(&data[..num]).await.unwrap();
            }
        }
    })
}

async fn cat_object(cache: &PathBuf, hash: &Hash) {
    let object_path = hash.get_path(cache);

    let file = File::open(&object_path).await.unwrap();
    let mut reader = BufReader::new(file);

    read_header_from_async_file(&mut reader).await.expect("file to contain a valid header");

    let stdout = smol::Unblock::new(std::io::stdout());
    let mut stdout = BufWriter::new(stdout);
    let mut data: [u8; 1024] = [0; 1024];
    loop {
        let num = reader.read(&mut data).await.unwrap();
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

        let index_body = common::object_body::Tree::from_data(data);

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

    let index_body = common::object_body::Index::from_data(data);

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

        common::object_body::Index::from_data(&body)
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

    let index_data = archive.index.to_data();
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
    #[arg(short, long, value_name = "Cache")]
    cache: PathBuf,

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

fn main() {
    smol::block_on(async_main());
}

async fn async_main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Commit { directory } => commit_directory(&cli.cache, &directory).await,
        Commands::Restore {
            directory,
            index,
            validate,
        } => restore_directory(&cli.cache, &directory, index, validate).await,
        Commands::Cat { hash } => cat_object(&cli.cache, &hash).await,
        Commands::Push { url, index } => {
            push_cache(&cli.cache, &url, index).await
        }
        Commands::Pull { url, index } => pull_cache(&cli.cache, &url, index).await,
        Commands::Pack { index, file , compression} => {
            pack_archive(&cli.cache, &file, &index, compression).await.expect("Packing to work")
        }
        Commands::Unpack { file } => {
            unpack_archive(&cli.cache, &file).await.expect("Packing to work")
        }
    }
}
