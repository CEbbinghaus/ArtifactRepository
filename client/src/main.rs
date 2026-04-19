#![allow(dead_code)]
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use common::{
    archive::{
        Archive, ArchiveBody, ArchiveEntryData, ArchiveHeaderEntry, CompressionAlgorithm,
        CompressionLevel, FileEntryData, RawEntryData, SourceFileEntryData, HEADER,
    },
    object_body::Object as OtherObject,
    read_header_and_body, read_header_from_file, read_header_from_slice,
    read_object_into_headers_sync, Hash, Header, Mode, ObjectType, BLOB_KEY, INDEX_KEY, TREE_KEY,
};
use sha2::{Digest, Sha512};
use std::{
    collections::HashMap,
    fs::{create_dir, create_dir_all, read_dir, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    ops::Deref,
    path::{Path, PathBuf},
    str::from_utf8,
};
use ureq::SendBody;

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
    // fn from_hash(cache: &PathBuf, hash: Hash) -> Self {
    //     let (dir, file) = hash.get_parts();

    //     let file_path = cache.join(dir).join(file);

    //     assert!(file_path.exists());

    //     let reader = T::read_file_and_verify_type(&file_path);

    //     drop(reader);

    //     Self {
    //         inner: T::from_file(cache, &file_path),
    //         hash,
    //     }
    // }

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
    fn write_to(&self, path: &Path);

    // fn from_file(cache: &PathBuf, file: &PathBuf) -> Self;

    // fn read_file_and_verify_type(path: &PathBuf) -> BufReader<File> {
    //     let f = File::open(file_path).unwrap();
    //     let mut reader = BufReader::new(f);

    //     let mut data = Vec::new();
    //     reader.read_until(0, &mut data);

    //     if data.last() == Some(&0) {
    //         data.pop();
    //     }

    //     let name = String::from_utf8(data).unwrap();

    //     let (typ, size) = name.split_once(' ').unwrap();

    //     let object_type = ObjectType::from_str(typ);

    //     assert!(object_type == T::get_object_type());

    //     reader
    // }
}

struct CacheObject<'a> {
    cache: &'a PathBuf,
    object_type: ObjectType,
    hash: Hash,
    size: u64,
    file: PathBuf,
}

impl<'a> CacheObject<'a> {
    fn from_file(cache: &'a PathBuf, file_path: &PathBuf) -> Self {
        let file = File::open(file_path).unwrap();
        let mut file = BufReader::new(file);

        let mut data = Vec::new();
        file.read_until(b'\0', &mut data).unwrap();

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

    fn to_index(&self) -> Hashed<Index> {
        assert!(self.object_type == ObjectType::Index);

        let file = File::open(&self.file).unwrap();
        let mut file: BufReader<File> = BufReader::new(file);

        let mut data = Vec::new();
        file.read_until(b'\0', &mut data).unwrap();

        let mut metadata = HashMap::new();

        let mut string_data = String::new();

        file.read_to_string(&mut string_data)
            .expect("Index to only contain string");

        let string_data = string_data.trim_end();

        let lines = string_data.split('\n').collect::<Vec<&str>>();

        for line in lines {
            let (key, value) = line.split_once(':').unwrap();

            _ = metadata.insert(key, value.trim());
        }

        let timestamp = DateTime::parse_from_rfc3339(metadata["timestamp"]).unwrap();

        let tree_hash = Hash::try_from(metadata["tree"]).expect("tree hash to be valid");

        let tree_object = CacheObject::from_file(self.cache, &tree_hash.get_path(self.cache));

        assert!(tree_object.get_object_type() == ObjectType::Tree);

        Hashed {
            hash: self.hash.clone(),
            inner: Index {
                timestamp: timestamp.into(),
                tree: tree_object.to_tree(Mode::Tree, ""),
            },
        }
    }

    fn to_tree(&self, mode: Mode, path: &str) -> Hashed<Tree> {
        assert!(self.object_type == ObjectType::Tree);

        // println!("Reading tree {}", self.hash);

        let file = File::open(&self.file).unwrap();
        let mut file: BufReader<File> = BufReader::new(file);

        // Read out the file header
        let _ = read_header_from_file(&mut file).expect("File header to be correct");

        let mut vec = Vec::new();

        loop {
            let mut buffer = Vec::new();
            let bytes = file
                .read_until(0, &mut buffer)
                .expect("To have a file header");

            if bytes == 0 {
                break;
            }

            let string = from_utf8(&buffer[..buffer.len() - 1]).expect("valid utf8");

            let (mode, name) = string.split_once(' ').expect("space");

            let mode = Mode::from_str(mode).expect("valid mode");

            let mut hash: [u8; 64] = [0; 64];
            file.read_exact(&mut hash).expect("file to contain hash");

            let hash = Hash::from(hash);

            let object_file = hash.get_path(self.cache);

            let cache_object = CacheObject::from_file(self.cache, &object_file);

            vec.push(match cache_object.object_type {
                ObjectType::Blob => TreeObject::Blob(cache_object.to_blob(mode, name)),
                ObjectType::Tree => TreeObject::Tree(cache_object.to_tree(mode, name)),
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
    }

    fn to_blob(&self, mode: Mode, path: &str) -> Hashed<Blob> {
        assert!(self.object_type == ObjectType::Blob);

        let file = File::open(&self.file).unwrap();
        let mut file: BufReader<File> = BufReader::new(file);

        // Read out the file header
        let Header { size, .. } =
            read_header_from_file(&mut file).expect("File header to be correct");

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
        self.object_type
    }

    fn get_hash(&self) -> Hash {
        self.hash.clone()
    }

    fn get_prefix(&self) -> String {
        format!("{} {}\0", self.object_type.to_str(), self.size)
    }

    fn write_to(&self, _: &Path) {
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

    fn from_path(path: &PathBuf) -> Index {
        assert!(path.is_dir());
        Index {
            timestamp: Utc::now(),
            tree: Hashed::from_object(Tree::from_dir(path)),
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
        write!(hasher, "{}{}", self.get_prefix(), body).unwrap();
        Hash::from(hasher)
    }

    fn get_prefix(&self) -> String {
        format!("{} {}\0", INDEX_KEY, self.get_body().len())
    }

    fn write_to(&self, path: &Path) {
        let mut file = File::create(path).unwrap();

        file.write_all(self.get_prefix().as_bytes()).unwrap();
        file.write_all(self.get_body().as_bytes()).unwrap();
    }

    // fn from_file(cache: &PathBuf, index: &PathBuf) -> Index {
    //     let mut reader = Index::read_file_and_verify_type(index);

    //     let mut line = String::new();

    //     let kv: HashMap<&str, &str> = HashMap::new();

    //     while reader.read_line(&mut line).is_ok() {
    //         let (key, value) = line.split_once(':').unwrap();

    //         kv.insert(key, value.trim())
    //     }

    //     let timestamp = DateTime<Utc>::from_utf8(kv["timestamp"]);

    //     Index {
    //         timestamp: ,
    //         tree: Hashed::from_hash(cache, kv["tree"].into())
    //     }
    // }
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
            value.extend_from_slice(&object.to_tree_bytes());
        }

        value
    }

    fn from_dir(path: &PathBuf) -> Self {
        assert!(path.is_dir());

        let mut contents: Vec<TreeObject> = std::fs::read_dir(path)
            .expect("Failed to read directory")
            .map(|entry| {
                let path = entry.expect("Failed to read directory entry").path();
                if path.is_dir() {
                    TreeObject::Tree(Hashed::from_object(Tree::from_dir(&path)))
                } else {
                    TreeObject::Blob(Hashed::from_object(Blob::from_path(&path)))
                }
            })
            .collect();

        // read_dir returns entries in filesystem order, which is not
        // guaranteed to be stable. Sort by name so the resulting tree hash
        // is deterministic across platforms and repeated runs.
        contents.sort_by(|a, b| a.path_component().cmp(b.path_component()));

        Self {
            mode: Mode::Tree,
            contents,
            path: match path.file_name() {
                Some(v) => v.to_string_lossy().to_string(),
                None => panic!("{path:?} did not have a filename"),
            },
        }
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
        write!(hasher, "{}", self.get_prefix()).unwrap();
        hasher
            .write_all(&body)
            .expect("Body to be added to the hasher");
        Hash::from(hasher)
    }

    fn write_to(&self, path: &Path) {
        let mut file = File::create(path).unwrap();

        file.write_all(self.get_prefix().as_bytes()).unwrap();
        file.write_all(&self.get_body()).unwrap();
    }

    fn get_prefix(&self) -> String {
        format!("{} {}\0", TREE_KEY, self.get_body().len())
    }

    // fn from_file(cache: &PathBuf, file: &PathBuf) -> Self {
    //     let mut reader = Object::read_file_and_verify_type(file);

    //     let mut line = String::new();

    //     while reader.read_line(&mut line).is_ok() {
    //         let (detail, hash) = line.split_once('\0').unwrap();

    //     }
    // }
}

#[derive(Debug)]
struct Blob {
    mode: Mode,
    path: String,
    file: PathBuf,
    size: u64,
}

impl Blob {
    fn from_path(path: &Path) -> Self {
        assert!(path.is_file());

        Self {
            // TODO: Support other types
            mode: Mode::Normal,
            path: path.file_name().unwrap().to_string_lossy().to_string(),
            size: path.metadata().unwrap().len(),
            file: path.to_path_buf(),
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
        let mut hasher = Sha512::new();
        hasher.write_all(self.get_prefix().as_bytes()).unwrap();

        let f = File::open(&self.file).unwrap();
        let mut reader = BufReader::new(f);

        let mut buf: [u8; 1024] = [0; 1024];

        while let Ok(bytes_read) = reader.read(&mut buf) {
            if bytes_read == 0 {
                break;
            }

            hasher.write_all(&buf[..bytes_read]).unwrap();
        }

        Hash::from(hasher)
    }

    fn write_to(&self, path: &Path) {
        let mut file = File::create(path).unwrap();

        file.write_all(self.get_prefix().as_bytes()).unwrap();

        let mut src = File::open(&self.file).unwrap();
        std::io::copy(&mut src, &mut file).unwrap();
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

trait ObjectWithPath: WithPath + Object {}

impl TreeObject {
    fn to_tree_bytes(&self) -> Vec<u8> {
        match self {
            Self::Tree(tree) => get_bytes_from_thing(tree.deref(), &tree.hash),
            Self::Blob(blob) => get_bytes_from_thing(blob.deref(), &blob.hash),
        }
    }

    fn path_component(&self) -> &str {
        match self {
            Self::Tree(tree) => tree.get_path_component(),
            Self::Blob(blob) => blob.get_path_component(),
        }
    }
}

fn get_bytes_from_thing<T: WithPath>(object: &T, hash: &Hash) -> Vec<u8> {
    let mut path = Vec::new();

    path.extend_from_slice(object.get_mode().to_string().as_bytes());
    path.push(b' ');
    path.extend_from_slice(object.get_path_component().as_bytes());
    path.push(0);
    path.extend_from_slice(&hash.hash);

    path
}

impl<T: Object> Hashed<T> {
    fn write_if_not_exists(&self, dir: &Path) {
        let path = self.hash.get_path(dir);
        let dir = path.parent().unwrap();

        let _ = create_dir_all(dir);

        if !path.exists() {
            // println!("writing {:?} {:?}", T::get_object_type(), path);
            self.write_to(&path);
        }
    }
}

fn write_index_to_folder(dir: &PathBuf, index: &Hashed<Index>) {
    index.write_if_not_exists(dir);

    write_tree_to_folder(dir, &index.tree);
}

fn write_tree_to_folder(dir: &PathBuf, tree: &Hashed<Tree>) {
    tree.write_if_not_exists(dir);

    for element in tree.contents.iter() {
        match element {
            TreeObject::Tree(tree) => write_tree_to_folder(dir, tree),
            TreeObject::Blob(blob) => blob.write_if_not_exists(dir),
        }
    }
}

fn get_total_size(index: &Hashed<Tree>) -> u128 {
    let mut total = 0;

    for element in &index.contents {
        total += match element {
            TreeObject::Tree(tree) => get_total_size(tree),
            TreeObject::Blob(blob) => blob.size as u128,
        }
    }

    total
}

fn commit_directory(cache: &PathBuf, path: &PathBuf) {
    assert!(path.exists());
    assert!(path.is_dir());

    if cache.exists() {
        assert!(cache.is_dir());
    } else {
        create_dir(cache).unwrap();
    }

    let Ok(path) = path.canonicalize() else {
        panic!("unable to canonicalize {path:?}");
    };

    let index = Hashed::from_object(Index::from_path(&path));

    println!(
        "Finished generating Index for {} bytes of data",
        get_total_size(&index.tree)
    );

    write_index_to_folder(cache, &index);

    println!("{}", index.hash);
}

fn restore_directory(cache: &PathBuf, path: &PathBuf, index: Hash, validate: bool) {
    if !path.exists() {
        create_dir_all(path).expect("Directory Creation to work");
    }

    if !path.is_dir() {
        panic!("Path provided must be a valid directory");
    }

    if read_dir(path).unwrap().any(|_| true) {
        panic!("Path provided must be an empty directory");
    }

    let index_path = index.get_path(cache);
    let index_cache = Hashed::from_object(CacheObject::from_file(cache, &index_path));

    let index = index_cache.to_index();

    // println!("{index:?}");

    write_tree(&index.tree, path);

    if validate {
        validate_tree(&index.tree, path);
    }
}

fn validate_tree(tree: &Tree, path: &Path) {
    for item in tree.contents.iter() {
        if let TreeObject::Tree(tree) = item {
            let tree_path = path.join(&tree.path);
            validate_tree(tree, &tree_path);
            continue;
        }

        let TreeObject::Blob(blob) = item else {
            unreachable!();
        };

        let blob_path = path.join(&blob.path);

        let blob_hash = Hashed::from_object(Blob::from_path(&blob_path));

        assert!(blob_hash.hash == blob.hash);
    }
}

fn write_tree(tree: &Tree, path: &Path) {
    for item in tree.contents.iter() {
        if let TreeObject::Tree(tree) = item {
            let tree_path = path.join(&tree.path);

            create_dir(&tree_path).expect("Directory creation to work");

            write_tree(tree, &tree_path);
            continue;
        }

        let TreeObject::Blob(blob) = item else {
            unreachable!();
        };

        let blob_path = path.join(&blob.path);

        let file = File::create(blob_path).expect("File to be created");
        let mut writer = BufWriter::new(file);

        let cache_file = File::open(&blob.file).unwrap();
        let mut reader = BufReader::new(cache_file);

        let _ = read_header_from_file(&mut reader);

        let mut data: [u8; 1024] = [0; 1024];
        while let Ok(num) = reader.read(&mut data) {
            if num == 0 {
                break;
            }
            writer.write_all(&data[..num]).unwrap();
        }
    }
}

fn cat_object(cache: &Path, hash: &Hash) {
    let object_path = hash.get_path(cache);

    let file = File::open(&object_path).unwrap();
    let mut reader: BufReader<File> = BufReader::new(file);

    read_header_from_file(&mut reader).expect("file to contain a valid header");

    let mut stdout = std::io::stdout();
    let mut data: [u8; 1024] = [0; 1024];
    while let Ok(num) = reader.read(&mut data) {
        if num == 0 {
            break;
        }
        stdout.write_all(&data[..num]).unwrap();
    }
    println!();
}

fn push_cache(cache: &PathBuf, url: &String, hash: Option<Hash>) {
    if let Some(hash) = hash {
        let file = hash.get_path(cache);
        upload_object(&hash, &file, url);
        return;
    }

    for entry in read_dir(cache).unwrap().filter_map(|x| x.ok()) {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };

        if metadata.is_file() {
            continue;
        }

        let prefix = entry.file_name();

        for entry in read_dir(entry.path()).unwrap().filter_map(|x| x.ok()) {
            let Ok(metadata) = entry.metadata() else {
                continue;
            };

            if !metadata.is_file() {
                continue;
            }

            let name = format!(
                "{}{}",
                prefix.to_string_lossy(),
                entry.file_name().to_string_lossy()
            );
            let hash = Hash::try_from(name).expect("Hash to be valid");

            upload_object(&hash, &entry.path(), url);
        }
    }
}

fn pull_tree(cache: &PathBuf, url: &String, tree_hash: &Hash) {
    let tree_path = tree_hash.get_path(cache);

    // tree already exists locally so we can skip downloading it
    if !tree_path.exists() {
        let Some(Header { object_type, .. }) = download_object(tree_hash, &tree_path, url) else {
            eprintln!("Unable to download object with hash {tree_hash}");
            return;
        };
        assert!(object_type == ObjectType::Tree);
    }

    let mut file = File::open(tree_path).expect("Index file to exist");
    let mut index_data = Vec::new();
    let _ = file
        .read_to_end(&mut index_data)
        .expect("file to be readable");

    let (_, data) = read_header_and_body(&index_data).expect("Index to be in the correct format");

    let index_body = common::object_body::Tree::from_data(data);

    for entry in index_body.contents {
        let obj_path = entry.hash.get_path(cache);
        let Some(Header { object_type, .. }) = download_object(&entry.hash, &obj_path, url) else {
            eprintln!("Unable to download object with hash {}", entry.hash);
            return;
        };

        assert!(object_type != ObjectType::Index);

        if object_type == ObjectType::Tree {
            pull_tree(cache, url, &entry.hash);
        }
    }
}

fn pull_cache(cache: &PathBuf, url: &String, hash: Hash) {
    let index_path = hash.get_path(cache);
    let Some(Header { object_type, .. }) = download_object(&hash, &index_path, url) else {
        eprintln!("Unable to download object with hash {hash}");
        return;
    };

    assert!(object_type == ObjectType::Index);

    let mut file = File::open(index_path).expect("Index file to exist");
    let mut index_data = Vec::new();
    let _ = file
        .read_to_end(&mut index_data)
        .expect("file to be readable");

    let (_, data) = read_header_and_body(&index_data).expect("Index to be in the correct format");

    let index_body = common::object_body::Index::from_data(data);

    pull_tree(cache, url, &index_body.tree);
}

fn upload_object(hash: &Hash, file: &PathBuf, url: &String) {
    let file = File::open(file).expect("File to exist");
    let mut reader = BufReader::new(file);

    let Header { object_type, size } =
        read_header_from_file(&mut reader).expect("file to be a valid object");

    let url = format!("{url}/object/{hash}");

    println!("Sending put request to {url}");

    let response = ureq::put(url)
        .header("Object-Type", object_type.to_str())
        .header("Object-Size", size.to_string())
        .send(SendBody::from_reader(&mut reader));

    if let Err(err) = response {
        eprintln!("There was an error sending request {err:?}")
    }
}

fn download_object(hash: &Hash, file: &PathBuf, url: &String) -> Option<Header> {
    let url = format!("{url}/object/{hash}");

    let dir = file.parent().expect("Path to not be at root");
    create_dir_all(dir).expect("Directory to be created");

    if file.exists() {
        let file = File::open(file).expect("File to exist");
        let mut reader = BufReader::new(file);

        let mut buffer = Vec::new();
        reader
            .read_until(0, &mut buffer)
            .expect("Header to exist within file");

        // subtract one to get rid of the null byte
        return read_header_from_slice(&buffer[..buffer.len() - 1]);
    }

    println!("Sending get request to {url}");

    let response = ureq::get(url).call();

    let mut response = match response {
        Ok(v) => v,
        Err(err) => {
            eprintln!("There was an error sending request {err:?}");
            return None;
        }
    };

    let file = File::create(file).expect("File to exist");
    let mut writer = BufWriter::new(file);

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

    writer.write_all(header.to_string().as_bytes()).unwrap();

    let mut data: [u8; 1024] = [0; 1024];
    let mut reader = response.body_mut().as_reader();
    while let Ok(num) = reader.read(&mut data) {
        if num == 0 {
            break;
        }

        writer.write_all(&data[..num]).unwrap();
    }

    Some(header)
}

fn pack_archive(
    cache: &Path,
    path: &Path,
    index_hash: &Hash,
    compression: CompressionAlgorithm,
    compression_level: CompressionLevel,
) -> anyhow::Result<()> {
    assert!(!path.exists());
    assert!(path.parent().map(|p| p.exists() && p.is_dir()) == Some(true));

    let index_path = index_hash.get_path(cache);
    assert!(index_path.exists());

    let index = {
        let file = File::open(index_path).expect("file to exist");
        let mut reader = BufReader::new(file);
        let mut data = Vec::new();
        let _ = reader.read_to_end(&mut data).expect("File to be readable");

        let (header, body) = read_header_and_body(&data).expect("File to be correctly formatted");
        assert!(header.object_type == ObjectType::Index);

        common::object_body::Index::from_data(body)
    };

    let mut headers: HashMap<Hash, Header> = HashMap::new();

    read_object_into_headers_sync(cache, &mut headers, &index.tree)?;

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
                .into_keys()
                .map(|hash| FileEntryData(hash.get_path(cache)))
                .collect(),
        },
    };

    let arx_file = File::create(path)?;
    let mut writer = BufWriter::new(arx_file);

    archive.to_data(compression_level, &mut writer)?;

    Ok(())
}

fn unpack_archive(cache: &Path, path: &Path) -> anyhow::Result<()> {
    assert!(path.exists() && path.is_file());

    let file = File::open(path)?;
    let mut file = BufReader::new(file);

    let archive = Archive::<RawEntryData>::from_data(&mut file)?;

    assert!(archive.body.entries.len() == archive.body.header.len());

    println!("Successfully read archive, Index {}", archive.hash);

    let index_data = archive.index.to_data();
    let index_header = Header::new(ObjectType::Index, index_data.len() as u64);

    let mut hasher = Sha512::new();
    hasher.write_all(index_header.to_string().as_bytes())?;
    hasher.write_all(&index_data)?;
    assert!(Hash::from(hasher) == archive.hash);

    let path = archive.hash.get_path(cache);
    let _ = create_dir_all(path.parent().unwrap());

    {
        let index_file = File::create(path)?;
        let mut writer = BufWriter::new(index_file);
        writer.write_all(index_header.to_string().as_bytes())?;
        writer.write_all(&index_data)?;
    }

    for (header, entry) in archive.body.header.into_iter().zip(archive.body.entries) {
        let path = header.hash.get_path(cache);
        let _ = create_dir_all(path.parent().unwrap());

        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        writer.write_all(&entry.turn_into_vec())?;
    }

    Ok(())
}

/// Archive body entry: either an already-serialised byte buffer (for tree and
/// index objects, which are small and built up in memory during the walk) or
/// a lazy source-file read (for blobs, which can be arbitrarily large).
enum ArchiveEntry {
    Raw(RawEntryData, u64),
    Source(SourceFileEntryData, u64),
}

impl ArchiveEntry {
    fn length(&self) -> u64 {
        match self {
            ArchiveEntry::Raw(_, len) => *len,
            ArchiveEntry::Source(_, len) => *len,
        }
    }
}

impl ArchiveEntryData for ArchiveEntry {
    fn turn_into_vec(self) -> Vec<u8> {
        match self {
            ArchiveEntry::Raw(data, _) => data.turn_into_vec(),
            ArchiveEntry::Source(data, _) => data.turn_into_vec(),
        }
    }
}

fn collect_archive_entries(tree: &Hashed<Tree>, entries: &mut HashMap<Hash, ArchiveEntry>) {
    if !entries.contains_key(&tree.hash) {
        let prefix = tree.get_prefix();
        let body = tree.get_body();
        let mut bytes = Vec::with_capacity(prefix.len() + body.len());
        bytes.extend_from_slice(prefix.as_bytes());
        bytes.extend_from_slice(&body);
        let length = bytes.len() as u64;
        entries.insert(
            tree.hash.clone(),
            ArchiveEntry::Raw(RawEntryData::new(bytes), length),
        );
    }

    for content in &tree.contents {
        match content {
            TreeObject::Tree(subtree) => collect_archive_entries(subtree, entries),
            TreeObject::Blob(blob) => {
                if entries.contains_key(&blob.hash) {
                    continue;
                }
                let header = Header::new(ObjectType::Blob, blob.size);
                let length = header.to_string().len() as u64 + blob.size;
                entries.insert(
                    blob.hash.clone(),
                    ArchiveEntry::Source(
                        SourceFileEntryData {
                            source_path: blob.file.clone(),
                            header,
                        },
                        length,
                    ),
                );
            }
        }
    }
}

fn archive_directory(
    directory: &Path,
    out_file: &Path,
    algorithm: CompressionAlgorithm,
    level: CompressionLevel,
) -> anyhow::Result<()> {
    assert!(!out_file.exists(), "output file must not already exist");
    assert!(
        out_file.parent().map(|p| p.exists() && p.is_dir()) == Some(true),
        "parent of output file must exist and be a directory"
    );
    assert!(directory.is_dir(), "source must be a directory");

    let directory = directory
        .canonicalize()
        .unwrap_or_else(|_| panic!("unable to canonicalize {directory:?}"));

    // start timer
    let start = std::time::Instant::now();

    let hashed_index = Hashed::from_object(Index::from_path(&directory));

    println!(
        "Finished generating Index for {} bytes of data in {} seconds",
        get_total_size(&hashed_index.tree),
        start.elapsed().as_secs_f64()
    );

    // Collect trees + blobs, deduping by hash. Index lives in the archive
    // header, not in body entries.
    let mut entries: HashMap<Hash, ArchiveEntry> = HashMap::new();
    collect_archive_entries(&hashed_index.tree, &mut entries);

    let mut offset: u64 = 0;
    let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::with_capacity(entries.len());
    let mut body_entries: Vec<ArchiveEntry> = Vec::with_capacity(entries.len());
    for (hash, entry) in entries {
        let length = entry.length();
        header_entries.push(ArchiveHeaderEntry {
            hash: hash.clone(),
            index: offset,
            length,
        });
        body_entries.push(entry);
        offset += length;
    }

    let archive_index = common::object_body::Index {
        tree: hashed_index.tree.hash.clone(),
        timestamp: hashed_index.timestamp,
        metadata: HashMap::new(),
    };

    let archive = Archive {
        header: HEADER,
        compression: algorithm,
        hash: hashed_index.hash.clone(),
        index: archive_index,
        body: ArchiveBody {
            header: header_entries,
            entries: body_entries,
        },
    };

    let out = File::create(out_file)?;
    let mut writer = BufWriter::new(out);
    archive.to_data(level, &mut writer)?;

    println!(
        "Finished writing archive in {} seconds",
        start.elapsed().as_secs_f64()
    );

    println!("{}", hashed_index.hash);
    Ok(())
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "Cache", default_value = "~/.cache/arx")]
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

        #[arg(long, default_value_t, alias = "compression", alias = "alg")]
        algorithm: CompressionAlgorithm,

        #[arg(long, default_value_t, allow_hyphen_values = true)]
        level: CompressionLevel,
    },

    Unpack {
        #[arg(long)]
        file: PathBuf,
    },

    /// Build an archive directly from a source directory, bypassing the local store.
    Archive {
        directory: PathBuf,

        #[arg(short, long, default_value = "archive.arx")]
        output: PathBuf,

        #[arg(long, default_value_t, alias = "compression", alias = "alg")]
        algorithm: CompressionAlgorithm,

        #[arg(long, default_value_t, allow_hyphen_values = true)]
        level: CompressionLevel,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Commit { directory } => commit_directory(&cli.cache, &directory),
        Commands::Restore {
            directory,
            index,
            validate,
        } => restore_directory(&cli.cache, &directory, index, validate),
        Commands::Cat { hash } => cat_object(&cli.cache, &hash),
        Commands::Push { url, index } => push_cache(&cli.cache, &url, index),
        Commands::Pull { url, index } => pull_cache(&cli.cache, &url, index),
        Commands::Pack {
            index,
            file,
            algorithm,
            level,
        } => pack_archive(&cli.cache, &file, &index, algorithm, level).expect("Packing to work"),
        Commands::Unpack { file } => unpack_archive(&cli.cache, &file).expect("Packing to work"),
        Commands::Archive {
            directory,
            output,
            algorithm,
            level,
        } => archive_directory(&directory, &output, algorithm, level).expect("Archiving to work"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_dir_with_files(files: &[&str]) -> TempDir {
        let dir = TempDir::new().unwrap();
        for name in files {
            std::fs::write(dir.path().join(name), name.as_bytes()).unwrap();
        }
        dir
    }

    #[test]
    fn tree_entries_are_sorted_by_name() {
        // Created in deliberately non-alphabetical order.
        let dir = make_dir_with_files(&["zebra.txt", "alpha.txt", "middle.txt"]);

        let tree = Tree::from_dir(&dir.path().to_path_buf());

        let names: Vec<&str> = tree.contents.iter().map(|o| o.path_component()).collect();
        assert_eq!(names, vec!["alpha.txt", "middle.txt", "zebra.txt"]);
    }

    #[test]
    fn tree_hash_is_deterministic() {
        let dir = make_dir_with_files(&["c.txt", "a.txt", "b.txt"]);

        let path = dir.path().to_path_buf();
        let first = Hashed::from_object(Tree::from_dir(&path)).hash;
        let second = Hashed::from_object(Tree::from_dir(&path)).hash;

        assert_eq!(first, second, "tree hash must be stable across runs");
    }

    #[test]
    fn archive_produces_parseable_output_with_expected_shape() {
        let src = make_dir_with_files(&["alpha.txt", "beta.txt", "gamma.txt"]);
        let out_dir = TempDir::new().unwrap();
        let out = out_dir.path().join("out.arx");

        archive_directory(
            src.path(),
            &out,
            CompressionAlgorithm::None,
            CompressionLevel::Default,
        )
        .expect("archive to succeed");

        // Reopen and parse.
        let f = File::open(&out).unwrap();
        let mut reader = BufReader::new(f);
        let archive = Archive::<RawEntryData>::from_data(&mut reader).expect("archive to parse");

        // Archive hash must match the SHA-512 of the index header + body bytes —
        // the same integrity invariant `unpack_archive` enforces when restoring
        // into a store. This catches mismatches between archive.hash and
        // archive.index without needing a separate reference build.
        let index_data = archive.index.to_data();
        let index_header = Header::new(ObjectType::Index, index_data.len() as u64);
        let mut hasher = Sha512::new();
        hasher
            .write_all(index_header.to_string().as_bytes())
            .unwrap();
        hasher.write_all(&index_data).unwrap();
        assert_eq!(
            archive.hash,
            Hash::from(hasher),
            "archive hash must equal sha512(index header + body)"
        );

        // Body has 1 tree + 3 blobs = 4 entries (index is in the archive header, not body).
        assert_eq!(
            archive.body.entries.len(),
            4,
            "expected 1 tree + 3 blob entries"
        );
        assert_eq!(archive.body.header.len(), 4);
    }

    #[test]
    fn archive_dedups_identical_file_contents() {
        // Two files with the same content → one blob entry in the archive.
        let src = TempDir::new().unwrap();
        std::fs::write(src.path().join("first.txt"), b"shared content").unwrap();
        std::fs::write(src.path().join("second.txt"), b"shared content").unwrap();

        let out_dir = TempDir::new().unwrap();
        let out = out_dir.path().join("out.arx");

        archive_directory(
            src.path(),
            &out,
            CompressionAlgorithm::None,
            CompressionLevel::Default,
        )
        .expect("archive to succeed");

        let f = File::open(&out).unwrap();
        let mut reader = BufReader::new(f);
        let archive = Archive::<RawEntryData>::from_data(&mut reader).expect("archive to parse");

        // 1 tree + 1 deduped blob = 2 body entries.
        assert_eq!(
            archive.body.entries.len(),
            2,
            "duplicate-content files must share a single blob entry"
        );
    }
}
