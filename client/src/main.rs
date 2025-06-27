#![allow(dead_code)]
use common::{read_header_from_file, Hash, Mode, ObjectType, BLOB_KEY, INDEX_KEY, TREE_KEY};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use sha2::{Digest, Sha512};
use std::{
    collections::HashMap,
    fs::{create_dir, create_dir_all, read_dir, File},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    ops::Deref,
    path::PathBuf, str::from_utf8,
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
    fn write_to(&self, path: &PathBuf);

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

        let lines = string_data.split('\n').collect::<Vec<&str>>();

        for line in lines {
            let (key, value) = line.split_once(':').unwrap();

            _ = metadata.insert(key, value.trim());
        }

        let timestamp = DateTime::parse_from_rfc3339(metadata["timestamp"]).unwrap();

        let tree_hash = Hash::from(metadata["tree"]);

        let tree_object = CacheObject::from_file(self.cache, &tree_hash.get_path(self.cache));

        assert!(tree_object.get_object_type() == ObjectType::Tree);

        Hashed {
            hash: self.hash.clone(),
            inner: Index {
                timestamp: timestamp.into(),
                tree: tree_object.to_tree(Mode::Tree, &""),
            },
        }
    }

    fn to_tree(&self, mode: Mode, path: &str) -> Hashed<Tree> {
        assert!(self.object_type == ObjectType::Tree);

        println!("Reading tree {}", self.hash);

        let file = File::open(&self.file).unwrap();
        let mut file: BufReader<File> = BufReader::new(file);

        // Read out the file header
        let (_, _) = read_header_from_file(&mut file).expect("File header to be correct");

        let mut vec = Vec::new();

        loop {
            let mut buffer = Vec::new();
            let bytes = file.read_until(0, &mut buffer).expect("To have a file header");

            if bytes == 0 {
                break;
            }

            let string = from_utf8(&buffer[..buffer.len() - 1]).expect("valid utf8");

            let (mode, name) = string.split_once(' ').expect("space");

            let mode = Mode::from_str(mode).expect("valid mode");

            let mut hash: [u8; 64] = [0; 64];
            file.read_exact(&mut hash).expect("file to contain hash");

            let hash = Hash::from(hash);

            let object_file = hash.get_path(&self.cache);
            
            let cache_object = CacheObject::from_file(&self.cache, &object_file);

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
            }
        }
    }

    fn to_blob(&self, mode: Mode, path: &str) -> Hashed<Blob> {
        assert!(self.object_type == ObjectType::Blob);

        let file = File::open(&self.file).unwrap();
        let mut file: BufReader<File> = BufReader::new(file);

        // Read out the file header
        let (_, size) = read_header_from_file(&mut file).expect("File header to be correct");

        Hashed {
            hash: self.hash.clone(),

            inner: Blob {
                mode,
                path: path.to_string(),
                file: self.file.clone(),
                size,
            }
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

    fn write_to(&self, _: &PathBuf) {
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
            "timestamp: {}\ntree: {}",
            self.timestamp.to_rfc3339(),
            self.tree.hash
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

    fn write_to(&self, path: &PathBuf) {
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
            value.extend_from_slice(&mut object.to_tree_bytes());
        }

        value
    }

    fn from_dir(path: &PathBuf) -> Self {
        assert!(path.is_dir());

        Self {
            mode: Mode::Tree,
            contents: std::fs::read_dir(&path)
                .unwrap()
                .map(|entry| {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        TreeObject::Tree(Hashed::from_object(Tree::from_dir(&path)))
                    } else {
                        TreeObject::Blob(Hashed::from_object(Blob::from_path(&path)))
                    }
                })
                .collect(),
            path: match path.file_name() {
                Some(v) => v.to_string_lossy().to_string(),
                None => panic!("{path:?} did not have a filename")
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
        hasher.write_all(&body).expect("Body to be added to the hasher");
        Hash::from(hasher)
    }

    fn write_to(&self, path: &PathBuf) {
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
    fn from_path(path: &PathBuf) -> Self {
        assert!(path.is_file());

        Self {
            // TODO: Support other types
            mode: Mode::Normal,
            path: path.file_name().unwrap().to_string_lossy().to_string(),
            size: path.metadata().unwrap().len(),
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
        let mut hasher = Sha512::new();
        hasher.write_all(self.get_prefix().as_bytes()).unwrap();

        let f = File::open(&self.file).unwrap();
        let mut reader = BufReader::new(f);

        let mut buf: [u8; 1024] = [0; 1024];
        loop {
            let Ok(bytes_read) = reader.read(&mut buf) else {
                break;
            };

            if bytes_read == 0 {
                break;
            }

            hasher.write_all(&buf[..bytes_read]).unwrap();
        }

        Hash::from(hasher)
    }

    fn write_to(&self, path: &PathBuf) {
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
    fn write_if_not_exists(&self, dir: &PathBuf) {
        let hash = &self.hash.to_string();

        let dir_name = &hash[..2];

        let dir = &dir.join(dir_name);

        let _ = create_dir(dir);

        let file_name = &hash[2..];

        let path = &dir.join(file_name);

        if !path.exists() {
            // println!("writing {:?} {:?}", T::get_object_type(), path);
            self.write_to(path);
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
            TreeObject::Tree(tree) => write_tree_to_folder(dir, &tree),
            TreeObject::Blob(blob) => blob.write_if_not_exists(dir),
        }
    }
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
        index: String,
    },

    Cat {
        #[arg(long)]
        hash: String,
    },

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

fn commit_directory(cache: &PathBuf, path: &PathBuf) {
    assert!(path.exists());
    assert!(path.is_dir());

    if cache.exists() {
        assert!(cache.is_dir());
    } else {
        create_dir(&cache).unwrap();
    }

    let Ok(path) = path.canonicalize() else {
        panic!("unable to canonicalize {path:?}");
    };

    let index = Hashed::from_object(Index::from_path(&path));

    println!("Finished generating Index for {} bytes of data", get_total_size(&index.tree));

    write_index_to_folder(&cache, &index);

    println!("{}", index.hash);
}

fn restore_directory(cache: &PathBuf, path: &PathBuf, index: &String) {
    if !path.exists() {
        create_dir_all(path).expect("Directory Creation to work");
    }

    if !path.is_dir() {
        panic!("Path provided must be a valid directory");
    }

    if read_dir(path).unwrap().any(|_| true) {
        panic!("Path provided must be an empty directory");
    }

    let index_hash = Hash::from(index);

    let index_path = index_hash.get_path(cache);
    let index_cache = Hashed::from_object(CacheObject::from_file(cache, &index_path));

    let index = index_cache.to_index();

    println!("{index:?}");
    
    write_tree(&index.tree, path);
}

fn write_tree(tree: &Tree, path: &PathBuf) {

    for item in tree.contents.iter() {
        if let TreeObject::Tree(tree) = item {
            let tree_path = path.join(&tree.path);

            create_dir(&tree_path).expect("Directory creation to work");

            write_tree(&tree, &tree_path);
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
            writer.write(&data[..num]).unwrap();
        }
    }
}

fn cat_object(cache: &PathBuf, hash: &String) {
    let hash = Hash::from(hash);

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
        stdout.write(&data[..num]).unwrap();
    }
    println!();
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Commit { directory } => commit_directory(&cli.cache, &directory),
        Commands::Restore { directory, index } => restore_directory(&cli.cache, &directory, &index),
        Commands::Cat { hash } => cat_object(&cli.cache, &hash),
    }
}
