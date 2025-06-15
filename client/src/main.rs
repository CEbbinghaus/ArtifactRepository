#![allow(dead_code)]

use chrono::{DateTime, Utc};
use sha2::{digest::FixedOutput, Digest, Sha512};
use std::{
    env,
    fmt::Display,
    fs::{create_dir, File},
    io::{BufRead, BufReader, Write},
    path::PathBuf,
};

const INDEX_KEY: &str = "index";
const TREE_KEY: &str = "tree";
const BLOB_KEY: &str = "blob";

struct Hash {
    // Sha512 Hash value
    hash: [u8; 64],
    hash_string: String,
}

impl From<[u8; 64]> for Hash {
    fn from(value: [u8; 64]) -> Self {
        Self {
            hash_string: hex::encode(&value),
            hash: value,
        }
    }
}

impl From<Sha512> for Hash {
    fn from(value: Sha512) -> Self {
        Self::from(Into::<[u8; 64]>::into(value.finalize_fixed()))
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.hash_string)
    }
}

struct Hashed<T: Object> {
    inner: T,
    hash: Hash,
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
    fn get_object_type() -> ObjectType;
    fn get_hash(&self) -> Hash;
    fn get_prefix(&self) -> String;
    fn write_to(&self, path: &PathBuf);
}

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

    fn from_path(path: PathBuf) -> Index {
        assert!(path.is_dir());
        Index {
            timestamp: Utc::now(),
            tree: Hashed::from_object(Tree::from_dir(path)),
        }
    }
}

impl Object for Index {
    fn get_object_type() -> ObjectType {
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
}

trait WithPath {
    fn get_path_component(&self) -> String;
}

struct Tree {
    path: PathBuf,
    contents: Vec<(Mode, TreeObject)>,
}

impl Tree {
    fn get_body(&self) -> String {
        let mut value = String::new();

        for (mode, object) in self.contents.iter() {
            value += format!("{} ", mode).as_str();
            value += object.to_tree_string().as_str();
        }

        value
    }

    fn from_dir(path: PathBuf) -> Self {
        assert!(path.is_dir());

        Self {
            contents: std::fs::read_dir(&path)
                .unwrap()
                .map(|entry| {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        (
                            Mode::Tree,
                            TreeObject::Tree(Hashed::from_object(Tree::from_dir(path))),
                        )
                    } else {
                        // TODO: Select mode correctly
                        (
                            Mode::Normal,
                            TreeObject::Blob(Hashed::from_object(Blob::from_path(path))),
                        )
                    }
                })
                .collect(),
            path,
        }
    }
}

impl WithPath for Tree {
    fn get_path_component(&self) -> String {
        self.path.file_name().unwrap().to_string_lossy().to_string()
    }
}

impl Object for Tree {
    fn get_object_type() -> ObjectType {
        ObjectType::Tree
    }

    fn get_hash(&self) -> Hash {
        let body = self.get_body();
        let mut hasher = Sha512::new();
        write!(hasher, "{}{}", self.get_prefix(), body).unwrap();
        Hash::from(hasher)
    }

    fn write_to(&self, path: &PathBuf) {
        let mut file = File::create(path).unwrap();

        file.write_all(self.get_prefix().as_bytes()).unwrap();
        file.write_all(self.get_body().as_bytes()).unwrap();
    }

    fn get_prefix(&self) -> String {
        format!("{} {}\0", TREE_KEY, self.get_body().len())
    }
}

struct Blob {
    file: PathBuf,
    size: u64,
}

impl Blob {
    fn from_path(path: PathBuf) -> Self {
        assert!(path.is_file());

        Self {
            size: path.metadata().unwrap().len(),
            file: path,
        }
    }
}

impl WithPath for Blob {
    fn get_path_component(&self) -> String {
        self.file.file_name().unwrap().to_string_lossy().to_string()
    }
}

impl Object for Blob {
    fn get_object_type() -> ObjectType {
        ObjectType::Blob
    }

    fn get_hash(&self) -> Hash {
        let mut hasher = Sha512::new();
        hasher.write_all(self.get_prefix().as_bytes()).unwrap();

        let f = File::open(&self.file).unwrap();
        let mut reader = BufReader::new(f);

        if reader.fill_buf().unwrap().len() > 0 {
            hasher.write_all(reader.buffer()).unwrap();
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

enum Mode {
    Tree = 040000,
    Normal = 100644,
    Executable = 100755,
    SymbolicLink = 120000,
}

impl Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Normal => "100644",
                Self::Tree => "040000",
                Self::Executable => "100755",
                Self::SymbolicLink => "120000",
            }
        )
    }
}

enum TreeObject {
    Tree(Hashed<Tree>),
    Blob(Hashed<Blob>),
}

impl TreeObject {
    fn to_tree_string(&self) -> String {
        match self {
            Self::Tree(tree) => format!("{}\0{}", tree.inner.get_path_component(), tree.hash),
            Self::Blob(blob) => format!("{}\0{}", blob.inner.get_path_component(), blob.hash),
        }
    }
}

#[derive(Debug)]
enum ObjectType {
    Blob,
    Tree,
    Index,
}

impl<T: Object> Hashed<T> {
    fn write_if_not_exists(&self, dir: &PathBuf) {
        let hash = &self.hash.hash_string;

        let dir_name = &hash[..2];

        let dir = &dir.join(dir_name);

        create_dir(dir).unwrap();

        let file_name = &hash[2..];

        let path = &dir.join(file_name);

        if !path.exists() {
            println!("writing {:?} {:?}", T::get_object_type(), path);
            self.inner.write_to(path);
        }
    }
}

fn write_index_to_folder(dir: &PathBuf, index: &Hashed<Index>) {
    index.write_if_not_exists(dir);

    write_tree_to_folder(dir, &index.inner.tree);
}

fn write_tree_to_folder(dir: &PathBuf, tree: &Hashed<Tree>) {
    tree.write_if_not_exists(dir);

    for (_, element) in tree.inner.contents.iter() {
        match element {
            TreeObject::Tree(tree) => write_tree_to_folder(dir, tree),
            TreeObject::Blob(blob) => blob.write_if_not_exists(dir),
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Not enough arguments");
    }

    let src_dir = PathBuf::from(&args[1]);
    let cache_dir = PathBuf::from(&args[2]);

    println!("src: {:?}", src_dir);
    println!("cache: {:?}", cache_dir);

    assert!(src_dir.exists());
    assert!(src_dir.is_dir());

    if cache_dir.exists() {
        assert!(cache_dir.is_dir());
    } else {
        create_dir(&cache_dir).unwrap();
    }

    let index = Hashed::from_object(Index::from_path(src_dir));

    write_index_to_folder(&cache_dir, &index);
}
