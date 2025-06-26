use std::{fmt::{Debug, Display}, fs::File, io::{BufRead, BufReader}, path::PathBuf, str::from_utf8};

use sha2::{digest::FixedOutput, Sha512};


pub const INDEX_KEY: &str = "index";
pub const TREE_KEY: &str = "tree";
pub const BLOB_KEY: &str = "blob";


#[derive(Clone)]
pub struct Hash {
    // Sha512 Hash value
    pub hash: [u8; 64],
    hash_string: String,
}

impl std::hash::Hash for Hash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl PartialEq for Hash {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for Hash {}

impl Hash {
    pub fn get_parts(&self) -> (&str, &str) {
        (&self.hash_string[..2], &self.hash_string[2..])
    }

    pub fn from_string(value: &String) -> Option<Self> {
        let value = value.as_str();

        if value.len() != 128 {
            return None;
        }

        let hash = hex::decode(value).ok()?;

        if hash.len() != 64 {
            return None;
        }

        Some(Self {
            hash: hash.try_into().unwrap(),
            hash_string: value.to_owned(),
        })
    }

    pub fn get_path(&self, cache_dir: &PathBuf) -> PathBuf {
        let (dir, file) = self.get_parts();
        cache_dir.join(dir).join(file)
    }

    pub fn from_path(file: &PathBuf) -> Option<Self> {
        let filename = file.file_name()?;
        let directory = file.parent()?.file_name()?;

        if directory.len() != 2 {
            return None;
        }

        if filename.len() != 126 {
            return None;
        }

        Some(Self::from(
            &(directory.to_str()?.to_owned() + filename.to_str()?),
        ))
    }
}

impl From<&String> for Hash {
    fn from(value: &String) -> Self {
        value.as_str().into()
    }
}

impl From<&str> for Hash {
    fn from(value: &str) -> Self {
        let value: &str = value.into();

        assert!(value.len() == 128);

        let hash = hex::decode(value).unwrap();

        assert!(hash.len() == 64);

        Self {
            hash: hash.try_into().unwrap(),
            hash_string: value.to_owned(),
        }
    }
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

impl Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Hash").field(&self.hash_string).finish()
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.hash_string)
    }
}


#[derive(Debug)]
pub enum Mode {
    Tree = 040000,
    Normal = 100644,
    Executable = 100755,
    SymbolicLink = 120000,
}

impl Mode {
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "040000" => Some(Mode::Tree),
            "100644" => Some(Mode::Normal),
            "100755" => Some(Mode::Executable),
            "120000" => Some(Mode::SymbolicLink),
            _ => None
        }
    }
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ObjectType {
    Blob,
    Tree,
    Index,
}

impl ObjectType {
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            BLOB_KEY => Some(Self::Blob),
            TREE_KEY => Some(Self::Tree),
            INDEX_KEY => Some(Self::Index),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Blob => BLOB_KEY,
            Self::Index => INDEX_KEY,
            Self::Tree => TREE_KEY,
        }
    }
}


pub fn read_header_from_slice(slice: &[u8]) -> Option<(ObjectType, u64)> {
    let string = from_utf8(slice).ok()?;

    let (object_type, size) = string.split_once(' ')?;

    Some((ObjectType::from_str(object_type)?, size.parse().ok()?))
}

pub fn read_header_from_file(reader: &mut BufReader<File>) -> Option<(ObjectType, u64)> {
    let mut vec = Vec::new();
    reader.read_until(b'\0', &mut vec).ok()?;

    read_header_from_slice(&vec[..vec.len() - 1])
}

pub fn get_object_prefix(object_type: ObjectType, object_size: u64) -> String {
    format!("{} {}\0", object_type.to_str(), object_size)
}
