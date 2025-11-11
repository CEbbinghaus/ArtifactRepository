use sha2::digest::FixedOutput;

use sha2::Sha512;
use std::path::PathBuf;
use std::fmt::{Display, Debug};
// use std::hash::Hash;

#[derive(Clone)]
pub struct Hash {
    // Sha512 Hash value
    pub hash: [u8; 64],
    hash_string: String,
}



impl Hash {
    pub fn get_parts(&self) -> (&str, &str) {
        (&self.hash_string[..2], &self.hash_string[2..])
    }

	pub fn as_str(&self) -> &str {
		&self.hash_string
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

impl From<&String> for Hash {
    fn from(value: &String) -> Self {
        value.as_str().into()
    }
}

impl From<&str> for Hash {
    fn from(value: &str) -> Self {
        assert!(value.len() == 128);

        let hash = hex::decode(value).unwrap();

        assert!(hash.len() == 64);

        Self {
            hash: hash.try_into().unwrap(),
            hash_string: value.to_owned(),
        }
    }
}

impl From<&[u8]> for Hash {
    fn from(value: &[u8]) -> Self {
        let data: [u8; 64] = value.try_into().expect("slice to be valid 64 byte array");
        Self {
            hash_string: hex::encode(&value),
            hash: data,
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
