use anyhow::anyhow;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::digest::FixedOutput;
use sha2::Sha512;
use std::fmt::{self, Debug, Display};
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Clone)]
pub struct Hash {
    pub hash: [u8; 64],
}

impl Hash {
    /// Parse a 128-char hex string into a Hash, returning an error on invalid input.
    fn from_hex(hex_str: &str) -> Result<Self, anyhow::Error> {
        if hex_str.len() != 128 {
            return Err(anyhow!("Invalid length. Hash has to be 128 characters long"));
        }

        let mut hash = [0u8; 64];
        hex::decode_to_slice(hex_str, &mut hash)?;

        Ok(Self { hash })
    }

    pub fn get_parts(&self) -> (String, String) {
        let s = self.as_str();
        (s[..2].to_owned(), s[2..].to_owned())
    }

    pub fn as_str(&self) -> String {
        hex::encode(&self.hash)
    }

    pub fn from_string(value: &String) -> Option<Self> {
        Self::from_hex(value.as_str()).ok()
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

        Self::try_from(directory.to_str()?.to_owned() + filename.to_str()?).ok()
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

impl FromStr for Hash {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

impl TryFrom<String> for Hash {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_hex(&value)
    }
}

impl TryFrom<&str> for Hash {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::from_hex(value)
    }
}

impl TryFrom<&[u8]> for Hash {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 64 {
            return Err(anyhow!("Invalid length. Slice must be 64 bytes long"));
        }

        let data: [u8; 64] = value.try_into()?;
        Ok(Self { hash: data })
    }
}

impl From<[u8; 64]> for Hash {
    fn from(value: [u8; 64]) -> Self {
        Self { hash: value }
    }
}

impl From<Sha512> for Hash {
    fn from(value: Sha512) -> Self {
        Self::from(Into::<[u8; 64]>::into(value.finalize_fixed()))
    }
}

impl Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Hash").field(&self.as_str()).finish()
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Serialize for Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.as_str())
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct HashVisitor;

        impl<'de> Visitor<'de> for HashVisitor {
            type Value = Hash;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Sha512 hex string Hash")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Hash::from_hex(value).map_err(de::Error::custom)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_string(HashVisitor)
    }
}
