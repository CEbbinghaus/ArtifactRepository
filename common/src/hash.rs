use anyhow::anyhow;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::digest::FixedOutput;

use sha2::Sha512;
use std::fmt::{self, Debug, Display};
use std::path::PathBuf;
use std::str::FromStr;
// use std::hash::Hash;

#[derive(Clone, Serialize)]
pub struct Hash {
    // Sha512 Hash value
    #[serde(skip)]
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

        Some(Self::try_from(directory.to_str()?.to_owned() + filename.to_str()?).ok()?)
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
        s.try_into()
    }
}

impl TryFrom<String> for Hash {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 128 {
            return Err(anyhow!(
                "Invalid length. Hash has to be 128 characters long"
            ));
        }

        let mut hash = [0u8; 64];
        hex::decode_to_slice(&value, &mut hash)?;

        Ok(Self {
            hash,
            hash_string: value,
        })
    }
}

impl TryFrom<&str> for Hash {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 128 {
            return Err(anyhow!(
                "Invalid length. Hash has to be 128 characters long"
            ));
        }

        let mut hash = [0u8; 64];
        hex::decode_to_slice(value, &mut hash)?;

        Ok(Self {
            hash,
            hash_string: value.to_owned(),
        })
    }
}

impl TryFrom<&[u8]> for Hash {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 64 {
            return Err(anyhow!("Invalid length. Slice must be 64 bytes long"));
        }

        let data: [u8; 64] = value.try_into()?;
        Ok(Self {
            hash_string: hex::encode(value),
            hash: data,
        })
    }
}

impl From<[u8; 64]> for Hash {
    fn from(value: [u8; 64]) -> Self {
        Self {
            hash_string: hex::encode(value),
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

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value.len() != 128 {
                    return Err(de::Error::invalid_length(
                        value.len(),
                        &"A hex string with 128 characters",
                    ));
                }

                value.try_into().map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_string(HashVisitor)
    }
}
