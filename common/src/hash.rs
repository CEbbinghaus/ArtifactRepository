use anyhow::anyhow;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::digest::FixedOutput;
use sha2::Sha512;
use std::fmt::{self, Debug, Display};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::OnceLock;

#[derive(Clone)]
pub struct Hash {
    pub hash: [u8; 64],
    hex_cache: OnceLock<String>,
}

impl Hash {
    /// Parse a 128-char hex string into a Hash, returning an error on invalid input.
    fn from_hex(hex_str: &str) -> Result<Self, anyhow::Error> {
        if hex_str.len() != 128 {
            return Err(anyhow!("invalid hash length: expected 128 hex characters"));
        }

        let mut hash = [0u8; 64];
        hex::decode_to_slice(hex_str, &mut hash)?;

        Ok(Self { hash, hex_cache: OnceLock::new() })
    }

    pub fn get_parts(&self) -> (String, String) {
        let s = self.as_str();
        (s[..2].to_owned(), s[2..].to_owned())
    }

    pub fn as_str(&self) -> &str {
        self.hex_cache.get_or_init(|| hex::encode(self.hash))
    }

    #[allow(clippy::ptr_arg)]
    pub fn from_string(value: &String) -> Option<Self> {
        Self::from_hex(value.as_str()).ok()
    }

    #[allow(clippy::ptr_arg)]
    pub fn get_path(&self, cache_dir: &PathBuf) -> PathBuf {
        let (dir, file) = self.get_parts();
        cache_dir.join(dir).join(file)
    }

    #[allow(clippy::ptr_arg)]
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
            return Err(anyhow!("invalid hash length: expected 64 bytes"));
        }

        let data: [u8; 64] = value.try_into()?;
        Ok(Self { hash: data, hex_cache: OnceLock::new() })
    }
}

impl From<[u8; 64]> for Hash {
    fn from(value: [u8; 64]) -> Self {
        Self { hash: value, hex_cache: OnceLock::new() }
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
        serializer.serialize_str(self.as_str())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_hash(fill: u8) -> Hash {
        Hash::from([fill; 64])
    }

    fn zero_hash_hex() -> String {
        "0".repeat(128)
    }

    #[test]
    fn as_str_returns_128_char_hex() {
        let h = make_hash(0x00);
        assert_eq!(h.as_str().len(), 128);
        assert_eq!(h.as_str(), zero_hash_hex());
    }

    #[test]
    fn as_str_nonzero_bytes() {
        let h = make_hash(0xab);
        let expected = "ab".repeat(64);
        assert_eq!(h.as_str(), expected);
    }

    #[test]
    fn hex_round_trip() {
        let original = make_hash(0x3f);
        let hex_str = original.as_str().to_string();
        let recovered = Hash::try_from(hex_str).unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn get_parts_splits_correctly() {
        let h = make_hash(0xab);
        let (prefix, rest) = h.get_parts();
        assert_eq!(prefix.len(), 2);
        assert_eq!(rest.len(), 126);
        assert_eq!(format!("{}{}", prefix, rest), h.as_str());
    }

    #[test]
    fn get_path_builds_correct_path() {
        let h = make_hash(0xab);
        let cache = PathBuf::from("cache");
        let path = h.get_path(&cache);
        let (prefix, rest) = h.get_parts();
        assert_eq!(path, PathBuf::from("cache").join(&prefix).join(&rest));
    }

    #[test]
    fn from_path_inverts_get_path() {
        let original = make_hash(0xcd);
        let cache = PathBuf::from("/tmp/cache");
        let path = original.get_path(&cache);
        let recovered = Hash::from_path(&path).unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn from_path_rejects_wrong_structure() {
        let bad = PathBuf::from("/tmp/abc/def");
        assert!(Hash::from_path(&bad).is_none());
    }

    #[test]
    fn try_from_slice_rejects_wrong_length() {
        let short: &[u8] = &[0u8; 32];
        assert!(Hash::try_from(short).is_err());

        let long: &[u8] = &[0u8; 65];
        assert!(Hash::try_from(long).is_err());
    }

    #[test]
    fn try_from_slice_accepts_64_bytes() {
        let data = [0x42u8; 64];
        let h = Hash::try_from(data.as_slice()).unwrap();
        assert_eq!(h.hash, data);
    }

    #[test]
    fn try_from_string_rejects_non_hex() {
        let bad = "g".repeat(128);
        assert!(Hash::try_from(bad).is_err());
    }

    #[test]
    fn try_from_string_rejects_wrong_length() {
        let short = "ab".repeat(32); // 64 chars, need 128
        assert!(Hash::try_from(short).is_err());

        let long = "ab".repeat(65); // 130 chars
        assert!(Hash::try_from(long).is_err());
    }

    #[test]
    fn display_matches_as_str() {
        let h = make_hash(0xfe);
        assert_eq!(format!("{}", h), h.as_str());
    }

    #[test]
    fn debug_contains_hex() {
        let h = make_hash(0xfe);
        let debug = format!("{:?}", h);
        assert!(debug.contains(h.as_str()));
    }

    #[test]
    fn partial_eq_identical_bytes() {
        let a = make_hash(0x11);
        let b = make_hash(0x11);
        assert_eq!(a, b);
    }

    #[test]
    fn partial_eq_different_bytes() {
        let a = make_hash(0x11);
        let b = make_hash(0x22);
        assert_ne!(a, b);
    }

    #[test]
    fn hash_as_hashmap_key() {
        let mut map = HashMap::new();
        let h1 = make_hash(0xaa);
        let h2 = make_hash(0xbb);
        map.insert(h1.clone(), "first");
        map.insert(h2.clone(), "second");
        assert_eq!(map.get(&h1), Some(&"first"));
        assert_eq!(map.get(&h2), Some(&"second"));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn oncelock_cache_returns_same_value() {
        let h = make_hash(0xcc);
        let first = h.as_str();
        let second = h.as_str();
        assert!(std::ptr::eq(first, second));
    }

    #[test]
    fn from_str_trait() {
        let hex = "ab".repeat(64);
        let h: Hash = hex.parse().unwrap();
        assert_eq!(h.as_str(), "ab".repeat(64));
    }
}
