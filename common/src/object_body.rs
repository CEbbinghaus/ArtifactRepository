use std::{collections::HashMap, io::Write, str::from_utf8};

use chrono::{DateTime, Utc};

use crate::{Hash, Mode};

pub trait Object: Sized {
    fn from_data(data: &[u8]) -> anyhow::Result<Self>;
    fn to_data(&self) -> Vec<u8>;
}

const TREE_KEY: &str = "tree";
const TIMESTAMP_KEY: &str = "timestamp";
#[derive(Debug)]
pub struct Index {
    pub tree: Hash,
    pub timestamp: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

impl Object for Index {
    fn from_data(data: &[u8]) -> anyhow::Result<Self> {
        let string_data = from_utf8(data)
            .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in index data: {}", e))?;

        if !string_data.ends_with("\n\n") {
            anyhow::bail!("Index must end with a double newline");
        }
        let string_data = &string_data[..string_data.len() - 2];

        let mut tree_hash: Option<Hash> = None;
        let mut timestamp: Option<DateTime<Utc>> = None;
        let mut metadata = HashMap::new();

        for line in string_data.split('\n') {
            if line.trim().is_empty() {
                anyhow::bail!("Index cannot contain a blank line");
            }

            let (key, value) = line.split_once(':')
                .ok_or_else(|| anyhow::anyhow!("Invalid line format: missing ':'"))?;
            let key = key.trim();
            let value = value.trim();

            match key {
                TREE_KEY => tree_hash = Some(Hash::try_from(value)?),
                TIMESTAMP_KEY => {
                    timestamp = Some(
                        DateTime::parse_from_rfc3339(value)
                            .map_err(|e| anyhow::anyhow!("Invalid timestamp: {}", e))?
                            .into(),
                    );
                }
                _ => {
                    if tree_hash.is_none() {
                        anyhow::bail!("Tree must come before metadata");
                    }
                    if timestamp.is_none() {
                        anyhow::bail!("Timestamp must come before metadata");
                    }
                    if metadata.contains_key(key) {
                        anyhow::bail!("Duplicate key in metadata: {}", key);
                    }
                    metadata.insert(key.to_string(), value.to_string());
                }
            }
        }

        Ok(Index {
            tree: tree_hash.ok_or_else(|| anyhow::anyhow!("Missing tree hash in index"))?,
            timestamp: timestamp.ok_or_else(|| anyhow::anyhow!("Missing timestamp in index"))?,
            metadata,
        })
    }

    fn to_data(&self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();

        fn write_kv(data: &mut Vec<u8>, key: &str, value: &str) -> anyhow::Result<()> {
            data.write(key.as_bytes())?;
            data.push(b':');
            data.push(b' ');
            data.write(value.as_bytes())?;
            data.push(b'\n');
            
            Ok(())
        }

        write_kv(&mut data, TREE_KEY, &self.tree.as_str()).expect("Write to work");
        write_kv(&mut data, TIMESTAMP_KEY, &self.timestamp.to_rfc3339()).expect("Write to work");

        for (key, value) in &self.metadata {
            write_kv(&mut data, &key, &value).expect("Write to work");
        }
        data.push(b'\n');

        data
    }
}

#[derive(Debug)]
pub struct TreeEntry {
    pub mode: Mode,
    pub path: String,
    pub hash: Hash,
}

#[derive(Debug)]
pub struct Tree {
    pub contents: Vec<TreeEntry>,
}
impl Object for Tree {
    fn from_data(data: &[u8]) -> anyhow::Result<Self> {
        let mut contents = Vec::new();

        let mut index: usize = 0;
        loop {
            if index == data.len() {
                break;
            }

            let remaining = &data[index..];

            let Some(position) = remaining.iter().position(|v| *v == 0) else {
                anyhow::bail!("Entry must contain null char");
            };

            let string = from_utf8(&remaining[..position])
                .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in tree entry: {}", e))?;
            let position = position + 1;

            let (mode, name) = string
                .split_once(' ')
                .ok_or_else(|| anyhow::anyhow!("mode and filename must be separated by space"))?;
            let mode = Mode::from_str(mode)
                .ok_or_else(|| anyhow::anyhow!("Invalid mode: {}", mode))?;

            if position + 64 > remaining.len() {
                anyhow::bail!("Insufficient data for hash: need {} bytes, have {}", position + 64, remaining.len());
            }
            let hash = Hash::try_from(&remaining[position..position + 64])?;
            contents.push(TreeEntry {
                hash,
                mode,
                path: name.to_string(),
            });

            index += position + 64;
        }

        Ok(Tree { contents })
    }

    fn to_data(&self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();

        fn write_entry(data: &mut Vec<u8>, entry: &TreeEntry) -> anyhow::Result<()> {
            data.write(entry.mode.as_str().as_bytes())?;
            data.push(b' ');
            data.write(&entry.path.as_bytes())?;
            data.push(0);
            data.write(&entry.hash.hash)?;

            Ok(())
        }

        for entry in &self.contents {
            write_entry(&mut data, &entry).expect("Writing to works");
        }

        data
    }
}
