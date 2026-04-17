use std::{collections::HashMap, io::Write, str::from_utf8};

use chrono::{DateTime, Utc};

use crate::{Hash, Mode};

pub trait Object {
    fn from_data(data: &[u8]) -> Self;
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
    //TODO: This HAS to return a result. We need to fix that
    fn from_data(data: &[u8]) -> Self {
        let string_data = from_utf8(data).expect("Data to be in valid utf8 format");

        assert!(
            &string_data[string_data.len() - 2..] == "\n\n",
            "Index MUST end in a double newline"
        );
        let string_data = &string_data[..string_data.len() - 2];

        let mut tree_hash: Option<Hash> = None;
        let mut timestamp: Option<DateTime<Utc>> = None;
        let mut metadata = HashMap::new();

        let lines: Vec<&str> = string_data.split('\n').collect();
        for line in lines {
            if line.trim() == "" {
                panic!("Index CANNOT contain a blank line")
            }

            let (key, value) = line
                .split_once(':')
                .expect("Each line to be properly formatted");
            let key = key.trim();
            let value = value.trim();

            match key.trim() {
                TREE_KEY => tree_hash = Some(Hash::try_from(value).expect("Hash to be valid")),
                TIMESTAMP_KEY => {
                    timestamp = Some(
                        DateTime::parse_from_rfc3339(value)
                            .expect("Timestamp to be in the rfc3339 format")
                            .into(),
                    )
                }
                _ => {
                    if tree_hash.is_none() {
                        panic!("Tree MUST come first");
                    }
                    if timestamp.is_none() {
                        panic!("Timestamp MUST come second");
                    }
                    if metadata.insert(key.to_string(), value.trim().to_string()).is_some() {
                        panic!("No duplicate keys allowed within Index Metadata");
                    }
                }
            }
        }

        Index {
            tree: tree_hash.expect("tree to exist within artifact metadata"),
            timestamp: timestamp.expect("timestamp to exist within artifact metadata"),
            metadata,
        }
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

        write_kv(&mut data, TREE_KEY, self.tree.as_str()).expect("Write to work");
        write_kv(&mut data, TIMESTAMP_KEY, &self.timestamp.to_rfc3339()).expect("Write to work");

        for (key, value) in &self.metadata {
            write_kv(&mut data, key, value).expect("Write to work");
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
    fn from_data(data: &[u8]) -> Self {
        let mut contents = Vec::new();

        let mut index: usize = 0;
        loop {
            if index == data.len() {
                break;
            }

            let remaining = &data[index..];

            let Some(position) = remaining.iter().position(|v| *v == 0) else {
                panic!("Entry must contain null char");
            };

            let string = from_utf8(&remaining[..position]).expect("Entry must be valid utf8");
            let position = position + 1;

            let (mode, name) = string
                .split_once(' ')
                .expect("mode and filename to be seperated by space");
            let mode = Mode::from_str(mode).expect("valid mode");

            let hash =
                Hash::try_from(&remaining[position..position + 64]).expect("Hash to be valid");
            contents.push(TreeEntry {
                hash,
                mode,
                path: name.to_string(),
            });

            index += position + 64;
        }

        Tree { contents }
    }

    fn to_data(&self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();

        fn write_entry(data: &mut Vec<u8>, entry: &TreeEntry) -> anyhow::Result<()> {
            data.write(entry.mode.as_str().as_bytes())?;
            data.push(b' ');
            data.write(entry.path.as_bytes())?;
            data.push(0);
            data.write(&entry.hash.hash)?;

            Ok(())
        }

        for entry in &self.contents {
            write_entry(&mut data, entry).expect("Writing to works");
        }

        data
    }
}
