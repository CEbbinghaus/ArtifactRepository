use std::{collections::HashMap, str::from_utf8};

use chrono::{DateTime, Utc};

use crate::{Hash, Mode};

pub trait Object {
    fn from_data(data: &[u8]) -> Self;
}

#[derive(Debug)]
pub struct Index {
    pub timestamp: DateTime<Utc>,
    pub tree: Hash,
    pub metadata: HashMap<String, String>,
}

impl Object for Index {
    fn from_data(data: &[u8]) -> Self {
        let string_data = from_utf8(data).expect("Data to be in valid utf8 format");

        let mut tree_hash: Option<Hash> = None;
        let mut timestamp: Option<DateTime<Utc>> = None;
        let mut metadata = HashMap::new();
        
        let lines: Vec<&str> = string_data.split('\n').collect();
        for line in lines {
            let (key, value) = line
                .split_once(':')
                .expect("Each line to be properly formatted");
            let key = key.trim();
            let value = value.trim();

            match key.trim() {
                "tree" => tree_hash = Some(Hash::from(value)),
                "timestamp" => {
                    timestamp = Some(
                        DateTime::parse_from_rfc3339(value)
                            .expect("Timestamp to be in the rfc3339 format")
                            .into(),
                    )
                }
                _ => {
                    if let Some(_) = metadata.insert(key.to_string(), value.trim().to_string()) {
                        panic!("No duplicate keys allowed within Index Metadata");
                    }
                }
            }
        }

        Index {
            tree: tree_hash.expect("tree to exist within artifact metadata"),
            timestamp: timestamp.expect("timestamp to exist within artifact metadata"),
            metadata: metadata,
        }
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

            let hash = Hash::from(&remaining[position..position+64]);
            contents.push(TreeEntry {
                hash,
                mode,
                path: name.to_string(),
            });

            index += position + 64;
        }

        Tree { contents }
    }
}

#[derive(Debug)]
pub struct Blob {
    pub size: u64,
}
impl Object for Blob {
    fn from_data(data: &[u8]) -> Self {
        Blob { size: data.len() as u64 }
    }
}
