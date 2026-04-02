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
            .map_err(|e| anyhow::anyhow!("invalid UTF-8 in index data: {}", e))?;

        if !string_data.ends_with("\n\n") {
            anyhow::bail!("index must end with a double newline");
        }
        let string_data = &string_data[..string_data.len() - 2];

        let mut tree_hash: Option<Hash> = None;
        let mut timestamp: Option<DateTime<Utc>> = None;
        let mut metadata = HashMap::new();

        for line in string_data.split('\n') {
            if line.trim().is_empty() {
                anyhow::bail!("index cannot contain a blank line");
            }

            let (key, value) = line.split_once(':')
                .ok_or_else(|| anyhow::anyhow!("invalid line format: missing ':'"))?;
            let key = key.trim();
            let value = value.trim();

            match key {
                TREE_KEY => tree_hash = Some(Hash::try_from(value)?),
                TIMESTAMP_KEY => {
                    timestamp = Some(
                        DateTime::parse_from_rfc3339(value)
                            .map_err(|e| anyhow::anyhow!("invalid timestamp: {}", e))?
                            .into(),
                    );
                }
                _ => {
                    if tree_hash.is_none() {
                        anyhow::bail!("tree must come before metadata");
                    }
                    if timestamp.is_none() {
                        anyhow::bail!("timestamp must come before metadata");
                    }
                    if metadata.contains_key(key) {
                        anyhow::bail!("duplicate key in metadata: {}", key);
                    }
                    metadata.insert(key.to_string(), value.to_string());
                }
            }
        }

        Ok(Index {
            tree: tree_hash.ok_or_else(|| anyhow::anyhow!("missing tree hash in index"))?,
            timestamp: timestamp.ok_or_else(|| anyhow::anyhow!("missing timestamp in index"))?,
            metadata,
        })
    }

    fn to_data(&self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();

        fn write_kv(data: &mut Vec<u8>, key: &str, value: &str) -> anyhow::Result<()> {
            data.write_all(key.as_bytes())?;
            data.push(b':');
            data.push(b' ');
            data.write_all(value.as_bytes())?;
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
    fn from_data(data: &[u8]) -> anyhow::Result<Self> {
        let mut contents = Vec::new();

        let mut index: usize = 0;
        loop {
            if index == data.len() {
                break;
            }

            let remaining = &data[index..];

            let Some(position) = remaining.iter().position(|v| *v == 0) else {
                anyhow::bail!("entry must contain null char");
            };

            let string = from_utf8(&remaining[..position])
                .map_err(|e| anyhow::anyhow!("invalid UTF-8 in tree entry: {}", e))?;
            let position = position + 1;

            let (mode, name) = string
                .split_once(' ')
                .ok_or_else(|| anyhow::anyhow!("mode and filename must be separated by space"))?;
            let mode = Mode::from_str(mode)
                .ok_or_else(|| anyhow::anyhow!("invalid mode: {}", mode))?;

            if position + 64 > remaining.len() {
                anyhow::bail!("insufficient data for hash: need {} bytes, have {}", position + 64, remaining.len());
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
            data.write_all(entry.mode.as_str().as_bytes())?;
            data.push(b' ');
            data.write_all(entry.path.as_bytes())?;
            data.push(0);
            data.write_all(&entry.hash.hash)?;

            Ok(())
        }

        for entry in &self.contents {
            write_entry(&mut data, entry).expect("Writing to works");
        }

        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_hash(fill: u8) -> Hash {
        Hash::from([fill; 64])
    }

    // --- Index tests ---

    #[test]
    fn index_round_trip_no_metadata() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap();
        let idx = Index {
            tree: make_hash(0xaa),
            timestamp: ts,
            metadata: HashMap::new(),
        };
        let data = idx.to_data();
        let recovered = Index::from_data(&data).unwrap();
        assert_eq!(recovered.tree, make_hash(0xaa));
        assert_eq!(recovered.timestamp, ts);
        assert!(recovered.metadata.is_empty());
    }

    #[test]
    fn index_round_trip_with_metadata() {
        let ts = Utc.with_ymd_and_hms(2025, 6, 1, 8, 30, 0).unwrap();
        let mut meta = HashMap::new();
        meta.insert("version".to_string(), "1.0".to_string());
        meta.insert("author".to_string(), "test".to_string());
        let idx = Index {
            tree: make_hash(0xbb),
            timestamp: ts,
            metadata: meta,
        };
        let data = idx.to_data();
        let recovered = Index::from_data(&data).unwrap();
        assert_eq!(recovered.tree, make_hash(0xbb));
        assert_eq!(recovered.timestamp, ts);
        assert_eq!(recovered.metadata.get("version").unwrap(), "1.0");
        assert_eq!(recovered.metadata.get("author").unwrap(), "test");
    }

    #[test]
    fn index_body_ends_with_double_newline() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let idx = Index {
            tree: make_hash(0x00),
            timestamp: ts,
            metadata: HashMap::new(),
        };
        let data = idx.to_data();
        let s = std::str::from_utf8(&data).unwrap();
        assert!(s.ends_with("\n\n"), "Index body must end with \\n\\n");
    }

    #[test]
    fn index_tree_must_be_first_line() {
        // Metadata before tree should fail
        let bad = format!(
            "custom_key: value\ntree: {}\ntimestamp: {}\n\n",
            make_hash(0x00).as_str(),
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap().to_rfc3339(),
        );
        let result = Index::from_data(bad.as_bytes());
        assert!(result.is_err(), "Metadata before tree/timestamp should fail");
    }

    #[test]
    fn index_rejects_missing_double_newline() {
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let bad = format!(
            "tree: {}\ntimestamp: {}\n",
            make_hash(0x00).as_str(),
            ts.to_rfc3339(),
        );
        let result = Index::from_data(bad.as_bytes());
        assert!(result.is_err());
    }

    // --- Tree tests ---

    #[test]
    fn tree_round_trip_empty() {
        let tree = Tree { contents: vec![] };
        let data = tree.to_data();
        assert!(data.is_empty());
        let recovered = Tree::from_data(&data).unwrap();
        assert!(recovered.contents.is_empty());
    }

    #[test]
    fn tree_round_trip_single_normal_entry() {
        let tree = Tree {
            contents: vec![TreeEntry {
                mode: Mode::Normal,
                path: "file.txt".to_string(),
                hash: make_hash(0x11),
            }],
        };
        let data = tree.to_data();
        let recovered = Tree::from_data(&data).unwrap();
        assert_eq!(recovered.contents.len(), 1);
        assert_eq!(recovered.contents[0].path, "file.txt");
        assert_eq!(recovered.contents[0].hash, make_hash(0x11));
        assert_eq!(recovered.contents[0].mode.as_str(), "100644");
    }

    #[test]
    fn tree_round_trip_mixed_entries() {
        let entries = vec![
            TreeEntry {
                mode: Mode::Tree,
                path: "subdir".to_string(),
                hash: make_hash(0x01),
            },
            TreeEntry {
                mode: Mode::Normal,
                path: "readme.md".to_string(),
                hash: make_hash(0x02),
            },
            TreeEntry {
                mode: Mode::Executable,
                path: "run.sh".to_string(),
                hash: make_hash(0x03),
            },
        ];
        let tree = Tree { contents: entries };
        let data = tree.to_data();
        let recovered = Tree::from_data(&data).unwrap();
        assert_eq!(recovered.contents.len(), 3);
        assert_eq!(recovered.contents[0].mode.as_str(), "040000");
        assert_eq!(recovered.contents[0].path, "subdir");
        assert_eq!(recovered.contents[1].mode.as_str(), "100644");
        assert_eq!(recovered.contents[1].path, "readme.md");
        assert_eq!(recovered.contents[2].mode.as_str(), "100755");
        assert_eq!(recovered.contents[2].path, "run.sh");
    }

    #[test]
    fn tree_entry_ordering_preserved() {
        let entries = vec![
            TreeEntry { mode: Mode::Normal, path: "z_last".to_string(), hash: make_hash(0x0a) },
            TreeEntry { mode: Mode::Normal, path: "a_first".to_string(), hash: make_hash(0x0b) },
            TreeEntry { mode: Mode::Normal, path: "m_middle".to_string(), hash: make_hash(0x0c) },
        ];
        let tree = Tree { contents: entries };
        let data = tree.to_data();
        let recovered = Tree::from_data(&data).unwrap();
        assert_eq!(recovered.contents[0].path, "z_last");
        assert_eq!(recovered.contents[1].path, "a_first");
        assert_eq!(recovered.contents[2].path, "m_middle");
    }

    #[test]
    fn tree_filenames_with_special_characters() {
        let entries = vec![
            TreeEntry { mode: Mode::Normal, path: "file with spaces.txt".to_string(), hash: make_hash(0xdd) },
            TreeEntry { mode: Mode::Normal, path: "file-with-dashes".to_string(), hash: make_hash(0xee) },
            TreeEntry { mode: Mode::Normal, path: "file.multiple.dots.rs".to_string(), hash: make_hash(0xff) },
        ];
        let tree = Tree { contents: entries };
        let data = tree.to_data();
        let recovered = Tree::from_data(&data).unwrap();
        assert_eq!(recovered.contents[0].path, "file with spaces.txt");
        assert_eq!(recovered.contents[1].path, "file-with-dashes");
        assert_eq!(recovered.contents[2].path, "file.multiple.dots.rs");
    }

    #[test]
    fn tree_entry_mode_values() {
        assert_eq!(Mode::Tree.as_str(), "040000");
        assert_eq!(Mode::Normal.as_str(), "100644");
        assert_eq!(Mode::Executable.as_str(), "100755");
        assert_eq!(Mode::SymbolicLink.as_str(), "120000");
    }
}
