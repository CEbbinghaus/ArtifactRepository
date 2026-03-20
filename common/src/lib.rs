#![warn(unused_imports)]

use std::{
    collections::HashMap, fs::File, io::{BufRead, BufReader, Read, Write}, str::from_utf8
};

use sha2::{Digest, Sha512};

use futures::AsyncReadExt;

pub use crate::constants::{BLOB_KEY, INDEX_KEY, TREE_KEY};
pub use crate::hash::Hash;
pub use crate::header::Header;
use crate::object_body::Object as _;
use crate::store::Store;
pub use crate::primitives::{Mode, ObjectType};
pub use crate::object::Object;

mod object;
mod constants;
mod hash;
mod header;
mod primitives;
pub mod object_body;
pub mod archive;
pub mod store;
pub mod tree_walk;

pub use tree_walk::{collect_index_metadata, FileInfo, IndexMetadata, ObjectInfo};

pub fn read_slice_until_byte(data: &[u8], byte: u8) -> Option<&[u8]> {
    let position = data.iter().position(|v| *v == byte)?;

    Some(&data[..position])
}

pub fn read_header_and_body(data: &[u8]) -> Option<(Header, &[u8])> {
    let header = read_slice_until_byte(data, 0)?;

    let body_index = header.len() + 1; // one extra for the 0 byte

    let header = read_header_from_slice(header)?;

    Some((header, &data[body_index..]))
}

pub fn read_header_from_slice(slice: &[u8]) -> Option<Header> {
    assert!(slice[slice.len() - 1] != 0);
    let string = from_utf8(slice).ok()?;

    let (object_type, size) = string.split_once(' ')?;

    Some(Header::new(ObjectType::from_str(object_type)?, size.parse().ok()?))
}

pub fn read_header_from_file(reader: &mut BufReader<File>) -> Option<Header> {
    let mut vec = Vec::new();
    reader.read_until(b'\0', &mut vec).ok()?;

    read_header_from_slice(&vec[..vec.len() - 1])
}

#[allow(clippy::mutable_key_type)]
pub async fn read_object_into_headers(
    store: &Store,
    headers: &mut HashMap<Hash, (Header, Vec<u8>)>,
    object_hash: &Hash,
) -> anyhow::Result<()> {
    tracing::debug!(root_hash = %object_hash, "reading object tree into headers");
    let mut stack = vec![object_hash.clone()];

    while let Some(current_hash) = stack.pop() {
        if headers.contains_key(&current_hash) {
            continue;
        }

        let mut object = store.get_object(&current_hash).await?;

        if object.header.object_type == ObjectType::Index {
            return Err(anyhow::anyhow!("indexes cannot exist within a tree (possible hash collision)"));
        }

        let header = object.header;

        let mut data = Vec::new();
        object.read_to_end(&mut data).await?;

        if header.object_type == ObjectType::Tree {
            let tree = crate::object_body::Tree::from_data(&data)?;

            for entry in &tree.contents {
                stack.push(entry.hash.clone());
            }
        }

        headers.insert(current_hash, (header, data));
    }

    tracing::debug!(objects_collected = headers.len(), "object tree walk complete");

    Ok(())
}

pub fn pipe(reader: &mut dyn Read, writer: &mut dyn Write) -> anyhow::Result<()> {
    let mut buffer: [u8; 65536] = [0; 65536];
    loop {
        let read = reader.read(&mut buffer)?;
        
        if read == 0 {
            break;
        }

        writer.write_all(&buffer[..read])?;
    }

    Ok(())
}

pub fn compute_hash(key: &str, data: &[u8]) -> Hash {
    tracing::trace!(key, data_len = data.len(), "computing hash");
    let mut hasher = Sha512::new();
    hasher.update(key.as_bytes());
    hasher.update(b" ");
    let mut buf = itoa::Buffer::new();
    hasher.update(buf.format(data.len()).as_bytes());
    hasher.update(b"\0");
    hasher.update(data);
    Hash::from(hasher)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_hash_deterministic() {
        let h1 = compute_hash("blob", b"hello world");
        let h2 = compute_hash("blob", b"hello world");
        assert_eq!(h1, h2);
    }

    #[test]
    fn compute_hash_different_keys_different_hashes() {
        let h1 = compute_hash("blob", b"data");
        let h2 = compute_hash("tree", b"data");
        assert_ne!(h1, h2);
    }

    #[test]
    fn compute_hash_different_data_different_hashes() {
        let h1 = compute_hash("blob", b"data1");
        let h2 = compute_hash("blob", b"data2");
        assert_ne!(h1, h2);
    }

    #[test]
    fn compute_hash_empty_data() {
        let h = compute_hash("blob", b"");
        // Just verify it doesn't panic and produces a valid hash
        assert_eq!(h.as_str().len(), 128);
    }

    #[test]
    fn pipe_copies_exact_bytes() {
        let input = b"hello world, this is test data!";
        let mut reader = &input[..];
        let mut writer = Vec::new();
        pipe(&mut reader, &mut writer).unwrap();
        assert_eq!(writer, input);
    }

    #[test]
    fn pipe_empty_reader() {
        let input: &[u8] = b"";
        let mut reader = input;
        let mut writer = Vec::new();
        pipe(&mut reader, &mut writer).unwrap();
        assert!(writer.is_empty());
    }

    #[test]
    fn pipe_large_data() {
        let input: Vec<u8> = (0..200_000).map(|i| (i % 256) as u8).collect();
        let mut reader = &input[..];
        let mut writer = Vec::new();
        pipe(&mut reader, &mut writer).unwrap();
        assert_eq!(writer, input);
    }

    #[test]
    fn read_header_and_body_splits_on_null() {
        let data = b"blob 5\0hello";
        let (header, body) = read_header_and_body(data).unwrap();
        assert_eq!(header.object_type, ObjectType::Blob);
        assert_eq!(header.size, 5);
        assert_eq!(body, b"hello");
    }

    #[test]
    fn read_header_and_body_no_null_returns_none() {
        let data = b"blob 5 hello";
        assert!(read_header_and_body(data).is_none());
    }

    #[test]
    fn read_header_from_slice_parses_correctly() {
        let data = b"blob 42";
        let h = read_header_from_slice(data).unwrap();
        assert_eq!(h.object_type, ObjectType::Blob);
        assert_eq!(h.size, 42);
    }

    #[test]
    fn read_header_from_slice_tree() {
        let data = b"tree 100";
        let h = read_header_from_slice(data).unwrap();
        assert_eq!(h.object_type, ObjectType::Tree);
        assert_eq!(h.size, 100);
    }

    #[test]
    fn read_header_from_slice_invalid_type() {
        let data = b"unknown 42";
        assert!(read_header_from_slice(data).is_none());
    }

    #[test]
    fn read_slice_until_byte_finds_byte() {
        let data = b"hello\0world";
        let result = read_slice_until_byte(data, 0).unwrap();
        assert_eq!(result, b"hello");
    }

    #[test]
    fn read_slice_until_byte_not_found() {
        let data = b"hello world";
        assert!(read_slice_until_byte(data, 0).is_none());
    }
}
