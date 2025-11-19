use std::{
    collections::HashMap, fs::File, io::{BufRead, BufReader, Read, Write}, str::from_utf8
};

use futures::AsyncReadExt;

pub use crate::constants::{BLOB_KEY, INDEX_KEY, TREE_KEY};
pub use crate::hash::Hash;
pub use crate::header::Header;
use crate::{object_body::Object as ObjectTrait, store::Store};
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

pub fn read_slice_until_byte<'a>(data: &'a [u8], byte: u8) -> Option<&'a [u8]> {
    let Some(position) = data.iter().position(|v| *v == byte) else {
        return None;
    };

    Some(&data[..position])
}

pub fn read_header_and_body<'a>(data: &'a [u8]) -> Option<(Header, &'a [u8])> {
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

pub async fn read_object_into_headers(
    store: &Store,
    headers: &mut HashMap<Hash, Header>,
    object_hash: &Hash,
) -> anyhow::Result<()> {
    let mut stack = vec![object_hash.clone()];

    while let Some(current_hash) = stack.pop() {
        if headers.contains_key(&current_hash) {
            continue;
        }

        let mut object = store.get_object(&current_hash).await?;

        if object.header.object_type == ObjectType::Index {
            return Err(anyhow::anyhow!("Indexes cannot exist within a tree. Likely a hash collision 😳"));
        }

        headers.insert(current_hash.clone(), object.header.clone());
        
        if object.header.object_type == ObjectType::Blob {
            continue;
        }

        let mut data = Vec::new();
        let bytes_read = object.read_to_end(&mut data).await?;

        assert!(bytes_read as u64 == object.header.size, "Read size must match header size");
    
        let tree = crate::object_body::Tree::from_data(&data);

        for entry in &tree.contents {
            stack.push(entry.hash.clone());
        }
    }

    Ok(())
}

pub fn pipe<'a, 'b>(reader: &'a mut dyn Read, writer: &'b mut dyn Write) -> anyhow::Result<()> {
    let mut buffer: [u8; 1024] = [0; 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        
        if read == 0 {
            break;
        }

        writer.write(&buffer[..read])?;
    }

    Ok(())
}
