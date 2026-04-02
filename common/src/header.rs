use std::{io::{Read, Write}, str::from_utf8};

use anyhow::{Result, anyhow};
use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::ObjectType;

#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Header {
    pub object_type: ObjectType,
    pub size: u64,
}

impl Header {
    pub fn new(object_type: ObjectType, size: u64) -> Self {
        Header { object_type, size }
    }

    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        format!("{} {}\0", self.object_type.to_str(), self.size)
    }

    pub async fn write_to_async(&self, writer: &mut (impl AsyncWrite + std::marker::Unpin)) -> Result<(), std::io::Error> {
        writer.write_all(self.to_string().as_bytes()).await
    }

    pub fn write_to(&self, writer: &mut impl Write) -> Result<(), std::io::Error> {
        writer.write_all(self.to_string().as_bytes())
    }

    pub fn from_data(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(anyhow!("invalid header: no data"));
        }

        let data = if data[data.len() - 1] == 0 {
            &data[..data.len() - 1]
        } else {
            data
        };

        Self::from_str(from_utf8(data)?)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(string: &str) -> Result<Self> {
        let (object_type, size) = string.split_once(' ').ok_or(anyhow!("invalid header: missing space character"))?;

        Ok(Header::new(
            ObjectType::from_str(object_type).ok_or(anyhow!("invalid header: invalid object type \"{object_type}\""))?,
            size.parse()?,
        ))
    }

    pub fn from_buf(buffer: &[u8]) -> Result<Self> {
        if buffer.is_empty() {
            return Err(anyhow!("invalid header: no data"));
        }
        // Find the null marker of the header. If its not available then we just gotta assume the whole buffer is a valid utf8 header
        let null_position = buffer.iter().position(|x| *x == 0).unwrap_or(buffer.len());
        let buffer = &buffer[..null_position];

        Self::from_data(buffer)
    }

    pub async fn read_from_async(reader: &mut (impl AsyncRead + AsyncSeek + std::marker::Unpin)) -> Result<Self> {
        let mut buffer = [0u8; 32];
        let bytes_read = reader.read(&mut buffer).await?;

        if bytes_read == 0 {
            return Err(anyhow!("invalid header: no data"));
        }

        let buffer = &buffer[..bytes_read];

        // Find the null marker of the header. If its not available then we just gotta assume the whole buffer is a valid utf8 header
        let null_position = buffer.iter().position(|x| *x == 0).unwrap_or(buffer.len());
        let buffer = &buffer[..null_position];

        // We set the reader position to after the null byte so the body doesn't contain it
        reader.seek(std::io::SeekFrom::Start(null_position as u64 + 1)).await?;

        Self::from_data(buffer)
    }

    pub fn read_from(reader: &mut impl Read) -> Result<Self> {
        let mut buffer = [0u8; 32];
        let bytes_read = reader.read(&mut buffer)?;
        
        if bytes_read == 0 {
            return Err(anyhow!("invalid header: no data"));
        }
        
        let buffer = &buffer[..bytes_read];
        Self::from_buf(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_string_produces_expected_format() {
        let h = Header::new(ObjectType::Blob, 42);
        assert_eq!(h.to_string(), "blob 42\0");
    }

    #[test]
    fn to_string_tree_type() {
        let h = Header::new(ObjectType::Tree, 0);
        assert_eq!(h.to_string(), "tree 0\0");
    }

    #[test]
    fn to_string_index_type() {
        let h = Header::new(ObjectType::Index, 1024);
        assert_eq!(h.to_string(), "indx 1024\0");
    }

    #[test]
    fn from_str_round_trip() {
        let original = Header::new(ObjectType::Blob, 999);
        let s = original.to_string();
        // Strip the null byte for from_str
        let s = &s[..s.len() - 1];
        let recovered = Header::from_str(s).unwrap();
        assert_eq!(recovered.object_type, original.object_type);
        assert_eq!(recovered.size, original.size);
    }

    #[test]
    fn from_data_with_null_byte() {
        let data = b"blob 42\0";
        let h = Header::from_data(data).unwrap();
        assert_eq!(h.object_type, ObjectType::Blob);
        assert_eq!(h.size, 42);
    }

    #[test]
    fn from_data_without_null_byte() {
        let data = b"tree 100";
        let h = Header::from_data(data).unwrap();
        assert_eq!(h.object_type, ObjectType::Tree);
        assert_eq!(h.size, 100);
    }

    #[test]
    fn write_to_read_from_sync_round_trip() {
        let original = Header::new(ObjectType::Blob, 512);
        let mut buf = Vec::new();
        original.write_to(&mut buf).unwrap();

        // Append some body data to make it realistic
        buf.extend_from_slice(b"some body data here");

        let mut cursor = std::io::Cursor::new(&buf);
        let recovered = Header::read_from(&mut cursor).unwrap();
        assert_eq!(recovered.object_type, original.object_type);
        assert_eq!(recovered.size, original.size);
    }

    #[test]
    fn from_str_rejects_missing_space() {
        assert!(Header::from_str("blob42").is_err());
    }

    #[test]
    fn from_str_rejects_invalid_type() {
        assert!(Header::from_str("unknown 42").is_err());
    }

    #[test]
    fn from_data_rejects_empty() {
        assert!(Header::from_data(b"").is_err());
    }

    #[test]
    fn from_buf_extracts_header_before_null() {
        let buf = b"blob 100\0body data here";
        let h = Header::from_buf(buf).unwrap();
        assert_eq!(h.object_type, ObjectType::Blob);
        assert_eq!(h.size, 100);
    }
}
