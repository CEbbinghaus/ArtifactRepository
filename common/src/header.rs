use std::{io::{Read, Write}, str::from_utf8};

use anyhow::{Result, anyhow};
use futures::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

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
        if data.len() == 0 {
            return Err(anyhow!("Invalid Header: No Data"));
        }

        let data = if data[data.len() - 1] == 0 {
            &data[..data.len() - 1]
        } else {
            data
        };

        Self::from_str(from_utf8(data)?)
    }

    pub fn from_str(string: &str) -> Result<Self> {
        let (object_type, size) = string.split_once(' ').ok_or(anyhow!("Invalid Header: missing space ' ' character"))?;

        Ok(Header::new(
            ObjectType::from_str(object_type).ok_or(anyhow!("Invalid Header: Invalid ObjectType \"{object_type}\""))?,
            size.parse()?,
        ))
    }

    pub fn from_buf(buffer: &[u8]) -> Result<Self> {
        if buffer.len() == 0 {
            return Err(anyhow!("Invalid Header: No Data"));
        }
        // Find the null marker of the header. If its not available then we just gotta assume the whole buffer is a valid utf8 header
        let null_position = buffer.iter().position(|x| *x == 0).unwrap_or(buffer.len());
        let buffer = &buffer[..null_position];

        Self::from_data(&buffer)
    }

    pub async fn read_from_async(reader: &mut (impl AsyncRead + AsyncSeek + std::marker::Unpin)) -> Result<Self> {
        let mut buffer = [0u8; 32];
        let bytes_read = reader.read(&mut buffer).await?;

        if bytes_read == 0 {
            return Err(anyhow!("Invalid Header: No Data"));
        }

        let buffer = &buffer[..bytes_read];

        // Find the null marker of the header. If its not available then we just gotta assume the whole buffer is a valid utf8 header
        let null_position = buffer.iter().position(|x| *x == 0).unwrap_or(buffer.len());
        let buffer = &buffer[..null_position];

        // We set the reader position to after the null byte so the body doesn't contain it
        reader.seek(std::io::SeekFrom::Start(null_position as u64 + 1)).await?;

        Self::from_data(&buffer)
    }

    pub fn read_from(reader: &mut impl Read) -> Result<Self> {
        let mut buffer = [0u8; 32];
        let bytes_read = reader.read(&mut buffer)?;
        
        if bytes_read == 0 {
            return Err(anyhow!("Invalid Header: No Data"));
        }
        
        let buffer = &buffer[..bytes_read];
        Self::from_buf(&buffer)
    }
}
