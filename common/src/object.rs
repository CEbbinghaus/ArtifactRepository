use crate::{Hash, Header};
use anyhow::{anyhow, Result};
use sha2::{Digest, Sha512};
use std::io::{BufReader, Read, Write};

pub struct Object {
    header: Header,
    data: Vec<u8>,
}

impl Object {
    pub fn to_hash(&self) -> Hash {
        let mut hasher = Sha512::new();
        hasher
            .write_all(self.header.to_string().as_bytes())
            .expect("Out of Memory");
        hasher.write_all(&self.data).expect("Out of Memory");
        hasher.into()
    }

    pub fn from_data(data: &[u8]) -> Option<Self> {
        let mut reader = BufReader::new(data);
        Self::read_from(&mut reader).ok()
    }

    pub fn read_from(reader: &mut impl Read) -> Result<Self> {
        let mut buffer = [0u8; 32];
        let bytes_read = reader.read(&mut buffer)?;
        let data = &buffer[..bytes_read];

        let Some(header_end) = data.iter().position(|x| *x == 0) else {
            return Err(anyhow!(
                "Invalid header. No null byte in the first 32 bytes"
            ));
        };

        let header = Header::from_data(&data[..header_end])?;

        let mut buffer = Vec::new();

        buffer.write_all(&data[header_end + 1..])?;
        reader.read_to_end(&mut buffer)?;

        Ok(Object {
            header,
            data: buffer,
        })
    }

    pub fn to_data(&self) -> Vec<u8> {
        let mut data = Vec::new();
        self.write_to(&mut data).expect("Out of Memory");
        data
    }

    pub fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        writer.write_all(self.header.to_string().as_bytes())?;
        writer.write_all(&self.data)?;
        Ok(())
    }
}
