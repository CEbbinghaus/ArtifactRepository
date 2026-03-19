use crate::{Hash, Header};
use anyhow::{Result, anyhow};
use sha2::{Digest, Sha512};
use std::io::{BufReader, Read, Write};

pub struct Object {
	header: Header,
	data: Vec<u8>
}

impl Object {
	pub fn to_hash(&self) -> Hash {
		let mut hasher = Sha512::new();
		hasher.write_all(self.header.to_string().as_bytes()).expect("Out of Memory");
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
			return Err(anyhow!("Invalid header. No null byte in the first 32 bytes"));
		};

		let header = Header::from_data(&data[..header_end])?;

		let mut buffer = Vec::new();

		buffer.write_all(&data[header_end + 1..])?;
		reader.read_to_end(&mut buffer)?;

		Ok(Object {
			header,
			data: buffer
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn to_data_from_data_round_trip() {
		let obj = Object {
			header: Header::new(crate::ObjectType::Blob, 5),
			data: b"hello".to_vec(),
		};
		let bytes = obj.to_data();
		let recovered = Object::from_data(&bytes).unwrap();
		assert_eq!(recovered.header.object_type, crate::ObjectType::Blob);
		assert_eq!(recovered.header.size, 5);
		assert_eq!(recovered.data, b"hello");
	}

	#[test]
	fn to_hash_deterministic() {
		let obj1 = Object {
			header: Header::new(crate::ObjectType::Blob, 3),
			data: b"abc".to_vec(),
		};
		let obj2 = Object {
			header: Header::new(crate::ObjectType::Blob, 3),
			data: b"abc".to_vec(),
		};
		assert_eq!(obj1.to_hash(), obj2.to_hash());
	}

	#[test]
	fn to_hash_different_data_different_hash() {
		let obj1 = Object {
			header: Header::new(crate::ObjectType::Blob, 3),
			data: b"abc".to_vec(),
		};
		let obj2 = Object {
			header: Header::new(crate::ObjectType::Blob, 3),
			data: b"xyz".to_vec(),
		};
		assert_ne!(obj1.to_hash(), obj2.to_hash());
	}

	#[test]
	fn write_to_read_from_round_trip() {
		let obj = Object {
			header: Header::new(crate::ObjectType::Tree, 11),
			data: b"tree body!!".to_vec(),
		};
		let mut buf = Vec::new();
		obj.write_to(&mut buf).unwrap();

		let mut cursor = std::io::Cursor::new(&buf);
		let recovered = Object::read_from(&mut cursor).unwrap();
		assert_eq!(recovered.header.object_type, crate::ObjectType::Tree);
		assert_eq!(recovered.header.size, 11);
		assert_eq!(recovered.data, b"tree body!!");
	}

	#[test]
	fn from_data_returns_none_for_garbage() {
		assert!(Object::from_data(b"no null byte here").is_none());
	}
}
