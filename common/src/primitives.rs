use crate::{BLOB_KEY, INDEX_KEY, TREE_KEY};
use std::{
	fmt::Display,
	fs::Permissions,
	os::unix::fs::{MetadataExt, PermissionsExt},
	time::{SystemTime, UNIX_EPOCH},
};

#[allow(clippy::zero_prefixed_literal)]
#[derive(Debug)]
pub enum Mode {
	Tree = 040000,
	Normal = 100644,
	SymbolicLink = 120000,
}

const TREE_MODE: &str = "040000";
const NORMAL_MODE: &str = "100644";
const SYMBOLIC_LINK_MODE: &str = "120000";

impl Mode {
	#[allow(clippy::should_implement_trait)]
	pub fn from_str(value: &str) -> Option<Self> {
		match value {
			TREE_MODE => Some(Mode::Tree),
			NORMAL_MODE => Some(Mode::Normal),
			SYMBOLIC_LINK_MODE => Some(Mode::SymbolicLink),
			_ => None,
		}
	}

	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Tree => TREE_MODE,
			Self::Normal => NORMAL_MODE,
			Self::SymbolicLink => SYMBOLIC_LINK_MODE,
		}
	}
}

impl Display for Mode {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.as_str())
	}
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ObjectType {
	Blob,
	Tree,
	Index,
}

impl ObjectType {
	#[allow(clippy::should_implement_trait)]
	pub fn from_str(value: &str) -> Option<Self> {
		match value {
			BLOB_KEY => Some(Self::Blob),
			TREE_KEY => Some(Self::Tree),
			INDEX_KEY => Some(Self::Index),
			_ => None,
		}
	}

	pub fn to_str(&self) -> &'static str {
		match self {
			Self::Blob => BLOB_KEY,
			Self::Index => INDEX_KEY,
			Self::Tree => TREE_KEY,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
	pub fn now() -> anyhow::Result<Self> {
		Self::from_system_time(SystemTime::now())
	}

	pub fn as_nanos(&self) -> u64 {
		self.0
	}

	pub fn from_system_time(time: SystemTime) -> anyhow::Result<Self> {
		let nanos = time.duration_since(UNIX_EPOCH)?.as_nanos();

		u64::try_from(nanos).map(Self).map_err(|_| {
			anyhow::anyhow!("Timestamp overflow: system time is too far in the future")
		})
	}

	pub const fn from_nanos(nanos: u64) -> Self {
		Self(nanos)
	}

	pub fn to_system_time(&self) -> SystemTime {
		UNIX_EPOCH + std::time::Duration::from_nanos(self.0)
	}

	pub fn as_bytes(&self) -> [u8; 8] {
		Into::<[u8; 8]>::into(*self)
	}
}

impl From<[u8; 8]> for Timestamp {
	fn from(value: [u8; 8]) -> Self {
		Self(u64::from_le_bytes(value))
	}
}

impl From<Timestamp> for [u8; 8] {
	fn from(value: Timestamp) -> Self {
		value.0.to_le_bytes()
	}
}

impl TryFrom<SystemTime> for Timestamp {
	type Error = anyhow::Error;

	fn try_from(value: SystemTime) -> Result<Self, Self::Error> {
		Self::from_system_time(value)
	}
}

impl From<Timestamp> for SystemTime {
	fn from(value: Timestamp) -> Self {
		value.to_system_time()
	}
}

#[derive(Debug)]
pub struct RWX {
	pub read: bool,
	pub write: bool,
	pub execute: bool,
}

impl RWX {
	pub fn from_u8(byte: u8) -> Self {
		Self {
			read: byte & 0b100 != 0,
			write: byte & 0b010 != 0,
			execute: byte & 0b001 != 0,
		}
	}

	pub fn to_u8(&self) -> u8 {
		let mut byte = 0u8;

		if self.read {
			byte |= 0b100;
		}
		if self.write {
			byte |= 0b010;
		}
		if self.execute {
			byte |= 0b001;
		}

		byte
	}
}

/// BitPacked Layout
/// User Permissions (3 bits) | Other Permissions (3 bits) | Hidden Flag (1 bit)
#[derive(Debug)]
pub struct FileMetadata {
	pub user_permissions: RWX,
	pub other_permissions: RWX,
	pub hidden_flag: bool,
}

impl FileMetadata {
	pub fn from_u8(byte: u8) -> Self {
		Self {
			user_permissions: RWX::from_u8((byte >> 5) & 0b111),
			other_permissions: RWX::from_u8((byte >> 2) & 0b111),
			hidden_flag: byte & 0b00000010 != 0,
		}
	}

	pub fn to_u8(&self) -> u8 {
		let mut byte = 0u8;

		byte |= (self.user_permissions.to_u8() & 0b111) << 5;
		byte |= (self.other_permissions.to_u8() & 0b111) << 2;

		if self.hidden_flag {
			byte |= 0b00000010;
		}

		byte
	}

	pub fn modify_permissions(&self, permissions: &mut Permissions) {
		let mut mode = permissions.mode();

		// Clear the user and other permission bits we manage (keep group bits + special bits intact)
		mode &= !0o707;

		mode |= u32::from(self.user_permissions.to_u8() & 0b111) << 6;
		mode |= u32::from(self.other_permissions.to_u8() & 0b111);

		permissions.set_mode(mode);
	}
}

impl From<std::fs::Metadata> for FileMetadata {
	#[cfg(unix)]
	fn from(metadata: std::fs::Metadata) -> Self {
		let mode = metadata.mode();
		Self {
			user_permissions: RWX {
				read: mode & 0o400 != 0,
				write: mode & 0o200 != 0,
				execute: mode & 0o100 != 0,
			},
			other_permissions: RWX {
				read: mode & 0o004 != 0,
				write: mode & 0o002 != 0,
				execute: mode & 0o001 != 0,
			},
			hidden_flag: false, // Unix doesn't have a hidden attribute in metadata
		}
	}

	#[cfg(windows)]
	fn from(metadata: std::fs::Metadata) -> Self {
		use std::os::windows::fs::MetadataExt;

		const FILE_ATTRIBUTE_HIDDEN: u32 = 0x00000002;

		let readonly = metadata.permissions().readonly();
		Self {
			user_permissions: RWX {
				read: true,
				write: !readonly,
				execute: false, // Windows doesn't have an execute permission in metadata
			},
			other_permissions: RWX {
				read: true,
				write: !readonly,
				execute: false, // Windows doesn't have an execute permission in metadata
			},
			hidden_flag: (metadata.file_attributes() & FILE_ATTRIBUTE_HIDDEN) != 0,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn timestamp_roundtrip() {
		let timestamp = Timestamp::now().expect("Failed to get current timestamp");
		let bytes = timestamp.as_bytes();
		let converted_timestamp = Timestamp::from(bytes);
		assert_eq!(timestamp, converted_timestamp);
	}

	#[test]
	fn timestamp_is_le() {
		let timestamp = Timestamp::now().expect("Failed to get current timestamp");
		let bytes = timestamp.as_bytes();
		assert_eq!(bytes, timestamp.0.to_le_bytes());
	}
}
