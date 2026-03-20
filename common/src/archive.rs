use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    num::NonZero,
    path::PathBuf,
    str::FromStr,
};

use anyhow::{anyhow, Error};
use futures::AsyncReadExt;
use sha2::{Digest, Sha512};

use crate::{
    object_body::{Index, Object},
    pipe,
    store::Store,
    Hash,
};

pub const HEADER: [u8; 4] = [b'a', b'r', b'x', b'a'];
pub const SUPPLEMENTAL_HEADER: [u8; 4] = [b'a', b'r', b'x', b's'];

#[repr(u16)]
#[derive(Clone, Copy)]
pub enum Compression {
    None = 0,
    Gzip = 4,
    Deflate = 8,
    LZMA2 = 16,
    Zstd = 32,
}

impl FromStr for Compression {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(Compression::None),
            "gzip" => Ok(Compression::Gzip),
            "deflate" => Ok(Compression::Deflate),
            "lzma2" => Ok(Compression::LZMA2),
            "zstd" => Ok(Compression::Zstd),
            _ => Err(anyhow!("invalid compression type")),
        }
    }
}

impl TryFrom<u16> for Compression {
    type Error = ();

    fn try_from(v: u16) -> Result<Self, Self::Error> {
        match v {
            x if x == Compression::None as u16 => Ok(Compression::None),
            x if x == Compression::Gzip as u16 => Ok(Compression::Gzip),
            x if x == Compression::Deflate as u16 => Ok(Compression::Deflate),
            x if x == Compression::LZMA2 as u16 => Ok(Compression::LZMA2),
            x if x == Compression::Zstd as u16 => Ok(Compression::Zstd),
            _ => Err(()),
        }
    }
}

pub struct Archive<T>
where
    T: ArchiveEntryData,
{
    pub header: [u8; 4],
    pub compression: Compression,
    pub hash: Hash,
    pub index: Index,
    pub body: ArchiveBody<T>,
}

impl<T> Archive<T>
where
    T: ArchiveEntryData,
{
    pub fn is_supplemental(&self) -> bool {
        self.header == SUPPLEMENTAL_HEADER
    }

    pub fn to_data(self, writer: &mut impl Write) -> anyhow::Result<()> {
        writer.write_all(&self.header)?;
        writer.write_all(&(self.compression as u16).to_be_bytes())?;
        writer.write_all(&self.hash.hash)?;
        writer.write_all(&self.index.to_data())?;
        writer.write_all(&[0])?;

        match self.compression {
            Compression::None => self.body.to_data(writer)?,
            Compression::Gzip => {
                let mut gz_encoder =
                    flate2::write::GzEncoder::new(writer, flate2::Compression::default());
                self.body.to_data(&mut gz_encoder)?;
                gz_encoder.finish()?.flush()?;
            }
            Compression::Deflate => {
                let mut gz_encoder =
                    flate2::write::DeflateEncoder::new(writer, flate2::Compression::default());
                self.body.to_data(&mut gz_encoder)?;
                gz_encoder.finish()?.flush()?;
            }
            Compression::Zstd => {
                let mut encoder = zstd::stream::write::Encoder::new(writer, 3)?;
                encoder.multithread(std::thread::available_parallelism().map(|n| n.get() as u32).unwrap_or(1))?;
                self.body.to_data(&mut encoder)?;
                encoder.finish()?.flush()?;
            }
            Compression::LZMA2 => self.body.to_data(
                &mut lzma_rust2::Lzma2WriterMt::new(
                    writer,
                    lzma_rust2::Lzma2Options {
                        lzma_options: Default::default(),
                        chunk_size: NonZero::new(1024 * 64),
                    },
                    std::thread::available_parallelism().unwrap().get() as u32,
                )?
                .auto_finish(),
            )?,
        }

        Ok(())
    }

    pub fn from_data(reader: &mut impl Read) -> anyhow::Result<Archive<RawEntryData>> {
        let mut reader = BufReader::new(reader);

        let mut header: [u8; 4] = [0; 4];
        reader.read_exact(&mut header)?;
        anyhow::ensure!(
            header == HEADER || header == SUPPLEMENTAL_HEADER,
            "invalid archive magic: expected 'arxa' or 'arxs', got {:?}",
            header
        );

        let mut compression: [u8; 2] = [0; 2];
        reader.read_exact(&mut compression)?;

        let compression: Compression = u16::from_be_bytes(compression)
            .try_into()
            .map_err(|_| anyhow!("invalid compression"))?;

        let mut hash: [u8; 64] = [0; 64];
        reader.read_exact(&mut hash)?;
        let hash: Hash = hash.into();

        let mut index_bytes = Vec::new();
        let index_bytes_read = reader.read_until(0, &mut index_bytes)?;

        let index = Index::from_data(&index_bytes[..index_bytes_read - 1])?;

        let body = match compression {
            Compression::None => ArchiveBody::<RawEntryData>::from_data(&mut reader)?,
            Compression::Gzip => ArchiveBody::<RawEntryData>::from_data(
                &mut flate2::read::GzDecoder::new(&mut reader),
            )?,
            Compression::Deflate => ArchiveBody::<RawEntryData>::from_data(
                &mut flate2::read::DeflateDecoder::new(&mut reader),
            )?,
            Compression::Zstd => ArchiveBody::<RawEntryData>::from_data(
                &mut zstd::stream::read::Decoder::new(&mut reader)?,
            )?,
            Compression::LZMA2 => ArchiveBody::<RawEntryData>::from_data({
                &mut lzma_rust2::Lzma2ReaderMt::new(
                    &mut reader,
                    lzma_rust2::LzmaOptions::DICT_SIZE_DEFAULT,
                    None,
                    std::thread::available_parallelism().unwrap().get() as u32,
                )
            })?,
        };

        Ok(Archive {
            header,
            compression,
            hash,
            index,
            body,
        })
    }
}

// /// Create a new `Body` from a [`Stream`].
// ///
// /// [`Stream`]: https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html
// pub fn from_stream<S>(stream: S) -> Self
// where
//     S: TryStream + Send + 'static,
//     S::Ok: Into<Bytes>,
//     S::Error: Into<BoxError>,
// {
//     Self::new(StreamBody {
//         stream: SyncWrapper::new(stream),
//     })
// }

// impl<T> Stream for Archive<T>
// where
//     T: ArchiveEntryData + Unpin,
// {
//     type Item = Result<Bytes, Error>;

//     fn poll_next(
//         self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Option<Self::Item>> {

//     }
// }

pub struct ArchiveHeaderEntry {
    pub hash: Hash,
    pub index: u64,
    pub length: u64,
}

pub trait ArchiveEntryData {
    fn turn_into_vec(self) -> Vec<u8>;
}

pub struct RawEntryData(pub Vec<u8>);

impl ArchiveEntryData for RawEntryData {
    fn turn_into_vec(self) -> Vec<u8> {
        self.0
    }
}
pub struct ReaderEntryData<T>(T)
where
    T: Read;

impl<T> ReaderEntryData<T>
where
    T: Read,
{
    pub fn new(reader: T) -> Self {
        ReaderEntryData(reader)
    }
}

impl<T> ArchiveEntryData for ReaderEntryData<T>
where
    T: Read,
{
    fn turn_into_vec(mut self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();
        self.0.read_to_end(&mut data).expect("Reading to work");

        data
    }
}

pub struct FileEntryData(pub PathBuf);

impl ArchiveEntryData for FileEntryData {
    fn turn_into_vec(self) -> Vec<u8> {
        let file = File::open(self.0).expect("File to be avaliable for read");
        let mut reader = BufReader::new(file);
        let mut data = Vec::new();
        pipe(&mut reader, &mut data).expect("reading to work");
        data
    }
}

pub struct StoreEntryData {
    pub store: Store,
    pub hash: Hash,
}

impl ArchiveEntryData for StoreEntryData {
    fn turn_into_vec(self) -> Vec<u8> {
        // Use block_in_place to avoid deadlocking the tokio runtime
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut object = self.store.get_object(&self.hash).await
                    .expect("Object to be available in store");

                let mut data: Vec<u8> = Vec::new();
                object.read_to_end(&mut data).await.expect("Reading to work");
                data
            })
        })
    }
}

pub struct ArchiveBody<T>
where
    T: ArchiveEntryData,
{
    pub header: Vec<ArchiveHeaderEntry>,
    pub entries: Vec<T>,
}

impl<T> ArchiveBody<T>
where
    T: ArchiveEntryData,
{
    #[allow(clippy::wrong_self_convention)]
    fn to_data(self, writer: &mut impl Write) -> anyhow::Result<()> {
        writer.write_all(&(self.header.len() as u64).to_be_bytes())?;
        for entry in &self.header {
            writer.write_all(&entry.hash.hash)?;
            writer.write_all(&entry.index.to_be_bytes())?;
            writer.write_all(&entry.length.to_be_bytes())?;
        }

        for entry in self.entries {
            writer.write_all(&entry.turn_into_vec())?;
        }

        writer.flush()?;

        Ok(())
    }

    fn from_data(reader: &mut impl Read) -> anyhow::Result<ArchiveBody<RawEntryData>> {
        let mut long: [u8; 8] = [0; 8];
        reader.read_exact(&mut long)?;
        let count = u64::from_be_bytes(long);

        if count == 0 {
            return Ok(ArchiveBody {
                header: Vec::new(),
                entries: Vec::new(),
            });
        }

        let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::with_capacity(count as usize);
        let mut counter = 0;
        loop {
            if counter >= count {
                break;
            }

            let mut hash: [u8; 64] = [0; 64];
            reader.read_exact(&mut hash)?;
            let hash: Hash = hash.into();

            reader.read_exact(&mut long)?;
            let index = u64::from_be_bytes(long);

            reader.read_exact(&mut long)?;
            let length = u64::from_be_bytes(long);


            header_entries.push(ArchiveHeaderEntry {
                hash,
                index,
                length,
            });
            counter += 1;
        }

        let mut counter: u64 = 0;

        header_entries.sort_by(|a, b| a.index.cmp(&b.index));
        assert!(header_entries[0].index == 0);

        let mut entries: Vec<RawEntryData> = Vec::with_capacity(header_entries.len());
        for entry in &header_entries {
            assert!(entry.index == counter);

            let amount = entry.length;
            let mut data: Vec<u8> = vec![0; amount as usize];
            reader.read_exact(&mut data[..])?;

            let mut hasher = Sha512::new();
            hasher.write_all(&data)?;
            assert!(Hash::from(hasher) == entry.hash);

            entries.push(RawEntryData(data.to_vec()));

            counter += amount;
        }

        Ok(ArchiveBody {
            header: header_entries,
            entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use sha2::Digest;
    use std::collections::HashMap;
    use std::io::Cursor;
    use chrono::DateTime;
    use crate::compute_hash;
    use crate::object_body::{Index, Tree, TreeEntry};
    use crate::Mode;

    fn make_hash(fill: u8) -> Hash {
        Hash::from([fill; 64])
    }

    fn compute_entry_hash(data: &[u8]) -> Hash {
        let mut hasher = Sha512::new();
        hasher.write(data).unwrap();
        Hash::from(hasher)
    }

    fn make_test_index() -> Index {
        let ts = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        Index {
            tree: make_hash(0xaa),
            timestamp: ts,
            metadata: HashMap::new(),
        }
    }

    fn make_archive_with_entries(
        compression: Compression,
        entries_data: Vec<Vec<u8>>,
    ) -> Archive<RawEntryData> {
        let mut header_entries = Vec::new();
        let mut raw_entries = Vec::new();
        let mut offset: u64 = 0;

        for data in &entries_data {
            let hash = compute_entry_hash(data);
            header_entries.push(ArchiveHeaderEntry {
                hash,
                index: offset,
                length: data.len() as u64,
            });
            raw_entries.push(RawEntryData(data.clone()));
            offset += data.len() as u64;
        }

        Archive {
            header: HEADER,
            compression,
            hash: make_hash(0xbb),
            index: make_test_index(),
            body: ArchiveBody {
                header: header_entries,
                entries: raw_entries,
            },
        }
    }

    fn round_trip(archive: Archive<RawEntryData>) -> Archive<RawEntryData> {
        let mut buf = Vec::new();
        archive.to_data(&mut buf).unwrap();
        let mut cursor = Cursor::new(buf);
        Archive::<RawEntryData>::from_data(&mut cursor).unwrap()
    }

    // --- Compression FromStr tests ---

    #[test]
    fn compression_from_str_all_variants() {
        assert!(matches!("none".parse::<Compression>().unwrap(), Compression::None));
        assert!(matches!("gzip".parse::<Compression>().unwrap(), Compression::Gzip));
        assert!(matches!("deflate".parse::<Compression>().unwrap(), Compression::Deflate));
        assert!(matches!("lzma2".parse::<Compression>().unwrap(), Compression::LZMA2));
        assert!(matches!("zstd".parse::<Compression>().unwrap(), Compression::Zstd));
    }

    #[test]
    fn compression_from_str_unknown() {
        assert!("brotli".parse::<Compression>().is_err());
    }

    #[test]
    fn compression_try_from_u16_all_discriminants() {
        assert!(matches!(Compression::try_from(0u16).unwrap(), Compression::None));
        assert!(matches!(Compression::try_from(4u16).unwrap(), Compression::Gzip));
        assert!(matches!(Compression::try_from(8u16).unwrap(), Compression::Deflate));
        assert!(matches!(Compression::try_from(16u16).unwrap(), Compression::LZMA2));
        assert!(matches!(Compression::try_from(32u16).unwrap(), Compression::Zstd));
    }

    #[test]
    fn compression_try_from_u16_rejects_unknown() {
        assert!(Compression::try_from(1u16).is_err());
        assert!(Compression::try_from(99u16).is_err());
        assert!(Compression::try_from(255u16).is_err());
    }

    // --- Archive round-trip tests ---

    #[test]
    fn archive_round_trip_none_single_entry() {
        let data = b"hello world".to_vec();
        let archive = make_archive_with_entries(Compression::None, vec![data.clone()]);
        let recovered = round_trip(archive);

        assert_eq!(recovered.header, HEADER);
        assert_eq!(recovered.body.entries.len(), 1);
        assert_eq!(recovered.body.entries[0].0, data);
    }

    #[test]
    fn archive_round_trip_gzip() {
        let data = b"compressed with gzip".to_vec();
        let archive = make_archive_with_entries(Compression::Gzip, vec![data.clone()]);
        let recovered = round_trip(archive);

        assert_eq!(recovered.body.entries.len(), 1);
        assert_eq!(recovered.body.entries[0].0, data);
    }

    #[test]
    fn archive_round_trip_zstd() {
        let data = b"compressed with zstd".to_vec();
        let archive = make_archive_with_entries(Compression::Zstd, vec![data.clone()]);
        let recovered = round_trip(archive);

        assert_eq!(recovered.body.entries.len(), 1);
        assert_eq!(recovered.body.entries[0].0, data);
    }

    #[test]
    fn archive_round_trip_empty_body() {
        let archive = make_archive_with_entries(Compression::None, vec![]);
        let recovered = round_trip(archive);

        assert_eq!(recovered.body.entries.len(), 0);
        assert_eq!(recovered.body.header.len(), 0);
    }

    #[test]
    fn archive_round_trip_multiple_entries() {
        let entries = vec![
            b"first entry data".to_vec(),
            b"second entry data".to_vec(),
            b"third entry data!".to_vec(),
        ];
        let archive = make_archive_with_entries(Compression::None, entries.clone());
        let recovered = round_trip(archive);

        assert_eq!(recovered.body.entries.len(), 3);
        assert_eq!(recovered.body.entries[0].0, entries[0]);
        assert_eq!(recovered.body.entries[1].0, entries[1]);
        assert_eq!(recovered.body.entries[2].0, entries[2]);
    }

    #[test]
    fn archive_magic_bytes_verified() {
        let archive = make_archive_with_entries(Compression::None, vec![b"data".to_vec()]);
        let mut buf = Vec::new();
        archive.to_data(&mut buf).unwrap();
        // Corrupt magic bytes
        buf[0] = b'X';
        let mut cursor = Cursor::new(buf);
        let result = Archive::<RawEntryData>::from_data(&mut cursor);
        assert!(result.is_err());
    }

    #[test]
    fn archive_hash_preserved() {
        let archive = make_archive_with_entries(Compression::None, vec![b"data".to_vec()]);
        let expected_hash = make_hash(0xbb);
        let recovered = round_trip(archive);
        assert_eq!(recovered.hash, expected_hash);
    }

    #[test]
    fn archive_index_preserved() {
        let archive = make_archive_with_entries(Compression::None, vec![b"data".to_vec()]);
        let recovered = round_trip(archive);
        assert_eq!(recovered.index.tree, make_hash(0xaa));
        let expected_ts = Utc.with_ymd_and_hms(2024, 6, 15, 10, 30, 0).unwrap();
        assert_eq!(recovered.index.timestamp, expected_ts);
    }

    #[test]
    fn archive_entry_data_verified_against_hashes() {
        let data = b"verify me".to_vec();
        let archive = make_archive_with_entries(Compression::None, vec![data.clone()]);
        let recovered = round_trip(archive);

        let expected_hash = compute_entry_hash(&data);
        assert_eq!(recovered.body.header[0].hash, expected_hash);
    }

    // --- Binary compatibility test ---

    #[test]
    fn archive_binary_compatibility() {
        // Build a known archive with fixed values
        let entry_data = b"test";
        let entry_hash = compute_entry_hash(entry_data);
        let archive_hash = make_hash(0x01);
        let ts = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let index = Index {
            tree: make_hash(0x02),
            timestamp: ts,
            metadata: HashMap::new(),
        };

        let archive = Archive {
            header: HEADER,
            compression: Compression::None,
            hash: archive_hash.clone(),
            index,
            body: ArchiveBody {
                header: vec![ArchiveHeaderEntry {
                    hash: entry_hash.clone(),
                    index: 0,
                    length: entry_data.len() as u64,
                }],
                entries: vec![RawEntryData(entry_data.to_vec())],
            },
        };

        let mut buf = Vec::new();
        archive.to_data(&mut buf).unwrap();

        // Verify magic bytes at offset 0-3
        assert_eq!(&buf[0..4], b"arxa", "Magic bytes must be 'arxa'");

        // Verify compression at offset 4-5 (None = 0)
        assert_eq!(&buf[4..6], &[0u8, 0u8], "Compression::None = 0x0000");

        // Verify hash at offset 6-69 (64 bytes of 0x01)
        assert_eq!(&buf[6..70], &[0x01u8; 64], "Archive hash at bytes 6-69");

        // After hash: index data (variable length), then null byte, then body
        // Find the null separator after index
        let index_start = 70;
        let null_pos = buf[index_start..].iter().position(|&b| b == 0).unwrap() + index_start;

        // Verify the index text contains expected fields
        let index_text = std::str::from_utf8(&buf[index_start..null_pos]).unwrap();
        assert!(index_text.contains("tree:"), "Index must contain tree field");
        assert!(index_text.contains("timestamp:"), "Index must contain timestamp field");

        // Body starts after null byte
        let body_start = null_pos + 1;

        // Entry count: 8 bytes big-endian = 1
        assert_eq!(
            u64::from_be_bytes(buf[body_start..body_start + 8].try_into().unwrap()),
            1,
            "Entry count must be 1"
        );

        // Header entry: hash (64) + index (8) + length (8) = 80 bytes
        let header_entry_start = body_start + 8;
        let header_entry_hash = &buf[header_entry_start..header_entry_start + 64];
        assert_eq!(header_entry_hash, &entry_hash.hash, "Entry hash in header");

        let entry_index = u64::from_be_bytes(
            buf[header_entry_start + 64..header_entry_start + 72].try_into().unwrap(),
        );
        assert_eq!(entry_index, 0, "Entry index must be 0");

        let entry_length = u64::from_be_bytes(
            buf[header_entry_start + 72..header_entry_start + 80].try_into().unwrap(),
        );
        assert_eq!(entry_length, 4, "Entry length must be 4");

        // Entry data follows
        let data_start = header_entry_start + 80;
        assert_eq!(&buf[data_start..data_start + 4], b"test", "Entry data must be 'test'");

        // Now deserialize the exact bytes and verify round-trip
        let mut cursor = Cursor::new(buf);
        let recovered = Archive::<RawEntryData>::from_data(&mut cursor).unwrap();
        assert_eq!(recovered.header, HEADER);
        assert_eq!(recovered.hash, make_hash(0x01));
        assert_eq!(recovered.index.tree, make_hash(0x02));
        assert_eq!(recovered.body.entries.len(), 1);
        assert_eq!(recovered.body.entries[0].0, b"test");
    }

    const KNOWN_ARCHIVE_HEX: &str = "\
        617278610000\
        c13812d5a5db2775e58935686f7e9976171a6d0263fb9ee1314880c9c1a84b79\
        cfb66358a2988c3506fc980e89d7b41b09b7d649d55ef1f0374c4564b36a02dc\
        747265653a2032346436396661643937376233323933666465623963313761386464643836\
        613033626332636131626531303665626661623533323839633139656534613036623233313630\
        616261353235363836393361373164386335383538393565626662323132346161663437613934\
        373134656165323832666236633433326336370a74696d657374616d703a20323032352d30312d\
        30315430303a30303a30302b30303a30300a0a00\
        0000000000000002\
        24d69fad977b3293fdeb9c17a8ddd86a03bc2ca1be106ebfab53289c19ee4a06\
        b23160aba52568693a71d8c585895ebfb2124aaf47a94714eae282fb6c432c67\
        0000000000000000\
        0000000000000058\
        bcf8802ae6e118cea3197843ac168b21b766f06276324ecd2cfcf77546918dbb\
        8877bb59719e6e33a0d458baf66374b39f475ceb070f7e123e4403b8f8a8094a\
        0000000000000058\
        0000000000000010\
        747265652038300031303036343420746573742e74787400\
        bcf8802ae6e118cea3197843ac168b21b766f06276324ecd2cfcf77546918dbb\
        8877bb59719e6e33a0d458baf66374b39f475ceb070f7e123e4403b8f8a8094a\
        626c6f62203900746573742064617461";

    const BLOB_HASH_HEX: &str = "\
        bcf8802ae6e118cea3197843ac168b21b766f06276324ecd2cfcf77546918dbb\
        8877bb59719e6e33a0d458baf66374b39f475ceb070f7e123e4403b8f8a8094a";
    const TREE_HASH_HEX: &str = "\
        24d69fad977b3293fdeb9c17a8ddd86a03bc2ca1be106ebfab53289c19ee4a06\
        b23160aba52568693a71d8c585895ebfb2124aaf47a94714eae282fb6c432c67";
    const INDEX_HASH_HEX: &str = "\
        c13812d5a5db2775e58935686f7e9976171a6d0263fb9ee1314880c9c1a84b79\
        cfb66358a2988c3506fc980e89d7b41b09b7d649d55ef1f0374c4564b36a02dc";

    fn make_known_tree() -> Tree {
        let blob_hash = Hash::try_from(BLOB_HASH_HEX).unwrap();
        Tree {
            contents: vec![TreeEntry {
                mode: Mode::Normal,
                path: "test.txt".to_string(),
                hash: blob_hash,
            }],
        }
    }

    fn make_known_index() -> Index {
        let tree_hash = Hash::try_from(TREE_HASH_HEX).unwrap();
        let ts: DateTime<Utc> = DateTime::parse_from_rfc3339("2025-01-01T00:00:00+00:00")
            .unwrap()
            .into();
        Index {
            tree: tree_hash,
            timestamp: ts,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn synthetic_archive_deserialize_known_bytes() {
        let archive_bytes = hex::decode(KNOWN_ARCHIVE_HEX).unwrap();
        let archive =
            Archive::<RawEntryData>::from_data(&mut Cursor::new(archive_bytes)).unwrap();

        // Magic header
        assert_eq!(archive.header, HEADER);

        // Compression
        assert!(matches!(archive.compression, Compression::None));

        // Index hash
        assert_eq!(archive.hash, Hash::try_from(INDEX_HASH_HEX).unwrap());

        // Index tree hash
        assert_eq!(archive.index.tree, Hash::try_from(TREE_HASH_HEX).unwrap());

        // Index timestamp
        let expected_ts: DateTime<Utc> =
            DateTime::parse_from_rfc3339("2025-01-01T00:00:00+00:00")
                .unwrap()
                .into();
        assert_eq!(archive.index.timestamp, expected_ts);

        // Body has 2 entries
        assert_eq!(archive.body.entries.len(), 2);

        // Blob entry (second) - strip "blob 9\0" prefix
        let blob_entry = &archive.body.entries[1].0;
        let blob_prefix = b"blob 9\0";
        assert_eq!(&blob_entry[..blob_prefix.len()], blob_prefix);
        assert_eq!(&blob_entry[blob_prefix.len()..], b"test data");

        // Tree entry (first) - strip "tree 80\0" prefix and parse
        let tree_entry = &archive.body.entries[0].0;
        let tree_prefix = b"tree 80\0";
        assert_eq!(&tree_entry[..tree_prefix.len()], tree_prefix);
        let tree = Tree::from_data(&tree_entry[tree_prefix.len()..]).unwrap();
        assert_eq!(tree.contents.len(), 1);
        assert_eq!(tree.contents[0].path, "test.txt");
    }

    #[test]
    fn synthetic_archive_serialize_matches_expected_bytes() {
        let blob_data = b"test data";
        let blob_hash = compute_hash("blob", blob_data);
        let tree = make_known_tree();
        let tree_data = tree.to_data();
        let tree_hash = compute_hash("tree", &tree_data);
        let index = make_known_index();
        let index_data = index.to_data();
        let index_hash = compute_hash("indx", &index_data);

        // Build prefixed entry data (matching object store format)
        let mut tree_entry_data = format!("tree {}\0", tree_data.len()).into_bytes();
        tree_entry_data.extend_from_slice(&tree_data);

        let mut blob_entry_data = format!("blob {}\0", blob_data.len()).into_bytes();
        blob_entry_data.extend_from_slice(blob_data);

        let tree_entry_hash = compute_entry_hash(&tree_entry_data);
        let blob_entry_hash = compute_entry_hash(&blob_entry_data);

        // Verify entry hashes match compute_hash (the prefix format is identical)
        assert_eq!(tree_entry_hash, tree_hash);
        assert_eq!(blob_entry_hash, blob_hash);

        let archive = Archive {
            header: HEADER,
            compression: Compression::None,
            hash: index_hash,
            index: make_known_index(),
            body: ArchiveBody {
                header: vec![
                    ArchiveHeaderEntry {
                        hash: tree_entry_hash,
                        index: 0,
                        length: tree_entry_data.len() as u64,
                    },
                    ArchiveHeaderEntry {
                        hash: blob_entry_hash,
                        index: tree_entry_data.len() as u64,
                        length: blob_entry_data.len() as u64,
                    },
                ],
                entries: vec![
                    RawEntryData(tree_entry_data),
                    RawEntryData(blob_entry_data),
                ],
            },
        };

        let mut output = Vec::new();
        archive.to_data(&mut output).unwrap();

        let expected = hex::decode(KNOWN_ARCHIVE_HEX).unwrap();
        assert_eq!(output, expected, "Serialized archive must match known bytes exactly");
    }

    #[test]
    fn synthetic_archive_object_hashes_stable() {
        // Blob hash
        let blob_hash = compute_hash("blob", b"test data");
        assert_eq!(blob_hash, Hash::try_from(BLOB_HASH_HEX).unwrap());

        // Tree hash
        let tree = make_known_tree();
        let tree_data = tree.to_data();
        let tree_hash = compute_hash("tree", &tree_data);
        assert_eq!(tree_hash, Hash::try_from(TREE_HASH_HEX).unwrap());

        // Index hash
        let index = make_known_index();
        let index_data = index.to_data();
        let index_hash = compute_hash("indx", &index_data);
        assert_eq!(index_hash, Hash::try_from(INDEX_HASH_HEX).unwrap());
    }

    #[test]
    fn synthetic_archive_tree_binary_format_stable() {
        let tree = make_known_tree();
        let data = tree.to_data();

        assert_eq!(data.len(), 80);
        assert_eq!(&data[0..6], b"100644", "mode");
        assert_eq!(data[6], 0x20, "space separator");
        assert_eq!(&data[7..15], b"test.txt", "filename");
        assert_eq!(data[15], 0x00, "null separator");

        let expected_blob_hash = hex::decode(BLOB_HASH_HEX).unwrap();
        assert_eq!(&data[16..80], &expected_blob_hash[..], "raw 64-byte blob hash");
    }

    #[test]
    fn synthetic_archive_index_text_format_stable() {
        let index = make_known_index();
        let data = index.to_data();

        assert_eq!(data.len(), 173);
        assert!(data.starts_with(b"tree: "), "must start with 'tree: '");

        let text = std::str::from_utf8(&data).unwrap();
        let tree_hex_in_output = &text[6..6 + 128];
        assert_eq!(tree_hex_in_output, TREE_HASH_HEX);

        assert!(
            text.contains("\ntimestamp: 2025-01-01T00:00:00+00:00\n"),
            "must contain timestamp line"
        );
        assert!(text.ends_with("\n\n"), "must end with double newline");
    }

    // --- Supplemental archive tests ---

    fn make_supplemental_archive(entries_data: Vec<Vec<u8>>) -> Archive<RawEntryData> {
        let mut archive = make_archive_with_entries(Compression::None, entries_data);
        archive.header = SUPPLEMENTAL_HEADER;
        archive
    }

    #[test]
    fn supplemental_archive_round_trip() {
        let data = b"supplemental data".to_vec();
        let archive = make_supplemental_archive(vec![data.clone()]);
        let recovered = round_trip(archive);

        assert!(recovered.is_supplemental());
        assert_eq!(recovered.header, SUPPLEMENTAL_HEADER);
        assert_eq!(recovered.body.entries.len(), 1);
        assert_eq!(recovered.body.entries[0].0, data);
    }

    #[test]
    fn regular_archive_is_not_supplemental() {
        let archive = make_archive_with_entries(Compression::None, vec![b"data".to_vec()]);
        let recovered = round_trip(archive);

        assert!(!recovered.is_supplemental());
        assert_eq!(recovered.header, HEADER);
    }

    #[test]
    fn invalid_magic_rejected() {
        let mut buf = Vec::new();
        let archive = make_archive_with_entries(Compression::None, vec![b"data".to_vec()]);
        archive.to_data(&mut buf).unwrap();
        // Overwrite magic with zeroes
        buf[0] = 0;
        buf[1] = 0;
        buf[2] = 0;
        buf[3] = 0;
        let mut cursor = Cursor::new(buf);
        let result = Archive::<RawEntryData>::from_data(&mut cursor);
        assert!(result.is_err());
    }
}
