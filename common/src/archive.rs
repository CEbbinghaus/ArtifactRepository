use std::{
    fmt::{self, Display},
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    num::NonZero,
    path::PathBuf,
    str::FromStr,
};

use anyhow::anyhow;
use futures::AsyncReadExt;
use lzma_rust2::LzmaOptions;
use sha2::{Digest, Sha512};

use crate::{
    object_body::{Index, Object},
    pipe,
    store::Store,
    Hash,
};

pub const HEADER: [u8; 4] = [b'a', b'r', b'x', b'a'];

#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    None = 0,
    Zstd = 2,
    Deflate = 4,
    LZMA2 = 8,
}

impl FromStr for CompressionAlgorithm {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(CompressionAlgorithm::None),
            "deflate" => Ok(CompressionAlgorithm::Deflate),
            "lzma2" => Ok(CompressionAlgorithm::LZMA2),
            "zstd" => Ok(CompressionAlgorithm::Zstd),
            _ => Err(anyhow!("Invalid Compression Type")),
        }
    }
}

impl Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionAlgorithm::None => write!(f, "none"),
            CompressionAlgorithm::Deflate => write!(f, "deflate"),
            CompressionAlgorithm::LZMA2 => write!(f, "lzma2"),
            CompressionAlgorithm::Zstd => write!(f, "zstd"),
        }
    }
}

impl TryFrom<u16> for CompressionAlgorithm {
    type Error = ();

    fn try_from(v: u16) -> Result<Self, Self::Error> {
        match v {
            x if x == CompressionAlgorithm::None as u16 => Ok(CompressionAlgorithm::None),
            x if x == CompressionAlgorithm::Zstd as u16 => Ok(CompressionAlgorithm::Zstd),
            x if x == CompressionAlgorithm::Deflate as u16 => Ok(CompressionAlgorithm::Deflate),
            x if x == CompressionAlgorithm::LZMA2 as u16 => Ok(CompressionAlgorithm::LZMA2),
            _ => Err(()),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionLevel {
    Default,
    Fast,
    Best,
    Exact(i32),
}

impl CompressionLevel {
    pub fn get_compression_level(
        &self,
        algorithm: CompressionAlgorithm,
    ) -> Result<i32, anyhow::Error> {
        // matrix of compression levels for each algorithm. The first dimension is the algorithm, the second dimension is the level (0-3)
        const LEVELS: [[i32; 3]; 4] = [
            [0, 0, 0],  // None
            [3, 6, 15], // Zstd
            [6, 1, 9],  // Deflate
            [5, 1, 9],  // LZMA2
        ];

        let algorithm_index = match algorithm {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Zstd => 1,
            CompressionAlgorithm::Deflate => 3,
            CompressionAlgorithm::LZMA2 => 4,
        };

        let level = match self {
            CompressionLevel::Default => LEVELS[algorithm_index][0],
            CompressionLevel::Fast => LEVELS[algorithm_index][1],
            CompressionLevel::Best => LEVELS[algorithm_index][2],
            CompressionLevel::Exact(i) => *i,
        };

        if !Self::is_valid_for_algorithm(level, algorithm) {
            return Err(anyhow!(
                "Invalid compression level {level} for algorithm {algorithm}"
            ));
        }

        Ok(level)
    }

    fn is_valid_for_algorithm(level: i32, algorithm: CompressionAlgorithm) -> bool {
        match algorithm {
            CompressionAlgorithm::None => true,
            CompressionAlgorithm::Zstd => (-22..=22).contains(&level),
            CompressionAlgorithm::Deflate => (0..=9).contains(&level),
            CompressionAlgorithm::LZMA2 => (0..=9).contains(&level),
        }
    }
}

impl Display for CompressionLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionLevel::Default => write!(f, "default"),
            CompressionLevel::Fast => write!(f, "fast"),
            CompressionLevel::Best => write!(f, "best"),
            CompressionLevel::Exact(i) => write!(f, "exact({i})"),
        }
    }
}

pub struct Archive<T>
where
    T: ArchiveEntryData,
{
    pub header: [u8; 4],
    pub compression: CompressionAlgorithm,
    pub hash: Hash,
    pub index: Index,
    pub body: ArchiveBody<T>,
}

impl<T> Archive<T>
where
    T: ArchiveEntryData,
{
    pub fn to_data(
        self,
        compression_level: CompressionLevel,
        writer: &mut impl Write,
    ) -> anyhow::Result<()> {
        writer.write_all(&HEADER)?;
        writer.write_all(&(self.compression as u16).to_be_bytes())?;
        writer.write_all(&self.hash.hash)?;
        writer.write_all(&self.index.to_data())?;
        writer.write_all(&[0])?;

        let numerical_level = compression_level.get_compression_level(self.compression)?;

        match self.compression {
            CompressionAlgorithm::None => self.body.to_data(writer)?,
            CompressionAlgorithm::Deflate => {
                let mut gz_encoder = flate2::write::DeflateEncoder::new(
                    writer,
                    flate2::Compression::new(numerical_level as u32),
                );
                self.body.to_data(&mut gz_encoder)?;
                gz_encoder.finish()?.flush()?;
            }
            CompressionAlgorithm::LZMA2 => self.body.to_data(
                &mut lzma_rust2::Lzma2WriterMt::new(
                    writer,
                    lzma_rust2::Lzma2Options {
                        lzma_options: LzmaOptions::with_preset(numerical_level as u32),
                        chunk_size: NonZero::new(1024 * 64),
                    },
                    std::thread::available_parallelism().unwrap().get() as u32,
                )?
                .auto_finish(),
            )?,
            CompressionAlgorithm::Zstd => {
                let mut encoder = zstd::stream::write::Encoder::new(writer, numerical_level)?;
                encoder.multithread(
                    std::thread::available_parallelism()
                        .map(|n| n.get() as u32)
                        .unwrap_or(1),
                )?;
                self.body.to_data(&mut encoder)?;
                encoder.finish()?.flush()?;
            }
        }

        Ok(())
    }

    pub fn from_data(reader: &mut impl Read) -> anyhow::Result<Archive<RawEntryData>> {
        let mut reader = BufReader::new(reader);

        let mut header: [u8; 4] = [0; 4];
        reader.read_exact(&mut header)?;
        assert!(header == HEADER);

        let mut compression: [u8; 2] = [0; 2];
        reader.read_exact(&mut compression)?;

        let compression: CompressionAlgorithm = u16::from_be_bytes(compression)
            .try_into()
            .map_err(|_| anyhow!("Invalid Compression"))?;

        let mut hash: [u8; 64] = [0; 64];
        reader.read_exact(&mut hash)?;
        let hash: Hash = hash.into();

        let mut index_bytes = Vec::new();
        let index_bytes_read = reader.read_until(0, &mut index_bytes)?;

        let index = Index::from_data(&index_bytes[..index_bytes_read - 1]);

        let body = match compression {
            CompressionAlgorithm::None => ArchiveBody::<RawEntryData>::from_data(&mut reader)?,
            CompressionAlgorithm::Deflate => ArchiveBody::<RawEntryData>::from_data(
                &mut flate2::read::DeflateDecoder::new(&mut reader),
            )?,
            CompressionAlgorithm::LZMA2 => ArchiveBody::<RawEntryData>::from_data({
                &mut lzma_rust2::Lzma2ReaderMt::new(
                    &mut reader,
                    lzma_rust2::LzmaOptions::DICT_SIZE_DEFAULT,
                    None,
                    std::thread::available_parallelism().unwrap().get() as u32,
                )
            })?,
            CompressionAlgorithm::Zstd => ArchiveBody::<RawEntryData>::from_data(
                &mut zstd::stream::read::Decoder::new(&mut reader)?,
            )?,
        };

        Ok(Archive {
            header: HEADER,
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

pub struct RawEntryData(Vec<u8>);

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

impl<'a> ArchiveEntryData for StoreEntryData {
    fn turn_into_vec(self) -> Vec<u8> {
        let mut object = futures::executor::block_on(self.store.get_object(&self.hash))
            .expect("Object to be available in store");

        let mut data: Vec<u8> = Vec::new();
        futures::executor::block_on(object.read_to_end(&mut data)).expect("Reading to work");

        data
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

        println!("Loading {count} entries");

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

            println!("Read object {hash}");
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
            hasher.write(&data)?;
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
    use std::collections::HashMap;

    fn empty_archive(compression: CompressionAlgorithm) -> Archive<RawEntryData> {
        let zero = Hash::from([0u8; 64]);
        Archive {
            header: HEADER,
            compression,
            hash: zero.clone(),
            index: Index {
                tree: zero,
                timestamp: Utc.timestamp_opt(0, 0).unwrap(),
                metadata: HashMap::new(),
            },
            body: ArchiveBody {
                header: Vec::new(),
                entries: Vec::new(),
            },
        }
    }

    #[test]
    fn zstd_archive_round_trip() {
        let mut bytes = Vec::new();
        empty_archive(CompressionAlgorithm::Zstd)
            .to_data(CompressionLevel::Default, &mut bytes)
            .expect("encode");

        let decoded = Archive::<RawEntryData>::from_data(&mut bytes.as_slice()).expect("decode");

        assert!(matches!(decoded.compression, CompressionAlgorithm::Zstd));
        assert!(decoded.body.header.is_empty());
        assert!(decoded.body.entries.is_empty());
    }
}
