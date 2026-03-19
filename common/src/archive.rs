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
            _ => Err(anyhow!("Invalid Compression Type")),
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
    pub fn to_data<'a>(self, writer: &'a mut impl Write) -> anyhow::Result<()> {
        writer.write_all(&HEADER)?;
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

    pub fn from_data<'a>(reader: &'a mut impl Read) -> anyhow::Result<Archive<RawEntryData>> {
        let mut reader = BufReader::new(reader);

        let mut header: [u8; 4] = [0; 4];
        reader.read_exact(&mut header)?;
        assert!(header == HEADER);

        let mut compression: [u8; 2] = [0; 2];
        reader.read_exact(&mut compression)?;

        let compression: Compression = u16::from_be_bytes(compression)
            .try_into()
            .map_err(|_| anyhow!("Invalid Compression"))?;

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

impl<'a> ArchiveEntryData for StoreEntryData {
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
    fn to_data<'a>(self, writer: &'a mut impl Write) -> anyhow::Result<()> {
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

    fn from_data<'a>(reader: &'a mut impl Read) -> anyhow::Result<ArchiveBody<RawEntryData>> {
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
