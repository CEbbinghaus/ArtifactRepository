use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    num::NonZero,
    ops::Deref,
    path::PathBuf,
    str::FromStr,
};

use anyhow::{anyhow, Error};
use async_compression::futures::*;
use async_stream::stream;
use auto_enums::auto_enum;
use bytes::Bytes;
use futures::{future, AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, Stream, TryStream};
use opendal::FuturesAsyncReader;
use sha2::{Digest, Sha512};
use tokio::pin;

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
    Gzip = 1,
    Deflate = 2,
    Lzma = 3,
    Brotli = 4,
    Bzip2 = 5,
    Lz4 = 6,
    Xz = 7,
    Zlib = 8,
}

impl FromStr for Compression {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(Compression::None),
            "gzip" => Ok(Compression::Gzip),
            "deflate" => Ok(Compression::Deflate),
            "lzma" => Ok(Compression::Lzma),
            "brotli" => Ok(Compression::Brotli),
            "bzip2" => Ok(Compression::Bzip2),
            "lz4" => Ok(Compression::Lz4),
            "xz" => Ok(Compression::Xz),
            "zlib" => Ok(Compression::Zlib),
            unknown => Err(anyhow!("Unknown compression type {unknown}")),
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
            x if x == Compression::Lzma as u16 => Ok(Compression::Lzma),
            x if x == Compression::Brotli as u16 => Ok(Compression::Brotli),
            x if x == Compression::Bzip2 as u16 => Ok(Compression::Bzip2),
            x if x == Compression::Lz4 as u16 => Ok(Compression::Lz4),
            x if x == Compression::Xz as u16 => Ok(Compression::Xz),
            x if x == Compression::Zlib as u16 => Ok(Compression::Zlib),
            _ => Err(()),
        }
    }
}

impl Compression {
    fn get_writer_for_compression<'a>(
        &self,
        writer: &'a mut (impl AsyncWrite + Unpin),
    ) -> Box<dyn AsyncWrite + 'a> {
        match self {
            Compression::Gzip => Box::new(write::GzipEncoder::new(writer)),
            Compression::Deflate => Box::new(write::DeflateEncoder::new(writer)),
            Compression::Lzma => Box::new(write::LzmaEncoder::new(writer)),
            Compression::Brotli => Box::new(write::BrotliEncoder::new(writer)),
            Compression::Bzip2 => Box::new(write::BzEncoder::new(writer)),
            Compression::Lz4 => Box::new(write::Lz4Encoder::new(writer)),
            Compression::Xz => Box::new(write::XzEncoder::new(writer)),
            Compression::Zlib => Box::new(write::ZlibEncoder::new(writer)),
            Compression::None => unimplemented!(),
        }
    }

    fn get_reader_for_compression<'a>(
        &self,
        reader: &'a mut (impl AsyncBufRead + Unpin),
    ) -> Box<dyn AsyncRead + 'a> {
        match self {
            Compression::Gzip => Box::new(bufread::GzipEncoder::new(reader)),
            Compression::Deflate => Box::new(bufread::DeflateEncoder::new(reader)),
            Compression::Lzma => Box::new(bufread::LzmaEncoder::new(reader)),
            Compression::Brotli => Box::new(bufread::BrotliEncoder::new(reader)),
            Compression::Bzip2 => Box::new(bufread::BzEncoder::new(reader)),
            Compression::Lz4 => Box::new(bufread::Lz4Encoder::new(reader)),
            Compression::Xz => Box::new(bufread::XzEncoder::new(reader)),
            Compression::Zlib => Box::new(bufread::ZlibEncoder::new(reader)),
            Compression::None => unimplemented!(),
        }
    }
}

pub struct Archive<T>
where
    T: ArchiveEntryData + Unpin,
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
    fn get_header_bytes(&self) -> anyhow::Result<Bytes> {
        let mut buffer = Vec::new();
        buffer.write_all(&HEADER)?;
        buffer.write_all(&(self.compression as u16).to_be_bytes())?;
        buffer.write_all(&self.hash.hash)?;
        buffer.write_all(&self.index.to_data())?;
        buffer.write_all(&[0])?;
        Ok(Bytes::from(buffer))
    }

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
            Compression::Lzma => self.body.to_data(
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
            _ => unimplemented!(),
        }

        Ok(())
    }

    pub fn into_stream<'a>(self) -> impl Stream<Item = Result<Bytes, Error>> {
        stream! {
            yield self.get_header_bytes();

            let mut body = Box::pin(&mut self.body);
            let body = self.compression.get_reader_for_compression(&mut body);
			



        }
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

        let index = Index::from_data(&index_bytes[..index_bytes_read - 1]);

        let body = match compression {
            Compression::None => ArchiveBody::<RawEntryData>::from_data(&mut reader)?,
            Compression::Gzip => ArchiveBody::<RawEntryData>::from_data(
                &mut flate2::read::GzDecoder::new(&mut reader),
            )?,
            Compression::Deflate => ArchiveBody::<RawEntryData>::from_data(
                &mut flate2::read::DeflateDecoder::new(&mut reader),
            )?,
            Compression::Lzma => ArchiveBody::<RawEntryData>::from_data({
                &mut lzma_rust2::Lzma2ReaderMt::new(
                    &mut reader,
                    lzma_rust2::LzmaOptions::DICT_SIZE_DEFAULT,
                    None,
                    std::thread::available_parallelism().unwrap().get() as u32,
                )
            })?,
            _ => unimplemented!(),
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

impl<T> Stream for Archive<T>
where
    T: ArchiveEntryData + Unpin,
{
    type Item = Result<Bytes, Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll::*;
    }
}

pub struct ArchiveHeaderEntry {
    pub hash: Hash,
    pub index: u64,
    pub length: u64,
}

pub trait ArchiveEntryData {
    fn turn_into_vec(self) -> Vec<u8>;
    async fn turn_into_async_reader(&mut self) -> anyhow::Result<impl AsyncBufRead + Unpin>
    {
        Err::<FuturesAsyncReader, _>(anyhow!("unimplemented"))
    }
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

impl ArchiveEntryData for StoreEntryData {
    fn turn_into_vec(self) -> Vec<u8> {
        let mut object = futures::executor::block_on(self.store.get_object(&self.hash))
            .expect("Object to be available in store");

        let mut data: Vec<u8> = Vec::new();
        futures::executor::block_on(object.read_to_end(&mut data)).expect("Reading to work");

        data
    }

    async fn turn_into_async_reader(&mut self) -> anyhow::Result<impl AsyncBufRead + Unpin>
    {
        let object = self.store.get_object(&self.hash).await?;

        Ok(object)
    }
}

pub struct FuturesAsyncReaderEntryData {
	pub async_reader: FuturesAsyncReader,
}

impl ArchiveEntryData for FuturesAsyncReaderEntryData {
	fn turn_into_vec(mut self) -> Vec<u8> {
		let mut data: Vec<u8> = Vec::new();
		futures::executor::block_on(self.async_reader.read_to_end(&mut data)).expect("Reading to work");

		data
	}

	async fn turn_into_async_reader(&mut self) -> anyhow::Result<impl AsyncBufRead + Unpin>
	{
		Ok(self.async_reader.by_ref())
	}
}

// impl AsyncRead for StoreEntryData {
// 	fn poll_read(
// 		self: std::pin::Pin<&mut Self>,
// 		cx: &mut std::task::Context<'_>,
// 		buf: &mut [u8],
// 	) -> std::task::Poll<std::io::Result<usize>> {
		
// 	}
// }

// impl AsyncBufRead for StoreEntryData {
// 	fn poll_fill_buf(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<&[u8]>> {
// 		if self.reader.is_none() {
// 			if self.reader_fut.is_none() {
// 				let store = self.store.clone();
// 				let hash = self.hash.clone();
// 				self.reader_fut = Some(Box::pin(async move {
// 					store.get_object(&hash).await
// 				}));
// 			}
			
// 			if let Some(mut fut) = self.reader_fut.take() {
// 				match fut.as_mut().poll(cx) {
// 					std::task::Poll::Ready(Ok(reader)) => {
// 						self.reader = Some(reader);
// 					}
// 					std::task::Poll::Ready(Err(e)) => {
// 						return std::task::Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
// 					}
// 					std::task::Poll::Pending => {
// 						self.reader_fut = Some(fut);
// 						return std::task::Poll::Pending;
// 					}
// 				}
// 			}
// 		}
		
// 		if let Some(ref mut reader) = self.reader {
// 			std::pin::Pin::new(reader).poll_fill_buf(cx)
// 		} else {
// 			std::task::Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, "No reader available")))
// 		}
// 	}

// 	fn consume(mut self: std::pin::Pin<&mut Self>, amt: usize) {
// 		if let Some(ref mut reader) = self.reader {
// 			std::pin::Pin::new(reader).consume(amt);
// 		}
// 	}
// }

pub struct ArchiveBody<T>
where
    T: ArchiveEntryData + Unpin,
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

impl<T> AsyncBufRead for ArchiveBody<T> {
	fn poll_fill_buf(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<&[u8]>> {
		todo!()
	}

	fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
		todo!()
	}
}