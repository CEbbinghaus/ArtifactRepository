use std::{io::{BufRead, BufReader, Read, Write}, num::NonZero};

use anyhow::anyhow;
use sha2::{Digest, Sha512};

use crate::{
    object_body::{Index, Object},
    read_header_and_body, Hash, Header,
};

pub const HEADER: [u8; 4] = [b'a', b'r', b'x', b'a'];

#[repr(u16)]
#[derive(Clone, Copy)]
pub enum Compression {
    None = 0,
    Deflate = 8,
    LZMA2 = 16,
}

impl TryFrom<u16> for Compression {
    type Error = ();

    fn try_from(v: u16) -> Result<Self, Self::Error> {
        match v {
            x if x == Compression::None as u16 => Ok(Compression::None),
            x if x == Compression::Deflate as u16 => Ok(Compression::Deflate),
            x if x == Compression::LZMA2 as u16 => Ok(Compression::LZMA2),
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
        writer.write(&HEADER)?;
        writer.write(&(self.compression as u16).to_be_bytes())?;
        writer.write(&self.hash.hash)?;
        writer.write(&self.index.to_data())?;
        writer.write(&[0])?;

        match self.compression {
            Compression::None => self.body.to_data(writer)?,
            Compression::Deflate => todo!(),
            Compression::LZMA2 => self.body.to_data(&mut lzma_rust2::Lzma2WriterMt::new(
                writer,
                lzma_rust2::Lzma2Options { lzma_options: Default::default(), chunk_size: NonZero::new(1024 * 64) },
                std::thread::available_parallelism().unwrap().get() as u32,
            )?)?,
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

        let index = Index::from_data(&index_bytes[..index_bytes_read - 1]);

        let body = match compression {
            Compression::None => ArchiveBody::<RawEntryData>::from_data(&mut reader)?,
            Compression::Deflate => todo!(),
            Compression::LZMA2 => {
                ArchiveBody::<RawEntryData>::from_data(&mut lzma_rust2::Lzma2ReaderMt::new(
                    &mut reader,
                    lzma_rust2::LzmaOptions::DICT_SIZE_DEFAULT,
                    None,
                    std::thread::available_parallelism().unwrap().get() as u32,
                ))?
            }
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

pub struct ArchiveEntry<T>
where
    T: ArchiveEntryData,
{
    pub header: Header,
    pub body: T,
}

pub struct ArchiveBody<T>
where
    T: ArchiveEntryData,
{
    pub header: Vec<ArchiveHeaderEntry>,
    pub entries: Vec<ArchiveEntry<T>>,
}

impl<T> ArchiveBody<T>
where
    T: ArchiveEntryData,
{
    fn to_data<'a>(self, writer: &'a mut impl Write) -> anyhow::Result<()> {
        writer.write(&(self.header.len() as u64).to_be_bytes())?;
        for entry in &self.header {
            writer.write(&entry.hash.hash)?;
            writer.write(&entry.index.to_be_bytes())?;
            writer.write(&entry.length.to_be_bytes())?;
        }

        for entry in self.entries {
            writer.write(entry.header.get_prefix().as_bytes())?;

            writer.write(&entry.body.turn_into_vec())?;
        }

        Ok(())
    }

    fn from_data<'a>(reader: &'a mut impl Read) -> anyhow::Result<ArchiveBody<RawEntryData>> {
        let mut long: [u8; 8] = [0; 8];
        reader.read_exact(&mut long)?;
        let count = u64::from_be_bytes(long);

        println!("Loading {count} entries");

        let mut header_entries: Vec<ArchiveHeaderEntry> = Vec::with_capacity(count as usize);
        let mut counter = 0;
        loop {
            if counter == count {
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

        let mut entries: Vec<ArchiveEntry<RawEntryData>> = Vec::with_capacity(header_entries.len());
        for entry in &header_entries {
            assert!(entry.index == counter);

            let amount = entry.length;
            let mut data: Vec<u8> = vec![0; amount as usize];
            reader.read_exact(&mut data[..])?;

            let mut hasher = Sha512::new();
            hasher.write(&data)?;
            assert!(Hash::from(hasher) == entry.hash);

            let (header, body) = read_header_and_body(&data).ok_or(anyhow!("Invalid Header"))?;

            entries.push(ArchiveEntry {
                header,
                body: RawEntryData(body.to_vec()),
            });

            counter += amount;
        }

        Ok(ArchiveBody {
            header: header_entries,
            entries,
        })
    }
}
