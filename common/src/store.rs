use crate::{header, Hash, Header};
use anyhow::{anyhow, Result};
use futures::io::copy;
use futures::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWriteExt};
use futures::{AsyncReadExt, AsyncSeekExt};
use opendal::{FuturesAsyncReader, Operator};

pub struct StoreObject<T>
where
    T: AsyncBufRead + AsyncRead + Unpin,
{
    header: Header,
    body: T,
}

impl<T> StoreObject<T>
where
    T: AsyncBufRead + AsyncRead + Unpin,
{
    // pub async fn new(mut reader: T) -> Result<Self>
	// {
    //     let mut buffer = [0u8; 32];
    //     let bytes_read = reader.read(&mut buffer).await?;
    //     let data = &buffer[..bytes_read];

    //     let Some(header_end) = data.iter().position(|x| *x == 0) else {
    //         return Err(anyhow!(
    //             "Invalid header. No null byte in the first 32 bytes"
    //         ));
    //     };
    //     let header = Header::from_data(&data[..header_end])?;
    //     reader
    //         .seek(std::io::SeekFrom::Start(header_end as u64))
    //         .await?;

    //     Ok(Self {
    //         header,
    //         body: reader,
    //     })
    // }

    pub fn new_with_header(header: Header, reader: T) -> Self {
        Self {
            header,
            body: reader,
        }
    }
}

impl<T> AsyncRead for StoreObject<T>
where
    T: AsyncBufRead + AsyncRead + Unpin
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.poll_read(cx, buf)
    }
}

impl<T> AsyncBufRead for StoreObject<T>
where
    T: AsyncBufRead + AsyncRead + Unpin
{
    fn poll_fill_buf(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        self.poll_fill_buf(cx)
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        self.consume(amt);
    }
}

pub struct Store {
    operator: Operator,
}

impl Store {
	pub fn new(operator: Operator) -> Self {
		Self { operator }
	}

	pub async fn exists(&self, hash: &Hash) -> Result<bool> {
		Ok(self.operator.exists(hash.as_str()).await?)
	}

    pub async fn get_object(&self, hash: &Hash) -> Result<StoreObject<FuturesAsyncReader>> {
        let mut reader = self
            .operator
            .reader(hash.as_str())
            .await?
            .into_futures_async_read(..)
            .await?;

        let header = Header::read_from_async(&mut reader).await?;

        Ok(StoreObject::new_with_header(header, reader))
    }

    pub async fn put_object<T>(&self, hash: &Hash, mut object: StoreObject<T>) -> Result<()>
    where
        T: AsyncBufRead + AsyncRead + Unpin + AsyncSeek,
    {
        let mut writer = self
            .operator
            .writer(hash.as_str())
            .await?
            .into_futures_async_write();

		object.header.write_to_async(&mut writer).await?;
		copy(&mut object.body, &mut writer).await?;

		writer.close().await?;

		Ok(())
    }
}
