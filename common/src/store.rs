use std::pin::Pin;
use std::task::Poll;

use crate::{header, Hash, Header};
use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::future::Ready;
use futures::io::copy;
use futures::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWriteExt, FutureExt, Stream, StreamExt};
use futures::{AsyncReadExt, AsyncSeekExt};
use opendal::{Buffer, BufferStream, Builder, FuturesAsyncReader, Operator};

pub struct StoreObject<T> 
where T : Unpin {
    pub header: Header,
    body: T,
}

impl<T : Unpin> StoreObject<T> {
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
    T: AsyncBufRead + AsyncRead + Unpin,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.body).poll_read(cx, buf)
    }
}

impl<T> AsyncBufRead for StoreObject<T>
where
    T: AsyncBufRead + AsyncRead + Unpin,
{
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        let this = self.get_mut();
        Pin::new(&mut this.body).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        Pin::new(&mut this.body).consume(amt);
    }
}

impl Stream for StoreObject<BufferStream> {
    type Item = Result<Bytes, Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll::*;
		let this = self.get_mut();
		let pinned_body = Pin::new(&mut this.body);
        let Ready(result) = BufferStream::poll_next(pinned_body, cx) else {
            return Pending;
        };

		// We are done, There is nothing left
		let Some(result) = result else {
			return Ready(None);
		};

		let buffer = match result {
			Ok(v) => v,
			Err(err) => {
				return Ready(Some(Err(err.into())));
			}
		};

		Ready(Some(Ok(buffer.to_bytes())))
    }
}

#[derive(Clone)]
pub struct Store {
    operator: Operator,
}

impl Store {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    pub fn from_builder(builder: impl Builder) -> Result<Self> {
        Ok(Self::new(Operator::new(builder)?.finish()))
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

	pub async fn get_object_stream(&self, hash: &Hash) -> Result<StoreObject<BufferStream>> {
        let mut reader = self
            .operator
            .reader(hash.as_str())
            .await?;

		let header = reader.read(0..32).await?;

        let (header, header_len) = Header::from_buf_with_len(&header.to_vec())?;

        Ok(StoreObject::new_with_header(header, reader.into_stream(header_len + 1..)
            .await?))
    }

    pub async fn put_object<T>(&self, hash: &Hash, mut object: StoreObject<T>) -> Result<()>
    where
        T: AsyncBufRead + AsyncRead + Unpin,
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
