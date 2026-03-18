use crate::{Hash, Header};
use anyhow::Result;
use futures::io::copy;
use futures::{AsyncBufRead, AsyncRead, AsyncWriteExt};
use opendal::{Builder, FuturesAsyncReader, Operator};

pub struct StoreObject<T>
{
    pub header: Header,
    body: T,
}

impl<T> StoreObject<T>
{
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
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.body).poll_read(cx, buf)
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
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.body).poll_fill_buf(cx)
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        std::pin::Pin::new(&mut this.body).consume(amt);
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
		Ok(self.operator.exists(&hash.as_str()).await?)
	}

    pub async fn get_object(&self, hash: &Hash) -> Result<StoreObject<FuturesAsyncReader>> {
        let mut reader = self
            .operator
            .reader(&hash.as_str())
            .await?
            .into_futures_async_read(..)
            .await?;

        let header = Header::read_from_async(&mut reader).await?;

        Ok(StoreObject::new_with_header(header, reader))
    }

    pub async fn put_object<T>(&self, hash: &Hash, mut object: StoreObject<T>) -> Result<()>
    where
        T: AsyncBufRead + AsyncRead + Unpin,
    {
        let mut writer = self
            .operator
            .writer(&hash.as_str())
            .await?
            .into_futures_async_write();

		object.header.write_to_async(&mut writer).await?;
		copy(&mut object.body, &mut writer).await?;

		writer.close().await?;

		Ok(())
    }
}
