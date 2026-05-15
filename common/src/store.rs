use crate::{Hash, Header};
use anyhow::Result;
use futures::io::copy;
use futures::AsyncReadExt;
use futures::{AsyncBufRead, AsyncRead, AsyncWriteExt};
use opendal::{Builder, FuturesAsyncReader, Operator};

pub struct StoreObject<T>
where
	T: AsyncBufRead + AsyncRead + Unpin,
{
	pub header: Header,
	body: T,
}

impl<T> StoreObject<T>
where
	T: AsyncBufRead + AsyncRead + Unpin,
{
	pub fn header(&self) -> &Header {
		&self.header
	}

	pub fn to_data(&mut self) -> Vec<u8> {
		// read body into Vec
		let mut body_data: Vec<u8> = Vec::new();
		futures::executor::block_on(self.body.read_to_end(&mut body_data))
			.expect("Reading body to work");

		body_data
	}
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
		Ok(self.operator.exists(&hash.as_path()).await?)
	}

	pub async fn get_object(&self, hash: &Hash) -> Result<StoreObject<FuturesAsyncReader>> {
		let mut reader = self
			.operator
			.reader(&hash.as_path())
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
			.writer(&hash.as_path())
			.await?
			.into_futures_async_write();

		object.header.write_to_async(&mut writer).await?;
		copy(&mut object.body, &mut writer).await?;

		writer.close().await?;

		Ok(())
	}
}
