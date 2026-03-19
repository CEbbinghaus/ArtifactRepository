use crate::{Hash, Header};
use anyhow::Result;
use futures::io::copy;
use futures::{AsyncBufRead, AsyncRead, AsyncWriteExt};
use opendal::{Builder, FuturesAsyncReader, Operator};

pub struct StoreObject<T> {
    pub header: Header,
    body: T,
}

impl<T> StoreObject<T> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;

    fn make_hash(fill: u8) -> Hash {
        Hash::from([fill; 64])
    }

    fn make_store(dir: &std::path::Path) -> Store {
        let builder = opendal::services::Fs::default().root(dir.to_str().unwrap());
        Store::from_builder(builder).unwrap()
    }

    #[tokio::test]
    async fn put_then_exists_returns_true() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let hash = make_hash(0x11);
        let header = Header::new(crate::ObjectType::Blob, 5);
        let data = Cursor::new(b"hello".to_vec());
        let obj = StoreObject::new_with_header(header, data);
        store.put_object(&hash, obj).await.unwrap();
        assert!(store.exists(&hash).await.unwrap());
    }

    #[tokio::test]
    async fn put_then_get_returns_same_data() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let hash = make_hash(0x22);
        let header = Header::new(crate::ObjectType::Blob, 5);
        let data = Cursor::new(b"world".to_vec());
        let obj = StoreObject::new_with_header(header, data);
        store.put_object(&hash, obj).await.unwrap();

        let mut retrieved = store.get_object(&hash).await.unwrap();
        assert_eq!(retrieved.header.object_type, crate::ObjectType::Blob);
        assert_eq!(retrieved.header.size, 5);

        let mut body = Vec::new();
        futures::AsyncReadExt::read_to_end(&mut retrieved, &mut body)
            .await
            .unwrap();
        assert_eq!(body, b"world");
    }

    #[tokio::test]
    async fn exists_returns_false_for_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let hash = make_hash(0x33);
        assert!(!store.exists(&hash).await.unwrap());
    }

    #[tokio::test]
    async fn get_object_missing_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let hash = make_hash(0x44);
        assert!(store.get_object(&hash).await.is_err());
    }

    #[tokio::test]
    async fn put_same_object_twice_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let hash = make_hash(0x55);

        for _ in 0..2 {
            let header = Header::new(crate::ObjectType::Tree, 4);
            let data = Cursor::new(b"data".to_vec());
            let obj = StoreObject::new_with_header(header, data);
            store.put_object(&hash, obj).await.unwrap();
        }

        assert!(store.exists(&hash).await.unwrap());
        let mut retrieved = store.get_object(&hash).await.unwrap();
        let mut body = Vec::new();
        futures::AsyncReadExt::read_to_end(&mut retrieved, &mut body)
            .await
            .unwrap();
        assert_eq!(body, b"data");
    }
}
