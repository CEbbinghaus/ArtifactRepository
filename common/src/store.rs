use crate::{Hash, Header};
use anyhow::Result;
use futures::io::copy;
use futures::{AsyncBufRead, AsyncRead, AsyncWriteExt};
use opendal::{Builder, FuturesAsyncReader, Operator};
use std::path::PathBuf;

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
    /// Root path for direct filesystem access (bypasses OpenDAL overhead for bulk writes)
    root_path: Option<PathBuf>,
}

impl Store {
    pub fn new(operator: Operator) -> Self {
        Self { operator, root_path: None }
    }

    pub fn from_builder(builder: impl Builder) -> Result<Self> {
        Ok(Self::new(Operator::new(builder)?.finish()))
    }

    /// Create a store from a filesystem path with direct I/O support.
    /// This enables `put_bytes_direct` for high-performance bulk writes.
    pub fn from_fs_path(path: &std::path::Path) -> Result<Self> {
        let builder = opendal::services::Fs::default()
            .root(path.to_str().ok_or_else(|| anyhow::anyhow!("store path must be valid UTF-8"))?);
        let operator = Operator::new(builder)?.finish();
        Ok(Self {
            operator,
            root_path: Some(path.to_path_buf()),
        })
    }

    pub async fn exists(&self, hash: &Hash) -> Result<bool> {
        let result = self.operator.exists(hash.as_str()).await?;
        tracing::trace!(hash = %hash, exists = result, "store exists check");
        Ok(result)
    }

    pub async fn get_object(&self, hash: &Hash) -> Result<StoreObject<FuturesAsyncReader>> {
        tracing::debug!(hash = %hash, "reading object from store");
        let mut reader = self
            .operator
            .reader(hash.as_str())
            .await?
            .into_futures_async_read(..)
            .await?;

        let header = Header::read_from_async(&mut reader).await?;

        tracing::trace!(hash = %hash, object_type = %header.object_type.to_str(), size = header.size, "object header read");
        Ok(StoreObject::new_with_header(header, reader))
    }

    /// Write an object from in-memory bytes (header + body concatenated).
    /// Preferred over `put_object` when the body is already in memory.
    pub async fn put_object_bytes(&self, hash: &Hash, header: Header, body: Vec<u8>) -> Result<()> {
        tracing::debug!(hash = %hash, object_type = %header.object_type.to_str(), size = body.len(), "storing object");
        let header_bytes = header.to_string();
        let mut buf = Vec::with_capacity(header_bytes.len() + body.len());
        buf.extend_from_slice(header_bytes.as_bytes());
        buf.extend(body);
        tracing::trace!(hash = %hash, total_bytes = buf.len(), "object written");
        self.operator.write(hash.as_str(), buf).await?;
        Ok(())
    }

    /// Write an object from a streaming reader.
    /// Use `put_object_bytes` instead when the body is already in memory.
    /// Read the complete raw bytes of an object (header prefix + body).
    pub async fn get_raw_bytes(&self, hash: &Hash) -> Result<Vec<u8>> {
        let bytes = self.operator.read(hash.as_str()).await?;
        Ok(bytes.to_vec())
    }

    /// Write raw bytes directly to the store (caller provides complete content).
    pub async fn put_raw_bytes(&self, hash: &Hash, data: Vec<u8>) -> Result<()> {
        self.operator.write(hash.as_str(), data).await?;
        Ok(())
    }

    /// Write raw bytes using direct filesystem I/O, bypassing OpenDAL's atomic write/fsync.
    /// Safe for content-addressable storage where partial writes are harmless.
    pub fn put_bytes_direct_blocking(&self, hash: &Hash, data: &[u8]) -> Result<()> {
        if let Some(root) = &self.root_path {
            let path = root.join(hash.as_str());
            std::fs::write(&path, data)?;
            Ok(())
        } else {
            Err(anyhow::anyhow!("direct write not available: store was not created with from_fs_path"))
        }
    }

    /// Read raw bytes using direct filesystem I/O, bypassing OpenDAL overhead.
    pub fn get_bytes_direct_blocking(&self, hash: &Hash) -> Result<Vec<u8>> {
        if let Some(root) = &self.root_path {
            let path = root.join(hash.as_str());
            let data = std::fs::read(&path)?;
            Ok(data)
        } else {
            Err(anyhow::anyhow!("direct read not available: store was not created with from_fs_path"))
        }
    }

    /// Check if an object exists using direct filesystem I/O.
    pub fn exists_direct_blocking(&self, hash: &Hash) -> Result<bool> {
        if let Some(root) = &self.root_path {
            Ok(root.join(hash.as_str()).exists())
        } else {
            Err(anyhow::anyhow!("direct exists not available: store was not created with from_fs_path"))
        }
    }

    /// List all object hashes in the store.
    pub async fn list_hashes(&self) -> Result<Vec<Hash>> {
        use opendal::EntryMode;
        let entries = self.operator.list("/").await?;
        let mut hashes = Vec::new();
        for entry in entries {
            let path = entry.path();
            if entry.metadata().mode() == EntryMode::DIR {
                continue;
            }
            if let Ok(hash) = Hash::try_from(path.trim_end_matches('/').to_string()) {
                hashes.push(hash);
            }
        }
        Ok(hashes)
    }

    pub async fn put_object<T>(&self, hash: &Hash, mut object: StoreObject<T>) -> Result<()>
    where
        T: AsyncBufRead + AsyncRead + Unpin,
    {
        tracing::debug!(hash = %hash, object_type = %object.header.object_type.to_str(), "storing object (streaming)");
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
        store.put_object_bytes(&hash, header, b"hello".to_vec()).await.unwrap();
        assert!(store.exists(&hash).await.unwrap());
    }

    #[tokio::test]
    async fn put_bytes_then_get_returns_same_data() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let hash = make_hash(0x22);
        let header = Header::new(crate::ObjectType::Blob, 5);
        store.put_object_bytes(&hash, header, b"world".to_vec()).await.unwrap();

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
    async fn put_stream_then_get_returns_same_data() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let hash = make_hash(0x66);
        let header = Header::new(crate::ObjectType::Blob, 5);
        let data = Cursor::new(b"hello".to_vec());
        let obj = StoreObject::new_with_header(header, data);
        store.put_object(&hash, obj).await.unwrap();

        let mut retrieved = store.get_object(&hash).await.unwrap();
        assert_eq!(retrieved.header.object_type, crate::ObjectType::Blob);
        assert_eq!(retrieved.header.size, 5);

        let mut body = Vec::new();
        futures::AsyncReadExt::read_to_end(&mut retrieved, &mut body)
            .await
            .unwrap();
        assert_eq!(body, b"hello");
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
    async fn get_raw_bytes_returns_header_and_body() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let hash = make_hash(0x77);
        let header = Header::new(crate::ObjectType::Blob, 5);
        store.put_object_bytes(&hash, header, b"hello".to_vec()).await.unwrap();

        let raw = store.get_raw_bytes(&hash).await.unwrap();
        assert_eq!(&raw[..7], b"blob 5\0");
        assert_eq!(&raw[7..], b"hello");
    }

    #[tokio::test]
    async fn get_raw_bytes_missing_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let hash = make_hash(0x88);
        assert!(store.get_raw_bytes(&hash).await.is_err());
    }

    #[tokio::test]
    async fn list_hashes_returns_stored_objects() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let h1 = make_hash(0xaa);
        let h2 = make_hash(0xbb);
        let header = Header::new(crate::ObjectType::Blob, 4);
        store.put_object_bytes(&h1, header, b"data".to_vec()).await.unwrap();
        let header2 = Header::new(crate::ObjectType::Tree, 4);
        store.put_object_bytes(&h2, header2, b"tree".to_vec()).await.unwrap();

        let mut hashes = store.list_hashes().await.unwrap();
        hashes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        assert_eq!(hashes.len(), 2);
        assert!(hashes.contains(&h1));
        assert!(hashes.contains(&h2));
    }

    #[tokio::test]
    async fn list_hashes_empty_store() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let hashes = store.list_hashes().await.unwrap();
        assert!(hashes.is_empty());
    }

    #[tokio::test]
    async fn put_same_object_twice_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());
        let hash = make_hash(0x55);

        for _ in 0..2 {
            let header = Header::new(crate::ObjectType::Tree, 4);
            store.put_object_bytes(&hash, header, b"data".to_vec()).await.unwrap();
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
