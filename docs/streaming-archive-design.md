## Problem

Currently, archives are processed as complete in-memory buffers in several critical paths:

1. **`POST /upload`** — `body: Bytes` buffers the entire request body before parsing begins. For large archives, this means the server must hold the full compressed archive in memory.
2. **`Archive::from_data()`** — Reads and decompresses the entire archive body before returning. No incremental processing.
3. **Client `unpack_archive()`** — Reads the full archive file into memory, then writes entries to the store.
4. **Client `pull` + `unpack` are separate steps** — The client downloads an archive to disk, then unpacks it in a second pass. Ideally these could be pipelined: extract objects directly from the download stream.

This creates several issues:
- **Memory pressure**: Server and client memory usage scales with archive size (can be multiple GB)
- **Latency**: No processing can begin until the full archive is received
- **DOS risk**: Unbounded memory allocation from request bodies
- **Decompression bombs**: No way to enforce limits during incremental decompression

## Proposed Design

### Streaming Archive Reader

A new `StreamingArchiveReader` that processes an archive incrementally from any `Read` (or `AsyncRead`) source:

```
Archive wire format:
[magic:4B][compression:u16BE][index_hash:64B][index_body...][0x00][compressed_body]

Compressed body:
[entry_count:u64BE][headers: (hash+offset+length) x N][entry_data: concatenated]
```

**Phase 1 — Header parsing (small, bounded memory):**
- Read magic (4B), compression (2B), index hash (64B)
- Read index body until null terminator (bounded by max index size)
- Begin decompression of the compressed body
- Read entry count + all entry headers (bounded: N x 136B per header)

**Phase 2 — Entry iteration (streaming, O(1) memory per entry):**
- Yield entries one at a time: `(Hash, Header, body bytes)`
- Each entry body is read from the decompression stream on demand
- Consumer processes and discards before the next entry is read
- Enforce a max decompressed size counter to detect decompression bombs

```rust
pub struct StreamingArchiveReader<R: Read> {
    decompressor: Box<dyn Read>,
    headers: Vec<ArchiveHeaderEntry>,  // read upfront (small)
    current_entry: usize,
    bytes_decompressed: u64,
    max_decompressed_bytes: Option<u64>,
}

impl<R: Read> StreamingArchiveReader<R> {
    /// Parse the archive header and entry metadata.
    /// Returns the reader positioned at the first entry body.
    pub fn new(reader: R) -> Result<(IndexInfo, Self)> { ... }

    /// Read the next entry. Returns None when all entries are consumed.
    pub fn next_entry(&mut self) -> Result<Option<ArchiveEntry>> { ... }
}

pub struct ArchiveEntry {
    pub hash: Hash,
    pub header: Header,
    pub body: Vec<u8>,  // or a bounded Read adapter
}
```

### Streaming Archive Writer

Already partially implemented via `write_streaming_archive()` and `ChannelWriter`. Formalize into a proper type:

```rust
pub struct StreamingArchiveWriter<W: Write> {
    writer: W,
    compression: Compression,
    entries_written: u64,
}

impl<W: Write> StreamingArchiveWriter<W> {
    pub fn new(writer: W, compression: Compression, index: &Index, index_hash: &Hash) -> Result<Self>;
    pub fn write_entry(&mut self, hash: &Hash, header: Header, body: &[u8]) -> Result<()>;
    pub fn finish(self) -> Result<()>;
}
```

### Use Cases Enabled

1. **Server upload streaming**: Parse archive from request body stream, write entries to store as they arrive. Memory: O(largest_entry) instead of O(archive_size).

2. **Client download+extract pipeline**: Stream archive from HTTP response directly into store or filesystem, no intermediate file needed:
   ```
   HTTP response stream -> StreamingArchiveReader -> store.put_object_bytes() per entry
   ```

3. **Decompression bomb protection**: `StreamingArchiveReader` tracks `bytes_decompressed` and aborts if it exceeds a configured limit.

4. **Progress reporting**: Entry count is known upfront from headers, so progress can be reported as entries are processed.

## Implementation Notes

- The archive format already supports streaming reads — entry headers (with offsets/lengths) come before entry data
- The compressed body wraps everything after the index, so the decompressor must be initialized once and read sequentially
- For async contexts, provide `AsyncStreamingArchiveReader<R: AsyncRead>` using the same design
- The existing `Archive::from_data()` and `Archive::to_data()` should remain for in-memory use cases (tests, small archives)
- Consider whether entries should be yielded as borrowed slices (zero-copy) or owned Vec (simpler lifetime management)

## Acceptance Criteria

- [ ] `StreamingArchiveReader` can parse and iterate entries from a `Read` source
- [ ] `StreamingArchiveWriter` can produce a valid archive to a `Write` sink
- [ ] Server `POST /upload` uses streaming reader (memory bounded)
- [ ] Client can pipeline download and extract without intermediate files
- [ ] Decompression size limit is enforced
- [ ] Existing `Archive` API preserved for backward compatibility
- [ ] All existing tests continue to pass
- [ ] Round-trip test: `StreamingArchiveWriter` output readable by both `StreamingArchiveReader` and `Archive::from_data()`
