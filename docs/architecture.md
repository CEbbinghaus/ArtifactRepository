# Architecture

## Crate Structure

```
ArtifactRepository/
├── common/     Shared library (object model, store, archive format)
├── client/     CLI binary (depends on common)
└── server/     HTTP server binary (depends on common)
```

No circular dependencies. `common` has zero knowledge of `client` or `server`.

## Data Model

The object model mirrors Git's design, using SHA-512 instead of SHA-1.

### Object Types

**Blob** — Raw file content. Leaf node with no internal structure.

**Tree** — A directory listing. Contains entries, each pointing to a child Blob or Tree by hash.

**Index** — Root pointer (analogous to a Git commit). Contains a tree hash, timestamp, and arbitrary key-value metadata.

### Merkle Tree

```
Index ──tree──▸ Tree ──entry──▸ Blob
                 │
                 ├──entry──▸ Blob
                 │
                 └──entry──▸ Tree ──entry──▸ Blob
```

Every object's identity is its SHA-512 hash, computed over its type, size, and body content. Changing any file produces a different blob hash, which changes the parent tree hash, which changes the index hash — providing tamper-evident integrity from root to leaf.

### Content Addressability

Two objects with identical content always produce the same hash. This gives automatic deduplication:
- Multiple artifacts sharing the same file store only one copy
- Archives containing duplicate files store each unique file once
- Servers backing onto shared storage never conflict on writes

### Hashing

```
SHA-512( "{type} {body_length}\0{body_bytes}" )
```

Where `{type}` is `blob`, `tree`, or `indx`. This matches Git's object format structurally.

## Storage Layer

The `Store` struct wraps an [OpenDAL](https://opendal.apache.org/) `Operator`, providing a uniform interface over storage backends.

```
Store
  └─ OpenDAL Operator
       ├─ Local filesystem (services-fs)
       └─ S3-compatible storage (services-s3, server only)
```

Objects are stored as flat files keyed by their 128-character hex hash. Each file contains the text header (`"type size\0"`) followed by the raw body bytes.

## Client Flows

### commit

```
Walk directory (parallel, bounded by semaphore)
  ├─ For each file:  read → hash → store as Blob
  └─ For each dir:   recurse → collect entries → store as Tree
Build Index (tree hash + timestamp) → store → print hash
```

Files and subdirectories within each directory are processed concurrently using `JoinSet` with a semaphore (64 concurrent operations).

### restore

```
Read Index → get tree hash
Recursively walk Trees:
  ├─ Tree entry → create directory, recurse
  └─ Blob entry → read from store, write to filesystem
Optional: --validate re-hashes every written file
```

### pack

```
Read Index → discover all objects via DFS (read_object_into_headers)
Build archive: magic + compression + hash + index + compressed body
Body = header table (hash/offset/length per entry) + concatenated object data
```

### unpack

```
Read archive envelope (magic, compression, hash, index)
Decompress body → read header table → read + verify entries
Write each object to the local store
```

### push / pull

```
push: Upload objects to server via PUT /object/{hash}
pull: Download Index + referenced objects via GET /object/{hash}
```

## Server

An Axum HTTP server with two endpoints:

- `PUT /object/{hash}` — Store an object (idempotent)
- `GET /object/{hash}` — Retrieve an object (streamed)

Transport compression (gzip, zstd, brotli, deflate) is handled transparently by `tower-http::CompressionLayer`.

The server is a pure content-addressed object store — no listing, no deletion, no metadata queries. Horizontal scaling is achieved by pointing multiple servers at the same backing store (filesystem or S3).

## Compression

Archive-level compression is applied to the body of `.arx` files:

| Algorithm | Multi-threaded | Notes |
|-----------|---------------|-------|
| None | — | Raw, no compression |
| Gzip | No | Standard gzip via flate2 |
| Deflate | No | Raw DEFLATE via flate2 |
| LZMA2 | Yes | All available cores |
| Zstd | Yes | Level 3, all available cores |

Only the archive body is compressed. The envelope (magic, compression type, hash, index) is always uncompressed for direct access.
