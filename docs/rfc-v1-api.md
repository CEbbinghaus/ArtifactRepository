# ArtifactRepository V1 HTTP API Specification

**Status:** Draft\
**Version:** 1.0.0

## 1. Introduction

This document defines the V1 HTTP API for ArtifactRepository, a content-addressed artifact storage system. It specifies the protocol a compliant server MUST implement to support upload, download, deduplication, and archive operations.

ArtifactRepository uses SHA-512 Merkle trees to organize objects. Every piece of data is identified by its cryptographic hash, enabling automatic deduplication and integrity verification.

### 1.1 Terminology

- **Object**: A stored unit of data identified by its SHA-512 hash. One of three types: blob, tree, or index.
- **Blob**: A leaf object containing raw file data.
- **Tree**: A directory listing mapping filenames to child object hashes and file modes.
- **Index**: A root object referencing a tree hash, a timestamp, and optional key-value metadata.
- **Hash**: A 64-byte SHA-512 digest, represented as a 128-character lowercase hexadecimal string in the API.
- **Store**: The backing storage containing all objects.
- **Archive (.arx)**: A binary container holding a complete set of objects reachable from an index.
- **Supplemental Archive (.sar)**: A binary container holding a subset of objects, used for incremental transfers.

### 1.2 Conventions

The key words "MUST", "MUST NOT", "SHOULD", "SHOULD NOT", and "MAY" are to be interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

All hashes in URL paths and JSON bodies MUST be 128-character lowercase hexadecimal strings encoding a 64-byte SHA-512 digest.

## 2. Object Model

### 2.1 Hash Computation

All hashes are computed using SHA-512. The input to the hash function is:

```
SHA-512( key + " " + length + "\0" + data )
```

Where:
- `key` is a type-specific string: `"blob"`, `"tree"`, or `"indx"`
- `" "` is a single ASCII space (0x20)
- `length` is the decimal string representation of the byte length of `data`
- `"\0"` is a single null byte (0x00)
- `data` is the raw serialized body

**Example:** For a blob containing `"hello world"` (11 bytes):
```
hash = SHA-512("blob 11\0hello world")
```

### 2.2 Object Storage Format

Each object is stored as a header followed by its body:

```
{type} {size}\0{body}
```

- `{type}`: One of `"blob"`, `"tree"`, `"indx"` (ASCII)
- `" "`: Single space (0x20)
- `{size}`: Decimal representation of body length in bytes (ASCII)
- `"\0"`: Null terminator (0x00)
- `{body}`: Raw body bytes

### 2.3 Blob Objects

A blob stores raw file contents. The body is the unmodified file data.

### 2.4 Tree Objects

A tree stores a directory listing. The body is a concatenation of entries in binary format. Entries MUST be sorted lexicographically by filename to ensure deterministic hashes.

Each entry:
```
{mode} {filename}\0{hash}
```

- `{mode}`: File mode as a 6-character ASCII decimal string:
  - `"040000"` — Directory (tree reference)
  - `"100644"` — Regular file
  - `"100755"` — Executable file
  - `"120000"` — Symbolic link
- `" "`: Single space (0x20)
- `{filename}`: UTF-8 encoded filename (no path separators)
- `"\0"`: Null byte (0x00)
- `{hash}`: 64 raw bytes of the referenced object's SHA-512 hash

An empty directory is a tree with zero entries (empty body).

### 2.5 Index Objects

An index is the root object of a stored snapshot. The body is a UTF-8 text format:

```
tree: {tree_hash_hex}\n
timestamp: {rfc3339_timestamp}\n
{key}: {value}\n
...
\n
```

- The `tree:` field is REQUIRED and MUST appear first, containing the 128-character hex hash of the root tree.
- The `timestamp:` field is REQUIRED and MUST appear second, containing an RFC 3339 formatted timestamp.
- Additional key-value metadata lines MAY follow.
- The body MUST end with an empty line (trailing `\n` after the last field produces `\n\n`).
- Duplicate metadata keys MUST be rejected.

## 3. API Endpoints

All V1 endpoints are prefixed with `/v1/`. Servers MUST serve all endpoints under this prefix.

### 3.1 Upload Object

```
PUT /v1/object/{hash}
```

Stores a single object in the server's store.

**Path Parameters:**
- `hash` (string, required): 128-character lowercase hex SHA-512 hash

**Request Headers:**
- `Object-Type` (string, required): One of `"blob"`, `"tree"`, `"indx"`
- `Object-Size` (string, required): Decimal byte count of the object body

**Request Body:** Raw object bytes (header prefix + null terminator + body, as stored)

**Responses:**

| Status | Description |
|--------|-------------|
| 200 OK | Object already exists (idempotent, no-op) |
| 201 Created | Object stored successfully |
| 400 Bad Request | Missing or invalid headers, or invalid hash format |
| 500 Internal Server Error | Storage failure |

**Idempotency:** Because hashes are content-derived, a PUT for an existing hash MUST return 200 and skip the write. The content is guaranteed identical for the same hash due to SHA-512 collision resistance.

### 3.2 Download Object

```
GET /v1/object/{hash}
```

Retrieves a single object from the server's store.

**Path Parameters:**
- `hash` (string, required): 128-character lowercase hex SHA-512 hash

**Response Headers:**
- `Object-Type` (string): `"blob"`, `"tree"`, or `"indx"`
- `Object-Size` (string): Decimal byte count of the body

**Response Body:** The raw stored object bytes (header prefix + null + body), streamed.

**Responses:**

| Status | Description |
|--------|-------------|
| 200 OK | Object returned in body |
| 404 Not Found | Object does not exist |
| 500 Internal Server Error | Storage failure |

### 3.3 Check Missing Objects

```
POST /v1/object/missing
```

Given a list of hashes, returns the subset that the server does not have.

**Request Body (JSON):**
```json
{
  "hashes": [
    "aabb...128chars...",
    "ccdd...128chars..."
  ]
}
```

**Response Body (JSON):**
```json
{
  "missing": [
    "ccdd...128chars..."
  ]
}
```

The `missing` array contains only hashes from the request that do not exist in the server's store. If all hashes exist, `missing` is an empty array.

**Responses:**

| Status | Description |
|--------|-------------|
| 200 OK | Response contains missing hashes |
| 400 Bad Request | Malformed JSON or invalid hash format |
| 500 Internal Server Error | Storage failure |

### 3.4 Download Archive

```
GET /v1/archive/{index_hash}
```

Downloads a complete archive (`.arx`) containing all objects reachable from the specified index.

**Path Parameters:**
- `index_hash` (string, required): Hash of the index object

**Query Parameters:**
- `compression` (string, optional): Compression algorithm. Default: `"zstd"`. One of:
  - `"none"` — No compression
  - `"gzip"` — Gzip compression
  - `"deflate"` — Raw DEFLATE compression
  - `"lzma2"` — LZMA2/XZ compression
  - `"zstd"` — Zstandard compression

**Response Headers:**
- `Content-Type`: `application/octet-stream`
- `Content-Disposition`: `attachment; filename="{hash_prefix}.arx"` (prefix is first 12 hex chars)

**Response Body:** Streamed `.arx` archive bytes (see Section 4).

**Responses:**

| Status | Description |
|--------|-------------|
| 200 OK | Archive streamed |
| 400 Bad Request | Invalid compression value or hash format |
| 404 Not Found | Index object does not exist |
| 500 Internal Server Error | Storage or serialization failure |

### 3.5 Download Supplemental Archive

```
POST /v1/archive/{index_hash}/supplemental
```

Downloads a supplemental archive (`.sar`) containing a requested subset of objects plus all structural objects (trees and the index itself).

**Path Parameters:**
- `index_hash` (string, required): Hash of the index object

**Query Parameters:**
- `compression` (string, optional): Same as Section 3.4. Default: `"zstd"`.

**Request Body (JSON):**
```json
{
  "hashes": [
    "aabb...128chars...",
    "ccdd...128chars..."
  ]
}
```

**Response Headers:**
- `Content-Type`: `application/octet-stream`
- `Content-Disposition`: `attachment; filename="{hash_prefix}.sar"`

**Response Body:** Streamed `.sar` archive bytes (see Section 4).

**Behavior:** The server MUST include:
1. All hashes listed in the request that exist in the store
2. All tree objects referenced by the index (for directory structure)
3. The index object itself

This enables a client to request only the objects it is missing while always receiving enough structural data to reconstruct the tree.

**Responses:**

| Status | Description |
|--------|-------------|
| 200 OK | Supplemental archive streamed |
| 400 Bad Request | Malformed JSON, invalid compression, or invalid hash format |
| 404 Not Found | Index object does not exist |
| 500 Internal Server Error | Storage or serialization failure |

### 3.6 Upload Archive

```
POST /v1/archive/upload
```

Uploads an archive (`.arx` or `.sar`) and unpacks all contained objects into the server's store.

**Request Headers:**
- `Content-Type`: SHOULD be `application/octet-stream`

**Request Body:** Raw archive bytes (`.arx` or `.sar` format, see Section 4).

**Response Body (plain text):**
```
Added {n} objects, skipped {m}
```

Where `n` is the number of newly stored objects and `m` is the number of objects that already existed.

**Responses:**

| Status | Description |
|--------|-------------|
| 200 OK | Archive unpacked (includes counts of added/skipped) |
| 400 Bad Request | Invalid archive format |
| 500 Internal Server Error | Storage failure |

**Idempotency:** Objects that already exist in the store MUST be skipped without error (same content-addressable guarantee as PUT).

### 3.7 Get Index Metadata

```
GET /v1/index/{index_hash}/metadata
```

Returns JSON metadata describing the full tree structure referenced by an index, without downloading any file contents.

**Path Parameters:**
- `index_hash` (string, required): Hash of the index object

**Response Body (JSON):**
```json
{
  "index": {
    "hash": "aabb...128chars...",
    "tree": "ccdd...128chars...",
    "timestamp": "2025-01-15T12:00:00Z",
    "metadata": {
      "key": "value"
    }
  },
  "objects": {
    "ccdd...128chars...": {
      "type": "tree",
      "entries": {
        "file.txt": {
          "mode": "100644",
          "hash": "eeff...128chars..."
        },
        "subdir": {
          "mode": "040000",
          "hash": "1122...128chars..."
        }
      }
    },
    "eeff...128chars...": {
      "type": "blob",
      "size": 1024
    }
  }
}
```

**Fields:**
- `index.hash`: Hash of the index object itself
- `index.tree`: Hash of the root tree
- `index.timestamp`: RFC 3339 timestamp of when the index was created
- `index.metadata`: Key-value pairs from the index object
- `objects`: Map from hash to object metadata. Each value is either:
  - `{"type": "blob", "size": N}` — a blob with size in bytes
  - `{"type": "tree", "entries": {...}}` — a tree with its directory entries, each having `mode` and `hash`

**Responses:**

| Status | Description |
|--------|-------------|
| 200 OK | Metadata returned |
| 404 Not Found | Index object does not exist |
| 500 Internal Server Error | Storage failure |

### 3.8 Server Information

```
GET /v1/info
```

Returns metadata about the server, its capabilities, and supported extensions. Clients SHOULD call this endpoint before other operations to discover what features are available.

**Response Body (JSON):**
```json
{
  "spec_version": "1.0.0",
  "server": {
    "name": "ArtifactRepository",
    "version": "0.1.0"
  },
  "extensions": [
    "zip-content-addressing"
  ],
  "authentication": {
    "methods": []
  },
  "capabilities": {
    "compression": ["none", "gzip", "deflate", "lzma2", "zstd"],
    "max_object_size": null
  }
}
```

**Fields:**

- `spec_version` (string, required): The V1 specification version the server implements (semver).
- `server.name` (string, required): Human-readable server implementation name.
- `server.version` (string, required): Server implementation version (semver).
- `extensions` (array of strings, required): List of supported extension identifiers. An empty array means no extensions. Defined extensions:
  - `"zip-content-addressing"` — Transparent zip file decomposition (see `rfc-v1-ext-zip.md`)
- `authentication.methods` (array of strings, required): Supported authentication methods. An empty array means no authentication is required. Values are defined by future authentication RFCs.
- `capabilities.compression` (array of strings, required): Compression algorithms supported for archive endpoints.
- `capabilities.max_object_size` (integer or null, required): Maximum object size in bytes the server will accept, or `null` for no limit.

Servers MAY include additional fields in the response. Clients MUST ignore fields they do not recognize.

**Responses:**

| Status | Description |
|--------|-------------|
| 200 OK | Server information returned |

This endpoint MUST NOT require authentication and MUST always return 200.

## 4. Archive Format

Archives are binary containers for transporting sets of objects. Two variants exist:

### 4.1 Envelope (Uncompressed Header)

The envelope is always uncompressed and has a fixed structure:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Magic | `"arxa"` (0x61727861) for .arx, `"arxs"` (0x61727873) for .sar |
| 4 | 2 | Compression | Big-endian u16 (see Section 4.2) |
| 6 | 64 | Index Hash | Raw 64-byte SHA-512 hash of the index object |
| 70 | variable | Index Body | UTF-8 serialized index body (see Section 2.5) |
| 70+N | 1 | Separator | Null byte (0x00) |
| 71+N | variable | Body | Optionally compressed archive body (see Section 4.3) |

### 4.2 Compression Types

| Value (u16 BE) | Name | Description |
|----------------|------|-------------|
| 0 | None | No compression |
| 4 | Gzip | Gzip compression |
| 8 | Deflate | Raw DEFLATE |
| 16 | LZMA2 | LZMA2/XZ compression |
| 32 | Zstd | Zstandard compression |

### 4.3 Archive Body

The body (after decompression if applicable) contains:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | Entry Count | Big-endian u64: number of entries |

Followed by an **entry header table** with `Entry Count` entries, each 80 bytes:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 64 | Hash | Raw 64-byte SHA-512 hash |
| 64 | 8 | Offset | Big-endian u64: byte offset into the data section |
| 72 | 8 | Length | Big-endian u64: total byte length of this entry's data |

Followed by the **data section** containing all entry data concatenated. Each entry's data is the complete stored object (header prefix + null + body), as described in Section 2.2.

### 4.4 .arx vs .sar

- **`.arx`** (magic: `"arxa"`): Contains ALL objects reachable from the index — the complete tree of blobs, trees, and the index itself.
- **`.sar`** (magic: `"arxs"`): Contains a SUBSET of objects. Always includes all tree objects and the index, plus any additional requested blobs. Used for incremental transfers.

Both formats share identical binary layout; only the magic bytes and contents differ.

## 5. Workflows

### 5.1 Full Upload (Push)

1. Client commits a directory to local store, obtaining an index hash.
2. Client iterates all objects in local store.
3. For each object, client calls `PUT /v1/object/{hash}` with the raw bytes.

### 5.2 Deduplicated Upload (Push Archive)

1. Client commits a directory to local store, obtaining an index hash.
2. Client collects all object hashes reachable from the index.
3. Client calls `POST /v1/object/missing` with the hash list.
4. Server responds with hashes it does not have.
5. Client builds a `.sar` containing only missing objects (plus trees and index).
6. Client calls `POST /v1/archive/upload` with the `.sar`.

### 5.3 Full Download (Pull)

1. Client calls `GET /v1/object/{index_hash}` to download the index.
2. Client parses the index to get the root tree hash.
3. Client recursively walks the tree, calling `GET /v1/object/{hash}` for each tree and blob.

### 5.4 Archive Download

1. Client calls `GET /v1/archive/{index_hash}` to download the complete archive.
2. Client unpacks the archive into its local store.

### 5.5 Incremental Download

1. Client already has some objects from a previous version.
2. Client calls `GET /v1/index/{index_hash}/metadata` to discover all objects.
3. Client determines which objects it already has locally.
4. Client calls `POST /v1/archive/{index_hash}/supplemental` with the hashes it needs.
5. Client unpacks the `.sar` into its local store.

## 6. Error Handling

All error responses MUST include a plain-text body describing the error. Servers SHOULD use the following status codes consistently:

| Status Code | Usage |
|-------------|-------|
| 200 OK | Successful retrieval or idempotent operation |
| 201 Created | New object stored |
| 400 Bad Request | Malformed input (invalid hash, bad JSON, unknown compression) |
| 404 Not Found | Referenced object does not exist in the store |
| 500 Internal Server Error | Storage backend failure |

## 7. Content Addressing Guarantees

1. **Idempotency**: Uploading an object with a hash that already exists MUST be a no-op. The server MUST NOT return an error, as hash collisions in SHA-512 are negligibly improbable.

2. **Determinism**: For identical input directories, clients MUST produce identical index hashes. This requires:
   - Tree entries sorted lexicographically by filename
   - Consistent timestamp handling (timestamps are part of the index hash)
   - Consistent metadata handling

3. **Integrity**: Clients SHOULD verify that downloaded objects match their expected hash. Servers MAY perform this verification on upload.
