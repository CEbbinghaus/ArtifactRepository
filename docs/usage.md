# Usage Guide

## Client

### Synopsis

```
client [OPTIONS] --store <PATH> <COMMAND>
```

### Global Options

| Option | Short | Description |
|--------|-------|-------------|
| `--store <PATH>` | `-s` | Path to the local artifact store (created if missing) |
| `--debug` | `-d` | Increase debug verbosity (repeatable) |

### Commands

#### commit

Recursively hash a directory into the store, producing an Index.

```bash
client -s ./store commit --directory ./build-output/
```

| Option | Short | Description |
|--------|-------|-------------|
| `--directory <PATH>` | `-d` | Directory to commit |

Prints the 128-character hex Index hash to stdout.

#### restore

Recreate a directory from a stored Index.

```bash
client -s ./store restore --index <HASH> --directory ./output/
```

| Option | Short | Description |
|--------|-------|-------------|
| `--directory <PATH>` | `-d` | Target directory (must not exist or be empty) |
| `--index <HASH>` | `-i` | Index hash to restore |
| `--validate` | | Re-hash restored files to verify integrity |

#### cat

Print a raw object's body to stdout.

```bash
client -s ./store cat --hash <HASH>
```

| Option | Description |
|--------|-------------|
| `--hash <HASH>` | Hash of the object to read |

#### pack

Create an `.arx` archive from a stored Index and all its referenced objects.

```bash
client -s ./store pack --index <HASH> --file output.arx --compression zstd
```

| Option | Description |
|--------|-------------|
| `--index <HASH>` | Index hash to pack |
| `--file <PATH>` | Output archive path (must not exist) |
| `--compression <ALG>` | `none`, `gzip`, `deflate`, `lzma2`, or `zstd` |

#### unpack

Extract an `.arx` archive into the local store.

```bash
client -s ./store unpack --file archive.arx
```

| Option | Description |
|--------|-------------|
| `--file <PATH>` | Archive file to unpack |

#### push

Upload objects from the local store to a remote server.

```bash
# Push all objects
client -s ./store push --url http://server:1287

# Push a single object
client -s ./store push --url http://server:1287 --index <HASH>
```

| Option | Description |
|--------|-------------|
| `--url <URL>` | Server base URL |
| `--index <HASH>` | (Optional) Specific object to push. If omitted, pushes all objects. |

#### push-archive

Push objects to a remote server with deduplication. Only uploads objects the server doesn't already have, packaged as a supplemental archive (`.sar`).

```bash
client -s ./store push-archive --url http://server:1287 --index <HASH> --compression zstd
```

| Option | Description |
|--------|-------------|
| `--url <URL>` | Server base URL |
| `--index <HASH>` | Index hash to push |
| `--compression <ALG>` | (Optional) `none`, `gzip`, `deflate`, `lzma2`, or `zstd` |

**Dedup upload workflow:**

1. Commit a directory to the local store → get an index hash
2. Run `push-archive --url http://server:1287 --index <HASH>`
3. Client collects all object hashes for the index, asks the server which are missing
4. Client builds a `.sar` containing only the missing objects, uploads it

#### pull

Download an Index and all referenced objects from a remote server.

```bash
client -s ./store pull --url http://server:1287 --index <HASH>
```

| Option | Description |
|--------|-------------|
| `--url <URL>` | Server base URL |
| `--index <HASH>` | Index hash to pull |

---

## Server

### Synopsis

```
server [OPTIONS] <STORE>
```

### Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `<STORE>` | | (required) | Path to the store directory (created if missing) |
| `--port <PORT>` | `-p` | `1287` | TCP port to listen on |
| `--verbose` | `-v` | | Increase verbosity (repeatable) |

### Starting the Server

```bash
server ./store -p 8080
```

The server binds to `0.0.0.0:<port>` and prints the listen address.

### API

All V1 endpoints are prefixed with `/v1/`. See [docs/rfc-v1-api.md](rfc-v1-api.md) for the full protocol specification and [docs/openapi-v1.yaml](openapi-v1.yaml) for the OpenAPI schema.

#### PUT /v1/object/{hash}

Store an object.

**Request:**
- `Object-Type` header: `blob`, `tree`, or `indx`
- `Object-Size` header: body size in bytes
- Body: raw object data

**Responses:**
- `201 Created` — stored successfully
- `200 OK` — object already exists (idempotent, see [Idempotent PUT Semantics](store-format.md))
- `400 Bad Request` — missing or invalid headers

#### GET /v1/object/{hash}

Retrieve an object.

**Response:**
- `200 OK` — body is the raw object data (streamed)
  - `Object-Type` and `Object-Size` headers set on response
- `404 Not Found` — object does not exist

Transport compression (gzip, zstd, brotli, deflate) is negotiated via standard `Accept-Encoding` headers.

#### POST /v1/object/missing

Determine which objects the server does not have.

**Request:**
```json
{"hashes": ["aabb...", "ccdd..."]}
```

**Response (`200 OK`):**
```json
{"missing": ["ccdd..."]}
```

#### GET /v1/archive/{index_hash}

Download a complete `.arx` archive containing all objects reachable from the index.

**Query parameters:**
- `compression`: `none`, `gzip`, `deflate`, `lzma2`, `zstd` (default: `zstd`)

**Response:**
- `200 OK` — body is the `.arx` archive (streamed)
- `404 Not Found` — index does not exist

#### POST /v1/archive/{index_hash}/supplemental

Build and return a `.sar` file containing only the requested objects (plus all tree objects and the index).

**Request:**
```json
{"hashes": ["aabb...", "ccdd..."]}
```

**Query parameters:**
- `compression`: `none`, `gzip`, `deflate`, `lzma2`, `zstd` (default: `zstd`)

**Response:**
- `200 OK` — body is the `.sar` archive (streamed)
- `404 Not Found` — index does not exist

#### POST /v1/archive/upload

Accept an `.arx` or `.sar` archive body. Unpacks all objects into the server's store, skipping duplicates.

**Request:**
- Body: raw archive bytes

**Responses:**
- `200 OK` — `"Added N objects, skipped M"`
- `400 Bad Request` — invalid archive

#### GET /v1/index/{index_hash}/metadata

Returns JSON metadata about an index and its full tree structure.

**Response (`200 OK`):**
```json
{
  "index": {
    "hash": "...", "tree": "...",
    "timestamp": "2025-01-15T12:00:00Z",
    "metadata": {}
  },
  "objects": {
    "hash...": {"type": "blob", "size": 1234},
    "hash...": {"type": "tree", "entries": {"file.txt": {"mode": "100644", "hash": "..."}}}
  }
}
```

- `404 Not Found` — index does not exist

#### GET /v1/zip/{index_hash} *(non-spec)*

Download the tree contents as a standard `.zip` file. This endpoint is provided for convenience but is not part of the V1 specification.
