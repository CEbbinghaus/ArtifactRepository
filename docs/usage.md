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

#### PUT /object/{hash}

Store an object.

**Request:**
- `Object-Type` header: `blob`, `tree`, or `indx`
- `Object-Size` header: body size in bytes
- Body: raw object data

**Responses:**
- `201 Created` — stored successfully
- `200 OK` — object already exists (idempotent)
- `400 Bad Request` — missing or invalid headers

#### GET /object/{hash}

Retrieve an object.

**Response:**
- `200 OK` — body is the raw object data (streamed)
  - `Object-Type` and `Object-Size` headers set on response
- `404 Not Found` — object does not exist

Transport compression (gzip, zstd, brotli, deflate) is negotiated via standard `Accept-Encoding` headers.

#### GET /metadata/{index_hash}

Returns JSON metadata about an index.

**Response (`200 OK`):**
```json
{
  "files": [
    {"path": "src/main.rs", "hash": "...", "size": 1234, "mode": 33188}
  ],
  "objects": [
    {"hash": "...", "type": "blob", "size": 1234}
  ]
}
```

- `404 Not Found` — index does not exist

#### POST /missing

Determine which objects the server does not have.

**Request:**
```json
{"hashes": ["aabb...", "ccdd..."]}
```

**Response (`200 OK`):**
```json
{"missing": ["ccdd..."]}
```

#### POST /upload

Accept an `.arx` or `.sar` archive body. Unpacks all objects into the server's store, skipping duplicates.

**Request:**
- Body: raw archive bytes

**Responses:**
- `200 OK` — objects unpacked successfully
- `400 Bad Request` — invalid archive

#### POST /supplemental/{index_hash}

Build and return a `.sar` file containing only the requested objects (plus all tree objects and the index).

**Request:**
```json
{"hashes": ["aabb...", "ccdd..."]}
```

**Response:**
- `200 OK` — body is the `.sar` archive (streamed)
- `404 Not Found` — index does not exist
