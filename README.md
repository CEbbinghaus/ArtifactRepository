# ArtifactRepository

A content-addressed artifact storage and distribution system built on SHA-512 Merkle trees. Create, store, and distribute file-based artifacts with automatic deduplication — if two artifacts share files, only one copy is stored.

## How It Works

Artifacts are directory trees hashed and stored as three object types — **Blobs** (files), **Trees** (directories), and **Indexes** (root pointers with metadata). Objects are identified by their SHA-512 hash, so identical content is automatically deduplicated across artifacts.

```
Index (root)
  └─ Tree (directory)
       ├─ Blob (file)
       ├─ Blob (file)
       └─ Tree (subdirectory)
            └─ Blob (file)
```

## Quick Start

```bash
# Commit a directory into the local store
client -s ./store commit --directory ./my-build-output/

# Pack into a distributable archive
client -s ./store pack --index <hash> --file output.arx --compression zstd

# Unpack an archive into a store
client -s ./store unpack --file output.arx

# Restore files from a store
client -s ./store restore --index <hash> --directory ./output/
```

## Components

| Crate | Description |
|-------|-------------|
| **common** | Shared library — object model, store abstraction, archive format |
| **client** | CLI tool — commit, restore, pack, unpack, push, pull |
| **server** | HTTP API — content-addressed object storage with optional S3 backing |

## Documentation

- [Architecture](docs/architecture.md) — system design, data model, component interactions
- [Store Format](docs/store-format.md) — byte-level specification of stored objects
- [Archive Format](docs/archive-format.md) — byte-level specification of `.arx` archives
- [Usage Guide](docs/usage.md) — client CLI and server API reference
- [Benchmarks](docs/benchmarks.md) — performance measurements

## License

See [LICENSE](LICENSE).
