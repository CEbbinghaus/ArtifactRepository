# ArtifactRepository V1 Extension: Zip Content-Addressing

**Status:** Draft\
**Version:** 1.0.0\
**Extends:** ArtifactRepository V1 HTTP API Specification v1.0.0\
**Extension Identifier:** `zip-content-addressing`

## 1. Introduction

This document defines an optional extension to the ArtifactRepository V1 specification that enables transparent content-addressing of zip archive contents. When active, zip files are decomposed into their constituent files and stored as individual content-addressed objects rather than opaque blobs.

### 1.1 Motivation

Many artifact workflows involve zip files whose contents overlap significantly with other artifacts or files already present in the store. Without this extension, each zip file is stored as a single blob, preventing deduplication of its internal contents.

This extension enables:

- Deduplication of files within a zip against all other files in the store
- Deduplication across multiple zip files (shared assets, libraries, etc.)
- Deduplication between zip contents and top-level directory files
- Incremental transfers when only a subset of a zip's contents change

### 1.2 Conventions

The key words "MUST", "MUST NOT", "SHOULD", "SHOULD NOT", and "MAY" are interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

All terminology from the base V1 specification applies unless overridden by this document.

## 2. Object Model Extension

### 2.1 Zip File Mode

This extension defines a new tree entry mode:

| Mode | Name | Description |
|------|------|-------------|
| `"100646"` | Zip | Content-addressed zip archive |

A tree entry with mode `100646` indicates that the file at this path is a zip archive whose contents have been decomposed into the object store. The entry's hash field MUST reference a **tree object** (not a blob). This tree represents the virtual directory structure of the zip file's internal contents.

### 2.2 Zip Content Tree

The tree referenced by a zip-mode entry follows the standard tree format (V1 spec Section 2.4). Files within the zip are stored as blob objects; subdirectories within the zip are stored as tree objects. Standard hash computation and deduplication rules apply — any blob that shares content with another object in the store (whether from the filesystem, another zip, or any other source) shares storage.

**Example:** A directory containing a regular file and a zip file:

```
index
└── tree (root)
    ├── 100644 readme.txt     → blob (abc123...)
    ├── 100755 build.sh       → blob (def456...)
    └── 100646 assets.zip     → tree (789abc...)
                                  ├── 100644 logo.png     → blob (aaa111...)
                                  ├── 100644 icon.png     → blob (bbb222...)
                                  └── 040000 fonts/       → tree (ccc333...)
                                      └── 100644 main.ttf → blob (ddd444...)
```

If `readme.txt` at the top level has identical content to a file inside the zip, they share the same blob hash. No duplicate data is stored.

### 2.3 Zip Content Trees Are Regular Trees

Zip content trees are indistinguishable from regular directory trees at the object level. They follow identical serialization, hashing, and sorting rules. The ONLY difference is the mode (`100646`) on the parent tree entry that references them. This means:

- Existing servers can store and serve zip content trees without any code changes
- Existing archive formats (.arx/.sar) transport zip content trees without modification
- The `POST /v1/object/missing` endpoint works unchanged
- The extension is primarily a **client-side concern** for commit and restore operations

## 3. Client Behavior

### 3.1 Detection

When committing a directory, clients supporting this extension MUST detect zip files using the **local file header magic bytes**: the first 4 bytes of the file MUST match `PK\x03\x04` (0x504B0304).

Files with a `.zip` extension that do not begin with these magic bytes MUST be stored as regular blobs (mode `100644` or `100755`). Files without a `.zip` extension that DO begin with the magic bytes SHOULD be decomposed (the magic bytes are authoritative, not the extension).

### 3.2 Decomposition (Commit)

When a zip file is detected during commit, the client MUST:

1. Open the zip archive and enumerate all entries.
2. For each file entry, read the decompressed contents and store as a blob.
3. For each directory entry, create a tree object.
4. Build the complete tree representing the zip's internal structure, following the same lexicographic sort and deterministic hash rules as regular trees.
5. Create a tree entry in the parent directory with mode `100646` and the zip content tree's hash.

The resulting tree hash is deterministic: two zip files with identical file contents (regardless of compression method, timestamps, comments, or entry ordering) produce the same tree hash.

### 3.3 Reconstruction (Restore / Extract)

When restoring or extracting, clients supporting this extension MUST:

1. Recognize tree entries with mode `100646`.
2. Retrieve the referenced tree and all descendant objects.
3. Reconstruct a zip file at the entry's path from the tree contents.
4. Add all blob entries to the zip as file entries.
5. Add all tree entries as directory entries in the zip.

**Reconstruction parameters:**

| Property | Value |
|----------|-------|
| Compression | Deflate (default) |
| Timestamps | Current system time |
| Entry ordering | Lexicographic (matching tree sort) |
| File permissions | Entries with mode `100755` SHOULD set the Unix executable bit in the zip external attributes |

### 3.4 Non-Preservation Notice

The reconstructed zip is **NOT** guaranteed to be byte-identical to the original. The following properties of the original zip are not preserved:

- Per-entry compression methods (all entries use Deflate)
- File and archive comments
- Extra field data (extended timestamps, Unicode paths, etc.)
- Original entry timestamps
- Original entry ordering
- Data descriptors and zip64 extended information layout
- Compression level

This is acceptable because ArtifactRepository is a content-addressed store — it preserves the **semantic content** (file data and directory structure), not the container format.

### 3.5 Nested Zip Files

If a zip file contains other zip files as entries, the inner zip files SHOULD also be decomposed recursively. Each nested zip becomes a `100646` tree entry within its parent zip's content tree.

Implementations MUST enforce a maximum nesting depth to prevent zip bombs. The minimum supported depth MUST be 8 levels. Implementations MAY support deeper nesting.

If the maximum depth is exceeded, the inner zip at the depth limit MUST be stored as a regular blob (mode `100644`).

### 3.6 Encrypted Entries

If a zip file contains any encrypted entries, the entire zip file MUST be stored as a regular blob (mode `100644` or `100755`). Partial decomposition of partially-encrypted zips is NOT supported in this version of the extension.

### 3.7 Empty Zip Files

A zip file with no entries produces an empty tree (zero entries, empty body). This is a valid tree object with a deterministic hash.

## 4. Server Behavior

### 4.1 Capability Advertisement

Servers that are aware of this extension MUST include `"zip-content-addressing"` in their capabilities response (V1 spec Section 3.8, `extensions` array).

### 4.2 Metadata Endpoint

When the metadata endpoint (V1 spec Section 3.7) encounters a tree entry with mode `100646`, it MUST include:

- The entry with mode `"100646"` in the parent tree's entries
- The referenced zip content tree in the `objects` map
- All descendant tree and blob objects recursively

This allows clients to discover the full structure of zip contents via metadata without downloading any object data.

### 4.3 Storage

Servers require **no special storage handling** for this extension. Zip content trees are regular tree objects; zip file contents are regular blob objects. All existing storage, retrieval, archiving, and deduplication mechanisms work unchanged.

## 5. Compatibility

### 5.1 Unsupporting Clients

Clients that do not support this extension and encounter a tree entry with mode `100646`:

- SHOULD emit a warning indicating an unrecognized mode was encountered
- MAY skip the entry and continue processing remaining entries
- MUST NOT crash or abort the entire restore/extract operation
- MAY treat the entry as a regular directory (creating a filesystem directory from the zip content tree) as a best-effort fallback

### 5.2 Unsupporting Servers

Since the extension uses standard object types (trees and blobs) with standard hashing and storage, servers that do not support this extension can still:

- Store all objects uploaded by supporting clients
- Serve all objects to supporting clients
- Generate archives containing zip content trees
- Generate supplemental archives for incremental transfers

The only limitation is the metadata endpoint, which will report the `100646` mode as-is. Unsupporting servers MAY omit the mode from the `TreeDirEntry` enum validation or reject it.

### 5.3 Mixed Environments

In environments where some clients support the extension and others do not:

- Supporting clients decompose zips on commit → all clients can download the individual objects
- Unsupporting clients will encounter `100646` entries on restore and should fall back gracefully
- The same index hash works for both supporting and unsupporting clients

## 6. Security Considerations

### 6.1 Zip Bombs

Malicious zip files can contain entries that decompress to extremely large sizes (zip bombs). Implementations MUST enforce limits on:

| Limit | Minimum Recommended |
|-------|-------------------|
| Maximum decompressed entry size | 4 GiB |
| Maximum total decompressed size per zip | 64 GiB |
| Maximum number of entries per zip | 1,000,000 |
| Maximum nesting depth | 8 levels |
| Maximum path length per entry | 4096 bytes |

These limits are implementation-defined. When a limit is exceeded, the zip file SHOULD be stored as a regular blob with a warning.

### 6.2 Path Traversal

Zip entries may contain path components like `../` that attempt directory traversal. Implementations MUST sanitize entry paths:

- Remove leading `/` or `\`
- Reject or sanitize entries containing `..` path components
- Reject entries with null bytes in paths
- Normalize path separators to `/`

### 6.3 Symlink Entries

Zip files may contain symbolic link entries. These SHOULD be stored with mode `120000` (symbolic link) in the content tree if the client supports symbolic links, or skipped with a warning otherwise.

## 7. Future Considerations

The following are explicitly out of scope for version 1.0.0 of this extension but may be addressed in future versions:

- **Metadata preservation**: Storing original compression methods, timestamps, and comments in a sidecar object
- **Partial encryption**: Decomposing only the unencrypted entries of a partially-encrypted zip
- **Other archive formats**: Extending the approach to tar, 7z, rar, etc. (each would define its own mode value)
- **Configurable reconstruction**: Allowing clients to specify compression method and level for reconstructed zips
