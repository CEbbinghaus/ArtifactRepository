# Store Format

Byte-level specification for objects stored in an ArtifactRepository store.

## Object Layout

Every stored object consists of a text header followed by the raw body:

```
┌──────────────────────────────┬─────────────────────┐
│ Header (null-terminated text)│ Body (raw bytes)    │
│ "{type} {size}\0"            │ {size} bytes        │
└──────────────────────────────┴─────────────────────┘
```

### Header

```
{type} {size}\0
```

| Field | Format | Values |
|-------|--------|--------|
| type | 4 ASCII bytes | `blob`, `tree`, `indx` |
| ` ` | 1 byte | `0x20` (space) |
| size | ASCII decimal digits | Body length in bytes |
| `\0` | 1 byte | `0x00` (null terminator) |

Examples:
```
"blob 0\0"     → 62 6C 6F 62 20 30 00           (7 bytes)
"tree 198\0"   → 74 72 65 65 20 31 39 38 00      (9 bytes)
"indx 512\0"   → 69 6E 64 78 20 35 31 32 00      (9 bytes)
```

## Hash Computation

All hashes use SHA-512 (64 bytes / 128 hex characters).

The hash is computed over the **entire stored representation** (header + body):

```
SHA-512( "{type} {size}\0{body}" )
```

Example — a 13-byte file containing `Hello, World!`:
```
SHA-512( "blob 13\0Hello, World!" )
         62 6C 6F 62 20 31 33 00 48 65 6C 6C 6F 2C 20 57 6F 72 6C 64 21
```

## Storage Key

Objects are keyed by their full 128-character lowercase hex hash.

On filesystem backends, the client uses a fan-out directory structure:

```
{store_root}/{hex[0..2]}/{hex[2..128]}
```

This creates 256 buckets (first byte of the hash). Example:

```
store/ab/1234567890abcdef...  (126 remaining hex chars)
```

The server's OpenDAL store uses the flat 128-char hex string as the key directly.

---

## Blob

Raw file content with no internal structure.

```
┌─────────────────┬──────────────────────┐
│ "blob {N}\0"    │ Raw file bytes       │
│ header          │ N bytes              │
└─────────────────┴──────────────────────┘
```

---

## Tree

A directory listing. The body is a concatenated sequence of entries with no count prefix and no separators between entries.

### Tree Entry

```
┌──────────┬──────┬──────────────┬──────┬──────────────┐
│ mode     │ 0x20 │ filename     │ 0x00 │ hash         │
│ 6 bytes  │ 1B   │ variable     │ 1B   │ 64 bytes     │
│ ASCII    │      │ UTF-8        │      │ raw binary   │
└──────────┴──────┴──────────────┴──────┴──────────────┘
```

| Field | Size | Format |
|-------|------|--------|
| mode | 6 bytes | ASCII octal (see below) |
| space | 1 byte | `0x20` |
| filename | variable | UTF-8, single path component |
| null | 1 byte | `0x00` |
| hash | 64 bytes | Raw SHA-512 binary (not hex) |

Entry size = `72 + len(filename)` bytes.

### Mode Values

| Mode | String | Meaning |
|------|--------|---------|
| Tree | `040000` | Subdirectory |
| Normal | `100644` | Regular file |
| Executable | `100755` | Executable file |
| SymbolicLink | `120000` | Symbolic link |

### Example

A tree with `hello.txt` (normal file) and `src/` (subdirectory):

```
             mode     sp  filename       nul  hash (64 bytes raw)
Entry 1:  [100644] [20] [hello.txt]    [00] [sha512 of hello.txt blob ...]
Entry 2:  [040000] [20] [src]          [00] [sha512 of src tree ........]
```

### Parsing

1. Scan forward for `0x00` to find the end of the mode+name segment
2. Split on the first `0x20` → mode string and filename
3. Read next 64 bytes as the raw hash
4. Repeat until end of body

---

## Index

Root pointer for an artifact. The body is UTF-8 text in a key-value format.

```
tree: {128-char hex hash}\n
timestamp: {RFC 3339 datetime}\n
{key}: {value}\n
...
\n
```

### Rules

- `tree` must be the first line
- `timestamp` must be the second line
- Additional metadata key-value pairs may follow (arbitrary order)
- Each line: `{key}: {value}\n`
- Body ends with `\n\n` (trailing blank line)
- The tree hash is 128 hex characters (not raw binary)

### Example

```
tree: cf83e135...a64f\n
timestamp: 2024-06-15T12:00:00+00:00\n
version: 1.0.0\n
\n
```

---

## Type Comparison

| | Blob | Tree | Index |
|-|------|------|-------|
| Type key | `blob` | `tree` | `indx` |
| Body format | Raw binary | Binary entries (mode + name + hash) | UTF-8 key-value text |
| Hash references | None | 64-byte raw binary per entry | 128-char hex text |
| Can contain | Nothing | Trees and Blobs | Points to one Tree |
