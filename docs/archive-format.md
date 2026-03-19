# Archive Format

Byte-level specification for `.arx` archive files.

## Overview

An `.arx` file has two regions: an **uncompressed envelope** and an optionally **compressed body**.

```
┌─────────────────────────────────────────────────────────────────┐
│                    UNCOMPRESSED ENVELOPE                        │
│ ┌────────┬───────────┬──────────────┬──────────────────┬──────┐ │
│ │ Magic  │ Compress. │ SHA-512 Hash │ Index (UTF-8 KV) │ 0x00 │ │
│ │ 4B     │ 2B u16 BE │ 64B raw      │ variable         │ 1B   │ │
│ └────────┴───────────┴──────────────┴──────────────────┴──────┘ │
├─────────────────────────────────────────────────────────────────┤
│                    BODY (may be compressed)                     │
│ ┌───────────┬──────────────────────────┬──────────────────────┐ │
│ │ Count     │ Header Table             │ Entry Data           │ │
│ │ 8B u64 BE │ N × 80B entries          │ concatenated objects │ │
│ └───────────┴──────────────────────────┴──────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

All multi-byte integers are **big-endian**.

---

## Envelope

### Magic (4 bytes)

```
Offset 0:  61 72 78 61  ("arxa")
```

### Compression Method (2 bytes, u16 BE)

| Variant | Value | Bytes | Algorithm |
|---------|-------|-------|-----------|
| None | 0 | `00 00` | No compression |
| Gzip | 4 | `00 04` | Gzip |
| Deflate | 8 | `00 08` | Raw DEFLATE |
| LZMA2 | 16 | `00 10` | LZMA2 (multi-threaded) |
| Zstd | 32 | `00 20` | Zstandard (level 3, multi-threaded) |

### Hash (64 bytes)

Raw SHA-512 digest of the archive's associated index. Binary, not hex-encoded.

### Index (variable length)

UTF-8 text containing the serialized Index object body (see [Store Format — Index](store-format.md#index)). Key-value pairs terminated by `\n\n`.

### Null Separator (1 byte)

`0x00` — marks the end of the Index field. The body stream begins immediately after.

---

## Body

Everything after the null separator. If compression is not `None`, this entire region is compressed with the specified algorithm. The decompressed content has the following layout:

### Entry Count (8 bytes)

```
Offset 0:  u64 BE — number of entries N
```

### Header Table (N × 80 bytes)

Starts at byte 8 of the decompressed body. Each entry:

```
┌──────────────────┬──────────────────┬──────────────────┐
│ Hash             │ Index            │ Length           │
│ 64 bytes raw     │ 8 bytes u64 BE   │ 8 bytes u64 BE   │
└──────────────────┴──────────────────┴──────────────────┘
```

| Field | Size | Description |
|-------|------|-------------|
| hash | 64 bytes | SHA-512 of the entry's data |
| index | 8 bytes | Byte offset into the entry data region |
| length | 8 bytes | Byte count of this entry's data |

### Entry Data Region

Starts at byte `8 + (N × 80)` of the decompressed body. Raw object data (header + body, as stored) concatenated with no padding or delimiters.

Each entry's data is located at `entry_data_start + header_entry.index` and is `header_entry.length` bytes long.

Entries are contiguous — sorted by `index`, each entry starts where the previous one ends.

---

## Integrity

Each entry's data is verified against its header hash during deserialization:

```
SHA-512(entry_data) == header_entry.hash
```

Entries that fail verification cause the unpack to fail.

---

## Serialization Steps

1. Write magic: `61 72 78 61`
2. Write compression as u16 BE
3. Write hash: 64 raw bytes
4. Write serialized Index body (UTF-8 text)
5. Write null separator: `00`
6. Open compression stream (if applicable)
7. Write entry count as u64 BE
8. For each entry: write hash (64B) + index (u64 BE) + length (u64 BE)
9. For each entry: write raw object data
10. Finalize compression stream

## Deserialization Steps

1. Read 4 bytes → verify magic `arxa`
2. Read 2 bytes → u16 BE → compression variant
3. Read 64 bytes → hash
4. Read until `0x00` → parse as Index
5. Open decompression stream
6. Read 8 bytes → u64 BE → entry count
7. Read N × 80 bytes → header table
8. Sort header entries by index ascending
9. For each entry (sorted): read `length` bytes, verify SHA-512 matches hash
