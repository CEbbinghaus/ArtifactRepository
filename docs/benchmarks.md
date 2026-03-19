# Benchmarks

All benchmarks performed on the same machine with warm filesystem caches. Each test was run 3 times; the best time is reported. Dataset: **6.8 GB across 31,598 files**.

## Archive Creation (Directory → Archive)

| Tool | Time | Output Size | Compression Ratio |
|------|------|-------------|-------------------|
| tar (none) | 8.3s | 6.8 GB | 1:1 |
| **arx (zstd)** | **15.4s** | **2.1 GB** | **3.2:1** |
| arx (none) | 15.4s | 5.7 GB | 1.2:1 |
| zip (`zip -r`) | 6m 47s | 2.6 GB | 2.6:1 |
| tar.gz (`tar czf`) | 6m 57s | 2.6 GB | 2.6:1 |

arx with Zstd compression is **~27× faster** than zip/tar.gz and produces **19% smaller** archives.

Uncompressed tar is ~2× faster than arx because it performs no hashing or deduplication — it simply copies bytes.

arx+zstd and arx+none take nearly the same time because the bottleneck is SHA-512 hashing and I/O, not compression. Multi-threaded Zstd runs in parallel with the hashing work.

## Archive Extraction (Archive → Directory)

| Tool | Time |
|------|------|
| **arx extract** | **39s** |
| tar.gz (`tar xzf`) | 1m 07s |
| unzip | 1m 17s |

arx extraction is roughly **2× faster** than zip and tar.gz.

## Key Observations

- **Multi-threaded Zstd is essentially free.** Because hashing dominates wall-clock time, multi-threaded Zstd compression overlaps with I/O and adds negligible latency compared to no compression at all.
- **Content-addressable deduplication pays off.** The arx format stores each unique blob once. For datasets with repeated content, archive sizes are significantly smaller than tar/zip equivalents.
- **zip and tar.gz are single-threaded.** Their compression (deflate/gzip) cannot use multiple cores, which is the primary reason for the ~27× speed gap. A fairer comparison would be against `pigz` (parallel gzip) or similar tools.
- **exists() before put_object() is critical.** Removing the stat check before writes caused a 17s → 253s regression on populated stores. Filesystem stat is far cheaper than unconditional file writes.
