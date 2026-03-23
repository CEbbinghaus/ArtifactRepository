# ArtifactRepository Benchmark Report

**Date**: 2026-03-23T02:34:18Z
**System**: Ubuntu 22.04.5 LTS, 22 cores, 31 GB RAM
**Dataset**: 785 files (623 unique), 1.0 GB total
**Duplicates**: 20%
**Seed**: 42
**Compression**: zstd
**Runs**: 3
**Index hash**: `d14fb0fe2ae199a3ac63a7584b37a77055955150c4a36cad463736530819d3b5e5edf12f5b213b3eabd4d7df1564ee2f3fb7d93a3695bfd178ff37a54fac15b9`

## Results

| Step | Min | Median | Max | Input | Output | Throughput (median) | Verified |
|------|-----|--------|-----|-------|--------|---------------------|----------|
| commit | 1.48s | 1.49s | 1.63s | 1.0 GB | 800.4 MB | 671.9 MB/s | — |
| restore | 1.64s | 1.67s | 1.75s | 800.4 MB | 1.0 GB | 479.3 MB/s | ✅ |
| restore+validate | 4.11s | 4.31s | 4.32s | 800.4 MB | 1.0 GB | 185.7 MB/s | ✅ |
| pack | 2.48s | 2.65s | 2.67s | 800.4 MB | 800.4 MB | 302.0 MB/s | — |
| unpack | 2.81s | 2.82s | 3.17s | 800.4 MB | 800.4 MB | 283.6 MB/s | — |
| archive | 1.70s | 1.99s | 3.29s | 1.0 GB | 800.4 MB | 503.5 MB/s | — |
| extract | 2.87s | 2.93s | 3.26s | 800.4 MB | 1.0 GB | 272.9 MB/s | ✅ |
| push | 13.09s | 13.09s | 13.09s | 800.4 MB | — | 61.1 MB/s | — |
| pull | 26.52s | 26.52s | 26.52s | 800.4 MB | — | 30.2 MB/s | — |
| push-archive (all exist) | 2.40s | 2.40s | 2.40s | 800.4 MB | — | 334.1 MB/s | — |
| GET /archive | 2.27s | 2.27s | 2.27s | 800.4 MB | 800.4 MB | 353.3 MB/s | — |
| GET /zip | 25.46s | 25.46s | 25.46s | 800.4 MB | 1.0 GB | 31.4 MB/s | — |
| GET /metadata | 341ms | 341ms | 341ms | 0 B | — | — | — |
| POST /missing | 67ms | 67ms | 67ms | 95.1 KB | — | 1.4 MB/s | — |
| POST /upload | 4.78s | 4.78s | 4.78s | 800.4 MB | — | 167.5 MB/s | — |

## Incremental / Supplemental Test

**Changes**: 39 files modified, 7 files added
**Original hash**: `d14fb0fe2ae199a3ac63a7584b37a77055955150c4a36cad463736530819d3b5e5edf12f5b213b3eabd4d7df1564ee2f3fb7d93a3695bfd178ff37a54fac15b9`
**Modified hash**: `7c2d0bae5db1acd910402379fefdb9e509636dc8a42000a09b337a6c3b53c5c22d8e8138966fa6081a886299958b8354d0fd7f562b8af069875a936123819801`

| Operation | Time |
|-----------|------|
| Re-commit (modified data) | 1.74s |
| push-archive (full, to empty server) | 24.01s |
| push-archive (incremental, original on server) | 3.40s |

**Incremental speedup**: 7.1x faster than full push

