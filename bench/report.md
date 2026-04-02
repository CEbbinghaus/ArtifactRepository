# ArtifactRepository Benchmark Report

**Date**: 2026-03-24T05:18:13Z
**System**: Windows, 22 cores
**Dataset**: 396 files (298 unique), 501.0 MB total
**Duplicates**: 20%
**Seed**: 42
**Compression**: zstd
**Runs**: 3
**Index hash**: `b02c230b968610eeec1d45a3b8d889032baf3c83cb653511bb81088ad9b9d469e04ab6425ac0bf38e8097b4aedccec3a39d8cf3208fc50fa46b065c38c4a0cd8`

## Results

| Step | Min | Median | Max | Input | Output | Throughput (median) | Verified |
|------|-----|--------|-----|-------|--------|---------------------|----------|
| commit | 712ms | 742ms | 764ms | 501.0 MB | 405.1 MB | 675.1 MB/s | — |
| restore | 1.43s | 1.50s | 1.53s | 405.1 MB | 501.0 MB | 270.5 MB/s | ✅ |
| restore+validate | 2.76s | 2.84s | 3.07s | 405.1 MB | 501.0 MB | 142.7 MB/s | ✅ |
| pack | 1.74s | 1.77s | 1.81s | 405.1 MB | 405.1 MB | 229.2 MB/s | — |
| unpack | 1.48s | 1.57s | 1.77s | 405.1 MB | 405.1 MB | 257.8 MB/s | — |
| archive | 861ms | 1.04s | 1.05s | 501.0 MB | 405.1 MB | 483.3 MB/s | — |
| extract | 1.53s | 1.61s | 1.63s | 405.1 MB | 501.0 MB | 251.0 MB/s | ✅ |
| push | 6.63s | 6.63s | 6.63s | 405.1 MB | — | 61.1 MB/s | — |
| pull | 14.86s | 14.86s | 14.86s | 405.1 MB | — | 27.3 MB/s | — |
| push-archive (all exist) | 1.54s | 1.54s | 1.54s | 405.1 MB | — | 262.6 MB/s | — |
| GET /v1/archive | 3.24s | 3.24s | 3.24s | 405.1 MB | 405.1 MB | 125.1 MB/s | — |
| GET /v1/zip | 16.90s | 16.90s | 16.90s | 405.1 MB | 501.2 MB | 24.0 MB/s | — |
| GET /v1/index/metadata | 592ms | 592ms | 592ms | 0 B | — | — | — |
| POST /v1/object/missing | 102ms | 102ms | 102ms | 52.5 KB | — | 0.5 MB/s | — |
| POST /v1/archive/upload | 4.51s | 4.51s | 4.51s | 405.1 MB | — | 89.8 MB/s | — |

## Incremental / Supplemental Test

**Changes**: 19 files modified, 3 files added
**Original hash**: `b02c230b968610eeec1d45a3b8d889032baf3c83cb653511bb81088ad9b9d469e04ab6425ac0bf38e8097b4aedccec3a39d8cf3208fc50fa46b065c38c4a0cd8`
**Modified hash**: `b81989ebc94513761e3521153c4aeebbddd8ca3bf13bbd2ae93efebedc92548a19d426876faf179b7ea7bea7cd49d5b95aef7657b2cb6287272b3812cbbb67e5`

| Operation | Time |
|-----------|------|
| Re-commit (modified data) | 1.00s |
| push-archive (full, to empty server) | 8.95s |
| push-archive (incremental, original on server) | 2.02s |

**Incremental speedup**: 4.4x faster than full push

