use std::fs::{self, File};
use std::hint::black_box;
use std::io::{Read, Write};
use std::path::Path;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use opendal::{services, Operator};
use rand::Rng;
use sha2::{Digest, Sha256, Sha512};
use tempdir::TempDir;
use tokio::runtime::Runtime;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const GIB: usize = 1024 * MIB;
const TEN_GIB: usize = 10 * GIB;

const CHUNK_SIZE: usize = 64 * KIB;
const MAGIC_HEADER: &[u8] = b"\xDEADBEEF";

struct FileAccessFixture {
    root: TempDir,
    file_names: Vec<String>,
    total_bytes: usize,
    operator: Operator,
    runtime: Runtime,
}

impl FileAccessFixture {
    fn path(&self) -> &Path {
        self.root.path()
    }
}

fn build_random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..size).map(|_| rng.random::<u8>()).collect()
}

fn write_file_with_repeated_chunk(path: &Path, size: usize, chunk: &[u8]) {
    let mut file = File::create(path).expect("create fixture file");
    let mut remaining = size;

    while remaining > 0 {
        let write_len = remaining.min(chunk.len());
        file.write_all(&chunk[..write_len]).expect("write fixture file");
        remaining -= write_len;
    }

    file.sync_all().expect("sync fixture file");
}

fn build_file_access_fixture() -> FileAccessFixture {
    let total_bytes: usize = 3 * GIB;
    let file_size = 64 * MIB;
    let file_count = total_bytes.div_ceil(file_size);

    let root = TempDir::new("file-access-bench").expect("create temp directory");

    let generation_chunk = build_random_data(file_size);

    let mut file_names = Vec::with_capacity(file_count);
    let mut remaining = total_bytes;

    for index in 0..file_count {
        let file_bytes = remaining.min(file_size);
        let file_name = format!("file-{index:04}.bin");

        write_file_with_repeated_chunk(&root.path().join(&file_name), file_bytes, &generation_chunk);

        file_names.push(file_name);
        remaining -= file_bytes;
    }

    let runtime = Runtime::new().expect("create tokio runtime");
    let operator = Operator::new(
        services::Fs::default().root(root.path().to_str().expect("temp path is valid utf-8")),
    )
    .expect("create fs operator")
    .finish();

    FileAccessFixture {
        root,
        file_names,
        total_bytes,
        operator,
        runtime,
    }
}

fn sha2_benchmarks(c: &mut Criterion) {
    let chunk: Vec<u8> = {
        let mut rng = rand::rng();
        (0..CHUNK_SIZE).map(|_| rng.random::<u8>()).collect()
    };
    let iterations = TEN_GIB / CHUNK_SIZE;

    let mut group = c.benchmark_group("hash-10GiB");
    group.throughput(Throughput::Bytes(TEN_GIB as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(200));

    group.bench_function(BenchmarkId::new("SHA-256", "10GiB"), |b| {
        b.iter(|| {
            let mut hasher = Sha256::new();
            for _ in 0..iterations {
                hasher.update(&chunk);
            }
            hasher.finalize()
        });
    });

    group.bench_function(BenchmarkId::new("SHA-512", "10GiB"), |b| {
        b.iter(|| {
            let mut hasher = Sha512::new();
            for _ in 0..iterations {
                hasher.update(&chunk);
            }
            hasher.finalize()
        });
    });

    group.finish();
}

fn file_access_benchmarks(c: &mut Criterion) {
    let fixture = build_file_access_fixture();
    let file_size = fixture.total_bytes / fixture.file_names.len();

    // --- Read ---
    let mut group = c.benchmark_group("file-access-read");
    group.throughput(Throughput::Bytes(fixture.total_bytes as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    group.bench_function(BenchmarkId::new("std", fixture.total_bytes), |b| {
        b.iter(|| {
            let mut total = 0usize;
            for name in &fixture.file_names {
                let mut buf = Vec::new();
                File::open(fixture.path().join(name))
                    .expect("open file")
                    .read_to_end(&mut buf)
                    .expect("read file");
                total += buf.len();
            }
            black_box(total)
        });
    });

    group.bench_function(BenchmarkId::new("opendal-fs", fixture.total_bytes), |b| {
        b.iter(|| {
            let total = fixture.runtime.block_on(async {
                let mut total = 0usize;
                for name in &fixture.file_names {
                    total += fixture.operator.read(name).await.expect("read file").len();
                }
                total
            });
            black_box(total)
        });
    });
    group.finish();

    // --- Exists ---
    let mut group = c.benchmark_group("file-access-exists");
    group.throughput(Throughput::Elements(fixture.file_names.len() as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    group.bench_function(BenchmarkId::new("std", fixture.file_names.len()), |b| {
        b.iter(|| {
            let count = fixture.file_names.iter()
                .filter(|name| fixture.path().join(name).exists())
                .count();
            black_box(count)
        });
    });

    group.bench_function(BenchmarkId::new("opendal-fs", fixture.file_names.len()), |b| {
        b.iter(|| {
            let count = fixture.runtime.block_on(async {
                let mut count = 0usize;
                for name in &fixture.file_names {
                    if fixture.operator.exists(name).await.expect("stat file") {
                        count += 1;
                    }
                }
                count
            });
            black_box(count)
        });
    });
    group.finish();

    // --- Write ---
    let mut group = c.benchmark_group("file-access-write");
    group.throughput(Throughput::Bytes(fixture.total_bytes as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    let write_chunk = build_random_data(file_size);

    group.bench_function(BenchmarkId::new("std", fixture.total_bytes), |b| {
        b.iter(|| {
            for name in &fixture.file_names {
                let mut file = File::create(fixture.path().join(name)).expect("create file");
                file.write_all(&write_chunk).expect("write file");
                file.sync_all().expect("sync file");
            }
            black_box(fixture.total_bytes)
        });
    });

    group.bench_function(BenchmarkId::new("opendal-fs", fixture.total_bytes), |b| {
        b.iter(|| {
            fixture.runtime.block_on(async {
                for name in &fixture.file_names {
                    fixture
                        .operator
                        .write(name, write_chunk.clone())
                        .await
                        .expect("write file");
                }
            });
            black_box(fixture.total_bytes)
        });
    });
    group.finish();

    // --- Copy ---
    let copy_dest = fixture.path().join("copy-dest");

    let mut group = c.benchmark_group("file-access-copy");
    group.throughput(Throughput::Bytes(fixture.total_bytes as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    group.bench_function(BenchmarkId::new("std", fixture.total_bytes), |b| {
        b.iter(|| {
            fs::create_dir_all(&copy_dest).expect("create copy dir");
            let mut copied = 0usize;
            for name in &fixture.file_names {
                let data = fs::read(fixture.path().join(name)).expect("read file");
                let mut out = File::create(copy_dest.join(name)).expect("create file");
                out.write_all(MAGIC_HEADER).expect("write header");
                out.write_all(&data).expect("write data");
                copied += data.len();
            }
            fs::remove_dir_all(&copy_dest).expect("remove copy dir");
            black_box(copied)
        });
    });

    group.bench_function(BenchmarkId::new("opendal-fs", fixture.total_bytes), |b| {
        b.iter(|| {
            fixture.runtime.block_on(async {
                for name in &fixture.file_names {
                    let data = fixture.operator.read(name).await.expect("read file");
                    let dest = format!("copy-dest/{name}");
                    let mut out = Vec::with_capacity(MAGIC_HEADER.len() + data.len());
                    out.extend_from_slice(MAGIC_HEADER);
                    out.extend_from_slice(&data.to_vec());
                    fixture.operator.write(&dest, out).await.expect("write file");
                }
            });
            let _ = fs::remove_dir_all(&copy_dest);
            black_box(fixture.total_bytes)
        });
    });
    group.finish();
}

criterion_group!(benches, sha2_benchmarks, file_access_benchmarks);
criterion_main!(benches);