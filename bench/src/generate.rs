use std::io::Write;
use std::path::{Path, PathBuf};

use rand::Rng;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};

const MIN_FILE_SIZE: u64 = 1_024;        // 1 KB
const MAX_FILE_SIZE: u64 = 20_971_520;   // 20 MB
const NUM_SUBDIRS: usize = 100;
const MANIFEST_FILE: &str = ".bench-meta.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataManifest {
    pub seed: u64,
    pub duplicate_pct: u8,
    pub total_files: usize,
    pub unique_files: usize,
    pub total_bytes: u64,
    pub data_dir: PathBuf,
}

pub fn load_manifest(data_dir: &Path) -> Result<DataManifest, String> {
    let manifest_path = data_dir.join(MANIFEST_FILE);
    let content = std::fs::read_to_string(&manifest_path)
        .map_err(|e| format!("cannot read {}: {}", manifest_path.display(), e))?;
    serde_json::from_str(&content)
        .map_err(|e| format!("invalid manifest: {}", e))
}

#[allow(clippy::collapsible_if)]
pub fn generate_data(data_dir: &Path, target_bytes: u64, duplicate_pct: u8, seed: u64) -> DataManifest {
    // Check for existing data with matching parameters
    if let Ok(existing) = load_manifest(data_dir) {
        if existing.seed == seed
            && existing.duplicate_pct == duplicate_pct
            && (0.9..=1.1).contains(&(existing.total_bytes as f64 / target_bytes as f64))
        {
            println!("  Reusing existing data (matching parameters)");
            return existing;
        }
    }

    // Clean and recreate
    if data_dir.exists() {
        std::fs::remove_dir_all(data_dir).expect("cannot clean data dir");
    }
    std::fs::create_dir_all(data_dir).expect("cannot create data dir");

    let mut rng = SmallRng::seed_from_u64(seed);

    // Create subdirectories
    let subdirs: Vec<PathBuf> = (0..NUM_SUBDIRS)
        .map(|i| {
            let dir = data_dir.join(format!("dir_{:03}", i));
            std::fs::create_dir_all(&dir).expect("cannot create subdir");
            dir
        })
        .collect();

    // Phase 1: Generate unique files until we hit target * (1 - dup_pct/100)
    let unique_target = target_bytes * (100 - duplicate_pct as u64) / 100;
    let mut unique_files: Vec<PathBuf> = Vec::new();
    let mut unique_bytes: u64 = 0;
    let mut file_idx: usize = 0;

    println!("  Generating unique files (target: {} bytes)...", unique_target);

    while unique_bytes < unique_target {
        let size = random_file_size(&mut rng);
        let subdir_idx = rng.random_range(0..NUM_SUBDIRS);
        let path = subdirs[subdir_idx].join(format!("file_{:06}.bin", file_idx));

        write_random_file(&path, size, &mut rng);

        unique_files.push(path);
        unique_bytes += size;
        file_idx += 1;

        if file_idx.is_multiple_of(1000) {
            let pct = (unique_bytes as f64 / unique_target as f64 * 100.0).min(100.0);
            print!("\r  Unique: {} files, {:.1}%  ", file_idx, pct);
            std::io::stdout().flush().ok();
        }
    }
    println!("\r  Unique: {} files, {} bytes          ", unique_files.len(), unique_bytes);

    // Phase 2: Create duplicates by copying random files
    let dup_target = target_bytes.saturating_sub(unique_bytes);
    let mut dup_count: usize = 0;
    let mut dup_bytes: u64 = 0;

    if duplicate_pct > 0 && !unique_files.is_empty() {
        println!("  Generating duplicates (target: {} bytes)...", dup_target);

        while dup_bytes < dup_target {
            let source_idx = rng.random_range(0..unique_files.len());
            let source = &unique_files[source_idx];
            let source_size = std::fs::metadata(source).expect("cannot stat source").len();

            let subdir_idx = rng.random_range(0..NUM_SUBDIRS);
            let dest = subdirs[subdir_idx].join(format!("file_{:06}.bin", file_idx));

            std::fs::copy(source, &dest).expect("cannot copy file");

            dup_bytes += source_size;
            dup_count += 1;
            file_idx += 1;

            if dup_count.is_multiple_of(500) {
                let pct = (dup_bytes as f64 / dup_target as f64 * 100.0).min(100.0);
                print!("\r  Duplicates: {} files, {:.1}%  ", dup_count, pct);
                std::io::stdout().flush().ok();
            }
        }
        println!("\r  Duplicates: {} files, {} bytes          ", dup_count, dup_bytes);
    }

    let total_files = unique_files.len() + dup_count;
    let total_bytes = unique_bytes + dup_bytes;

    let manifest = DataManifest {
        seed,
        duplicate_pct,
        total_files,
        unique_files: unique_files.len(),
        total_bytes,
        data_dir: data_dir.to_path_buf(),
    };

    // Write manifest
    let manifest_path = data_dir.join(MANIFEST_FILE);
    let content = serde_json::to_string_pretty(&manifest).expect("cannot serialize manifest");
    std::fs::write(&manifest_path, content).expect("cannot write manifest");

    manifest
}

/// Generate a random file with the given size using reproducible RNG.
fn write_random_file(path: &Path, size: u64, rng: &mut SmallRng) {
    let mut file = std::fs::File::create(path).expect("cannot create file");
    let mut remaining = size;
    let mut buf = [0u8; 65536];

    while remaining > 0 {
        let chunk = remaining.min(buf.len() as u64) as usize;
        rng.fill(&mut buf[..chunk]);
        file.write_all(&buf[..chunk]).expect("cannot write file");
        remaining -= chunk as u64;
    }
}

/// Log-normal file size distribution between MIN and MAX.
/// Weighted toward smaller files (median ~500KB) to produce many files.
fn random_file_size(rng: &mut SmallRng) -> u64 {
    // Box-Muller transform for normal distribution
    let u1: f64 = rng.random_range(0.0001f64..1.0);
    let u2: f64 = rng.random_range(0.0001f64..1.0);
    let normal = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();

    // Log-normal: mean=13 (exp(13)≈442KB), stddev=1.5
    let log_size = 13.0 + 1.5 * normal;
    let size = log_size.exp() as u64;

    size.clamp(MIN_FILE_SIZE, MAX_FILE_SIZE)
}

/// Apply a small modification to a subset of files in the data directory.
/// Returns the paths of modified files and the number of new files added.
#[allow(clippy::collapsible_if)]
pub fn apply_small_changes(data_dir: &Path, change_pct: u8, seed: u64) -> (Vec<PathBuf>, usize) {
    let mut rng = SmallRng::seed_from_u64(seed.wrapping_add(9999));
    let mut modified = Vec::new();
    let mut added = 0;

    // Collect all .bin files
    let mut all_files: Vec<PathBuf> = Vec::new();
    collect_bin_files(data_dir, &mut all_files);

    let num_to_modify = (all_files.len() * change_pct as usize / 100).max(1);

    // Modify existing files (change random bytes)
    for _ in 0..num_to_modify.min(all_files.len()) {
        let idx = rng.random_range(0..all_files.len());
        let path = &all_files[idx];

        if let Ok(mut data) = std::fs::read(path) {
            if !data.is_empty() {
                let pos = rng.random_range(0..data.len());
                data[pos] = data[pos].wrapping_add(1);
                std::fs::write(path, &data).ok();
                modified.push(path.clone());
            }
        }
    }

    // Add a few new small files
    let new_count = (num_to_modify / 5).max(1);
    let next_idx = all_files.len();
    for i in 0..new_count {
        let subdir = data_dir.join(format!("dir_{:03}", rng.random_range(0..NUM_SUBDIRS)));
        if subdir.exists() {
            let path = subdir.join(format!("file_{:06}.bin", next_idx + i));
            let size = rng.random_range(MIN_FILE_SIZE..MIN_FILE_SIZE * 10);
            write_random_file(&path, size, &mut rng);
            modified.push(path);
            added += 1;
        }
    }

    (modified, added)
}

fn collect_bin_files(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_bin_files(&path, out);
            } else if path.extension().is_some_and(|e| e == "bin") {
                out.push(path);
            }
        }
    }
}
