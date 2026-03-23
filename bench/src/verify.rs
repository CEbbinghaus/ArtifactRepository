use std::collections::BTreeMap;
use std::path::Path;

/// Compare two directories recursively. Returns true if all files match.
/// Ignores .bench-meta.json manifest files.
pub fn dirs_match(expected: &Path, actual: &Path) -> bool {
    let expected_files = collect_files(expected, expected);
    let actual_files = collect_files(actual, actual);

    let mut ok = true;

    // Check all expected files exist in actual with same content
    for (rel_path, expected_path) in &expected_files {
        if let Some(actual_path) = actual_files.get(rel_path) {
            if !files_equal(expected_path, actual_path) {
                eprintln!("  DIFF: {} content mismatch", rel_path);
                ok = false;
            }
        } else {
            eprintln!("  MISSING in actual: {}", rel_path);
            ok = false;
        }
    }

    // Check for extra files in actual
    for rel_path in actual_files.keys() {
        if !expected_files.contains_key(rel_path) {
            eprintln!("  EXTRA in actual: {}", rel_path);
            ok = false;
        }
    }

    ok
}

/// Collect all files in a directory tree, returning relative paths.
fn collect_files(root: &Path, dir: &Path) -> BTreeMap<String, std::path::PathBuf> {
    let mut files = BTreeMap::new();
    collect_files_recursive(root, dir, &mut files);
    files
}

fn collect_files_recursive(
    root: &Path,
    dir: &Path,
    files: &mut BTreeMap<String, std::path::PathBuf>,
) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_files_recursive(root, &path, files);
            } else {
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                // Skip manifest files
                if name == ".bench-meta.json" {
                    continue;
                }
                let rel = path
                    .strip_prefix(root)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .to_string();
                files.insert(rel, path);
            }
        }
    }
}

/// Compare two files byte-by-byte.
fn files_equal(a: &Path, b: &Path) -> bool {
    let meta_a = match std::fs::metadata(a) {
        Ok(m) => m,
        Err(_) => return false,
    };
    let meta_b = match std::fs::metadata(b) {
        Ok(m) => m,
        Err(_) => return false,
    };

    if meta_a.len() != meta_b.len() {
        return false;
    }

    // For small files, compare directly
    if meta_a.len() < 1_048_576 {
        let data_a = std::fs::read(a).unwrap_or_default();
        let data_b = std::fs::read(b).unwrap_or_default();
        return data_a == data_b;
    }

    // For large files, compare in chunks
    use std::io::Read;
    let mut fa = match std::fs::File::open(a) {
        Ok(f) => std::io::BufReader::new(f),
        Err(_) => return false,
    };
    let mut fb = match std::fs::File::open(b) {
        Ok(f) => std::io::BufReader::new(f),
        Err(_) => return false,
    };

    let mut buf_a = [0u8; 65536];
    let mut buf_b = [0u8; 65536];

    loop {
        let n_a = fa.read(&mut buf_a).unwrap_or(0);
        let n_b = fb.read(&mut buf_b).unwrap_or(0);

        if n_a != n_b || buf_a[..n_a] != buf_b[..n_b] {
            return false;
        }
        if n_a == 0 {
            return true;
        }
    }
}
