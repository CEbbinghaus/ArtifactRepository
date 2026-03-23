use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::generate::DataManifest;
use crate::verify;

pub struct BenchConfig {
    pub client_bin: PathBuf,
    pub server_bin: PathBuf,
    pub data_dir: PathBuf,
    pub work_dir: PathBuf,
    pub compression: String,
    pub runs: usize,
    pub verify: bool,
    pub skip_network: bool,
    pub server_port: u16,
}

#[derive(Debug, Clone)]
pub struct StepResult {
    pub name: String,
    pub timings: Vec<Duration>,
    pub input_bytes: u64,
    pub output_bytes: Option<u64>,
    pub verified: Option<bool>,
}

impl StepResult {
    pub fn min(&self) -> Duration {
        *self.timings.iter().min().unwrap()
    }
    pub fn max(&self) -> Duration {
        *self.timings.iter().max().unwrap()
    }
    pub fn median(&self) -> Duration {
        let mut sorted = self.timings.clone();
        sorted.sort();
        sorted[sorted.len() / 2]
    }
}

pub struct BenchResults {
    pub steps: Vec<StepResult>,
    pub index_hash: String,
    pub incremental: Option<IncrementalResult>,
}

#[derive(Debug, Clone)]
pub struct IncrementalResult {
    pub files_changed: usize,
    pub files_added: usize,
    pub original_hash: String,
    pub modified_hash: String,
    pub recommit_time: Duration,
    pub push_archive_full_time: Duration,
    pub push_archive_incremental_time: Duration,
}

struct ServerHandle {
    pid: u32,
    port: u16,
    #[allow(dead_code)]
    store_dir: PathBuf,
}

impl ServerHandle {
    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        // Kill the server process
        let _ = Command::new("kill").arg(self.pid.to_string()).output();
        // Wait briefly for cleanup
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn start_server(config: &BenchConfig, store_dir: &Path) -> ServerHandle {
    let port = if config.server_port == 0 {
        // Find an available port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("cannot bind random port");
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        port
    } else {
        config.server_port
    };

    std::fs::create_dir_all(store_dir).expect("cannot create server store dir");

    let child = Command::new(&config.server_bin)
        .arg(store_dir.to_str().unwrap())
        .arg("-p")
        .arg(port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("cannot start server");

    let pid = child.id();

    // Wait for server to be ready
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if Instant::now() > deadline {
            panic!("server did not start within 10 seconds");
        }
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Leak the child handle — we manage the process via pid
    std::mem::forget(child);

    ServerHandle {
        pid,
        port,
        store_dir: store_dir.to_path_buf(),
    }
}

fn run_client(config: &BenchConfig, store: &Path, args: &[&str]) -> (Duration, String) {
    let start = Instant::now();
    let output = Command::new(&config.client_bin)
        .arg("--store")
        .arg(store.to_str().unwrap())
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("cannot run client");
    let elapsed = start.elapsed();

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        panic!(
            "client command failed: {:?}\nstdout: {}\nstderr: {}",
            args, stdout, stderr
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    (elapsed, stdout)
}

/// Run client without --store (for archive/extract which don't need one)
fn run_client_no_store(config: &BenchConfig, args: &[&str]) -> (Duration, String) {
    let start = Instant::now();
    let output = Command::new(&config.client_bin)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("cannot run client");
    let elapsed = start.elapsed();

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        panic!(
            "client command failed: {:?}\nstdout: {}\nstderr: {}",
            args, stdout, stderr
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    (elapsed, stdout)
}

fn run_curl(url: &str, output_file: Option<&Path>) -> Duration {
    let start = Instant::now();
    let mut cmd = Command::new("curl");
    cmd.arg("-s").arg("-f");
    if let Some(path) = output_file {
        cmd.arg("-o").arg(path.to_str().unwrap());
    } else {
        cmd.arg("-o").arg("/dev/null");
    }
    cmd.arg(url);

    let output = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("cannot run curl");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("curl failed for {}: {}", url, stderr);
    }

    start.elapsed()
}

fn run_curl_post_file(url: &str, file: &Path) -> Duration {
    let start = Instant::now();
    let output = Command::new("curl")
        .arg("-s")
        .arg("-f")
        .arg("-X")
        .arg("POST")
        .arg("-H")
        .arg("Content-Type: application/octet-stream")
        .arg("--data-binary")
        .arg(format!("@{}", file.to_str().unwrap()))
        .arg("-o")
        .arg("/dev/null")
        .arg(url)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("cannot run curl");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("curl POST failed for {}: {}", url, stderr);
    }

    start.elapsed()
}

fn run_curl_post_json(url: &str, json_file: &Path) -> Duration {
    let start = Instant::now();
    let output = Command::new("curl")
        .arg("-s")
        .arg("-f")
        .arg("-X")
        .arg("POST")
        .arg("-H")
        .arg("Content-Type: application/json")
        .arg("-d")
        .arg(format!("@{}", json_file.to_str().unwrap()))
        .arg("-o")
        .arg("/dev/null")
        .arg(url)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("cannot run curl");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("curl POST JSON failed for {}: {}", url, stderr);
    }

    start.elapsed()
}

fn fresh_dir(base: &Path, name: &str) -> PathBuf {
    let dir = base.join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("cannot remove dir");
    }
    std::fs::create_dir_all(&dir).expect("cannot create dir");
    dir
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                total += dir_size(&path);
            } else if let Ok(meta) = path.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

fn file_size(path: &Path) -> u64 {
    path.metadata().map(|m| m.len()).unwrap_or(0)
}

/// Extract the index hash from client commit stdout (last non-empty line)
fn extract_hash(stdout: &str) -> String {
    stdout
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .unwrap_or("")
        .trim()
        .to_string()
}

/// Build the list of all object hashes from a store directory
fn collect_store_hashes(store_dir: &Path) -> Vec<String> {
    let mut hashes = Vec::new();
    if let Ok(entries) = std::fs::read_dir(store_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.len() == 128 && name.chars().all(|c| c.is_ascii_hexdigit()) {
                hashes.push(name);
            }
        }
    }
    hashes
}

pub fn run_all_benchmarks(config: &BenchConfig, manifest: &DataManifest) -> BenchResults {
    let mut steps: Vec<StepResult> = Vec::new();
    let input_bytes = manifest.total_bytes;

    // === Phase 0: Initial commit to get index hash and reference store ===
    println!("  [setup] Initial commit...");
    let ref_store = fresh_dir(&config.work_dir, "ref-store");
    let (_, stdout) = run_client(config, &ref_store, &["commit", "-d", config.data_dir.to_str().unwrap()]);
    let index_hash = extract_hash(&stdout);
    println!("  [setup] Index hash: {}", &index_hash[..24]);
    let ref_store_size = dir_size(&ref_store);

    // === Phase 1: Local store benchmarks (repeated) ===

    // 1. commit
    {
        println!("  [bench] commit...");
        let mut timings = Vec::new();
        for run in 0..config.runs {
            let store = fresh_dir(&config.work_dir, &format!("commit-store-{}", run));
            let (elapsed, _) = run_client(config, &store, &["commit", "-d", config.data_dir.to_str().unwrap()]);
            timings.push(elapsed);
            print_run(run, elapsed);
        }
        steps.push(StepResult {
            name: "commit".into(),
            timings,
            input_bytes,
            output_bytes: Some(ref_store_size),
            verified: None,
        });
    }

    // 2. restore
    {
        println!("  [bench] restore...");
        let mut timings = Vec::new();
        let mut verified = None;
        for run in 0..config.runs {
            let out_dir = fresh_dir(&config.work_dir, &format!("restore-out-{}", run));
            let (elapsed, _) = run_client(
                config, &ref_store,
                &["restore", "-d", out_dir.to_str().unwrap(), "-i", &index_hash],
            );
            timings.push(elapsed);
            print_run(run, elapsed);

            if config.verify && run == config.runs - 1 {
                let ok = verify::dirs_match(&config.data_dir, &out_dir);
                if !ok {
                    panic!("VERIFICATION FAILED: restore output differs from source");
                }
                verified = Some(true);
            }
        }
        steps.push(StepResult {
            name: "restore".into(),
            timings,
            input_bytes: ref_store_size,
            output_bytes: Some(input_bytes),
            verified,
        });
    }

    // 3. restore --validate
    {
        println!("  [bench] restore --validate...");
        let mut timings = Vec::new();
        for run in 0..config.runs {
            let out_dir = fresh_dir(&config.work_dir, &format!("restore-val-{}", run));
            let (elapsed, _) = run_client(
                config, &ref_store,
                &["restore", "-d", out_dir.to_str().unwrap(), "-i", &index_hash, "--validate"],
            );
            timings.push(elapsed);
            print_run(run, elapsed);
        }
        steps.push(StepResult {
            name: "restore+validate".into(),
            timings,
            input_bytes: ref_store_size,
            output_bytes: Some(input_bytes),
            verified: Some(true), // validate flag does this internally
        });
    }

    // 4. pack
    let archive_path = config.work_dir.join("bench.arx");
    {
        println!("  [bench] pack...");
        let mut timings = Vec::new();
        for run in 0..config.runs {
            if archive_path.exists() {
                std::fs::remove_file(&archive_path).ok();
            }
            let (elapsed, _) = run_client(
                config, &ref_store,
                &[
                    "pack", "--index", &index_hash,
                    "--file", archive_path.to_str().unwrap(),
                    "--compression", &config.compression,
                ],
            );
            timings.push(elapsed);
            print_run(run, elapsed);
        }
        let archive_size = file_size(&archive_path);
        steps.push(StepResult {
            name: "pack".into(),
            timings,
            input_bytes: ref_store_size,
            output_bytes: Some(archive_size),
            verified: None,
        });
    }

    // 5. unpack
    {
        println!("  [bench] unpack...");
        let mut timings = Vec::new();
        for run in 0..config.runs {
            let store = fresh_dir(&config.work_dir, &format!("unpack-store-{}", run));
            let (elapsed, _) = run_client(
                config, &store,
                &["unpack", "--file", archive_path.to_str().unwrap()],
            );
            timings.push(elapsed);
            print_run(run, elapsed);
        }
        steps.push(StepResult {
            name: "unpack".into(),
            timings,
            input_bytes: file_size(&archive_path),
            output_bytes: Some(ref_store_size),
            verified: None,
        });
    }

    // 6. archive (direct)
    let direct_archive_path = config.work_dir.join("direct.arx");
    {
        println!("  [bench] archive (direct)...");
        let mut timings = Vec::new();
        for run in 0..config.runs {
            if direct_archive_path.exists() {
                std::fs::remove_file(&direct_archive_path).ok();
            }
            let (elapsed, _) = run_client_no_store(
                config,
                &[
                    "archive", "-d", config.data_dir.to_str().unwrap(),
                    "-f", direct_archive_path.to_str().unwrap(),
                    "-c", &config.compression,
                ],
            );
            timings.push(elapsed);
            print_run(run, elapsed);
        }
        steps.push(StepResult {
            name: "archive".into(),
            timings,
            input_bytes,
            output_bytes: Some(file_size(&direct_archive_path)),
            verified: None,
        });
    }

    // 7. extract (direct)
    {
        println!("  [bench] extract (direct)...");
        let mut timings = Vec::new();
        let mut verified = None;
        for run in 0..config.runs {
            let out_dir = fresh_dir(&config.work_dir, &format!("extract-out-{}", run));
            let (elapsed, _) = run_client_no_store(
                config,
                &[
                    "extract", "-f", direct_archive_path.to_str().unwrap(),
                    "-d", out_dir.to_str().unwrap(),
                ],
            );
            timings.push(elapsed);
            print_run(run, elapsed);

            if config.verify && run == config.runs - 1 {
                let ok = verify::dirs_match(&config.data_dir, &out_dir);
                if !ok {
                    panic!("VERIFICATION FAILED: extract output differs from source");
                }
                verified = Some(true);
            }
        }
        steps.push(StepResult {
            name: "extract".into(),
            timings,
            input_bytes: file_size(&direct_archive_path),
            output_bytes: Some(input_bytes),
            verified,
        });
    }

    // === Phase 2: Network benchmarks ===
    let mut incremental = None;

    if !config.skip_network {
        println!("  [bench] Starting test server...");
        let server_store = fresh_dir(&config.work_dir, "server-store");
        let server = start_server(config, &server_store);
        println!("  [bench] Server running on port {}", server.port);

        // 8. push (full — pushes all objects in the store)
        {
            println!("  [bench] push...");
            let (elapsed, _) = run_client(
                config, &ref_store,
                &["push", "--url", &server.url()],
            );
            steps.push(StepResult {
                name: "push".into(),
                timings: vec![elapsed],
                input_bytes: ref_store_size,
                output_bytes: None,
                verified: None,
            });
            print_run(0, elapsed);
        }

        // 9. pull
        {
            println!("  [bench] pull...");
            let pull_store = fresh_dir(&config.work_dir, "pull-store");
            let (elapsed, _) = run_client(
                config, &pull_store,
                &["pull", "--url", &server.url(), "--index", &index_hash],
            );
            steps.push(StepResult {
                name: "pull".into(),
                timings: vec![elapsed],
                input_bytes: ref_store_size,
                output_bytes: None,
                verified: None,
            });
            print_run(0, elapsed);

            // Verify pulled data round-trips correctly
            if config.verify {
                let pull_out = fresh_dir(&config.work_dir, "pull-verify");
                run_client(
                    config, &pull_store,
                    &["restore", "-d", pull_out.to_str().unwrap(), "-i", &index_hash],
                );
                let ok = verify::dirs_match(&config.data_dir, &pull_out);
                if !ok {
                    panic!("VERIFICATION FAILED: pull+restore output differs from source");
                }
            }
        }

        // 10. push-archive (dedup — all objects already on server)
        {
            println!("  [bench] push-archive (all exist)...");
            let (elapsed, _) = run_client(
                config, &ref_store,
                &[
                    "push-archive", "--url", &server.url(), "--index", &index_hash,
                    "--compression", &config.compression,
                ],
            );
            steps.push(StepResult {
                name: "push-archive (all exist)".into(),
                timings: vec![elapsed],
                input_bytes: ref_store_size,
                output_bytes: None,
                verified: None,
            });
            print_run(0, elapsed);
        }

        // 11. GET /v1/archive
        {
            println!("  [bench] GET /v1/archive...");
            let dl_path = config.work_dir.join("server-dl.arx");
            let url = format!("{}/v1/archive/{}", server.url(), index_hash);
            let elapsed = run_curl(&url, Some(&dl_path));
            steps.push(StepResult {
                name: "GET /v1/archive".into(),
                timings: vec![elapsed],
                input_bytes: ref_store_size,
                output_bytes: Some(file_size(&dl_path)),
                verified: None,
            });
            print_run(0, elapsed);
        }

        // 12. GET /v1/zip
        {
            println!("  [bench] GET /v1/zip...");
            let dl_path = config.work_dir.join("server-dl.zip");
            let url = format!("{}/v1/zip/{}", server.url(), index_hash);
            let elapsed = run_curl(&url, Some(&dl_path));
            steps.push(StepResult {
                name: "GET /v1/zip".into(),
                timings: vec![elapsed],
                input_bytes: ref_store_size,
                output_bytes: Some(file_size(&dl_path)),
                verified: None,
            });
            print_run(0, elapsed);
        }

        // 13. GET /v1/index/metadata
        {
            println!("  [bench] GET /v1/index/metadata...");
            let url = format!("{}/v1/index/{}/metadata", server.url(), index_hash);
            let elapsed = run_curl(&url, None);
            steps.push(StepResult {
                name: "GET /v1/index/metadata".into(),
                timings: vec![elapsed],
                input_bytes: 0,
                output_bytes: None,
                verified: None,
            });
            print_run(0, elapsed);
        }

        // 14. POST /v1/object/missing
        {
            println!("  [bench] POST /v1/object/missing...");
            let hashes = collect_store_hashes(&ref_store);
            let json = serde_json::json!({ "hashes": hashes });
            let json_path = config.work_dir.join("missing-req.json");
            std::fs::write(&json_path, json.to_string()).expect("cannot write json");
            let url = format!("{}/v1/object/missing", server.url());
            let elapsed = run_curl_post_json(&url, &json_path);
            steps.push(StepResult {
                name: "POST /v1/object/missing".into(),
                timings: vec![elapsed],
                input_bytes: file_size(&json_path),
                output_bytes: None,
                verified: None,
            });
            print_run(0, elapsed);
        }

        // 15. POST /v1/archive/upload
        {
            println!("  [bench] POST /v1/archive/upload...");
            let url = format!("{}/v1/archive/upload", server.url());
            let elapsed = run_curl_post_file(&url, &archive_path);
            steps.push(StepResult {
                name: "POST /v1/archive/upload".into(),
                timings: vec![elapsed],
                input_bytes: file_size(&archive_path),
                output_bytes: None,
                verified: None,
            });
            print_run(0, elapsed);
        }

        // === Phase 3: Incremental / Supplemental test ===
        {
            println!("  [bench] Incremental test (small changes)...");
            let original_hash = index_hash.clone();

            // Apply small changes to source data
            let (modified_paths, added_count) =
                crate::generate::apply_small_changes(&config.data_dir, 5, manifest.seed);
            println!(
                "    Modified {} files, added {} new files",
                modified_paths.len() - added_count,
                added_count
            );

            // Re-commit the modified data
            let incr_store = fresh_dir(&config.work_dir, "incr-store");
            let (recommit_time, stdout) = run_client(
                config, &incr_store,
                &["commit", "-d", config.data_dir.to_str().unwrap()],
            );
            let modified_hash = extract_hash(&stdout);
            println!("    New hash: {}", &modified_hash[..24]);
            println!("    Re-commit: {:.3}s", recommit_time.as_secs_f64());

            // push-archive with fresh server (full upload)
            drop(server);
            let fresh_server_store = fresh_dir(&config.work_dir, "incr-server-fresh");
            let fresh_server = start_server(config, &fresh_server_store);

            let (push_full_time, _) = run_client(
                config, &incr_store,
                &[
                    "push-archive", "--url", &fresh_server.url(), "--index", &modified_hash,
                    "--compression", &config.compression,
                ],
            );
            println!("    push-archive (full): {:.3}s", push_full_time.as_secs_f64());

            // push-archive where original data already exists on server
            drop(fresh_server);
            let incr_server_store = fresh_dir(&config.work_dir, "incr-server-existing");
            let incr_server = start_server(config, &incr_server_store);

            // First push original data
            run_client(
                config, &ref_store,
                &["push", "--url", &incr_server.url()],
            );

            // Now push-archive only the delta
            let (push_incr_time, _) = run_client(
                config, &incr_store,
                &[
                    "push-archive", "--url", &incr_server.url(), "--index", &modified_hash,
                    "--compression", &config.compression,
                ],
            );
            println!("    push-archive (incremental): {:.3}s", push_incr_time.as_secs_f64());
            println!(
                "    Speedup: {:.1}x",
                push_full_time.as_secs_f64() / push_incr_time.as_secs_f64().max(0.001)
            );

            // Verify the incremental push produced correct data
            if config.verify {
                let incr_pull_store = fresh_dir(&config.work_dir, "incr-pull-store");
                run_client(
                    config, &incr_pull_store,
                    &["pull", "--url", &incr_server.url(), "--index", &modified_hash],
                );
                let incr_out = fresh_dir(&config.work_dir, "incr-verify");
                run_client(
                    config, &incr_pull_store,
                    &["restore", "-d", incr_out.to_str().unwrap(), "-i", &modified_hash],
                );
                let ok = verify::dirs_match(&config.data_dir, &incr_out);
                if !ok {
                    panic!("VERIFICATION FAILED: incremental push+pull output differs from source");
                }
                println!("    Incremental verification: ✅");
            }

            // Clean up: undo the modifications so data_dir is back to original
            // (We can't easily undo random changes, so we note this in the report)
            incremental = Some(IncrementalResult {
                files_changed: modified_paths.len() - added_count,
                files_added: added_count,
                original_hash,
                modified_hash,
                recommit_time,
                push_archive_full_time: push_full_time,
                push_archive_incremental_time: push_incr_time,
            });
        }
    }

    BenchResults {
        steps,
        index_hash,
        incremental,
    }
}

fn print_run(run: usize, elapsed: Duration) {
    println!("    run {}: {:.3}s", run + 1, elapsed.as_secs_f64());
}
