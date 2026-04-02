mod generate;
mod report;
mod runner;
mod verify;

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[clap(name = "artifact-bench", about = "Benchmark harness for ArtifactRepository")]
pub struct Cli {
    /// Directory for generated test data
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Directory for benchmark working files
    #[arg(long)]
    work_dir: Option<PathBuf>,

    /// Target total data size (e.g. "10G", "500M", "1G")
    #[arg(long, default_value = "10G")]
    total_size: String,

    /// Percentage of files that are duplicates (0-100)
    #[arg(long, default_value_t = 20)]
    duplicate_pct: u8,

    /// Random seed for reproducible data generation
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Compression algorithm: none, gzip, deflate, lzma2, zstd
    #[arg(long, default_value = "zstd")]
    compression: String,

    /// Number of repeat runs per benchmark step
    #[arg(long, default_value_t = 3)]
    runs: usize,

    /// Skip diff verification after round-trips
    #[arg(long, default_value_t = false)]
    no_verify: bool,

    /// Reuse existing data in --data-dir (skip generation)
    #[arg(long, default_value_t = false)]
    skip_generate: bool,

    /// Skip server-based benchmarks (push/pull/endpoints)
    #[arg(long, default_value_t = false)]
    skip_network: bool,

    /// Output report path
    #[arg(long, default_value = "bench/report.md")]
    report: PathBuf,

    /// Port for the test server (0 = random available port)
    #[arg(long, default_value_t = 0)]
    server_port: u16,
}

fn main() {
    let cli = Cli::parse();

    let data_dir = cli.data_dir.unwrap_or_else(|| {
        std::env::temp_dir().join("artifact-bench").join("data")
    });
    let work_dir = cli.work_dir.unwrap_or_else(|| {
        std::env::temp_dir().join("artifact-bench").join("work")
    });

    // Validate args
    if cli.duplicate_pct > 100 {
        eprintln!("Error: --duplicate-pct must be 0-100");
        std::process::exit(1);
    }
    if cli.runs == 0 {
        eprintln!("Error: --runs must be at least 1");
        std::process::exit(1);
    }

    let total_bytes = parse_size(&cli.total_size).unwrap_or_else(|| {
        eprintln!("Error: invalid --total-size '{}'. Use e.g. '10G', '500M', '1G'", cli.total_size);
        std::process::exit(1);
    });

    println!("=== ArtifactRepository Benchmark ===");
    println!("Data dir:      {}", data_dir.display());
    println!("Work dir:      {}", work_dir.display());
    println!("Total size:    {} ({} bytes)", cli.total_size, total_bytes);
    println!("Duplicate %:   {}%", cli.duplicate_pct);
    println!("Seed:          {}", cli.seed);
    println!("Compression:   {}", cli.compression);
    println!("Runs:          {}", cli.runs);
    println!("Verify:        {}", !cli.no_verify);
    println!("Network:       {}", !cli.skip_network);
    println!();

    // Resolve binary paths (sibling to our own binary)
    let self_path = std::env::current_exe().expect("cannot determine own path");
    let bin_dir = self_path.parent().expect("binary has no parent dir");
    let client_bin = bin_dir.join(format!("client{}", std::env::consts::EXE_SUFFIX));
    let server_bin = bin_dir.join(format!("server{}", std::env::consts::EXE_SUFFIX));

    if !client_bin.exists() {
        eprintln!("Error: client binary not found at {}", client_bin.display());
        eprintln!("Build with: cargo build --release -p client -p server -p bench");
        std::process::exit(1);
    }
    if !cli.skip_network && !server_bin.exists() {
        eprintln!("Error: server binary not found at {}", server_bin.display());
        eprintln!("Build with: cargo build --release -p client -p server -p bench");
        std::process::exit(1);
    }

    // Step 1: Generate test data
    let manifest = if cli.skip_generate {
        println!("[1/4] Skipping data generation (--skip-generate)");
        generate::load_manifest(&data_dir).unwrap_or_else(|e| {
            eprintln!("Error: cannot load existing data manifest: {}", e);
            eprintln!("Run without --skip-generate first");
            std::process::exit(1);
        })
    } else {
        println!("[1/4] Generating test data...");
        generate::generate_data(&data_dir, total_bytes, cli.duplicate_pct, cli.seed)
    };

    println!(
        "  {} files, {} unique, {} bytes total",
        manifest.total_files, manifest.unique_files, manifest.total_bytes
    );
    println!();

    // Step 2: Run benchmarks
    println!("[2/4] Running benchmarks ({} runs each)...", cli.runs);
    std::fs::create_dir_all(&work_dir).expect("cannot create work dir");

    let config = runner::BenchConfig {
        client_bin,
        server_bin,
        data_dir: data_dir.clone(),
        work_dir: work_dir.clone(),
        compression: cli.compression.clone(),
        runs: cli.runs,
        verify: !cli.no_verify,
        skip_network: cli.skip_network,
        server_port: cli.server_port,
    };

    let results = runner::run_all_benchmarks(&config, &manifest);

    // Step 3: Verify summary
    println!();
    if !cli.no_verify {
        println!("[3/4] Verification: all round-trip checks passed ✅");
    } else {
        println!("[3/4] Verification: skipped (--no-verify)");
    }

    // Step 4: Generate report
    println!("[4/4] Writing report to {}...", cli.report.display());
    if let Some(parent) = cli.report.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    report::write_report(&cli.report, &manifest, &config, &results);

    println!();
    println!("Done! Report written to {}", cli.report.display());
}

fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let (num_str, multiplier) = if s.ends_with('G') || s.ends_with('g') {
        (&s[..s.len() - 1], 1_000_000_000u64)
    } else if s.ends_with('M') || s.ends_with('m') {
        (&s[..s.len() - 1], 1_000_000u64)
    } else if s.ends_with('K') || s.ends_with('k') {
        (&s[..s.len() - 1], 1_000u64)
    } else {
        (s, 1u64)
    };

    let num: f64 = num_str.parse().ok()?;
    Some((num * multiplier as f64) as u64)
}
