use std::io::Write;
use std::path::Path;
use std::time::Duration;

use crate::generate::DataManifest;
use crate::runner::{BenchConfig, BenchResults};

pub fn write_report(
    path: &Path,
    manifest: &DataManifest,
    config: &BenchConfig,
    results: &BenchResults,
) {
    let mut out = String::new();

    // Header
    out.push_str("# ArtifactRepository Benchmark Report\n\n");
    out.push_str(&format!(
        "**Date**: {}\n",
        chrono_now()
    ));
    out.push_str(&format!("**System**: {}\n", system_info()));
    out.push_str(&format!(
        "**Dataset**: {} files ({} unique), {} total\n",
        manifest.total_files,
        manifest.unique_files,
        human_bytes(manifest.total_bytes),
    ));
    out.push_str(&format!(
        "**Duplicates**: {}%\n",
        manifest.duplicate_pct,
    ));
    out.push_str(&format!("**Seed**: {}\n", manifest.seed));
    out.push_str(&format!("**Compression**: {}\n", config.compression));
    out.push_str(&format!("**Runs**: {}\n", config.runs));
    out.push_str(&format!("**Index hash**: `{}`\n", results.index_hash));
    out.push('\n');

    // Results table
    out.push_str("## Results\n\n");

    if config.runs > 1 {
        out.push_str("| Step | Min | Median | Max | Input | Output | Throughput (median) | Verified |\n");
        out.push_str("|------|-----|--------|-----|-------|--------|---------------------|----------|\n");

        for step in &results.steps {
            let throughput = if step.input_bytes > 0 {
                let mb = step.input_bytes as f64 / 1_000_000.0;
                let secs = step.median().as_secs_f64();
                if secs > 0.0 {
                    format!("{:.1} MB/s", mb / secs)
                } else {
                    "—".into()
                }
            } else {
                "—".into()
            };

            let output = step
                .output_bytes
                .map(human_bytes)
                .unwrap_or_else(|| "—".into());

            let verified = match step.verified {
                Some(true) => "✅",
                Some(false) => "❌",
                None => "—",
            };

            out.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} |\n",
                step.name,
                fmt_duration(step.min()),
                fmt_duration(step.median()),
                fmt_duration(step.max()),
                human_bytes(step.input_bytes),
                output,
                throughput,
                verified,
            ));
        }
    } else {
        out.push_str("| Step | Time | Input | Output | Throughput | Verified |\n");
        out.push_str("|------|------|-------|--------|------------|----------|\n");

        for step in &results.steps {
            let time = step.timings.first().copied().unwrap_or_default();
            let throughput = if step.input_bytes > 0 {
                let mb = step.input_bytes as f64 / 1_000_000.0;
                let secs = time.as_secs_f64();
                if secs > 0.0 {
                    format!("{:.1} MB/s", mb / secs)
                } else {
                    "—".into()
                }
            } else {
                "—".into()
            };

            let output = step
                .output_bytes
                .map(human_bytes)
                .unwrap_or_else(|| "—".into());

            let verified = match step.verified {
                Some(true) => "✅",
                Some(false) => "❌",
                None => "—",
            };

            out.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} |\n",
                step.name,
                fmt_duration(time),
                human_bytes(step.input_bytes),
                output,
                throughput,
                verified,
            ));
        }
    }

    // Incremental results
    if let Some(incr) = &results.incremental {
        out.push_str("\n## Incremental / Supplemental Test\n\n");
        out.push_str(&format!(
            "**Changes**: {} files modified, {} files added\n",
            incr.files_changed, incr.files_added
        ));
        out.push_str(&format!(
            "**Original hash**: `{}`\n",
            incr.original_hash
        ));
        out.push_str(&format!(
            "**Modified hash**: `{}`\n",
            incr.modified_hash
        ));
        out.push('\n');

        out.push_str("| Operation | Time |\n");
        out.push_str("|-----------|------|\n");
        out.push_str(&format!(
            "| Re-commit (modified data) | {} |\n",
            fmt_duration(incr.recommit_time)
        ));
        out.push_str(&format!(
            "| push-archive (full, to empty server) | {} |\n",
            fmt_duration(incr.push_archive_full_time)
        ));
        out.push_str(&format!(
            "| push-archive (incremental, original on server) | {} |\n",
            fmt_duration(incr.push_archive_incremental_time)
        ));

        let speedup = incr.push_archive_full_time.as_secs_f64()
            / incr.push_archive_incremental_time.as_secs_f64().max(0.001);
        out.push_str(&format!(
            "\n**Incremental speedup**: {:.1}x faster than full push\n",
            speedup
        ));
    }

    out.push('\n');

    // Write to file
    let mut file = std::fs::File::create(path).expect("cannot create report file");
    file.write_all(out.as_bytes()).expect("cannot write report");
}

fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs_f64();
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.2}s", secs)
    } else {
        let mins = (secs / 60.0).floor() as u64;
        let remaining = secs - (mins as f64 * 60.0);
        format!("{}m{:.1}s", mins, remaining)
    }
}

fn human_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "0 B".into();
    }
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit_idx = 0;
    while value >= 1000.0 && unit_idx < units.len() - 1 {
        value /= 1000.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{} B", bytes)
    } else {
        format!("{:.1} {}", value, units[unit_idx])
    }
}

fn system_info() -> String {
    let os = std::fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|c| {
            c.lines()
                .find(|l| l.starts_with("PRETTY_NAME="))
                .map(|l| l.trim_start_matches("PRETTY_NAME=").trim_matches('"').to_string())
        })
        .unwrap_or_else(|| "Linux".into());

    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let mem_kb = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|c| {
            c.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| {
                    l.split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse::<u64>().ok())
                })
        })
        .unwrap_or(0);

    let mem_gb = mem_kb as f64 / 1_048_576.0;

    format!("{}, {} cores, {:.0} GB RAM", os, cpus, mem_gb)
}

fn chrono_now() -> String {
    // Simple ISO 8601 timestamp without chrono dependency
    let output = std::process::Command::new("date")
        .arg("-u")
        .arg("+%Y-%m-%dT%H:%M:%SZ")
        .output()
        .ok();

    output
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}
