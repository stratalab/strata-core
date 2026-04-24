//! Result recorder for saving benchmark results to JSON files.
//!
//! Creates JSON files in the `results/` directory following the shared schema
//! defined in `strata_benchmarks::schema`.

use super::{read_cpu_model, read_total_ram_gb, Percentiles};
use crate::schema::*;
use stratadb::WalCounters;

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::time::SystemTime;

/// Accumulates benchmark results and writes them to a JSON file.
pub struct ResultRecorder {
    category: String,
    metadata: RunMetadata,
    results: Vec<BenchmarkResult>,
}

impl ResultRecorder {
    /// Create a new recorder for the given category.
    ///
    /// Captures metadata (hardware, git, timestamp) at construction time.
    pub fn new(category: &str) -> Self {
        Self {
            category: category.to_string(),
            metadata: RunMetadata {
                timestamp: iso8601_now(),
                git_commit: git_short_commit(),
                git_branch: git_branch(),
                git_dirty: git_is_dirty(),
                sdk: "rust".to_string(),
                sdk_version: env!("CARGO_PKG_VERSION").to_string(),
                hardware: capture_hardware(),
            },
            results: Vec::new(),
        }
    }

    /// Record a raw benchmark result.
    pub fn record(&mut self, result: BenchmarkResult) {
        self.results.push(result);
    }

    /// Convenience method to record a latency benchmark result from harness types.
    pub fn record_latency(
        &mut self,
        name: &str,
        parameters: HashMap<String, serde_json::Value>,
        p: &Percentiles,
        wal: Option<&WalCounters>,
        iterations: u64,
    ) {
        let (wal_appends_per_op, wal_syncs_per_op) = match wal {
            Some(w) if w.wal_appends > 0 || w.sync_calls > 0 => (
                Some(w.wal_appends as f64 / iterations as f64),
                Some(w.sync_calls as f64 / iterations as f64),
            ),
            _ => (None, None),
        };

        self.results.push(BenchmarkResult {
            benchmark: name.to_string(),
            category: self.category.clone(),
            parameters,
            metrics: BenchmarkMetrics {
                p50_ns: Some(p.p50.as_nanos() as u64),
                p95_ns: Some(p.p95.as_nanos() as u64),
                p99_ns: Some(p.p99.as_nanos() as u64),
                min_ns: Some(p.min.as_nanos() as u64),
                max_ns: Some(p.max.as_nanos() as u64),
                samples: Some(p.samples as u64),
                wal_appends_per_op,
                wal_syncs_per_op,
                ..Default::default()
            },
        });
    }

    /// Write all accumulated results to a JSON file in `results/`.
    ///
    /// File naming: `<category>-<timestamp>-<commit>.json`
    pub fn save(self) -> io::Result<PathBuf> {
        let report = BenchmarkReport {
            schema_version: 1,
            metadata: self.metadata.clone(),
            results: self.results,
        };

        // Build filename
        let commit = self.metadata.git_commit.as_deref().unwrap_or("unknown");
        // Sanitize timestamp for filename (replace colons)
        let ts = self.metadata.timestamp.replace(':', "-");
        let filename = format!("{}-{}-{}.json", self.category, ts, commit);

        let results_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("results");
        std::fs::create_dir_all(&results_dir)?;
        let path = results_dir.join(&filename);

        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        std::fs::write(&path, json)?;

        eprintln!("Results saved to {}", path.display());
        Ok(path)
    }
}

// ---------------------------------------------------------------------------
// Metadata capture helpers
// ---------------------------------------------------------------------------

fn iso8601_now() -> String {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();

    // Manual UTC formatting (no chrono dependency)
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Days since epoch to Y-M-D (simplified Gregorian)
    let (year, month, day) = days_to_ymd(days);

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    // Algorithm from Howard Hinnant's date library
    days += 719468;
    let era = days / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn git_short_commit() -> Option<String> {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}

fn git_branch() -> Option<String> {
    std::process::Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}

fn git_is_dirty() -> Option<bool> {
    std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| !o.stdout.is_empty())
}

fn capture_hardware() -> HardwareInfo {
    HardwareInfo {
        cpu: read_cpu_model(),
        cores: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        ram_gb: read_total_ram_gb(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
    }
}
