//! Shared benchmark harness for StrataDB.
//!
//! Provides database factory, data generators, latency percentile reporting,
//! and configuration types used across all primitive benchmark files.

pub mod metrics;
pub mod recorder;
pub mod scaling;

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use stratadb::{Strata, Value, WalCounters};
use tempfile::TempDir;

// =============================================================================
// Constants
// =============================================================================

/// Number of entries to pre-populate for read benchmarks.
pub const WARMUP_COUNT: u64 = 10_000;

/// Number of samples for percentile measurement.
pub const PERCENTILE_SAMPLES: usize = 1_000;

// =============================================================================
// Hardware Info
// =============================================================================

static HARDWARE_INFO_ONCE: std::sync::Once = std::sync::Once::new();

/// Print hardware specs once per benchmark binary.
pub fn print_hardware_info() {
    HARDWARE_INFO_ONCE.call_once(|| {
        let cpu = read_cpu_model();
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0);
        let ram_gb = read_total_ram_gb();
        let os = std::env::consts::OS;
        let arch = std::env::consts::ARCH;

        eprintln!("=== Hardware ===");
        eprintln!("CPU:    {}", cpu);
        eprintln!("Cores:  {}", cores);
        eprintln!("RAM:    {} GB", ram_gb);
        eprintln!("OS:     {} ({})", os, arch);
        eprintln!("================");
    });
}

pub fn read_cpu_model() -> String {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/cpuinfo") {
            for line in contents.lines() {
                if line.starts_with("model name") {
                    if let Some(val) = line.split(':').nth(1) {
                        return val.trim().to_string();
                    }
                }
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(output) = std::process::Command::new("sysctl")
            .arg("-n")
            .arg("machdep.cpu.brand_string")
            .output()
        {
            if output.status.success() {
                return String::from_utf8_lossy(&output.stdout).trim().to_string();
            }
        }
    }
    "unknown".to_string()
}

pub fn read_total_ram_gb() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for line in contents.lines() {
                if line.starts_with("MemTotal:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<u64>() {
                            return kb / 1_048_576;
                        }
                    }
                }
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(output) = std::process::Command::new("sysctl")
            .arg("-n")
            .arg("hw.memsize")
            .output()
        {
            if output.status.success() {
                if let Ok(bytes) = String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .parse::<u64>()
                {
                    return bytes / (1024 * 1024 * 1024);
                }
            }
        }
    }
    0
}

// =============================================================================
// Latency Percentiles
// =============================================================================

/// Collected latency percentiles.
pub struct Percentiles {
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub min: Duration,
    pub max: Duration,
    pub samples: usize,
}

/// Run `f` for `n` iterations, time each call individually, return percentiles.
pub fn measure_percentiles<F: FnMut()>(n: usize, mut f: F) -> Percentiles {
    let mut timings = Vec::with_capacity(n);
    for _ in 0..n {
        let start = Instant::now();
        f();
        timings.push(start.elapsed());
    }
    timings.sort();
    let len = timings.len();
    Percentiles {
        p50: timings[len * 50 / 100],
        p95: timings[len * 95 / 100],
        p99: timings[len * 99 / 100],
        min: timings[0],
        max: timings[len - 1],
        samples: len,
    }
}

/// Format a Duration for human-readable latency display.
fn fmt_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{} ns", nanos)
    } else if nanos < 1_000_000 {
        format!("{:.2} µs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.2} ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", nanos as f64 / 1_000_000_000.0)
    }
}

/// Print percentiles to stderr in a compact table.
pub fn report_percentiles(label: &str, p: &Percentiles) {
    eprintln!(
        "  {:<45} p50={:<12} p95={:<12} p99={:<12} (n={})",
        label,
        fmt_duration(p.p50),
        fmt_duration(p.p95),
        fmt_duration(p.p99),
        p.samples,
    );
}

// =============================================================================
// Durability Configuration
// =============================================================================

/// Durability mode for benchmark parameterization.
#[derive(Debug, Clone, Copy)]
pub enum DurabilityConfig {
    Cache,
    Standard,
    Always,
}

impl DurabilityConfig {
    /// All three modes for iteration in parameterized benchmarks.
    pub const ALL: [Self; 3] = [Self::Cache, Self::Standard, Self::Always];

    /// Short label for `BenchmarkId`.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Cache => "cache",
            Self::Standard => "standard",
            Self::Always => "always",
        }
    }
}

impl fmt::Display for DurabilityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

// =============================================================================
// Value Size Sweep
// =============================================================================

/// Value sizes for cache-effect benchmarking.
#[derive(Debug, Clone, Copy)]
pub enum ValueSize {
    /// 128 bytes — fits in L1 cache line pair
    Small,
    /// 1 KB — typical small document
    Medium,
    /// 8 KB — starts to stress cache hierarchy
    Large,
}

impl ValueSize {
    pub const ALL: [Self; 3] = [Self::Small, Self::Medium, Self::Large];

    pub fn label(&self) -> &'static str {
        match self {
            Self::Small => "128B",
            Self::Medium => "1KB",
            Self::Large => "8KB",
        }
    }

    pub fn byte_count(&self) -> usize {
        match self {
            Self::Small => 128,
            Self::Medium => 1024,
            Self::Large => 8192,
        }
    }
}

impl fmt::Display for ValueSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Generate a byte value of the given size.
pub fn kv_value_sized(size: ValueSize) -> Value {
    Value::Bytes(vec![0x42; size.byte_count()])
}

// =============================================================================
// BenchDb
// =============================================================================

/// Database wrapper that keeps temp directories alive for disk-backed modes.
pub struct BenchDb {
    pub db: Strata,
    _temp_dir: Option<TempDir>,
}

impl BenchDb {
    /// Return the on-disk path for this database (None for cache-only).
    pub fn db_path(&self) -> Option<&std::path::Path> {
        self._temp_dir.as_ref().map(|td| td.path())
    }
}

/// Open an existing database at a fixed path (no temp dir, won't be cleaned up).
pub fn open_existing_db<P: AsRef<std::path::Path>>(path: P) -> BenchDb {
    let strata = Strata::open(path).expect("failed to open existing database");
    BenchDb {
        db: strata,
        _temp_dir: None,
    }
}

/// Create a database configured for the given durability mode.
pub fn create_db(config: DurabilityConfig) -> BenchDb {
    print_hardware_info();

    match config {
        DurabilityConfig::Cache => {
            let strata = Strata::cache().expect("failed to create cache database");
            BenchDb {
                db: strata,
                _temp_dir: None,
            }
        }
        DurabilityConfig::Standard => {
            let temp_dir = TempDir::new().expect("failed to create temp dir");
            let strata = Strata::open(temp_dir.path()).expect("failed to open standard database");
            BenchDb {
                db: strata,
                _temp_dir: Some(temp_dir),
            }
        }
        DurabilityConfig::Always => {
            let temp_dir = TempDir::new().expect("failed to create temp dir");
            std::fs::write(
                temp_dir.path().join("strata.toml"),
                "durability = \"always\"\n",
            )
            .expect("failed to write always config");
            let strata = Strata::open(temp_dir.path()).expect("failed to open always database");
            BenchDb {
                db: strata,
                _temp_dir: Some(temp_dir),
            }
        }
    }
}

// =============================================================================
// Data Generators
// =============================================================================

/// Generate a 100-byte KV key from a counter.
pub fn kv_key(i: u64) -> String {
    format!("{:0>100}", i)
}

/// Generate a 100-byte KV key with a prefix.
pub fn kv_key_with_prefix(prefix: &str, i: u64) -> String {
    let base = format!("{}{}", prefix, i);
    if base.len() >= 100 {
        base[..100].to_string()
    } else {
        format!("{:0>width$}", base, width = 100)
    }
}

/// Generate a 1KB byte value for KV benchmarks (default size).
pub fn kv_value() -> Value {
    Value::Bytes(vec![0x42; 1024])
}

/// Generate a 64-byte value for State benchmarks.
pub fn state_value() -> Value {
    Value::Bytes(vec![0x53; 64])
}

/// Generate a ~512-byte JSON Object for Event payloads.
pub fn event_payload() -> Value {
    let mut map = HashMap::new();
    for j in 0..8 {
        let field = format!("field_{}", j);
        let val = format!("benchmark_event_payload_value_{:0>30}", j);
        map.insert(field, Value::String(val));
    }
    Value::object(map)
}

/// Generate a 10-field, 3-level nested JSON document.
pub fn json_document(i: u64) -> Value {
    let mut map = HashMap::new();
    map.insert("id".to_string(), Value::Int(i as i64));
    map.insert("name".to_string(), Value::String(format!("doc_{}", i)));
    map.insert("score".to_string(), Value::Float((i as f64) * 0.1));
    map.insert("active".to_string(), Value::Bool(i % 2 == 0));
    map.insert(
        "tags".to_string(),
        Value::array(vec![
            Value::String("bench".to_string()),
            Value::String(format!("tag_{}", i % 10)),
        ]),
    );
    map.insert("counter".to_string(), Value::Int((i * 7) as i64));
    map.insert(
        "description".to_string(),
        Value::String(format!("benchmark document number {}", i)),
    );

    let mut deep = HashMap::new();
    deep.insert("deep_a".to_string(), Value::Int((i * 3) as i64));
    deep.insert("deep_b".to_string(), Value::String(format!("deep_{}", i)));
    deep.insert("deep_c".to_string(), Value::Float((i as f64) * 0.001));

    let mut mid = HashMap::new();
    mid.insert("mid_score".to_string(), Value::Float((i as f64) * 1.5));
    mid.insert("mid_label".to_string(), Value::String(format!("mid_{}", i)));
    mid.insert("mid_nested".to_string(), Value::object(deep));

    map.insert("metadata".to_string(), Value::object(mid));

    map.insert("version".to_string(), Value::Int(1));
    map.insert(
        "checksum".to_string(),
        Value::String(format!("{:016x}", i.wrapping_mul(0x9E3779B97F4A7C15))),
    );

    Value::object(map)
}

/// Generate a deterministic 128-dimensional vector from an index.
pub fn vector_128d(i: u64) -> Vec<f32> {
    let seed = i as f32;
    (0..128)
        .map(|d| (seed * 0.1 + d as f32 * 0.7).sin() * 0.5 + 0.5)
        .collect()
}

// =============================================================================
// WAL Counter Helpers
// =============================================================================

/// Snapshot WAL counters from a BenchDb (returns zeros for ephemeral).
pub fn snapshot_counters(bench_db: &BenchDb) -> WalCounters {
    bench_db.db.durability_counters().unwrap_or_default()
}

/// Compute delta between two counter snapshots.
pub fn counter_delta(before: &WalCounters, after: &WalCounters) -> WalCounters {
    WalCounters {
        wal_appends: after.wal_appends - before.wal_appends,
        sync_calls: after.sync_calls - before.sync_calls,
        bytes_written: after.bytes_written - before.bytes_written,
        sync_nanos: after.sync_nanos - before.sync_nanos,
    }
}

/// Print WAL counter delta to stderr.
pub fn report_counters(label: &str, delta: &WalCounters, iterations: u64) {
    if delta.wal_appends == 0 && delta.sync_calls == 0 {
        return; // Skip for ephemeral mode
    }
    let appends_per_op = delta.wal_appends as f64 / iterations as f64;
    let syncs_per_op = delta.sync_calls as f64 / iterations as f64;
    let bytes_per_op = delta.bytes_written as f64 / iterations as f64;
    let avg_sync_us = if delta.sync_calls > 0 {
        (delta.sync_nanos as f64 / delta.sync_calls as f64) / 1_000.0
    } else {
        0.0
    };
    eprintln!(
        "  {:<45} appends/op={:.1}  syncs/op={:.1}  bytes/op={:.0}  avg_sync={:.0}\u{b5}s",
        label, appends_per_op, syncs_per_op, bytes_per_op, avg_sync_us,
    );
}

/// Run `f` for `n` iterations with WAL counter tracking.
pub fn measure_with_counters<F: FnMut()>(
    bench_db: &BenchDb,
    n: usize,
    mut f: F,
) -> (Percentiles, WalCounters) {
    let before = snapshot_counters(bench_db);
    let percentiles = measure_percentiles(n, &mut f);
    let after = snapshot_counters(bench_db);
    let delta = counter_delta(&before, &after);
    (percentiles, delta)
}
