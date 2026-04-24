//! Multi-threaded scaling experiment infrastructure.
//!
//! Provides the core `run_scaling_experiment` function that spawns N threads,
//! coordinates warmup and measurement phases via barriers, and aggregates
//! per-thread results into a single `ScalingResult`.

use super::metrics::{delta_process_metrics, snapshot_process_metrics, ProcessMetrics};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use stratadb::Strata;

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// Per-thread results collected during the measurement phase.
pub struct ThreadResult {
    /// Number of successful operations.
    pub ops: u64,
    /// Number of aborted/conflicted attempts.
    pub aborts: u64,
    /// Sampled operation latencies (reservoir sampling, max `RESERVOIR_SIZE`).
    pub latencies: Vec<Duration>,
}

/// Maximum latency samples kept per thread (reservoir sampling).
const RESERVOIR_SIZE: usize = 10_000;

/// Aggregated results for one (workload, mode, thread_count) run.
#[allow(dead_code)]
pub struct ScalingResult {
    pub threads: usize,
    pub duration: Duration,
    pub total_ops: u64,
    pub total_aborts: u64,
    pub ops_per_sec: f64,
    pub ops_per_sec_per_core: f64,
    pub abort_rate_pct: f64,
    pub retries_per_commit: f64,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub cpu: ProcessMetrics,
    pub wal: WalDelta,
}

/// Delta of WAL counters between before and after measurement.
#[derive(Debug, Clone, Default)]
pub struct WalDelta {
    pub wal_appends: u64,
    pub sync_calls: u64,
}

// ---------------------------------------------------------------------------
// Thread sweep
// ---------------------------------------------------------------------------

/// Generate thread counts for the sweep: 1, 2, 4, 8, ... up to 2x physical cores.
pub fn thread_counts() -> Vec<usize> {
    let cores = physical_cores();
    let max = cores * 2;
    let mut counts = Vec::new();
    let mut n = 1usize;
    while n <= max {
        counts.push(n);
        if n == 1 {
            n = 2;
        } else {
            n *= 2;
        }
    }
    // Ensure we include the exact core count if it's not a power of 2
    if !counts.contains(&cores) && cores > 1 {
        counts.push(cores);
        counts.sort();
    }
    counts
}

/// Parse a comma-separated thread count string (e.g. "1,2,4").
pub fn parse_thread_counts(s: &str) -> Vec<usize> {
    s.split(',')
        .filter_map(|x| x.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .collect()
}

/// Detect number of physical cores (falls back to available parallelism).
pub fn physical_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

// ---------------------------------------------------------------------------
// Reservoir sampler
// ---------------------------------------------------------------------------

/// Simple reservoir sampler that keeps at most `RESERVOIR_SIZE` items.
///
/// Uses Algorithm R (Vitter, 1985) with a simple LCG for speed.
pub struct ReservoirSampler {
    samples: Vec<Duration>,
    count: u64,
    rng_state: u64,
}

impl ReservoirSampler {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            samples: Vec::with_capacity(RESERVOIR_SIZE),
            count: 0,
            // seed with address of self + thread id for uniqueness
            rng_state: 0xdeadbeef,
        }
    }

    /// Seed with a unique value per thread.
    pub fn with_seed(seed: u64) -> Self {
        Self {
            samples: Vec::with_capacity(RESERVOIR_SIZE),
            count: 0,
            rng_state: seed.wrapping_add(0xdeadbeef),
        }
    }

    /// Record a latency sample.
    #[inline]
    pub fn record(&mut self, d: Duration) {
        self.count += 1;
        if self.samples.len() < RESERVOIR_SIZE {
            self.samples.push(d);
        } else {
            // Replace with probability RESERVOIR_SIZE / count
            let j = self.fast_rand() % self.count;
            if j < RESERVOIR_SIZE as u64 {
                self.samples[j as usize] = d;
            }
        }
    }

    /// Consume into the collected samples.
    pub fn into_samples(self) -> Vec<Duration> {
        self.samples
    }

    /// Fast LCG random number generator (not cryptographic, fine for sampling).
    #[inline]
    fn fast_rand(&mut self) -> u64 {
        self.rng_state = self
            .rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.rng_state >> 33
    }
}

// ---------------------------------------------------------------------------
// Core experiment runner
// ---------------------------------------------------------------------------

/// Run a scaling experiment with the given number of threads.
///
/// # Arguments
///
/// * `strata` - The Strata instance to benchmark. Per-thread handles are created via `new_handle()`.
/// * `num_threads` - Number of worker threads to spawn.
/// * `warmup_secs` - Warmup duration (ops run but aren't counted).
/// * `measure_secs` - Measurement duration.
/// * `work_fn` - Closure called by each thread. Receives `(thread_id, Strata, stop_flag)`.
///   Each thread gets its own `Strata` instance created via `strata.new_handle()`.
///   The closure must respect the `stop` flag.
pub fn run_scaling_experiment<F>(
    strata: &Strata,
    num_threads: usize,
    warmup_secs: u64,
    measure_secs: u64,
    work_fn: F,
) -> ScalingResult
where
    F: Fn(usize, Strata, Arc<AtomicBool>) -> ThreadResult + Send + Sync + 'static,
{
    let work_fn = Arc::new(work_fn);
    let cores = physical_cores();

    // --- Warmup phase ---
    {
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::with_capacity(num_threads);

        for tid in 0..num_threads {
            let thread_strata = strata
                .new_handle()
                .expect("failed to create Strata for thread");
            let barrier = Arc::clone(&barrier);
            let stop = Arc::clone(&stop);
            let work_fn = Arc::clone(&work_fn);

            handles.push(std::thread::spawn(move || {
                barrier.wait();
                work_fn(tid, thread_strata, stop)
            }));
        }

        barrier.wait(); // release all threads
        std::thread::sleep(Duration::from_secs(warmup_secs));
        stop.store(true, Ordering::SeqCst);

        for h in handles {
            let _ = h.join();
        }
    }

    // --- Measurement phase ---
    let barrier = Arc::new(Barrier::new(num_threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(num_threads);

    // Snapshot WAL counters before measurement
    let wal_before = strata.durability_counters().unwrap_or_default();
    let cpu_before = snapshot_process_metrics();

    for tid in 0..num_threads {
        let thread_strata = strata
            .new_handle()
            .expect("failed to create Strata for thread");
        let barrier = Arc::clone(&barrier);
        let stop = Arc::clone(&stop);
        let work_fn = Arc::clone(&work_fn);

        handles.push(std::thread::spawn(move || {
            barrier.wait();
            work_fn(tid, thread_strata, stop)
        }));
    }

    barrier.wait(); // release all threads
    let measure_start = Instant::now();
    std::thread::sleep(Duration::from_secs(measure_secs));
    stop.store(true, Ordering::SeqCst);

    let mut thread_results = Vec::with_capacity(num_threads);
    for h in handles {
        thread_results.push(h.join().expect("worker thread panicked"));
    }
    let actual_duration = measure_start.elapsed();

    // Snapshot WAL counters after measurement
    let wal_after = strata.durability_counters().unwrap_or_default();
    let cpu_after = snapshot_process_metrics();

    // --- Aggregate ---
    let total_ops: u64 = thread_results.iter().map(|r| r.ops).sum();
    let total_aborts: u64 = thread_results.iter().map(|r| r.aborts).sum();
    let total_attempts = total_ops + total_aborts;

    let secs = actual_duration.as_secs_f64();
    let ops_per_sec = total_ops as f64 / secs;
    let ops_per_sec_per_core = ops_per_sec / cores as f64;

    let abort_rate_pct = if total_attempts > 0 {
        total_aborts as f64 / total_attempts as f64 * 100.0
    } else {
        0.0
    };

    let retries_per_commit = if total_ops > 0 {
        total_attempts as f64 / total_ops as f64
    } else {
        0.0
    };

    // Merge latency reservoirs and compute percentiles
    let mut all_latencies: Vec<Duration> = thread_results
        .into_iter()
        .flat_map(|r| r.latencies)
        .collect();
    let (p50, p95, p99) = compute_percentiles(&mut all_latencies);

    let cpu = delta_process_metrics(&cpu_before, &cpu_after);
    let wal = WalDelta {
        wal_appends: wal_after.wal_appends.saturating_sub(wal_before.wal_appends),
        sync_calls: wal_after.sync_calls.saturating_sub(wal_before.sync_calls),
    };

    ScalingResult {
        threads: num_threads,
        duration: actual_duration,
        total_ops,
        total_aborts,
        ops_per_sec,
        ops_per_sec_per_core,
        abort_rate_pct,
        retries_per_commit,
        p50,
        p95,
        p99,
        cpu,
        wal,
    }
}

// ---------------------------------------------------------------------------
// Percentile computation
// ---------------------------------------------------------------------------

fn compute_percentiles(latencies: &mut [Duration]) -> (Duration, Duration, Duration) {
    if latencies.is_empty() {
        return (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    }
    latencies.sort_unstable();
    let len = latencies.len();
    let p50 = latencies[len * 50 / 100];
    let p95 = latencies[(len * 95 / 100).min(len - 1)];
    let p99 = latencies[(len * 99 / 100).min(len - 1)];
    (p50, p95, p99)
}

// ---------------------------------------------------------------------------
// Display helpers
// ---------------------------------------------------------------------------

/// Format a Duration for table display (human-readable).
pub fn fmt_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{} ns", nanos)
    } else if nanos < 1_000_000 {
        format!("{:.1} us", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.1} ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", nanos as f64 / 1_000_000_000.0)
    }
}

/// Format a large number with comma separators.
pub fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let bytes: Vec<u8> = s.bytes().rev().collect();
    let chunks: Vec<String> = bytes
        .chunks(3)
        .map(|chunk| chunk.iter().rev().map(|&b| b as char).collect::<String>())
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    chunks.join(",")
}

/// Format ops/sec with comma separators.
pub fn fmt_ops(ops: f64) -> String {
    fmt_num(ops as u64)
}

/// Print the header row for a scaling result table.
pub fn print_table_header() {
    eprintln!(
        "{:<8}| {:<12}| {:<11}| {:<9}| {:<9}| {:<9}| {:<7}| {:<7}| {:<11}| {:<8}| {:<8}| {:<7}| {:<9}| {:<8}| {:<8}",
        "threads", "ops/sec", "ops/s/core", "p50", "p95", "p99",
        "aborts", "abort%", "retries/op",
        "cpu_usr", "cpu_sys", "vol_cs", "invol_cs",
        "wal_app", "wal_sync"
    );
    eprintln!("{}", "-".repeat(150));
}

/// Print one row of a scaling result table.
pub fn print_table_row(r: &ScalingResult) {
    eprintln!(
        "{:<8}| {:<12}| {:<11}| {:<9}| {:<9}| {:<9}| {:<7}| {:<7.2}| {:<11.2}| {:<8}| {:<8}| {:<7}| {:<9}| {:<8}| {:<8}",
        r.threads,
        fmt_ops(r.ops_per_sec),
        fmt_ops(r.ops_per_sec_per_core),
        fmt_duration(r.p50),
        fmt_duration(r.p95),
        fmt_duration(r.p99),
        fmt_num(r.total_aborts),
        r.abort_rate_pct,
        r.retries_per_commit,
        format!("{} ms", r.cpu.user_time_ms),
        format!("{} ms", r.cpu.system_time_ms),
        r.cpu.voluntary_ctx,
        r.cpu.involuntary_ctx,
        r.wal.wal_appends,
        r.wal.sync_calls,
    );
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::{
        compute_percentiles, fmt_duration, fmt_num, parse_thread_counts, thread_counts,
        ReservoirSampler, RESERVOIR_SIZE,
    };
    use std::time::Duration;

    #[test]
    fn test_thread_counts_includes_1_and_powers_of_2() {
        let tc = thread_counts();
        assert!(tc.contains(&1));
        assert!(tc.contains(&2));
        assert!(tc.len() >= 3);
        // Should be monotonically increasing
        for w in tc.windows(2) {
            assert!(w[0] < w[1]);
        }
    }

    #[test]
    fn test_parse_thread_counts() {
        assert_eq!(parse_thread_counts("1,2,4"), vec![1, 2, 4]);
        assert_eq!(parse_thread_counts("1, 2, 4"), vec![1, 2, 4]);
        assert_eq!(parse_thread_counts("8"), vec![8]);
        assert!(parse_thread_counts("").is_empty());
    }

    #[test]
    fn test_reservoir_sampler_under_capacity() {
        let mut s = ReservoirSampler::new();
        for i in 0..100 {
            s.record(Duration::from_micros(i));
        }
        let samples = s.into_samples();
        assert_eq!(samples.len(), 100);
    }

    #[test]
    fn test_reservoir_sampler_over_capacity() {
        let mut s = ReservoirSampler::new();
        for i in 0..100_000 {
            s.record(Duration::from_nanos(i));
        }
        let samples = s.into_samples();
        assert_eq!(samples.len(), RESERVOIR_SIZE);
    }

    #[test]
    fn test_percentiles_empty() {
        let (p50, p95, p99) = compute_percentiles(&mut []);
        assert_eq!(p50, Duration::ZERO);
        assert_eq!(p95, Duration::ZERO);
        assert_eq!(p99, Duration::ZERO);
    }

    #[test]
    fn test_percentiles_single() {
        let mut v = vec![Duration::from_micros(42)];
        let (p50, p95, p99) = compute_percentiles(&mut v);
        assert_eq!(p50, Duration::from_micros(42));
        assert_eq!(p95, Duration::from_micros(42));
        assert_eq!(p99, Duration::from_micros(42));
    }

    #[test]
    fn test_fmt_num() {
        assert_eq!(fmt_num(0), "0");
        assert_eq!(fmt_num(999), "999");
        assert_eq!(fmt_num(1_000), "1,000");
        assert_eq!(fmt_num(1_000_000), "1,000,000");
    }

    #[test]
    fn test_fmt_duration_ranges() {
        assert!(fmt_duration(Duration::from_nanos(500)).contains("ns"));
        assert!(fmt_duration(Duration::from_micros(50)).contains("us"));
        assert!(fmt_duration(Duration::from_millis(50)).contains("ms"));
        assert!(fmt_duration(Duration::from_secs(2)).contains("s"));
    }
}
