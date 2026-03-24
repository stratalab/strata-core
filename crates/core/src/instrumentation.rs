//! Performance instrumentation for optimization
//!
//! Feature-gated to avoid overhead in production.
//! Enable with: cargo build --features perf-trace
//!
//! # Usage
//!
//! ```text
//! use strata_core::instrumentation::PerfTrace;
//!
//! #[cfg(feature = "perf-trace")]
//! let mut trace = PerfTrace::new();
//!
//! // Time a section
//! let result = perf_time!(trace, snapshot_acquire_ns, {
//!     engine.snapshot()
//! });
//!
//! #[cfg(feature = "perf-trace")]
//! println!("{}", trace.summary());
//! ```

/// Per-operation performance trace
///
/// When `perf-trace` feature is enabled, this struct captures
/// timing information for each phase of an operation.
#[cfg(feature = "perf-trace")]
#[derive(Debug, Default, Clone)]
pub struct PerfTrace {
    /// Time to acquire snapshot (ns)
    pub snapshot_acquire_ns: u64,
    /// Time to validate read set (ns)
    pub read_set_validate_ns: u64,
    /// Time to apply write set (ns)
    pub write_set_apply_ns: u64,
    /// Time to append to WAL (ns)
    pub wal_append_ns: u64,
    /// Time to fsync (ns)
    pub fsync_ns: u64,
    /// Total commit time (ns)
    pub commit_total_ns: u64,
    /// Number of keys read
    pub keys_read: usize,
    /// Number of keys written
    pub keys_written: usize,
}

#[cfg(feature = "perf-trace")]
impl PerfTrace {
    /// Create new empty trace
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a timed section
    pub fn time<F, T>(f: F) -> (T, u64)
    where
        F: FnOnce() -> T,
    {
        let start = std::time::Instant::now();
        let result = f();
        let elapsed = start.elapsed().as_nanos() as u64;
        (result, elapsed)
    }

    /// Format as human-readable string
    pub fn summary(&self) -> String {
        format!(
            "snapshot: {}ns, validate: {}ns, apply: {}ns, wal: {}ns, fsync: {}ns, total: {}ns ({} reads, {} writes)",
            self.snapshot_acquire_ns,
            self.read_set_validate_ns,
            self.write_set_apply_ns,
            self.wal_append_ns,
            self.fsync_ns,
            self.commit_total_ns,
            self.keys_read,
            self.keys_written,
        )
    }

    /// Calculate percentage breakdown
    pub fn breakdown(&self) -> PerfBreakdown {
        let total = self.commit_total_ns.max(1) as f64;
        PerfBreakdown {
            snapshot_pct: self.snapshot_acquire_ns as f64 / total * 100.0,
            validate_pct: self.read_set_validate_ns as f64 / total * 100.0,
            apply_pct: self.write_set_apply_ns as f64 / total * 100.0,
            wal_pct: self.wal_append_ns as f64 / total * 100.0,
            fsync_pct: self.fsync_ns as f64 / total * 100.0,
        }
    }
}

/// Percentage breakdown of operation time
#[cfg(feature = "perf-trace")]
#[derive(Debug, Clone)]
pub struct PerfBreakdown {
    /// Percentage of time in snapshot acquisition
    pub snapshot_pct: f64,
    /// Percentage of time in read set validation
    pub validate_pct: f64,
    /// Percentage of time in write set application
    pub apply_pct: f64,
    /// Percentage of time in WAL append
    pub wal_pct: f64,
    /// Percentage of time in fsync
    pub fsync_pct: f64,
}

/// No-op trace for production builds
#[cfg(not(feature = "perf-trace"))]
#[derive(Debug, Default, Clone, Copy)]
pub struct PerfTrace;

#[cfg(not(feature = "perf-trace"))]
impl PerfTrace {
    /// Create new empty trace (no-op)
    pub fn new() -> Self {
        Self
    }

    /// Format as human-readable string (no-op)
    pub fn summary(&self) -> &'static str {
        "perf-trace disabled"
    }
}

/// Macro for conditional timing
///
/// When `perf-trace` is enabled, times the expression and stores in trace.
/// When disabled, just evaluates the expression with zero overhead.
///
/// # Example
///
/// ```text
/// let mut trace = PerfTrace::new();
/// let snapshot = perf_time!(trace, snapshot_acquire_ns, {
///     engine.snapshot()
/// });
/// ```
#[cfg(feature = "perf-trace")]
#[macro_export]
macro_rules! perf_time {
    ($trace:expr, $field:ident, $expr:expr) => {{
        let start = std::time::Instant::now();
        let result = $expr;
        $trace.$field = start.elapsed().as_nanos() as u64;
        result
    }};
}

/// No-op version of perf_time! macro when `perf-trace` feature is disabled.
///
/// Simply evaluates the expression with zero overhead.
#[cfg(not(feature = "perf-trace"))]
#[macro_export]
macro_rules! perf_time {
    ($trace:expr, $field:ident, $expr:expr) => {
        $expr
    };
}

/// Aggregate performance statistics
#[cfg(feature = "perf-trace")]
#[derive(Debug, Default)]
pub struct PerfStats {
    traces: Vec<PerfTrace>,
}

#[cfg(feature = "perf-trace")]
impl PerfStats {
    /// Create new empty stats collector
    pub fn new() -> Self {
        Self { traces: Vec::new() }
    }

    /// Record a trace
    pub fn record(&mut self, trace: PerfTrace) {
        self.traces.push(trace);
    }

    /// Get number of recorded traces
    pub fn count(&self) -> usize {
        self.traces.len()
    }

    /// Calculate mean commit time (ns)
    pub fn mean_commit_ns(&self) -> f64 {
        if self.traces.is_empty() {
            return 0.0;
        }
        let sum: u64 = self.traces.iter().map(|t| t.commit_total_ns).sum();
        sum as f64 / self.traces.len() as f64
    }

    /// Calculate p99 commit time (ns)
    pub fn p99_commit_ns(&self) -> u64 {
        if self.traces.is_empty() {
            return 0;
        }
        let mut sorted: Vec<_> = self.traces.iter().map(|t| t.commit_total_ns).collect();
        sorted.sort();
        let idx = (sorted.len() as f64 * 0.99) as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    /// Calculate mean snapshot acquisition time (ns)
    pub fn mean_snapshot_ns(&self) -> f64 {
        if self.traces.is_empty() {
            return 0.0;
        }
        let sum: u64 = self.traces.iter().map(|t| t.snapshot_acquire_ns).sum();
        sum as f64 / self.traces.len() as f64
    }

    /// Get aggregate breakdown percentages
    pub fn aggregate_breakdown(&self) -> Option<PerfBreakdown> {
        if self.traces.is_empty() {
            return None;
        }

        let total_commit: u64 = self.traces.iter().map(|t| t.commit_total_ns).sum();
        let total_snapshot: u64 = self.traces.iter().map(|t| t.snapshot_acquire_ns).sum();
        let total_validate: u64 = self.traces.iter().map(|t| t.read_set_validate_ns).sum();
        let total_apply: u64 = self.traces.iter().map(|t| t.write_set_apply_ns).sum();
        let total_wal: u64 = self.traces.iter().map(|t| t.wal_append_ns).sum();
        let total_fsync: u64 = self.traces.iter().map(|t| t.fsync_ns).sum();

        let total = total_commit.max(1) as f64;

        Some(PerfBreakdown {
            snapshot_pct: total_snapshot as f64 / total * 100.0,
            validate_pct: total_validate as f64 / total * 100.0,
            apply_pct: total_apply as f64 / total * 100.0,
            wal_pct: total_wal as f64 / total * 100.0,
            fsync_pct: total_fsync as f64 / total * 100.0,
        })
    }

    /// Format as summary string
    pub fn summary(&self) -> String {
        format!(
            "traces: {}, mean: {:.0}ns, p99: {}ns",
            self.count(),
            self.mean_commit_ns(),
            self.p99_commit_ns()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perf_trace_creation() {
        let trace = PerfTrace::new();
        let _ = trace.summary();
    }

    #[cfg(feature = "perf-trace")]
    #[test]
    fn test_perf_time_macro() {
        let mut trace = PerfTrace::new();
        let result = perf_time!(trace, snapshot_acquire_ns, {
            std::thread::sleep(std::time::Duration::from_micros(10));
            42
        });
        assert_eq!(result, 42);
        assert!(trace.snapshot_acquire_ns > 0);
    }

    #[cfg(feature = "perf-trace")]
    #[test]
    fn test_perf_breakdown() {
        let mut trace = PerfTrace::new();
        trace.commit_total_ns = 1000;
        trace.snapshot_acquire_ns = 100;
        trace.fsync_ns = 500;

        let breakdown = trace.breakdown();
        assert!((breakdown.snapshot_pct - 10.0).abs() < 0.1);
        assert!((breakdown.fsync_pct - 50.0).abs() < 0.1);
    }

    #[cfg(feature = "perf-trace")]
    #[test]
    fn test_perf_stats() {
        let mut stats = PerfStats::new();

        for i in 1..=100 {
            let mut trace = PerfTrace::new();
            trace.commit_total_ns = i * 1000;
            stats.record(trace);
        }

        assert_eq!(stats.count(), 100);
        assert!(stats.mean_commit_ns() > 0.0);
        assert!(stats.p99_commit_ns() >= 99000);
    }

    #[cfg(feature = "perf-trace")]
    #[test]
    fn test_perf_stats_aggregate_breakdown() {
        let mut stats = PerfStats::new();

        let mut trace = PerfTrace::new();
        trace.commit_total_ns = 1000;
        trace.snapshot_acquire_ns = 100;
        trace.fsync_ns = 500;
        stats.record(trace);

        let breakdown = stats.aggregate_breakdown().unwrap();
        assert!((breakdown.snapshot_pct - 10.0).abs() < 0.1);
        assert!((breakdown.fsync_pct - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_perf_trace_summary_without_feature() {
        let trace = PerfTrace::new();
        let summary = trace.summary();
        assert!(!summary.is_empty());
    }
}
