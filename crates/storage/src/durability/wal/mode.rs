//! Durability mode configuration
//!
//! Controls WAL sync behavior (Cache, Standard, Always).

/// Default maximum time between fsyncs in Standard mode: 100 ms.
pub const DEFAULT_SYNC_INTERVAL_MS: u64 = 100;

/// Default retained write-count threshold in Standard mode.
pub const DEFAULT_SYNC_BATCH_SIZE: usize = 1000;

/// Durability mode for WAL operations
///
/// Controls when the WAL is fsynced to disk. This is orthogonal to
/// PersistenceMode (which controls whether files exist at all).
///
/// # Modes
///
/// | Mode | fsync | Data Loss Window |
/// |------|-------|-----------------|
/// | Cache | Never | All uncommitted |
/// | Always | Every commit | Zero |
/// | Standard | Background/periodic, plus interval fallback | Up to interval |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    /// In-memory cache — all data lost on crash (fastest mode)
    ///
    /// Bypasses WAL entirely. No fsync, no file I/O.
    /// Target latency: <3µs for blind writes.
    /// Use case: Tests, caches, ephemeral data, development.
    ///
    /// # Performance
    ///
    /// This mode enables 250K+ ops/sec by eliminating I/O entirely.
    Cache,

    /// fsync after every commit (slow, maximum durability)
    ///
    /// Use when data loss is unacceptable, even for a single write.
    /// Expect 10ms+ latency per write.
    Always,

    /// fsync through the background flush lifecycle plus interval fallback.
    ///
    /// Good balance of speed and safety. May lose up to `interval_ms` of data
    /// on crash while the background flush thread is active. `batch_size` is
    /// retained in the public type for compatibility, but the current writer
    /// does not use it as an inline fsync trigger.
    /// Target latency: <30µs.
    Standard {
        /// Maximum time between fsyncs in milliseconds
        interval_ms: u64,
        /// Retained write-count threshold. Not currently an inline fsync trigger.
        batch_size: usize,
    },
}

impl DurabilityMode {
    /// Check if this mode requires WAL persistence
    ///
    /// Returns false for Cache mode, true for all others.
    pub fn requires_wal(&self) -> bool {
        !matches!(self, DurabilityMode::Cache)
    }

    /// Check if this mode requires immediate fsync on every commit
    ///
    /// Returns true only for Always mode.
    pub fn requires_immediate_fsync(&self) -> bool {
        matches!(self, DurabilityMode::Always)
    }

    /// Human-readable description of the mode
    pub fn description(&self) -> &'static str {
        match self {
            DurabilityMode::Cache => "Cache (fastest, all data lost on crash)",
            DurabilityMode::Always => "Always sync (safest, slowest)",
            DurabilityMode::Standard { .. } => "Standard (balanced speed/safety)",
        }
    }

    /// Create a standard mode with recommended defaults
    ///
    /// Returns `Standard { interval_ms: 100, batch_size: 1000 }`.
    ///
    /// # Default Values
    ///
    /// - **interval_ms**: 100 - Maximum 100ms between fsyncs
    /// - **batch_size**: 1000 - retained write-count threshold
    ///
    /// # Rationale
    ///
    /// These defaults balance performance and durability:
    /// - 100ms interval keeps data loss window bounded
    /// - 1000 batch size remains available to callers that persist or compare
    ///   the full durability mode, but it is not an inline fsync trigger in the
    ///   current writer runtime
    ///
    /// This is the recommended mode for production workloads.
    pub fn standard_default() -> Self {
        DurabilityMode::Standard {
            interval_ms: DEFAULT_SYNC_INTERVAL_MS,
            batch_size: DEFAULT_SYNC_BATCH_SIZE,
        }
    }
}

impl Default for DurabilityMode {
    fn default() -> Self {
        DurabilityMode::Standard {
            interval_ms: DEFAULT_SYNC_INTERVAL_MS,
            batch_size: DEFAULT_SYNC_BATCH_SIZE,
        }
    }
}
