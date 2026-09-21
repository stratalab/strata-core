//! Lifecycle configuration facts.

use super::{LifecycleError, LifecycleResult, StorageRuntimeBudget};
use crate::format::TableCompression;

const DEFAULT_MAX_MAINTENANCE_QUEUE_DEPTH: usize = 1024;
const DEFAULT_MAX_RECOVERY_FAULTS: usize = 64;
const DEFAULT_WAL_GROWTH_MAX_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_WAL_GROWTH_MAX_SEGMENTS: usize = 64;
/// Hard cap on retained WAL bytes. When retained WAL exceeds this, mutating commits are
/// rejected (retryable) before appending more WAL, so the WAL can never outrun flush-driven
/// truncation and fill the disk. Sits above the reclaim trigger (`DEFAULT_WAL_GROWTH_MAX_BYTES`)
/// and far above the frozen-mutable budget, so flushing already-frozen data can always drop
/// retained WAL back below it. Edge deployments may tune this lower.
const DEFAULT_WAL_HARD_CAP_BYTES: u64 = 1024 * 1024 * 1024;
/// Write-throttle soft engage point, in permille of pool fullness. Below this, writes run
/// at full speed; above it the per-commit delay ramps toward `max_delay`. 700 sits just
/// below the 750 (`Urgent`) byte tier and the 800 budget high-water, so pacing begins
/// before any hard reject is approached. (Benchmarking: lowering it to 500 / raising the cap
/// to 50 ms did not reduce the rare max tail — that residual is the flush-bound relief of the
/// hard admission block, i.e. RC1/fix #3, not throttle-preventable.)
const DEFAULT_WRITE_THROTTLE_SOFT_PERMILLE: u16 = 700;
/// Maximum per-commit throttle delay (ms) at full pool fullness. Capped small so the
/// throttle paces rather than stalls; the hard budget/admission paths remain the ceiling.
const DEFAULT_WRITE_THROTTLE_MAX_DELAY_MILLIS: u64 = 20;
/// BS3.4b graded-admission rate ramp bounds. `max` is `RocksDB`'s `max_delayed_write_rate` default
/// (the un-throttled ceiling the rate returns to); `min` is `RocksDB`'s 16 KiB/s floor near the hard
/// stop. Only consulted on the `graded` admission path; the `legacy` quadratic path ignores them.
const DEFAULT_WRITE_THROTTLE_MAX_RATE_BYTES_PER_SEC: u64 = 16 * 1024 * 1024;
const DEFAULT_WRITE_THROTTLE_MIN_RATE_BYTES_PER_SEC: u64 = 16 * 1024;
/// BS3.4b graded per-commit delay cap (ms). Bounds a single commit's token-bucket sleep so a large
/// batch at the near-stop floor rate (a 115 KB batch / 16 KiB/s would be ~7 s) cannot stall for
/// seconds; the L0 hard-stop grade handles the sustained extreme instead. Well above the effective
/// in-band pacing (single-digit ms), so it clips only the pathological tail. Graded path only.
const DEFAULT_WRITE_THROTTLE_MAX_GRADED_DELAY_MILLIS: u64 = 250;

/// #3502 Slice D: per-database MVCC version-retention policy. `KeepAll`
/// (the default) retains every version — unbounded time-travel history.
/// `KeepRecentVersions` opts into pruning: a compaction drops versions older
/// than `visible_version - window` per key (keep-newer-than-watermark), under
/// the full safety proof (healthy recovery, no shared tables, no readable
/// inherited layers), publishing the retained-history floor in lockstep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StorageVersionRetentionPolicy {
    KeepAll,
    KeepRecentVersions { window: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleConfig {
    max_maintenance_queue_depth: usize,
    max_recovery_faults: usize,
    close_timeout_policy: LifecycleCloseTimeoutPolicy,
    lossy_recovery: LifecycleLossyRecoveryPolicy,
    storage_budget: StorageRuntimeBudget,
    wal_growth_policy: LifecycleWalGrowthPolicy,
    maintenance_scheduling_policy: LifecycleMaintenanceSchedulingPolicy,
    compaction_io_policy: LifecycleCompactionIoPolicy,
    write_throttle_policy: LifecycleWriteThrottlePolicy,
    // B2: per-database data-block byte target for lifecycle-built tables;
    // None = the table runtime's built-in default. Validated at the options
    // layer (4 KiB minimum, encoded-block ceiling).
    data_block_bytes: Option<u32>,
    cache_preheat_policy: LifecycleCachePreheatPolicy,
    // #3499: compression codec for lifecycle-built L0/compacted tables. Zstd by
    // default (the codec self-describes per block, so mixed uncompressed/Zstd
    // tables read correctly); an edge/measurement deployment can opt out to
    // Uncompressed. Applies to flush and compaction; backpressure, the WAL, and
    // recovery are unaffected.
    table_compression: TableCompression,
    // #3502 Slice D: MVCC version-retention policy. `KeepAll` by default
    // (unbounded history); `KeepRecentVersions` opts a database into pruning.
    version_retention: StorageVersionRetentionPolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleCloseTimeoutPolicy {
    ReturnTypedTimeout,
    WaitForStorageDrain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleLossyRecoveryPolicy {
    Disabled,
    ExplicitlyAllowed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleWalGrowthPolicy {
    enabled: bool,
    max_retained_wal_bytes: u64,
    max_retained_wal_segments: usize,
    /// Optional recovery-replay-count safety: force a checkpoint after this many
    /// commits since the last one. `None` (the default) disables the commit-count
    /// trigger entirely, so flush/checkpoint cadence is size-driven (byte/segment
    /// triggers + size-based memtable rotation) rather than firing on tiny commits.
    max_commits_since_checkpoint: Option<u64>,
    /// Hard cap on retained WAL bytes; see `DEFAULT_WAL_HARD_CAP_BYTES`. Should exceed
    /// `max_retained_wal_bytes` so the reclaim trigger fires before the cap rejects writes.
    max_total_wal_bytes: u64,
}

/// Proportional write-throttle policy (fix #2). Below `soft_ratio_permille` pool fullness
/// writes run at full speed; above it a per-commit delay ramps quadratically toward
/// `max_delay_millis` as fullness approaches the hard memory budget, pacing the writer to the
/// flush-limited rate so the hard reject is not reached under steady load. Durations are
/// stored as millis to keep `const fn` constructors and avoid a `Duration` import here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleWriteThrottlePolicy {
    enabled: bool,
    soft_ratio_permille: u16,
    max_delay_millis: u64,
    /// BS3.4b graded path: the un-throttled ceiling and the near-stop floor for the debt-adaptive
    /// rate ramp (bytes/sec), plus the per-commit delay cap (ms). Unused on the legacy quadratic path.
    max_rate_bytes_per_sec: u64,
    min_rate_bytes_per_sec: u64,
    max_graded_delay_millis: u64,
}

impl LifecycleWriteThrottlePolicy {
    pub(crate) const fn new(soft_ratio_permille: u16, max_delay_millis: u64) -> Self {
        Self {
            enabled: true,
            soft_ratio_permille,
            max_delay_millis,
            max_rate_bytes_per_sec: DEFAULT_WRITE_THROTTLE_MAX_RATE_BYTES_PER_SEC,
            min_rate_bytes_per_sec: DEFAULT_WRITE_THROTTLE_MIN_RATE_BYTES_PER_SEC,
            max_graded_delay_millis: DEFAULT_WRITE_THROTTLE_MAX_GRADED_DELAY_MILLIS,
        }
    }

    #[cfg(test)]
    pub(crate) const fn disabled() -> Self {
        Self {
            enabled: false,
            soft_ratio_permille: DEFAULT_WRITE_THROTTLE_SOFT_PERMILLE,
            max_delay_millis: DEFAULT_WRITE_THROTTLE_MAX_DELAY_MILLIS,
            max_rate_bytes_per_sec: DEFAULT_WRITE_THROTTLE_MAX_RATE_BYTES_PER_SEC,
            min_rate_bytes_per_sec: DEFAULT_WRITE_THROTTLE_MIN_RATE_BYTES_PER_SEC,
            max_graded_delay_millis: DEFAULT_WRITE_THROTTLE_MAX_GRADED_DELAY_MILLIS,
        }
    }

    pub(crate) const fn enabled(self) -> bool {
        self.enabled
    }

    pub(crate) const fn soft_ratio_permille(self) -> u16 {
        self.soft_ratio_permille
    }

    pub(crate) const fn max_delay_millis(self) -> u64 {
        self.max_delay_millis
    }

    pub(crate) const fn max_rate_bytes_per_sec(self) -> u64 {
        self.max_rate_bytes_per_sec
    }

    pub(crate) const fn min_rate_bytes_per_sec(self) -> u64 {
        self.min_rate_bytes_per_sec
    }

    pub(crate) const fn max_graded_delay_millis(self) -> u64 {
        self.max_graded_delay_millis
    }

    pub(crate) fn validate(self) -> LifecycleResult<()> {
        if self.enabled && (self.soft_ratio_permille == 0 || self.soft_ratio_permille >= 1000) {
            return Err(LifecycleError::InvalidConfig {
                field: "write_throttle_soft_ratio_permille",
                reason: "must be in 1..1000 when the write throttle is enabled",
            });
        }
        if self.enabled && self.max_delay_millis == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "write_throttle_max_delay_millis",
                reason: "must be nonzero when the write throttle is enabled",
            });
        }
        if self.enabled
            && (self.min_rate_bytes_per_sec == 0
                || self.min_rate_bytes_per_sec > self.max_rate_bytes_per_sec)
        {
            return Err(LifecycleError::InvalidConfig {
                field: "write_throttle_rate_bounds",
                reason: "min rate must be nonzero and not exceed max rate",
            });
        }
        if self.enabled && self.max_graded_delay_millis == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "write_throttle_max_graded_delay_millis",
                reason: "must be nonzero when the write throttle is enabled",
            });
        }
        Ok(())
    }
}

impl Default for LifecycleWriteThrottlePolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_WRITE_THROTTLE_SOFT_PERMILLE,
            DEFAULT_WRITE_THROTTLE_MAX_DELAY_MILLIS,
        )
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum LifecycleMaintenanceSchedulingPolicy {
    Disabled,
    #[default]
    EvaluateAndEnqueue,
    DeterministicInline,
    Background,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum LifecycleCompactionIoPolicy {
    #[default]
    Unlimited,
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    PerTaskByteBudget { max_bytes: u64 },
}

/// C2: whether the durable runtime re-fills the block cache from live tables
/// when background maintenance is otherwise idle. Configured at open.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum LifecycleCachePreheatPolicy {
    #[default]
    WhenIdle,
    Disabled,
}

impl LifecycleConfig {
    pub(crate) fn new(
        max_maintenance_queue_depth: usize,
        max_recovery_faults: usize,
        close_timeout_policy: LifecycleCloseTimeoutPolicy,
        lossy_recovery: LifecycleLossyRecoveryPolicy,
    ) -> LifecycleResult<Self> {
        let config = Self {
            max_maintenance_queue_depth,
            max_recovery_faults,
            close_timeout_policy,
            lossy_recovery,
            storage_budget: StorageRuntimeBudget::default(),
            wal_growth_policy: LifecycleWalGrowthPolicy::default(),
            maintenance_scheduling_policy: LifecycleMaintenanceSchedulingPolicy::default(),
            compaction_io_policy: LifecycleCompactionIoPolicy::default(),
            write_throttle_policy: LifecycleWriteThrottlePolicy::default(),
            data_block_bytes: None,
            cache_preheat_policy: LifecycleCachePreheatPolicy::default(),
            // Uncompressed at this layer keeps internal/test scaffolding
            // byte-stable and golden-safe; the production default (Zstd) is
            // applied at the options layer via `with_table_compression`, and
            // #3499's compaction-request default is Zstd there.
            table_compression: TableCompression::Uncompressed,
            // #3502 Slice D: pruning is opt-in; retain all versions by default.
            version_retention: StorageVersionRetentionPolicy::KeepAll,
        };
        config.validate()?;
        Ok(config)
    }

    /// #3502 Slice D: the MVCC version-retention policy for this database.
    pub(crate) const fn version_retention(&self) -> StorageVersionRetentionPolicy {
        self.version_retention
    }

    /// #3502 Slice D: opt into version pruning (`KeepAll` by default).
    pub(crate) const fn with_version_retention(
        mut self,
        version_retention: StorageVersionRetentionPolicy,
    ) -> Self {
        self.version_retention = version_retention;
        self
    }

    /// #3499: the compression codec for lifecycle-built tables.
    pub(crate) const fn table_compression(&self) -> TableCompression {
        self.table_compression
    }

    /// #3499: override the table compression codec (Zstd by default).
    pub(crate) const fn with_table_compression(mut self, compression: TableCompression) -> Self {
        self.table_compression = compression;
        self
    }

    /// B2: override the data-block byte target for lifecycle-built tables.
    pub(crate) fn with_data_block_bytes(
        mut self,
        data_block_bytes: Option<u32>,
    ) -> LifecycleResult<Self> {
        self.data_block_bytes = data_block_bytes;
        self.validate()?;
        Ok(self)
    }

    /// C2: set the idle block-cache preheat policy (configured at open).
    pub(crate) fn with_cache_preheat_policy(
        mut self,
        cache_preheat_policy: LifecycleCachePreheatPolicy,
    ) -> LifecycleResult<Self> {
        self.cache_preheat_policy = cache_preheat_policy;
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn with_storage_budget(
        mut self,
        storage_budget: StorageRuntimeBudget,
    ) -> LifecycleResult<Self> {
        storage_budget.validate()?;
        self.storage_budget = storage_budget;
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn with_wal_growth_policy(
        mut self,
        wal_growth_policy: LifecycleWalGrowthPolicy,
    ) -> LifecycleResult<Self> {
        wal_growth_policy.validate()?;
        self.wal_growth_policy = wal_growth_policy;
        self.validate()?;
        Ok(self)
    }

    #[cfg(test)]
    pub(crate) fn with_write_throttle_policy(
        mut self,
        write_throttle_policy: LifecycleWriteThrottlePolicy,
    ) -> LifecycleResult<Self> {
        write_throttle_policy.validate()?;
        self.write_throttle_policy = write_throttle_policy;
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn with_maintenance_scheduling_policy(
        mut self,
        maintenance_scheduling_policy: LifecycleMaintenanceSchedulingPolicy,
    ) -> LifecycleResult<Self> {
        self.maintenance_scheduling_policy = maintenance_scheduling_policy;
        self.validate()?;
        Ok(self)
    }

    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn with_compaction_io_policy(
        mut self,
        compaction_io_policy: LifecycleCompactionIoPolicy,
    ) -> LifecycleResult<Self> {
        compaction_io_policy.validate()?;
        self.compaction_io_policy = compaction_io_policy;
        self.validate()?;
        Ok(self)
    }

    pub(crate) const fn max_maintenance_queue_depth(self) -> usize {
        self.max_maintenance_queue_depth
    }

    pub(crate) const fn max_recovery_faults(self) -> usize {
        self.max_recovery_faults
    }

    pub(crate) const fn close_timeout_policy(self) -> LifecycleCloseTimeoutPolicy {
        self.close_timeout_policy
    }

    pub(crate) const fn lossy_recovery(self) -> LifecycleLossyRecoveryPolicy {
        self.lossy_recovery
    }

    pub(crate) const fn data_block_bytes(self) -> Option<u32> {
        self.data_block_bytes
    }

    pub(crate) const fn cache_preheat_policy(self) -> LifecycleCachePreheatPolicy {
        self.cache_preheat_policy
    }

    pub(crate) const fn storage_budget(self) -> StorageRuntimeBudget {
        self.storage_budget
    }

    pub(crate) const fn wal_growth_policy(self) -> LifecycleWalGrowthPolicy {
        self.wal_growth_policy
    }

    pub(crate) const fn write_throttle_policy(self) -> LifecycleWriteThrottlePolicy {
        self.write_throttle_policy
    }

    pub(crate) const fn maintenance_scheduling_policy(
        self,
    ) -> LifecycleMaintenanceSchedulingPolicy {
        self.maintenance_scheduling_policy
    }

    pub(crate) const fn compaction_io_policy(self) -> LifecycleCompactionIoPolicy {
        self.compaction_io_policy
    }

    pub(crate) fn validate(self) -> LifecycleResult<()> {
        if self.max_maintenance_queue_depth == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "max_maintenance_queue_depth",
                reason: "must be nonzero",
            });
        }
        if self.max_recovery_faults == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "max_recovery_faults",
                reason: "must be nonzero",
            });
        }
        self.storage_budget.validate()?;
        self.wal_growth_policy.validate()?;
        self.compaction_io_policy.validate()?;
        self.write_throttle_policy.validate()?;
        Ok(())
    }
}

impl LifecycleWalGrowthPolicy {
    pub(crate) const fn new(
        max_retained_wal_bytes: u64,
        max_retained_wal_segments: usize,
        max_commits_since_checkpoint: Option<u64>,
    ) -> Self {
        Self {
            enabled: true,
            max_retained_wal_bytes,
            max_retained_wal_segments,
            max_commits_since_checkpoint,
            max_total_wal_bytes: DEFAULT_WAL_HARD_CAP_BYTES,
        }
    }

    pub(crate) const fn disabled() -> Self {
        Self {
            enabled: false,
            max_retained_wal_bytes: DEFAULT_WAL_GROWTH_MAX_BYTES,
            max_retained_wal_segments: DEFAULT_WAL_GROWTH_MAX_SEGMENTS,
            max_commits_since_checkpoint: None,
            max_total_wal_bytes: DEFAULT_WAL_HARD_CAP_BYTES,
        }
    }

    pub(crate) const fn enabled(self) -> bool {
        self.enabled
    }

    pub(crate) const fn max_retained_wal_bytes(self) -> u64 {
        self.max_retained_wal_bytes
    }

    pub(crate) const fn max_retained_wal_segments(self) -> usize {
        self.max_retained_wal_segments
    }

    pub(crate) const fn max_commits_since_checkpoint(self) -> Option<u64> {
        self.max_commits_since_checkpoint
    }

    pub(crate) const fn max_total_wal_bytes(self) -> u64 {
        self.max_total_wal_bytes
    }

    /// Override the retained-WAL hard cap (bytes). The default (`DEFAULT_WAL_HARD_CAP_BYTES`)
    /// suits server deployments; smaller suits edge. Used by tests to exercise the cap with
    /// small data.
    #[cfg(test)]
    pub(crate) const fn with_max_total_wal_bytes(mut self, max_total_wal_bytes: u64) -> Self {
        self.max_total_wal_bytes = max_total_wal_bytes;
        self
    }

    pub(crate) fn validate(self) -> LifecycleResult<()> {
        if self.enabled && self.max_retained_wal_bytes == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "max_retained_wal_bytes",
                reason: "must be nonzero when WAL growth policy is enabled",
            });
        }
        if self.enabled && self.max_retained_wal_segments == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "max_retained_wal_segments",
                reason: "must be nonzero when WAL growth policy is enabled",
            });
        }
        if self.enabled && self.max_commits_since_checkpoint == Some(0) {
            return Err(LifecycleError::InvalidConfig {
                field: "max_commits_since_checkpoint",
                reason: "must be nonzero when WAL growth policy is enabled",
            });
        }
        if self.enabled && self.max_total_wal_bytes == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "max_total_wal_bytes",
                reason: "must be nonzero when WAL growth policy is enabled",
            });
        }
        Ok(())
    }
}

impl Default for LifecycleWalGrowthPolicy {
    fn default() -> Self {
        // Commit-count trigger off by default: flush/checkpoint cadence is
        // size-driven (byte/segment triggers + size-based rotation), matching the
        // pre-V1 engine and RocksDB. Callers opt into the commit-count safety via
        // `StorageWalGrowthPolicy::Thresholds`.
        Self::new(
            DEFAULT_WAL_GROWTH_MAX_BYTES,
            DEFAULT_WAL_GROWTH_MAX_SEGMENTS,
            None,
        )
    }
}

impl LifecycleMaintenanceSchedulingPolicy {
    pub(crate) const fn enabled(self) -> bool {
        matches!(
            self,
            Self::EvaluateAndEnqueue | Self::DeterministicInline | Self::Background
        )
    }
}

impl LifecycleCompactionIoPolicy {
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) const fn per_task_byte_budget(max_bytes: u64) -> Self {
        Self::PerTaskByteBudget { max_bytes }
    }

    pub(crate) const fn max_bytes_per_task(self) -> Option<u64> {
        match self {
            Self::Unlimited => None,
            Self::PerTaskByteBudget { max_bytes } => Some(max_bytes),
        }
    }

    pub(crate) const fn validate(self) -> LifecycleResult<()> {
        match self {
            Self::PerTaskByteBudget { max_bytes: 0 } => Err(LifecycleError::InvalidConfig {
                field: "compaction_io_policy.max_bytes_per_task",
                reason: "must be nonzero",
            }),
            Self::Unlimited | Self::PerTaskByteBudget { .. } => Ok(()),
        }
    }
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_MAINTENANCE_QUEUE_DEPTH,
            DEFAULT_MAX_RECOVERY_FAULTS,
            LifecycleCloseTimeoutPolicy::ReturnTypedTimeout,
            LifecycleLossyRecoveryPolicy::Disabled,
        )
        .expect("default lifecycle configuration is valid")
    }
}
