//! API open options.

use super::{StorageApiError, StorageApiResult};
#[cfg(any(test, feature = "testkit"))]
use crate::lifecycle::StorageRuntimeBudget;
use std::time::Duration;

const DEFAULT_BACKGROUND_WORKER_COUNT: usize = 4;
const DEFAULT_BACKGROUND_QUEUE_DEPTH: usize = 4096;
// A drain wake should accomplish a meaningful chunk of maintenance before
// re-submitting: a small budget fragments work (a single compaction merge can
// exceed 25ms, blowing the old per-wake runtime cap every wake) and adds
// re-submission overhead. Raised so concurrent drains do real work per wake.
const DEFAULT_BACKGROUND_MAX_TASKS_PER_WAKE: usize = 32;
const DEFAULT_BACKGROUND_MAX_RUNTIME_PER_WAKE: Duration = Duration::from_millis(250);

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageDurabilityPolicy {
    Standard,
    Always,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageMode {
    /// Volatile in-memory storage for tests, demos, and explicit
    /// ephemeral sessions. This mode does not persist data across process
    /// lifetime.
    Cache,
    /// Directory-backed local durable storage. Callers must provide an
    /// explicit durable backend handle when opening this mode.
    DurableLocal {
        policy: StorageDurabilityPolicy,
    },
    ObjectDurableCandidate,
    DistributedCandidate,
}

/// How an open without an explicit `memory_budget` sizes storage.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageBudgetPolicy {
    /// The fixed built-in default (512 MiB) — deterministic, the choice for test lanes.
    Default,
    /// Derive the default from host memory at open (25% of usable memory, clamped to
    /// `[1 MiB, 8 GiB]`, container-limit aware); falls back to the fixed default when
    /// the host reports nothing. The product open path uses this (#2905).
    DerivedFromHost,
}

/// Minimum supported explicit storage memory budget. Below this the derived per-pool split would
/// leave a pool too small to be usable; real profiles sit far above it.
const MIN_STORAGE_MEMORY_BUDGET_BYTES: u64 = 1024 * 1024;

/// An explicit storage memory budget, in bytes: the total memory storage may use for its caches,
/// mutable tables, table readers, and maintenance buffers. Storage derives its internal per-pool
/// split from this total. Construct with [`StorageMemoryBudget::new`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct StorageMemoryBudget(u64);

impl StorageMemoryBudget {
    /// Create a budget from a total byte count, rejecting values below the minimum supported size.
    pub fn new(total_bytes: u64) -> StorageApiResult<Self> {
        if total_bytes < MIN_STORAGE_MEMORY_BUDGET_BYTES {
            return Err(StorageApiError::InvalidArgument {
                field: "memory_budget",
                reason: "storage memory budget is below the minimum supported size",
            });
        }
        Ok(Self(total_bytes))
    }

    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.0
    }
}

/// Policy controlling when WAL growth forces a checkpoint.
///
/// Flush/checkpoint cadence is size-driven by default: the byte and segment
/// bounds (plus size-based memtable rotation) drive flushing, matching the
/// pre-V1 engine and `RocksDB`. A commit-count bound is available only as an
/// explicit opt-in via [`Self::Thresholds`] — it forces a checkpoint after a
/// fixed number of commits regardless of how little data accumulated, which is a
/// recovery-replay-count safety, not a normal flush trigger.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageWalGrowthPolicy {
    /// Size-driven defaults: byte and segment bounds only; the commit-count
    /// trigger is **off** (no checkpoint is forced purely on commit count).
    Default,
    /// WAL growth checkpointing fully disabled.
    Disabled,
    /// Explicit bounds. Setting `max_commits_since_checkpoint` opts back into the
    /// commit-count safety in addition to the byte/segment bounds.
    Thresholds {
        max_retained_wal_bytes: u64,
        max_retained_wal_segments: usize,
        max_commits_since_checkpoint: u64,
    },
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageMaintenanceSchedulingPolicy {
    Background,
    DeterministicInline,
    EvaluateAndEnqueue,
    Disabled,
}

/// C2: whether the durable runtime re-fills the block cache from live tables
/// while background maintenance is otherwise idle (publish- and reopen-
/// triggered). `Disabled` is the measurement/opt-out side; the default keeps
/// read-heavy reopens and post-load steady states warm without user action.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StorageCachePreheatPolicy {
    /// Fill the block cache in the background whenever maintenance is idle.
    #[default]
    WhenIdle,
    /// Never preheat; the cache fills from demand misses only.
    Disabled,
}

/// B2: bounds for the configurable data-block byte target.
const MIN_DATA_BLOCK_BYTES: u32 = 4 * 1024;
const MAX_DATA_BLOCK_BYTES: u32 = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageOpenOptions {
    mode: StorageMode,
    strict_recovery: bool,
    budget_policy: StorageBudgetPolicy,
    memory_budget: Option<StorageMemoryBudget>,
    wal_growth_policy: StorageWalGrowthPolicy,
    maintenance_scheduling_policy: StorageMaintenanceSchedulingPolicy,
    background_maintenance: StorageBackgroundMaintenanceOptions,
    wal_segment_size_for_test: Option<u64>,
    data_block_bytes: Option<u32>,
    /// #3499: compression codec for durable lifecycle-built tables (Zstd by
    /// default). Cache mode ignores it (its in-memory tables stay uncompressed).
    table_compression: crate::format::TableCompression,
    /// #3502 Slice D: MVCC version-retention policy (`KeepAll` by default;
    /// pruning is opt-in).
    version_retention: crate::lifecycle::StorageVersionRetentionPolicy,
    cache_preheat_policy: StorageCachePreheatPolicy,
    #[cfg(any(test, feature = "testkit"))]
    storage_budget_for_test: Option<StorageRuntimeBudget>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageBackgroundMaintenanceOptions {
    worker_count: usize,
    scheduler_queue_depth: usize,
    max_tasks_per_wake: usize,
    max_runtime_per_wake: Duration,
}

impl StorageBackgroundMaintenanceOptions {
    #[must_use]
    pub const fn product_default() -> Self {
        Self {
            worker_count: DEFAULT_BACKGROUND_WORKER_COUNT,
            scheduler_queue_depth: DEFAULT_BACKGROUND_QUEUE_DEPTH,
            max_tasks_per_wake: DEFAULT_BACKGROUND_MAX_TASKS_PER_WAKE,
            max_runtime_per_wake: DEFAULT_BACKGROUND_MAX_RUNTIME_PER_WAKE,
        }
    }

    #[must_use]
    pub const fn with_worker_count(mut self, worker_count: usize) -> Self {
        self.worker_count = worker_count;
        self
    }

    #[must_use]
    pub const fn with_scheduler_queue_depth(mut self, scheduler_queue_depth: usize) -> Self {
        self.scheduler_queue_depth = scheduler_queue_depth;
        self
    }

    #[must_use]
    pub const fn with_max_tasks_per_wake(mut self, max_tasks_per_wake: usize) -> Self {
        self.max_tasks_per_wake = max_tasks_per_wake;
        self
    }

    #[must_use]
    pub const fn with_max_runtime_per_wake(mut self, max_runtime_per_wake: Duration) -> Self {
        self.max_runtime_per_wake = max_runtime_per_wake;
        self
    }

    #[must_use]
    pub const fn worker_count(self) -> usize {
        self.worker_count
    }

    #[must_use]
    pub const fn scheduler_queue_depth(self) -> usize {
        self.scheduler_queue_depth
    }

    #[must_use]
    pub const fn max_tasks_per_wake(self) -> usize {
        self.max_tasks_per_wake
    }

    #[must_use]
    pub const fn max_runtime_per_wake(self) -> Duration {
        self.max_runtime_per_wake
    }

    fn validate(self) -> StorageApiResult<()> {
        if self.worker_count == 0 {
            return Err(StorageApiError::InvalidArgument {
                field: "background_worker_count",
                reason: "background maintenance worker count must be greater than zero",
            });
        }
        if self.scheduler_queue_depth == 0 {
            return Err(StorageApiError::InvalidArgument {
                field: "background_scheduler_queue_depth",
                reason: "background maintenance scheduler queue depth must be greater than zero",
            });
        }
        if self.max_tasks_per_wake == 0 {
            return Err(StorageApiError::InvalidArgument {
                field: "background_max_tasks_per_wake",
                reason: "background maintenance max tasks per wake must be greater than zero",
            });
        }
        if self.max_runtime_per_wake.is_zero() {
            return Err(StorageApiError::InvalidArgument {
                field: "background_max_runtime_per_wake",
                reason: "background maintenance max runtime per wake must be greater than zero",
            });
        }
        Ok(())
    }
}

impl StorageOpenOptions {
    /// Open volatile cache storage.
    ///
    /// Cache mode is intentionally non-durable. Use this for tests, demos,
    /// and explicitly ephemeral sessions rather than normal database opens.
    #[must_use]
    pub const fn cache() -> Self {
        Self {
            mode: StorageMode::Cache,
            strict_recovery: true,
            budget_policy: StorageBudgetPolicy::Default,
            memory_budget: None,
            wal_growth_policy: StorageWalGrowthPolicy::Default,
            maintenance_scheduling_policy: StorageMaintenanceSchedulingPolicy::Background,
            background_maintenance: StorageBackgroundMaintenanceOptions::product_default(),
            wal_segment_size_for_test: None,
            data_block_bytes: None,
            table_compression: crate::format::TableCompression::Zstd,
            version_retention: crate::lifecycle::StorageVersionRetentionPolicy::KeepAll,
            cache_preheat_policy: StorageCachePreheatPolicy::WhenIdle,
            #[cfg(any(test, feature = "testkit"))]
            storage_budget_for_test: None,
        }
    }

    /// Open explicitly ephemeral storage.
    ///
    /// This is an intent-revealing alias for [`Self::cache`].
    #[must_use]
    pub const fn ephemeral() -> Self {
        Self::cache()
    }

    /// Open durable local storage through a caller-provided backend handle.
    #[must_use]
    pub const fn durable_local(policy: StorageDurabilityPolicy) -> Self {
        Self {
            mode: StorageMode::DurableLocal { policy },
            strict_recovery: true,
            budget_policy: StorageBudgetPolicy::Default,
            memory_budget: None,
            wal_growth_policy: StorageWalGrowthPolicy::Default,
            maintenance_scheduling_policy: StorageMaintenanceSchedulingPolicy::Background,
            background_maintenance: StorageBackgroundMaintenanceOptions::product_default(),
            wal_segment_size_for_test: None,
            data_block_bytes: None,
            table_compression: crate::format::TableCompression::Zstd,
            version_retention: crate::lifecycle::StorageVersionRetentionPolicy::KeepAll,
            cache_preheat_policy: StorageCachePreheatPolicy::WhenIdle,
            #[cfg(any(test, feature = "testkit"))]
            storage_budget_for_test: None,
        }
    }

    #[must_use]
    pub const fn object_durable_candidate() -> Self {
        Self {
            mode: StorageMode::ObjectDurableCandidate,
            strict_recovery: true,
            budget_policy: StorageBudgetPolicy::Default,
            memory_budget: None,
            wal_growth_policy: StorageWalGrowthPolicy::Default,
            maintenance_scheduling_policy: StorageMaintenanceSchedulingPolicy::Background,
            background_maintenance: StorageBackgroundMaintenanceOptions::product_default(),
            wal_segment_size_for_test: None,
            data_block_bytes: None,
            table_compression: crate::format::TableCompression::Zstd,
            version_retention: crate::lifecycle::StorageVersionRetentionPolicy::KeepAll,
            cache_preheat_policy: StorageCachePreheatPolicy::WhenIdle,
            #[cfg(any(test, feature = "testkit"))]
            storage_budget_for_test: None,
        }
    }

    #[must_use]
    pub const fn distributed_candidate() -> Self {
        Self {
            mode: StorageMode::DistributedCandidate,
            strict_recovery: true,
            budget_policy: StorageBudgetPolicy::Default,
            memory_budget: None,
            wal_growth_policy: StorageWalGrowthPolicy::Default,
            maintenance_scheduling_policy: StorageMaintenanceSchedulingPolicy::Background,
            background_maintenance: StorageBackgroundMaintenanceOptions::product_default(),
            wal_segment_size_for_test: None,
            data_block_bytes: None,
            table_compression: crate::format::TableCompression::Zstd,
            version_retention: crate::lifecycle::StorageVersionRetentionPolicy::KeepAll,
            cache_preheat_policy: StorageCachePreheatPolicy::WhenIdle,
            #[cfg(any(test, feature = "testkit"))]
            storage_budget_for_test: None,
        }
    }

    #[must_use]
    pub const fn with_strict_recovery(mut self, strict_recovery: bool) -> Self {
        self.strict_recovery = strict_recovery;
        self
    }

    #[must_use]
    pub const fn with_budget_policy(mut self, budget_policy: StorageBudgetPolicy) -> Self {
        self.budget_policy = budget_policy;
        self
    }

    /// Set an explicit storage memory budget. When set it takes precedence over `budget_policy`
    /// and bounds both cache and durable opens.
    #[must_use]
    pub const fn with_memory_budget(mut self, memory_budget: StorageMemoryBudget) -> Self {
        self.memory_budget = Some(memory_budget);
        self
    }

    #[must_use]
    pub const fn with_wal_growth_policy(
        mut self,
        wal_growth_policy: StorageWalGrowthPolicy,
    ) -> Self {
        self.wal_growth_policy = wal_growth_policy;
        self
    }

    #[must_use]
    pub const fn with_maintenance_scheduling_policy(
        mut self,
        maintenance_scheduling_policy: StorageMaintenanceSchedulingPolicy,
    ) -> Self {
        self.maintenance_scheduling_policy = maintenance_scheduling_policy;
        self
    }

    #[must_use]
    pub const fn with_background_maintenance(
        mut self,
        background_maintenance: StorageBackgroundMaintenanceOptions,
    ) -> Self {
        self.background_maintenance = background_maintenance;
        self
    }

    #[must_use]
    pub const fn with_background_worker_count(mut self, worker_count: usize) -> Self {
        self.background_maintenance = self.background_maintenance.with_worker_count(worker_count);
        self
    }

    #[must_use]
    pub const fn with_background_scheduler_queue_depth(
        mut self,
        scheduler_queue_depth: usize,
    ) -> Self {
        self.background_maintenance = self
            .background_maintenance
            .with_scheduler_queue_depth(scheduler_queue_depth);
        self
    }

    #[must_use]
    pub const fn with_background_max_tasks_per_wake(mut self, max_tasks_per_wake: usize) -> Self {
        self.background_maintenance = self
            .background_maintenance
            .with_max_tasks_per_wake(max_tasks_per_wake);
        self
    }

    #[must_use]
    pub const fn with_background_max_runtime_per_wake(
        mut self,
        max_runtime_per_wake: Duration,
    ) -> Self {
        self.background_maintenance = self
            .background_maintenance
            .with_max_runtime_per_wake(max_runtime_per_wake);
        self
    }

    /// B2: the data-block byte target for lifecycle-built tables (durable
    /// modes). Smaller blocks cut per-miss read amplification and raise the
    /// block-cache hit rate; the cost is per-table index metadata (decoded at
    /// open) and per-block build overhead. Bounds: 4 KiB..=1 MiB. `None`
    /// keeps the built-in default (64 KiB).
    #[must_use]
    pub const fn with_data_block_bytes(mut self, data_block_bytes: u32) -> Self {
        self.data_block_bytes = Some(data_block_bytes);
        self
    }

    /// #3499: the compression codec for durable lifecycle-built tables.
    /// `pub(crate)` to match `TableCompression`'s visibility — the codec is a
    /// storage-internal type; engine opts in through its own surface.
    pub(crate) const fn table_compression(&self) -> crate::format::TableCompression {
        self.table_compression
    }

    /// #3499: override the durable table compression codec (Zstd by default).
    /// Test-only: the production opt-out belongs on the engine's
    /// `DurableLocalOpenOptions` (deferred, tracked separately) — no shipping
    /// caller overrides the default yet, so gating this to tests keeps the lib
    /// free of dead code while still letting the storage suite pin both codecs.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn with_table_compression_for_test(
        mut self,
        compression: crate::format::TableCompression,
    ) -> Self {
        self.table_compression = compression;
        self
    }

    /// #3502 Slice D: the MVCC version-retention policy (read by the open path
    /// to seed the lifecycle config).
    pub(crate) const fn version_retention(
        &self,
    ) -> crate::lifecycle::StorageVersionRetentionPolicy {
        self.version_retention
    }

    /// #3502 Slice D2: opt into MVCC version pruning — retain versions newer
    /// than `visible - window` per key (keep-newer-than-watermark); `None` keeps
    /// every version (the default). Takes a plain window so the storage boundary
    /// need not expose the internal policy enum; the engine's
    /// `DurableLocalOpenOptions` is the user-facing opt-in surface.
    #[must_use]
    pub const fn with_version_retention_window(mut self, window: Option<u64>) -> Self {
        self.version_retention = match window {
            None => crate::lifecycle::StorageVersionRetentionPolicy::KeepAll,
            Some(window) => {
                crate::lifecycle::StorageVersionRetentionPolicy::KeepRecentVersions { window }
            }
        };
        self
    }

    pub(crate) const fn data_block_bytes(&self) -> Option<u32> {
        self.data_block_bytes
    }

    /// C2: set the idle block-cache preheat policy (durable modes; cache mode
    /// has no disk-resident tables to warm from).
    #[must_use]
    pub const fn with_cache_preheat_policy(
        mut self,
        cache_preheat_policy: StorageCachePreheatPolicy,
    ) -> Self {
        self.cache_preheat_policy = cache_preheat_policy;
        self
    }

    pub(crate) const fn cache_preheat_policy(&self) -> StorageCachePreheatPolicy {
        self.cache_preheat_policy
    }

    #[cfg(any(test, feature = "testkit"))]
    #[must_use]
    pub const fn with_wal_segment_size_for_test(mut self, segment_size: u64) -> Self {
        self.wal_segment_size_for_test = Some(segment_size);
        self
    }

    #[cfg(any(test, feature = "testkit"))]
    #[must_use]
    pub(crate) const fn with_storage_budget_for_test(
        mut self,
        storage_budget: StorageRuntimeBudget,
    ) -> Self {
        self.storage_budget_for_test = Some(storage_budget);
        self
    }

    pub fn validate(&self) -> StorageApiResult<()> {
        if let Some(bytes) = self.data_block_bytes {
            if !(MIN_DATA_BLOCK_BYTES..=MAX_DATA_BLOCK_BYTES).contains(&bytes) {
                return Err(StorageApiError::InvalidArgument {
                    field: "data_block_bytes",
                    reason: "data block byte target must be within 4 KiB..=1 MiB",
                });
            }
        }
        self.wal_growth_policy.validate()?;
        self.background_maintenance.validate()?;
        if let Some(segment_size) = self.wal_segment_size_for_test {
            crate::service::WalServiceConfig::new(segment_size)
                .validate()
                .map_err(|_| StorageApiError::InvalidArgument {
                    field: "wal_segment_size",
                    reason: "test WAL segment size is invalid",
                })?;
        }
        match self.mode {
            StorageMode::Cache if !self.strict_recovery => Err(StorageApiError::InvalidArgument {
                field: "strict_recovery",
                reason: "cache mode cannot request lossy durable recovery fallback",
            }),
            StorageMode::Cache | StorageMode::DurableLocal { .. } => Ok(()),
            StorageMode::ObjectDurableCandidate => Err(StorageApiError::UnsupportedCapability {
                capability: "object_durable",
                reason: "object-durable storage is not a V1 production mode",
            }),
            StorageMode::DistributedCandidate => Err(StorageApiError::UnsupportedCapability {
                capability: "distributed_writer",
                reason: "distributed writer coordination is not a V1 production mode",
            }),
        }
    }

    #[must_use]
    pub const fn requires_backend(self) -> bool {
        matches!(self.mode, StorageMode::DurableLocal { .. })
    }

    #[must_use]
    pub const fn mode(&self) -> StorageMode {
        self.mode
    }

    #[must_use]
    pub const fn strict_recovery(&self) -> bool {
        self.strict_recovery
    }

    #[must_use]
    pub const fn budget_policy(&self) -> StorageBudgetPolicy {
        self.budget_policy
    }

    #[must_use]
    pub const fn memory_budget(&self) -> Option<StorageMemoryBudget> {
        self.memory_budget
    }

    #[must_use]
    pub const fn wal_growth_policy(&self) -> StorageWalGrowthPolicy {
        self.wal_growth_policy
    }

    #[must_use]
    pub const fn maintenance_scheduling_policy(&self) -> StorageMaintenanceSchedulingPolicy {
        self.maintenance_scheduling_policy
    }

    #[must_use]
    pub const fn background_maintenance(&self) -> StorageBackgroundMaintenanceOptions {
        self.background_maintenance
    }

    pub(crate) const fn wal_segment_size_for_test(&self) -> Option<u64> {
        self.wal_segment_size_for_test
    }

    #[cfg(any(test, feature = "testkit"))]
    pub(crate) const fn storage_budget_for_test(&self) -> Option<StorageRuntimeBudget> {
        self.storage_budget_for_test
    }
}

impl StorageWalGrowthPolicy {
    #[must_use]
    pub const fn thresholds(
        max_retained_wal_bytes: u64,
        max_retained_wal_segments: usize,
        max_commits_since_checkpoint: u64,
    ) -> Self {
        Self::Thresholds {
            max_retained_wal_bytes,
            max_retained_wal_segments,
            max_commits_since_checkpoint,
        }
    }

    fn validate(self) -> StorageApiResult<()> {
        match self {
            Self::Thresholds {
                max_retained_wal_bytes: 0,
                ..
            } => Err(StorageApiError::InvalidArgument {
                field: "max_retained_wal_bytes",
                reason: "WAL growth byte limit must be greater than zero",
            }),
            Self::Thresholds {
                max_retained_wal_segments: 0,
                ..
            } => Err(StorageApiError::InvalidArgument {
                field: "max_retained_wal_segments",
                reason: "WAL growth segment limit must be greater than zero",
            }),
            Self::Thresholds {
                max_commits_since_checkpoint: 0,
                ..
            } => Err(StorageApiError::InvalidArgument {
                field: "max_commits_since_checkpoint",
                reason: "WAL growth commit limit must be greater than zero",
            }),
            Self::Default | Self::Disabled | Self::Thresholds { .. } => Ok(()),
        }
    }
}
