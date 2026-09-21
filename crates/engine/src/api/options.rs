//! Explicit database open options.

use crate::branch::BranchName;
use crate::diagnostics::EngineError;

/// Options for explicit cache database open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheOpenOptions {
    default_branch: Option<BranchName>,
    memory_budget_bytes: Option<u64>,
}

#[allow(clippy::new_without_default)]
impl CacheOpenOptions {
    /// Creates cache open options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            default_branch: None,
            memory_budget_bytes: None,
        }
    }

    /// Selects the default branch for a newly-created database.
    pub fn with_default_branch(mut self, name: impl Into<String>) -> Result<Self, EngineError> {
        self.default_branch = Some(BranchName::new(name)?);
        Ok(self)
    }

    /// Sets the total storage memory budget, in bytes, for the opened database.
    ///
    /// The value is validated by the storage layer at open time; values below
    /// the minimum supported budget are rejected with a storage error.
    #[must_use]
    pub const fn with_memory_budget(mut self, total_bytes: u64) -> Self {
        self.memory_budget_bytes = Some(total_bytes);
        self
    }

    pub(crate) fn into_default_branch(self) -> Option<BranchName> {
        self.default_branch
    }

    pub(crate) const fn memory_budget_bytes(&self) -> Option<u64> {
        self.memory_budget_bytes
    }
}

/// C2: whether the durable database re-fills its block cache from live
/// tables while background maintenance is otherwise idle. `WhenIdle` (the
/// default) keeps read-heavy reopens and post-load steady states warm;
/// `Disabled` opts out (measurement A/B, IO-constrained hosts).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CachePreheat {
    /// Fill the block cache in the background whenever maintenance is idle.
    #[default]
    WhenIdle,
    /// Never preheat; the cache fills from demand misses only.
    Disabled,
}

/// Commit durability policy for a durable-local database.
///
/// `Standard` (the default) acknowledges commits from a buffered WAL and
/// syncs at close, threshold, and rotation points — highest throughput,
/// with a documented crash-loss window for unsynced acknowledgements.
/// `Always` syncs every commit before acknowledging it — every receipt
/// attests `always`, and acknowledged commits survive process kill.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DurabilityMode {
    /// Buffered WAL; commits become durable at the next sync point.
    #[default]
    Standard,
    /// Every commit is synced before acknowledgement.
    Always,
}

/// #3502: MVCC version-retention policy for a durable database. `KeepAll` (the
/// default) retains every committed version — unbounded time-travel history.
/// `KeepRecentVersions` opts into pruning: compaction drops versions older than
/// `window` commits behind the visible head, per key (keep-newer-than-watermark),
/// keeping one below-floor survivor. Pruning is safe-gated (healthy recovery,
/// no tables shared with another branch, complete timeline) and publishes a
/// retained-history floor, so a read `as_of` a pruned version surfaces a
/// history-unavailable error rather than a stale value.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VersionRetention {
    /// Retain every version (the default) — unbounded history.
    KeepAll,
    /// Retain versions newer than `window` commits behind the visible head;
    /// older versions are eligible for pruning during compaction.
    KeepRecentVersions {
        /// Number of commits behind the visible head to keep; versions older
        /// than `visible - window` are eligible for pruning.
        window: u64,
    },
}

impl VersionRetention {
    /// Maps the policy to the keep-newer-than window the storage boundary
    /// consumes: `None` keeps every version (`KeepAll`), `Some(window)` opts
    /// into pruning older than `visible - window`.
    pub(crate) const fn retained_window(self) -> Option<u64> {
        match self {
            Self::KeepAll => None,
            Self::KeepRecentVersions { window } => Some(window),
        }
    }
}

/// Options for explicit durable-local database open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableLocalOpenOptions {
    default_branch: Option<BranchName>,
    memory_budget_bytes: Option<u64>,
    data_block_bytes: Option<u32>,
    cache_preheat: CachePreheat,
    durability: DurabilityMode,
    version_retention: VersionRetention,
}

#[allow(clippy::new_without_default)]
impl DurableLocalOpenOptions {
    /// Creates durable-local open options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            default_branch: None,
            memory_budget_bytes: None,
            data_block_bytes: None,
            cache_preheat: CachePreheat::WhenIdle,
            durability: DurabilityMode::Standard,
            version_retention: VersionRetention::KeepAll,
        }
    }

    /// #3502: opts into MVCC version pruning (`KeepAll` by default). Pruning is
    /// safe-gated and behavior-preserving except that reads `as_of` a version
    /// older than the retention window become history-unavailable.
    #[must_use]
    pub const fn with_version_retention(mut self, version_retention: VersionRetention) -> Self {
        self.version_retention = version_retention;
        self
    }

    pub(crate) const fn version_retention(&self) -> VersionRetention {
        self.version_retention
    }

    /// Selects the commit durability policy (#2756: `Always` makes every
    /// acknowledgement a survival guarantee).
    #[must_use]
    pub const fn with_durability(mut self, durability: DurabilityMode) -> Self {
        self.durability = durability;
        self
    }

    pub(crate) const fn durability(&self) -> DurabilityMode {
        self.durability
    }

    /// Selects the default branch for a newly-created database.
    pub fn with_default_branch(mut self, name: impl Into<String>) -> Result<Self, EngineError> {
        self.default_branch = Some(BranchName::new(name)?);
        Ok(self)
    }

    /// Sets the total storage memory budget, in bytes, for the opened database.
    ///
    /// The value is validated by the storage layer at open time; values below
    /// the minimum supported budget are rejected with a storage error.
    #[must_use]
    pub const fn with_memory_budget(mut self, total_bytes: u64) -> Self {
        self.memory_budget_bytes = Some(total_bytes);
        self
    }

    /// B2: sets the data-block byte target for durable tables (4 KiB..=1 MiB,
    /// validated by the storage layer at open). Smaller blocks reduce
    /// per-miss read amplification at the cost of per-table index metadata.
    #[must_use]
    pub const fn with_data_block_bytes(mut self, data_block_bytes: u32) -> Self {
        self.data_block_bytes = Some(data_block_bytes);
        self
    }

    pub(crate) fn into_default_branch(self) -> Option<BranchName> {
        self.default_branch
    }

    pub(crate) const fn data_block_bytes(&self) -> Option<u32> {
        self.data_block_bytes
    }

    /// C2: sets the idle block-cache preheat policy for the durable database.
    #[must_use]
    pub const fn with_cache_preheat(mut self, cache_preheat: CachePreheat) -> Self {
        self.cache_preheat = cache_preheat;
        self
    }

    pub(crate) const fn cache_preheat(&self) -> CachePreheat {
        self.cache_preheat
    }

    pub(crate) const fn memory_budget_bytes(&self) -> Option<u64> {
        self.memory_budget_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// C2: the preheat policy defaults to `WhenIdle` and round-trips through
    /// the builder.
    #[test]
    fn durable_options_cache_preheat_round_trips() {
        let options = DurableLocalOpenOptions::new();
        assert_eq!(options.cache_preheat(), CachePreheat::WhenIdle);
        let disabled = options.with_cache_preheat(CachePreheat::Disabled);
        assert_eq!(disabled.cache_preheat(), CachePreheat::Disabled);
    }

    /// #3502 Slice D2: retention defaults to `KeepAll` and round-trips through
    /// the builder.
    #[test]
    fn durable_options_version_retention_round_trips() {
        let options = DurableLocalOpenOptions::new();
        assert_eq!(options.version_retention(), VersionRetention::KeepAll);
        let pruning =
            options.with_version_retention(VersionRetention::KeepRecentVersions { window: 4 });
        assert_eq!(
            pruning.version_retention(),
            VersionRetention::KeepRecentVersions { window: 4 }
        );
    }

    /// #3502 Slice D2: the policy→window mapping the durable open path threads
    /// into storage — `KeepAll` keeps every version, `KeepRecentVersions`
    /// carries its window verbatim.
    #[test]
    fn version_retention_maps_to_keep_newer_than_window() {
        assert_eq!(VersionRetention::KeepAll.retained_window(), None);
        assert_eq!(
            VersionRetention::KeepRecentVersions { window: 0 }.retained_window(),
            Some(0)
        );
        assert_eq!(
            VersionRetention::KeepRecentVersions { window: 7 }.retained_window(),
            Some(7)
        );
    }
}
