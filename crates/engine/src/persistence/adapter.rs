//! Adapter from engine persistence plans to the storage crate.

use std::path::PathBuf;

use strata_core::BranchId;
use strata_core::{CommitVersion, Timestamp};
use strata_storage::api::{
    BranchAction, BranchCleanupSummary as StorageBranchCleanupSummary,
    BranchGeneration as StorageBranchGeneration, BranchOutcome as StorageBranchOutcome,
    BranchRequest, BranchStatus as StorageBranchStatus, BranchSummary as StorageBranchSummary,
    CommitBatch, CommitDurabilitySummary, CommitInstantsRequest, CommitMutation, CommitOptions,
    HistoryReadRequest, ImmutableSourceScanReadRequest, PointReadRequest, PrefixScanReadRequest,
    ReadBound, ReadLimit, ScanRange, ScanReadRequest, StorageApiError, StorageApiErrorClass,
    StorageBudgetPolicy, StorageBudgetSource, StorageCachePreheatPolicy, StorageCloseSummary,
    StorageDurabilityPolicy, StorageImmutableSource, StorageKey, StorageMemoryBudget,
    StorageOpenDisposition, StorageOpenOptions, StorageReadRow, StorageRuntime,
    StorageRuntimeState, StorageSpaceId, StorageValue, TimelineBoundsRequest,
    WallClockLookupRequest,
};
use strata_storage::api::{
    MaintenanceRequest, MaintenanceScope,
    MaintenanceSummaryStatus as StorageMaintenanceSummaryStatus, MaintenanceTask,
};

use crate::branch::catalog::{DEFAULT_BRANCH_GENERATION, SYSTEM_BRANCH_ID};
use crate::commit::CommitOutcome;
use crate::diagnostics::{
    CommitOutcomeStatus, EngineError, EngineErrorClass, EngineErrorStatus, EngineResult,
    ErrorClass, ErrorDetail, RetryPolicy,
};
use crate::time_compat::{SystemTime, UNIX_EPOCH};

use super::fault::FaultOp;
#[cfg(any(test, feature = "testkit"))]
use super::fault::{CorruptionSchedule, FaultSchedule, RowCorruption, StorageFaultKind};
use super::{CommitPlan, ReadSelector, RowAddress, RowClass, RowMutation};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PersistenceOpenTarget {
    Cache,
    DurableLocal(PathBuf, crate::api::DurabilityMode),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PersistenceOpenSummary {
    created: bool,
    durable: bool,
    memory_budget_source: MemoryBudgetSource,
}

/// Engine-owned mirror of the storage budget provenance (#2905): consumers of
/// the engine never see storage types.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryBudgetSource {
    /// The caller set a memory budget explicitly.
    Explicit {
        /// The explicit total, in bytes.
        total_bytes: u64,
    },
    /// Derived at open from host memory (25% of usable memory, clamped to `[1 MiB, 8 GiB]`).
    DerivedFromHost {
        /// The derived total, in bytes.
        total_bytes: u64,
        /// The usable host memory the derivation started from (the smaller of
        /// available memory and the cgroup limit), in bytes.
        usable_host_bytes: u64,
    },
    /// The fixed built-in default (the host reported no memory facts, or a deterministic open).
    FixedDefault {
        /// The fixed default total, in bytes.
        total_bytes: u64,
    },
}

impl MemoryBudgetSource {
    /// The resolved total, whatever its provenance.
    #[must_use]
    pub const fn total_bytes(self) -> u64 {
        match self {
            Self::Explicit { total_bytes }
            | Self::DerivedFromHost { total_bytes, .. }
            | Self::FixedDefault { total_bytes } => total_bytes,
        }
    }

    const fn from_storage(source: StorageBudgetSource) -> Self {
        match source {
            StorageBudgetSource::Explicit { total_bytes } => Self::Explicit { total_bytes },
            StorageBudgetSource::DerivedFromHost {
                total_bytes,
                usable_host_bytes,
            } => Self::DerivedFromHost {
                total_bytes,
                usable_host_bytes,
            },
            StorageBudgetSource::FixedDefault { total_bytes } => Self::FixedDefault { total_bytes },
            // Storage's enum is non_exhaustive; an unknown future provenance is
            // reported as the fixed default rather than failing the open.
            _ => Self::FixedDefault {
                total_bytes: source.total_bytes(),
            },
        }
    }
}

impl PersistenceOpenSummary {
    #[must_use]
    pub(crate) const fn memory_budget_source(self) -> MemoryBudgetSource {
        self.memory_budget_source
    }

    #[must_use]
    pub(crate) const fn created(self) -> bool {
        self.created
    }

    #[must_use]
    pub(crate) const fn durable(self) -> bool {
        self.durable
    }
}

/// The host wall-clock instant to stamp on a commit as `committed_at` (UTC
/// epoch micros), or `None` when the clock is before the Unix epoch or overflows
/// `u64` micros (never in practice). Read through the wasm-safe `time_compat`
/// shim. It is never the MVCC clock — `committed_at` never affects ordering or
/// visibility (#3112). Determinism for the IDL tooling is handled downstream by
/// masking `committed_at` (a genuinely volatile value); an injectable clock for
/// replay determinism lands with the durable slice (S2/S3).
fn sample_committed_at() -> Option<Timestamp> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_micros()).ok())
        .map(Timestamp::from_micros)
}

pub(crate) struct StoragePersistence {
    runtime: StorageRuntime<'static>,
    durable: bool,
    /// Armed by artifact import replay: the NEXT commit is stamped with
    /// this timestamp (consumed exactly once). See `crate::artifact`.
    replay_commit_timestamp: Option<Timestamp>,
    /// Held during multi-branch import setup: any commit without a one-shot
    /// replay timestamp (branch and space bookkeeping) is stamped with this
    /// value so structural writes never advance the floor past the content
    /// that replays next. Not consumed; cleared explicitly. See #3070.
    replay_structural_timestamp: Option<Timestamp>,
    #[cfg(any(test, feature = "testkit"))]
    faults: FaultSchedule,
    #[cfg(any(test, feature = "testkit"))]
    corruption: CorruptionSchedule,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PersistenceBranchStatus {
    Active,
    Deleted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PersistenceBranchParent {
    source_branch_id: BranchId,
    fork_version: CommitVersion,
}

impl PersistenceBranchParent {
    pub(crate) const fn fork_version(self) -> CommitVersion {
        self.fork_version
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PersistenceBranchSummary {
    branch_id: BranchId,
    generation: u64,
    status: PersistenceBranchStatus,
    parent: Option<PersistenceBranchParent>,
    created_at: Option<CommitVersion>,
    deleted_at: Option<CommitVersion>,
    state_revision: u64,
}

impl PersistenceBranchSummary {
    pub(crate) const fn generation(self) -> u64 {
        self.generation
    }

    pub(crate) const fn status(self) -> PersistenceBranchStatus {
        self.status
    }

    pub(crate) const fn parent(self) -> Option<PersistenceBranchParent> {
        self.parent
    }

    pub(crate) const fn created_at(self) -> Option<CommitVersion> {
        self.created_at
    }

    pub(crate) const fn deleted_at(self) -> Option<CommitVersion> {
        self.deleted_at
    }

    pub(crate) const fn state_revision(self) -> u64 {
        self.state_revision
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PersistenceBranchCleanup {
    removed_refs: usize,
    releasable_tables: usize,
    protected_tables: usize,
}

impl PersistenceBranchCleanup {
    pub(crate) const fn removed_refs(self) -> usize {
        self.removed_refs
    }

    pub(crate) const fn releasable_tables(self) -> usize {
        self.releasable_tables
    }

    pub(crate) const fn protected_tables(self) -> usize {
        self.protected_tables
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistenceBranchOutcome {
    branch: PersistenceBranchSummary,
    generation_before: Option<u64>,
    generation_after: Option<u64>,
    source_branch_id: Option<BranchId>,
    fork_version: Option<CommitVersion>,
    fork_timestamp: Option<Timestamp>,
    cleanup: Option<PersistenceBranchCleanup>,
}

impl PersistenceBranchOutcome {
    pub(crate) const fn branch(&self) -> PersistenceBranchSummary {
        self.branch
    }

    pub(crate) const fn generation_before(&self) -> Option<u64> {
        self.generation_before
    }

    pub(crate) const fn generation_after(&self) -> Option<u64> {
        self.generation_after
    }

    pub(crate) const fn fork_version(&self) -> Option<CommitVersion> {
        self.fork_version
    }

    pub(crate) const fn fork_timestamp(&self) -> Option<Timestamp> {
        self.fork_timestamp
    }

    pub(crate) const fn cleanup(&self) -> Option<PersistenceBranchCleanup> {
        self.cleanup
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistenceReadRow {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    commit_version: CommitVersion,
    commit_timestamp: Timestamp,
    tombstone: bool,
}

impl PersistenceReadRow {
    fn from_storage(row: &StorageReadRow) -> Self {
        Self {
            key: row.key().as_bytes().to_vec(),
            value: row.value().map(|value| value.as_bytes().to_vec()),
            commit_version: row.commit_version(),
            commit_timestamp: row.commit_timestamp(),
            tombstone: row.is_tombstone(),
        }
    }

    /// B4: the move-based twin for point reads — key and value Vecs move
    /// across the crate boundary instead of being re-copied. Scan and
    /// history paths keep the by-ref constructor above.
    fn from_storage_owned(row: StorageReadRow) -> Self {
        let (key, value, commit_version, commit_timestamp, tombstone) = row.into_read_parts();
        Self {
            key: key.into_bytes(),
            value: value.map(StorageValue::into_bytes),
            commit_version,
            commit_timestamp,
            tombstone,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test(key: Vec<u8>, value: Option<Vec<u8>>, tombstone: bool) -> Self {
        Self {
            key,
            value,
            commit_version: CommitVersion::new(1),
            commit_timestamp: Timestamp::from_micros(1),
            tombstone,
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        &self.key
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    pub(crate) const fn commit_version(&self) -> CommitVersion {
        self.commit_version
    }

    pub(crate) const fn commit_timestamp(&self) -> Timestamp {
        self.commit_timestamp
    }

    pub(crate) const fn is_tombstone(&self) -> bool {
        self.tombstone
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistenceImmutableSource {
    source_id: String,
    source_branch_id: BranchId,
    source_generation: CommitVersion,
    fork_version_cap: Option<CommitVersion>,
    rows: Vec<PersistenceReadRow>,
}

impl PersistenceImmutableSource {
    fn from_storage(source: &StorageImmutableSource) -> Self {
        Self {
            source_id: source.source_id().to_owned(),
            source_branch_id: source.source_branch_id(),
            source_generation: source.source_generation(),
            fork_version_cap: source.fork_version_cap(),
            rows: source
                .rows()
                .iter()
                .map(PersistenceReadRow::from_storage)
                .collect(),
        }
    }

    pub(crate) fn source_id(&self) -> &str {
        &self.source_id
    }

    pub(crate) const fn source_branch_id(&self) -> BranchId {
        self.source_branch_id
    }

    pub(crate) const fn source_generation(&self) -> CommitVersion {
        self.source_generation
    }

    #[allow(dead_code)]
    pub(crate) const fn fork_version_cap(&self) -> Option<CommitVersion> {
        self.fork_version_cap
    }

    pub(crate) fn rows(&self) -> &[PersistenceReadRow] {
        &self.rows
    }
}

impl StoragePersistence {
    #[cfg(test)]
    pub(crate) fn open(
        target: PersistenceOpenTarget,
    ) -> EngineResult<(Self, PersistenceOpenSummary)> {
        Self::open_with_budget(target, None, None, crate::api::CachePreheat::WhenIdle)
    }

    pub(crate) fn open_with_budget(
        target: PersistenceOpenTarget,
        memory_budget_bytes: Option<u64>,
        data_block_bytes: Option<u32>,
        cache_preheat: crate::api::CachePreheat,
    ) -> EngineResult<(Self, PersistenceOpenSummary)> {
        let (runtime, summary, durable) = match target {
            PersistenceOpenTarget::Cache => {
                let options =
                    apply_memory_budget(StorageOpenOptions::cache(), memory_budget_bytes)?;
                let outcome = StorageRuntime::open(options).map_err(map_storage_error)?;
                let (runtime, summary) = outcome.into_parts();
                (runtime, summary, false)
            }
            PersistenceOpenTarget::DurableLocal(path, durability) => {
                let policy = match durability {
                    crate::api::DurabilityMode::Standard => StorageDurabilityPolicy::Standard,
                    crate::api::DurabilityMode::Always => StorageDurabilityPolicy::Always,
                };
                let mut options = apply_memory_budget(
                    StorageOpenOptions::durable_local(policy),
                    memory_budget_bytes,
                )?;
                if let Some(bytes) = data_block_bytes {
                    // B2: durable-only data-block byte target (see options doc).
                    options = options.with_data_block_bytes(bytes);
                }
                // C2: durable-only idle cache preheat (cache mode has no
                // disk-resident tables to warm from).
                options = options.with_cache_preheat_policy(match cache_preheat {
                    crate::api::CachePreheat::WhenIdle => StorageCachePreheatPolicy::WhenIdle,
                    crate::api::CachePreheat::Disabled => StorageCachePreheatPolicy::Disabled,
                });
                let outcome = StorageRuntime::open_durable_local_with_options(path, options)
                    .map_err(map_storage_error)?;
                let (runtime, summary) = outcome.into_parts();
                (runtime, summary, true)
            }
        };
        let created = matches!(summary.disposition(), StorageOpenDisposition::Created);
        let memory_budget_source = MemoryBudgetSource::from_storage(summary.budget_source());
        Ok((
            Self {
                runtime,
                durable,
                replay_commit_timestamp: None,
                replay_structural_timestamp: None,
                #[cfg(any(test, feature = "testkit"))]
                faults: FaultSchedule::default(),
                #[cfg(any(test, feature = "testkit"))]
                corruption: CorruptionSchedule::default(),
            },
            PersistenceOpenSummary {
                created,
                durable,
                memory_budget_source,
            },
        ))
    }

    /// Returns an injected fault for `op` as a mapped engine error, if a test
    /// armed one.
    #[cfg(any(test, feature = "testkit"))]
    fn guard_fault(&mut self, op: FaultOp) -> EngineResult<()> {
        if let Some(error) = self.faults.take(op) {
            return Err(map_storage_error(error));
        }
        Ok(())
    }

    /// Production builds carry no fault schedule, so this is a no-op. The
    /// `unused_self`/`unnecessary_wraps` relaxations are intentional: the method
    /// must keep the same `self.guard_fault(op)?` call shape as the test build so
    /// callers need no `cfg` branching.
    #[cfg(not(any(test, feature = "testkit")))]
    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    fn guard_fault(&self, _op: FaultOp) -> EngineResult<()> {
        Ok(())
    }

    /// Arms a storage fault that fires after `skip` matching persistence calls.
    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn arm_storage_fault(&mut self, op: FaultOp, kind: StorageFaultKind, skip: usize) {
        self.faults.arm(op, kind, skip);
    }

    /// Arms a content corruption applied to the rows the next matching read
    /// returns (after `skip` matching reads pass). The read still succeeds; its
    /// rows come back corrupted, so the engine's decoders run on the real path.
    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn arm_row_corruption(
        &mut self,
        op: FaultOp,
        corruption: RowCorruption,
        skip: usize,
    ) {
        self.corruption.arm(op, corruption, skip);
    }

    /// Applies an armed corruption to a batch of just-read rows, if one is due.
    #[cfg(any(test, feature = "testkit"))]
    fn corrupt_rows(&mut self, op: FaultOp, rows: &mut [PersistenceReadRow]) {
        if let Some(corruption) = self.corruption.take(op) {
            for row in rows.iter_mut() {
                corruption.apply(&mut row.key, &mut row.value);
            }
        }
    }

    /// Production builds carry no corruption schedule, so this is a no-op — the
    /// call shape matches the test build so read paths need no `cfg` branching.
    #[cfg(not(any(test, feature = "testkit")))]
    #[allow(clippy::unused_self)]
    fn corrupt_rows(&self, _op: FaultOp, _rows: &mut [PersistenceReadRow]) {}

    pub(crate) fn create_system_branch_for_new_database(&mut self) -> EngineResult<()> {
        self.ensure_branch_created(SYSTEM_BRANCH_ID, DEFAULT_BRANCH_GENERATION)
    }

    pub(crate) fn ensure_branch_created(
        &mut self,
        branch_id: BranchId,
        generation: u64,
    ) -> EngineResult<()> {
        if self.branch_exists(branch_id)? {
            return Ok(());
        }
        let request = BranchRequest::new(
            branch_id,
            BranchAction::Create,
            Some(StorageBranchGeneration::new(generation)),
        );
        match self.runtime.branch(&request) {
            Ok(_) | Err(StorageApiError::BranchAlreadyExists { .. }) => Ok(()),
            Err(error) => Err(map_storage_error(error)),
        }
    }

    pub(crate) fn branch_exists(&mut self, branch_id: BranchId) -> EngineResult<bool> {
        let request = BranchRequest::new(branch_id, BranchAction::Describe, None);
        match self.runtime.branch(&request) {
            Ok(outcome) => Ok(outcome
                .branch()
                .is_some_and(|branch| branch.status() == StorageBranchStatus::Active)),
            Err(StorageApiError::BranchNotFound { .. }) => Ok(false),
            Err(error) => Err(map_storage_error(error)),
        }
    }

    pub(crate) fn create_branch(
        &mut self,
        branch_id: BranchId,
        generation: u64,
    ) -> EngineResult<PersistenceBranchOutcome> {
        self.branch_action(
            branch_id,
            BranchAction::Create,
            Some(StorageBranchGeneration::new(generation)),
        )
    }

    pub(crate) fn describe_branch(
        &mut self,
        branch_id: BranchId,
    ) -> EngineResult<PersistenceBranchSummary> {
        let outcome = self.branch_action(branch_id, BranchAction::Describe, None)?;
        Ok(outcome.branch())
    }

    /// The most-recent committed version and timestamp on a branch's timeline, or
    /// `(None, None)` when the branch has no committed history.
    ///
    /// Promotion recovery uses this to learn whether a target branch's data
    /// commit landed before a crash: a version higher than the pre-promote
    /// baseline means the data committed and the merge edge should be finalized.
    pub(crate) fn branch_timeline_head(
        &mut self,
        branch_id: BranchId,
    ) -> EngineResult<(Option<CommitVersion>, Option<Timestamp>)> {
        let outcome = self
            .runtime
            .timeline_bounds(TimelineBoundsRequest::new(branch_id))
            .map_err(map_storage_error)?;
        Ok((outcome.max_version(), outcome.max_timestamp()))
    }

    /// #3112 S3a: resolve a wall-clock instant to the LOGICAL commit timestamp
    /// a time-travel read should run at.
    ///
    /// Returning the logical timestamp — rather than threading a wall-clock
    /// bound down the read path — is what keeps `as_of_time` exactly
    /// equivalent to an `as_of` at the resolved value. Every temporal rule
    /// already locked (at-or-before, greatest-version-wins on ties, MVCC
    /// visibility at the frontier, tombstone and TTL handling) is inherited
    /// rather than restated, so the two forms cannot drift apart.
    pub(crate) fn resolve_wall_clock(
        &mut self,
        branch_id: BranchId,
        instant: Timestamp,
    ) -> EngineResult<Timestamp> {
        let outcome = self
            .runtime
            .resolve_wall_clock(WallClockLookupRequest::new(branch_id, instant))
            .map_err(map_storage_error)?;
        Ok(outcome.timestamp())
    }

    /// #3112 S4: the wall-clock instants for a batch of commit versions, in
    /// the order asked.
    ///
    /// Instants are commit-scoped, so a row cannot carry its own — history
    /// joins them here instead. An unknown instant is reported as `None`
    /// rather than failing: the history row is exact either way, and a date
    /// the branch cannot vouch for is better shown as absent than guessed.
    pub(crate) fn committed_at_for_versions(
        &mut self,
        branch_id: BranchId,
        versions: &[CommitVersion],
    ) -> EngineResult<Vec<Option<Timestamp>>> {
        if versions.is_empty() {
            return Ok(Vec::new());
        }
        self.runtime
            .commit_instants(&CommitInstantsRequest::new(branch_id, versions.to_vec()))
            .map_err(map_storage_error)
    }

    pub(crate) fn fork_branch_current(
        &mut self,
        branch_id: BranchId,
        source: BranchId,
        generation: u64,
    ) -> EngineResult<PersistenceBranchOutcome> {
        self.branch_action(
            branch_id,
            BranchAction::ForkCurrent { source },
            Some(StorageBranchGeneration::new(generation)),
        )
    }

    pub(crate) fn fork_branch_at_version(
        &mut self,
        branch_id: BranchId,
        source: BranchId,
        version: CommitVersion,
        generation: u64,
    ) -> EngineResult<PersistenceBranchOutcome> {
        self.branch_action(
            branch_id,
            BranchAction::ForkAtVersion { source, version },
            Some(StorageBranchGeneration::new(generation)),
        )
    }

    pub(crate) fn fork_branch_at_timestamp(
        &mut self,
        branch_id: BranchId,
        source: BranchId,
        timestamp: Timestamp,
        generation: u64,
    ) -> EngineResult<PersistenceBranchOutcome> {
        self.branch_action(
            branch_id,
            BranchAction::ForkAtTimestamp { source, timestamp },
            Some(StorageBranchGeneration::new(generation)),
        )
    }

    pub(crate) fn delete_branch(
        &mut self,
        branch_id: BranchId,
        generation: u64,
    ) -> EngineResult<PersistenceBranchOutcome> {
        self.branch_action(
            branch_id,
            BranchAction::Delete,
            Some(StorageBranchGeneration::new(generation)),
        )
    }

    fn branch_action(
        &mut self,
        branch_id: BranchId,
        action: BranchAction,
        generation: Option<StorageBranchGeneration>,
    ) -> EngineResult<PersistenceBranchOutcome> {
        self.guard_fault(FaultOp::Branch)?;
        let request = BranchRequest::new(branch_id, action, generation);
        let outcome = self.runtime.branch(&request).map_err(map_storage_error)?;
        map_branch_outcome(&outcome)
    }

    /// Arms the next commit with an explicit replay timestamp (consumed
    /// exactly once by [`Self::commit`]).
    pub(crate) fn arm_replay_commit_timestamp(&mut self, timestamp: Timestamp) {
        self.replay_commit_timestamp = Some(timestamp);
    }

    /// Holds (or clears with `None`) the structural replay timestamp used for
    /// import setup commits that carry no one-shot timestamp. See #3070.
    pub(crate) fn set_replay_structural_timestamp(&mut self, timestamp: Option<Timestamp>) {
        self.replay_structural_timestamp = timestamp;
    }

    pub(crate) fn commit(&mut self, plan: &CommitPlan) -> EngineResult<CommitOutcome> {
        self.guard_fault(FaultOp::Commit)?;
        let mut mutations = Vec::with_capacity(plan.mutations().len());
        for mutation in plan.mutations() {
            mutations.push(to_storage_mutation(mutation)?);
        }
        // A per-item replay timestamp (consumed once) wins; otherwise a held
        // structural timestamp (branch/space setup during import) keeps those
        // bookkeeping commits from advancing the floor past the content that
        // replays next. Neither set ⇒ ordinary generated allocation. See #3070.
        let replay_timestamp = self
            .replay_commit_timestamp
            .take()
            .or(self.replay_structural_timestamp);
        let mut options = CommitOptions::default();
        if let Some(generation) = plan.expected_generation() {
            options = options.with_expected_generation(StorageBranchGeneration::new(generation));
        }
        // Stamp the wall-clock instant only on ordinary generated commits. A
        // replayed/imported commit has no authentic wall-clock here (its
        // original instant is recovered in S2), so it is left unknown rather
        // than backdated to now (#3112).
        if replay_timestamp.is_none() {
            if let Some(committed_at) = sample_committed_at() {
                options = options.with_committed_at(committed_at);
            }
        }
        let batch =
            CommitBatch::new(plan.branch_id(), mutations, options).map_err(map_storage_error)?;
        let summary = match replay_timestamp {
            Some(timestamp) => self
                .runtime
                .commit_at(&batch, timestamp)
                .map_err(map_storage_error)?,
            None => self.runtime.commit(&batch).map_err(map_storage_error)?,
        };
        Ok(CommitOutcome::new(
            summary.commit_version(),
            summary.commit_timestamp(),
            summary.put_count(),
            summary.delete_count(),
            commit_durability(summary.durability()),
        )
        .with_committed_at(summary.committed_at()))
    }

    pub(crate) fn read(
        &mut self,
        address: RowAddress,
        selector: ReadSelector,
    ) -> EngineResult<Option<Vec<u8>>> {
        Ok(self
            .read_row(address, selector)?
            .and_then(|row| (!row.is_tombstone()).then_some(row))
            .and_then(|row| row.value().map(<[u8]>::to_vec)))
    }

    pub(crate) fn read_row(
        &mut self,
        address: RowAddress,
        selector: ReadSelector,
    ) -> EngineResult<Option<PersistenceReadRow>> {
        self.guard_fault(FaultOp::Read)?;
        let outcome = self
            .runtime
            .read_point(&point_read_request(address, selector)?)
            .map_err(map_storage_error)?;
        let mut row = outcome
            .into_row()
            .map(PersistenceReadRow::from_storage_owned);
        if let Some(inner) = row.as_mut() {
            self.corrupt_rows(FaultOp::Read, std::slice::from_mut(inner));
        }
        Ok(row)
    }

    pub(crate) fn read_history(
        &mut self,
        address: &RowAddress,
        include_tombstones: bool,
    ) -> EngineResult<Vec<PersistenceReadRow>> {
        self.guard_fault(FaultOp::Read)?;
        let request = HistoryReadRequest::new(
            address.branch_id(),
            storage_space(address)?,
            storage_key(address)?,
        )
        .include_tombstones(include_tombstones);
        let outcome = self
            .runtime
            .read_history(&request)
            .map_err(map_storage_error)?;
        Ok(outcome
            .rows()
            .iter()
            .map(PersistenceReadRow::from_storage)
            .collect())
    }

    pub(crate) fn scan_prefix(
        &mut self,
        branch_id: BranchId,
        row_class: RowClass,
        prefix: Vec<u8>,
        selector: ReadSelector,
        limit: Option<usize>,
    ) -> EngineResult<Vec<PersistenceReadRow>> {
        self.scan_prefix_inner(branch_id, row_class, prefix, selector, limit, None)
    }

    /// Scan a prefix returning only rows whose committed version is strictly greater than
    /// `after_version` — i.e. the "active delta" written after a watermark. Lets a caller that
    /// already holds covering index artifacts skip re-reading the sealed rows.
    pub(crate) fn scan_prefix_after_version(
        &mut self,
        branch_id: BranchId,
        row_class: RowClass,
        prefix: Vec<u8>,
        selector: ReadSelector,
        limit: Option<usize>,
        after_version: CommitVersion,
    ) -> EngineResult<Vec<PersistenceReadRow>> {
        self.scan_prefix_inner(
            branch_id,
            row_class,
            prefix,
            selector,
            limit,
            Some(after_version),
        )
    }

    fn scan_prefix_inner(
        &mut self,
        branch_id: BranchId,
        row_class: RowClass,
        prefix: Vec<u8>,
        selector: ReadSelector,
        limit: Option<usize>,
        after_version: Option<CommitVersion>,
    ) -> EngineResult<Vec<PersistenceReadRow>> {
        self.guard_fault(FaultOp::Scan)?;
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let limit = read_limit(limit)?;
        let outcome = self
            .runtime
            .scan_prefix(&prefix_scan_request(
                branch_id,
                row_class,
                prefix,
                selector,
                limit,
                after_version,
            )?)
            .map_err(map_storage_error)?;
        let mut rows: Vec<PersistenceReadRow> = outcome
            .rows()
            .iter()
            .map(PersistenceReadRow::from_storage)
            .collect();
        self.corrupt_rows(FaultOp::Scan, &mut rows);
        Ok(rows)
    }

    pub(crate) fn scan_range(
        &mut self,
        branch_id: BranchId,
        row_class: RowClass,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        selector: ReadSelector,
        limit: Option<usize>,
    ) -> EngineResult<Vec<PersistenceReadRow>> {
        self.guard_fault(FaultOp::Scan)?;
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let limit = read_limit(limit)?;
        let outcome = self
            .runtime
            .scan_range(&scan_range_request(
                branch_id, row_class, start, end, selector, limit,
            )?)
            .map_err(map_storage_error)?;
        let mut rows: Vec<PersistenceReadRow> = outcome
            .rows()
            .iter()
            .map(PersistenceReadRow::from_storage)
            .collect();
        self.corrupt_rows(FaultOp::Scan, &mut rows);
        Ok(rows)
    }

    pub(crate) fn scan_immutable_sources(
        &mut self,
        branch_id: BranchId,
        row_class: RowClass,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        selector: ReadSelector,
    ) -> EngineResult<Vec<PersistenceImmutableSource>> {
        self.guard_fault(FaultOp::Scan)?;
        let outcome = self
            .runtime
            .scan_immutable_sources(&immutable_source_scan_request(
                branch_id, row_class, start, end, selector,
            )?)
            .map_err(map_storage_error)?;
        Ok(outcome
            .sources()
            .iter()
            .map(PersistenceImmutableSource::from_storage)
            .collect())
    }

    pub(crate) fn close(&mut self) -> EngineResult<StorageCloseSummary> {
        self.runtime.close().map_err(map_storage_error)
    }

    /// Creation durability barrier: force the just-seeded control plane
    /// durable before the new database is handed to the caller. A checkpoint
    /// syncs the active WAL segment before publishing its snapshot, so the
    /// seed commits stop depending on the user-space WAL staging that a
    /// process kill vaporizes — a store's durable manifest must never
    /// outlive its control plane. Cache targets have nothing to force.
    pub(crate) fn force_creation_durability(&mut self) -> EngineResult<()> {
        if !self.durable() {
            return Ok(());
        }
        let summary = self
            .runtime
            .maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Checkpoint,
                MaintenanceScope::Global,
            ))
            .map_err(map_storage_error)?;
        // A deferred or failed creation checkpoint means the seed is NOT
        // durable — silently accepting it would quietly reopen the
        // first-session-kill window. Fail creation loudly instead.
        if summary.status() != StorageMaintenanceSummaryStatus::Completed {
            return Err(EngineError::corruption(
                "data_loss.engine.control_plane_missing",
                "creation durability checkpoint did not complete; the new database's \
                 control plane is not yet durable",
            ));
        }
        // The completed checkpoint IS the barrier: its snapshot is durably
        // published with the seed rows. Follow-up hygiene tasks it enqueued
        // run under the normal scheduler; draining here would only add
        // failure surface to creation.
        Ok(())
    }

    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn flush_branch_for_test(&mut self, branch_id: BranchId) -> EngineResult<usize> {
        self.runtime
            .maintenance(&MaintenanceRequest::new(
                MaintenanceTask::Flush,
                MaintenanceScope::Branch(branch_id),
            ))
            .map_err(map_storage_error)?;
        let summary = self
            .runtime
            .drain_maintenance()
            .map_err(map_storage_error)?;
        Ok(summary.drained_tasks().saturating_add(1))
    }

    #[must_use]
    pub(crate) const fn durable(&self) -> bool {
        self.durable
    }
}

fn map_branch_outcome(outcome: &StorageBranchOutcome) -> EngineResult<PersistenceBranchOutcome> {
    let branch = outcome.branch().ok_or_else(|| {
        EngineError::corruption(
            "data_loss.engine.branch_catalog",
            "storage branch operation did not return a branch summary",
        )
    })?;
    Ok(PersistenceBranchOutcome {
        branch: map_branch_summary(branch)?,
        generation_before: outcome
            .generation_before()
            .map(StorageBranchGeneration::as_u64),
        generation_after: outcome
            .generation_after()
            .map(StorageBranchGeneration::as_u64),
        source_branch_id: outcome.source_branch_id(),
        fork_version: outcome.fork_version(),
        fork_timestamp: outcome.fork_timestamp(),
        cleanup: outcome.cleanup().map(map_branch_cleanup),
    })
}

fn map_branch_summary(summary: StorageBranchSummary) -> EngineResult<PersistenceBranchSummary> {
    let status = match summary.status() {
        StorageBranchStatus::Active => PersistenceBranchStatus::Active,
        StorageBranchStatus::Deleted => PersistenceBranchStatus::Deleted,
        _ => {
            return Err(EngineError::incompatible_layout(
                "failed_precondition.engine.branch_status",
                "storage branch status is not supported by this engine",
            ))
        }
    };
    let parent = summary.parent().map(|parent| PersistenceBranchParent {
        source_branch_id: parent.source_branch_id(),
        fork_version: parent.fork_version(),
    });
    Ok(PersistenceBranchSummary {
        branch_id: summary.branch_id(),
        generation: summary.generation().as_u64(),
        status,
        parent,
        created_at: summary.created_at(),
        deleted_at: summary.deleted_at(),
        state_revision: summary.state_revision(),
    })
}

fn map_branch_cleanup(cleanup: StorageBranchCleanupSummary) -> PersistenceBranchCleanup {
    PersistenceBranchCleanup {
        removed_refs: cleanup.removed_refs(),
        releasable_tables: cleanup.releasable_tables(),
        protected_tables: cleanup.protected_tables(),
    }
}

fn to_storage_mutation(mutation: &RowMutation) -> EngineResult<CommitMutation> {
    match mutation {
        RowMutation::Put { address, value } => Ok(CommitMutation::Put {
            storage_space: storage_space(address)?,
            key: storage_key(address)?,
            value: StorageValue::new(value.clone()),
            ttl: None,
        }),
        RowMutation::Delete { address } => Ok(CommitMutation::Delete {
            storage_space: storage_space(address)?,
            key: storage_key(address)?,
        }),
    }
}

fn storage_space(address: &RowAddress) -> EngineResult<StorageSpaceId> {
    storage_space_for_class(address.row_class())
}

fn storage_space_for_class(row_class: RowClass) -> EngineResult<StorageSpaceId> {
    StorageSpaceId::new(vec![row_class.storage_space_id()]).map_err(map_storage_error)
}

fn storage_key(address: &RowAddress) -> EngineResult<StorageKey> {
    storage_key_from_bytes(address.key().to_vec())
}

fn storage_key_from_bytes(bytes: Vec<u8>) -> EngineResult<StorageKey> {
    StorageKey::new(bytes).map_err(map_storage_error)
}

fn point_read_request(
    address: RowAddress,
    selector: ReadSelector,
) -> EngineResult<PointReadRequest> {
    // B4: the encoded key is allocated once (encode_kv_key and friends) and
    // MOVED through the request instead of re-copied.
    Ok(PointReadRequest::new(
        address.branch_id(),
        storage_space_for_class(address.row_class())?,
        storage_key_from_bytes(address.into_key())?,
        storage_read_bound(selector),
    ))
}

fn prefix_scan_request(
    branch_id: BranchId,
    row_class: RowClass,
    prefix: Vec<u8>,
    selector: ReadSelector,
    limit: Option<ReadLimit>,
    after_version: Option<CommitVersion>,
) -> EngineResult<PrefixScanReadRequest> {
    let request = PrefixScanReadRequest::new(
        branch_id,
        storage_space_for_class(row_class)?,
        storage_key_from_bytes(prefix)?,
        storage_read_bound(selector),
        limit,
    );
    Ok(match after_version {
        Some(after_version) => request.with_after_version(after_version),
        None => request,
    })
}

fn scan_range_request(
    branch_id: BranchId,
    row_class: RowClass,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    selector: ReadSelector,
    limit: Option<ReadLimit>,
) -> EngineResult<ScanReadRequest> {
    let range = ScanRange::new(
        start.map(storage_key_from_bytes).transpose()?,
        end.map(storage_key_from_bytes).transpose()?,
    )
    .map_err(map_storage_error)?;
    Ok(ScanReadRequest::new(
        branch_id,
        storage_space_for_class(row_class)?,
        range,
        storage_read_bound(selector),
        limit,
    ))
}

fn immutable_source_scan_request(
    branch_id: BranchId,
    row_class: RowClass,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    selector: ReadSelector,
) -> EngineResult<ImmutableSourceScanReadRequest> {
    let range = ScanRange::new(
        start.map(storage_key_from_bytes).transpose()?,
        end.map(storage_key_from_bytes).transpose()?,
    )
    .map_err(map_storage_error)?;
    Ok(ImmutableSourceScanReadRequest::new(
        branch_id,
        storage_space_for_class(row_class)?,
        range,
        storage_read_bound(selector),
    ))
}

fn storage_read_bound(selector: ReadSelector) -> ReadBound {
    match selector {
        ReadSelector::Latest => ReadBound::Latest,
        ReadSelector::AtVersion(version) => ReadBound::AtVersion(version),
        ReadSelector::AtTimestamp(timestamp) => ReadBound::AtTimestamp(timestamp),
    }
}

fn read_limit(limit: Option<usize>) -> EngineResult<Option<ReadLimit>> {
    match limit {
        Some(limit) => ReadLimit::new(limit).map(Some).map_err(map_storage_error),
        None => Ok(None),
    }
}

/// Faithful per-commit durability mapping. #2756: the old fold to a bool
/// (`Standard | Always => true`) attested unsynced Standard-mode commits as
/// durable; each storage state now reaches the caller unchanged.
const fn commit_durability(summary: CommitDurabilitySummary) -> crate::commit::CommitDurability {
    match summary {
        CommitDurabilitySummary::NotDurable => crate::commit::CommitDurability::NotDurable,
        CommitDurabilitySummary::Standard => crate::commit::CommitDurability::Standard,
        CommitDurabilitySummary::Always => crate::commit::CommitDurability::Always,
        _ => crate::commit::CommitDurability::Uncertain,
    }
}

fn apply_memory_budget(
    options: StorageOpenOptions,
    memory_budget_bytes: Option<u64>,
) -> EngineResult<StorageOpenOptions> {
    match memory_budget_bytes {
        Some(bytes) => {
            let budget = StorageMemoryBudget::new(bytes).map_err(map_storage_error)?;
            Ok(options.with_memory_budget(budget))
        }
        // No explicit budget: the product path derives the default from host
        // memory at open (#2905) — a ceiling, never a reservation.
        None => Ok(options.with_budget_policy(StorageBudgetPolicy::DerivedFromHost)),
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn map_storage_error(error: StorageApiError) -> EngineError {
    let details = storage_error_details(&error);
    let suggested_fix = persistence_suggested_fix(&error);
    match &error {
        StorageApiError::IncompatibleLayout { .. } => {
            // V1 cutover (hard rule 42): pre-V1 database layouts surface a
            // structured layout error, never a generic persistence failure.
            return EngineError::with_status(
                EngineErrorClass::IncompatibleLayout,
                EngineErrorStatus::new(
                    ErrorClass::FailedPrecondition,
                    "failed_precondition.engine.layout_version",
                    RetryPolicy::Never,
                    CommitOutcomeStatus::NotApplicable,
                    "this directory holds a database from a pre-V1 version of Strata",
                    suggested_fix,
                    details,
                    vec![
                        "Point Strata at an empty directory or an existing V1 database; pre-V1 databases are not migrated.".to_owned(),
                    ],
                ),
                error,
            );
        }
        StorageApiError::BranchGenerationMismatch { .. } => {
            return EngineError::with_status(
                EngineErrorClass::Conflict,
                EngineErrorStatus::new(
                    ErrorClass::Conflict,
                    "conflict.engine.branch_generation",
                    RetryPolicy::AfterStateChange,
                    CommitOutcomeStatus::DefinitelyNotCommitted,
                    "branch generation changed before the write could commit",
                    suggested_fix,
                    details,
                    vec!["Reload the branch state before retrying this write.".to_owned()],
                ),
                error,
            );
        }
        StorageApiError::RecoveryDegraded { .. } => {
            return EngineError::with_status(
                EngineErrorClass::Corruption,
                EngineErrorStatus::new(
                    ErrorClass::Corruption,
                    "corruption.engine.persistence_recovery",
                    RetryPolicy::Never,
                    CommitOutcomeStatus::NotApplicable,
                    "persistence recovery reported degraded state",
                    suggested_fix,
                    details,
                    vec!["Inspect recovery diagnostics before resuming writes.".to_owned()],
                ),
                error,
            );
        }
        StorageApiError::LowerLayer { .. } => {
            return EngineError::with_status(
                EngineErrorClass::Unavailable,
                EngineErrorStatus::new(
                    ErrorClass::Unavailable,
                    "unavailable.engine.persistence",
                    RetryPolicy::SameRequest,
                    CommitOutcomeStatus::NotApplicable,
                    "persistence lower layer is unavailable",
                    suggested_fix,
                    details,
                    vec!["Retry after the local persistence layer is available.".to_owned()],
                ),
                error,
            );
        }
        _ => {}
    }
    let (legacy_class, public_class, code, retry_policy, commit_outcome, message) =
        match error.class() {
            StorageApiErrorClass::InvalidArgument => (
                EngineErrorClass::InvalidInput,
                ErrorClass::InvalidArgument,
                "invalid_argument.engine.persistence",
                RetryPolicy::Never,
                CommitOutcomeStatus::NotStarted,
                "persistence request was invalid",
            ),
            StorageApiErrorClass::NotFound => (
                EngineErrorClass::NotFound,
                ErrorClass::NotFound,
                "not_found.engine.persistence",
                RetryPolicy::Never,
                CommitOutcomeStatus::NotApplicable,
                "persistence target was not found",
            ),
            StorageApiErrorClass::AlreadyExists => (
                EngineErrorClass::Conflict,
                ErrorClass::AlreadyExists,
                "already_exists.engine.persistence",
                RetryPolicy::Never,
                CommitOutcomeStatus::NotStarted,
                "persistence target already exists",
            ),
            StorageApiErrorClass::Conflict => (
                EngineErrorClass::Conflict,
                ErrorClass::Conflict,
                "conflict.engine.persistence",
                RetryPolicy::AfterStateChange,
                CommitOutcomeStatus::DefinitelyNotCommitted,
                "persistence target conflicted with existing state",
            ),
            StorageApiErrorClass::Unsupported => (
                EngineErrorClass::Unavailable,
                ErrorClass::Unsupported,
                "unsupported.engine.persistence_capability",
                RetryPolicy::AfterStateChange,
                CommitOutcomeStatus::NotApplicable,
                "requested persistence capability is unavailable",
            ),
            StorageApiErrorClass::HistoryUnavailable => (
                EngineErrorClass::NotFound,
                ErrorClass::HistoryUnavailable,
                "history_unavailable.engine.persistence_history",
                RetryPolicy::AfterStateChange,
                CommitOutcomeStatus::NotApplicable,
                "requested persistence history is unavailable",
            ),
            StorageApiErrorClass::AmbiguousCommit => (
                EngineErrorClass::AmbiguousCommit,
                ErrorClass::AmbiguousCommit,
                "ambiguous_commit.engine.persistence",
                RetryPolicy::Unknown,
                CommitOutcomeStatus::MaybeCommitted,
                "persistence could not prove whether the commit succeeded",
            ),
            StorageApiErrorClass::FailedPrecondition => (
                EngineErrorClass::Unavailable,
                ErrorClass::FailedPrecondition,
                "failed_precondition.engine.persistence",
                RetryPolicy::AfterStateChange,
                CommitOutcomeStatus::DefinitelyNotCommitted,
                "persistence is temporarily unable to accept the request",
            ),
            StorageApiErrorClass::ResourceExhausted => (
                EngineErrorClass::Unavailable,
                ErrorClass::ResourceExhausted,
                "resource_exhausted.engine.persistence_budget",
                RetryPolicy::AfterStateChange,
                CommitOutcomeStatus::DefinitelyNotCommitted,
                "persistence resource budget is exhausted",
            ),
            StorageApiErrorClass::Internal => (
                EngineErrorClass::Internal,
                ErrorClass::Internal,
                "internal.engine.persistence",
                RetryPolicy::Unknown,
                CommitOutcomeStatus::NotApplicable,
                "persistence returned an internal failure",
            ),
            _ => (
                EngineErrorClass::Internal,
                ErrorClass::Internal,
                "internal.engine.persistence",
                RetryPolicy::Unknown,
                CommitOutcomeStatus::NotApplicable,
                "persistence returned an unknown failure",
            ),
        };
    EngineError::with_status(
        legacy_class,
        EngineErrorStatus::new(
            public_class,
            code,
            retry_policy,
            commit_outcome,
            message,
            suggested_fix,
            details,
            Vec::new(),
        ),
        error,
    )
}

fn storage_error_details(error: &StorageApiError) -> Vec<ErrorDetail> {
    let mut details = Vec::new();
    match error {
        StorageApiError::InvalidArgument { field, reason } => {
            details.push(ErrorDetail::new("field", *field));
            details.push(ErrorDetail::new("reason", *reason));
        }
        StorageApiError::UnsupportedCapability { capability, reason } => {
            details.push(ErrorDetail::new("capability", *capability));
            details.push(ErrorDetail::new("reason", *reason));
        }
        StorageApiError::InvalidRuntimeState { reason }
        | StorageApiError::RetainedHistoryUnavailable { reason, .. }
        | StorageApiError::TimestampHistoryUnavailable { reason, .. }
        | StorageApiError::DurableUncertain { reason, .. }
        | StorageApiError::RecoveryDegraded { reason }
        | StorageApiError::MaintenanceRejected { reason } => {
            details.push(ErrorDetail::new("reason", *reason));
        }
        StorageApiError::BranchNotFound { branch_id }
        | StorageApiError::BranchAlreadyExists { branch_id } => {
            details.push(ErrorDetail::new("branch_id", branch_id.to_string()));
        }
        StorageApiError::BranchGenerationMismatch {
            branch_id,
            expected,
            actual,
        } => {
            details.push(ErrorDetail::new("branch_id", branch_id.to_string()));
            details.push(ErrorDetail::new(
                "expected_generation",
                expected.to_string(),
            ));
            details.push(ErrorDetail::new("actual_generation", actual.to_string()));
        }
        StorageApiError::Conflict {
            branch_id,
            storage_space,
            key_fingerprint,
            user_key_len,
            reason,
        } => {
            details.push(ErrorDetail::new("branch_id", branch_id.to_string()));
            if let Some(space) = storage_space {
                details.push(ErrorDetail::new("space_id", space.to_string()));
            }
            if let Some(fingerprint) = key_fingerprint {
                details.push(ErrorDetail::new("key_fingerprint", fingerprint.to_string()));
            }
            if let Some(length) = user_key_len {
                details.push(ErrorDetail::new("user_key_len", length.to_string()));
            }
            details.push(ErrorDetail::new("reason", *reason));
        }
        StorageApiError::StoragePressure {
            branch_id,
            severity,
            pressure_reason,
            reason,
            retryable,
        } => {
            details.push(ErrorDetail::new("branch_id", branch_id.to_string()));
            details.push(ErrorDetail::new("severity", format!("{severity:?}")));
            details.push(ErrorDetail::new(
                "pressure_reason",
                format!("{pressure_reason:?}"),
            ));
            details.push(ErrorDetail::new("reason", *reason));
            details.push(ErrorDetail::new("retryable", retryable.to_string()));
        }
        StorageApiError::ResourceExhausted {
            resource,
            requested_bytes,
            used_bytes,
            limit_bytes,
            reason,
        } => {
            details.push(ErrorDetail::new("resource", *resource));
            details.push(ErrorDetail::new(
                "requested_bytes",
                requested_bytes.to_string(),
            ));
            details.push(ErrorDetail::new("used_bytes", used_bytes.to_string()));
            details.push(ErrorDetail::new("limit_bytes", limit_bytes.to_string()));
            details.push(ErrorDetail::new("reason", *reason));
        }
        StorageApiError::LowerLayer { layer, reason, .. } => {
            details.push(ErrorDetail::new("layer", format!("{layer:?}")));
            details.push(ErrorDetail::new("reason", *reason));
        }
        _ => {}
    }
    details
}

const fn persistence_suggested_fix(error: &StorageApiError) -> &'static str {
    match error {
        StorageApiError::InvalidArgument { .. } => "Correct the request input and retry.",
        StorageApiError::UnsupportedCapability { .. } => {
            "Use a supported database mode, backend, or capability."
        }
        StorageApiError::InvalidRuntimeState { .. }
        | StorageApiError::MaintenanceRejected { .. }
        | StorageApiError::StoragePressure { .. } => {
            "Wait for the database to become ready, then retry."
        }
        StorageApiError::BranchNotFound { .. } => "Target an existing branch before retrying.",
        StorageApiError::BranchAlreadyExists { .. } => {
            "Use the existing branch or choose a new branch name."
        }
        StorageApiError::BranchGenerationMismatch { .. } | StorageApiError::Conflict { .. } => {
            "Reload current state and retry against the latest version."
        }
        StorageApiError::RetainedHistoryUnavailable { .. }
        | StorageApiError::TimestampHistoryUnavailable { .. } => {
            "Request history inside the retained window."
        }
        StorageApiError::DurableUncertain { .. } => {
            "Re-open or inspect the database state before assuming whether the write committed."
        }
        StorageApiError::RecoveryDegraded { .. } => {
            "Stop writing and inspect recovery diagnostics before continuing."
        }
        StorageApiError::ResourceExhausted { .. } => {
            "Reduce resource pressure or raise the configured limit, then retry."
        }
        StorageApiError::LowerLayer { .. } => {
            "Retry after the local persistence layer is available."
        }
        StorageApiError::IncompatibleLayout { .. } => {
            "Point Strata at an empty directory or an existing V1 database; pre-V1 databases are not migrated."
        }
        _ => persistence_suggested_fix_for_class(error.class()),
    }
}

const fn persistence_suggested_fix_for_class(class: StorageApiErrorClass) -> &'static str {
    match class {
        StorageApiErrorClass::InvalidArgument => "Correct the request input and retry.",
        StorageApiErrorClass::NotFound => "Check that the requested object exists before retrying.",
        StorageApiErrorClass::AlreadyExists => "Use the existing object or choose a new name.",
        StorageApiErrorClass::Conflict => {
            "Reload current state and retry against the latest version."
        }
        StorageApiErrorClass::Unsupported => {
            "Use a supported database mode, backend, or capability."
        }
        StorageApiErrorClass::HistoryUnavailable => "Request history inside the retained window.",
        StorageApiErrorClass::AmbiguousCommit => {
            "Re-open or inspect the database state before assuming whether the write committed."
        }
        StorageApiErrorClass::ResourceExhausted => {
            "Reduce resource pressure or raise the configured limit, then retry."
        }
        StorageApiErrorClass::FailedPrecondition => {
            "Change database state or wait for the database to become ready, then retry."
        }
        _ => "Capture diagnostics and report this as a Strata bug.",
    }
}

pub(crate) fn close_summary_is_durable(summary: StorageCloseSummary) -> bool {
    summary.state() == StorageRuntimeState::Closed && summary.durable_synced()
}

#[cfg(test)]
mod tests {
    use strata_core::BranchId;
    use strata_storage::api::{
        CommitAdmissionPressureReason, CommitAdmissionPressureSeverity, StorageApiError,
        StorageApiLowerLayer,
    };

    use super::map_storage_error;
    use crate::diagnostics::{
        CommitOutcomeStatus, EngineError, EngineErrorClass, ErrorClass, RetryPolicy,
    };

    fn assert_v1_status(
        error: &EngineError,
        class: ErrorClass,
        retry_policy: RetryPolicy,
        commit_outcome: CommitOutcomeStatus,
    ) {
        assert_eq!(error.public_class(), class);
        assert_eq!(error.retry_policy(), retry_policy);
        assert_eq!(error.commit_outcome(), commit_outcome);
        assert!(
            !error.suggested_fix().is_empty(),
            "mapped storage errors should carry remediation"
        );
        assert!(
            !error.details().is_empty(),
            "mapped storage errors should carry structured details"
        );
        assert_public_details_do_not_leak_storage_terms(error);
    }

    fn assert_public_details_do_not_leak_storage_terms(error: &EngineError) {
        for detail in error.details() {
            assert!(
                !detail.key().contains("storage"),
                "public detail key leaked storage vocabulary: {detail:?}"
            );
            assert!(
                !detail.value().contains("storage_api"),
                "public detail value leaked storage API vocabulary: {detail:?}"
            );
        }
        assert!(
            !error.suggested_fix().contains("StorageApiError"),
            "suggested fix leaked storage API type name"
        );
    }

    #[test]
    fn storage_conflict_maps_to_engine_conflict() {
        let error = map_storage_error(StorageApiError::Conflict {
            branch_id: BranchId::from_bytes([0x11; BranchId::BYTE_LEN]),
            storage_space: None,
            key_fingerprint: None,
            user_key_len: None,
            reason: "test conflict",
        });
        assert_eq!(error.class(), EngineErrorClass::Conflict);
        assert_eq!(error.code(), "conflict.engine.persistence");
        assert!(error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::Conflict,
            RetryPolicy::AfterStateChange,
            CommitOutcomeStatus::DefinitelyNotCommitted,
        );
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn storage_pressure_maps_to_retryable_unavailable() {
        let error = map_storage_error(StorageApiError::StoragePressure {
            branch_id: BranchId::from_bytes([0x11; BranchId::BYTE_LEN]),
            severity: CommitAdmissionPressureSeverity::Blocking,
            pressure_reason: CommitAdmissionPressureReason::MaintenanceQueueBacklog,
            reason: "test pressure",
            retryable: true,
        });
        assert_eq!(error.class(), EngineErrorClass::Unavailable);
        assert_eq!(error.code(), "failed_precondition.engine.persistence");
        assert!(error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::FailedPrecondition,
            RetryPolicy::AfterStateChange,
            CommitOutcomeStatus::DefinitelyNotCommitted,
        );
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn ambiguous_storage_commit_reports_unknown_retry_and_maybe_committed() {
        let error = map_storage_error(StorageApiError::durable_uncertain("test uncertainty"));
        assert_eq!(error.class(), EngineErrorClass::AmbiguousCommit);
        assert_eq!(error.code(), "ambiguous_commit.engine.persistence");
        assert!(!error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::AmbiguousCommit,
            RetryPolicy::Unknown,
            CommitOutcomeStatus::MaybeCommitted,
        );
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn storage_branch_generation_mismatch_maps_to_conflict() {
        let error = map_storage_error(StorageApiError::BranchGenerationMismatch {
            branch_id: BranchId::from_bytes([0x11; BranchId::BYTE_LEN]),
            expected: 1,
            actual: 2,
        });
        assert_eq!(error.class(), EngineErrorClass::Conflict);
        assert_eq!(error.code(), "conflict.engine.branch_generation");
        assert!(error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::Conflict,
            RetryPolicy::AfterStateChange,
            CommitOutcomeStatus::DefinitelyNotCommitted,
        );
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn degraded_recovery_maps_to_corruption() {
        let error = map_storage_error(StorageApiError::RecoveryDegraded {
            reason: "test recovery degradation",
        });
        assert_eq!(error.class(), EngineErrorClass::Corruption);
        assert_eq!(error.code(), "corruption.engine.persistence_recovery");
        assert!(!error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::Corruption,
            RetryPolicy::Never,
            CommitOutcomeStatus::NotApplicable,
        );
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn lower_layer_storage_error_maps_to_retryable_unavailable() {
        let error = map_storage_error(StorageApiError::lower_layer_with(
            StorageApiLowerLayer::Service,
            "test lower layer",
            std::io::Error::new(std::io::ErrorKind::Other, "test source"),
        ));
        assert_eq!(error.class(), EngineErrorClass::Unavailable);
        assert_eq!(error.code(), "unavailable.engine.persistence");
        assert!(error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::Unavailable,
            RetryPolicy::SameRequest,
            CommitOutcomeStatus::NotApplicable,
        );
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn storage_invalid_argument_maps_to_invalid_input() {
        let error = map_storage_error(StorageApiError::InvalidArgument {
            field: "test field",
            reason: "test reason",
        });
        assert_eq!(error.class(), EngineErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.engine.persistence");
        assert!(!error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::InvalidArgument,
            RetryPolicy::Never,
            CommitOutcomeStatus::NotStarted,
        );
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn storage_unsupported_capability_maps_to_unavailable_capability() {
        let error = map_storage_error(StorageApiError::UnsupportedCapability {
            capability: "test capability",
            reason: "test reason",
        });
        assert_eq!(error.class(), EngineErrorClass::Unavailable);
        assert_eq!(error.code(), "unsupported.engine.persistence_capability");
        assert!(error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::Unsupported,
            RetryPolicy::AfterStateChange,
            CommitOutcomeStatus::NotApplicable,
        );
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn storage_branch_not_found_maps_to_not_found() {
        let error = map_storage_error(StorageApiError::BranchNotFound {
            branch_id: BranchId::from_bytes([0x11; BranchId::BYTE_LEN]),
        });
        assert_eq!(error.class(), EngineErrorClass::NotFound);
        assert_eq!(error.code(), "not_found.engine.persistence");
        assert!(!error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::NotFound,
            RetryPolicy::Never,
            CommitOutcomeStatus::NotApplicable,
        );
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn storage_branch_already_exists_maps_to_conflict() {
        let error = map_storage_error(StorageApiError::BranchAlreadyExists {
            branch_id: BranchId::from_bytes([0x11; BranchId::BYTE_LEN]),
        });
        assert_eq!(error.class(), EngineErrorClass::Conflict);
        assert_eq!(error.code(), "already_exists.engine.persistence");
        assert!(!error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::AlreadyExists,
            RetryPolicy::Never,
            CommitOutcomeStatus::NotStarted,
        );
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn storage_retained_history_unavailable_maps_to_not_found_history() {
        let error = map_storage_error(StorageApiError::RetainedHistoryUnavailable {
            branch_id: BranchId::from_bytes([0x11; BranchId::BYTE_LEN]),
            reason: "test retained history",
        });
        assert_eq!(error.class(), EngineErrorClass::NotFound);
        assert_eq!(
            error.code(),
            "history_unavailable.engine.persistence_history"
        );
        assert!(error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::HistoryUnavailable,
            RetryPolicy::AfterStateChange,
            CommitOutcomeStatus::NotApplicable,
        );
        assert!(error.source_arc().is_some());
    }

    #[test]
    fn storage_timestamp_history_unavailable_maps_to_not_found_history() {
        let error = map_storage_error(StorageApiError::TimestampHistoryUnavailable {
            branch_id: BranchId::from_bytes([0x11; BranchId::BYTE_LEN]),
            reason: "test timestamp history",
        });
        assert_eq!(error.class(), EngineErrorClass::NotFound);
        assert_eq!(
            error.code(),
            "history_unavailable.engine.persistence_history"
        );
        assert!(error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::HistoryUnavailable,
            RetryPolicy::AfterStateChange,
            CommitOutcomeStatus::NotApplicable,
        );
    }

    #[test]
    fn storage_maintenance_rejected_maps_to_retryable_unavailable() {
        let error = map_storage_error(StorageApiError::MaintenanceRejected {
            reason: "test maintenance rejection",
        });
        assert_eq!(error.class(), EngineErrorClass::Unavailable);
        assert_eq!(error.code(), "failed_precondition.engine.persistence");
        assert!(error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::FailedPrecondition,
            RetryPolicy::AfterStateChange,
            CommitOutcomeStatus::DefinitelyNotCommitted,
        );
    }

    #[test]
    fn storage_invalid_runtime_state_maps_to_retryable_unavailable() {
        let error = map_storage_error(StorageApiError::InvalidRuntimeState {
            reason: "test runtime state",
        });
        assert_eq!(error.class(), EngineErrorClass::Unavailable);
        assert_eq!(error.code(), "failed_precondition.engine.persistence");
        assert!(error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::FailedPrecondition,
            RetryPolicy::AfterStateChange,
            CommitOutcomeStatus::DefinitelyNotCommitted,
        );
    }

    // Storage resource-budget exhaustion is a transient pressure condition, so
    // it maps to a retryable `unavailable`, not a non-retryable internal failure
    // (which would tell callers to treat a recoverable budget limit as a bug).
    // This resolves the open scope item in `engine-test-plan.md` (§11.1).
    #[test]
    fn storage_resource_exhausted_maps_to_retryable_unavailable() {
        let error = map_storage_error(StorageApiError::ResourceExhausted {
            resource: "memory",
            requested_bytes: 4096,
            used_bytes: 1024,
            limit_bytes: 2048,
            reason: "test budget exhaustion",
        });
        assert_eq!(error.class(), EngineErrorClass::Unavailable);
        assert_eq!(error.code(), "resource_exhausted.engine.persistence_budget");
        assert!(error.retryable());
        assert_v1_status(
            &error,
            ErrorClass::ResourceExhausted,
            RetryPolicy::AfterStateChange,
            CommitOutcomeStatus::DefinitelyNotCommitted,
        );
        assert!(error.source_arc().is_some());
    }
}
