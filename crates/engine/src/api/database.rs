//! Database handle and open/close outcomes.

use std::path::PathBuf;

use crate::branch::BranchService;
use crate::control::{bootstrap_or_load, ControlPlane};
use crate::data::event::EventService;
use crate::data::graph::GraphService;
#[cfg(any(test, feature = "testkit"))]
use crate::data::json::JsonIndexName;
use crate::data::json::JsonService;
use crate::data::kv::{KvService, ProductSpace};
use crate::data::vector::{VectorArtifactStore, VectorService};
use crate::diagnostics::EngineError;
pub use crate::persistence::MemoryBudgetSource;
use crate::persistence::{
    close_summary_is_durable, PersistenceOpenSummary, PersistenceOpenTarget, StoragePersistence,
};
#[cfg(any(test, feature = "testkit"))]
use crate::persistence::{
    encode_json_index_entry_prefix, FaultOp, ReadSelector, RowClass, RowCorruption,
    StorageFaultKind,
};

use strata_core::Timestamp;

use super::{
    AdminService, BranchName, CacheOpenOptions, CachePreheat, ControlDiagnostics,
    DurableLocalOpenOptions, SpaceService,
};

/// Explicit storage target used to open a database.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatabaseOpenTarget {
    /// Volatile cache-backed database.
    Cache,
    /// Durable local filesystem-backed database.
    DurableLocal,
}

/// Summary of an engine database open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DatabaseOpenSummary {
    target: DatabaseOpenTarget,
    created: bool,
    durable: bool,
    memory_budget_source: MemoryBudgetSource,
}

impl DatabaseOpenSummary {
    pub(crate) const fn new(
        target: DatabaseOpenTarget,
        persistence: PersistenceOpenSummary,
    ) -> Self {
        Self {
            target,
            created: persistence.created(),
            durable: persistence.durable(),
            memory_budget_source: persistence.memory_budget_source(),
        }
    }

    #[must_use]
    /// Where the opened database's storage memory budget came from: explicit,
    /// derived from host memory at open, or the fixed default.
    pub const fn memory_budget_source(self) -> MemoryBudgetSource {
        self.memory_budget_source
    }

    #[must_use]
    /// Returns the explicit open target.
    pub const fn target(self) -> DatabaseOpenTarget {
        self.target
    }

    #[must_use]
    /// Returns true when the backing database was newly initialized.
    pub const fn created(self) -> bool {
        self.created
    }

    #[must_use]
    /// Returns true when the target is durable.
    pub const fn durable(self) -> bool {
        self.durable
    }
}

/// Database open result containing the handle and open facts.
pub struct DatabaseOpenOutcome {
    database: Database,
    summary: DatabaseOpenSummary,
}

impl DatabaseOpenOutcome {
    pub(crate) const fn new(database: Database, summary: DatabaseOpenSummary) -> Self {
        Self { database, summary }
    }

    #[must_use]
    /// Returns the open summary.
    pub const fn summary(&self) -> DatabaseOpenSummary {
        self.summary
    }

    #[must_use]
    /// Consumes the outcome and returns the database handle.
    pub fn into_database(self) -> Database {
        self.database
    }
}

/// Database close facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CloseOutcome {
    durable: bool,
    durable_synced: bool,
    idempotent: bool,
}

impl CloseOutcome {
    pub(crate) const fn new(durable: bool, durable_synced: bool, idempotent: bool) -> Self {
        Self {
            durable,
            durable_synced,
            idempotent,
        }
    }

    #[must_use]
    /// Returns true when the closed database used durable storage.
    pub const fn durable(self) -> bool {
        self.durable
    }

    #[must_use]
    /// Returns true when durable storage reported synced close effects.
    pub const fn durable_synced(self) -> bool {
        self.durable_synced
    }

    #[must_use]
    /// Returns true when this close result came from an already closed handle.
    pub const fn idempotent(self) -> bool {
        self.idempotent
    }
}

/// Executor-facing database handle.
pub struct Database {
    persistence: StoragePersistence,
    control: ControlPlane,
    vector_artifacts: VectorArtifactStore,
    summary: DatabaseOpenSummary,
    last_close: Option<CloseOutcome>,
    open: bool,
}

impl Database {
    /// Opens an explicit cache database.
    pub fn open_cache(options: CacheOpenOptions) -> Result<DatabaseOpenOutcome, EngineError> {
        let memory_budget_bytes = options.memory_budget_bytes();
        Self::open(
            PersistenceOpenTarget::Cache,
            DatabaseOpenTarget::Cache,
            options.into_default_branch(),
            memory_budget_bytes,
            None,
            CachePreheat::WhenIdle,
        )
    }

    /// Opens an explicit durable local database.
    pub fn open_local(
        path: impl Into<PathBuf>,
        options: DurableLocalOpenOptions,
    ) -> Result<DatabaseOpenOutcome, EngineError> {
        let memory_budget_bytes = options.memory_budget_bytes();
        let data_block_bytes = options.data_block_bytes();
        let cache_preheat = options.cache_preheat();
        Self::open(
            PersistenceOpenTarget::DurableLocal(path.into(), options.durability()),
            DatabaseOpenTarget::DurableLocal,
            options.into_default_branch(),
            memory_budget_bytes,
            data_block_bytes,
            cache_preheat,
        )
    }

    /// Exports the branch's logical content as a deterministic artifact.
    ///
    /// Sections are portable SAP1 payload streams (see [`crate::artifact`]);
    /// identical logical databases produce identical bytes. The exporter
    /// holds this handle exclusively for the whole export, so the result is
    /// a consistent snapshot. V1 buffers sections in memory (dataset-scale,
    /// not backup-scale).
    pub fn export_branch_artifact(
        &mut self,
        branch: &BranchName,
    ) -> Result<crate::artifact::BranchArtifact, EngineError> {
        crate::artifact::export_branch(self, branch)
    }

    /// Imports a branch artifact's content into this database.
    ///
    /// The artifact's branch is created when absent and must hold no
    /// content (`conflict.engine.artifact_import` otherwise). Rows replay
    /// with their original commit timestamps, so re-exporting the imported
    /// branch reproduces the source artifact byte-for-byte.
    pub fn import_branch_artifact(
        &mut self,
        artifact: &crate::artifact::BranchArtifact,
    ) -> Result<crate::artifact::BranchImportSummary, EngineError> {
        crate::artifact::import_branch(self, artifact)
    }

    /// Imports several branch artifacts as one globally-ordered replay.
    ///
    /// Branches originally share a single interleaved commit stream, so the
    /// per-branch monotonic floor can only be honored by replaying all of
    /// them together in global timestamp order — importing one at a time
    /// rejects any branch whose history predates another's latest commit
    /// (#3070). Each target branch is created when absent and must hold no
    /// content; summaries are returned in artifact order.
    pub fn import_branch_artifacts(
        &mut self,
        artifacts: &[crate::artifact::BranchArtifact],
    ) -> Result<Vec<crate::artifact::BranchImportSummary>, EngineError> {
        crate::artifact::import_branches(self, artifacts)
    }

    pub(crate) fn arm_replay_commit_timestamp(&mut self, timestamp: strata_core::Timestamp) {
        self.persistence.arm_replay_commit_timestamp(timestamp);
    }

    pub(crate) fn set_replay_structural_timestamp(
        &mut self,
        timestamp: Option<strata_core::Timestamp>,
    ) {
        self.persistence.set_replay_structural_timestamp(timestamp);
    }

    /// Records (or overwrites) where this database was cloned from.
    ///
    /// Written by clone orchestration after a successful bundle import;
    /// future sync operations overwrite it with each new fetch.
    pub fn set_remote_origin(
        &mut self,
        origin: &crate::artifact::RemoteOrigin,
    ) -> Result<(), EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        let mutation = crate::persistence::RowMutation::put(
            crate::persistence::RowAddress::new(
                crate::branch::catalog::SYSTEM_BRANCH_ID,
                crate::persistence::RowClass::DatasetIdentity,
                crate::persistence::remote_origin_key(),
            ),
            crate::artifact::encode_remote_origin(origin),
        );
        self.persistence
            .commit(&crate::persistence::CommitPlan::new(
                crate::branch::catalog::SYSTEM_BRANCH_ID,
                vec![mutation],
                None,
            ))?;
        Ok(())
    }

    /// Reads this database's remote origin, when one was recorded.
    pub fn remote_origin(&mut self) -> Result<Option<crate::artifact::RemoteOrigin>, EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        let bytes = self.persistence.read(
            crate::persistence::RowAddress::new(
                crate::branch::catalog::SYSTEM_BRANCH_ID,
                crate::persistence::RowClass::DatasetIdentity,
                crate::persistence::remote_origin_key(),
            ),
            crate::persistence::ReadSelector::Latest,
        )?;
        bytes
            .map(|bytes| crate::artifact::decode_remote_origin(&bytes))
            .transpose()
    }

    /// Returns a branch service for this database.
    pub fn branches(&mut self) -> Result<BranchService<'_>, EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        Ok(BranchService::new(&mut self.persistence, &mut self.control))
    }

    /// Returns a byte-oriented KV service for the selected branch and space.
    pub fn kv(
        &self,
        branch: BranchName,
        space: ProductSpace,
    ) -> Result<KvService<'_>, EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        self.require_branch(&branch)?;
        Ok(KvService::new(
            &self.persistence,
            &self.control,
            branch,
            space,
        ))
    }

    /// Resolves a wall-clock instant to the logical commit timestamp a
    /// time-travel read on `branch` should run at (#3112 S3a).
    ///
    /// The instant is UTC epoch micros. Resolution selects the commit boundary
    /// at or before it, so the caller then performs an ordinary `as_of` read at
    /// the returned value — `as_of_time` is defined to be exactly that, which
    /// is why the two forms of time travel cannot diverge.
    ///
    /// Refuses rather than guessing when the instant falls outside the branch's
    /// dated range, when the branch predates wall-clock recording, or when the
    /// branch cannot prove its wall-clock coverage. Wall-clock history has no
    /// scan fallback — commit instants live only in the retained-timeline
    /// index, never in rows — so an unprovable branch makes this question
    /// unanswerable rather than merely slow.
    pub fn resolve_wall_clock(
        &mut self,
        branch: &BranchName,
        instant: Timestamp,
    ) -> Result<Timestamp, EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        let record = self.control.lookup_branch(branch).cloned().ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("branch `{branch}` does not exist"),
            )
        })?;
        self.persistence
            .resolve_wall_clock(record.storage_branch_id(), instant)
    }

    /// Returns a JSON document service for the selected branch and space.
    pub fn json(
        &self,
        branch: BranchName,
        space: ProductSpace,
    ) -> Result<JsonService<'_>, EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        self.require_branch(&branch)?;
        Ok(JsonService::new(
            &self.persistence,
            &self.control,
            branch,
            space,
        ))
    }

    /// Returns a vector service for the selected branch and space.
    /// Still `&self`-less, unlike its kv/json/event/graph siblings: the vector
    /// service also holds `&mut VectorArtifactStore`, an LRU runtime cache with
    /// genuine eviction state. Sharing that is a real design question — which
    /// lock, at what granularity, and whether eviction contends — not the
    /// incidental exclusivity the rest of this change removed. Tracked as
    /// follow-up work rather than decided here.
    pub fn vector(
        &mut self,
        branch: BranchName,
        space: ProductSpace,
    ) -> Result<VectorService<'_>, EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        self.require_branch(&branch)?;
        Ok(VectorService::new(
            &mut self.persistence,
            &mut self.control,
            &mut self.vector_artifacts,
            branch,
            space,
        ))
    }

    /// Returns an event log service for the selected branch and space.
    pub fn event(
        &self,
        branch: BranchName,
        space: ProductSpace,
    ) -> Result<EventService<'_>, EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        self.require_branch(&branch)?;
        Ok(EventService::new(
            &self.persistence,
            &self.control,
            branch,
            space,
        ))
    }

    /// Returns a graph core service for the selected branch and space.
    pub fn graph(
        &self,
        branch: BranchName,
        space: ProductSpace,
    ) -> Result<GraphService<'_>, EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        self.require_branch(&branch)?;
        Ok(GraphService::new(
            &self.persistence,
            &self.control,
            branch,
            space,
        ))
    }

    /// Returns a product space service for the selected branch.
    pub fn spaces(&mut self, branch: BranchName) -> Result<SpaceService<'_>, EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        Ok(SpaceService::new(
            &mut self.persistence,
            &mut self.control,
            branch,
        ))
    }

    /// Returns a safe administration and introspection service.
    pub fn admin(&mut self) -> Result<AdminService<'_>, EngineError> {
        self.require_open()?;
        Ok(AdminService::new(
            &mut self.persistence,
            &mut self.control,
            &mut self.vector_artifacts,
            self.summary,
            self.open,
        ))
    }

    /// Returns typed core control-plane diagnostics.
    pub fn control_diagnostics(
        &mut self,
        branch: Option<&BranchName>,
    ) -> Result<ControlDiagnostics, EngineError> {
        self.require_open()?;
        Ok(self.control.diagnostics(&mut self.persistence, branch))
    }

    /// Counts visible JSON secondary-index entries for conformance tests.
    #[cfg(any(test, feature = "testkit"))]
    pub fn json_index_entry_count_for_test(
        &mut self,
        branch: &BranchName,
        space: &ProductSpace,
        index: &JsonIndexName,
    ) -> Result<u64, EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        let record = self.control.lookup_branch(branch).cloned().ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("branch `{branch}` does not exist"),
            )
        })?;
        let count = self
            .persistence
            .scan_prefix(
                record.storage_branch_id(),
                RowClass::JsonIndex,
                encode_json_index_entry_prefix(space, index),
                ReadSelector::Latest,
                None,
            )?
            .into_iter()
            .filter(|row| !row.is_tombstone())
            .count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    /// Flushes a branch into immutable storage sources for tests.
    #[cfg(any(test, feature = "testkit"))]
    pub fn flush_storage_branch_for_test(
        &mut self,
        branch: &BranchName,
    ) -> Result<usize, EngineError> {
        self.require_open()?;
        self.control.require_healthy()?;
        let record = self.control.lookup_branch(branch).cloned().ok_or_else(|| {
            EngineError::not_found(
                "not_found.engine.branch",
                format!("branch `{branch}` does not exist"),
            )
        })?;
        self.persistence
            .flush_branch_for_test(record.storage_branch_id())
    }

    /// Arms a storage fault that fires on the next persistence commit.
    #[cfg(any(test, feature = "testkit"))]
    pub fn inject_commit_fault_for_test(&mut self, kind: StorageFaultKind) {
        self.persistence.arm_storage_fault(FaultOp::Commit, kind, 0);
    }

    /// Arms a storage fault that fires after `skip` further commits pass — used
    /// to fail a later commit (such as a pending-operation cleanup) while
    /// letting earlier commits succeed.
    #[cfg(any(test, feature = "testkit"))]
    pub fn inject_commit_fault_after_for_test(&mut self, skip: usize, kind: StorageFaultKind) {
        self.persistence
            .arm_storage_fault(FaultOp::Commit, kind, skip);
    }

    /// Arms a storage fault that fires on the next persistence point read.
    #[cfg(any(test, feature = "testkit"))]
    pub fn inject_read_fault_for_test(&mut self, kind: StorageFaultKind) {
        self.persistence.arm_storage_fault(FaultOp::Read, kind, 0);
    }

    /// Arms a storage fault that fires on the next persistence scan.
    #[cfg(any(test, feature = "testkit"))]
    pub fn inject_scan_fault_for_test(&mut self, kind: StorageFaultKind) {
        self.persistence.arm_storage_fault(FaultOp::Scan, kind, 0);
    }

    /// Corrupts the rows the next persistence scan returns — the read succeeds
    /// but its rows come back malformed, so the engine's `data_loss.*` decoders
    /// run on the genuine read path. Used by corruption-injection tests.
    #[cfg(any(test, feature = "testkit"))]
    pub fn inject_scan_corruption_for_test(&mut self, corruption: RowCorruption) {
        self.persistence
            .arm_row_corruption(FaultOp::Scan, corruption, 0);
    }

    /// Corrupts the row the next persistence point read returns (see
    /// [`Self::inject_scan_corruption_for_test`]).
    #[cfg(any(test, feature = "testkit"))]
    pub fn inject_read_corruption_for_test(&mut self, corruption: RowCorruption) {
        self.persistence
            .arm_row_corruption(FaultOp::Read, corruption, 0);
    }

    /// Arms a storage fault that fires on the next persistence branch action.
    #[cfg(any(test, feature = "testkit"))]
    pub fn inject_branch_fault_for_test(&mut self, kind: StorageFaultKind) {
        self.persistence.arm_storage_fault(FaultOp::Branch, kind, 0);
    }

    /// Arms a storage fault that fires after `skip` further branch actions pass —
    /// used to fail a later branch action (such as the describe that follows a
    /// successful fork) while letting the fork itself succeed.
    #[cfg(any(test, feature = "testkit"))]
    pub fn inject_branch_fault_after_for_test(&mut self, skip: usize, kind: StorageFaultKind) {
        self.persistence
            .arm_storage_fault(FaultOp::Branch, kind, skip);
    }

    /// Closes the database handle.
    pub fn close(&mut self) -> Result<CloseOutcome, EngineError> {
        if let Some(close) = self.last_close {
            return Ok(CloseOutcome::new(
                close.durable(),
                close.durable_synced(),
                true,
            ));
        }
        let durable = self.persistence.durable();
        let close = self.persistence.close()?;
        let outcome =
            CloseOutcome::new(durable, close_summary_is_durable(close), close.idempotent());
        self.open = false;
        self.last_close = Some(outcome);
        Ok(outcome)
    }

    #[must_use]
    /// Returns the open facts captured when this handle was created.
    pub const fn open_summary(&self) -> DatabaseOpenSummary {
        self.summary
    }

    #[must_use]
    /// Returns the configured default product branch.
    pub fn default_branch(&self) -> &BranchName {
        self.control.default_branch()
    }

    fn open(
        target: PersistenceOpenTarget,
        open_target: DatabaseOpenTarget,
        default_branch: Option<BranchName>,
        memory_budget_bytes: Option<u64>,
        data_block_bytes: Option<u32>,
        cache_preheat: CachePreheat,
    ) -> Result<DatabaseOpenOutcome, EngineError> {
        let vector_artifacts = vector_artifact_store_for_target(&target);
        // Captured before the open consumes the target: the dataset dir gets
        // its advisory README after a successful durable open (#3004).
        let dataset_dir = match &target {
            PersistenceOpenTarget::DurableLocal(path, _) => Some(path.clone()),
            PersistenceOpenTarget::Cache => None,
        };
        let (mut persistence, persistence_summary) = StoragePersistence::open_with_budget(
            target,
            memory_budget_bytes,
            data_block_bytes,
            cache_preheat,
        )?;
        let control = bootstrap_or_load(
            &mut persistence,
            persistence_summary.created(),
            default_branch,
        )?;
        if persistence_summary.created() {
            // A database must be born durable: without this barrier the
            // control-plane seed can ride user-space WAL staging while the
            // manifest is already fsync-durable, and a process kill during
            // the first session bricks the store (#2618).
            persistence.force_creation_durability()?;
        }
        if let Some(dir) = dataset_dir.as_deref() {
            // Creation writes it; later opens self-heal a missing one so
            // pre-existing datasets gain the breadcrumb too (#3004).
            super::dataset_readme::ensure_dataset_readme(dir);
        }
        let summary = DatabaseOpenSummary::new(open_target, persistence_summary);
        Ok(DatabaseOpenOutcome::new(
            Self {
                persistence,
                control,
                vector_artifacts,
                summary,
                last_close: None,
                open: true,
            },
            summary,
        ))
    }

    fn require_open(&self) -> Result<(), EngineError> {
        if self.open {
            Ok(())
        } else {
            Err(EngineError::closed_runtime(
                "database handle is closed and cannot accept operations",
            ))
        }
    }

    /// Validates that the named branch exists before handing out a data
    /// service. Branch existence is engine-owned (hard rule 7); enforcing it
    /// at service construction keeps the check uniform — including for
    /// operations that short-circuit before touching a branch record (an
    /// empty batch) — and removes the per-op validation the executor used to
    /// duplicate across every capability.
    fn require_branch(&self, branch: &BranchName) -> Result<(), EngineError> {
        if self.control.lookup_branch(branch).is_some() {
            Ok(())
        } else {
            Err(EngineError::not_found(
                "not_found.engine.branch",
                format!("branch `{branch}` does not exist"),
            ))
        }
    }
}

fn vector_artifact_store_for_target(target: &PersistenceOpenTarget) -> VectorArtifactStore {
    match target {
        PersistenceOpenTarget::Cache => VectorArtifactStore::memory(),
        PersistenceOpenTarget::DurableLocal(path, _) => {
            VectorArtifactStore::durable_local(path.join("engine-artifacts").join("vector"))
        }
    }
}
