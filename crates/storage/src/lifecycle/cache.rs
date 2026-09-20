//! Cache-mode lifecycle runtime.

use super::{
    branch_config_with_storage_budget_for_cache, branch_resident_bytes,
    compaction::{
        begin_cache_materialization_build, bind_materialization_task_for_enqueue,
        collect_storage_pressure_with_budget, compact_cache_branch_to_fixed_point_with_policy,
        compact_cache_branch_with_budget, compaction_score_key_for_task,
        current_compaction_request_from_maintenance_task_with_budget,
        defer_compaction_for_resource_policy, install_prepared_cache_compaction,
        install_prepared_cache_materialization, materialization_request_from_maintenance_task,
        materialize_cache_branch, prepare_cache_compaction_with_budget,
        record_lifecycle_compaction_outcome,
        record_lifecycle_table_rewrite_post_operation_score_with_budget,
        stale_compaction_maintenance_outcome, table_rewrite_outcome_allows_chain_resubmit,
        table_rewrite_outcome_was_flush_preempted, table_rewrite_score_key_for_branch_with_budget,
        table_rewrite_score_key_for_task_with_budget,
        table_rewrite_task_request_for_branch_with_budget, CacheMaterializationBegin,
        CacheMaterializationBuild, LifecycleCompactionScoreKey, LifecycleTableRewriteScoreKey,
        PreparedCacheCompaction, PreparedCacheMaterialization,
    },
    estimate_commit_batch_active_bytes, evaluate_mutating_write_admission,
    flush::{
        flush_branch_drain_with, flush_cache_branch_with_budget,
        flush_drain_maintenance_outcome_for_scope,
        flush_drain_request_for_branch_from_maintenance_task,
        flush_drain_request_from_maintenance_task, install_prepared_cache_flush,
        prepare_cache_flush_with_budget, PreparedCacheFlush,
    },
    projected_commit_rotation_would_exceed_frozen_budget, require_maintenance_enqueue_budget,
    require_rotate_budget, snapshot_with_runtime_usage, validate_backend_capabilities_for_open,
    BudgetedCommitBranch, CloseOutcome, CloseOutcomeEffects, CloseOutcomeStatus, ClosePhase,
    FlushFrozenOutcome, FlushFrozenRequest, FlushTableIdentitySeed, FlushTableObjectId,
    LifecycleBranchCatalog, LifecycleBranchClearOutcome, LifecycleBranchCreateOutcome,
    LifecycleBranchDeleteOutcome, LifecycleBranchDescriptor, LifecycleBranchForkOutcome,
    LifecycleCapabilityOutcome, LifecycleCloseFact, LifecycleCompactionDrainOutcome,
    LifecycleCompactionDrainRequest, LifecycleCompactionIoPolicy, LifecycleCompactionOutcome,
    LifecycleCompactionRequest, LifecycleError, LifecycleMaintenanceExecutor,
    LifecycleMaintenanceSchedulingPolicy, LifecycleMaterializationOutcome,
    LifecycleMaterializationRequest, LifecycleOperationKind, LifecyclePostCommitMaintenanceOutcome,
    LifecycleResult, LifecycleState, LifecycleStateMachine, LifecycleStats,
    LifecycleStoragePressure, LifecycleStoragePressureReason, LifecycleStoragePressureSeverity,
    LifecycleTransitionTrigger, LifecycleWalGrowthOutcome, LifecycleWriteAdmissionOutcome,
    MaintenanceCancelOutcome, MaintenanceEnqueueOutcome, MaintenanceExecutorStatus,
    MaintenanceOutcome, MaintenanceOutcomeStatus, MaintenanceTask, MaintenanceTaskId,
    MaintenanceTaskKind, MaintenanceTaskRequest, MaintenanceTaskRunner, MaintenanceTaskScope,
    RecoveryHealth, StorageBudgetLedger, StorageBudgetPool, StorageBudgetPressureSeverity,
    StorageBudgetSnapshot, StorageMode, StorageOpenDisposition, StorageOpenOutcome,
    StorageOpenPlan, StorageRuntimeBudget,
};
use crate::backend::Backend;
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::read::{BranchHistoryRow, BranchReadBound, BranchReadView, BranchScanBounds};
use crate::branch::snapshot::{BranchSnapshotPublisher, BranchSnapshotRegistry};
use crate::branch::state::{BranchLocalState, BranchRotationOutcome};
use crate::commit::{
    CommitBatch, CommitBatchKind, CommitBranchGeneration, CommitBranchGenerationGuard,
    CommitBranchGuardSet, CommitCacheRuntime, CommitDurabilityMode, CommitFactAllocator,
    CommitManualTimestampSource, CommitOutcome, CommitRuntimeConfig, CommitRuntimeError,
    CommitTimestampGuard, CommitTimestampSource, CommitUnresolvedDurable,
    CommitUnresolvedDurableGate, CommitVersionAllocator, VisibleVersionTracker,
};
use crate::lifecycle::maintenance::{
    schedule_post_commit_maintenance as schedule_suggested_post_commit_maintenance,
    MAINTENANCE_COVERAGE_IDLE_ROUND_LIMIT,
};
use crate::lifecycle::RuntimeReadHandles;
use crate::row::PhysicalKey;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use strata_core::{BranchId, CommitVersion, Timestamp};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleCacheOpenRequest {
    plan: StorageOpenPlan,
    initial_branch_id: BranchId,
    branch_generation: CommitBranchGeneration,
}

#[derive(Debug)]
pub(crate) struct LifecycleCacheRuntime<S = CommitManualTimestampSource> {
    state: LifecycleStateMachine,
    open_plan: StorageOpenPlan,
    open_outcome: StorageOpenOutcome,
    capability_outcome: LifecycleCapabilityOutcome,
    branch_catalog: LifecycleBranchCatalog,
    initial_branch_id: BranchId,
    guard_set: CommitBranchGuardSet,
    allocator: CommitFactAllocator<S>,
    visible: VisibleVersionTracker,
    /// BS2.2: release-published mirror of `visible.visible_version()` so a future off-lock reader
    /// (BS2.4) observes the visibility bound without the runtime lock. Cache mode starts at
    /// `CommitVersion::ZERO` (no recovery).
    visible_commit_version: Arc<AtomicU64>,
    /// BS2.3: per-branch published `Arc<BranchReadView>` snapshots. Published under the lock at the
    /// mutation sites; in BS2.3 read only by the debug equivalence oracle (reads still lock).
    snapshot_publisher: BranchSnapshotPublisher,
    durable_gate: CommitUnresolvedDurableGate,
    commit_config: CommitRuntimeConfig,
    maintenance: LifecycleMaintenanceExecutor,
    maintenance_coverage_idle_rounds: usize,
    /// Coverage hysteresis — see `schedule_background_maintenance_coverage`.
    coverage_completed_watermark: Option<usize>,
    pressure_rejected_commit_branches: HashSet<BranchId>,
    last_write_admission: Option<LifecycleWriteAdmissionOutcome>,
    budget: StorageBudgetLedger,
    // The CloseOutcome from the first successful close is preserved here so
    // subsequent idempotent close calls return the *prior final facts*
    // (cancel count, stats, close fact) instead of fabricating fresh
    // values. The lifecycle contract requires "Close after Closed is a
    // no-op success with the prior final facts" — without caching the
    // returned facts here, a second close would invent stats that diverge
    // from what callers already observed on the first call.
    last_close_outcome: Option<CloseOutcome>,
}

#[derive(Clone, Debug)]
pub(crate) enum CacheBackgroundMaintenanceStep {
    Completed(Box<MaintenanceOutcome>),
    Build(Box<CacheBackgroundMaintenanceBuild>),
}

#[derive(Clone, Debug)]
pub(crate) enum CacheBackgroundMaintenanceBuild {
    Flush {
        task: MaintenanceTask,
        branch_id: BranchId,
        request: FlushFrozenRequest,
        branch_snapshot: BranchLocalState,
        budget: StorageBudgetLedger,
        started_at: crate::time_compat::Instant,
    },
    Compaction {
        task: MaintenanceTask,
        branch_id: BranchId,
        request: LifecycleCompactionRequest,
        branch_snapshot: BranchLocalState,
        storage_budget: StorageRuntimeBudget,
        started_at: crate::time_compat::Instant,
    },
    Materialization {
        task: MaintenanceTask,
        branch_id: BranchId,
        build: CacheMaterializationBuild,
        started_at: crate::time_compat::Instant,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum CacheBackgroundMaintenanceBuilt {
    Flush {
        task: MaintenanceTask,
        branch_id: BranchId,
        request: FlushFrozenRequest,
        prepared: Option<PreparedCacheFlush>,
        elapsed: std::time::Duration,
    },
    Compaction {
        task: MaintenanceTask,
        branch_id: BranchId,
        prepared: PreparedCacheCompaction,
        elapsed: std::time::Duration,
    },
    Materialization {
        task: MaintenanceTask,
        branch_id: BranchId,
        prepared: PreparedCacheMaterialization,
        elapsed: std::time::Duration,
    },
}

#[cfg(all(test, feature = "perf-trace"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CacheBackgroundBuildKind {
    Flush,
    Compaction,
    Materialization,
}

#[cfg(all(test, feature = "perf-trace"))]
pub(crate) struct CacheBackgroundBuildPauseGuard {
    entered: std::sync::mpsc::Receiver<()>,
    release: Option<std::sync::mpsc::Sender<()>>,
}

#[cfg(all(test, feature = "perf-trace"))]
pub(crate) fn pause_next_cache_background_build_for_test(
    kind: CacheBackgroundBuildKind,
) -> CacheBackgroundBuildPauseGuard {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    background_build_pause::install(kind, entered_tx, release_rx);
    CacheBackgroundBuildPauseGuard {
        entered: entered_rx,
        release: Some(release_tx),
    }
}

#[cfg(all(test, feature = "perf-trace"))]
impl CacheBackgroundBuildPauseGuard {
    pub(crate) fn wait_until_entered(&self) {
        self.entered
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("background build did not enter test pause hook");
    }

    pub(crate) fn release(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

#[cfg(all(test, feature = "perf-trace"))]
impl Drop for CacheBackgroundBuildPauseGuard {
    fn drop(&mut self) {
        self.release();
        background_build_pause::clear();
    }
}

#[cfg(all(test, feature = "perf-trace"))]
mod background_build_pause {
    use super::CacheBackgroundBuildKind;
    use std::sync::{mpsc, Mutex, OnceLock};

    struct PauseHook {
        kind: CacheBackgroundBuildKind,
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }

    static PAUSE_HOOK: OnceLock<Mutex<Option<PauseHook>>> = OnceLock::new();

    fn slot() -> &'static Mutex<Option<PauseHook>> {
        PAUSE_HOOK.get_or_init(|| Mutex::new(None))
    }

    pub(super) fn install(
        kind: CacheBackgroundBuildKind,
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    ) {
        *slot().lock().expect("cache background build pause hook") = Some(PauseHook {
            kind,
            entered,
            release,
        });
    }

    pub(super) fn clear() {
        *slot().lock().expect("cache background build pause hook") = None;
    }

    pub(super) fn maybe_pause(kind: CacheBackgroundBuildKind) {
        let hook = {
            let mut hook = slot().lock().expect("cache background build pause hook");
            if hook.as_ref().is_some_and(|hook| hook.kind == kind) {
                hook.take()
            } else {
                None
            }
        };
        if let Some(hook) = hook {
            let _ = hook.entered.send(());
            let _ = hook.release.recv();
        }
    }
}

impl CacheBackgroundMaintenanceBuild {
    pub(crate) const fn task(&self) -> MaintenanceTask {
        match self {
            Self::Flush { task, .. }
            | Self::Compaction { task, .. }
            | Self::Materialization { task, .. } => *task,
        }
    }

    pub(crate) fn build(self) -> LifecycleResult<CacheBackgroundMaintenanceBuilt> {
        match self {
            Self::Flush {
                task,
                branch_id,
                request,
                branch_snapshot,
                budget,
                started_at,
            } => {
                #[cfg(all(test, feature = "perf-trace"))]
                background_build_pause::maybe_pause(CacheBackgroundBuildKind::Flush);
                Ok(CacheBackgroundMaintenanceBuilt::Flush {
                    task,
                    branch_id,
                    request: request.clone(),
                    prepared: prepare_cache_flush_with_budget(
                        &branch_snapshot,
                        &request,
                        Some(&budget),
                        // B2: the data-block byte target is a durable-read
                        // knob; cache-mode tables are memory-resident and
                        // keep the built-in default.
                        None,
                    )?,
                    elapsed: started_at.elapsed(),
                })
            }
            Self::Compaction {
                task,
                branch_id,
                request,
                branch_snapshot,
                storage_budget,
                started_at,
            } => {
                #[cfg(all(test, feature = "perf-trace"))]
                background_build_pause::maybe_pause(CacheBackgroundBuildKind::Compaction);
                Ok(CacheBackgroundMaintenanceBuilt::Compaction {
                    task,
                    branch_id,
                    prepared: prepare_cache_compaction_with_budget(
                        &branch_snapshot,
                        &request,
                        Some(storage_budget),
                    )?,
                    elapsed: started_at.elapsed(),
                })
            }
            Self::Materialization {
                task,
                branch_id,
                build,
                started_at,
            } => {
                #[cfg(all(test, feature = "perf-trace"))]
                background_build_pause::maybe_pause(CacheBackgroundBuildKind::Materialization);
                Ok(CacheBackgroundMaintenanceBuilt::Materialization {
                    task,
                    branch_id,
                    prepared: build.build()?,
                    elapsed: started_at.elapsed(),
                })
            }
        }
    }
}

impl LifecycleCacheOpenRequest {
    pub(crate) fn new(
        plan: StorageOpenPlan,
        initial_branch_id: BranchId,
        branch_generation: CommitBranchGeneration,
    ) -> LifecycleResult<Self> {
        let request = Self {
            plan,
            initial_branch_id,
            branch_generation,
        };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn plan(&self) -> &StorageOpenPlan {
        &self.plan
    }

    pub(crate) const fn initial_branch_id(&self) -> BranchId {
        self.initial_branch_id
    }

    pub(crate) const fn branch_generation(&self) -> CommitBranchGeneration {
        self.branch_generation
    }

    fn validate(&self) -> LifecycleResult<()> {
        self.plan.validate()?;
        if self.plan.storage_mode() != StorageMode::Cache {
            return Err(LifecycleError::InvalidOpenPlan {
                reason: "cache lifecycle runtime requires cache storage mode",
            });
        }
        Ok(())
    }
}

impl<S> RuntimeReadHandles for LifecycleCacheRuntime<S> {
    fn visible_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.visible_commit_version)
    }

    fn snapshot_registry(&self) -> Arc<BranchSnapshotRegistry> {
        self.snapshot_publisher.registry_handle()
    }
}

impl<S> LifecycleCacheRuntime<S> {
    pub(crate) fn open(
        request: LifecycleCacheOpenRequest,
        backend: &dyn Backend,
        branch_config: BranchRuntimeConfig,
        commit_config: CommitRuntimeConfig,
        timestamp_source: S,
    ) -> LifecycleResult<Self> {
        request.validate()?;
        let mut state = LifecycleStateMachine::new();
        require_admitted(state, LifecycleOperationKind::Open)?;
        state.transition(LifecycleTransitionTrigger::OpenRequested)?;

        let capability_outcome = validate_backend_capabilities_for_open(request.plan(), backend)?;
        let budget = StorageBudgetLedger::new(request.plan.lifecycle_config().storage_budget())?;
        let branch_config =
            branch_config_with_storage_budget_for_cache(branch_config, budget.budget())?;
        let branch = BranchLocalState::new(request.initial_branch_id(), branch_config)
            .map_err(branch_error)?;
        // W3.1b: cache mode never recovers — the initial branch is born
        // in-process with complete (empty) timeline coverage.
        branch.retained_timeline().mark_complete_from_birth();
        let branch_catalog = LifecycleBranchCatalog::with_existing_branch(
            &branch,
            request.branch_generation(),
            None,
        )?;
        commit_config.validate().map_err(commit_error)?;
        let open_outcome = StorageOpenOutcome::new(
            StorageMode::Cache,
            StorageOpenDisposition::Created,
            None,
            RecoveryHealth::Healthy,
            true,
        )?
        .with_backend_capabilities(capability_outcome.capabilities())
        .with_stats(LifecycleStats::new(1, 0, 0, 0, 0))
        .with_budget_snapshot(budget.snapshot());
        let max_maintenance_queue_depth = request
            .plan
            .lifecycle_config()
            .max_maintenance_queue_depth();

        state.transition(LifecycleTransitionTrigger::CacheOpenReady)?;

        let initial_branch_id = request.initial_branch_id();
        // The local `branch` value was used to seed the catalog via
        // `with_existing_branch` and is no longer needed; drop it to make
        // the catalog the sole owner.
        drop(branch);
        let mut runtime = Self {
            state,
            open_plan: request.plan,
            open_outcome,
            capability_outcome,
            branch_catalog,
            initial_branch_id,
            guard_set: CommitBranchGuardSet::new(),
            allocator: CommitFactAllocator::new(
                CommitVersionAllocator::default(),
                CommitTimestampGuard::default(),
                timestamp_source,
            ),
            visible: VisibleVersionTracker::default(),
            visible_commit_version: Arc::new(AtomicU64::new(0)),
            snapshot_publisher: BranchSnapshotPublisher::new(),
            durable_gate: CommitUnresolvedDurableGate::new(),
            commit_config,
            maintenance: LifecycleMaintenanceExecutor::new(max_maintenance_queue_depth)?,
            coverage_completed_watermark: None,
            maintenance_coverage_idle_rounds: 0,
            pressure_rejected_commit_branches: HashSet::new(),
            last_write_admission: None,
            budget,
            last_close_outcome: None,
        };
        // BS2.3: seed a published snapshot for every branch before any read can observe the runtime.
        runtime.republish_all_branch_snapshots();
        Ok(runtime)
    }

    pub(crate) const fn state(&self) -> LifecycleState {
        self.state.state()
    }

    pub(crate) const fn open_plan(&self) -> &StorageOpenPlan {
        &self.open_plan
    }

    pub(crate) const fn open_outcome(&self) -> &StorageOpenOutcome {
        &self.open_outcome
    }

    pub(crate) const fn allocator(&self) -> &CommitFactAllocator<S> {
        &self.allocator
    }

    pub(crate) fn budget_snapshot(&self) -> StorageBudgetSnapshot {
        self.refresh_runtime_memory_total();
        let branch = self
            .branch_catalog
            .branch_state(self.initial_branch_id)
            .expect("seeded branch is always present in the catalog");
        snapshot_with_runtime_usage(&self.budget, branch, self.maintenance.status())
    }

    /// Recompute and record the database-wide runtime memory total for cache mode: resident bytes
    /// (memtables plus owned-table readers) across active branches. Cache mode holds no durable
    /// block cache, so there is no cache term.
    fn refresh_runtime_memory_total(&self) {
        let resident =
            self.branch_catalog
                .list_branches(false)
                .iter()
                .fold(0u64, |total, descriptor| {
                    self.branch_catalog
                        .branch_state(descriptor.branch_id())
                        .map_or(total, |branch| {
                            total.saturating_add(branch_resident_bytes(branch))
                        })
                });
        self.budget.set_runtime_total_bytes(resident);
    }

    pub(crate) fn budget_total_used_bytes(&self) -> u64 {
        self.budget.total_used_bytes()
    }

    /// The isolated database-wide runtime memory total (branch resident bytes; cache mode has
    /// no block cache), without the in-flight ledger pool reservations that
    /// `budget_total_used_bytes` adds. Test-only: lets the budget drift test assert the
    /// published total against an independent full fold.
    #[cfg(test)]
    pub(crate) fn runtime_total_bytes(&self) -> u64 {
        self.budget.runtime_total_bytes()
    }

    pub(crate) fn budget_global_pressure(&self) -> StorageBudgetPressureSeverity {
        self.budget.global_pressure()
    }

    pub(crate) const fn capability_outcome(&self) -> &LifecycleCapabilityOutcome {
        &self.capability_outcome
    }

    /// Return the seeded branch's state. The seeded branch is registered
    /// at open time via `LifecycleBranchCatalog::with_existing_branch` and
    /// is the canonical anchor for the runtime's default-branch view; the
    /// `.expect(...)` reflects that invariant.
    pub(crate) fn branch_state(&self) -> &BranchLocalState {
        self.branch_catalog
            .branch_state(self.initial_branch_id)
            .expect("seeded branch is always present in the catalog")
    }

    pub(crate) const fn branch_catalog(&self) -> &LifecycleBranchCatalog {
        &self.branch_catalog
    }

    #[cfg(test)]
    pub(crate) fn branch_catalog_mut_for_test(&mut self) -> &mut LifecycleBranchCatalog {
        &mut self.branch_catalog
    }

    pub(crate) const fn visible_version(&self) -> CommitVersion {
        self.visible.visible_version()
    }

    #[cfg(test)]
    pub(crate) fn catch_up_commit_frontier_for_test(
        &mut self,
        version: CommitVersion,
        timestamp: Timestamp,
    ) {
        self.allocator.catch_up_to_recovered_version(version);
        self.allocator.catch_up_to_recovered_timestamp(timestamp);
        self.visible
            .catch_up_visible_after_replay(version)
            .expect("test commit frontier must not regress");
        // Keep the release-published mirror in lockstep with the tracker (BS2.2): bounded
        // Latest reads load the atomic, so a tracker-only advance would strand them.
        self.visible_commit_version
            .store(self.visible.visible_version().as_u64(), Ordering::Release);
    }

    pub(crate) fn unresolved_durable(&self) -> LifecycleResult<Option<CommitUnresolvedDurable>> {
        self.durable_gate.unresolved().map_err(commit_error)
    }

    /// Test seam: direct gate access so tests can recreate the `applied_not_visible` state
    /// (row applied above `visible`, gate tripped) without a publish-failure injection point.
    #[cfg(test)]
    pub(crate) const fn durable_gate_for_test(&self) -> &CommitUnresolvedDurableGate {
        &self.durable_gate
    }

    /// BS2.3: capture and publish a fresh snapshot for `branch_id` (under the runtime lock). The
    /// capture is O(#tables) refcount-bumps (BS2.1); on capture error — infallible on a well-formed
    /// post-mutation state — the prior snapshot is kept. Called at every branch mutation site.
    /// Republish the branch snapshot when a commit's append auto-rotated the active
    /// memtable (frozen count grew). Mirrors the durable runtime's helper of the same
    /// name: rotation is a structural change, and the Model-2 published snapshot's
    /// live-active handle stops covering new appends the moment the active is swapped.
    /// Returns whether a rotation was detected.
    fn republish_branch_snapshot_after_rotation(
        &mut self,
        branch_id: BranchId,
        frozen_before: usize,
    ) -> LifecycleResult<bool> {
        let frozen_after = self
            .branch_catalog
            .branch_state(branch_id)?
            .frozen_table_count();
        if frozen_after != frozen_before {
            self.publish_branch_snapshot(branch_id);
            return Ok(true);
        }
        Ok(false)
    }

    /// #2541: inline frozen drain for cache mode. Converts every sealed frozen
    /// memtable into an in-memory level-zero table via the existing flush path
    /// (which republishes the snapshot per install). Best-effort: an admission
    /// refusal, a deferred outcome, or any error leaves the remaining backlog
    /// for the next rotation — visibility was already republished by the
    /// caller, so correctness never depends on the drain.
    fn drain_frozen_inline(&mut self, branch_id: BranchId) {
        loop {
            // The branch vanished mid-hold only in pathological states;
            // draining is best-effort, so stop quietly.
            let Ok(branch) = self.branch_catalog.branch_state(branch_id) else {
                return;
            };
            let frozen = branch.frozen_table_count();
            if frozen == 0 {
                return;
            }
            let version = self.visible.visible_version().as_u64();
            let Ok(request) = cache_inline_flush_request(branch_id, version, frozen) else {
                return;
            };
            match self.flush_frozen(&request) {
                Ok(outcome) if outcome.completed() => {}
                // Deferred / failed / refused: retry at the next rotation.
                _ => return,
            }
        }
    }

    fn publish_branch_snapshot(&mut self, branch_id: BranchId) {
        let view = match self.branch_catalog.branch_state(branch_id) {
            Ok(state) => match state.capture_snapshot() {
                Ok(view) => Arc::new(view),
                Err(_error) => {
                    debug_assert!(
                        false,
                        "snapshot capture failed post-mutation for {branch_id:?}"
                    );
                    return;
                }
            },
            // Branch is gone (deleted / not yet installed); nothing to publish.
            Err(_) => return,
        };
        self.snapshot_publisher.publish_view(branch_id, view);
    }

    /// BS2.3: (re)publish a snapshot for every active branch. Used at construction (so a read
    /// before any mutation finds a snapshot) and after multi-branch or hard-to-attribute mutations
    /// (maintenance rewrites, fork's new child) — idempotent, and branch count is small.
    fn republish_all_branch_snapshots(&mut self) {
        for descriptor in self.branch_catalog.list_branches(false) {
            self.publish_branch_snapshot(descriptor.branch_id());
        }
    }

    /// BS2.3: drop a branch's slot on delete. A racing reader completes on the snapshot it holds.
    fn remove_branch_snapshot(&mut self, branch_id: BranchId) {
        self.snapshot_publisher.remove(branch_id);
    }

    /// BS2.3 test seam: republish after a direct `branch_state_mut` manipulation that bypasses the
    /// commit/maintenance publish sites (used by tests that synthesize a branch state).
    #[cfg(test)]
    pub(crate) fn publish_branch_snapshot_for_test(&mut self, branch_id: BranchId) {
        self.publish_branch_snapshot(branch_id);
    }

    pub(crate) fn read_view(&self) -> LifecycleResult<BranchReadView> {
        self.read_view_for_branch(self.initial_branch_id)
    }

    pub(crate) fn read_view_for_branch(
        &self,
        branch_id: strata_core::BranchId,
    ) -> LifecycleResult<BranchReadView> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryRead)?;
        let branch = self.branch_catalog.branch_state(branch_id)?;
        branch.capture_read_view().map_err(branch_error)
    }

    /// BS2.2: the visibility bound for a Latest read — `visible` loaded from the release-published
    /// atomic — with a debug check that bounding by it drops nothing under the lock (a no-op except
    /// in the deliberate `applied_not_visible` state, where the durable gate is tripped).
    fn latest_visibility_bound(&self, branch: &BranchLocalState) -> BranchReadBound {
        let visible = CommitVersion::new(self.visible_commit_version.load(Ordering::Acquire));
        debug_assert!(
            !self.durable_gate.is_clean()
                || branch
                    .max_commit_version()
                    .is_none_or(|max| max.as_u64() <= visible.as_u64()),
            "BS2.2 visibility bound must be a no-op under the lock: branch max {:?} > visible {:?} with a clean gate",
            branch.max_commit_version(),
            visible,
        );
        BranchReadBound::at_version(visible)
    }

    pub(crate) fn read_latest_point_or_tombstone_for_branch(
        &self,
        branch_id: strata_core::BranchId,
        key: &PhysicalKey,
    ) -> LifecycleResult<Option<BranchHistoryRow>> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryRead)?;
        let branch = self.branch_catalog.branch_state(branch_id)?;
        let bound = self.latest_visibility_bound(branch);
        branch
            .read_point_or_tombstone_borrowed(key, bound)
            .map_err(branch_error)
    }

    pub(crate) fn scan_latest_including_tombstones_for_branch(
        &self,
        branch_id: strata_core::BranchId,
        bounds: &BranchScanBounds,
        visible_limit: Option<usize>,
    ) -> LifecycleResult<Vec<BranchHistoryRow>> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryRead)?;
        let branch = self.branch_catalog.branch_state(branch_id)?;
        let bound = self.latest_visibility_bound(branch);
        branch
            .scan_including_tombstones_borrowed(bounds, bound, visible_limit, None)
            .map_err(branch_error)
    }

    pub(crate) fn rotate_active_for_maintenance(
        &mut self,
    ) -> LifecycleResult<BranchRotationOutcome> {
        self.rotate_active_for_branch_for_maintenance(self.initial_branch_id)
    }

    /// Flush-time rotation: seal the active table when the frozen pool
    /// affords it; skip the rotation (the flush then drains the existing
    /// frozen backlog, freeing the pool) when it does not. Errors only when
    /// the pool is exhausted with nothing frozen to flush.
    pub(crate) fn rotate_active_for_flush(
        &mut self,
        branch_id: strata_core::BranchId,
    ) -> LifecycleResult<()> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        let decision = crate::lifecycle::decide_flush_rotation(
            &self.budget,
            self.branch_catalog
                .branch_state(branch_id)
                .expect("seeded branch present"),
        );
        match decision {
            crate::lifecycle::FlushRotationDecision::Rotate => {
                let branch = self
                    .branch_catalog
                    .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
                branch.rotate_active();
                // BS2.3: rotation sealed active into a frozen table — republish
                // so the snapshot reflects it.
                self.publish_branch_snapshot(branch_id);
                Ok(())
            }
            crate::lifecycle::FlushRotationDecision::FlushBacklogWithoutRotation => Ok(()),
            crate::lifecycle::FlushRotationDecision::Defer(error) => Err(error),
        }
    }

    pub(crate) fn rotate_active_for_branch_for_maintenance(
        &mut self,
        branch_id: strata_core::BranchId,
    ) -> LifecycleResult<BranchRotationOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        require_rotate_budget(
            &self.budget,
            self.branch_catalog
                .branch_state(branch_id)
                .expect("seeded branch present"),
        )?;
        let branch = self
            .branch_catalog
            .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
        let outcome = branch.rotate_active();
        // BS2.3: rotation seals active into a frozen table — republish so the snapshot reflects it.
        self.publish_branch_snapshot(branch_id);
        Ok(outcome)
    }

    /// Storage-internal: create a new branch in the catalog. The new
    /// branch is visible via `list_branches` and
    /// `branch_catalog().branch_state(...)` and can accept commits routed
    /// through the catalog's per-branch slot.
    pub(crate) fn create_branch(
        &mut self,
        branch_id: BranchId,
        generation: CommitBranchGeneration,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<LifecycleBranchCreateOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let outcome = self
            .branch_catalog
            .create_branch(branch_id, generation, created_at)?;
        // BS2.3: publish a snapshot for the new branch (republish-all also seeds its slot).
        self.republish_all_branch_snapshots();
        Ok(outcome)
    }

    pub(crate) fn list_branches(&self, include_deleted: bool) -> Vec<LifecycleBranchDescriptor> {
        self.branch_catalog.list_branches(include_deleted)
    }

    pub(crate) fn fork_current(
        &mut self,
        source: BranchId,
        destination: BranchId,
        destination_generation: CommitBranchGeneration,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<LifecycleBranchForkOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let _quiesce = self.guard_set.try_begin_quiesce().map_err(commit_error)?;
        let outcome = self.branch_catalog.fork_current(
            source,
            destination,
            destination_generation,
            created_at,
        )?;
        // BS2.3: the forked child needs a published snapshot over its inherited layers.
        self.republish_all_branch_snapshots();
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "fork-at-history surface exposed for cache callers"
    )]
    pub(crate) fn fork_at_retained_version(
        &mut self,
        source: BranchId,
        destination: BranchId,
        destination_generation: CommitBranchGeneration,
        fork_version: CommitVersion,
        retained_floor: CommitVersion,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<LifecycleBranchForkOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let _quiesce = self.guard_set.try_begin_quiesce().map_err(commit_error)?;
        let outcome = self.branch_catalog.fork_at_retained_version(
            source,
            destination,
            destination_generation,
            fork_version,
            retained_floor,
            created_at,
        )?;
        self.republish_all_branch_snapshots(); // BS2.3: publish the forked child's snapshot.
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "fork-at-history surface exposed for cache callers"
    )]
    pub(crate) fn fork_at_retained_timestamp(
        &mut self,
        source: BranchId,
        destination: BranchId,
        destination_generation: CommitBranchGeneration,
        timestamp: Timestamp,
        retained_floor: CommitVersion,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<LifecycleBranchForkOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let _quiesce = self.guard_set.try_begin_quiesce().map_err(commit_error)?;
        let outcome = self.branch_catalog.fork_at_retained_timestamp(
            source,
            destination,
            destination_generation,
            timestamp,
            retained_floor,
            created_at,
        )?;
        self.republish_all_branch_snapshots(); // BS2.3: publish the forked child's snapshot.
        Ok(outcome)
    }

    pub(crate) fn clear_branch(
        &mut self,
        branch_id: BranchId,
        generation_guard: CommitBranchGenerationGuard,
    ) -> LifecycleResult<LifecycleBranchClearOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let _quiesce = self.guard_set.try_begin_quiesce().map_err(commit_error)?;
        let outcome = self
            .branch_catalog
            .clear_branch(branch_id, generation_guard)?;
        // BS2.3: clear reset the branch to an empty state; republish the (now empty) snapshot.
        self.publish_branch_snapshot(branch_id);
        // Cache has no retention pass; release plan is discarded after
        // the outcome leaves this method.
        Ok(outcome)
    }

    pub(crate) fn delete_branch(
        &mut self,
        branch_id: BranchId,
        generation_guard: CommitBranchGenerationGuard,
        deleted_at: Option<CommitVersion>,
    ) -> LifecycleResult<LifecycleBranchDeleteOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let _quiesce = self.guard_set.try_begin_quiesce().map_err(commit_error)?;
        let outcome = self
            .branch_catalog
            .delete_branch(branch_id, generation_guard, deleted_at)?;
        // BS2.3: the branch is tombstoned; drop its snapshot slot.
        self.remove_branch_snapshot(branch_id);
        // Cache mode discards the release plan — no retention to drain.
        Ok(outcome)
    }

    pub(crate) fn flush_frozen(
        &mut self,
        request: &FlushFrozenRequest,
    ) -> LifecycleResult<FlushFrozenOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let branch_id = request.branch_id();
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        let outcome = {
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            flush_cache_branch_with_budget(branch, request, Some(&self.budget), None)
        };
        // BS2.3: flush sealed frozen into an L0 table; republish the branch snapshot.
        self.publish_branch_snapshot(branch_id);
        outcome
    }

    #[allow(
        dead_code,
        reason = "runtime hook is consumed by concrete maintenance modules"
    )]
    pub(crate) fn compact_branch_tables(
        &mut self,
        request: &LifecycleCompactionRequest,
    ) -> LifecycleResult<LifecycleCompactionOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let branch_id = request.branch_id();
        let generation = self.branch_catalog.lookup(branch_id)?.generation();
        let outcome = {
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            compact_cache_branch_with_budget(
                branch,
                request,
                Some(self.open_plan.lifecycle_config().storage_budget()),
            )
        };
        if let Ok(compaction) = &outcome {
            record_lifecycle_compaction_outcome(compaction);
        }
        outcome
    }

    #[allow(
        dead_code,
        reason = "runtime hook is consumed by explicit maintenance dispatch"
    )]
    pub(crate) fn compact_branch_tables_to_fixed_point(
        &mut self,
        request: &LifecycleCompactionDrainRequest,
    ) -> LifecycleResult<LifecycleCompactionDrainOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let branch_id = request.branch_id();
        let generation = self.branch_catalog.lookup(branch_id)?.generation();
        let outcome = {
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            compact_cache_branch_to_fixed_point_with_policy(
                branch,
                request,
                self.open_plan.lifecycle_config().compaction_io_policy(),
            )
        };
        // BS2.3: compaction promoted/rewrote the branch's tables; republish the branch snapshot.
        self.publish_branch_snapshot(branch_id);
        outcome
    }

    #[allow(
        dead_code,
        reason = "runtime hook is consumed by concrete maintenance modules"
    )]
    pub(crate) fn materialize_inherited_layer(
        &mut self,
        request: &LifecycleMaterializationRequest,
    ) -> LifecycleResult<LifecycleMaterializationOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let branch_id = request.child_branch_id();
        let generation = self.branch_catalog.lookup(branch_id)?.generation();
        let outcome = {
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            materialize_cache_branch(branch, request)
        };
        // BS2.3: materialization replaced inherited layers with owned tables; republish.
        self.publish_branch_snapshot(branch_id);
        outcome
    }

    #[allow(
        dead_code,
        reason = "runtime hook is consumed by concrete maintenance modules"
    )]
    pub(crate) fn storage_pressure(&self) -> LifecycleStoragePressure {
        self.storage_pressure_for_branch(self.initial_branch_id)
    }

    pub(crate) fn storage_pressure_for_branch(
        &self,
        branch_id: BranchId,
    ) -> LifecycleStoragePressure {
        let branch = self
            .branch_catalog
            .branch_state(branch_id)
            .expect("pressure target branch is present in the catalog");
        let pressure = collect_storage_pressure_with_budget(
            branch,
            self.maintenance.status(),
            Some(self.open_plan.lifecycle_config().storage_budget()),
        );
        // Volatile modes (cache) never slow, block, or schedule on source-table
        // shape. Neutralizing here is the single chokepoint feeding both write
        // admission and post-commit maintenance suggestion, so neither path acts
        // on source-shape pressure while diagnostics keep the descriptive counts.
        if self
            .open_plan
            .lifecycle_policy()
            .may_apply_source_shape_admission_pressure()
        {
            pressure
        } else {
            pressure.with_source_shape_neutralized()
        }
    }

    fn evaluate_mutating_write_admission_for_branch(
        &mut self,
        branch_id: BranchId,
    ) -> LifecycleResult<()> {
        self.last_write_admission = None;
        let pressure = self.storage_pressure_for_branch(branch_id);
        let mut outcome = evaluate_mutating_write_admission(
            pressure,
            &mut self.pressure_rejected_commit_branches,
        )?;
        if self
            .open_plan
            .lifecycle_config()
            .maintenance_scheduling_policy()
            == LifecycleMaintenanceSchedulingPolicy::DeterministicInline
            && outcome.status() == super::LifecycleWriteAdmissionStatus::AcceptedUnderPressure
            && outcome.pressure().severity() == super::LifecycleStoragePressureSeverity::Urgent
            && self.run_inline_admission_maintenance(outcome.pressure())
        {
            outcome = outcome.with_inline_maintenance_driven();
        }
        self.last_write_admission = Some(outcome);
        Ok(())
    }

    fn require_projected_mutating_commit_budget(
        &self,
        branch_id: BranchId,
        batch: &CommitBatch,
    ) -> LifecycleResult<()> {
        let incoming_active_bytes = estimate_commit_batch_active_bytes(batch)?;
        // The database-wide memory budget applies to cache too: refresh the global total and refuse
        // a commit that would push the database over its memory budget before any visible mutation.
        self.refresh_runtime_memory_total();
        if self.budget.would_exceed_total(incoming_active_bytes) {
            return Err(LifecycleError::StorageBudgetExceeded {
                pool: StorageBudgetPool::ActiveMutable,
                requested_bytes: incoming_active_bytes,
                used_bytes: self.budget.total_used_bytes(),
                limit_bytes: self.budget.budget().total_bytes(),
                requested_count: 0,
                used_count: 0,
                limit_count: None,
                reason: "commit would exceed the database memory budget",
            });
        }
        // Source-shape (frozen rotation -> durable flush) pressure stays volatile-exempt: cache
        // never flushes mutable state to table sources, so it does not block on the frozen budget.
        if !self
            .open_plan
            .lifecycle_policy()
            .may_apply_source_shape_admission_pressure()
        {
            return Ok(());
        }
        let branch = self
            .branch_catalog
            .branch_state(branch_id)
            .expect("commit target branch is present in the catalog");
        if projected_commit_rotation_would_exceed_frozen_budget(
            &self.budget,
            branch,
            incoming_active_bytes,
        )? {
            return Err(LifecycleError::StoragePressureRejected {
                branch_id,
                severity: LifecycleStoragePressureSeverity::BlockMutatingAdmission,
                pressure_reason: LifecycleStoragePressureReason::FrozenBacklog,
                retryable: branch.frozen_table_count() > 0,
                reason: "incoming commit would exceed frozen mutable storage budget after rotation",
            });
        }
        Ok(())
    }

    fn require_generation_guard(
        branch_id: BranchId,
        generation: CommitBranchGeneration,
        generation_guard: CommitBranchGenerationGuard,
    ) -> LifecycleResult<()> {
        match generation_guard {
            CommitBranchGenerationGuard::NotSupplied => Ok(()),
            CommitBranchGenerationGuard::Exact(supplied) if supplied == generation => Ok(()),
            CommitBranchGenerationGuard::Exact(supplied) => {
                Err(LifecycleError::BranchGenerationMismatch {
                    branch_id,
                    expected: generation.get(),
                    actual: supplied.get(),
                })
            }
        }
    }

    fn require_cache_commit_mode(batch: &CommitBatch) -> LifecycleResult<()> {
        if batch.kind() == CommitBatchKind::Mutating
            && batch.options().durability() != CommitDurabilityMode::Cache
        {
            return Err(commit_error(CommitRuntimeError::DurabilityUnavailable {
                reason: "cache commit executor requires cache durability mode",
            }));
        }
        Ok(())
    }

    fn require_no_unresolved_durable_commit(&self) -> LifecycleResult<()> {
        self.durable_gate
            .require_admission_available()
            .map_err(commit_error)
    }

    fn require_branch_commit_guard_available(&self, branch_id: BranchId) -> LifecycleResult<()> {
        self.guard_set
            .require_branch_guard_available(branch_id)
            .map_err(commit_error)
    }

    pub(crate) const fn last_write_admission(&self) -> Option<LifecycleWriteAdmissionOutcome> {
        self.last_write_admission
    }

    #[allow(
        dead_code,
        reason = "runtime hook is consumed by concrete maintenance modules"
    )]
    pub(crate) fn maintenance_status(&self) -> MaintenanceExecutorStatus {
        self.maintenance.status()
    }

    pub(crate) fn recent_maintenance_failures(
        &self,
    ) -> Vec<crate::lifecycle::MaintenanceFailureRecord> {
        self.maintenance.recent_failures()
    }

    #[cfg(test)]
    pub(crate) fn set_active_maintenance_for_test(&mut self, task: MaintenanceTask) {
        self.maintenance.set_active_for_test(task);
    }

    #[allow(
        dead_code,
        reason = "pre-public-boundary policy hook is consumed by lifecycle hardening tests"
    )]
    pub(crate) fn evaluate_wal_growth_policy(&self) -> LifecycleWalGrowthOutcome {
        debug_assert_eq!(self.state(), LifecycleState::Open);
        LifecycleWalGrowthOutcome::cache_mode()
    }

    #[allow(
        dead_code,
        reason = "post-commit scheduler is also exercised through commit execution"
    )]
    pub(crate) fn schedule_post_commit_maintenance(
        &mut self,
    ) -> LifecyclePostCommitMaintenanceOutcome {
        self.schedule_post_commit_maintenance_for_branch(self.initial_branch_id)
    }

    pub(crate) fn schedule_post_commit_maintenance_for_branch(
        &mut self,
        branch_id: BranchId,
    ) -> LifecyclePostCommitMaintenanceOutcome {
        let policy = self
            .open_plan
            .lifecycle_config()
            .maintenance_scheduling_policy();
        let pressure = self.storage_pressure_for_branch(branch_id);
        let outcome = schedule_suggested_post_commit_maintenance(policy, pressure, |request| {
            self.enqueue_maintenance(request)
        });
        let outcome = if policy == LifecycleMaintenanceSchedulingPolicy::DeterministicInline {
            self.run_inline_post_commit_maintenance(outcome)
        } else {
            outcome
        };
        self.schedule_maintenance_coverage_after_branch(branch_id, policy);
        outcome
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn schedule_post_commit_maintenance_for_test(
        &mut self,
        branch_id: BranchId,
    ) -> LifecyclePostCommitMaintenanceOutcome {
        self.schedule_post_commit_maintenance_for_branch(branch_id)
    }

    pub(crate) fn schedule_background_maintenance_coverage(&mut self) -> bool {
        // Coverage hysteresis — same rationale as the durable runtime's
        // scheduler: only re-fire coverage after a real maintenance
        // completion, or an empty-queue-with-deferring-tasks state spins the
        // drain generating work that instantly defers.
        let completed = self.maintenance.stats().completed();
        if let Some(last) = self.coverage_completed_watermark {
            if completed == last {
                return false;
            }
        }
        self.coverage_completed_watermark = Some(completed);
        let outcome = self.schedule_post_commit_maintenance_for_branch(self.initial_branch_id);
        matches!(
            outcome.status(),
            crate::lifecycle::maintenance::LifecyclePostCommitMaintenanceStatus::Enqueued
        )
    }

    fn schedule_maintenance_coverage_after_branch(
        &mut self,
        source_branch_id: BranchId,
        policy: LifecycleMaintenanceSchedulingPolicy,
    ) {
        if policy == LifecycleMaintenanceSchedulingPolicy::Disabled {
            return;
        }
        if !self
            .state
            .admit(LifecycleOperationKind::OrdinaryMaintenance)
            .is_allowed()
        {
            crate::observability::perf_trace::record_lifecycle_maintenance_coverage_stop_failure();
            return;
        }
        let descriptors = self.branch_catalog.list_branches(false);
        crate::observability::perf_trace::record_lifecycle_maintenance_coverage_scan(
            descriptors.len(),
        );
        let maintenance_status = self.maintenance.status();
        let mut saw_eligible_work = false;
        for descriptor in descriptors {
            let branch_id = descriptor.branch_id();
            if branch_id == source_branch_id {
                continue;
            }
            let Ok(branch) = self.branch_catalog.branch_state(branch_id) else {
                crate::observability::perf_trace::
                    record_lifecycle_maintenance_coverage_stop_failure();
                return;
            };
            let pressure = collect_storage_pressure_with_budget(
                branch,
                maintenance_status,
                Some(self.open_plan.lifecycle_config().storage_budget()),
            );
            let Some(request) = pressure.suggested_task() else {
                continue;
            };
            if pressure.severity() != LifecycleStoragePressureSeverity::None {
                crate::observability::perf_trace::
                    record_lifecycle_maintenance_coverage_quiet_branch_pressure();
            }
            saw_eligible_work = true;
            match self.enqueue_maintenance(request) {
                Ok(enqueue) => {
                    crate::observability::perf_trace::record_lifecycle_maintenance_coverage_enqueue(
                        enqueue.was_enqueued(),
                        enqueue.was_coalesced(),
                    );
                }
                Err(LifecycleError::MaintenanceQueueFull { .. }) => {
                    crate::observability::perf_trace::
                        record_lifecycle_maintenance_coverage_stop_queue_full();
                    return;
                }
                Err(_) => {
                    crate::observability::perf_trace::
                        record_lifecycle_maintenance_coverage_stop_failure();
                    return;
                }
            }
        }
        if saw_eligible_work {
            self.maintenance_coverage_idle_rounds = 0;
        } else {
            self.record_maintenance_coverage_idle_stop();
        }
    }

    fn record_maintenance_coverage_idle_stop(&mut self) {
        let mut reached_idle_limit = false;
        if self.maintenance_coverage_idle_rounds < MAINTENANCE_COVERAGE_IDLE_ROUND_LIMIT {
            self.maintenance_coverage_idle_rounds =
                self.maintenance_coverage_idle_rounds.saturating_add(1);
            crate::observability::perf_trace::record_lifecycle_maintenance_coverage_idle_round();
            reached_idle_limit =
                self.maintenance_coverage_idle_rounds >= MAINTENANCE_COVERAGE_IDLE_ROUND_LIMIT;
        }
        if reached_idle_limit {
            crate::observability::perf_trace::record_lifecycle_maintenance_coverage_stop_idle_limit(
            );
        } else if self.maintenance_coverage_idle_rounds < MAINTENANCE_COVERAGE_IDLE_ROUND_LIMIT {
            crate::observability::perf_trace::
                record_lifecycle_maintenance_coverage_stop_no_pressure();
        }
    }

    fn run_inline_post_commit_maintenance(
        &mut self,
        outcome: LifecyclePostCommitMaintenanceOutcome,
    ) -> LifecyclePostCommitMaintenanceOutcome {
        // Simulation-boundary deletion condition: remove this lifecycle-local
        // deterministic-inline path once lower-level lifecycle tests migrate to
        // the API Background + InlineMaintenanceExecutor path.
        let (Some(request), Some(enqueue)) = (outcome.suggested_task(), outcome.enqueue()) else {
            return outcome;
        };
        let inline_start = crate::time_compat::Instant::now();
        let result = self.run_inline_maintenance_task(request, enqueue.task_id());
        crate::observability::perf_trace::record_lifecycle_inline_maintenance(
            inline_start.elapsed(),
        );
        match result {
            Ok(()) => outcome,
            Err(error) => {
                crate::observability::perf_trace::record_lifecycle_post_commit_maintenance_deferred(
                );
                outcome.with_inline_failure(error)
            }
        }
    }

    fn run_inline_maintenance_task(
        &mut self,
        request: MaintenanceTaskRequest,
        task_id: MaintenanceTaskId,
    ) -> LifecycleResult<()> {
        let outcome = match request.kind() {
            MaintenanceTaskKind::Flush => self.run_flush_maintenance_task(task_id)?,
            MaintenanceTaskKind::Compaction => self.run_compaction_maintenance_task(task_id)?,
            MaintenanceTaskKind::Materialization => {
                self.run_materialization_maintenance_task(task_id)?
            }
            _ => {
                return Err(LifecycleError::MaintenanceTaskFailed {
                    reason: "post-commit inline scheduling does not support task kind",
                });
            }
        };
        if outcome.is_none() {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "post-commit inline task was not pending",
            });
        }
        Ok(())
    }

    fn run_inline_admission_maintenance(&mut self, pressure: LifecycleStoragePressure) -> bool {
        // Simulation-boundary deletion condition: remove this lifecycle-local
        // deterministic-inline path once lower-level lifecycle tests migrate to
        // the API Background + InlineMaintenanceExecutor path.
        let Some(request) = pressure.suggested_task() else {
            return false;
        };
        crate::observability::perf_trace::record_lifecycle_write_admission_inline_attempt();
        crate::observability::perf_trace::record_lifecycle_write_admission_urgent_inline_attempt();
        let Ok(enqueue) = self.enqueue_maintenance(request) else {
            return false;
        };
        let inline_start = crate::time_compat::Instant::now();
        let result = self.run_inline_maintenance_task(request, enqueue.task_id());
        crate::observability::perf_trace::record_lifecycle_inline_maintenance(
            inline_start.elapsed(),
        );
        result.is_ok()
    }

    #[cfg(test)]
    pub(crate) fn force_close_requested_for_test(&mut self) -> LifecycleResult<()> {
        self.state
            .transition(LifecycleTransitionTrigger::CloseRequested)?;
        Ok(())
    }

    #[allow(
        dead_code,
        reason = "runtime hook is consumed by concrete maintenance modules"
    )]
    pub(crate) fn enqueue_maintenance(
        &mut self,
        request: MaintenanceTaskRequest,
    ) -> LifecycleResult<MaintenanceEnqueueOutcome> {
        if !cache_supports_maintenance_kind(request.kind()) {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "volatile runtime does not support durable maintenance task",
            });
        }
        let budget = self.budget.clone();
        let maintenance_status = self.maintenance.status();
        let state = self.state;
        {
            let maintenance = &mut self.maintenance;
            let branch_catalog = &mut self.branch_catalog;
            maintenance.enqueue_with_binding(state, request, |request| {
                require_maintenance_enqueue_budget(&budget, maintenance_status)?;
                bind_materialization_request_in_catalog(branch_catalog, request)
            })
        }
    }

    #[allow(
        dead_code,
        reason = "runtime hook is consumed by concrete maintenance modules"
    )]
    pub(crate) fn run_next_maintenance(
        &mut self,
        runner: &mut impl MaintenanceTaskRunner,
    ) -> LifecycleResult<Option<super::MaintenanceOutcome>> {
        let outcome = self.maintenance.run_next(self.state, runner);
        // BS2.3: the runner may have rewritten any branch's tables; republish snapshots.
        self.republish_all_branch_snapshots();
        outcome
    }

    pub(crate) fn run_next_flush_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let Some(task) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::Flush)
        else {
            return Ok(None);
        };
        self.run_flush_maintenance_task(task.id())
    }

    fn run_flush_maintenance_task(
        &mut self,
        task_id: MaintenanceTaskId,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let state = self.state;
        require_admitted(state, LifecycleOperationKind::OrdinaryMaintenance)?;
        if self
            .maintenance
            .next_matching_task(|task| {
                task.id() == task_id && task.kind() == MaintenanceTaskKind::Flush
            })
            .is_none()
        {
            return Ok(None);
        }
        let result = {
            let maintenance = &mut self.maintenance;
            let mut runner = CacheFlushMaintenanceRunner {
                branch_catalog: &mut self.branch_catalog,
                budget: &self.budget,
            };
            maintenance.run_next_matching(state, &mut runner, |task| task.id() == task_id)
        };
        // BS2.3: the flush runner sealed active into an L0 table; republish snapshots.
        self.republish_all_branch_snapshots();
        result
    }

    pub(crate) fn start_next_background_flush_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<CacheBackgroundMaintenanceStep>> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let Some(task) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::Flush)
        else {
            return Ok(None);
        };
        let MaintenanceTaskScope::Branch(branch_id) = task.scope() else {
            return Ok(None);
        };
        let state = self.state;
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == task.id())?
        else {
            return Ok(None);
        };
        let flush_operation_index = usize::try_from(task.sequence()).unwrap_or(usize::MAX);
        let request = flush_drain_request_for_branch_from_maintenance_task(&task, branch_id)?
            .flush_request(flush_operation_index)?;
        let generation = match self.branch_catalog.registry().lookup(branch_id) {
            Ok(descriptor) => descriptor.generation(),
            Err(error) => {
                let outcome = background_candidate_failed_outcome(task.kind(), commit_error(error));
                return self
                    .maintenance
                    .finish_started(task, outcome, false)
                    .map(|outcome| {
                        Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))
                    });
            }
        };
        let branch = match self
            .branch_catalog
            .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))
        {
            Ok(branch) => branch,
            Err(error) => {
                let outcome = background_candidate_failed_outcome(task.kind(), error);
                return self
                    .maintenance
                    .finish_started(task, outcome, false)
                    .map(|outcome| {
                        Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))
                    });
            }
        };
        if branch.active_row_count() > 0 {
            if let Err(error) = require_rotate_budget(&self.budget, branch) {
                let outcome = background_candidate_failed_outcome(task.kind(), error);
                return self
                    .maintenance
                    .finish_started(task, outcome, false)
                    .map(|outcome| {
                        Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))
                    });
            }
            branch.rotate_active();
        }
        Ok(Some(CacheBackgroundMaintenanceStep::Build(Box::new(
            CacheBackgroundMaintenanceBuild::Flush {
                task,
                branch_id,
                request,
                branch_snapshot: branch.clone(),
                budget: self.budget.clone(),
                started_at: crate::time_compat::Instant::now(),
            },
        ))))
    }

    #[allow(
        dead_code,
        reason = "runtime maintenance entry point is consumed by dedicated tests"
    )]
    pub(crate) fn run_next_compaction_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let Some(task) = self.next_scored_compaction_task() else {
            return Ok(None);
        };
        self.run_compaction_maintenance_task(task.id())
    }

    pub(crate) fn run_next_table_rewrite_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let Some(task) = self.next_scored_table_rewrite_task() else {
            return Ok(None);
        };
        match task.kind() {
            MaintenanceTaskKind::Compaction => self.run_compaction_maintenance_task(task.id()),
            MaintenanceTaskKind::Materialization => {
                self.run_materialization_maintenance_task(task.id())
            }
            _ => Ok(None),
        }
    }

    pub(crate) fn start_next_background_table_rewrite_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<CacheBackgroundMaintenanceStep>> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let Some(task) = self.next_scored_table_rewrite_task() else {
            return Ok(None);
        };
        match task.kind() {
            MaintenanceTaskKind::Compaction => self.start_background_compaction_task(task),
            MaintenanceTaskKind::Materialization => {
                self.start_background_materialization_task(task)
            }
            _ => Ok(None),
        }
    }

    fn start_background_compaction_task(
        &mut self,
        task: MaintenanceTask,
    ) -> LifecycleResult<Option<CacheBackgroundMaintenanceStep>> {
        let state = self.state;
        let (branch_id, _level) = table_level_scope_from_task(task)?;
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == task.id())?
        else {
            return Ok(None);
        };
        let compaction_io_policy = self.open_plan.lifecycle_config().compaction_io_policy();
        let generation = match self.branch_catalog.registry().lookup(branch_id) {
            Ok(descriptor) => descriptor.generation(),
            Err(error) => {
                let outcome = background_candidate_failed_outcome(task.kind(), commit_error(error));
                return self
                    .maintenance
                    .finish_started(task, outcome, false)
                    .map(|outcome| {
                        Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))
                    });
            }
        };
        let branch = match self
            .branch_catalog
            .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))
        {
            Ok(branch) => branch,
            Err(error) => {
                let outcome = background_candidate_failed_outcome(task.kind(), error);
                return self
                    .maintenance
                    .finish_started(task, outcome, false)
                    .map(|outcome| {
                        Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))
                    });
            }
        };
        let budget = self.open_plan.lifecycle_config().storage_budget();
        let request = match current_compaction_request_from_maintenance_task_with_budget(
            &task,
            branch,
            Some(budget),
        ) {
            Ok(Some(request)) => request,
            Ok(None) => {
                crate::observability::perf_trace::record_lifecycle_background_candidate_stale_deferred(
                );
                let outcome = stale_compaction_maintenance_outcome();
                return self
                    .maintenance
                    .finish_started(task, outcome, false)
                    .map(|outcome| {
                        Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))
                    });
            }
            Err(error) => {
                let outcome = background_candidate_failed_outcome(task.kind(), error);
                return self
                    .maintenance
                    .finish_started(task, outcome, false)
                    .map(|outcome| {
                        Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))
                    });
            }
        };
        let deferred =
            match defer_compaction_for_resource_policy(branch, &request, compaction_io_policy) {
                Ok(deferred) => deferred,
                Err(error) => {
                    let outcome = background_candidate_failed_outcome(task.kind(), error);
                    return self
                        .maintenance
                        .finish_started(task, outcome, false)
                        .map(|outcome| {
                            Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))
                        });
                }
            };
        if let Some(outcome) = deferred {
            return self
                .maintenance
                .finish_started(task, outcome, false)
                .map(|outcome| Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome))));
        }
        Ok(Some(CacheBackgroundMaintenanceStep::Build(Box::new(
            CacheBackgroundMaintenanceBuild::Compaction {
                task,
                branch_id,
                request,
                branch_snapshot: branch.clone(),
                storage_budget: budget,
                started_at: crate::time_compat::Instant::now(),
            },
        ))))
    }

    fn start_background_materialization_task(
        &mut self,
        task: MaintenanceTask,
    ) -> LifecycleResult<Option<CacheBackgroundMaintenanceStep>> {
        let state = self.state;
        let branch_id = branch_id_from_inherited_layer_task(task)?;
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == task.id())?
        else {
            return Ok(None);
        };
        let request = materialization_request_from_maintenance_task(&task)?;
        let generation = match self.branch_catalog.registry().lookup(branch_id) {
            Ok(descriptor) => descriptor.generation(),
            Err(error) => {
                let outcome = background_candidate_failed_outcome(task.kind(), commit_error(error));
                return self
                    .maintenance
                    .finish_started(task, outcome, false)
                    .map(|outcome| {
                        Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))
                    });
            }
        };
        let branch = match self
            .branch_catalog
            .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))
        {
            Ok(branch) => branch,
            Err(error) => {
                let outcome = background_candidate_failed_outcome(task.kind(), error);
                return self
                    .maintenance
                    .finish_started(task, outcome, false)
                    .map(|outcome| {
                        Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))
                    });
            }
        };
        let begin = match begin_cache_materialization_build(branch, &request) {
            Ok(begin) => begin,
            Err(error) => {
                let outcome = background_candidate_failed_outcome(task.kind(), error);
                return self
                    .maintenance
                    .finish_started(task, outcome, false)
                    .map(|outcome| {
                        Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))
                    });
            }
        };
        match begin {
            CacheMaterializationBegin::Deferred(outcome) => self
                .maintenance
                .finish_started(task, outcome.maintenance_outcome(), false)
                .map(|outcome| Some(CacheBackgroundMaintenanceStep::Completed(Box::new(outcome)))),
            CacheMaterializationBegin::Build(build) => {
                Ok(Some(CacheBackgroundMaintenanceStep::Build(Box::new(
                    CacheBackgroundMaintenanceBuild::Materialization {
                        task,
                        branch_id,
                        build: *build,
                        started_at: crate::time_compat::Instant::now(),
                    },
                ))))
            }
        }
    }

    pub(crate) fn finish_background_maintenance(
        &mut self,
        built: CacheBackgroundMaintenanceBuilt,
    ) -> LifecycleResult<MaintenanceOutcome> {
        match built {
            CacheBackgroundMaintenanceBuilt::Flush {
                task,
                branch_id,
                request,
                prepared,
                elapsed,
            } => self.finish_background_flush_task(task, branch_id, &request, prepared, elapsed),
            CacheBackgroundMaintenanceBuilt::Compaction {
                task,
                branch_id,
                prepared,
                elapsed,
            } => self.finish_background_compaction_task(task, branch_id, prepared, elapsed),
            CacheBackgroundMaintenanceBuilt::Materialization {
                task,
                branch_id,
                prepared,
                elapsed,
            } => self.finish_background_materialization_task(task, branch_id, prepared, elapsed),
        }
    }

    pub(crate) fn finish_background_build_error(
        &mut self,
        task: MaintenanceTask,
        error: LifecycleError,
    ) -> LifecycleResult<MaintenanceOutcome> {
        let outcome = background_candidate_failed_outcome(task.kind(), error);
        self.maintenance.finish_started(task, outcome, false)
    }

    fn finish_background_flush_task(
        &mut self,
        task: MaintenanceTask,
        branch_id: BranchId,
        request: &FlushFrozenRequest,
        prepared: Option<PreparedCacheFlush>,
        _elapsed: std::time::Duration,
    ) -> LifecycleResult<MaintenanceOutcome> {
        let result: LifecycleResult<FlushFrozenOutcome> = match prepared {
            Some(prepared) => (|| {
                let generation = self
                    .branch_catalog
                    .registry()
                    .lookup(branch_id)
                    .map_err(commit_error)?
                    .generation();
                let branch = self
                    .branch_catalog
                    .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
                Ok(install_prepared_cache_flush(branch, prepared))
            })(),
            None => Ok(FlushFrozenOutcome::deferred(request)),
        };
        let outcome = match result {
            Ok(flush) if flush.failure().is_none() => flush.maintenance_outcome(),
            Ok(flush) => {
                crate::observability::perf_trace::record_lifecycle_background_candidate_stale_deferred(
                );
                let error = LifecycleError::MaintenanceTaskFailed {
                    reason: if flush.failure().is_some() {
                        "background flush candidate became stale before publish"
                    } else {
                        "background flush publish failed"
                    },
                };
                background_candidate_stale_outcome(task.kind(), error)
            }
            Err(error) => {
                crate::observability::perf_trace::record_lifecycle_background_candidate_stale_deferred(
                );
                background_candidate_stale_outcome(task.kind(), error)
            }
        };
        let outcome = self.maintenance.finish_started(task, outcome, false)?;
        // BS2.3: flush installed an L0 table (or deferred) — republish the branch snapshot.
        self.publish_branch_snapshot(branch_id);
        if let Ok(branch) = self.branch_catalog.branch_state(branch_id) {
            if branch.frozen_table_count() > 0 {
                let _ = self.enqueue_maintenance(MaintenanceTaskRequest::flush(branch_id));
            }
        }
        Ok(outcome)
    }

    fn finish_background_compaction_task(
        &mut self,
        task: MaintenanceTask,
        branch_id: BranchId,
        prepared: PreparedCacheCompaction,
        elapsed: std::time::Duration,
    ) -> LifecycleResult<MaintenanceOutcome> {
        let (_, level) = table_level_scope_from_task(task)?;
        let result: LifecycleResult<LifecycleCompactionOutcome> = (|| {
            let generation = self
                .branch_catalog
                .registry()
                .lookup(branch_id)
                .map_err(commit_error)?
                .generation();
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            install_prepared_cache_compaction(branch, prepared, elapsed)
        })();
        let mut stale_deferred = false;
        let outcome = match result {
            Ok(compaction) => {
                record_lifecycle_compaction_outcome(&compaction);
                compaction.maintenance_outcome()
            }
            Err(error) => {
                crate::observability::perf_trace::record_lifecycle_background_candidate_stale_deferred(
                );
                stale_deferred = true;
                background_candidate_stale_outcome(task.kind(), error)
            }
        };
        let outcome = self.maintenance.finish_started(task, outcome, false)?;
        // BS2.3: compaction rewrote the branch's tables — republish the branch snapshot.
        self.publish_branch_snapshot(branch_id);
        if stale_deferred {
            self.resubmit_table_rewrite_if_any_branch_still_unhealthy(branch_id);
        } else if table_rewrite_outcome_was_flush_preempted(&outcome) {
            self.requeue_flush_preempted_compaction(branch_id, level);
        } else if table_rewrite_outcome_allows_chain_resubmit(&outcome) {
            self.resubmit_table_rewrite_if_any_branch_still_unhealthy(branch_id);
        }
        Ok(outcome)
    }

    fn finish_background_materialization_task(
        &mut self,
        task: MaintenanceTask,
        branch_id: BranchId,
        prepared: PreparedCacheMaterialization,
        _elapsed: std::time::Duration,
    ) -> LifecycleResult<MaintenanceOutcome> {
        let result: LifecycleResult<LifecycleMaterializationOutcome> = (|| {
            let generation = self
                .branch_catalog
                .registry()
                .lookup(branch_id)
                .map_err(commit_error)?
                .generation();
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            install_prepared_cache_materialization(branch, prepared)
        })();
        let mut stale_deferred = false;
        let outcome = match result {
            Ok(materialization) => materialization.maintenance_outcome(),
            Err(error) => {
                crate::observability::perf_trace::record_lifecycle_background_candidate_stale_deferred(
                );
                stale_deferred = true;
                background_candidate_stale_outcome(task.kind(), error)
            }
        };
        let outcome = self.maintenance.finish_started(task, outcome, false)?;
        // BS2.3: materialization replaced inherited layers with owned tables — republish.
        self.publish_branch_snapshot(branch_id);
        if stale_deferred || table_rewrite_outcome_allows_chain_resubmit(&outcome) {
            self.resubmit_table_rewrite_if_any_branch_still_unhealthy(branch_id);
        }
        Ok(outcome)
    }

    fn next_scored_table_rewrite_task(&self) -> Option<MaintenanceTask> {
        self.maintenance
            .pending_tasks()
            .iter()
            .copied()
            .filter(|task| {
                matches!(
                    task.kind(),
                    MaintenanceTaskKind::Compaction | MaintenanceTaskKind::Materialization
                )
            })
            .max_by_key(|task| {
                (
                    self.table_rewrite_task_score_key(*task),
                    std::cmp::Reverse(task.sequence()),
                )
            })
    }

    fn table_rewrite_task_score_key(
        &self,
        task: MaintenanceTask,
    ) -> Option<LifecycleTableRewriteScoreKey> {
        let branch_id = branch_id_from_table_rewrite_task(task).ok()?;
        let branch = self.branch_catalog.branch_state(branch_id).ok()?;
        table_rewrite_score_key_for_task_with_budget(
            branch,
            task,
            Some(self.open_plan.lifecycle_config().storage_budget()),
        )
    }

    fn next_scored_compaction_task(&self) -> Option<MaintenanceTask> {
        self.maintenance
            .pending_tasks()
            .iter()
            .copied()
            .filter(|task| task.kind() == MaintenanceTaskKind::Compaction)
            .max_by_key(|task| {
                (
                    self.compaction_task_score_key(*task),
                    std::cmp::Reverse(task.sequence()),
                )
            })
    }

    fn compaction_task_score_key(
        &self,
        task: MaintenanceTask,
    ) -> Option<LifecycleCompactionScoreKey> {
        let branch_id = branch_id_from_table_level_task(task).ok()?;
        let branch = self.branch_catalog.branch_state(branch_id).ok()?;
        compaction_score_key_for_task(branch, task)
    }

    pub(crate) fn run_compaction_maintenance_task(
        &mut self,
        task_id: MaintenanceTaskId,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let state = self.state;
        require_admitted(state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let Some(task) = self.maintenance.next_matching_task(|task| {
            task.id() == task_id && task.kind() == MaintenanceTaskKind::Compaction
        }) else {
            return Ok(None);
        };
        let (branch_id, level) = table_level_scope_from_task(task)?;
        // Pre-sync the shadow into the catalog so any direct shadow
        // mutations (e.g. test-only branch_state_mut writes) are visible
        // when the runner fetches branch state via the catalog.
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        let outcome = {
            let maintenance = &mut self.maintenance;
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            let mut runner = CacheCompactionMaintenanceRunner {
                branch,
                compaction_io_policy: self.open_plan.lifecycle_config().compaction_io_policy(),
                storage_budget: self.open_plan.lifecycle_config().storage_budget(),
            };
            maintenance.run_next_matching(state, &mut runner, |task| task.id() == task_id)
        }?;
        // BS2.3: compaction rewrote the branch's tables; republish the branch snapshot.
        self.publish_branch_snapshot(branch_id);
        if outcome
            .as_ref()
            .is_some_and(table_rewrite_outcome_was_flush_preempted)
        {
            self.requeue_flush_preempted_compaction(branch_id, level);
        } else if outcome
            .as_ref()
            .is_some_and(table_rewrite_outcome_allows_chain_resubmit)
        {
            self.resubmit_table_rewrite_if_any_branch_still_unhealthy(branch_id);
        }
        Ok(outcome)
    }

    fn requeue_flush_preempted_compaction(&mut self, branch_id: BranchId, level: u8) {
        let _ = self.enqueue_maintenance(MaintenanceTaskRequest::flush(branch_id));
        match self.enqueue_maintenance(MaintenanceTaskRequest::compaction(branch_id, level)) {
            Ok(enqueue) => {
                crate::observability::perf_trace::record_lifecycle_compaction_resubmit(
                    enqueue.was_coalesced(),
                );
            }
            Err(_error) => {
                crate::observability::perf_trace::record_lifecycle_compaction_resubmit_deferred();
            }
        }
    }

    fn resubmit_table_rewrite_if_any_branch_still_unhealthy(&mut self, branch_id: BranchId) {
        if let Ok(branch) = self.branch_catalog.branch_state(branch_id) {
            record_lifecycle_table_rewrite_post_operation_score_with_budget(
                branch,
                Some(self.budget.budget()),
            );
        }
        let Some(request) = self.highest_scored_table_rewrite_request() else {
            return;
        };
        let request_kind = request.kind();
        match self.enqueue_maintenance(request) {
            Ok(enqueue) => {
                if request_kind == MaintenanceTaskKind::Compaction {
                    crate::observability::perf_trace::record_lifecycle_compaction_resubmit(
                        enqueue.was_coalesced(),
                    );
                }
            }
            Err(_error) => {
                if request_kind == MaintenanceTaskKind::Compaction {
                    crate::observability::perf_trace::record_lifecycle_compaction_resubmit_deferred(
                    );
                }
            }
        }
    }

    #[cfg(all(test, feature = "perf-trace"))]
    pub(crate) fn resubmit_table_rewrite_if_branch_still_unhealthy_for_test(
        &mut self,
        branch_id: BranchId,
    ) {
        self.resubmit_table_rewrite_if_any_branch_still_unhealthy(branch_id);
    }

    fn highest_scored_table_rewrite_request(&self) -> Option<MaintenanceTaskRequest> {
        self.branch_catalog
            .list_branches(false)
            .into_iter()
            .filter_map(|descriptor| {
                let branch = self
                    .branch_catalog
                    .branch_state(descriptor.branch_id())
                    .ok()?;
                Some((
                    table_rewrite_score_key_for_branch_with_budget(
                        branch,
                        Some(self.open_plan.lifecycle_config().storage_budget()),
                    )?,
                    std::cmp::Reverse(*descriptor.branch_id().as_bytes()),
                    table_rewrite_task_request_for_branch_with_budget(
                        branch,
                        Some(self.open_plan.lifecycle_config().storage_budget()),
                    )?,
                ))
            })
            .max_by_key(|(score, branch_tiebreaker, _)| (*score, *branch_tiebreaker))
            .map(|(_, _, request)| request)
    }

    #[allow(
        dead_code,
        reason = "runtime maintenance entry point is consumed by dedicated tests"
    )]
    pub(crate) fn run_next_materialization_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let Some(task) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::Materialization)
        else {
            return Ok(None);
        };
        self.run_materialization_maintenance_task(task.id())
    }

    pub(crate) fn run_materialization_maintenance_task(
        &mut self,
        task_id: MaintenanceTaskId,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let state = self.state;
        require_admitted(state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let Some(task) = self.maintenance.next_matching_task(|task| {
            task.id() == task_id && task.kind() == MaintenanceTaskKind::Materialization
        }) else {
            return Ok(None);
        };
        let branch_id = branch_id_from_inherited_layer_task(task)?;
        // Pre-sync the shadow into the catalog so any direct shadow
        // mutations (e.g. test-only branch_state_mut writes) are visible
        // when the runner fetches branch state via the catalog.
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        let outcome = {
            let maintenance = &mut self.maintenance;
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            let mut runner = CacheMaterializationMaintenanceRunner { branch };
            maintenance.run_next_matching(state, &mut runner, |task| task.id() == task_id)
        }?;
        // BS2.3: materialization replaced inherited layers with owned tables; republish.
        self.publish_branch_snapshot(branch_id);
        if outcome
            .as_ref()
            .is_some_and(table_rewrite_outcome_allows_chain_resubmit)
        {
            self.resubmit_table_rewrite_if_any_branch_still_unhealthy(branch_id);
        }
        Ok(outcome)
    }

    pub(crate) fn close(&mut self) -> LifecycleResult<CloseOutcome> {
        match self.state.state() {
            LifecycleState::Closed => {
                self.state
                    .transition(LifecycleTransitionTrigger::CloseRetried)?;
                // Return the prior final facts from the first close so
                // callers observing the second outcome see the same stats
                // (canceled count, status, close fact) they saw on the
                // first call, just with the close fact remapped to
                // AlreadyClosed and the status to Idempotent. Falling
                // back to a fabricated outcome would silently drift from
                // the first-close stats.
                Ok(self
                    .last_close_outcome
                    .as_ref()
                    .map_or_else(cache_idempotent_close_outcome, idempotent_from_prior_close))
            }
            LifecycleState::Open => {
                require_admitted(self.state, LifecycleOperationKind::Close)?;
                self.state
                    .transition(LifecycleTransitionTrigger::CloseRequested)?;
                // Cache supports its own drain-required path by dispatching
                // queued drain tasks through the per-kind cache runners.
                // The cache runtime is volatile-only, so drained tasks have
                // no durable side effects; the work still runs so frozen
                // mutable state is folded into branch read state before
                // close completes.
                let drained = self.drain_cache_required_tasks()?;
                self.finish_cache_close(drained)
            }
            LifecycleState::Closing => {
                require_admitted(self.state, LifecycleOperationKind::CloseRetry)?;
                self.state
                    .transition(LifecycleTransitionTrigger::CloseRetried)?;
                // A retry from Closing must also drain any drain-required
                // tasks that the first close didn't complete — otherwise
                // they leak past the close transition.
                let drained = self.drain_cache_required_tasks()?;
                self.finish_cache_close(drained)
            }
            LifecycleState::Failed => {
                // Symmetric with the durable runtime: Failed admits Close
                // only when the prior failure was a close-class failure.
                // The state machine's `admit` enforces this; rejection
                // surfaces here as `InvalidLifecycleState`. The drain must
                // still run on the retry so the queue is empty when the
                // runtime reaches Closed.
                require_admitted(self.state, LifecycleOperationKind::Close)?;
                self.state
                    .transition(LifecycleTransitionTrigger::CloseRequested)?;
                let drained = self.drain_cache_required_tasks()?;
                self.finish_cache_close(drained)
            }
            LifecycleState::New | LifecycleState::Opening | LifecycleState::Recovering => {
                Err(LifecycleError::InvalidLifecycleState {
                    reason: "cache runtime is not open for close",
                })
            }
        }
    }

    fn finish_cache_close(&mut self, drained: usize) -> LifecycleResult<CloseOutcome> {
        let cancel = self.maintenance.cancel_pending_for_close(self.state)?;
        self.state
            .transition(LifecycleTransitionTrigger::CloseCompleted)?;
        let outcome = cache_close_outcome(cancel, drained);
        // Snapshot the outcome so subsequent idempotent close calls return
        // the same prior facts instead of fabricating fresh values.
        self.last_close_outcome = Some(outcome);
        Ok(outcome)
    }

    fn drain_cache_required_tasks(&mut self) -> LifecycleResult<usize> {
        // Cache only supports Flush / Compaction / Materialization /
        // HealthCollection. Each kind has a per-kind runner that the
        // close-time drain dispatches through. The drain executor's
        // `drain_for_close` runs every queued drain-required task in
        // order; if any task errors the drain aborts and surfaces the
        // typed lifecycle error, which the caller routes to close-retry.
        // The returned count feeds the close outcome's
        // `maintenance_tasks` stat so callers see drained work, parity
        // with the durable runtime.
        let state = self.state;
        let branch_id = self.initial_branch_id;
        // Pre-sync shadow into catalog so the close-time drain runs over
        // up-to-date branch state.
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        let drained_tasks = {
            let maintenance = &mut self.maintenance;
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            let mut runner = CacheCloseRunner {
                branch,
                budget: &self.budget,
            };
            let mut active_tasks = 0_usize;
            while maintenance
                .drain_active_for_close(state, &mut runner)?
                .is_some()
            {
                active_tasks = active_tasks.saturating_add(1);
            }
            let drain = maintenance.drain_for_close(state, &mut runner)?;
            drain.drained_tasks().saturating_add(active_tasks)
        };
        Ok(drained_tasks)
    }
}

struct CacheCloseRunner<'a> {
    branch: &'a mut BranchLocalState,
    budget: &'a StorageBudgetLedger,
}

impl MaintenanceTaskRunner for CacheCloseRunner<'_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        match task.kind() {
            MaintenanceTaskKind::Flush => {
                let request = flush_drain_request_from_maintenance_task(task)?;
                if self.branch.active_row_count() > 0 {
                    require_rotate_budget(self.budget, self.branch)?;
                    self.branch.rotate_active();
                }
                Ok(
                    flush_branch_drain_with(self.branch, &request, |branch, request| {
                        Ok(flush_cache_branch_with_budget(
                            branch,
                            request,
                            Some(self.budget),
                            None,
                        )?
                        .maintenance_outcome())
                    })?
                    .maintenance_outcome(),
                )
            }
            MaintenanceTaskKind::Compaction => {
                let Some(request) = current_compaction_request_from_maintenance_task_with_budget(
                    task,
                    self.branch,
                    Some(self.budget.budget()),
                )?
                else {
                    return Ok(stale_compaction_maintenance_outcome());
                };
                let compaction = compact_cache_branch_with_budget(
                    self.branch,
                    &request,
                    Some(self.budget.budget()),
                )?;
                record_lifecycle_compaction_outcome(&compaction);
                Ok(compaction.maintenance_outcome())
            }
            MaintenanceTaskKind::Materialization => {
                let request = materialization_request_from_maintenance_task(task)?;
                Ok(materialize_cache_branch(self.branch, &request)?.maintenance_outcome())
            }
            MaintenanceTaskKind::HealthCollection => Ok(MaintenanceOutcome::new(
                MaintenanceTaskKind::HealthCollection,
                MaintenanceOutcomeStatus::Completed,
            )),
            other => Err(LifecycleError::MaintenanceTaskFailed {
                reason: cache_unsupported_drain_reason(other),
            }),
        }
    }
}

const fn cache_unsupported_drain_reason(_kind: MaintenanceTaskKind) -> &'static str {
    // Volatile cache runtimes only host the four kinds enumerated by
    // `cache_supports_maintenance_kind`. Reaching this arm means a
    // checkpoint/wal/retention/quarantine task was enqueued through a
    // path that bypassed the enqueue-time guard — surface a typed
    // failure rather than running it.
    "cache drain rejected a task kind that cache mode does not implement"
}

fn background_candidate_stale_outcome(
    kind: MaintenanceTaskKind,
    error: LifecycleError,
) -> MaintenanceOutcome {
    MaintenanceOutcome::new(kind, MaintenanceOutcomeStatus::Deferred)
        .with_reason("background maintenance candidate became stale before publish")
        .with_effects(0, 0, true)
        .with_source_error(error)
}

fn background_candidate_failed_outcome(
    kind: MaintenanceTaskKind,
    error: LifecycleError,
) -> MaintenanceOutcome {
    MaintenanceOutcome::new(kind, MaintenanceOutcomeStatus::Failed).with_source_error(error)
}

struct CacheFlushMaintenanceRunner<'a> {
    branch_catalog: &'a mut LifecycleBranchCatalog,
    budget: &'a StorageBudgetLedger,
}

impl MaintenanceTaskRunner for CacheFlushMaintenanceRunner<'_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let mut outcomes = Vec::new();
        for descriptor in flush_branch_descriptors(self.branch_catalog, task)? {
            let branch_id = descriptor.branch_id();
            let request = flush_drain_request_for_branch_from_maintenance_task(task, branch_id)?;
            let branch = self.branch_catalog.branch_state_mut(
                branch_id,
                CommitBranchGenerationGuard::exact(descriptor.generation()),
            )?;
            if branch.active_row_count() > 0 {
                require_rotate_budget(self.budget, branch)?;
                branch.rotate_active();
            }
            outcomes.push(flush_branch_drain_with(
                branch,
                &request,
                |branch, request| {
                    Ok(
                        flush_cache_branch_with_budget(branch, request, Some(self.budget), None)?
                            .maintenance_outcome(),
                    )
                },
            )?);
        }
        Ok(flush_drain_maintenance_outcome_for_scope(&outcomes))
    }
}

fn flush_branch_descriptors(
    branch_catalog: &LifecycleBranchCatalog,
    task: &MaintenanceTask,
) -> LifecycleResult<Vec<LifecycleBranchDescriptor>> {
    flush_branch_descriptors_for_scope(branch_catalog, task.scope())
}

fn flush_branch_descriptors_for_scope(
    branch_catalog: &LifecycleBranchCatalog,
    scope: MaintenanceTaskScope,
) -> LifecycleResult<Vec<LifecycleBranchDescriptor>> {
    match scope {
        MaintenanceTaskScope::Branch(branch_id) => Ok(vec![branch_catalog.lookup(branch_id)?]),
        MaintenanceTaskScope::Global => Ok(branch_catalog.list_branches(false)),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "flush task must target a branch or global scope",
        }),
    }
}

struct CacheCompactionMaintenanceRunner<'a> {
    branch: &'a mut BranchLocalState,
    compaction_io_policy: LifecycleCompactionIoPolicy,
    storage_budget: StorageRuntimeBudget,
}

impl MaintenanceTaskRunner for CacheCompactionMaintenanceRunner<'_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let Some(request) = current_compaction_request_from_maintenance_task_with_budget(
            task,
            self.branch,
            Some(self.storage_budget),
        )?
        else {
            return Ok(stale_compaction_maintenance_outcome());
        };
        if let Some(outcome) =
            defer_compaction_for_resource_policy(self.branch, &request, self.compaction_io_policy)?
        {
            return Ok(outcome);
        }
        let compaction =
            compact_cache_branch_with_budget(self.branch, &request, Some(self.storage_budget))?;
        record_lifecycle_compaction_outcome(&compaction);
        Ok(compaction.maintenance_outcome())
    }
}

struct CacheMaterializationMaintenanceRunner<'a> {
    branch: &'a mut BranchLocalState,
}

impl MaintenanceTaskRunner for CacheMaterializationMaintenanceRunner<'_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let request = materialization_request_from_maintenance_task(task)?;
        Ok(materialize_cache_branch(self.branch, &request)?.maintenance_outcome())
    }
}

impl<S> LifecycleCacheRuntime<S> {
    #[allow(
        dead_code,
        reason = "close integration consumes this drain/cancel hook"
    )]
    pub(crate) fn cancel_pending_maintenance_for_close(
        &mut self,
    ) -> LifecycleResult<MaintenanceCancelOutcome> {
        self.maintenance.cancel_pending_for_close(self.state)
    }

    #[cfg(test)]
    pub(crate) const fn guard_set(&self) -> &CommitBranchGuardSet {
        &self.guard_set
    }
}

impl<S> LifecycleCacheRuntime<S>
where
    S: CommitTimestampSource,
{
    pub(crate) fn execute_cache_commit(
        &mut self,
        batch: CommitBatch,
        generation_guard: CommitBranchGenerationGuard,
    ) -> LifecycleResult<CommitOutcome> {
        self.last_write_admission = None;
        require_admitted(self.state, LifecycleOperationKind::Commit)?;
        let branch_id = batch.branch_id();
        // Pre-sync shadow into catalog so the commit runtime sees any direct
        // shadow mutations (test-only) before fetching from the catalog.
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        Self::require_generation_guard(branch_id, generation, generation_guard)?;
        Self::require_cache_commit_mode(&batch)?;
        let frozen_before = self
            .branch_catalog
            .branch_state(branch_id)?
            .frozen_table_count();
        if batch.kind() == CommitBatchKind::Mutating {
            self.require_no_unresolved_durable_commit()?;
            self.require_branch_commit_guard_available(branch_id)?;
            self.evaluate_mutating_write_admission_for_branch(branch_id)?;
            self.require_projected_mutating_commit_budget(branch_id, &batch)?;
        }
        let outcome = {
            let (branch, registry) = self.branch_catalog.branch_state_mut_with_registry(
                branch_id,
                CommitBranchGenerationGuard::exact(generation),
            )?;
            let mut budgeted_branch = BudgetedCommitBranch::new(branch, &self.budget);
            CommitCacheRuntime::new(
                &self.commit_config,
                registry,
                &self.guard_set,
                &mut self.allocator,
                &mut budgeted_branch,
                &mut self.visible,
                &self.durable_gate,
            )
            .execute(batch, generation_guard)
            .map_err(commit_error)
        };
        if outcome.is_ok() {
            // Commit-triggered auto-rotation is a STRUCTURAL change: per the Model-2 contract
            // below, it must republish the snapshot in the same lock hold — the published
            // view's live-active handle now points at the rotated-out (frozen) table, so later
            // commits' rows in the fresh active would otherwise be invisible until the next
            // structural publish (which a pure-commit cache workload may never perform; #2538
            // lost every acknowledged row after the first rotation this way). Republish BEFORE
            // advancing the atomic mirror so any reader observing the new visible version finds
            // a covering snapshot (V-before-S).
            let rotated =
                self.republish_branch_snapshot_after_rotation(branch_id, frozen_before)?;
            if rotated {
                // #2541: cache mode has no background maintenance, so the committing
                // thread drains the frozen backlog inline — each rotation's sealed
                // memtable becomes an in-memory L0 table. Without this, frozen tables
                // accumulate until rotation stalls at max_frozen_tables (silently) or
                // the frozen-pool projection refuses commits. Best-effort by design:
                // the commit is already applied and visible, so a drain refusal must
                // not fail it — the backlog is retried at the next rotation.
                self.drain_frozen_inline(branch_id);
            }
            // BS2.2: mirror the just-advanced visible version to the atomic (release) so off-lock
            // readers (BS2.4) observe it without the runtime lock. On the `applied_not_visible`
            // error path `outcome` is `Err`, so the atomic correctly does not advance.
            self.visible_commit_version
                .store(self.visible.visible_version().as_u64(), Ordering::Release);
        }
        // BS2.4 Model 2: commits do NOT republish the snapshot (except the rotation case above).
        // The published snapshot holds the live (unpinned) active handle, so it already sees this
        // commit's appends; each off-lock read pins the active at read time and bounds by the
        // visible version. Only structural changes (rotation/flush/compaction/materialization/
        // fork/lifecycle) republish.
        if outcome.is_ok()
            && self
                .open_plan
                .lifecycle_policy()
                .may_schedule_post_commit_source_maintenance()
        {
            let _ = self.schedule_post_commit_maintenance_for_branch(branch_id);
        }
        outcome
    }
}

fn require_admitted(
    state: LifecycleStateMachine,
    operation: LifecycleOperationKind,
) -> LifecycleResult<()> {
    let admission = state.admit(operation);
    if admission.is_allowed() {
        Ok(())
    } else {
        Err(LifecycleError::InvalidLifecycleState {
            reason: admission
                .rejection_reason()
                .unwrap_or("operation is not admitted in current lifecycle state"),
        })
    }
}

fn cache_close_outcome(cancel: MaintenanceCancelOutcome, drained_tasks: usize) -> CloseOutcome {
    let maintenance_tasks = cancel.canceled_tasks().saturating_add(drained_tasks);
    CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Complete)
        .with_close_fact(LifecycleCloseFact::Complete)
        .with_close_effects(CloseOutcomeEffects::volatile_complete(false))
        .with_stats(LifecycleStats::new(1, 0, maintenance_tasks, 0, 1))
}

const fn cache_idempotent_close_outcome() -> CloseOutcome {
    CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Idempotent)
        .with_close_fact(LifecycleCloseFact::AlreadyClosed)
        .with_close_effects(CloseOutcomeEffects::volatile_complete(true))
        .with_stats(LifecycleStats::new(1, 0, 0, 0, 1))
}

/// Convert a prior first-close `CloseOutcome` into the idempotent retry
/// shape. The stats from the first close are preserved verbatim; the
/// effects are remapped to the volatile-complete shape with the
/// `prior_final` bit set so `CloseOutcome::validate` accepts the
/// `Idempotent` status. Status flips to `Idempotent` and the close fact
/// to `AlreadyClosed`. Callers observing the second outcome see the
/// same canceled/drained counts they saw on the first call.
fn idempotent_from_prior_close(prior: &CloseOutcome) -> CloseOutcome {
    CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Idempotent)
        .with_close_fact(LifecycleCloseFact::AlreadyClosed)
        .with_close_effects(CloseOutcomeEffects::volatile_complete(true))
        .with_stats(prior.stats())
}

const fn cache_supports_maintenance_kind(kind: MaintenanceTaskKind) -> bool {
    matches!(
        kind,
        MaintenanceTaskKind::Flush
            | MaintenanceTaskKind::Compaction
            | MaintenanceTaskKind::Materialization
            | MaintenanceTaskKind::HealthCollection
    )
}

fn bind_materialization_request_in_catalog(
    branch_catalog: &mut LifecycleBranchCatalog,
    request: MaintenanceTaskRequest,
) -> LifecycleResult<MaintenanceTaskRequest> {
    if request.kind() != MaintenanceTaskKind::Materialization
        || request.materialization_handle().is_some()
    {
        return Ok(request);
    }
    let branch_id = branch_id_from_inherited_layer_request(request)?;
    let generation = branch_catalog
        .registry()
        .lookup(branch_id)
        .map_err(commit_error)?
        .generation();
    let branch = branch_catalog
        .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
    bind_materialization_task_for_enqueue(branch, request)
}

const fn branch_id_from_table_level_task(task: MaintenanceTask) -> LifecycleResult<BranchId> {
    match task.scope() {
        MaintenanceTaskScope::TableLevel { branch_id, .. } => Ok(branch_id),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "compaction task must target a table level",
        }),
    }
}

const fn table_level_scope_from_task(task: MaintenanceTask) -> LifecycleResult<(BranchId, u8)> {
    match task.scope() {
        MaintenanceTaskScope::TableLevel { branch_id, level } => Ok((branch_id, level)),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "compaction task must target a table level",
        }),
    }
}

const fn branch_id_from_inherited_layer_task(task: MaintenanceTask) -> LifecycleResult<BranchId> {
    match task.scope() {
        MaintenanceTaskScope::InheritedLayer { branch_id, .. } => Ok(branch_id),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "materialization task must target an inherited layer",
        }),
    }
}

const fn branch_id_from_table_rewrite_task(task: MaintenanceTask) -> LifecycleResult<BranchId> {
    match task.scope() {
        MaintenanceTaskScope::TableLevel { branch_id, .. }
        | MaintenanceTaskScope::InheritedLayer { branch_id, .. } => Ok(branch_id),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "table rewrite task must target a branch table scope",
        }),
    }
}

const fn branch_id_from_inherited_layer_request(
    request: MaintenanceTaskRequest,
) -> LifecycleResult<BranchId> {
    match request.scope() {
        MaintenanceTaskScope::InheritedLayer { branch_id, .. } => Ok(branch_id),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "materialization task must target an inherited layer",
        }),
    }
}

fn branch_error(error: impl std::error::Error + Send + Sync + 'static) -> LifecycleError {
    LifecycleError::lower_layer_with(
        super::LifecycleLowerLayer::BranchRuntime,
        "branch runtime failed",
        error,
    )
}

fn commit_error(error: CommitRuntimeError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        super::LifecycleLowerLayer::CommitRuntime,
        "commit runtime failed",
        error,
    )
}

/// Identity inputs for one inline cache flush: the visible version and the
/// remaining frozen count make each request's table identity seed unique
/// within a branch's lifetime.
fn cache_inline_flush_request(
    branch_id: BranchId,
    visible_version: u64,
    frozen_remaining: usize,
) -> LifecycleResult<FlushFrozenRequest> {
    FlushFrozenRequest::new(
        branch_id,
        None,
        FlushTableIdentitySeed::new(format!(
            "cache-inline-flush-{branch_id}-v{visible_version}-f{frozen_remaining}"
        ))?,
        FlushTableObjectId::new(format!(
            "cache-inline-flush-{branch_id}-v{visible_version}-f{frozen_remaining}"
        ))?,
    )
}
