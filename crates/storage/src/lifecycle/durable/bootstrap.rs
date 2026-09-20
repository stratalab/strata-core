//! Commit-runtime bootstrap after durable recovery.

use super::{
    branch_error, commit_error, require_admitted, LifecycleDurableLocalServices,
    LifecycleDurableLocalShell,
};
use crate::branch::read::{BranchHistoryRow, BranchReadBound, BranchReadView, BranchScanBounds};
use crate::branch::snapshot::{BranchSnapshotPublisher, BranchSnapshotRegistry};
use crate::branch::state::BranchLocalState;
use crate::commit::{
    publish_commit_group, CommitBatch, CommitBatchKind, CommitBranchApplyTarget,
    CommitBranchGeneration, CommitBranchGenerationGuard, CommitBranchGuardSet,
    CommitDurabilityClass, CommitDurabilityMode, CommitDurableRuntime, CommitFactAllocator,
    CommitGroupState, CommitManualTimestampSource, CommitOutcome, CommitReplayAction,
    CommitReplayRequest, CommitReplayRuntime, CommitRuntimeError, CommitStamp,
    CommitTimestampSource, CommitUnresolvedDurable, CommitUnresolvedDurableGate,
    VisibleVersionPublish, VisibleVersionTracker,
};
use crate::config::mode::DurabilityPolicy;
use crate::format::WalRecord;
use crate::lifecycle::admission_ramp::{
    admission_mode_from_env, LifecycleAdmissionMode, WriteRateBucket,
};
use crate::lifecycle::background::{MaintenanceClock, RealMaintenanceClock};
use crate::lifecycle::flush::{
    flush_branch_drain_with, flush_drain_request_for_branch, flush_durable_branch_with_budget,
};
use crate::lifecycle::{
    branch_resident_bytes, compaction_lane_cap, estimate_commit_batch_active_bytes,
    maintenance_ready_for_recovery_health, projected_commit_rotation_would_exceed_frozen_budget,
    BudgetedCommitBranch, LifecycleBranchCatalog, LifecycleDurableTableCatalog, LifecycleError,
    LifecycleMaintenanceExecutor, LifecycleOperationKind, LifecycleRecoveryOutcome,
    LifecycleResult, LifecycleState, LifecycleStateMachine, LifecycleStats,
    LifecycleStoragePressureReason, LifecycleStoragePressureSeverity, LifecycleTransitionTrigger,
    LifecycleWalGrowthOutcome, LifecycleWalGrowthTrigger, LifecycleWriteAdmissionOutcome,
    RecoveryExclusivityToken, RecoveryHealth, RuntimeReadHandles, StorageBudgetLedger,
    StorageBudgetPressureSeverity, StorageBudgetSnapshot, StorageMode, StorageOpenOutcome,
    StorageOpenPlan,
};
use crate::observability::perf_trace;
use crate::row::PhysicalKey;
use crate::service::WalGrowthFacts;
use crate::service::{WalGroupSyncTicket, WalServiceError};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use strata_core::{BranchId, CommitVersion, Timestamp};

/// Cached database-manifest retention watermark (`max(snapshot_watermark,
/// flushed_through_commit_id)`), the value the per-commit growth/backpressure
/// checks need to derive `commits_since_checkpoint`. `Unknown` means it must be
/// refreshed from the manifest on the next read; `Known` is memoized until a
/// checkpoint/flush completion invalidates it. Avoids a manifest read per commit.
#[derive(Clone, Copy, Debug)]
pub(super) enum CachedRetentionWatermark {
    Unknown,
    Known(Option<CommitVersion>),
}

#[derive(Debug)]
pub(crate) struct LifecycleDurableLocalRuntime<'a, S = CommitManualTimestampSource> {
    pub(super) state: LifecycleStateMachine,
    pub(super) open_plan: StorageOpenPlan,
    pub(super) open_outcome: StorageOpenOutcome,
    pub(super) bootstrap_report: LifecycleRecoveryBootstrapReport,
    pub(super) services: LifecycleDurableLocalServices<'a>,
    pub(super) branch_catalog: LifecycleBranchCatalog,
    pub(super) initial_branch_id: BranchId,
    pub(super) guard_set: CommitBranchGuardSet,
    pub(super) allocator: CommitFactAllocator<S>,
    pub(super) visible: VisibleVersionTracker,
    /// BS2.2: release-published mirror of `visible.visible_version()` so a future off-lock reader
    /// (BS2.4) observes the visibility bound without the runtime lock. Stored under the lock on
    /// commit success; initial value tracks the recovered visible version (`0` == `CommitVersion::ZERO`).
    pub(super) visible_commit_version: Arc<AtomicU64>,
    /// BS2.3: per-branch published `Arc<BranchReadView>` snapshots. Published under the lock at the
    /// mutation sites; in BS2.3 read only by the debug equivalence oracle (reads still lock).
    pub(super) snapshot_publisher: BranchSnapshotPublisher,
    /// #2524: object names off-lock builds have published but not yet
    /// installed. The table-object mark pins these instead of deferring
    /// wholesale on `has_active_build_task` — reclaim stays live under
    /// sustained load.
    pub(super) inflight_outputs: super::inflight::InFlightTableOutputs,
    /// #2553: names an in-flight table-object sweep stage has frozen for
    /// deletion. Content-derived rewrite identities are deterministic across
    /// retries, so a re-planned pass can find its output already on disk (an
    /// orphan of an abandoned attempt) and ADOPT it — resurrecting a name the
    /// sweep already staged. Installs consult this registry (plus an
    /// existence probe) and defer instead of referencing a doomed object.
    pub(super) sweep_staged_names: super::inflight::InFlightTableOutputs,
    pub(super) durable_gate: CommitUnresolvedDurableGate,
    pub(super) commit_config: crate::commit::CommitRuntimeConfig,
    pub(super) table_catalog: LifecycleDurableTableCatalog,
    pub(super) budget: StorageBudgetLedger,
    pub(super) recovered_checkpoint_timestamp_max: Option<Timestamp>,
    pub(super) next_checkpoint_snapshot_id: u64,
    pub(super) current_recovery_health: RecoveryHealth,
    pub(super) last_wal_growth_outcome: Option<LifecycleWalGrowthOutcome>,
    pub(super) pressure_rejected_commit_branches: HashSet<BranchId>,
    pub(super) last_write_admission: Option<LifecycleWriteAdmissionOutcome>,
    /// BS5.2 pipeline frontier: the highest commit version APPLIED by an
    /// in-flight write group (its covering fsync may still be off-lock, its
    /// publish pending). Later admissions bound their conflict validation and
    /// branch-not-ahead checks at `max(visible, frontier)` — serial-equivalent
    /// semantics across the pipeline. Monotone; equals the visible version
    /// whenever no group is in flight.
    pub(super) pipeline_frontier: CommitVersion,
    // BS3.4b graded write-admission (the default since BS3.4c). The debt-adaptive write rate is
    // recomputed at structural-change events (`republish_all_branch_snapshots`, which both the inline
    // and background install paths converge on) and enforced per-commit by the token bucket;
    // `admission_clock` times both. `Cell` interior mutability keeps the commit/read paths `&self`,
    // exactly like `retention_watermark` — the runtime is `!Sync` behind the runtime mutex.
    pub(super) admission_mode: LifecycleAdmissionMode,
    pub(super) admission_clock: Arc<dyn MaintenanceClock>,
    /// Coverage hysteresis (see `schedule_background_maintenance_coverage`):
    /// the maintenance `completed` count at the last coverage attempt. `None`
    /// until the first attempt, so the first coverage always fires.
    pub(super) coverage_completed_watermark: Option<usize>,
    /// C2: resume point of an in-flight block-cache preheat pass, so chained
    /// chunks continue instead of re-walking. In-memory only — restart just
    /// restarts the fill (the reopen trigger re-arms it).
    pub(super) cache_preheat_cursor: Option<super::maintenance::CachePreheatCursor>,
    /// C3a: "one fresh pass owed" — armed by structural changes
    /// (flush/compaction installs, reopen, sweep reclaim). A flag rather
    /// than a standing queued task: the queue stays noise-free and a bool is
    /// the ultimate coalescer. Consumed ONLY when a fresh pass begins
    /// (cursor empty); an in-flight pass chains on the cursor, so a trigger
    /// landing mid-pass survives to start a follow-up pass that walks the
    /// fresh tables (C2's chunk-scoped consume ate those triggers — the
    /// ~15% never-walked block population behind the miss floor).
    pub(super) cache_preheat_rearm: bool,
    /// C3a: an in-flight pass is suspended (saturated shards or a deferral)
    /// awaiting the next trigger; the kept cursor resumes it.
    pub(super) cache_preheat_paused: bool,
    /// C3a: blocks covered so far by the pass in flight
    /// (admitted + present + rejects), accumulated across chunks; published
    /// as the coverage-numerator gauge when the pass completes.
    pub(super) cache_preheat_pass_blocks: u64,
    /// Test seam: shrink the per-chunk byte budget so multi-chunk passes are
    /// constructible on tiny fixtures.
    #[cfg(test)]
    pub(crate) cache_preheat_chunk_bytes_for_test: Option<u64>,
    pub(super) admission_current_rate: Cell<u64>,
    pub(super) admission_last_debt: Cell<u64>,
    pub(super) admission_bucket: Cell<WriteRateBucket>,
    // Cached manifest retention watermark for the per-commit growth/backpressure
    // checks. `Cell` keeps the read paths `&self` (no `&mut` cascade); invalidated
    // at checkpoint/flush completion and refreshed lazily. Interior mutability
    // makes the runtime `!Sync`, which is fine behind the runtime mutex (only
    // `Send` is required, as for the WAL service's `sealed_retention` cache).
    pub(super) retention_watermark: Cell<CachedRetentionWatermark>,
    // Released table references from `clear_branch`/`delete_branch` queue
    // here until the next retention pass drains them. In-memory only —
    // restart loses the buffer; durable persistence of release tombstones
    // is tracked in the closeout doc as a separate workstream.
    pub(super) pending_releases: Vec<crate::branch::facts::BranchReleasePlan>,
    // Branch catalog publish sequence counter. Increments on each
    // BranchCatalogManifest publication. Loaded from the manifest on
    // recovery so monotonicity holds across restarts.
    pub(super) branch_catalog_sequence: u64,
    // Pending-releases manifest publish sequence counter. Increments on
    // each PendingReleasesManifest publication (push by clear/delete or
    // drain by retention). Loaded from the manifest on recovery so the
    // sequence remains monotonic across restarts. Zero before the first
    // publication.
    pub(super) pending_releases_sequence: u64,
    // Per-branch publish slots. The off-lock publish phase performs the manifest fsync with the
    // global runtime lock released; this map serializes same-branch publishes so only one is ever
    // between sequence-reserve and record (no durable manifest-sequence regression). In-memory
    // only, populated lazily per branch. Held here rather than in the branch catalog because the
    // catalog is cloned during recovery/staging and the slots must be unique to the live runtime.
    pub(super) branch_publish_locks: HashMap<BranchId, Arc<AtomicBool>>,
    #[allow(
        dead_code,
        reason = "runtime hook is consumed by concrete maintenance modules"
    )]
    pub(super) maintenance: LifecycleMaintenanceExecutor,
    pub(super) maintenance_coverage_idle_rounds: usize,
    // Opaque close-session retry state owned by `lifecycle/durable/close.rs`.
    // Bootstrap stores the snapshot but does not interpret it; subsequent
    // idempotent close calls inside `close.rs` deconstruct it through
    // its own helpers. The wrapper exists so this file does not need
    // to reference any concrete close types, preserving the
    // bootstrap-vs-close layering enforced by the lifecycle source guard.
    pub(super) close_retry_state: Option<super::close::DurableCloseRetryState>,
}

/// RAII guard for a per-branch publish slot (see
/// [`LifecycleDurableLocalRuntime::try_acquire_branch_publish_guard`]). Held across the off-lock
/// manifest fsync so at most one publish per branch sits between sequence-reserve and record;
/// clears the per-branch flag on drop, including on early return or panic.
pub(super) struct BranchPublishGuard {
    flag: Arc<AtomicBool>,
}

impl Drop for BranchPublishGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleRecoveryBootstrapReport {
    records_seen: usize,
    records_applied: usize,
    records_already_applied: usize,
    rows_checked: usize,
    rows_applied: usize,
    gates_cleared: usize,
    checkpoint_visible_publish: Option<VisibleVersionPublish>,
    recovered_visible_version: CommitVersion,
    recovery_health: RecoveryHealth,
}

impl<'a, S> LifecycleDurableLocalShell<'a, S> {
    #[allow(
        clippy::too_many_lines,
        reason = "recovery assembly: replay report, open outcome, and runtime construction in one flow"
    )]
    pub(crate) fn complete_recovery(
        mut self,
        recovery: &LifecycleRecoveryOutcome,
    ) -> LifecycleResult<LifecycleDurableLocalRuntime<'a, S>> {
        let (
            report,
            branch_catalog,
            branch_catalog_sequence,
            pending_releases_sequence,
            pending_releases,
            initial_branch_id,
        ) = match self.prepare_catalog_and_replay(recovery) {
            Ok(values) => values,
            Err(error) => {
                self.mark_recovery_bootstrap_failed();
                return Err(error);
            }
        };
        let open_outcome = match StorageOpenOutcome::new(
            self.assembly_facts().mode(),
            self.assembly_facts().disposition(),
            Some(report.recovered_visible_version()),
            report.recovery_health().clone(),
            maintenance_ready_for_recovery_health(report.recovery_health()),
        ) {
            Ok(outcome) => outcome
                .with_backend_capabilities(self.services.capability_outcome().capabilities())
                .with_database_identity(
                    *self.assembly_facts().database_id(),
                    self.assembly_facts().codec_id().to_owned(),
                )
                .with_recovered_max_commit_version(Some(report.recovered_visible_version()))
                .with_durable_recovery_facts(recovery, &report)
                .with_budget_snapshot(self.budget.snapshot())
                .with_stats(LifecycleStats::new(
                    1,
                    recovery.health().fault_count(),
                    0,
                    0,
                    0,
                )),
            Err(error) => {
                self.mark_recovery_bootstrap_failed();
                return Err(error);
            }
        };
        if let Err(error) = self
            .state
            .transition(LifecycleTransitionTrigger::RecoveryAccepted)
        {
            self.mark_recovery_bootstrap_failed();
            return Err(error);
        }
        let max_maintenance_queue_depth = self
            .open_plan
            .lifecycle_config()
            .max_maintenance_queue_depth();
        let next_checkpoint_snapshot_id = self
            .assembly_facts()
            .manifest_snapshot_id()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(LifecycleError::CheckpointPublicationFailed {
                reason: "checkpoint snapshot id overflow",
            })?;
        // Seed the retention-watermark cache from the open-time manifest facts so
        // the first commit is a cache hit rather than a cold manifest read.
        let retention_watermark_seed = crate::lifecycle::wal_retention_watermark(
            self.assembly_facts()
                .manifest_snapshot_watermark()
                .map(CommitVersion::new),
            self.assembly_facts().manifest_flush_watermark(),
        );
        // BS3.4b: seed the graded-admission rate at the un-throttled ceiling and the token bucket at
        // the current clock. Production uses the real clock; tests swap in a manual clock via
        // `with_admission_clock_for_test`. `admission_mode` defaults to `Graded` (BS3.4c);
        // STRATA_ADMISSION=legacy selects the P-controller escape hatch.
        let admission_clock: Arc<dyn MaintenanceClock> = Arc::new(RealMaintenanceClock::new());
        let admission_initial_rate = self
            .open_plan
            .lifecycle_config()
            .write_throttle_policy()
            .max_rate_bytes_per_sec();
        let admission_now = admission_clock.now();
        let mut runtime = LifecycleDurableLocalRuntime {
            state: self.state,
            open_plan: self.open_plan,
            open_outcome,
            bootstrap_report: report,
            services: self.services,
            branch_catalog,
            initial_branch_id,
            guard_set: self.guard_set,
            allocator: self.allocator,
            visible: self.visible,
            visible_commit_version: visible_version_mirror(self.visible),
            snapshot_publisher: BranchSnapshotPublisher::new(),
            inflight_outputs: super::inflight::InFlightTableOutputs::default(),
            sweep_staged_names: super::inflight::InFlightTableOutputs::default(),
            durable_gate: self.durable_gate,
            commit_config: self.commit_config,
            table_catalog: self.table_catalog,
            budget: self.budget,
            recovered_checkpoint_timestamp_max: recovery.checkpoint().timestamp_max(),
            next_checkpoint_snapshot_id,
            current_recovery_health: recovery.health().clone(),
            last_wal_growth_outcome: None,
            pressure_rejected_commit_branches: HashSet::new(),
            last_write_admission: None,
            // Recovered state has no in-flight groups; the floor starts at the
            // recovered visible version via the max() in the floor read.
            pipeline_frontier: CommitVersion::ZERO,
            admission_mode: admission_mode_from_env(),
            coverage_completed_watermark: None,
            cache_preheat_cursor: None,
            cache_preheat_rearm: false,
            cache_preheat_paused: false,
            cache_preheat_pass_blocks: 0,
            #[cfg(test)]
            cache_preheat_chunk_bytes_for_test: None,
            admission_clock,
            admission_current_rate: Cell::new(admission_initial_rate),
            admission_last_debt: Cell::new(0),
            admission_bucket: Cell::new(WriteRateBucket::new(admission_now)),
            retention_watermark: Cell::new(CachedRetentionWatermark::Known(
                retention_watermark_seed,
            )),
            pending_releases,
            branch_catalog_sequence,
            pending_releases_sequence,
            branch_publish_locks: HashMap::new(),
            maintenance: Self::build_maintenance_executor(max_maintenance_queue_depth)?,
            maintenance_coverage_idle_rounds: 0,
            close_retry_state: None,
        };
        // BS2.3: seed a published snapshot for every recovered branch before any read observes it.
        runtime.republish_all_branch_snapshots();
        // Table-object GC reconcile on REOPEN only: one coalescing mark covers the whole prior
        // session's backlog (the mark lists the global inventory against every current manifest),
        // so stale objects from crashes or pre-GC sessions are reclaimed instead of persisting
        // forever. A freshly created database has no prior backlog to reconcile. Best-effort: a
        // rejected enqueue only defers reclaim to the next publish-driven cycle.
        if runtime.open_outcome.disposition()
            == crate::lifecycle::StorageOpenDisposition::OpenedExisting
        {
            let _ = runtime.enqueue_maintenance(
                crate::lifecycle::MaintenanceTaskRequest::table_object_retention(
                    runtime.initial_branch_id,
                ),
            );
            // C2: a reopen starts with a cold block cache and may never see a
            // publish (read-only workloads) — arm the preheat here so the
            // fill happens as soon as recovery-driven maintenance quiesces.
            if runtime.open_plan.lifecycle_config().cache_preheat_policy()
                == crate::lifecycle::LifecycleCachePreheatPolicy::WhenIdle
            {
                runtime.cache_preheat_rearm = true;
            }
        }
        Ok(runtime)
    }

    /// Build the maintenance executor with the durable runtime's Rewrite-lane concurrency cap
    /// (env-tunable during the perf sweep; see [`compaction_lane_cap`]).
    fn build_maintenance_executor(
        max_queue_depth: usize,
    ) -> LifecycleResult<LifecycleMaintenanceExecutor> {
        let mut maintenance = LifecycleMaintenanceExecutor::new(max_queue_depth)?;
        maintenance.set_rewrite_lane_cap(compaction_lane_cap());
        Ok(maintenance)
    }

    /// Build the runtime catalog, replay durable manifests, and dispatch
    /// WAL replay per branch. Runs inside `complete_recovery`; any error
    /// here triggers the bootstrap-failure transition in the caller.
    /// Shared prelude of both replay entry points: recovery-step admission,
    /// failed-package refusal, and the mode-derived durability facts.
    fn replay_preconditions(
        &self,
        recovery: &LifecycleRecoveryOutcome,
    ) -> LifecycleResult<(CommitDurabilityClass, CommitVersion)> {
        require_admitted(self.state, LifecycleOperationKind::RecoveryStep)?;
        if matches!(recovery.health(), RecoveryHealth::Failed { .. }) {
            return Err(LifecycleError::RecoveryFailed {
                reason: "failed recovery package cannot be opened",
            });
        }
        let durability = commit_durability_class_for_mode(self.assembly_facts().mode())?;
        let checkpoint_watermark = recovery
            .checkpoint()
            .trusted_watermark()
            .unwrap_or(CommitVersion::ZERO);
        Ok((durability, checkpoint_watermark))
    }

    fn prepare_catalog_and_replay(
        &mut self,
        recovery: &LifecycleRecoveryOutcome,
    ) -> LifecycleResult<(
        LifecycleRecoveryBootstrapReport,
        LifecycleBranchCatalog,
        u64,
        u64,
        Vec<crate::branch::facts::BranchReleasePlan>,
        BranchId,
    )> {
        let (durability, checkpoint_watermark) = self.replay_preconditions(recovery)?;

        // Build the catalog from the seeded branch's post-checkpoint state.
        let initial_branch_id = self.branch.branch_id();
        let branch_generation = self
            .registry
            .lookup(initial_branch_id)
            .map_err(commit_error)?
            .generation();
        // #2826: the seeded branch's `created_at` starts UNKNOWN (None), not
        // the restored max commit version — created_at is now the WAL-replay
        // generation fence, and stamping the rebuild watermark here would
        // fence the branch's own already-applied records. The durable
        // manifest's truthful value is adopted in
        // `replay_branch_catalog_manifest` before WAL replay runs.
        let mut branch_catalog =
            LifecycleBranchCatalog::with_existing_branch(&self.branch, branch_generation, None)?;
        // The shell's `self.branch` was cloned into the catalog above;
        // the catalog owns the canonical state from here on. The shell's
        // copy is dropped together with `self` when `complete_recovery`
        // returns.

        // Replay the durable BranchCatalogManifest if present so non-seeded
        // descriptors survive restart. A missing manifest means the database
        // never published a multi-branch catalog; the seeded branch is the
        // only catalog entry.
        let branch_catalog_sequence = match self
            .services
            .branch_catalog_manifest()
            .load_current()
            .map_err(branch_catalog_manifest_service_error)?
        {
            Some(manifest) => {
                replay_branch_catalog_manifest(&mut branch_catalog, initial_branch_id, &manifest)?;
                manifest.manifest_sequence()
            }
            None => 0,
        };

        // Reload the durable PendingReleasesManifest if present. Each
        // entry is converted back to a release plan carrying the
        // persisted releasable-table list; protected_tables and
        // removed_refs stay empty (the next retention pass recomputes
        // reachability from the manifest).
        let (pending_releases, pending_releases_sequence) = match self
            .services
            .pending_releases_manifest()
            .load_current()
            .map_err(pending_releases_manifest_service_error)?
        {
            Some(manifest) => {
                let mut plans = Vec::with_capacity(manifest.entries().len());
                for entry in manifest.entries() {
                    let identities = entry
                        .released_tables()
                        .iter()
                        .map(|identity| {
                            crate::table::TableIdentity::new(identity.clone()).map_err(|source| {
                                LifecycleError::lower_layer_with(
                                    crate::lifecycle::LifecycleLowerLayer::Format,
                                    "pending releases manifest table identity invalid",
                                    source,
                                )
                            })
                        })
                        .collect::<LifecycleResult<Vec<_>>>()?;
                    plans.push(
                        crate::branch::facts::BranchReleasePlan::from_releasable_tables(
                            entry.branch_id(),
                            identities,
                        ),
                    );
                }
                (plans, manifest.manifest_sequence())
            }
            None => (Vec::new(), 0),
        };

        // Install per-branch durable table manifests into non-seeded slots.
        // The seeded branch's manifest was already applied by the pre-catalog
        // recovery phase (apply_table_manifest_recovery on the shell).
        recover_per_branch_table_manifests(
            &self.services,
            &mut self.table_catalog,
            &mut branch_catalog,
            initial_branch_id,
            Some(&self.budget),
        )?;

        // Install non-seeded checkpoint rows and seed non-seeded timeline
        // indexes (seeded-branch state was handled by `recover_checkpoint`).
        install_non_seeded_checkpoint_state(
            &mut branch_catalog,
            recovery.checkpoint(),
            initial_branch_id,
        )?;

        // Dispatch WAL replay by branch_id into per-branch catalog slots,
        // streaming the tail and validating each record in the same pass
        // (multi-branch aware; formerly `validate_recovered_wal_package`).
        let mut flush_context = ReplayFlushContext {
            table_object: self.services.table_object(),
            table_reader: self.services.table_reader(),
            table_catalog: &mut self.table_catalog,
            budget: &self.budget,
            data_block_bytes: self.open_plan.lifecycle_config().data_block_bytes(),
            table_compression: self.open_plan.lifecycle_config().table_compression(),
        };
        let report = replay_wal_into_catalog(
            self.services.wal(),
            &mut flush_context,
            &mut branch_catalog,
            &self.commit_config,
            &mut self.allocator,
            &mut self.visible,
            &self.durable_gate,
            recovery,
            durability,
            checkpoint_watermark,
        )?;
        rebuild_fork_snapshot_rows(&mut branch_catalog)?;
        complete_forked_branch_timelines_after_replay(&branch_catalog)?;
        Ok((
            report,
            branch_catalog,
            branch_catalog_sequence,
            pending_releases_sequence,
            pending_releases,
            initial_branch_id,
        ))
    }

    /// Test-only entry point that mirrors the production validation +
    /// replay path against a temporary catalog seeded from the shell's
    /// branch. Used to exercise per-record replay failures (e.g.
    /// unresolved-gate mismatches) without consuming the shell.
    #[cfg(test)]
    pub(crate) fn try_bootstrap_commit_runtime_for_test(
        &mut self,
        recovery: &LifecycleRecoveryOutcome,
    ) -> LifecycleResult<LifecycleRecoveryBootstrapReport> {
        match self.try_bootstrap_commit_runtime_for_test_inner(recovery) {
            Ok(report) => Ok(report),
            Err(error) => {
                self.mark_recovery_bootstrap_failed();
                Err(error)
            }
        }
    }

    #[cfg(test)]
    fn try_bootstrap_commit_runtime_for_test_inner(
        &mut self,
        recovery: &LifecycleRecoveryOutcome,
    ) -> LifecycleResult<LifecycleRecoveryBootstrapReport> {
        let (durability, checkpoint_watermark) = self.replay_preconditions(recovery)?;
        let branch_generation = self
            .registry
            .lookup(self.branch.branch_id())
            .map_err(commit_error)?
            .generation();
        // #2826: None for the same reason as the primary assembly path —
        // created_at now feeds the replay generation fence.
        let mut branch_catalog =
            LifecycleBranchCatalog::with_existing_branch(&self.branch, branch_generation, None)?;
        let mut flush_context = ReplayFlushContext {
            table_object: self.services.table_object(),
            table_reader: self.services.table_reader(),
            table_catalog: &mut self.table_catalog,
            budget: &self.budget,
            data_block_bytes: self.open_plan.lifecycle_config().data_block_bytes(),
            table_compression: self.open_plan.lifecycle_config().table_compression(),
        };
        replay_wal_into_catalog(
            self.services.wal(),
            &mut flush_context,
            &mut branch_catalog,
            &self.commit_config,
            &mut self.allocator,
            &mut self.visible,
            &self.durable_gate,
            recovery,
            durability,
            checkpoint_watermark,
        )
    }

    fn mark_recovery_bootstrap_failed(&mut self) {
        // The original bootstrap error is already being returned; this best-effort
        // transition only preserves the state-machine terminal fact.
        let _ = self
            .state
            .transition(LifecycleTransitionTrigger::PhaseFailed {
                reason: "recovery bootstrap failed",
            });
    }
}

impl<S> RuntimeReadHandles for LifecycleDurableLocalRuntime<'_, S> {
    fn visible_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.visible_commit_version)
    }

    fn snapshot_registry(&self) -> Arc<BranchSnapshotRegistry> {
        self.snapshot_publisher.registry_handle()
    }
}

impl<S> LifecycleDurableLocalRuntime<'_, S> {
    /// Try to claim the per-branch publish slot for the off-lock publish phase. Returns the guard
    /// when no other publish is in flight for this branch; `None` (the caller should defer the
    /// task) when one already holds it. Claimed under the global runtime lock and held across the
    /// lock release during the off-lock fsync — the global lock is never blocked on this slot, so
    /// the global→per-branch acquisition order cannot deadlock.
    pub(super) fn try_acquire_branch_publish_guard(
        &mut self,
        branch_id: BranchId,
    ) -> Option<BranchPublishGuard> {
        let flag = self
            .branch_publish_locks
            .entry(branch_id)
            .or_insert_with(|| Arc::new(AtomicBool::new(false)))
            .clone();
        match flag.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed) {
            Ok(_) => Some(BranchPublishGuard { flag }),
            Err(_) => None,
        }
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

    #[allow(
        dead_code,
        reason = "durable budget facts are consumed by integration and closeout slices"
    )]
    pub(crate) fn budget_snapshot(&self) -> StorageBudgetSnapshot {
        // Refresh the database-wide total so diagnostics report a current global figure alongside
        // the per-pool snapshot.
        self.refresh_runtime_memory_total();
        let branch = self
            .branch_catalog
            .branch_state(self.initial_branch_id)
            .expect("seeded branch is always present in the catalog");
        crate::lifecycle::snapshot_with_runtime_usage(
            &self.budget,
            branch,
            self.maintenance.status(),
        )
    }

    pub(crate) fn budget_total_used_bytes(&self) -> u64 {
        self.budget.total_used_bytes()
    }

    /// The isolated database-wide runtime memory total (branch resident bytes + block cache),
    /// without the in-flight ledger pool reservations that `budget_total_used_bytes` adds.
    /// Test-only: lets the budget drift test assert the published total against an independent
    /// full fold.
    #[cfg(test)]
    pub(crate) fn runtime_total_bytes(&self) -> u64 {
        self.budget.runtime_total_bytes()
    }

    pub(crate) fn budget_global_pressure(&self) -> StorageBudgetPressureSeverity {
        self.budget.global_pressure()
    }

    pub(crate) const fn bootstrap_report(&self) -> &LifecycleRecoveryBootstrapReport {
        &self.bootstrap_report
    }

    #[allow(
        dead_code,
        reason = "exposed for runtime tests; first non-test caller lands with the public storage api"
    )]
    pub(crate) fn pending_releases(&self) -> &[crate::branch::facts::BranchReleasePlan] {
        &self.pending_releases
    }

    pub(crate) const fn current_recovery_health(&self) -> &RecoveryHealth {
        &self.current_recovery_health
    }

    pub(crate) const fn last_wal_growth_outcome(&self) -> Option<&LifecycleWalGrowthOutcome> {
        self.last_wal_growth_outcome.as_ref()
    }

    pub(crate) fn current_wal_growth_facts(&self) -> LifecycleResult<WalGrowthFacts> {
        self.services.wal().growth_facts().map_err(super::wal_error)
    }

    /// Returns the cached manifest retention watermark, refreshing it from the
    /// manifest only when a checkpoint/flush has invalidated it. This is the sole
    /// manifest read left on the per-commit growth/backpressure path; steady-state
    /// commits hit the memoized value.
    pub(crate) fn cached_retention_watermark(&self) -> LifecycleResult<Option<CommitVersion>> {
        if let CachedRetentionWatermark::Known(watermark) = self.retention_watermark.get() {
            return Ok(watermark);
        }
        let manifest_start = perf_trace::start_timer();
        let manifest_result = self.services.manifest().load_required();
        perf_trace::record_commit_wal_growth_manifest_elapsed(manifest_start);
        let current_manifest = manifest_result.map_err(super::manifest_error)?;
        let watermark = crate::lifecycle::wal_retention_watermark(
            current_manifest
                .snapshot_watermark()
                .map(CommitVersion::new),
            current_manifest.flushed_through_commit_id(),
        );
        self.retention_watermark
            .set(CachedRetentionWatermark::Known(watermark));
        Ok(watermark)
    }

    /// Invalidates the cached retention watermark so the next growth/backpressure
    /// check re-reads it from the manifest. Called at every checkpoint/flush
    /// completion — the only operations that advance the watermark.
    pub(crate) fn invalidate_retention_watermark_cache(&self) {
        self.retention_watermark
            .set(CachedRetentionWatermark::Unknown);
    }

    pub(crate) fn current_wal_growth_backpressure_snapshot(
        &self,
    ) -> LifecycleResult<(WalGrowthFacts, u64, Option<LifecycleWalGrowthTrigger>)> {
        let policy = self.open_plan.lifecycle_config().wal_growth_policy();
        let facts = self.current_wal_growth_facts()?;
        let commits_since_checkpoint = crate::lifecycle::commits_since_checkpoint(
            self.visible.visible_version(),
            self.cached_retention_watermark()?,
        );
        let trigger = policy.backpressure_trigger_for(facts, commits_since_checkpoint);
        Ok((facts, commits_since_checkpoint, trigger))
    }

    /// Whether retained WAL has exceeded the hard disk-safety cap (`max_total_wal_bytes`).
    /// When true the foreground WAL pacing must not give up — the WAL must never keep growing
    /// past this ceiling — so the writer waits on background reclaim until it drops back below.
    pub(crate) fn current_wal_growth_exceeds_hard_cap(&self) -> LifecycleResult<bool> {
        let policy = self.open_plan.lifecycle_config().wal_growth_policy();
        Ok(policy.hard_cap_exceeded(self.current_wal_growth_facts()?))
    }

    pub(crate) const fn last_write_admission(&self) -> Option<LifecycleWriteAdmissionOutcome> {
        self.last_write_admission
    }

    /// BS3.4b: whether the durable runtime is on the graded (debt-adaptive rate) admission path.
    /// The commit path branches on this to pick the token-bucket delay vs. the legacy quadratic.
    pub(crate) fn is_graded_admission(&self) -> bool {
        self.admission_mode == LifecycleAdmissionMode::Graded
    }

    /// BS3.4b (graded): recompute the debt-adaptive write rate at an install event. The rate is
    /// updated *only* here (event cadence — the `RocksDB` `InstallSuperVersion` analog); the commit
    /// path just reads it. The single global rate paces the writer to the most-behind branch (max
    /// compaction debt). No-op on the legacy path.
    pub(crate) fn recompute_admission_rate(&self) {
        if self.admission_mode != LifecycleAdmissionMode::Graded {
            return;
        }
        let mut worst_debt = 0u64;
        let mut worst_l0 = 0usize;
        // W1.4b: nonzero-level overage below budget/8 is structural (shape
        // converging at compaction's pace) and never paces writers; only the
        // excess beyond it joins L0 bytes in the pacing signal. Budget-
        // relative so a 512MB device and a 32GB server brake proportionally.
        let soft_nonzero_debt_limit = self
            .open_plan
            .lifecycle_config()
            .storage_budget()
            .total_bytes()
            / 8;
        for descriptor in self.branch_catalog.list_branches(false) {
            let Ok(branch) = self.branch_catalog.branch_state(descriptor.branch_id()) else {
                continue;
            };
            let per_level = branch.per_level_bytes();
            let targets =
                crate::lifecycle::compaction::nonzero_level_targets_from_level_bytes(per_level);
            let debt = crate::lifecycle::admission_ramp::pacing_debt(
                per_level,
                &targets,
                soft_nonzero_debt_limit,
            );
            let l0 = branch.owned_levels().first().map_or(0, Vec::len);
            if debt > worst_debt || (debt == worst_debt && l0 > worst_l0) {
                worst_debt = debt;
                worst_l0 = l0;
            }
        }
        // W1.4b: the rate is a pure function of L0 depth within the band
        // (proportional-quadratic; admission_ramp::next_write_rate). Byte debt
        // is telemetry only — structural overage cannot pin the rate low.
        let policy = self.open_plan.lifecycle_config().write_throttle_policy();
        let new_rate = crate::lifecycle::admission_ramp::next_write_rate(
            worst_l0,
            crate::lifecycle::compaction::level_zero_urgent_threshold(),
            crate::lifecycle::compaction::level_zero_blocking_threshold(),
            policy.max_rate_bytes_per_sec(),
            policy.min_rate_bytes_per_sec(),
        );
        self.admission_current_rate.set(new_rate);
        self.admission_last_debt.set(worst_debt);
        perf_trace::record_lifecycle_admission_rate_recompute(
            new_rate,
            worst_debt,
            new_rate <= policy.min_rate_bytes_per_sec(),
        );
    }

    /// BS3.4b (graded): the per-commit token-bucket delay in millis. `0` on the legacy path, and `0`
    /// while the rate sits at the un-throttled ceiling (outside the delay band); otherwise the token
    /// bucket paces this commit to the current rate.
    pub(crate) fn graded_write_throttle_delay_millis(&self, batch_bytes: u64) -> u64 {
        if self.admission_mode != LifecycleAdmissionMode::Graded {
            return 0;
        }
        let policy = self.open_plan.lifecycle_config().write_throttle_policy();
        let rate = self.admission_current_rate.get();
        if rate >= policy.max_rate_bytes_per_sec() {
            return 0;
        }
        let mut bucket = self.admission_bucket.get();
        let delay = bucket.charge(batch_bytes, rate, self.admission_clock.now());
        self.admission_bucket.set(bucket);
        // Cap a single commit's sleep: at the near-stop floor rate a large batch would otherwise pace
        // for seconds (batch_bytes / 16 KiB/s). Beyond the cap, admit the write — it grows L0 toward
        // the hard stop, which then rejects with a bounded retry-wait instead of a multi-second sleep.
        u64::try_from(delay.as_millis())
            .unwrap_or(u64::MAX)
            .min(policy.max_graded_delay_millis())
    }

    #[cfg(test)]
    pub(crate) fn with_admission_mode_for_test(&mut self, mode: LifecycleAdmissionMode) {
        self.admission_mode = mode;
    }

    #[cfg(test)]
    pub(crate) fn admission_current_rate_for_test(&self) -> u64 {
        self.admission_current_rate.get()
    }

    #[cfg(test)]
    pub(crate) fn with_admission_clock_for_test(&mut self, clock: Arc<dyn MaintenanceClock>) {
        let now = clock.now();
        self.admission_clock = clock;
        self.admission_bucket.set(WriteRateBucket::new(now));
        self.admission_current_rate.set(
            self.open_plan
                .lifecycle_config()
                .write_throttle_policy()
                .max_rate_bytes_per_sec(),
        );
        self.admission_last_debt.set(0);
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

    fn require_durable_commit_mode(batch: &CommitBatch) -> LifecycleResult<()> {
        if batch.kind() == CommitBatchKind::Mutating
            && batch.options().durability() == CommitDurabilityMode::Cache
        {
            return Err(commit_error(CommitRuntimeError::DurabilityUnavailable {
                reason: "durable commit executor requires durable mode",
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

    fn require_write_admission_recovery_health(&self, branch_id: BranchId) -> LifecycleResult<()> {
        if maintenance_ready_for_recovery_health(&self.current_recovery_health) {
            return Ok(());
        }
        Err(LifecycleError::StoragePressureRejected {
            branch_id,
            severity: LifecycleStoragePressureSeverity::BlockMutatingAdmission,
            pressure_reason: LifecycleStoragePressureReason::None,
            retryable: false,
            reason: "durable recovery health blocks mutating commit admission",
        })
    }

    fn require_projected_mutating_commit_budget(
        &self,
        branch_id: BranchId,
        batch: &CommitBatch,
    ) -> LifecycleResult<()> {
        let incoming_active_bytes = estimate_commit_batch_active_bytes(batch)?;
        // BS4.5a: database-wide memory budget as an *observability gauge*, not an admission failure.
        // After the disk-resident flip (BS4.4j/4.4l), durable tables hold lazy readers whose residency
        // is metadata-only, so a durable dataset is no longer bounded by RAM — the old hard reject
        // predated lazy block reads and only made sense while whole-object readers materialized every
        // table. `refresh_runtime_memory_total` records the measured residency (readable via
        // diagnostics); when it exceeds the budget we count the over-budget admission so health
        // monitoring can WARN, then admit. Cache mode keeps its hard reject (constraint C2), and
        // memtable/frozen pressure is still enforced by the rotation check below.
        self.refresh_runtime_memory_total();
        if self.budget.would_exceed_total(incoming_active_bytes) {
            perf_trace::record_durable_commit_admitted_over_budget();
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

    /// Recompute and record the database-wide runtime memory total: the resident bytes of every
    /// active branch (memtables plus owned-table readers) plus the block cache. The
    /// commit-admission path and diagnostics read this so a per-branch check can reason about the
    /// whole database against `budget.total_bytes()`.
    pub(crate) fn refresh_runtime_memory_total(&self) {
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
        let cache = self.services.table_object().block_cache_resident_bytes();
        self.budget
            .set_runtime_total_bytes(resident.saturating_add(cache));
    }

    /// Drop every cached table block so the next lazy read is forced to the backend. Lets a test
    /// exercise the cold-read failure path (#3047) after removing an object under a live reader.
    #[cfg(all(test, feature = "localfs"))]
    pub(crate) fn clear_block_cache_for_test(&self) {
        if let Some(cache) = self.services.table_object().block_cache() {
            cache.clear();
        }
    }

    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn force_close_requested_for_test(&mut self) -> LifecycleResult<()> {
        self.state
            .transition(LifecycleTransitionTrigger::CloseRequested)?;
        Ok(())
    }

    pub(crate) const fn services(&self) -> &LifecycleDurableLocalServices<'_> {
        &self.services
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

    #[allow(
        dead_code,
        reason = "branch lifecycle API uses this runtime catalog surface when public wrappers land"
    )]
    pub(crate) const fn branch_catalog(&self) -> &LifecycleBranchCatalog {
        &self.branch_catalog
    }

    #[cfg(test)]
    pub(crate) fn branch_catalog_mut_for_test(&mut self) -> &mut LifecycleBranchCatalog {
        &mut self.branch_catalog
    }

    /// Test-only mutable accessor; delegates to the catalog's
    /// `branch_state_mut` with the seeded branch's current generation.
    /// Each call advances the catalog's `state_revision` counter.
    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn branch_state_mut(&mut self) -> &mut BranchLocalState {
        let branch_id = self.initial_branch_id;
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .expect("seeded branch is always registered")
            .generation();
        self.branch_catalog
            .branch_state_mut(
                branch_id,
                crate::commit::CommitBranchGenerationGuard::exact(generation),
            )
            .expect("seeded branch is always present in the catalog")
    }

    #[allow(
        dead_code,
        reason = "durable table catalog is asserted by recovery tests"
    )]
    pub(crate) const fn table_catalog(&self) -> &LifecycleDurableTableCatalog {
        &self.table_catalog
    }

    #[cfg(test)]
    pub(crate) const fn guard_set(&self) -> &CommitBranchGuardSet {
        &self.guard_set
    }

    #[cfg(test)]
    pub(crate) const fn durable_gate(&self) -> &CommitUnresolvedDurableGate {
        &self.durable_gate
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

    pub(crate) const fn allocator(&self) -> &CommitFactAllocator<S> {
        &self.allocator
    }

    pub(crate) fn unresolved_durable(&self) -> LifecycleResult<Option<CommitUnresolvedDurable>> {
        self.durable_gate.unresolved().map_err(commit_error)
    }

    /// BS2.3: capture and publish a fresh snapshot for `branch_id` (under the runtime lock). The
    /// capture is O(#tables) refcount-bumps (BS2.1); on capture error — infallible on a well-formed
    /// post-mutation state — the prior snapshot is kept. Called at every branch mutation site.
    pub(super) fn publish_branch_snapshot(&mut self, branch_id: BranchId) {
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
    pub(super) fn republish_all_branch_snapshots(&mut self) {
        for descriptor in self.branch_catalog.list_branches(false) {
            self.publish_branch_snapshot(descriptor.branch_id());
        }
        // BS3.4b: every structural change (rotation / flush install / compaction install) republishes
        // here — the RocksDB `InstallSuperVersion` analog and the event-cadence point where the
        // graded-admission write rate is recomputed from the settled branch shapes. A flush that grew
        // L0 raises debt (rate down); a compaction that drained it lowers debt (rate recovers). No-op
        // on the legacy path.
        self.recompute_admission_rate();
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

    /// Storage-internal: create a new branch in the catalog. The new
    /// branch is visible via `list_branches` and the catalog accessor and
    /// can accept commits routed through the catalog's per-branch slot.
    /// `publish_branch_catalog` writes the updated descriptor list to
    /// `manifest/branch-catalog` so the entry survives restart.
    pub(crate) fn create_branch(
        &mut self,
        branch_id: BranchId,
        generation: CommitBranchGeneration,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<crate::lifecycle::LifecycleBranchCreateOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let outcome = self
            .branch_catalog
            .create_branch(branch_id, generation, created_at)?;
        self.publish_branch_catalog()?;
        self.republish_all_branch_snapshots(); // BS2.3: publish the new/forked branch snapshot.
        Ok(outcome)
    }

    pub(crate) fn list_branches(
        &self,
        include_deleted: bool,
    ) -> Vec<crate::lifecycle::LifecycleBranchDescriptor> {
        self.branch_catalog.list_branches(include_deleted)
    }

    #[allow(
        dead_code,
        reason = "fork-at-history surface exposed for durable callers"
    )]
    /// Publish the forked child's table manifest at fork time, durably recording its COW
    /// inherited-layer references. This is what makes the fork's parent-table references visible
    /// to manifest-based recovery (O(tables) reopen instead of the O(parent dataset)
    /// `rebuild_fork_snapshot_rows` re-materialization) and to table-object reachability (the
    /// child's references pin shared parent objects against GC across reopen — the COW invariant:
    /// an object is deletable only when unreachable from *every* branch's durable manifest).
    ///
    /// Skipped for eager (layer-less) children: their fork rows live only in the child memtable,
    /// so an empty manifest would make recovery treat the child as durably covered and skip the
    /// rebuild fallback — data loss on crash. Publish failure does not fail the fork (the catalog
    /// is already durable); it records health debt, and the child recovers through the gated
    /// rebuild fallback instead.
    fn publish_fork_child_table_manifest(
        &mut self,
        outcome: &crate::lifecycle::LifecycleBranchForkOutcome,
    ) {
        // #2833: the layer-less fork paths (eager/materialized fork-of-a-fork
        // and empty forks) own no durable tables — publishing their manifest
        // is impossible (the child holds a volatile materialized table with
        // no durable catalog entry, which the manifest builder rightly
        // refuses). But silently returning left a DELETED predecessor
        // generation's manifest on disk as the re-created name's recovery
        // provenance: fork children are #2830-fence-exempt by design and the
        // #2820 rebuild skips children that hold a layer, so recovery served
        // the DEAD generation's lineage. REMOVE the stale artifact instead:
        // an absent manifest restores the layer-less shape the fork rebuild
        // expects. Best-effort like the publish arm — on failure recovery
        // health carries the debt.
        if outcome.inherited_layer_count() == 0 {
            let remove_result = self
                .services
                .table_manifest()
                .remove_manifest(outcome.descriptor().branch_id());
            if remove_result.is_err() {
                if let Ok(health) = crate::lifecycle::telemetry_health_debt(
                    "fork child stale table manifest removal failed; recovery may serve a \
                     dead generation's provenance",
                ) {
                    self.record_recovery_health(Some(&health));
                }
            }
            return;
        }
        let publish_result = self
            .branch_catalog
            .branch_state(outcome.descriptor().branch_id())
            .map_err(branch_error)
            .and_then(|child| {
                crate::lifecycle::table_manifest::publish_table_manifest_for_branch_with_budget(
                    child,
                    self.services.table_manifest(),
                    &mut self.table_catalog,
                    Some(&self.budget),
                )
                .map(|_| ())
            });
        if publish_result.is_err() {
            if let Ok(health) = crate::lifecycle::telemetry_health_debt(
                "fork child table manifest publish failed; recovery falls back to fork rebuild",
            ) {
                self.record_recovery_health(Some(&health));
            }
        }
    }

    pub(crate) fn fork_current(
        &mut self,
        source: BranchId,
        destination: BranchId,
        destination_generation: CommitBranchGeneration,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<crate::lifecycle::LifecycleBranchForkOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let _quiesce = self.guard_set.try_begin_quiesce().map_err(commit_error)?;
        let outcome = self.branch_catalog.fork_current(
            source,
            destination,
            destination_generation,
            created_at,
        )?;
        self.publish_branch_catalog()?;
        self.publish_fork_child_table_manifest(&outcome);
        self.republish_all_branch_snapshots(); // BS2.3: publish the new/forked branch snapshot.
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "fork-at-history surface exposed for durable callers"
    )]
    pub(crate) fn fork_at_retained_version(
        &mut self,
        source: BranchId,
        destination: BranchId,
        destination_generation: CommitBranchGeneration,
        fork_version: CommitVersion,
        retained_floor: CommitVersion,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<crate::lifecycle::LifecycleBranchForkOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let _quiesce = self.guard_set.try_begin_quiesce().map_err(commit_error)?;
        let mut published_slice = None;
        let outcome = {
            let services = &self.services;
            let budget = &self.budget;
            let data_block_bytes = self.open_plan.lifecycle_config().data_block_bytes();
            let compression = self.open_plan.lifecycle_config().table_compression();
            let published_slice = &mut published_slice;
            let mut builder = |child: BranchId,
                               fork_version: CommitVersion,
                               rows: Vec<crate::row::StorageRow>| {
                build_and_publish_fork_unsealed_table(
                    services,
                    budget,
                    data_block_bytes,
                    compression,
                    child,
                    fork_version,
                    &rows,
                    published_slice,
                )
            };
            // #2855: COW only over durably cataloged tables — a volatile
            // layer target guarantees the child-manifest publish fails.
            let table_catalog = &self.table_catalog;
            let is_durable = |identity: &crate::table::TableIdentity| {
                table_catalog.object_for_identity(identity).is_some()
            };
            self.branch_catalog
                .fork_at_retained_version_with_unsealed_builder(
                    source,
                    destination,
                    destination_generation,
                    fork_version,
                    retained_floor,
                    created_at,
                    Some(&mut builder),
                    Some(&is_durable),
                )?
        };
        self.record_fork_unsealed_slice(published_slice);
        self.publish_branch_catalog()?;
        self.publish_fork_child_table_manifest(&outcome);
        self.republish_all_branch_snapshots(); // BS2.3: publish the new/forked branch snapshot.
        Ok(outcome)
    }

    pub(crate) fn fork_at_retained_timestamp(
        &mut self,
        source: BranchId,
        destination: BranchId,
        destination_generation: CommitBranchGeneration,
        timestamp: Timestamp,
        retained_floor: CommitVersion,
        created_at: Option<CommitVersion>,
    ) -> LifecycleResult<crate::lifecycle::LifecycleBranchForkOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let _quiesce = self.guard_set.try_begin_quiesce().map_err(commit_error)?;
        let mut published_slice = None;
        let outcome = {
            let services = &self.services;
            let budget = &self.budget;
            let data_block_bytes = self.open_plan.lifecycle_config().data_block_bytes();
            let compression = self.open_plan.lifecycle_config().table_compression();
            let published_slice = &mut published_slice;
            let mut builder = |child: BranchId,
                               fork_version: CommitVersion,
                               rows: Vec<crate::row::StorageRow>| {
                build_and_publish_fork_unsealed_table(
                    services,
                    budget,
                    data_block_bytes,
                    compression,
                    child,
                    fork_version,
                    &rows,
                    published_slice,
                )
            };
            // #2855: COW only over durably cataloged tables — a volatile
            // layer target guarantees the child-manifest publish fails.
            let table_catalog = &self.table_catalog;
            let is_durable = |identity: &crate::table::TableIdentity| {
                table_catalog.object_for_identity(identity).is_some()
            };
            self.branch_catalog
                .fork_at_retained_timestamp_with_unsealed_builder(
                    source,
                    destination,
                    destination_generation,
                    timestamp,
                    retained_floor,
                    created_at,
                    Some(&mut builder),
                    Some(&is_durable),
                )?
        };
        self.record_fork_unsealed_slice(published_slice);
        self.publish_branch_catalog()?;
        self.publish_fork_child_table_manifest(&outcome);
        self.republish_all_branch_snapshots(); // BS2.3: publish the new/forked branch snapshot.
        Ok(outcome)
    }

    /// #2527: record the fork's published unsealed-slice table in the
    /// durable table catalog BEFORE the child manifest publish references
    /// it. Best-effort like the manifest itself: on failure the recovered
    /// child is layer-less and the recovery rebuild re-materializes it.
    fn record_fork_unsealed_slice(
        &mut self,
        published: Option<(
            crate::table::TableIdentity,
            crate::service::TableObjectFacts,
        )>,
    ) {
        let Some((identity, object_facts)) = published else {
            return;
        };
        if self
            .table_catalog
            .record_table(identity, object_facts)
            .is_err()
        {
            if let Ok(health) = crate::lifecycle::telemetry_health_debt(
                "fork unsealed-slice catalog record failed; recovery falls back to fork rebuild",
            ) {
                self.record_recovery_health(Some(&health));
            }
        }
    }

    pub(crate) fn clear_branch(
        &mut self,
        branch_id: BranchId,
        generation_guard: CommitBranchGenerationGuard,
    ) -> LifecycleResult<crate::lifecycle::LifecycleBranchClearOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let _quiesce = self.guard_set.try_begin_quiesce().map_err(commit_error)?;
        let outcome = self
            .branch_catalog
            .clear_branch(branch_id, generation_guard)?;
        let plan = outcome.release_plan().clone();
        if !plan.protected_tables().is_empty() {
            if let Ok(health) =
                crate::lifecycle::telemetry_health_debt("pinned view blocks clear/delete release")
            {
                self.record_recovery_health(Some(&health));
            }
        }
        self.pending_releases.push(plan);
        self.publish_branch_catalog()?;
        self.publish_pending_releases()?;
        // Table-object GC: the buffered release plan (and the refs the clear dropped) are
        // reclaimed by the retention → quarantine → purge cycle. Best-effort, coalescing.
        let _ = self.enqueue_maintenance(
            crate::lifecycle::MaintenanceTaskRequest::table_object_retention(branch_id),
        );
        // BS2.3: clear reset the branch to an empty state; republish the (now empty) snapshot.
        self.publish_branch_snapshot(branch_id);
        Ok(outcome)
    }

    pub(crate) fn delete_branch(
        &mut self,
        branch_id: BranchId,
        generation_guard: CommitBranchGenerationGuard,
        deleted_at: Option<CommitVersion>,
    ) -> LifecycleResult<crate::lifecycle::LifecycleBranchDeleteOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let _quiesce = self.guard_set.try_begin_quiesce().map_err(commit_error)?;
        // #2820: the durable live-delete refuses while a fork child's
        // recovery depends on this branch (replay and cache mode do not
        // consult this — see the helper's doc).
        self.branch_catalog
            .require_no_recovery_dependent_children(branch_id)?;
        let outcome = self
            .branch_catalog
            .delete_branch(branch_id, generation_guard, deleted_at)?;
        let plan = outcome.release_plan().clone();
        if !plan.protected_tables().is_empty() {
            if let Ok(health) =
                crate::lifecycle::telemetry_health_debt("pinned view blocks clear/delete release")
            {
                self.record_recovery_health(Some(&health));
            }
        }
        self.pending_releases.push(plan);
        self.publish_branch_catalog()?;
        self.publish_pending_releases()?;
        // #2553: the branch tombstone is durable (catalog published above), so recovery ignores
        // the branch's table manifest — drop its frontier sets or they would permanently
        // protect exactly the objects deletion frees.
        self.table_catalog.clear_branch_frontier(branch_id);
        // Table-object GC: the deleted branch's now-unreachable objects are reclaimed by the
        // retention → quarantine → purge cycle. Best-effort, coalescing.
        let _ = self.enqueue_maintenance(
            crate::lifecycle::MaintenanceTaskRequest::table_object_retention(branch_id),
        );
        // BS2.3: the branch is tombstoned; drop its snapshot slot.
        self.remove_branch_snapshot(branch_id);
        Ok(outcome)
    }

    /// Publish a fresh `BranchCatalogManifest` reflecting the current
    /// catalog state. Called after every durable catalog mutation. The
    /// runtime's `branch_catalog_sequence` is incremented monotonically
    /// so recovery can resolve concurrent-writer scenarios.
    fn publish_branch_catalog(&mut self) -> LifecycleResult<()> {
        let entries = self
            .branch_catalog
            .durable_entries()
            .map_err(branch_catalog_format_error)?;
        self.branch_catalog_sequence = self.branch_catalog_sequence.saturating_add(1);
        if self.branch_catalog_sequence == 0 {
            return Err(LifecycleError::CheckpointPublicationFailed {
                reason: "branch catalog sequence overflow",
            });
        }
        let manifest = crate::format::BranchCatalogManifest::new(
            *self.services.assembly_facts().database_id(),
            self.branch_catalog_sequence,
            entries,
        )
        .map_err(branch_catalog_format_error)?;
        self.services
            .branch_catalog_manifest()
            .publish_replace(&manifest)
            .map_err(branch_catalog_manifest_service_error)?;
        Ok(())
    }

    /// Publish a fresh `PendingReleasesManifest` reflecting the current
    /// in-memory buffer. Called after `clear_branch`, `delete_branch`,
    /// and after each retention drain that consumed entries. The
    /// sequence counter advances monotonically so recovery can resolve
    /// concurrent-writer scenarios across restarts.
    pub(super) fn publish_pending_releases(&mut self) -> LifecycleResult<()> {
        let entries = pending_releases_to_durable_entries(&self.pending_releases)
            .map_err(pending_releases_format_error)?;
        self.pending_releases_sequence = self.pending_releases_sequence.saturating_add(1);
        if self.pending_releases_sequence == 0 {
            return Err(LifecycleError::CheckpointPublicationFailed {
                reason: "pending releases sequence overflow",
            });
        }
        let manifest = crate::format::PendingReleasesManifest::new(
            *self.services.assembly_facts().database_id(),
            self.pending_releases_sequence,
            entries,
        )
        .map_err(pending_releases_format_error)?;
        self.services
            .pending_releases_manifest()
            .publish_replace(&manifest)
            .map_err(pending_releases_manifest_service_error)?;
        Ok(())
    }
}

fn pending_releases_to_durable_entries(
    plans: &[crate::branch::facts::BranchReleasePlan],
) -> Result<Vec<crate::format::PendingReleasesEntry>, crate::format::FormatError> {
    // Group by branch_id; multiple plans for the same branch (multiple
    // clear/delete operations between drains) merge their releasable
    // tables into a single entry. Entries are sorted by branch_id byte
    // order to match the manifest's canonical encoding.
    use std::collections::BTreeMap;
    let mut grouped: BTreeMap<[u8; 16], Vec<String>> = BTreeMap::new();
    for plan in plans {
        let key = *plan.released_branch_id().as_bytes();
        let bucket = grouped.entry(key).or_default();
        for identity in plan.releasable_tables() {
            bucket.push(identity.as_str().to_owned());
        }
    }
    let mut entries = Vec::with_capacity(grouped.len());
    for (key, mut tables) in grouped {
        tables.sort();
        tables.dedup();
        let branch_id = strata_core::BranchId::from_bytes(key);
        entries.push(crate::format::PendingReleasesEntry::new(branch_id, tables)?);
    }
    Ok(entries)
}

fn branch_catalog_format_error(error: crate::format::FormatError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        crate::lifecycle::LifecycleLowerLayer::Format,
        "branch catalog manifest encode failed",
        error,
    )
}

fn pending_releases_format_error(error: crate::format::FormatError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        crate::lifecycle::LifecycleLowerLayer::Format,
        "pending releases manifest encode failed",
        error,
    )
}

fn pending_releases_manifest_service_error(
    error: crate::service::ManifestServiceError,
) -> LifecycleError {
    LifecycleError::lower_layer_with(
        crate::lifecycle::LifecycleLowerLayer::Service,
        "pending releases manifest service failed",
        error,
    )
}

/// Reconstruct the in-memory catalog from a persisted
/// `BranchCatalogManifest`. The seeded branch is already in the catalog
/// (registered via `with_existing_branch`); reconcile its state against
/// the manifest entry. Other entries are created (Active) or registered
/// then deleted (Deleted) to produce the same descriptor as the original
/// runtime did before close.
fn replay_branch_catalog_manifest(
    catalog: &mut LifecycleBranchCatalog,
    initial_branch_id: BranchId,
    manifest: &crate::format::BranchCatalogManifest,
) -> LifecycleResult<()> {
    use crate::commit::{CommitBranchGeneration, CommitBranchGenerationGuard};
    use crate::format::BranchCatalogStatus;
    use crate::lifecycle::LifecycleBranchParent;
    for entry in manifest.entries() {
        let branch_id = entry.branch_id();
        let generation_value = entry.generation();
        let generation = CommitBranchGeneration::new(generation_value).map_err(|_| {
            LifecycleError::RecoveryFailed {
                reason: "branch catalog manifest entry has invalid generation",
            }
        })?;
        let created_at = entry.created_at().map(CommitVersion::new);

        if branch_id == initial_branch_id {
            // The seeded branch is already registered via with_existing_branch
            // with created_at UNKNOWN; adopt the manifest's truthful value so
            // the WAL-replay generation fence sees the real creation point.
            catalog.set_created_at_for_recovery(
                initial_branch_id,
                created_at,
                RecoveryExclusivityToken::new(),
            )?;
            // For Active entries, no further work; for Deleted entries, mark
            // the seeded branch as deleted now. Generation mismatches against
            // the seeded branch's runtime generation surface as a recovery
            // conflict (the catalog says "this generation was seen at close
            // time"; if it disagrees with the runtime's seeded generation,
            // the seeded generation wins since it was just constructed).
            if let Some(parent) = entry.parent() {
                catalog.set_parent_for_recovery(
                    initial_branch_id,
                    LifecycleBranchParent::new(
                        parent.source_branch_id(),
                        CommitVersion::new(parent.fork_version()),
                    ),
                    RecoveryExclusivityToken::new(),
                )?;
            }
            if matches!(entry.status(), BranchCatalogStatus::Deleted) {
                let deleted_at = entry.deleted_at().map(CommitVersion::new);
                // Use the current seeded generation rather than the manifest
                // generation: tombstone applies to whichever generation the
                // seeded branch carries today. Mismatches indicate corruption
                // or an in-flight restart; in either case the catalog wins
                // because the manifest survived past the runtime that wrote it.
                let current = catalog.lookup(initial_branch_id)?.generation();
                catalog.delete_branch(
                    initial_branch_id,
                    CommitBranchGenerationGuard::exact(current),
                    deleted_at,
                )?;
            }
            continue;
        }

        // Non-seeded branch: create_branch handles both fresh Active and
        // resurrection-after-deleted-by-newer-generation flows via its
        // existing generation arbitration.
        catalog.create_branch(branch_id, generation, created_at)?;
        if let Some(parent) = entry.parent() {
            catalog.set_parent_for_recovery(
                branch_id,
                LifecycleBranchParent::new(
                    parent.source_branch_id(),
                    CommitVersion::new(parent.fork_version()),
                ),
                RecoveryExclusivityToken::new(),
            )?;
            // #2521/#2522: `create_branch` marked the restored fork
            // complete-from-birth, which fabricates empty coverage over its
            // inherited pre-fork history. Post-replay derivation re-completes
            // it from the parent chain.
            if let Ok(branch) = catalog.branch_state(branch_id) {
                branch
                    .retained_timeline()
                    .mark_incomplete_for_fork_recovery();
            }
        }
        if matches!(entry.status(), BranchCatalogStatus::Deleted) {
            let deleted_at = entry.deleted_at().map(CommitVersion::new);
            catalog.delete_branch(
                branch_id,
                CommitBranchGenerationGuard::exact(generation),
                deleted_at,
            )?;
        }
    }
    Ok(())
}

fn branch_catalog_manifest_service_error(
    error: crate::service::ManifestServiceError,
) -> LifecycleError {
    LifecycleError::lower_layer_with(
        crate::lifecycle::LifecycleLowerLayer::Service,
        "branch catalog manifest service failed",
        error,
    )
}

/// Per-member result of a write group (BS5.1): the commit outcome plus the member's write
/// admission snapshot, captured immediately after the member executed so the API layer can
/// apply the same per-caller post-commit handling as a solo commit.
#[derive(Debug)]
pub(crate) struct DurableGroupMemberResult {
    pub(crate) outcome: LifecycleResult<CommitOutcome>,
    pub(crate) admission: Option<LifecycleWriteAdmissionOutcome>,
}

/// Which durable-gate check commit admission runs (BS5.1): a solo commit requires the gate
/// admission span to be available; a group member runs inside the leader's active span.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DurableGateAdmission {
    Solo,
    Member,
}

/// A write group between its two under-lock phases (BS5.2): phase 1 appended
/// and applied the members and captured the covering-sync ticket; the caller
/// runs the fsync WITHOUT the runtime lock (or proves coverage by a later
/// completed sync via the ticket's captured sequence against the WAL's
/// durable watermark), then redeems phase 2. Member outcomes inside must not
/// be released to callers until phase 2 settles them.
pub(crate) struct DurableGroupInFlight<'a> {
    results: Vec<DurableGroupMemberResult>,
    group: CommitGroupState,
    fatal: bool,
    /// Whether the whole-group admission span was opened (phase 2 must
    /// release it exactly when it was).
    admitted: bool,
    touched_branches: Vec<BranchId>,
    ticket: Option<WalGroupSyncTicket<'a>>,
    /// Open gate spans at phase-1 end: >1 means other commits are
    /// mid-pipeline and a sync-chain batching beat is worth waiting.
    concurrent_spans: usize,
    /// Per-branch deferred applies (BS5.4): each entry is the committed rows
    /// of that branch's single deferred member, applied before the group
    /// publishes. A branch defers only while it has ONE member in the group;
    /// a second same-branch member flushes the pending apply and the branch
    /// goes eager for the rest of the group (a later member's conflict source
    /// must see the earlier rows applied), so shared-branch groups keep
    /// today's path exactly.
    deferred_applies: Vec<DeferredBranchApply>,
}

/// One branch's deferred group apply (BS5.4): the rows are WAL-durable
/// (appended in phase 1) and invisible until the group publishes; an apply
/// failure records the group's widened durable-not-applied fact, exactly as
/// an eager member apply failure would.
#[derive(Debug)]
pub(crate) struct DeferredBranchApply {
    branch_id: BranchId,
    generation: CommitBranchGeneration,
    stamp: CommitStamp,
    durability: CommitDurabilityClass,
    rows: Vec<crate::row::StorageRow>,
}

/// A deferred branch apply packaged with its checked-out state (BS5.4c): a
/// self-contained unit an applier THREAD can run with no locks and no runtime
/// access — the state is owned, the budget handle is shared-safe. Produced by
/// [`LifecycleDurableLocalRuntime::take_deferred_apply_work`] under the
/// runtime mutex; the outcome (state included) must return to
/// [`LifecycleDurableLocalRuntime::finish_deferred_apply_work`] under it.
#[derive(Debug)]
pub(crate) struct DurableGroupApplyWork {
    state: BranchLocalState,
    pending: DeferredBranchApply,
    budget: StorageBudgetLedger,
    frozen_before: usize,
}

/// The result of one [`DurableGroupApplyWork::apply`], carrying the owned
/// branch state back for restoration.
#[derive(Debug)]
pub(crate) struct DurableGroupApplyDone {
    state: BranchLocalState,
    branch_id: BranchId,
    stamp: CommitStamp,
    durability: CommitDurabilityClass,
    frozen_before: usize,
    result: Result<(), CommitRuntimeError>,
}

impl DurableGroupApplyWork {
    /// The branch this work applies to (for member-thread routing).
    pub(crate) fn branch_id(&self) -> BranchId {
        self.pending.branch_id
    }

    /// Run the apply against the owned state — callable from ANY thread, no
    /// locks (the same budget wrapper as an eager member apply).
    pub(crate) fn apply(self) -> DurableGroupApplyDone {
        let DurableGroupApplyWork {
            mut state,
            pending,
            budget,
            frozen_before,
        } = self;
        let DeferredBranchApply {
            branch_id,
            generation: _,
            stamp,
            durability,
            rows,
        } = pending;
        let result = {
            let mut budgeted = BudgetedCommitBranch::new(&mut state, &budget);
            let applied =
                CommitBranchApplyTarget::append_committed_rows_atomically(&mut budgeted, rows);
            // #3112 S2b: the row-driven apply funnel cannot see the
            // commit-scoped `committed_at`, so upgrade the entry it just
            // created — only on success, matching the funnel's rule that a
            // rolled-back batch leaves no timeline trace.
            if applied.is_ok() {
                if let Some(instant) = stamp.committed_at() {
                    CommitBranchApplyTarget::observe_commit_instant(
                        &mut budgeted,
                        stamp.commit_version(),
                        instant,
                    );
                }
            }
            applied
        };
        DurableGroupApplyDone {
            state,
            branch_id,
            stamp,
            durability,
            frozen_before,
            result,
        }
    }
}

impl DurableGroupInFlight<'_> {
    /// The covering-sync ticket, when this group requires one (`Always` mode
    /// with at least one member past the WAL).
    pub(crate) const fn ticket(&self) -> Option<&WalGroupSyncTicket<'_>> {
        self.ticket.as_ref()
    }

    /// The group's commit-state view, for the parallel-apply finish
    /// (BS5.4c fact widening).
    pub(crate) const fn group_state(&self) -> &CommitGroupState {
        &self.group
    }

    /// Open gate spans at phase-1 end (see the field doc).
    pub(crate) const fn concurrent_spans(&self) -> usize {
        self.concurrent_spans
    }
}

impl<'a, S> LifecycleDurableLocalRuntime<'a, S>
where
    S: CommitTimestampSource,
{
    pub(crate) fn execute_durable_commit(
        &mut self,
        batch: CommitBatch,
        generation_guard: CommitBranchGenerationGuard,
    ) -> LifecycleResult<CommitOutcome> {
        let branch_id = batch.branch_id();
        let generation =
            self.admit_durable_commit(&batch, generation_guard, DurableGateAdmission::Solo)?;
        let frozen_before = self
            .branch_catalog
            .branch_state(branch_id)?
            .frozen_table_count();
        let floor = self.pipeline_visibility_floor();
        let outcome = {
            let setup_timer = perf_trace::start_timer();
            let (branch, registry) = self.branch_catalog.branch_state_mut_with_registry(
                branch_id,
                CommitBranchGenerationGuard::exact(generation),
            )?;
            let mut budgeted_branch = BudgetedCommitBranch::new(branch, &self.budget);
            let mut runtime = CommitDurableRuntime::new(
                &self.commit_config,
                registry,
                &self.guard_set,
                &mut self.allocator,
                &mut budgeted_branch,
                &mut self.visible,
                &mut self.services.wal,
                &self.durable_gate,
            );
            perf_trace::record_commit_setup_elapsed(setup_timer);
            // BS5.2: solo commits validate at the pipeline floor so a commit
            // admitted while a group's fsync is off-lock keeps serial
            // semantics; with an empty pipeline the floor equals the visible
            // version and this is byte-identical to plain `execute`.
            runtime
                .execute_with_visibility_floor(batch, generation_guard, floor)
                .map_err(commit_error)
        };
        // Commit-triggered auto-rotation is a STRUCTURAL change: per the Model-2 contract
        // below, it must republish the snapshot in the same lock hold — the published view's
        // live-active handle now points at the rotated-out (frozen) table, so later commits'
        // rows in the fresh active would otherwise be invisible until the next background
        // publish. Republish BEFORE advancing the atomic mirror so any reader observing the
        // new visible version finds a covering snapshot (V-before-S).
        if outcome.is_ok() {
            self.republish_branch_snapshot_after_rotation(branch_id, frozen_before)?;
            // BS2.2: mirror the just-advanced visible version to the atomic (release) so off-lock
            // readers (BS2.4) observe it without the runtime lock. On the `applied_not_visible`
            // error path `outcome` is `Err`, so the atomic correctly does not advance.
            self.finish_durable_commit_post_publish(branch_id);
        }
        // BS2.4 Model 2: commits do NOT republish the snapshot (except the rotation case above).
        // The published snapshot holds the live (unpinned) active handle, so it already sees this
        // commit's appends; each off-lock read pins the active at read time and bounds by the
        // visible version. Only structural changes (rotation/flush/compaction/materialization/
        // fork/lifecycle) republish.
        outcome
    }

    /// Execute a write group (BS5.1): the caller is the group leader holding the runtime lock.
    /// Members execute sequentially in member order — WAL order equals version order equals
    /// member order — under one durable-gate admission span, deferring fsync and visible
    /// publish to one finalize. Pre-WAL failures are clean per-member rejections; a post-WAL
    /// failure is group-fatal: every member that reached the WAL reports durability-uncertain
    /// and recovery reconciles the group's version range through the widened gate fact.
    pub(crate) fn execute_durable_commit_group(
        &mut self,
        members: Vec<(CommitBatch, CommitBranchGenerationGuard)>,
    ) -> Vec<DurableGroupMemberResult> {
        let in_flight = self.execute_durable_commit_group_begin(members);
        let sync_result = in_flight.ticket.as_ref().map(WalGroupSyncTicket::sync);
        self.execute_durable_commit_group_finish(in_flight, sync_result)
    }

    /// Phase 1 of a write group (BS5.2): everything that must run under the
    /// runtime lock BEFORE the covering fsync — the whole-group admission
    /// span, per-member admission + WAL append + memtable apply, and the sync
    /// ticket capture (`Always` runtimes only; `Standard` needs no covering
    /// fsync). The caller then runs the fsync WITHOUT the lock and redeems
    /// [`execute_durable_commit_group_finish`] back under it. Between the
    /// phases the group's rows are applied but unpublished: reads stay bounded
    /// by the visible version, flush maintenance defers on this branch state,
    /// and the pipeline frontier keeps later admissions serial-equivalent.
    pub(crate) fn execute_durable_commit_group_begin(
        &mut self,
        members: Vec<(CommitBatch, CommitBranchGenerationGuard)>,
    ) -> DurableGroupInFlight<'a> {
        let mut results: Vec<DurableGroupMemberResult> = Vec::with_capacity(members.len());
        // The leader's whole-group admission span. Failure (an unresolved fact) rejects every
        // member with the same typed error a solo commit would get from the gate.
        if self.durable_gate.begin_group_admission().is_err() {
            for _ in &members {
                results.push(DurableGroupMemberResult {
                    outcome: Err(self.group_admission_unavailable_error()),
                    admission: None,
                });
            }
            return DurableGroupInFlight {
                results,
                group: CommitGroupState::new(self.visible.visible_version()),
                fatal: false,
                admitted: false,
                touched_branches: Vec::new(),
                ticket: None,
                concurrent_spans: 0,
                deferred_applies: Vec::new(),
            };
        }
        // The group frontier starts at the pipeline floor: later members (and
        // later groups) validate against everything already APPLIED by
        // in-flight groups, not just what is published — serial-equivalent
        // same-branch semantics across the pipeline.
        let mut group = CommitGroupState::new(self.pipeline_visibility_floor());
        let mut fatal = false;
        let mut touched_branches: Vec<BranchId> = Vec::new();
        let mut deferred_applies: Vec<DeferredBranchApply> = Vec::new();
        let mut eager_branches: Vec<BranchId> = Vec::new();
        for (batch, generation_guard) in members {
            if fatal {
                results.push(DurableGroupMemberResult {
                    outcome: Err(commit_error(CommitRuntimeError::DurabilityUnavailable {
                        reason: "write group aborted before this member's WAL append",
                    })),
                    admission: None,
                });
                continue;
            }
            let branch_id = batch.branch_id();
            // Deferral rule (BS5.4): a branch defers its (single) apply while
            // it has one member in the group. A repeat same-branch member
            // flushes the pending apply NOW — its conflict validation must
            // see those rows — and the branch goes eager for the rest of the
            // group, preserving today's shared-branch semantics exactly.
            if let Some(position) = deferred_applies
                .iter()
                .position(|pending| pending.branch_id == branch_id)
            {
                let pending = deferred_applies.swap_remove(position);
                if let Err(error) = self.apply_deferred_branch(&group, pending) {
                    results.push(DurableGroupMemberResult {
                        outcome: Err(error),
                        admission: self.last_write_admission,
                    });
                    fatal = true;
                    continue;
                }
                eager_branches.push(branch_id);
            }
            let defer = !eager_branches.contains(&branch_id);
            let outcome = if defer {
                self.execute_group_member_commit_deferring_apply(
                    batch,
                    generation_guard,
                    &mut group,
                    &mut deferred_applies,
                )
            } else {
                self.execute_group_member_commit(batch, generation_guard, &mut group)
            };
            if outcome.is_ok() && !touched_branches.contains(&branch_id) {
                touched_branches.push(branch_id);
            }
            if outcome.is_err() {
                // A recorded fact means the failure happened after a WAL append
                // (durable-not-applied) — group-fatal. Pre-WAL rejections leave
                // the gate clean and the group continues.
                fatal = matches!(self.durable_gate.unresolved(), Ok(Some(_)) | Err(_));
            }
            results.push(DurableGroupMemberResult {
                outcome,
                admission: self.last_write_admission,
            });
        }
        let ticket = if !fatal && group.last_stamp().is_some() {
            if let Some(last) = group.last_stamp() {
                self.pipeline_frontier = self.pipeline_frontier.max(last.commit_version());
            }
            // Standard mode publishes without a covering fsync (same durability
            // class as its solo path); only Always captures a sync ticket.
            if self.services.wal.durability_policy() == DurabilityPolicy::Always {
                // W3.3a: capture flushes the append-coalescing buffer (the
                // group flush). A flush failure means the group's appends
                // cannot be covered by any fsync — same terminal handling as
                // a failed covering sync, recorded here in phase 1.
                match self.services.wal.begin_group_sync() {
                    Ok(ticket) => Some(ticket),
                    Err(error) => {
                        fatal = self.record_group_capture_failure(&group, &error);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };
        DurableGroupInFlight {
            results,
            group,
            fatal,
            admitted: true,
            touched_branches,
            ticket,
            concurrent_spans: self.durable_gate.open_admission_spans(),
            deferred_applies,
        }
    }

    /// Phase 2 of a write group (BS5.2): everything after the covering fsync,
    /// back under the runtime lock — redeem the sync outcome, publish the
    /// group's visibility, release the admission span, and run post-commit
    /// bookkeeping. `sync_result` is `None` when phase 1 captured no ticket
    /// (Standard mode, rejected-only groups) or when the caller proved the
    /// group's appends were already covered by a later completed sync.
    pub(crate) fn execute_durable_commit_group_finish(
        &mut self,
        in_flight: DurableGroupInFlight<'a>,
        sync_result: Option<Result<(), WalServiceError>>,
    ) -> Vec<DurableGroupMemberResult> {
        self.execute_durable_commit_group_finish_with_apply_outcome(in_flight, sync_result, false)
    }

    /// [`execute_durable_commit_group_finish`] for callers that ran the
    /// group's deferred applies in parallel (BS5.4c): `parallel_apply_fatal`
    /// carries [`finish_deferred_apply_work`]'s verdict into the group-fatal
    /// path.
    pub(crate) fn execute_durable_commit_group_finish_with_apply_outcome(
        &mut self,
        in_flight: DurableGroupInFlight<'a>,
        sync_result: Option<Result<(), WalServiceError>>,
        parallel_apply_fatal: bool,
    ) -> Vec<DurableGroupMemberResult> {
        let DurableGroupInFlight {
            mut results,
            group,
            mut fatal,
            admitted,
            touched_branches,
            ticket,
            concurrent_spans: _,
            deferred_applies,
        } = in_flight;
        if !admitted {
            return results;
        }
        fatal |= parallel_apply_fatal;
        // BS5.4: land the deferred per-branch applies BEFORE durability
        // settles and visibility publishes. The rows are WAL-durable from
        // phase 1; an apply failure here records the group's widened
        // durable-not-applied fact — the same failure class as an eager
        // member apply — and the group goes fatal without publishing.
        if !fatal {
            for pending in deferred_applies {
                if let Err(_error) = self.apply_deferred_branch(&group, pending) {
                    fatal = true;
                    break;
                }
            }
        }
        if !fatal {
            match (&ticket, sync_result) {
                (Some(ticket), Some(Ok(()))) => {
                    self.services.wal.complete_group_sync(ticket);
                }
                (Some(ticket), Some(Err(error))) => {
                    // #2766: an off-lock sync that found the active object
                    // missing is permanent — latch the writer halted so no
                    // further appends are admitted against the dead log.
                    if error.is_active_object_missing() {
                        self.services.wal.record_active_object_lost();
                    }
                    fatal = self.record_group_sync_failure(&group, ticket, &error);
                }
                // `(Some(_), None)`: the caller proved coverage by a later
                // completed sync (the durable watermark passed this group's
                // appends) — nothing to redeem. `(None, _)`: no ticket was
                // captured (Standard mode or rejected-only group).
                (Some(_) | None, _) => {}
            }
        }
        // A fact recorded by ANOTHER in-flight group at or below our range end
        // makes publishing unsafe (it would expose that group's unresolved
        // versions beneath ours); a fact strictly above our range does not.
        if !fatal {
            if let Ok(Some(unresolved)) = self.durable_gate.unresolved() {
                if let Some(last) = group.last_stamp() {
                    fatal = unresolved.first_commit_version() <= last.commit_version();
                }
            }
        }
        if !fatal {
            let publish = publish_commit_group(&group, &mut self.visible, &self.durable_gate);
            fatal = publish.is_err();
        }
        self.durable_gate.end_group_admission();
        if fatal {
            // Group-fatal: nothing was published. Members that reached the WAL are covered by
            // the widened gate fact (or a halted writer) and must not be acked — replay
            // reconciles them idempotently after reopen.
            for result in &mut results {
                if let Ok(outcome) = &result.outcome {
                    // A successful group member is always a mutating visible outcome
                    // with an allocated version; ZERO is unreachable and only
                    // satisfies the type.
                    let commit_version = outcome.commit_version().unwrap_or(CommitVersion::ZERO);
                    result.outcome = Err(commit_error(CommitRuntimeError::DurabilityUncertain {
                        branch_id: outcome.branch_id(),
                        commit_version,
                        reason: "write group failed after this member's WAL append; \
                                 replay must reconcile",
                        source: None,
                    }));
                }
            }
            return results;
        }
        if group.last_stamp().is_some() {
            self.mirror_visible_and_evaluate_wal_growth();
            for branch_id in touched_branches {
                self.schedule_post_commit_maintenance_best_effort(branch_id);
            }
        }
        results
    }

    /// Record the gate fact for a group whose sync-ticket capture failed
    /// (W3.3a: the append-buffer flush at capture). No watermark rescue is
    /// possible — unflushed bytes cannot be covered by any completed fsync.
    /// Returns whether the group is fatal (always, unless it had no stamp).
    fn record_group_capture_failure(
        &mut self,
        group: &CommitGroupState,
        error: &WalServiceError,
    ) -> bool {
        let Some(stamp) = group.last_stamp() else {
            return true;
        };
        let _ = error;
        let reason = "write group sync capture failed to flush the WAL append buffer";
        let fact = CommitUnresolvedDurable::applied_not_visible(
            stamp,
            CommitDurabilityClass::NotDurable,
            reason,
        )
        .and_then(|fact| group.widen_fact(fact));
        if let Ok(fact) = fact {
            // Rationale: recording can only fail when a DIFFERENT fact is
            // already present; that fact already gates commits, and replay
            // reconciles our range from the WAL contents.
            let _ = self.durable_gate.record_unresolved(fact);
        }
        true
    }

    /// Record the gate fact for a failed covering fsync (BS5.2), unless a
    /// later completed sync already proved this group's appends durable (the
    /// watermark rescue) or another group's fact already covers the range.
    /// Returns whether the group is fatal.
    fn record_group_sync_failure(
        &mut self,
        group: &CommitGroupState,
        ticket: &WalGroupSyncTicket<'_>,
        error: &WalServiceError,
    ) -> bool {
        // Watermark rescue: a later group's completed sync covered every
        // append before its capture — including ours — so our records are
        // durable despite our own sync failing.
        if self
            .services
            .wal
            .durable_seq_handle()
            .load(Ordering::Acquire)
            >= ticket.captured_seq()
        {
            return false;
        }
        let Some(stamp) = group.last_stamp() else {
            return true;
        };
        // Fail closed: members are applied but the covering fsync did not
        // complete (`error` says why), so the fact claims no durable progress
        // for the range. If another in-flight group already recorded a fact,
        // ours stays unrecorded — recovery replays the WAL contents
        // idempotently and these members report durability-uncertain either
        // way.
        let _ = error;
        let reason = "write group covering fsync failed after members applied";
        let fact = CommitUnresolvedDurable::applied_not_visible(
            stamp,
            CommitDurabilityClass::NotDurable,
            reason,
        )
        .and_then(|fact| group.widen_fact(fact));
        if let Ok(fact) = fact {
            // Rationale: recording can only fail when a DIFFERENT fact is
            // already present; that fact already gates commits, and replay
            // reconciles our range from the WAL contents.
            let _ = self.durable_gate.record_unresolved(fact);
        }
        true
    }

    /// The pipeline visibility floor (BS5.2): what a newly admitted commit
    /// must validate against — everything published PLUS everything applied
    /// by in-flight groups whose covering fsync is still off-lock.
    fn pipeline_visibility_floor(&self) -> CommitVersion {
        self.visible.visible_version().max(self.pipeline_frontier)
    }

    /// Off-lock handle to the WAL's durable-append watermark (BS5.2): the
    /// pipelined caller polls coverage through this atomic without the
    /// runtime lock.
    pub(crate) fn wal_durable_seq_handle(&self) -> Arc<AtomicU64> {
        self.services.wal.durable_seq_handle()
    }

    /// A fresh covering-sync ticket at the CURRENT append frontier (BS5.2):
    /// the sync-chain token holder re-captures right before syncing so one
    /// fsync covers every group that appended while the previous sync ran.
    pub(crate) fn wal_group_sync_ticket(
        &mut self,
    ) -> Result<WalGroupSyncTicket<'a>, WalServiceError> {
        self.services.wal.begin_group_sync()
    }

    /// Execute one group member: solo-equivalent bootstrap admission (with the gate check in
    /// member mode), the member commit protocol, and the same-lock-hold rotation republish.
    /// Apply one branch's deferred group rows (BS5.4), with the same budget
    /// wrapper, rotation republish, and failure fact as an eager member apply.
    fn apply_deferred_branch(
        &mut self,
        group: &CommitGroupState,
        pending: DeferredBranchApply,
    ) -> LifecycleResult<()> {
        let DeferredBranchApply {
            branch_id,
            generation,
            stamp,
            durability,
            rows,
        } = pending;
        let frozen_before = self
            .branch_catalog
            .branch_state(branch_id)?
            .frozen_table_count();
        let apply_result = {
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            let mut budgeted = BudgetedCommitBranch::new(branch, &self.budget);
            let applied =
                CommitBranchApplyTarget::append_committed_rows_atomically(&mut budgeted, rows);
            // #3112 S2b: see `apply_deferred_group_branch` — the funnel is
            // row-driven and cannot carry the commit-scoped instant.
            if applied.is_ok() {
                if let Some(instant) = stamp.committed_at() {
                    CommitBranchApplyTarget::observe_commit_instant(
                        &mut budgeted,
                        stamp.commit_version(),
                        instant,
                    );
                }
            }
            applied
        };
        if let Err(source) = apply_result {
            let reason = "branch state rejected durable commit rows after WAL append";
            let fact =
                CommitUnresolvedDurable::durable_not_applied_with_facts(stamp, durability, reason)
                    .and_then(|fact| group.widen_fact(fact))
                    .map_err(commit_error)?;
            // Rationale: recording can only fail when a DIFFERENT fact is
            // already present; that fact already gates commits, and replay
            // reconciles this range from the WAL contents.
            let _ = self.durable_gate.record_unresolved(fact);
            return Err(commit_error(
                CommitRuntimeError::durable_but_not_visible_with(
                    branch_id,
                    stamp.commit_version(),
                    reason,
                    source,
                ),
            ));
        }
        self.republish_branch_snapshot_after_rotation(branch_id, frozen_before)?;
        Ok(())
    }

    /// BS5.4c: drain the in-flight group's deferred applies as owned,
    /// thread-portable work units (states checked OUT of the catalog).
    /// Caller contract: runs under the runtime mutex; every unit's outcome
    /// must come back through [`finish_deferred_apply_work`] before the
    /// group publishes (also under the mutex). Units left undrained fall
    /// back to the leader-applied path in
    /// [`execute_durable_commit_group_finish`].
    pub(crate) fn take_deferred_apply_work(
        &mut self,
        in_flight: &mut DurableGroupInFlight<'a>,
    ) -> Vec<DurableGroupApplyWork> {
        if in_flight.fatal {
            return Vec::new();
        }
        let mut work = Vec::with_capacity(in_flight.deferred_applies.len());
        // Drain back-to-front so a checkout failure leaves the remainder for
        // the leader-applied fallback in finish.
        while let Some(pending) = in_flight.deferred_applies.pop() {
            let Ok(current) = self.branch_catalog.branch_state(pending.branch_id) else {
                in_flight.deferred_applies.push(pending);
                break;
            };
            let frozen_before = current.frozen_table_count();
            let Ok(state) = self.branch_catalog.take_branch_state(
                pending.branch_id,
                CommitBranchGenerationGuard::exact(pending.generation),
            ) else {
                in_flight.deferred_applies.push(pending);
                break;
            };
            work.push(DurableGroupApplyWork {
                state,
                pending,
                budget: self.budget.clone(),
                frozen_before,
            });
        }
        work
    }

    /// BS5.4c: restore the checked-out states and fold the apply outcomes.
    /// Returns whether the group is fatal (any apply failed; the widened
    /// durable-not-applied fact is recorded). Must run under the runtime
    /// mutex, before the group publishes.
    pub(crate) fn finish_deferred_apply_work(
        &mut self,
        done: Vec<DurableGroupApplyDone>,
        group: &CommitGroupState,
    ) -> bool {
        let mut fatal = false;
        for outcome in done {
            let DurableGroupApplyDone {
                state,
                branch_id,
                stamp,
                durability,
                frozen_before,
                result,
            } = outcome;
            // Restore FIRST: even a failed apply returns the state (fail-
            // closed absence would wedge the branch until reopen).
            if self.branch_catalog.check_in_branch_state(state).is_err() {
                fatal = true;
                continue;
            }
            match result {
                Ok(()) => {
                    // Rationale: republish failure here means the branch
                    // vanished mid-group (structurally impossible under the
                    // held mutex); the group-fatal path fails closed.
                    if self
                        .republish_branch_snapshot_after_rotation(branch_id, frozen_before)
                        .is_err()
                    {
                        fatal = true;
                    }
                }
                Err(_source) => {
                    let reason = "branch state rejected durable commit rows after WAL append";
                    if let Ok(fact) = CommitUnresolvedDurable::durable_not_applied_with_facts(
                        stamp, durability, reason,
                    )
                    .and_then(|fact| group.widen_fact(fact))
                    {
                        // Rationale: recording can only fail when a DIFFERENT
                        // fact is already present; that fact already gates
                        // commits, and replay reconciles from the WAL.
                        let _ = self.durable_gate.record_unresolved(fact);
                    }
                    fatal = true;
                }
            }
        }
        fatal
    }

    /// [`execute_group_member_commit`] with the memtable apply deferred
    /// (BS5.4): the committed rows are queued per branch and applied before
    /// the group publishes. Only the branch's FIRST member in the group may
    /// defer; the caller owns that rule.
    fn execute_group_member_commit_deferring_apply(
        &mut self,
        batch: CommitBatch,
        generation_guard: CommitBranchGenerationGuard,
        group: &mut CommitGroupState,
        deferred_applies: &mut Vec<DeferredBranchApply>,
    ) -> LifecycleResult<CommitOutcome> {
        let branch_id = batch.branch_id();
        let generation =
            self.admit_durable_commit(&batch, generation_guard, DurableGateAdmission::Member)?;
        let frozen_before = self
            .branch_catalog
            .branch_state(branch_id)?
            .frozen_table_count();
        let mut deferred_rows: Option<Vec<crate::row::StorageRow>> = None;
        let outcome = {
            let setup_timer = perf_trace::start_timer();
            let (branch, registry) = self.branch_catalog.branch_state_mut_with_registry(
                branch_id,
                CommitBranchGenerationGuard::exact(generation),
            )?;
            let mut budgeted_branch = BudgetedCommitBranch::new(branch, &self.budget);
            let mut runtime = CommitDurableRuntime::new(
                &self.commit_config,
                registry,
                &self.guard_set,
                &mut self.allocator,
                &mut budgeted_branch,
                &mut self.visible,
                &mut self.services.wal,
                &self.durable_gate,
            );
            perf_trace::record_commit_setup_elapsed(setup_timer);
            runtime
                .execute_group_member_deferring_apply(
                    batch,
                    generation_guard,
                    group,
                    &mut deferred_rows,
                )
                .map_err(commit_error)
        };
        if let Ok(committed) = &outcome {
            let rows = deferred_rows
                .take()
                .ok_or(LifecycleError::InvalidLifecycleState {
                    reason: "deferred group member produced no rows to apply",
                })?;
            // The member's own stamp for failure-fact context at apply time.
            let stamp = group
                .last_stamp()
                .ok_or(LifecycleError::InvalidLifecycleState {
                    reason: "deferred group member recorded no stamp",
                })?;
            deferred_applies.push(DeferredBranchApply {
                branch_id,
                generation,
                stamp,
                durability: committed.durability(),
                rows,
            });
            // Rotation cannot have happened (the apply was deferred), but the
            // check is cheap and keeps this wrapper shape-identical to the
            // eager one.
            self.republish_branch_snapshot_after_rotation(branch_id, frozen_before)?;
        }
        outcome
    }

    fn execute_group_member_commit(
        &mut self,
        batch: CommitBatch,
        generation_guard: CommitBranchGenerationGuard,
        group: &mut CommitGroupState,
    ) -> LifecycleResult<CommitOutcome> {
        let branch_id = batch.branch_id();
        let generation =
            self.admit_durable_commit(&batch, generation_guard, DurableGateAdmission::Member)?;
        let frozen_before = self
            .branch_catalog
            .branch_state(branch_id)?
            .frozen_table_count();
        let outcome = {
            let setup_timer = perf_trace::start_timer();
            let (branch, registry) = self.branch_catalog.branch_state_mut_with_registry(
                branch_id,
                CommitBranchGenerationGuard::exact(generation),
            )?;
            let mut budgeted_branch = BudgetedCommitBranch::new(branch, &self.budget);
            let mut runtime = CommitDurableRuntime::new(
                &self.commit_config,
                registry,
                &self.guard_set,
                &mut self.allocator,
                &mut budgeted_branch,
                &mut self.visible,
                &mut self.services.wal,
                &self.durable_gate,
            );
            perf_trace::record_commit_setup_elapsed(setup_timer);
            runtime
                .execute_group_member(batch, generation_guard, group)
                .map_err(commit_error)
        };
        if outcome.is_ok() {
            // Republishing the snapshot before the group's visible publish is the safe
            // direction of V-before-S: the snapshot covers more than the visible bound.
            self.republish_branch_snapshot_after_rotation(branch_id, frozen_before)?;
        }
        outcome
    }

    /// Solo-equivalent commit admission (the phase before the commit protocol runs). In member
    /// mode the gate check verifies only that no unresolved fact exists: the leader's admission
    /// span is active by construction, which the solo check would reject.
    fn admit_durable_commit(
        &mut self,
        batch: &CommitBatch,
        generation_guard: CommitBranchGenerationGuard,
        gate_admission: DurableGateAdmission,
    ) -> LifecycleResult<CommitBranchGeneration> {
        let admit_timer = perf_trace::start_timer();
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
        Self::require_durable_commit_mode(batch)?;
        if batch.kind() == CommitBatchKind::Mutating {
            match gate_admission {
                DurableGateAdmission::Solo => self.require_no_unresolved_durable_commit()?,
                DurableGateAdmission::Member => self
                    .durable_gate
                    .require_no_unresolved_fact()
                    .map_err(commit_error)?,
            }
            self.require_branch_commit_guard_available(branch_id)?;
            self.require_write_admission_recovery_health(branch_id)?;
            self.evaluate_mutating_write_admission_for_branch(branch_id)?;
            self.require_projected_mutating_commit_budget(branch_id, batch)?;
        }
        perf_trace::record_commit_admit_elapsed(admit_timer);
        Ok(generation)
    }

    /// Same-lock-hold snapshot republish when a commit's auto-rotation changed the branch's
    /// frozen-table count (see the Model-2 contract note in `execute_durable_commit`).
    fn republish_branch_snapshot_after_rotation(
        &mut self,
        branch_id: BranchId,
        frozen_before: usize,
    ) -> LifecycleResult<()> {
        let frozen_after = self
            .branch_catalog
            .branch_state(branch_id)?
            .frozen_table_count();
        if frozen_after != frozen_before {
            self.publish_branch_snapshot(branch_id);
        }
        Ok(())
    }

    /// Post-publish bookkeeping shared by solo commits and group finalize: mirror the visible
    /// version to the off-lock atomic, evaluate WAL growth, and schedule post-commit maintenance.
    fn finish_durable_commit_post_publish(&mut self, branch_id: BranchId) {
        self.mirror_visible_and_evaluate_wal_growth();
        self.schedule_post_commit_maintenance_best_effort(branch_id);
    }

    fn mirror_visible_and_evaluate_wal_growth(&mut self) {
        self.visible_commit_version
            .store(self.visible.visible_version().as_u64(), Ordering::Release);
        let wal_growth_start = perf_trace::start_timer();
        self.evaluate_and_record_wal_growth_policy();
        perf_trace::record_commit_post_wal_growth_elapsed(wal_growth_start);
    }

    fn schedule_post_commit_maintenance_best_effort(&mut self, branch_id: BranchId) {
        let maintenance_start = perf_trace::start_timer();
        // Rationale: maintenance scheduling is best-effort after a successful commit; a
        // scheduling refusal must not fail the already-durable commit (same as solo).
        let _ = self.schedule_post_commit_maintenance_for_branch(branch_id);
        perf_trace::record_commit_post_maintenance_elapsed(maintenance_start);
    }

    /// Reproduce the per-member gate-admission error after the leader's group admission failed.
    /// Deterministic under the runtime lock: the gate cannot change concurrently.
    fn group_admission_unavailable_error(&self) -> LifecycleError {
        match self.durable_gate.require_admission_available() {
            Err(error) => commit_error(error),
            Ok(()) => commit_error(CommitRuntimeError::InvalidCommitState {
                reason: "write group admission unavailable",
            }),
        }
    }

    pub(crate) fn evaluate_and_record_wal_growth_policy(&mut self) {
        match self.evaluate_wal_growth_policy() {
            Ok(policy_outcome) => self.record_wal_growth_outcome(policy_outcome),
            Err(error) => self.record_wal_growth_policy_error(error),
        }
    }

    fn record_wal_growth_outcome(&mut self, outcome: LifecycleWalGrowthOutcome) {
        let facts = outcome.facts();
        let checkpoint_enqueued =
            outcome.status() == crate::lifecycle::LifecycleWalGrowthStatus::MaintenanceEnqueued;
        let checkpoint_coalesced =
            outcome.status() == crate::lifecycle::LifecycleWalGrowthStatus::MaintenanceCoalesced;
        perf_trace::record_lifecycle_wal_growth_sample(
            facts.retained_bytes(),
            facts.retained_segments(),
            checkpoint_enqueued,
            checkpoint_coalesced,
        );
        self.record_recovery_health(outcome.recovery_health());
        self.last_wal_growth_outcome = Some(outcome);
    }

    fn record_wal_growth_policy_error(&mut self, error: LifecycleError) {
        if let Ok(outcome) =
            LifecycleWalGrowthOutcome::deferred_with_health(WalGrowthFacts::empty(), 0, None, error)
        {
            self.record_wal_growth_outcome(outcome);
        }
    }
}

impl LifecycleRecoveryBootstrapReport {
    const fn new(recovery_health: RecoveryHealth) -> Self {
        Self {
            records_seen: 0,
            records_applied: 0,
            records_already_applied: 0,
            rows_checked: 0,
            rows_applied: 0,
            gates_cleared: 0,
            checkpoint_visible_publish: None,
            recovered_visible_version: CommitVersion::ZERO,
            recovery_health,
        }
    }

    fn record_replay(&mut self, replay: &crate::commit::CommitReplayReport) {
        self.records_seen = self.records_seen.saturating_add(1);
        match replay.action() {
            CommitReplayAction::Applied => {
                self.records_applied = self.records_applied.saturating_add(1);
            }
            CommitReplayAction::AlreadyApplied => {
                self.records_already_applied = self.records_already_applied.saturating_add(1);
            }
        }
        self.rows_checked = self.rows_checked.saturating_add(replay.rows_checked());
        self.rows_applied = self.rows_applied.saturating_add(replay.rows_applied());
        if replay.gate_cleared() {
            self.gates_cleared = self.gates_cleared.saturating_add(1);
        }
    }

    fn finish(
        &mut self,
        checkpoint_visible_publish: Option<VisibleVersionPublish>,
        recovered_visible_version: CommitVersion,
    ) {
        self.checkpoint_visible_publish = checkpoint_visible_publish;
        self.recovered_visible_version = recovered_visible_version;
    }

    pub(crate) const fn records_seen(&self) -> usize {
        self.records_seen
    }

    pub(crate) const fn records_applied(&self) -> usize {
        self.records_applied
    }

    pub(crate) const fn records_already_applied(&self) -> usize {
        self.records_already_applied
    }

    pub(crate) const fn rows_checked(&self) -> usize {
        self.rows_checked
    }

    pub(crate) const fn rows_applied(&self) -> usize {
        self.rows_applied
    }

    pub(crate) const fn gates_cleared(&self) -> usize {
        self.gates_cleared
    }

    pub(crate) const fn checkpoint_visible_publish(&self) -> Option<VisibleVersionPublish> {
        self.checkpoint_visible_publish
    }

    pub(crate) const fn recovered_visible_version(&self) -> CommitVersion {
        self.recovered_visible_version
    }

    pub(crate) const fn recovery_health(&self) -> &RecoveryHealth {
        &self.recovery_health
    }
}

fn commit_durability_class_for_mode(mode: StorageMode) -> LifecycleResult<CommitDurabilityClass> {
    match mode {
        StorageMode::DurableLocalStandard => Ok(CommitDurabilityClass::Standard),
        StorageMode::DurableLocalAlways => Ok(CommitDurabilityClass::Always),
        StorageMode::Cache | StorageMode::ObjectDurableCandidate => {
            Err(LifecycleError::InvalidOpenPlan {
                reason: "commit recovery bootstrap requires durable local storage mode",
            })
        }
    }
}

/// Validate the recovered WAL package against the rebuilt catalog. Each
/// record must reference a branch present in the catalog. Records for a
/// deleted branch are valid only when they are at or before that branch's
/// durable deletion watermark; later records indicate a post-deletion
/// resurrection attempt and must fail closed. Records must remain strictly
/// ordered by commit version across all branches — the WAL is a single
/// durable log.
fn deleted_branch_allows_recovered_version(
    descriptor: crate::lifecycle::LifecycleBranchDescriptor,
    version: CommitVersion,
) -> bool {
    descriptor
        .deleted_at()
        .is_some_and(|deleted_at| version <= deleted_at)
}

/// Enumerate persisted per-branch table manifests and install each into
/// its catalog slot. The seeded branch's manifest was already applied by
/// the pre-catalog recovery phase; skip it. Deleted descriptors are
/// resurrection-guarded — the manifest is treated as stale.
/// #2830: parentless branches own no content at or below their creation
/// point — `created_at` is visible-at-creation (#2826) and the allocator
/// stays strictly above it — so restored base-state content at
/// `version <= created_at` belongs to a DEAD predecessor generation of the
/// same branch id and must not install. Fork children are exempt: their
/// inherited rows legitimately sit at or below `created_at`.
pub(crate) fn parentless_content_predates_generation(
    has_parent: bool,
    created_at: Option<CommitVersion>,
    version: CommitVersion,
) -> bool {
    !has_parent && created_at.is_some_and(|created| version <= created)
}

/// The highest commit version any table in the manifest covers (owned
/// levels and inherited layers), or `None` for a table-free manifest.
fn manifest_max_commit_version(manifest: &crate::format::TableManifest) -> Option<CommitVersion> {
    let owned = manifest
        .levels()
        .iter()
        .flat_map(crate::format::TableManifestLevel::tables)
        .map(|table| table.facts().commit_max());
    let inherited = manifest
        .inherited_layers()
        .iter()
        .flat_map(crate::format::TableManifestInheritedLayer::levels)
        .flat_map(crate::format::TableManifestLevel::tables)
        .map(|table| table.facts().commit_max());
    owned.chain(inherited).max()
}

fn recover_per_branch_table_manifests(
    services: &LifecycleDurableLocalServices<'_>,
    table_catalog: &mut crate::lifecycle::LifecycleDurableTableCatalog,
    branch_catalog: &mut LifecycleBranchCatalog,
    initial_branch_id: BranchId,
    budget: Option<&StorageBudgetLedger>,
) -> LifecycleResult<()> {
    use crate::lifecycle::LifecycleBranchStatus;
    let manifests = services
        .table_manifest()
        .load_all_current()
        .map_err(|error| {
            LifecycleError::lower_layer_with(
                crate::lifecycle::LifecycleLowerLayer::Service,
                "branch table manifest enumeration failed",
                error,
            )
        })?;
    for manifest in manifests {
        let branch_id = manifest.branch_id();
        if branch_id == initial_branch_id {
            continue;
        }
        let descriptor =
            branch_catalog
                .lookup(branch_id)
                .map_err(|_| LifecycleError::RecoveryFailed {
                    reason: "persisted table manifest references an unknown branch",
                })?;
        if descriptor.status() == LifecycleBranchStatus::Deleted {
            continue;
        }
        // #2830: a manifest published by a dead predecessor generation of a
        // re-created (parentless) branch survives the delete on disk; every
        // version it covers is <= the current generation's creation point,
        // so it is skipped like a deleted branch's manifest (the #2553 rule
        // extended from status to generation). A table-free stale manifest
        // is skipped too: its retained-history extension would claim the
        // dead generation's coverage. Fork children keep their manifests
        // (fork publishes a fresh one; inherited content sits below
        // created_at by design).
        if descriptor.parent().is_none() {
            if let Some(created) = descriptor.created_at() {
                if manifest_max_commit_version(&manifest)
                    .is_none_or(|max_version| max_version <= created)
                {
                    continue;
                }
            }
        }
        let generation = branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        let branch_state = branch_catalog
            .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
        crate::lifecycle::apply_loaded_table_manifest_to_branch(
            branch_state,
            &manifest,
            services.table_reader(),
            table_catalog,
            budget,
        )?;
    }
    Ok(())
}

/// Install checkpoint rows that did not belong to the seeded branch.
/// `recover_checkpoint` partitions rows by `branch_id` at decode time
/// and returns non-seeded rows in the recovery outcome; this helper
/// drives them into the catalog's per-branch slots after the catalog
/// has been rebuilt from `BranchCatalogManifest` and per-branch table
/// manifests.
///
/// Validation: each row's `branch_id` must be present in the catalog. Rows
/// for deleted branches are valid only when they are at or before that
/// branch's durable deletion watermark, and are skipped rather than
/// installed. Unknown branches or post-delete rows fail closed with typed
/// `RecoveryFailed` errors, mirroring the multi-branch WAL validator. Empty
/// input is a no-op (common when the seeded branch is the only branch with
/// checkpoint coverage).
/// Non-seeded checkpoint state: rows into catalog slots, then W3.1b timeline
/// groups into their branches' indexes (before WAL replay extends them).
fn install_non_seeded_checkpoint_state(
    branch_catalog: &mut LifecycleBranchCatalog,
    checkpoint: &crate::lifecycle::LifecycleRecoveredCheckpoint,
    seeded_branch_id: BranchId,
) -> LifecycleResult<()> {
    install_non_seeded_checkpoint_rows(
        branch_catalog,
        checkpoint.non_seeded_rows(),
        checkpoint.install_identity_seed(),
    )?;
    seed_non_seeded_branch_timelines(
        branch_catalog,
        checkpoint.timeline_groups(),
        seeded_branch_id,
    );
    // W3.1c invariant, split by parentage (#2521/#2522): PARENTLESS branches
    // complete here (section-seeded no-op; the rest scan-seed from
    // pre-elision rows, or complete-empty when fresh — exact for a branch
    // created from birth, whose replayed commits then observe-append).
    // FORKED branches must stay incomplete through WAL replay: their
    // pre-fork coverage derives from the parent chain, and the parents'
    // entries only exist AFTER replay re-observes them — see
    // `complete_forked_branch_timelines_after_replay`.
    for descriptor in branch_catalog.list_branches(false) {
        if descriptor.parent().is_some() {
            continue;
        }
        let branch = branch_catalog.branch_state(descriptor.branch_id())?;
        crate::lifecycle::recovery::ensure_branch_timeline_complete(branch)?;
    }
    Ok(())
}

/// #2521/#2522: complete every forked branch's retained-timeline index once
/// WAL replay has re-observed the parents' commits. Runs after
/// `rebuild_fork_snapshot_rows` in the recovery assembly.
fn complete_forked_branch_timelines_after_replay(
    branch_catalog: &LifecycleBranchCatalog,
) -> LifecycleResult<()> {
    seed_forked_branch_timelines_from_parents(branch_catalog);
    // Corruption-guard remainder (a parent whose snapshot was refused):
    // fall back to the child's own scan — fail-safe, same as pre-fork-aware
    // recovery behaved.
    for descriptor in branch_catalog.list_branches(false) {
        let branch = branch_catalog.branch_state(descriptor.branch_id())?;
        crate::lifecycle::recovery::ensure_branch_timeline_complete(branch)?;
    }
    Ok(())
}

/// #2522: a forked branch that reopens without its own checkpoint timeline
/// group must NOT complete-empty from the post-elision scan — timeline rows
/// no longer exist, and "empty is exact" only holds for branches created
/// from birth. A fork's pre-fork coverage is its parent's index clamped to
/// the fork version (exactly what fork-time seeding built in-process, which
/// a restart discards when no checkpoint persisted the child's group). The
/// fixpoint loop resolves fork-of-a-fork chains: each pass completes children
/// whose parent completed in an earlier pass. Section-seeded children are
/// already complete and skip; a child with WAL-replayed post-fork
/// observations merges them inside `seed_from_scan`.
fn seed_forked_branch_timelines_from_parents(branch_catalog: &LifecycleBranchCatalog) {
    let descriptors = branch_catalog.list_branches(false);
    let mut progressed = true;
    while progressed {
        progressed = false;
        for descriptor in &descriptors {
            let Some(parent) = descriptor.parent() else {
                continue;
            };
            let Ok(branch) = branch_catalog.branch_state(descriptor.branch_id()) else {
                continue;
            };
            if branch.retained_timeline().is_complete() {
                continue;
            }
            let Ok(parent_state) = branch_catalog.branch_state(parent.source_branch_id()) else {
                continue;
            };
            let Some(entries) = parent_state
                .retained_timeline()
                .snapshot_entries(parent.fork_version())
            else {
                continue;
            };
            branch.retained_timeline().seed_from_scan(&entries);
            progressed = branch.retained_timeline().is_complete();
        }
    }
}

/// W3.1b: seed each non-seeded active branch's retained-timeline index from
/// its decoded checkpoint group. Deleted or unknown branches are skipped —
/// their indexes stay unseeded and fall back to the timeline-space scan.
fn seed_non_seeded_branch_timelines(
    branch_catalog: &LifecycleBranchCatalog,
    groups: &[crate::format::SnapshotTimelineBranchGroup],
    seeded_branch_id: BranchId,
) {
    use crate::lifecycle::LifecycleBranchStatus;
    for group in groups {
        if group.branch_id == seeded_branch_id {
            continue;
        }
        let Ok(descriptor) = branch_catalog.lookup(group.branch_id) else {
            continue;
        };
        if descriptor.status() == LifecycleBranchStatus::Deleted {
            continue;
        }
        let Ok(branch) = branch_catalog.branch_state(group.branch_id) else {
            continue;
        };
        // #2830: drop a dead predecessor generation's timeline entries for a
        // re-created (parentless) branch — stale coverage claims would let
        // temporal reads resolve versions the generation never had.
        let fenced;
        let group = if descriptor.parent().is_none() && descriptor.created_at().is_some() {
            fenced = crate::format::SnapshotTimelineBranchGroup {
                branch_id: group.branch_id,
                entries: group
                    .entries
                    .iter()
                    .copied()
                    .filter(|entry| {
                        !parentless_content_predates_generation(
                            false,
                            descriptor.created_at(),
                            entry.commit_version,
                        )
                    })
                    .collect(),
            };
            &fenced
        } else {
            group
        };
        crate::lifecycle::recovery::seed_branch_timeline_from_groups(
            branch,
            std::slice::from_ref(group),
        );
    }
}

fn install_non_seeded_checkpoint_rows(
    branch_catalog: &mut LifecycleBranchCatalog,
    rows: &[crate::row::StorageRow],
    identity_seed: Option<&crate::table::TableIdentity>,
) -> LifecycleResult<()> {
    use crate::lifecycle::LifecycleBranchStatus;
    if rows.is_empty() {
        return Ok(());
    }
    let mut affected: Vec<BranchId> = Vec::new();
    for row in rows {
        let id = row.physical_key().branch_id();
        if !affected.contains(&id) {
            affected.push(id);
        }
    }
    affected.sort_by_key(|id| *id.as_bytes());
    let mut active_affected = Vec::new();
    // #2830/#2833 fence inputs per active branch: (id, created_at).
    let mut fences: Vec<(BranchId, Option<CommitVersion>)> = Vec::new();
    for id in &affected {
        let descriptor =
            branch_catalog
                .lookup(*id)
                .map_err(|_| LifecycleError::RecoveryFailed {
                    reason: "checkpoint references an unknown branch",
                })?;
        if descriptor.status() == LifecycleBranchStatus::Deleted {
            if rows
                .iter()
                .filter(|row| row.physical_key().branch_id() == *id)
                .all(|row| {
                    deleted_branch_allows_recovered_version(descriptor, row.commit_version())
                })
            {
                continue;
            }
            return Err(LifecycleError::RecoveryFailed {
                reason: "checkpoint references a deleted branch",
            });
        }
        active_affected.push(*id);
        fences.push((*id, descriptor.created_at()));
    }
    if active_affected.is_empty() {
        return Ok(());
    }
    let Some(identity_seed) = identity_seed else {
        return Err(LifecycleError::RecoveryFailed {
            reason: "non-seeded checkpoint rows require an install identity seed",
        });
    };
    // #2847: partition the affected branches by what recovery already holds
    // for them. A branch recovered EMPTY takes the snapshot-install path
    // (its checkpoint rows become fresh owned L0 tables). A branch recovered
    // OCCUPIED — a flush published its table manifest AFTER the live
    // snapshot recorded the same rows, the direction the checkpoint-side
    // `non_seeded_branch_has_durable_base` guard cannot see — takes the
    // COMBINE arm instead: Recovery Protocol rule 9 extended to non-seeded
    // branches. Refusing (the old behavior) left the store permanently
    // unopenable.
    let mut snapshot_states: Vec<BranchLocalState> = Vec::new();
    let mut snapshot_rows: Vec<crate::row::StorageRow> = Vec::new();
    for (id, created_at) in &fences {
        // #2830/#2833: NO branch installs checkpoint rows at or below its
        // creation point — the same band the WAL fence drops (#2826). For a
        // re-created (parentless) branch those rows belong to the dead
        // predecessor generation. For a fork child the band also covers its
        // legitimate inherited content, but the checkpoint is never the
        // authority for it: `rebuild_fork_snapshot_rows` re-materializes a
        // layer-less child from its live parent, and a covered child rides
        // its manifest layers — while a stale pre-delete checkpoint for a
        // re-created-BY-FORK name is indistinguishable from inheritance by
        // (branch, key, version) alone and resurrected dead generations.
        let branch_rows: Vec<crate::row::StorageRow> = rows
            .iter()
            .filter(|row| {
                row.physical_key().branch_id() == *id
                    && !record_predates_current_generation(row.commit_version(), *created_at)
            })
            .cloned()
            .collect();
        if branch_rows.is_empty() {
            continue;
        }
        let staged = branch_catalog.branch_state(*id)?.clone();
        if staged.is_empty() {
            snapshot_states.push(staged);
            snapshot_rows.extend(branch_rows);
        } else {
            let mut combined = staged;
            combine_non_seeded_checkpoint_rows(&mut combined, branch_rows)?;
            let descriptor = branch_catalog.lookup(*id)?;
            branch_catalog.replace_active_branch_state_with_descriptor(descriptor, combined)?;
        }
    }
    if !snapshot_rows.is_empty() {
        let request = crate::branch::state::snapshot::BranchSnapshotInstallRequest::from_rows(
            identity_seed.as_str(),
            snapshot_rows,
        )
        .map_err(branch_error)?;
        crate::branch::state::snapshot::install_snapshot_rows_into_branches(
            &mut snapshot_states,
            &request,
        )
        .map_err(branch_error)?;
        for branch in snapshot_states {
            let branch_id = branch.branch_id();
            let descriptor = branch_catalog.lookup(branch_id)?;
            branch_catalog.replace_active_branch_state_with_descriptor(descriptor, branch)?;
        }
    }
    Ok(())
}

/// #2847: COMBINE arm for a non-seeded branch that recovery found occupied.
/// The occupied state exists because a flush published the branch's table
/// manifest after a live snapshot had already recorded the branch's rows —
/// the reverse direction of the checkpoint's structural guard, which can
/// defer a checkpoint over an existing base but cannot defer a future
/// flush. Mirrors the seeded branch's rule-9 COMBINE: checkpoint rows
/// byte-identical to a manifest-recovered row at the same internal key are
/// duplicates and drop; rows the manifest does not cover append to the
/// active memtable. A leftover row is always strictly newer than any
/// manifest row at its physical key — the flush that published the manifest
/// covered every memtable row of its time, a superset of the snapshot's —
/// so active-first newest-wins reads stay correct. Divergent bytes at the
/// same internal key fail closed, matching
/// `preflight_table_manifest_with_checkpoint`.
pub(crate) fn combine_non_seeded_checkpoint_rows(
    staged: &mut BranchLocalState,
    rows: Vec<crate::row::StorageRow>,
) -> LifecycleResult<()> {
    let mut pending = std::collections::BTreeMap::<Vec<u8>, crate::row::StorageRow>::new();
    for row in rows {
        let key = crate::table::TableInternalKeyBytes::from_row(&row)
            .as_slice()
            .to_vec();
        // Fail closed on a duplicated internal key in the snapshot itself,
        // matching the empty-target path (the table builder rejects it) —
        // silent last-wins would drop a row from a corrupt checkpoint.
        if pending.insert(key, row).is_some() {
            return Err(LifecycleError::RecoveryFailed {
                reason: "non-seeded checkpoint rows carry a duplicate internal key",
            });
        }
    }
    for table in staged.owned_levels().iter().flatten() {
        if pending.is_empty() {
            break;
        }
        crate::lifecycle::try_for_each_reader_row(table.reader(), |table_row| {
            if let Some(candidate) = pending.remove(table_row.key().as_slice()) {
                if &candidate != table_row.row() {
                    return Err(LifecycleError::table_manifest_checkpoint_conflict(
                        "non-seeded checkpoint row bytes diverge from the manifest-recovered base",
                    ));
                }
            }
            Ok(())
        })?;
    }
    if !pending.is_empty() {
        staged
            .append_committed_rows_atomically(pending.into_values())
            .map_err(branch_error)?;
    }
    Ok(())
}

/// #2527: build and durably publish the hybrid COW fork's unsealed-slice
/// table — ONE child L0 table over the source's unsealed `<= V` rows
/// (bounded by the rotation threshold, so this is a small build + one
/// publish fsync instead of the eager path's O(dataset) materialization).
/// Runs under the runtime lock, so the table-object mark cannot observe the
/// published-but-unreferenced object mid-fork; if the fork fails after the
/// publish, the object is unreachable and the sweep reclaims it. The
/// identity is content-derived: a replayed fork with identical rows
/// idempotently loads the existing object.
fn build_and_publish_fork_unsealed_table(
    services: &crate::lifecycle::LifecycleDurableLocalServices<'_>,
    budget: &crate::lifecycle::StorageBudgetLedger,
    data_block_bytes: Option<u32>,
    compression: crate::format::TableCompression,
    child: BranchId,
    fork_version: CommitVersion,
    rows: &[crate::row::StorageRow],
    published_slice: &mut Option<(
        crate::table::TableIdentity,
        crate::service::TableObjectFacts,
    )>,
) -> LifecycleResult<Option<crate::branch::read::BranchOwnedTable>> {
    use crate::lifecycle::flush::{branch_owned_table, publish_or_load_existing};

    if rows.is_empty() {
        return Ok(None);
    }
    let identity = crate::table::TableIdentity::new(format!(
        "fork-{}-{}-{}-{:016x}",
        child,
        fork_version.as_u64(),
        rows.len(),
        fork_rows_span_digest(rows),
    ))
    .map_err(crate::lifecycle::flush::table_error)?;
    let artifact = crate::table::ImmutableTableBuilder::new(
        crate::lifecycle::compaction::lifecycle_table_builder_config(data_block_bytes, compression)?,
    )
    .map_err(crate::lifecycle::flush::table_error)?
    .build_from_storage_rows(identity.clone(), rows)
    .map_err(crate::lifecycle::flush::table_error)?;
    crate::lifecycle::budget::require_generated_artifact_budget(
        budget,
        artifact.byte_count(),
        "fork unsealed slice exceeds generated artifact budget",
    )?;
    let branch_component = child.to_string();
    // The adoption marker drives the maintenance dispatcher's benign-race
    // deferral (#3382); this fork publish surfaces failures to the fork
    // caller directly, which retries at its own level, so the marker is
    // unused here.
    let (object_facts, _adopted) = publish_or_load_existing(
        services.table_object(),
        &branch_component,
        0,
        identity.as_str(),
        artifact.bytes(),
        artifact.facts(),
    )?;
    let reader = services
        .table_reader()
        .open_reader(
            identity.clone(),
            &object_facts,
            crate::table::TableReaderConfig::default()
                .with_eager_filter_unavailable()
                .deny_runtime_materialization(),
        )
        .map_err(crate::lifecycle::flush::table_error)?;
    let extras = artifact.extras().clone();
    let table = branch_owned_table(child, identity.clone(), reader, extras)?;
    *published_slice = Some((identity, object_facts));
    Ok(Some(table))
}

/// FNV-1a over the unsealed slice's first and last internal keys — the same
/// span-digest idea the flush identity uses, keeping fork replays idempotent
/// without hashing every row.
fn fork_rows_span_digest(rows: &[crate::row::StorageRow]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    fn mix(mut hash: u64, bytes: &[u8]) -> u64 {
        for &byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }
    let mut hash = FNV_OFFSET;
    if let Some(first) = rows.first() {
        hash = mix(
            hash,
            crate::table::TableInternalKeyBytes::from_row(first).as_slice(),
        );
    }
    hash = mix(hash, b"\x1f");
    if let Some(last) = rows.last() {
        hash = mix(
            hash,
            crate::table::TableInternalKeyBytes::from_row(last).as_slice(),
        );
    }
    hash
}

fn rebuild_fork_snapshot_rows(branch_catalog: &mut LifecycleBranchCatalog) -> LifecycleResult<()> {
    let descriptors = branch_catalog
        .list_branches(false)
        .into_iter()
        .filter(|descriptor| descriptor.parent().is_some())
        .collect::<Vec<_>>();
    if descriptors.is_empty() {
        return Ok(());
    }

    for _ in 0..descriptors.len() {
        let mut appended_in_pass = 0;
        for descriptor in &descriptors {
            let Some(parent) = descriptor.parent() else {
                continue;
            };
            // A child whose recovered state carries inherited layers is durably COW-covered:
            // layers only enter recovered state through the child's own table manifest, which the
            // fork now publishes at fork time. Re-materializing the parent's rows for such a child
            // is pure redundancy — and O(parent dataset) per child (the reopen OOM). The rebuild
            // below remains only for layer-less children: eager/historical forks whose materialized
            // rows are not WAL'd, and the crash window where the fork's catalog publish landed but
            // its child-manifest publish did not.
            if !branch_catalog
                .branch_state(descriptor.branch_id())?
                .inherited_layers()
                .is_empty()
            {
                continue;
            }
            // #2820: a deleted source proves no live child depended on it —
            // `delete_branch` refuses while any layer-less child has
            // non-empty fork-visible rows, so reaching a Deleted source here
            // means this child re-materializes nothing. Skipping is the
            // recovery-side half of that invariant; dereferencing (the old
            // behavior) permanently bricked the reopen.
            if branch_catalog
                .lookup_descriptor(parent.source_branch_id())
                .is_ok_and(|source| {
                    source.status() == crate::lifecycle::LifecycleBranchStatus::Deleted
                })
            {
                continue;
            }
            let rows = branch_catalog
                .branch_state(parent.source_branch_id())?
                .fork_snapshot_rows(parent.fork_version(), descriptor.branch_id())
                .map_err(branch_error)?;
            if rows.is_empty() {
                continue;
            }

            let mut staged = branch_catalog.branch_state(descriptor.branch_id())?.clone();
            let mut appended_for_branch = 0;
            for row in rows {
                // A previous reopen's rebuild may already sit durably in the child's
                // owned tables (a flush seals the re-materialized memtable), and WAL
                // replay or checkpoint installs may have re-applied rows to the active
                // memtable. Re-appending any of those would put a second copy of the
                // internal key into the branch stack — the next flush seals it and the
                // next compaction fails on the duplicate. Elide by key across the whole
                // local stack; a duplicate that still surfaces past this probe is real
                // corruption and fails recovery loudly.
                let key = crate::table::TableInternalKeyBytes::from_row(&row);
                if staged.contains_internal_key(&key).map_err(branch_error)? {
                    continue;
                }
                staged.append_committed_row(row).map_err(branch_error)?;
                appended_for_branch += 1;
            }
            if appended_for_branch > 0 {
                let current_descriptor = branch_catalog.lookup(descriptor.branch_id())?;
                branch_catalog
                    .replace_active_branch_state_with_descriptor(current_descriptor, staged)?;
                appended_in_pass += appended_for_branch;
            }
        }
        if appended_in_pass == 0 {
            return Ok(());
        }
    }
    Ok(())
}

/// Fresh release-published mirror cell seeded from the tracker's current visible version
/// (BS2.2; constructed after recovery so the seed covers the recovered frontier).
fn visible_version_mirror(visible: VisibleVersionTracker) -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(visible.visible_version().as_u64()))
}

/// Replay WAL records against the rebuilt catalog. Each record is routed
/// to its branch's slot in the catalog. After all records replay, advance
/// the allocator and the visibility tracker to the highest version
/// observed (across all branches, including the checkpoint watermark).
/// #2826: `true` when a replayed WAL record belongs to a DEAD predecessor
/// generation of its branch id — its version was allocated at or before the
/// current generation's creation point. `None` (a branch created before any
/// commit existed) fences nothing: no predecessor row can exist either.
pub(crate) fn record_predates_current_generation(
    record_version: CommitVersion,
    created_at: Option<CommitVersion>,
) -> bool {
    created_at.is_some_and(|created| record_version <= created)
}

/// #2850: the highest commit version a catalog descriptor durably references
/// — its `created_at` stamp, its fork anchor, and its deletion watermark.
/// The recovered allocator must resume ABOVE every such anchor, or the
/// restarted clock re-issues versions the catalog already attributes to
/// other content. `fork_version` is dominated by `created_at` on catalogs
/// stamped since #2832 (a child's creation point is at or above its fork
/// anchor) but stands alone for fork children recorded before the stamp
/// existed (v1.0.0-era databases), where `created_at` is `None`.
pub(crate) fn descriptor_version_anchor(
    descriptor: &crate::lifecycle::LifecycleBranchDescriptor,
) -> CommitVersion {
    descriptor
        .created_at()
        .unwrap_or(CommitVersion::ZERO)
        .max(descriptor.parent().map_or(
            CommitVersion::ZERO,
            crate::lifecycle::LifecycleBranchParent::fork_version,
        ))
        .max(descriptor.deleted_at().unwrap_or(CommitVersion::ZERO))
}

/// Everything a mid-replay durable flush needs (#3319 S3b), bundled so the
/// streamed replay can drain a branch's replayed backlog to disk-resident
/// tables — the maintenance flush runner's publish/install recipe, with the
/// manifest publish deferred to the post-open machinery.
struct ReplayFlushContext<'a> {
    table_object: &'a crate::service::TableObjectService<'static>,
    table_reader: &'a crate::service::TableObjectReaderService<'static>,
    table_catalog: &'a mut LifecycleDurableTableCatalog,
    budget: &'a StorageBudgetLedger,
    data_block_bytes: Option<u32>,
    table_compression: crate::format::TableCompression,
}

/// #3319 S3b: replay cannot refuse (recovery has no backpressure), so the
/// memory budget is enforced by DRAINING — past the budget-derived rotation
/// threshold the branch's replayed rows seal and flush to L0. A crash
/// between flushes is already sound: the flushed objects are unlisted by
/// any manifest, so the sweep reclaims them and the intact WAL replays
/// their rows again. Flush failures fail the open closed — an I/O refusal
/// at open time, where pre-S3b the same store OOM-killed instead.
fn flush_replayed_state_if_over_threshold(
    context: &mut ReplayFlushContext<'_>,
    branch_catalog: &mut LifecycleBranchCatalog,
    branch_id: strata_core::BranchId,
) -> LifecycleResult<()> {
    let generation = branch_catalog
        .registry()
        .lookup(branch_id)
        .map_err(commit_error)?
        .generation();
    let branch = branch_catalog
        .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
    // Replay's own appends rotate at the budget-derived threshold
    // (`branch/state/append.rs`) — but a table sealed at the FULL threshold
    // builds a flush artifact just over the generated-artifact pool derived
    // from the same budget (the #2541 tension, cache-mode's half-pool bound).
    // Replay therefore pre-rotates at HALF the threshold, so every sealed
    // table's artifact fits its own budget check, and drains whatever is
    // frozen immediately.
    if branch.active().approximate_size_bytes() >= branch.config().active_rotation_bytes() / 2 {
        branch.rotate_active();
    }
    if branch.frozen_table_count() == 0 {
        return Ok(());
    }
    let request = flush_drain_request_for_branch(branch_id)?;
    let table_catalog = &mut *context.table_catalog;
    let outcome = flush_branch_drain_with(branch, &request, |branch, request| {
        let outcome = flush_durable_branch_with_budget(
            branch,
            context.table_object,
            context.table_reader,
            request,
            Some(context.budget),
            context.data_block_bytes,
            context.table_compression,
        )?;
        // Record each flushed table in the durable catalog, but leave the
        // MANIFEST publish to the post-open machinery: a mid-recovery
        // publish walks the branch and refuses on the checkpoint-recovered
        // volatile base (#2855's class), and the catalog's
        // `manifest_publish_pending` debt is exactly the state the runtime
        // already reconciles after open. A crash before that publish is
        // sound: the unlisted objects are sweep fodder and the WAL replays
        // their rows again.
        if outcome.completed() {
            for table in outcome.tables() {
                let Some(object_facts) = table.object_facts() else {
                    continue;
                };
                table_catalog.record_table(table.table_identity().clone(), object_facts.clone())?;
            }
        }
        Ok(outcome.maintenance_outcome())
    })?;
    let maintenance = outcome.maintenance_outcome();
    if maintenance.status() == crate::lifecycle::MaintenanceOutcomeStatus::Failed {
        return Err(maintenance.source_error().cloned().unwrap_or(
            LifecycleError::MaintenanceTaskFailed {
                reason: "replay flush failed without a typed source",
            },
        ));
    }
    // The cached replay read view is untouched: its Arc-snapshot pins at
    // most the FIRST sealed memtable per branch (~half the rotation
    // threshold) — later memtables are new objects the view never captured,
    // so the flushed rows' memory is genuinely released.
    Ok(())
}

/// What the fused package validation decided for one recovered record.
enum RecoveredRecordDisposition {
    Replay,
    /// Legal but not replayable: a deleted branch's pre-deletion record, or
    /// one fenced by the #2826 generation check. Its version still counts
    /// (`replayed_max`, the ordering fence) — the versions were really
    /// allocated.
    Skip,
}

/// The former `validate_recovered_wal_package`, fused per record into the
/// streamed replay (#3319): unknown branch, deleted-branch resurrection, and
/// strict commit-version ordering — followed by the #2826 generation fence.
/// WAL records carry no generation, so a record for a branch id whose name
/// was deleted and re-created (or re-forked) is indistinguishable from the
/// current generation's by id alone — and replaying it resurrects
/// acknowledged-deleted rows. `created_at` is the globally visible version
/// when the CURRENT generation was created; the allocator is globally
/// monotonic, so every predecessor-generation record is `<= created_at` and
/// every own record is `> created_at`.
fn admit_recovered_record(
    branch_catalog: &LifecycleBranchCatalog,
    record: &WalRecord,
    previous: Option<CommitVersion>,
) -> LifecycleResult<RecoveredRecordDisposition> {
    let descriptor =
        branch_catalog
            .lookup(record.branch_id())
            .map_err(|_| LifecycleError::RecoveryFailed {
                reason: "recovered WAL package references an unknown branch",
            })?;
    let deleted = descriptor.status() == crate::lifecycle::LifecycleBranchStatus::Deleted;
    if deleted && !deleted_branch_allows_recovered_version(descriptor, record.commit_version()) {
        return Err(LifecycleError::RecoveryFailed {
            reason: "recovered WAL package references a deleted branch",
        });
    }
    if previous.is_some_and(|previous| record.commit_version() <= previous) {
        return Err(LifecycleError::RecoveryFailed {
            reason: "recovered WAL package must be strictly ordered",
        });
    }
    if deleted
        || record_predates_current_generation(record.commit_version(), descriptor.created_at())
    {
        return Ok(RecoveredRecordDisposition::Skip);
    }
    Ok(RecoveredRecordDisposition::Replay)
}

/// Replays one admitted record into its branch's catalog slot, reusing the
/// per-branch read view (see the capture rationale at the call site).
#[allow(
    clippy::too_many_arguments,
    reason = "the streamed replay's per-record inputs, spelled explicitly"
)]
fn replay_admitted_record<S>(
    branch_catalog: &mut LifecycleBranchCatalog,
    commit_config: &crate::commit::CommitRuntimeConfig,
    allocator: &mut CommitFactAllocator<S>,
    visible: &mut VisibleVersionTracker,
    durable_gate: &CommitUnresolvedDurableGate,
    durability: CommitDurabilityClass,
    branch_views: &mut Vec<(strata_core::BranchId, crate::branch::read::BranchReadView)>,
    record: &WalRecord,
) -> LifecycleResult<crate::commit::CommitReplayReport> {
    let branch_id = record.branch_id();
    let generation = branch_catalog
        .registry()
        .lookup(branch_id)
        .map_err(commit_error)?
        .generation();
    let target_branch = branch_catalog
        .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
    if !branch_views.iter().any(|(cached, _)| *cached == branch_id) {
        branch_views.push((
            branch_id,
            target_branch.capture_read_view().map_err(branch_error)?,
        ));
    }
    // Rationale: the entry was just inserted above when absent.
    let read_view = branch_views
        .iter()
        .find_map(|(cached, view)| (*cached == branch_id).then_some(view))
        .expect("replay read view present after insert");
    let replay = CommitReplayRequest::new(record.clone(), durability);
    CommitReplayRuntime::new(
        commit_config,
        allocator,
        target_branch,
        visible,
        durable_gate,
    )
    .replay_with_view(&replay, read_view)
    .map_err(commit_error)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the streamed replay adds the WAL service to the explicit inputs"
)]
fn replay_wal_into_catalog<S>(
    wal: &crate::service::WalService<'_>,
    flush_context: &mut ReplayFlushContext<'_>,
    branch_catalog: &mut LifecycleBranchCatalog,
    commit_config: &crate::commit::CommitRuntimeConfig,
    allocator: &mut CommitFactAllocator<S>,
    visible: &mut VisibleVersionTracker,
    durable_gate: &CommitUnresolvedDurableGate,
    recovery: &LifecycleRecoveryOutcome,
    durability: CommitDurabilityClass,
    checkpoint_watermark: CommitVersion,
) -> LifecycleResult<LifecycleRecoveryBootstrapReport> {
    let mut report = LifecycleRecoveryBootstrapReport::new(recovery.health().clone());
    let mut replayed_max = CommitVersion::ZERO;
    // One read view per branch for the WHOLE replay (replay-probe slice):
    // capture clones every owned-table reader handle — O(tables) — and a
    // per-record capture made replay O(records × tables) (~169ms/record at a
    // table-heavy close). A pre-replay capture classifies every later record
    // correctly: replayed commit versions are unique/ascending, so a
    // record's (key, version) row can only pre-exist from a pre-crash apply,
    // which the first capture observed. S3b's mid-replay rotation and flush
    // restructure the BRANCH, not the view: the view's Arc-snapshot handles
    // are immune to installs and removals beneath it, and replayed versions
    // are unique, so the classification stays exact.
    let mut branch_views: Vec<(strata_core::BranchId, crate::branch::read::BranchReadView)> =
        Vec::new();
    // #2567 S3a (#3319): the replay STREAMS the WAL tail (the recovered-WAL
    // package no longer carries it), and the package validation that used to
    // pre-walk the whole tail (`validate_recovered_wal_package`) runs fused,
    // per record, in the same pass: unknown branch, deleted-branch
    // resurrection, and strict commit-version ordering. A failed check
    // surfaces the same error it always has; partial in-memory replay before
    // it is discarded with the failed open, exactly as a mid-replay commit
    // failure always was.
    let replay_start = recovery.wal().replay_start();
    let replay_ceiling = recovery.wal().replay_ceiling();
    let mut previous: Option<CommitVersion> = None;
    let mut failure: Option<LifecycleError> = None;
    let mut replay_one = |record: &WalRecord| -> LifecycleResult<()> {
        replayed_max = replayed_max.max(record.commit_version());
        let disposition = admit_recovered_record(branch_catalog, record, previous)?;
        previous = Some(record.commit_version());
        if matches!(disposition, RecoveredRecordDisposition::Skip) {
            return Ok(());
        }
        let replay_report = replay_admitted_record(
            branch_catalog,
            commit_config,
            allocator,
            visible,
            durable_gate,
            durability,
            &mut branch_views,
            record,
        )?;
        report.record_replay(&replay_report);
        // #3319 S3b: bound the INSTALLED state too — past the budget-derived
        // rotation threshold the replayed rows seal and flush to L0.
        flush_replayed_state_if_over_threshold(flush_context, branch_catalog, record.branch_id())?;
        Ok(())
    };
    wal.visit_records_after(replay_start, &mut |record| {
        // The contiguity fence (recovering past a lost table-manifest base):
        // records above it are the orphaned tail recovery deliberately drops.
        if replay_ceiling.is_some_and(|ceiling| record.commit_version() > ceiling) {
            return std::ops::ControlFlow::Continue(());
        }
        match replay_one(record) {
            Ok(()) => std::ops::ControlFlow::Continue(()),
            Err(error) => {
                failure = Some(error);
                std::ops::ControlFlow::Break(())
            }
        }
    })
    .map_err(super::wal_error)?;
    if let Some(error) = failure {
        return Err(error);
    }
    // Fold in the highest committed version present in the restored branch states. Flushed
    // tables can be ahead of both the checkpoint watermark and the surviving WAL (e.g. a
    // checkpoint publish fault after a successful flush pruned the covering WAL segments);
    // without this term those durable, acknowledged rows would sit above `visible` — the
    // branch would reject all mutating commits (`require_branch_not_ahead_of_visible` fails
    // closed) and the allocator would re-issue their commit versions.
    let restored_catalog_max = branch_catalog
        .list_branches(false)
        .into_iter()
        .map(|descriptor| {
            Ok(branch_catalog
                .branch_state(descriptor.branch_id())?
                .max_commit_version()
                .unwrap_or(CommitVersion::ZERO))
        })
        .try_fold(
            CommitVersion::ZERO,
            |max, version: LifecycleResult<CommitVersion>| version.map(|version| max.max(version)),
        )?;
    // #2850: fold in the catalog's version-domain ANCHORS as well. Branch
    // lifecycle publishes are durably fenced, so the catalog (with
    // `created_at`, fork anchors, and deletion watermarks) can survive a
    // crash that sheds the WAL and every branch state — all three terms
    // above are then ZERO and the allocator would restart below versions
    // the catalog durably references. On the restarted clock every anchored
    // fact silently points at different content: the #2826/#2830 generation
    // fences eat legitimate new commits, and `rebuild_fork_snapshot_rows`
    // materializes the parent's NEW-clock slice into fork children. Deleted
    // descriptors count — a dead generation's anchors are still allocated
    // versions.
    let catalog_anchor_max = branch_catalog
        .list_branches(true)
        .into_iter()
        .map(|descriptor| descriptor_version_anchor(&descriptor))
        .fold(CommitVersion::ZERO, CommitVersion::max);
    let recovered_visible_version = checkpoint_watermark
        .max(replayed_max)
        .max(restored_catalog_max)
        .max(catalog_anchor_max);
    allocator.catch_up_to_recovered_version(recovered_visible_version);
    if let Some(timestamp) = recovery.checkpoint().timestamp_max() {
        allocator.catch_up_to_recovered_timestamp(timestamp);
    }
    let checkpoint_visible_publish = if recovered_visible_version > visible.visible_version() {
        Some(
            visible
                .catch_up_visible_after_replay(recovered_visible_version)
                .map_err(|error| LifecycleError::RecoveryVisibilityFailed {
                    recovered_visible_version,
                    reason: "recovered rows were installed but visibility catch-up failed",
                    source: Some(Arc::new(error)),
                })?,
        )
    } else {
        None
    };
    report.finish(checkpoint_visible_publish, recovered_visible_version);
    Ok(report)
}
