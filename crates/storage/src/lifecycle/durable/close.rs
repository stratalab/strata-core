//! Durable-local close orchestration.

use super::{commit_error, manifest_error, require_admitted, wal_error};
use crate::branch::state::BranchLocalState;
use crate::commit::CommitRuntimeError;
use crate::lifecycle::checkpoint::{
    checkpoint_durable_runtime_with_budget, checkpoint_structural_deferral, truncate_wal,
    wal_truncation_request_from_maintenance_task, CheckpointStructuralDeferral,
    LifecycleCheckpointOutcome, LifecycleCheckpointRequest, LifecycleCheckpointStatus,
};
use crate::lifecycle::compaction::{
    materialization_request_from_maintenance_task, materialize_durable_branch,
};
use crate::lifecycle::durable::maintenance::{
    checkpoint_created_at, durable_quarantine_service_error, publish_table_manifest_after_flush,
    purge_branch_id_from_task, DurableTableObjectSweepRunner,
};
use crate::lifecycle::flush::{
    flush_branch_drain_with, flush_drain_request_from_maintenance_task,
    flush_durable_branch_with_budget,
};
use crate::lifecycle::retention::{
    build_retention_proof_for_manifest, build_retention_proof_from_facts,
    prune_snapshots_with_proof, retention_outcome_for_delegated_families,
    retention_outcome_for_scope, retention_request_from_maintenance_task,
    with_unconfirmed_manifest_error, LifecycleRetentionRequest, LifecycleRetentionScope,
    LifecycleRetentionStatus, LifecycleSnapshotPruningRequest,
};
use crate::lifecycle::{
    purge_proof_from_maintenance_task, purge_quarantine as purge_lifecycle_quarantine,
    repair_branch_from_maintenance_task,
    repair_branch_quarantine as repair_branch_lifecycle_quarantine,
    repair_quarantine_family as repair_lifecycle_quarantine_family, require_rotate_budget,
    CloseCheckpointDecision, CloseCheckpointReport, CloseCheckpointSkip, CloseOutcome,
    CloseOutcomeEffects, CloseOutcomeStatus, ClosePhase, LifecycleCloseFact, LifecycleCodecId,
    LifecycleDurableLocalRuntime, LifecycleDurableLocalServices, LifecycleError,
    LifecycleLowerLayer, LifecycleOperationKind, LifecycleResult, LifecycleState, LifecycleStats,
    LifecycleTransitionTrigger, MaintenanceOutcome, MaintenanceOutcomeStatus, MaintenanceTask,
    MaintenanceTaskKind, MaintenanceTaskRequest, MaintenanceTaskRunner, RecoveryDegradationClass,
    RecoveryHealth, StorageBudgetLedger,
};
use std::time::{Duration, Instant};
use strata_core::Timestamp;

/// Space-reclamation contract §3.1 (slice 3, #3596): how much wall-clock time
/// a clean close may spend draining the session's table-object reclaim debt
/// (mark → sweep → purge) before it hands the remainder to the next open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleCloseReclaimBudget {
    /// Skip the close-time reclaim drive entirely; the next open reconciles.
    Disabled,
    /// Run reclaim rounds while this budget has not elapsed.
    Bounded(Duration),
}

/// Space-reclamation contract §3.1 (slice 7): the single decision the close
/// path takes about its checkpoint — the budget gate first (a disabled budget
/// skips every close-time reclaim), then the registry's structural deferral,
/// then whether anything sits above the retention watermark.
/// #3625: the unflushed delta (active plus frozen memtable bytes) at or
/// above which a clean close flushes a branch into tables before its
/// checkpoint. Snapshot rows are stored uncompressed while tables are
/// Zstd-compressed, so a large delta is several times smaller as a table; a
/// small one stays in the snapshot, where a table's fixed overhead and one
/// more level-0 table per close would cost more than they save.
pub(crate) const CLOSE_FLUSH_MIN_DELTA_BYTES: u64 = 64 * 1024;

/// #3625: whether a clean close flushes a branch whose unflushed delta is
/// `unflushed_bytes` before the close checkpoint.
pub(crate) const fn close_flushes_branch(unflushed_bytes: u64, threshold: u64) -> bool {
    unflushed_bytes >= threshold
}

/// #3614: the unflushed delta at or above which the close's retry after a
/// delta-cap deferral flushes a branch — any row at all, since the retry
/// exists to bring the snapshot under the cap.
pub(crate) const CLOSE_RETRY_FLUSH_MIN_DELTA_BYTES: u64 = 1;

/// #3614: whether the close flushes every branch and retries its checkpoint
/// after the first attempt reported `status`. Only a delta-cap deferral
/// retries; the close calls this once, so it retries at most once.
pub(crate) const fn close_checkpoint_retries_after(status: LifecycleCheckpointStatus) -> bool {
    matches!(status, LifecycleCheckpointStatus::DeferredDeltaExceedsCap)
}

pub(crate) const fn close_checkpoint_decision(
    budget: LifecycleCloseReclaimBudget,
    structural: Option<CheckpointStructuralDeferral>,
    commits_since_checkpoint: u64,
) -> CloseCheckpointDecision {
    match (budget, structural, commits_since_checkpoint) {
        (LifecycleCloseReclaimBudget::Disabled, _, _) => {
            CloseCheckpointDecision::Skip(CloseCheckpointSkip::Disabled)
        }
        (LifecycleCloseReclaimBudget::Bounded(_), Some(deferral), _) => {
            CloseCheckpointDecision::Skip(CloseCheckpointSkip::Structural(deferral))
        }
        (LifecycleCloseReclaimBudget::Bounded(_), None, 0) => {
            CloseCheckpointDecision::Skip(CloseCheckpointSkip::NothingNew)
        }
        (LifecycleCloseReclaimBudget::Bounded(_), None, _) => CloseCheckpointDecision::Publish,
    }
}

impl LifecycleCloseReclaimBudget {
    /// The default close budget: enough for a session's ordinary debt, far
    /// below what an operator would notice at close.
    pub(crate) const DEFAULT: Self = Self::Bounded(Duration::from_millis(500));
}

impl Default for LifecycleCloseReclaimBudget {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// #3644: whether a clean close may START a queued reclaim task after
/// `elapsed` of its reclaim time: never under a disabled budget, and only
/// while a bounded budget has time left (so a zero budget starts nothing).
pub(crate) const fn close_starts_reclaim(
    budget: LifecycleCloseReclaimBudget,
    elapsed: Duration,
) -> bool {
    match budget {
        LifecycleCloseReclaimBudget::Disabled => false,
        LifecycleCloseReclaimBudget::Bounded(limit) => elapsed.as_nanos() < limit.as_nanos(),
    }
}

/// Whether the close-time reclaim drive runs another sweep → purge round. It
/// stops once the budget has elapsed, once the last sweep found nothing left
/// (`debt_remaining` is false), or once a sweep deferred behind a held read
/// view — a retry inside the same close resolves none of those. The first
/// round asks with `debt_remaining = true` and no deferral: a zero budget
/// therefore runs no round at all.
pub(crate) fn close_reclaim_should_continue(
    elapsed: Duration,
    budget: Duration,
    debt_remaining: bool,
    sweep_deferred: bool,
) -> bool {
    debt_remaining && !sweep_deferred && elapsed < budget
}

/// What the last close-time sweep found, read back by the drive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CloseSweepFacts {
    quarantined: usize,
    remaining: usize,
    deferred: bool,
}

impl<S> LifecycleDurableLocalRuntime<'_, S> {
    pub(crate) fn close(&mut self) -> LifecycleResult<CloseOutcome> {
        self.close_with_reclaim_budget(LifecycleCloseReclaimBudget::DEFAULT)
    }

    /// Close with an explicit close-time reclaim budget (space-reclamation
    /// contract §3.1, slice 3). `close` is this with the default budget.
    pub(crate) fn close_with_reclaim_budget(
        &mut self,
        reclaim_budget: LifecycleCloseReclaimBudget,
    ) -> LifecycleResult<CloseOutcome> {
        match self.state.state() {
            LifecycleState::Closed => {
                self.state
                    .transition(LifecycleTransitionTrigger::CloseRetried)?;
                // Return the prior final facts from the first close so
                // callers see the same canceled/drained/durable stats they
                // already observed, with only status/close_fact remapped to
                // idempotent shape.
                Ok(self.close_retry_state.as_ref().map_or_else(
                    durable_idempotent_close_outcome,
                    DurableCloseRetryState::idempotent_outcome,
                ))
            }
            LifecycleState::Open => {
                require_admitted(self.state, LifecycleOperationKind::Close)?;
                self.state
                    .transition(LifecycleTransitionTrigger::CloseRequested)?;
                self.finish_close(reclaim_budget)
            }
            LifecycleState::Closing => {
                require_admitted(self.state, LifecycleOperationKind::CloseRetry)?;
                self.state
                    .transition(LifecycleTransitionTrigger::CloseRetried)?;
                self.finish_close(reclaim_budget)
            }
            LifecycleState::Failed => {
                // Failed admits Close only when the prior failure was raised
                // during close-class work (i.e., `failure.failed_state ==
                // Closing`). State-machine admission below enforces this;
                // if the failure came from Open/Recovering, this returns
                // Rejected with a typed reason and we map it to
                // `InvalidLifecycleState`. The narrow rule prevents
                // silently retrying open-time failures through the close
                // ordering.
                require_admitted(self.state, LifecycleOperationKind::Close)?;
                self.state
                    .transition(LifecycleTransitionTrigger::CloseRequested)?;
                self.finish_close(reclaim_budget)
            }
            LifecycleState::New | LifecycleState::Opening | LifecycleState::Recovering => {
                Err(LifecycleError::InvalidLifecycleState {
                    reason: "durable runtime is not open for close",
                })
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "close drain orchestrates several phases that read cleaner inline than split"
    )]
    fn finish_close(
        &mut self,
        reclaim_budget: LifecycleCloseReclaimBudget,
    ) -> LifecycleResult<CloseOutcome> {
        let cancel = self.maintenance.cancel_pending_for_close(self.state)?;
        let created_at = checkpoint_created_at(
            self.allocator.timestamp_guard().last_allocated(),
            self.recovered_checkpoint_timestamp_max,
        );
        let branch_id = self.initial_branch_id;
        // Space-reclamation contract §3.1 (slice 3): the inline sweep's inputs,
        // captured before the runner takes its borrows of this runtime.
        let database_id = *self.services.assembly_facts().database_id();
        let codec_id = LifecycleCodecId::new(self.services.assembly_facts().codec_id())?;
        let retired_readers_alive = self.snapshot_publisher.retired_views_alive();
        let pinned_objects = self.reclaim_pinned_table_objects();
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        let mut runner = DurableCloseMaintenanceRunner {
            branch: self.branch_catalog.branch_state_mut(
                branch_id,
                crate::commit::CommitBranchGenerationGuard::exact(generation),
            )?,
            services: &self.services,
            created_at,
            health: self.current_recovery_health.clone(),
            budget: &self.budget,
            table_catalog: &mut self.table_catalog,
            data_block_bytes: self.open_plan.lifecycle_config().data_block_bytes(),
            table_compression: self.open_plan.lifecycle_config().table_compression(),
            database_id,
            codec_id,
            retired_readers_alive,
            pinned_objects,
            last_sweep: None,
        };
        let mut observed_health = Vec::new();
        let mut active_tasks = 0_usize;
        loop {
            let active = match self
                .maintenance
                .drain_active_for_close(self.state, &mut runner)
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.mark_close_retry_pending()?;
                    return Err(close_drain_error(error));
                }
            };
            let Some(outcome) = active else {
                break;
            };
            active_tasks = active_tasks.saturating_add(1);
            if let Some(health) = outcome.recovery_health() {
                observed_health.push(health.clone());
            }
        }
        // #3644: one clock for every reclaim task this close may start —
        // already-queued ones here, and the rounds the drive below seeds.
        // (Tasks that were already running above always finish: stopping
        // in-flight work part-way is not safe.)
        let reclaim_started = Instant::now();
        let drain = match self
            .maintenance
            .drain_for_close_within(self.state, &mut runner, || {
                close_starts_reclaim(reclaim_budget, reclaim_started.elapsed())
            }) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.mark_close_retry_pending()?;
                return Err(close_drain_error(error));
            }
        };
        for outcome in drain.outcomes() {
            if let Some(health) = outcome.recovery_health() {
                observed_health.push(health.clone());
            }
        }
        // Space-reclamation contract §3.1 (slice 3): the close-time reclaim
        // drive. Each round queues one sweep (which marks afresh inside) and,
        // when the sweep quarantined anything, one purge; both run through
        // this runner under the close lock. Best-effort by design: a refused
        // enqueue or a failing round ends the drive and the close proceeds —
        // the next open's reconcile owns whatever is left, and a failing
        // reclaim must never turn a clean close into a retry.
        let mut reclaim_drained = 0_usize;
        if let LifecycleCloseReclaimBudget::Bounded(limit) = reclaim_budget {
            let started = reclaim_started;
            let mut debt_remaining = true;
            let mut sweep_deferred = false;
            while close_reclaim_should_continue(
                started.elapsed(),
                limit,
                debt_remaining,
                sweep_deferred,
            ) {
                runner.last_sweep = None;
                let sweep = self
                    .maintenance
                    .enqueue_for_close(self.state, MaintenanceTaskRequest::quarantine())
                    .and_then(|_| self.maintenance.drain_for_close(self.state, &mut runner));
                let Ok(sweep) = sweep else {
                    break;
                };
                reclaim_drained = reclaim_drained.saturating_add(sweep.drained_tasks());
                for outcome in sweep.outcomes() {
                    if let Some(health) = outcome.recovery_health() {
                        observed_health.push(health.clone());
                    }
                }
                let Some(facts) = runner.last_sweep.take() else {
                    break;
                };
                if facts.quarantined > 0 {
                    let purge = self
                        .maintenance
                        .enqueue_for_close(
                            self.state,
                            MaintenanceTaskRequest::purge_quarantine(branch_id),
                        )
                        .and_then(|_| self.maintenance.drain_for_close(self.state, &mut runner));
                    let Ok(purge) = purge else {
                        break;
                    };
                    reclaim_drained = reclaim_drained.saturating_add(purge.drained_tasks());
                    for outcome in purge.outcomes() {
                        if let Some(health) = outcome.recovery_health() {
                            observed_health.push(health.clone());
                        }
                    }
                }
                debt_remaining = facts.remaining > 0;
                sweep_deferred = facts.deferred;
            }
        }
        drop(runner);
        for health in &observed_health {
            self.record_recovery_health(Some(health));
        }

        // Space-reclamation contract §3.1 (slice 7): the close-time checkpoint —
        // one multi-branch snapshot covering every branch's uncovered rows, the
        // covered WAL truncated behind it, the superseded snapshots pruned.
        // It runs before this close's own quiesce because the checkpoint takes
        // its own. Any refusal (a storage budget with no room for the
        // artifact, a snapshot id an orphan occupies — #3612, a commit still
        // in flight at the checkpoint's quiesce probe) leaves the WAL as the
        // durable record: the next open replays it, and the next session's
        // growth chain checkpoints. The close itself stays clean; a commit
        // still in flight is reported by this close's own quiesce below, and
        // the retry it triggers checkpoints once the commit has drained.
        let checkpoint = match self.close_checkpoint(reclaim_budget, created_at) {
            Ok(report) => report,
            Err(_) => CloseCheckpointReport::Refused,
        };

        let quiesce = match self.guard_set.try_begin_quiesce() {
            Ok(guard) => guard,
            Err(error) => {
                self.mark_close_retry_pending()?;
                return Err(match error {
                    crate::commit::CommitRuntimeError::CommitQuiesceUnavailable { .. } => {
                        close_timeout(ClosePhase::QuiesceCommits, "commit quiesce unavailable")
                    }
                    other => commit_error(other),
                });
            }
        };

        if self
            .durable_gate
            .unresolved()
            .map_err(commit_error)?
            .is_some()
        {
            drop(quiesce);
            self.mark_close_retry_pending()?;
            return Err(LifecycleError::CloseFailed {
                reason: "unresolved durable commit prevents clean close",
            });
        }

        if let Err(error) = self.services.wal_mut().close().map_err(wal_error) {
            drop(quiesce);
            self.mark_close_retry_pending()?;
            return Err(error);
        }

        if let Err(error) = self.force_final_manifest_fsync_on_health_change() {
            drop(quiesce);
            self.mark_close_retry_pending()?;
            return Err(error);
        }

        let guard_released = self.services.release_writer_guard();
        if !guard_released {
            drop(quiesce);
            self.mark_close_retry_pending()?;
            return Err(LifecycleError::CloseFailed {
                reason: "writer guard was already released before close completed",
            });
        }
        drop(quiesce);
        self.state
            .transition(LifecycleTransitionTrigger::CloseCompleted)?;
        let outcome = durable_close_outcome(
            cancel.canceled_tasks(),
            active_tasks
                .saturating_add(drain.drained_tasks())
                .saturating_add(reclaim_drained),
            checkpoint,
        );
        // Snapshot the first-close outcome so subsequent idempotent close
        // calls return the same stats. Without this cache, a retry after
        // Closed would surface a fabricated baseline that diverges from
        // what the caller observed on the first call. The opaque wrapper
        // keeps the close type out of the bootstrap source per layering.
        self.close_retry_state = Some(DurableCloseRetryState::new(outcome));
        Ok(outcome)
    }

    /// The close-time checkpoint (space-reclamation contract §3.1, slice 7).
    /// A publish that fails part-way is reported through its outcome and
    /// recovery health, and every error is the caller's `Refused` — the WAL a
    /// skipped or failed checkpoint did not truncate is replayed by the next
    /// open.
    fn close_checkpoint(
        &mut self,
        budget: LifecycleCloseReclaimBudget,
        created_at: strata_core::Timestamp,
    ) -> LifecycleResult<CloseCheckpointReport> {
        let structural =
            checkpoint_structural_deferral(&self.branch_catalog, self.initial_branch_id)?;
        let commits_since_checkpoint = self.wal_growth_commits_since_checkpoint()?;
        match close_checkpoint_decision(budget, structural, commits_since_checkpoint) {
            CloseCheckpointDecision::Publish => {}
            CloseCheckpointDecision::Skip(skip) => return Ok(CloseCheckpointReport::Skipped(skip)),
        }
        let visible_version = self.visible.visible_version();
        // Probe the quiesce before sealing anything: a commit still in flight
        // refuses this checkpoint (the checkpoint's own quiesce would refuse
        // the same way) and makes the close's own quiesce report the retry,
        // and that retry must not rotate the log again.
        drop(self.guard_set.try_begin_quiesce().map_err(commit_error)?);
        self.flush_deltas_before_close_checkpoint(CLOSE_FLUSH_MIN_DELTA_BYTES);
        // Reclaim rotation first (#3494): the snapshot below covers every
        // record in the active segment, so sealing it now lets the
        // checkpoint's own truncation pass release it, leaving an empty
        // active segment as the whole WAL. Sealing is safe even if the
        // checkpoint then defers or fails: the sealed segment is replayed.
        self.services
            .wal_mut()
            .rotate_active_segment_for_reclaim(visible_version)
            .map_err(wal_error)?;
        let request = LifecycleCheckpointRequest::new(
            self.initial_branch_id,
            self.next_checkpoint_snapshot_id,
            created_at,
        )?
        .with_wal_truncation_after_checkpoint(true)
        .with_delta_cap_bytes(self.checkpoint_delta_cap_bytes)?;
        let mut outcome = self.close_checkpoint_attempt(&request, visible_version)?;
        if close_checkpoint_retries_after(outcome.status()) {
            // #3614: the delta outgrew the cap despite the large-delta flush;
            // flush every branch that holds a row and retry once. A delta the
            // flush cannot shrink defers again and the WAL stays the record.
            self.flush_deltas_before_close_checkpoint(CLOSE_RETRY_FLUSH_MIN_DELTA_BYTES);
            outcome = self.close_checkpoint_attempt(&request, visible_version)?;
        }
        if let Some(snapshot_id) = outcome.snapshot_id() {
            self.next_checkpoint_snapshot_id =
                snapshot_id
                    .checked_add(1)
                    .ok_or(LifecycleError::CheckpointPublicationFailed {
                        reason: "checkpoint snapshot id overflow",
                    })?;
        }
        self.invalidate_retention_watermark_cache();
        // Not a queued task: the checkpoint and its truncation follow-up reach
        // the reclaim ledger through the inline entry point.
        self.maintenance
            .record_reclaim(&outcome.maintenance_outcome());
        if let Some(health) = outcome.recovery_health() {
            self.record_recovery_health(Some(health));
        }
        if outcome.status() == LifecycleCheckpointStatus::Completed {
            // Best-effort like the chained prune (slice 5): a prune that
            // cannot run leaves the superseded objects for the next open's
            // reconcile. Its health debt is recorded, never a close failure.
            let request = LifecycleRetentionRequest::snapshot_pruning(1)
                .with_snapshot_prune_mode(crate::service::SnapshotPruneMode::Superseded);
            match self.prune_snapshots_unadmitted(&request) {
                Ok(prune) => {
                    if let Some(health) = prune.recovery_health() {
                        self.record_recovery_health(Some(health));
                    }
                }
                Err(_) => {
                    // The static reason cannot fail validation; a prune error
                    // is health debt for the next open, never a close failure.
                    if let Ok(debt) =
                        crate::lifecycle::telemetry_health_debt("close-time snapshot prune failed")
                    {
                        self.record_recovery_health(Some(&debt));
                    }
                }
            }
        }
        Ok(CloseCheckpointReport::Attempted(outcome.status()))
    }

    /// One close-time checkpoint publish of every branch's uncovered rows.
    fn close_checkpoint_attempt(
        &self,
        request: &LifecycleCheckpointRequest,
        visible_version: strata_core::CommitVersion,
    ) -> Result<LifecycleCheckpointOutcome, LifecycleError> {
        let table_catalog = &self.table_catalog;
        let table_is_durable =
            |identity: &crate::table::TableIdentity| table_catalog.is_durable_base(identity);
        checkpoint_durable_runtime_with_budget(
            &self.branch_catalog,
            &self.services,
            &self.guard_set,
            || visible_version,
            request,
            self.initial_branch_id,
            Some(&self.budget),
            &table_is_durable,
        )
    }

    /// #3625: flush every branch whose unflushed delta is at least
    /// `threshold` bytes, so the close checkpoint snapshots only small deltas.
    /// Best-effort: a branch whose flush is refused or fails keeps its rows in
    /// the memtable, and the checkpoint below snapshots them as before; the
    /// failure is health debt, never a close failure.
    fn flush_deltas_before_close_checkpoint(&mut self, threshold: u64) {
        for branch_id in self.branch_catalog.registry().active_branch_ids() {
            let Ok(branch) = self.branch_catalog.branch_state(branch_id) else {
                continue;
            };
            let unflushed = branch
                .active_byte_count()
                .saturating_add(branch.frozen_byte_count());
            if !close_flushes_branch(unflushed, threshold) {
                continue;
            }
            let flushed = self
                .rotate_active_for_flush_unadmitted(branch_id)
                .and_then(|()| close_flush_request(branch_id))
                .and_then(|request| self.flush_frozen_unadmitted(&request));
            if flushed.is_err() {
                if let Ok(debt) = crate::lifecycle::telemetry_health_debt(
                    "close-time flush failed; the checkpoint snapshots the delta",
                ) {
                    self.record_recovery_health(Some(&debt));
                }
            }
        }
    }

    fn mark_close_retry_pending(&mut self) -> LifecycleResult<()> {
        if self.state.state() == LifecycleState::Closing {
            self.state
                .transition(LifecycleTransitionTrigger::CloseRetried)?;
        }
        Ok(())
    }

    /// Force a final manifest fsync if recovery health degraded during the
    /// session.
    ///
    /// V1 deliberately does **not** persist `LifecycleDurableLocalRuntime`'s
    /// in-memory `current_recovery_health` into the database manifest. The
    /// durable manifest format is frozen (gated by golden fixtures under
    /// `testdata/goldens/storage-format-v1/`) and carries only the
    /// recovery facts required to reconstruct visibility on next open:
    /// `database_id`, `codec_id`, `active_wal_segment`,
    /// `snapshot_watermark`, `snapshot_id`, `flushed_through_commit_id`.
    /// All session-observed degradation that this hook reacts to —
    /// quarantine inventory mismatches, partial publication windows,
    /// orphan snapshots — already lives on disk in inventory/snapshot
    /// state. Recovery on the next open re-walks that state and re-derives
    /// the same `RecoveryHealth` from scratch, so the health is durable by
    /// virtue of its source-of-truth facts, not by any new manifest field.
    ///
    /// What this hook _does_ do: when health changed, force one final
    /// `PublishMode::Replace` of the existing manifest bytes. The bytes are
    /// identical but the publish exercises the backend's full durable-write
    /// path, guaranteeing any pending `fdatasync` on the manifest file is
    /// flushed before close releases the writer guard. This is a tighten
    /// of close-time durability for the manifest specifically, not a
    /// health-persistence step.
    fn force_final_manifest_fsync_on_health_change(&self) -> LifecycleResult<()> {
        if self.current_recovery_health == *self.open_outcome.recovery_health() {
            return Ok(());
        }
        let manifest = self
            .services
            .manifest()
            .load_required()
            .map_err(manifest_error)?;
        self.services
            .manifest()
            .publish_current(&manifest)
            .map_err(manifest_error)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn record_recovery_health_for_test(&mut self, health: &RecoveryHealth) {
        self.record_recovery_health(Some(health));
    }

    /// The session's current recovery health, for tests that observe the
    /// debt a close-time step recorded.
    #[cfg(test)]
    pub(crate) const fn current_recovery_health_for_test(&self) -> &RecoveryHealth {
        &self.current_recovery_health
    }

    #[cfg(test)]
    pub(crate) fn release_writer_guard_for_test(&mut self) -> bool {
        self.services.release_writer_guard()
    }
}

/// A checkpoint task drained at close defers to the close's own checkpoint
/// (space-reclamation contract §3.1, slice 7), which runs right after the
/// drains through the multi-branch collector under the registry. The close
/// runner borrows the seeded branch alone, so it could only publish a
/// seeded-only snapshot — one whose watermark would advance the replay floor
/// past every other branch's rows. It never publishes.
fn close_drained_checkpoint_outcome() -> MaintenanceOutcome {
    MaintenanceOutcome::new(
        MaintenanceTaskKind::Checkpoint,
        MaintenanceOutcomeStatus::Deferred,
    )
    .with_reason("checkpoint deferred during close: the close publishes its own checkpoint")
}

/// #3678: the outcome of a compaction the close drain services (see the
/// runner's `Compaction` arm). A spelled-out `Result` rather than
/// `LifecycleResult`, so the mutation gate can write a constructible `Err`
/// replacement for it (#3337).
#[expect(
    clippy::unnecessary_wraps,
    reason = "the Result lets the mutation gate write a viable Err replacement (#3337)"
)]
fn close_deferred_compaction_outcome() -> Result<MaintenanceOutcome, LifecycleError> {
    Ok(MaintenanceOutcome::new(
        MaintenanceTaskKind::Compaction,
        MaintenanceOutcomeStatus::Deferred,
    )
    .with_reason("compaction is deferred during close"))
}

struct DurableCloseMaintenanceRunner<'a, 'b> {
    branch: &'a mut BranchLocalState,
    services: &'a LifecycleDurableLocalServices<'b>,
    created_at: Timestamp,
    health: RecoveryHealth,
    budget: &'a StorageBudgetLedger,
    table_catalog: &'a mut crate::lifecycle::LifecycleDurableTableCatalog,
    data_block_bytes: Option<u32>,
    table_compression: crate::format::TableCompression,
    /// Space-reclamation contract §3.1 (slice 3): what the inline close-time
    /// sweep needs from the runtime, captured before this runner borrows it.
    database_id: [u8; 16],
    codec_id: LifecycleCodecId,
    retired_readers_alive: bool,
    pinned_objects: super::maintenance::ReclaimPins,
    /// The last sweep's facts, read back by the close-time reclaim drive.
    last_sweep: Option<CloseSweepFacts>,
}

impl MaintenanceTaskRunner for DurableCloseMaintenanceRunner<'_, '_> {
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
                        let outcome = flush_durable_branch_with_budget(
                            branch,
                            self.services.table_object(),
                            self.services.table_reader(),
                            request,
                            Some(self.budget),
                            self.data_block_bytes,
                            self.table_compression,
                        )?;
                        let maintenance_outcome = outcome.maintenance_outcome();
                        if let Some(error) = publish_table_manifest_after_flush(
                            branch,
                            self.services.table_manifest(),
                            self.table_catalog,
                            Some(self.budget),
                            &outcome,
                        ) {
                            return Ok(crate::lifecycle::table_manifest_debt_outcome(
                                maintenance_outcome,
                                error,
                            ));
                        }
                        Ok(maintenance_outcome)
                    })?
                    .maintenance_outcome(),
                )
            }
            MaintenanceTaskKind::Checkpoint => Ok(close_drained_checkpoint_outcome()),
            MaintenanceTaskKind::FlushWatermark => Ok(MaintenanceOutcome::new(
                MaintenanceTaskKind::FlushWatermark,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason("flush watermark maintenance is deferred during close")),
            MaintenanceTaskKind::CachePreheat => Ok(MaintenanceOutcome::new(
                MaintenanceTaskKind::CachePreheat,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason("cache preheat is deferred during close")),
            MaintenanceTaskKind::WalTruncation => self.run_wal_truncation(task),
            // #3678: a compaction claimed before close began (in flight when
            // the workers stopped) is not finished here. Running it inline
            // would install output tables with no catalog entry and no table
            // manifest: the close checkpoint would treat them as volatile and
            // re-capture every row they hold (past the payload cap after a
            // bulk load, so the checkpoint defers and the WAL stays), and the
            // branch could no longer publish a table manifest. Compaction is
            // optional; its inputs stay durable, and the next session
            // recompacts. The abandoned build's objects are orphans the next
            // open reclaims.
            MaintenanceTaskKind::Compaction => close_deferred_compaction_outcome(),
            MaintenanceTaskKind::Materialization => {
                let request = materialization_request_from_maintenance_task(task)?;
                Ok(materialize_durable_branch(self.branch, &request)?.maintenance_outcome())
            }
            MaintenanceTaskKind::SnapshotPruning | MaintenanceTaskKind::Retention => {
                self.run_retention(task)
            }
            MaintenanceTaskKind::Purge => self.run_purge(task),
            MaintenanceTaskKind::Repair => self.run_repair(task),
            MaintenanceTaskKind::Quarantine => self.run_sweep(task),
            MaintenanceTaskKind::HealthCollection => Ok(MaintenanceOutcome::new(
                MaintenanceTaskKind::HealthCollection,
                MaintenanceOutcomeStatus::Completed,
            )),
        }
    }
}

impl DurableCloseMaintenanceRunner<'_, '_> {
    fn run_wal_truncation(
        &mut self,
        task: &MaintenanceTask,
    ) -> LifecycleResult<MaintenanceOutcome> {
        let manifest = self.services.manifest_gate().confirmed_manifest()?;
        let Some(request) = wal_truncation_request_from_maintenance_task(task, &manifest)? else {
            return Ok(with_unconfirmed_manifest_error(
                MaintenanceOutcome::new(
                    MaintenanceTaskKind::WalTruncation,
                    MaintenanceOutcomeStatus::Deferred,
                )
                .with_reason("WAL truncation has no retention proof")
                .with_deferral_reason(crate::lifecycle::MaintenanceDeferralReason::IncompleteProof),
                &manifest,
            ));
        };
        Ok(truncate_wal(self.services.wal(), request)?.maintenance_outcome())
    }

    fn run_retention(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let request = retention_request_from_maintenance_task(task)?;
        if recovery_health_prevents_listing(&request, &self.health) {
            let proof = retention_proof_from_assembly(&request, self.services, &self.health);
            return match request.scope() {
                LifecycleRetentionScope::SnapshotObjects => {
                    let pruning = LifecycleSnapshotPruningRequest::for_request(proof, &request)?;
                    Ok(prune_snapshots_with_proof(
                        self.services.snapshot(),
                        &pruning,
                        self.services
                            .timeline_segments_referenced_by(pruning.live_snapshot_id())
                            .as_ref(),
                    )?
                    .maintenance_outcome())
                }
                LifecycleRetentionScope::Global
                | LifecycleRetentionScope::TableObjects { .. }
                | LifecycleRetentionScope::WalObjects
                | LifecycleRetentionScope::QuarantineObjects => {
                    Ok(retention_outcome_for_scope(&request, proof, &[])?.maintenance_outcome())
                }
            };
        }

        let manifest = self.services.manifest_gate().confirmed_manifest()?;
        let snapshot_count = self
            .services
            .snapshot()
            .list_snapshots()
            .map_err(snapshot_error)?
            .len();
        let proof =
            build_retention_proof_for_manifest(&request, &manifest, &self.health, snapshot_count);
        let outcome = match request.scope() {
            LifecycleRetentionScope::SnapshotObjects => {
                let pruning = LifecycleSnapshotPruningRequest::for_request(proof, &request)?;
                Ok(prune_snapshots_with_proof(
                    self.services.snapshot(),
                    &pruning,
                    self.services
                        .timeline_segments_referenced_by(pruning.live_snapshot_id())
                        .as_ref(),
                )?
                .maintenance_outcome())
            }
            LifecycleRetentionScope::Global => {
                let pruning =
                    LifecycleSnapshotPruningRequest::for_request(proof.clone(), &request)?;
                let snapshot_outcome = prune_snapshots_with_proof(
                    self.services.snapshot(),
                    &pruning,
                    self.services
                        .timeline_segments_referenced_by(pruning.live_snapshot_id())
                        .as_ref(),
                )?;
                let retention_outcome = retention_outcome_for_delegated_families(proof)?;
                Ok(global_retention_maintenance_outcome(
                    &snapshot_outcome,
                    &retention_outcome,
                ))
            }
            _ => Ok(retention_outcome_for_delegated_families(proof)?.maintenance_outcome()),
        };
        outcome.map(|outcome| with_unconfirmed_manifest_error(outcome, &manifest))
    }

    fn run_purge(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let database_id = *self.services.assembly_facts().database_id();
        let codec_id = LifecycleCodecId::new(self.services.assembly_facts().codec_id())?;
        let branch_id = purge_branch_id_from_task(task, self.branch.branch_id())?;
        let inventory = self
            .services
            .quarantine()
            .load_inventory(branch_id, database_id, codec_id.as_str())
            .map_err(durable_quarantine_service_error)?;
        let (branch_id, proof) = purge_proof_from_maintenance_task(
            task,
            self.health.clone(),
            self.branch.branch_id(),
            inventory.token(),
        )?;
        Ok(purge_lifecycle_quarantine(
            self.services.quarantine(),
            branch_id,
            database_id,
            &codec_id,
            &proof,
        )?
        .maintenance_outcome())
    }

    /// Space-reclamation contract §3.1 (slice 3): the close-time sweep is the
    /// same inline runner the foreground path uses — a fresh table-object mark
    /// under the close lock, then staging of every unreachable object into
    /// quarantine, deferred while a retired read view is still held. Its
    /// facts are kept for the drive in `finish_close`.
    fn run_sweep(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let mut sweep = DurableTableObjectSweepRunner {
            services: self.services,
            branch_id: self.branch.branch_id(),
            health: self.health.clone(),
            database_id: self.database_id,
            codec_id: self.codec_id.clone(),
            staged_at: self.created_at,
            retired_readers_alive: self.retired_readers_alive,
            pinned_objects: self.pinned_objects.clone(),
            quarantined_objects: 0,
            staged_bytes: 0,
            remaining_candidates: 0,
            sweep_health: None,
        };
        let outcome = sweep.run_task(task)?;
        self.last_sweep = Some(CloseSweepFacts {
            quarantined: sweep.quarantined_objects,
            remaining: sweep.remaining_candidates,
            deferred: outcome.status() == MaintenanceOutcomeStatus::Deferred,
        });
        Ok(outcome)
    }

    fn run_repair(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let database_id = *self.services.assembly_facts().database_id();
        let codec_id = LifecycleCodecId::new(self.services.assembly_facts().codec_id())?;
        let branch_id = repair_branch_from_maintenance_task(task)?;
        let outcome = match branch_id {
            Some(branch_id) => repair_branch_lifecycle_quarantine(
                self.services.quarantine(),
                branch_id,
                database_id,
                &codec_id,
            )?,
            None => repair_lifecycle_quarantine_family(
                self.services.quarantine(),
                database_id,
                &codec_id,
            )?,
        };
        Ok(outcome.maintenance_outcome())
    }
}

fn durable_close_outcome(
    canceled_tasks: usize,
    drained_tasks: usize,
    checkpoint: CloseCheckpointReport,
) -> CloseOutcome {
    CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Complete)
        .with_checkpoint(checkpoint)
        .with_close_fact(LifecycleCloseFact::Complete)
        .with_close_effects(CloseOutcomeEffects::durable_complete(false))
        .with_stats(LifecycleStats::new(
            0,
            0,
            canceled_tasks.saturating_add(drained_tasks),
            0,
            1,
        ))
}

const fn durable_idempotent_close_outcome() -> CloseOutcome {
    CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Idempotent)
        .with_close_fact(LifecycleCloseFact::AlreadyClosed)
        .with_close_effects(CloseOutcomeEffects::durable_complete(true))
        .with_stats(LifecycleStats::new(0, 0, 0, 0, 1))
}

/// Opaque close-retry snapshot stored on the durable runtime by
/// `finish_close` so subsequent idempotent close calls return the
/// caller's observed stats. The wrapper exists so the runtime struct in
/// `lifecycle/durable/bootstrap.rs` does not directly reference any
/// `Close*` types — the lifecycle source guard enforces that
/// bootstrap and close stay decoupled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DurableCloseRetryState {
    prior: CloseOutcome,
}

impl DurableCloseRetryState {
    const fn new(prior: CloseOutcome) -> Self {
        Self { prior }
    }

    /// Build the durable idempotent retry shape from the cached first
    /// close. Stats are preserved verbatim from the first close; only
    /// status flips to `Idempotent` and the close fact to
    /// `AlreadyClosed`. The `prior_final` bit on the durable-complete
    /// effects satisfies `CloseOutcome::validate` for the Idempotent
    /// status.
    fn idempotent_outcome(&self) -> CloseOutcome {
        CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Idempotent)
            .with_close_fact(LifecycleCloseFact::AlreadyClosed)
            .with_close_effects(CloseOutcomeEffects::durable_complete(true))
            .with_stats(self.prior.stats())
    }
}

const fn close_timeout(phase: ClosePhase, reason: &'static str) -> LifecycleError {
    LifecycleError::CloseTimeout { phase, reason }
}

fn retention_proof_from_assembly(
    request: &LifecycleRetentionRequest,
    services: &LifecycleDurableLocalServices<'_>,
    health: &RecoveryHealth,
) -> crate::lifecycle::LifecycleRetentionProof {
    build_retention_proof_from_facts(
        request,
        services.assembly_facts().manifest_snapshot_id(),
        services.assembly_facts().manifest_snapshot_watermark(),
        services.assembly_facts().manifest_flush_watermark(),
        health,
        0,
    )
}

fn recovery_health_prevents_listing(
    request: &LifecycleRetentionRequest,
    health: &RecoveryHealth,
) -> bool {
    match health {
        RecoveryHealth::Healthy => false,
        RecoveryHealth::Degraded { class, .. } => match class {
            RecoveryDegradationClass::Telemetry => !request.allow_telemetry_degraded_recovery(),
            RecoveryDegradationClass::PolicyDowngrade => {
                !request.allow_telemetry_degraded_recovery()
                    || !retention_scope_is_telemetry_only(request.scope())
            }
            RecoveryDegradationClass::DataLoss => true,
        },
        RecoveryHealth::Failed { .. } => true,
    }
}

const fn retention_scope_is_telemetry_only(scope: LifecycleRetentionScope) -> bool {
    matches!(
        scope,
        LifecycleRetentionScope::WalObjects
            | LifecycleRetentionScope::QuarantineObjects
            | LifecycleRetentionScope::TableObjects { .. }
    )
}

fn global_retention_maintenance_outcome(
    snapshot_outcome: &crate::lifecycle::LifecycleSnapshotPruningOutcome,
    retention_outcome: &crate::lifecycle::LifecycleRetentionOutcome,
) -> MaintenanceOutcome {
    let status = if !snapshot_outcome.completed()
        || matches!(
            retention_outcome.status(),
            LifecycleRetentionStatus::DeferredIncompleteProof
                | LifecycleRetentionStatus::DeferredUnsupportedScope
                | LifecycleRetentionStatus::BlockedByRecoveryHealth
        ) {
        MaintenanceOutcomeStatus::Deferred
    } else {
        MaintenanceOutcomeStatus::Completed
    };
    let mut names = snapshot_outcome
        .deleted()
        .iter()
        .map(|snapshot| snapshot.object().to_string())
        .collect::<Vec<_>>();
    names.extend(
        snapshot_outcome
            .protected()
            .iter()
            .map(|snapshot| snapshot.object().to_string()),
    );
    names.extend(
        snapshot_outcome
            .failed()
            .iter()
            .map(|failure| failure.snapshot().object().to_string()),
    );
    names.extend(
        retention_outcome
            .decisions()
            .iter()
            .filter_map(|decision| decision.object().map(ToString::to_string)),
    );
    let recovery_health = snapshot_outcome
        .recovery_health()
        .cloned()
        .or_else(|| retention_outcome.recovery_health().cloned());
    let mut outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Retention, status)
        .with_affected_object_names(names)
        .with_state_changes(snapshot_outcome.deleted().len())
        .with_stats(LifecycleStats::new(
            0,
            recovery_health
                .as_ref()
                .map_or(0, RecoveryHealth::fault_count),
            1,
            usize::from(status != MaintenanceOutcomeStatus::Completed),
            0,
        ));
    if let Some(health) = recovery_health {
        outcome = outcome.with_recovery_health(health);
    }
    if status == MaintenanceOutcomeStatus::Deferred {
        outcome = outcome.with_reason("retention proof is incomplete");
    }
    outcome
}

fn snapshot_error(error: crate::service::SnapshotServiceError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::Service,
        "snapshot service failed",
        error,
    )
}

fn close_drain_error(error: LifecycleError) -> LifecycleError {
    if let LifecycleError::LowerLayer {
        layer: LifecycleLowerLayer::CommitRuntime,
        source: Some(source),
        ..
    } = &error
    {
        if matches!(
            source.as_ref().downcast_ref::<CommitRuntimeError>(),
            Some(CommitRuntimeError::CommitQuiesceUnavailable { .. })
        ) {
            return close_timeout(
                ClosePhase::QuiesceCommits,
                "commit quiesce unavailable during maintenance drain",
            );
        }
    }
    error
}

/// The flush request a close-time flush (#3625) issues for `branch_id`.
fn close_flush_request(
    branch_id: strata_core::BranchId,
) -> LifecycleResult<crate::lifecycle::FlushFrozenRequest> {
    crate::lifecycle::FlushFrozenRequest::new(
        branch_id,
        None,
        crate::lifecycle::FlushTableIdentitySeed::new(format!("close-flush-{branch_id}"))?,
        crate::lifecycle::FlushTableObjectId::new(format!("close-flush-{branch_id}"))?,
    )
}
