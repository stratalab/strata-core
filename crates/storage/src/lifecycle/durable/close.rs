//! Durable-local close orchestration.

use super::{commit_error, manifest_error, require_admitted, wal_error};
use crate::branch::state::BranchLocalState;
use crate::commit::{CommitBranchGuardSet, CommitRuntimeError, VisibleVersionTracker};
use crate::lifecycle::checkpoint::{
    checkpoint_durable_branch_with_budget,
    checkpoint_request_from_maintenance_task_with_snapshot_id, truncate_wal,
    wal_truncation_request_from_maintenance_task,
};
use crate::lifecycle::compaction::{
    compact_durable_branch, current_compaction_request_from_maintenance_task,
    materialization_request_from_maintenance_task, materialize_durable_branch,
    record_lifecycle_compaction_outcome, stale_compaction_maintenance_outcome,
};
use crate::lifecycle::durable::maintenance::{
    checkpoint_created_at, durable_quarantine_service_error, publish_table_manifest_after_flush,
    purge_branch_id_from_task,
};
use crate::lifecycle::flush::{
    flush_branch_drain_with, flush_drain_request_from_maintenance_task,
    flush_durable_branch_with_budget,
};
use crate::lifecycle::retention::{
    build_retention_proof, build_retention_proof_from_facts, prune_snapshots_with_proof,
    retention_outcome_for_delegated_families, retention_outcome_for_scope,
    retention_request_from_maintenance_task, LifecycleRetentionRequest, LifecycleRetentionScope,
    LifecycleRetentionStatus, LifecycleSnapshotPruningRequest,
};
use crate::lifecycle::{
    purge_proof_from_maintenance_task, purge_quarantine as purge_lifecycle_quarantine,
    quarantine_task_without_request, repair_branch_from_maintenance_task,
    repair_branch_quarantine as repair_branch_lifecycle_quarantine,
    repair_quarantine_family as repair_lifecycle_quarantine_family, require_rotate_budget,
    CloseOutcome, CloseOutcomeEffects, CloseOutcomeStatus, ClosePhase, LifecycleCloseFact,
    LifecycleCodecId, LifecycleDurableLocalRuntime, LifecycleDurableLocalServices, LifecycleError,
    LifecycleLowerLayer, LifecycleOperationKind, LifecycleResult, LifecycleState, LifecycleStats,
    LifecycleTransitionTrigger, MaintenanceOutcome, MaintenanceOutcomeStatus, MaintenanceTask,
    MaintenanceTaskKind, MaintenanceTaskRunner, RecoveryDegradationClass, RecoveryHealth,
    StorageBudgetLedger,
};
use strata_core::Timestamp;

impl<S> LifecycleDurableLocalRuntime<'_, S> {
    pub(crate) fn close(&mut self) -> LifecycleResult<CloseOutcome> {
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
                self.finish_close()
            }
            LifecycleState::Closing => {
                require_admitted(self.state, LifecycleOperationKind::CloseRetry)?;
                self.state
                    .transition(LifecycleTransitionTrigger::CloseRetried)?;
                self.finish_close()
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
                self.finish_close()
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
    fn finish_close(&mut self) -> LifecycleResult<CloseOutcome> {
        let cancel = self.maintenance.cancel_pending_for_close(self.state)?;
        let created_at = checkpoint_created_at(
            self.allocator.timestamp_guard().last_allocated(),
            self.recovered_checkpoint_timestamp_max,
        );
        let branch_id = self.initial_branch_id;
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        // A close-drained checkpoint publishes through the single-branch
        // collector below, so it must defer whenever any branch besides the
        // seeded one exists: a seeded-only snapshot advances the WAL-replay
        // floor (its watermark is the global visible version) past non-seeded
        // rows it does not carry — silent loss on the next open, and the exact
        // snapshot the multi-branch guard (`non_seeded_branch_has_durable_base`)
        // exists to prevent. Deferring is always safe at close: the rows stay
        // in the WAL and the next open replays them. Lifted together with the
        // guard by the per-branch fix tracked in
        // multi-branch-orphaned-delta-recovery-gap.md.
        let non_seeded_branches_present = self
            .branch_catalog
            .list_branches(false)
            .iter()
            .any(|descriptor| descriptor.branch_id() != branch_id);
        let mut runner = DurableCloseMaintenanceRunner {
            branch: self.branch_catalog.branch_state_mut(
                branch_id,
                crate::commit::CommitBranchGenerationGuard::exact(generation),
            )?,
            non_seeded_branches_present,
            services: &self.services,
            guard_set: &self.guard_set,
            visible: &self.visible,
            created_at,
            next_snapshot_id: &mut self.next_checkpoint_snapshot_id,
            health: self.current_recovery_health.clone(),
            budget: &self.budget,
            table_catalog: &mut self.table_catalog,
            data_block_bytes: self.open_plan.lifecycle_config().data_block_bytes(),
            table_compression: self.open_plan.lifecycle_config().table_compression(),
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
        let drain = match self.maintenance.drain_for_close(self.state, &mut runner) {
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
        drop(runner);
        for health in &observed_health {
            self.record_recovery_health(Some(health));
        }

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
            active_tasks.saturating_add(drain.drained_tasks()),
        );
        // Snapshot the first-close outcome so subsequent idempotent close
        // calls return the same stats. Without this cache, a retry after
        // Closed would surface a fabricated baseline that diverges from
        // what the caller observed on the first call. The opaque wrapper
        // keeps the close type out of the bootstrap source per layering.
        self.close_retry_state = Some(DurableCloseRetryState::new(outcome));
        Ok(outcome)
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

    #[cfg(test)]
    pub(crate) fn release_writer_guard_for_test(&mut self) -> bool {
        self.services.release_writer_guard()
    }
}

struct DurableCloseMaintenanceRunner<'a, 'b> {
    branch: &'a mut BranchLocalState,
    /// Branches other than the seeded one exist: the close-drained checkpoint
    /// must defer (see the construction site in `finish_close`).
    non_seeded_branches_present: bool,
    services: &'a LifecycleDurableLocalServices<'b>,
    guard_set: &'a CommitBranchGuardSet,
    visible: &'a VisibleVersionTracker,
    created_at: Timestamp,
    next_snapshot_id: &'a mut u64,
    health: RecoveryHealth,
    budget: &'a StorageBudgetLedger,
    table_catalog: &'a mut crate::lifecycle::LifecycleDurableTableCatalog,
    data_block_bytes: Option<u32>,
    table_compression: crate::format::TableCompression,
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
            MaintenanceTaskKind::Checkpoint => self.run_checkpoint(task),
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
            MaintenanceTaskKind::Compaction => {
                let Some(request) =
                    current_compaction_request_from_maintenance_task(task, self.branch)?
                else {
                    return Ok(stale_compaction_maintenance_outcome());
                };
                let compaction = compact_durable_branch(self.branch, &request)?;
                record_lifecycle_compaction_outcome(&compaction);
                Ok(compaction.maintenance_outcome())
            }
            MaintenanceTaskKind::Materialization => {
                let request = materialization_request_from_maintenance_task(task)?;
                Ok(materialize_durable_branch(self.branch, &request)?.maintenance_outcome())
            }
            MaintenanceTaskKind::SnapshotPruning | MaintenanceTaskKind::Retention => {
                self.run_retention(task)
            }
            MaintenanceTaskKind::Purge => self.run_purge(task),
            MaintenanceTaskKind::Repair => self.run_repair(task),
            MaintenanceTaskKind::Quarantine => Ok(quarantine_task_without_request()),
            MaintenanceTaskKind::HealthCollection => Ok(MaintenanceOutcome::new(
                MaintenanceTaskKind::HealthCollection,
                MaintenanceOutcomeStatus::Completed,
            )),
        }
    }
}

impl DurableCloseMaintenanceRunner<'_, '_> {
    fn run_checkpoint(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        if self.non_seeded_branches_present {
            return Ok(MaintenanceOutcome::new(
                MaintenanceTaskKind::Checkpoint,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason(
                "checkpoint deferred during close: non-seeded branches present, and a \
                 seeded-only snapshot would advance the replay floor past their rows",
            ));
        }
        let request = checkpoint_request_from_maintenance_task_with_snapshot_id(
            task,
            self.branch.branch_id(),
            self.services.manifest(),
            self.created_at,
            Some(*self.next_snapshot_id),
        )?;
        let table_catalog = &*self.table_catalog;
        let table_is_durable = |identity: &crate::table::TableIdentity| {
            table_catalog.object_for_identity(identity).is_some()
        };
        let outcome = checkpoint_durable_branch_with_budget(
            self.branch,
            self.services,
            self.guard_set,
            || self.visible.visible_version(),
            &request,
            Some(self.budget),
            &table_is_durable,
        )?;
        if let Some(snapshot_id) = outcome.snapshot_id() {
            *self.next_snapshot_id =
                snapshot_id
                    .checked_add(1)
                    .ok_or(LifecycleError::CheckpointPublicationFailed {
                        reason: "checkpoint snapshot id overflow",
                    })?;
        }
        Ok(outcome.maintenance_outcome())
    }

    fn run_wal_truncation(
        &mut self,
        task: &MaintenanceTask,
    ) -> LifecycleResult<MaintenanceOutcome> {
        let Some(request) =
            wal_truncation_request_from_maintenance_task(task, self.services.manifest())?
        else {
            return Ok(MaintenanceOutcome::new(
                MaintenanceTaskKind::WalTruncation,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason("WAL truncation has no retention proof")
            .with_deferral_reason(crate::lifecycle::MaintenanceDeferralReason::IncompleteProof));
        };
        Ok(truncate_wal(self.services.wal(), request)?.maintenance_outcome())
    }

    fn run_retention(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let request = retention_request_from_maintenance_task(task)?;
        if recovery_health_prevents_listing(&request, &self.health) {
            let proof = retention_proof_from_assembly(&request, self.services, &self.health);
            return match request.scope() {
                LifecycleRetentionScope::SnapshotObjects => {
                    let pruning = LifecycleSnapshotPruningRequest::new(
                        proof,
                        request.retain_newest_snapshots(),
                    )?;
                    Ok(
                        prune_snapshots_with_proof(self.services.snapshot(), &pruning)?
                            .maintenance_outcome(),
                    )
                }
                _ => Ok(retention_outcome_for_scope(&request, proof, &[])?.maintenance_outcome()),
            };
        }

        let manifest = self
            .services
            .manifest()
            .load_current()
            .map_err(manifest_error)?;
        let snapshot_count = self
            .services
            .snapshot()
            .list_snapshots()
            .map_err(snapshot_error)?
            .len();
        let proof =
            build_retention_proof(&request, manifest.as_ref(), &self.health, snapshot_count);
        match request.scope() {
            LifecycleRetentionScope::SnapshotObjects => {
                let pruning =
                    LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())?;
                Ok(
                    prune_snapshots_with_proof(self.services.snapshot(), &pruning)?
                        .maintenance_outcome(),
                )
            }
            LifecycleRetentionScope::Global => {
                let pruning = LifecycleSnapshotPruningRequest::new(
                    proof.clone(),
                    request.retain_newest_snapshots(),
                )?;
                let snapshot_outcome =
                    prune_snapshots_with_proof(self.services.snapshot(), &pruning)?;
                let retention_outcome = retention_outcome_for_delegated_families(proof)?;
                Ok(global_retention_maintenance_outcome(
                    &snapshot_outcome,
                    &retention_outcome,
                ))
            }
            _ => Ok(retention_outcome_for_delegated_families(proof)?.maintenance_outcome()),
        }
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

fn durable_close_outcome(canceled_tasks: usize, drained_tasks: usize) -> CloseOutcome {
    CloseOutcome::new(ClosePhase::Closed, CloseOutcomeStatus::Complete)
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
