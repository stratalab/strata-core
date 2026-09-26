//! Durable-local maintenance dispatch.

use super::bootstrap::{BranchPublishGuard, LifecycleDurableLocalRuntime};
use super::{branch_error, commit_error, require_admitted};
use crate::branch::read::{BranchInheritedLayer, BranchLayout};
use crate::branch::state::{BranchLocalState, BranchRotationOutcome};
use crate::commit::{
    CommitBranchGenerationGuard, CommitBranchGuardSet, CommitBranchRegistry, VisibleVersionTracker,
};
use crate::format::TableManifest;
use crate::lifecycle::checkpoint::{
    branch_has_unflushed_rows_at_or_below, checkpoint_durable_rows_with_budget,
    checkpoint_durable_runtime_with_budget,
    checkpoint_request_from_maintenance_task_with_snapshot_id, checkpoint_structural_deferral,
    persist_flush_watermark, persist_flush_watermark_with_table_manifest_proof,
    recovery_health_epoch, truncate_wal, wal_truncation_request_from_maintenance_task,
    CheckpointStructuralDeferral, LifecycleCheckpointOutcome, LifecycleCheckpointRequest,
    LifecycleFlushWatermarkOutcome, LifecycleFlushWatermarkProof,
    LifecycleTableManifestFlushCoverageProof, LifecycleWalTruncationOutcome,
};
use crate::lifecycle::compaction::{
    bind_materialization_task_for_enqueue, collect_storage_pressure_with_budget,
    compact_branch_to_fixed_point_with_resource_policy, compaction_score_key_for_task,
    current_compaction_request_from_maintenance_task_with_budget,
    defer_compaction_for_resource_policy, materialization_request_from_maintenance_task,
    record_lifecycle_compaction_outcome,
    record_lifecycle_table_rewrite_post_operation_score_with_budget,
    stale_compaction_maintenance_outcome, table_rewrite_outcome_allows_chain_resubmit,
    table_rewrite_outcome_was_flush_preempted, table_rewrite_score_key_for_branch_with_budget,
    table_rewrite_score_key_for_task_with_budget,
    table_rewrite_task_request_for_branch_with_budget, LifecycleCompactionScoreKey,
    LifecycleTableRewriteScoreKey,
};
use crate::lifecycle::flush::{
    flush_branch_drain_with, flush_drain_maintenance_outcome_for_scope,
    flush_drain_request_for_branch_from_maintenance_task, flush_durable_branch_with_budget,
    install_prepared_durable_flush, install_prepared_durable_flush_drain_with,
    prepare_durable_flush_drain_with_budget, PreparedDurableFlushDrain,
};
use crate::lifecycle::maintenance::{
    schedule_post_commit_maintenance as schedule_suggested_post_commit_maintenance,
    MAINTENANCE_COVERAGE_IDLE_ROUND_LIMIT,
};
use crate::lifecycle::retention::{
    build_retention_proof, build_retention_proof_from_facts, prune_snapshots_with_proof,
    retention_outcome_for_delegated_families, retention_outcome_for_scope,
    retention_request_from_maintenance_task, LifecycleRetentionOutcome, LifecycleRetentionRequest,
    LifecycleRetentionScope, LifecycleRetentionStatus, LifecycleSnapshotPruningOutcome,
    LifecycleSnapshotPruningRequest,
};
use crate::lifecycle::table_manifest::{
    persist_reserved_manifest, publish_table_manifest_for_branch_with_budget,
    table_manifest_debt_outcome,
};
use crate::lifecycle::table_reachability::{
    table_object_retention_outcome, LifecycleTableObjectInventoryEntry,
    LifecycleTableObjectProofEpochs, LifecycleTableObjectRetentionRequest,
};
use crate::lifecycle::{
    begin_durable_materialization_build, commits_since_checkpoint,
    compact_durable_branch_manifest_backed, evaluate_mutating_write_admission,
    install_prepared_durable_compaction_without_publish,
    install_prepared_durable_materialization_without_publish,
    materialize_durable_branch_manifest_backed, policy_admission_error,
    prepare_durable_compaction_publication, purge_proof_from_maintenance_task,
    purge_quarantine as purge_lifecycle_quarantine,
    quarantine_object as quarantine_lifecycle_object, repair_branch_from_maintenance_task,
    repair_branch_quarantine as repair_branch_lifecycle_quarantine,
    repair_quarantine_family as repair_lifecycle_quarantine_family,
    require_maintenance_enqueue_budget, require_rotate_budget, telemetry_health_debt,
    wal_retention_watermark, DurableMaterializationBegin, DurableMaterializationBuild,
    FlushFrozenOutcome, FlushFrozenRequest, LifecycleCachePreheatPolicy, LifecycleCodecId,
    LifecycleCompactionDrainOutcome, LifecycleCompactionDrainRequest, LifecycleCompactionIoPolicy,
    LifecycleCompactionOutcome, LifecycleCompactionRequest, LifecycleError, LifecycleLowerLayer,
    LifecycleMaintenanceSchedulingPolicy, LifecycleMaterializationOutcome,
    LifecycleMaterializationRequest, LifecycleOperationKind, LifecyclePostCommitMaintenanceOutcome,
    LifecyclePurgeOutcome, LifecycleQuarantineOutcome, LifecycleQuarantineRepairOutcome,
    LifecycleQuarantineRequest, LifecycleResult, LifecycleStats, LifecycleStoragePressure,
    LifecycleStoragePressureSeverity, LifecycleWalGrowthOutcome, MaintenanceCheckpointOptions,
    MaintenanceDeferralReason, MaintenanceEnqueueOutcome, MaintenanceExecutorStatus,
    MaintenanceOutcome, MaintenanceOutcomeStatus, MaintenanceTask, MaintenanceTaskId,
    MaintenanceTaskKind, MaintenanceTaskRequest, MaintenanceTaskRunner, MaintenanceTaskScope,
    PreparedDurableCompaction, PreparedDurableMaterialization, RecoveryDegradationClass,
    RecoveryHealth,
};
use crate::service::{
    QuarantineService, TableManifestService, TableManifestWrite, TableObjectReaderService,
    TableObjectService, WalGrowthFacts, WalRetentionProof, WalService,
};
use crate::table::{TableBlockCache, TableCacheTableId, TableIdentity};
use std::sync::Arc;
use strata_core::{BranchId, CommitVersion, Timestamp};

pub(crate) enum DurableBackgroundMaintenanceStep<'a> {
    Completed(Box<MaintenanceOutcome>),
    Build(Box<DurableBackgroundMaintenanceBuild<'a>>),
    /// A pending flush-watermark coverage computation: the O(rows) coverage scan runs
    /// off the runtime lock on the captured snapshot, then the result is applied under
    /// the lock (D.2b-2). Inputs are fully owned so the value survives the
    /// lock release.
    FlushWatermarkCompute(Box<FlushWatermarkCoverageInputs>),
    /// BS5.5: a marked table-object sweep whose per-object staging I/O
    /// (quarantine publish + source delete, several fsyncs each) runs off the
    /// runtime lock. The mark ran under the lock; unreachability is monotone
    /// (table identities are never reused), so staging cannot race a build.
    SweepStage(Box<SweepStageInputs>),
    /// BS5.5: a quarantine purge whose inventory load and object deletes run
    /// off the runtime lock — purge only touches already-quarantined objects.
    PurgeStage(Box<PurgeStageInputs>),
    /// C2: a block-cache preheat chunk whose table reads (the bulk IO) run
    /// off the runtime lock. The layout snapshots are immutable Arcs and the
    /// cache is internally synchronized, so staging cannot race a build.
    PreheatStage(Box<PreheatStageInputs>),
}

impl DurableBackgroundMaintenanceStep<'_> {
    pub(crate) fn completed(outcome: MaintenanceOutcome) -> Self {
        Self::Completed(Box::new(outcome))
    }
}

/// Inputs captured under the runtime lock for an off-lock flush-watermark coverage
/// scan (D.2b-2). The owned durable-layout snapshot (`owned_levels`) plus the
/// inherited layers let the O(rows) coverage scan run with the lock released;
/// `min_unflushed_commit` lets candidate selection match the under-lock behavior
/// off-lock (the apply step re-checks the *current* memtable under the lock, which is
/// the anti-corruption gate). Every field is owned so the value crosses the
/// lock-release boundary.
pub(crate) struct FlushWatermarkCoverageInputs {
    task: MaintenanceTask,
    branch_id: BranchId,
    owned_levels: Arc<BranchLayout>,
    inherited_layers: Vec<BranchInheritedLayer>,
    table_manifest: TableManifest,
    floor: CommitVersion,
    recovery_health_epoch: u64,
    task_candidate: Option<CommitVersion>,
    candidates: Vec<CommitVersion>,
    min_unflushed_commit: Option<CommitVersion>,
}

/// B2/W1: the off-lock flush-drain build step (extracted for line-count).
fn build_flush_drain(
    branch_snapshot: &BranchLocalState,
    table_object: &TableObjectService<'static>,
    table_reader: &TableObjectReaderService<'static>,
    request: &crate::lifecycle::flush::FlushDrainRequest,
    budget: &crate::lifecycle::StorageBudgetLedger,
    data_block_bytes: Option<u32>,
    table_compression: crate::format::TableCompression,
    inflight: &super::inflight::InFlightTableOutputs,
) -> LifecycleResult<crate::lifecycle::flush::PreparedDurableFlushDrain> {
    prepare_durable_flush_drain_with_budget(
        branch_snapshot,
        table_object,
        table_reader,
        request,
        Some(budget),
        data_block_bytes,
        table_compression,
        Some(inflight),
    )
}

pub(crate) enum DurableBackgroundMaintenanceBuild<'a> {
    Flush {
        task: MaintenanceTask,
        branch_id: BranchId,
        request: crate::lifecycle::flush::FlushDrainRequest,
        branch_snapshot: BranchLocalState,
        table_object: TableObjectService<'static>,
        table_reader: TableObjectReaderService<'static>,
        budget: crate::lifecycle::StorageBudgetLedger,
        data_block_bytes: Option<u32>,
        table_compression: crate::format::TableCompression,
        started_at: std::time::Instant,
        inflight: super::inflight::InFlightTableOutputs,
    },
    Checkpoint {
        task: MaintenanceTask,
        request: LifecycleCheckpointRequest,
        visible_version: CommitVersion,
        /// Each branch's cloned state plus the identities of its owned tables
        /// that were DURABLY cataloged at start time (#2863) — the off-lock
        /// build has no catalog access, so the durability facts ride along.
        branches: Vec<(
            BranchLocalState,
            std::collections::HashSet<crate::table::TableIdentity>,
        )>,
    },
    WalTruncation {
        task: MaintenanceTask,
        proof: WalRetentionProof,
        wal: WalService<'a>,
    },
    Compaction {
        task: MaintenanceTask,
        branch_id: BranchId,
        level: u8,
        request: LifecycleCompactionRequest,
        branch_snapshot: BranchLocalState,
        table_object: TableObjectService<'static>,
        table_reader: TableObjectReaderService<'static>,
        budget: crate::lifecycle::StorageBudgetLedger,
        inflight: super::inflight::InFlightTableOutputs,
    },
    Materialization {
        task: MaintenanceTask,
        branch_id: BranchId,
        build: DurableMaterializationBuild,
        table_object: TableObjectService<'static>,
        table_reader: TableObjectReaderService<'static>,
        budget: crate::lifecycle::StorageBudgetLedger,
        inflight: super::inflight::InFlightTableOutputs,
    },
}

pub(crate) enum DurableBackgroundMaintenanceBuilt {
    Flush {
        task: MaintenanceTask,
        branch_id: BranchId,
        prepared: PreparedDurableFlushDrain,
        elapsed: std::time::Duration,
    },
    Checkpoint {
        task: MaintenanceTask,
        request: LifecycleCheckpointRequest,
        visible_version: CommitVersion,
        rows: Vec<crate::row::StorageRow>,
        has_durable_rows: bool,
        flush_boundary: Option<CommitVersion>,
        /// W3.1b: per-branch retained-timeline groups (present only for
        /// branches whose index is provably complete at the watermark).
        timeline_groups: Vec<crate::format::SnapshotTimelineBranchGroup>,
    },
    WalTruncation {
        task: MaintenanceTask,
        outcome: LifecycleWalTruncationOutcome,
    },
    Compaction {
        task: MaintenanceTask,
        branch_id: BranchId,
        level: u8,
        prepared: Box<PreparedDurableCompaction>,
    },
    Materialization {
        task: MaintenanceTask,
        branch_id: BranchId,
        prepared: Box<PreparedDurableMaterialization>,
    },
}

impl DurableBackgroundMaintenanceBuild<'_> {
    pub(crate) const fn task(&self) -> MaintenanceTask {
        match self {
            Self::Flush { task, .. }
            | Self::Checkpoint { task, .. }
            | Self::WalTruncation { task, .. }
            | Self::Compaction { task, .. }
            | Self::Materialization { task, .. } => *task,
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one arm per maintenance kind; splitting the dispatch obscures the build/publish pairing"
    )]
    pub(crate) fn build(self) -> LifecycleResult<DurableBackgroundMaintenanceBuilt> {
        match self {
            Self::Flush {
                task,
                branch_id,
                request,
                branch_snapshot,
                table_object,
                table_reader,
                budget,
                data_block_bytes,
                table_compression,
                started_at,
                inflight,
            } => Ok(DurableBackgroundMaintenanceBuilt::Flush {
                task,
                branch_id,
                prepared: build_flush_drain(
                    &branch_snapshot,
                    &table_object,
                    &table_reader,
                    &request,
                    &budget,
                    data_block_bytes,
                    table_compression,
                    &inflight,
                )?,
                elapsed: started_at.elapsed(),
            }),
            Self::Checkpoint {
                task,
                request,
                visible_version,
                branches,
            } => {
                let mut rows = Vec::new();
                let mut has_durable_rows = false;
                let mut flush_boundary: Option<CommitVersion> = None;
                let mut timeline_groups = Vec::new();
                for (branch, durable_identities) in &branches {
                    if let Some(group) = crate::lifecycle::checkpoint::timeline_group_for_branch(
                        branch,
                        visible_version,
                    ) {
                        timeline_groups.push(group);
                    }
                    let (mut branch_rows, branch_has_durable, branch_boundary) =
                        crate::lifecycle::checkpoint::branch_checkpoint_collection(
                            branch,
                            visible_version,
                            &|identity| durable_identities.contains(identity),
                        )?;
                    has_durable_rows |= branch_has_durable;
                    if let Some(boundary) = branch_boundary {
                        flush_boundary = Some(flush_boundary.map_or(boundary, |f| f.max(boundary)));
                    }
                    rows.append(&mut branch_rows);
                }
                Ok(DurableBackgroundMaintenanceBuilt::Checkpoint {
                    task,
                    request,
                    visible_version,
                    rows,
                    has_durable_rows,
                    flush_boundary,
                    timeline_groups,
                })
            }
            Self::WalTruncation { task, proof, wal } => {
                let outcome = truncate_wal(&wal, proof)?;
                Ok(DurableBackgroundMaintenanceBuilt::WalTruncation { task, outcome })
            }
            Self::Compaction {
                task,
                branch_id,
                level,
                request,
                branch_snapshot,
                table_object,
                table_reader,
                budget,
                inflight,
            } => Ok(DurableBackgroundMaintenanceBuilt::Compaction {
                task,
                branch_id,
                level,
                prepared: Box::new(prepare_durable_compaction_publication(
                    &branch_snapshot,
                    &table_object,
                    &table_reader,
                    &request,
                    Some(&budget),
                    Some(&inflight),
                )?),
            }),
            Self::Materialization {
                task,
                branch_id,
                build,
                table_object,
                table_reader,
                budget,
                inflight,
            } => Ok(DurableBackgroundMaintenanceBuilt::Materialization {
                task,
                branch_id,
                prepared: Box::new(build.build(
                    &table_object,
                    &table_reader,
                    Some(&budget),
                    Some(&inflight),
                )?),
            }),
        }
    }
}

/// Outcome of the first locked phase of the three-phase background publish
/// ([`LifecycleDurableLocalRuntime::begin_publish_phase`]). Either the publish completed entirely
/// under the lock (`Done`), or an off-lock manifest fsync is staged (`OffLock`).
#[allow(
    clippy::large_enum_variant,
    reason = "transient drain-loop return consumed immediately; never stored in a collection, and \
              MaintenanceOutcome is passed by value throughout the maintenance layer"
)]
pub(crate) enum PreparedPublishStep<'a> {
    Done(LifecycleResult<MaintenanceOutcome>),
    OffLock(PreparedPublish<'a>),
}

/// A staged off-lock table-manifest publish. Carries everything the off-lock fsync needs
/// (`service`, `manifest`, `budget`) plus the per-branch publish-slot guard and the kind-specific
/// data needed to finish the task once the manifest is durable. The durable write runs in
/// [`PreparedPublish::persist_off_lock`] with the global runtime lock released; nothing in this
/// struct touches catalog or runtime state, so the off-lock step is free of shared mutable access.
pub(crate) struct PreparedPublish<'a> {
    task: MaintenanceTask,
    branch_id: BranchId,
    base_outcome: MaintenanceOutcome,
    manifest: TableManifest,
    /// The manifest's object names, collected at build time so the frontier advance in
    /// [`LifecycleDurableTableCatalog::confirm_reserved_manifest_published`] stays an O(1)
    /// `Arc` swap (#2553, BS5.3).
    manifest_objects: std::sync::Arc<std::collections::BTreeSet<crate::object::ObjectName>>,
    service: TableManifestService<'a>,
    budget: crate::lifecycle::StorageBudgetLedger,
    guard: BranchPublishGuard,
    post: PreparedPublishPost,
}

/// Kind-specific finish data carried across the off-lock fsync. Compaction and materialization
/// keep their full lifecycle outcome so manifest debt and post-install requeue decisions are
/// resolved against the persisted result in [`LifecycleDurableLocalRuntime::finish_publish_phase`].
enum PreparedPublishPost {
    Flush,
    Compaction {
        branch_id: BranchId,
        level: u8,
        /// Boxed: the outcome dominates the enum size (clippy
        /// `large_enum_variant`), and one post is built per publish.
        outcome: Box<LifecycleCompactionOutcome>,
    },
    Materialization {
        branch_id: BranchId,
        /// Boxed for the same reason as the compaction outcome.
        outcome: Box<LifecycleMaterializationOutcome>,
    },
}

impl PreparedPublish<'_> {
    /// Perform the durable manifest fsync with the global runtime lock released. This is the only
    /// step of the background publish that runs off-lock; it reads only owned fields (the manifest
    /// was built and its sequence reserved under the lock) and records the off-lock publish
    /// duration. The per-branch publish-slot guard keeps any concurrent same-branch publish out of
    /// the reserve→record window. Returns `self` (slot still held) plus the persist result for
    /// [`LifecycleDurableLocalRuntime::finish_publish_phase`].
    pub(crate) fn persist_off_lock(self) -> (Self, LifecycleResult<TableManifestWrite>) {
        let started = crate::observability::perf_trace::start_timer();
        let write_result = persist_reserved_manifest(
            &self.service,
            self.branch_id,
            &self.manifest,
            Some(&self.budget),
        );
        crate::observability::perf_trace::record_lifecycle_background_publish_offlock(
            crate::observability::perf_trace::timer_elapsed(started),
        );
        (self, write_result)
    }
}

impl<'a, S> LifecycleDurableLocalRuntime<'a, S> {
    #[allow(
        dead_code,
        reason = "durable maintenance tests and later dispatch use explicit active rotation"
    )]
    pub(crate) fn rotate_active_for_maintenance(
        &mut self,
    ) -> LifecycleResult<BranchRotationOutcome> {
        self.rotate_active_for_branch_for_maintenance(self.initial_branch_id)
    }

    /// Rotate any branch's active state into frozen. Used by maintenance
    /// flows that operate on non-seeded branches; the seeded entry point
    /// `rotate_active_for_maintenance` is preserved for compatibility
    /// with existing test callers.
    /// Flush-time rotation: seal the active table when the frozen pool
    /// affords it; skip the rotation (the flush then drains the existing
    /// frozen backlog, freeing the pool) when it does not. Errors only when
    /// the pool is exhausted with nothing frozen to flush — the one state
    /// where deferral cannot make progress.
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
                .map_err(branch_error)?,
        );
        match decision {
            crate::lifecycle::FlushRotationDecision::Rotate => {
                {
                    let branch = self.branch_catalog.branch_state_mut(
                        branch_id,
                        CommitBranchGenerationGuard::exact(generation),
                    )?;
                    branch.rotate_active();
                }
                // BS2.3: rotation sealed active into a frozen table; republish
                // the branch snapshot.
                self.publish_branch_snapshot(branch_id);
                Ok(())
            }
            crate::lifecycle::FlushRotationDecision::FlushBacklogWithoutRotation => Ok(()),
            crate::lifecycle::FlushRotationDecision::Defer(error) => Err(error),
        }
    }

    #[allow(
        dead_code,
        reason = "non-seeded rotation is exercised by multi-branch checkpoint tests"
    )]
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
                .map_err(branch_error)?,
        )?;
        let outcome = {
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            branch.rotate_active()
        };
        // BS2.3: rotation sealed active into a frozen table; republish the branch snapshot.
        self.publish_branch_snapshot(branch_id);
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete handler"
    )]
    pub(crate) fn flush_frozen(
        &mut self,
        request: &FlushFrozenRequest,
    ) -> LifecycleResult<FlushFrozenOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let branch_id = request.branch_id();
        // Coordinate this foreground publish with the background drain's off-lock fsync via the
        // per-branch publish slot. The slot is busy only while a background publish for this same
        // branch is between sequence-reserve and record; defer rather than risk a concurrent
        // same-branch manifest fsync. Try-lock only — never block the global lock on this slot.
        let Some(_publish_guard) = self.try_acquire_branch_publish_guard(branch_id) else {
            return Ok(FlushFrozenOutcome::deferred(request));
        };
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
            flush_durable_branch_with_budget(
                branch,
                self.services.table_object(),
                self.services.table_reader(),
                request,
                Some(&self.budget),
                self.open_plan.lifecycle_config().data_block_bytes(),
                self.open_plan.lifecycle_config().table_compression(),
            )?
        };
        if publish_table_manifest_after_flush(
            self.branch_catalog
                .branch_state(branch_id)
                .map_err(branch_error)?,
            self.services.table_manifest(),
            &mut self.table_catalog,
            Some(&self.budget),
            &outcome,
        )
        .is_some()
        {
            if let Ok(health) = telemetry_health_debt("table manifest publication needs recovery") {
                self.record_recovery_health(Some(&health));
            }
        }
        // BS2.3: flush installed an L0 table (rows stay visible even if the manifest publish is
        // deferred); republish the branch snapshot.
        self.publish_branch_snapshot(branch_id);
        self.note_cache_preheat_trigger();
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete table rewrite hook"
    )]
    pub(crate) fn compact_branch_tables(
        &mut self,
        request: &LifecycleCompactionRequest,
    ) -> LifecycleResult<LifecycleCompactionOutcome> {
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
            compact_durable_branch_manifest_backed(
                branch,
                self.services.table_object(),
                self.services.table_reader(),
                self.services.table_manifest(),
                &mut self.table_catalog,
                request,
                Some(&self.budget),
                &self.sweep_staged_names,
            )
        };
        if let Ok(compaction) = &outcome {
            record_lifecycle_compaction_outcome(compaction);
            // Table-object GC: the foreground compaction dropped its input refs; enqueue the
            // coalescing mark (best-effort — a rejected enqueue defers reclaim to the next cycle).
            if !compaction.retained_input_objects().is_empty() {
                let _ = self
                    .enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch_id));
            }
        }
        outcome
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete table rewrite hook"
    )]
    pub(crate) fn compact_branch_tables_to_fixed_point(
        &mut self,
        request: &LifecycleCompactionDrainRequest,
    ) -> LifecycleResult<LifecycleCompactionDrainOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        // #3516: stamp the database's configured codec onto the drain so the
        // fixed-point rewrites are compressed like flush/followup/background
        // compaction (the explicit `maintenance(Compact)` path otherwise
        // defaulted to Uncompressed, regressing #3499/#3492).
        let stamped = request
            .clone()
            .with_table_compression(self.open_plan.lifecycle_config().table_compression());
        let request = &stamped;
        let branch_id = request.branch_id();
        // Hold the per-branch publish slot across this foreground compaction drain so its manifest
        // fsync cannot run concurrently with a background off-lock fsync for the same branch.
        let Some(_publish_guard) = self.try_acquire_branch_publish_guard(branch_id) else {
            let layout = self.branch_catalog.branch_state(branch_id)?.source_layout();
            return Ok(LifecycleCompactionDrainOutcome::deferred_for_publish_busy(
                branch_id, layout,
            ));
        };
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        let services = &self.services;
        let table_catalog = &mut self.table_catalog;
        let budget = &self.budget;
        let sweep_staged = &self.sweep_staged_names;
        let outcome = {
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            compact_branch_to_fixed_point_with_resource_policy(
                branch,
                request,
                self.open_plan.lifecycle_config().compaction_io_policy(),
                |branch, compaction| {
                    compact_durable_branch_manifest_backed(
                        branch,
                        services.table_object(),
                        services.table_reader(),
                        services.table_manifest(),
                        table_catalog,
                        compaction,
                        Some(budget),
                        sweep_staged,
                    )
                },
            )
        };
        // BS2.3: compaction promoted/rewrote the branch's tables; republish the branch snapshot.
        self.publish_branch_snapshot(branch_id);
        self.note_cache_preheat_trigger();
        // Table-object GC: a fixed-point drain that ran passes dropped input refs; enqueue the
        // coalescing mark (best-effort — a rejected enqueue defers reclaim to the next cycle).
        if outcome
            .as_ref()
            .is_ok_and(|drain| drain.input_tables_removed() > 0)
        {
            let _ =
                self.enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch_id));
        }
        outcome
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete table rewrite hook"
    )]
    pub(crate) fn materialize_inherited_layer(
        &mut self,
        request: &LifecycleMaterializationRequest,
    ) -> LifecycleResult<LifecycleMaterializationOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let branch_id = request.child_branch_id();
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
            materialize_durable_branch_manifest_backed(
                branch,
                self.services.table_object(),
                self.services.table_reader(),
                self.services.table_manifest(),
                &mut self.table_catalog,
                request,
                Some(&self.budget),
                &self.sweep_staged_names,
            )
        };
        outcome
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete pressure hook"
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
        collect_storage_pressure_with_budget(
            branch,
            self.maintenance.status(),
            Some(self.open_plan.lifecycle_config().storage_budget()),
        )
    }

    pub(super) fn evaluate_mutating_write_admission_for_branch(
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
            && outcome.status()
                == crate::lifecycle::LifecycleWriteAdmissionStatus::AcceptedUnderPressure
            && outcome.pressure().severity()
                == crate::lifecycle::LifecycleStoragePressureSeverity::Urgent
            && self.run_inline_admission_maintenance(outcome.pressure())
        {
            outcome = outcome.with_inline_maintenance_driven();
        }
        self.last_write_admission = Some(outcome);
        Ok(())
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
        // Defer optional (background) maintenance when the database-wide memory budget is
        // under pressure: with materialized readers, starting an optional flush/compaction
        // holds its inputs and output resident at once. This is one of the two durable
        // scheduling sites; the other is `schedule_maintenance_coverage_after_branch`, and a
        // deferral change must touch both. The runtime total is refreshed here so the global
        // pressure read reflects post-commit resident bytes (also covers the background
        // coverage scan this method triggers).
        self.refresh_runtime_memory_total();
        let global_pressure = self.budget.global_pressure();
        let pressure = self
            .storage_pressure_for_branch(branch_id)
            .deferred_under_global_memory_pressure(global_pressure);
        let outcome = schedule_suggested_post_commit_maintenance(policy, pressure, |request| {
            self.enqueue_maintenance(request)
        });
        // Compaction is orthogonal to the flush-first `suggested_task`: with frozen
        // memtables essentially always present under load, a backed-up level would never
        // be scheduled. Derive the eligible table-rewrites directly from the branch and
        // enqueue every eligible level independently; per-(branch, level) coalescing bounds
        // this to one task per level, and concurrent workers pick disjoint levels.
        if matches!(
            policy,
            LifecycleMaintenanceSchedulingPolicy::EvaluateAndEnqueue
                | LifecycleMaintenanceSchedulingPolicy::Background
        ) {
            let compactions = self
                .branch_catalog
                .branch_state(branch_id)
                .ok()
                .map(|branch| {
                    crate::lifecycle::compaction::eligible_compaction_tasks(
                        branch,
                        Some(self.open_plan.lifecycle_config().storage_budget()),
                        global_pressure,
                    )
                })
                .unwrap_or_default();
            for compaction in compactions {
                // Best-effort: a full queue or coalesce is non-fatal — the next commit
                // re-derives and re-enqueues.
                let _ = self.enqueue_maintenance(compaction);
            }
        }
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

    /// W3.3b: trickle-flush the WAL append buffer when its staged bytes have
    /// aged past the flush window. Called at the start of maintenance drains.
    /// Best-effort — a flush failure is swallowed here (rationale: the drain
    /// has no commit to fail; the buffer stays intact, later triggers retry,
    /// and the commit path's durability barriers own the error surface).
    pub(crate) fn flush_stale_wal_buffer(&mut self) -> bool {
        self.services.wal.flush_pending_if_stale().unwrap_or(false)
    }

    pub(crate) fn schedule_background_maintenance_coverage(&mut self) -> bool {
        // Coverage hysteresis: under saturation interlocks every task the
        // coverage scan generates can DEFER instantly, draining the queue
        // right back to empty — re-scheduling coverage on every empty-queue
        // observation then spins the drain generating-and-deferring tasks
        // (measured 312K enqueues/s across rounds, the churn holding the
        // runtime lock the deferred-upon flush/compaction needed to run).
        // Fire coverage only when a maintenance task has actually COMPLETED
        // since the last attempt; external enqueues bypass coverage entirely,
        // so nothing is lost while the queue is being fed.
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
        // Second of the two durable scheduling sites: defer optional maintenance under
        // global memory pressure. The runtime total was refreshed by the post-commit caller
        // (`schedule_post_commit_maintenance_for_branch`), so this global read is current.
        let global_pressure = self.budget.global_pressure();
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
            )
            .deferred_under_global_memory_pressure(global_pressure);
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
        let inline_start = std::time::Instant::now();
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
        let inline_start = std::time::Instant::now();
        let result = self.run_inline_maintenance_task(request, enqueue.task_id());
        crate::observability::perf_trace::record_lifecycle_inline_maintenance(
            inline_start.elapsed(),
        );
        result.is_ok()
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete checkpoint hook"
    )]
    pub(crate) fn checkpoint(
        &mut self,
        request: &LifecycleCheckpointRequest,
    ) -> LifecycleResult<LifecycleCheckpointOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let table_catalog = &self.table_catalog;
        let table_is_durable = |identity: &crate::table::TableIdentity| {
            table_catalog.object_for_identity(identity).is_some()
        };
        let outcome = checkpoint_durable_runtime_with_budget(
            &self.branch_catalog,
            &self.services,
            &self.guard_set,
            || self.visible.visible_version(),
            request,
            self.initial_branch_id,
            Some(&self.budget),
            &table_is_durable,
        )?;
        // A checkpoint advances the manifest snapshot watermark.
        self.invalidate_retention_watermark_cache();
        // Explicit (non-queued) checkpoint: its follow-up WAL truncation, if
        // any, reaches the reclaim ledger through the inline entry point.
        self.maintenance
            .record_reclaim(&outcome.maintenance_outcome());
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "explicit maintenance callers run checkpoints without queueing"
    )]
    pub(crate) fn checkpoint_for_explicit_maintenance(
        &mut self,
        branch_id: strata_core::BranchId,
        truncate_wal_after_checkpoint: bool,
    ) -> LifecycleResult<LifecycleCheckpointOutcome> {
        let created_at = checkpoint_created_at(
            self.allocator.timestamp_guard().last_allocated(),
            self.recovered_checkpoint_timestamp_max,
        );
        let request = LifecycleCheckpointRequest::new(
            branch_id,
            self.next_checkpoint_snapshot_id,
            created_at,
        )?
        .with_wal_truncation_after_checkpoint(truncate_wal_after_checkpoint);
        let outcome = self.checkpoint(&request)?;
        if let Some(snapshot_id) = outcome.snapshot_id() {
            self.next_checkpoint_snapshot_id =
                snapshot_id
                    .checked_add(1)
                    .ok_or(LifecycleError::CheckpointPublicationFailed {
                        reason: "checkpoint snapshot id overflow",
                    })?;
        }
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete watermark hook"
    )]
    pub(crate) fn persist_flush_watermark(
        &mut self,
        candidate: strata_core::CommitVersion,
        proof: &LifecycleFlushWatermarkProof,
    ) -> LifecycleResult<LifecycleFlushWatermarkOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let outcome = persist_flush_watermark(
            self.services.manifest(),
            self.visible.visible_version(),
            candidate,
            proof,
        )?;
        // A flush-watermark persist advances flushed_through_commit_id.
        self.invalidate_retention_watermark_cache();
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this table-manifest-backed watermark hook"
    )]
    pub(crate) fn persist_table_manifest_flush_watermark(
        &mut self,
        candidate: strata_core::CommitVersion,
    ) -> LifecycleResult<LifecycleFlushWatermarkOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let branch_id = self.initial_branch_id;
        let manifest = self
            .services
            .table_manifest()
            .load_current(branch_id)
            .map_err(manifest_error)?
            .ok_or(LifecycleError::WalRetentionProofIncomplete {
                reason: "table manifest flush proof requires durable table manifest",
            })?;
        let branch = self
            .branch_catalog
            .branch_state(branch_id)
            .expect("seeded branch is always present in the catalog");
        let current_manifest = self
            .services
            .manifest()
            .load_required()
            .map_err(manifest_error)?;
        let floor = wal_retention_watermark(
            current_manifest
                .snapshot_watermark()
                .map(CommitVersion::new),
            current_manifest.flushed_through_commit_id(),
        )
        .unwrap_or(CommitVersion::ZERO);
        let proof = LifecycleTableManifestFlushCoverageProof::from_branch_manifest_with_floor(
            candidate,
            branch,
            &manifest,
            &self.current_recovery_health,
            floor,
        )?;
        // Forward-compat guard. The current durable runtime opens exactly one
        // branch, so this check passes. If multi-branch runtimes land, the
        // proof construction above must be expanded to load every active
        // branch's manifest and per-branch state before this guard relaxes.
        let active_branches = self.branch_catalog.registry().active_branch_ids();
        if active_branches != vec![branch_id] {
            return Err(LifecycleError::WalRetentionProofIncomplete {
                reason: "table manifest flush proof requires all active branches to be loaded",
            });
        }
        let outcome = persist_flush_watermark_with_table_manifest_proof(
            self.services.manifest(),
            self.visible.visible_version(),
            candidate,
            &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
            proof.manifest_epoch(),
            proof.recovery_health_epoch(),
            &[(branch_id, manifest.manifest_sequence())],
        )?;
        // A flush-watermark persist advances flushed_through_commit_id.
        self.invalidate_retention_watermark_cache();
        Ok(outcome)
    }

    /// Advance the flush watermark to the highest L0-covered commit and enqueue WAL
    /// truncation, so retained WAL is reclaimed when a memtable reaches L0 rather than
    /// waiting for a checkpoint. Reuses the table-manifest coverage path
    /// (`highest_coverable_flush_watermark_candidate` + `persist_table_manifest_flush_watermark`),
    /// which validates against the `wal_retention_watermark` floor independently of checkpoint.
    ///
    /// Best-effort by design: a flush must never fail because reclaim could not advance. The
    /// watermark is monotone (the persist no-ops when the candidate is not above the persisted
    /// value) and the periodic WAL-growth policy remains the backstop, so a transient error here
    /// only defers reclaim to the next flush.
    fn reclaim_wal_after_flush(&mut self) {
        // BS5.3: this previously ran the coverage scan and the watermark
        // persist INLINE — two durable-manifest loads, an O(rows) coverage
        // proof, and a durable manifest replace (write + fsync) per flush,
        // all inside the publish phase's runtime-lock hold (measured as the
        // single largest writer-starvation source under sustained load:
        // ~950 ms of a 3 s window). Route through the coalescing background
        // flush-watermark task instead — its coverage scan runs with the lock
        // RELEASED (D.2b-2) — exactly as the periodic WAL-growth policy
        // already schedules reclaim. Both enqueues are best-effort: a full
        // queue only defers reclaim to that periodic backstop.
        let candidate = self.visible.visible_version();
        if candidate == CommitVersion::ZERO {
            // Nothing visible yet — no WAL below any watermark to reclaim.
            return;
        }
        // Same gate as the previous inline path: multi-branch runtimes reclaim
        // via checkpoint (a single branch's flush coverage cannot prove the
        // other branches' rows durable).
        if self.branch_catalog.registry().active_branch_ids() != vec![self.initial_branch_id] {
            return;
        }
        let _ = self.enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
            candidate,
        ));
        let _ = self.enqueue_maintenance(MaintenanceTaskRequest::wal_truncation());
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete truncation hook"
    )]
    pub(crate) fn truncate_wal(
        &mut self,
        proof: crate::service::WalRetentionProof,
    ) -> LifecycleResult<LifecycleWalTruncationOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        truncate_wal(self.services.wal(), proof)
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete retention hook"
    )]
    pub(crate) fn prove_retention(
        &mut self,
        request: &LifecycleRetentionRequest,
    ) -> LifecycleResult<LifecycleRetentionOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let health = self.current_recovery_health.clone();
        if let LifecycleRetentionScope::TableObjects { branch_id } = request.scope() {
            // #2553 rider: this entry previously omitted the in-flight output pins the other
            // mark sites carried; the unified helper closes that gap too.
            let pinned_objects = self.reclaim_pinned_table_objects();
            let table_request = table_object_retention_request(&self.services, branch_id, &health)?
                .with_pinned_objects(pinned_objects);
            let outcome = table_object_retention_outcome(&table_request)?;
            // The public Reclaim verb performs reclaim, not just a report: chain the sweep
            // (Quarantine → Purge) for the marked candidates. Best-effort, coalescing.
            if outcome.decisions().iter().any(|decision| {
                decision.decision() == crate::lifecycle::RetentionDecision::QuarantineCandidate
            }) {
                let _ = self.enqueue_maintenance(MaintenanceTaskRequest::quarantine());
            }
            // Inline verb: not a queued task, so record the mark into the
            // reclaim ledger here (branch scope = the table-object mark).
            self.maintenance.record_reclaim(
                &outcome
                    .retention()
                    .maintenance_outcome()
                    .with_task_scope(MaintenanceTaskScope::Branch(branch_id)),
            );
            return Ok(outcome.retention().clone());
        }
        if recovery_health_prevents_listing(request, &health) {
            let proof = retention_proof_from_assembly(request, &self.services, &health);
            let outcome = retention_outcome_for_scope(request, proof, &[])?;
            self.maintenance
                .record_reclaim(&outcome.maintenance_outcome());
            return Ok(outcome);
        }
        let manifest = self
            .services
            .manifest()
            .load_current()
            .map_err(manifest_error)?;
        let snapshots = self
            .services
            .snapshot()
            .list_snapshots()
            .map_err(snapshot_error)?;
        let snapshot_count = snapshots.len();
        let proof = build_retention_proof(request, manifest.as_ref(), &health, snapshot_count);
        let outcome = retention_outcome_for_scope(request, proof, &snapshots)?;
        self.maintenance
            .record_reclaim(&outcome.maintenance_outcome());
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete snapshot pruning hook"
    )]
    pub(crate) fn prune_snapshots(
        &mut self,
        request: &LifecycleRetentionRequest,
    ) -> LifecycleResult<LifecycleSnapshotPruningOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let health = self.current_recovery_health.clone();
        if recovery_health_prevents_listing(request, &health) {
            let proof = retention_proof_from_assembly(request, &self.services, &health);
            let pruning =
                LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())?;
            let outcome = prune_snapshots_with_proof(self.services.snapshot(), &pruning)?;
            self.maintenance
                .record_reclaim(&outcome.maintenance_outcome());
            return Ok(outcome);
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
        let proof = build_retention_proof(request, manifest.as_ref(), &health, snapshot_count);
        let pruning =
            LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())?;
        // Inline verb: not a queued task, so record the prune here.
        let outcome = prune_snapshots_with_proof(self.services.snapshot(), &pruning)?;
        self.maintenance
            .record_reclaim(&outcome.maintenance_outcome());
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete quarantine hook"
    )]
    pub(crate) fn quarantine_object(
        &mut self,
        request: &LifecycleQuarantineRequest,
    ) -> LifecycleResult<LifecycleQuarantineOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let outcome = quarantine_lifecycle_object(self.services.quarantine(), request);
        self.record_recovery_health(outcome.recovery_health());
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete purge hook"
    )]
    pub(crate) fn purge_quarantine(
        &mut self,
        branch_id: strata_core::BranchId,
        proof: &crate::lifecycle::LifecyclePurgeProof,
    ) -> LifecycleResult<LifecyclePurgeOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let codec_id = LifecycleCodecId::new(self.services.assembly_facts().codec_id())?;
        let outcome = purge_lifecycle_quarantine(
            self.services.quarantine(),
            branch_id,
            *self.services.assembly_facts().database_id(),
            &codec_id,
            proof,
        )?;
        self.record_recovery_health(outcome.recovery_health());
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "durable maintenance dispatch uses this concrete repair hook"
    )]
    pub(crate) fn repair_branch_quarantine(
        &mut self,
        branch_id: strata_core::BranchId,
    ) -> LifecycleResult<LifecycleQuarantineRepairOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let codec_id = LifecycleCodecId::new(self.services.assembly_facts().codec_id())?;
        let outcome = repair_branch_lifecycle_quarantine(
            self.services.quarantine(),
            branch_id,
            *self.services.assembly_facts().database_id(),
            &codec_id,
        )?;
        self.record_recovery_health(outcome.recovery_health());
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "runtime hook is consumed by concrete maintenance modules"
    )]
    pub(crate) fn maintenance_status(&self) -> MaintenanceExecutorStatus {
        self.maintenance.status()
    }

    /// The per-runtime reclaim ledger (space-reclamation contract §3.5): the
    /// last outcome of every reclaim family this runtime has run.
    pub(crate) const fn reclaim_ledger(&self) -> &crate::lifecycle::ReclaimLedger {
        self.maintenance.reclaim_ledger()
    }

    pub(crate) fn recent_maintenance_failures(
        &self,
    ) -> Vec<crate::lifecycle::MaintenanceFailureRecord> {
        self.maintenance.recent_failures()
    }

    #[cfg(test)]
    pub(crate) fn pending_flush_watermark_candidate_for_test(&self) -> Option<CommitVersion> {
        self.maintenance.pending_flush_watermark_candidate()
    }

    #[cfg(test)]
    pub(crate) fn pending_maintenance_kinds_for_test(&self) -> Vec<MaintenanceTaskKind> {
        self.maintenance.pending_kinds()
    }

    #[cfg(test)]
    pub(crate) fn set_active_maintenance_for_test(&mut self, task: MaintenanceTask) {
        self.maintenance.set_active_for_test(task);
    }

    #[allow(
        dead_code,
        reason = "runtime hook is consumed by concrete maintenance modules"
    )]
    pub(crate) fn enqueue_maintenance(
        &mut self,
        request: MaintenanceTaskRequest,
    ) -> LifecycleResult<MaintenanceEnqueueOutcome> {
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

    /// Derives commits-since-checkpoint (the count surfaced in the WAL-growth
    /// outcome and the maintenance summary record) from the cached retention
    /// watermark — no per-commit manifest read. Extracted so
    /// `evaluate_wal_growth_policy` stays within the per-function line budget.
    fn wal_growth_commits_since_checkpoint(&self) -> LifecycleResult<u64> {
        Ok(commits_since_checkpoint(
            self.visible.visible_version(),
            self.cached_retention_watermark()?,
        ))
    }

    #[allow(
        dead_code,
        reason = "pre-public-boundary policy hook is consumed by lifecycle hardening tests"
    )]
    /// The multi-branch guard states under which the checkpoint a growth
    /// trigger would enqueue is structurally deferred at execution:
    /// enqueue-mirrors-execution, so the policy never feeds the executor
    /// tasks that can only churn (#2792 — a non-seeded branch with a durable
    /// table base; the deferral-completion storm livelocked small-core
    /// hosts) and never hard-fails on the fresh-fork window (#2798 —
    /// unmaterialized COW inherited layers that `checkpoint_rows` refuses).
    /// The execution-time guards stay authoritative; while either holds, the
    /// WAL hard cap is advisory — unbounded growth is the documented V1
    /// multi-branch trade, and pacing writers against it converts disk
    /// headroom into unavailability.
    fn structurally_deferred_checkpoint_reason(&self) -> LifecycleResult<Option<&'static str>> {
        Ok(
            checkpoint_structural_deferral(&self.branch_catalog, self.initial_branch_id)?.map(
                |deferral| match deferral {
                    CheckpointStructuralDeferral::NonSeededDurableBase => {
                        "checkpoint policy deferred while a non-seeded branch holds a durable table base"
                    }
                    CheckpointStructuralDeferral::UnmaterializedInheritedLayers => {
                        "checkpoint policy deferred while a branch holds unmaterialized inherited layers"
                    }
                },
            ),
        )
    }

    pub(crate) fn evaluate_wal_growth_policy(
        &mut self,
    ) -> LifecycleResult<LifecycleWalGrowthOutcome> {
        let policy = self.open_plan.lifecycle_config().wal_growth_policy();
        if !policy.enabled() {
            return Ok(LifecycleWalGrowthOutcome::disabled(
                crate::service::WalGrowthFacts::empty(),
                0,
            ));
        }
        let facts_start = crate::observability::perf_trace::start_timer();
        let facts_result = self.services.wal().growth_facts();
        crate::observability::perf_trace::record_commit_wal_growth_facts_elapsed(facts_start);
        let facts = match facts_result {
            Ok(facts) => facts,
            Err(error) => {
                return LifecycleWalGrowthOutcome::deferred_with_health(
                    WalGrowthFacts::empty(),
                    0,
                    None,
                    wal_error(error),
                );
            }
        };
        let commits_since_checkpoint = match self.wal_growth_commits_since_checkpoint() {
            Ok(count) => count,
            Err(error) => {
                let trigger = policy.trigger_for(facts, 0);
                return LifecycleWalGrowthOutcome::deferred_with_health(facts, 0, trigger, error);
            }
        };
        let Some(trigger) = policy.trigger_for(facts, commits_since_checkpoint) else {
            return Ok(LifecycleWalGrowthOutcome::below_threshold(
                facts,
                commits_since_checkpoint,
            ));
        };
        if self.guard_set.is_quiescing().map_err(commit_error)? {
            return Ok(LifecycleWalGrowthOutcome::deferred(
                facts,
                commits_since_checkpoint,
                Some(trigger),
                LifecycleError::InvalidLifecycleState {
                    reason: "checkpoint policy deferred while commit quiesce is active",
                },
            ));
        }
        if self.guard_set.active_guard_count().map_err(commit_error)? > 0 {
            return Ok(LifecycleWalGrowthOutcome::deferred(
                facts,
                commits_since_checkpoint,
                Some(trigger),
                LifecycleError::InvalidLifecycleState {
                    reason: "checkpoint policy deferred while branch commit guard is active",
                },
            ));
        }
        if let Some(reason) = self.structurally_deferred_checkpoint_reason()? {
            return Ok(LifecycleWalGrowthOutcome::deferred(
                facts,
                commits_since_checkpoint,
                Some(trigger),
                LifecycleError::InvalidLifecycleState { reason },
            ));
        }
        if let Some(error) = policy_admission_error(self.state) {
            return Ok(LifecycleWalGrowthOutcome::deferred(
                facts,
                commits_since_checkpoint,
                Some(trigger),
                error,
            ));
        }
        let enqueue = (|| {
            self.enqueue_maintenance(MaintenanceTaskRequest::flush(self.initial_branch_id))?;
            self.enqueue_maintenance(MaintenanceTaskRequest::checkpoint_with_options(
                MaintenanceCheckpointOptions::new(None, true).retention_critical(),
            ))?;
            self.enqueue_maintenance(MaintenanceTaskRequest::table_manifest_flush_watermark(
                self.visible.visible_version(),
            ))?;
            self.enqueue_maintenance(MaintenanceTaskRequest::wal_truncation())
        })();
        match enqueue {
            Ok(enqueue) => Ok(LifecycleWalGrowthOutcome::maintenance_enqueued(
                facts,
                commits_since_checkpoint,
                trigger,
                enqueue,
            )),
            Err(error) => LifecycleWalGrowthOutcome::deferred_with_health(
                facts,
                commits_since_checkpoint,
                Some(trigger),
                error,
            ),
        }
    }

    #[allow(
        dead_code,
        reason = "runtime hook is consumed by concrete maintenance modules"
    )]
    pub(crate) fn run_next_maintenance(
        &mut self,
        runner: &mut impl MaintenanceTaskRunner,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let outcome = self.maintenance.run_next(self.state, runner);
        self.record_optional_maintenance_health(&outcome);
        // BS2.3: the runner may have rewritten any branch's tables; republish snapshots.
        self.republish_all_branch_snapshots();
        // C2: only table INSTALLS re-arm the preheat — repair/retention/GC
        // tasks change no table content, and arming on them would re-walk
        // the presence probes after every unrelated low-tier task.
        if let Ok(Some(completed)) = &outcome {
            if completed.status() == MaintenanceOutcomeStatus::Completed
                && matches!(
                    completed.task_kind(),
                    MaintenanceTaskKind::Flush
                        | MaintenanceTaskKind::Compaction
                        | MaintenanceTaskKind::Materialization
                )
            {
                self.note_cache_preheat_trigger();
            }
        }
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
        let outcome = self.run_flush_maintenance_task(task.id())?;
        // BS2.3: flush installed L0 tables (a global flush touches every active branch); republish.
        self.republish_all_branch_snapshots();
        self.note_cache_preheat_trigger();
        // Flush-driven WAL reclaim for the inline scheduling path (the background path reclaims
        // in `finish_publish_phase`). Best-effort; only when the flush actually completed.
        if matches!(&outcome, Some(outcome) if outcome.status() == MaintenanceOutcomeStatus::Completed)
        {
            self.reclaim_wal_after_flush();
        }
        Ok(outcome)
    }

    fn run_flush_maintenance_task(
        &mut self,
        task_id: MaintenanceTaskId,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let state = self.state;
        let Some(task) = self.maintenance.next_matching_task(|task| {
            task.id() == task_id && task.kind() == MaintenanceTaskKind::Flush
        }) else {
            return Ok(None);
        };
        // A branch-scoped flush legally races a branch delete (same as the compaction
        // arms): a Deleted target cancels the task instead of failing the drain.
        if let MaintenanceTaskScope::Branch(branch_id) = task.scope() {
            if self.branch_catalog.branch_is_deleted(branch_id) {
                let Some(task) = self
                    .maintenance
                    .start_next_matching(state, |queued| queued.id() == task_id)?
                else {
                    return Ok(None);
                };
                let outcome = deleted_scope_canceled_outcome(task.kind());
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(outcome));
            }
        }
        // Hold the per-branch publish slot for every branch this flush will publish, so its
        // manifest fsync(s) cannot run concurrently with a background off-lock fsync for the same
        // branch. A global flush publishes each active branch; defer the whole task if any slot is
        // busy (try-lock only) rather than block the global lock.
        let flush_branch_ids: Vec<BranchId> =
            flush_branch_descriptors_for_scope(&self.branch_catalog, task.scope())?
                .iter()
                .map(|descriptor| descriptor.branch_id())
                .collect();
        let mut publish_guards = Vec::with_capacity(flush_branch_ids.len());
        for branch_id in flush_branch_ids {
            match self.try_acquire_branch_publish_guard(branch_id) {
                Some(guard) => publish_guards.push(guard),
                None => {
                    return Ok(Some(self.defer_started_task_for_publish_busy(
                        task_id,
                        MaintenanceTaskKind::Flush,
                    )?));
                }
            }
        }
        let outcome = {
            let maintenance = &mut self.maintenance;
            let table_object = self.services.table_object();
            let table_reader = self.services.table_reader();
            let table_manifest = self.services.table_manifest();
            let table_catalog = &mut self.table_catalog;
            let mut runner = DurableFlushMaintenanceRunner {
                branch_catalog: &mut self.branch_catalog,
                table_object,
                table_reader,
                table_manifest,
                table_catalog,
                budget: &self.budget,
                data_block_bytes: self.open_plan.lifecycle_config().data_block_bytes(),
                table_compression: self.open_plan.lifecycle_config().table_compression(),
            };
            maintenance.run_next_matching(state, &mut runner, |task| task.id() == task_id)
        };
        self.record_optional_maintenance_health(&outcome);
        // Hold the per-branch publish slots until the manifest publish(es) above complete.
        drop(publish_guards);
        outcome
    }

    pub(crate) fn start_next_background_flush_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<DurableBackgroundMaintenanceStep<'a>>> {
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
        // Same enqueue/delete race as the foreground flush runner: a Deleted target
        // cancels the task instead of failing it into the ring.
        if self.branch_catalog.branch_is_deleted(branch_id) {
            let outcome = deleted_scope_canceled_outcome(task.kind());
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
        }
        let request = flush_drain_request_for_branch_from_maintenance_task(&task, branch_id)?;
        let generation = match self.branch_catalog.registry().lookup(branch_id) {
            Ok(descriptor) => descriptor.generation(),
            Err(error) => {
                let outcome =
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                        .with_source_error(commit_error(error));
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
            }
        };
        let branch = match self
            .branch_catalog
            .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))
        {
            Ok(branch) => branch,
            Err(error) => {
                let outcome =
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                        .with_source_error(error);
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
            }
        };
        // BS5.2: applied rows above the visible version mean a write-group
        // pipeline is mid-flight on this branch (rows applied, covering fsync
        // off-lock, publish pending). Freezing or snapshotting now would hand
        // the off-lock build rows whose WAL records are not yet durable — an
        // installed table could then survive a crash that tears those records,
        // resurrecting an unacked half-group. Defer; the pipeline publishes
        // within milliseconds and post-commit scheduling re-enqueues the flush.
        if branch
            .max_commit_version()
            .is_some_and(|version| version > self.visible.visible_version())
        {
            let outcome = MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Deferred)
                .with_source_error(LifecycleError::InvalidLifecycleState {
                    reason: "flush deferred: write-group publish in flight on this branch",
                });
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
        }
        // Deferring the WHOLE flush on a rotate refusal livelocked under
        // saturation: with several branches' frozen tables filling the
        // shared pool, every branch's flush deferred on the rotate budget
        // — while the existing frozen backlog those flushes would drain
        // is precisely what frees that budget (measured: 4 writer
        // branches at default budgets, 30s stall-wall timeouts, flush
        // deferring ~13x/s per branch until the watchdog fired). The
        // shared decision flushes the backlog WITHOUT rotating and defers
        // only when there is nothing frozen to flush. An empty active table
        // decides Rotate and the rotation itself reports Skipped.
        let rotated = match crate::lifecycle::decide_flush_rotation(&self.budget, branch) {
            crate::lifecycle::FlushRotationDecision::Rotate => matches!(
                branch.rotate_active(),
                crate::branch::state::BranchRotationOutcome::Rotated { .. }
            ),
            crate::lifecycle::FlushRotationDecision::FlushBacklogWithoutRotation => false,
            crate::lifecycle::FlushRotationDecision::Defer(error) => {
                let outcome =
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Deferred)
                        .with_source_error(error);
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
            }
        };
        let branch_snapshot = branch.clone();
        // Model-2 V-before-S coverage: the rotation swapped in a fresh live active, but the
        // published snapshot still holds (only) the pre-rotation one. Republish in the SAME lock
        // hold — otherwise every commit landing in the new active during the off-lock build stays
        // invisible to readers (reads at visible V missing rows ≤ V) until the finish-phase
        // republish. Caught by the BS5.0 multi-writer stress: acked batches were unreadable for
        // 15–140 ms whenever a flush build was in flight.
        if rotated {
            self.publish_branch_snapshot(branch_id);
        }
        Ok(Some(DurableBackgroundMaintenanceStep::Build(Box::new(
            DurableBackgroundMaintenanceBuild::Flush {
                task,
                branch_id,
                request,
                branch_snapshot,
                table_object: self.services.table_object().clone(),
                table_reader: self.services.table_reader().clone(),
                budget: self.budget.clone(),
                data_block_bytes: self.open_plan.lifecycle_config().data_block_bytes(),
                table_compression: self.open_plan.lifecycle_config().table_compression(),
                started_at: std::time::Instant::now(),
                inflight: self.inflight_outputs.clone(),
            },
        ))))
    }

    #[allow(
        dead_code,
        reason = "runtime maintenance entry point is consumed by dedicated tests"
    )]
    pub(crate) fn run_next_checkpoint_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let state = self.state;
        let manifest_debt = self.table_catalog.has_outstanding_manifest_debt();
        let maintenance = &mut self.maintenance;
        let branch_catalog = &self.branch_catalog;
        let initial_branch_id = self.initial_branch_id;
        let services = &self.services;
        let guard_set = &self.guard_set;
        let visible = &self.visible;
        let budget = &self.budget;
        let table_catalog = &self.table_catalog;
        let next_snapshot_id = &mut self.next_checkpoint_snapshot_id;
        let created_at = checkpoint_created_at(
            self.allocator.timestamp_guard().last_allocated(),
            self.recovered_checkpoint_timestamp_max,
        );
        let mut runner = DurableCheckpointMaintenanceRunner {
            branch_catalog,
            initial_branch_id,
            services,
            guard_set,
            visible,
            created_at,
            next_snapshot_id,
            budget,
            manifest_debt,
            table_catalog,
        };
        let outcome = maintenance.run_next_matching(state, &mut runner, |task| {
            task.kind() == MaintenanceTaskKind::Checkpoint
        });
        // A completed checkpoint advanced the manifest snapshot watermark.
        if matches!(outcome, Ok(Some(_))) {
            self.invalidate_retention_watermark_cache();
        }
        outcome
    }

    pub(crate) fn start_next_background_checkpoint_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<DurableBackgroundMaintenanceStep<'a>>> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let Some(task) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::Checkpoint)
        else {
            return Ok(None);
        };
        let state = self.state;
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == task.id())?
        else {
            return Ok(None);
        };
        let created_at = checkpoint_created_at(
            self.allocator.timestamp_guard().last_allocated(),
            self.recovered_checkpoint_timestamp_max,
        );
        let request = match checkpoint_request_from_maintenance_task_with_snapshot_id(
            &task,
            self.initial_branch_id,
            self.services.manifest(),
            created_at,
            Some(self.next_checkpoint_snapshot_id),
        ) {
            Ok(request) => request,
            Err(error) => {
                let outcome = MaintenanceOutcome::new(
                    crate::lifecycle::MaintenanceTaskKind::Checkpoint,
                    MaintenanceOutcomeStatus::Failed,
                )
                .with_source_error(error);
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
            }
        };
        let visible_version = self.visible.visible_version();
        let mut branches = Vec::new();
        for descriptor in self.branch_catalog.list_branches(false) {
            let branch = self
                .branch_catalog
                .branch_state(descriptor.branch_id())?
                .clone();
            // #2863: capture which owned tables are DURABLY cataloged now, under
            // the lock — the off-lock build uses this to keep the snapshot
            // self-contained over volatile tables.
            let durable_identities: std::collections::HashSet<crate::table::TableIdentity> = branch
                .owned_levels()
                .iter()
                .flatten()
                .filter(|table| {
                    self.table_catalog
                        .object_for_identity(table.descriptor().identity())
                        .is_some()
                })
                .map(|table| table.descriptor().identity().clone())
                .collect();
            branches.push((branch, durable_identities));
        }
        // A checkpoint advances the WAL-replay floor (active WAL segment) trusting that
        // every flushed table is covered by a durably-published table manifest. An off-lock
        // flush/rewrite that incurred reserved-manifest debt (its manifest publish failed)
        // left a reserved manifest sequence unpublished; snapshotting over it would advance
        // the floor past rows no durable manifest covers, silently losing them on recovery.
        // Defer until a later flush republishes the manifest in-process (clearing the debt),
        // after which a later checkpoint proceeds. The deferral advances no durable state.
        if self.table_catalog.has_outstanding_manifest_debt() {
            let outcome = MaintenanceOutcome::new(
                crate::lifecycle::MaintenanceTaskKind::Checkpoint,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason("checkpoint deferred: outstanding table-manifest publish debt");
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
        }
        // Multi-branch durability guard: defer when a branch other than the recovery-seeded
        // branch holds a durable table-manifest base. Recovery rebuilds non-seeded branches from a
        // global snapshot delta plus their per-branch table manifest, never replaying the WAL below
        // the snapshot watermark for them, so a snapshot taken over such a branch would recover a
        // non-contiguous gap if a crash later dropped that branch's manifest (the seeded-only
        // orphan detector cannot see it). The per-branch fix that lifts this guard is tracked in
        // multi-branch-orphaned-delta-recovery-gap.md.
        // TCP4.14: one registry decides structural deferral for BOTH this
        // execution-time arm and every enqueue/pacing site (#2792, #2798).
        if let Some(deferral) =
            checkpoint_structural_deferral(&self.branch_catalog, self.initial_branch_id)?
        {
            let reason = match deferral {
                CheckpointStructuralDeferral::NonSeededDurableBase => {
                    "checkpoint deferred: non-seeded branch holds a durable table base"
                }
                CheckpointStructuralDeferral::UnmaterializedInheritedLayers => {
                    "checkpoint deferred: branch holds unmaterialized inherited layers"
                }
            };
            let outcome = MaintenanceOutcome::new(
                crate::lifecycle::MaintenanceTaskKind::Checkpoint,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason(reason);
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
        }
        Ok(Some(DurableBackgroundMaintenanceStep::Build(Box::new(
            DurableBackgroundMaintenanceBuild::Checkpoint {
                task,
                request,
                visible_version,
                branches,
            },
        ))))
    }

    #[allow(
        dead_code,
        reason = "runtime maintenance entry point is consumed by dedicated tests"
    )]
    pub(crate) fn run_next_wal_truncation_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        // No pending-flush/flush-watermark gate: truncation reads the *current* persisted
        // retention watermark and only deletes sealed segments fully below it, so running
        // while a flush or watermark advance is in flight is safe — it reclaims what is
        // already covered and the next pass reclaims the rest (truncation tasks coalesce).
        let state = self.state;
        let maintenance = &mut self.maintenance;
        let (manifest, wal) = self.services.manifest_and_wal_mut();
        let mut runner = DurableWalTruncationMaintenanceRunner { manifest, wal };
        maintenance.run_next_matching(state, &mut runner, |task| {
            task.kind() == MaintenanceTaskKind::WalTruncation
        })
    }

    pub(crate) fn start_next_background_wal_truncation_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<DurableBackgroundMaintenanceStep<'a>>> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        // No pending-flush/flush-watermark gate: see `run_next_wal_truncation_maintenance`.
        let Some(task) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::WalTruncation)
        else {
            return Ok(None);
        };
        let state = self.state;
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == task.id())?
        else {
            return Ok(None);
        };
        let proof =
            match wal_truncation_request_from_maintenance_task(&task, self.services.manifest()) {
                Ok(Some(proof)) => proof,
                Ok(None) => {
                    let outcome = MaintenanceOutcome::new(
                        crate::lifecycle::MaintenanceTaskKind::WalTruncation,
                        MaintenanceOutcomeStatus::Deferred,
                    )
                    .with_reason("WAL truncation has no retention proof")
                    .with_deferral_reason(MaintenanceDeferralReason::IncompleteProof);
                    let outcome = self.maintenance.finish_started(task, outcome, false)?;
                    return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
                }
                Err(error) => {
                    let outcome = MaintenanceOutcome::new(
                        crate::lifecycle::MaintenanceTaskKind::WalTruncation,
                        MaintenanceOutcomeStatus::Failed,
                    )
                    .with_source_error(error);
                    let outcome = self.maintenance.finish_started(task, outcome, false)?;
                    self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                    return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
                }
            };
        // Reclaim rotation (#3494) happens HERE, under the lock, before the
        // retention clone is taken: rotation mutates writer state (active
        // pointer, watermark publish) and must never run off-lock, while the
        // segment it seals is immutable from that point on — exactly what the
        // off-lock delete pass below is allowed to touch.
        if let Err(error) = self
            .services
            .wal_mut()
            .rotate_active_segment_for_reclaim(proof.covered_through())
        {
            let outcome = MaintenanceOutcome::new(
                crate::lifecycle::MaintenanceTaskKind::WalTruncation,
                MaintenanceOutcomeStatus::Failed,
            )
            .with_source_error(wal_error(error));
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
        }
        Ok(Some(DurableBackgroundMaintenanceStep::Build(Box::new(
            DurableBackgroundMaintenanceBuild::WalTruncation {
                task,
                proof,
                wal: self.services.wal().clone_for_background_retention(),
            },
        ))))
    }

    /// First locked phase of the three-phase background publish. The in-memory install,
    /// sequence-reserve, and (for table-manifest-backed flush/compaction/materialization) the
    /// per-branch publish-slot claim all happen here under the global runtime lock. The actual
    /// durable manifest fsync is deferred to [`PreparedPublish::persist_off_lock`], which runs
    /// with the global lock released; [`finish_publish_phase`](Self::finish_publish_phase) folds
    /// the persisted manifest back into the catalog under the lock again.
    ///
    /// Checkpoint and WAL-truncation publishes are unaffected: they run to completion here and
    /// return [`PreparedPublishStep::Done`]. Flush/compaction/materialization either return
    /// `Done` (nothing to publish, or the per-branch slot is busy so the task defers) or
    /// [`PreparedPublishStep::OffLock`] carrying the manifest to persist off-lock.
    pub(crate) fn begin_publish_phase(
        &mut self,
        built: DurableBackgroundMaintenanceBuilt,
    ) -> LifecycleResult<PreparedPublishStep<'a>> {
        let step = match built {
            DurableBackgroundMaintenanceBuilt::Flush {
                task,
                branch_id,
                prepared,
                elapsed,
            } => self.begin_flush_publish(task, branch_id, prepared, elapsed),
            DurableBackgroundMaintenanceBuilt::Checkpoint {
                task,
                request,
                visible_version,
                rows,
                has_durable_rows,
                flush_boundary,
                timeline_groups,
            } => {
                let checkpoint = checkpoint_durable_rows_with_budget(
                    &self.services,
                    &request,
                    visible_version,
                    &rows,
                    &timeline_groups,
                    has_durable_rows,
                    flush_boundary,
                    Some(&self.budget),
                );
                let outcome = match checkpoint {
                    Ok(outcome) => {
                        if let Some(snapshot_id) = outcome.snapshot_id() {
                            self.next_checkpoint_snapshot_id = snapshot_id.checked_add(1).ok_or(
                                LifecycleError::CheckpointPublicationFailed {
                                    reason: "checkpoint snapshot id overflow",
                                },
                            )?;
                        }
                        // The background checkpoint advanced the manifest snapshot
                        // watermark (and possibly the flush boundary); drop the
                        // cached value so the next commit re-reads it.
                        self.invalidate_retention_watermark_cache();
                        self.maintenance
                            .finish_started(task, outcome.maintenance_outcome(), false)
                    }
                    Err(error) => {
                        let outcome =
                            MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                                .with_source_error(error);
                        self.maintenance.finish_started(task, outcome, false)
                    }
                };
                PreparedPublishStep::Done(self.record_publish_phase_health(outcome))
            }
            DurableBackgroundMaintenanceBuilt::WalTruncation { task, outcome } => {
                let outcome =
                    self.maintenance
                        .finish_started(task, outcome.maintenance_outcome(), false);
                // The deletion ran on a background retention clone, so the
                // primary WAL service's cached retention totals are now stale.
                self.services.wal().invalidate_sealed_retention();
                PreparedPublishStep::Done(self.record_publish_phase_health(outcome))
            }
            DurableBackgroundMaintenanceBuilt::Compaction {
                task,
                branch_id,
                level,
                prepared,
            } => self.begin_compaction_publish(task, branch_id, level, *prepared),
            DurableBackgroundMaintenanceBuilt::Materialization {
                task,
                branch_id,
                prepared,
            } => self.begin_materialization_publish(task, branch_id, *prepared),
        };
        Ok(step)
    }

    /// Record maintenance health for an outcome resolved entirely under the lock (checkpoint,
    /// WAL-truncation, deferred/error short-circuits, and the off-lock finish). Mirrors the
    /// health bookkeeping the previous single-phase `finish_background_maintenance` applied to
    /// every outcome it returned.
    /// The complete pinned set for a table-object reclaim mark: in-memory branch state,
    /// in-flight build outputs (#2531), and the manifest frontier — objects the last
    /// durably-confirmed manifest or a built-but-unconfirmed publication still lists (#2553).
    /// Every mark entry point MUST use this helper; a mark missing any component can classify
    /// a recovery-relevant object as garbage.
    fn reclaim_pinned_table_objects(&self) -> Vec<crate::object::ObjectName> {
        let mut pinned = in_memory_pinned_table_objects(&self.branch_catalog, &self.table_catalog);
        pinned.extend(self.inflight_outputs.snapshot());
        let frontier = self.table_catalog.manifest_frontier_pinned_objects();
        crate::observability::perf_trace::record_table_object_frontier_pins(frontier.len() as u64);
        pinned.extend(frontier);
        pinned
    }

    fn record_publish_phase_health(
        &mut self,
        outcome: LifecycleResult<MaintenanceOutcome>,
    ) -> LifecycleResult<MaintenanceOutcome> {
        self.record_optional_maintenance_health(&outcome.clone().map(Some));
        outcome
    }

    pub(crate) fn finish_background_build_error(
        &mut self,
        task: MaintenanceTask,
        error: LifecycleError,
    ) -> LifecycleResult<MaintenanceOutcome> {
        // A stale compaction candidate is a benign scheduling race, not a
        // task failure: concurrent maintenance superseded the candidate's
        // inputs between enqueue and build, and coverage re-derives fresh
        // candidates on the next pass. So is a build whose adopted output a
        // concurrent table-object sweep deleted mid-read (#3382, the
        // build-phase leg of #2553): the retried task publishes fresh bytes
        // once the sweep completes. Everything else stays Failed.
        let outcome = if error.is_stale_compaction_candidate() {
            MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Deferred)
                .with_reason("compaction candidate superseded by concurrent maintenance")
                .with_stats(LifecycleStats::new(0, 0, 1, 1, 0))
        } else if error.is_rewrite_output_sweep_race() {
            MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Deferred)
                .with_reason("build output raced a table-object sweep")
                .with_stats(LifecycleStats::new(0, 0, 1, 1, 0))
        } else {
            MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                .with_source_error(error)
        };
        let outcome = self.maintenance.finish_started(task, outcome, false);
        self.record_optional_maintenance_health(&outcome.clone().map(Some));
        outcome
    }

    /// Install a prepared flush drain in memory and record its frozen tables in the catalog
    /// under the lock, then prepare a single off-lock table-manifest publish for the whole drain
    /// (the previous path published one manifest per frozen table). The drain may flush several
    /// frozen tables; each install records its table, and one reserved-sequence manifest covers
    /// them all. When nothing completed there is no durable change to publish.
    fn begin_flush_publish(
        &mut self,
        task: MaintenanceTask,
        branch_id: BranchId,
        prepared: PreparedDurableFlushDrain,
        _elapsed: std::time::Duration,
    ) -> PreparedPublishStep<'a> {
        // The branch was deleted between the off-lock build and this publish —
        // the same enqueue/delete race as the starter arms, one phase later.
        // Cancel; the built objects are unreferenced and the sweep reclaims them.
        if self.branch_catalog.branch_is_deleted(branch_id) {
            return self.finish_locked_publish(task, deleted_scope_canceled_outcome(task.kind()));
        }
        let install: LifecycleResult<(MaintenanceOutcome, bool)> = (|| {
            let generation = self
                .branch_catalog
                .registry()
                .lookup(branch_id)
                .map_err(commit_error)?
                .generation();
            let table_catalog = &mut self.table_catalog;
            let sweep_staged = &self.sweep_staged_names;
            let table_object = self.services.table_object();
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            let mut any_completed = false;
            let drain_outcome = install_prepared_durable_flush_drain_with(
                branch,
                prepared,
                |branch, prepared_flush| {
                    // #2553: a retried flush can adopt (content-identical
                    // dedupe) an orphan a sweep stage has frozen for deletion.
                    crate::lifecycle::rewrite_publication::verify_output_objects_not_swept(
                        &prepared_flush.output_object_names(),
                        sweep_staged,
                        table_object,
                    )?;
                    let outcome = install_prepared_durable_flush(branch, prepared_flush);
                    let maintenance_outcome = outcome.maintenance_outcome();
                    if outcome.completed() {
                        // A1 (#2524): one catalog record per flushed table
                        // (a frozen memtable flushes to N key-disjoint
                        // tables once A2's zone cuts land).
                        for table in outcome.tables() {
                            let Some(object_facts) = table.object_facts() else {
                                continue;
                            };
                            table_catalog.record_table(
                                table.table_identity().clone(),
                                object_facts.clone(),
                            )?;
                            any_completed = true;
                        }
                    }
                    Ok(maintenance_outcome)
                },
            )?;
            Ok((drain_outcome, any_completed))
        })();
        let (drain_outcome, any_completed) = match install {
            Ok(values) => values,
            Err(error) => {
                // #2553: adopting a sweep-staged object is a benign race —
                // defer; the retried drain publishes fresh bytes.
                let outcome = if error.is_rewrite_output_sweep_race() {
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Deferred)
                        .with_reason("flush output raced a table-object sweep")
                        .with_stats(LifecycleStats::new(0, 0, 1, 1, 0))
                } else {
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                        .with_source_error(error)
                };
                return self.finish_locked_publish(task, outcome);
            }
        };
        if !any_completed {
            return self.finish_locked_publish(task, drain_outcome);
        }
        self.begin_off_lock_publish(task, branch_id, drain_outcome, PreparedPublishPost::Flush)
    }

    /// Install a prepared compaction in memory and record its output tables in the catalog under
    /// the lock, splitting the durable manifest publish off to the off-lock phase. Outcomes that
    /// rewrote nothing (no-candidate) carry no manifest to publish and finish under the lock.
    fn begin_compaction_publish(
        &mut self,
        task: MaintenanceTask,
        branch_id: BranchId,
        level: u8,
        prepared: PreparedDurableCompaction,
    ) -> PreparedPublishStep<'a> {
        // Same mid-flight enqueue/delete race as the flush publish above.
        if self.branch_catalog.branch_is_deleted(branch_id) {
            return self.finish_locked_publish(task, deleted_scope_canceled_outcome(task.kind()));
        }
        // #3526: re-validate an off-lock pruning proof against the LIVE branch
        // before installing the pruned output. The proof was built at start from
        // a snapshot; a fork sharing the pruned tables or a concurrent prune that
        // moved the floor during the off-lock window makes it unsafe (its frozen
        // shared-table gate would not catch the fork). If stale, DEFER — discard
        // the pruned output; the next pass re-derives a fresh proof (or compacts
        // as KeepAll while the tables stay shared).
        if let Some(proof) = prepared.branch_request().pruning_proof() {
            if !self.revalidate_pruning_for_publish(branch_id, &proof) {
                crate::observability::perf_trace::record_lifecycle_background_candidate_stale_deferred();
                let outcome =
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Deferred)
                        .with_reason(
                            "version-pruning proof became unsafe before publish \
                         (input tables shared or retained-history floor moved)",
                        )
                        .with_stats(LifecycleStats::new(0, 0, 1, 1, 0));
                return self.finish_locked_publish(task, outcome);
            }
        }
        let install: LifecycleResult<LifecycleCompactionOutcome> = (|| {
            let generation = self
                .branch_catalog
                .registry()
                .lookup(branch_id)
                .map_err(commit_error)?
                .generation();
            let table_catalog = &mut self.table_catalog;
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            install_prepared_durable_compaction_without_publish(
                branch,
                table_catalog,
                prepared,
                Some(&self.budget),
                &self.sweep_staged_names,
                self.services.table_object(),
            )
        })();
        let compaction = match install {
            Ok(compaction) => compaction,
            Err(error) => {
                // Install re-validates the candidate against the CURRENT
                // branch state; a candidate whose inputs were superseded by
                // concurrent maintenance during the off-lock build is a
                // benign scheduling race, not a task failure — coverage
                // re-derives fresh candidates on the next pass.
                let outcome = if error.is_stale_compaction_candidate() {
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Deferred)
                        .with_reason("compaction candidate superseded by concurrent maintenance")
                        .with_stats(LifecycleStats::new(0, 0, 1, 1, 0))
                } else if error.is_rewrite_output_sweep_race() {
                    // #2553: the build adopted a content-identical object a
                    // sweep already staged or deleted; the rebuilt pass
                    // publishes fresh bytes once the sweep completes.
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Deferred)
                        .with_reason("rewrite output raced a table-object sweep")
                        .with_stats(LifecycleStats::new(0, 0, 1, 1, 0))
                } else {
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                        .with_source_error(error)
                };
                return self.finish_locked_publish(task, outcome);
            }
        };
        if compaction.status() == crate::lifecycle::LifecycleCompactionStatus::DeferredNoCandidate {
            // No rewrite output: no manifest to publish. Run the post-install requeue logic and
            // finish under the lock, exactly as the no-candidate path did before.
            record_lifecycle_compaction_outcome(&compaction);
            let maintenance = compaction.maintenance_outcome();
            self.apply_compaction_post(branch_id, level, &maintenance);
            return self.finish_locked_publish(task, maintenance);
        }
        let base_outcome = compaction.maintenance_outcome();
        self.begin_off_lock_publish(
            task,
            branch_id,
            base_outcome,
            PreparedPublishPost::Compaction {
                branch_id,
                level,
                outcome: Box::new(compaction),
            },
        )
    }

    /// Install a prepared materialization in memory and record its replacement tables in the
    /// catalog under the lock, splitting the durable manifest publish off to the off-lock phase.
    fn begin_materialization_publish(
        &mut self,
        task: MaintenanceTask,
        branch_id: BranchId,
        prepared: PreparedDurableMaterialization,
    ) -> PreparedPublishStep<'a> {
        let install: LifecycleResult<LifecycleMaterializationOutcome> = (|| {
            let generation = self
                .branch_catalog
                .registry()
                .lookup(branch_id)
                .map_err(commit_error)?
                .generation();
            let table_catalog = &mut self.table_catalog;
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            install_prepared_durable_materialization_without_publish(
                branch,
                table_catalog,
                prepared,
                Some(&self.budget),
                &self.sweep_staged_names,
                self.services.table_object(),
            )
        })();
        let materialization = match install {
            Ok(materialization) => materialization,
            Err(error) => {
                let outcome =
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                        .with_source_error(error);
                return self.finish_locked_publish(task, outcome);
            }
        };
        let base_outcome = materialization.maintenance_outcome();
        self.begin_off_lock_publish(
            task,
            branch_id,
            base_outcome,
            PreparedPublishPost::Materialization {
                branch_id,
                outcome: Box::new(materialization),
            },
        )
    }

    /// Reserve the manifest sequence, build the branch's table manifest, and claim the per-branch
    /// publish slot for an off-lock fsync. With the slot busy (another publish for the branch is
    /// between sequence-reserve and record), defer the task under the lock rather than block —
    /// blocking on the per-branch slot while holding the global lock would deadlock the drain.
    fn begin_off_lock_publish(
        &mut self,
        task: MaintenanceTask,
        branch_id: BranchId,
        base_outcome: MaintenanceOutcome,
        post: PreparedPublishPost,
    ) -> PreparedPublishStep<'a> {
        let reserved = match self.table_catalog.reserve_manifest_sequence() {
            Ok(reserved) => reserved,
            Err(error) => {
                let outcome = table_manifest_debt_outcome(base_outcome, error);
                return self.finish_locked_publish(task, outcome);
            }
        };
        let manifest = {
            let branch = match self.branch_catalog.branch_state(branch_id) {
                Ok(branch) => branch,
                Err(error) => {
                    let outcome = table_manifest_debt_outcome(base_outcome, error);
                    return self.finish_locked_publish(task, outcome);
                }
            };
            match self
                .table_catalog
                .build_manifest_with_sequence(branch, reserved)
            {
                Ok(manifest) => manifest,
                Err(error) => {
                    let outcome = table_manifest_debt_outcome(base_outcome, error);
                    return self.finish_locked_publish(task, outcome);
                }
            }
        };
        let Some(guard) = self.try_acquire_branch_publish_guard(branch_id) else {
            // Another publish for this branch holds the slot. Defer; the next drain round retries.
            let outcome = MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Deferred)
                .with_reason("table manifest publish slot is busy for this branch");
            return self.finish_locked_publish(task, outcome);
        };
        // #2553: from this point the manifest WILL be persisted (possibly ending uncertain),
        // so its object names become recovery-relevant. Register them as the branch's pending
        // publication before the lock is released — the reclaim mark must not sweep an object
        // this manifest lists while the persist is in flight or its durability is unconfirmed.
        // Inserted only after guard acquisition: deferred attempts never persist.
        let manifest_objects = crate::lifecycle::table_manifest::manifest_object_names(&manifest);
        self.table_catalog
            .record_pending_publication(branch_id, manifest_objects.clone());
        PreparedPublishStep::OffLock(PreparedPublish {
            task,
            branch_id,
            base_outcome,
            manifest,
            manifest_objects,
            service: self.services.table_manifest().clone(),
            budget: self.budget.clone(),
            guard,
            post,
        })
    }

    /// Finish a task whose publish resolved entirely under the lock (deferred, errored, or with
    /// no durable manifest to write) and record its maintenance health.
    fn finish_locked_publish(
        &mut self,
        task: MaintenanceTask,
        outcome: MaintenanceOutcome,
    ) -> PreparedPublishStep<'a> {
        let finished = self.maintenance.finish_started(task, outcome, false);
        PreparedPublishStep::Done(self.record_publish_phase_health(finished))
    }

    /// Apply the post-install requeue/resubmit decisions for a compaction outcome. Mirrors the
    /// requeue logic the previous single-phase compaction finisher ran right after install.
    fn apply_compaction_post(
        &mut self,
        branch_id: BranchId,
        level: u8,
        maintenance: &MaintenanceOutcome,
    ) {
        if table_rewrite_outcome_was_flush_preempted(maintenance) {
            self.requeue_flush_preempted_compaction(branch_id, level);
        } else if table_rewrite_outcome_allows_chain_resubmit(maintenance) {
            self.resubmit_table_rewrite_if_any_branch_still_unhealthy(branch_id);
        }
    }

    /// Final locked phase of the three-phase background publish. Folds the off-lock-persisted
    /// manifest back into the catalog (or stamps manifest debt on persist failure), applies any
    /// kind-specific post-install requeue logic, finishes the task, and records its health. The
    /// per-branch publish slot guard held by `prepared` is released when it drops here.
    pub(crate) fn finish_publish_phase(
        &mut self,
        prepared: PreparedPublish<'_>,
        write_result: LifecycleResult<TableManifestWrite>,
    ) -> LifecycleResult<MaintenanceOutcome> {
        let PreparedPublish {
            task,
            branch_id,
            base_outcome,
            manifest: _,
            manifest_objects,
            service: _,
            budget: _,
            guard,
            post,
        } = prepared;
        // Set when a flush actually installed new L0 table(s); gates the flush-driven WAL
        // reclaim at the tail (after the task is finished and the publish guard released).
        let mut flush_published = false;
        // Rewrites (compaction/materialization) supersede input objects: trigger the table-object
        // GC mark after the publish lands (flushes only add tables — no trigger).
        let rewrite_branch = match &post {
            PreparedPublishPost::Compaction { branch_id, .. }
            | PreparedPublishPost::Materialization { branch_id, .. } => Some(*branch_id),
            PreparedPublishPost::Flush => None,
        };
        let outcome = match post {
            PreparedPublishPost::Flush => match write_result {
                Ok(_write) => {
                    self.table_catalog
                        .confirm_reserved_manifest_published(branch_id, manifest_objects);
                    flush_published = true;
                    base_outcome
                }
                // Publication-uncertain persist: the pending publication registered at guard
                // acquisition stays in place — the manifest may be durable even though its
                // durability is unconfirmed, so its listed objects remain protected (#2553).
                Err(error) => table_manifest_debt_outcome(base_outcome, error),
            },
            PreparedPublishPost::Compaction {
                branch_id,
                level,
                outcome,
            } => {
                let outcome = self.fold_rewrite_publish(
                    write_result,
                    branch_id,
                    manifest_objects,
                    *outcome,
                    LifecycleCompactionOutcome::manifest_debt,
                );
                record_lifecycle_compaction_outcome(&outcome);
                let maintenance = outcome.maintenance_outcome();
                self.apply_compaction_post(branch_id, level, &maintenance);
                maintenance
            }
            PreparedPublishPost::Materialization { branch_id, outcome } => {
                let outcome = self.fold_rewrite_publish(
                    write_result,
                    branch_id,
                    manifest_objects,
                    *outcome,
                    LifecycleMaterializationOutcome::manifest_debt,
                );
                let maintenance = outcome.maintenance_outcome();
                if table_rewrite_outcome_allows_chain_resubmit(&maintenance) {
                    self.resubmit_table_rewrite_if_any_branch_still_unhealthy(branch_id);
                }
                maintenance
            }
        };
        // The publish slot is released only after the catalog record above, so no concurrent
        // publish for this branch can interleave between persist and record.
        drop(guard);
        let finished = self.maintenance.finish_started(task, outcome, false);
        let result = self.record_publish_phase_health(finished);
        // Flush-driven WAL reclaim: advance the flush watermark to the just-published L0
        // coverage and enqueue truncation, decoupled from checkpoint. Runs after the guard is
        // released and the task finished; best-effort, never disturbs the flush result.
        if flush_published {
            self.reclaim_wal_after_flush();
        }
        // Table-object GC: the published rewrite dropped its input refs from the branch manifest;
        // enqueue the coalescing mark so the superseded objects are reclaimed. Best-effort — a
        // rejected enqueue only defers reclaim to the next cycle.
        if let Some(branch_id) = rewrite_branch {
            let _ =
                self.enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch_id));
        }
        // C2: a background publish installed tables; re-arm the preheat.
        self.note_cache_preheat_trigger();
        // BS2.3: the background publish phase installed flushed/compacted/materialized tables;
        // republish snapshots (a flush drain can touch several branches). BS3.4b's graded-admission
        // rate recompute rides inside `republish_all_branch_snapshots`, the one point both the
        // background and inline install paths converge on.
        self.republish_all_branch_snapshots();
        result
    }

    /// Fold an off-lock persist result into a rewrite outcome: record the persisted manifest's
    /// tables on success (the sequence was advanced at reserve time), or stamp the outcome with
    /// manifest debt on persist or record failure. On the debt arm the pending publication
    /// registered at guard acquisition stays in place (#2553).
    fn fold_rewrite_publish<T>(
        &mut self,
        write_result: LifecycleResult<TableManifestWrite>,
        branch_id: BranchId,
        manifest_objects: std::sync::Arc<std::collections::BTreeSet<crate::object::ObjectName>>,
        outcome: T,
        manifest_debt: impl FnOnce(T, LifecycleError) -> T,
    ) -> T {
        match write_result {
            Ok(_write) => {
                self.table_catalog
                    .confirm_reserved_manifest_published(branch_id, manifest_objects);
                outcome
            }
            Err(error) => manifest_debt(outcome, error),
        }
    }

    #[allow(
        dead_code,
        reason = "runtime maintenance entry point is consumed by dedicated tests"
    )]
    pub(crate) fn run_next_flush_watermark_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        // No pending-flush gate: see `run_next_background_flush_watermark_maintenance`.
        let state = self.state;
        let maintenance = &mut self.maintenance;
        let branch = self
            .branch_catalog
            .branch_state(self.initial_branch_id)
            .expect("seeded branch is always present in the catalog");
        let registry = self.branch_catalog.registry();
        let manifest = self.services.manifest();
        let table_manifest = self.services.table_manifest();
        let visible_version = self.visible.visible_version();
        let health = &self.current_recovery_health;
        let mut runner = DurableFlushWatermarkMaintenanceRunner {
            branch,
            registry,
            manifest,
            table_manifest,
            visible_version,
            health,
        };
        let outcome = maintenance.run_next_matching(state, &mut runner, |task| {
            task.kind() == MaintenanceTaskKind::FlushWatermark
        });
        // A completed flush-watermark persist advanced flushed_through_commit_id.
        if matches!(outcome, Ok(Some(_))) {
            self.invalidate_retention_watermark_cache();
        }
        outcome
    }

    /// D.2b-2 background flush-watermark entry: the checkpoint-covered fast path (no
    /// coverage scan) persists synchronously under the lock; otherwise it captures an
    /// owned snapshot and returns `FlushWatermarkCompute`, so the O(rows) coverage scan
    /// runs off the lock. Returns `None` (fall through to the maintenance after
    /// flush-watermark) when there is no task or no candidate to try.
    pub(crate) fn start_next_background_flush_watermark_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<DurableBackgroundMaintenanceStep<'a>>> {
        let Some(task) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::FlushWatermark)
        else {
            return Ok(None);
        };
        if let Some(outcome) = self.run_background_flush_watermark_if_checkpoint_covered(task)? {
            return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
        }
        if let Some(inputs) = self.capture_flush_watermark_coverage_inputs(task)? {
            Ok(Some(
                DurableBackgroundMaintenanceStep::FlushWatermarkCompute(Box::new(inputs)),
            ))
        } else {
            // Coverage is not provable NOW (no candidate at/below the
            // durable boundary, no table manifest, or multi-branch). A
            // pending task pinned here can never complete once the write
            // load stops — the queue then never drains and the runtime
            // reports "idle" over lifecycle debt. Defer (consume) it
            // instead: every flush publish and WAL-growth evaluation
            // re-enqueues a fresh coalescing candidate, so nothing is
            // lost and the retry rides the next real trigger.
            let state = self.state;
            let Some(task) = self
                .maintenance
                .start_next_matching(state, |queued| queued.id() == task.id())?
            else {
                return Ok(None);
            };
            let outcome = MaintenanceOutcome::new(
                MaintenanceTaskKind::FlushWatermark,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason("flush watermark deferred: coverage not provable")
            .with_stats(LifecycleStats::new(0, 0, 1, 1, 0));
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)))
        }
    }

    /// Capture (under the runtime lock) the owned inputs the off-lock coverage scan
    /// needs. Returns `None` when there is nothing to try (not a single-branch runtime,
    /// no durable table manifest, or no candidate), so the drain falls through.
    fn capture_flush_watermark_coverage_inputs(
        &self,
        task: MaintenanceTask,
    ) -> LifecycleResult<Option<FlushWatermarkCoverageInputs>> {
        let branch_id = self.initial_branch_id;
        // Single-branch guard: matches the existing coverage checks. Multi-branch
        // runtimes must capture every active branch before this relaxes.
        if self.branch_catalog.registry().active_branch_ids() != vec![branch_id] {
            return Ok(None);
        }
        let Some(table_manifest) = self
            .services
            .table_manifest()
            .load_current(branch_id)
            .map_err(manifest_error)?
        else {
            return Ok(None);
        };
        let current_manifest = self
            .services
            .manifest()
            .load_required()
            .map_err(manifest_error)?;
        let floor = wal_retention_watermark(
            current_manifest
                .snapshot_watermark()
                .map(CommitVersion::new),
            current_manifest.flushed_through_commit_id(),
        )
        .unwrap_or(CommitVersion::ZERO);
        let visible_version = self.visible.visible_version();
        let candidates =
            flush_watermark_candidates_from_manifest(&table_manifest, visible_version, floor);
        let task_candidate = task.flush_watermark_candidate();
        if candidates.is_empty() && task_candidate.is_none() {
            return Ok(None);
        }
        let recovery_health_epoch = recovery_health_epoch(&self.current_recovery_health)?;
        let branch = self
            .branch_catalog
            .branch_state(branch_id)
            .expect("seeded branch is always present in the catalog");
        let owned_levels = branch.layout_snapshot();
        let inherited_layers = branch.inherited_layers().to_vec();
        // The lowest commit still in the memtable: a candidate at or above it is not
        // durably flushed. Bounds candidate selection off-lock; the apply step re-checks
        // the current memtable under the lock.
        let min_unflushed_commit = branch
            .active()
            .iter()
            .map(|row| row.row().commit_version())
            .chain(
                branch
                    .frozen()
                    .iter()
                    .flat_map(|table| table.iter().map(|row| row.row().commit_version())),
            )
            .min();
        Ok(Some(FlushWatermarkCoverageInputs {
            task,
            branch_id,
            owned_levels,
            inherited_layers,
            table_manifest,
            floor,
            recovery_health_epoch,
            task_candidate,
            candidates,
            min_unflushed_commit,
        }))
    }

    /// Apply (under the runtime lock) the result of the off-lock coverage scan. A
    /// `None` compute (nothing coverable) DEFERS the task — a pinned pending task
    /// can never complete once the write load stops, and every flush publish or
    /// WAL-growth evaluation re-enqueues a fresh coalescing candidate (the same
    /// liveness rule as the capture-time gate). Otherwise it claims the task,
    /// persists with the pre-built proof (re-validating epochs and the memtable
    /// against current state), and finishes the task.
    pub(crate) fn apply_flush_watermark_coverage(
        &mut self,
        inputs: &FlushWatermarkCoverageInputs,
        computed: Option<(CommitVersion, LifecycleTableManifestFlushCoverageProof)>,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let Some((candidate, proof)) = computed else {
            let state = self.state;
            let Some(task) = self
                .maintenance
                .start_next_matching(state, |queued| queued.id() == inputs.task.id())?
            else {
                return Ok(None);
            };
            let outcome = MaintenanceOutcome::new(
                MaintenanceTaskKind::FlushWatermark,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason("flush watermark deferred: coverage not provable")
            .with_stats(LifecycleStats::new(0, 0, 1, 1, 0));
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            return Ok(Some(outcome));
        };
        let state = self.state;
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == inputs.task.id())?
        else {
            return Ok(None);
        };
        let result = self.persist_off_lock_flush_watermark_coverage(candidate, proof);
        let maintenance = match result {
            Ok(outcome) => outcome.maintenance_outcome(),
            // The proof carries its build-time epochs and every
            // `WalRetentionProofIncomplete` fires before the manifest write:
            // a rejection means the world moved under the off-lock scan
            // (concurrent flush/compaction advanced the manifest, a branch
            // appeared, rows landed below the candidate) and nothing was
            // persisted. That is the designed optimistic retry path — the
            // next flush publish or WAL-growth evaluation re-enqueues a
            // fresh coalescing candidate — so it defers like the inline
            // runner's proof arm, and never records a task failure.
            Err(error @ LifecycleError::WalRetentionProofIncomplete { .. }) => {
                MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Deferred)
                    .with_reason("flush watermark deferred: coverage proof went stale")
                    .with_source_error(error)
                    .with_stats(LifecycleStats::new(0, 0, 1, 1, 0))
            }
            Err(error) => MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                .with_source_error(error),
        };
        let outcome = self.maintenance.finish_started(task, maintenance, false)?;
        self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
        Ok(Some(outcome))
    }

    /// Persist a flush watermark from a proof built off-lock. The proof carries its
    /// build-time epochs; here we re-read the *current* table-manifest sequence and
    /// recovery-health epoch and validate the proof against those, so a concurrent
    /// flush/compaction that advanced the manifest rejects the stale proof. The current
    /// memtable is re-checked as the anti-corruption gate.
    fn persist_off_lock_flush_watermark_coverage(
        &mut self,
        candidate: CommitVersion,
        proof: LifecycleTableManifestFlushCoverageProof,
    ) -> LifecycleResult<LifecycleFlushWatermarkOutcome> {
        require_admitted(self.state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let branch_id = self.initial_branch_id;
        let manifest = self
            .services
            .table_manifest()
            .load_current(branch_id)
            .map_err(manifest_error)?
            .ok_or(LifecycleError::WalRetentionProofIncomplete {
                reason: "table manifest flush proof requires durable table manifest",
            })?;
        let branch = self
            .branch_catalog
            .branch_state(branch_id)
            .expect("seeded branch is always present in the catalog");
        if branch_has_unflushed_rows_at_or_below(branch, candidate) {
            return Err(LifecycleError::WalRetentionProofIncomplete {
                reason:
                    "mutable rows at or below flush watermark are not covered by table manifest",
            });
        }
        let active_branches = self.branch_catalog.registry().active_branch_ids();
        if active_branches != vec![branch_id] {
            return Err(LifecycleError::WalRetentionProofIncomplete {
                reason: "table manifest flush proof requires all active branches to be loaded",
            });
        }
        let recovery_health_epoch = recovery_health_epoch(&self.current_recovery_health)?;
        let outcome = persist_flush_watermark_with_table_manifest_proof(
            self.services.manifest(),
            self.visible.visible_version(),
            candidate,
            &LifecycleFlushWatermarkProof::TableManifestCovered(proof),
            manifest.manifest_sequence(),
            recovery_health_epoch,
            &[(branch_id, manifest.manifest_sequence())],
        )?;
        self.invalidate_retention_watermark_cache();
        Ok(outcome)
    }

    pub(crate) fn run_next_background_flush_watermark_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        // No pending-flush gate: the watermark advance (cases A and B below) is proven
        // against the *current* published table manifest, so an unpublished in-flight
        // flush is simply not counted — advancing to already-covered L0 while a flush
        // is active is safe and is what lets WAL reclaim keep pace under write pressure.
        let Some(task) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::FlushWatermark)
        else {
            return Ok(None);
        };
        if let Some(outcome) = self.run_background_flush_watermark_if_checkpoint_covered(task)? {
            return Ok(Some(outcome));
        }
        if !self.flush_watermark_task_has_table_coverage(task)? {
            let Some(candidate) = self.highest_coverable_flush_watermark_candidate()? else {
                return Ok(None);
            };
            if !self
                .maintenance
                .replace_pending_flush_watermark_candidate(task.id(), candidate)?
            {
                return Ok(None);
            }
        }
        self.run_next_flush_watermark_maintenance()
    }

    fn run_background_flush_watermark_if_checkpoint_covered(
        &mut self,
        task: MaintenanceTask,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let Some(candidate) = task.flush_watermark_candidate() else {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "flush watermark task requires a candidate",
            });
        };
        let current_manifest = self
            .services
            .manifest()
            .load_required()
            .map_err(manifest_error)?;
        let Some(snapshot_watermark) = current_manifest
            .snapshot_watermark()
            .map(CommitVersion::new)
        else {
            return Ok(None);
        };
        if candidate > snapshot_watermark {
            return Ok(None);
        }
        let state = self.state;
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == task.id())?
        else {
            return Ok(None);
        };
        let outcome = self.persist_flush_watermark(
            candidate,
            &LifecycleFlushWatermarkProof::CheckpointCovered { snapshot_watermark },
        );
        let maintenance = match outcome {
            Ok(outcome) => outcome.maintenance_outcome(),
            Err(error) => MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                .with_source_error(error),
        };
        let outcome = self.maintenance.finish_started(task, maintenance, false)?;
        self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
        Ok(Some(outcome))
    }

    fn flush_watermark_task_has_table_coverage(
        &self,
        task: MaintenanceTask,
    ) -> LifecycleResult<bool> {
        let Some(candidate) = task.flush_watermark_candidate() else {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "flush watermark task requires a candidate",
            });
        };
        let branch_id = self.initial_branch_id;
        let Some(manifest) = self
            .services
            .table_manifest()
            .load_current(branch_id)
            .map_err(manifest_error)?
        else {
            return Ok(false);
        };
        let branch = self
            .branch_catalog
            .branch_state(branch_id)
            .expect("seeded branch is always present in the catalog");
        let current_manifest = self
            .services
            .manifest()
            .load_required()
            .map_err(manifest_error)?;
        let floor = wal_retention_watermark(
            current_manifest
                .snapshot_watermark()
                .map(CommitVersion::new),
            current_manifest.flushed_through_commit_id(),
        )
        .unwrap_or(CommitVersion::ZERO);
        match self.flush_watermark_candidate_has_table_coverage(candidate, branch, &manifest, floor)
        {
            Ok(()) => Ok(self.branch_catalog.registry().active_branch_ids() == vec![branch_id]),
            Err(LifecycleError::WalRetentionProofIncomplete { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn highest_coverable_flush_watermark_candidate(
        &self,
    ) -> LifecycleResult<Option<CommitVersion>> {
        let branch_id = self.initial_branch_id;
        if self.branch_catalog.registry().active_branch_ids() != vec![branch_id] {
            return Ok(None);
        }
        let Some(manifest) = self
            .services
            .table_manifest()
            .load_current(branch_id)
            .map_err(manifest_error)?
        else {
            return Ok(None);
        };
        let current_manifest = self
            .services
            .manifest()
            .load_required()
            .map_err(manifest_error)?;
        let retention_watermark = wal_retention_watermark(
            current_manifest
                .snapshot_watermark()
                .map(CommitVersion::new),
            current_manifest.flushed_through_commit_id(),
        );
        let branch = self
            .branch_catalog
            .branch_state(branch_id)
            .expect("seeded branch is always present in the catalog");
        let floor = retention_watermark.unwrap_or(CommitVersion::ZERO);
        for candidate in flush_watermark_candidates_from_manifest(
            &manifest,
            self.visible.visible_version(),
            floor,
        ) {
            match self
                .flush_watermark_candidate_has_table_coverage(candidate, branch, &manifest, floor)
            {
                Ok(()) => return Ok(Some(candidate)),
                Err(LifecycleError::WalRetentionProofIncomplete { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    fn flush_watermark_candidate_has_table_coverage(
        &self,
        candidate: CommitVersion,
        branch: &BranchLocalState,
        manifest: &TableManifest,
        floor: CommitVersion,
    ) -> LifecycleResult<()> {
        // `floor` is the current checkpoint watermark — the same value
        // `validate_extends_checkpoint` proves the candidate extends — so it both
        // bounds the coverage scan to the needed tail and validates the extension.
        let proof = LifecycleTableManifestFlushCoverageProof::from_branch_manifest_with_floor(
            candidate,
            branch,
            manifest,
            &self.current_recovery_health,
            floor,
        )?;
        proof.validate_extends_checkpoint(floor)
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
    ) -> LifecycleResult<Option<DurableBackgroundMaintenanceStep<'a>>> {
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
            // Skip candidates that would contend with an in-flight rewrite (same branch,
            // equal/adjacent level) so concurrent workers pick disjoint levels. Correctness
            // does not rely on this — a conflicting compaction that slips through is rejected
            // at publish by candidate revalidation; this only avoids wasted build work.
            .filter(|task| !self.maintenance.rewrite_conflicts_with_active(*task))
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

    /// #3502 Slice D: build the version-pruning policy + safety proof for a
    /// compaction of `branch_id` from live state, when the database has opted
    /// into a keep-newer-than retention window. Returns `None` (`KeepAll` —
    /// compaction retains every version) whenever any safety precondition is
    /// unmet, so a cycle that cannot prove pruning safe simply does not prune.
    /// Called BEFORE the `&mut branch` borrow so it can read sibling branch
    /// state for the shared-table gate.
    /// #3502 Slice D2: true when no OTHER active branch shares a table with
    /// `branch_id` — the layering-clean shared-table safety predicate that
    /// backs the pruning proof's `candidate_tables_not_shared`. Builds a fresh
    /// `SharedTableRegistry` over every active branch's reachability snapshot,
    /// then requires every table this branch references to be referenced by it
    /// alone (reference count 1). A COW fork re-references the parent's table
    /// identities, so a shared table lifts its count above 1 and the branch is
    /// refused; independent roots hold disjoint identities and pass. Any lookup
    /// or snapshot failure is treated conservatively as "cannot prove unshared"
    /// and refuses the prune.
    fn branch_tables_unshared_with_other_branches(&self, branch_id: BranchId) -> bool {
        let active = self.branch_catalog.registry().active_branch_ids();
        let mut snapshots = Vec::with_capacity(active.len());
        for id in &active {
            let Ok(state) = self.branch_catalog.branch_state(*id) else {
                return false;
            };
            let Ok(snapshot) = state.reachability_snapshot() else {
                return false;
            };
            snapshots.push(snapshot);
        }
        let Ok(registry) =
            crate::branch::facts::SharedTableRegistry::rebuild_from_snapshots(&snapshots)
        else {
            return false;
        };
        let Some(mine) = snapshots.iter().find(|s| s.branch_id() == branch_id) else {
            return false;
        };
        mine.table_refs()
            .iter()
            .all(|table_ref| registry.reference_count(table_ref.table_identity()) == 1)
    }

    fn build_version_pruning_for_compaction(
        &self,
        branch_id: BranchId,
    ) -> Option<(
        crate::branch::state::compaction::BranchCompactionRetentionPolicy,
        crate::branch::pruning::BranchCompactionPruningProof,
    )> {
        let crate::lifecycle::StorageVersionRetentionPolicy::KeepRecentVersions { window } =
            self.open_plan.lifecycle_config().version_retention()
        else {
            return None;
        };
        // Recovery must be healthy to attest the proof (DUR-017 / ARCH-005).
        if !self.current_recovery_health.is_healthy() {
            return None;
        }
        // Shared-table safety: prune only when no OTHER active branch shares a
        // table with this one, so a prune that rewrites/drops rows cannot
        // affect a COW fork's reads. Derived per-table from a fresh
        // `SharedTableRegistry` over every active branch's reachability
        // snapshot — independent roots (the engine's `_system_` branch,
        // sibling product roots) hold disjoint table identities and pass; a COW
        // fork shares identities and is refused. This replaces the earlier
        // single-branch count proxy, which baked the engine's "always one
        // system branch" topology into the storage layer (D2).
        if !self.branch_tables_unshared_with_other_branches(branch_id) {
            return None;
        }
        let branch = self.branch_catalog.branch_state(branch_id).ok()?;
        // No readable inherited layers over the candidates (a COW child).
        if !branch.inherited_layers().is_empty() {
            return None;
        }
        let visible = branch.max_commit_version()?;
        // keep-newer-than: retain versions at or above `visible - window`.
        let floor = strata_core::CommitVersion::new(visible.as_u64().saturating_sub(window));
        if floor == strata_core::CommitVersion::ZERO {
            return None;
        }
        // The timestamp floor is the floor version's commit timestamp — the
        // boundary the post-prune `CompleteSince` coverage records. Requires a
        // complete-from-birth timeline (D0); an unproven lookup skips pruning.
        let crate::timeline_index::RetainedVersionLookup::Found(timestamp_floor) = branch
            .retained_timeline()
            .timestamp_for_version(floor, None)
        else {
            return None;
        };
        // #3520: the proof's apply-time validation requires timestamp coverage
        // that attests this floor. A branch that recovered `Unknown` coverage (a
        // created-never-pruned branch reopened with retention now opted in — no
        // D0 completeness marker on reopen) cannot attest it, so building the
        // proof here would hard-fail the compaction at apply
        // (`TimestampFloorWithoutCoverage`). Validate coverage up front and SKIP
        // (no-prune → KeepAll for this compaction) instead.
        if !branch
            .timestamp_coverage()
            .covers_timestamp_floor(timestamp_floor)
        {
            return None;
        }
        let proof =
            crate::branch::pruning::BranchCompactionPruningProof::from_branch_state(branch, floor)
                .ok()?
                .with_retained_timestamp_floor(timestamp_floor)
                .ok()?
                .with_no_readable_inherited_layers()
                .ok()?
                .with_candidate_tables_not_shared()
                .ok()?
                .with_recovery_health(
                    crate::branch::pruning::BranchRecoveryHealthAttestation::Healthy,
                )
                .ok()?;
        Some((
            crate::branch::state::compaction::BranchCompactionRetentionPolicy::DropOlderVersions,
            proof,
        ))
    }

    /// #3526: re-validate a version-pruning proof against the LIVE branch at
    /// publish, for the OFF-LOCK background path. The proof was built at start
    /// from a start-time snapshot; by publish a fork may share the tables it
    /// prunes — the proof's frozen shared-table gate cannot see a fork that
    /// changed neither this branch's fingerprint nor that gate (Hazard A) — or a
    /// concurrent prune may have moved the floor. This re-checks only the SAFETY
    /// gates a concurrent op can flip (live shared-table safety, floor movement,
    /// inherited layers, recovery health, timestamp coverage), and deliberately
    /// NOT the content fingerprint, which a benign above-floor commit trips
    /// (Hazard B) and which is irrelevant to a below-floor drop. `false` → the
    /// caller defers and discards the pruned output; the next pass re-derives a
    /// fresh proof (or compacts as `KeepAll` while the tables stay shared).
    fn revalidate_pruning_for_publish(
        &self,
        branch_id: BranchId,
        proof: &crate::branch::pruning::BranchCompactionPruningProof,
    ) -> bool {
        let tables_unshared = self.branch_tables_unshared_with_other_branches(branch_id);
        let recovery_healthy = self.current_recovery_health.is_healthy();
        let Ok(branch) = self.branch_catalog.branch_state(branch_id) else {
            return false;
        };
        let inherited_layers_empty = branch.inherited_layers().is_empty();
        let floor_not_regressed = retained_floor_not_regressed(
            branch.retained_history_floor(),
            proof.retained_version_floor(),
        );
        let timestamp_coverage_ok = proof
            .retained_timestamp_floor()
            .is_none_or(|floor| branch.timestamp_coverage().covers_timestamp_floor(floor));
        pruning_publish_gates_hold(
            tables_unshared,
            recovery_healthy,
            inherited_layers_empty,
            floor_not_regressed,
            timestamp_coverage_ok,
        )
    }

    pub(crate) fn run_compaction_maintenance_task(
        &mut self,
        task_id: MaintenanceTaskId,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let state = self.state;
        let Some(task) = self.maintenance.next_matching_task(|task| {
            task.id() == task_id && task.kind() == MaintenanceTaskKind::Compaction
        }) else {
            return Ok(None);
        };
        let (branch_id, level) = table_level_scope_from_task(task)?;
        // A branch-scoped task legally races a branch delete: the target's descriptor
        // survives with status Deleted and every state accessor below would refuse.
        // The work is vacuously complete (delete cleanup reclaims the tables) —
        // consume the task as Canceled. Deferred would re-run it against a re-created
        // name's new generation; Failed would ring a legal race; propagating (the old
        // behavior) failed the whole drain.
        if self.branch_catalog.branch_is_deleted(branch_id) {
            let Some(task) = self
                .maintenance
                .start_next_matching(state, |queued| queued.id() == task_id)?
            else {
                return Ok(None);
            };
            let outcome = deleted_scope_canceled_outcome(task.kind());
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(Some(outcome));
        }
        // Hold the per-branch publish slot across this foreground rewrite so its manifest fsync
        // cannot run concurrently with a background off-lock fsync for the same branch. Defer
        // (try-lock only) if the slot is busy rather than block the global lock.
        let Some(_publish_guard) = self.try_acquire_branch_publish_guard(branch_id) else {
            return Ok(Some(
                self.defer_started_task_for_publish_busy(task_id, task.kind())?,
            ));
        };
        // Pre-sync shadow into catalog so the runner sees direct shadow
        // mutations (test-only) before fetching from the catalog.
        let generation = self
            .branch_catalog
            .registry()
            .lookup(branch_id)
            .map_err(commit_error)?
            .generation();
        // #3502 Slice D: build the version-pruning proof from live state BEFORE
        // taking the branch `&mut` borrow (it reads sibling branch state).
        let version_pruning = self.build_version_pruning_for_compaction(branch_id);
        let outcome = {
            let maintenance = &mut self.maintenance;
            let branch = self
                .branch_catalog
                .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))?;
            let table_object = self.services.table_object();
            let table_reader = self.services.table_reader();
            let table_manifest = self.services.table_manifest();
            let table_catalog = &mut self.table_catalog;
            let budget = &self.budget;
            let mut runner = DurableCompactionMaintenanceRunner {
                branch,
                table_object,
                table_reader,
                table_manifest,
                table_catalog,
                budget,
                version_pruning,
                sweep_staged: &self.sweep_staged_names,
                data_block_bytes: self.open_plan.lifecycle_config().data_block_bytes(),
                table_compression: self.open_plan.lifecycle_config().table_compression(),
                compaction_io_policy: self.open_plan.lifecycle_config().compaction_io_policy(),
            };
            maintenance.run_next_matching(state, &mut runner, |task| task.id() == task_id)
        }?;
        // BS2.3: compaction rewrote the branch's tables; republish the branch snapshot.
        self.publish_branch_snapshot(branch_id);
        self.note_cache_preheat_trigger();
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
        // Table-object GC: a completed foreground compaction dropped its input refs; enqueue the
        // coalescing mark (best-effort — a rejected enqueue defers reclaim to the next cycle).
        if outcome
            .as_ref()
            .is_some_and(|completed| completed.status() == MaintenanceOutcomeStatus::Completed)
        {
            let _ =
                self.enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch_id));
        }
        Ok(outcome)
    }

    fn start_background_compaction_task(
        &mut self,
        task: MaintenanceTask,
    ) -> LifecycleResult<Option<DurableBackgroundMaintenanceStep<'a>>> {
        let state = self.state;
        let (branch_id, level) = table_level_scope_from_task(task)?;
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == task.id())?
        else {
            return Ok(None);
        };
        // Same enqueue/delete race as the foreground runner: a Deleted target
        // cancels the task instead of failing it into the ring.
        if self.branch_catalog.branch_is_deleted(branch_id) {
            let outcome = deleted_scope_canceled_outcome(task.kind());
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
        }
        let generation = match self.branch_catalog.registry().lookup(branch_id) {
            Ok(descriptor) => descriptor.generation(),
            Err(error) => {
                let outcome =
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                        .with_source_error(commit_error(error));
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
            }
        };
        // #3526: build the version-pruning proof from live state BEFORE the
        // `&mut branch` borrow (it reads sibling branch state), mirroring the
        // foreground path (`run_compaction_maintenance_task`). It is attached to
        // the request so the OFF-LOCK build prunes the snapshot, then
        // re-validated against the live branch under the lock at publish
        // (`revalidate_pruning_for_publish`) — the proof is fingerprint-bound to
        // this snapshot and cannot be trusted after the off-lock window.
        let version_pruning = self.build_version_pruning_for_compaction(branch_id);
        let branch = match self
            .branch_catalog
            .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))
        {
            Ok(branch) => branch,
            Err(error) => {
                let outcome =
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                        .with_source_error(error);
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
            }
        };
        let budget = self.open_plan.lifecycle_config().storage_budget();
        let request = match current_compaction_request_from_maintenance_task_with_budget(
            &task,
            branch,
            Some(budget),
        ) {
            // B2: stamp the per-database data-block byte target at dispatch.
            Ok(Some(request)) => {
                let request = request
                    .with_data_block_bytes(self.open_plan.lifecycle_config().data_block_bytes())
                    .with_table_compression(self.open_plan.lifecycle_config().table_compression());
                // #3526: opt-in pruning — the build applies it on the snapshot.
                match version_pruning {
                    Some((policy, proof)) => request
                        .with_retention_policy(policy)
                        .with_pruning_proof(proof),
                    None => request,
                }
            }
            Ok(None) => {
                crate::observability::perf_trace::record_lifecycle_background_candidate_stale_deferred(
                );
                let outcome = stale_compaction_maintenance_outcome();
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
            }
            Err(error) => {
                let outcome =
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                        .with_source_error(error);
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
            }
        };
        let compaction_io_policy = self.open_plan.lifecycle_config().compaction_io_policy();
        if let Some(outcome) =
            defer_compaction_for_resource_policy(branch, &request, compaction_io_policy)?
        {
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
        }
        Ok(Some(DurableBackgroundMaintenanceStep::Build(Box::new(
            DurableBackgroundMaintenanceBuild::Compaction {
                task,
                branch_id,
                level,
                request,
                branch_snapshot: branch.clone(),
                table_object: self.services.table_object().clone(),
                table_reader: self.services.table_reader().clone(),
                budget: self.budget.clone(),
                inflight: self.inflight_outputs.clone(),
            },
        ))))
    }

    fn requeue_flush_preempted_compaction(&mut self, branch_id: strata_core::BranchId, level: u8) {
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
        let Some(task) = self.maintenance.next_matching_task(|task| {
            task.id() == task_id && task.kind() == MaintenanceTaskKind::Materialization
        }) else {
            return Ok(None);
        };
        let branch_id = branch_id_from_inherited_layer_task(task)?;
        // Hold the per-branch publish slot across this foreground materialization so its manifest
        // fsync cannot run concurrently with a background off-lock fsync for the same branch.
        let Some(_publish_guard) = self.try_acquire_branch_publish_guard(branch_id) else {
            return Ok(Some(
                self.defer_started_task_for_publish_busy(task_id, task.kind())?,
            ));
        };
        // Pre-sync shadow into catalog so the runner sees direct shadow
        // mutations (test-only) before fetching from the catalog.
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
            let table_object = self.services.table_object();
            let table_reader = self.services.table_reader();
            let table_manifest = self.services.table_manifest();
            let table_catalog = &mut self.table_catalog;
            let budget = &self.budget;
            let mut runner = DurableMaterializationMaintenanceRunner {
                branch,
                table_object,
                table_reader,
                table_manifest,
                table_catalog,
                budget,
                sweep_staged: &self.sweep_staged_names,
            };
            maintenance.run_next_matching(state, &mut runner, |task| task.id() == task_id)
        }?;
        if outcome
            .as_ref()
            .is_some_and(table_rewrite_outcome_allows_chain_resubmit)
        {
            self.resubmit_table_rewrite_if_any_branch_still_unhealthy(branch_id);
        }
        // Table-object GC: a completed materialization replaced inherited-layer refs; enqueue the
        // coalescing mark (best-effort — a rejected enqueue defers reclaim to the next cycle).
        if outcome
            .as_ref()
            .is_some_and(|completed| completed.status() == MaintenanceOutcomeStatus::Completed)
        {
            let _ =
                self.enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(branch_id));
        }
        Ok(outcome)
    }

    fn start_background_materialization_task(
        &mut self,
        task: MaintenanceTask,
    ) -> LifecycleResult<Option<DurableBackgroundMaintenanceStep<'a>>> {
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
                let outcome =
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                        .with_source_error(commit_error(error));
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
            }
        };
        let branch = match self
            .branch_catalog
            .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))
        {
            Ok(branch) => branch,
            Err(error) => {
                let outcome =
                    MaintenanceOutcome::new(task.kind(), MaintenanceOutcomeStatus::Failed)
                        .with_source_error(error);
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
            }
        };
        match begin_durable_materialization_build(branch, &request)? {
            DurableMaterializationBegin::Deferred(outcome) => {
                let outcome =
                    self.maintenance
                        .finish_started(task, outcome.maintenance_outcome(), false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)))
            }
            DurableMaterializationBegin::Build(build) => {
                Ok(Some(DurableBackgroundMaintenanceStep::Build(Box::new(
                    DurableBackgroundMaintenanceBuild::Materialization {
                        task,
                        branch_id,
                        build: *build,
                        table_object: self.services.table_object().clone(),
                        table_reader: self.services.table_reader().clone(),
                        budget: self.budget.clone(),
                        inflight: self.inflight_outputs.clone(),
                    },
                ))))
            }
        }
    }

    #[allow(
        dead_code,
        reason = "runtime maintenance entry point is consumed by dedicated tests"
    )]
    /// Whether any build-producing task is currently in flight (executor passthrough). The
    /// drain's interleave and the table-object mark/sweep all defer on this condition.
    pub(crate) fn has_active_build_task(&self) -> bool {
        self.maintenance.has_active_build_task()
    }

    /// Whether any low-tier maintenance (retention / pruning / quarantine / purge / repair /
    /// health) is queued. The background drain's anti-starvation interleave consults this so a
    /// sustained stream of upper-tier work cannot starve reclaim indefinitely.
    /// C2: the durable table set changed; arm the preheat so the ladder's
    /// low tier tops up the block cache once the queue is otherwise idle.
    /// A flag, not an enqueue — the queue never holds a standing preheat
    /// task (see the field rationale on the runtime struct).
    pub(super) fn note_cache_preheat_trigger(&mut self) {
        if self.open_plan.lifecycle_config().cache_preheat_policy()
            == LifecycleCachePreheatPolicy::WhenIdle
        {
            self.cache_preheat_rearm = true;
            // A suspended pass (saturation/deferral) resumes on any trigger.
            self.cache_preheat_paused = false;
        }
    }

    /// C3a: preheat work exists when a fresh pass is owed (`rearm`) or an
    /// in-flight pass has a runnable cursor. Feeds the drain-round re-arm
    /// accounting and the low-tier interleave.
    pub(crate) fn cache_preheat_work_pending(&self) -> bool {
        self.cache_preheat_rearm
            || (self.cache_preheat_cursor.is_some() && !self.cache_preheat_paused)
    }

    pub(crate) fn has_pending_low_tier_maintenance(&self) -> bool {
        self.cache_preheat_work_pending()
            || self.maintenance.pending_tasks().iter().any(|task| {
                matches!(
                    task.kind(),
                    MaintenanceTaskKind::Retention
                        | MaintenanceTaskKind::SnapshotPruning
                        | MaintenanceTaskKind::Quarantine
                        | MaintenanceTaskKind::Purge
                        | MaintenanceTaskKind::Repair
                        | MaintenanceTaskKind::HealthCollection
                        | MaintenanceTaskKind::CachePreheat
                )
            })
    }

    pub(crate) fn run_next_retention_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        // Existence check BEFORE the mark scan: the pinned-object mark below is
        // O(branches x tables) with allocations, and this entry point runs on
        // every drain poll whose upper tiers are empty. Paying the scan without
        // a pending task measured 29.7s of a 36.5s YCSB-A run held INSIDE the
        // runtime lock (~0.8ms x 37K empty probes) — the durable write path's
        // dominant stall (v1 e2e baseline, billion-scale-ledger.md).
        if !self.maintenance.pending_tasks().iter().any(|task| {
            matches!(
                task.kind(),
                MaintenanceTaskKind::SnapshotPruning | MaintenanceTaskKind::Retention
            )
        }) {
            return Ok(None);
        }
        let state = self.state;
        // #2524: the mark runs DURING builds now — in-flight outputs are
        // pinned by name (reserved before their bytes land), so the wholesale
        // `has_active_build_task` defer that starved reclaim under sustained
        // load (zero retention runs across a whole seed) is gone.
        let pinned_objects = self.reclaim_pinned_table_objects();
        let maintenance = &mut self.maintenance;
        let services = &self.services;
        let health = self.current_recovery_health.clone();
        let branch_id = self.initial_branch_id;
        let pending_releases = &mut self.pending_releases;
        let pending_releases_sequence = &mut self.pending_releases_sequence;
        let mut runner = DurableRetentionMaintenanceRunner {
            services,
            branch_id,
            health,
            pending_releases,
            pending_releases_sequence,
            pinned_objects,
        };
        let outcome = maintenance.run_next_matching(state, &mut runner, |task| {
            matches!(
                task.kind(),
                MaintenanceTaskKind::SnapshotPruning | MaintenanceTaskKind::Retention
            )
        });
        self.record_optional_maintenance_health(&outcome);
        // A completed retention pass may have marked unreachable table objects; the sweep
        // (Quarantine task) acts on a fresh mark of its own, so chaining is unconditional and
        // idempotent — a sweep with no candidates completes trivially, and coalescing folds
        // repeats. Best-effort: a rejected enqueue only delays reclaim to the next cycle.
        if matches!(
            &outcome,
            Ok(Some(completed)) if completed.task_kind() == MaintenanceTaskKind::Retention
                && completed.status() == MaintenanceOutcomeStatus::Completed
        ) {
            crate::observability::perf_trace::record_table_object_retention_run();
            let _ = self.enqueue_maintenance(MaintenanceTaskRequest::quarantine());
        }
        outcome
    }

    #[allow(
        dead_code,
        reason = "runtime maintenance entry point is consumed by dedicated tests"
    )]
    pub(crate) fn run_next_purge_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let Some(task) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::Purge)
        else {
            return Ok(None);
        };
        self.run_purge_maintenance_task(task.id())
    }

    pub(crate) fn run_purge_maintenance_task(
        &mut self,
        task_id: MaintenanceTaskId,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let state = self.state;
        let maintenance = &mut self.maintenance;
        let quarantine = self.services.quarantine();
        let database_id = *self.services.assembly_facts().database_id();
        let codec_id = LifecycleCodecId::new(self.services.assembly_facts().codec_id())?;
        let health = self.current_recovery_health.clone();
        let default_branch_id = self.initial_branch_id;
        let mut runner = DurablePurgeMaintenanceRunner {
            quarantine,
            database_id,
            codec_id,
            health,
            default_branch_id,
        };
        let outcome = maintenance.run_next_matching(state, &mut runner, |task| {
            task.id() == task_id && task.kind() == MaintenanceTaskKind::Purge
        });
        self.record_optional_maintenance_health(&outcome);
        outcome
    }

    #[allow(
        dead_code,
        reason = "runtime maintenance entry point is consumed by dedicated tests"
    )]
    pub(crate) fn run_next_quarantine_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let Some(task) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::Quarantine)
        else {
            return Ok(None);
        };
        self.run_quarantine_maintenance_task(task.id())
    }

    /// #2524 test seam: the in-flight build-output registry, so tests can
    /// observe reservations and pin objects directly.
    #[cfg(test)]
    pub(crate) fn inflight_table_outputs(&self) -> &super::inflight::InFlightTableOutputs {
        &self.inflight_outputs
    }

    pub(crate) fn run_quarantine_maintenance_task(
        &mut self,
        task_id: MaintenanceTaskId,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let state = self.state;
        let database_id = *self.services.assembly_facts().database_id();
        let codec_id = LifecycleCodecId::new(self.services.assembly_facts().codec_id())?;
        let staged_at = checkpoint_created_at(
            self.allocator.timestamp_guard().last_allocated(),
            self.recovered_checkpoint_timestamp_max,
        );
        let retired_readers_alive = self.snapshot_publisher.retired_views_alive();
        // #2524: in-flight build outputs are pinned by name (see the
        // background sweep entry) — no wholesale build defer.
        let pinned_objects = self.reclaim_pinned_table_objects();
        let mut runner = DurableTableObjectSweepRunner {
            services: &self.services,
            branch_id: self.initial_branch_id,
            health: self.current_recovery_health.clone(),
            database_id,
            codec_id,
            staged_at,
            retired_readers_alive,
            pinned_objects,
            quarantined_objects: 0,
            staged_bytes: 0,
            remaining_candidates: 0,
            sweep_health: None,
        };
        let maintenance = &mut self.maintenance;
        let outcome = maintenance.run_next_matching(state, &mut runner, |task| {
            task.id() == task_id && task.kind() == MaintenanceTaskKind::Quarantine
        });
        let quarantined = runner.quarantined_objects;
        let remaining = runner.remaining_candidates;
        self.record_optional_maintenance_health(&outcome);
        if quarantined > 0 {
            // Best-effort: the staged bytes are reclaimed by the purge; if the enqueue is
            // rejected (budget/shutdown), the next sweep cycle re-stages idempotently.
            let _ = self.enqueue_maintenance(MaintenanceTaskRequest::purge_quarantine(
                self.initial_branch_id,
            ));
        }
        if remaining > 0 {
            // Best-effort: capped/deferred candidates re-mark on the next retention cycle.
            let _ = self.enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(
                self.initial_branch_id,
            ));
        }
        outcome
    }

    /// BS5.5 background sweep entry: the mark (inventory listing + manifest
    /// decode) and the interlock checks run under the lock; the per-object
    /// staging I/O (a quarantine publish plus a source delete, several fsyncs
    /// each — measured ~320ms lock holds per pass under YCSB churn) returns as
    /// a [`DurableBackgroundMaintenanceStep::SweepStage`] and runs off-lock.
    pub(crate) fn start_next_background_quarantine_sweep(
        &mut self,
    ) -> LifecycleResult<Option<DurableBackgroundMaintenanceStep<'a>>> {
        let Some(pending) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::Quarantine)
        else {
            return Ok(None);
        };
        let state = self.state;
        let database_id = *self.services.assembly_facts().database_id();
        let codec_id = LifecycleCodecId::new(self.services.assembly_facts().codec_id())?;
        let staged_at = checkpoint_created_at(
            self.allocator.timestamp_guard().last_allocated(),
            self.recovered_checkpoint_timestamp_max,
        );
        let retired_readers_alive = self.snapshot_publisher.retired_views_alive();
        // #2524: in-flight build outputs are pinned by name (reserved before
        // their bytes land), so the mark no longer defers wholesale on
        // `has_active_build_task` — reclaim stays live under sustained load.
        let pinned_objects = self.reclaim_pinned_table_objects();
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == pending.id())?
        else {
            return Ok(None);
        };
        // Fresh mark under this lock hold (metadata-only: inventory listing + manifest decode,
        // never table data).
        let candidates = table_object_retention_request(
            &self.services,
            self.initial_branch_id,
            &self.current_recovery_health,
        )
        .map(|request| request.with_pinned_objects(pinned_objects))
        .and_then(|request| table_object_retention_outcome(&request));
        let candidates: Vec<crate::object::ObjectName> = match candidates {
            Ok(outcome) => outcome
                .decisions()
                .iter()
                .filter(|decision| {
                    decision.decision() == crate::lifecycle::RetentionDecision::QuarantineCandidate
                })
                .filter_map(|decision| decision.object().cloned())
                .collect(),
            Err(error) => {
                let outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Quarantine, {
                    MaintenanceOutcomeStatus::Failed
                })
                .with_source_error(error);
                let outcome = self.maintenance.finish_started(task, outcome, false)?;
                self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
                return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
            }
        };
        if candidates.is_empty() {
            let outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Quarantine, {
                MaintenanceOutcomeStatus::Completed
            })
            .with_reason("no unreachable table objects")
            .with_stats(LifecycleStats::new(0, 0, 1, 0, 0));
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
        }
        if retired_readers_alive {
            // Interlock deferral does NOT self-requeue (that would spin the retention →
            // quarantine chain against a long-held reader); the next rewrite publish, reopen,
            // or explicit Reclaim re-triggers the cycle. Retired readers remain a wholesale
            // defer: they are name-addressed with no held fd, and no per-object bookkeeping
            // says which candidate a live view might still fetch blocks from.
            crate::observability::perf_trace::record_table_object_sweep_deferred(false);
            let outcome = MaintenanceOutcome::new(
                MaintenanceTaskKind::Quarantine,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason("table-object sweep deferred: retired read view still held")
            .with_deferral_reason(MaintenanceDeferralReason::ReaderPinned)
            .with_stats(LifecycleStats::new(0, 0, 1, 1, 0));
            let outcome = self.maintenance.finish_started(task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(Some(DurableBackgroundMaintenanceStep::completed(outcome)));
        }
        let capped: Vec<crate::object::ObjectName> = candidates
            .iter()
            .take(TABLE_OBJECT_SWEEP_MAX_OBJECTS)
            .cloned()
            .collect();
        let remaining_candidates = candidates.len().saturating_sub(capped.len());
        // #2553: freeze the staged names in the sweep registry BEFORE the
        // lock is released — installs check it so a content-identical rewrite
        // output cannot adopt an object this stage deletes off-lock.
        let staged_guard = self.sweep_staged_names.guard();
        for name in &capped {
            staged_guard.reserve(name.clone());
        }
        Ok(Some(DurableBackgroundMaintenanceStep::SweepStage(
            Box::new(SweepStageInputs {
                task,
                branch_id: self.initial_branch_id,
                database_id,
                codec_id,
                staged_at,
                health: self.current_recovery_health.clone(),
                candidates: capped,
                remaining_candidates,
                quarantine: self.services.quarantine().clone(),
                block_cache: self.services.table_object().block_cache().cloned(),
                staged_guard,
            }),
        )))
    }

    /// Fold the off-lock sweep staging back in under the lock: finish the
    /// started task and chain the follow-up purge / re-mark enqueues.
    pub(crate) fn finish_quarantine_sweep(
        &mut self,
        staged: SweepStaged,
    ) -> LifecycleResult<MaintenanceOutcome> {
        if let Some(error) = staged.request_error {
            let outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Quarantine, {
                MaintenanceOutcomeStatus::Failed
            })
            .with_source_error(error);
            let outcome = self
                .maintenance
                .finish_started(staged.task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(outcome);
        }
        crate::observability::perf_trace::record_table_object_sweep_run();
        let staged_count = staged.staged_names.len();
        let mut outcome = MaintenanceOutcome::new(
            MaintenanceTaskKind::Quarantine,
            MaintenanceOutcomeStatus::Completed,
        )
        .with_affected_object_names(staged.staged_names)
        .with_effects(staged_count, staged.staged_bytes, false)
        .with_state_changes(staged.quarantined_objects)
        .with_stats(LifecycleStats::new(0, staged.faults, 1, 0, 0));
        if let Some(health) = staged.sweep_health {
            outcome = outcome.with_recovery_health(health);
        }
        let outcome = self
            .maintenance
            .finish_started(staged.task, outcome, false)?;
        self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
        if staged.quarantined_objects > 0 {
            // Best-effort: the staged bytes are reclaimed by the purge; if the enqueue is
            // rejected (budget/shutdown), the next sweep cycle re-stages idempotently.
            let _ = self.enqueue_maintenance(MaintenanceTaskRequest::purge_quarantine(
                self.initial_branch_id,
            ));
        }
        if staged.remaining_candidates > 0 {
            // Best-effort: capped candidates re-mark on the next retention cycle.
            let _ = self.enqueue_maintenance(MaintenanceTaskRequest::table_object_retention(
                self.initial_branch_id,
            ));
        }
        if staged.quarantined_objects > 0 {
            // C2: sweeping dead tables dropped their cache blocks, freeing the
            // very capacity a saturation-stopped preheat chain was waiting on —
            // and late in a quiet period no publish will re-arm it. The sweep
            // is that re-arm (the kept cursor resumes the walk).
            self.note_cache_preheat_trigger();
        }
        Ok(outcome)
    }

    /// C2 background preheat entry: when the trigger flag is armed, enqueue
    /// and claim the transient task in this same lock hold (the queue never
    /// observably holds a pending preheat), capture the owned inputs, and
    /// return a [`DurableBackgroundMaintenanceStep::PreheatStage`]; the table
    /// reads (the bulk IO) run off-lock. Optional work — a deferral (global
    /// memory pressure, build task in flight) disarms the flag and waits for
    /// the next trigger rather than spinning.
    pub(crate) fn start_next_background_cache_preheat(
        &mut self,
    ) -> LifecycleResult<Option<DurableBackgroundMaintenanceStep<'a>>> {
        if !self.cache_preheat_work_pending() {
            return Ok(None);
        }
        if self.open_plan.lifecycle_config().cache_preheat_policy()
            == LifecycleCachePreheatPolicy::Disabled
        {
            // Belt over the trigger gates (they are policy-checked too).
            self.cache_preheat_rearm = false;
            self.cache_preheat_cursor = None;
            self.cache_preheat_paused = false;
            return Ok(None);
        }
        self.refresh_runtime_memory_total();
        // Pressure gate EXCLUDES the block cache's own resident bytes: the
        // pool is self-evicting and budget-bounded, so cache-at-capacity is
        // its intended state — gating the fill on a total the fill itself
        // raises froze coverage at ~85% (permanent deferral past the 80%
        // high water). Non-cache pressure and in-flight builds still defer.
        let cache_bytes = self.services.table_object().block_cache_resident_bytes();
        if self
            .budget
            .global_pressure_excluding(cache_bytes)
            .defers_optional_maintenance()
            || self.maintenance.has_active_build_task()
        {
            // Pause rather than retry (a deferral loop would spin the low
            // tier) — but KEEP the cursor: the next trigger resumes the pass
            // where it stopped instead of losing the fill progress.
            self.cache_preheat_rearm = false;
            self.cache_preheat_paused = true;
            crate::observability::perf_trace::record_table_preheat_deferred();
            return Ok(None);
        }
        let state = self.state;
        let enqueued = self.enqueue_maintenance(MaintenanceTaskRequest::cache_preheat());
        let Ok(outcome) = enqueued else {
            // Best-effort (queue full / draining for close): state unchanged;
            // an idle retry or the next trigger picks it up.
            return Ok(None);
        };
        let task_id = outcome.task_id();
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == task_id)?
        else {
            return Ok(None);
        };
        // Consume the fresh-pass trigger ONLY when actually beginning a
        // fresh pass. A chunk of an in-flight pass leaves `rearm` untouched,
        // so a publish landing mid-pass still gets its follow-up pass — the
        // convergence property behind ~100% coverage at quiesce.
        if self.cache_preheat_cursor.is_none() {
            self.cache_preheat_rearm = false;
            self.cache_preheat_pass_blocks = 0;
        }
        let layouts = self
            .branch_catalog
            .list_branches(false)
            .iter()
            .filter_map(|descriptor| {
                let branch_id = descriptor.branch_id();
                self.branch_catalog
                    .branch_state(branch_id)
                    .ok()
                    .map(|branch| {
                        (
                            branch_id,
                            branch.layout_snapshot(),
                            branch.inherited_layers().to_vec(),
                        )
                    })
            })
            .collect();
        #[cfg(test)]
        let chunk_max_bytes = self
            .cache_preheat_chunk_bytes_for_test
            .unwrap_or(CACHE_PREHEAT_CHUNK_MAX_BYTES);
        #[cfg(not(test))]
        let chunk_max_bytes = CACHE_PREHEAT_CHUNK_MAX_BYTES;
        Ok(Some(DurableBackgroundMaintenanceStep::PreheatStage(
            Box::new(PreheatStageInputs {
                task,
                layouts,
                cursor: self.cache_preheat_cursor.take(),
                chunk_max_bytes,
                block_cache: self.services.table_object().block_cache().cloned(),
            }),
        )))
    }

    /// Fold the off-lock preheat chunk back in under the lock: persist the
    /// resume cursor, refresh the memory total the fill just raised, and
    /// chain the next chunk while work remains.
    pub(crate) fn finish_cache_preheat(
        &mut self,
        staged: PreheatStaged,
    ) -> LifecycleResult<MaintenanceOutcome> {
        crate::observability::perf_trace::record_table_preheat_pass(
            staged.admitted,
            staged.skipped_present,
            staged.skipped_full,
            staged.bytes_read,
        );
        self.refresh_runtime_memory_total();
        if let Some(error) = staged.stage_error {
            // Abandon the pass; a trigger set mid-pass (rearm) still starts
            // a fresh one.
            self.cache_preheat_cursor = None;
            self.cache_preheat_paused = false;
            let outcome = MaintenanceOutcome::new(
                MaintenanceTaskKind::CachePreheat,
                MaintenanceOutcomeStatus::Failed,
            )
            .with_source_error(error);
            let outcome = self
                .maintenance
                .finish_started(staged.task, outcome, false)?;
            self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
            return Ok(outcome);
        }
        self.cache_preheat_pass_blocks = self
            .cache_preheat_pass_blocks
            .saturating_add(staged.admitted)
            .saturating_add(staged.skipped_present);
        let pass_complete = staged.next_cursor.is_none();
        self.cache_preheat_cursor = staged.next_cursor;
        // Chaining rides the CURSOR: a kept cursor with `paused == false` is
        // pending work by itself (`cache_preheat_work_pending`), so no flag
        // re-arm happens here — the flag stays reserved for fresh-pass
        // triggers. Saturation suspends the pass until the next trigger
        // (the sweep that frees dead-block capacity re-arms).
        self.cache_preheat_paused = staged.stopped_full;
        let reason = if staged.stopped_full {
            "cache preheat stopped: shards saturated"
        } else if pass_complete {
            "cache preheat pass complete"
        } else {
            "cache preheat chunk complete, more pending"
        };
        if pass_complete {
            crate::observability::perf_trace::set_table_preheat_last_pass_blocks(
                self.cache_preheat_pass_blocks,
            );
        }
        let outcome = MaintenanceOutcome::new(
            MaintenanceTaskKind::CachePreheat,
            MaintenanceOutcomeStatus::Completed,
        )
        .with_reason(reason)
        .with_stats(LifecycleStats::new(0, 0, 1, 0, 0));
        let outcome = self
            .maintenance
            .finish_started(staged.task, outcome, false)?;
        self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
        Ok(outcome)
    }

    /// Inline (non-background) preheat: one bounded chunk per call, for the
    /// deterministic drain paths. Start and finish run back-to-back on the
    /// caller's thread.
    pub(crate) fn run_next_cache_preheat_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        match self.start_next_background_cache_preheat()? {
            None => Ok(None),
            Some(DurableBackgroundMaintenanceStep::Completed(outcome)) => Ok(Some(*outcome)),
            Some(DurableBackgroundMaintenanceStep::PreheatStage(inputs)) => {
                Ok(Some(self.finish_cache_preheat(inputs.stage())?))
            }
            Some(_) => unreachable!("cache preheat start returns only preheat steps"),
        }
    }

    /// BS5.5 background purge entry: capture the owned inputs under the lock
    /// and return a [`DurableBackgroundMaintenanceStep::PurgeStage`]; the
    /// inventory load and the deletes run off-lock (quarantine namespace
    /// only — nothing else references those objects).
    pub(crate) fn start_next_background_purge(
        &mut self,
    ) -> LifecycleResult<Option<DurableBackgroundMaintenanceStep<'a>>> {
        let Some(pending) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::Purge)
        else {
            return Ok(None);
        };
        let state = self.state;
        let database_id = *self.services.assembly_facts().database_id();
        let codec_id = LifecycleCodecId::new(self.services.assembly_facts().codec_id())?;
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == pending.id())?
        else {
            return Ok(None);
        };
        Ok(Some(DurableBackgroundMaintenanceStep::PurgeStage(
            Box::new(PurgeStageInputs {
                task,
                default_branch_id: self.initial_branch_id,
                database_id,
                codec_id,
                health: self.current_recovery_health.clone(),
                quarantine: self.services.quarantine().clone(),
            }),
        )))
    }

    /// Fold the off-lock purge back in under the lock.
    pub(crate) fn finish_quarantine_purge(
        &mut self,
        staged: PurgeStaged,
    ) -> LifecycleResult<MaintenanceOutcome> {
        let outcome = match staged.result {
            Ok(outcome) => outcome,
            Err(error) => MaintenanceOutcome::new(MaintenanceTaskKind::Purge, {
                MaintenanceOutcomeStatus::Failed
            })
            .with_source_error(error),
        };
        let outcome = self
            .maintenance
            .finish_started(staged.task, outcome, false)?;
        self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
        Ok(outcome)
    }

    #[allow(
        dead_code,
        reason = "runtime maintenance entry point is consumed by dedicated tests"
    )]
    pub(crate) fn run_next_quarantine_repair_maintenance(
        &mut self,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let Some(task) = self
            .maintenance
            .next_matching_task(|task| task.kind() == MaintenanceTaskKind::Repair)
        else {
            return Ok(None);
        };
        self.run_quarantine_repair_maintenance_task(task.id())
    }

    pub(crate) fn run_quarantine_repair_maintenance_task(
        &mut self,
        task_id: MaintenanceTaskId,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        let state = self.state;
        let maintenance = &mut self.maintenance;
        let quarantine = self.services.quarantine();
        let database_id = *self.services.assembly_facts().database_id();
        let codec_id = LifecycleCodecId::new(self.services.assembly_facts().codec_id())?;
        let mut runner = DurableQuarantineRepairMaintenanceRunner {
            quarantine,
            database_id,
            codec_id,
        };
        let outcome = maintenance.run_next_matching(state, &mut runner, |task| {
            task.id() == task_id && task.kind() == MaintenanceTaskKind::Repair
        });
        self.record_optional_maintenance_health(&outcome);
        outcome
    }

    fn record_optional_maintenance_health(
        &mut self,
        outcome: &LifecycleResult<Option<MaintenanceOutcome>>,
    ) {
        if let Ok(Some(outcome)) = outcome {
            self.record_recovery_health(outcome.recovery_health());
        }
    }

    /// Start a pending task and immediately finish it `Deferred` because its branch's publish slot
    /// is held by an in-flight off-lock publish. The next maintenance pass retries once the slot
    /// frees; the task is not lost. Used by the foreground rewrite paths to coordinate with the
    /// background drain's off-lock fsync without blocking the global lock.
    fn defer_started_task_for_publish_busy(
        &mut self,
        task_id: MaintenanceTaskId,
        kind: MaintenanceTaskKind,
    ) -> LifecycleResult<MaintenanceOutcome> {
        let state = self.state;
        let Some(task) = self
            .maintenance
            .start_next_matching(state, |queued| queued.id() == task_id)?
        else {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "maintenance task is no longer startable",
            });
        };
        let outcome = MaintenanceOutcome::new(kind, MaintenanceOutcomeStatus::Deferred)
            .with_reason("table manifest publish slot is busy for this branch");
        let outcome = self.maintenance.finish_started(task, outcome, false)?;
        self.record_optional_maintenance_health(&Ok(Some(outcome.clone())));
        Ok(outcome)
    }

    pub(super) fn record_recovery_health(&mut self, health: Option<&RecoveryHealth>) {
        let Some(health) = health else {
            return;
        };
        if health_rank(health) > health_rank(&self.current_recovery_health) {
            self.current_recovery_health = health.clone();
        }
    }
}

const fn health_rank(health: &RecoveryHealth) -> u8 {
    match health {
        RecoveryHealth::Healthy => 0,
        RecoveryHealth::Degraded { class, .. } => match class {
            RecoveryDegradationClass::Telemetry => 1,
            RecoveryDegradationClass::PolicyDowngrade => 2,
            RecoveryDegradationClass::DataLoss => 3,
        },
        RecoveryHealth::Failed { .. } => 4,
    }
}

struct DurableFlushMaintenanceRunner<'a, 'b> {
    branch_catalog: &'a mut crate::lifecycle::LifecycleBranchCatalog,
    table_object: &'a TableObjectService<'static>,
    table_reader: &'a TableObjectReaderService<'static>,
    table_manifest: &'a TableManifestService<'b>,
    table_catalog: &'a mut crate::lifecycle::LifecycleDurableTableCatalog,
    budget: &'a crate::lifecycle::StorageBudgetLedger,
    data_block_bytes: Option<u32>,
    table_compression: crate::format::TableCompression,
}

impl MaintenanceTaskRunner for DurableFlushMaintenanceRunner<'_, '_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let mut outcomes = Vec::new();
        let mut deferred_rotation: Option<LifecycleError> = None;
        for descriptor in flush_branch_descriptors(self.branch_catalog, task)? {
            let branch_id = descriptor.branch_id();
            let request = flush_drain_request_for_branch_from_maintenance_task(task, branch_id)?;
            let branch = self.branch_catalog.branch_state_mut(
                branch_id,
                CommitBranchGenerationGuard::exact(descriptor.generation()),
            )?;
            // Failing the whole task on a rotate refusal livelocked under
            // saturation (the background step path carries the same fix):
            // the frozen backlog this flush drains is what frees the pool.
            // An empty active table decides Rotate and rotation self-skips.
            match crate::lifecycle::decide_flush_rotation(self.budget, branch) {
                crate::lifecycle::FlushRotationDecision::Rotate => {
                    branch.rotate_active();
                }
                crate::lifecycle::FlushRotationDecision::FlushBacklogWithoutRotation => {}
                crate::lifecycle::FlushRotationDecision::Defer(error) => {
                    // Nothing frozen on this branch: skip it (another
                    // branch's backlog may still flush) and report the
                    // deferral only if no branch makes progress.
                    if deferred_rotation.is_none() {
                        deferred_rotation = Some(error);
                    }
                    continue;
                }
            }
            outcomes.push(flush_branch_drain_with(
                branch,
                &request,
                |branch, request| {
                    let outcome = flush_durable_branch_with_budget(
                        branch,
                        self.table_object,
                        self.table_reader,
                        request,
                        Some(self.budget),
                        self.data_block_bytes,
                        self.table_compression,
                    )?;
                    let maintenance_outcome = outcome.maintenance_outcome();
                    if let Some(error) = publish_table_manifest_after_flush(
                        branch,
                        self.table_manifest,
                        self.table_catalog,
                        Some(self.budget),
                        &outcome,
                    ) {
                        return Ok(table_manifest_debt_outcome(maintenance_outcome, error));
                    }
                    Ok(maintenance_outcome)
                },
            )?);
        }
        if outcomes.is_empty() {
            if let Some(error) = deferred_rotation {
                return Ok(MaintenanceOutcome::new(
                    task.kind(),
                    MaintenanceOutcomeStatus::Deferred,
                )
                .with_source_error(error));
            }
        }
        Ok(flush_drain_maintenance_outcome_for_scope(&outcomes))
    }
}

pub(super) fn publish_table_manifest_after_flush(
    branch: &BranchLocalState,
    service: &TableManifestService<'_>,
    catalog: &mut crate::lifecycle::LifecycleDurableTableCatalog,
    budget: Option<&crate::lifecycle::StorageBudgetLedger>,
    outcome: &FlushFrozenOutcome,
) -> Option<LifecycleError> {
    if !outcome.completed() {
        return None;
    }
    // A1 (#2524): one catalog record per flushed table.
    let mut recorded = false;
    for table in outcome.tables() {
        let Some(object_facts) = table.object_facts() else {
            continue;
        };
        if let Err(error) =
            catalog.record_table(table.table_identity().clone(), object_facts.clone())
        {
            return Some(error);
        }
        recorded = true;
    }
    if !recorded {
        return None;
    }
    publish_table_manifest_for_branch_with_budget(branch, service, catalog, budget)
        .map_or_else(Some, |_| None)
}

fn flush_branch_descriptors(
    branch_catalog: &crate::lifecycle::LifecycleBranchCatalog,
    task: &MaintenanceTask,
) -> LifecycleResult<Vec<crate::lifecycle::LifecycleBranchDescriptor>> {
    flush_branch_descriptors_for_scope(branch_catalog, task.scope())
}

fn flush_branch_descriptors_for_scope(
    branch_catalog: &crate::lifecycle::LifecycleBranchCatalog,
    scope: MaintenanceTaskScope,
) -> LifecycleResult<Vec<crate::lifecycle::LifecycleBranchDescriptor>> {
    match scope {
        MaintenanceTaskScope::Branch(branch_id) => Ok(vec![branch_catalog.lookup(branch_id)?]),
        MaintenanceTaskScope::Global => Ok(branch_catalog.list_branches(false)),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "flush task must target a branch or global scope",
        }),
    }
}

struct DurableCheckpointMaintenanceRunner<'a, 'b> {
    branch_catalog: &'a crate::lifecycle::LifecycleBranchCatalog,
    initial_branch_id: strata_core::BranchId,
    services: &'a crate::lifecycle::LifecycleDurableLocalServices<'b>,
    guard_set: &'a CommitBranchGuardSet,
    visible: &'a VisibleVersionTracker,
    created_at: Timestamp,
    next_snapshot_id: &'a mut u64,
    budget: &'a crate::lifecycle::StorageBudgetLedger,
    manifest_debt: bool,
    table_catalog: &'a crate::lifecycle::LifecycleDurableTableCatalog,
}

impl MaintenanceTaskRunner for DurableCheckpointMaintenanceRunner<'_, '_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        // A table-manifest publish is outstanding: the in-memory branch state owns L0 rows the
        // durable manifest does not yet cover. A checkpoint advances the WAL-replay floor, so
        // running it now could strand those rows below a snapshot a clean reopen cannot recover.
        // Defer until a successful manifest publish clears the debt.
        if self.manifest_debt {
            return Ok(MaintenanceOutcome::new(
                crate::lifecycle::MaintenanceTaskKind::Checkpoint,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason("checkpoint deferred: outstanding table-manifest publish debt"));
        }
        // The maintenance task may not be branch-scoped (checkpoint is
        // a global operation in this catalog-aware path); fall back to
        // the seeded branch_id for request identification when the task
        // scope does not carry one.
        let request = checkpoint_request_from_maintenance_task_with_snapshot_id(
            task,
            self.initial_branch_id,
            self.services.manifest(),
            self.created_at,
            Some(*self.next_snapshot_id),
        )?;
        let table_catalog = self.table_catalog;
        let table_is_durable = |identity: &crate::table::TableIdentity| {
            table_catalog.object_for_identity(identity).is_some()
        };
        let outcome = checkpoint_durable_runtime_with_budget(
            self.branch_catalog,
            self.services,
            self.guard_set,
            || self.visible.visible_version(),
            &request,
            self.initial_branch_id,
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
}

pub(super) const fn checkpoint_created_at(
    last_commit_timestamp: Option<Timestamp>,
    recovered_checkpoint_timestamp: Option<Timestamp>,
) -> Timestamp {
    match last_commit_timestamp {
        Some(timestamp) => timestamp,
        None => match recovered_checkpoint_timestamp {
            Some(timestamp) => timestamp,
            None => Timestamp::from_micros(1),
        },
    }
}

struct DurableWalTruncationMaintenanceRunner<'a, 'b> {
    manifest: &'a crate::service::DatabaseManifestService<'b>,
    wal: &'a mut crate::service::WalService<'b>,
}

struct DurableFlushWatermarkMaintenanceRunner<'a, 'b> {
    branch: &'a BranchLocalState,
    registry: &'a CommitBranchRegistry,
    manifest: &'a crate::service::DatabaseManifestService<'b>,
    table_manifest: &'a TableManifestService<'b>,
    visible_version: strata_core::CommitVersion,
    health: &'a RecoveryHealth,
}

impl MaintenanceTaskRunner for DurableWalTruncationMaintenanceRunner<'_, '_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let Some(request) = wal_truncation_request_from_maintenance_task(task, self.manifest)?
        else {
            return Ok(MaintenanceOutcome::new(
                crate::lifecycle::MaintenanceTaskKind::WalTruncation,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason("WAL truncation has no retention proof")
            .with_deferral_reason(MaintenanceDeferralReason::IncompleteProof));
        };
        // Reclaim rotation (#3494): a fully-covered active segment is sealed
        // first, so the delete pass below — which protects the active id —
        // can release it in the same run.
        self.wal
            .rotate_active_segment_for_reclaim(request.covered_through())
            .map_err(wal_error)?;
        Ok(truncate_wal(self.wal, request)?.maintenance_outcome())
    }
}

impl MaintenanceTaskRunner for DurableFlushWatermarkMaintenanceRunner<'_, '_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        if task.kind() != MaintenanceTaskKind::FlushWatermark {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "maintenance task is not a flush watermark task",
            });
        }
        let Some(candidate) = task.flush_watermark_candidate() else {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "flush watermark task requires a candidate",
            });
        };
        let branch_id = self.branch.branch_id();
        let Some(table_manifest) = self
            .table_manifest
            .load_current(branch_id)
            .map_err(manifest_error)?
        else {
            return Ok(MaintenanceOutcome::new(
                MaintenanceTaskKind::FlushWatermark,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason("table manifest coverage is missing"));
        };
        let current_manifest = self.manifest.load_required().map_err(manifest_error)?;
        let floor = wal_retention_watermark(
            current_manifest
                .snapshot_watermark()
                .map(CommitVersion::new),
            current_manifest.flushed_through_commit_id(),
        )
        .unwrap_or(CommitVersion::ZERO);
        let proof = match LifecycleTableManifestFlushCoverageProof::from_branch_manifest_with_floor(
            candidate,
            self.branch,
            &table_manifest,
            self.health,
            floor,
        ) {
            Ok(proof) => proof,
            Err(error @ LifecycleError::WalRetentionProofIncomplete { .. }) => {
                return Ok(MaintenanceOutcome::new(
                    MaintenanceTaskKind::FlushWatermark,
                    MaintenanceOutcomeStatus::Deferred,
                )
                .with_reason("table manifest coverage is incomplete")
                .with_source_error(error));
            }
            Err(error) => return Err(error),
        };
        if self.registry.active_branch_ids() != vec![branch_id] {
            return Err(LifecycleError::WalRetentionProofIncomplete {
                reason: "table manifest flush proof requires all active branches to be loaded",
            });
        }
        Ok(persist_flush_watermark_with_table_manifest_proof(
            self.manifest,
            self.visible_version,
            candidate,
            &LifecycleFlushWatermarkProof::TableManifestCovered(proof.clone()),
            proof.manifest_epoch(),
            proof.recovery_health_epoch(),
            &[(branch_id, table_manifest.manifest_sequence())],
        )?
        .maintenance_outcome())
    }
}

struct DurableCompactionMaintenanceRunner<'a, 'b> {
    branch: &'a mut BranchLocalState,
    table_object: &'a TableObjectService<'static>,
    table_reader: &'a TableObjectReaderService<'static>,
    table_manifest: &'a TableManifestService<'b>,
    table_catalog: &'a mut crate::lifecycle::LifecycleDurableTableCatalog,
    budget: &'a crate::lifecycle::StorageBudgetLedger,
    compaction_io_policy: LifecycleCompactionIoPolicy,
    data_block_bytes: Option<u32>,
    table_compression: crate::format::TableCompression,
    /// #3502 Slice D: the opt-in version-pruning policy + proof to drive this
    /// compaction, pre-built from live state before the branch `&mut` borrow.
    /// `None` = `KeepAll` (retain every version).
    version_pruning: Option<(
        crate::branch::state::compaction::BranchCompactionRetentionPolicy,
        crate::branch::pruning::BranchCompactionPruningProof,
    )>,
    sweep_staged: &'a super::inflight::InFlightTableOutputs,
}

impl MaintenanceTaskRunner for DurableCompactionMaintenanceRunner<'_, '_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let Some(request) = current_compaction_request_from_maintenance_task_with_budget(
            task,
            self.branch,
            Some(self.budget.budget()),
        )?
        else {
            return Ok(stale_compaction_maintenance_outcome());
        };
        // B2: stamp the per-database data-block byte target at dispatch.
        let mut request = request
            .with_data_block_bytes(self.data_block_bytes)
            .with_table_compression(self.table_compression);
        // #3502 Slice D: drive opt-in version pruning under the pre-built proof.
        if let Some((policy, proof)) = self.version_pruning.take() {
            request = request
                .with_retention_policy(policy)
                .with_pruning_proof(proof);
        }
        if let Some(outcome) =
            defer_compaction_for_resource_policy(self.branch, &request, self.compaction_io_policy)?
        {
            return Ok(outcome);
        }
        let compaction = compact_durable_branch_manifest_backed(
            self.branch,
            self.table_object,
            self.table_reader,
            self.table_manifest,
            self.table_catalog,
            &request,
            Some(self.budget),
            self.sweep_staged,
        )?;
        record_lifecycle_compaction_outcome(&compaction);
        Ok(compaction.maintenance_outcome())
    }
}

struct DurableMaterializationMaintenanceRunner<'a, 'b> {
    branch: &'a mut BranchLocalState,
    table_object: &'a TableObjectService<'static>,
    table_reader: &'a TableObjectReaderService<'static>,
    table_manifest: &'a TableManifestService<'b>,
    table_catalog: &'a mut crate::lifecycle::LifecycleDurableTableCatalog,
    budget: &'a crate::lifecycle::StorageBudgetLedger,
    sweep_staged: &'a super::inflight::InFlightTableOutputs,
}

impl MaintenanceTaskRunner for DurableMaterializationMaintenanceRunner<'_, '_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let request = materialization_request_from_maintenance_task(task)?;
        Ok(materialize_durable_branch_manifest_backed(
            self.branch,
            self.table_object,
            self.table_reader,
            self.table_manifest,
            self.table_catalog,
            &request,
            Some(self.budget),
            self.sweep_staged,
        )?
        .maintenance_outcome())
    }
}

/// Owned inputs for the off-lock sweep staging (BS5.5): everything the
/// per-object quarantine loop needs, captured under the runtime lock.
pub(crate) struct SweepStageInputs {
    task: MaintenanceTask,
    branch_id: strata_core::BranchId,
    database_id: [u8; 16],
    codec_id: LifecycleCodecId,
    staged_at: Timestamp,
    health: RecoveryHealth,
    candidates: Vec<crate::object::ObjectName>,
    remaining_candidates: usize,
    quarantine: QuarantineService<'static>,
    /// #2553: registers the candidate names in the runtime's sweep-staged
    /// registry from stage-prepare (under the lock) until the staged result
    /// folds back (under the lock). Installs consult the registry so an
    /// adopted content-identical output cannot resurrect a name this stage
    /// is deleting off-lock.
    #[allow(dead_code, reason = "held for its Drop; never read")]
    staged_guard: super::inflight::InFlightOutputsGuard,
    // C2: dead tables' blocks are dropped from the cache as their objects are
    // swept — without this, `remove_table` had no production caller and
    // compacted-away blocks held pool capacity forever (starving the no-evict
    // warm/preheat inserts).
    block_cache: Option<Arc<TableBlockCache>>,
}

/// The off-lock sweep result, folded back under the lock by
/// [`finish_quarantine_sweep`].
pub(crate) struct SweepStaged {
    task: MaintenanceTask,
    staged_names: Vec<String>,
    quarantined_objects: usize,
    /// Bytes of the source objects this pass staged into quarantine.
    staged_bytes: u64,
    faults: usize,
    remaining_candidates: usize,
    sweep_health: Option<RecoveryHealth>,
    request_error: Option<LifecycleError>,
    /// #2553: keeps the staged names registered until this result folds back
    /// under the lock (see `SweepStageInputs::staged_guard`).
    #[allow(dead_code, reason = "held for its Drop; never read")]
    staged_guard: super::inflight::InFlightOutputsGuard,
}

/// C2: one preheat chunk reads at most this many source bytes before folding
/// back under the lock, so a drain round stays inside its runtime budget and
/// commit waiters never sit behind a long fill.
const CACHE_PREHEAT_CHUNK_MAX_BYTES: u64 = 128 * 1024 * 1024;
/// C2: a run of this many consecutive full-shard skips means the walk is
/// hashing into saturated shards everywhere (keys spread uniformly), so
/// further passes only burn IO — stop and wait for the next trigger.
const CACHE_PREHEAT_SKIPPED_FULL_STOP: usize = 512;

/// C2: where a bounded preheat pass stopped, so the next chunk resumes
/// instead of re-walking. Identity-addressed: if the table is compacted away
/// between chunks the walk restarts from the beginning of the sequence, and
/// the recency-neutral presence probe makes the re-walk ~free.
#[derive(Debug)]
pub(crate) struct CachePreheatCursor {
    branch_id: BranchId,
    table_identity: TableIdentity,
    next_block: usize,
}

/// Inputs captured under the runtime lock for an off-lock block-cache
/// preheat chunk. Layout snapshots are immutable and the readers inside them
/// carry the shared cache handle, so the walk needs nothing else.
pub(crate) struct PreheatStageInputs {
    task: MaintenanceTask,
    layouts: Vec<(BranchId, Arc<BranchLayout>, Vec<BranchInheritedLayer>)>,
    cursor: Option<CachePreheatCursor>,
    chunk_max_bytes: u64,
    // C3a: for the completed-pass dead-entry purge (`retain_tables`) — the
    // capped quarantine sweep leaves compacted-away tables' blocks pinning
    // the pool at capacity, whose eviction churn was the residual miss floor.
    block_cache: Option<Arc<TableBlockCache>>,
}

/// The off-lock preheat result, folded back under the lock by
/// [`finish_cache_preheat`].
pub(crate) struct PreheatStaged {
    task: MaintenanceTask,
    admitted: u64,
    skipped_present: u64,
    skipped_full: u64,
    bytes_read: u64,
    next_cursor: Option<CachePreheatCursor>,
    stopped_full: bool,
    stage_error: Option<LifecycleError>,
    /// C3a: dead-table cache entries dropped by the completed-pass purge.
    dead_entries_removed: u64,
}

impl PreheatStageInputs {
    /// Walk the captured layouts and warm the block cache — callable with NO
    /// locks held. Admission is verify-then-no-evict (the W2.4 contract), so
    /// a concurrent demand reader can at worst race a duplicate insert, which
    /// the cache folds as `DuplicateExisting`.
    pub(crate) fn stage(mut self) -> PreheatStaged {
        let mut staged = PreheatStaged {
            task: self.task,
            admitted: 0,
            skipped_present: 0,
            skipped_full: 0,
            bytes_read: 0,
            next_cursor: None,
            stopped_full: false,
            stage_error: None,
            dead_entries_removed: 0,
        };
        let cursor = self.cursor.take();
        if cursor.is_none() {
            // Fresh pass: purge dead-table entries BEFORE filling. Left in
            // place they inflate occupancy by gigabytes and the fill's own
            // fair inserts then evict the earliest-walked LIVE blocks once
            // the pool saturates mid-pass (measured: ~100K live blocks lost
            // to exactly that, re-appearing as run-phase first-touch
            // misses). The completion purge below still handles tables
            // compacted away DURING the pass.
            staged.dead_entries_removed = self.purge_dead_table_entries();
        }
        // Until the cursor's table is found, tables are skipped without IO;
        // a vanished cursor table restarts coverage (cheap: presence probes).
        let mut resuming = cursor.is_some();
        let mut full_streak = 0usize;
        for (branch_id, layout, inherited) in &self.layouts {
            if resuming && cursor.as_ref().map(|c| c.branch_id) != Some(*branch_id) {
                continue;
            }
            // Deepest level first: L0/L1 blocks were publish-warmed recently;
            // the deep levels are the cold mass a fill must cover.
            let owned = layout.levels().iter().rev();
            let inherited_levels = inherited
                .iter()
                .flat_map(|layer| layer.owned_levels().iter().rev());
            for level in owned.chain(inherited_levels) {
                for table in level {
                    let mut start_block = 0usize;
                    if resuming {
                        match cursor.as_ref() {
                            Some(c) if &c.table_identity == table.facts().identity() => {
                                start_block = c.next_block;
                                resuming = false;
                            }
                            _ => continue,
                        }
                    }
                    let remaining = self.chunk_max_bytes.saturating_sub(staged.bytes_read);
                    if remaining == 0 {
                        staged.next_cursor = Some(CachePreheatCursor {
                            branch_id: *branch_id,
                            table_identity: table.facts().identity().clone(),
                            next_block: start_block,
                        });
                        return staged;
                    }
                    let report = match table
                        .reader()
                        .warm_data_blocks_from_source(start_block, remaining)
                    {
                        Ok(report) => report,
                        Err(error) => {
                            staged.stage_error = Some(LifecycleError::lower_layer_with(
                                LifecycleLowerLayer::TableRuntime,
                                "table runtime failed",
                                error,
                            ));
                            return staged;
                        }
                    };
                    staged.admitted = staged.admitted.saturating_add(report.admitted as u64);
                    staged.skipped_present = staged
                        .skipped_present
                        .saturating_add(report.skipped_present as u64);
                    staged.skipped_full = staged
                        .skipped_full
                        .saturating_add(report.skipped_full as u64);
                    staged.bytes_read = staged.bytes_read.saturating_add(report.bytes_read);
                    // The streak carries across tables only when a table ended
                    // in full-skips AND produced nothing else after them.
                    if report.trailing_skipped_full > 0 {
                        full_streak = full_streak.saturating_add(report.trailing_skipped_full);
                    } else if report.admitted > 0 || report.skipped_present > 0 {
                        full_streak = 0;
                    }
                    if full_streak >= CACHE_PREHEAT_SKIPPED_FULL_STOP {
                        staged.stopped_full = true;
                        staged.next_cursor = Some(CachePreheatCursor {
                            branch_id: *branch_id,
                            table_identity: table.facts().identity().clone(),
                            next_block: report.next_block.unwrap_or(usize::MAX),
                        });
                        return staged;
                    }
                    if let Some(next_block) = report.next_block {
                        staged.next_cursor = Some(CachePreheatCursor {
                            branch_id: *branch_id,
                            table_identity: table.facts().identity().clone(),
                            next_block,
                        });
                        return staged;
                    }
                }
            }
        }
        staged.dead_entries_removed = staged
            .dead_entries_removed
            .saturating_add(self.purge_dead_table_entries());
        staged
    }

    /// C3a: the pass covered the full walked set — purge every cache entry
    /// whose table is OUTSIDE it. LRU alone cannot reclaim this garbage fast
    /// enough (dead blocks only leave via the capped sweep or eviction
    /// pressure, and that pressure evicts cold-but-LIVE victims too — the
    /// measured miss floor). A table published DURING the walk is absent
    /// from the snapshot and loses its warm blocks here; the trigger it
    /// fired survives to the follow-up pass, which re-admits them.
    fn purge_dead_table_entries(&self) -> u64 {
        let Some(cache) = &self.block_cache else {
            return 0;
        };
        let mut live = std::collections::BTreeSet::new();
        for (_, layout, inherited) in &self.layouts {
            let tables = layout.levels().iter().flatten().chain(
                inherited
                    .iter()
                    .flat_map(|layer| layer.owned_levels().iter().flatten()),
            );
            for table in tables {
                if let Ok(id) = TableCacheTableId::new(table.facts().identity().as_str().as_bytes())
                {
                    live.insert(id);
                }
            }
        }
        cache.retain_tables(&live) as u64
    }
}

impl SweepStageInputs {
    /// Stage every candidate into quarantine — callable with NO locks held.
    /// Each staging is idempotent (`AlreadyQuarantined` / source-missing fold
    /// as progress), so a crash or repeated pass never double-counts.
    pub(crate) fn stage(self) -> SweepStaged {
        let mut staged_names = Vec::new();
        let mut quarantined_objects = 0usize;
        let mut staged_bytes = 0u64;
        let mut faults = 0usize;
        let mut sweep_health = None;
        let mut request_error = None;
        for object in &self.candidates {
            // C2: drop the dead table's blocks from the cache. Every candidate
            // was proven unreachable by the under-lock mark and unreachability
            // is monotone, so removal is safe even if the staging below
            // faults. Identity == the table component of the object name ==
            // the bytes the cache id was built from at reader open.
            if let Some(cache) = &self.block_cache {
                if let Ok(Some(crate::layout::TableObjectClassification::Data {
                    table_id, ..
                })) = crate::layout::ObjectLayout::classify_table_object(object)
                {
                    if let Ok(id) = TableCacheTableId::new(table_id.as_bytes()) {
                        cache.remove_table(&id);
                    }
                }
            }
            let proof = crate::lifecycle::LifecycleQuarantineProof::from_retention_decision(
                crate::lifecycle::RetentionDecision::QuarantineCandidate,
                self.health.clone(),
            );
            let quarantine_request = match LifecycleQuarantineRequest::from_source_object(
                self.branch_id,
                self.database_id,
                self.codec_id.clone(),
                object.clone(),
                self.staged_at,
                proof,
            ) {
                Ok(request) => request,
                Err(error) => {
                    request_error = Some(error);
                    break;
                }
            };
            let quarantine_outcome =
                quarantine_lifecycle_object(&self.quarantine, &quarantine_request);
            if let Some(health) = quarantine_outcome.recovery_health() {
                sweep_health = Some(health.clone());
            }
            match quarantine_outcome.status() {
                crate::lifecycle::LifecycleQuarantineStatus::QuarantinedSourceDeleted
                | crate::lifecycle::LifecycleQuarantineStatus::SourceDeleteRetried => {
                    crate::observability::perf_trace::record_table_object_quarantined(
                        quarantine_outcome.byte_count(),
                    );
                    quarantined_objects += 1;
                    staged_bytes = staged_bytes.saturating_add(quarantine_outcome.byte_count());
                    staged_names.push(object.to_string());
                }
                // Idempotent replays after a partial earlier pass: the object is already staged
                // (or its source already gone) — the purge will reclaim it.
                crate::lifecycle::LifecycleQuarantineStatus::AlreadyQuarantined
                | crate::lifecycle::LifecycleQuarantineStatus::SourceAlreadyMissingAfterPublish => {
                    quarantined_objects += 1;
                }
                // Every other status (publish failures, health blocks, service rejections) leaves
                // the object on disk for a later pass; the recorded health debt surfaces it.
                _ => {
                    faults += 1;
                }
            }
        }
        SweepStaged {
            task: self.task,
            staged_names,
            quarantined_objects,
            staged_bytes,
            faults,
            remaining_candidates: self.remaining_candidates,
            sweep_health,
            request_error,
            staged_guard: self.staged_guard,
        }
    }
}

/// Owned inputs for the off-lock quarantine purge (BS5.5).
pub(crate) struct PurgeStageInputs {
    task: MaintenanceTask,
    default_branch_id: strata_core::BranchId,
    database_id: [u8; 16],
    codec_id: LifecycleCodecId,
    health: RecoveryHealth,
    quarantine: QuarantineService<'static>,
}

/// The off-lock purge result, folded back under the lock by
/// [`finish_quarantine_purge`].
pub(crate) struct PurgeStaged {
    task: MaintenanceTask,
    result: LifecycleResult<MaintenanceOutcome>,
}

impl PurgeStageInputs {
    /// Load the inventory, derive the proof, and delete the quarantined
    /// objects — callable with NO locks held (quarantine namespace only).
    pub(crate) fn purge(self) -> PurgeStaged {
        let result = (|| {
            let branch_id = purge_branch_id_from_task(&self.task, self.default_branch_id)?;
            let inventory = self
                .quarantine
                .load_inventory(branch_id, self.database_id, self.codec_id.as_str())
                .map_err(durable_quarantine_service_error)?;
            let (branch_id, proof) = purge_proof_from_maintenance_task(
                &self.task,
                self.health.clone(),
                self.default_branch_id,
                inventory.token(),
            )?;
            let purge_outcome = purge_lifecycle_quarantine(
                &self.quarantine,
                branch_id,
                self.database_id,
                &self.codec_id,
                &proof,
            )?;
            crate::observability::perf_trace::record_quarantine_purge(
                purge_outcome.reclaimed_bytes(),
            );
            Ok(purge_outcome.maintenance_outcome())
        })();
        PurgeStaged {
            task: self.task,
            result,
        }
    }
}

struct DurableRetentionMaintenanceRunner<'a, 'b> {
    services: &'a crate::lifecycle::LifecycleDurableLocalServices<'b>,
    branch_id: strata_core::BranchId,
    health: crate::lifecycle::RecoveryHealth,
    pending_releases: &'a mut Vec<crate::branch::facts::BranchReleasePlan>,
    pending_releases_sequence: &'a mut u64,
    /// In-memory-reachable table objects plus in-flight build outputs
    /// (#2524), pinned live in the mark so the report matches what the sweep
    /// will actually reclaim.
    pinned_objects: Vec<crate::object::ObjectName>,
}

impl DurableRetentionMaintenanceRunner<'_, '_> {
    /// Drain pending release plans matching this retention pass's scope.
    /// `Global` scope drains all; `TableObjects { branch_id }` drains plans
    /// targeting that branch. Returns the drained release plans for
    /// downstream tagging; durable physical reclaim is manifest-driven and
    /// runs through the table-object retention handler.
    fn drain_pending_releases(
        &mut self,
        scope: LifecycleRetentionScope,
    ) -> Vec<crate::branch::facts::BranchReleasePlan> {
        let drain_all = matches!(scope, LifecycleRetentionScope::Global);
        let scoped_branch = match scope {
            LifecycleRetentionScope::TableObjects { branch_id } => Some(branch_id),
            _ => None,
        };
        if !drain_all && scoped_branch.is_none() {
            return Vec::new();
        }
        let mut remaining = Vec::with_capacity(self.pending_releases.len());
        let mut drained = Vec::new();
        for plan in self.pending_releases.drain(..) {
            let matches_scope = drain_all
                || scoped_branch.is_some_and(|target| plan.released_branch_id() == target);
            if matches_scope {
                drained.push(plan);
            } else {
                remaining.push(plan);
            }
        }
        *self.pending_releases = remaining;
        drained
    }

    /// Publish a fresh `PendingReleasesManifest` reflecting the
    /// post-drain buffer. Called after each drain that consumed
    /// entries so the audit trail stays consistent across restarts.
    fn publish_pending_releases(&mut self) -> LifecycleResult<()> {
        let entries = pending_releases_to_durable_entries(self.pending_releases)
            .map_err(pending_releases_format_error)?;
        *self.pending_releases_sequence = self.pending_releases_sequence.saturating_add(1);
        if *self.pending_releases_sequence == 0 {
            return Err(LifecycleError::CheckpointPublicationFailed {
                reason: "pending releases sequence overflow",
            });
        }
        let manifest = crate::format::PendingReleasesManifest::new(
            *self.services.assembly_facts().database_id(),
            *self.pending_releases_sequence,
            entries,
        )
        .map_err(pending_releases_format_error)?;
        self.services
            .pending_releases_manifest()
            .publish_replace(&manifest)
            .map_err(|error| {
                LifecycleError::lower_layer_with(
                    crate::lifecycle::LifecycleLowerLayer::Service,
                    "pending releases manifest service failed",
                    error,
                )
            })?;
        Ok(())
    }
}

fn pending_releases_to_durable_entries(
    plans: &[crate::branch::facts::BranchReleasePlan],
) -> Result<Vec<crate::format::PendingReleasesEntry>, crate::format::FormatError> {
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

fn pending_releases_format_error(error: crate::format::FormatError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        crate::lifecycle::LifecycleLowerLayer::Format,
        "pending releases manifest encode failed",
        error,
    )
}

impl MaintenanceTaskRunner for DurableRetentionMaintenanceRunner<'_, '_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let request = retention_request_from_maintenance_task(task)?;
        let drained = self.drain_pending_releases(request.scope());
        // Persist the post-drain buffer when entries were actually
        // removed so the audit trail stays in sync with the in-memory
        // state across restarts.
        if !drained.is_empty() {
            self.publish_pending_releases()?;
        }
        if let LifecycleRetentionScope::TableObjects { branch_id } = request.scope() {
            let table_retention =
                table_object_retention_request(self.services, branch_id, &self.health)
                    .map(|request| request.with_pinned_objects(self.pinned_objects.clone()))
                    .and_then(|request| table_object_retention_outcome(&request))?;
            return Ok(append_released_table_names(
                table_retention.retention().maintenance_outcome(),
                &drained,
            ));
        }
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
                _ => Ok(append_released_table_names(
                    retention_outcome_for_scope(&request, proof, &[])?.maintenance_outcome(),
                    &drained,
                )),
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
                let table_retention =
                    table_object_retention_request(self.services, self.branch_id, &self.health)
                        .map(|request| request.with_pinned_objects(self.pinned_objects.clone()))
                        .and_then(|request| table_object_retention_outcome(&request))?;
                Ok(append_released_table_names(
                    global_retention_maintenance_outcome(
                        &snapshot_outcome,
                        &retention_outcome,
                        table_retention.retention(),
                    ),
                    &drained,
                ))
            }
            // `retention_request_from_maintenance_task` only emits
            // `SnapshotObjects` or `Global` scopes — every other scope is
            // rejected at request construction with a typed
            // `InvalidRequest`. The remaining match arms are therefore
            // unreachable through the runner, and we keep them as such
            // rather than silently routing to WAL/Quarantine delegations
            // that would not match a `TableObjects`/`WalObjects`/
            // `QuarantineObjects` request.
            _ => unreachable!(
                "retention runner reached an unsupported scope: {:?}; \
                 retention_request_from_maintenance_task should reject this kind",
                request.scope(),
            ),
        }
    }
}

struct DurablePurgeMaintenanceRunner<'a, 'b> {
    quarantine: &'a QuarantineService<'b>,
    database_id: [u8; 16],
    codec_id: LifecycleCodecId,
    health: RecoveryHealth,
    default_branch_id: strata_core::BranchId,
}

impl MaintenanceTaskRunner for DurablePurgeMaintenanceRunner<'_, '_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let branch_id = purge_branch_id_from_task(task, self.default_branch_id)?;
        let inventory = self
            .quarantine
            .load_inventory(branch_id, self.database_id, self.codec_id.as_str())
            .map_err(durable_quarantine_service_error)?;
        let (branch_id, proof) = purge_proof_from_maintenance_task(
            task,
            self.health.clone(),
            self.default_branch_id,
            inventory.token(),
        )?;
        Ok(purge_lifecycle_quarantine(
            self.quarantine,
            branch_id,
            self.database_id,
            &self.codec_id,
            &proof,
        )?
        .maintenance_outcome())
    }
}

pub(super) fn purge_branch_id_from_task(
    task: &MaintenanceTask,
    default_branch_id: strata_core::BranchId,
) -> LifecycleResult<strata_core::BranchId> {
    if task.kind() != MaintenanceTaskKind::Purge {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "purge request requires purge task",
        });
    }
    match task.scope() {
        crate::lifecycle::MaintenanceTaskScope::Branch(branch_id) => Ok(branch_id),
        crate::lifecycle::MaintenanceTaskScope::Quarantine => Ok(default_branch_id),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "purge task scope is invalid",
        }),
    }
}

pub(super) fn durable_quarantine_service_error(
    error: crate::service::QuarantineServiceError,
) -> LifecycleError {
    LifecycleError::lower_layer_with(
        crate::lifecycle::LifecycleLowerLayer::Service,
        "quarantine service failed",
        error,
    )
}

/// Per-pass cap on quarantined objects. Quarantine staging copies the object's bytes into
/// `quarantine/` before deleting the source, so this bounds the copy I/O held under the runtime
/// lock; a larger backlog converges over successive retention → quarantine → purge cycles (each
/// cycle's purge reclaims the previous cycle's staged bytes, bounding transient disk growth).
const TABLE_OBJECT_SWEEP_MAX_OBJECTS: usize = 32;

/// The table-object sweep (the reclaim half of GC). Recomputes the reachability mark fresh under
/// the same runtime-lock hold it acts in — there is no decision-to-action gap for a fork, publish,
/// or manifest advance to invalidate — then stages each unreachable object into quarantine (the
/// existing two-phase copy-then-delete-source safety net; physical bytes are reclaimed by the
/// follow-up Purge task). Never deletes an object that any branch can reach: the mark unions
/// durable-manifest reachability with in-memory pins (COW crash windows) AND the in-flight
/// build-output registry (#2524 — outputs reserve their names before their bytes land, so the
/// sweep runs DURING builds instead of deferring wholesale). The one remaining wholesale defer
/// is a held retired read view (durable readers are name-addressed with no held fd, so a source
/// delete would break their block fetches).
struct DurableTableObjectSweepRunner<'a, 'b> {
    services: &'a crate::lifecycle::LifecycleDurableLocalServices<'b>,
    branch_id: strata_core::BranchId,
    health: RecoveryHealth,
    database_id: [u8; 16],
    codec_id: LifecycleCodecId,
    staged_at: Timestamp,
    retired_readers_alive: bool,
    pinned_objects: Vec<crate::object::ObjectName>,
    /// Out: objects staged into quarantine this pass (drives the follow-up Purge enqueue).
    quarantined_objects: usize,
    /// Out: bytes of the source objects staged this pass (the reclaim ledger's bytes).
    staged_bytes: u64,
    /// Out: candidates left unprocessed (cap) or deferred (interlocks) — drives re-enqueue.
    remaining_candidates: usize,
    /// Out: worst recovery health reported by the quarantine service this pass.
    sweep_health: Option<RecoveryHealth>,
}

impl MaintenanceTaskRunner for DurableTableObjectSweepRunner<'_, '_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        if task.kind() != MaintenanceTaskKind::Quarantine {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "quarantine runner requires quarantine task",
            });
        }
        // Fresh mark under this lock hold (metadata-only: inventory listing + manifest decode,
        // never table data).
        let request = table_object_retention_request(self.services, self.branch_id, &self.health)?
            .with_pinned_objects(self.pinned_objects.clone());
        let outcome = table_object_retention_outcome(&request)?;
        let candidates: Vec<crate::object::ObjectName> = outcome
            .decisions()
            .iter()
            .filter(|decision| {
                decision.decision() == crate::lifecycle::RetentionDecision::QuarantineCandidate
            })
            .filter_map(|decision| decision.object().cloned())
            .collect();
        if candidates.is_empty() {
            return Ok(MaintenanceOutcome::new(MaintenanceTaskKind::Quarantine, {
                MaintenanceOutcomeStatus::Completed
            })
            .with_reason("no unreachable table objects")
            .with_stats(LifecycleStats::new(0, 0, 1, 0, 0)));
        }
        if self.retired_readers_alive {
            // Interlock deferral does NOT self-requeue (that would spin the retention →
            // quarantine chain against a long-held reader); the next rewrite publish, reopen,
            // or explicit Reclaim re-triggers the cycle. In-flight build outputs are pinned
            // by name (#2524), so builds no longer defer the sweep.
            crate::observability::perf_trace::record_table_object_sweep_deferred(false);
            return Ok(MaintenanceOutcome::new(
                MaintenanceTaskKind::Quarantine,
                MaintenanceOutcomeStatus::Deferred,
            )
            .with_reason("table-object sweep deferred: retired read view still held")
            .with_deferral_reason(MaintenanceDeferralReason::ReaderPinned)
            .with_stats(LifecycleStats::new(0, 0, 1, 1, 0)));
        }

        let mut staged_names = Vec::new();
        let mut faults = 0usize;
        for object in candidates.iter().take(TABLE_OBJECT_SWEEP_MAX_OBJECTS) {
            let proof = crate::lifecycle::LifecycleQuarantineProof::from_retention_decision(
                crate::lifecycle::RetentionDecision::QuarantineCandidate,
                self.health.clone(),
            );
            let quarantine_request = LifecycleQuarantineRequest::from_source_object(
                self.branch_id,
                self.database_id,
                self.codec_id.clone(),
                object.clone(),
                self.staged_at,
                proof,
            )?;
            let quarantine_outcome =
                quarantine_lifecycle_object(self.services.quarantine(), &quarantine_request);
            if let Some(health) = quarantine_outcome.recovery_health() {
                self.sweep_health = Some(health.clone());
            }
            match quarantine_outcome.status() {
                crate::lifecycle::LifecycleQuarantineStatus::QuarantinedSourceDeleted
                | crate::lifecycle::LifecycleQuarantineStatus::SourceDeleteRetried => {
                    crate::observability::perf_trace::record_table_object_quarantined(
                        quarantine_outcome.byte_count(),
                    );
                    self.quarantined_objects += 1;
                    self.staged_bytes = self
                        .staged_bytes
                        .saturating_add(quarantine_outcome.byte_count());
                    staged_names.push(object.to_string());
                }
                // Idempotent replays after a partial earlier pass: the object is already staged
                // (or its source already gone) — the purge will reclaim it.
                crate::lifecycle::LifecycleQuarantineStatus::AlreadyQuarantined
                | crate::lifecycle::LifecycleQuarantineStatus::SourceAlreadyMissingAfterPublish => {
                    self.quarantined_objects += 1;
                }
                // Every other status (publish failures, health blocks, service rejections) leaves
                // the object on disk for a later pass; the recorded health debt surfaces it.
                _ => {
                    faults += 1;
                }
            }
        }
        self.remaining_candidates = candidates
            .len()
            .saturating_sub(TABLE_OBJECT_SWEEP_MAX_OBJECTS.min(candidates.len()));

        crate::observability::perf_trace::record_table_object_sweep_run();
        let staged_count = staged_names.len();
        let mut outcome = MaintenanceOutcome::new(
            MaintenanceTaskKind::Quarantine,
            MaintenanceOutcomeStatus::Completed,
        )
        .with_affected_object_names(staged_names)
        .with_effects(staged_count, self.staged_bytes, false)
        .with_state_changes(self.quarantined_objects)
        .with_stats(LifecycleStats::new(0, faults, 1, 0, 0));
        if let Some(health) = self.sweep_health.clone() {
            outcome = outcome.with_recovery_health(health);
        }
        Ok(outcome)
    }
}

/// Table objects reachable from any branch's IN-MEMORY state (owned levels + inherited layers,
/// every live catalog branch), mapped to inventory object names via the durable table catalog.
/// The sweep pins these as live even when no durable manifest references them — the COW crash
/// windows (a fork child whose fork-time manifest publish failed and records only health debt)
/// must never lose shared parent objects to reclaim.
fn in_memory_pinned_table_objects(
    branch_catalog: &crate::lifecycle::LifecycleBranchCatalog,
    table_catalog: &crate::lifecycle::LifecycleDurableTableCatalog,
) -> Vec<crate::object::ObjectName> {
    let mut pinned = std::collections::BTreeSet::new();
    for descriptor in branch_catalog.list_branches(false) {
        let Ok(state) = branch_catalog.branch_state(descriptor.branch_id()) else {
            continue;
        };
        let owned = state.owned_levels().iter().flatten();
        let inherited = state
            .inherited_layers()
            .iter()
            .flat_map(|layer| layer.owned_levels().iter().flatten());
        for table in owned.chain(inherited) {
            if let Some(object) = table_catalog.object_for_identity(table.descriptor().identity()) {
                pinned.insert(object.clone());
            }
        }
    }
    pinned.into_iter().collect()
}

struct DurableQuarantineRepairMaintenanceRunner<'a, 'b> {
    quarantine: &'a QuarantineService<'b>,
    database_id: [u8; 16],
    codec_id: LifecycleCodecId,
}

impl MaintenanceTaskRunner for DurableQuarantineRepairMaintenanceRunner<'_, '_> {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome> {
        let branch_id = repair_branch_from_maintenance_task(task)?;
        let outcome = match branch_id {
            Some(branch_id) => repair_branch_lifecycle_quarantine(
                self.quarantine,
                branch_id,
                self.database_id,
                &self.codec_id,
            )?,
            None => repair_lifecycle_quarantine_family(
                self.quarantine,
                self.database_id,
                &self.codec_id,
            )?,
        };
        Ok(outcome.maintenance_outcome())
    }
}

fn retention_proof_from_assembly(
    request: &LifecycleRetentionRequest,
    services: &crate::lifecycle::LifecycleDurableLocalServices<'_>,
    health: &crate::lifecycle::RecoveryHealth,
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
    health: &crate::lifecycle::RecoveryHealth,
) -> bool {
    match health {
        crate::lifecycle::RecoveryHealth::Healthy => false,
        crate::lifecycle::RecoveryHealth::Degraded { class, .. } => match class {
            RecoveryDegradationClass::Telemetry => !request.allow_telemetry_degraded_recovery(),
            RecoveryDegradationClass::PolicyDowngrade => {
                !request.allow_telemetry_degraded_recovery()
                    || !retention_scope_is_telemetry_only(request.scope())
            }
            RecoveryDegradationClass::DataLoss => true,
        },
        crate::lifecycle::RecoveryHealth::Failed { .. } => true,
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

/// Terminal outcome for a branch-scoped task whose target branch was deleted
/// after enqueue. Canceled deliberately: the task is consumed (no queue churn,
/// no re-run against a re-created name's new generation) and Canceled outcomes
/// never enter the maintenance failure ring — a legal race is not a failure.
fn deleted_scope_canceled_outcome(kind: MaintenanceTaskKind) -> MaintenanceOutcome {
    MaintenanceOutcome::new(kind, MaintenanceOutcomeStatus::Canceled)
        .with_reason("maintenance target branch was deleted after enqueue")
}

/// #3526: the publish-time pruning re-validation decision — every safety gate a
/// concurrent op can flip must still hold for the off-lock pruned output to be
/// installed. Pure and truth-tabled; `revalidate_pruning_for_publish` gathers
/// the live inputs.
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "five independent safety gates, each a distinct live-branch condition; a struct would obscure the truth table"
)]
const fn pruning_publish_gates_hold(
    tables_unshared: bool,
    recovery_healthy: bool,
    inherited_layers_empty: bool,
    floor_not_regressed: bool,
    timestamp_coverage_ok: bool,
) -> bool {
    tables_unshared
        && recovery_healthy
        && inherited_layers_empty
        && floor_not_regressed
        && timestamp_coverage_ok
}

/// #3526: the live retained-history floor must not have advanced past the floor
/// the off-lock output was pruned against — a concurrent prune that raised the
/// floor kept LESS history, so installing this (lower-floor) output would
/// resurrect versions it already dropped. `None` live floor = nothing pruned
/// yet, so no regression. Equal floors are allowed.
fn retained_floor_not_regressed(
    live: Option<strata_core::CommitVersion>,
    proof_floor: strata_core::CommitVersion,
) -> bool {
    live.is_none_or(|live| live <= proof_floor)
}

fn global_retention_maintenance_outcome(
    snapshot_outcome: &LifecycleSnapshotPruningOutcome,
    retention_outcome: &LifecycleRetentionOutcome,
    table_retention_outcome: &LifecycleRetentionOutcome,
) -> MaintenanceOutcome {
    let status = if !snapshot_outcome.completed()
        || matches!(
            retention_outcome.status(),
            LifecycleRetentionStatus::DeferredIncompleteProof
                | LifecycleRetentionStatus::DeferredUnsupportedScope
                | LifecycleRetentionStatus::BlockedByRecoveryHealth
        )
        || matches!(
            table_retention_outcome.status(),
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
    names.extend(
        table_retention_outcome
            .decisions()
            .iter()
            .filter_map(|decision| decision.object().map(ToString::to_string)),
    );
    let recovery_health = snapshot_outcome
        .recovery_health()
        .cloned()
        .or_else(|| retention_outcome.recovery_health().cloned())
        .or_else(|| table_retention_outcome.recovery_health().cloned());
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

fn table_object_retention_request(
    services: &crate::lifecycle::LifecycleDurableLocalServices<'_>,
    branch_id: strata_core::BranchId,
    health: &RecoveryHealth,
) -> LifecycleResult<LifecycleTableObjectRetentionRequest> {
    let manifests = services
        .table_manifest()
        .load_all_current()
        .map_err(manifest_error)?;
    let inventory = services
        .table_object()
        .list_inventory()
        .map_err(table_object_service_error)?
        .into_iter()
        .map(|(object, byte_count)| LifecycleTableObjectInventoryEntry::new(object, byte_count))
        .collect::<LifecycleResult<Vec<_>>>()?;
    // Inventory is global (`tables/` prefix); to avoid re-classifying another
    // branch's already-quarantined object as a fresh candidate, load the
    // quarantine inventory for every branch that owns a known table manifest,
    // plus the branch under retention so a branch with no manifest yet but a
    // populated quarantine is still consulted.
    //
    // BranchId is not `Ord`, so dedupe via a byte-keyed BTreeSet of the
    // 16-byte representations rather than the type itself.
    let mut seen_branches: std::collections::BTreeSet<[u8; 16]> = std::collections::BTreeSet::new();
    let mut quarantine_branches: Vec<strata_core::BranchId> = Vec::new();
    let record_branch = |branch: strata_core::BranchId,
                         seen: &mut std::collections::BTreeSet<[u8; 16]>,
                         ordered: &mut Vec<strata_core::BranchId>| {
        if seen.insert(*branch.as_bytes()) {
            ordered.push(branch);
        }
    };
    record_branch(branch_id, &mut seen_branches, &mut quarantine_branches);
    for manifest in &manifests {
        record_branch(
            manifest.branch_id(),
            &mut seen_branches,
            &mut quarantine_branches,
        );
    }
    let database_id = *services.assembly_facts().database_id();
    let codec_id = services.assembly_facts().codec_id();
    let mut quarantined_objects: Vec<crate::object::ObjectName> = Vec::new();
    let mut quarantine_inventory_bytes: u64 = 0;
    for branch in quarantine_branches {
        let load = services
            .quarantine()
            .load_inventory(branch, database_id, codec_id)
            .map_err(durable_quarantine_service_error)?;
        quarantine_inventory_bytes = quarantine_inventory_bytes.saturating_add(load.byte_count());
        quarantined_objects.extend(
            load.inventory()
                .entries()
                .iter()
                .map(|entry| entry.source_object().clone()),
        );
    }
    let epochs =
        table_object_proof_epochs(&manifests, &inventory, quarantine_inventory_bytes, health)?;
    LifecycleTableObjectRetentionRequest::new(
        branch_id,
        health.clone(),
        epochs,
        manifests,
        inventory,
        quarantined_objects,
    )
}

fn table_object_proof_epochs(
    manifests: &[crate::format::TableManifest],
    inventory: &[LifecycleTableObjectInventoryEntry],
    quarantine_inventory_bytes: u64,
    health: &RecoveryHealth,
) -> LifecycleResult<LifecycleTableObjectProofEpochs> {
    let manifest_epoch = manifests
        .iter()
        .map(crate::format::TableManifest::manifest_sequence)
        .max()
        .unwrap_or(1);
    // Backend listings do not expose a monotonic version, so the inventory
    // and quarantine "epochs" are SHA-256 derived content fingerprints
    // truncated to u64. Collisions are statistically negligible, so two
    // distinct inventories never alias the same epoch. The fingerprint on
    // the proof context is the authoritative freshness anchor; this field
    // gives the quarantine maintenance dispatch a cheap pre-check that
    // does not require hashing the full request.
    let table_inventory_epoch = inventory_content_epoch(inventory);
    let quarantine_inventory_epoch = quarantine_content_epoch(quarantine_inventory_bytes);
    let recovery_health_epoch = u64::try_from(health.fault_count())
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    LifecycleTableObjectProofEpochs::new(
        manifest_epoch.max(1),
        table_inventory_epoch.max(1),
        quarantine_inventory_epoch.max(1),
        recovery_health_epoch.max(1),
    )
}

fn inventory_content_epoch(inventory: &[LifecycleTableObjectInventoryEntry]) -> u64 {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"inventory");
    hasher.update((inventory.len() as u64).to_be_bytes());
    let mut sorted: Vec<&LifecycleTableObjectInventoryEntry> = inventory.iter().collect();
    sorted.sort_by(|left, right| left.object().cmp(right.object()));
    for entry in sorted {
        hasher.update((entry.object().as_str().len() as u64).to_be_bytes());
        hasher.update(entry.object().as_str().as_bytes());
        hasher.update(entry.byte_count().to_be_bytes());
    }
    truncate_hash_to_u64(hasher.finalize().as_slice())
}

fn quarantine_content_epoch(byte_count: u64) -> u64 {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"quarantine");
    hasher.update(byte_count.to_be_bytes());
    truncate_hash_to_u64(hasher.finalize().as_slice())
}

fn truncate_hash_to_u64(digest: &[u8]) -> u64 {
    let mut buffer = [0_u8; 8];
    let take = digest.len().min(8);
    buffer[..take].copy_from_slice(&digest[..take]);
    u64::from_be_bytes(buffer)
}

/// Tag the maintenance outcome with the table identities of branch
/// releases that were drained from the pending-releases buffer during
/// this retention pass. The classification itself remains
/// manifest-driven; this is informational so callers can observe what
/// the buffer surfaced.
fn append_released_table_names(
    outcome: MaintenanceOutcome,
    drained: &[crate::branch::facts::BranchReleasePlan],
) -> MaintenanceOutcome {
    if drained.is_empty() {
        return outcome;
    }
    let mut names: Vec<String> = outcome.affected_object_names().to_vec();
    for plan in drained {
        for table in plan.releasable_tables() {
            names.push(format!("branch-release:{}", table.as_str()));
        }
    }
    outcome.with_affected_object_names(names)
}

/// Off-lock flush-watermark coverage scan (D.2b-2): runs the O(rows) durable
/// coverage scan on the captured snapshot with the runtime lock released, returning the
/// coverable candidate and its proof, or `None`. Matches the under-lock selection order
/// — the task's own candidate first, then the highest coverable manifest candidate. The
/// memtable filter mirrors the under-lock behavior; the apply step re-checks the current
/// memtable under the lock (the anti-corruption gate).
impl FlushWatermarkCoverageInputs {
    pub(crate) fn compute_coverage(
        &self,
    ) -> Option<(CommitVersion, LifecycleTableManifestFlushCoverageProof)> {
        let build = |candidate: CommitVersion| -> Option<LifecycleTableManifestFlushCoverageProof> {
            if let Some(min_unflushed) = self.min_unflushed_commit {
                if min_unflushed <= candidate {
                    return None;
                }
            }
            let proof = LifecycleTableManifestFlushCoverageProof::from_durable_snapshot(
                candidate,
                self.branch_id,
                self.owned_levels.levels(),
                &self.inherited_layers,
                &self.table_manifest,
                self.recovery_health_epoch,
                self.floor,
            )
            .ok()?;
            proof.validate_extends_checkpoint(self.floor).ok()?;
            Some(proof)
        };
        if let Some(candidate) = self.task_candidate {
            if let Some(proof) = build(candidate) {
                return Some((candidate, proof));
            }
        }
        for &candidate in &self.candidates {
            if let Some(proof) = build(candidate) {
                return Some((candidate, proof));
            }
        }
        None
    }
}

fn flush_watermark_candidates_from_manifest(
    manifest: &TableManifest,
    visible_version: CommitVersion,
    retention_watermark: CommitVersion,
) -> Vec<CommitVersion> {
    let mut candidates = Vec::new();
    for level in manifest.levels() {
        for table in level.tables() {
            let candidate = table.facts().commit_max();
            if candidate <= visible_version && candidate > retention_watermark {
                candidates.push(candidate);
            }
        }
    }
    for layer in manifest.inherited_layers() {
        for level in layer.levels() {
            for table in level.tables() {
                let candidate = table.facts().commit_max();
                if candidate <= visible_version && candidate > retention_watermark {
                    candidates.push(candidate);
                }
            }
        }
    }
    candidates.sort();
    candidates.dedup();
    candidates.reverse();
    candidates
}

fn bind_materialization_request_in_catalog(
    branch_catalog: &mut crate::lifecycle::LifecycleBranchCatalog,
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

const fn branch_id_from_table_level_task(
    task: MaintenanceTask,
) -> LifecycleResult<strata_core::BranchId> {
    match task.scope() {
        MaintenanceTaskScope::TableLevel { branch_id, .. } => Ok(branch_id),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "compaction task must target a table level",
        }),
    }
}

const fn table_level_scope_from_task(
    task: MaintenanceTask,
) -> LifecycleResult<(strata_core::BranchId, u8)> {
    match task.scope() {
        MaintenanceTaskScope::TableLevel { branch_id, level } => Ok((branch_id, level)),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "compaction task must target a table level",
        }),
    }
}

const fn branch_id_from_inherited_layer_task(
    task: MaintenanceTask,
) -> LifecycleResult<strata_core::BranchId> {
    match task.scope() {
        MaintenanceTaskScope::InheritedLayer { branch_id, .. } => Ok(branch_id),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "materialization task must target an inherited layer",
        }),
    }
}

const fn branch_id_from_table_rewrite_task(
    task: MaintenanceTask,
) -> LifecycleResult<strata_core::BranchId> {
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
) -> LifecycleResult<strata_core::BranchId> {
    match request.scope() {
        MaintenanceTaskScope::InheritedLayer { branch_id, .. } => Ok(branch_id),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "materialization task must target an inherited layer",
        }),
    }
}

fn manifest_error(error: crate::service::ManifestServiceError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        crate::lifecycle::LifecycleLowerLayer::Service,
        "manifest service failed",
        error,
    )
}

fn snapshot_error(error: crate::service::SnapshotServiceError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        crate::lifecycle::LifecycleLowerLayer::Service,
        "snapshot service failed",
        error,
    )
}

fn wal_error(error: crate::service::WalServiceError) -> LifecycleError {
    LifecycleError::lower_layer_with(LifecycleLowerLayer::Service, "WAL service failed", error)
}

fn table_object_service_error(error: crate::service::TableObjectServiceError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::Service,
        "table object service failed",
        error,
    )
}

#[cfg(test)]
mod tests {
    use super::{checkpoint_created_at, pruning_publish_gates_hold, retained_floor_not_regressed};
    use strata_core::{CommitVersion, Timestamp};

    // #3526: the off-lock pruning publish gate — every safety input must hold, or
    // the caller defers. Each argument is independently load-bearing.
    #[test]
    fn pruning_publish_gates_hold_requires_every_safety_gate() {
        assert!(pruning_publish_gates_hold(true, true, true, true, true));
        assert!(!pruning_publish_gates_hold(false, true, true, true, true));
        assert!(!pruning_publish_gates_hold(true, false, true, true, true));
        assert!(!pruning_publish_gates_hold(true, true, false, true, true));
        assert!(!pruning_publish_gates_hold(true, true, true, false, true));
        assert!(!pruning_publish_gates_hold(true, true, true, true, false));
    }

    // #3526: the floor-movement gate — a live floor advanced past the proof's
    // floor (a concurrent prune kept less) must defer; equal or lower is fine.
    #[test]
    fn retained_floor_not_regressed_rejects_an_advanced_live_floor() {
        let v = CommitVersion::new;
        assert!(retained_floor_not_regressed(None, v(5)));
        assert!(retained_floor_not_regressed(Some(v(4)), v(5)));
        assert!(retained_floor_not_regressed(Some(v(5)), v(5)));
        assert!(!retained_floor_not_regressed(Some(v(6)), v(5)));
    }

    #[test]
    fn checkpoint_timestamp_fallback_is_non_epoch_without_commits_or_manifest_timestamp() {
        assert_eq!(checkpoint_created_at(None, None), Timestamp::from_micros(1));
    }

    #[test]
    fn checkpoint_timestamp_prefers_last_commit_then_recovered_checkpoint() {
        assert_eq!(
            checkpoint_created_at(
                Some(Timestamp::from_micros(9)),
                Some(Timestamp::from_micros(7))
            ),
            Timestamp::from_micros(9)
        );
        assert_eq!(
            checkpoint_created_at(None, Some(Timestamp::from_micros(7))),
            Timestamp::from_micros(7)
        );
    }
}
