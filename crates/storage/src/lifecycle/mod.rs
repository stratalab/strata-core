//! Storage lifecycle coordination.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "lifecycle scaffold is consumed by later lifecycle slices"
    )
)]

mod admission_ramp;
mod background;
mod branch_lifecycle;
mod budget;
mod cache;
mod capability;
mod checkpoint;
mod compaction;
mod config;
mod durable;
mod error;
mod facts;
mod flush;
mod health;
mod maintenance;
mod outcome;
mod quarantine;
mod recovery;
mod result;
mod retained_history_extension;
mod retention;
mod rewrite_publication;
mod state;
mod table_manifest;
mod table_reachability;
mod wal_growth;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use crate::branch::snapshot::BranchSnapshotRegistry;

/// Off-lock read handles a runtime exposes so the API slot can bound (the visible-commit-version
/// atomic `V`) and load (the published per-branch snapshot) a branch read without taking the
/// runtime lock (BS2.4). Both concrete runtimes hold these as shared `Arc`s already; this trait
/// lets `RuntimeSlot<R>` clone them out at construction.
pub(crate) trait RuntimeReadHandles {
    /// The shared visible-commit-version atomic (the read visibility bound `V`).
    fn visible_handle(&self) -> Arc<AtomicU64>;
    /// The shared per-branch published-snapshot registry.
    fn snapshot_registry(&self) -> Arc<BranchSnapshotRegistry>;
}

#[allow(
    unused_imports,
    reason = "background scheduler exports define the local lifecycle surface"
)]
pub(crate) use background::{
    BackgroundBackpressureError, BackgroundTaskPriority, InlineMaintenanceExecutor,
    MaintenanceClock, MaintenanceExecutor, MaintenanceExecutorStats, MaintenanceInstant,
    ManualMaintenanceClock, RealMaintenanceClock, ThreadedMaintenanceExecutor,
};
#[allow(
    unused_imports,
    reason = "branch lifecycle catalog exports define the local surface for branch completion"
)]
pub(crate) use branch_lifecycle::{
    LifecycleBranchCatalog, LifecycleBranchClearOutcome, LifecycleBranchCreateOutcome,
    LifecycleBranchDeleteOutcome, LifecycleBranchDescriptor, LifecycleBranchForkOutcome,
    LifecycleBranchParent, LifecycleBranchStatus, RecoveryExclusivityToken,
};
#[allow(
    unused_imports,
    reason = "storage budget exports define the local surface for budget enforcement"
)]
pub(crate) use budget::{
    branch_config_with_storage_budget, branch_config_with_storage_budget_for_cache,
    branch_resident_bytes, decide_flush_rotation, estimate_commit_batch_active_bytes,
    projected_commit_rotation_would_exceed_frozen_budget, require_generated_artifact_budget,
    require_maintenance_enqueue_budget, require_manifest_catalog_budget, require_rotate_budget,
    require_table_reader_budget, snapshot_with_runtime_usage,
    table_block_cache_from_storage_budget, BudgetedCommitBranch, FlushRotationDecision,
    StorageBudgetLedger, StorageBudgetPool, StorageBudgetPressureSeverity, StorageBudgetSnapshot,
    StorageBudgetUsage, StorageRuntimeBudget, StorageRuntimeBudgetParts,
};
#[allow(
    unused_imports,
    reason = "lifecycle cache runtime exports define the local surface for later slices"
)]
pub(crate) use cache::{
    CacheBackgroundMaintenanceStep, LifecycleCacheOpenRequest, LifecycleCacheRuntime,
};
#[allow(
    unused_imports,
    reason = "lifecycle scaffold exports define the local surface for later slices"
)]
pub(crate) use capability::{
    validate_backend_capabilities_for_open, validate_storage_mode_capabilities,
    LifecycleCapabilityOutcome, ObjectDurableFenceMode,
};
#[allow(
    unused_imports,
    reason = "checkpoint maintenance exports define the local surface for later slices"
)]
pub(crate) use checkpoint::{
    checkpoint_delta_cap_decision, checkpoint_durable_branch, checkpoint_durable_rows_with_budget,
    checkpoint_request_from_maintenance_task, persist_flush_watermark,
    persist_flush_watermark_with_table_manifest_proof, should_retry_checkpoint_after_delta_cap,
    truncate_wal, validate_wal_retention_proof, wal_truncation_request_from_maintenance_task,
    DeltaCapDecision, LifecycleCheckpointOutcome, LifecycleCheckpointRequest,
    LifecycleCheckpointStatus, LifecycleFlushWatermarkOutcome, LifecycleFlushWatermarkProof,
    LifecycleTableManifestBranchCoverage, LifecycleTableManifestCoverageFamilies,
    LifecycleTableManifestFlushCoverageProof, LifecycleWalTruncationOutcome,
};
#[allow(
    unused_imports,
    reason = "table rewrite maintenance exports define the local surface for later slices"
)]
pub(crate) use compaction::{
    bind_materialization_task_for_enqueue, collect_storage_pressure,
    collect_storage_pressure_with_budget, compact_cache_branch,
    compact_cache_branch_to_fixed_point, compact_durable_branch,
    compaction_request_from_maintenance_task, materialization_request_from_maintenance_task,
    materialize_cache_branch, materialize_durable_branch, LifecycleCompactionDrainOutcome,
    LifecycleCompactionDrainRequest, LifecycleCompactionOutcome, LifecycleCompactionRequest,
    LifecycleCompactionStatus, LifecycleMaterializationOutcome, LifecycleMaterializationRequest,
    LifecycleMaterializationStatus, LifecycleStoragePressure, LifecycleStoragePressureReason,
    LifecycleStoragePressureSeverity, LifecycleTableRewriteDurability,
    LEVEL_ZERO_BLOCKING_COMPACTION_THRESHOLD,
};
#[allow(
    unused_imports,
    reason = "lifecycle scaffold exports define the local surface for later slices"
)]
pub(crate) use config::{
    LifecycleCachePreheatPolicy, LifecycleCloseTimeoutPolicy, LifecycleCompactionIoPolicy,
    LifecycleConfig, LifecycleLossyRecoveryPolicy, LifecycleMaintenanceSchedulingPolicy,
    LifecycleWalGrowthPolicy, LifecycleWriteThrottlePolicy, StorageVersionRetentionPolicy,
};
#[allow(
    unused_imports,
    reason = "durable lifecycle assembly exports define the local surface for recovery slices"
)]
pub(crate) use durable::{
    combine_non_seeded_checkpoint_rows, descriptor_version_anchor,
    parentless_content_predates_generation, record_predates_current_generation,
    DurableBackgroundMaintenanceBuild, DurableBackgroundMaintenanceBuilt,
    DurableBackgroundMaintenanceStep, DurableGroupApplyDone, DurableGroupApplyWork,
    DurableGroupInFlight, DurableGroupMemberResult, LifecycleDurableAssemblyFacts,
    LifecycleDurableLocalOpenRequest, LifecycleDurableLocalRuntime, LifecycleDurableLocalServices,
    LifecycleDurableLocalShell, LifecycleRecoveryBootstrapReport, PreparedPublishStep,
};
#[allow(
    unused_imports,
    reason = "lifecycle scaffold exports define the local surface for later slices"
)]
pub(crate) use error::{LifecycleError, LifecycleLowerLayer};
#[allow(
    unused_imports,
    reason = "lifecycle scaffold exports define the local surface for later slices"
)]
pub(crate) use facts::{
    ClosePhase, LifecycleCodecId, LifecycleState, LifecycleStats, MaintenanceTaskKind,
    ModeLifecyclePolicy, QuarantineStage, RecoveryStrictness, RetentionDecision, StorageMode,
    StorageOpenPlan,
};
#[allow(
    unused_imports,
    reason = "flush maintenance exports define the local surface for later slices"
)]
pub(crate) use flush::{
    flush_cache_branch, flush_durable_branch, FlushFrozenOutcome, FlushFrozenRequest,
    FlushTableIdentitySeed, FlushTableObjectId,
};
#[allow(
    unused_imports,
    reason = "lifecycle scaffold exports define the local surface for later slices"
)]
pub(crate) use health::{
    RecoveryDegradationClass, RecoveryFault, RecoveryFaultKind, RecoveryHealth,
};
#[allow(
    unused_imports,
    reason = "maintenance executor exports define the local surface for later slices"
)]
pub(crate) use maintenance::{
    compaction_lane_cap, evaluate_mutating_write_admission, maintenance_ready_for_recovery_health,
    subcompaction_cap, telemetry_health_debt, LifecycleMaintenanceExecutor,
    LifecycleMaintenanceStats, LifecyclePostCommitMaintenanceOutcome,
    LifecyclePostCommitMaintenanceStatus, LifecycleWriteAdmissionOutcome,
    LifecycleWriteAdmissionStatus, MaintenanceCancelOutcome, MaintenanceCheckpointOptions,
    MaintenanceClosePolicy, MaintenanceCoalesceKey, MaintenanceEnqueueOutcome,
    MaintenanceExecutorStatus, MaintenanceFailureRecord, MaintenanceFaultHook,
    MaintenanceFaultPoint, MaintenanceRetentionOptions, MaintenanceTask, MaintenanceTaskId,
    MaintenanceTaskPolicy, MaintenanceTaskPriority, MaintenanceTaskRequest, MaintenanceTaskRunner,
    MaintenanceTaskScope, MAINTENANCE_FAILURE_RECORD_CAPACITY,
};
#[allow(
    unused_imports,
    reason = "lifecycle scaffold exports define the local surface for later slices"
)]
pub(crate) use outcome::{
    CloseOutcome, CloseOutcomeEffects, CloseOutcomeStatus, MaintenanceOutcome,
    MaintenanceOutcomeReasonClass, MaintenanceOutcomeStatus, StorageOpenDisposition,
    StorageOpenOutcome,
};
#[allow(
    unused_imports,
    reason = "quarantine maintenance exports define the local surface for reclaim slices"
)]
pub(crate) use quarantine::{
    purge_proof_from_maintenance_task, purge_quarantine, quarantine_object,
    quarantine_task_without_request, repair_branch_from_maintenance_task, repair_branch_quarantine,
    repair_quarantine_family, unsupported_quarantine_maintenance, LifecyclePurgeOutcome,
    LifecyclePurgeProof, LifecyclePurgeStatus, LifecycleQuarantineOutcome,
    LifecycleQuarantineProof, LifecycleQuarantineProofStatus, LifecycleQuarantineRepairOutcome,
    LifecycleQuarantineRequest, LifecycleQuarantineStatus,
};
#[allow(
    unused_imports,
    reason = "lifecycle recovery exports define the local surface for bootstrap"
)]
pub(crate) use recovery::{
    encode_checkpoint_row_section, LifecycleRecoveredCheckpoint, LifecycleRecoveredQuarantine,
    LifecycleRecoveredTables, LifecycleRecoveredWal, LifecycleRecoveryOutcome,
    LifecycleRecoveryRequest, LifecycleRecoveryRuntime,
};
#[allow(
    unused_imports,
    reason = "lifecycle scaffold exports define the local surface for later slices"
)]
pub(crate) use result::LifecycleResult;
#[allow(
    unused_imports,
    reason = "retention maintenance exports define the local surface for later slices"
)]
pub(crate) use retention::{
    build_retention_proof, build_retention_proof_from_facts, prune_snapshots_with_proof,
    retention_outcome_for_delegated_families, retention_outcome_for_scope,
    retention_request_from_maintenance_task, table_quarantine_candidate,
    LifecycleRetentionDecisionReason, LifecycleRetentionDecisionRecord,
    LifecycleRetentionObjectFamily, LifecycleRetentionOutcome, LifecycleRetentionProof,
    LifecycleRetentionProofStatus, LifecycleRetentionRequest, LifecycleRetentionScope,
    LifecycleRetentionStatus, LifecycleSnapshotPruningOutcome, LifecycleSnapshotPruningRequest,
};
#[allow(
    unused_imports,
    reason = "durable rewrite publication exports define the local surface for maintenance dispatch"
)]
pub(crate) use rewrite_publication::{
    begin_durable_materialization_build, compact_durable_branch_manifest_backed,
    install_prepared_durable_compaction, install_prepared_durable_compaction_without_publish,
    install_prepared_durable_materialization,
    install_prepared_durable_materialization_without_publish,
    materialize_durable_branch_manifest_backed, prepare_durable_compaction_publication,
    publish_compaction_outcome_manifest, publish_materialization_outcome_manifest,
    DurableMaterializationBegin, DurableMaterializationBuild, PreparedDurableCompaction,
    PreparedDurableMaterialization,
};
#[allow(
    unused_imports,
    reason = "lifecycle state exports define the local surface for later slices"
)]
pub(crate) use state::{
    LifecycleAdmissionEffect, LifecycleCloseFact, LifecycleOperationAdmission,
    LifecycleOperationKind, LifecycleStateMachine, LifecycleTransitionEffect,
    LifecycleTransitionTrigger,
};
#[allow(
    unused_imports,
    reason = "durable table-manifest lifecycle exports define the local surface for recovery and retention slices"
)]
pub(crate) use table_manifest::{
    apply_loaded_table_manifest_to_branch, preflight_table_manifest_with_checkpoint,
    publish_table_manifest_for_branch_with_budget, stage_table_manifest_for_branch,
    table_manifest_debt_outcome, try_for_each_reader_row, LifecycleDurableTableCatalog,
    LifecycleTableManifestRecoveryOutcome, LifecycleTableManifestRecoveryStage,
};
#[allow(
    unused_imports,
    reason = "table-object reachability exports define the local retention proof surface"
)]
pub(crate) use table_reachability::{
    table_object_retention_health_debt, table_object_retention_outcome,
    LifecycleTableObjectInventoryEntry, LifecycleTableObjectProofContext,
    LifecycleTableObjectProofEpochs, LifecycleTableObjectProofToken,
    LifecycleTableObjectRetentionOutcome, LifecycleTableObjectRetentionRequest,
};
#[allow(
    unused_imports,
    reason = "WAL growth facts define the pre-public-boundary lifecycle policy surface"
)]
pub(crate) use wal_growth::{
    commits_since_checkpoint, policy_admission_error, wal_retention_watermark,
    LifecycleWalGrowthOutcome, LifecycleWalGrowthStatus, LifecycleWalGrowthTrigger,
};

#[cfg(test)]
mod tests;
