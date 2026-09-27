use super::error::{commit_error, default_branch_generation};
use super::{
    collect_storage_pressure_with_budget, BranchCleanupSummary, BranchGeneration, BranchId,
    BranchParentSummary, BranchReleasePlan, BranchStatus, BranchSummary, CommitBranchGeneration,
    CommitBranchGenerationGuard, CommitVersion, DiagnosticsBranchCatalogReport,
    DiagnosticsBudgetAccuracy, DiagnosticsBudgetPool, DiagnosticsBudgetPressure,
    DiagnosticsBudgetReport, DiagnosticsBudgetUsage, DiagnosticsCheckpointReport,
    DiagnosticsDetail, DiagnosticsFootprintReport, DiagnosticsQuarantineReport,
    DiagnosticsReclaimDeferral, DiagnosticsReclaimOutcome, DiagnosticsReclaimPass,
    DiagnosticsReclaimReport, DiagnosticsRecoveryClass, DiagnosticsRecoveryFault,
    DiagnosticsRecoveryFaultKind, DiagnosticsRecoveryReport, DiagnosticsScope,
    DiagnosticsSourceLayoutReport, DiagnosticsSourceLevelTableCount,
    DiagnosticsStoragePressureReason, DiagnosticsStoragePressureReport,
    DiagnosticsStoragePressureSeverity, DiagnosticsWalGrowthReport, LifecycleBranchCatalog,
    LifecycleBranchDescriptor, LifecycleBranchStatus, LifecycleDurableLocalRuntime,
    LifecycleStorageMode, LifecycleStoragePressureReason, LifecycleStoragePressureSeverity,
    LifecycleWalGrowthPolicy, MaintenanceExecutorStatus, MaintenanceWalGrowthSummary,
    ModeLifecyclePolicy, RecoveryDegradationClass, RecoveryFaultKind, RecoveryHealth,
    RecoveryHealthSummary, StorageApiError, StorageApiResult, StorageBudgetPool,
    StorageBudgetPressureSeverity, StorageBudgetSnapshot, StorageDurabilityPolicy, StorageMode,
    StorageOpenPlan, StorageOpenSummary, StorageRuntime, StorageRuntimeBudget, StorageRuntimeInner,
    WalGrowthFacts, DEFAULT_BRANCH_ID,
};
use crate::lifecycle::{
    LifecycleFootprintAuditFacts, LifecycleFootprintWatermark, MaintenanceDeferralReason,
    ReclaimFamily, ReclaimLedger, ReclaimOutcome, ReclaimPass,
};

pub(super) fn map_generation_guard(
    generation: Option<BranchGeneration>,
) -> StorageApiResult<CommitBranchGenerationGuard> {
    match generation {
        Some(generation) if generation == BranchGeneration::ZERO => {
            Err(StorageApiError::InvalidArgument {
                field: "branch_generation",
                reason: "expected branch generation must be nonzero",
            })
        }
        Some(generation) => CommitBranchGeneration::new(generation.as_u64())
            .map(CommitBranchGenerationGuard::exact)
            .map_err(commit_error),
        None => Ok(CommitBranchGenerationGuard::not_supplied()),
    }
}

pub(super) fn branch_generation_or_default(
    generation: Option<BranchGeneration>,
) -> StorageApiResult<CommitBranchGeneration> {
    match generation {
        Some(generation) if generation == BranchGeneration::ZERO => {
            Err(StorageApiError::InvalidArgument {
                field: "branch_generation",
                reason: "branch generation must be nonzero",
            })
        }
        Some(generation) => CommitBranchGeneration::new(generation.as_u64()).map_err(commit_error),
        None => default_branch_generation(),
    }
}

pub(super) fn require_valid_branch_identifier(
    branch_id: BranchId,
    field: &'static str,
) -> StorageApiResult<()> {
    if branch_id.as_bytes().iter().all(|byte| *byte == 0) {
        Err(StorageApiError::InvalidArgument {
            field,
            reason: "branch id must not be all zero",
        })
    } else {
        Ok(())
    }
}

pub(super) fn current_visible(runtime: &StorageRuntime<'_>) -> Option<CommitVersion> {
    let version = match &runtime.inner {
        StorageRuntimeInner::Cache(slot) => {
            let runtime = slot.lock();
            runtime.visible_version()
        }
        StorageRuntimeInner::DurableOwned(slot) => {
            let runtime = slot.lock();
            runtime.visible_version()
        }
        StorageRuntimeInner::Closed => CommitVersion::ZERO,
    };
    (version != CommitVersion::ZERO).then_some(version)
}

pub(super) fn branch_for_diagnostics_scope(scope: DiagnosticsScope) -> BranchId {
    match scope {
        DiagnosticsScope::Global => DEFAULT_BRANCH_ID,
        DiagnosticsScope::Branch(branch_id) => branch_id,
    }
}

pub(super) fn diagnostics_mode_from_plan(
    open_summary: Option<StorageOpenSummary>,
    plan: &StorageOpenPlan,
) -> StorageMode {
    open_summary.map_or_else(
        || map_lifecycle_storage_mode(plan.storage_mode()),
        StorageOpenSummary::mode,
    )
}

pub(super) const fn map_lifecycle_storage_mode(mode: LifecycleStorageMode) -> StorageMode {
    match mode {
        LifecycleStorageMode::Cache => StorageMode::Cache,
        LifecycleStorageMode::DurableLocalStandard => StorageMode::DurableLocal {
            policy: StorageDurabilityPolicy::Standard,
        },
        LifecycleStorageMode::DurableLocalAlways => StorageMode::DurableLocal {
            policy: StorageDurabilityPolicy::Always,
        },
        LifecycleStorageMode::ObjectDurableCandidate => StorageMode::ObjectDurableCandidate,
    }
}

pub(super) fn durable_checkpoint_report<S>(
    runtime: &LifecycleDurableLocalRuntime<'_, S>,
) -> DiagnosticsCheckpointReport {
    let Some(manifest) = runtime.services().manifest().load_current().ok().flatten() else {
        return DiagnosticsCheckpointReport::unknown();
    };
    DiagnosticsCheckpointReport::known(
        manifest.snapshot_id(),
        manifest.snapshot_watermark().map(CommitVersion::new),
        manifest.flushed_through_commit_id(),
    )
}

pub(super) fn map_diagnostics_recovery(health: &RecoveryHealth) -> DiagnosticsRecoveryReport {
    match health {
        RecoveryHealth::Healthy => DiagnosticsRecoveryReport::healthy(),
        RecoveryHealth::Degraded { class, faults } => DiagnosticsRecoveryReport::new(
            RecoveryHealthSummary::Degraded,
            Some(map_diagnostics_recovery_class(*class)),
            faults.iter().map(map_diagnostics_recovery_fault).collect(),
        ),
        RecoveryHealth::Failed { fault } => DiagnosticsRecoveryReport::new(
            RecoveryHealthSummary::Failed,
            Some(map_failed_diagnostics_recovery_class(fault.kind())),
            vec![map_diagnostics_recovery_fault(fault)],
        ),
    }
}

pub(super) const fn map_diagnostics_recovery_class(
    class: RecoveryDegradationClass,
) -> DiagnosticsRecoveryClass {
    match class {
        RecoveryDegradationClass::DataLoss => DiagnosticsRecoveryClass::Corruption,
        RecoveryDegradationClass::PolicyDowngrade => DiagnosticsRecoveryClass::Policy,
        RecoveryDegradationClass::Telemetry => DiagnosticsRecoveryClass::Telemetry,
    }
}

pub(super) const fn map_failed_diagnostics_recovery_class(
    kind: RecoveryFaultKind,
) -> DiagnosticsRecoveryClass {
    match kind {
        RecoveryFaultKind::IoFailure
        | RecoveryFaultKind::WalTailRepairFailed
        | RecoveryFaultKind::WalCommittedSuffixMissing => DiagnosticsRecoveryClass::Io,
        RecoveryFaultKind::NoManifestFallback => DiagnosticsRecoveryClass::Policy,
        RecoveryFaultKind::CorruptManifest
        | RecoveryFaultKind::CorruptSnapshot
        | RecoveryFaultKind::CorruptWal
        | RecoveryFaultKind::MissingManifestObject
        | RecoveryFaultKind::MissingSnapshotObject
        | RecoveryFaultKind::MissingTableObject
        | RecoveryFaultKind::MissingTableManifestBase
        | RecoveryFaultKind::InheritedLayerLoss
        | RecoveryFaultKind::QuarantineInventoryMismatch
        | RecoveryFaultKind::TimelineMismatch => DiagnosticsRecoveryClass::Corruption,
    }
}

pub(super) fn map_diagnostics_recovery_fault(
    fault: &crate::lifecycle::RecoveryFault,
) -> DiagnosticsRecoveryFault {
    DiagnosticsRecoveryFault::new(
        map_diagnostics_recovery_fault_kind(fault.kind()),
        fault.reason(),
        fault.affected_branch(),
    )
}

pub(super) const fn map_diagnostics_recovery_fault_kind(
    kind: RecoveryFaultKind,
) -> DiagnosticsRecoveryFaultKind {
    match kind {
        RecoveryFaultKind::CorruptManifest => DiagnosticsRecoveryFaultKind::CorruptManifest,
        RecoveryFaultKind::CorruptSnapshot => DiagnosticsRecoveryFaultKind::CorruptSnapshot,
        RecoveryFaultKind::CorruptWal => DiagnosticsRecoveryFaultKind::CorruptWal,
        RecoveryFaultKind::MissingManifestObject => {
            DiagnosticsRecoveryFaultKind::MissingManifestObject
        }
        RecoveryFaultKind::MissingSnapshotObject => {
            DiagnosticsRecoveryFaultKind::MissingSnapshotObject
        }
        RecoveryFaultKind::MissingTableObject | RecoveryFaultKind::MissingTableManifestBase => {
            // A lost table-manifest base is a missing durable table artifact; report it under
            // the existing table-object diagnostic class rather than expanding the D4 surface.
            DiagnosticsRecoveryFaultKind::MissingTableObject
        }
        RecoveryFaultKind::InheritedLayerLoss => DiagnosticsRecoveryFaultKind::InheritedLayerLoss,
        RecoveryFaultKind::NoManifestFallback => DiagnosticsRecoveryFaultKind::NoManifestFallback,
        RecoveryFaultKind::IoFailure => DiagnosticsRecoveryFaultKind::IoFailure,
        RecoveryFaultKind::QuarantineInventoryMismatch => {
            DiagnosticsRecoveryFaultKind::QuarantineInventoryMismatch
        }
        RecoveryFaultKind::TimelineMismatch => DiagnosticsRecoveryFaultKind::TimelineMismatch,
        RecoveryFaultKind::WalTailRepairFailed => DiagnosticsRecoveryFaultKind::WalTailRepairFailed,
        RecoveryFaultKind::WalCommittedSuffixMissing => {
            DiagnosticsRecoveryFaultKind::WalCommittedSuffixMissing
        }
    }
}

pub(super) fn map_budget_report(
    snapshot: &StorageBudgetSnapshot,
    total_used_bytes: u64,
    global_pressure: StorageBudgetPressureSeverity,
) -> DiagnosticsBudgetReport {
    let usages = snapshot
        .usages()
        .iter()
        .map(|usage| {
            DiagnosticsBudgetUsage::new(
                map_budget_pool(usage.pool()),
                usage.used_bytes(),
                usage.limit_bytes(),
                usage.used_count(),
                usage.limit_count(),
                map_budget_pressure(snapshot.pressure(usage.pool())),
                map_budget_accuracy(usage.pool()),
            )
        })
        .collect();
    DiagnosticsBudgetReport::known(
        snapshot.budget().total_bytes(),
        total_used_bytes,
        map_budget_pressure(global_pressure),
        usages,
    )
}

pub(super) const fn map_budget_pool(pool: StorageBudgetPool) -> DiagnosticsBudgetPool {
    match pool {
        StorageBudgetPool::BlockCache => DiagnosticsBudgetPool::BlockCache,
        StorageBudgetPool::TableReader => DiagnosticsBudgetPool::TableReader,
        StorageBudgetPool::ActiveMutable => DiagnosticsBudgetPool::ActiveMutable,
        StorageBudgetPool::FrozenMutable => DiagnosticsBudgetPool::FrozenMutable,
        StorageBudgetPool::MaintenanceQueue => DiagnosticsBudgetPool::MaintenanceQueue,
        StorageBudgetPool::GeneratedArtifact => DiagnosticsBudgetPool::GeneratedArtifact,
        StorageBudgetPool::ManifestCatalog => DiagnosticsBudgetPool::ManifestCatalog,
    }
}

/// How each pool's reported usage is accounted, for the diagnostics accuracy flag. The
/// runtime-summed (resident memtables, owned-table readers, block cache) and counted
/// (maintenance queue) pools report a live `Tracked` figure; the pools admitted per allocation
/// via `check_available` do not retain a charge, so their reported usage is `AdmissionOnly`.
pub(super) const fn map_budget_accuracy(pool: StorageBudgetPool) -> DiagnosticsBudgetAccuracy {
    match pool {
        StorageBudgetPool::BlockCache
        | StorageBudgetPool::ActiveMutable
        | StorageBudgetPool::FrozenMutable
        | StorageBudgetPool::MaintenanceQueue => DiagnosticsBudgetAccuracy::Tracked,
        StorageBudgetPool::TableReader
        | StorageBudgetPool::GeneratedArtifact
        | StorageBudgetPool::ManifestCatalog => DiagnosticsBudgetAccuracy::AdmissionOnly,
    }
}

pub(super) const fn map_budget_pressure(
    severity: StorageBudgetPressureSeverity,
) -> DiagnosticsBudgetPressure {
    match severity {
        StorageBudgetPressureSeverity::Normal => DiagnosticsBudgetPressure::Normal,
        StorageBudgetPressureSeverity::Evicting => DiagnosticsBudgetPressure::Evicting,
        StorageBudgetPressureSeverity::DeferOptionalMaintenance => {
            DiagnosticsBudgetPressure::DeferOptionalMaintenance
        }
        StorageBudgetPressureSeverity::RejectOptionalWork => {
            DiagnosticsBudgetPressure::RejectOptionalWork
        }
        StorageBudgetPressureSeverity::RejectMutatingAdmission => {
            DiagnosticsBudgetPressure::RejectMutatingAdmission
        }
    }
}

pub(super) fn diagnostics_pressure_report(
    catalog: &LifecycleBranchCatalog,
    branch_id: BranchId,
    maintenance: MaintenanceExecutorStatus,
    budget: StorageRuntimeBudget,
    policy: ModeLifecyclePolicy,
) -> DiagnosticsStoragePressureReport {
    let Ok(branch) = catalog.branch_state(branch_id) else {
        return DiagnosticsStoragePressureReport::unknown();
    };
    let pressure = collect_storage_pressure_with_budget(branch, maintenance, Some(budget));
    // Mirror the admission chokepoint: volatile modes (cache) apply no
    // source-shape pressure, so diagnostics must report the neutralized view
    // rather than raw backlog the engine never acts on.
    let pressure = if policy.may_apply_source_shape_admission_pressure() {
        pressure
    } else {
        pressure.with_source_shape_neutralized()
    };
    DiagnosticsStoragePressureReport::known(
        branch_id,
        map_storage_pressure_severity(pressure.severity()),
        map_storage_pressure_reason(pressure.reason()),
        pressure.active_rows(),
        pressure.active_bytes(),
        pressure.frozen_tables(),
        pressure.frozen_bytes(),
        pressure.level_zero_tables(),
        pressure.owned_tables(),
        pressure.inherited_layers(),
        pressure.pending_maintenance(),
    )
}

pub(super) fn diagnostics_source_layout_report(
    catalog: &LifecycleBranchCatalog,
    branch_id: BranchId,
) -> DiagnosticsSourceLayoutReport {
    let Ok(branch) = catalog.branch_state(branch_id) else {
        return DiagnosticsSourceLayoutReport::unknown();
    };
    let layout = branch.source_layout();
    DiagnosticsSourceLayoutReport::known(
        layout.active_rows(),
        layout.frozen_table_count(),
        layout.frozen_rows(),
        layout.owned_l0_tables(),
        map_source_level_table_counts(layout.owned_nonzero_level_table_counts()),
        layout.owned_total_tables(),
        layout.inherited_layers(),
        layout.inherited_l0_tables(),
        map_source_level_table_counts(layout.inherited_nonzero_level_table_counts()),
        layout.inherited_total_tables(),
    )
}

pub(super) fn map_source_level_table_counts(
    counts: &[crate::branch::facts::BranchLevelTableCount],
) -> Vec<DiagnosticsSourceLevelTableCount> {
    counts
        .iter()
        .map(|count| {
            DiagnosticsSourceLevelTableCount::new(count.level().raw(), count.table_count())
        })
        .collect()
}

pub(super) const fn map_storage_pressure_severity(
    severity: LifecycleStoragePressureSeverity,
) -> DiagnosticsStoragePressureSeverity {
    match severity {
        LifecycleStoragePressureSeverity::None => DiagnosticsStoragePressureSeverity::None,
        LifecycleStoragePressureSeverity::Background => {
            DiagnosticsStoragePressureSeverity::Background
        }
        LifecycleStoragePressureSeverity::Urgent => DiagnosticsStoragePressureSeverity::Urgent,
        LifecycleStoragePressureSeverity::BlockMutatingAdmission => {
            DiagnosticsStoragePressureSeverity::BlockMutatingAdmission
        }
    }
}

pub(super) const fn map_storage_pressure_reason(
    reason: LifecycleStoragePressureReason,
) -> DiagnosticsStoragePressureReason {
    match reason {
        LifecycleStoragePressureReason::None => DiagnosticsStoragePressureReason::None,
        LifecycleStoragePressureReason::ActiveMutableBytes => {
            DiagnosticsStoragePressureReason::ActiveMutableBytes
        }
        LifecycleStoragePressureReason::FrozenBacklog => {
            DiagnosticsStoragePressureReason::FrozenBacklog
        }
        LifecycleStoragePressureReason::LevelZeroTableBacklog => {
            DiagnosticsStoragePressureReason::LevelZeroTableBacklog
        }
        LifecycleStoragePressureReason::NonZeroLevelTableBacklog => {
            DiagnosticsStoragePressureReason::NonZeroLevelTableBacklog
        }
        LifecycleStoragePressureReason::InheritedLayerBacklog => {
            DiagnosticsStoragePressureReason::InheritedLayerBacklog
        }
        LifecycleStoragePressureReason::MaintenanceQueueBacklog => {
            DiagnosticsStoragePressureReason::MaintenanceQueueBacklog
        }
    }
}

pub(super) fn map_wal_growth_report(
    policy: LifecycleWalGrowthPolicy,
    current_facts: Option<WalGrowthFacts>,
    last_status: Option<MaintenanceWalGrowthSummary>,
) -> DiagnosticsWalGrowthReport {
    DiagnosticsWalGrowthReport::known_with_current_retention(
        policy.enabled(),
        Some(policy.max_retained_wal_bytes()),
        Some(policy.max_retained_wal_segments()),
        current_facts.map(WalGrowthFacts::retained_bytes),
        current_facts.map(WalGrowthFacts::retained_segments),
        policy.max_commits_since_checkpoint(),
        last_status,
    )
}

pub(super) fn map_branch_catalog_report(
    branches: &[BranchSummary],
) -> DiagnosticsBranchCatalogReport {
    let mut active_branches = 0;
    let mut deleted_branches = 0;
    let mut min_generation = None;
    let mut max_generation = None;
    for branch in branches {
        match branch.status() {
            BranchStatus::Active => {
                active_branches += 1;
                min_generation = Some(
                    min_generation.map_or(branch.generation(), |generation: BranchGeneration| {
                        generation.min(branch.generation())
                    }),
                );
                max_generation = Some(
                    max_generation.map_or(branch.generation(), |generation: BranchGeneration| {
                        generation.max(branch.generation())
                    }),
                );
            }
            BranchStatus::Deleted => deleted_branches += 1,
        }
    }
    DiagnosticsBranchCatalogReport::known(
        active_branches,
        deleted_branches,
        min_generation,
        max_generation,
    )
}

pub(super) fn map_branch_descriptor(descriptor: LifecycleBranchDescriptor) -> BranchSummary {
    let status = match descriptor.status() {
        LifecycleBranchStatus::Active => BranchStatus::Active,
        LifecycleBranchStatus::Deleted => BranchStatus::Deleted,
    };
    let parent = descriptor
        .parent()
        .map(|parent| BranchParentSummary::new(parent.source_branch_id(), parent.fork_version()));
    BranchSummary::new(
        descriptor.branch_id(),
        BranchGeneration::new(descriptor.generation().get()),
        status,
        parent,
        descriptor.created_at(),
        descriptor.deleted_at(),
        descriptor.state_revision(),
    )
}

pub(super) fn map_branch_cleanup(release_plan: &BranchReleasePlan) -> BranchCleanupSummary {
    BranchCleanupSummary::new(
        release_plan.removed_refs().len(),
        release_plan.releasable_tables().len(),
        release_plan.protected_tables().len(),
    )
}

/// Space-reclamation contract §3.5 (slice 2): the footprint, reclaim and
/// quarantine reports of a durable runtime. The live tier reads only state the
/// runtime holds; the audit tier performs the reclaim runners' listings and
/// stats, and populates the quarantine report from the inventories it read.
pub(super) fn durable_footprint_report<S>(
    runtime: &LifecycleDurableLocalRuntime<'_, S>,
    detail: DiagnosticsDetail,
) -> (
    DiagnosticsFootprintReport,
    DiagnosticsReclaimReport,
    DiagnosticsQuarantineReport,
) {
    let live = runtime.footprint_live_facts();
    let live_report = DiagnosticsFootprintReport::known_live(
        live.live_table_objects(),
        live.live_table_bytes(),
        live.wal().map(WalGrowthFacts::retained_bytes),
        live.wal().map(WalGrowthFacts::active_segment_size),
        live.wal().map(WalGrowthFacts::retained_segments),
        match live.wal_retention_watermark() {
            LifecycleFootprintWatermark::Known(watermark) => watermark,
            LifecycleFootprintWatermark::Cold => None,
        },
    );
    let audit = match detail {
        // Rationale: an audit that fails mid-listing degrades to "audit facts
        // unknown" on an otherwise complete report, the same partial-report
        // contract the checkpoint report follows on a manifest read failure.
        DiagnosticsDetail::Audit => runtime.footprint_audit_facts().ok(),
        DiagnosticsDetail::Live => None,
    };
    let quarantine = audit
        .as_ref()
        .map_or_else(DiagnosticsQuarantineReport::unknown, |audit| {
            DiagnosticsQuarantineReport::known(
                audit.quarantined_objects(),
                audit.quarantined_bytes(),
            )
        });
    let footprint = crate::api::diagnostics::footprint_for_detail(
        detail,
        live_report,
        audit.map(map_footprint_audit),
    );
    let reclaim = map_reclaim_report(runtime.reclaim_ledger(), live.pending_reclaim_tasks());
    (footprint, reclaim, quarantine)
}

pub(super) fn map_footprint_audit(
    audit: LifecycleFootprintAuditFacts,
) -> crate::api::diagnostics::DiagnosticsFootprintAudit {
    let (unreferenced_objects, unreferenced_bytes) = match audit.unreferenced() {
        Some((objects, bytes)) => (Some(objects), Some(bytes)),
        None => (None, None),
    };
    crate::api::diagnostics::DiagnosticsFootprintAudit {
        unreferenced_objects,
        unreferenced_bytes,
        snapshot_objects: audit.snapshot_objects(),
        snapshot_bytes: audit.snapshot_bytes(),
        superseded_snapshots: audit.superseded_snapshots(),
        superseded_snapshot_bytes: audit.superseded_snapshot_bytes(),
        wal_reclaimable_bytes: audit.wal_reclaimable_bytes(),
        wal_tail_bytes: audit.wal_tail_bytes(),
    }
}

pub(super) fn map_reclaim_report(
    ledger: &ReclaimLedger,
    pending_reclaim_tasks: usize,
) -> DiagnosticsReclaimReport {
    let last = crate::api::diagnostics::DiagnosticsReclaimPasses {
        mark: ledger
            .last(ReclaimFamily::TableObjectMark)
            .map(map_reclaim_pass),
        sweep: ledger
            .last(ReclaimFamily::TableObjectSweep)
            .map(map_reclaim_pass),
        purge: ledger
            .last(ReclaimFamily::QuarantinePurge)
            .map(map_reclaim_pass),
        snapshot_prune: ledger
            .last(ReclaimFamily::SnapshotPrune)
            .map(map_reclaim_pass),
        wal_truncation: ledger
            .last(ReclaimFamily::WalTruncation)
            .map(map_reclaim_pass),
    };
    let totals = ledger.totals();
    DiagnosticsReclaimReport::known(
        last,
        totals.passes(),
        totals.bytes_reclaimed(),
        totals.reclaimed_passes(),
        totals.deferred_passes(),
        pending_reclaim_tasks,
    )
}

pub(super) fn map_reclaim_pass(pass: ReclaimPass) -> DiagnosticsReclaimPass {
    DiagnosticsReclaimPass::new(
        map_reclaim_outcome(pass.outcome()),
        pass.deferral().map(map_reclaim_deferral),
        pass.bytes_reclaimed(),
        pass.objects_affected(),
        pass.state_changes(),
    )
}

pub(super) const fn map_reclaim_outcome(outcome: ReclaimOutcome) -> DiagnosticsReclaimOutcome {
    match outcome {
        ReclaimOutcome::Reclaimed => DiagnosticsReclaimOutcome::Reclaimed,
        ReclaimOutcome::Nothing => DiagnosticsReclaimOutcome::Nothing,
        ReclaimOutcome::Deferred => DiagnosticsReclaimOutcome::Deferred,
        ReclaimOutcome::Failed => DiagnosticsReclaimOutcome::Failed,
        ReclaimOutcome::Canceled => DiagnosticsReclaimOutcome::Canceled,
    }
}

pub(super) const fn map_reclaim_deferral(
    reason: MaintenanceDeferralReason,
) -> DiagnosticsReclaimDeferral {
    match reason {
        MaintenanceDeferralReason::ReaderPinned => DiagnosticsReclaimDeferral::ReaderPinned,
        MaintenanceDeferralReason::Referenced => DiagnosticsReclaimDeferral::Referenced,
        MaintenanceDeferralReason::IncompleteProof => DiagnosticsReclaimDeferral::IncompleteProof,
        MaintenanceDeferralReason::StaleProof => DiagnosticsReclaimDeferral::StaleProof,
        MaintenanceDeferralReason::RecoveryHealth => DiagnosticsReclaimDeferral::RecoveryHealth,
        MaintenanceDeferralReason::InventoryAdvanced => {
            DiagnosticsReclaimDeferral::InventoryAdvanced
        }
        MaintenanceDeferralReason::UnsupportedScope => DiagnosticsReclaimDeferral::UnsupportedScope,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::DiagnosticsFactState;
    use crate::lifecycle::{
        classify_reclaim, MaintenanceOutcome, MaintenanceOutcomeStatus, MaintenanceTaskKind,
        MaintenanceTaskScope,
    };

    #[test]
    fn map_reclaim_outcome_truth_table() {
        for (outcome, expected) in [
            (
                ReclaimOutcome::Reclaimed,
                DiagnosticsReclaimOutcome::Reclaimed,
            ),
            (ReclaimOutcome::Nothing, DiagnosticsReclaimOutcome::Nothing),
            (
                ReclaimOutcome::Deferred,
                DiagnosticsReclaimOutcome::Deferred,
            ),
            (ReclaimOutcome::Failed, DiagnosticsReclaimOutcome::Failed),
            (
                ReclaimOutcome::Canceled,
                DiagnosticsReclaimOutcome::Canceled,
            ),
        ] {
            assert_eq!(map_reclaim_outcome(outcome), expected, "{outcome:?}");
        }
    }

    #[test]
    fn map_reclaim_deferral_truth_table() {
        for (reason, expected) in [
            (
                MaintenanceDeferralReason::ReaderPinned,
                DiagnosticsReclaimDeferral::ReaderPinned,
            ),
            (
                MaintenanceDeferralReason::Referenced,
                DiagnosticsReclaimDeferral::Referenced,
            ),
            (
                MaintenanceDeferralReason::IncompleteProof,
                DiagnosticsReclaimDeferral::IncompleteProof,
            ),
            (
                MaintenanceDeferralReason::StaleProof,
                DiagnosticsReclaimDeferral::StaleProof,
            ),
            (
                MaintenanceDeferralReason::RecoveryHealth,
                DiagnosticsReclaimDeferral::RecoveryHealth,
            ),
            (
                MaintenanceDeferralReason::InventoryAdvanced,
                DiagnosticsReclaimDeferral::InventoryAdvanced,
            ),
            (
                MaintenanceDeferralReason::UnsupportedScope,
                DiagnosticsReclaimDeferral::UnsupportedScope,
            ),
        ] {
            assert_eq!(map_reclaim_deferral(reason), expected, "{reason:?}");
        }
    }

    fn recorded(
        kind: MaintenanceTaskKind,
        scope: MaintenanceTaskScope,
        bytes: u64,
    ) -> ReclaimLedger {
        let outcome = MaintenanceOutcome::new(kind, MaintenanceOutcomeStatus::Completed)
            .with_task_scope(scope)
            .with_effects(2, bytes, true)
            .with_state_changes(1);
        let (family, pass) = classify_reclaim(&outcome).expect("a reclaim family");
        let mut ledger = ReclaimLedger::default();
        ledger.record(family, pass);
        ledger
    }

    #[test]
    fn map_reclaim_report_places_each_family_in_its_slot_with_the_totals() {
        let branch = strata_core::BranchId::from_bytes([0x33; 16]);
        for (case, kind, scope, pick) in [
            (
                "mark",
                MaintenanceTaskKind::Retention,
                MaintenanceTaskScope::Branch(branch),
                DiagnosticsReclaimReport::last_mark
                    as fn(DiagnosticsReclaimReport) -> Option<DiagnosticsReclaimPass>,
            ),
            (
                "sweep",
                MaintenanceTaskKind::Quarantine,
                MaintenanceTaskScope::Global,
                DiagnosticsReclaimReport::last_sweep,
            ),
            (
                "purge",
                MaintenanceTaskKind::Purge,
                MaintenanceTaskScope::Branch(branch),
                DiagnosticsReclaimReport::last_purge,
            ),
            (
                "snapshot prune",
                MaintenanceTaskKind::SnapshotPruning,
                MaintenanceTaskScope::Retention,
                DiagnosticsReclaimReport::last_snapshot_prune,
            ),
            (
                "wal truncation",
                MaintenanceTaskKind::WalTruncation,
                MaintenanceTaskScope::Global,
                DiagnosticsReclaimReport::last_wal_truncation,
            ),
        ] {
            let ledger = recorded(kind, scope, 4096);

            let report = map_reclaim_report(&ledger, 3);

            assert_eq!(report.state(), DiagnosticsFactState::Known, "{case}");
            let pass = pick(report).unwrap_or_else(|| panic!("{case} is in its slot"));
            assert_eq!(
                pass.outcome(),
                DiagnosticsReclaimOutcome::Reclaimed,
                "{case}"
            );
            assert_eq!(pass.deferral(), None, "{case}");
            assert_eq!(pass.bytes_reclaimed(), 4096, "{case}");
            assert_eq!(pass.objects_affected(), 2, "{case}");
            assert_eq!(pass.state_changes(), 1, "{case}");
            assert_eq!(report.total_passes(), 1, "{case}");
            assert_eq!(report.total_bytes_reclaimed(), 4096, "{case}");
            assert_eq!(report.reclaimed_passes(), 1, "{case}");
            assert_eq!(report.deferred_passes(), 0, "{case}");
            assert_eq!(report.pending_reclaim_tasks(), Some(3), "{case}");
            let others = [
                report.last_mark(),
                report.last_sweep(),
                report.last_purge(),
                report.last_snapshot_prune(),
                report.last_wal_truncation(),
            ]
            .iter()
            .filter(|slot| slot.is_some())
            .count();
            assert_eq!(others, 1, "{case}: only its own slot is filled");
        }
    }

    #[test]
    fn map_reclaim_report_of_an_empty_ledger_is_known_and_empty() {
        let report = map_reclaim_report(&ReclaimLedger::default(), 0);

        assert_eq!(report.state(), DiagnosticsFactState::Known);
        assert_eq!(report.last_mark(), None);
        assert_eq!(report.last_wal_truncation(), None);
        assert_eq!(report.total_passes(), 0);
        assert_eq!(report.pending_reclaim_tasks(), Some(0));
    }
}
