use super::{
    commit_receipt, create_effect, delete_effect, output_vector_metric,
    EngineAdminCapabilitySummary, EngineAdminConfigSummary, EngineAdminDatabaseInfo,
    EngineAdminDescribeSummary, EngineAdminGraphSummary, EngineAdminHealthStatus,
    EngineAdminHealthSummary, EngineAdminMetricsSummary, EngineAdminPrimitiveSummary,
    EngineAdminVectorCollectionSummary, EngineControlHealthStatus, EngineDatabaseOpenTarget,
    EngineFootprintDetail, EngineMemoryBudgetSource, EngineReclaimDeferralReason,
    EngineReclaimOutcome, EngineReclaimPass, EngineSpaceCreateOutcome, EngineSpaceDeleteOutcome,
    EngineStorageFootprint, EngineStorageReclaimStatus, Output, OutputAdminCapabilities,
    OutputAdminConfig, OutputAdminControlStatus, OutputAdminDatabaseInfo, OutputAdminDescribe,
    OutputAdminGraph, OutputAdminHealth, OutputAdminHealthStatus, OutputAdminMemoryBudget,
    OutputAdminMemoryBudgetSource, OutputAdminMetrics, OutputAdminOpenTarget,
    OutputAdminPrimitives, OutputAdminReclaimDeferralReason, OutputAdminReclaimOutcome,
    OutputAdminReclaimPass, OutputAdminStorage, OutputAdminStorageReclaim,
    OutputAdminVectorCollection,
};

#[cfg(not(feature = "arrow"))]
use super::ExecutorError;

#[cfg(not(feature = "arrow"))]
pub(super) fn arrow_feature_disabled() -> ExecutorError {
    ExecutorError::new(
        "unsupported.executor.arrow_feature_disabled",
        "Arrow import/export requires the executor arrow feature",
    )
}

pub(super) const fn output_admin_open_target(
    target: EngineDatabaseOpenTarget,
) -> OutputAdminOpenTarget {
    match target {
        EngineDatabaseOpenTarget::Cache => OutputAdminOpenTarget::Cache,
        EngineDatabaseOpenTarget::DurableLocal => OutputAdminOpenTarget::DurableLocal,
    }
}

pub(super) const fn output_admin_health_status(
    status: EngineAdminHealthStatus,
) -> OutputAdminHealthStatus {
    match status {
        EngineAdminHealthStatus::Healthy => OutputAdminHealthStatus::Healthy,
        EngineAdminHealthStatus::Degraded => OutputAdminHealthStatus::Degraded,
        EngineAdminHealthStatus::Unhealthy => OutputAdminHealthStatus::Unhealthy,
    }
}

pub(super) const fn output_admin_control_status(
    status: EngineControlHealthStatus,
) -> OutputAdminControlStatus {
    match status {
        EngineControlHealthStatus::Healthy => OutputAdminControlStatus::Healthy,
        EngineControlHealthStatus::Missing => OutputAdminControlStatus::Missing,
        EngineControlHealthStatus::Corrupt => OutputAdminControlStatus::Corrupt,
        EngineControlHealthStatus::Unavailable => OutputAdminControlStatus::Unavailable,
    }
}

pub(super) fn output_admin_info(info: &EngineAdminDatabaseInfo) -> OutputAdminDatabaseInfo {
    OutputAdminDatabaseInfo {
        version: info.version.clone(),
        target: output_admin_open_target(info.target),
        created: info.created,
        durable: info.durable,
        default_branch: info.default_branch.as_str().to_owned(),
        branch_count: info.branch_count,
        space_count: info.space_count,
        memory_budget: output_admin_memory_budget(info.memory_budget),
        open: info.open,
    }
}

fn output_admin_memory_budget(source: EngineMemoryBudgetSource) -> OutputAdminMemoryBudget {
    match source {
        EngineMemoryBudgetSource::Explicit { total_bytes } => OutputAdminMemoryBudget {
            total_bytes,
            source: OutputAdminMemoryBudgetSource::Explicit,
            usable_host_bytes: None,
        },
        EngineMemoryBudgetSource::DerivedFromHost {
            total_bytes,
            usable_host_bytes,
        } => OutputAdminMemoryBudget {
            total_bytes,
            source: OutputAdminMemoryBudgetSource::DerivedFromHost,
            usable_host_bytes: Some(usable_host_bytes),
        },
        EngineMemoryBudgetSource::FixedDefault { total_bytes } => OutputAdminMemoryBudget {
            total_bytes,
            source: OutputAdminMemoryBudgetSource::FixedDefault,
            usable_host_bytes: None,
        },
        // The engine enum is #[non_exhaustive]; report an unknown future
        // provenance as the fixed default rather than failing the command.
        other => OutputAdminMemoryBudget {
            total_bytes: other.total_bytes(),
            source: OutputAdminMemoryBudgetSource::FixedDefault,
            usable_host_bytes: None,
        },
    }
}

pub(super) fn output_admin_health(health: &EngineAdminHealthSummary) -> OutputAdminHealth {
    OutputAdminHealth {
        status: output_admin_health_status(health.status),
        identity: output_admin_control_status(health.identity),
        registry: output_admin_control_status(health.registry),
        branch_catalog: output_admin_control_status(health.branch_catalog),
        space_catalog: health.space_catalog.map(output_admin_control_status),
        default_branch: health.default_branch.as_str().to_owned(),
        branch_count: health.branch_count,
    }
}

/// The wire name for a reclaim outcome; `None` for a variant this build does
/// not know (the engine enum is `#[non_exhaustive]`).
pub(super) const fn output_admin_reclaim_outcome(
    outcome: EngineReclaimOutcome,
) -> Option<OutputAdminReclaimOutcome> {
    match outcome {
        EngineReclaimOutcome::Reclaimed => Some(OutputAdminReclaimOutcome::Reclaimed),
        EngineReclaimOutcome::Nothing => Some(OutputAdminReclaimOutcome::Nothing),
        EngineReclaimOutcome::Deferred => Some(OutputAdminReclaimOutcome::Deferred),
        EngineReclaimOutcome::Failed => Some(OutputAdminReclaimOutcome::Failed),
        EngineReclaimOutcome::Canceled => Some(OutputAdminReclaimOutcome::Canceled),
        _ => None,
    }
}

/// The wire name for a reclaim deferral; `None` for a variant this build does
/// not know.
pub(super) const fn output_admin_reclaim_deferral(
    deferral: EngineReclaimDeferralReason,
) -> Option<OutputAdminReclaimDeferralReason> {
    match deferral {
        EngineReclaimDeferralReason::ReaderPinned => {
            Some(OutputAdminReclaimDeferralReason::ReaderPinned)
        }
        EngineReclaimDeferralReason::Referenced => {
            Some(OutputAdminReclaimDeferralReason::Referenced)
        }
        EngineReclaimDeferralReason::IncompleteProof => {
            Some(OutputAdminReclaimDeferralReason::IncompleteProof)
        }
        EngineReclaimDeferralReason::StaleProof => {
            Some(OutputAdminReclaimDeferralReason::StaleProof)
        }
        EngineReclaimDeferralReason::RecoveryHealth => {
            Some(OutputAdminReclaimDeferralReason::RecoveryHealth)
        }
        EngineReclaimDeferralReason::InventoryAdvanced => {
            Some(OutputAdminReclaimDeferralReason::InventoryAdvanced)
        }
        EngineReclaimDeferralReason::UnsupportedScope => {
            Some(OutputAdminReclaimDeferralReason::UnsupportedScope)
        }
        _ => None,
    }
}

/// One reclaim pass on the wire; a pass whose outcome this build cannot name
/// is left out rather than misreported.
fn output_admin_reclaim_pass(pass: EngineReclaimPass) -> Option<OutputAdminReclaimPass> {
    Some(OutputAdminReclaimPass {
        outcome: output_admin_reclaim_outcome(pass.outcome)?,
        deferral: pass.deferral.and_then(output_admin_reclaim_deferral),
        bytes_reclaimed: pass.bytes_reclaimed,
        objects_affected: pass.objects_affected,
        state_changes: pass.state_changes,
    })
}

fn output_admin_storage_reclaim(reclaim: &EngineStorageReclaimStatus) -> OutputAdminStorageReclaim {
    OutputAdminStorageReclaim {
        last_mark: reclaim.last_mark.and_then(output_admin_reclaim_pass),
        last_sweep: reclaim.last_sweep.and_then(output_admin_reclaim_pass),
        last_purge: reclaim.last_purge.and_then(output_admin_reclaim_pass),
        last_snapshot_prune: reclaim
            .last_snapshot_prune
            .and_then(output_admin_reclaim_pass),
        last_wal_truncation: reclaim
            .last_wal_truncation
            .and_then(output_admin_reclaim_pass),
        total_passes: reclaim.total_passes,
        total_bytes_reclaimed: reclaim.total_bytes_reclaimed,
        reclaimed_passes: reclaim.reclaimed_passes,
        deferred_passes: reclaim.deferred_passes,
        pending_reclaim_tasks: reclaim.pending_reclaim_tasks,
    }
}

pub(super) fn output_admin_storage(footprint: &EngineStorageFootprint) -> OutputAdminStorage {
    OutputAdminStorage {
        audit: matches!(footprint.detail, EngineFootprintDetail::Audit),
        live_table_objects: footprint.live_table_objects,
        live_table_bytes: footprint.live_table_bytes,
        wal_retained_bytes: footprint.wal_retained_bytes,
        wal_active_bytes: footprint.wal_active_bytes,
        wal_retained_segments: footprint.wal_retained_segments,
        wal_retention_watermark: footprint
            .wal_retention_watermark
            .map(strata_core::CommitVersion::as_u64),
        unreferenced_objects: footprint.unreferenced_objects,
        unreferenced_bytes: footprint.unreferenced_bytes,
        quarantined_objects: footprint.quarantined_objects,
        quarantined_bytes: footprint.quarantined_bytes,
        snapshot_objects: footprint.snapshot_objects,
        snapshot_bytes: footprint.snapshot_bytes,
        superseded_snapshots: footprint.superseded_snapshots,
        superseded_snapshot_bytes: footprint.superseded_snapshot_bytes,
        wal_reclaimable_bytes: footprint.wal_reclaimable_bytes,
        wal_tail_bytes: footprint.wal_tail_bytes,
        total_bytes: footprint.total_bytes,
        reclaim: output_admin_storage_reclaim(&footprint.reclaim),
    }
}

pub(super) fn output_admin_metrics(metrics: &EngineAdminMetricsSummary) -> OutputAdminMetrics {
    OutputAdminMetrics {
        target: output_admin_open_target(metrics.target),
        durable: metrics.durable,
        open: metrics.open,
        branch_count: metrics.branch_count,
        space_count: metrics.space_count,
        control_status: output_admin_health_status(metrics.control_status),
    }
}

pub(super) fn output_admin_config(config: &EngineAdminConfigSummary) -> OutputAdminConfig {
    OutputAdminConfig {
        target: output_admin_open_target(config.target),
        created: config.created,
        durable: config.durable,
        default_branch: config.default_branch.as_str().to_owned(),
    }
}

pub(super) fn output_admin_capabilities(
    capabilities: &EngineAdminCapabilitySummary,
) -> OutputAdminCapabilities {
    OutputAdminCapabilities {
        kv: capabilities.kv,
        json: capabilities.json,
        event: capabilities.event,
        vector: capabilities.vector,
        vector_index: capabilities.vector_index,
        graph_core: capabilities.graph_core,
        arrow: cfg!(feature = "arrow"),
        inference: cfg!(feature = "inference"),
    }
}

pub(super) fn output_admin_vector_collection(
    collection: &EngineAdminVectorCollectionSummary,
) -> OutputAdminVectorCollection {
    OutputAdminVectorCollection {
        name: collection.name.clone(),
        dimension: collection.dimension,
        metric: output_vector_metric(collection.metric),
        count: collection.count,
    }
}

pub(super) fn output_admin_graph(graph: &EngineAdminGraphSummary) -> OutputAdminGraph {
    OutputAdminGraph {
        name: graph.name.clone(),
        node_count: graph.node_count,
        edge_count: graph.edge_count,
    }
}

pub(super) fn output_admin_primitives(
    primitives: &EngineAdminPrimitiveSummary,
) -> OutputAdminPrimitives {
    OutputAdminPrimitives {
        kv_count: primitives.kv_count,
        json_count: primitives.json_count,
        event_count: primitives.event_count,
        vector_collections: primitives
            .vector_collections
            .iter()
            .map(output_admin_vector_collection)
            .collect(),
        graphs: primitives.graphs.iter().map(output_admin_graph).collect(),
    }
}

pub(super) fn output_admin_describe(describe: &EngineAdminDescribeSummary) -> OutputAdminDescribe {
    OutputAdminDescribe {
        version: describe.version.clone(),
        target: output_admin_open_target(describe.target),
        default_branch: describe.default_branch.as_str().to_owned(),
        branch: describe.branch.as_str().to_owned(),
        branches: describe
            .branches
            .iter()
            .map(|branch| branch.as_str().to_owned())
            .collect(),
        spaces: describe
            .spaces
            .iter()
            .map(|space| space.as_str().to_owned())
            .collect(),
        primitives: output_admin_primitives(&describe.primitives),
        config: output_admin_config(&describe.config),
        capabilities: output_admin_capabilities(&describe.capabilities),
    }
}

pub(super) fn output_space_create(outcome: &EngineSpaceCreateOutcome) -> Output {
    Output::SpaceCreateResult {
        space: outcome.space().as_str().to_owned(),
        effect: create_effect(outcome.created()),
        commit: outcome.commit().map(commit_receipt),
    }
}

pub(super) fn output_space_delete(outcome: &EngineSpaceDeleteOutcome) -> Output {
    Output::SpaceDeleteResult {
        space: outcome.space().as_str().to_owned(),
        force: outcome.force(),
        deleted_rows: outcome.deleted_rows(),
        effect: delete_effect(outcome.deleted()),
        commit: outcome.commit().map(commit_receipt),
    }
}

#[cfg(test)]
mod memory_budget_tests {
    use super::{
        output_admin_memory_budget, EngineMemoryBudgetSource, OutputAdminMemoryBudgetSource,
    };

    #[test]
    fn explicit_maps_total_with_no_host_basis() {
        let out =
            output_admin_memory_budget(EngineMemoryBudgetSource::Explicit { total_bytes: 123 });
        assert_eq!(out.total_bytes, 123);
        assert_eq!(out.source, OutputAdminMemoryBudgetSource::Explicit);
        assert_eq!(out.usable_host_bytes, None);
    }

    #[test]
    fn derived_maps_total_and_carries_the_host_basis() {
        let out = output_admin_memory_budget(EngineMemoryBudgetSource::DerivedFromHost {
            total_bytes: 456,
            usable_host_bytes: 789,
        });
        assert_eq!(out.total_bytes, 456);
        assert_eq!(out.source, OutputAdminMemoryBudgetSource::DerivedFromHost);
        assert_eq!(out.usable_host_bytes, Some(789));
    }

    #[test]
    fn fixed_default_maps_total_with_no_host_basis() {
        let out =
            output_admin_memory_budget(EngineMemoryBudgetSource::FixedDefault { total_bytes: 512 });
        assert_eq!(out.total_bytes, 512);
        assert_eq!(out.source, OutputAdminMemoryBudgetSource::FixedDefault);
        assert_eq!(out.usable_host_bytes, None);
    }
}

#[cfg(test)]
mod storage_tests {
    use super::{
        output_admin_reclaim_deferral, output_admin_reclaim_outcome, EngineReclaimDeferralReason,
        EngineReclaimOutcome, OutputAdminReclaimDeferralReason, OutputAdminReclaimOutcome,
    };

    #[test]
    fn reclaim_outcome_truth_table() {
        for (engine, wire) in [
            (
                EngineReclaimOutcome::Reclaimed,
                OutputAdminReclaimOutcome::Reclaimed,
            ),
            (
                EngineReclaimOutcome::Nothing,
                OutputAdminReclaimOutcome::Nothing,
            ),
            (
                EngineReclaimOutcome::Deferred,
                OutputAdminReclaimOutcome::Deferred,
            ),
            (
                EngineReclaimOutcome::Failed,
                OutputAdminReclaimOutcome::Failed,
            ),
            (
                EngineReclaimOutcome::Canceled,
                OutputAdminReclaimOutcome::Canceled,
            ),
        ] {
            assert_eq!(output_admin_reclaim_outcome(engine), Some(wire));
        }
    }

    #[test]
    fn reclaim_deferral_truth_table() {
        for (engine, wire) in [
            (
                EngineReclaimDeferralReason::ReaderPinned,
                OutputAdminReclaimDeferralReason::ReaderPinned,
            ),
            (
                EngineReclaimDeferralReason::Referenced,
                OutputAdminReclaimDeferralReason::Referenced,
            ),
            (
                EngineReclaimDeferralReason::IncompleteProof,
                OutputAdminReclaimDeferralReason::IncompleteProof,
            ),
            (
                EngineReclaimDeferralReason::StaleProof,
                OutputAdminReclaimDeferralReason::StaleProof,
            ),
            (
                EngineReclaimDeferralReason::RecoveryHealth,
                OutputAdminReclaimDeferralReason::RecoveryHealth,
            ),
            (
                EngineReclaimDeferralReason::InventoryAdvanced,
                OutputAdminReclaimDeferralReason::InventoryAdvanced,
            ),
            (
                EngineReclaimDeferralReason::UnsupportedScope,
                OutputAdminReclaimDeferralReason::UnsupportedScope,
            ),
        ] {
            assert_eq!(output_admin_reclaim_deferral(engine), Some(wire));
        }
    }
}
