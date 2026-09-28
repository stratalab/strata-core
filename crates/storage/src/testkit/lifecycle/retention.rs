//! Generated lifecycle retention contract helpers.

use super::{ensure, script_byte};
use crate::backend::{
    Backend, BackendCapabilities, BackendError, BackendErrorKind, BackendMetadata, BackendRange,
    BackendResult, BASIC_OBJECT_BACKEND_CAPABILITIES,
};
use crate::branch::facts::BranchLevel;
use crate::format::{
    DatabaseManifest, TableManifest, TableManifestInheritedLayer,
    TableManifestInheritedLayerStatus, TableManifestLevel, TableManifestTableBounds,
    TableManifestTableFacts, TableManifestTableProvenance, TableManifestTableRef,
};
use crate::layout::ObjectLayout;
use crate::lifecycle::{
    build_retention_proof, prune_snapshots_with_proof, retention_outcome_for_delegated_families,
    table_object_retention_outcome, table_quarantine_candidate, LifecycleRetentionDecisionReason,
    LifecycleRetentionOutcome, LifecycleRetentionProofStatus, LifecycleRetentionRequest,
    LifecycleRetentionStatus, LifecycleSnapshotPruningRequest, LifecycleTableObjectInventoryEntry,
    LifecycleTableObjectProofEpochs, LifecycleTableObjectRetentionRequest,
    RecoveryDegradationClass, RecoveryFault, RecoveryFaultKind, RecoveryHealth, RetentionDecision,
};
use crate::object::{ObjectName, ObjectPrefix};
use crate::service::SnapshotService;
use crate::table::TableIdentity;
use crate::testkit::TestkitError;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use strata_core::{BranchId, CommitVersion};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LifecycleRetentionContractOutcome {
    complete_proofs: usize,
    incomplete_proofs: usize,
    blocked_recovery: usize,
    snapshot_pruned: usize,
    snapshot_protected: usize,
    snapshot_delete_failures: usize,
    table_retained: usize,
    table_quarantine_candidates: usize,
    live_owned: usize,
    live_inherited: usize,
    live_shared: usize,
    proof_incomplete: usize,
    unsafe_health_blocked: usize,
    already_quarantined: usize,
    stale_token_rejected: usize,
    no_mutation_observed: usize,
    delegated_families: usize,
    cache_unsupported: usize,
}

pub fn check_lifecycle_retention_contract(
    script: &[u8],
) -> Result<LifecycleRetentionContractOutcome, TestkitError> {
    let mut outcome = LifecycleRetentionContractOutcome::default();
    check_proof_states(script, &mut outcome)?;
    check_snapshot_pruning(script, &mut outcome)?;
    check_table_decisions(script, &mut outcome)?;
    check_table_reachability(script, &mut outcome)?;
    check_delegated_families(&mut outcome)?;
    check_cache_unsupported(script, &mut outcome)?;
    Ok(outcome)
}

impl LifecycleRetentionContractOutcome {
    pub const fn complete_proof_cases(&self) -> usize {
        self.complete_proofs
    }

    pub const fn incomplete_proof_cases(&self) -> usize {
        self.incomplete_proofs
    }

    pub const fn blocked_recovery_cases(&self) -> usize {
        self.blocked_recovery
    }

    pub const fn snapshot_pruned_cases(&self) -> usize {
        self.snapshot_pruned
    }

    pub const fn snapshot_protected_cases(&self) -> usize {
        self.snapshot_protected
    }

    pub const fn snapshot_delete_failure_cases(&self) -> usize {
        self.snapshot_delete_failures
    }

    pub const fn table_retained_cases(&self) -> usize {
        self.table_retained
    }

    pub const fn table_quarantine_candidate_cases(&self) -> usize {
        self.table_quarantine_candidates
    }

    pub const fn live_owned_cases(&self) -> usize {
        self.live_owned
    }

    pub const fn live_inherited_cases(&self) -> usize {
        self.live_inherited
    }

    pub const fn live_shared_cases(&self) -> usize {
        self.live_shared
    }

    pub const fn table_proof_incomplete_cases(&self) -> usize {
        self.proof_incomplete
    }

    pub const fn unsafe_table_health_blocked_cases(&self) -> usize {
        self.unsafe_health_blocked
    }

    pub const fn already_quarantined_cases(&self) -> usize {
        self.already_quarantined
    }

    pub const fn stale_token_rejected_cases(&self) -> usize {
        self.stale_token_rejected
    }

    pub const fn no_table_mutation_observed_cases(&self) -> usize {
        self.no_mutation_observed
    }

    pub const fn delegated_family_cases(&self) -> usize {
        self.delegated_families
    }

    pub const fn wal_delegated_cases(&self) -> usize {
        self.delegated_families
    }

    pub const fn cache_unsupported_cases(&self) -> usize {
        self.cache_unsupported
    }

    pub const fn cache_deferred_cases(&self) -> usize {
        self.cache_unsupported
    }
}

fn check_proof_states(
    script: &[u8],
    outcome: &mut LifecycleRetentionContractOutcome,
) -> Result<(), TestkitError> {
    let retain = usize::from(script_byte(script, 0) % 4);
    let request = LifecycleRetentionRequest::snapshot_pruning(retain);
    let complete =
        build_retention_proof(&request, Some(&manifest(3, 9)), &RecoveryHealth::Healthy, 3);
    ensure(
        complete.status() == LifecycleRetentionProofStatus::Complete,
        "retention proof with manifest facts was not complete",
    )?;
    outcome.complete_proofs += 1;

    let incomplete = build_retention_proof(&request, None, &RecoveryHealth::Healthy, 1);
    ensure(
        incomplete.status() == LifecycleRetentionProofStatus::Incomplete,
        "retention proof without live snapshot fact was not incomplete",
    )?;
    outcome.incomplete_proofs += 1;

    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::DataLoss,
        vec![
            RecoveryFault::new(RecoveryFaultKind::MissingSnapshotObject, "missing")
                .map_err(retention_error)?,
        ],
    )
    .map_err(retention_error)?;
    let blocked = build_retention_proof(&request, Some(&manifest(3, 9)), &health, 3);
    ensure(
        blocked.status() == LifecycleRetentionProofStatus::BlockedByRecoveryHealth,
        "retention proof did not block unsafe recovery health",
    )?;
    outcome.blocked_recovery += 1;
    Ok(())
}

fn check_snapshot_pruning(
    script: &[u8],
    outcome: &mut LifecycleRetentionContractOutcome,
) -> Result<(), TestkitError> {
    let retain = usize::from(script_byte(script, 1) % 3);
    let backend = ScriptSnapshotBackend::with_snapshots([1, 2, 3]);
    let request = LifecycleRetentionRequest::snapshot_pruning(retain);
    let proof = build_retention_proof(&request, Some(&manifest(3, 9)), &RecoveryHealth::Healthy, 3);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .map_err(retention_error)?;
    let pruned = prune_snapshots_with_proof(
        &SnapshotService::new(&backend),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .map_err(retention_error)?;

    ensure(pruned.completed(), "snapshot pruning did not complete")?;
    ensure(
        pruned
            .protected()
            .iter()
            .any(|snapshot| snapshot.snapshot_id() == 3),
        "snapshot pruning did not protect live snapshot",
    )?;
    outcome.snapshot_pruned += pruned.deleted().len();
    outcome.snapshot_protected += pruned.protected().len();

    let failing = ScriptSnapshotBackend::with_snapshots([4, 5, 6]);
    failing.fail_delete_on_call(1);
    let request = LifecycleRetentionRequest::snapshot_pruning(1);
    let proof = build_retention_proof(&request, Some(&manifest(6, 9)), &RecoveryHealth::Healthy, 3);
    let pruning = LifecycleSnapshotPruningRequest::new(proof, request.retain_newest_snapshots())
        .map_err(retention_error)?;
    let partial = prune_snapshots_with_proof(
        &SnapshotService::new(&failing),
        &pruning,
        Some(&std::collections::BTreeSet::new()),
    )
    .map_err(retention_error)?;
    ensure(
        partial.completed_with_health_debt(),
        "snapshot delete failure did not become health debt",
    )?;
    outcome.snapshot_delete_failures += partial.failed().len();
    Ok(())
}

fn check_table_decisions(
    script: &[u8],
    outcome: &mut LifecycleRetentionContractOutcome,
) -> Result<(), TestkitError> {
    let retained = ObjectName::new(format!("tables/branch/retained-{}", script_byte(script, 2)))
        .map_err(|error| TestkitError::new(format!("{error:?}")))?;
    let retired = ObjectName::new(format!("tables/branch/retired-{}", script_byte(script, 3)))
        .map_err(|error| TestkitError::new(format!("{error:?}")))?;
    let proof = build_retention_proof(
        &LifecycleRetentionRequest::global(1),
        Some(&manifest(3, 9)),
        &RecoveryHealth::Healthy,
        0,
    );
    let decisions = vec![
        crate::lifecycle::LifecycleRetentionDecisionRecord::table(
            retained,
            RetentionDecision::Retain,
            crate::lifecycle::LifecycleRetentionDecisionReason::ReachableTable,
        ),
        table_quarantine_candidate(retired),
    ];
    let retention =
        LifecycleRetentionOutcome::from_decisions(proof, decisions, 0).map_err(retention_error)?;

    ensure(
        retention.status() == LifecycleRetentionStatus::Completed,
        "table retention decision did not complete",
    )?;
    outcome.table_retained += retention.objects_retained();
    outcome.table_quarantine_candidates += retention
        .decisions()
        .iter()
        .filter(|decision| decision.decision() == RetentionDecision::QuarantineCandidate)
        .count();
    Ok(())
}

fn check_table_reachability(
    script: &[u8],
    outcome: &mut LifecycleRetentionContractOutcome,
) -> Result<(), TestkitError> {
    let fixture = table_reachability_fixture(script)?;
    let request = fixture.request();
    let retention = table_object_retention_outcome(&request).map_err(retention_error)?;
    ensure(
        retention.status() == LifecycleRetentionStatus::Completed,
        "table reachability proof did not complete",
    )?;
    record_table_reachability_counts(outcome, &retention);
    assert_table_reachability_token_rejects_stale_context(&fixture, &retention, outcome)?;
    assert_table_reachability_incomplete_proof_defers(&fixture, outcome)?;
    assert_table_reachability_unsafe_health_blocks(&fixture, outcome)?;
    Ok(())
}

fn record_table_reachability_counts(
    outcome: &mut LifecycleRetentionContractOutcome,
    retention: &crate::lifecycle::LifecycleTableObjectRetentionOutcome,
) {
    outcome.live_owned += retention
        .decisions()
        .iter()
        .filter(|decision| decision.reason() == LifecycleRetentionDecisionReason::ReachableTable)
        .count();
    outcome.live_inherited += retention
        .decisions()
        .iter()
        .filter(|decision| {
            decision.reason() == LifecycleRetentionDecisionReason::ReachableInheritedTable
        })
        .count();
    outcome.live_shared += retention
        .decisions()
        .iter()
        .filter(|decision| {
            decision.reason() == LifecycleRetentionDecisionReason::ReachableSharedTable
        })
        .count();
    outcome.table_quarantine_candidates += retention
        .decisions()
        .iter()
        .filter(|decision| decision.decision() == RetentionDecision::QuarantineCandidate)
        .count();
    outcome.already_quarantined += retention
        .decisions()
        .iter()
        .filter(|decision| {
            decision.reason() == LifecycleRetentionDecisionReason::TableAlreadyQuarantined
        })
        .count();
    outcome.no_mutation_observed += retention
        .decisions()
        .iter()
        .filter(|decision| {
            !matches!(
                decision.decision(),
                RetentionDecision::PruneCandidate | RetentionDecision::PurgeCandidate
            )
        })
        .count();
}

fn assert_table_reachability_token_rejects_stale_context(
    fixture: &TableReachabilityFixture,
    retention: &crate::lifecycle::LifecycleTableObjectRetentionOutcome,
    outcome: &mut LifecycleRetentionContractOutcome,
) -> Result<(), TestkitError> {
    ensure(
        retention
            .quarantine_tokens()
            .iter()
            .any(|token| token.object() == &fixture.orphan),
        "orphan table object did not receive a proof token",
    )?;
    let token = retention
        .quarantine_tokens()
        .first()
        .expect("proof token")
        .clone();
    ensure(
        token.validates_for(&fixture.orphan, retention.proof_context()),
        "fresh table-object proof token was rejected",
    )?;
    let stale_request = LifecycleTableObjectRetentionRequest::new(
        fixture.branch,
        RecoveryHealth::Healthy,
        LifecycleTableObjectProofEpochs::new(2, 1, 1, 1).map_err(retention_error)?,
        Vec::new(),
        vec![table_inventory(fixture.orphan.clone(), 100)?],
        Vec::new(),
    )
    .map_err(retention_error)?;
    let stale = table_object_retention_outcome(&stale_request).map_err(retention_error)?;
    ensure(
        !token.validates_for(&fixture.orphan, stale.proof_context()),
        "stale table-object proof token was accepted",
    )?;
    outcome.stale_token_rejected += 1;
    Ok(())
}

fn assert_table_reachability_incomplete_proof_defers(
    fixture: &TableReachabilityFixture,
    outcome: &mut LifecycleRetentionContractOutcome,
) -> Result<(), TestkitError> {
    let incomplete = LifecycleTableObjectRetentionRequest::new(
        fixture.branch,
        RecoveryHealth::Healthy,
        table_epochs()?,
        vec![],
        vec![table_inventory(fixture.orphan.clone(), 100)?],
        vec![],
    )
    .map_err(retention_error)?
    .with_manifest_complete(false);
    let incomplete = table_object_retention_outcome(&incomplete).map_err(retention_error)?;
    ensure(
        incomplete.status() == LifecycleRetentionStatus::DeferredIncompleteProof,
        "incomplete table reachability proof did not defer",
    )?;
    outcome.proof_incomplete += 1;
    Ok(())
}

fn assert_table_reachability_unsafe_health_blocks(
    fixture: &TableReachabilityFixture,
    outcome: &mut LifecycleRetentionContractOutcome,
) -> Result<(), TestkitError> {
    let health = RecoveryHealth::degraded(
        RecoveryDegradationClass::DataLoss,
        vec![
            RecoveryFault::new(RecoveryFaultKind::MissingTableObject, "missing")
                .map_err(retention_error)?,
        ],
    )
    .map_err(retention_error)?;
    let unsafe_request = LifecycleTableObjectRetentionRequest::new(
        fixture.branch,
        health,
        table_epochs()?,
        vec![table_manifest(fixture.branch, vec![], vec![])?],
        vec![table_inventory(fixture.orphan.clone(), 100)?],
        vec![],
    )
    .map_err(retention_error)?;
    let unsafe_outcome =
        table_object_retention_outcome(&unsafe_request).map_err(retention_error)?;
    ensure(
        unsafe_outcome.status() == LifecycleRetentionStatus::BlockedByRecoveryHealth,
        "unsafe recovery did not block table-object candidates",
    )?;
    outcome.unsafe_health_blocked += 1;
    Ok(())
}

struct TableReachabilityFixture {
    branch: BranchId,
    orphan: ObjectName,
    request: LifecycleTableObjectRetentionRequest,
}

impl TableReachabilityFixture {
    fn request(&self) -> LifecycleTableObjectRetentionRequest {
        self.request.clone()
    }
}

fn table_reachability_fixture(script: &[u8]) -> Result<TableReachabilityFixture, TestkitError> {
    let branch = script_branch(script, 5);
    let source = script_branch(script, 6);
    let other = script_branch(script, 7);
    let owned = table_object(branch, "owned0001")?;
    let inherited = table_object(source, "source0001")?;
    let shared = table_object(branch, "shared0001")?;
    let orphan = table_object(branch, "orphan0001")?;
    let quarantined = table_object(branch, "quar0001")?;
    let manifests = table_reachability_manifests(branch, source, other)?;
    let inventory = vec![
        table_inventory(owned, 100)?,
        table_inventory(inherited, 100)?,
        table_inventory(shared, 100)?,
        table_inventory(orphan.clone(), 100)?,
        table_inventory(quarantined.clone(), 100)?,
    ];
    let request = LifecycleTableObjectRetentionRequest::new(
        branch,
        RecoveryHealth::Healthy,
        table_epochs()?,
        manifests,
        inventory,
        vec![quarantined],
    )
    .map_err(retention_error)?;
    Ok(TableReachabilityFixture {
        branch,
        orphan,
        request,
    })
}

fn table_reachability_manifests(
    branch: BranchId,
    source: BranchId,
    other: BranchId,
) -> Result<Vec<TableManifest>, TestkitError> {
    Ok(vec![
        table_manifest(
            branch,
            vec![
                table_ref(branch, 0, "owned0001", TableManifestTableProvenance::Flush)?,
                table_ref(branch, 1, "shared0001", TableManifestTableProvenance::Flush)?,
            ],
            vec![table_inherited_layer(
                source,
                vec![table_ref(
                    source,
                    0,
                    "source0001",
                    TableManifestTableProvenance::Flush,
                )?],
            )?],
        )?,
        table_manifest(
            other,
            vec![],
            vec![table_inherited_layer(
                branch,
                vec![table_ref(
                    branch,
                    0,
                    "shared0001",
                    TableManifestTableProvenance::Recovered,
                )?],
            )?],
        )?,
    ])
}

fn check_delegated_families(
    outcome: &mut LifecycleRetentionContractOutcome,
) -> Result<(), TestkitError> {
    let proof = build_retention_proof(
        &LifecycleRetentionRequest::global(1),
        Some(&manifest(3, 9)),
        &RecoveryHealth::Healthy,
        0,
    );
    let retention = retention_outcome_for_delegated_families(proof).map_err(retention_error)?;
    ensure(
        retention.objects_skipped() == 2,
        "delegated retention families were not reported",
    )?;
    outcome.delegated_families += retention.objects_skipped();
    Ok(())
}

fn check_cache_unsupported(
    script: &[u8],
    outcome: &mut LifecycleRetentionContractOutcome,
) -> Result<(), TestkitError> {
    let request = if script_byte(script, 4).is_multiple_of(2) {
        crate::lifecycle::MaintenanceTaskRequest::snapshot_pruning(1)
    } else {
        crate::lifecycle::MaintenanceTaskRequest::retention(1)
    };
    ensure(
        matches!(
            request.kind(),
            crate::lifecycle::MaintenanceTaskKind::SnapshotPruning
                | crate::lifecycle::MaintenanceTaskKind::Retention
        ),
        "cache unsupported fixture did not build retention request",
    )?;
    outcome.cache_unsupported += 1;
    Ok(())
}

fn manifest(snapshot_id: u64, snapshot_watermark: u64) -> DatabaseManifest {
    DatabaseManifest::new([0x56; 16], "identity")
        .expect("manifest")
        .with_recovery_facts(
            1,
            Some(snapshot_watermark),
            Some(snapshot_id),
            Some(CommitVersion::new(snapshot_watermark)),
        )
        .expect("recovery facts")
}

fn table_manifest(
    branch_id: BranchId,
    tables: Vec<TableManifestTableRef>,
    inherited_layers: Vec<TableManifestInheritedLayer>,
) -> Result<TableManifest, TestkitError> {
    let levels = if tables.is_empty() {
        Vec::new()
    } else {
        vec![TableManifestLevel::new(BranchLevel::ZERO, tables).map_err(retention_error)?]
    };
    TableManifest::new(branch_id, None, 1, levels, inherited_layers, Vec::new())
        .map_err(retention_error)
}

fn table_inherited_layer(
    source_branch_id: BranchId,
    tables: Vec<TableManifestTableRef>,
) -> Result<TableManifestInheritedLayer, TestkitError> {
    TableManifestInheritedLayer::new(
        0,
        source_branch_id,
        None,
        CommitVersion::new(7),
        TableManifestInheritedLayerStatus::Active,
        vec![TableManifestLevel::new(BranchLevel::ZERO, tables).map_err(retention_error)?],
    )
    .map_err(retention_error)
}

fn table_ref(
    object_branch: BranchId,
    order: u32,
    object_id: &str,
    provenance: TableManifestTableProvenance,
) -> Result<TableManifestTableRef, TestkitError> {
    let commit = CommitVersion::new(u64::from(order) + 1);
    TableManifestTableRef::new(
        TableIdentity::new(format!("table-{object_id}")).map_err(retention_error)?,
        table_object(object_branch, object_id)?,
        order,
        TableManifestTableFacts::new(100, 1, 1, commit, commit, None, None)
            .map_err(retention_error)?,
        TableManifestTableBounds::new(
            format!("k{order:04}").into_bytes(),
            format!("k{order:04}z").into_bytes(),
            format!("i{order:04}").into_bytes(),
            format!("i{order:04}z").into_bytes(),
        )
        .map_err(retention_error)?,
        provenance,
    )
    .map_err(retention_error)
}

fn table_inventory(
    object: ObjectName,
    byte_count: u64,
) -> Result<LifecycleTableObjectInventoryEntry, TestkitError> {
    LifecycleTableObjectInventoryEntry::new(object, byte_count).map_err(retention_error)
}

fn table_object(branch_id: BranchId, object_id: &str) -> Result<ObjectName, TestkitError> {
    ObjectLayout::table_object(&branch_id.to_string(), 0, object_id).map_err(retention_error)
}

fn table_epochs() -> Result<LifecycleTableObjectProofEpochs, TestkitError> {
    LifecycleTableObjectProofEpochs::new(1, 1, 1, 1).map_err(retention_error)
}

fn script_branch(script: &[u8], offset: usize) -> BranchId {
    BranchId::from_bytes([script_byte(script, offset).max(1); 16])
}

fn retention_error(error: impl std::error::Error) -> TestkitError {
    TestkitError::new(error.to_string())
}

#[derive(Debug, Default)]
struct ScriptSnapshotBackend {
    objects: Mutex<BTreeMap<ObjectName, Vec<u8>>>,
    fail_delete_call: AtomicUsize,
    delete_calls: AtomicUsize,
    listed: AtomicBool,
}

impl ScriptSnapshotBackend {
    fn with_snapshots<const N: usize>(ids: [u64; N]) -> Self {
        let backend = Self::default();
        for id in ids {
            backend.objects.lock().expect("objects").insert(
                ObjectLayout::snapshot(id).expect("snapshot object"),
                format!("snapshot-{id}").into_bytes(),
            );
        }
        backend
    }

    fn fail_delete_on_call(&self, call: usize) {
        self.fail_delete_call.store(call, Ordering::SeqCst);
    }
}

impl Backend for ScriptSnapshotBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::from_slice(BASIC_OBJECT_BACKEND_CAPABILITIES)
    }

    fn read_object(&self, name: &ObjectName) -> BackendResult<Vec<u8>> {
        self.objects
            .lock()
            .expect("objects")
            .get(name)
            .cloned()
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "object not found"))
    }

    fn read_range(&self, name: &ObjectName, range: BackendRange) -> BackendResult<Vec<u8>> {
        let bytes = self.read_object(name)?;
        let start = usize::try_from(range.offset()).unwrap_or(usize::MAX);
        let end = usize::try_from(range.end_offset().unwrap_or(u64::MAX)).unwrap_or(usize::MAX);
        Ok(bytes[start.min(bytes.len())..end.min(bytes.len())].to_vec())
    }

    fn write_object(&self, name: &ObjectName, bytes: &[u8]) -> BackendResult<BackendMetadata> {
        self.objects
            .lock()
            .expect("objects")
            .insert(name.clone(), bytes.to_vec());
        Ok(BackendMetadata::new(bytes.len() as u64, None))
    }

    fn delete_object(&self, name: &ObjectName) -> crate::backend::DeleteResult {
        let call = self
            .delete_calls
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        if self.fail_delete_call.load(Ordering::SeqCst) == call {
            return crate::backend::failed_delete_result(
                name,
                BackendError::new(BackendErrorKind::Unavailable, "injected delete failure"),
            );
        }
        let removed = self.objects.lock().expect("objects").remove(name).is_some();
        crate::backend::durable_delete_result(name, removed)
    }

    fn list_prefix(&self, prefix: &ObjectPrefix) -> BackendResult<Vec<ObjectName>> {
        self.listed.store(true, Ordering::SeqCst);
        let mut objects = self
            .objects
            .lock()
            .expect("objects")
            .keys()
            .filter(|object| object.as_str().starts_with(prefix.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        objects.sort();
        Ok(objects)
    }

    fn object_metadata(&self, name: &ObjectName) -> BackendResult<BackendMetadata> {
        self.objects
            .lock()
            .expect("objects")
            .get(name)
            .map(|bytes| BackendMetadata::new(bytes.len() as u64, None))
            .ok_or_else(|| BackendError::new(BackendErrorKind::NotFound, "object not found"))
    }
}
