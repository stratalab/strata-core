//! Durable table-object reachability proof.

#![allow(
    dead_code,
    reason = "table-object reachability proof is consumed by retention and quarantine slices"
)]

use super::{
    telemetry_health_debt, LifecycleError, LifecycleResult, LifecycleRetentionDecisionReason,
    LifecycleRetentionDecisionRecord, LifecycleRetentionOutcome, LifecycleRetentionProof,
    LifecycleRetentionProofStatus, LifecycleRetentionStatus, RecoveryDegradationClass,
    RecoveryFault, RecoveryFaultKind, RecoveryHealth, RetentionDecision,
};
use crate::format::{TableManifest, TableManifestTableProvenance};
use crate::layout::{ObjectLayout, TableObjectClassification};
use crate::object::ObjectName;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use strata_core::BranchId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleTableObjectProofEpochs {
    manifest: u64,
    table_inventory: u64,
    quarantine_inventory: u64,
    recovery_health: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LifecycleTableObjectProofCompleteness {
    manifest: bool,
    table_inventory: bool,
    quarantine_inventory: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleTableObjectInventoryEntry {
    object: ObjectName,
    byte_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleTableObjectRetentionRequest {
    branch_id: BranchId,
    recovery_health: RecoveryHealth,
    epochs: LifecycleTableObjectProofEpochs,
    manifests: Vec<TableManifest>,
    inventory: Vec<LifecycleTableObjectInventoryEntry>,
    quarantined_objects: Vec<ObjectName>,
    /// In-memory-reachable table objects (every catalog branch's owned levels + inherited
    /// layers) that must be treated as live even when no durable manifest references them.
    /// COW invariant: an object is deletable only when unreachable from EVERY branch, and a
    /// branch's references can be durably invisible in narrow windows (a fork child whose
    /// fork-time manifest publish failed, a freshly installed table whose manifest publish
    /// is still in flight). Empty for report-only marks; the sweep supplies it.
    pinned_objects: Vec<ObjectName>,
    completeness: LifecycleTableObjectProofCompleteness,
    allow_telemetry_degraded_recovery: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleTableObjectProofContext {
    branch_id: BranchId,
    epochs: LifecycleTableObjectProofEpochs,
    fingerprint: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleTableObjectProofToken {
    object: ObjectName,
    branch_id: BranchId,
    epochs: LifecycleTableObjectProofEpochs,
    fingerprint: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleTableObjectRetentionOutcome {
    retention: LifecycleRetentionOutcome,
    proof_context: LifecycleTableObjectProofContext,
    quarantine_tokens: Vec<LifecycleTableObjectProofToken>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LiveTableReason {
    Own,
    Inherited,
    Materialized,
    Shared,
}

impl LifecycleTableObjectProofEpochs {
    pub(crate) fn new(
        manifest_epoch: u64,
        table_inventory_epoch: u64,
        quarantine_inventory_epoch: u64,
        recovery_health_epoch: u64,
    ) -> LifecycleResult<Self> {
        let epochs = Self {
            manifest: manifest_epoch,
            table_inventory: table_inventory_epoch,
            quarantine_inventory: quarantine_inventory_epoch,
            recovery_health: recovery_health_epoch,
        };
        epochs.validate()?;
        Ok(epochs)
    }

    fn validate(&self) -> LifecycleResult<()> {
        if self.manifest == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "manifest_epoch",
                reason: "must be nonzero",
            });
        }
        if self.table_inventory == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "table_inventory_epoch",
                reason: "must be nonzero",
            });
        }
        if self.quarantine_inventory == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "quarantine_inventory_epoch",
                reason: "must be nonzero",
            });
        }
        if self.recovery_health == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "recovery_health_epoch",
                reason: "must be nonzero",
            });
        }
        Ok(())
    }

    pub(crate) const fn manifest_epoch(&self) -> u64 {
        self.manifest
    }

    pub(crate) const fn table_inventory_epoch(&self) -> u64 {
        self.table_inventory
    }

    pub(crate) const fn quarantine_inventory_epoch(&self) -> u64 {
        self.quarantine_inventory
    }

    pub(crate) const fn recovery_health_epoch(&self) -> u64 {
        self.recovery_health
    }
}

impl LifecycleTableObjectProofCompleteness {
    const fn complete() -> Self {
        Self {
            manifest: true,
            table_inventory: true,
            quarantine_inventory: true,
        }
    }
}

impl LifecycleTableObjectInventoryEntry {
    pub(crate) fn new(object: ObjectName, byte_count: u64) -> LifecycleResult<Self> {
        if byte_count == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "table_object_byte_count",
                reason: "must be nonzero",
            });
        }
        Ok(Self { object, byte_count })
    }

    pub(crate) const fn object(&self) -> &ObjectName {
        &self.object
    }

    pub(crate) const fn byte_count(&self) -> u64 {
        self.byte_count
    }
}

impl LifecycleTableObjectRetentionRequest {
    pub(crate) fn new(
        branch_id: BranchId,
        recovery_health: RecoveryHealth,
        epochs: LifecycleTableObjectProofEpochs,
        manifests: Vec<TableManifest>,
        inventory: Vec<LifecycleTableObjectInventoryEntry>,
        quarantined_objects: Vec<ObjectName>,
    ) -> LifecycleResult<Self> {
        let request = Self {
            branch_id,
            recovery_health,
            epochs,
            manifests,
            inventory,
            quarantined_objects,
            pinned_objects: Vec::new(),
            completeness: LifecycleTableObjectProofCompleteness::complete(),
            allow_telemetry_degraded_recovery: true,
        };
        request.validate()?;
        Ok(request)
    }

    /// Objects reachable from in-memory branch state (see the field doc): treated as live
    /// alongside the durable-manifest reachability set.
    pub(crate) fn with_pinned_objects(mut self, pinned_objects: Vec<ObjectName>) -> Self {
        self.pinned_objects = pinned_objects;
        self
    }

    pub(crate) fn with_manifest_complete(mut self, complete: bool) -> Self {
        self.completeness.manifest = complete;
        self
    }

    pub(crate) fn with_table_inventory_complete(mut self, complete: bool) -> Self {
        self.completeness.table_inventory = complete;
        self
    }

    pub(crate) fn with_quarantine_inventory_complete(mut self, complete: bool) -> Self {
        self.completeness.quarantine_inventory = complete;
        self
    }

    pub(crate) const fn with_telemetry_degraded_recovery_allowed(mut self, allowed: bool) -> Self {
        self.allow_telemetry_degraded_recovery = allowed;
        self
    }

    /// The table-object inventory the mark classified, with each object's
    /// on-disk size (the audit footprint joins candidates against it).
    pub(crate) fn inventory(&self) -> &[LifecycleTableObjectInventoryEntry] {
        &self.inventory
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) fn recovery_health(&self) -> &RecoveryHealth {
        &self.recovery_health
    }

    pub(crate) const fn epochs(&self) -> LifecycleTableObjectProofEpochs {
        self.epochs
    }

    fn validate(&self) -> LifecycleResult<()> {
        self.epochs.validate()?;
        let mut seen = BTreeSet::new();
        for entry in &self.inventory {
            if !seen.insert(entry.object().clone()) {
                return Err(LifecycleError::InvalidConfig {
                    field: "table_object_inventory",
                    reason: "duplicate table object inventory entry",
                });
            }
        }
        Ok(())
    }
}

impl LifecycleTableObjectProofContext {
    #[cfg(test)]
    pub(crate) const fn new(
        branch_id: BranchId,
        epochs: LifecycleTableObjectProofEpochs,
        fingerprint: [u8; 32],
    ) -> Self {
        Self {
            branch_id,
            epochs,
            fingerprint,
        }
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn epochs(&self) -> LifecycleTableObjectProofEpochs {
        self.epochs
    }

    pub(crate) const fn fingerprint(&self) -> &[u8; 32] {
        &self.fingerprint
    }
}

impl LifecycleTableObjectProofToken {
    pub(crate) fn object(&self) -> &ObjectName {
        &self.object
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn epochs(&self) -> LifecycleTableObjectProofEpochs {
        self.epochs
    }

    pub(crate) fn validates_for(
        &self,
        object: &ObjectName,
        context: &LifecycleTableObjectProofContext,
    ) -> bool {
        &self.object == object
            && self.branch_id == context.branch_id
            && self.epochs == context.epochs
            && self.fingerprint == context.fingerprint
    }

    #[cfg(test)]
    pub(crate) fn validates_against(&self, context: &LifecycleTableObjectProofContext) -> bool {
        self.branch_id == context.branch_id
            && self.epochs == context.epochs
            && self.fingerprint == context.fingerprint
    }
}

impl LifecycleTableObjectRetentionOutcome {
    pub(crate) fn new(request: &LifecycleTableObjectRetentionRequest) -> LifecycleResult<Self> {
        let mut live_tables = live_table_objects(&request.manifests);
        // COW invariant: union in-memory-reachable objects (durably-invisible references such
        // as a fork child in its manifest-publish crash window) into the live set — an object
        // is deletable only when unreachable from EVERY branch.
        for object in &request.pinned_objects {
            live_tables
                .entry(object.clone())
                .or_insert(LiveTableReason::Shared);
        }
        let proof_status = proof_status_for_request(request, &live_tables);
        let proof = LifecycleRetentionProof::new(
            proof_status,
            request.recovery_health.clone(),
            None,
            None,
            None,
            missing_fact_for_status(proof_status),
        );
        let context = proof_context(request, &live_tables);
        let (decisions, tokens) =
            table_object_decisions(request, proof_status, &live_tables, &context);
        let retention = LifecycleRetentionOutcome::from_decisions(proof, decisions, 0)?;
        Ok(Self {
            retention,
            proof_context: context,
            quarantine_tokens: tokens,
        })
    }

    pub(crate) const fn status(&self) -> LifecycleRetentionStatus {
        self.retention.status()
    }

    pub(crate) fn retention(&self) -> &LifecycleRetentionOutcome {
        &self.retention
    }

    pub(crate) fn decisions(&self) -> &[LifecycleRetentionDecisionRecord] {
        self.retention.decisions()
    }

    pub(crate) fn proof_context(&self) -> &LifecycleTableObjectProofContext {
        &self.proof_context
    }

    pub(crate) fn quarantine_tokens(&self) -> &[LifecycleTableObjectProofToken] {
        &self.quarantine_tokens
    }

    pub(crate) fn recovery_health(&self) -> Option<&RecoveryHealth> {
        self.retention.recovery_health()
    }
}

pub(crate) fn table_object_retention_outcome(
    request: &LifecycleTableObjectRetentionRequest,
) -> LifecycleResult<LifecycleTableObjectRetentionOutcome> {
    LifecycleTableObjectRetentionOutcome::new(request)
}

fn proof_status_for_request(
    request: &LifecycleTableObjectRetentionRequest,
    live_tables: &BTreeMap<ObjectName, LiveTableReason>,
) -> LifecycleRetentionProofStatus {
    if recovery_health_blocks_table_retention(
        &request.recovery_health,
        request.allow_telemetry_degraded_recovery,
    ) {
        return LifecycleRetentionProofStatus::BlockedByRecoveryHealth;
    }
    if !request.completeness.manifest
        || !request.completeness.table_inventory
        || !request.completeness.quarantine_inventory
        || live_tables.keys().any(|object| {
            !request
                .inventory
                .iter()
                .any(|entry| entry.object() == object)
        })
    {
        return LifecycleRetentionProofStatus::Incomplete;
    }
    LifecycleRetentionProofStatus::Complete
}

fn missing_fact_for_status(status: LifecycleRetentionProofStatus) -> Option<&'static str> {
    match status {
        LifecycleRetentionProofStatus::Complete => None,
        LifecycleRetentionProofStatus::Incomplete => Some("table_reachability"),
        LifecycleRetentionProofStatus::BlockedByRecoveryHealth => Some("recovery_health"),
    }
}

fn table_object_decisions(
    request: &LifecycleTableObjectRetentionRequest,
    proof_status: LifecycleRetentionProofStatus,
    live_tables: &BTreeMap<ObjectName, LiveTableReason>,
    context: &LifecycleTableObjectProofContext,
) -> (
    Vec<LifecycleRetentionDecisionRecord>,
    Vec<LifecycleTableObjectProofToken>,
) {
    let quarantined: BTreeSet<ObjectName> = request.quarantined_objects.iter().cloned().collect();
    let mut decisions = Vec::new();
    let mut tokens = Vec::new();
    for entry in sorted_inventory(&request.inventory) {
        let object = entry.object().clone();
        if is_non_table_object(&object) || is_table_manifest_object(&object) {
            continue;
        }
        let (decision, reason) = match proof_status {
            LifecycleRetentionProofStatus::BlockedByRecoveryHealth => (
                RetentionDecision::SkipUntilProof,
                LifecycleRetentionDecisionReason::UnsafeRecoveryHealth,
            ),
            LifecycleRetentionProofStatus::Incomplete => (
                RetentionDecision::SkipUntilProof,
                LifecycleRetentionDecisionReason::ProofIncomplete,
            ),
            LifecycleRetentionProofStatus::Complete => {
                if let Some(reason) = live_tables.get(&object) {
                    (
                        RetentionDecision::Retain,
                        retention_reason_for_live_table(*reason),
                    )
                } else if quarantined.contains(&object) {
                    (
                        RetentionDecision::SkipUntilProof,
                        LifecycleRetentionDecisionReason::TableAlreadyQuarantined,
                    )
                } else if !is_table_data_object(&object) {
                    (
                        RetentionDecision::RepairCandidate,
                        LifecycleRetentionDecisionReason::MalformedTableObject,
                    )
                } else {
                    tokens.push(LifecycleTableObjectProofToken {
                        object: object.clone(),
                        branch_id: context.branch_id,
                        epochs: context.epochs,
                        fingerprint: context.fingerprint,
                    });
                    (
                        RetentionDecision::QuarantineCandidate,
                        LifecycleRetentionDecisionReason::TableRequiresQuarantine,
                    )
                }
            }
        };
        decisions.push(LifecycleRetentionDecisionRecord::table(
            object, decision, reason,
        ));
    }
    (decisions, tokens)
}

fn retention_reason_for_live_table(reason: LiveTableReason) -> LifecycleRetentionDecisionReason {
    match reason {
        LiveTableReason::Own => LifecycleRetentionDecisionReason::ReachableTable,
        LiveTableReason::Inherited => LifecycleRetentionDecisionReason::ReachableInheritedTable,
        LiveTableReason::Materialized => {
            LifecycleRetentionDecisionReason::ReachableMaterializedTable
        }
        LiveTableReason::Shared => LifecycleRetentionDecisionReason::ReachableSharedTable,
    }
}

fn live_table_objects(manifests: &[TableManifest]) -> BTreeMap<ObjectName, LiveTableReason> {
    let mut live = BTreeMap::<ObjectName, LiveTableReason>::new();
    for manifest in manifests {
        for level in manifest.levels() {
            for table in level.tables() {
                record_live_table(
                    &mut live,
                    table.object().clone(),
                    reason_for_provenance(table.provenance(), false),
                );
            }
        }
        for layer in manifest.inherited_layers() {
            for level in layer.levels() {
                for table in level.tables() {
                    record_live_table(
                        &mut live,
                        table.object().clone(),
                        reason_for_provenance(table.provenance(), true),
                    );
                }
            }
        }
    }
    live
}

fn record_live_table(
    live: &mut BTreeMap<ObjectName, LiveTableReason>,
    object: ObjectName,
    reason: LiveTableReason,
) {
    match live.get_mut(&object) {
        Some(existing) => {
            *existing = merge_live_table_reason(*existing, reason);
        }
        None => {
            live.insert(object, reason);
        }
    }
}

/// Combine two reasons for retaining the same table object.
///
/// Cross-manifest duplicates (e.g. branch X owns the object and branch Y
/// inherits it) promote to `Shared`. But `Materialized` carries a stronger
/// semantic — the object replaced an inherited layer — and must not be
/// downgraded just because another branch also references it; otherwise the
/// emitted reason no longer matches the manifest's stated provenance.
const fn merge_live_table_reason(
    existing: LiveTableReason,
    new: LiveTableReason,
) -> LiveTableReason {
    match (existing, new) {
        (LiveTableReason::Materialized, _) | (_, LiveTableReason::Materialized) => {
            LiveTableReason::Materialized
        }
        (LiveTableReason::Own, LiveTableReason::Own) => LiveTableReason::Own,
        (LiveTableReason::Inherited, LiveTableReason::Inherited) => LiveTableReason::Inherited,
        _ => LiveTableReason::Shared,
    }
}

fn reason_for_provenance(
    provenance: &TableManifestTableProvenance,
    inherited_layer: bool,
) -> LiveTableReason {
    match provenance {
        TableManifestTableProvenance::MaterializationReplacement { .. } => {
            LiveTableReason::Materialized
        }
        TableManifestTableProvenance::Flush
        | TableManifestTableProvenance::SnapshotInstall
        | TableManifestTableProvenance::Compaction
        | TableManifestTableProvenance::Recovered
            if inherited_layer =>
        {
            LiveTableReason::Inherited
        }
        TableManifestTableProvenance::Flush
        | TableManifestTableProvenance::SnapshotInstall
        | TableManifestTableProvenance::Compaction
        | TableManifestTableProvenance::Recovered => LiveTableReason::Own,
    }
}

fn sorted_inventory(
    inventory: &[LifecycleTableObjectInventoryEntry],
) -> Vec<&LifecycleTableObjectInventoryEntry> {
    let mut sorted = inventory.iter().collect::<Vec<_>>();
    sorted.sort_by(|left, right| left.object().cmp(right.object()));
    sorted
}

fn proof_context(
    request: &LifecycleTableObjectRetentionRequest,
    live_tables: &BTreeMap<ObjectName, LiveTableReason>,
) -> LifecycleTableObjectProofContext {
    LifecycleTableObjectProofContext {
        branch_id: request.branch_id,
        epochs: request.epochs,
        fingerprint: reachability_fingerprint(request, live_tables),
    }
}

fn reachability_fingerprint(
    request: &LifecycleTableObjectRetentionRequest,
    live_tables: &BTreeMap<ObjectName, LiveTableReason>,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hash_branch(&mut hasher, request.branch_id);
    hash_u64(&mut hasher, request.epochs.manifest_epoch());
    hash_u64(&mut hasher, request.epochs.table_inventory_epoch());
    hash_u64(&mut hasher, request.epochs.quarantine_inventory_epoch());
    hash_u64(&mut hasher, request.epochs.recovery_health_epoch());
    hash_bool(&mut hasher, request.completeness.manifest);
    hash_bool(&mut hasher, request.completeness.table_inventory);
    hash_bool(&mut hasher, request.completeness.quarantine_inventory);
    hash_recovery_health(&mut hasher, &request.recovery_health);
    for (object, reason) in live_tables {
        hash_str(&mut hasher, object.as_str());
        hash_live_table_reason(&mut hasher, *reason);
    }
    for entry in sorted_inventory(&request.inventory) {
        hash_str(&mut hasher, entry.object().as_str());
        hash_u64(&mut hasher, entry.byte_count());
    }
    let mut quarantined = request.quarantined_objects.clone();
    quarantined.sort();
    for object in quarantined {
        hash_str(&mut hasher, object.as_str());
    }
    hasher.finalize().into()
}

fn hash_recovery_health(hasher: &mut Sha256, health: &RecoveryHealth) {
    match health {
        RecoveryHealth::Healthy => {
            hasher.update([0x01, 0xfb]);
        }
        RecoveryHealth::Degraded { class, faults } => {
            hasher.update([0x02, 0xfb]);
            hasher.update([recovery_degradation_class_tag(*class), 0xfa]);
            for fault in faults {
                hash_recovery_fault(hasher, fault);
            }
            // length terminator so two fault lists of different lengths
            // hashing to the same prefix still diverge
            hash_u64(hasher, faults.len() as u64);
        }
        RecoveryHealth::Failed { fault } => {
            hasher.update([0x03, 0xfb]);
            hash_recovery_fault(hasher, fault);
        }
    }
}

fn hash_recovery_fault(hasher: &mut Sha256, fault: &RecoveryFault) {
    hasher.update([recovery_fault_kind_tag(fault.kind()), 0xf9]);
    hash_str(hasher, fault.reason());
    match fault.affected_branch() {
        Some(branch) => {
            hasher.update([0x01, 0xf8]);
            hash_branch(hasher, branch);
        }
        None => {
            hasher.update([0x00, 0xf8]);
        }
    }
}

const fn recovery_degradation_class_tag(class: RecoveryDegradationClass) -> u8 {
    match class {
        RecoveryDegradationClass::DataLoss => 0x01,
        RecoveryDegradationClass::PolicyDowngrade => 0x02,
        RecoveryDegradationClass::Telemetry => 0x03,
    }
}

const fn recovery_fault_kind_tag(kind: RecoveryFaultKind) -> u8 {
    match kind {
        RecoveryFaultKind::CorruptManifest => 0x01,
        RecoveryFaultKind::CorruptSnapshot => 0x02,
        RecoveryFaultKind::CorruptWal => 0x03,
        RecoveryFaultKind::MissingManifestObject => 0x04,
        RecoveryFaultKind::MissingSnapshotObject => 0x05,
        RecoveryFaultKind::MissingTableObject => 0x06,
        RecoveryFaultKind::InheritedLayerLoss => 0x07,
        RecoveryFaultKind::NoManifestFallback => 0x08,
        RecoveryFaultKind::IoFailure => 0x09,
        RecoveryFaultKind::QuarantineInventoryMismatch => 0x0a,
        RecoveryFaultKind::TimelineMismatch => 0x0b,
        RecoveryFaultKind::WalTailRepairFailed => 0x0c,
        RecoveryFaultKind::MissingTableManifestBase => 0x0d,
        RecoveryFaultKind::WalCommittedSuffixMissing => 0x0e,
    }
}

fn hash_live_table_reason(hasher: &mut Sha256, reason: LiveTableReason) {
    let tag = match reason {
        LiveTableReason::Own => 0x01_u8,
        LiveTableReason::Inherited => 0x02,
        LiveTableReason::Materialized => 0x03,
        LiveTableReason::Shared => 0x04,
    };
    hasher.update([tag, 0xf7]);
}

fn hash_branch(hasher: &mut Sha256, branch_id: BranchId) {
    hasher.update(branch_id.as_bytes());
    hasher.update([0xff]);
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
    hasher.update([0xfe]);
}

fn hash_bool(hasher: &mut Sha256, value: bool) {
    hasher.update([u8::from(value), 0xfd]);
}

fn hash_str(hasher: &mut Sha256, value: &str) {
    hasher.update(value.as_bytes());
    hasher.update([0xfc]);
}

fn recovery_health_blocks_table_retention(health: &RecoveryHealth, allow_telemetry: bool) -> bool {
    match health {
        RecoveryHealth::Healthy => false,
        RecoveryHealth::Degraded { class, .. } => match class {
            RecoveryDegradationClass::Telemetry => !allow_telemetry,
            RecoveryDegradationClass::PolicyDowngrade | RecoveryDegradationClass::DataLoss => true,
        },
        RecoveryHealth::Failed { .. } => true,
    }
}

fn is_non_table_object(object: &ObjectName) -> bool {
    matches!(ObjectLayout::classify_table_object(object), Ok(None))
}

fn is_table_manifest_object(object: &ObjectName) -> bool {
    matches!(
        ObjectLayout::classify_table_object(object),
        Ok(Some(TableObjectClassification::Manifest { .. }))
    )
}

fn is_table_data_object(object: &ObjectName) -> bool {
    matches!(
        ObjectLayout::classify_table_object(object),
        Ok(Some(TableObjectClassification::Data { .. }))
    )
}

pub(crate) fn table_object_retention_health_debt(
    outcome: &LifecycleTableObjectRetentionOutcome,
) -> LifecycleResult<Option<RecoveryHealth>> {
    match outcome.status() {
        LifecycleRetentionStatus::CompletedWithHealthDebt
        | LifecycleRetentionStatus::DeferredIncompleteProof => Ok(Some(telemetry_health_debt(
            "table reachability proof is incomplete",
        )?)),
        LifecycleRetentionStatus::BlockedByRecoveryHealth => Ok(outcome.recovery_health().cloned()),
        LifecycleRetentionStatus::Completed
        | LifecycleRetentionStatus::DeferredUnsupportedScope => Ok(None),
    }
}

#[cfg(test)]
mod fault_tag_tests {
    use super::recovery_fault_kind_tag;
    use crate::lifecycle::RecoveryFaultKind;

    /// Reachability hashes fold fault kinds by tag: every kind must map to a
    /// distinct, nonzero tag or two different faults hash identically.
    #[test]
    fn recovery_fault_kind_tags_are_distinct_and_nonzero() {
        let kinds = [
            RecoveryFaultKind::CorruptManifest,
            RecoveryFaultKind::CorruptSnapshot,
            RecoveryFaultKind::CorruptWal,
            RecoveryFaultKind::MissingManifestObject,
            RecoveryFaultKind::MissingSnapshotObject,
            RecoveryFaultKind::MissingTableObject,
            RecoveryFaultKind::MissingTableManifestBase,
            RecoveryFaultKind::InheritedLayerLoss,
            RecoveryFaultKind::NoManifestFallback,
            RecoveryFaultKind::IoFailure,
            RecoveryFaultKind::QuarantineInventoryMismatch,
            RecoveryFaultKind::TimelineMismatch,
            RecoveryFaultKind::WalTailRepairFailed,
            RecoveryFaultKind::WalCommittedSuffixMissing,
        ];
        let mut seen = std::collections::BTreeSet::new();
        for kind in kinds {
            let tag = recovery_fault_kind_tag(kind);
            assert_ne!(tag, 0, "{kind:?} must have a nonzero tag");
            assert!(seen.insert(tag), "{kind:?} tag {tag:#04x} collides");
        }
    }
}
