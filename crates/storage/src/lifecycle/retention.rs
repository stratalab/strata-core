//! Retention proof and snapshot pruning coordination.

#![allow(
    dead_code,
    reason = "retention proof hooks are consumed by maintenance dispatch and tests"
)]

use super::{
    telemetry_health_debt, LifecycleError, LifecycleLowerLayer, LifecycleResult, LifecycleStats,
    MaintenanceDeferralReason, MaintenanceOutcome, MaintenanceOutcomeStatus,
    MaintenanceRetentionOptions, MaintenanceTask, MaintenanceTaskKind, RecoveryDegradationClass,
    RecoveryFault, RecoveryFaultKind, RecoveryHealth, RetentionDecision,
};
use crate::format::DatabaseManifest;
use crate::object::ObjectName;
use crate::service::{
    SnapshotDeleteFailure, SnapshotDeleteOutcome, SnapshotDeleteReport, SnapshotObject,
    SnapshotService, SnapshotServiceError,
};
use strata_core::{BranchId, CommitVersion};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleRetentionScope {
    Global,
    SnapshotObjects,
    TableObjects { branch_id: BranchId },
    WalObjects,
    QuarantineObjects,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleRetentionRequest {
    scope: LifecycleRetentionScope,
    retain_newest_snapshots: usize,
    allow_telemetry_degraded_recovery: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleRetentionProof {
    status: LifecycleRetentionProofStatus,
    recovery_health: RecoveryHealth,
    live_snapshot_id: Option<u64>,
    snapshot_watermark: Option<CommitVersion>,
    flush_watermark: Option<CommitVersion>,
    missing_fact: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleRetentionProofStatus {
    Complete,
    Incomplete,
    BlockedByRecoveryHealth,
}

/// The typed deferral a retention proof status carries into a maintenance
/// outcome; `None` when the proof is complete.
pub(crate) const fn proof_deferral_reason(
    status: LifecycleRetentionProofStatus,
) -> Option<MaintenanceDeferralReason> {
    match status {
        LifecycleRetentionProofStatus::Incomplete => {
            Some(MaintenanceDeferralReason::IncompleteProof)
        }
        LifecycleRetentionProofStatus::BlockedByRecoveryHealth => {
            Some(MaintenanceDeferralReason::RecoveryHealth)
        }
        LifecycleRetentionProofStatus::Complete => None,
    }
}

/// The typed deferral a retention status carries into its maintenance
/// outcome; `None` for the completed statuses.
pub(crate) const fn retention_deferral_reason(
    status: LifecycleRetentionStatus,
) -> Option<MaintenanceDeferralReason> {
    match status {
        LifecycleRetentionStatus::DeferredIncompleteProof => {
            Some(MaintenanceDeferralReason::IncompleteProof)
        }
        LifecycleRetentionStatus::DeferredUnsupportedScope => {
            Some(MaintenanceDeferralReason::UnsupportedScope)
        }
        LifecycleRetentionStatus::BlockedByRecoveryHealth => {
            Some(MaintenanceDeferralReason::RecoveryHealth)
        }
        LifecycleRetentionStatus::Completed | LifecycleRetentionStatus::CompletedWithHealthDebt => {
            None
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleRetentionDecisionRecord {
    object: Option<ObjectName>,
    family: LifecycleRetentionObjectFamily,
    decision: RetentionDecision,
    reason: LifecycleRetentionDecisionReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleRetentionObjectFamily {
    Snapshot,
    Table,
    Wal,
    Quarantine,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleRetentionDecisionReason {
    LiveManifestSnapshot,
    NewestSnapshotWindow,
    SnapshotPruneCandidate,
    ReachableTable,
    ReachableInheritedTable,
    ReachableMaterializedTable,
    ReachableSharedTable,
    TableRequiresQuarantine,
    TableAlreadyQuarantined,
    MalformedTableObject,
    ProofIncomplete,
    UnsafeRecoveryHealth,
    DelegatedToWalTruncation,
    DelegatedToQuarantine,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleRetentionOutcome {
    status: LifecycleRetentionStatus,
    proof: LifecycleRetentionProof,
    decisions: Vec<LifecycleRetentionDecisionRecord>,
    objects_pruned: usize,
    objects_retained: usize,
    objects_skipped: usize,
    reclaimed_bytes: u64,
    recovery_health: Option<RecoveryHealth>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleRetentionStatus {
    Completed,
    CompletedWithHealthDebt,
    DeferredIncompleteProof,
    DeferredUnsupportedScope,
    BlockedByRecoveryHealth,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleSnapshotPruningRequest {
    live_snapshot_id: Option<u64>,
    retain_newest: usize,
    proof: LifecycleRetentionProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleSnapshotPruningOutcome {
    proof_status: LifecycleRetentionProofStatus,
    deleted: Vec<SnapshotObject>,
    delete_outcomes: Vec<SnapshotDeleteOutcome>,
    protected: Vec<SnapshotObject>,
    failed: Vec<SnapshotDeleteFailure>,
    recovery_health: Option<RecoveryHealth>,
}

impl LifecycleRetentionRequest {
    pub(crate) fn new(scope: LifecycleRetentionScope, retain_newest_snapshots: usize) -> Self {
        let request = Self {
            scope,
            retain_newest_snapshots,
            allow_telemetry_degraded_recovery: true,
        };
        request.validate();
        request
    }

    pub(crate) fn snapshot_pruning(retain_newest_snapshots: usize) -> Self {
        Self::new(
            LifecycleRetentionScope::SnapshotObjects,
            retain_newest_snapshots,
        )
    }

    pub(crate) fn global(retain_newest_snapshots: usize) -> Self {
        Self::new(LifecycleRetentionScope::Global, retain_newest_snapshots)
    }

    pub(crate) const fn with_telemetry_degraded_recovery_allowed(mut self, allowed: bool) -> Self {
        self.allow_telemetry_degraded_recovery = allowed;
        self
    }

    pub(crate) const fn scope(&self) -> LifecycleRetentionScope {
        self.scope
    }

    pub(crate) const fn retain_newest_snapshots(&self) -> usize {
        self.retain_newest_snapshots
    }

    pub(crate) const fn effective_retain_newest_snapshots(&self) -> usize {
        if self.retain_newest_snapshots == 0 {
            1
        } else {
            self.retain_newest_snapshots
        }
    }

    pub(crate) const fn allow_telemetry_degraded_recovery(&self) -> bool {
        self.allow_telemetry_degraded_recovery
    }

    fn validate(&self) {
        match self.scope {
            LifecycleRetentionScope::TableObjects { .. }
            | LifecycleRetentionScope::WalObjects
            | LifecycleRetentionScope::QuarantineObjects
            | LifecycleRetentionScope::SnapshotObjects
            | LifecycleRetentionScope::Global => {}
        }
    }
}

pub(crate) fn reject_implicit_snapshot_floor_advancement(
    _current_floor: Option<CommitVersion>,
    _requested_floor: CommitVersion,
) -> LifecycleResult<()> {
    crate::observability::perf_trace::record_lifecycle_snapshot_floor_implicit_rejection();
    Err(LifecycleError::RetentionBlocked {
        reason: "snapshot floor advancement requires caller-supplied retention proof",
    })
}

impl LifecycleRetentionProof {
    pub(crate) const fn new(
        status: LifecycleRetentionProofStatus,
        recovery_health: RecoveryHealth,
        live_snapshot_id: Option<u64>,
        snapshot_watermark: Option<CommitVersion>,
        flush_watermark: Option<CommitVersion>,
        missing_fact: Option<&'static str>,
    ) -> Self {
        Self {
            status,
            recovery_health,
            live_snapshot_id,
            snapshot_watermark,
            flush_watermark,
            missing_fact,
        }
    }

    pub(crate) const fn status(&self) -> LifecycleRetentionProofStatus {
        self.status
    }

    pub(crate) const fn recovery_health(&self) -> &RecoveryHealth {
        &self.recovery_health
    }

    pub(crate) const fn live_snapshot_id(&self) -> Option<u64> {
        self.live_snapshot_id
    }

    pub(crate) const fn snapshot_watermark(&self) -> Option<CommitVersion> {
        self.snapshot_watermark
    }

    pub(crate) const fn flush_watermark(&self) -> Option<CommitVersion> {
        self.flush_watermark
    }

    pub(crate) const fn missing_fact(&self) -> Option<&'static str> {
        self.missing_fact
    }

    pub(crate) const fn is_complete(&self) -> bool {
        matches!(self.status, LifecycleRetentionProofStatus::Complete)
    }
}

impl LifecycleRetentionDecisionRecord {
    pub(crate) const fn new(
        object: Option<ObjectName>,
        family: LifecycleRetentionObjectFamily,
        decision: RetentionDecision,
        reason: LifecycleRetentionDecisionReason,
    ) -> Self {
        Self {
            object,
            family,
            decision,
            reason,
        }
    }

    pub(crate) const fn snapshot(
        object: ObjectName,
        decision: RetentionDecision,
        reason: LifecycleRetentionDecisionReason,
    ) -> Self {
        Self::new(
            Some(object),
            LifecycleRetentionObjectFamily::Snapshot,
            decision,
            reason,
        )
    }

    pub(crate) const fn table(
        object: ObjectName,
        decision: RetentionDecision,
        reason: LifecycleRetentionDecisionReason,
    ) -> Self {
        Self::new(
            Some(object),
            LifecycleRetentionObjectFamily::Table,
            decision,
            reason,
        )
    }

    pub(crate) const fn delegated(
        family: LifecycleRetentionObjectFamily,
        reason: LifecycleRetentionDecisionReason,
    ) -> Self {
        Self::new(None, family, RetentionDecision::SkipUntilProof, reason)
    }

    pub(crate) const fn object(&self) -> Option<&ObjectName> {
        self.object.as_ref()
    }

    pub(crate) const fn family(&self) -> LifecycleRetentionObjectFamily {
        self.family
    }

    pub(crate) const fn decision(&self) -> RetentionDecision {
        self.decision
    }

    pub(crate) const fn reason(&self) -> LifecycleRetentionDecisionReason {
        self.reason
    }
}

impl LifecycleRetentionOutcome {
    pub(crate) const fn deferred_unsupported_scope(proof: LifecycleRetentionProof) -> Self {
        Self {
            status: LifecycleRetentionStatus::DeferredUnsupportedScope,
            proof,
            decisions: Vec::new(),
            objects_pruned: 0,
            objects_retained: 0,
            objects_skipped: 0,
            reclaimed_bytes: 0,
            recovery_health: None,
        }
    }

    pub(crate) fn from_decisions(
        proof: LifecycleRetentionProof,
        mut decisions: Vec<LifecycleRetentionDecisionRecord>,
        reclaimed_bytes: u64,
    ) -> LifecycleResult<Self> {
        decisions.sort_by(decision_sort_key);
        let objects_pruned = decisions
            .iter()
            .filter(|decision| decision.decision() == RetentionDecision::PruneCandidate)
            .count();
        let objects_retained = decisions
            .iter()
            .filter(|decision| decision.decision() == RetentionDecision::Retain)
            .count();
        let objects_skipped = decisions
            .iter()
            .filter(|decision| decision.decision() == RetentionDecision::SkipUntilProof)
            .count();
        let status = status_for_proof(&proof);
        let recovery_health = match status {
            LifecycleRetentionStatus::Completed
            | LifecycleRetentionStatus::DeferredUnsupportedScope => None,
            LifecycleRetentionStatus::CompletedWithHealthDebt
            | LifecycleRetentionStatus::DeferredIncompleteProof => {
                Some(telemetry_health_debt("retention proof is incomplete")?)
            }
            LifecycleRetentionStatus::BlockedByRecoveryHealth => {
                Some(proof.recovery_health.clone())
            }
        };
        Ok(Self {
            status,
            proof,
            decisions,
            objects_pruned,
            objects_retained,
            objects_skipped,
            reclaimed_bytes,
            recovery_health,
        })
    }

    pub(crate) const fn status(&self) -> LifecycleRetentionStatus {
        self.status
    }

    pub(crate) const fn proof(&self) -> &LifecycleRetentionProof {
        &self.proof
    }

    pub(crate) fn decisions(&self) -> &[LifecycleRetentionDecisionRecord] {
        &self.decisions
    }

    pub(crate) const fn objects_pruned(&self) -> usize {
        self.objects_pruned
    }

    pub(crate) const fn objects_retained(&self) -> usize {
        self.objects_retained
    }

    pub(crate) const fn objects_skipped(&self) -> usize {
        self.objects_skipped
    }

    pub(crate) const fn reclaimed_bytes(&self) -> u64 {
        self.reclaimed_bytes
    }

    pub(crate) const fn recovery_health(&self) -> Option<&RecoveryHealth> {
        self.recovery_health.as_ref()
    }

    pub(crate) fn maintenance_outcome(&self) -> MaintenanceOutcome {
        let status = match self.status {
            LifecycleRetentionStatus::Completed
            | LifecycleRetentionStatus::CompletedWithHealthDebt => {
                MaintenanceOutcomeStatus::Completed
            }
            LifecycleRetentionStatus::DeferredIncompleteProof
            | LifecycleRetentionStatus::DeferredUnsupportedScope
            | LifecycleRetentionStatus::BlockedByRecoveryHealth => {
                MaintenanceOutcomeStatus::Deferred
            }
        };
        let mut outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Retention, status)
            .with_effects(self.decisions.len(), self.reclaimed_bytes, false)
            .with_affected_object_names(object_names(&self.decisions))
            .with_state_changes(self.objects_pruned)
            .with_stats(LifecycleStats::new(
                0,
                self.recovery_health
                    .as_ref()
                    .map_or(0, RecoveryHealth::fault_count),
                1,
                usize::from(status != MaintenanceOutcomeStatus::Completed),
                0,
            ));
        if let Some(health) = self.recovery_health.clone() {
            outcome = outcome.with_recovery_health(health);
        }
        if let Some(deferral) = retention_deferral_reason(self.status) {
            outcome = outcome.with_deferral_reason(deferral);
        }
        match self.status {
            LifecycleRetentionStatus::DeferredIncompleteProof => {
                outcome.with_reason("retention proof is incomplete")
            }
            LifecycleRetentionStatus::DeferredUnsupportedScope => {
                outcome.with_reason("retention scope not supported by generic path")
            }
            LifecycleRetentionStatus::BlockedByRecoveryHealth => {
                outcome.with_reason("recovery health blocks retention")
            }
            LifecycleRetentionStatus::Completed
            | LifecycleRetentionStatus::CompletedWithHealthDebt => outcome,
        }
    }
}

impl LifecycleSnapshotPruningRequest {
    pub(crate) fn new(
        proof: LifecycleRetentionProof,
        retain_newest: usize,
    ) -> LifecycleResult<Self> {
        let request = Self {
            live_snapshot_id: proof.live_snapshot_id(),
            retain_newest,
            proof,
        };
        request.validate()?;
        Ok(request)
    }

    fn validate(&self) -> LifecycleResult<()> {
        if self.live_snapshot_id == Some(0) {
            return Err(LifecycleError::InvalidConfig {
                field: "live_snapshot_id",
                reason: "live snapshot id must be nonzero",
            });
        }
        if self.proof.is_complete()
            && (self.live_snapshot_id.is_none() || self.proof.snapshot_watermark().is_none())
        {
            return Err(LifecycleError::RetentionBlocked {
                reason: "snapshot pruning requires manifest snapshot proof",
            });
        }
        Ok(())
    }

    pub(crate) const fn live_snapshot_id(&self) -> Option<u64> {
        self.live_snapshot_id
    }

    pub(crate) const fn retain_newest(&self) -> usize {
        self.retain_newest
    }

    pub(crate) const fn effective_retain_newest(&self) -> usize {
        if self.retain_newest == 0 {
            1
        } else {
            self.retain_newest
        }
    }

    pub(crate) const fn proof(&self) -> &LifecycleRetentionProof {
        &self.proof
    }
}

impl LifecycleSnapshotPruningOutcome {
    fn from_completed_report(report: &SnapshotDeleteReport) -> LifecycleResult<Self> {
        let deleted = report.deleted().to_vec();
        let delete_outcomes = report.delete_outcomes().to_vec();
        let protected = report.protected().to_vec();
        let failed = report.failed().to_vec();
        let recovery_health = if failed.is_empty() {
            None
        } else {
            // Emit one fault per failed deletion so `fault_count` reflects
            // the real number of stuck snapshots. The per-failure object
            // identity and backend source error remain on the typed
            // `failed: Vec<SnapshotDeleteFailure>` field for callers that
            // need to surface object-level diagnostics; the health debt's
            // count provides at-a-glance signal that retention has more
            // than one stranded snapshot to address.
            let faults: Vec<RecoveryFault> = (0..failed.len())
                .map(|_| {
                    RecoveryFault::new(
                        RecoveryFaultKind::IoFailure,
                        "snapshot pruning delete failure",
                    )
                })
                .collect::<LifecycleResult<_>>()?;
            Some(RecoveryHealth::degraded(
                RecoveryDegradationClass::Telemetry,
                faults,
            )?)
        };
        Ok(Self {
            proof_status: LifecycleRetentionProofStatus::Complete,
            deleted,
            delete_outcomes,
            protected,
            failed,
            recovery_health,
        })
    }

    fn deferred(
        proof_status: LifecycleRetentionProofStatus,
        recovery_health: Option<RecoveryHealth>,
    ) -> Self {
        Self {
            proof_status,
            deleted: Vec::new(),
            delete_outcomes: Vec::new(),
            protected: Vec::new(),
            failed: Vec::new(),
            recovery_health,
        }
    }

    pub(crate) const fn completed(&self) -> bool {
        matches!(self.proof_status, LifecycleRetentionProofStatus::Complete)
    }

    pub(crate) const fn completed_noop(&self) -> bool {
        self.completed() && self.deleted.is_empty() && self.failed.is_empty()
    }

    pub(crate) const fn completed_with_health_debt(&self) -> bool {
        self.completed() && !self.failed.is_empty()
    }

    pub(crate) const fn deferred_incomplete_proof(&self) -> bool {
        matches!(self.proof_status, LifecycleRetentionProofStatus::Incomplete)
    }

    pub(crate) const fn blocked_by_recovery_health(&self) -> bool {
        matches!(
            self.proof_status,
            LifecycleRetentionProofStatus::BlockedByRecoveryHealth
        )
    }

    pub(crate) fn deleted(&self) -> &[SnapshotObject] {
        &self.deleted
    }

    pub(crate) fn delete_outcomes(&self) -> &[SnapshotDeleteOutcome] {
        &self.delete_outcomes
    }

    pub(crate) fn protected(&self) -> &[SnapshotObject] {
        &self.protected
    }

    pub(crate) fn failed(&self) -> &[SnapshotDeleteFailure] {
        &self.failed
    }

    pub(crate) const fn recovery_health(&self) -> Option<&RecoveryHealth> {
        self.recovery_health.as_ref()
    }

    pub(crate) fn maintenance_outcome(&self) -> MaintenanceOutcome {
        let status = if self.completed() {
            MaintenanceOutcomeStatus::Completed
        } else {
            MaintenanceOutcomeStatus::Deferred
        };
        let names = snapshot_object_names(&self.deleted, &self.protected, &self.failed);
        let mut outcome = MaintenanceOutcome::new(MaintenanceTaskKind::SnapshotPruning, status)
            .with_effects(names.len(), 0, false)
            .with_affected_object_names(names)
            .with_state_changes(self.deleted.len())
            .with_stats(LifecycleStats::new(
                0,
                self.recovery_health
                    .as_ref()
                    .map_or(0, RecoveryHealth::fault_count),
                1,
                usize::from(!self.completed()),
                0,
            ));
        if let Some(health) = self.recovery_health.clone() {
            outcome = outcome.with_recovery_health(health);
        }
        if let Some(deferral) = proof_deferral_reason(self.proof_status) {
            outcome = outcome.with_deferral_reason(deferral);
        }
        match self.proof_status {
            LifecycleRetentionProofStatus::Incomplete => {
                outcome.with_reason("retention proof is incomplete")
            }
            LifecycleRetentionProofStatus::BlockedByRecoveryHealth => {
                outcome.with_reason("recovery health blocks retention")
            }
            LifecycleRetentionProofStatus::Complete => outcome,
        }
    }
}

pub(crate) fn build_retention_proof(
    request: &LifecycleRetentionRequest,
    manifest: Option<&DatabaseManifest>,
    recovery_health: &RecoveryHealth,
    snapshot_objects: usize,
) -> LifecycleRetentionProof {
    build_retention_proof_from_facts(
        request,
        manifest.and_then(DatabaseManifest::snapshot_id),
        manifest.and_then(DatabaseManifest::snapshot_watermark),
        manifest.and_then(DatabaseManifest::flushed_through_commit_id),
        recovery_health,
        snapshot_objects,
    )
}

pub(crate) fn build_retention_proof_from_facts(
    request: &LifecycleRetentionRequest,
    live_snapshot_id: Option<u64>,
    raw_snapshot_watermark: Option<u64>,
    flush_watermark: Option<CommitVersion>,
    recovery_health: &RecoveryHealth,
    _snapshot_objects: usize,
) -> LifecycleRetentionProof {
    if recovery_health_blocks_retention(recovery_health, request) {
        return LifecycleRetentionProof::new(
            LifecycleRetentionProofStatus::BlockedByRecoveryHealth,
            recovery_health.clone(),
            live_snapshot_id,
            raw_snapshot_watermark.map(CommitVersion::new),
            flush_watermark,
            Some("recovery_health"),
        );
    }

    let snapshot_watermark = raw_snapshot_watermark.map(CommitVersion::new);
    let missing_fact = match request.scope() {
        LifecycleRetentionScope::SnapshotObjects | LifecycleRetentionScope::Global
            if live_snapshot_id.is_none() || snapshot_watermark.is_none() =>
        {
            Some("manifest_snapshot")
        }
        LifecycleRetentionScope::TableObjects { .. } => Some("table_reachability"),
        LifecycleRetentionScope::WalObjects => Some("wal_retention_proof"),
        LifecycleRetentionScope::QuarantineObjects => Some("quarantine_inventory"),
        _ => None,
    };
    let status = if missing_fact.is_some() {
        LifecycleRetentionProofStatus::Incomplete
    } else {
        LifecycleRetentionProofStatus::Complete
    };
    LifecycleRetentionProof::new(
        status,
        recovery_health.clone(),
        live_snapshot_id,
        snapshot_watermark,
        flush_watermark,
        missing_fact,
    )
}

pub(crate) fn retention_outcome_for_scope(
    request: &LifecycleRetentionRequest,
    proof: LifecycleRetentionProof,
    snapshots: &[SnapshotObject],
) -> LifecycleResult<LifecycleRetentionOutcome> {
    let mut decisions = Vec::new();
    match request.scope() {
        LifecycleRetentionScope::Global => {
            decisions.extend(snapshot_retention_decisions(
                &proof,
                snapshots,
                request.retain_newest_snapshots(),
            ));
            decisions.extend(delegated_family_decisions([
                (
                    LifecycleRetentionObjectFamily::Wal,
                    LifecycleRetentionDecisionReason::DelegatedToWalTruncation,
                ),
                (
                    LifecycleRetentionObjectFamily::Quarantine,
                    LifecycleRetentionDecisionReason::DelegatedToQuarantine,
                ),
            ]));
        }
        LifecycleRetentionScope::SnapshotObjects => {
            decisions.extend(snapshot_retention_decisions(
                &proof,
                snapshots,
                request.retain_newest_snapshots(),
            ));
        }
        LifecycleRetentionScope::WalObjects => {
            decisions.extend(delegated_family_decisions([(
                LifecycleRetentionObjectFamily::Wal,
                LifecycleRetentionDecisionReason::DelegatedToWalTruncation,
            )]));
        }
        LifecycleRetentionScope::QuarantineObjects => {
            decisions.extend(delegated_family_decisions([(
                LifecycleRetentionObjectFamily::Quarantine,
                LifecycleRetentionDecisionReason::DelegatedToQuarantine,
            )]));
        }
        LifecycleRetentionScope::TableObjects { .. } => {
            // Table-object retention requires branch reachability facts
            // that retention does not own; the scope is deliberately
            // unsupported by retention regardless of proof completeness.
            // Returning `DeferredUnsupportedScope` for both complete and
            // incomplete proofs prevents callers from inferring two
            // different "deferred" reasons for the same unsupported
            // scope based on proof state.
            return Ok(LifecycleRetentionOutcome::deferred_unsupported_scope(proof));
        }
    }
    LifecycleRetentionOutcome::from_decisions(proof, decisions, 0)
}

pub(crate) fn prune_snapshots_with_proof(
    snapshots: &SnapshotService<'_>,
    request: &LifecycleSnapshotPruningRequest,
) -> LifecycleResult<LifecycleSnapshotPruningOutcome> {
    match request.proof().status() {
        LifecycleRetentionProofStatus::Complete => {}
        LifecycleRetentionProofStatus::Incomplete => {
            return Ok(LifecycleSnapshotPruningOutcome::deferred(
                LifecycleRetentionProofStatus::Incomplete,
                Some(telemetry_health_debt("retention proof is incomplete")?),
            ));
        }
        LifecycleRetentionProofStatus::BlockedByRecoveryHealth => {
            return Ok(LifecycleSnapshotPruningOutcome::deferred(
                LifecycleRetentionProofStatus::BlockedByRecoveryHealth,
                Some(request.proof().recovery_health().clone()),
            ));
        }
    }
    let report = snapshots
        .prune_snapshots(
            request.live_snapshot_id(),
            request.effective_retain_newest(),
        )
        .map_err(snapshot_error)?;
    let outcome = LifecycleSnapshotPruningOutcome::from_completed_report(&report)?;
    crate::observability::perf_trace::record_lifecycle_snapshot_pruning_with_proof(
        outcome.deleted().len(),
        outcome.protected().len(),
        outcome.failed().len(),
    );
    Ok(outcome)
}

pub(crate) fn retention_request_from_maintenance_task(
    task: &MaintenanceTask,
) -> LifecycleResult<LifecycleRetentionRequest> {
    let options = task
        .retention_options()
        .unwrap_or(MaintenanceRetentionOptions::new(1));
    match task.kind() {
        MaintenanceTaskKind::SnapshotPruning => Ok(LifecycleRetentionRequest::snapshot_pruning(
            options.retain_newest_snapshots(),
        )),
        MaintenanceTaskKind::Retention => match task.scope() {
            crate::lifecycle::MaintenanceTaskScope::Branch(branch_id) => {
                Ok(LifecycleRetentionRequest::new(
                    LifecycleRetentionScope::TableObjects { branch_id },
                    options.retain_newest_snapshots(),
                ))
            }
            crate::lifecycle::MaintenanceTaskScope::Retention => Ok(
                LifecycleRetentionRequest::global(options.retain_newest_snapshots()),
            ),
            _ => Err(LifecycleError::MaintenanceTaskFailed {
                reason: "retention task scope is invalid",
            }),
        },
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "retention request requires retention task",
        }),
    }
}

pub(crate) fn retention_outcome_for_delegated_families(
    proof: LifecycleRetentionProof,
) -> LifecycleResult<LifecycleRetentionOutcome> {
    LifecycleRetentionOutcome::from_decisions(
        proof,
        delegated_family_decisions([
            (
                LifecycleRetentionObjectFamily::Wal,
                LifecycleRetentionDecisionReason::DelegatedToWalTruncation,
            ),
            (
                LifecycleRetentionObjectFamily::Quarantine,
                LifecycleRetentionDecisionReason::DelegatedToQuarantine,
            ),
        ]),
        0,
    )
}

pub(crate) fn table_quarantine_candidate(object: ObjectName) -> LifecycleRetentionDecisionRecord {
    LifecycleRetentionDecisionRecord::table(
        object,
        RetentionDecision::QuarantineCandidate,
        LifecycleRetentionDecisionReason::TableRequiresQuarantine,
    )
}

fn status_for_proof(proof: &LifecycleRetentionProof) -> LifecycleRetentionStatus {
    match proof.status() {
        LifecycleRetentionProofStatus::Complete => LifecycleRetentionStatus::Completed,
        LifecycleRetentionProofStatus::Incomplete => {
            LifecycleRetentionStatus::DeferredIncompleteProof
        }
        LifecycleRetentionProofStatus::BlockedByRecoveryHealth => {
            LifecycleRetentionStatus::BlockedByRecoveryHealth
        }
    }
}

fn recovery_health_blocks_retention(
    health: &RecoveryHealth,
    request: &LifecycleRetentionRequest,
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

fn snapshot_retention_decisions(
    proof: &LifecycleRetentionProof,
    snapshots: &[SnapshotObject],
    retain_newest: usize,
) -> Vec<LifecycleRetentionDecisionRecord> {
    if !proof.is_complete() {
        return Vec::new();
    }
    let mut snapshots = snapshots.to_vec();
    snapshots.sort_by_key(SnapshotObject::snapshot_id);
    let retain_newest = retain_newest.max(1);
    let retain_start = snapshots.len().saturating_sub(retain_newest);
    snapshots
        .into_iter()
        .enumerate()
        .map(|(index, snapshot)| {
            let live = proof.live_snapshot_id() == Some(snapshot.snapshot_id());
            let newest_retained = index >= retain_start;
            let (decision, reason) = if live {
                (
                    RetentionDecision::Retain,
                    LifecycleRetentionDecisionReason::LiveManifestSnapshot,
                )
            } else if newest_retained {
                (
                    RetentionDecision::Retain,
                    LifecycleRetentionDecisionReason::NewestSnapshotWindow,
                )
            } else {
                (
                    RetentionDecision::PruneCandidate,
                    LifecycleRetentionDecisionReason::SnapshotPruneCandidate,
                )
            };
            LifecycleRetentionDecisionRecord::snapshot(snapshot.object().clone(), decision, reason)
        })
        .collect()
}

fn delegated_family_decisions<const N: usize>(
    families: [(
        LifecycleRetentionObjectFamily,
        LifecycleRetentionDecisionReason,
    ); N],
) -> Vec<LifecycleRetentionDecisionRecord> {
    families
        .into_iter()
        .map(|(family, reason)| LifecycleRetentionDecisionRecord::delegated(family, reason))
        .collect()
}

fn decision_sort_key(
    left: &LifecycleRetentionDecisionRecord,
    right: &LifecycleRetentionDecisionRecord,
) -> std::cmp::Ordering {
    (
        family_rank(left.family()),
        left.object().map(ObjectName::as_str),
        decision_rank(left.decision()),
    )
        .cmp(&(
            family_rank(right.family()),
            right.object().map(ObjectName::as_str),
            decision_rank(right.decision()),
        ))
}

const fn family_rank(family: LifecycleRetentionObjectFamily) -> u8 {
    match family {
        LifecycleRetentionObjectFamily::Snapshot => 0,
        LifecycleRetentionObjectFamily::Table => 1,
        LifecycleRetentionObjectFamily::Wal => 2,
        LifecycleRetentionObjectFamily::Quarantine => 3,
    }
}

const fn decision_rank(decision: RetentionDecision) -> u8 {
    match decision {
        RetentionDecision::Retain => 0,
        RetentionDecision::PruneCandidate => 1,
        RetentionDecision::QuarantineCandidate => 2,
        RetentionDecision::PurgeCandidate => 3,
        RetentionDecision::RepairCandidate => 4,
        RetentionDecision::SkipUntilProof => 5,
    }
}

fn object_names(decisions: &[LifecycleRetentionDecisionRecord]) -> Vec<String> {
    decisions
        .iter()
        .filter_map(LifecycleRetentionDecisionRecord::object)
        .map(ToString::to_string)
        .collect()
}

fn snapshot_object_names(
    deleted: &[SnapshotObject],
    protected: &[SnapshotObject],
    failed: &[SnapshotDeleteFailure],
) -> Vec<String> {
    deleted
        .iter()
        .map(|snapshot| snapshot.object().to_string())
        .chain(
            protected
                .iter()
                .map(|snapshot| snapshot.object().to_string()),
        )
        .chain(
            failed
                .iter()
                .map(|failure| failure.snapshot().object().to_string()),
        )
        .collect()
}

fn snapshot_error(error: SnapshotServiceError) -> LifecycleError {
    LifecycleError::lower_layer_with(
        LifecycleLowerLayer::Service,
        "snapshot pruning failed",
        error,
    )
}
