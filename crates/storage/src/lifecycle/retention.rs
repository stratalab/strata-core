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
    reconcilable_orphan, superseded_snapshot, SnapshotDeleteFailure, SnapshotDeleteOutcome,
    SnapshotDeleteReport, SnapshotObject, SnapshotPruneMode, SnapshotService, SnapshotServiceError,
    TimelineSegmentPruneMode, TimelineSegmentPruneReport,
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
    /// Space-reclamation contract §3.4 (slice 5): which snapshot objects the
    /// snapshot family prunes; the newest window unless a proof-driven mode
    /// was requested.
    snapshot_prune_mode: SnapshotPruneMode,
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
    /// Space-reclamation contract §3.4 (slice 5): below the live id after a
    /// completed checkpoint.
    SupersededSnapshot,
    /// Above the live id: possibly a publish in flight, never touched by the
    /// `Superseded` mode.
    AboveLiveSnapshot,
    /// Not the attested id, in the open reclaim window: a crash orphan or a
    /// superseded object, reclaimed by `ReconcileToAttested`.
    NonAttestedSnapshot,
    ReachableTable,
    ReachableInheritedTable,
    ReachableMaterializedTable,
    ReachableSharedTable,
    TableRequiresQuarantine,
    /// Another branch's quarantine inventory names this source: that branch's
    /// sweep owns it (delegate, never a fresh candidate from here).
    TableAlreadyQuarantined,
    /// This branch's quarantine inventory names the source and it is still on
    /// disk — a refused or failed source delete (#3608). A candidate again: the
    /// sweep retries the delete through the existing entry.
    QuarantinedSourceStillPresent,
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
    snapshot_prune_mode: SnapshotPruneMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleSnapshotPruningOutcome {
    proof_status: LifecycleRetentionProofStatus,
    deleted: Vec<SnapshotObject>,
    delete_outcomes: Vec<SnapshotDeleteOutcome>,
    protected: Vec<SnapshotObject>,
    failed: Vec<SnapshotDeleteFailure>,
    /// Bytes the deletes released (#3622), snapshot objects and (#3643)
    /// timeline segments together.
    reclaimed_bytes: u64,
    /// #3643: timeline segments the prune deleted.
    deleted_timeline_segments: usize,
    recovery_health: Option<RecoveryHealth>,
}

impl LifecycleRetentionRequest {
    pub(crate) fn new(scope: LifecycleRetentionScope, retain_newest_snapshots: usize) -> Self {
        let request = Self {
            scope,
            retain_newest_snapshots,
            allow_telemetry_degraded_recovery: true,
            snapshot_prune_mode: SnapshotPruneMode::RetainNewest,
        };
        request.validate();
        request
    }

    /// Space-reclamation contract §3.4 (slice 5): prune the snapshot family
    /// by a proof-driven mode instead of the newest window.
    pub(crate) const fn with_snapshot_prune_mode(mut self, mode: SnapshotPruneMode) -> Self {
        self.snapshot_prune_mode = mode;
        self
    }

    pub(crate) const fn snapshot_prune_mode(&self) -> SnapshotPruneMode {
        self.snapshot_prune_mode
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
    /// The newest-window verb; the mode-aware entry is `for_request`.
    pub(crate) fn new(
        proof: LifecycleRetentionProof,
        retain_newest: usize,
    ) -> LifecycleResult<Self> {
        Self::with_mode(proof, retain_newest, SnapshotPruneMode::RetainNewest)
    }

    /// Space-reclamation contract §3.4 (slice 5): the pruning request a
    /// retention request implies — its newest window AND its prune mode.
    pub(crate) fn for_request(
        proof: LifecycleRetentionProof,
        request: &LifecycleRetentionRequest,
    ) -> LifecycleResult<Self> {
        Self::with_mode(
            proof,
            request.retain_newest_snapshots(),
            request.snapshot_prune_mode(),
        )
    }

    fn with_mode(
        proof: LifecycleRetentionProof,
        retain_newest: usize,
        snapshot_prune_mode: SnapshotPruneMode,
    ) -> LifecycleResult<Self> {
        let request = Self {
            live_snapshot_id: proof.live_snapshot_id(),
            retain_newest,
            proof,
            snapshot_prune_mode,
        };
        request.validate()?;
        Ok(request)
    }

    pub(crate) const fn snapshot_prune_mode(&self) -> SnapshotPruneMode {
        self.snapshot_prune_mode
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
        let reclaimed_bytes = report.reclaimed_bytes();
        let recovery_health = pruning_delete_health_debt(failed.len())?;
        Ok(Self {
            proof_status: LifecycleRetentionProofStatus::Complete,
            deleted,
            delete_outcomes,
            protected,
            failed,
            reclaimed_bytes,
            deleted_timeline_segments: 0,
            recovery_health,
        })
    }

    /// #3643: fold a timeline segment prune into this snapshot prune — the
    /// segments are the snapshot family's other object kind. A segment that
    /// failed to delete is health debt exactly like a snapshot that did.
    fn record_timeline_segments(
        &mut self,
        report: &TimelineSegmentPruneReport,
    ) -> LifecycleResult<()> {
        self.deleted_timeline_segments = report.deleted.len();
        self.reclaimed_bytes = self
            .reclaimed_bytes
            .saturating_add(report.reclaimed_bytes());
        self.recovery_health = pruning_delete_health_debt(self.failed.len() + report.failed)?;
        Ok(())
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
            reclaimed_bytes: 0,
            deleted_timeline_segments: 0,
            recovery_health,
        }
    }

    pub(crate) const fn completed(&self) -> bool {
        matches!(self.proof_status, LifecycleRetentionProofStatus::Complete)
    }

    /// Completed having deleted nothing and failed nothing — snapshots and
    /// (#3643) timeline segments alike (a failed delete is health debt).
    pub(crate) const fn completed_noop(&self) -> bool {
        self.completed()
            && self.deleted.is_empty()
            && self.deleted_timeline_segments == 0
            && self.recovery_health.is_none()
    }

    /// Completed with a delete that failed (a snapshot's or, #3643, a timeline
    /// segment's), left as telemetry health debt.
    pub(crate) const fn completed_with_health_debt(&self) -> bool {
        self.completed() && self.recovery_health.is_some()
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
            .with_effects(names.len(), self.reclaimed_bytes, false)
            .with_affected_object_names(names)
            .with_state_changes(self.deleted.len() + self.deleted_timeline_segments)
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

/// #3671: the database manifest a destructive reclaim may act on. A manifest
/// replacement can become visible without its durability being confirmed
/// (`VisibleDurabilityUnconfirmed`: the rename happened, the directory sync
/// did not), and a crash may then restore the previous manifest. Reclaim acts
/// only on a manifest this session has confirmed durable; an unconfirmed one
/// means both the visible and the previous checkpoint are possible recovery
/// states, so nothing either references may be deleted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConfirmedManifest {
    /// Durable: recovery reads exactly this manifest (or finds none).
    Confirmed(Option<DatabaseManifest>),
    /// Visible but not confirmed durable: the confirming publish failed with
    /// this error. Reclaim defers and reports it.
    Unconfirmed(LifecycleError),
}

impl ConfirmedManifest {
    /// The error that left the manifest unconfirmed, for the deferred
    /// reclaim's outcome to report (a backend fault is never absorbed).
    pub(crate) fn unconfirmed_error(&self) -> Option<LifecycleError> {
        match self {
            Self::Confirmed(_) => None,
            Self::Unconfirmed(error) => Some(error.clone()),
        }
    }
}

/// #3671: a reclaim deferred because the manifest could not be confirmed
/// durable reports the backend fault that prevented it.
pub(crate) fn with_unconfirmed_manifest_error(
    outcome: MaintenanceOutcome,
    manifest: &ConfirmedManifest,
) -> MaintenanceOutcome {
    match manifest.unconfirmed_error() {
        Some(error) => outcome.with_source_error(error),
        None => outcome,
    }
}

/// #3671: the retention proof over a manifest's confirmed durability. An
/// unconfirmed manifest proves nothing: the proof is incomplete, so no
/// snapshot, timeline segment or WAL object is deleted on its word.
pub(crate) fn build_retention_proof_for_manifest(
    request: &LifecycleRetentionRequest,
    manifest: &ConfirmedManifest,
    recovery_health: &RecoveryHealth,
    snapshot_objects: usize,
) -> LifecycleRetentionProof {
    match manifest {
        ConfirmedManifest::Confirmed(manifest) => build_retention_proof(
            request,
            manifest.as_ref(),
            recovery_health,
            snapshot_objects,
        ),
        ConfirmedManifest::Unconfirmed(_) => LifecycleRetentionProof::new(
            LifecycleRetentionProofStatus::Incomplete,
            recovery_health.clone(),
            None,
            None,
            None,
            Some("manifest_durability"),
        ),
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
                request.snapshot_prune_mode(),
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
                request.snapshot_prune_mode(),
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

/// The telemetry health debt a prune's failed deletions leave: one fault per
/// failed object (snapshot or, #3643, timeline segment), so `fault_count`
/// reflects how many are stranded; `None` when every delete succeeded. The
/// per-snapshot identities and sources stay on the outcome's typed `failed`.
fn pruning_delete_health_debt(failures: usize) -> LifecycleResult<Option<RecoveryHealth>> {
    if failures == 0 {
        return Ok(None);
    }
    let faults: Vec<RecoveryFault> = (0..failures)
        .map(|_| {
            RecoveryFault::new(
                RecoveryFaultKind::IoFailure,
                "snapshot pruning delete failure",
            )
        })
        .collect::<LifecycleResult<_>>()?;
    Ok(Some(RecoveryHealth::degraded(
        RecoveryDegradationClass::Telemetry,
        faults,
    )?))
}

/// #3643: which timeline segment prune a snapshot prune mode carries. The
/// newest-N verb keeps older snapshots on purpose, so it deletes no segment
/// they might reference; the proof-driven modes prune segments by the same
/// rule they prune snapshots.
pub(crate) const fn timeline_segment_prune_mode(
    mode: SnapshotPruneMode,
) -> Option<TimelineSegmentPruneMode> {
    match mode {
        SnapshotPruneMode::RetainNewest => None,
        SnapshotPruneMode::Superseded => Some(TimelineSegmentPruneMode::Superseded),
        SnapshotPruneMode::ReconcileToAttested => {
            Some(TimelineSegmentPruneMode::ReconcileToAttested)
        }
    }
}

/// #3643 (re-review P2): the one incomplete proof a prune may still act on —
/// the open reconcile of a database with NO attested snapshot, incomplete only
/// because there is no snapshot. Then no snapshot references any timeline
/// segment, so every segment is a crash orphan (a first checkpoint that died
/// after writing its segments, before its snapshot). Recovery health still
/// gates it: a blocked proof never reaches here.
pub(crate) fn snapshotless_segment_reconcile(
    proof: &LifecycleRetentionProof,
    mode: SnapshotPruneMode,
) -> bool {
    mode == SnapshotPruneMode::ReconcileToAttested
        && proof.status() == LifecycleRetentionProofStatus::Incomplete
        && proof.live_snapshot_id().is_none()
        && proof.missing_fact() == Some("manifest_snapshot")
}

/// Prune snapshots (and, #3643, the timeline segments nothing live
/// references) under a complete retention proof. `live_segments` are the
/// segments the manifest-attested snapshot references, established for the
/// proof's live snapshot id; `None` (it could not be established) prunes no
/// segment.
pub(crate) fn prune_snapshots_with_proof(
    snapshots: &SnapshotService<'_>,
    request: &LifecycleSnapshotPruningRequest,
    live_segments: Option<&std::collections::BTreeSet<crate::layout::TimelineSegmentId>>,
) -> LifecycleResult<LifecycleSnapshotPruningOutcome> {
    match request.proof().status() {
        LifecycleRetentionProofStatus::Complete => {}
        LifecycleRetentionProofStatus::Incomplete
            if snapshotless_segment_reconcile(request.proof(), request.snapshot_prune_mode()) =>
        {
            if let Some(referenced) = live_segments {
                let segments = snapshots
                    .prune_timeline_segments(
                        TimelineSegmentPruneMode::ReconcileToAttested,
                        0,
                        referenced,
                    )
                    .map_err(snapshot_error)?;
                let mut outcome = LifecycleSnapshotPruningOutcome::from_completed_report(
                    &SnapshotDeleteReport::default(),
                )?;
                outcome.record_timeline_segments(&segments)?;
                return Ok(outcome);
            }
            return Ok(LifecycleSnapshotPruningOutcome::deferred(
                LifecycleRetentionProofStatus::Incomplete,
                Some(telemetry_health_debt("retention proof is incomplete")?),
            ));
        }
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
        .prune_snapshots_with_mode(
            request.live_snapshot_id(),
            request.snapshot_prune_mode(),
            request.effective_retain_newest(),
        )
        .map_err(snapshot_error)?;
    let mut outcome = LifecycleSnapshotPruningOutcome::from_completed_report(&report)?;
    if let (Some(live), Some(mode), Some(referenced)) = (
        request.live_snapshot_id(),
        timeline_segment_prune_mode(request.snapshot_prune_mode()),
        live_segments,
    ) {
        let segments = snapshots
            .prune_timeline_segments(mode, live, referenced)
            .map_err(snapshot_error)?;
        outcome.record_timeline_segments(&segments)?;
    }
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
        )
        .with_snapshot_prune_mode(options.snapshot_prune_mode())),
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
    mode: SnapshotPruneMode,
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
            let (decision, reason) = if live {
                (
                    RetentionDecision::Retain,
                    LifecycleRetentionDecisionReason::LiveManifestSnapshot,
                )
            } else {
                snapshot_mode_decision(mode, snapshot.snapshot_id(), proof.live_snapshot_id(), {
                    index >= retain_start
                })
            };
            LifecycleRetentionDecisionRecord::snapshot(snapshot.object().clone(), decision, reason)
        })
        .collect()
}

/// Space-reclamation contract §3.4 (slice 5): the per-object verdict of each
/// prune mode for a non-live snapshot. `RetainNewest` keeps the newest window;
/// `Superseded` prunes below the live id and keeps anything at or above it;
/// `ReconcileToAttested` prunes everything that is not the attested id.
/// Without a live id the proof-driven modes can prove nothing dead (the proof
/// is incomplete) and retain everything.
pub(super) fn snapshot_mode_decision(
    mode: SnapshotPruneMode,
    snapshot_id: u64,
    live_snapshot_id: Option<u64>,
    newest_retained: bool,
) -> (RetentionDecision, LifecycleRetentionDecisionReason) {
    match (mode, live_snapshot_id) {
        (SnapshotPruneMode::RetainNewest, _) if newest_retained => (
            RetentionDecision::Retain,
            LifecycleRetentionDecisionReason::NewestSnapshotWindow,
        ),
        (SnapshotPruneMode::RetainNewest, _) => (
            RetentionDecision::PruneCandidate,
            LifecycleRetentionDecisionReason::SnapshotPruneCandidate,
        ),
        (SnapshotPruneMode::Superseded | SnapshotPruneMode::ReconcileToAttested, None) => (
            RetentionDecision::Retain,
            LifecycleRetentionDecisionReason::ProofIncomplete,
        ),
        (SnapshotPruneMode::Superseded, Some(live)) if superseded_snapshot(snapshot_id, live) => (
            RetentionDecision::PruneCandidate,
            LifecycleRetentionDecisionReason::SupersededSnapshot,
        ),
        (SnapshotPruneMode::Superseded, Some(_)) => (
            RetentionDecision::Retain,
            LifecycleRetentionDecisionReason::AboveLiveSnapshot,
        ),
        (SnapshotPruneMode::ReconcileToAttested, Some(attested))
            if reconcilable_orphan(snapshot_id, attested) =>
        {
            (
                RetentionDecision::PruneCandidate,
                LifecycleRetentionDecisionReason::NonAttestedSnapshot,
            )
        }
        (SnapshotPruneMode::ReconcileToAttested, Some(_)) => (
            RetentionDecision::Retain,
            LifecycleRetentionDecisionReason::LiveManifestSnapshot,
        ),
    }
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
