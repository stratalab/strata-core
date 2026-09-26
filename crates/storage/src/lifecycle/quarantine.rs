//! Lifecycle quarantine, purge, and repair orchestration.

#![allow(
    dead_code,
    reason = "quarantine orchestration hooks are consumed by durable maintenance and tests"
)]

use super::{
    telemetry_health_debt, LifecycleCodecId, LifecycleError, LifecycleLowerLayer, LifecycleResult,
    LifecycleStats, MaintenanceDeferralReason, MaintenanceOutcome, MaintenanceOutcomeStatus,
    MaintenanceTask, MaintenanceTaskKind, RecoveryDegradationClass, RecoveryFault,
    RecoveryFaultKind, RecoveryHealth, RetentionDecision,
};
use crate::format::quarantine::QuarantineEntry;
use crate::layout::{ObjectFamily, ObjectLayout};
use crate::object::ObjectName;
use crate::service::{
    QuarantineDeleteOutcome, QuarantineFamilyReconciliation, QuarantineGate,
    QuarantineInventoryToken, QuarantineObjectReport, QuarantineObjectRequest,
    QuarantineObjectStatus, QuarantinePurgeReport, QuarantinePurgeRequest,
    QuarantineReconciliationKind, QuarantineReconciliationReport, QuarantineService,
    QuarantineServiceError,
};
use sha2::{Digest, Sha256};
use strata_core::{BranchId, Timestamp};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleQuarantineProof {
    status: LifecycleQuarantineProofStatus,
    recovery_health: RecoveryHealth,
    source_reachable: bool,
    missing_fact: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleQuarantineProofStatus {
    CompleteSafe,
    Referenced,
    Incomplete,
    BlockedByRecoveryHealth,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleQuarantineRequest {
    branch_id: BranchId,
    database_id: [u8; 16],
    codec_id: LifecycleCodecId,
    object_id: String,
    source_object: ObjectName,
    staged_at: Timestamp,
    proof: LifecycleQuarantineProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleQuarantineOutcome {
    status: LifecycleQuarantineStatus,
    branch_id: BranchId,
    source_object: Option<ObjectName>,
    quarantine_object: Option<ObjectName>,
    inventory_object: Option<ObjectName>,
    byte_count: u64,
    entry_count: usize,
    recovery_health: Option<RecoveryHealth>,
    source_error: Option<LifecycleError>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleQuarantineStatus {
    QuarantinedSourceDeleted,
    AlreadyQuarantined,
    SourceDeleteRetried,
    SourceAlreadyMissingAfterPublish,
    QuarantinedSourceDeleteFailed,
    DeferredReferenced,
    DeferredIncompleteProof,
    BlockedByRecoveryHealth,
    InventoryPublishFailed,
    InventoryPublishUncertain,
    QuarantinePublishFailed,
    QuarantinePublishUncertain,
    InventoryMismatch,
    /// Service-layer rejection that is terminal: the request itself
    /// violates a policy/contract (`UnsafeGate`, `InvalidRequest`,
    /// `UnsupportedCapability`, `Layout`, `Encode`,
    /// `InvalidPublishMetadata`). Retrying without changing the request
    /// will fail the same way; the scheduler must surface a typed
    /// rejection rather than re-enqueueing.
    ServiceRejected,
    /// Service-layer failure that is potentially transient: a backend
    /// IO/state issue (`Read`, `Metadata`, `BackendState`, `Missing`)
    /// that may resolve on retry. The scheduler may re-enqueue.
    ServiceTransient,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecyclePurgeProof {
    status: LifecyclePurgeProofStatus,
    recovery_health: RecoveryHealth,
    inventory_token: Option<QuarantineInventoryToken>,
    stale: bool,
    missing_fact: Option<&'static str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecyclePurgeProofStatus {
    CompleteFresh,
    Stale,
    Incomplete,
    BlockedByRecoveryHealth,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecyclePurgeOutcome {
    status: LifecyclePurgeStatus,
    branch_id: BranchId,
    inventory_object: Option<ObjectName>,
    deleted_objects: Vec<ObjectName>,
    already_missing_objects: Vec<ObjectName>,
    failed_objects: Vec<ObjectName>,
    deleted_outcomes: Vec<QuarantineDeleteOutcome>,
    already_missing_outcomes: Vec<QuarantineDeleteOutcome>,
    failed_outcomes: Vec<QuarantineDeleteOutcome>,
    retained_entries: Vec<QuarantineEntry>,
    reclaimed_bytes: u64,
    recovery_health: Option<RecoveryHealth>,
    source_error: Option<LifecycleError>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecyclePurgeStatus {
    Completed,
    CompletedNoop,
    CompletedWithHealthDebt,
    DeferredIncompleteProof,
    BlockedByRecoveryHealth,
    StaleProof,
    /// #2524 Fix B: a concurrent sweep staged new entries between this
    /// purge's token capture and its mutation. Normal under load — the
    /// staging sweep's own follow-up purge covers the inventory; defer
    /// WITHOUT health debt.
    InventoryAdvanced,
    InventoryRewriteFailed,
}

/// The typed deferral a sweep status carries into its maintenance outcome;
/// `None` for statuses that complete or fail.
pub(crate) const fn quarantine_deferral_reason(
    status: LifecycleQuarantineStatus,
) -> Option<MaintenanceDeferralReason> {
    match status {
        LifecycleQuarantineStatus::DeferredReferenced => {
            Some(MaintenanceDeferralReason::Referenced)
        }
        LifecycleQuarantineStatus::DeferredIncompleteProof => {
            Some(MaintenanceDeferralReason::IncompleteProof)
        }
        LifecycleQuarantineStatus::BlockedByRecoveryHealth => {
            Some(MaintenanceDeferralReason::RecoveryHealth)
        }
        LifecycleQuarantineStatus::QuarantinedSourceDeleted
        | LifecycleQuarantineStatus::AlreadyQuarantined
        | LifecycleQuarantineStatus::SourceDeleteRetried
        | LifecycleQuarantineStatus::SourceAlreadyMissingAfterPublish
        | LifecycleQuarantineStatus::QuarantinedSourceDeleteFailed
        | LifecycleQuarantineStatus::InventoryPublishFailed
        | LifecycleQuarantineStatus::InventoryPublishUncertain
        | LifecycleQuarantineStatus::QuarantinePublishFailed
        | LifecycleQuarantineStatus::QuarantinePublishUncertain
        | LifecycleQuarantineStatus::InventoryMismatch
        | LifecycleQuarantineStatus::ServiceRejected
        | LifecycleQuarantineStatus::ServiceTransient => None,
    }
}

/// The typed deferral a purge status carries into its maintenance outcome;
/// `None` for statuses that complete or fail.
pub(crate) const fn purge_deferral_reason(
    status: LifecyclePurgeStatus,
) -> Option<MaintenanceDeferralReason> {
    match status {
        LifecyclePurgeStatus::DeferredIncompleteProof => {
            Some(MaintenanceDeferralReason::IncompleteProof)
        }
        LifecyclePurgeStatus::BlockedByRecoveryHealth => {
            Some(MaintenanceDeferralReason::RecoveryHealth)
        }
        LifecyclePurgeStatus::StaleProof => Some(MaintenanceDeferralReason::StaleProof),
        LifecyclePurgeStatus::InventoryAdvanced => {
            Some(MaintenanceDeferralReason::InventoryAdvanced)
        }
        LifecyclePurgeStatus::Completed
        | LifecyclePurgeStatus::CompletedNoop
        | LifecyclePurgeStatus::CompletedWithHealthDebt
        | LifecyclePurgeStatus::InventoryRewriteFailed => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleQuarantineRepairOutcome {
    report_count: usize,
    family_malformed_objects: usize,
    inventory_present_reports: usize,
    listed_objects: usize,
    missing_objects: usize,
    unlisted_objects: usize,
    malformed_objects: usize,
    recovery_health: Option<RecoveryHealth>,
    source_error: Option<LifecycleError>,
}

impl LifecycleQuarantineProof {
    pub(crate) fn new(
        status: LifecycleQuarantineProofStatus,
        recovery_health: RecoveryHealth,
        source_reachable: bool,
        missing_fact: Option<&'static str>,
    ) -> Self {
        Self {
            status,
            recovery_health,
            source_reachable,
            missing_fact,
        }
    }

    pub(crate) fn safe(recovery_health: RecoveryHealth) -> Self {
        let status = if recovery_health_blocks_reclaim(&recovery_health) {
            LifecycleQuarantineProofStatus::BlockedByRecoveryHealth
        } else {
            LifecycleQuarantineProofStatus::CompleteSafe
        };
        Self::new(status, recovery_health, false, None)
    }

    /// Construct a `CompleteSafe` proof while enforcing the unrelatedness
    /// rule for Telemetry debt: if recovery health is Telemetry-degraded
    /// and any fault names `candidate_branch`, the proof is downgraded
    /// to `BlockedByRecoveryHealth` instead of `CompleteSafe`. Callers
    /// who know the candidate's branch should prefer this over
    /// `safe(...)` so the next quarantine cycle does not silently admit
    /// reclaim while a related telemetry fault is open.
    pub(crate) fn safe_for_candidate(
        recovery_health: RecoveryHealth,
        candidate_branch: strata_core::BranchId,
    ) -> Self {
        let status = if recovery_health_blocks_reclaim(&recovery_health)
            || recovery_health.has_fault_targeting_branch(candidate_branch)
        {
            LifecycleQuarantineProofStatus::BlockedByRecoveryHealth
        } else {
            LifecycleQuarantineProofStatus::CompleteSafe
        };
        Self::new(status, recovery_health, false, None)
    }

    pub(crate) fn from_retention_decision(
        decision: RetentionDecision,
        recovery_health: RecoveryHealth,
    ) -> Self {
        if recovery_health_blocks_reclaim(&recovery_health) {
            return Self::new(
                LifecycleQuarantineProofStatus::BlockedByRecoveryHealth,
                recovery_health,
                false,
                Some("recovery_health"),
            );
        }
        match decision {
            RetentionDecision::QuarantineCandidate => Self::new(
                LifecycleQuarantineProofStatus::CompleteSafe,
                recovery_health,
                false,
                None,
            ),
            RetentionDecision::Retain => Self::new(
                LifecycleQuarantineProofStatus::Referenced,
                recovery_health,
                true,
                None,
            ),
            RetentionDecision::SkipUntilProof => Self::new(
                LifecycleQuarantineProofStatus::Incomplete,
                recovery_health,
                false,
                Some("retention_proof"),
            ),
            RetentionDecision::PruneCandidate | RetentionDecision::PurgeCandidate => Self::new(
                LifecycleQuarantineProofStatus::Incomplete,
                recovery_health,
                false,
                Some("quarantine_candidate"),
            ),
            RetentionDecision::RepairCandidate => Self::new(
                LifecycleQuarantineProofStatus::Incomplete,
                recovery_health,
                false,
                Some("repair_candidate"),
            ),
        }
    }

    pub(crate) const fn status(&self) -> LifecycleQuarantineProofStatus {
        self.status
    }

    pub(crate) const fn recovery_health(&self) -> &RecoveryHealth {
        &self.recovery_health
    }

    pub(crate) const fn source_reachable(&self) -> bool {
        self.source_reachable
    }

    pub(crate) const fn missing_fact(&self) -> Option<&'static str> {
        self.missing_fact
    }

    const fn gate(&self) -> QuarantineGate {
        match self.status {
            LifecycleQuarantineProofStatus::CompleteSafe => QuarantineGate::Safe,
            LifecycleQuarantineProofStatus::Referenced => QuarantineGate::Referenced,
            LifecycleQuarantineProofStatus::Incomplete => QuarantineGate::ProofIncomplete,
            LifecycleQuarantineProofStatus::BlockedByRecoveryHealth => {
                QuarantineGate::UnsafeRecovery
            }
        }
    }
}

impl LifecycleQuarantineRequest {
    pub(crate) fn new(
        branch_id: BranchId,
        database_id: [u8; 16],
        codec_id: LifecycleCodecId,
        object_id: impl Into<String>,
        source_object: ObjectName,
        staged_at: Timestamp,
        proof: LifecycleQuarantineProof,
    ) -> LifecycleResult<Self> {
        let request = Self {
            branch_id,
            database_id,
            codec_id,
            object_id: object_id.into(),
            source_object,
            staged_at,
            proof,
        };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn from_source_object(
        branch_id: BranchId,
        database_id: [u8; 16],
        codec_id: LifecycleCodecId,
        source_object: ObjectName,
        staged_at: Timestamp,
        proof: LifecycleQuarantineProof,
    ) -> LifecycleResult<Self> {
        let object_id = derived_quarantine_object_id(&source_object);
        Self::new(
            branch_id,
            database_id,
            codec_id,
            object_id,
            source_object,
            staged_at,
            proof,
        )
    }

    fn validate(&self) -> LifecycleResult<()> {
        if self.object_id.is_empty() {
            return Err(LifecycleError::InvalidConfig {
                field: "quarantine_object_id",
                reason: "must not be empty",
            });
        }
        if self.object_id == ObjectLayout::quarantine_inventory_object_id() {
            return Err(LifecycleError::InvalidConfig {
                field: "quarantine_object_id",
                reason: "must not be the quarantine inventory object id",
            });
        }
        if self.database_id == [0; 16] {
            return Err(LifecycleError::InvalidConfig {
                field: "database_id",
                reason: "must not be zero",
            });
        }
        if matches!(
            ObjectFamily::from_object_name(&self.source_object),
            None | Some(ObjectFamily::Quarantine)
        ) {
            return Err(LifecycleError::InvalidConfig {
                field: "source_object",
                reason: "must name a non-quarantine storage object",
            });
        }
        if self.staged_at == Timestamp::EPOCH {
            return Err(LifecycleError::InvalidConfig {
                field: "quarantine_timestamp",
                reason: "must not be epoch",
            });
        }
        Ok(())
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) fn object_id(&self) -> &str {
        &self.object_id
    }

    pub(crate) const fn source_object(&self) -> &ObjectName {
        &self.source_object
    }

    pub(crate) const fn proof(&self) -> &LifecycleQuarantineProof {
        &self.proof
    }
}

impl LifecycleQuarantineOutcome {
    fn deferred(request: &LifecycleQuarantineRequest, status: LifecycleQuarantineStatus) -> Self {
        let recovery_health = match status {
            LifecycleQuarantineStatus::BlockedByRecoveryHealth => {
                Some(request.proof.recovery_health.clone())
            }
            LifecycleQuarantineStatus::DeferredIncompleteProof
            | LifecycleQuarantineStatus::DeferredReferenced => {
                Some(telemetry_health_debt("quarantine proof is not safe").expect("health debt"))
            }
            _ => None,
        };
        Self {
            status,
            branch_id: request.branch_id,
            source_object: Some(request.source_object.clone()),
            quarantine_object: None,
            inventory_object: None,
            byte_count: 0,
            entry_count: 0,
            recovery_health,
            source_error: None,
        }
    }

    fn from_report(report: &QuarantineObjectReport) -> Self {
        let status = status_from_report(report.status());
        let recovery_health = health_for_quarantine_status(status, report.branch_id());
        let source_error = quarantine_report_error(report, status);
        Self {
            status,
            branch_id: report.branch_id(),
            source_object: Some(report.source_object().clone()),
            quarantine_object: Some(report.quarantine_object().clone()),
            inventory_object: report.inventory_write().map(|write| write.object().clone()),
            byte_count: report.byte_count(),
            entry_count: report.entry_count(),
            recovery_health,
            source_error,
        }
    }

    fn failed_from_service(
        branch_id: BranchId,
        source_object: ObjectName,
        source: QuarantineServiceError,
    ) -> Self {
        let status = match &source {
            QuarantineServiceError::InventoryMismatch { .. }
            | QuarantineServiceError::InventoryTokenMismatch { .. }
            | QuarantineServiceError::Decode { .. }
            | QuarantineServiceError::DatabaseMismatch { .. }
            | QuarantineServiceError::BranchMismatch { .. }
            | QuarantineServiceError::CodecMismatch { .. } => {
                LifecycleQuarantineStatus::InventoryMismatch
            }
            QuarantineServiceError::Publish { .. } => {
                LifecycleQuarantineStatus::QuarantinePublishFailed
            }
            // Terminal logic / policy rejections — retrying the same
            // request will not change the outcome.
            QuarantineServiceError::UnsafeGate { .. }
            | QuarantineServiceError::InvalidRequest { .. }
            | QuarantineServiceError::UnsupportedCapability { .. }
            | QuarantineServiceError::Layout { .. }
            | QuarantineServiceError::Encode { .. }
            | QuarantineServiceError::InvalidPublishMetadata { .. } => {
                LifecycleQuarantineStatus::ServiceRejected
            }
            // Potentially-transient backend / IO failures — the
            // scheduler may safely retry.
            QuarantineServiceError::Missing { .. }
            | QuarantineServiceError::Read { .. }
            | QuarantineServiceError::Metadata { .. }
            | QuarantineServiceError::BackendState { .. } => {
                LifecycleQuarantineStatus::ServiceTransient
            }
        };
        Self {
            status,
            branch_id,
            source_object: Some(source_object),
            quarantine_object: None,
            inventory_object: None,
            byte_count: 0,
            entry_count: 0,
            recovery_health: health_for_quarantine_status(status, branch_id),
            source_error: Some(quarantine_service_error(source)),
        }
    }

    pub(crate) const fn status(&self) -> LifecycleQuarantineStatus {
        self.status
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn source_object(&self) -> Option<&ObjectName> {
        self.source_object.as_ref()
    }

    pub(crate) const fn quarantine_object(&self) -> Option<&ObjectName> {
        self.quarantine_object.as_ref()
    }

    pub(crate) const fn inventory_object(&self) -> Option<&ObjectName> {
        self.inventory_object.as_ref()
    }

    pub(crate) const fn byte_count(&self) -> u64 {
        self.byte_count
    }

    pub(crate) const fn entry_count(&self) -> usize {
        self.entry_count
    }

    pub(crate) const fn recovery_health(&self) -> Option<&RecoveryHealth> {
        self.recovery_health.as_ref()
    }

    pub(crate) const fn source_error(&self) -> Option<&LifecycleError> {
        self.source_error.as_ref()
    }

    pub(crate) fn maintenance_outcome(&self) -> MaintenanceOutcome {
        let status = match self.status {
            LifecycleQuarantineStatus::QuarantinedSourceDeleted
            | LifecycleQuarantineStatus::AlreadyQuarantined
            | LifecycleQuarantineStatus::SourceDeleteRetried
            | LifecycleQuarantineStatus::SourceAlreadyMissingAfterPublish => {
                MaintenanceOutcomeStatus::Completed
            }
            LifecycleQuarantineStatus::DeferredReferenced
            | LifecycleQuarantineStatus::DeferredIncompleteProof
            | LifecycleQuarantineStatus::BlockedByRecoveryHealth => {
                MaintenanceOutcomeStatus::Deferred
            }
            LifecycleQuarantineStatus::QuarantinedSourceDeleteFailed
            | LifecycleQuarantineStatus::InventoryPublishFailed
            | LifecycleQuarantineStatus::InventoryPublishUncertain
            | LifecycleQuarantineStatus::QuarantinePublishFailed
            | LifecycleQuarantineStatus::QuarantinePublishUncertain
            | LifecycleQuarantineStatus::InventoryMismatch
            | LifecycleQuarantineStatus::ServiceRejected
            | LifecycleQuarantineStatus::ServiceTransient => MaintenanceOutcomeStatus::Failed,
        };
        let mut outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Quarantine, status)
            .with_affected_object_names(self.affected_object_names())
            .with_effects(
                self.affected_object_count(),
                self.reclaimed_bytes(),
                self.retryable(),
            )
            .with_state_changes(self.state_changes())
            .with_stats(LifecycleStats::new(
                0,
                self.recovery_health
                    .as_ref()
                    .map_or(0, RecoveryHealth::fault_count),
                1,
                usize::from(status != MaintenanceOutcomeStatus::Completed),
                0,
            ));
        if let Some(reason) = self.reason() {
            outcome = outcome.with_reason(reason);
        }
        if let Some(deferral) = quarantine_deferral_reason(self.status) {
            outcome = outcome.with_deferral_reason(deferral);
        }
        if let Some(health) = self.recovery_health.clone() {
            outcome = outcome.with_recovery_health(health);
        }
        if let Some(error) = self.source_error.clone() {
            outcome = outcome.with_source_error(error);
        }
        outcome
    }

    fn affected_object_names(&self) -> Vec<String> {
        [
            self.source_object.as_ref(),
            self.quarantine_object.as_ref(),
            self.inventory_object.as_ref(),
        ]
        .into_iter()
        .flatten()
        .map(ToString::to_string)
        .collect()
    }

    fn affected_object_count(&self) -> usize {
        usize::from(self.source_object.is_some())
            + usize::from(self.quarantine_object.is_some())
            + usize::from(self.inventory_object.is_some())
    }

    const fn reclaimed_bytes(&self) -> u64 {
        match self.status {
            LifecycleQuarantineStatus::QuarantinedSourceDeleted
            | LifecycleQuarantineStatus::SourceDeleteRetried => self.byte_count,
            _ => 0,
        }
    }

    const fn retryable(&self) -> bool {
        // Definite-failure statuses (`*Failed`) are safer to retry than
        // uncertain ones: a definite failure means the prior attempt left
        // no durable state, so a retry starts from a clean baseline. An
        // *Uncertain* status means the publish may or may not have
        // landed durably — retrying could re-publish over a partial
        // success, so it is not retryable without a fresh proof. The
        // split `ServiceTransient` status (potentially-transient backend
        // IO/state errors) is also retryable; `ServiceRejected`
        // (terminal policy violation) is not.
        matches!(
            self.status,
            LifecycleQuarantineStatus::QuarantinedSourceDeleteFailed
                | LifecycleQuarantineStatus::QuarantinePublishFailed
                | LifecycleQuarantineStatus::InventoryPublishFailed
                | LifecycleQuarantineStatus::ServiceTransient
        )
    }

    const fn state_changes(&self) -> usize {
        match self.status {
            LifecycleQuarantineStatus::QuarantinedSourceDeleted
            | LifecycleQuarantineStatus::SourceDeleteRetried => 2,
            LifecycleQuarantineStatus::AlreadyQuarantined
            | LifecycleQuarantineStatus::SourceAlreadyMissingAfterPublish => 1,
            _ => 0,
        }
    }

    const fn reason(&self) -> Option<&'static str> {
        match self.status {
            LifecycleQuarantineStatus::DeferredReferenced => Some("object is still referenced"),
            LifecycleQuarantineStatus::DeferredIncompleteProof => {
                Some("quarantine proof is incomplete")
            }
            LifecycleQuarantineStatus::BlockedByRecoveryHealth => {
                Some("recovery health blocks quarantine")
            }
            LifecycleQuarantineStatus::QuarantinedSourceDeleteFailed => {
                Some("source delete failed after quarantine")
            }
            LifecycleQuarantineStatus::InventoryPublishFailed
            | LifecycleQuarantineStatus::InventoryPublishUncertain => {
                Some("quarantine inventory publication failed")
            }
            LifecycleQuarantineStatus::QuarantinePublishFailed
            | LifecycleQuarantineStatus::QuarantinePublishUncertain => {
                Some("quarantine object publication failed")
            }
            LifecycleQuarantineStatus::InventoryMismatch => Some("quarantine inventory mismatch"),
            LifecycleQuarantineStatus::ServiceRejected => {
                Some("quarantine service rejected request")
            }
            LifecycleQuarantineStatus::ServiceTransient => {
                Some("quarantine service transient failure")
            }
            LifecycleQuarantineStatus::QuarantinedSourceDeleted
            | LifecycleQuarantineStatus::AlreadyQuarantined
            | LifecycleQuarantineStatus::SourceDeleteRetried
            | LifecycleQuarantineStatus::SourceAlreadyMissingAfterPublish => None,
        }
    }
}

impl LifecyclePurgeProof {
    pub(crate) fn fresh(
        recovery_health: RecoveryHealth,
        inventory_token: QuarantineInventoryToken,
    ) -> Self {
        Self::fresh_inner(recovery_health, inventory_token, None)
    }

    /// Construct a `CompleteFresh` purge proof while enforcing the
    /// unrelatedness rule for Telemetry debt that names the candidate's
    /// branch (see `LifecycleQuarantineProof::safe_for_candidate`).
    pub(crate) fn fresh_for_candidate(
        recovery_health: RecoveryHealth,
        inventory_token: QuarantineInventoryToken,
        candidate_branch: strata_core::BranchId,
    ) -> Self {
        Self::fresh_inner(recovery_health, inventory_token, Some(candidate_branch))
    }

    fn fresh_inner(
        recovery_health: RecoveryHealth,
        inventory_token: QuarantineInventoryToken,
        candidate_branch: Option<strata_core::BranchId>,
    ) -> Self {
        let related_branch_fault = candidate_branch
            .is_some_and(|branch| recovery_health.has_fault_targeting_branch(branch));
        let status = if recovery_health_blocks_reclaim(&recovery_health) || related_branch_fault {
            LifecyclePurgeProofStatus::BlockedByRecoveryHealth
        } else {
            LifecyclePurgeProofStatus::CompleteFresh
        };
        Self {
            status,
            recovery_health,
            inventory_token: Some(inventory_token),
            stale: false,
            missing_fact: None,
        }
    }

    pub(crate) fn stale(recovery_health: RecoveryHealth) -> Self {
        Self {
            status: LifecyclePurgeProofStatus::Stale,
            recovery_health,
            inventory_token: None,
            stale: true,
            missing_fact: Some("fresh_proof"),
        }
    }

    pub(crate) fn incomplete(recovery_health: RecoveryHealth, missing_fact: &'static str) -> Self {
        Self {
            status: LifecyclePurgeProofStatus::Incomplete,
            recovery_health,
            inventory_token: None,
            stale: false,
            missing_fact: Some(missing_fact),
        }
    }

    pub(crate) const fn status(&self) -> LifecyclePurgeProofStatus {
        self.status
    }

    pub(crate) const fn recovery_health(&self) -> &RecoveryHealth {
        &self.recovery_health
    }

    pub(crate) const fn inventory_token(&self) -> Option<QuarantineInventoryToken> {
        self.inventory_token
    }

    pub(crate) const fn stale_flag(&self) -> bool {
        self.stale
    }

    pub(crate) const fn missing_fact(&self) -> Option<&'static str> {
        self.missing_fact
    }

    const fn gate(&self) -> QuarantineGate {
        match self.status {
            LifecyclePurgeProofStatus::CompleteFresh => QuarantineGate::Safe,
            LifecyclePurgeProofStatus::Stale | LifecyclePurgeProofStatus::Incomplete => {
                QuarantineGate::ProofIncomplete
            }
            LifecyclePurgeProofStatus::BlockedByRecoveryHealth => QuarantineGate::UnsafeRecovery,
        }
    }
}

impl LifecyclePurgeOutcome {
    fn deferred(
        branch_id: BranchId,
        proof: &LifecyclePurgeProof,
        status: LifecyclePurgeStatus,
    ) -> Self {
        let recovery_health = match status {
            LifecyclePurgeStatus::BlockedByRecoveryHealth => Some(proof.recovery_health.clone()),
            LifecyclePurgeStatus::DeferredIncompleteProof | LifecyclePurgeStatus::StaleProof => {
                Some(telemetry_health_debt("purge proof is not safe").expect("health debt"))
            }
            _ => None,
        };
        Self {
            status,
            branch_id,
            inventory_object: None,
            deleted_objects: Vec::new(),
            already_missing_objects: Vec::new(),
            failed_objects: Vec::new(),
            deleted_outcomes: Vec::new(),
            already_missing_outcomes: Vec::new(),
            failed_outcomes: Vec::new(),
            retained_entries: Vec::new(),
            reclaimed_bytes: 0,
            recovery_health,
            source_error: None,
        }
    }

    fn from_report(report: &QuarantinePurgeReport) -> Self {
        let failed = !report.failed().is_empty();
        let inventory_failed = report.inventory_publish_failure().is_some();
        let changed = !report.deleted().is_empty() || !report.already_missing().is_empty();
        let status = if failed || inventory_failed {
            LifecyclePurgeStatus::CompletedWithHealthDebt
        } else if changed {
            LifecyclePurgeStatus::Completed
        } else {
            LifecyclePurgeStatus::CompletedNoop
        };
        let recovery_health = if failed || inventory_failed {
            Some(telemetry_health_debt("quarantine purge has health debt").expect("health debt"))
        } else {
            None
        };
        let source_error = purge_report_error(report);
        Self {
            status,
            branch_id: report.branch_id(),
            inventory_object: Some(report.inventory_object().clone()),
            deleted_objects: delete_objects(report.deleted()),
            already_missing_objects: delete_objects(report.already_missing()),
            failed_objects: delete_objects(report.failed()),
            deleted_outcomes: report.deleted().to_vec(),
            already_missing_outcomes: report.already_missing().to_vec(),
            failed_outcomes: report.failed().to_vec(),
            retained_entries: report.retained_entries().to_vec(),
            reclaimed_bytes: report.reclaimed_bytes(),
            recovery_health,
            source_error,
        }
    }

    fn failed_from_service(branch_id: BranchId, source: QuarantineServiceError) -> Self {
        Self {
            status: LifecyclePurgeStatus::InventoryRewriteFailed,
            branch_id,
            inventory_object: None,
            deleted_objects: Vec::new(),
            already_missing_objects: Vec::new(),
            failed_objects: Vec::new(),
            deleted_outcomes: Vec::new(),
            already_missing_outcomes: Vec::new(),
            failed_outcomes: Vec::new(),
            retained_entries: Vec::new(),
            reclaimed_bytes: 0,
            recovery_health: Some(
                telemetry_health_debt("quarantine purge failed").expect("health debt"),
            ),
            source_error: Some(quarantine_service_error(source)),
        }
    }

    pub(crate) const fn status(&self) -> LifecyclePurgeStatus {
        self.status
    }

    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }

    pub(crate) const fn inventory_object(&self) -> Option<&ObjectName> {
        self.inventory_object.as_ref()
    }

    pub(crate) fn deleted_objects(&self) -> &[ObjectName] {
        &self.deleted_objects
    }

    pub(crate) fn already_missing_objects(&self) -> &[ObjectName] {
        &self.already_missing_objects
    }

    pub(crate) fn failed_objects(&self) -> &[ObjectName] {
        &self.failed_objects
    }

    pub(crate) fn deleted_outcomes(&self) -> &[QuarantineDeleteOutcome] {
        &self.deleted_outcomes
    }

    pub(crate) fn already_missing_outcomes(&self) -> &[QuarantineDeleteOutcome] {
        &self.already_missing_outcomes
    }

    pub(crate) fn failed_outcomes(&self) -> &[QuarantineDeleteOutcome] {
        &self.failed_outcomes
    }

    pub(crate) fn retained_entries(&self) -> &[QuarantineEntry] {
        &self.retained_entries
    }

    pub(crate) const fn reclaimed_bytes(&self) -> u64 {
        self.reclaimed_bytes
    }

    pub(crate) const fn recovery_health(&self) -> Option<&RecoveryHealth> {
        self.recovery_health.as_ref()
    }

    pub(crate) const fn source_error(&self) -> Option<&LifecycleError> {
        self.source_error.as_ref()
    }

    pub(crate) fn maintenance_outcome(&self) -> MaintenanceOutcome {
        let status = match self.status {
            LifecyclePurgeStatus::Completed
            | LifecyclePurgeStatus::CompletedNoop
            | LifecyclePurgeStatus::CompletedWithHealthDebt => MaintenanceOutcomeStatus::Completed,
            LifecyclePurgeStatus::DeferredIncompleteProof
            | LifecyclePurgeStatus::BlockedByRecoveryHealth
            | LifecyclePurgeStatus::StaleProof
            | LifecyclePurgeStatus::InventoryAdvanced => MaintenanceOutcomeStatus::Deferred,
            LifecyclePurgeStatus::InventoryRewriteFailed => MaintenanceOutcomeStatus::Failed,
        };
        let mut outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Purge, status)
            .with_affected_object_names(self.affected_object_names())
            .with_effects(
                self.affected_object_count(),
                self.reclaimed_bytes,
                self.retryable(),
            )
            .with_state_changes(self.deleted_objects.len() + self.already_missing_objects.len())
            .with_stats(LifecycleStats::new(
                0,
                self.recovery_health
                    .as_ref()
                    .map_or(0, RecoveryHealth::fault_count),
                1,
                usize::from(status != MaintenanceOutcomeStatus::Completed),
                0,
            ));
        if let Some(reason) = self.reason() {
            outcome = outcome.with_reason(reason);
        }
        if let Some(deferral) = purge_deferral_reason(self.status) {
            outcome = outcome.with_deferral_reason(deferral);
        }
        if let Some(health) = self.recovery_health.clone() {
            outcome = outcome.with_recovery_health(health);
        }
        if let Some(error) = self.source_error.clone() {
            outcome = outcome.with_source_error(error);
        }
        outcome
    }

    fn affected_object_names(&self) -> Vec<String> {
        self.inventory_object
            .as_ref()
            .into_iter()
            .chain(self.deleted_objects.iter())
            .chain(self.already_missing_objects.iter())
            .chain(self.failed_objects.iter())
            .map(ToString::to_string)
            .collect()
    }

    fn affected_object_count(&self) -> usize {
        usize::from(self.inventory_object.is_some())
            + self.deleted_objects.len()
            + self.already_missing_objects.len()
            + self.failed_objects.len()
    }

    const fn retryable(&self) -> bool {
        matches!(
            self.status,
            LifecyclePurgeStatus::CompletedWithHealthDebt
                | LifecyclePurgeStatus::InventoryRewriteFailed
        )
    }

    const fn reason(&self) -> Option<&'static str> {
        match self.status {
            LifecyclePurgeStatus::CompletedWithHealthDebt => {
                Some("quarantine purge has health debt")
            }
            LifecyclePurgeStatus::DeferredIncompleteProof => Some("purge proof is incomplete"),
            LifecyclePurgeStatus::BlockedByRecoveryHealth => Some("recovery health blocks purge"),
            LifecyclePurgeStatus::StaleProof => Some("purge proof is stale"),
            LifecyclePurgeStatus::InventoryAdvanced => {
                Some("quarantine inventory advanced past the purge proof")
            }
            LifecyclePurgeStatus::InventoryRewriteFailed => {
                Some("quarantine inventory rewrite failed")
            }
            LifecyclePurgeStatus::Completed | LifecyclePurgeStatus::CompletedNoop => None,
        }
    }
}

impl LifecycleQuarantineRepairOutcome {
    fn from_branch_report(report: &QuarantineReconciliationReport) -> LifecycleResult<Self> {
        let recovery_health =
            health_for_reconciliation_kind(report.kind(), Some(report.branch_id()))?;
        let source_error = repair_report_error(report);
        Ok(Self {
            report_count: 1,
            family_malformed_objects: 0,
            inventory_present_reports: usize::from(report.inventory_present()),
            listed_objects: report.listed_objects().len(),
            missing_objects: report.missing_objects().len(),
            unlisted_objects: report.unlisted_objects().len(),
            malformed_objects: report.malformed_objects().len(),
            recovery_health,
            source_error,
        })
    }

    fn from_family_report(report: &QuarantineFamilyReconciliation) -> LifecycleResult<Self> {
        // Family-level reconciliation aggregates multiple branches; no
        // single branch owns the resulting health so the fault stays
        // unscoped.
        let recovery_health = health_for_reconciliation_kind(report.kind(), None)?;
        let branch_reports = report.branch_reports();
        Ok(Self {
            report_count: branch_reports.len(),
            family_malformed_objects: report.malformed_objects().len(),
            inventory_present_reports: branch_reports
                .iter()
                .filter(|report| report.inventory_present())
                .count(),
            listed_objects: branch_reports
                .iter()
                .map(|report| report.listed_objects().len())
                .sum(),
            missing_objects: branch_reports
                .iter()
                .map(|report| report.missing_objects().len())
                .sum(),
            unlisted_objects: branch_reports
                .iter()
                .map(|report| report.unlisted_objects().len())
                .sum(),
            malformed_objects: branch_reports
                .iter()
                .map(|report| report.malformed_objects().len())
                .sum::<usize>()
                + report.malformed_objects().len(),
            recovery_health,
            source_error: report
                .unavailable()
                .map(|unavailable| {
                    LifecycleError::quarantine_repair_inconclusive_with(
                        "quarantine backend is unavailable",
                        unavailable.source().clone(),
                    )
                })
                .or_else(|| report.branch_reports().iter().find_map(repair_report_error)),
        })
    }

    fn failed_from_service(source: QuarantineServiceError) -> Self {
        Self {
            report_count: 0,
            family_malformed_objects: 0,
            inventory_present_reports: 0,
            listed_objects: 0,
            missing_objects: 0,
            unlisted_objects: 0,
            malformed_objects: 0,
            recovery_health: Some(RecoveryHealth::failed(
                RecoveryFault::new(
                    RecoveryFaultKind::QuarantineInventoryMismatch,
                    "quarantine reconciliation failed",
                )
                .expect("fault"),
            )),
            source_error: Some(quarantine_service_error(source)),
        }
    }

    pub(crate) const fn completed_clean(&self) -> bool {
        self.recovery_health.is_none()
    }

    pub(crate) const fn completed_with_health_debt(&self) -> bool {
        matches!(self.recovery_health, Some(RecoveryHealth::Degraded { .. }))
    }

    pub(crate) const fn backend_unavailable(&self) -> bool {
        matches!(self.recovery_health, Some(RecoveryHealth::Failed { .. }))
    }

    pub(crate) const fn report_count(&self) -> usize {
        self.report_count
    }

    pub(crate) const fn inventory_present_reports(&self) -> usize {
        self.inventory_present_reports
    }

    pub(crate) const fn listed_objects(&self) -> usize {
        self.listed_objects
    }

    pub(crate) const fn missing_objects(&self) -> usize {
        self.missing_objects
    }

    pub(crate) const fn unlisted_objects(&self) -> usize {
        self.unlisted_objects
    }

    pub(crate) const fn malformed_objects(&self) -> usize {
        self.malformed_objects
    }

    pub(crate) const fn recovery_health(&self) -> Option<&RecoveryHealth> {
        self.recovery_health.as_ref()
    }

    pub(crate) const fn source_error(&self) -> Option<&LifecycleError> {
        self.source_error.as_ref()
    }

    pub(crate) fn maintenance_outcome(&self) -> MaintenanceOutcome {
        let status = if self.backend_unavailable() {
            MaintenanceOutcomeStatus::Failed
        } else {
            MaintenanceOutcomeStatus::Completed
        };
        let mut outcome = MaintenanceOutcome::new(MaintenanceTaskKind::Repair, status)
            .with_effects(self.affected_object_count(), 0, self.retryable())
            .with_state_changes(0)
            .with_stats(LifecycleStats::new(
                0,
                self.recovery_health
                    .as_ref()
                    .map_or(0, RecoveryHealth::fault_count),
                1,
                usize::from(status != MaintenanceOutcomeStatus::Completed),
                0,
            ));
        if let Some(reason) = self.reason() {
            outcome = outcome.with_reason(reason);
        }
        if let Some(health) = self.recovery_health.clone() {
            outcome = outcome.with_recovery_health(health);
        }
        if let Some(error) = self.source_error.clone() {
            outcome = outcome.with_source_error(error);
        }
        outcome
    }

    const fn affected_object_count(&self) -> usize {
        self.report_count + self.family_malformed_objects
    }

    const fn retryable(&self) -> bool {
        self.backend_unavailable()
    }

    const fn reason(&self) -> Option<&'static str> {
        if self.backend_unavailable() {
            Some("quarantine reconciliation backend unavailable")
        } else if self.completed_with_health_debt() {
            Some("quarantine reconciliation reported health debt")
        } else {
            None
        }
    }
}

pub(crate) fn quarantine_object(
    service: &QuarantineService<'_>,
    request: &LifecycleQuarantineRequest,
) -> LifecycleQuarantineOutcome {
    match request.proof.status() {
        LifecycleQuarantineProofStatus::CompleteSafe => {}
        LifecycleQuarantineProofStatus::Referenced => {
            return LifecycleQuarantineOutcome::deferred(
                request,
                LifecycleQuarantineStatus::DeferredReferenced,
            );
        }
        LifecycleQuarantineProofStatus::Incomplete => {
            return LifecycleQuarantineOutcome::deferred(
                request,
                LifecycleQuarantineStatus::DeferredIncompleteProof,
            );
        }
        LifecycleQuarantineProofStatus::BlockedByRecoveryHealth => {
            return LifecycleQuarantineOutcome::deferred(
                request,
                LifecycleQuarantineStatus::BlockedByRecoveryHealth,
            );
        }
    }

    let service_request = QuarantineObjectRequest::new(
        request.branch_id,
        request.database_id,
        request.codec_id.as_str(),
        request.object_id.clone(),
        request.source_object.clone(),
        request.staged_at,
        request.proof.gate(),
    );
    match service.quarantine_object(&service_request) {
        Ok(report) => LifecycleQuarantineOutcome::from_report(&report),
        Err(source) => LifecycleQuarantineOutcome::failed_from_service(
            request.branch_id,
            request.source_object.clone(),
            source,
        ),
    }
}

pub(crate) fn purge_quarantine(
    service: &QuarantineService<'_>,
    branch_id: BranchId,
    database_id: [u8; 16],
    codec_id: &LifecycleCodecId,
    proof: &LifecyclePurgeProof,
) -> LifecycleResult<LifecyclePurgeOutcome> {
    validate_purge_request(database_id, proof)?;
    match proof.status() {
        LifecyclePurgeProofStatus::CompleteFresh => {}
        LifecyclePurgeProofStatus::Stale => {
            return Ok(LifecyclePurgeOutcome::deferred(
                branch_id,
                proof,
                LifecyclePurgeStatus::StaleProof,
            ));
        }
        LifecyclePurgeProofStatus::Incomplete => {
            return Ok(LifecyclePurgeOutcome::deferred(
                branch_id,
                proof,
                LifecyclePurgeStatus::DeferredIncompleteProof,
            ));
        }
        LifecyclePurgeProofStatus::BlockedByRecoveryHealth => {
            return Ok(LifecyclePurgeOutcome::deferred(
                branch_id,
                proof,
                LifecyclePurgeStatus::BlockedByRecoveryHealth,
            ));
        }
    }

    let service_request = QuarantinePurgeRequest::new(
        branch_id,
        database_id,
        codec_id.as_str(),
        proof.gate(),
        proof.inventory_token(),
    );
    Ok(match service.purge_quarantine(service_request) {
        Ok(report) => LifecyclePurgeOutcome::from_report(&report),
        Err(QuarantineServiceError::InventoryTokenMismatch { .. }) => {
            LifecyclePurgeOutcome::deferred(
                branch_id,
                proof,
                LifecyclePurgeStatus::InventoryAdvanced,
            )
        }
        Err(source) => LifecyclePurgeOutcome::failed_from_service(branch_id, source),
    })
}

pub(crate) fn repair_branch_quarantine(
    service: &QuarantineService<'_>,
    branch_id: BranchId,
    database_id: [u8; 16],
    codec_id: &LifecycleCodecId,
) -> LifecycleResult<LifecycleQuarantineRepairOutcome> {
    repair_quarantine_inner(service, Some(branch_id), database_id, codec_id, false)
}

pub(crate) fn repair_quarantine_family(
    service: &QuarantineService<'_>,
    database_id: [u8; 16],
    codec_id: &LifecycleCodecId,
) -> LifecycleResult<LifecycleQuarantineRepairOutcome> {
    repair_quarantine_inner(service, None, database_id, codec_id, false)
}

#[cfg(test)]
pub(crate) fn repair_branch_quarantine_with_mutation_for_test(
    service: &QuarantineService<'_>,
    branch_id: BranchId,
    database_id: [u8; 16],
    codec_id: &LifecycleCodecId,
) -> LifecycleResult<LifecycleQuarantineRepairOutcome> {
    repair_quarantine_inner(service, Some(branch_id), database_id, codec_id, true)
}

fn repair_quarantine_inner(
    service: &QuarantineService<'_>,
    branch_id: Option<BranchId>,
    database_id: [u8; 16],
    codec_id: &LifecycleCodecId,
    allow_mutation: bool,
) -> LifecycleResult<LifecycleQuarantineRepairOutcome> {
    validate_repair_request(database_id)?;
    if allow_mutation {
        return Err(LifecycleError::QuarantineRepairInconclusive {
            reason: "mutating quarantine repair is not supported",
            source: None,
        });
    }
    match branch_id {
        Some(branch_id) => {
            match service.reconcile_branch_quarantine(branch_id, database_id, codec_id.as_str()) {
                Ok(report) => LifecycleQuarantineRepairOutcome::from_branch_report(&report),
                Err(source) => Ok(LifecycleQuarantineRepairOutcome::failed_from_service(
                    source,
                )),
            }
        }
        None => match service.reconcile_quarantine_family(database_id, codec_id.as_str()) {
            Ok(report) => LifecycleQuarantineRepairOutcome::from_family_report(&report),
            Err(source) => Ok(LifecycleQuarantineRepairOutcome::failed_from_service(
                source,
            )),
        },
    }
}

pub(crate) fn purge_proof_from_maintenance_task(
    task: &MaintenanceTask,
    recovery_health: RecoveryHealth,
    default_branch_id: BranchId,
    inventory_token: QuarantineInventoryToken,
) -> LifecycleResult<(BranchId, LifecyclePurgeProof)> {
    if task.kind() != MaintenanceTaskKind::Purge {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "purge request requires purge task",
        });
    }
    let branch_id = match task.scope() {
        super::MaintenanceTaskScope::Branch(branch_id) => branch_id,
        super::MaintenanceTaskScope::Quarantine => default_branch_id,
        _ => {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "purge task scope is invalid",
            });
        }
    };
    Ok((
        branch_id,
        // Pass the candidate's branch so the proof refuses reclaim under
        // Telemetry debt that names this branch (the live current
        // recovery health may carry branch-scoped faults attached by
        // recovery or by prior quarantine attempts via
        // `with_affected_branch`).
        LifecyclePurgeProof::fresh_for_candidate(recovery_health, inventory_token, branch_id),
    ))
}

pub(crate) fn repair_branch_from_maintenance_task(
    task: &MaintenanceTask,
) -> LifecycleResult<Option<BranchId>> {
    if task.kind() != MaintenanceTaskKind::Repair {
        return Err(LifecycleError::MaintenanceTaskFailed {
            reason: "repair request requires repair task",
        });
    }
    match task.scope() {
        super::MaintenanceTaskScope::Branch(branch_id) => Ok(Some(branch_id)),
        super::MaintenanceTaskScope::Quarantine | super::MaintenanceTaskScope::Global => Ok(None),
        _ => Err(LifecycleError::MaintenanceTaskFailed {
            reason: "repair task scope is invalid",
        }),
    }
}

pub(crate) fn unsupported_quarantine_maintenance(kind: MaintenanceTaskKind) -> MaintenanceOutcome {
    MaintenanceOutcome::new(kind, MaintenanceOutcomeStatus::Deferred)
        .with_reason("storage mode does not support durable quarantine maintenance")
        .with_stats(LifecycleStats::new(0, 0, 1, 1, 0))
}

pub(crate) fn quarantine_task_without_request() -> MaintenanceOutcome {
    MaintenanceOutcome::new(
        MaintenanceTaskKind::Quarantine,
        MaintenanceOutcomeStatus::Deferred,
    )
    .with_reason("quarantine task requires an explicit quarantine request")
    .with_stats(LifecycleStats::new(0, 0, 1, 1, 0))
}

fn validate_purge_request(
    database_id: [u8; 16],
    proof: &LifecyclePurgeProof,
) -> LifecycleResult<()> {
    if database_id == [0; 16] {
        return Err(LifecycleError::InvalidConfig {
            field: "database_id",
            reason: "must not be zero",
        });
    }
    if proof.status == LifecyclePurgeProofStatus::CompleteFresh && proof.inventory_token.is_none() {
        return Err(LifecycleError::InvalidConfig {
            field: "inventory_token",
            reason: "fresh purge proof requires an inventory token",
        });
    }
    Ok(())
}

fn validate_repair_request(database_id: [u8; 16]) -> LifecycleResult<()> {
    if database_id == [0; 16] {
        return Err(LifecycleError::InvalidConfig {
            field: "database_id",
            reason: "must not be zero",
        });
    }
    Ok(())
}

fn status_from_report(status: QuarantineObjectStatus) -> LifecycleQuarantineStatus {
    match status {
        QuarantineObjectStatus::QuarantinedSourceDeleted => {
            LifecycleQuarantineStatus::QuarantinedSourceDeleted
        }
        QuarantineObjectStatus::AlreadyQuarantined => LifecycleQuarantineStatus::AlreadyQuarantined,
        QuarantineObjectStatus::SourceDeleteRetried => {
            LifecycleQuarantineStatus::SourceDeleteRetried
        }
        QuarantineObjectStatus::SourceAlreadyMissingAfterPublish => {
            LifecycleQuarantineStatus::SourceAlreadyMissingAfterPublish
        }
        QuarantineObjectStatus::QuarantinedSourceDeleteFailed => {
            LifecycleQuarantineStatus::QuarantinedSourceDeleteFailed
        }
        QuarantineObjectStatus::InventoryPublishFailed => {
            LifecycleQuarantineStatus::InventoryPublishFailed
        }
        QuarantineObjectStatus::InventoryPublishUncertain => {
            LifecycleQuarantineStatus::InventoryPublishUncertain
        }
        QuarantineObjectStatus::QuarantinePublishFailed => {
            LifecycleQuarantineStatus::QuarantinePublishFailed
        }
        QuarantineObjectStatus::QuarantinePublishUncertain => {
            LifecycleQuarantineStatus::QuarantinePublishUncertain
        }
    }
}

fn quarantine_report_error(
    report: &QuarantineObjectReport,
    status: LifecycleQuarantineStatus,
) -> Option<LifecycleError> {
    match status {
        LifecycleQuarantineStatus::InventoryPublishFailed => {
            report.inventory_publish_failure().map(|failure| {
                LifecycleError::quarantine_publication_failed_with(
                    "quarantine inventory publication failed",
                    failure.source().clone(),
                )
            })
        }
        LifecycleQuarantineStatus::InventoryPublishUncertain => {
            report.inventory_publish_failure().map(|failure| {
                LifecycleError::quarantine_publication_uncertain_with(
                    "quarantine inventory publication uncertain",
                    failure.source().clone(),
                )
            })
        }
        LifecycleQuarantineStatus::QuarantinePublishFailed => {
            report.quarantine_publish_failure().map(|failure| {
                LifecycleError::quarantine_publication_failed_with(
                    "quarantine object publication failed",
                    failure.source().clone(),
                )
            })
        }
        LifecycleQuarantineStatus::QuarantinePublishUncertain => {
            report.quarantine_publish_failure().map(|failure| {
                LifecycleError::quarantine_publication_uncertain_with(
                    "quarantine object publication uncertain",
                    failure.source().clone(),
                )
            })
        }
        LifecycleQuarantineStatus::QuarantinedSourceDeleteFailed => report
            .source_delete()
            .and_then(QuarantineDeleteOutcome::failure)
            .map(|failure| {
                LifecycleError::lower_layer_with(
                    LifecycleLowerLayer::Backend,
                    "quarantine source delete failed",
                    failure.clone(),
                )
            }),
        _ => None,
    }
}

fn purge_report_error(report: &QuarantinePurgeReport) -> Option<LifecycleError> {
    if let Some(failure) = report.inventory_publish_failure() {
        return Some(LifecycleError::quarantine_publication_failed_with(
            "quarantine purge inventory rewrite failed",
            failure.source().clone(),
        ));
    }
    report.failed().iter().find_map(|failure| {
        failure.failure().map(|source| {
            LifecycleError::lower_layer_with(
                LifecycleLowerLayer::Backend,
                "quarantine purge delete failed",
                source.clone(),
            )
        })
    })
}

fn repair_report_error(report: &QuarantineReconciliationReport) -> Option<LifecycleError> {
    report.unavailable().map(|unavailable| {
        LifecycleError::quarantine_repair_inconclusive_with(
            "quarantine backend is unavailable",
            unavailable.source().clone(),
        )
    })
}

fn health_for_quarantine_status(
    status: LifecycleQuarantineStatus,
    branch_id: BranchId,
) -> Option<RecoveryHealth> {
    match status {
        LifecycleQuarantineStatus::InventoryMismatch => Some(
            RecoveryHealth::degraded(
                RecoveryDegradationClass::PolicyDowngrade,
                vec![RecoveryFault::new(
                    RecoveryFaultKind::QuarantineInventoryMismatch,
                    "quarantine inventory mismatch",
                )
                .expect("health debt")
                .with_affected_branch(branch_id)],
            )
            .expect("health debt"),
        ),
        LifecycleQuarantineStatus::QuarantinedSourceDeleteFailed
        | LifecycleQuarantineStatus::InventoryPublishFailed
        | LifecycleQuarantineStatus::InventoryPublishUncertain
        | LifecycleQuarantineStatus::QuarantinePublishFailed
        | LifecycleQuarantineStatus::QuarantinePublishUncertain
        | LifecycleQuarantineStatus::ServiceRejected
        | LifecycleQuarantineStatus::ServiceTransient => Some(
            RecoveryHealth::degraded(
                RecoveryDegradationClass::Telemetry,
                vec![RecoveryFault::new(
                    RecoveryFaultKind::IoFailure,
                    "quarantine operation has health debt",
                )
                .expect("health debt")
                .with_affected_branch(branch_id)],
            )
            .expect("health debt"),
        ),
        _ => None,
    }
}

fn health_for_reconciliation_kind(
    kind: QuarantineReconciliationKind,
    branch_id: Option<BranchId>,
) -> LifecycleResult<Option<RecoveryHealth>> {
    match kind {
        QuarantineReconciliationKind::CleanEmpty | QuarantineReconciliationKind::CleanInventory => {
            Ok(None)
        }
        QuarantineReconciliationKind::CorruptInventory
        | QuarantineReconciliationKind::UnlistedQuarantineObject
        | QuarantineReconciliationKind::MissingQuarantineObject
        | QuarantineReconciliationKind::MalformedListedObject => {
            let mut fault = RecoveryFault::new(
                RecoveryFaultKind::QuarantineInventoryMismatch,
                "quarantine inventory mismatch",
            )?;
            if let Some(branch_id) = branch_id {
                fault = fault.with_affected_branch(branch_id);
            }
            Ok(Some(RecoveryHealth::degraded(
                RecoveryDegradationClass::PolicyDowngrade,
                vec![fault],
            )?))
        }
        QuarantineReconciliationKind::BackendUnavailable => {
            let mut fault = RecoveryFault::new(
                RecoveryFaultKind::QuarantineInventoryMismatch,
                "quarantine backend unavailable",
            )?;
            if let Some(branch_id) = branch_id {
                fault = fault.with_affected_branch(branch_id);
            }
            Ok(Some(RecoveryHealth::failed(fault)))
        }
    }
}

fn delete_objects(outcomes: &[QuarantineDeleteOutcome]) -> Vec<ObjectName> {
    outcomes
        .iter()
        .map(|outcome| outcome.object().clone())
        .collect()
}

fn recovery_health_blocks_reclaim(health: &RecoveryHealth) -> bool {
    !matches!(
        health,
        RecoveryHealth::Healthy
            | RecoveryHealth::Degraded {
                class: RecoveryDegradationClass::Telemetry,
                ..
            }
    )
}

fn quarantine_service_error(source: QuarantineServiceError) -> LifecycleError {
    match source {
        QuarantineServiceError::UnsafeGate { .. } => LifecycleError::QuarantineProofBlocked {
            reason: "quarantine proof is not safe",
        },
        QuarantineServiceError::InventoryMismatch { .. }
        | QuarantineServiceError::InventoryTokenMismatch { .. }
        | QuarantineServiceError::Decode { .. }
        | QuarantineServiceError::DatabaseMismatch { .. }
        | QuarantineServiceError::BranchMismatch { .. }
        | QuarantineServiceError::CodecMismatch { .. } => {
            LifecycleError::quarantine_inventory_mismatch_with(
                "quarantine inventory mismatch",
                source,
            )
        }
        QuarantineServiceError::Publish { .. } => {
            LifecycleError::quarantine_publication_failed_with(
                "quarantine publication failed",
                source,
            )
        }
        QuarantineServiceError::InvalidRequest { field } => LifecycleError::InvalidConfig {
            field,
            reason: "quarantine request is invalid",
        },
        QuarantineServiceError::UnsupportedCapability { .. }
        | QuarantineServiceError::Layout { .. }
        | QuarantineServiceError::Missing { .. }
        | QuarantineServiceError::Read { .. }
        | QuarantineServiceError::Encode { .. }
        | QuarantineServiceError::InvalidPublishMetadata { .. }
        | QuarantineServiceError::Metadata { .. }
        | QuarantineServiceError::BackendState { .. } => LifecycleError::lower_layer_with(
            LifecycleLowerLayer::Service,
            "quarantine service failed",
            source,
        ),
    }
}

fn derived_quarantine_object_id(source_object: &ObjectName) -> String {
    let digest = Sha256::digest(source_object.as_str().as_bytes());
    let mut suffix = String::with_capacity(32);
    for byte in &digest[..16] {
        use std::fmt::Write as _;
        write!(suffix, "{byte:02x}").expect("write to String");
    }
    format!("object-{suffix}")
}
