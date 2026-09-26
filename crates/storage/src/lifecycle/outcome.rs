//! Lifecycle outcome facts.

use super::{
    ClosePhase, LifecycleCloseFact, LifecycleError, LifecycleRecoveredCheckpoint,
    LifecycleRecoveredQuarantine, LifecycleRecoveredTables, LifecycleRecoveredWal,
    LifecycleRecoveryBootstrapReport, LifecycleRecoveryOutcome, LifecycleResult, LifecycleStats,
    MaintenanceTaskKind, RecoveryHealth, StorageBudgetSnapshot, StorageMode,
};
use crate::backend::BackendCapabilities;
use crate::lifecycle::maintenance::{MaintenanceTaskId, MaintenanceTaskScope};
use strata_core::CommitVersion;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageOpenOutcome {
    mode: StorageMode,
    disposition: StorageOpenDisposition,
    recovered_visible_version: Option<CommitVersion>,
    recovery_health: RecoveryHealth,
    maintenance_ready: bool,
    backend_capabilities: Option<BackendCapabilities>,
    database_id: Option<[u8; 16]>,
    codec_id: Option<String>,
    recovered_max_commit_version: Option<CommitVersion>,
    checkpoint: Option<LifecycleRecoveredCheckpoint>,
    wal: Option<LifecycleRecoveredWal>,
    tables: Option<LifecycleRecoveredTables>,
    quarantine: Option<LifecycleRecoveredQuarantine>,
    bootstrap: Option<LifecycleRecoveryBootstrapReport>,
    budget: Option<StorageBudgetSnapshot>,
    stats: LifecycleStats,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StorageOpenDisposition {
    Created,
    OpenedExisting,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceOutcome {
    task_id: Option<MaintenanceTaskId>,
    task_scope: Option<MaintenanceTaskScope>,
    task_kind: MaintenanceTaskKind,
    status: MaintenanceOutcomeStatus,
    recovery_health: Option<RecoveryHealth>,
    affected_objects: usize,
    affected_object_names: Vec<String>,
    bytes_reclaimed: u64,
    retryable: bool,
    reason: Option<&'static str>,
    reason_class: Option<MaintenanceOutcomeReasonClass>,
    deferral: Option<MaintenanceDeferralReason>,
    source_error: Option<LifecycleError>,
    state_changes: usize,
    checkpoint_required: bool,
    stats: LifecycleStats,
    wal_truncation_follow_up: Option<WalTruncationFollowUp>,
}

/// A WAL truncation that ran as a checkpoint's follow-up rather than as its
/// own task, carried on the checkpoint's outcome so the reclaim ledger sees
/// it (space-reclamation contract §3.5).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WalTruncationFollowUp {
    completed: bool,
    deleted_segments: usize,
}

impl WalTruncationFollowUp {
    pub(crate) const fn new(completed: bool, deleted_segments: usize) -> Self {
        Self {
            completed,
            deleted_segments,
        }
    }

    pub(crate) const fn completed(self) -> bool {
        self.completed
    }

    pub(crate) const fn deleted_segments(self) -> usize {
        self.deleted_segments
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceOutcomeStatus {
    Completed,
    Deferred,
    Failed,
    Canceled,
}

/// Why a reclaim pass deferred, as a typed fact beside the human `reason`
/// text. Runners set it from their own typed status so the reclaim ledger
/// never classifies from prose (CLAUDE.md rule 39).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceDeferralReason {
    /// A retired read view still names objects the sweep would delete.
    ReaderPinned,
    /// A live manifest or in-flight publication still references the object.
    Referenced,
    /// The retention, purge or truncation proof was not complete.
    IncompleteProof,
    /// The proof was built against state that has since changed.
    StaleProof,
    /// Recovery health forbids listing or mutating durable objects.
    RecoveryHealth,
    /// A concurrent sweep advanced the quarantine inventory past the proof.
    InventoryAdvanced,
    /// The generic retention path does not serve the requested scope.
    UnsupportedScope,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceOutcomeReasonClass {
    Deferred,
    Failed,
    Canceled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CloseOutcome {
    phase: ClosePhase,
    status: CloseOutcomeStatus,
    close_fact: Option<LifecycleCloseFact>,
    effects: CloseOutcomeEffects,
    stats: LifecycleStats,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CloseOutcomeEffects {
    bits: u8,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CloseOutcomeStatus {
    Complete,
    Idempotent,
    Timeout,
    Failed,
}

impl StorageOpenOutcome {
    pub(crate) fn new(
        mode: StorageMode,
        disposition: StorageOpenDisposition,
        recovered_visible_version: Option<CommitVersion>,
        recovery_health: RecoveryHealth,
        maintenance_ready: bool,
    ) -> LifecycleResult<Self> {
        if matches!(mode, StorageMode::Cache) && recovered_visible_version.is_some() {
            return Err(LifecycleError::InvalidOpenPlan {
                reason: "cache mode cannot report recovered durable visibility",
            });
        }
        if matches!(mode, StorageMode::Cache) && !recovery_health.is_healthy() {
            return Err(LifecycleError::InvalidOpenPlan {
                reason: "cache mode cannot report durable recovery degradation",
            });
        }
        Ok(Self {
            mode,
            disposition,
            recovered_visible_version,
            recovery_health,
            maintenance_ready,
            backend_capabilities: None,
            database_id: None,
            codec_id: None,
            recovered_max_commit_version: recovered_visible_version,
            checkpoint: None,
            wal: None,
            tables: None,
            quarantine: None,
            bootstrap: None,
            budget: None,
            stats: LifecycleStats::default(),
        })
    }

    pub(crate) fn with_backend_capabilities(mut self, capabilities: BackendCapabilities) -> Self {
        self.backend_capabilities = Some(capabilities);
        self
    }

    pub(crate) fn with_database_identity(
        mut self,
        database_id: [u8; 16],
        codec_id: impl Into<String>,
    ) -> Self {
        self.database_id = Some(database_id);
        self.codec_id = Some(codec_id.into());
        self
    }

    pub(crate) const fn with_recovered_max_commit_version(
        mut self,
        version: Option<CommitVersion>,
    ) -> Self {
        self.recovered_max_commit_version = version;
        self
    }

    pub(crate) fn with_durable_recovery_facts(
        mut self,
        recovery: &LifecycleRecoveryOutcome,
        bootstrap: &LifecycleRecoveryBootstrapReport,
    ) -> Self {
        self.checkpoint = Some(recovery.checkpoint().clone());
        self.wal = Some(recovery.wal().clone());
        self.tables = Some(recovery.tables().clone());
        self.quarantine = Some(recovery.quarantine().clone());
        self.bootstrap = Some(bootstrap.clone());
        self.recovered_max_commit_version = Some(bootstrap.recovered_visible_version());
        self
    }

    pub(crate) const fn with_stats(mut self, stats: LifecycleStats) -> Self {
        self.stats = stats;
        self
    }

    pub(crate) fn with_budget_snapshot(mut self, budget: StorageBudgetSnapshot) -> Self {
        self.budget = Some(budget);
        self
    }

    pub(crate) const fn mode(&self) -> StorageMode {
        self.mode
    }

    pub(crate) const fn disposition(&self) -> StorageOpenDisposition {
        self.disposition
    }

    pub(crate) const fn opened_existing(&self) -> bool {
        matches!(self.disposition, StorageOpenDisposition::OpenedExisting)
    }

    pub(crate) const fn recovered_visible_version(&self) -> Option<CommitVersion> {
        self.recovered_visible_version
    }

    pub(crate) const fn recovery_health(&self) -> &RecoveryHealth {
        &self.recovery_health
    }

    pub(crate) const fn maintenance_ready(&self) -> bool {
        self.maintenance_ready
    }

    pub(crate) const fn backend_capabilities(&self) -> Option<BackendCapabilities> {
        self.backend_capabilities
    }

    pub(crate) const fn database_id(&self) -> Option<&[u8; 16]> {
        self.database_id.as_ref()
    }

    pub(crate) fn codec_id(&self) -> Option<&str> {
        self.codec_id.as_deref()
    }

    pub(crate) const fn recovered_max_commit_version(&self) -> Option<CommitVersion> {
        self.recovered_max_commit_version
    }

    pub(crate) const fn checkpoint(&self) -> Option<&LifecycleRecoveredCheckpoint> {
        self.checkpoint.as_ref()
    }

    pub(crate) const fn wal(&self) -> Option<&LifecycleRecoveredWal> {
        self.wal.as_ref()
    }

    pub(crate) const fn tables(&self) -> Option<&LifecycleRecoveredTables> {
        self.tables.as_ref()
    }

    pub(crate) const fn quarantine(&self) -> Option<&LifecycleRecoveredQuarantine> {
        self.quarantine.as_ref()
    }

    pub(crate) const fn bootstrap(&self) -> Option<&LifecycleRecoveryBootstrapReport> {
        self.bootstrap.as_ref()
    }

    pub(crate) const fn budget(&self) -> Option<&StorageBudgetSnapshot> {
        self.budget.as_ref()
    }

    pub(crate) const fn stats(&self) -> LifecycleStats {
        self.stats
    }
}

impl MaintenanceOutcome {
    pub(crate) const fn new(
        task_kind: MaintenanceTaskKind,
        status: MaintenanceOutcomeStatus,
    ) -> Self {
        Self {
            task_id: None,
            task_scope: None,
            task_kind,
            status,
            recovery_health: None,
            affected_objects: 0,
            affected_object_names: Vec::new(),
            bytes_reclaimed: 0,
            retryable: false,
            reason: None,
            reason_class: None,
            deferral: None,
            source_error: None,
            state_changes: 0,
            checkpoint_required: false,
            stats: LifecycleStats::new(0, 0, 0, 0, 0),
            wal_truncation_follow_up: None,
        }
    }

    pub(crate) const fn with_deferral_reason(mut self, reason: MaintenanceDeferralReason) -> Self {
        self.deferral = Some(reason);
        self
    }

    pub(crate) const fn deferral_reason(&self) -> Option<MaintenanceDeferralReason> {
        self.deferral
    }

    pub(crate) const fn with_wal_truncation_follow_up(
        mut self,
        follow_up: WalTruncationFollowUp,
    ) -> Self {
        self.wal_truncation_follow_up = Some(follow_up);
        self
    }

    pub(crate) const fn wal_truncation_follow_up(&self) -> Option<WalTruncationFollowUp> {
        self.wal_truncation_follow_up
    }

    pub(crate) fn with_recovery_health(mut self, health: RecoveryHealth) -> Self {
        self.recovery_health = Some(health);
        self
    }

    pub(crate) const fn with_status(mut self, status: MaintenanceOutcomeStatus) -> Self {
        self.status = status;
        if self.reason.is_some() {
            self.reason_class = reason_class_for_status(status);
        }
        self
    }

    pub(crate) const fn with_effects(
        mut self,
        affected_objects: usize,
        bytes_reclaimed: u64,
        retryable: bool,
    ) -> Self {
        self.affected_objects = affected_objects;
        self.bytes_reclaimed = bytes_reclaimed;
        self.retryable = retryable;
        self
    }

    pub(crate) fn with_affected_object_names(mut self, names: Vec<String>) -> Self {
        self.affected_objects = names.len();
        self.affected_object_names = names;
        self
    }

    pub(crate) const fn with_reason(mut self, reason: &'static str) -> Self {
        self.reason = Some(reason);
        self.reason_class = reason_class_for_status(self.status);
        self
    }

    pub(crate) fn with_source_error(mut self, error: LifecycleError) -> Self {
        self.source_error = Some(error);
        self
    }

    pub(crate) const fn with_state_changes(mut self, state_changes: usize) -> Self {
        self.state_changes = state_changes;
        self
    }

    pub(crate) const fn with_checkpoint_required(mut self, checkpoint_required: bool) -> Self {
        self.checkpoint_required = checkpoint_required;
        self
    }

    pub(crate) const fn with_task_id(mut self, task_id: MaintenanceTaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }

    pub(crate) const fn with_task_scope(mut self, task_scope: MaintenanceTaskScope) -> Self {
        self.task_scope = Some(task_scope);
        self
    }

    pub(crate) const fn with_stats(mut self, stats: LifecycleStats) -> Self {
        self.stats = stats;
        self
    }

    pub(crate) const fn task_id(&self) -> Option<MaintenanceTaskId> {
        self.task_id
    }

    pub(crate) const fn task_scope(&self) -> Option<MaintenanceTaskScope> {
        self.task_scope
    }

    pub(crate) const fn task_kind(&self) -> MaintenanceTaskKind {
        self.task_kind
    }

    pub(crate) const fn status(&self) -> MaintenanceOutcomeStatus {
        self.status
    }

    pub(crate) const fn recovery_health(&self) -> Option<&RecoveryHealth> {
        self.recovery_health.as_ref()
    }

    pub(crate) const fn affected_objects(&self) -> usize {
        self.affected_objects
    }

    pub(crate) fn affected_object_names(&self) -> &[String] {
        &self.affected_object_names
    }

    pub(crate) const fn bytes_reclaimed(&self) -> u64 {
        self.bytes_reclaimed
    }

    pub(crate) const fn retryable(&self) -> bool {
        self.retryable
    }

    pub(crate) const fn reason(&self) -> Option<&'static str> {
        self.reason
    }

    pub(crate) const fn reason_class(&self) -> Option<MaintenanceOutcomeReasonClass> {
        self.reason_class
    }

    pub(crate) const fn source_error(&self) -> Option<&LifecycleError> {
        self.source_error.as_ref()
    }

    pub(crate) const fn state_changes(&self) -> usize {
        self.state_changes
    }

    pub(crate) const fn checkpoint_required(&self) -> bool {
        self.checkpoint_required
    }

    pub(crate) const fn stats(&self) -> LifecycleStats {
        self.stats
    }
}

const fn reason_class_for_status(
    status: MaintenanceOutcomeStatus,
) -> Option<MaintenanceOutcomeReasonClass> {
    match status {
        MaintenanceOutcomeStatus::Completed => None,
        MaintenanceOutcomeStatus::Deferred => Some(MaintenanceOutcomeReasonClass::Deferred),
        MaintenanceOutcomeStatus::Failed => Some(MaintenanceOutcomeReasonClass::Failed),
        MaintenanceOutcomeStatus::Canceled => Some(MaintenanceOutcomeReasonClass::Canceled),
    }
}

impl CloseOutcome {
    pub(crate) const fn new(phase: ClosePhase, status: CloseOutcomeStatus) -> Self {
        Self {
            phase,
            status,
            close_fact: None,
            effects: CloseOutcomeEffects::empty(),
            stats: LifecycleStats::new(0, 0, 0, 0, 0),
        }
    }

    pub(crate) const fn with_close_fact(mut self, close_fact: LifecycleCloseFact) -> Self {
        self.close_fact = Some(close_fact);
        self
    }

    pub(crate) const fn with_close_effects(mut self, effects: CloseOutcomeEffects) -> Self {
        self.effects = effects;
        self
    }

    pub(crate) const fn with_stats(mut self, stats: LifecycleStats) -> Self {
        self.stats = stats;
        self
    }

    pub(crate) fn validate(&self) -> LifecycleResult<()> {
        match self.status {
            CloseOutcomeStatus::Complete => {
                if self.phase != ClosePhase::Closed {
                    return Err(LifecycleError::CloseFailed {
                        reason: "complete close outcome must report closed phase",
                    });
                }
                if self.close_fact != Some(LifecycleCloseFact::Complete) {
                    return Err(LifecycleError::CloseFailed {
                        reason: "complete close outcome must report complete fact",
                    });
                }
                if !self.commits_quiesced()
                    || !self.maintenance_drained()
                    || !self.guards_released()
                {
                    return Err(LifecycleError::CloseFailed {
                        reason: "complete close outcome must report completed close effects",
                    });
                }
            }
            CloseOutcomeStatus::Idempotent => {
                if self.phase != ClosePhase::Closed {
                    return Err(LifecycleError::CloseFailed {
                        reason: "idempotent close outcome must report closed phase",
                    });
                }
                if self.close_fact != Some(LifecycleCloseFact::AlreadyClosed) || !self.prior_final()
                {
                    return Err(LifecycleError::CloseFailed {
                        reason: "idempotent close outcome must report prior final fact",
                    });
                }
            }
            CloseOutcomeStatus::Timeout => {
                if self.phase == ClosePhase::Closed
                    || matches!(
                        self.close_fact,
                        Some(LifecycleCloseFact::Complete | LifecycleCloseFact::AlreadyClosed)
                    )
                {
                    return Err(LifecycleError::CloseFailed {
                        reason: "timeout close outcome must not report final close facts",
                    });
                }
            }
            CloseOutcomeStatus::Failed => {
                if matches!(
                    self.close_fact,
                    Some(LifecycleCloseFact::Complete | LifecycleCloseFact::AlreadyClosed)
                ) {
                    return Err(LifecycleError::CloseFailed {
                        reason: "failed close outcome must not report final close facts",
                    });
                }
            }
        }
        Ok(())
    }

    pub(crate) const fn phase(self) -> ClosePhase {
        self.phase
    }

    pub(crate) const fn status(self) -> CloseOutcomeStatus {
        self.status
    }

    pub(crate) const fn close_fact(self) -> Option<LifecycleCloseFact> {
        self.close_fact
    }

    pub(crate) const fn commits_quiesced(self) -> bool {
        self.effects.commits_quiesced()
    }

    pub(crate) const fn maintenance_drained(self) -> bool {
        self.effects.maintenance_drained()
    }

    pub(crate) const fn durable_synced(self) -> bool {
        self.effects.durable_synced()
    }

    pub(crate) const fn guards_released(self) -> bool {
        self.effects.guards_released()
    }

    pub(crate) const fn prior_final(self) -> bool {
        self.effects.prior_final()
    }

    pub(crate) const fn stats(self) -> LifecycleStats {
        self.stats
    }
}

impl CloseOutcomeEffects {
    const COMMITS_QUIESCED: u8 = 1 << 0;
    const MAINTENANCE_DRAINED: u8 = 1 << 1;
    const DURABLE_SYNCED: u8 = 1 << 2;
    const GUARDS_RELEASED: u8 = 1 << 3;
    const PRIOR_FINAL: u8 = 1 << 4;

    pub(crate) const fn empty() -> Self {
        Self { bits: 0 }
    }

    pub(crate) const fn volatile_complete(prior_final: bool) -> Self {
        let prior = if prior_final { Self::PRIOR_FINAL } else { 0 };
        Self {
            bits: Self::COMMITS_QUIESCED
                | Self::MAINTENANCE_DRAINED
                | Self::GUARDS_RELEASED
                | prior,
        }
    }

    pub(crate) const fn durable_complete(prior_final: bool) -> Self {
        let prior = if prior_final { Self::PRIOR_FINAL } else { 0 };
        Self {
            bits: Self::COMMITS_QUIESCED
                | Self::MAINTENANCE_DRAINED
                | Self::DURABLE_SYNCED
                | Self::GUARDS_RELEASED
                | prior,
        }
    }

    pub(crate) const fn commits_quiesced(self) -> bool {
        self.bits & Self::COMMITS_QUIESCED != 0
    }

    pub(crate) const fn maintenance_drained(self) -> bool {
        self.bits & Self::MAINTENANCE_DRAINED != 0
    }

    pub(crate) const fn durable_synced(self) -> bool {
        self.bits & Self::DURABLE_SYNCED != 0
    }

    pub(crate) const fn guards_released(self) -> bool {
        self.bits & Self::GUARDS_RELEASED != 0
    }

    pub(crate) const fn prior_final(self) -> bool {
        self.bits & Self::PRIOR_FINAL != 0
    }
}
