//! API maintenance request shells.

use strata_core::BranchId;

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceTask {
    Checkpoint,
    Flush,
    Compact,
    Materialize,
    Retain,
    SnapshotPruning,
    Reclaim,
    Quarantine,
    Purge,
    Repair,
    WalGrowth,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceScope {
    Global,
    Branch(BranchId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintenanceRequest {
    task: MaintenanceTask,
    scope: MaintenanceScope,
}

impl MaintenanceRequest {
    #[must_use]
    pub const fn new(task: MaintenanceTask, scope: MaintenanceScope) -> Self {
        Self { task, scope }
    }

    #[must_use]
    pub const fn task(self) -> MaintenanceTask {
        self.task
    }

    #[must_use]
    pub const fn scope(self) -> MaintenanceScope {
        self.scope
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceSummaryStatus {
    Completed,
    Deferred,
    Failed,
    Canceled,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceReasonClass {
    Deferred,
    Failed,
    Canceled,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceWalGrowthStatus {
    Disabled,
    BelowThreshold,
    CheckpointEnqueued,
    CheckpointCoalesced,
    MaintenanceEnqueued,
    MaintenanceCoalesced,
    Deferred,
    NoDurableAction,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceWalGrowthTrigger {
    RetainedBytes,
    RetainedSegments,
    CommitsSinceCheckpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceSummary {
    task: MaintenanceTask,
    scope: MaintenanceScope,
    status: MaintenanceSummaryStatus,
    reason_class: Option<MaintenanceReasonClass>,
    reason: Option<&'static str>,
    affected_objects: usize,
    affected_object_names: Vec<String>,
    bytes_reclaimed: u64,
    retryable: bool,
    checkpoint_required: bool,
    recovery_health: Option<crate::api::RecoveryHealthSummary>,
    source_error_code: Option<&'static str>,
    source_error_class: Option<crate::api::StorageApiErrorClass>,
    state_changes: usize,
    checkpoint_watermark: Option<strata_core::CommitVersion>,
    snapshot_id: Option<u64>,
    rows_processed: u64,
    wal_truncated: bool,
    wal_growth: Option<MaintenanceWalGrowthSummary>,
}

/// Detail for one recorded maintenance failure: the exact task class that
/// failed (finer-grained than [`MaintenanceTask`] — e.g. `"flush_watermark"`
/// and `"wal_truncation"` are distinct) plus the failure's reason and error
/// code. Without this, `failed` counts are unclassifiable after the fact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintenanceFailureSummary {
    task_kind: &'static str,
    reason: Option<&'static str>,
    source_error_code: Option<&'static str>,
}

impl MaintenanceFailureSummary {
    #[must_use]
    pub(crate) const fn new(
        task_kind: &'static str,
        reason: Option<&'static str>,
        source_error_code: Option<&'static str>,
    ) -> Self {
        Self {
            task_kind,
            reason,
            source_error_code,
        }
    }

    #[must_use]
    pub const fn task_kind(self) -> &'static str {
        self.task_kind
    }

    #[must_use]
    pub const fn reason(self) -> Option<&'static str> {
        self.reason
    }

    #[must_use]
    pub const fn source_error_code(self) -> Option<&'static str> {
        self.source_error_code
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintenanceQueueSummary {
    pending_tasks: usize,
    active_task: Option<u64>,
    enqueued: usize,
    coalesced: usize,
    max_pending_tasks: usize,
    started: usize,
    completed: usize,
    deferred: usize,
    failed: usize,
    canceled: usize,
    drained: usize,
    queue_full: usize,
    background_worker_count: usize,
    background_queue_depth: usize,
    background_active_tasks: usize,
    background_tasks_completed: u64,
    recent_failures:
        [Option<MaintenanceFailureSummary>; crate::lifecycle::MAINTENANCE_FAILURE_RECORD_CAPACITY],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceDrainSummary {
    drained_tasks: usize,
    outcomes: Vec<MaintenanceSummary>,
    queue: MaintenanceQueueSummary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintenanceWalGrowthSummary {
    status: MaintenanceWalGrowthStatus,
    retained_bytes: u64,
    retained_segments: u64,
    commits_since_checkpoint: u64,
    trigger: Option<MaintenanceWalGrowthTrigger>,
    checkpoint_enqueued: bool,
    recovery_health: Option<crate::api::RecoveryHealthSummary>,
    source_error_code: Option<&'static str>,
    source_error_class: Option<crate::api::StorageApiErrorClass>,
}

impl MaintenanceSummary {
    #[must_use]
    pub(crate) fn new(
        task: MaintenanceTask,
        scope: MaintenanceScope,
        status: MaintenanceSummaryStatus,
    ) -> Self {
        Self {
            task,
            scope,
            status,
            reason_class: None,
            reason: None,
            affected_objects: 0,
            affected_object_names: Vec::new(),
            bytes_reclaimed: 0,
            retryable: false,
            checkpoint_required: false,
            recovery_health: None,
            source_error_code: None,
            source_error_class: None,
            state_changes: 0,
            checkpoint_watermark: None,
            snapshot_id: None,
            rows_processed: 0,
            wal_truncated: false,
            wal_growth: None,
        }
    }

    #[must_use]
    pub(crate) const fn with_reason(
        mut self,
        reason_class: Option<MaintenanceReasonClass>,
        reason: Option<&'static str>,
    ) -> Self {
        self.reason_class = reason_class;
        self.reason = reason;
        self
    }

    #[must_use]
    pub(crate) fn with_affected_objects(mut self, names: Vec<String>, count: usize) -> Self {
        self.affected_object_names = names;
        self.affected_objects = count;
        self
    }

    #[must_use]
    pub(crate) const fn with_effects(
        mut self,
        bytes_reclaimed: u64,
        retryable: bool,
        checkpoint_required: bool,
        state_changes: usize,
    ) -> Self {
        self.bytes_reclaimed = bytes_reclaimed;
        self.retryable = retryable;
        self.checkpoint_required = checkpoint_required;
        self.state_changes = state_changes;
        self
    }

    #[must_use]
    pub(crate) const fn with_health(
        mut self,
        recovery_health: Option<crate::api::RecoveryHealthSummary>,
    ) -> Self {
        self.recovery_health = recovery_health;
        self
    }

    #[must_use]
    pub(crate) const fn with_source_error(
        mut self,
        code: Option<&'static str>,
        class: Option<crate::api::StorageApiErrorClass>,
    ) -> Self {
        self.source_error_code = code;
        self.source_error_class = class;
        self
    }

    #[must_use]
    pub(crate) const fn with_checkpoint_facts(
        mut self,
        checkpoint_watermark: Option<strata_core::CommitVersion>,
        snapshot_id: Option<u64>,
        rows_processed: u64,
        wal_truncated: bool,
    ) -> Self {
        self.checkpoint_watermark = checkpoint_watermark;
        self.snapshot_id = snapshot_id;
        self.rows_processed = rows_processed;
        self.wal_truncated = wal_truncated;
        self
    }

    #[must_use]
    pub(crate) const fn with_rows_processed(mut self, rows_processed: u64) -> Self {
        self.rows_processed = rows_processed;
        self
    }

    #[must_use]
    pub(crate) const fn with_wal_growth(mut self, wal_growth: MaintenanceWalGrowthSummary) -> Self {
        self.wal_growth = Some(wal_growth);
        self
    }

    #[must_use]
    pub const fn task(&self) -> MaintenanceTask {
        self.task
    }

    #[must_use]
    pub const fn scope(&self) -> MaintenanceScope {
        self.scope
    }

    #[must_use]
    pub const fn status(&self) -> MaintenanceSummaryStatus {
        self.status
    }

    #[must_use]
    pub const fn reason_class(&self) -> Option<MaintenanceReasonClass> {
        self.reason_class
    }

    #[must_use]
    pub const fn reason(&self) -> Option<&'static str> {
        self.reason
    }

    #[must_use]
    pub const fn affected_objects(&self) -> usize {
        self.affected_objects
    }

    #[must_use]
    pub fn affected_object_names(&self) -> &[String] {
        &self.affected_object_names
    }

    #[must_use]
    pub const fn bytes_reclaimed(&self) -> u64 {
        self.bytes_reclaimed
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        self.retryable
    }

    #[must_use]
    pub const fn checkpoint_required(&self) -> bool {
        self.checkpoint_required
    }

    #[must_use]
    pub const fn recovery_health(&self) -> Option<crate::api::RecoveryHealthSummary> {
        self.recovery_health
    }

    #[must_use]
    pub const fn source_error_code(&self) -> Option<&'static str> {
        self.source_error_code
    }

    #[must_use]
    pub const fn source_error_class(&self) -> Option<crate::api::StorageApiErrorClass> {
        self.source_error_class
    }

    #[must_use]
    pub const fn state_changes(&self) -> usize {
        self.state_changes
    }

    #[must_use]
    pub const fn checkpoint_watermark(&self) -> Option<strata_core::CommitVersion> {
        self.checkpoint_watermark
    }

    #[must_use]
    pub const fn snapshot_id(&self) -> Option<u64> {
        self.snapshot_id
    }

    #[must_use]
    pub const fn rows_processed(&self) -> u64 {
        self.rows_processed
    }

    #[must_use]
    pub const fn wal_truncated(&self) -> bool {
        self.wal_truncated
    }

    #[must_use]
    pub const fn wal_growth(&self) -> Option<MaintenanceWalGrowthSummary> {
        self.wal_growth
    }
}

impl MaintenanceQueueSummary {
    #[expect(
        clippy::too_many_arguments,
        reason = "queue summary is a flat diagnostic view of executor counters"
    )]
    #[must_use]
    pub(crate) const fn new(
        pending_tasks: usize,
        active_task: Option<u64>,
        enqueued: usize,
        coalesced: usize,
        max_pending_tasks: usize,
        started: usize,
        completed: usize,
        deferred: usize,
        failed: usize,
        canceled: usize,
        drained: usize,
        queue_full: usize,
        background_worker_count: usize,
        background_queue_depth: usize,
        background_active_tasks: usize,
        background_tasks_completed: u64,
    ) -> Self {
        Self {
            pending_tasks,
            active_task,
            enqueued,
            coalesced,
            max_pending_tasks,
            started,
            completed,
            deferred,
            failed,
            canceled,
            drained,
            queue_full,
            background_worker_count,
            background_queue_depth,
            background_active_tasks,
            background_tasks_completed,
            recent_failures: [None; crate::lifecycle::MAINTENANCE_FAILURE_RECORD_CAPACITY],
        }
    }

    #[must_use]
    pub(crate) const fn with_recent_failures(
        mut self,
        recent_failures: [Option<MaintenanceFailureSummary>;
            crate::lifecycle::MAINTENANCE_FAILURE_RECORD_CAPACITY],
    ) -> Self {
        self.recent_failures = recent_failures;
        self
    }

    #[must_use]
    pub const fn pending_tasks(self) -> usize {
        self.pending_tasks
    }

    #[must_use]
    pub const fn active_task(self) -> Option<u64> {
        self.active_task
    }

    #[must_use]
    pub const fn enqueued(self) -> usize {
        self.enqueued
    }

    #[must_use]
    pub const fn coalesced(self) -> usize {
        self.coalesced
    }

    #[must_use]
    pub const fn max_pending_tasks(self) -> usize {
        self.max_pending_tasks
    }

    #[must_use]
    pub const fn started(self) -> usize {
        self.started
    }

    #[must_use]
    pub const fn completed(self) -> usize {
        self.completed
    }

    #[must_use]
    pub const fn deferred(self) -> usize {
        self.deferred
    }

    #[must_use]
    pub const fn failed(self) -> usize {
        self.failed
    }

    #[must_use]
    pub const fn canceled(self) -> usize {
        self.canceled
    }

    #[must_use]
    pub const fn drained(self) -> usize {
        self.drained
    }

    #[must_use]
    pub const fn queue_full(self) -> usize {
        self.queue_full
    }

    #[must_use]
    pub const fn background_worker_count(self) -> usize {
        self.background_worker_count
    }

    #[must_use]
    pub const fn background_queue_depth(self) -> usize {
        self.background_queue_depth
    }

    #[must_use]
    pub const fn background_active_tasks(self) -> usize {
        self.background_active_tasks
    }

    #[must_use]
    pub const fn background_tasks_completed(self) -> u64 {
        self.background_tasks_completed
    }

    /// The newest recorded maintenance failures, oldest first. Empty unless
    /// `failed() > 0`; bounded, so only the most recent failures survive.
    #[must_use]
    pub fn recent_failures(&self) -> Vec<MaintenanceFailureSummary> {
        self.recent_failures.iter().flatten().copied().collect()
    }
}

impl MaintenanceDrainSummary {
    // See the note in `diagnostics.rs`: target-dependent, so `allow`.
    #[allow(
        clippy::large_types_passed_by_value,
        reason = "constructor stores the queue summary; callers move it in"
    )]
    #[must_use]
    pub(crate) fn new(
        drained_tasks: usize,
        outcomes: Vec<MaintenanceSummary>,
        queue: MaintenanceQueueSummary,
    ) -> Self {
        Self {
            drained_tasks,
            outcomes,
            queue,
        }
    }

    #[must_use]
    pub const fn drained_tasks(&self) -> usize {
        self.drained_tasks
    }

    #[must_use]
    pub fn outcomes(&self) -> &[MaintenanceSummary] {
        &self.outcomes
    }

    #[must_use]
    pub const fn queue(&self) -> MaintenanceQueueSummary {
        self.queue
    }
}

impl MaintenanceWalGrowthSummary {
    #[expect(
        clippy::too_many_arguments,
        reason = "WAL growth summary mirrors lower-layer policy facts"
    )]
    #[must_use]
    pub(crate) const fn new(
        status: MaintenanceWalGrowthStatus,
        retained_bytes: u64,
        retained_segments: u64,
        commits_since_checkpoint: u64,
        trigger: Option<MaintenanceWalGrowthTrigger>,
        checkpoint_enqueued: bool,
        recovery_health: Option<crate::api::RecoveryHealthSummary>,
        source_error_code: Option<&'static str>,
        source_error_class: Option<crate::api::StorageApiErrorClass>,
    ) -> Self {
        Self {
            status,
            retained_bytes,
            retained_segments,
            commits_since_checkpoint,
            trigger,
            checkpoint_enqueued,
            recovery_health,
            source_error_code,
            source_error_class,
        }
    }

    #[must_use]
    pub const fn status(self) -> MaintenanceWalGrowthStatus {
        self.status
    }

    #[must_use]
    pub const fn retained_bytes(self) -> u64 {
        self.retained_bytes
    }

    #[must_use]
    pub const fn retained_segments(self) -> u64 {
        self.retained_segments
    }

    #[must_use]
    pub const fn commits_since_checkpoint(self) -> u64 {
        self.commits_since_checkpoint
    }

    #[must_use]
    pub const fn trigger(self) -> Option<MaintenanceWalGrowthTrigger> {
        self.trigger
    }

    #[must_use]
    pub const fn checkpoint_enqueued(self) -> bool {
        self.checkpoint_enqueued
    }

    #[must_use]
    pub const fn recovery_health(self) -> Option<crate::api::RecoveryHealthSummary> {
        self.recovery_health
    }

    #[must_use]
    pub const fn source_error_code(self) -> Option<&'static str> {
        self.source_error_code
    }

    #[must_use]
    pub const fn source_error_class(self) -> Option<crate::api::StorageApiErrorClass> {
        self.source_error_class
    }
}
