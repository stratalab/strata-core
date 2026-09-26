//! Deterministic lifecycle maintenance task executor.

#![allow(
    dead_code,
    reason = "executor hooks are consumed by concrete maintenance modules"
)]

use super::{
    classify_reclaim, classify_reclaim_follow_up, LifecycleAdmissionEffect, LifecycleError,
    LifecycleMaintenanceSchedulingPolicy, LifecycleOperationAdmission, LifecycleOperationKind,
    LifecycleResult, LifecycleStateMachine, LifecycleStats, LifecycleStoragePressure,
    LifecycleStoragePressureSeverity, MaintenanceOutcome, MaintenanceOutcomeStatus,
    MaintenanceTaskKind, ReclaimLedger, RecoveryDegradationClass, RecoveryFault, RecoveryFaultKind,
    RecoveryHealth,
};
use crate::branch::state::materialization::BranchMaterializationHandle;
use crate::observability::perf_trace;
use std::collections::HashSet;
use strata_core::{BranchId, CommitVersion};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct MaintenanceTaskId(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum MaintenanceTaskPriority {
    Critical,
    High,
    Normal,
    Low,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum MaintenanceTaskScope {
    Global,
    Branch(BranchId),
    Wal,
    Checkpoint,
    Quarantine,
    Retention,
    TableLevel {
        branch_id: BranchId,
        level: u8,
    },
    InheritedLayer {
        branch_id: BranchId,
        layer_index: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum MaintenanceClosePolicy {
    Ordinary,
    DrainBeforeClose,
    CancelBeforeClose,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceTaskPolicy {
    close_policy: MaintenanceClosePolicy,
    coalesce: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceTaskRequest {
    kind: MaintenanceTaskKind,
    priority: MaintenanceTaskPriority,
    scope: MaintenanceTaskScope,
    policy: MaintenanceTaskPolicy,
    checkpoint_options: Option<MaintenanceCheckpointOptions>,
    flush_watermark_candidate: Option<CommitVersion>,
    retention_options: Option<MaintenanceRetentionOptions>,
    materialization_handle: Option<BranchMaterializationHandle>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceCheckpointOptions {
    snapshot_id: Option<u64>,
    truncate_wal_after_checkpoint: bool,
    retention_critical: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceRetentionOptions {
    retain_newest_snapshots: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceTask {
    id: MaintenanceTaskId,
    sequence: u64,
    request: MaintenanceTaskRequest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceCoalesceKey {
    kind: MaintenanceTaskKind,
    scope: MaintenanceTaskScope,
    checkpoint_options: Option<MaintenanceCheckpointOptions>,
    flush_watermark_candidate: Option<CommitVersion>,
    retention_options: Option<MaintenanceRetentionOptions>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceEnqueueOutcome {
    task_id: MaintenanceTaskId,
    coalesced: bool,
    pending_tasks: usize,
    stats: LifecycleMaintenanceStats,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceDrainOutcome {
    drained_tasks: usize,
    outcomes: Vec<MaintenanceOutcome>,
    stats: LifecycleMaintenanceStats,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceCancelOutcome {
    canceled_tasks: usize,
    stats: LifecycleMaintenanceStats,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LifecyclePostCommitMaintenanceOutcome {
    status: LifecyclePostCommitMaintenanceStatus,
    pressure: LifecycleStoragePressure,
    suggested_task: Option<MaintenanceTaskRequest>,
    enqueue: Option<MaintenanceEnqueueOutcome>,
    failure: Option<LifecycleError>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleWriteAdmissionOutcome {
    status: LifecycleWriteAdmissionStatus,
    pressure: LifecycleStoragePressure,
    cleared_prior_rejection: bool,
    inline_maintenance_driven: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecyclePostCommitMaintenanceStatus {
    Disabled,
    NoSuggestedTask,
    Enqueued,
    Coalesced,
    Deferred,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum LifecycleWriteAdmissionStatus {
    AcceptedClean,
    AcceptedUnderPressure,
}

/// How many failed-task detail records the executor retains. The counters alone
/// cannot classify a failure after the fact (#2763: seven red nightlies whose
/// logs said only `failed: 4`); the ring keeps the newest failures' kind,
/// reason, and error code for the status/summary surfaces.
pub(crate) const MAINTENANCE_FAILURE_RECORD_CAPACITY: usize = 4;

/// Detail captured when a maintenance task fails: which task class failed and
/// why, in code form. Everything is `Copy` so the record rides the `Copy`
/// status/summary types unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceFailureRecord {
    kind: MaintenanceTaskKind,
    reason: Option<&'static str>,
    source_error_code: Option<&'static str>,
}

impl MaintenanceFailureRecord {
    fn from_outcome(outcome: &MaintenanceOutcome) -> Self {
        Self {
            kind: outcome.task_kind(),
            reason: outcome.reason(),
            source_error_code: outcome.source_error().map(LifecycleError::code),
        }
    }

    fn from_error(kind: MaintenanceTaskKind, error: &LifecycleError) -> Self {
        Self {
            kind,
            reason: None,
            source_error_code: Some(error.code()),
        }
    }

    pub(crate) const fn task_kind(self) -> MaintenanceTaskKind {
        self.kind
    }

    pub(crate) const fn reason(self) -> Option<&'static str> {
        self.reason
    }

    pub(crate) const fn source_error_code(self) -> Option<&'static str> {
        self.source_error_code
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MaintenanceExecutorStatus {
    pending_tasks: usize,
    active_task: Option<MaintenanceTaskId>,
    active_tasks: usize,
    stats: LifecycleMaintenanceStats,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LifecycleMaintenanceStats {
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
}

#[derive(Debug)]
pub(crate) struct LifecycleMaintenanceExecutor {
    next_id: u64,
    max_queue_depth: usize,
    queue: Vec<MaintenanceTask>,
    active: Vec<MaintenanceTask>,
    stats: LifecycleMaintenanceStats,
    /// Ring of the newest failed-task details; `stats.failed` counts, this
    /// classifies. Indexed by `failure_sequence % capacity`.
    recent_failures: [Option<MaintenanceFailureRecord>; MAINTENANCE_FAILURE_RECORD_CAPACITY],
    failure_sequence: usize,
    /// Max number of concurrent Rewrite-lane tasks (compaction/materialization). `1` is the
    /// legacy single-lane behavior; the durable runtime raises this to run non-conflicting
    /// compactions concurrently. Every other lane is always effectively `1`.
    rewrite_lane_cap: usize,
    /// The last outcome of every reclaim family, fed by `record_outcome` — the one
    /// point every task completion passes through (foreground runs, off-lock stage
    /// completions, close drains).
    reclaim_ledger: ReclaimLedger,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum MaintenanceTaskLane {
    Flush,
    Rewrite,
    Checkpoint,
    Wal,
    Retention,
    Quarantine,
    Health,
    Preheat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum MaintenanceFaultPoint {
    BeforeEnqueue,
    AfterEnqueue,
    AtTaskStart,
    AfterTaskRun,
    DuringDrain,
}

pub(crate) trait MaintenanceTaskRunner {
    fn run_task(&mut self, task: &MaintenanceTask) -> LifecycleResult<MaintenanceOutcome>;
}

pub(crate) trait MaintenanceFaultHook {
    fn check(
        &mut self,
        point: MaintenanceFaultPoint,
        task: Option<&MaintenanceTask>,
    ) -> LifecycleResult<()>;
}

pub(crate) const MAINTENANCE_COVERAGE_IDLE_ROUND_LIMIT: usize = 5;

#[derive(Default)]
pub(crate) struct NoopMaintenanceFaultHook;

impl MaintenanceTaskId {
    pub(crate) const fn new(value: u64) -> LifecycleResult<Self> {
        if value == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "maintenance_task_id",
                reason: "maintenance task id must be nonzero",
            });
        }
        Ok(Self(value))
    }

    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

impl MaintenanceTaskPriority {
    const fn rank(self) -> u8 {
        match self {
            Self::Critical => 0,
            Self::High => 1,
            Self::Normal => 2,
            Self::Low => 3,
        }
    }
}

impl MaintenanceTaskPolicy {
    pub(crate) const fn ordinary() -> Self {
        Self {
            close_policy: MaintenanceClosePolicy::Ordinary,
            coalesce: false,
        }
    }

    pub(crate) const fn coalescing() -> Self {
        Self {
            close_policy: MaintenanceClosePolicy::Ordinary,
            coalesce: true,
        }
    }

    pub(crate) const fn drain_before_close() -> Self {
        Self {
            close_policy: MaintenanceClosePolicy::DrainBeforeClose,
            coalesce: false,
        }
    }

    pub(crate) const fn cancel_before_close() -> Self {
        Self {
            close_policy: MaintenanceClosePolicy::CancelBeforeClose,
            coalesce: false,
        }
    }

    pub(crate) const fn coalescing_drain_before_close() -> Self {
        Self {
            close_policy: MaintenanceClosePolicy::DrainBeforeClose,
            coalesce: true,
        }
    }

    pub(crate) const fn close_policy(self) -> MaintenanceClosePolicy {
        self.close_policy
    }

    pub(crate) const fn coalesces(self) -> bool {
        self.coalesce
    }
}

impl MaintenanceTaskRequest {
    pub(crate) fn new(
        kind: MaintenanceTaskKind,
        priority: MaintenanceTaskPriority,
        scope: MaintenanceTaskScope,
        policy: MaintenanceTaskPolicy,
    ) -> LifecycleResult<Self> {
        let request = Self {
            kind,
            priority,
            scope,
            policy,
            checkpoint_options: None,
            flush_watermark_candidate: None,
            retention_options: None,
            materialization_handle: None,
        };
        request.validate()?;
        Ok(request)
    }

    pub(crate) fn health_collection() -> Self {
        Self::new(
            MaintenanceTaskKind::HealthCollection,
            MaintenanceTaskPriority::Low,
            MaintenanceTaskScope::Global,
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("health collection task request is valid")
    }

    pub(crate) fn flush(branch_id: BranchId) -> Self {
        Self::new(
            MaintenanceTaskKind::Flush,
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskScope::Branch(branch_id),
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("flush task request is valid")
    }

    pub(crate) fn checkpoint() -> Self {
        Self::new(
            MaintenanceTaskKind::Checkpoint,
            MaintenanceTaskPriority::High,
            MaintenanceTaskScope::Checkpoint,
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("checkpoint task request is valid")
    }

    pub(crate) fn checkpoint_with_options(options: MaintenanceCheckpointOptions) -> Self {
        let mut request = Self::checkpoint();
        request.checkpoint_options = Some(options);
        request
            .validate()
            .expect("checkpoint task options are valid");
        request
    }

    pub(crate) fn wal_truncation() -> Self {
        Self::new(
            MaintenanceTaskKind::WalTruncation,
            MaintenanceTaskPriority::Low,
            MaintenanceTaskScope::Wal,
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("WAL truncation task request is valid")
    }

    pub(crate) fn table_manifest_flush_watermark(candidate: CommitVersion) -> Self {
        let request = Self {
            kind: MaintenanceTaskKind::FlushWatermark,
            priority: MaintenanceTaskPriority::Low,
            scope: MaintenanceTaskScope::Wal,
            policy: MaintenanceTaskPolicy::coalescing(),
            checkpoint_options: None,
            flush_watermark_candidate: Some(candidate),
            retention_options: None,
            materialization_handle: None,
        };
        request
            .validate()
            .expect("flush watermark task candidate is valid");
        request
    }

    pub(crate) fn snapshot_pruning(retain_newest_snapshots: usize) -> Self {
        let mut request = Self::new(
            MaintenanceTaskKind::SnapshotPruning,
            MaintenanceTaskPriority::Low,
            MaintenanceTaskScope::Retention,
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("snapshot pruning task request is valid");
        request.retention_options = Some(MaintenanceRetentionOptions::new(retain_newest_snapshots));
        request
    }

    pub(crate) fn retention(retain_newest_snapshots: usize) -> Self {
        let mut request = Self::new(
            MaintenanceTaskKind::Retention,
            MaintenanceTaskPriority::Low,
            MaintenanceTaskScope::Retention,
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("retention task request is valid");
        request.retention_options = Some(MaintenanceRetentionOptions::new(retain_newest_snapshots));
        request
    }

    /// Table-object retention (the GC mark) for `branch_id`'s runtime — the same
    /// `Retention`/`Branch` shape the public `Reclaim` API enqueues, emitted automatically when a
    /// publish drops table refs, when a branch clear/delete buffers a release plan, and once after
    /// recovery (reconciling any prior session's backlog). Branch-scoped only: the table-object
    /// path never touches snapshots (no implicit snapshot pruning).
    pub(crate) fn table_object_retention(branch_id: BranchId) -> Self {
        Self::new(
            MaintenanceTaskKind::Retention,
            MaintenanceTaskPriority::Low,
            MaintenanceTaskScope::Branch(branch_id),
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("table object retention task request is valid")
    }

    pub(crate) fn quarantine() -> Self {
        Self::new(
            MaintenanceTaskKind::Quarantine,
            MaintenanceTaskPriority::Low,
            MaintenanceTaskScope::Quarantine,
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("quarantine task request is valid")
    }

    /// C2: background block-cache preheat. Low priority + a dedicated lane
    /// keeps it strictly behind flush/checkpoint/WAL/rewrite work, and
    /// coalescing keeps the steady state at one pending task no matter how
    /// many publishes trigger it.
    pub(crate) fn cache_preheat() -> Self {
        Self::new(
            MaintenanceTaskKind::CachePreheat,
            MaintenanceTaskPriority::Low,
            MaintenanceTaskScope::Global,
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("cache preheat task request is valid")
    }

    pub(crate) fn purge_quarantine(branch_id: BranchId) -> Self {
        Self::new(
            MaintenanceTaskKind::Purge,
            MaintenanceTaskPriority::Low,
            MaintenanceTaskScope::Branch(branch_id),
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("purge task request is valid")
    }

    pub(crate) fn repair_quarantine(branch_id: BranchId) -> Self {
        Self::new(
            MaintenanceTaskKind::Repair,
            MaintenanceTaskPriority::Low,
            MaintenanceTaskScope::Branch(branch_id),
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("repair task request is valid")
    }

    pub(crate) fn repair_quarantine_family() -> Self {
        Self::new(
            MaintenanceTaskKind::Repair,
            MaintenanceTaskPriority::Low,
            MaintenanceTaskScope::Global,
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("repair task request is valid")
    }

    pub(crate) fn compaction(branch_id: BranchId, level: u8) -> Self {
        Self::new(
            MaintenanceTaskKind::Compaction,
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskScope::TableLevel { branch_id, level },
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("compaction task request is valid")
    }

    pub(crate) fn materialization(branch_id: BranchId) -> Self {
        Self::materialization_layer(branch_id, 0)
    }

    pub(crate) fn materialization_layer(branch_id: BranchId, layer_index: usize) -> Self {
        Self::new(
            MaintenanceTaskKind::Materialization,
            MaintenanceTaskPriority::Normal,
            MaintenanceTaskScope::InheritedLayer {
                branch_id,
                layer_index,
            },
            MaintenanceTaskPolicy::coalescing(),
        )
        .expect("materialization task request is valid")
    }

    pub(crate) fn with_materialization_handle(
        mut self,
        handle: BranchMaterializationHandle,
    ) -> LifecycleResult<Self> {
        self.materialization_handle = Some(handle);
        self.validate()?;
        Ok(self)
    }

    pub(crate) const fn kind(self) -> MaintenanceTaskKind {
        self.kind
    }

    pub(crate) const fn priority(self) -> MaintenanceTaskPriority {
        self.priority
    }

    pub(crate) const fn scope(self) -> MaintenanceTaskScope {
        self.scope
    }

    pub(crate) const fn policy(self) -> MaintenanceTaskPolicy {
        self.policy
    }

    pub(crate) const fn checkpoint_options(self) -> Option<MaintenanceCheckpointOptions> {
        self.checkpoint_options
    }

    pub(crate) const fn flush_watermark_candidate(self) -> Option<CommitVersion> {
        self.flush_watermark_candidate
    }

    pub(crate) const fn retention_options(self) -> Option<MaintenanceRetentionOptions> {
        self.retention_options
    }

    pub(crate) const fn materialization_handle(self) -> Option<BranchMaterializationHandle> {
        self.materialization_handle
    }

    pub(crate) const fn coalesce_key(self) -> Option<MaintenanceCoalesceKey> {
        if self.policy.coalesces() {
            Some(MaintenanceCoalesceKey {
                kind: self.kind,
                scope: normalized_coalesce_scope(self.kind, self.scope),
                checkpoint_options: match self.kind {
                    MaintenanceTaskKind::Checkpoint => self.checkpoint_options,
                    _ => None,
                },
                flush_watermark_candidate: None,
                retention_options: match self.kind {
                    MaintenanceTaskKind::SnapshotPruning | MaintenanceTaskKind::Retention => {
                        self.retention_options
                    }
                    _ => None,
                },
            })
        } else {
            None
        }
    }

    fn validate(self) -> LifecycleResult<()> {
        if !scope_matches_kind(self.kind, self.scope) {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "maintenance task scope does not match task kind",
            });
        }
        if self.kind == MaintenanceTaskKind::Flush
            && self.scope == MaintenanceTaskScope::Global
            && self.policy.close_policy() == MaintenanceClosePolicy::DrainBeforeClose
        {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "global flush tasks cannot be drained during close",
            });
        }
        if self.checkpoint_options.is_some() && self.kind != MaintenanceTaskKind::Checkpoint {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "checkpoint options require a checkpoint task",
            });
        }
        match (self.kind, self.flush_watermark_candidate) {
            (MaintenanceTaskKind::FlushWatermark, Some(candidate))
                if candidate != CommitVersion::ZERO => {}
            (MaintenanceTaskKind::FlushWatermark, _) => {
                return Err(LifecycleError::MaintenanceTaskFailed {
                    reason: "flush watermark task requires a nonzero candidate",
                });
            }
            (_, Some(_)) => {
                return Err(LifecycleError::MaintenanceTaskFailed {
                    reason: "flush watermark candidate requires a flush watermark task",
                });
            }
            (_, None) => {}
        }
        if self.retention_options.is_some()
            && !matches!(
                self.kind,
                MaintenanceTaskKind::SnapshotPruning | MaintenanceTaskKind::Retention
            )
        {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "retention options require a retention task",
            });
        }
        if let Some(handle) = self.materialization_handle {
            let MaintenanceTaskScope::InheritedLayer {
                branch_id,
                layer_index,
            } = self.scope
            else {
                return Err(LifecycleError::MaintenanceTaskFailed {
                    reason: "materialization handle requires an inherited layer scope",
                });
            };
            if self.kind != MaintenanceTaskKind::Materialization
                || branch_id != handle.child_branch_id()
                || layer_index != handle.layer_index()
            {
                return Err(LifecycleError::MaintenanceTaskFailed {
                    reason: "materialization handle must match task scope",
                });
            }
        }
        Ok(())
    }
}

impl MaintenanceCheckpointOptions {
    pub(crate) const fn new(snapshot_id: Option<u64>, truncate_wal_after_checkpoint: bool) -> Self {
        Self {
            snapshot_id,
            truncate_wal_after_checkpoint,
            retention_critical: false,
        }
    }

    pub(crate) const fn retention_critical(mut self) -> Self {
        self.retention_critical = true;
        self
    }

    pub(crate) const fn snapshot_id(self) -> Option<u64> {
        self.snapshot_id
    }

    pub(crate) const fn truncate_wal_after_checkpoint(self) -> bool {
        self.truncate_wal_after_checkpoint
    }

    pub(crate) const fn is_retention_critical(self) -> bool {
        self.retention_critical
    }
}

impl MaintenanceRetentionOptions {
    pub(crate) const fn new(retain_newest_snapshots: usize) -> Self {
        Self {
            retain_newest_snapshots,
        }
    }

    pub(crate) const fn retain_newest_snapshots(self) -> usize {
        self.retain_newest_snapshots
    }
}

impl MaintenanceTask {
    const fn new(id: MaintenanceTaskId, sequence: u64, request: MaintenanceTaskRequest) -> Self {
        Self {
            id,
            sequence,
            request,
        }
    }

    pub(crate) const fn id(self) -> MaintenanceTaskId {
        self.id
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(id: u64, request: MaintenanceTaskRequest) -> LifecycleResult<Self> {
        let id = MaintenanceTaskId::new(id)?;
        Ok(Self::new(id, id.get(), request))
    }

    pub(crate) const fn sequence(self) -> u64 {
        self.sequence
    }

    pub(crate) const fn kind(self) -> MaintenanceTaskKind {
        self.request.kind()
    }

    pub(crate) const fn priority(self) -> MaintenanceTaskPriority {
        self.request.priority()
    }

    pub(crate) const fn scope(self) -> MaintenanceTaskScope {
        self.request.scope()
    }

    pub(crate) const fn policy(self) -> MaintenanceTaskPolicy {
        self.request.policy()
    }

    pub(crate) const fn checkpoint_options(self) -> Option<MaintenanceCheckpointOptions> {
        self.request.checkpoint_options()
    }

    pub(crate) const fn flush_watermark_candidate(self) -> Option<CommitVersion> {
        self.request.flush_watermark_candidate()
    }

    pub(crate) const fn retention_options(self) -> Option<MaintenanceRetentionOptions> {
        self.request.retention_options()
    }

    pub(crate) const fn materialization_handle(self) -> Option<BranchMaterializationHandle> {
        self.request.materialization_handle()
    }

    const fn coalesce_key(self) -> Option<MaintenanceCoalesceKey> {
        self.request.coalesce_key()
    }

    const fn lane(self) -> MaintenanceTaskLane {
        match self.kind() {
            MaintenanceTaskKind::Flush => MaintenanceTaskLane::Flush,
            MaintenanceTaskKind::Compaction | MaintenanceTaskKind::Materialization => {
                MaintenanceTaskLane::Rewrite
            }
            MaintenanceTaskKind::Checkpoint => MaintenanceTaskLane::Checkpoint,
            MaintenanceTaskKind::FlushWatermark | MaintenanceTaskKind::WalTruncation => {
                MaintenanceTaskLane::Wal
            }
            MaintenanceTaskKind::SnapshotPruning | MaintenanceTaskKind::Retention => {
                MaintenanceTaskLane::Retention
            }
            MaintenanceTaskKind::Quarantine
            | MaintenanceTaskKind::Purge
            | MaintenanceTaskKind::Repair => MaintenanceTaskLane::Quarantine,
            MaintenanceTaskKind::HealthCollection => MaintenanceTaskLane::Health,
            MaintenanceTaskKind::CachePreheat => MaintenanceTaskLane::Preheat,
        }
    }
}

impl MaintenanceEnqueueOutcome {
    const fn enqueued(
        task_id: MaintenanceTaskId,
        pending_tasks: usize,
        stats: LifecycleMaintenanceStats,
    ) -> Self {
        Self {
            task_id,
            coalesced: false,
            pending_tasks,
            stats,
        }
    }

    const fn coalesced(
        task_id: MaintenanceTaskId,
        pending_tasks: usize,
        stats: LifecycleMaintenanceStats,
    ) -> Self {
        Self {
            task_id,
            coalesced: true,
            pending_tasks,
            stats,
        }
    }

    pub(crate) const fn task_id(self) -> MaintenanceTaskId {
        self.task_id
    }

    pub(crate) const fn was_enqueued(self) -> bool {
        !self.coalesced
    }

    pub(crate) const fn was_coalesced(self) -> bool {
        self.coalesced
    }

    pub(crate) const fn pending_tasks(self) -> usize {
        self.pending_tasks
    }

    pub(crate) const fn stats(self) -> LifecycleMaintenanceStats {
        self.stats
    }
}

impl LifecyclePostCommitMaintenanceOutcome {
    const fn disabled(pressure: LifecycleStoragePressure) -> Self {
        Self {
            status: LifecyclePostCommitMaintenanceStatus::Disabled,
            pressure,
            suggested_task: None,
            enqueue: None,
            failure: None,
        }
    }

    const fn no_suggested_task(pressure: LifecycleStoragePressure) -> Self {
        Self {
            status: LifecyclePostCommitMaintenanceStatus::NoSuggestedTask,
            pressure,
            suggested_task: None,
            enqueue: None,
            failure: None,
        }
    }

    const fn enqueued(
        pressure: LifecycleStoragePressure,
        suggested_task: MaintenanceTaskRequest,
        enqueue: MaintenanceEnqueueOutcome,
    ) -> Self {
        Self {
            status: if enqueue.was_coalesced() {
                LifecyclePostCommitMaintenanceStatus::Coalesced
            } else {
                LifecyclePostCommitMaintenanceStatus::Enqueued
            },
            pressure,
            suggested_task: Some(suggested_task),
            enqueue: Some(enqueue),
            failure: None,
        }
    }

    const fn deferred(
        pressure: LifecycleStoragePressure,
        suggested_task: MaintenanceTaskRequest,
        failure: LifecycleError,
    ) -> Self {
        Self {
            status: LifecyclePostCommitMaintenanceStatus::Deferred,
            pressure,
            suggested_task: Some(suggested_task),
            enqueue: None,
            failure: Some(failure),
        }
    }

    pub(crate) fn with_inline_failure(mut self, failure: LifecycleError) -> Self {
        self.status = LifecyclePostCommitMaintenanceStatus::Deferred;
        self.failure = Some(failure);
        self
    }

    pub(crate) const fn status(&self) -> LifecyclePostCommitMaintenanceStatus {
        self.status
    }

    pub(crate) const fn pressure(&self) -> LifecycleStoragePressure {
        self.pressure
    }

    pub(crate) const fn suggested_task(&self) -> Option<MaintenanceTaskRequest> {
        self.suggested_task
    }

    pub(crate) const fn enqueue(&self) -> Option<MaintenanceEnqueueOutcome> {
        self.enqueue
    }

    pub(crate) const fn failure(&self) -> Option<&LifecycleError> {
        self.failure.as_ref()
    }
}

impl MaintenanceDrainOutcome {
    const fn new(
        drained_tasks: usize,
        outcomes: Vec<MaintenanceOutcome>,
        stats: LifecycleMaintenanceStats,
    ) -> Self {
        Self {
            drained_tasks,
            outcomes,
            stats,
        }
    }

    pub(crate) const fn drained_tasks(&self) -> usize {
        self.drained_tasks
    }

    pub(crate) fn outcomes(&self) -> &[MaintenanceOutcome] {
        &self.outcomes
    }

    pub(crate) const fn stats(&self) -> LifecycleMaintenanceStats {
        self.stats
    }
}

impl MaintenanceCancelOutcome {
    const fn new(canceled_tasks: usize, stats: LifecycleMaintenanceStats) -> Self {
        Self {
            canceled_tasks,
            stats,
        }
    }

    pub(crate) const fn canceled_tasks(self) -> usize {
        self.canceled_tasks
    }

    pub(crate) const fn stats(self) -> LifecycleMaintenanceStats {
        self.stats
    }
}

impl MaintenanceExecutorStatus {
    const fn new(
        pending_tasks: usize,
        active_task: Option<MaintenanceTaskId>,
        active_tasks: usize,
        stats: LifecycleMaintenanceStats,
    ) -> Self {
        Self {
            pending_tasks,
            active_task,
            active_tasks,
            stats,
        }
    }

    pub(crate) const fn pending_tasks(self) -> usize {
        self.pending_tasks
    }

    pub(crate) const fn active_task(self) -> Option<MaintenanceTaskId> {
        self.active_task
    }

    pub(crate) const fn active_tasks(self) -> usize {
        self.active_tasks
    }

    pub(crate) const fn stats(self) -> LifecycleMaintenanceStats {
        self.stats
    }
}

impl LifecycleMaintenanceStats {
    pub(crate) const fn enqueued(self) -> usize {
        self.enqueued
    }

    pub(crate) const fn coalesced(self) -> usize {
        self.coalesced
    }

    pub(crate) const fn max_pending_tasks(self) -> usize {
        self.max_pending_tasks
    }

    pub(crate) const fn started(self) -> usize {
        self.started
    }

    pub(crate) const fn completed(self) -> usize {
        self.completed
    }

    pub(crate) const fn deferred(self) -> usize {
        self.deferred
    }

    pub(crate) const fn failed(self) -> usize {
        self.failed
    }

    pub(crate) const fn canceled(self) -> usize {
        self.canceled
    }

    pub(crate) const fn drained(self) -> usize {
        self.drained
    }

    pub(crate) const fn queue_full(self) -> usize {
        self.queue_full
    }

    pub(crate) const fn lifecycle_stats(self) -> LifecycleStats {
        LifecycleStats::new(0, 0, self.completed + self.deferred + self.failed, 0, 0)
    }
}

impl LifecycleWriteAdmissionOutcome {
    const fn new(
        status: LifecycleWriteAdmissionStatus,
        pressure: LifecycleStoragePressure,
        cleared_prior_rejection: bool,
    ) -> Self {
        Self {
            status,
            pressure,
            cleared_prior_rejection,
            inline_maintenance_driven: false,
        }
    }

    pub(crate) const fn status(self) -> LifecycleWriteAdmissionStatus {
        self.status
    }

    pub(crate) const fn pressure(self) -> LifecycleStoragePressure {
        self.pressure
    }

    pub(crate) const fn cleared_prior_rejection(self) -> bool {
        self.cleared_prior_rejection
    }

    pub(crate) const fn inline_maintenance_driven(self) -> bool {
        self.inline_maintenance_driven
    }

    pub(crate) const fn with_inline_maintenance_driven(mut self) -> Self {
        self.inline_maintenance_driven = true;
        self
    }
}

pub(crate) fn schedule_post_commit_maintenance(
    policy: LifecycleMaintenanceSchedulingPolicy,
    pressure: LifecycleStoragePressure,
    enqueue: impl FnOnce(MaintenanceTaskRequest) -> LifecycleResult<MaintenanceEnqueueOutcome>,
) -> LifecyclePostCommitMaintenanceOutcome {
    perf_trace::record_lifecycle_post_commit_maintenance_evaluation();
    if !policy.enabled() {
        perf_trace::record_lifecycle_post_commit_maintenance_disabled();
        return LifecyclePostCommitMaintenanceOutcome::disabled(pressure);
    }
    let Some(suggested_task) = pressure.suggested_task() else {
        perf_trace::record_lifecycle_post_commit_maintenance_no_task();
        return LifecyclePostCommitMaintenanceOutcome::no_suggested_task(pressure);
    };
    perf_trace::record_lifecycle_post_commit_maintenance_task_suggested();
    match enqueue(suggested_task) {
        Ok(enqueue) => {
            perf_trace::record_lifecycle_post_commit_maintenance_enqueue(
                enqueue.was_enqueued(),
                enqueue.was_coalesced(),
            );
            LifecyclePostCommitMaintenanceOutcome::enqueued(pressure, suggested_task, enqueue)
        }
        Err(error) => {
            perf_trace::record_lifecycle_post_commit_maintenance_deferred();
            LifecyclePostCommitMaintenanceOutcome::deferred(pressure, suggested_task, error)
        }
    }
}

pub(crate) fn evaluate_mutating_write_admission(
    pressure: LifecycleStoragePressure,
    pressure_rejected_branches: &mut HashSet<BranchId>,
) -> LifecycleResult<LifecycleWriteAdmissionOutcome> {
    let branch_id = pressure.branch_id();
    match pressure.severity() {
        LifecycleStoragePressureSeverity::None | LifecycleStoragePressureSeverity::Background => {
            let cleared_prior_rejection =
                clear_pressure_rejected_branch(pressure_rejected_branches, branch_id);
            perf_trace::record_lifecycle_write_admission_clean(cleared_prior_rejection);
            Ok(LifecycleWriteAdmissionOutcome::new(
                LifecycleWriteAdmissionStatus::AcceptedClean,
                pressure,
                cleared_prior_rejection,
            ))
        }
        LifecycleStoragePressureSeverity::Urgent => {
            let cleared_prior_rejection =
                clear_pressure_rejected_branch(pressure_rejected_branches, branch_id);
            perf_trace::record_lifecycle_write_admission_under_pressure(cleared_prior_rejection);
            Ok(LifecycleWriteAdmissionOutcome::new(
                LifecycleWriteAdmissionStatus::AcceptedUnderPressure,
                pressure,
                cleared_prior_rejection,
            ))
        }
        LifecycleStoragePressureSeverity::BlockMutatingAdmission => {
            perf_trace::record_lifecycle_write_admission_requires_maintenance();
            remember_pressure_rejected_branch(pressure_rejected_branches, branch_id);
            let retryable = pressure.suggested_task().is_some()
                || pressure.reason()
                    == crate::lifecycle::LifecycleStoragePressureReason::MaintenanceQueueBacklog;
            perf_trace::record_lifecycle_write_admission_pressure_reject(retryable);
            Err(LifecycleError::StoragePressureRejected {
                branch_id,
                severity: pressure.severity(),
                pressure_reason: pressure.reason(),
                retryable,
                reason: "mutating commit admission requires maintenance progress",
            })
        }
    }
}

fn remember_pressure_rejected_branch(branches: &mut HashSet<BranchId>, branch_id: BranchId) {
    branches.insert(branch_id);
}

fn clear_pressure_rejected_branch(branches: &mut HashSet<BranchId>, branch_id: BranchId) -> bool {
    branches.remove(&branch_id)
}

impl LifecycleMaintenanceExecutor {
    pub(crate) fn new(max_queue_depth: usize) -> LifecycleResult<Self> {
        if max_queue_depth == 0 {
            return Err(LifecycleError::InvalidConfig {
                field: "max_maintenance_queue_depth",
                reason: "must be nonzero",
            });
        }
        Ok(Self {
            next_id: 1,
            max_queue_depth,
            queue: Vec::new(),
            active: Vec::new(),
            stats: LifecycleMaintenanceStats::default(),
            recent_failures: [None; MAINTENANCE_FAILURE_RECORD_CAPACITY],
            failure_sequence: 0,
            rewrite_lane_cap: 1,
            reclaim_ledger: ReclaimLedger::default(),
        })
    }

    /// The per-runtime reclaim ledger (space-reclamation contract §3.5).
    pub(crate) const fn reclaim_ledger(&self) -> &ReclaimLedger {
        &self.reclaim_ledger
    }

    /// Record a reclaim pass that ran OUTSIDE the task queue: the inline
    /// `Retain`/`SnapshotPruning`/`Reclaim` verbs and an explicit checkpoint
    /// whose follow-up truncated the WAL. Task completions record through
    /// `record_outcome`; these two entry points are the only ways an event
    /// reaches the ledger, and both record the outcome's own family (if any)
    /// plus its WAL-truncation follow-up (if any).
    pub(crate) fn record_reclaim(&mut self, outcome: &MaintenanceOutcome) {
        for (family, event) in classify_reclaim(outcome)
            .into_iter()
            .chain(classify_reclaim_follow_up(outcome))
        {
            self.reclaim_ledger.record(family, event);
        }
    }

    /// Set the max number of concurrent Rewrite-lane tasks (compaction/materialization). `1`
    /// is the legacy single-lane behavior; the durable runtime raises this to run
    /// non-conflicting compactions concurrently. Clamped to at least `1`.
    pub(crate) fn set_rewrite_lane_cap(&mut self, cap: usize) {
        self.rewrite_lane_cap = cap.max(1);
    }

    pub(crate) fn enqueue(
        &mut self,
        state: LifecycleStateMachine,
        request: MaintenanceTaskRequest,
    ) -> LifecycleResult<MaintenanceEnqueueOutcome> {
        self.enqueue_with_binding(state, request, Ok)
    }

    pub(crate) fn would_coalesce(&self, request: MaintenanceTaskRequest) -> bool {
        request.coalesce_key().is_some_and(|key| {
            self.queue
                .iter()
                .any(|task| task.coalesce_key() == Some(key))
        })
    }

    pub(crate) fn enqueue_with_binding(
        &mut self,
        state: LifecycleStateMachine,
        request: MaintenanceTaskRequest,
        bind: impl FnOnce(MaintenanceTaskRequest) -> LifecycleResult<MaintenanceTaskRequest>,
    ) -> LifecycleResult<MaintenanceEnqueueOutcome> {
        self.enqueue_with_fault_and_binding(state, request, &mut NoopMaintenanceFaultHook, bind)
    }

    pub(crate) fn enqueue_with_fault(
        &mut self,
        state: LifecycleStateMachine,
        request: MaintenanceTaskRequest,
        fault: &mut impl MaintenanceFaultHook,
    ) -> LifecycleResult<MaintenanceEnqueueOutcome> {
        self.enqueue_with_fault_and_binding(state, request, fault, Ok)
    }

    fn enqueue_with_fault_and_binding(
        &mut self,
        state: LifecycleStateMachine,
        request: MaintenanceTaskRequest,
        fault: &mut impl MaintenanceFaultHook,
        bind: impl FnOnce(MaintenanceTaskRequest) -> LifecycleResult<MaintenanceTaskRequest>,
    ) -> LifecycleResult<MaintenanceEnqueueOutcome> {
        require_admitted(state, LifecycleOperationKind::OrdinaryMaintenance)?;
        fault.check(MaintenanceFaultPoint::BeforeEnqueue, None)?;
        if let Some(key) = request.coalesce_key() {
            if let Some(existing_index) = self
                .queue
                .iter()
                .position(|task| task.coalesce_key() == Some(key))
            {
                if request.kind == MaintenanceTaskKind::FlushWatermark {
                    let candidate = self.queue[existing_index]
                        .request
                        .flush_watermark_candidate
                        .zip(request.flush_watermark_candidate)
                        .map(|(existing, requested)| existing.max(requested));
                    self.queue[existing_index].request.flush_watermark_candidate = candidate;
                }
                self.stats.coalesced = self.stats.coalesced.saturating_add(1);
                return Ok(MaintenanceEnqueueOutcome::coalesced(
                    self.queue[existing_index].id(),
                    self.queue.len(),
                    self.stats,
                ));
            }
        }
        if self.queue.len() >= self.max_queue_depth {
            self.stats.queue_full = self.stats.queue_full.saturating_add(1);
            return Err(LifecycleError::MaintenanceQueueFull {
                reason: "maintenance queue is full",
            });
        }
        let request = bind(request)?;
        let id = MaintenanceTaskId::new(self.next_id)?;
        let sequence = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        let task = MaintenanceTask::new(id, sequence, request);
        self.queue.push(task);
        self.stats.enqueued = self.stats.enqueued.saturating_add(1);
        self.stats.max_pending_tasks = self.stats.max_pending_tasks.max(self.queue.len());
        fault.check(MaintenanceFaultPoint::AfterEnqueue, Some(&task))?;
        Ok(MaintenanceEnqueueOutcome::enqueued(
            id,
            self.queue.len(),
            self.stats,
        ))
    }

    pub(crate) fn run_next(
        &mut self,
        state: LifecycleStateMachine,
        runner: &mut impl MaintenanceTaskRunner,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        self.run_next_matching_with_fault(state, runner, |_| true, &mut NoopMaintenanceFaultHook)
    }

    pub(crate) fn run_next_matching(
        &mut self,
        state: LifecycleStateMachine,
        runner: &mut impl MaintenanceTaskRunner,
        predicate: impl Fn(&MaintenanceTask) -> bool,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        self.run_next_matching_with_fault(state, runner, predicate, &mut NoopMaintenanceFaultHook)
    }

    pub(crate) fn run_next_with_fault(
        &mut self,
        state: LifecycleStateMachine,
        runner: &mut impl MaintenanceTaskRunner,
        fault: &mut impl MaintenanceFaultHook,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        self.run_next_matching_with_fault(state, runner, |_| true, fault)
    }

    pub(crate) fn run_next_matching_with_fault(
        &mut self,
        state: LifecycleStateMachine,
        runner: &mut impl MaintenanceTaskRunner,
        predicate: impl Fn(&MaintenanceTask) -> bool,
        fault: &mut impl MaintenanceFaultHook,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        require_admitted(state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let Some(index) = self.next_startable_task_index(predicate) else {
            return Ok(None);
        };
        self.run_index(index, runner, fault, false).map(Some)
    }

    pub(crate) fn cancel_pending_for_close(
        &mut self,
        state: LifecycleStateMachine,
    ) -> LifecycleResult<MaintenanceCancelOutcome> {
        require_admitted(state, LifecycleOperationKind::CloseRequiredDrain)?;
        let before = self.queue.len();
        self.queue.retain(|task| {
            task.policy().close_policy() == MaintenanceClosePolicy::DrainBeforeClose
        });
        let canceled = before - self.queue.len();
        self.stats.canceled = self.stats.canceled.saturating_add(canceled);
        Ok(MaintenanceCancelOutcome::new(canceled, self.stats))
    }

    pub(crate) fn drain_for_close(
        &mut self,
        state: LifecycleStateMachine,
        runner: &mut impl MaintenanceTaskRunner,
    ) -> LifecycleResult<MaintenanceDrainOutcome> {
        self.drain_for_close_with_fault(state, runner, &mut NoopMaintenanceFaultHook)
    }

    pub(crate) fn drain_active_for_close(
        &mut self,
        state: LifecycleStateMachine,
        runner: &mut impl MaintenanceTaskRunner,
    ) -> LifecycleResult<Option<MaintenanceOutcome>> {
        require_admitted(state, LifecycleOperationKind::CloseRequiredDrain)?;
        if self.active.is_empty() {
            return Ok(None);
        }
        let task = self.active.remove(0);
        let outcome = match runner.run_task(&task) {
            Ok(outcome) => attach_executor_facts(outcome, task)?,
            Err(error) => {
                self.stats.failed = self.stats.failed.saturating_add(1);
                self.record_failure_detail(MaintenanceFailureRecord::from_error(
                    task.kind(),
                    &error,
                ));
                self.active.insert(0, task);
                return Err(error);
            }
        };
        self.record_outcome(&outcome, true);
        Ok(Some(outcome))
    }

    pub(crate) fn drain_for_close_with_fault(
        &mut self,
        state: LifecycleStateMachine,
        runner: &mut impl MaintenanceTaskRunner,
        fault: &mut impl MaintenanceFaultHook,
    ) -> LifecycleResult<MaintenanceDrainOutcome> {
        require_admitted(state, LifecycleOperationKind::CloseRequiredDrain)?;
        let mut outcomes = Vec::new();
        while let Some(index) = self.next_task_index(|task| {
            task.policy().close_policy() == MaintenanceClosePolicy::DrainBeforeClose
        }) {
            let task = self.queue[index];
            if let Err(error) = fault.check(MaintenanceFaultPoint::DuringDrain, Some(&task)) {
                self.stats.failed = self.stats.failed.saturating_add(1);
                self.record_failure_detail(MaintenanceFailureRecord::from_error(
                    task.kind(),
                    &error,
                ));
                return Err(error);
            }
            let outcome = self.run_index(index, runner, fault, true)?;
            outcomes.push(outcome);
        }
        Ok(MaintenanceDrainOutcome::new(
            outcomes.len(),
            outcomes,
            self.stats,
        ))
    }

    pub(crate) fn status(&self) -> MaintenanceExecutorStatus {
        MaintenanceExecutorStatus::new(
            self.queue.len(),
            self.active.first().map(|task| task.id()),
            self.active.len(),
            self.stats,
        )
    }

    /// The newest failed-task records, oldest first. Kept off
    /// [`MaintenanceExecutorStatus`] so the hot-path pressure/budget copies of
    /// that status stay small; diagnostic summaries pull this separately.
    pub(crate) fn recent_failures(&self) -> Vec<MaintenanceFailureRecord> {
        self.ordered_recent_failures()
            .into_iter()
            .flatten()
            .collect()
    }

    pub(crate) fn pending_tasks(&self) -> &[MaintenanceTask] {
        &self.queue
    }

    pub(crate) fn replace_pending_flush_watermark_candidate(
        &mut self,
        task_id: MaintenanceTaskId,
        candidate: CommitVersion,
    ) -> LifecycleResult<bool> {
        let Some(task) = self.queue.iter_mut().find(|task| task.id() == task_id) else {
            return Ok(false);
        };
        if task.kind() != MaintenanceTaskKind::FlushWatermark {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "flush watermark candidate replacement requires a flush watermark task",
            });
        }
        task.request.flush_watermark_candidate = Some(candidate);
        task.request.validate()?;
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn pending_flush_watermark_candidate(&self) -> Option<CommitVersion> {
        self.queue
            .iter()
            .find(|task| task.kind() == MaintenanceTaskKind::FlushWatermark)
            .and_then(|task| task.flush_watermark_candidate())
    }

    #[cfg(test)]
    pub(crate) fn pending_kinds(&self) -> Vec<MaintenanceTaskKind> {
        self.queue.iter().map(|task| task.kind()).collect()
    }

    pub(crate) fn next_matching_task(
        &self,
        predicate: impl Fn(&MaintenanceTask) -> bool,
    ) -> Option<MaintenanceTask> {
        self.next_task_index(predicate)
            .map(|index| self.queue[index])
    }

    pub(crate) fn start_next_matching(
        &mut self,
        state: LifecycleStateMachine,
        predicate: impl Fn(&MaintenanceTask) -> bool,
    ) -> LifecycleResult<Option<MaintenanceTask>> {
        require_admitted(state, LifecycleOperationKind::OrdinaryMaintenance)?;
        let Some(index) = self.next_startable_task_index(predicate) else {
            return Ok(None);
        };
        let task = self.queue.remove(index);
        self.active.push(task);
        self.stats.started = self.stats.started.saturating_add(1);
        Ok(Some(task))
    }

    pub(crate) fn finish_started(
        &mut self,
        task: MaintenanceTask,
        outcome: MaintenanceOutcome,
        draining: bool,
    ) -> LifecycleResult<MaintenanceOutcome> {
        let Some(active_index) = self
            .active
            .iter()
            .position(|active| active.id() == task.id())
        else {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "maintenance task completion requires an active task",
            });
        };
        let active = self.active.remove(active_index);
        if active.id() != task.id() {
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "maintenance task completion id must match active task",
            });
        }
        let outcome = attach_executor_facts(outcome, task)?;
        self.record_outcome(&outcome, draining);
        Ok(outcome)
    }

    #[cfg(test)]
    pub(crate) fn set_active_for_test(&mut self, task: MaintenanceTask) {
        self.active.push(task);
    }

    pub(crate) const fn stats(&self) -> LifecycleMaintenanceStats {
        self.stats
    }

    fn next_task_index(&self, predicate: impl Fn(&MaintenanceTask) -> bool) -> Option<usize> {
        self.queue
            .iter()
            .enumerate()
            .filter(|(_, task)| predicate(task))
            .min_by_key(|(_, task)| (task.priority().rank(), task.sequence()))
            .map(|(index, _)| index)
    }

    fn next_startable_task_index(
        &self,
        predicate: impl Fn(&MaintenanceTask) -> bool,
    ) -> Option<usize> {
        self.queue
            .iter()
            .enumerate()
            .filter(|(_, task)| predicate(task) && !self.lane_at_capacity(**task))
            .min_by_key(|(_, task)| (task.priority().rank(), task.sequence()))
            .map(|(index, _)| index)
    }

    /// Whether the task's lane already has its maximum concurrent tasks in flight. The
    /// Rewrite lane admits up to `rewrite_lane_cap` concurrent tasks; every other lane is
    /// single-occupancy. The cap alone does not guarantee non-conflicting inputs — the
    /// dispatch scorer skips conflicting levels, and correctness is enforced at publish by
    /// candidate revalidation regardless.
    fn lane_at_capacity(&self, task: MaintenanceTask) -> bool {
        let lane = task.lane();
        let cap = if lane == MaintenanceTaskLane::Rewrite {
            self.rewrite_lane_cap
        } else {
            1
        };
        self.active
            .iter()
            .filter(|active| active.lane() == lane)
            .count()
            >= cap
    }

    /// Whether any build-producing task (flush / compaction / materialization) is in flight.
    /// The table-object sweep defers while one is: an off-lock build may have written output
    /// objects whose manifest fold has not landed yet, and such an object is inventory-listed
    /// but reachable from no manifest — indistinguishable from garbage to the mark.
    pub(crate) fn has_active_build_task(&self) -> bool {
        self.active.iter().any(|task| {
            matches!(
                task.kind(),
                MaintenanceTaskKind::Flush
                    | MaintenanceTaskKind::Compaction
                    | MaintenanceTaskKind::Materialization
            )
        })
    }

    /// Whether a candidate Rewrite task would contend with any in-flight rewrite (same branch,
    /// equal or adjacent level). Dispatch uses this to hand concurrent workers non-conflicting
    /// levels; it is a waste-avoidance filter, not a correctness guard (publish-time candidate
    /// revalidation is authoritative).
    pub(crate) fn rewrite_conflicts_with_active(&self, candidate: MaintenanceTask) -> bool {
        self.active
            .iter()
            .filter(|active| active.lane() == MaintenanceTaskLane::Rewrite)
            .any(|active| rewrite_tasks_conflict(*active, candidate))
    }

    fn run_index(
        &mut self,
        index: usize,
        runner: &mut impl MaintenanceTaskRunner,
        fault: &mut impl MaintenanceFaultHook,
        draining: bool,
    ) -> LifecycleResult<MaintenanceOutcome> {
        let task = self.queue.remove(index);
        if self.lane_at_capacity(task) {
            self.queue.insert(index, task);
            return Err(LifecycleError::MaintenanceTaskFailed {
                reason: "maintenance task lane is already active",
            });
        }
        self.active.push(task);
        self.stats.started = self.stats.started.saturating_add(1);
        let restore_on_error = draining;
        if let Err(error) = fault.check(MaintenanceFaultPoint::AtTaskStart, Some(&task)) {
            self.stats.failed = self.stats.failed.saturating_add(1);
            self.record_failure_detail(MaintenanceFailureRecord::from_error(task.kind(), &error));
            self.clear_active_task(task);
            if restore_on_error {
                self.queue.insert(index, task);
            }
            return Err(error);
        }
        let outcome = match runner.run_task(&task) {
            Ok(outcome) => attach_executor_facts(outcome, task)?,
            Err(error) => {
                self.stats.failed = self.stats.failed.saturating_add(1);
                self.record_failure_detail(MaintenanceFailureRecord::from_error(
                    task.kind(),
                    &error,
                ));
                self.clear_active_task(task);
                if restore_on_error {
                    self.queue.insert(index, task);
                }
                return Err(error);
            }
        };
        if let Err(_error) = fault.check(MaintenanceFaultPoint::AfterTaskRun, Some(&task)) {
            let outcome = outcome
                .with_status(MaintenanceOutcomeStatus::Failed)
                .with_reason("maintenance task failed after run")
                .with_recovery_health(telemetry_health_debt("maintenance task failed after run")?);
            self.record_outcome(&outcome, draining);
            self.clear_active_task(task);
            return Ok(outcome);
        }
        self.record_outcome(&outcome, draining);
        self.clear_active_task(task);
        Ok(outcome)
    }

    fn clear_active_task(&mut self, task: MaintenanceTask) {
        if let Some(index) = self
            .active
            .iter()
            .position(|active| active.id() == task.id())
        {
            self.active.remove(index);
        }
    }

    fn record_outcome(&mut self, outcome: &MaintenanceOutcome, draining: bool) {
        self.record_reclaim(outcome);
        match outcome.status() {
            MaintenanceOutcomeStatus::Completed => {
                self.stats.completed = self.stats.completed.saturating_add(1);
            }
            MaintenanceOutcomeStatus::Deferred => {
                self.stats.deferred = self.stats.deferred.saturating_add(1);
            }
            MaintenanceOutcomeStatus::Canceled => {
                self.stats.canceled = self.stats.canceled.saturating_add(1);
            }
            MaintenanceOutcomeStatus::Failed => {
                self.stats.failed = self.stats.failed.saturating_add(1);
                self.record_failure_detail(MaintenanceFailureRecord::from_outcome(outcome));
            }
        }
        if draining {
            self.stats.drained = self.stats.drained.saturating_add(1);
        }
    }

    fn record_failure_detail(&mut self, record: MaintenanceFailureRecord) {
        self.recent_failures[self.failure_sequence % MAINTENANCE_FAILURE_RECORD_CAPACITY] =
            Some(record);
        self.failure_sequence = self.failure_sequence.wrapping_add(1);
    }

    /// The retained failure records, oldest first.
    fn ordered_recent_failures(
        &self,
    ) -> [Option<MaintenanceFailureRecord>; MAINTENANCE_FAILURE_RECORD_CAPACITY] {
        let mut ordered = [None; MAINTENANCE_FAILURE_RECORD_CAPACITY];
        for (position, slot) in ordered.iter_mut().enumerate() {
            *slot = self.recent_failures[self.failure_sequence.wrapping_add(position)
                % MAINTENANCE_FAILURE_RECORD_CAPACITY];
        }
        // Before the ring wraps, the occupied slots start at index zero rather
        // than at the cursor; compact the `None` gaps out so records read
        // oldest-to-newest from the front.
        let mut compacted = [None; MAINTENANCE_FAILURE_RECORD_CAPACITY];
        for (slot, record) in compacted.iter_mut().zip(ordered.into_iter().flatten()) {
            *slot = Some(record);
        }
        compacted
    }
}

fn attach_executor_facts(
    outcome: MaintenanceOutcome,
    task: MaintenanceTask,
) -> LifecycleResult<MaintenanceOutcome> {
    let mut outcome = outcome
        .with_task_id(task.id())
        .with_task_scope(task.scope());
    if outcome.status() == MaintenanceOutcomeStatus::Failed && outcome.reason().is_none() {
        outcome = outcome.with_reason("maintenance task failed");
    }
    if outcome.status() == MaintenanceOutcomeStatus::Failed && outcome.recovery_health().is_none() {
        Ok(outcome.with_recovery_health(telemetry_health_debt("maintenance task failed")?))
    } else {
        Ok(outcome)
    }
}

impl MaintenanceFaultHook for NoopMaintenanceFaultHook {
    fn check(
        &mut self,
        _point: MaintenanceFaultPoint,
        _task: Option<&MaintenanceTask>,
    ) -> LifecycleResult<()> {
        Ok(())
    }
}

pub(crate) const fn maintenance_ready_for_recovery_health(health: &RecoveryHealth) -> bool {
    match health {
        RecoveryHealth::Healthy
        | RecoveryHealth::Degraded {
            class: RecoveryDegradationClass::Telemetry,
            ..
        } => true,
        RecoveryHealth::Degraded { .. } | RecoveryHealth::Failed { .. } => false,
    }
}

pub(crate) fn telemetry_health_debt(reason: &'static str) -> LifecycleResult<RecoveryHealth> {
    RecoveryHealth::degraded(
        RecoveryDegradationClass::Telemetry,
        vec![RecoveryFault::new(RecoveryFaultKind::IoFailure, reason)?],
    )
}

fn scope_matches_kind(kind: MaintenanceTaskKind, scope: MaintenanceTaskScope) -> bool {
    matches!(
        (kind, scope),
        (
            MaintenanceTaskKind::Flush | MaintenanceTaskKind::Purge | MaintenanceTaskKind::Repair,
            MaintenanceTaskScope::Branch(_)
        ) | (
            MaintenanceTaskKind::Flush
                | MaintenanceTaskKind::HealthCollection
                | MaintenanceTaskKind::CachePreheat,
            MaintenanceTaskScope::Global
        ) | (
            MaintenanceTaskKind::Checkpoint,
            MaintenanceTaskScope::Checkpoint | MaintenanceTaskScope::Global
        ) | (
            MaintenanceTaskKind::WalTruncation | MaintenanceTaskKind::FlushWatermark,
            MaintenanceTaskScope::Wal
        ) | (
            MaintenanceTaskKind::Compaction,
            MaintenanceTaskScope::TableLevel { .. }
        ) | (
            MaintenanceTaskKind::Materialization,
            MaintenanceTaskScope::InheritedLayer { .. }
        ) | (
            MaintenanceTaskKind::SnapshotPruning,
            MaintenanceTaskScope::Retention
        ) | (
            MaintenanceTaskKind::Retention,
            MaintenanceTaskScope::Retention | MaintenanceTaskScope::Branch(_)
        ) | (
            MaintenanceTaskKind::Quarantine | MaintenanceTaskKind::Purge,
            MaintenanceTaskScope::Quarantine
        ) | (
            MaintenanceTaskKind::Repair,
            MaintenanceTaskScope::Quarantine | MaintenanceTaskScope::Global
        )
    )
}

const fn normalized_coalesce_scope(
    kind: MaintenanceTaskKind,
    scope: MaintenanceTaskScope,
) -> MaintenanceTaskScope {
    match (kind, scope) {
        (MaintenanceTaskKind::Checkpoint, MaintenanceTaskScope::Global) => {
            MaintenanceTaskScope::Checkpoint
        }
        // Compaction keeps its full `TableLevel { branch, level }` scope so a branch can hold
        // one pending compaction per level, giving concurrent workers non-conflicting levels
        // to pick. (Previously collapsed to `Branch`, allowing only one compaction per branch.)
        (_, scope) => scope,
    }
}

/// Whether two Rewrite-lane tasks would contend for overlapping inputs. A level-`L` compaction
/// reads level `L` and writes `L+1`, so two same-branch compactions conflict when their levels
/// are equal or adjacent. Materialization (a non-`TableLevel` rewrite) is treated as conflicting
/// with any rewrite on the same branch. Different branches never conflict.
fn rewrite_tasks_conflict(a: MaintenanceTask, b: MaintenanceTask) -> bool {
    if let (
        MaintenanceTaskScope::TableLevel {
            branch_id: branch_a,
            level: level_a,
        },
        MaintenanceTaskScope::TableLevel {
            branch_id: branch_b,
            level: level_b,
        },
    ) = (a.scope(), b.scope())
    {
        return branch_a == branch_b && level_a.abs_diff(level_b) <= 1;
    }
    match (
        rewrite_scope_branch(a.scope()),
        rewrite_scope_branch(b.scope()),
    ) {
        (Some(branch_a), Some(branch_b)) => branch_a == branch_b,
        _ => false,
    }
}

const fn rewrite_scope_branch(scope: MaintenanceTaskScope) -> Option<BranchId> {
    match scope {
        MaintenanceTaskScope::Branch(branch_id)
        | MaintenanceTaskScope::TableLevel { branch_id, .. }
        | MaintenanceTaskScope::InheritedLayer { branch_id, .. } => Some(branch_id),
        _ => None,
    }
}

/// Default Rewrite-lane concurrency for the durable runtime — matches the default background
/// worker count. Concurrent compaction is correctness-gated (recovery oracle + fault sweep pass
/// at cap > 1); the throughput/admission tuning that will set the final value is ongoing.
const DEFAULT_COMPACTION_LANES: usize = 4;

/// The Rewrite-lane concurrency cap for the durable runtime, env-overridable for the perf A/B
/// sweep (`STRATA_COMPACTION_LANES`, e.g. `1` for a single-lane control). Actual concurrency is
/// additionally bounded by the background worker count.
pub(crate) fn compaction_lane_cap() -> usize {
    std::env::var("STRATA_COMPACTION_LANES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .map_or(DEFAULT_COMPACTION_LANES, |lanes| lanes.max(1))
}

/// Default number of subcompactions (parallel key-range builds) per L0-to-next compaction.
///
/// Off by default (`1` = serial): the parallel fan-out is a measured ~25% L0→L1 regression in the
/// memory-bound (resident) regime, where the merge is CPU-bound and the extra threads only add
/// contention. The machinery stays reachable via `STRATA_SUBCOMPACTIONS` for its honest re-test in
/// BS4, when compaction becomes I/O-bound and the range parallelism should finally pay off.
const DEFAULT_SUBCOMPACTIONS: usize = 1;

/// The subcompaction fan-out for one L0-to-next compaction, env-overridable for the perf A/B
/// sweep (`STRATA_SUBCOMPACTIONS`, e.g. `1` for a serial control). The effective count is
/// additionally bounded by the computed key-range boundaries and available parallelism.
pub(crate) fn subcompaction_cap() -> usize {
    std::env::var("STRATA_SUBCOMPACTIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .map_or(DEFAULT_SUBCOMPACTIONS, |count| count.max(1))
}

fn require_admitted(
    state: LifecycleStateMachine,
    operation: LifecycleOperationKind,
) -> LifecycleResult<LifecycleAdmissionEffect> {
    match state.admit(operation) {
        LifecycleOperationAdmission::Allowed { effect } => Ok(effect),
        LifecycleOperationAdmission::Rejected { reason } => {
            Err(LifecycleError::InvalidLifecycleState { reason })
        }
    }
}
