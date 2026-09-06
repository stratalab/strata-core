//! API runtime handle.

use crate::backend::BackendHandle;
use crate::branch::config::BranchRuntimeConfig;
use crate::branch::facts::BranchReleasePlan;
use crate::branch::read::{
    BranchHistoryOptions, BranchReadBound, BranchReadView, BranchScanBounds, BranchUserKeyBound,
};
use crate::commit::{
    CommitBranchGeneration, CommitBranchGenerationGuard, CommitDurabilityClass,
    CommitRuntimeConfig, CommitTimelineEntry, CommitTimelineLookup, CommitTimelineMiss,
    CommitTimelineView, CommitTimestampSource, COMMIT_TIMELINE_SPACE,
};
use crate::lifecycle::{
    collect_storage_pressure_with_budget, estimate_commit_batch_active_bytes,
    BackgroundBackpressureError, BackgroundTaskPriority, CacheBackgroundMaintenanceStep,
    CloseOutcome, CloseOutcomeStatus, DurableBackgroundMaintenanceStep, FlushFrozenRequest,
    FlushTableIdentitySeed, FlushTableObjectId, InlineMaintenanceExecutor, LifecycleBranchCatalog,
    LifecycleBranchDescriptor, LifecycleBranchStatus, LifecycleCacheOpenRequest,
    LifecycleCachePreheatPolicy, LifecycleCacheRuntime, LifecycleCheckpointOutcome,
    LifecycleCodecId, LifecycleCompactionDrainRequest, LifecycleConfig,
    LifecycleDurableLocalOpenRequest, LifecycleDurableLocalRuntime, LifecycleDurableLocalShell,
    LifecycleError, LifecycleMaintenanceSchedulingPolicy, LifecycleMaintenanceStats,
    LifecycleRecoveryRuntime, LifecycleRetentionRequest, LifecycleRetentionScope,
    LifecycleStoragePressure, LifecycleStoragePressureReason, LifecycleStoragePressureSeverity,
    LifecycleWalGrowthOutcome, LifecycleWalGrowthPolicy, LifecycleWalGrowthStatus,
    LifecycleWalGrowthTrigger, LifecycleWriteAdmissionOutcome, LifecycleWriteAdmissionStatus,
    LifecycleWriteThrottlePolicy, MaintenanceCheckpointOptions, MaintenanceClock,
    MaintenanceExecutor, MaintenanceExecutorStats, MaintenanceExecutorStatus, MaintenanceInstant,
    MaintenanceOutcome as LifecycleMaintenanceOutcome,
    MaintenanceOutcomeReasonClass as LifecycleMaintenanceOutcomeReasonClass,
    MaintenanceOutcomeStatus as LifecycleMaintenanceOutcomeStatus,
    MaintenanceTaskKind as LifecycleMaintenanceTaskKind,
    MaintenanceTaskPolicy as LifecycleMaintenanceTaskPolicy,
    MaintenanceTaskPriority as LifecycleMaintenanceTaskPriority,
    MaintenanceTaskRequest as LifecycleMaintenanceTaskRequest,
    MaintenanceTaskScope as LifecycleMaintenanceTaskScope, ManualMaintenanceClock,
    ModeLifecyclePolicy, PreparedPublishStep, RealMaintenanceClock, RecoveryDegradationClass,
    RecoveryFaultKind, RecoveryHealth, RecoveryStrictness, StorageBudgetPool,
    StorageBudgetPressureSeverity, StorageBudgetSnapshot, StorageMode as LifecycleStorageMode,
    StorageOpenOutcome as LifecycleStorageOpenOutcome, StorageOpenPlan, StorageRuntimeBudget,
    ThreadedMaintenanceExecutor,
};
use crate::observability::perf_trace;
use crate::row::{PhysicalKey, StorageRow, StorageSpaceId as RowStorageSpaceId};
use crate::service::{WalGrowthFacts, WalServiceConfig};
use strata_core::{BranchId, CommitVersion, Timestamp};

use super::{
    BranchAction, BranchCleanupSummary, BranchGeneration, BranchOperation, BranchOutcome,
    BranchParentSummary, BranchRequest, BranchStatus, BranchSummary, CommitAdmissionPressureReason,
    CommitAdmissionPressureSeverity, CommitAdmissionSummary, CommitBatch, CommitDurability,
    CommitDurabilitySummary, CommitExpectedVersion, CommitInstantsRequest, CommitSummary,
    DiagnosticsBranchCatalogReport, DiagnosticsBudgetAccuracy, DiagnosticsBudgetPool,
    DiagnosticsBudgetPressure, DiagnosticsBudgetReport, DiagnosticsBudgetUsage,
    DiagnosticsCheckpointReport, DiagnosticsOutcome, DiagnosticsQuarantineReport,
    DiagnosticsReadActivityReport, DiagnosticsRecoveryClass, DiagnosticsRecoveryFault,
    DiagnosticsRecoveryFaultKind, DiagnosticsRecoveryReport, DiagnosticsRequest,
    DiagnosticsRetentionReport, DiagnosticsScope, DiagnosticsSourceLayoutReport,
    DiagnosticsSourceLevelTableCount, DiagnosticsStoragePressureReason,
    DiagnosticsStoragePressureReport, DiagnosticsStoragePressureSeverity,
    DiagnosticsTableReachabilityReport, DiagnosticsTimelineReport, DiagnosticsWalGrowthReport,
    HistoryReadOutcome, HistoryReadRequest, ImmutableSourceScanReadOutcome,
    ImmutableSourceScanReadRequest, MaintenanceDrainSummary, MaintenanceQueueSummary,
    MaintenanceReasonClass, MaintenanceRequest, MaintenanceScope, MaintenanceSummary,
    MaintenanceSummaryStatus, MaintenanceTask, MaintenanceWalGrowthStatus,
    MaintenanceWalGrowthSummary, MaintenanceWalGrowthTrigger, PointReadOutcome, PointReadRequest,
    PrefixScanReadRequest, ReadBound, ReadLimit, RecoveryHealthSummary, ScanReadOutcome,
    ScanReadRequest, StorageApiError, StorageApiErrorClass, StorageApiLowerLayer, StorageApiResult,
    StorageBackend, StorageBackgroundMaintenanceOptions, StorageBudgetPolicy, StorageBudgetSource,
    StorageCachePreheatPolicy, StorageCloseSummary, StorageDurabilityPolicy, StorageKey,
    StorageMaintenanceSchedulingPolicy, StorageMode, StorageOpenDisposition, StorageOpenOptions,
    StorageOpenOutcome, StorageOpenSummary, StorageReadRow, StorageRuntimeState, StorageSpaceId,
    StorageValue, StorageWalGrowthPolicy, TimelineBoundsOutcome, TimelineBoundsRequest,
    TimestampLookupMiss, TimestampLookupOutcome, TimestampLookupRequest, VersionLookupOutcome,
    VersionLookupRequest, WallClockLookupOutcome, WallClockLookupRequest,
};
use crate::api::outcome::StorageCloseEffects;
use parking_lot::{Mutex as ParkingMutex, MutexGuard as ParkingMutexGuard};
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

mod background;
mod commit_group;
#[cfg(all(loom, test))]
mod commit_group_loom;
mod data;
mod diagnostics;
mod error;
mod maintenance;
mod open_close;

use background::{
    BackgroundBlockWaitConfig, BackgroundPressureSnapshot, BackgroundWalGrowthSnapshot, RuntimeSlot,
};

use data::{
    cap_bound_at_visible, flush_request_for_boundary, map_api_commit_batch, map_commit_summary,
    map_immutable_sources, map_scan_rows, map_storage_space, physical_key,
    point_read_row_from_storage_owned, read_row_from_storage, require_version_retained,
    resolve_read_bound, row_is_expired_at_selected_frontier, timeline_committed_at_for_versions,
    timeline_resolve_wall_clock, timeline_view_or_index, visible_tombstone_at_bound,
};
use diagnostics::{
    branch_for_diagnostics_scope, branch_generation_or_default, current_visible,
    diagnostics_mode_from_plan, diagnostics_pressure_report, diagnostics_source_layout_report,
    durable_checkpoint_report, map_branch_catalog_report, map_branch_cleanup,
    map_branch_descriptor, map_budget_report, map_diagnostics_recovery, map_generation_guard,
    map_wal_growth_report, require_valid_branch_identifier,
};
#[cfg(test)]
use error::commit_error;
use error::{branch_error, default_branch_generation, map_lifecycle_error};
#[cfg(any(test, feature = "testkit"))]
pub(crate) use error::{
    map_commit_error_for_test, map_lifecycle_error_for_test, map_maintenance_outcome_for_test,
};
use maintenance::{
    background_priority_for_task_request, drain_cache_background_round,
    drain_durable_background_round, map_checkpoint_summary, map_maintenance_queue_summary,
    map_maintenance_summary, map_maintenance_task_request, map_wal_growth_maintenance_summary,
    map_wal_growth_summary, request_for_outcome, run_next_cache_maintenance,
    run_next_durable_maintenance, unsupported_maintenance_summary, validate_maintenance_request,
};
use open_close::{
    background_executor_mode, background_shutdown_panic_error, durable_backend_handle_for_open,
    lifecycle_plan, map_close_summary, map_open_summary, record_background_close_maintenance_facts,
    with_background_close_facts,
};

const DEFAULT_DATABASE_ID: [u8; 16] = [0x53; 16];
const DEFAULT_BRANCH_ID: BranchId = BranchId::from_bytes([0x01; BranchId::BYTE_LEN]);
const DEFAULT_BRANCH_GENERATION: u64 = 1;
const DEFAULT_TIMESTAMP: Timestamp = Timestamp::from_micros(1);
const API_PHYSICAL_SPACE: &str = "api";
const DEFAULT_BACKGROUND_BLOCK_WAIT_SLICE: Duration = Duration::from_millis(250);
const DEFAULT_BACKGROUND_BLOCK_STALL_DEADLINE: Duration = Duration::from_secs(30);
const DEFAULT_BACKGROUND_BLOCK_NO_RELIEF_ROUNDS: usize = 4;
pub(super) const DEFAULT_BACKGROUND_CLOSE_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedReadBound {
    branch_bound: BranchReadBound,
    selected_timestamp: Option<Timestamp>,
}

#[derive(Debug)]
pub struct StorageRuntime<'a> {
    inner: StorageRuntimeInner,
    open_summary: Option<StorageOpenSummary>,
    last_recovery: Option<DiagnosticsRecoveryReport>,
    last_close: Option<StorageCloseSummary>,
    /// Off-lock mirror of the allocator's last-allocated commit timestamp, in
    /// micros (BS5.1). `0` = not yet sampled (one locked read refreshes it).
    /// Reading this under the runtime lock serialized every writer BEFORE the
    /// commit path, so the write-group join queue always looked empty. The
    /// value is only a clamp candidate: the allocator enforces the real
    /// monotonic floor under the lock, exactly as with the (equally
    /// stale-by-interleaving) locked read this replaces.
    last_allocated_timestamp_micros: AtomicU64,
    // BS4.4i: every durable runtime is owned/`'static` (the borrowed `Durable(<'a>)` variant was
    // removed), so `StorageRuntimeInner` no longer needs a lifetime. The public `StorageRuntime<'a>`
    // signature is retained (it is spelled across downstream crates) by parking the now-inert lifetime
    // here; `open_with_backend` still takes `&'a StorageBackend`, so callers are unchanged.
    _marker: PhantomData<&'a ()>,
}

#[derive(Debug)]
enum StorageRuntimeInner {
    Cache(Box<RuntimeSlot<LifecycleCacheRuntime<ApiTimestampSource>>>),
    DurableOwned(Box<RuntimeSlot<LifecycleDurableLocalRuntime<'static, ApiTimestampSource>>>),
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ApiTimestampSource {
    next_timestamp: Timestamp,
}

impl ApiTimestampSource {
    const fn new(next_timestamp: Timestamp) -> Self {
        Self { next_timestamp }
    }
}

impl CommitTimestampSource for ApiTimestampSource {
    fn next_timestamp(&mut self) -> crate::commit::CommitRuntimeResult<Timestamp> {
        let timestamp = self.next_timestamp;
        self.next_timestamp = timestamp.saturating_add(Duration::from_micros(1));
        Ok(timestamp)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageCloseOptions {
    background_shutdown_timeout: Duration,
}

impl StorageCloseOptions {
    #[must_use]
    pub const fn graceful() -> Self {
        Self {
            background_shutdown_timeout: DEFAULT_BACKGROUND_CLOSE_SHUTDOWN_TIMEOUT,
        }
    }

    #[must_use]
    pub const fn with_background_shutdown_timeout(
        mut self,
        background_shutdown_timeout: Duration,
    ) -> Self {
        self.background_shutdown_timeout = background_shutdown_timeout;
        self
    }

    const fn background_shutdown_timeout(self) -> Duration {
        self.background_shutdown_timeout
    }
}

impl Default for StorageCloseOptions {
    fn default() -> Self {
        Self::graceful()
    }
}

impl StorageRuntime<'static> {
    #[must_use]
    pub const fn closed() -> Self {
        Self {
            inner: StorageRuntimeInner::Closed,
            open_summary: None,
            last_recovery: None,
            last_close: None,
            last_allocated_timestamp_micros: AtomicU64::new(0),
            _marker: PhantomData,
        }
    }

    /// Open an explicit volatile runtime backed by in-memory cache storage.
    pub fn open_ephemeral() -> StorageApiResult<StorageOpenOutcome<'static>> {
        Self::open_cache()
    }

    /// Open a cache-mode runtime for cache-specific tests and demos.
    pub fn open_cache() -> StorageApiResult<StorageOpenOutcome<'static>> {
        Self::open(StorageOpenOptions::cache())
    }

    /// Open durable local storage at `root` with standard durability.
    ///
    /// This is the native product-facing open helper. It never falls back to
    /// cache mode; builds without the `localfs` feature return an explicit
    /// unsupported-capability error instead.
    pub fn open_local(
        root: impl Into<std::path::PathBuf>,
    ) -> StorageApiResult<StorageOpenOutcome<'static>> {
        Self::open_durable_local(root, StorageDurabilityPolicy::Standard)
    }

    /// Open durable local storage at `root` with an explicit durability policy.
    pub fn open_durable_local(
        root: impl Into<std::path::PathBuf>,
        policy: StorageDurabilityPolicy,
    ) -> StorageApiResult<StorageOpenOutcome<'static>> {
        open_durable_local_owned(root, policy)
    }

    /// Open durable local storage at `root` with explicit open options.
    ///
    /// Like [`Self::open_durable_local`], but accepts a full
    /// [`StorageOpenOptions`] so callers can set an explicit memory budget. The
    /// options must select a durable-local mode; other modes are rejected.
    pub fn open_durable_local_with_options(
        root: impl Into<std::path::PathBuf>,
        options: StorageOpenOptions,
    ) -> StorageApiResult<StorageOpenOutcome<'static>> {
        open_durable_local_owned_with_options(root, options)
    }

    pub fn open(options: StorageOpenOptions) -> StorageApiResult<StorageOpenOutcome<'static>> {
        options.validate()?;
        match options.mode() {
            StorageMode::Cache => {
                let backend = StorageBackend::memory();
                Self::open_cache_with_backend(options, &backend)
            }
            StorageMode::DurableLocal { .. } => Err(StorageApiError::InvalidArgument {
                field: "backend",
                reason: "durable local open requires an explicit storage backend handle",
            }),
            StorageMode::ObjectDurableCandidate | StorageMode::DistributedCandidate => {
                unreachable!("unsupported modes are rejected during validation")
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn submit_runtime_state_background_probe_for_test(
        &self,
        ready: std::sync::Arc<std::sync::Barrier>,
        release: std::sync::Arc<std::sync::Barrier>,
        observed_open: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> bool {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot
                .submit_background(BackgroundTaskPriority::High, move |runtime| {
                    ready.wait();
                    release.wait();
                    let runtime = runtime.lock();
                    observed_open.store(
                        runtime.state() == crate::lifecycle::LifecycleState::Open,
                        std::sync::atomic::Ordering::Release,
                    );
                })
                .is_ok(),
            StorageRuntimeInner::DurableOwned(slot) => slot
                .submit_background(BackgroundTaskPriority::High, move |runtime| {
                    ready.wait();
                    release.wait();
                    let runtime = runtime.lock();
                    observed_open.store(
                        runtime.state() == crate::lifecycle::LifecycleState::Open,
                        std::sync::atomic::Ordering::Release,
                    );
                })
                .is_ok(),
            StorageRuntimeInner::Closed => false,
        }
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn submit_panicking_background_task_for_test(
        &self,
        ready: std::sync::Arc<std::sync::Barrier>,
        release: std::sync::Arc<std::sync::Barrier>,
    ) -> bool {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot
                .submit_background(BackgroundTaskPriority::High, move |_runtime| {
                    ready.wait();
                    release.wait();
                    panic!("intentional background close panic test");
                })
                .is_ok(),
            StorageRuntimeInner::DurableOwned(slot) => slot
                .submit_background(BackgroundTaskPriority::High, move |_runtime| {
                    ready.wait();
                    release.wait();
                    panic!("intentional background close panic test");
                })
                .is_ok(),
            StorageRuntimeInner::Closed => false,
        }
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn background_shutdown_requested_flag_for_test(
        &self,
    ) -> Option<std::sync::Arc<std::sync::atomic::AtomicBool>> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot.background_shutdown_requested_flag(),
            StorageRuntimeInner::DurableOwned(slot) => slot.background_shutdown_requested_flag(),
            StorageRuntimeInner::Closed => None,
        }
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn wait_background_idle_for_test(&self) {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot.wait_background_idle(),
            StorageRuntimeInner::DurableOwned(slot) => slot.wait_background_idle(),
            StorageRuntimeInner::Closed => {}
        }
    }

    /// C3a test seam: shrink the preheat chunk budget so multi-chunk passes
    /// are constructible on tiny fixtures.
    #[cfg(test)]
    #[allow(dead_code, reason = "consumer is gated on localfs + perf-trace")]
    pub(crate) fn set_cache_preheat_chunk_bytes_for_test(&self, chunk_bytes: u64) {
        if let StorageRuntimeInner::DurableOwned(slot) = &self.inner {
            slot.lock().cache_preheat_chunk_bytes_for_test = Some(chunk_bytes);
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn wait_background_idle_until_for_test(
        &self,
        timeout: Duration,
    ) -> Option<MaintenanceExecutorStats> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot.wait_background_idle_until(timeout),
            StorageRuntimeInner::DurableOwned(slot) => slot.wait_background_idle_until(timeout),
            StorageRuntimeInner::Closed => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_lifecycle_maintenance_kinds_for_test(
        &self,
    ) -> Vec<LifecycleMaintenanceTaskKind> {
        match &self.inner {
            StorageRuntimeInner::Cache(_) | StorageRuntimeInner::Closed => Vec::new(),
            StorageRuntimeInner::DurableOwned(slot) => {
                slot.lock().pending_maintenance_kinds_for_test()
            }
        }
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn pending_flush_watermark_candidate_for_test(&self) -> Option<CommitVersion> {
        match &self.inner {
            StorageRuntimeInner::Cache(_) | StorageRuntimeInner::Closed => None,
            StorageRuntimeInner::DurableOwned(slot) => {
                slot.lock().pending_flush_watermark_candidate_for_test()
            }
        }
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn background_now_for_test(&self) -> Option<MaintenanceInstant> {
        self.background_now_for_current_runtime()
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn set_background_drain_limits_for_test(
        &mut self,
        max_tasks: usize,
        max_runtime: Duration,
    ) -> bool {
        match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                slot.set_background_drain_limits(max_tasks, max_runtime)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                slot.set_background_drain_limits(max_tasks, max_runtime)
            }
            StorageRuntimeInner::Closed => false,
        }
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn set_background_block_wait_for_test(
        &mut self,
        wait_slice: Duration,
        stall_deadline: Duration,
        no_relief_rounds: usize,
    ) -> bool {
        match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => slot.set_background_block_wait_for_test(
                wait_slice,
                stall_deadline,
                no_relief_rounds,
            ),
            StorageRuntimeInner::DurableOwned(slot) => slot.set_background_block_wait_for_test(
                wait_slice,
                stall_deadline,
                no_relief_rounds,
            ),
            StorageRuntimeInner::Closed => false,
        }
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn shutdown_background_for_test(&self) {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let _ = slot.shutdown_background(Some(DEFAULT_BACKGROUND_CLOSE_SHUTDOWN_TIMEOUT));
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let _ = slot.shutdown_background(Some(DEFAULT_BACKGROUND_CLOSE_SHUTDOWN_TIMEOUT));
            }
            StorageRuntimeInner::Closed => {}
        }
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(crate) fn submit_stale_background_wake_for_test(&self) {
        self.notify_background_drain_for_current_runtime(BackgroundTaskPriority::Low);
    }

    #[cfg(test)]
    pub(crate) fn enqueue_lifecycle_maintenance_for_test(
        &mut self,
        task: LifecycleMaintenanceTaskRequest,
    ) -> StorageApiResult<MaintenanceQueueSummary> {
        self.require_open("maintenance enqueue requires an open runtime")?;
        match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let (status, failures) = {
                    let mut runtime = slot.lock();
                    runtime
                        .enqueue_maintenance(task)
                        .map_err(map_lifecycle_error)?;
                    (
                        runtime.maintenance_status(),
                        runtime.recent_maintenance_failures(),
                    )
                };
                slot.notify_background_drain(background_priority_for_task_request(task));
                Ok(map_maintenance_queue_summary(
                    status,
                    failures,
                    slot.background_stats(),
                ))
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let (status, failures) = {
                    let mut runtime = slot.lock();
                    runtime
                        .enqueue_maintenance(task)
                        .map_err(map_lifecycle_error)?;
                    (
                        runtime.maintenance_status(),
                        runtime.recent_maintenance_failures(),
                    )
                };
                slot.notify_background_drain(background_priority_for_task_request(task));
                Ok(map_maintenance_queue_summary(
                    status,
                    failures,
                    slot.background_stats(),
                ))
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "maintenance enqueue requires an open runtime",
            }),
        }
    }
}

fn assemble_durable_runtime(
    options: StorageOpenOptions,
    backend: BackendHandle<'static>,
) -> StorageApiResult<(
    LifecycleDurableLocalRuntime<'static, ApiTimestampSource>,
    StorageOpenSummary,
    DiagnosticsRecoveryReport,
    LifecycleConfig,
)> {
    let (plan, budget_source) = lifecycle_plan(options)?;
    let wal_config = wal_service_config(options)?;
    let request = LifecycleDurableLocalOpenRequest::new(
        plan,
        DEFAULT_DATABASE_ID,
        DEFAULT_BRANCH_ID,
        default_branch_generation()?,
        BranchRuntimeConfig::default(),
        CommitRuntimeConfig::default(),
        wal_config,
    )
    .map_err(map_lifecycle_error)?;
    let mut shell =
        LifecycleDurableLocalShell::assemble(request, backend, default_timestamp_source())
            .map_err(map_lifecycle_error)?;
    let recovery_request =
        crate::lifecycle::LifecycleRecoveryRequest::from_open_plan(shell.open_plan())
            .map_err(map_lifecycle_error)?;
    let recovery = LifecycleRecoveryRuntime::new(&mut shell)
        .recover(&recovery_request)
        .map_err(map_lifecycle_error)?;
    let runtime = shell
        .complete_recovery(&recovery)
        .map_err(map_lifecycle_error)?;
    let summary = map_open_summary(
        runtime.open_outcome(),
        options.mode(),
        options,
        budget_source,
    );
    let recovery_report = map_diagnostics_recovery(runtime.current_recovery_health());
    let config = runtime.open_plan().lifecycle_config();
    Ok((runtime, summary, recovery_report, config))
}

/// Whether a commit's WAL-growth evaluation warrants pacing the writer on
/// background progress: only when it actually enqueued (or coalesced into)
/// maintenance. A deferred evaluation enqueued nothing (#2792) — pacing would
/// wait on relief the structurally-deferred task class cannot deliver, which
/// on a multi-branch store with the checkpoint guard latched turns bounded
/// backpressure into an unbounded stall.
pub(crate) const fn wal_growth_pacing_applies(status: LifecycleWalGrowthStatus) -> bool {
    matches!(
        status,
        LifecycleWalGrowthStatus::MaintenanceEnqueued
            | LifecycleWalGrowthStatus::MaintenanceCoalesced
    )
}

fn wal_service_config(options: StorageOpenOptions) -> StorageApiResult<WalServiceConfig> {
    // W3.3a: every production durable open coalesces WAL appends; the bare
    // constructors stay direct for service-level byte-contract tests and the
    // buffered-vs-direct differential oracle.
    let config = options
        .wal_segment_size_for_test()
        .map_or_else(WalServiceConfig::default, WalServiceConfig::new)
        .with_append_buffer_bytes(crate::service::DEFAULT_WAL_APPEND_BUFFER_BYTES);
    config
        .validate()
        .map_err(|_| StorageApiError::InvalidArgument {
            field: "wal_segment_size",
            reason: "WAL segment size is invalid",
        })?;
    Ok(config)
}

// Called only by the localfs open path.
#[cfg_attr(not(feature = "localfs"), allow(dead_code))]
fn open_durable_with_owned_backend_handle<'runtime>(
    options: StorageOpenOptions,
    backend: BackendHandle<'static>,
) -> StorageApiResult<StorageOpenOutcome<'runtime>> {
    let executor_mode = background_executor_mode(options.maintenance_scheduling_policy());
    let background_config = options.background_maintenance();
    let (runtime, summary, recovery_report, config) = assemble_durable_runtime(options, backend)?;
    let mode_policy = runtime.open_plan().lifecycle_policy();
    Ok(StorageOpenOutcome::new(
        StorageRuntime {
            inner: StorageRuntimeInner::DurableOwned(Box::new(
                RuntimeSlot::new_with_background_arc_drain(
                    runtime,
                    config,
                    background_config,
                    executor_mode,
                    mode_policy,
                    drain_durable_background_round,
                ),
            )),
            open_summary: Some(summary),
            last_recovery: Some(recovery_report),
            last_close: None,
            last_allocated_timestamp_micros: AtomicU64::new(0),
            _marker: PhantomData,
        },
        summary,
    ))
}

#[cfg(feature = "localfs")]
fn open_durable_local_owned(
    root: impl Into<std::path::PathBuf>,
    policy: StorageDurabilityPolicy,
) -> StorageApiResult<StorageOpenOutcome<'static>> {
    open_durable_local_owned_with_options(root, StorageOpenOptions::durable_local(policy))
}

#[cfg(not(feature = "localfs"))]
fn open_durable_local_owned(
    _root: impl Into<std::path::PathBuf>,
    _policy: StorageDurabilityPolicy,
) -> StorageApiResult<StorageOpenOutcome<'static>> {
    Err(StorageApiError::UnsupportedCapability {
        capability: "localfs",
        reason: "durable local storage requires the localfs feature",
    })
}

#[cfg(feature = "localfs")]
fn open_durable_local_owned_with_options(
    root: impl Into<std::path::PathBuf>,
    options: StorageOpenOptions,
) -> StorageApiResult<StorageOpenOutcome<'static>> {
    options.validate()?;
    if !matches!(options.mode(), StorageMode::DurableLocal { .. }) {
        return Err(StorageApiError::InvalidArgument {
            field: "mode",
            reason: "durable local open requires a durable-local mode",
        });
    }
    let root = root.into();
    reject_pre_v1_layout(&root)?;
    let backend = StorageBackend::local_fs(root);
    open_durable_with_owned_backend_handle(options, backend.into_backend_handle())
}

/// V1 cutover (hard rule 42): pre-V1 development databases are rejected with
/// a structured layout error — a fresh V1 layout must never be silently
/// created inside one. The marker file names are owned by the layout module.
#[cfg(feature = "localfs")]
fn reject_pre_v1_layout(root: &std::path::Path) -> StorageApiResult<()> {
    if crate::layout::PRE_V1_LAYOUT_MARKER_FILES
        .iter()
        .any(|marker| root.join(marker).is_file())
    {
        return Err(StorageApiError::IncompatibleLayout {
            reason: "directory holds a pre-V1 database layout",
        });
    }
    Ok(())
}

#[cfg(not(feature = "localfs"))]
fn open_durable_local_owned_with_options(
    _root: impl Into<std::path::PathBuf>,
    _options: StorageOpenOptions,
) -> StorageApiResult<StorageOpenOutcome<'static>> {
    Err(StorageApiError::UnsupportedCapability {
        capability: "localfs",
        reason: "durable local storage requires the localfs feature",
    })
}

impl<'a> StorageRuntime<'a> {
    /// Open durable local storage with an explicit backend handle.
    ///
    /// The returned runtime borrows `backend`; keep the backend alive for at
    /// least as long as the runtime.
    pub fn open_durable_local_with_backend(
        policy: StorageDurabilityPolicy,
        backend: &'a StorageBackend,
    ) -> StorageApiResult<StorageOpenOutcome<'a>> {
        Self::open_with_backend(StorageOpenOptions::durable_local(policy), backend)
    }

    pub fn open_with_backend(
        options: StorageOpenOptions,
        backend: &'a StorageBackend,
    ) -> StorageApiResult<StorageOpenOutcome<'a>> {
        options.validate()?;
        match options.mode() {
            StorageMode::Cache => Self::open_cache_with_backend(options, backend),
            StorageMode::DurableLocal { .. } => {
                // BS4.4i: durable runtimes are uniformly owned/`'static`; the borrowed `Durable`
                // variant is gone. A non-ownable backend (in-memory) yields a policy-appropriate
                // error from `durable_backend_handle_for_open` rather than a borrowed runtime.
                let handle = durable_backend_handle_for_open(options, backend)?;
                open_durable_with_owned_backend_handle(options, handle)
            }
            StorageMode::ObjectDurableCandidate | StorageMode::DistributedCandidate => {
                unreachable!("unsupported modes are rejected during validation")
            }
        }
    }

    #[must_use]
    pub const fn state(&self) -> StorageRuntimeState {
        match self.inner {
            StorageRuntimeInner::Cache(_) | StorageRuntimeInner::DurableOwned(_) => {
                StorageRuntimeState::Open
            }
            StorageRuntimeInner::Closed => StorageRuntimeState::Closed,
        }
    }

    #[must_use]
    pub const fn is_open(&self) -> bool {
        matches!(
            self.inner,
            StorageRuntimeInner::Cache(_) | StorageRuntimeInner::DurableOwned(_)
        )
    }

    pub fn close(&mut self) -> StorageApiResult<StorageCloseSummary> {
        self.close_with_options(StorageCloseOptions::graceful())
    }

    /// Closes the runtime using the supplied close policy.
    ///
    /// If a background worker panic is discovered during shutdown, this method
    /// returns that error and leaves the runtime open so callers can inspect the
    /// failure and retry close. Dropping the runtime after such an error only
    /// requests background shutdown; durable runtimes rely on recovery at the
    /// next open rather than a clean close summary.
    pub fn close_with_options(
        &mut self,
        options: StorageCloseOptions,
    ) -> StorageApiResult<StorageCloseSummary> {
        let background_shutdown_timeout = Some(options.background_shutdown_timeout());
        match &mut self.inner {
            StorageRuntimeInner::Cache(runtime) => {
                let background_shutdown = runtime.shutdown_background(background_shutdown_timeout);
                if let Some(error) = background_shutdown_panic_error(background_shutdown.as_ref()) {
                    return Err(error);
                }
                let mut runtime = runtime.lock();
                let maintenance_before_close = runtime.maintenance_status().stats();
                let recovery = map_diagnostics_recovery(runtime.open_outcome().recovery_health());
                let close = runtime.close().map_err(map_lifecycle_error)?;
                let maintenance_after_close = runtime.maintenance_status().stats();
                record_background_close_maintenance_facts(
                    maintenance_before_close,
                    maintenance_after_close,
                );
                let summary = with_background_close_facts(
                    map_close_summary(close, false),
                    background_shutdown.as_ref().map(|shutdown| &shutdown.stats),
                );
                drop(runtime);
                self.inner = StorageRuntimeInner::Closed;
                self.last_recovery = Some(recovery);
                self.last_close = Some(summary);
                Ok(summary)
            }
            StorageRuntimeInner::DurableOwned(runtime) => {
                let background_shutdown = runtime.shutdown_background(background_shutdown_timeout);
                if let Some(error) = background_shutdown_panic_error(background_shutdown.as_ref()) {
                    return Err(error);
                }
                let mut runtime = runtime.lock();
                let maintenance_before_close = runtime.maintenance_status().stats();
                let close = runtime.close().map_err(map_lifecycle_error)?;
                let maintenance_after_close = runtime.maintenance_status().stats();
                record_background_close_maintenance_facts(
                    maintenance_before_close,
                    maintenance_after_close,
                );
                let recovery = map_diagnostics_recovery(runtime.current_recovery_health());
                let summary = with_background_close_facts(
                    map_close_summary(close, false),
                    background_shutdown.as_ref().map(|shutdown| &shutdown.stats),
                );
                drop(runtime);
                self.inner = StorageRuntimeInner::Closed;
                self.last_recovery = Some(recovery);
                self.last_close = Some(summary);
                Ok(summary)
            }
            StorageRuntimeInner::Closed => Ok(self.last_close.map_or_else(
                || {
                    StorageCloseSummary::with_close_facts(
                        StorageRuntimeState::Closed,
                        true,
                        StorageCloseEffects::empty(),
                    )
                },
                |summary| summary.with_idempotent(true),
            )),
        }
    }

    pub fn require_open(&self, operation: &'static str) -> StorageApiResult<()> {
        if self.is_open() {
            Ok(())
        } else {
            Err(StorageApiError::InvalidRuntimeState { reason: operation })
        }
    }

    /// Commit a batch. Takes `&self` (BS2.4b) so a shared runtime can serve reads and forks
    /// concurrently with a writer; concurrent `commit` calls are serialized by the runtime lock.
    /// Commit **versions** are always strictly monotonic. Commit **timestamps** are strictly
    /// monotonic under the intended single-writer pattern; concurrent writers may share a timestamp
    /// (the base is read before the commit lock), which the MVCC timeline keeps both of — resolved
    /// by version, with `AtTimestamp` returning the latest. Serialize commits for strict timestamp
    /// monotonicity.
    pub fn commit(&self, batch: &CommitBatch) -> StorageApiResult<CommitSummary> {
        self.execute_commit(batch, None)
    }

    /// Commit a batch stamped with a caller-supplied timestamp.
    ///
    /// This is the replay entry point: writers reproducing logical content
    /// that already carries commit timestamps (artifact import,
    /// replay-shaped tooling) use it to preserve those temporal facts. The
    /// explicit timestamp must be at or after the runtime's monotonic
    /// commit-timestamp floor — equal is allowed, earlier is rejected with
    /// `invalid_argument.storage_api.argument`. Replaying commits in
    /// non-decreasing timestamp order always satisfies the floor. Commit
    /// versions remain runtime-allocated and strictly monotonic; only the
    /// timestamp is caller-controlled. Ordinary writers use [`Self::commit`].
    pub fn commit_at(
        &self,
        batch: &CommitBatch,
        timestamp: Timestamp,
    ) -> StorageApiResult<CommitSummary> {
        self.execute_commit(batch, Some(timestamp))
    }

    pub fn branch(&self, request: &BranchRequest) -> StorageApiResult<BranchOutcome> {
        match request.action() {
            BranchAction::Create => self.create_branch_request(request),
            BranchAction::Describe => self.describe_branch_request(request),
            BranchAction::List => self.list_branch_request(),
            BranchAction::ForkCurrent { source } => {
                require_valid_branch_identifier(request.branch_id(), "branch_id")?;
                require_valid_branch_identifier(source, "source_branch_id")?;
                // #2521: a source with NO rows forks at version zero — the
                // legitimate empty-fork case (empty child, parent linkage
                // intact). Callers must never paper over other fork errors
                // by fabricating an unparented empty branch: `current_branch_
                // version` resolves from content facts (#2852), so a
                // populated source always resolves a real version here.
                let version = match self.current_branch_version(source) {
                    Ok(version) => version,
                    Err(StorageApiError::RetainedHistoryUnavailable { .. }) => CommitVersion::ZERO,
                    Err(error) => return Err(error),
                };
                self.fork_branch_at_version(request, source, version, None)
            }
            BranchAction::ForkAtVersion { source, version } => {
                require_valid_branch_identifier(request.branch_id(), "branch_id")?;
                require_valid_branch_identifier(source, "source_branch_id")?;
                self.require_retained_version_watermark(source, version)?;
                self.fork_branch_at_version(request, source, version, None)
            }
            BranchAction::ForkAtTimestamp { source, timestamp } => {
                require_valid_branch_identifier(request.branch_id(), "branch_id")?;
                require_valid_branch_identifier(source, "source_branch_id")?;
                let timeline = self.timeline_view(source)?;
                let lookup = timeline.version_at_or_before(timestamp);
                let version = match lookup.miss() {
                    CommitTimelineMiss::Matched => lookup.matched_version().ok_or(
                        StorageApiError::RetainedHistoryUnavailable {
                            branch_id: source,
                            reason: "timestamp lookup did not return a retained version",
                        },
                    )?,
                    CommitTimelineMiss::BeforeRetainedHistory | CommitTimelineMiss::Empty => {
                        return Err(StorageApiError::TimestampHistoryUnavailable {
                            branch_id: source,
                            reason: "timestamp is outside retained timeline history",
                        });
                    }
                    CommitTimelineMiss::AfterLatestRetained => {
                        return Err(StorageApiError::TimestampHistoryUnavailable {
                            branch_id: source,
                            reason: "timestamp is newer than retained timeline history",
                        });
                    }
                };
                self.fork_branch_at_version(request, source, version, Some(timestamp))
            }
            BranchAction::Clear => self.clear_branch_request(request),
            BranchAction::Delete => self.delete_branch_request(request),
        }
    }

    pub fn maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        self.require_open("maintenance requires an open runtime")?;
        validate_maintenance_request(request)?;
        match request.task() {
            MaintenanceTask::Checkpoint => self.checkpoint_maintenance(request),
            MaintenanceTask::Flush => self.flush_maintenance(request),
            MaintenanceTask::Compact => self.compaction_maintenance(request),
            MaintenanceTask::Materialize => self.materialization_maintenance(request),
            MaintenanceTask::Retain => self.retention_maintenance(request),
            MaintenanceTask::SnapshotPruning => self.snapshot_pruning_maintenance(request),
            MaintenanceTask::Reclaim => self.reclaim_maintenance(request),
            MaintenanceTask::Quarantine => self.quarantine_maintenance(request),
            MaintenanceTask::Purge => self.purge_maintenance(request),
            MaintenanceTask::Repair => self.repair_maintenance(request),
            MaintenanceTask::WalGrowth => self.wal_growth_maintenance(request),
        }
    }

    pub fn maintenance_status(&self) -> StorageApiResult<MaintenanceQueueSummary> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let runtime = slot.lock();
                Ok(map_maintenance_queue_summary(
                    runtime.maintenance_status(),
                    runtime.recent_maintenance_failures(),
                    slot.background_stats(),
                ))
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let runtime = slot.lock();
                Ok(map_maintenance_queue_summary(
                    runtime.maintenance_status(),
                    runtime.recent_maintenance_failures(),
                    slot.background_stats(),
                ))
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "maintenance status requires an open runtime",
            }),
        }
    }

    pub fn diagnostics(&self, request: DiagnosticsRequest) -> StorageApiResult<DiagnosticsOutcome> {
        match &self.inner {
            StorageRuntimeInner::Cache(runtime) => self.cache_diagnostics(request, runtime),
            StorageRuntimeInner::DurableOwned(runtime) => {
                self.durable_diagnostics(request, runtime)
            }
            StorageRuntimeInner::Closed => Ok(DiagnosticsOutcome::new(
                request.scope(),
                StorageRuntimeState::Closed,
                self.open_summary.map(StorageOpenSummary::mode),
                None,
                self.last_recovery
                    .clone()
                    .unwrap_or_else(DiagnosticsRecoveryReport::unknown),
                None,
                DiagnosticsBudgetReport::unknown(),
                DiagnosticsStoragePressureReport::unknown(),
                DiagnosticsSourceLayoutReport::unknown(),
                DiagnosticsReadActivityReport::unknown(),
                DiagnosticsTableReachabilityReport::unknown(),
                DiagnosticsRetentionReport::unknown(),
                DiagnosticsQuarantineReport::unknown(),
                DiagnosticsCheckpointReport::unknown(),
                DiagnosticsWalGrowthReport::unknown(),
                DiagnosticsBranchCatalogReport::unknown(),
                DiagnosticsTimelineReport::unknown(),
            )),
        }
    }

    fn cache_diagnostics<S>(
        &self,
        request: DiagnosticsRequest,
        slot: &RuntimeSlot<LifecycleCacheRuntime<S>>,
    ) -> StorageApiResult<DiagnosticsOutcome> {
        let branch_id = branch_for_diagnostics_scope(request.scope());
        let branches = self.list_branches(true)?;
        let visible = current_visible(self);
        let timeline = self.diagnostics_timeline(branch_id);
        let runtime = slot.lock();
        let wal_growth = runtime.evaluate_wal_growth_policy();
        Ok(DiagnosticsOutcome::new(
            request.scope(),
            StorageRuntimeState::Open,
            Some(diagnostics_mode_from_plan(
                self.open_summary,
                runtime.open_plan(),
            )),
            visible,
            map_diagnostics_recovery(runtime.open_outcome().recovery_health()),
            Some(map_maintenance_queue_summary(
                runtime.maintenance_status(),
                runtime.recent_maintenance_failures(),
                slot.background_stats(),
            )),
            map_budget_report(
                &runtime.budget_snapshot(),
                runtime.budget_total_used_bytes(),
                runtime.budget_global_pressure(),
            ),
            diagnostics_pressure_report(
                runtime.branch_catalog(),
                branch_id,
                runtime.maintenance_status(),
                runtime.open_plan().lifecycle_config().storage_budget(),
                runtime.open_plan().lifecycle_policy(),
            ),
            diagnostics_source_layout_report(runtime.branch_catalog(), branch_id),
            DiagnosticsReadActivityReport::unknown(),
            DiagnosticsTableReachabilityReport::unsupported(),
            DiagnosticsRetentionReport::unsupported(),
            DiagnosticsQuarantineReport::unsupported(),
            DiagnosticsCheckpointReport::unsupported(),
            map_wal_growth_report(
                runtime.open_plan().lifecycle_config().wal_growth_policy(),
                Some(wal_growth.facts()),
                Some(map_wal_growth_summary(&wal_growth)),
            ),
            map_branch_catalog_report(&branches),
            timeline,
        ))
    }

    fn durable_diagnostics<S>(
        &self,
        request: DiagnosticsRequest,
        slot: &RuntimeSlot<LifecycleDurableLocalRuntime<'_, S>>,
    ) -> StorageApiResult<DiagnosticsOutcome> {
        let branch_id = branch_for_diagnostics_scope(request.scope());
        let branches = self.list_branches(true)?;
        let visible = current_visible(self);
        let timeline = self.diagnostics_timeline(branch_id);
        let runtime = slot.lock();
        let table_catalog = runtime.table_catalog();
        Ok(DiagnosticsOutcome::new(
            request.scope(),
            StorageRuntimeState::Open,
            Some(diagnostics_mode_from_plan(
                self.open_summary,
                runtime.open_plan(),
            )),
            visible,
            map_diagnostics_recovery(runtime.current_recovery_health()),
            Some(map_maintenance_queue_summary(
                runtime.maintenance_status(),
                runtime.recent_maintenance_failures(),
                slot.background_stats(),
            )),
            map_budget_report(
                &runtime.budget_snapshot(),
                runtime.budget_total_used_bytes(),
                runtime.budget_global_pressure(),
            ),
            diagnostics_pressure_report(
                runtime.branch_catalog(),
                branch_id,
                runtime.maintenance_status(),
                runtime.open_plan().lifecycle_config().storage_budget(),
                runtime.open_plan().lifecycle_policy(),
            ),
            diagnostics_source_layout_report(runtime.branch_catalog(), branch_id),
            DiagnosticsReadActivityReport::unknown(),
            DiagnosticsTableReachabilityReport::known(
                table_catalog.entry_count(),
                table_catalog.object_count(),
                Some(table_catalog.next_manifest_sequence()),
            ),
            DiagnosticsRetentionReport::known(None, Some(runtime.pending_releases().len()), None),
            DiagnosticsQuarantineReport::unknown(),
            durable_checkpoint_report(&runtime),
            map_wal_growth_report(
                runtime.open_plan().lifecycle_config().wal_growth_policy(),
                runtime.current_wal_growth_facts().ok(),
                runtime
                    .last_wal_growth_outcome()
                    .map(map_wal_growth_summary),
            ),
            map_branch_catalog_report(&branches),
            timeline,
        ))
    }

    pub fn enqueue_maintenance(
        &self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceQueueSummary> {
        self.require_open("maintenance enqueue requires an open runtime")?;
        validate_maintenance_request(request)?;
        let task = map_maintenance_task_request(self, request)?;
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let (status, failures) = {
                    let mut runtime = slot.lock();
                    runtime
                        .enqueue_maintenance(task)
                        .map_err(map_lifecycle_error)?;
                    (
                        runtime.maintenance_status(),
                        runtime.recent_maintenance_failures(),
                    )
                };
                slot.notify_background_drain(background_priority_for_task_request(task));
                Ok(map_maintenance_queue_summary(
                    status,
                    failures,
                    slot.background_stats(),
                ))
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let (status, failures) = {
                    let mut runtime = slot.lock();
                    runtime
                        .enqueue_maintenance(task)
                        .map_err(map_lifecycle_error)?;
                    (
                        runtime.maintenance_status(),
                        runtime.recent_maintenance_failures(),
                    )
                };
                slot.notify_background_drain(background_priority_for_task_request(task));
                Ok(map_maintenance_queue_summary(
                    status,
                    failures,
                    slot.background_stats(),
                ))
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "maintenance enqueue requires an open runtime",
            }),
        }
    }

    pub fn run_next_maintenance(&mut self) -> StorageApiResult<Option<MaintenanceSummary>> {
        let outcome = match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                run_next_cache_maintenance(&mut runtime)?
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                run_next_durable_maintenance(&mut runtime)?
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "maintenance run requires an open runtime",
                });
            }
        };
        Ok(outcome.map(|outcome| map_maintenance_summary(request_for_outcome(&outcome), &outcome)))
    }

    pub fn drain_maintenance(&mut self) -> StorageApiResult<MaintenanceDrainSummary> {
        self.require_open("maintenance drain requires an open runtime")?;
        let mut outcomes = Vec::new();
        while let Some(outcome) = self.run_next_maintenance()? {
            outcomes.push(outcome);
        }
        let queue = self.maintenance_status()?;
        let drained_tasks = outcomes.len();
        Ok(MaintenanceDrainSummary::new(drained_tasks, outcomes, queue))
    }

    /// Load the off-lock read handles for a branch: the visible-version bound `V` (Acquire) FIRST,
    /// then the published snapshot `S` (a single `ArcSwap` load). V-before-S is the read protocol's
    /// ordering guarantee — a reader seeing `V=v` observes a snapshot at least as new as any
    /// structural change published under the lock before `v`. A `None` snapshot means the branch
    /// has no published slot (never created, or deleted) → not found.
    fn load_published_snapshot(
        &self,
        branch_id: BranchId,
    ) -> StorageApiResult<(u64, Arc<BranchReadView>)> {
        // Tuple evaluation is left-to-right, so `V` (Acquire) is loaded before `S`.
        let (visible, snapshot) = match &self.inner {
            StorageRuntimeInner::Cache(slot) => (slot.visible(), slot.load_snapshot(branch_id)),
            StorageRuntimeInner::DurableOwned(slot) => {
                (slot.visible(), slot.load_snapshot(branch_id))
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "read requires an open runtime",
                });
            }
        };
        let snapshot = snapshot.ok_or(StorageApiError::BranchNotFound { branch_id })?;
        Ok((visible, snapshot))
    }

    /// Load a branch's published snapshot off-lock, for the BS2.4b snapshot-lifetime probe (holding
    /// an `Arc<BranchReadView>` across compaction/flush installs). `None` if the branch has no slot.
    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn load_snapshot_for_test(
        &self,
        branch_id: BranchId,
    ) -> Option<Arc<BranchReadView>> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot.load_snapshot(branch_id),
            StorageRuntimeInner::DurableOwned(slot) => slot.load_snapshot(branch_id),
            StorageRuntimeInner::Closed => None,
        }
    }

    pub fn read_point(&self, request: &PointReadRequest) -> StorageApiResult<PointReadOutcome> {
        // B4: the point-read runtime timer splits engine-layer tax from
        // storage work (bench read latency minus this timer's mean).
        let read_timer = perf_trace::start_timer();
        let outcome = self.read_point_inner(request);
        perf_trace::record_api_read_point_runtime_elapsed(read_timer);
        outcome
    }

    fn read_point_inner(&self, request: &PointReadRequest) -> StorageApiResult<PointReadOutcome> {
        let key = physical_key(request.branch_id(), request.storage_space(), request.key())?;
        let (visible, view) = self.load_published_snapshot(request.branch_id())?;
        if request.bound() == ReadBound::Latest {
            let row = view
                .read_point_or_tombstone(
                    &key,
                    BranchReadBound::at_version(CommitVersion::new(visible)),
                )
                .map_err(branch_error)?;
            // B4: the branch row is owned — move its key and value into the
            // public row instead of copying at the boundary.
            let row = row
                .map(|row| point_read_row_from_storage_owned(row.into_storage_row()))
                .transpose()?;
            return Ok(PointReadOutcome::new(row));
        }
        let resolved = resolve_read_bound(&view, request.bound())?;
        let capped = cap_bound_at_visible(resolved.branch_bound, visible);
        let row = match view.read_point(&key, capped).map_err(branch_error)? {
            Some(row) => {
                // Expiry is decided on the reference (scalar reads), then the
                // surviving row moves out.
                if row_is_expired_at_selected_frontier(row.row(), resolved.selected_timestamp) {
                    None
                } else {
                    Some(point_read_row_from_storage_owned(row.into_storage_row())?)
                }
            }
            None => visible_tombstone_at_bound(&view, &key, capped)?,
        };
        Ok(PointReadOutcome::new(row))
    }

    pub fn read_history(
        &self,
        request: &HistoryReadRequest,
    ) -> StorageApiResult<HistoryReadOutcome> {
        let (visible, view) = self.load_published_snapshot(request.branch_id())?;
        let key = physical_key(request.branch_id(), request.storage_space(), request.key())?;
        if let Some(version) = request.before_version_bound() {
            require_version_retained(&view, version)?;
        }
        let mut options =
            BranchHistoryOptions::all().include_tombstones(request.includes_tombstones());
        if let Some(version) = request.before_version_bound() {
            options = options.before_version(version);
        }
        if let Some(limit) = request.limit_bound() {
            options = options.limit(limit.get());
        }
        let rows = view
            .history_visible(&key, options, CommitVersion::new(visible))
            .map_err(branch_error)?
            .iter()
            .map(|row| read_row_from_storage(row.row()))
            .collect::<StorageApiResult<Vec<_>>>()?;
        Ok(HistoryReadOutcome::new(rows))
    }

    pub fn scan_prefix(
        &self,
        request: &PrefixScanReadRequest,
    ) -> StorageApiResult<ScanReadOutcome> {
        let prefix = physical_key(
            request.branch_id(),
            request.storage_space(),
            request.prefix(),
        )?;
        let (visible, view) = self.load_published_snapshot(request.branch_id())?;
        let bounds = BranchScanBounds::prefix(&prefix);
        // The Latest fast path does not apply a version lower bound, so route `after_version`
        // reads through the resolving view path (which honors the bound at selection).
        if matches!(request.bound(), ReadBound::Latest) && request.after_version().is_none() {
            let scan_timer = perf_trace::start_timer();
            let rows = view
                .scan_including_tombstones_visible(
                    &bounds,
                    BranchReadBound::at_version(CommitVersion::new(visible)),
                    request.limit().map(ReadLimit::get),
                )
                .map_err(branch_error)?;
            perf_trace::record_api_scan_runtime_elapsed(scan_timer);
            let map_timer = perf_trace::start_timer();
            let outcome = map_scan_rows(
                rows.iter().map(crate::branch::read::BranchHistoryRow::row),
                request.limit(),
                None,
            );
            perf_trace::record_api_scan_map_elapsed(map_timer);
            return outcome;
        }

        let resolved = resolve_read_bound(&view, request.bound())?;
        let capped = cap_bound_at_visible(resolved.branch_bound, visible);
        let rows = view
            .scan_prefix_including_tombstones(&bounds, capped, request.after_version())
            .map_err(branch_error)?;
        map_scan_rows(
            rows.iter().map(crate::branch::read::BranchHistoryRow::row),
            request.limit(),
            resolved.selected_timestamp,
        )
    }

    pub fn scan_range(&self, request: &ScanReadRequest) -> StorageApiResult<ScanReadOutcome> {
        let bounds_timer = perf_trace::start_timer();
        let storage_space = map_storage_space(request.storage_space())?;
        let bounds = BranchScanBounds::range(
            request.branch_id(),
            API_PHYSICAL_SPACE,
            storage_space,
            request
                .range()
                .start()
                .map_or(BranchUserKeyBound::Unbounded, |key| {
                    BranchUserKeyBound::included(key.as_bytes())
                }),
            request
                .range()
                .end()
                .map_or(BranchUserKeyBound::Unbounded, |key| {
                    BranchUserKeyBound::excluded(key.as_bytes())
                }),
        )
        .map_err(branch_error)?;
        perf_trace::record_api_scan_bounds_elapsed(bounds_timer);
        let (visible, view) = self.load_published_snapshot(request.branch_id())?;
        if matches!(request.bound(), ReadBound::Latest) {
            let scan_timer = perf_trace::start_timer();
            let rows = view
                .scan_including_tombstones_visible(
                    &bounds,
                    BranchReadBound::at_version(CommitVersion::new(visible)),
                    request.limit().map(ReadLimit::get),
                )
                .map_err(branch_error)?;
            perf_trace::record_api_scan_runtime_elapsed(scan_timer);
            let map_timer = perf_trace::start_timer();
            let outcome = map_scan_rows(
                rows.iter().map(crate::branch::read::BranchHistoryRow::row),
                request.limit(),
                None,
            );
            perf_trace::record_api_scan_map_elapsed(map_timer);
            return outcome;
        }

        let resolved = resolve_read_bound(&view, request.bound())?;
        let capped = cap_bound_at_visible(resolved.branch_bound, visible);
        let rows = view
            .scan_range_including_tombstones(&bounds, capped)
            .map_err(branch_error)?;
        map_scan_rows(
            rows.iter().map(crate::branch::read::BranchHistoryRow::row),
            request.limit(),
            resolved.selected_timestamp,
        )
    }

    pub fn scan_immutable_sources(
        &self,
        request: &ImmutableSourceScanReadRequest,
    ) -> StorageApiResult<ImmutableSourceScanReadOutcome> {
        let storage_space = map_storage_space(request.storage_space())?;
        let bounds = BranchScanBounds::range(
            request.branch_id(),
            API_PHYSICAL_SPACE,
            storage_space,
            request
                .range()
                .start()
                .map_or(BranchUserKeyBound::Unbounded, |key| {
                    BranchUserKeyBound::included(key.as_bytes())
                }),
            request
                .range()
                .end()
                .map_or(BranchUserKeyBound::Unbounded, |key| {
                    BranchUserKeyBound::excluded(key.as_bytes())
                }),
        )
        .map_err(branch_error)?;
        let view = self.read_view_for_branch(request.branch_id())?;
        let resolved = resolve_read_bound(&view, request.bound())?;
        let sources = view
            .scan_immutable_sources(&bounds, resolved.branch_bound)
            .map_err(branch_error)?;
        Ok(ImmutableSourceScanReadOutcome::new(map_immutable_sources(
            &sources,
            resolved.selected_timestamp,
        )?))
    }

    pub fn lookup_version_at_or_before_timestamp(
        &self,
        request: TimestampLookupRequest,
    ) -> StorageApiResult<TimestampLookupOutcome> {
        let timeline = self.timeline_view(request.branch_id())?;
        let lookup = timeline.version_at_or_before(request.timestamp());
        match lookup.miss() {
            CommitTimelineMiss::Matched | CommitTimelineMiss::AfterLatestRetained => {
                let matched_version = lookup.matched_version().ok_or(
                    StorageApiError::RetainedHistoryUnavailable {
                        branch_id: request.branch_id(),
                        reason: "timeline lookup did not return a retained version",
                    },
                )?;
                let matched_timestamp = lookup.matched_timestamp().ok_or(
                    StorageApiError::RetainedHistoryUnavailable {
                        branch_id: request.branch_id(),
                        reason: "timeline lookup did not return a retained timestamp",
                    },
                )?;
                Ok(TimestampLookupOutcome::new(
                    lookup.query_timestamp(),
                    matched_version,
                    matched_timestamp,
                    (lookup.miss() == CommitTimelineMiss::AfterLatestRetained)
                        .then_some(TimestampLookupMiss::AfterLatestRetained),
                ))
            }
            CommitTimelineMiss::BeforeRetainedHistory | CommitTimelineMiss::Empty => {
                Err(StorageApiError::TimestampHistoryUnavailable {
                    branch_id: request.branch_id(),
                    reason: "timestamp is outside retained timeline history",
                })
            }
        }
    }

    pub fn lookup_timestamp_for_version(
        &self,
        request: VersionLookupRequest,
    ) -> StorageApiResult<VersionLookupOutcome> {
        let timeline = self.timeline_view(request.branch_id())?;
        let timestamp = timeline.timestamp_for_version(request.version()).ok_or(
            StorageApiError::RetainedHistoryUnavailable {
                branch_id: request.branch_id(),
                reason: "commit version is outside retained timeline history",
            },
        )?;
        Ok(VersionLookupOutcome::new(request.version(), timestamp))
    }

    /// #3112 S3a: resolve a wall-clock instant to the commit boundary at or
    /// before it. The caller then reads at the returned LOGICAL timestamp, so
    /// `as_of_time` is by construction exactly an `as_of` at a resolved value
    /// — the two forms of time travel cannot diverge.
    pub fn resolve_wall_clock(
        &self,
        request: WallClockLookupRequest,
    ) -> StorageApiResult<WallClockLookupOutcome> {
        let view = self.read_view_for_branch(request.branch_id())?;
        timeline_resolve_wall_clock(&view, request.instant())
    }

    /// #3112 S4: the wall-clock instants recorded for a batch of commit
    /// versions, in the order asked. A version with no recorded instant — one
    /// predating `committed_at`, or outside retained history — reports `None`
    /// rather than failing the batch.
    pub fn commit_instants(
        &self,
        request: &CommitInstantsRequest,
    ) -> StorageApiResult<Vec<Option<Timestamp>>> {
        let view = self.read_view_for_branch(request.branch_id())?;
        Ok(timeline_committed_at_for_versions(
            &view,
            request.versions(),
        ))
    }

    pub fn timeline_bounds(
        &self,
        request: TimelineBoundsRequest,
    ) -> StorageApiResult<TimelineBoundsOutcome> {
        let bounds = self.timeline_view(request.branch_id())?.bounds();
        Ok(TimelineBoundsOutcome::new(
            bounds.min_timestamp(),
            bounds.max_timestamp(),
            bounds.min_version(),
            bounds.max_version(),
        ))
    }

    fn checkpoint_maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        let branch_id = self.branch_for_maintenance_scope(request.scope())?;
        match &mut self.inner {
            StorageRuntimeInner::Cache(_) => Ok(unsupported_maintenance_summary(
                request,
                "cache runtime does not support durable checkpoint maintenance",
            )),
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let outcome = runtime
                    .checkpoint_for_explicit_maintenance(branch_id, false)
                    .map_err(map_lifecycle_error)?;
                Ok(map_checkpoint_summary(request, &outcome))
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "checkpoint maintenance requires an open runtime",
            }),
        }
    }

    fn flush_maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        let branch_id = self.branch_for_maintenance_scope(request.scope())?;
        let flush_request = flush_request_for_boundary(branch_id)?;
        let outcome = match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                runtime
                    .rotate_active_for_flush(branch_id)
                    .map_err(map_lifecycle_error)?;
                runtime.flush_frozen(&flush_request)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                runtime
                    .rotate_active_for_flush(branch_id)
                    .map_err(map_lifecycle_error)?;
                runtime.flush_frozen(&flush_request)
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "flush maintenance requires an open runtime",
                });
            }
        }
        .map_err(map_lifecycle_error)?;
        self.run_flush_followup_compaction(branch_id)?;
        Ok(
            map_maintenance_summary(*request, &outcome.maintenance_outcome())
                .with_rows_processed(outcome.rows_flushed()),
        )
    }

    fn run_flush_followup_compaction(&mut self, branch_id: BranchId) -> StorageApiResult<()> {
        if !self.storage_pressure_suggests_compaction(branch_id)? {
            return Ok(());
        }
        let task = LifecycleMaintenanceTaskRequest::compaction(branch_id, 0);
        let outcome = match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                let enqueue = runtime
                    .enqueue_maintenance(task)
                    .map_err(map_lifecycle_error)?;
                runtime
                    .run_compaction_maintenance_task(enqueue.task_id())
                    .map_err(map_lifecycle_error)?
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let enqueue = runtime
                    .enqueue_maintenance(task)
                    .map_err(map_lifecycle_error)?;
                runtime
                    .run_compaction_maintenance_task(enqueue.task_id())
                    .map_err(map_lifecycle_error)?
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "flush follow-up compaction requires an open runtime",
                });
            }
        }
        .ok_or(StorageApiError::InvalidRuntimeState {
            reason: "flush follow-up compaction task was not runnable",
        })?;
        match outcome.status() {
            LifecycleMaintenanceOutcomeStatus::Completed
            | LifecycleMaintenanceOutcomeStatus::Deferred => Ok(()),
            LifecycleMaintenanceOutcomeStatus::Failed
            | LifecycleMaintenanceOutcomeStatus::Canceled => {
                Err(StorageApiError::InvalidRuntimeState {
                    reason: "flush follow-up compaction did not complete",
                })
            }
        }
    }

    fn storage_pressure_suggests_compaction(&self, branch_id: BranchId) -> StorageApiResult<bool> {
        let suggested_task = match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let runtime = slot.lock();
                runtime.storage_pressure().suggested_task()
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let runtime = slot.lock();
                runtime.storage_pressure().suggested_task()
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "flush follow-up compaction requires an open runtime",
                });
            }
        };
        Ok(suggested_task.is_some_and(|task| {
            task.kind() == LifecycleMaintenanceTaskKind::Compaction
                && matches!(
                    task.scope(),
                    LifecycleMaintenanceTaskScope::TableLevel {
                        branch_id: task_branch_id,
                        level: 0,
                    } if task_branch_id == branch_id
                )
        }))
    }

    fn compaction_maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        let branch_id = self.branch_for_maintenance_scope(request.scope())?;
        let compaction = LifecycleCompactionDrainRequest::new(
            branch_id,
            format!("storage-boundary-compaction-{branch_id}"),
        )
        .map_err(map_lifecycle_error)?;
        let outcome = match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                runtime.compact_branch_tables_to_fixed_point(&compaction)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                runtime.compact_branch_tables_to_fixed_point(&compaction)
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "compaction maintenance requires an open runtime",
                });
            }
        }
        .map_err(map_lifecycle_error)?;
        Ok(map_maintenance_summary(
            *request,
            &outcome.maintenance_outcome(),
        ))
    }

    #[cfg(test)]
    pub(crate) fn branch_source_layout_for_test(
        &self,
        branch_id: BranchId,
    ) -> StorageApiResult<crate::branch::facts::BranchSourceLayout> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let runtime = slot.lock();
                Ok(runtime
                    .branch_catalog()
                    .branch_state(branch_id)
                    .map_err(map_lifecycle_error)?
                    .source_layout())
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let runtime = slot.lock();
                Ok(runtime
                    .branch_catalog()
                    .branch_state(branch_id)
                    .map_err(map_lifecycle_error)?
                    .source_layout())
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "source layout requires an open runtime",
            }),
        }
    }

    fn materialization_maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        let branch_id = self.branch_for_maintenance_scope(request.scope())?;
        let task = LifecycleMaintenanceTaskRequest::materialization(branch_id);
        let outcome = match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                let enqueue = runtime
                    .enqueue_maintenance(task)
                    .map_err(map_lifecycle_error)?;
                runtime
                    .run_materialization_maintenance_task(enqueue.task_id())
                    .map_err(map_lifecycle_error)?
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let enqueue = runtime
                    .enqueue_maintenance(task)
                    .map_err(map_lifecycle_error)?;
                runtime
                    .run_materialization_maintenance_task(enqueue.task_id())
                    .map_err(map_lifecycle_error)?
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "materialization maintenance requires an open runtime",
                });
            }
        };
        outcome.map_or_else(
            || {
                Ok(unsupported_maintenance_summary(
                    request,
                    "materialization maintenance was deferred",
                ))
            },
            |outcome| Ok(map_maintenance_summary(*request, &outcome)),
        )
    }

    fn retention_maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        match &mut self.inner {
            StorageRuntimeInner::Cache(_) => Ok(unsupported_maintenance_summary(
                request,
                "cache runtime does not support durable retention maintenance",
            )),
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let outcome = runtime
                    .prove_retention(&LifecycleRetentionRequest::global(1))
                    .map_err(map_lifecycle_error)?;
                Ok(map_maintenance_summary(
                    *request,
                    &outcome.maintenance_outcome(),
                ))
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "retention maintenance requires an open runtime",
            }),
        }
    }

    fn snapshot_pruning_maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        match &mut self.inner {
            StorageRuntimeInner::Cache(_) => Ok(unsupported_maintenance_summary(
                request,
                "cache runtime does not support durable snapshot pruning maintenance",
            )),
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let retention = LifecycleRetentionRequest::snapshot_pruning(1);
                let outcome = runtime
                    .prune_snapshots(&retention)
                    .map_err(map_lifecycle_error)?;
                Ok(map_maintenance_summary(
                    *request,
                    &outcome.maintenance_outcome(),
                ))
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "snapshot pruning maintenance requires an open runtime",
            }),
        }
    }

    fn reclaim_maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        let branch_id = self.branch_for_maintenance_scope(request.scope())?;
        match &mut self.inner {
            StorageRuntimeInner::Cache(_) => Ok(unsupported_maintenance_summary(
                request,
                "cache runtime does not support durable reclaim maintenance",
            )),
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let retention = LifecycleRetentionRequest::new(
                    LifecycleRetentionScope::TableObjects { branch_id },
                    1,
                );
                let outcome = runtime
                    .prove_retention(&retention)
                    .map_err(map_lifecycle_error)?;
                Ok(map_maintenance_summary(
                    *request,
                    &outcome.maintenance_outcome(),
                ))
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "reclaim maintenance requires an open runtime",
            }),
        }
    }

    fn quarantine_maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        let task = map_maintenance_task_request(self, request)?;
        match &mut self.inner {
            StorageRuntimeInner::Cache(_) => Ok(unsupported_maintenance_summary(
                request,
                "cache runtime does not support durable quarantine maintenance",
            )),
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let enqueue = runtime
                    .enqueue_maintenance(task)
                    .map_err(map_lifecycle_error)?;
                let outcome = runtime
                    .run_quarantine_maintenance_task(enqueue.task_id())
                    .map_err(map_lifecycle_error)?;
                Ok(outcome.map_or_else(
                    || {
                        unsupported_maintenance_summary(
                            request,
                            "quarantine maintenance was deferred",
                        )
                    },
                    |outcome| map_maintenance_summary(*request, &outcome),
                ))
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "quarantine maintenance requires an open runtime",
            }),
        }
    }

    fn purge_maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        let task = map_maintenance_task_request(self, request)?;
        match &mut self.inner {
            StorageRuntimeInner::Cache(_) => Ok(unsupported_maintenance_summary(
                request,
                "cache runtime does not support durable purge maintenance",
            )),
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let enqueue = runtime
                    .enqueue_maintenance(task)
                    .map_err(map_lifecycle_error)?;
                let outcome = runtime
                    .run_purge_maintenance_task(enqueue.task_id())
                    .map_err(map_lifecycle_error)?;
                Ok(outcome.map_or_else(
                    || unsupported_maintenance_summary(request, "purge maintenance was deferred"),
                    |outcome| map_maintenance_summary(*request, &outcome),
                ))
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "purge maintenance requires an open runtime",
            }),
        }
    }

    fn repair_maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        let task = map_maintenance_task_request(self, request)?;
        match &mut self.inner {
            StorageRuntimeInner::Cache(_) => Ok(unsupported_maintenance_summary(
                request,
                "cache runtime does not support quarantine repair maintenance",
            )),
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let enqueue = runtime
                    .enqueue_maintenance(task)
                    .map_err(map_lifecycle_error)?;
                let outcome = runtime
                    .run_quarantine_repair_maintenance_task(enqueue.task_id())
                    .map_err(map_lifecycle_error)?;
                Ok(outcome.map_or_else(
                    || unsupported_maintenance_summary(request, "repair maintenance was deferred"),
                    |outcome| map_maintenance_summary(*request, &outcome),
                ))
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "repair maintenance requires an open runtime",
            }),
        }
    }

    fn wal_growth_maintenance(
        &mut self,
        request: &MaintenanceRequest,
    ) -> StorageApiResult<MaintenanceSummary> {
        let outcome = match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let runtime = slot.lock();
                Ok(runtime.evaluate_wal_growth_policy())
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                runtime.evaluate_wal_growth_policy()
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "WAL growth maintenance requires an open runtime",
                });
            }
        }
        .map_err(map_lifecycle_error)?;
        if matches!(
            outcome.status(),
            LifecycleWalGrowthStatus::MaintenanceEnqueued
                | LifecycleWalGrowthStatus::MaintenanceCoalesced
        ) {
            self.notify_background_drain_for_current_runtime(BackgroundTaskPriority::High);
        }
        Ok(map_wal_growth_maintenance_summary(*request, &outcome))
    }

    fn branch_for_maintenance_scope(&self, scope: MaintenanceScope) -> StorageApiResult<BranchId> {
        match scope {
            MaintenanceScope::Global => Ok(DEFAULT_BRANCH_ID),
            MaintenanceScope::Branch(branch_id) => {
                require_valid_branch_identifier(branch_id, "branch_id")?;
                self.describe_branch(branch_id).map(|_| branch_id)
            }
        }
    }

    fn create_branch(
        &self,
        branch_id: BranchId,
        generation: CommitBranchGeneration,
        created_at: Option<CommitVersion>,
    ) -> StorageApiResult<crate::lifecycle::LifecycleBranchCreateOutcome> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                runtime.create_branch(branch_id, generation, created_at)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                runtime.create_branch(branch_id, generation, created_at)
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "branch operation requires an open runtime",
                });
            }
        }
        .map_err(map_lifecycle_error)
    }

    fn create_branch_request(&self, request: &BranchRequest) -> StorageApiResult<BranchOutcome> {
        require_valid_branch_identifier(request.branch_id(), "branch_id")?;
        let generation_before = self.recreate_generation_before(request.branch_id())?;
        let generation = branch_generation_or_default(request.expected_generation())?;
        let created_at = current_visible(self);
        let outcome = self.create_branch(request.branch_id(), generation, created_at)?;
        let branch = map_branch_descriptor(outcome.descriptor());
        Ok(BranchOutcome::new(BranchOperation::Created, vec![branch])
            .with_generations(generation_before, Some(branch.generation())))
    }

    fn describe_branch_request(&self, request: &BranchRequest) -> StorageApiResult<BranchOutcome> {
        require_valid_branch_identifier(request.branch_id(), "branch_id")?;
        let branch = self.describe_branch(request.branch_id())?;
        Ok(BranchOutcome::new(BranchOperation::Described, vec![branch])
            .with_generations(Some(branch.generation()), Some(branch.generation())))
    }

    fn list_branch_request(&self) -> StorageApiResult<BranchOutcome> {
        let branches = self.list_branches(false)?;
        Ok(BranchOutcome::new(BranchOperation::Listed, branches))
    }

    fn list_branches(&self, include_deleted: bool) -> StorageApiResult<Vec<BranchSummary>> {
        let descriptors = match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let runtime = slot.lock();
                runtime.list_branches(include_deleted)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let runtime = slot.lock();
                runtime.list_branches(include_deleted)
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "branch operation requires an open runtime",
                });
            }
        };
        Ok(descriptors.into_iter().map(map_branch_descriptor).collect())
    }

    fn describe_branch(&self, branch_id: BranchId) -> StorageApiResult<BranchSummary> {
        let descriptor = match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let runtime = slot.lock();
                runtime.branch_catalog().lookup(branch_id)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let runtime = slot.lock();
                runtime.branch_catalog().lookup(branch_id)
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "branch operation requires an open runtime",
                });
            }
        }
        .map_err(map_lifecycle_error)?;
        Ok(map_branch_descriptor(descriptor))
    }

    fn active_branch_count(&self) -> StorageApiResult<usize> {
        Ok(self.list_branches(false)?.len())
    }

    fn retained_floor(&self, branch_id: BranchId) -> StorageApiResult<CommitVersion> {
        self.timeline_view(branch_id)?.bounds().min_version().ok_or(
            StorageApiError::RetainedHistoryUnavailable {
                branch_id,
                reason: "branch has no retained commit history",
            },
        )
    }

    /// #2852: the branch's current visible CONTENT watermark — the max commit
    /// version across every row source (active/frozen/owned/inherited), the
    /// same facts the lifecycle fork validates fork versions against. The
    /// retained TIMELINE is deliberately not consulted: after a lossy crash,
    /// flush-published content outlives timeline coverage (the
    /// version→timestamp facts shed with the WAL), and resolving "current"
    /// from the timeline silently forked an empty child over a populated
    /// source. Fork-current is a content operation; only the timestamp-based
    /// fork needs temporal coverage.
    fn current_branch_version(&self, branch_id: BranchId) -> StorageApiResult<CommitVersion> {
        self.read_view_for_branch(branch_id)?
            .facts()
            .max_commit_version()
            .ok_or(StorageApiError::RetainedHistoryUnavailable {
                branch_id,
                reason: "branch has no retained commit history",
            })
    }

    fn require_retained_version_watermark(
        &self,
        branch_id: BranchId,
        version: CommitVersion,
    ) -> StorageApiResult<()> {
        if version == CommitVersion::ZERO {
            return Err(StorageApiError::RetainedHistoryUnavailable {
                branch_id,
                reason: "commit version is outside retained branch history",
            });
        }
        let bounds = self.timeline_view(branch_id)?.bounds();
        let Some(min_version) = bounds.min_version() else {
            return Err(StorageApiError::RetainedHistoryUnavailable {
                branch_id,
                reason: "branch has no retained commit history",
            });
        };
        let Some(max_version) = bounds.max_version() else {
            return Err(StorageApiError::RetainedHistoryUnavailable {
                branch_id,
                reason: "branch has no retained commit history",
            });
        };
        if version < min_version || version > max_version {
            return Err(StorageApiError::RetainedHistoryUnavailable {
                branch_id,
                reason: "commit version is outside retained branch history",
            });
        }
        Ok(())
    }

    fn recreate_generation_before(
        &self,
        branch_id: BranchId,
    ) -> StorageApiResult<Option<BranchGeneration>> {
        match self.describe_branch(branch_id) {
            Ok(branch) if branch.status() == BranchStatus::Deleted => Ok(Some(branch.generation())),
            Ok(_) | Err(StorageApiError::BranchNotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn fork_branch_at_version(
        &self,
        request: &BranchRequest,
        source: BranchId,
        version: CommitVersion,
        timestamp: Option<Timestamp>,
    ) -> StorageApiResult<BranchOutcome> {
        let generation = branch_generation_or_default(request.expected_generation())?;
        // #2521: a history-less source (the legitimate empty-fork case) has
        // no retained floor; zero matches its zero fork version.
        // #2852: a source whose timeline degraded after a lossy crash (content
        // outlives timeline coverage) has no provable floor either — ZERO is
        // safe for every caller that reaches here: fork-current anchors at the
        // content watermark (≥ any true floor), and the at-version/at-timestamp
        // arms validate retained timeline coverage BEFORE this call, so a
        // degraded timeline never gets this far with a below-floor version.
        // The lifecycle still validates the version against content facts.
        let retained_floor = match self.retained_floor(source) {
            Ok(floor) => floor,
            Err(StorageApiError::RetainedHistoryUnavailable { .. }) => CommitVersion::ZERO,
            Err(error) => return Err(error),
        };
        // #2826: stamp the fork's `created_at` with the CURRENT visible
        // version, not the fork point (`parent.fork_version` carries that):
        // recovery's generation fence drops dead-predecessor WAL records at
        // `version <= created_at`, so the stamp must upper-bound every commit
        // a deleted prior generation of this branch id could have made.
        let created_at = current_visible(self);
        let outcome = match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                runtime.fork_at_retained_version(
                    source,
                    request.branch_id(),
                    generation,
                    version,
                    retained_floor,
                    created_at,
                )
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                runtime.fork_at_retained_version(
                    source,
                    request.branch_id(),
                    generation,
                    version,
                    retained_floor,
                    created_at,
                )
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "branch operation requires an open runtime",
                });
            }
        }
        .map_err(map_lifecycle_error)?;
        let branch = map_branch_descriptor(outcome.descriptor());
        Ok(BranchOutcome::new(BranchOperation::Forked, vec![branch])
            .with_generations(None, Some(branch.generation()))
            .with_fork_facts(
                outcome.source_branch_id(),
                outcome.fork_version(),
                timestamp,
            ))
    }

    fn clear_branch_request(&self, request: &BranchRequest) -> StorageApiResult<BranchOutcome> {
        require_valid_branch_identifier(request.branch_id(), "branch_id")?;
        let before = self.describe_branch(request.branch_id())?;
        let guard = map_generation_guard(request.expected_generation())?;
        let outcome = match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                runtime.clear_branch(request.branch_id(), guard)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                runtime.clear_branch(request.branch_id(), guard)
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "branch operation requires an open runtime",
                });
            }
        }
        .map_err(map_lifecycle_error)?;
        let branch = map_branch_descriptor(outcome.descriptor());
        Ok(BranchOutcome::new(BranchOperation::Cleared, vec![branch])
            .with_generations(Some(before.generation()), Some(branch.generation()))
            .with_cleanup(map_branch_cleanup(outcome.release_plan())))
    }

    fn delete_branch_request(&self, request: &BranchRequest) -> StorageApiResult<BranchOutcome> {
        require_valid_branch_identifier(request.branch_id(), "branch_id")?;
        let before = self.describe_branch(request.branch_id())?;
        if before.status() == BranchStatus::Active && self.active_branch_count()? <= 1 {
            return Err(StorageApiError::InvalidRuntimeState {
                reason: "delete would remove the last active branch",
            });
        }
        let guard = map_generation_guard(request.expected_generation())?;
        let deleted_at = current_visible(self);
        let outcome = match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                runtime.delete_branch(request.branch_id(), guard, deleted_at)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                runtime.delete_branch(request.branch_id(), guard, deleted_at)
            }
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "branch operation requires an open runtime",
                });
            }
        }
        .map_err(map_lifecycle_error)?;
        let branch = map_branch_descriptor(outcome.descriptor());
        Ok(BranchOutcome::new(BranchOperation::Deleted, vec![branch])
            .with_generations(Some(before.generation()), Some(branch.generation()))
            .with_cleanup(map_branch_cleanup(outcome.release_plan())))
    }

    fn open_cache_with_backend(
        options: StorageOpenOptions,
        backend: &StorageBackend,
    ) -> StorageApiResult<StorageOpenOutcome<'a>> {
        let executor_mode = background_executor_mode(options.maintenance_scheduling_policy());
        let background_config = options.background_maintenance();
        let (plan, budget_source) = lifecycle_plan(options)?;
        let request =
            LifecycleCacheOpenRequest::new(plan, DEFAULT_BRANCH_ID, default_branch_generation()?)
                .map_err(map_lifecycle_error)?;
        let runtime = LifecycleCacheRuntime::open(
            request,
            backend.as_backend(),
            BranchRuntimeConfig::default(),
            CommitRuntimeConfig::default(),
            default_timestamp_source(),
        )
        .map_err(map_lifecycle_error)?;
        let summary = map_open_summary(
            runtime.open_outcome(),
            options.mode(),
            options,
            budget_source,
        );
        let recovery = map_diagnostics_recovery(runtime.open_outcome().recovery_health());
        let config = runtime.open_plan().lifecycle_config();
        let mode_policy = runtime.open_plan().lifecycle_policy();
        Ok(StorageOpenOutcome::new(
            Self {
                inner: StorageRuntimeInner::Cache(Box::new(
                    RuntimeSlot::new_with_background_arc_drain(
                        runtime,
                        config,
                        background_config,
                        executor_mode,
                        mode_policy,
                        drain_cache_background_round,
                    ),
                )),
                open_summary: Some(summary),
                last_recovery: Some(recovery),
                last_close: None,
                last_allocated_timestamp_micros: AtomicU64::new(0),
                _marker: PhantomData,
            },
            summary,
        ))
    }

    #[cfg(all(test, feature = "localfs"))]
    pub(crate) fn release_writer_guard_for_test(&mut self) -> bool {
        match &mut self.inner {
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                runtime.release_writer_guard_for_test()
            }
            StorageRuntimeInner::Cache(_) | StorageRuntimeInner::Closed => false,
        }
    }

    fn read_view_for_branch(&self, branch_id: BranchId) -> StorageApiResult<BranchReadView> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let runtime = slot.lock();
                runtime
                    .read_view_for_branch(branch_id)
                    .map_err(map_lifecycle_error)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let runtime = slot.lock();
                runtime
                    .read_view_for_branch(branch_id)
                    .map_err(map_lifecycle_error)
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "read requires an open runtime",
            }),
        }
    }

    fn timeline_view(&self, branch_id: BranchId) -> StorageApiResult<CommitTimelineView> {
        let view = self.read_view_for_branch(branch_id)?;
        // W3.1c: index-first — commits no longer write timeline rows; the
        // scan inside the helper covers testkit views and legacy rows only.
        timeline_view_or_index(&view)
    }

    fn diagnostics_timeline(&self, branch_id: BranchId) -> DiagnosticsTimelineReport {
        match self.timeline_view(branch_id) {
            Ok(timeline) => {
                let bounds = timeline.bounds();
                DiagnosticsTimelineReport::known(
                    bounds.min_version(),
                    bounds.max_version(),
                    bounds.min_timestamp(),
                    bounds.max_timestamp(),
                )
            }
            Err(_) => DiagnosticsTimelineReport::unknown(),
        }
    }

    #[cfg(test)]
    pub(crate) const fn default_branch_id_for_test() -> BranchId {
        DEFAULT_BRANCH_ID
    }

    #[cfg(test)]
    pub(crate) fn maintenance_scheduling_policy_for_test(
        &self,
    ) -> LifecycleMaintenanceSchedulingPolicy {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let runtime = slot.lock();
                runtime
                    .open_plan()
                    .lifecycle_config()
                    .maintenance_scheduling_policy()
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let runtime = slot.lock();
                runtime
                    .open_plan()
                    .lifecycle_config()
                    .maintenance_scheduling_policy()
            }
            StorageRuntimeInner::Closed => LifecycleMaintenanceSchedulingPolicy::Disabled,
        }
    }

    #[cfg(any(test, feature = "testkit"))]
    pub(crate) fn commit_for_test(
        &self,
        batch: &CommitBatch,
        timestamp: Timestamp,
    ) -> StorageApiResult<CommitSummary> {
        self.commit_at(batch, timestamp)
    }

    #[cfg(test)]
    pub(crate) fn diagnostics_recovery_report_for_test(
        health: &RecoveryHealth,
    ) -> DiagnosticsRecoveryReport {
        map_diagnostics_recovery(health)
    }

    #[cfg(test)]
    #[cfg_attr(
        not(feature = "localfs"),
        expect(
            dead_code,
            reason = "durable recovery health hook is exercised by localfs diagnostics tests"
        )
    )]
    pub(crate) fn record_recovery_health_for_test(
        &mut self,
        health: &RecoveryHealth,
    ) -> StorageApiResult<()> {
        match &mut self.inner {
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                runtime.record_recovery_health_for_test(health);
                self.last_recovery = Some(map_diagnostics_recovery(health));
                Ok(())
            }
            StorageRuntimeInner::Cache(_) | StorageRuntimeInner::Closed => {
                Err(StorageApiError::InvalidRuntimeState {
                    reason: "durable recovery health test hook requires an open durable runtime",
                })
            }
        }
    }

    fn execute_commit(
        &self,
        batch: &CommitBatch,
        explicit_timestamp: Option<Timestamp>,
    ) -> StorageApiResult<CommitSummary> {
        let timestamp_base = explicit_timestamp.unwrap_or_else(|| self.next_commit_timestamp());
        // The API computes the timestamp before mapping TTL so lower commit
        // stamping and expiry facts use the same monotonic frontier. An internally
        // generated base uses the CLAMPING policy: with concurrent writers, another
        // commit can advance the monotonic floor between this pre-lock read and the
        // allocator — ordinary interleaving that must not reject the commit. Only a
        // caller-supplied timestamp takes the strict Explicit path.
        let timestamp_policy = match explicit_timestamp {
            Some(timestamp) => crate::commit::CommitTimestampPolicy::Explicit(timestamp),
            None => crate::commit::CommitTimestampPolicy::RuntimeGeneratedBase(timestamp_base),
        };
        let durability = self.resolve_commit_durability(batch.options().durability())?;
        let generation_guard = map_generation_guard(batch.options().expected_generation())?;
        let map_timer = perf_trace::start_timer();
        let runtime_batch_result =
            map_api_commit_batch(batch, timestamp_base, timestamp_policy, durability);
        perf_trace::record_api_commit_map_elapsed(map_timer);
        let runtime_batch = runtime_batch_result?;

        let runtime_timer = perf_trace::start_timer();
        let mut pressure_wait_deadline = None;
        loop {
            let (outcome_result, admission, pending_tasks, wal_growth, throttle_delay_millis) =
                match &self.inner {
                    StorageRuntimeInner::Cache(slot) => {
                        let mut runtime = slot.lock_for_commit();
                        let result =
                            runtime.execute_cache_commit(runtime_batch.clone(), generation_guard);
                        (
                            result,
                            runtime.last_write_admission(),
                            runtime.maintenance_status().pending_tasks(),
                            None,
                            // Cache mode neutralizes throttle pressure to 0; never throttles.
                            0,
                        )
                    }
                    StorageRuntimeInner::DurableOwned(slot) => {
                        let clone_timer = perf_trace::start_timer();
                        let exec_batch = runtime_batch.clone();
                        perf_trace::record_commit_api_batch_clone_elapsed(clone_timer);
                        // BS5.1 write groups: uncontended callers take the exact
                        // solo path; contended callers join a group led by
                        // whichever caller holds the runtime lock.
                        let dispatch_timer = perf_trace::start_timer();
                        let response = execute_durable_commit_grouped(
                            slot,
                            exec_batch,
                            &runtime_batch,
                            generation_guard,
                        );
                        perf_trace::record_commit_group_dispatch_elapsed(dispatch_timer);
                        (
                            response.outcome,
                            response.admission,
                            response.pending_tasks,
                            response.wal_growth,
                            response.throttle_delay_millis,
                        )
                    }
                    StorageRuntimeInner::Closed => {
                        return Err(StorageApiError::InvalidRuntimeState {
                            reason: "commit requires an open runtime",
                        });
                    }
                };
            if pending_tasks > 0 {
                let notify_timer = perf_trace::start_timer();
                self.notify_background_drain_for_current_runtime(BackgroundTaskPriority::High);
                perf_trace::record_commit_drain_notify_elapsed(notify_timer);
            }
            match outcome_result {
                Ok(outcome) => {
                    // Keep the off-lock timestamp mirror at the frontier
                    // (monotone via fetch_max under concurrent committers).
                    if let Some(timestamp) = outcome.commit_timestamp() {
                        self.last_allocated_timestamp_micros
                            .fetch_max(timestamp.as_micros(), Ordering::AcqRel);
                    }
                    // W3.2: the runtime timer measures commit-path work only.
                    // The post-completion policy sleeps below (WAL-growth
                    // backpressure, graded write throttle) are counted by their
                    // own probes; folding them in here misattributed pacing as
                    // path cost twice during W3 attribution.
                    perf_trace::record_api_commit_runtime_elapsed(runtime_timer);
                    self.background_wait_after_wal_growth_enqueue(wal_growth.as_ref());
                    self.background_wait_after_write_throttle(throttle_delay_millis);
                    return map_commit_summary(&outcome, admission);
                }
                Err(error)
                    if self.background_wait_after_pressure_rejection(
                        &error,
                        &mut pressure_wait_deadline,
                    ) => {}
                Err(error) => {
                    perf_trace::record_api_commit_runtime_elapsed(runtime_timer);
                    return Err(map_lifecycle_error(error));
                }
            }
        }
    }

    fn notify_background_drain_for_current_runtime(&self, priority: BackgroundTaskPriority) {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                slot.notify_background_drain(priority);
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                slot.notify_background_drain(priority);
            }
            StorageRuntimeInner::Closed => {}
        }
    }

    fn has_background_runtime(&self) -> bool {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot.has_background(),
            StorageRuntimeInner::DurableOwned(slot) => slot.has_background(),
            StorageRuntimeInner::Closed => false,
        }
    }

    fn background_stats_for_current_runtime(&self) -> Option<MaintenanceExecutorStats> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot.background_stats(),
            StorageRuntimeInner::DurableOwned(slot) => slot.background_stats(),
            StorageRuntimeInner::Closed => None,
        }
    }

    #[cfg(test)]
    fn background_block_wait_for_current_runtime(&self) -> BackgroundBlockWaitConfig {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot.background_block_wait,
            StorageRuntimeInner::DurableOwned(slot) => slot.background_block_wait,
            StorageRuntimeInner::Closed => BackgroundBlockWaitConfig::default(),
        }
    }

    fn background_pressure_snapshot_for_branch(
        &self,
        branch_id: BranchId,
    ) -> Option<BackgroundPressureSnapshot> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => Some(BackgroundPressureSnapshot::from_pressure(
                slot.lock().storage_pressure_for_branch(branch_id),
            )),
            StorageRuntimeInner::DurableOwned(slot) => {
                Some(BackgroundPressureSnapshot::from_pressure(
                    slot.lock().storage_pressure_for_branch(branch_id),
                ))
            }
            StorageRuntimeInner::Closed => None,
        }
    }

    fn background_lifecycle_work_for_current_runtime(&self) -> Option<(usize, bool)> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let status = slot.lock().maintenance_status();
                Some((status.pending_tasks(), status.active_tasks() > 0))
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let status = slot.lock().maintenance_status();
                Some((status.pending_tasks(), status.active_tasks() > 0))
            }
            StorageRuntimeInner::Closed => None,
        }
    }

    fn background_lifecycle_completed_for_current_runtime(&self) -> u64 {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                u64::try_from(slot.lock().maintenance_status().stats().completed())
                    .unwrap_or(u64::MAX)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                u64::try_from(slot.lock().maintenance_status().stats().completed())
                    .unwrap_or(u64::MAX)
            }
            StorageRuntimeInner::Closed => 0,
        }
    }

    fn background_now_for_current_runtime(&self) -> Option<MaintenanceInstant> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot.background_now(),
            StorageRuntimeInner::DurableOwned(slot) => slot.background_now(),
            StorageRuntimeInner::Closed => None,
        }
    }

    /// Advance the injected manual maintenance clock (deterministic-inline runtimes
    /// only); returns whether a manual clock was reached. Test / `fault-injection`-only
    /// — the seam the simulation driver uses to drive time deterministically. Lives in
    /// the lifetime-generic impl so it is callable on a borrowed-backend runtime.
    #[cfg(any(test, feature = "fault-injection"))]
    pub(crate) fn advance_maintenance_clock_for_test(&self, by: std::time::Duration) -> bool {
        self.advance_maintenance_clock_for_current_runtime(by)
    }

    #[cfg(any(test, feature = "fault-injection"))]
    fn advance_maintenance_clock_for_current_runtime(&self, by: std::time::Duration) -> bool {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot.advance_maintenance_clock(by),
            StorageRuntimeInner::DurableOwned(slot) => slot.advance_maintenance_clock(by),
            StorageRuntimeInner::Closed => false,
        }
    }

    fn background_wait_after_pressure_rejection(
        &self,
        error: &LifecycleError,
        deadline: &mut Option<MaintenanceInstant>,
    ) -> bool {
        let LifecycleError::StoragePressureRejected {
            branch_id,
            pressure_reason,
            retryable: true,
            ..
        } = error
        else {
            return false;
        };
        if !self.has_background_runtime() {
            return false;
        }
        #[cfg(test)]
        let block_wait = self.background_block_wait_for_current_runtime();
        #[cfg(not(test))]
        let block_wait = BackgroundBlockWaitConfig::default();
        let Some(now) = self.background_now_for_current_runtime() else {
            return false;
        };
        perf_trace::record_lifecycle_write_admission_wait_attempt();
        let stall_deadline =
            *deadline.get_or_insert_with(|| now.saturating_add(block_wait.stall_deadline));
        if now >= stall_deadline {
            perf_trace::record_lifecycle_write_admission_wait_timeout();
            return false;
        }
        let wait_deadline = now
            .saturating_add(block_wait.wait_slice)
            .min(stall_deadline);
        // Ensure the maintenance this pressure needs is enqueued before we wait
        // on it (forced flush for FrozenBacklog, forced compaction for
        // LevelZeroTableBacklog); the writer is then paced on its progress
        // rather than rejected for lack of immediately-visible work.
        self.enqueue_pressure_maintenance_for_background_wait(*branch_id, *pressure_reason);
        let Some(stats_before_wait) = self.background_stats_for_current_runtime() else {
            return false;
        };
        let pressure_before_wait = self.background_pressure_snapshot_for_branch(*branch_id);
        let completed_before_wait = stats_before_wait.tasks_completed;
        let lifecycle_completed_before = self.background_lifecycle_completed_for_current_runtime();
        let wait_start = self.background_now_for_current_runtime().unwrap_or(now);
        self.notify_background_drain_for_current_runtime(BackgroundTaskPriority::High);
        // Drive the background drain for one bounded slice (and advance the
        // manual clock under deterministic simulation). The executor-level
        // wake counts every drain STEP — including tasks that immediately
        // DEFER under saturation interlocks — so a single wait call can
        // return microseconds in. Treating that as the slice let a stalled
        // writer cycle enqueue→(start-and-defer)→wake→re-enqueue at ~16µs:
        // measured 5.4M generated-and-deferred tasks in one 34s window, with
        // the drain churn holding the runtime lock the real flush/compaction
        // needed to relieve the very pressure being waited on. Exhaust the
        // full slice unless REAL progress (a lifecycle maintenance
        // completion) lands; spurious step-wakes just re-arm the wait with a
        // fresh baseline.
        let mut executor_completed_baseline = completed_before_wait;
        loop {
            let drove_drain =
                match &self.inner {
                    StorageRuntimeInner::Cache(slot) => slot
                        .wait_background_progress_until(executor_completed_baseline, wait_deadline),
                    StorageRuntimeInner::DurableOwned(slot) => slot
                        .wait_background_progress_until(executor_completed_baseline, wait_deadline),
                    StorageRuntimeInner::Closed => return false,
                };
            if !drove_drain
                || self.background_lifecycle_completed_for_current_runtime()
                    > lifecycle_completed_before
            {
                break;
            }
            let Some(slice_now) = self.background_now_for_current_runtime() else {
                break;
            };
            if slice_now >= wait_deadline {
                break;
            }
            let Some(stats) = self.background_stats_for_current_runtime() else {
                break;
            };
            executor_completed_baseline = stats.tasks_completed;
        }
        let wait_elapsed = self
            .background_now_for_current_runtime()
            .unwrap_or(wait_start)
            .saturating_duration_since(wait_start);
        perf_trace::record_lifecycle_write_admission_block_wait(wait_elapsed);
        let pressure_after_wait = self.background_pressure_snapshot_for_branch(*branch_id);
        let backlog_reduced = pressure_before_wait
            .zip(pressure_after_wait)
            .is_some_and(|(before, after)| after.relieved_since(before, *pressure_reason));
        let maintenance_completed_task =
            self.background_lifecycle_completed_for_current_runtime() > lifecycle_completed_before;
        // A running maintenance task counts as liveness even before it
        // completes: at 10M scale one L0→L1 compaction pass rewrites GBs of
        // overlap and can exceed a full watchdog window with zero COMPLETIONS
        // — the watchdog then converted a legitimately-busy executor into a
        // caller-visible failed_precondition abort mid-load (observed on the
        // 10M three-way rerun; the same run passed other attempts, because
        // firing required one >30s window where that giant pass was the only
        // live task). The watchdog's purpose is a DEAD executor, not a busy
        // one; a wedged build thread is the same failure class as a hung
        // fsync — outside this backstop's scope.
        let maintenance_running = self
            .background_stats_for_current_runtime()
            .is_some_and(|stats| stats.active_tasks > 0);
        if backlog_reduced || maintenance_completed_task || maintenance_running {
            // The executor is alive and making real maintenance progress (a
            // completion, backlog reduction, or an in-flight task this slice).
            // Reset the stall watchdog so a sustained overload that maintenance
            // can service keeps pacing the writer instead of timing out on an
            // absolute clock. The top-of-function `now >= stall_deadline` check
            // then fires only after a full window with zero completions, no
            // backlog reduction, and NO running task — a provably dead
            // executor (the bounded liveness backstop).
            *deadline = None;
            perf_trace::record_lifecycle_write_admission_wait_progress_reset();
        }
        // Keep pacing the writer in wait-slices; the sole give-up is the
        // top-of-function watchdog. Backlog that maintenance is still working
        // through is throttled, never converted into a rejection.
        true
    }

    fn background_wait_after_wal_growth_enqueue(
        &self,
        wal_growth: Option<&LifecycleWalGrowthOutcome>,
    ) {
        let Some(outcome) = wal_growth else {
            return;
        };
        // BS5.3c: enter the wait loop only when the commit's OWN growth
        // evaluation (computed under the lock it just held, carried in the
        // response) signaled pressure. Re-probing current facts here cost two
        // EXTRA runtime-lock acquisitions per commit — on every commit, for a
        // condition that is almost always below threshold. A cap/threshold
        // crossing between this commit and the next is caught by the next
        // commit's own evaluation (the loop's documented re-check semantics).
        if !wal_growth_pacing_applies(outcome.status()) {
            return;
        }
        if !self.has_background_runtime() {
            return;
        }
        let Some(now) = self.background_now_for_current_runtime() else {
            return;
        };
        #[cfg(test)]
        let block_wait = self.background_block_wait_for_current_runtime();
        #[cfg(not(test))]
        let block_wait = BackgroundBlockWaitConfig::default();
        let stall_deadline = now.saturating_add(block_wait.stall_deadline);
        let mut no_relief_rounds = 0usize;
        while self.current_wal_growth_exceeds_backpressure()
            || self.current_wal_growth_exceeds_hard_cap()
        {
            let Some(now) = self.background_now_for_current_runtime() else {
                return;
            };
            if now >= stall_deadline {
                return;
            }
            let snapshot_before_wait = self.background_wal_growth_snapshot_for_current_runtime();
            let wait_deadline = now
                .saturating_add(block_wait.wait_slice)
                .min(stall_deadline);
            self.evaluate_wal_growth_policy_for_background_wait();
            self.notify_background_drain_for_current_runtime(BackgroundTaskPriority::High);
            let Some(stats) = self.background_stats_for_current_runtime() else {
                return;
            };
            let (lifecycle_pending_tasks, lifecycle_active_task) = self
                .background_lifecycle_work_for_current_runtime()
                .unwrap_or((0, false));
            if stats
                .queue_depth
                .saturating_add(stats.active_tasks)
                .saturating_add(lifecycle_pending_tasks)
                .saturating_add(usize::from(lifecycle_active_task))
                == 0
            {
                return;
            }
            let completed_before_wait = stats.tasks_completed;
            let progressed = match &self.inner {
                StorageRuntimeInner::Cache(slot) => {
                    slot.wait_background_progress_until(completed_before_wait, wait_deadline)
                }
                StorageRuntimeInner::DurableOwned(slot) => {
                    slot.wait_background_progress_until(completed_before_wait, wait_deadline)
                }
                StorageRuntimeInner::Closed => return,
            };
            let snapshot_after_wait = self.background_wal_growth_snapshot_for_current_runtime();
            if snapshot_before_wait
                .zip(snapshot_after_wait)
                .is_some_and(|(before, after)| after.relieved_since(before))
            {
                no_relief_rounds = 0;
                continue;
            }
            let (lifecycle_pending_tasks, lifecycle_active_task) = self
                .background_lifecycle_work_for_current_runtime()
                .unwrap_or((0, false));
            if lifecycle_active_task {
                no_relief_rounds = 0;
                continue;
            }
            if progressed && lifecycle_pending_tasks > 0 {
                no_relief_rounds = no_relief_rounds.saturating_add(1);
                if no_relief_rounds < block_wait.no_relief_rounds {
                    continue;
                }
            }
            if !progressed || no_relief_rounds >= block_wait.no_relief_rounds {
                // Disk-safety guard: above the hard WAL cap the soft give-up does not apply —
                // the WAL must not keep growing past the ceiling, so keep waiting on background
                // reclaim (flush → flush-watermark → truncation, re-enqueued each iteration by
                // `evaluate_wal_growth_policy_for_background_wait`) until relief or the stall
                // deadline. Below the cap this stays the soft give-up that lets the writer make
                // progress when maintenance is merely slow.
                if self.current_wal_growth_exceeds_hard_cap() {
                    no_relief_rounds = 0;
                    continue;
                }
                return;
            }
        }
    }

    /// Proportional pre-budget write throttle (fix #2): pace the writer by `delay_millis`
    /// (computed in `execute_commit` from pool fullness while the slot lock was held) so a
    /// sustained load settles at the flush-limited rate before the hard memory budget is hit.
    /// Like the WAL-growth wait, this runs AFTER the commit released the slot lock and paces via
    /// `wait_background_progress_until` (deterministic under the manual clock). Unlike the relief
    /// waits it sleeps the FULL delay (see the `u64::MAX` baseline below) — a deliberate pace, not
    /// a wait-for-relief. No runtime lock is held across the wait, so the background flusher can
    /// take it and drain — the throttle softens, never stalls.
    fn background_wait_after_write_throttle(&self, delay_millis: u64) {
        if delay_millis == 0 || !self.has_background_runtime() {
            return;
        }
        let Some(now) = self.background_now_for_current_runtime() else {
            return;
        };
        perf_trace::record_lifecycle_graded_throttle_delay(delay_millis);
        self.notify_background_drain_for_current_runtime(BackgroundTaskPriority::High);
        // Sleep the FULL computed delay: this is a deliberate pace, not a wait-for-relief, so it
        // must not wake early on background task completions (which fire constantly under load and
        // would shrink the pace to ~0). `u64::MAX` as the progress baseline makes the
        // "tasks_completed > baseline" wake condition unreachable, so it waits the whole deadline.
        let completed_before = u64::MAX;
        let deadline = now.saturating_add(Duration::from_millis(delay_millis));
        let _drove_drain = match &self.inner {
            StorageRuntimeInner::DurableOwned(slot) => {
                slot.wait_background_progress_until(completed_before, deadline)
            }
            StorageRuntimeInner::Cache(_) | StorageRuntimeInner::Closed => false,
        };
    }

    fn evaluate_wal_growth_policy_for_background_wait(&self) {
        match &self.inner {
            StorageRuntimeInner::Cache(_) | StorageRuntimeInner::Closed => {}
            StorageRuntimeInner::DurableOwned(slot) => {
                slot.lock().evaluate_and_record_wal_growth_policy();
            }
        }
    }

    fn current_wal_growth_exceeds_backpressure(&self) -> bool {
        self.background_wal_growth_snapshot_for_current_runtime()
            .is_some_and(|snapshot| snapshot.exceeds_backpressure)
    }

    fn current_wal_growth_exceeds_hard_cap(&self) -> bool {
        // A transient facts-read error is treated as "not over the cap" (mirrors the
        // backpressure check): we never block the writer on a read failure — the next
        // commit's pacing re-checks.
        match &self.inner {
            StorageRuntimeInner::Cache(_) | StorageRuntimeInner::Closed => false,
            StorageRuntimeInner::DurableOwned(slot) => slot
                .lock()
                .current_wal_growth_exceeds_hard_cap()
                .unwrap_or(false),
        }
    }

    fn background_wal_growth_snapshot_for_current_runtime(
        &self,
    ) -> Option<BackgroundWalGrowthSnapshot> {
        match &self.inner {
            StorageRuntimeInner::Cache(_) | StorageRuntimeInner::Closed => None,
            StorageRuntimeInner::DurableOwned(slot) => slot
                .lock()
                .current_wal_growth_backpressure_snapshot()
                .ok()
                .map(|(facts, commits_since_checkpoint, trigger)| {
                    BackgroundWalGrowthSnapshot::from_parts(
                        facts,
                        commits_since_checkpoint,
                        trigger,
                    )
                }),
        }
    }

    fn enqueue_pressure_maintenance_for_background_wait(
        &self,
        branch_id: BranchId,
        pressure_reason: LifecycleStoragePressureReason,
    ) -> usize {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                let _ = runtime.schedule_post_commit_maintenance_for_branch(branch_id);
                if pressure_reason == LifecycleStoragePressureReason::FrozenBacklog
                    && runtime.maintenance_status().pending_tasks() == 0
                {
                    let _ = runtime
                        .enqueue_maintenance(LifecycleMaintenanceTaskRequest::flush(branch_id));
                }
                runtime.maintenance_status().pending_tasks()
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let _ = runtime.schedule_post_commit_maintenance_for_branch(branch_id);
                if pressure_reason == LifecycleStoragePressureReason::FrozenBacklog
                    && runtime.maintenance_status().pending_tasks() == 0
                {
                    let _ = runtime
                        .enqueue_maintenance(LifecycleMaintenanceTaskRequest::flush(branch_id));
                }
                // Symmetric to the forced flush above: an L0 backlog that blocks
                // admission must have its L0->L1 compaction enqueued before the
                // wait path can give up, so the writer is paced on real
                // maintenance progress rather than rejected for lack of a task.
                if pressure_reason == LifecycleStoragePressureReason::LevelZeroTableBacklog
                    && runtime.maintenance_status().pending_tasks() == 0
                {
                    let _ = runtime.enqueue_maintenance(
                        LifecycleMaintenanceTaskRequest::compaction(branch_id, 0),
                    );
                }
                runtime.maintenance_status().pending_tasks()
            }
            StorageRuntimeInner::Closed => 0,
        }
    }

    fn resolve_commit_durability(
        &self,
        requested: CommitDurability,
    ) -> StorageApiResult<crate::commit::CommitDurabilityMode> {
        match &self.inner {
            StorageRuntimeInner::Cache(_) => match requested {
                CommitDurability::RuntimeDefault | CommitDurability::NotDurable => {
                    Ok(crate::commit::CommitDurabilityMode::Cache)
                }
                CommitDurability::Standard | CommitDurability::Always => {
                    Err(StorageApiError::UnsupportedCapability {
                        capability: "commit_durability",
                        reason: "cache runtime cannot satisfy durable commit requests",
                    })
                }
            },
            StorageRuntimeInner::DurableOwned(slot) => {
                // BS5.1: the storage mode is fixed at open. Reading it through
                // the runtime lock serialized every writer ahead of the commit
                // path (the write-group join queue then always looked empty),
                // so read the recorded open-summary mode off-lock; the locked
                // read remains only as the fallback for a missing summary.
                let storage_mode = match self.open_summary.map(StorageOpenSummary::mode) {
                    Some(StorageMode::DurableLocal {
                        policy: StorageDurabilityPolicy::Standard,
                    }) => LifecycleStorageMode::DurableLocalStandard,
                    Some(StorageMode::DurableLocal {
                        policy: StorageDurabilityPolicy::Always,
                    }) => LifecycleStorageMode::DurableLocalAlways,
                    Some(_) | None => {
                        let runtime = slot.lock();
                        runtime.open_plan().storage_mode()
                    }
                };
                match (storage_mode, requested) {
                    (
                        LifecycleStorageMode::DurableLocalStandard,
                        CommitDurability::RuntimeDefault | CommitDurability::Standard,
                    ) => Ok(crate::commit::CommitDurabilityMode::Standard),
                    (
                        LifecycleStorageMode::DurableLocalAlways,
                        CommitDurability::RuntimeDefault | CommitDurability::Always,
                    ) => Ok(crate::commit::CommitDurabilityMode::Always),
                    (_, CommitDurability::NotDurable) => {
                        Err(StorageApiError::UnsupportedCapability {
                            capability: "commit_durability",
                            reason: "durable runtime cannot accept cache-only commit requests",
                        })
                    }
                    (LifecycleStorageMode::DurableLocalStandard, CommitDurability::Always) => {
                        Err(StorageApiError::UnsupportedCapability {
                            capability: "commit_durability",
                            reason: "always commit durability requires an always-durable runtime",
                        })
                    }
                    (LifecycleStorageMode::DurableLocalAlways, CommitDurability::Standard) => {
                        Err(StorageApiError::UnsupportedCapability {
                            capability: "commit_durability",
                            reason:
                                "standard commit durability cannot weaken an always-durable runtime",
                        })
                    }
                    _ => Err(StorageApiError::UnsupportedCapability {
                        capability: "commit_durability",
                        reason: "commit durability is unsupported for this runtime mode",
                    }),
                }
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "commit requires an open runtime",
            }),
        }
    }

    fn next_commit_timestamp(&self) -> Timestamp {
        // BS5.1: read the frontier off-lock (see the field's doc). The mirror
        // is exact in steady state (updated from every commit outcome); the
        // one-time locked sample below seeds it with the recovered allocator
        // state after open.
        let cached = self.last_allocated_timestamp_micros.load(Ordering::Acquire);
        let last_allocated = if cached == 0 {
            let sampled = match &self.inner {
                StorageRuntimeInner::Cache(slot) => {
                    let runtime = slot.lock();
                    runtime.allocator().timestamp_guard().last_allocated()
                }
                StorageRuntimeInner::DurableOwned(slot) => {
                    let runtime = slot.lock();
                    runtime.allocator().timestamp_guard().last_allocated()
                }
                StorageRuntimeInner::Closed => None,
            };
            // `1` marks "sampled, nothing allocated yet" — still below the
            // default timestamp, so the base resolution is unchanged.
            self.last_allocated_timestamp_micros.fetch_max(
                sampled.map_or(1, |timestamp| timestamp.as_micros().max(1)),
                Ordering::AcqRel,
            );
            sampled
        } else {
            Some(Timestamp::from_micros(cached))
        };
        match last_allocated {
            Some(timestamp) if timestamp >= DEFAULT_TIMESTAMP => {
                timestamp.saturating_add(Duration::from_micros(1))
            }
            Some(_) | None => DEFAULT_TIMESTAMP,
        }
    }

    /// W3.1b oracle: whether the branch's retained-timeline index claims
    /// complete coverage (a checkpoint-seeded index must arrive complete
    /// BEFORE any read scan-seeds it).
    #[cfg(test)]
    pub(crate) fn retained_timeline_complete_for_test(
        &self,
        branch_id: BranchId,
    ) -> StorageApiResult<bool> {
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => Ok(slot
                .lock()
                .branch_catalog()
                .branch_state(branch_id)
                .map_err(map_lifecycle_error)?
                .retained_timeline()
                .is_complete_for_test()),
            StorageRuntimeInner::DurableOwned(slot) => Ok(slot
                .lock()
                .branch_catalog()
                .branch_state(branch_id)
                .map_err(map_lifecycle_error)?
                .retained_timeline()
                .is_complete_for_test()),
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "timeline inspection requires an open runtime",
            }),
        }
    }

    /// #3112 S3a: drops the index back to unproven coverage — the state a
    /// fork rebuilt from the catalog manifest and a corruption-guard poison
    /// both produce. Wall-clock resolution has no scan fallback, so this is the
    /// only way to exercise its refusal through the real API.
    #[cfg(test)]
    pub(crate) fn mark_retained_timeline_incomplete_for_test(
        &self,
        branch_id: BranchId,
    ) -> StorageApiResult<()> {
        let mark = |state: &crate::branch::state::BranchLocalState| {
            state
                .retained_timeline()
                .mark_incomplete_for_fork_recovery();
        };
        match &self.inner {
            StorageRuntimeInner::Cache(slot) => mark(
                slot.lock()
                    .branch_catalog()
                    .branch_state(branch_id)
                    .map_err(map_lifecycle_error)?,
            ),
            StorageRuntimeInner::DurableOwned(slot) => mark(
                slot.lock()
                    .branch_catalog()
                    .branch_state(branch_id)
                    .map_err(map_lifecycle_error)?,
            ),
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "timeline inspection requires an open runtime",
                })
            }
        }
        Ok(())
    }

    /// #3112 S2b: the wall-clock instant the retained-timeline index holds for
    /// one commit, or `None` when unknown. Lets a reopen test prove the instant
    /// survived the WAL round-trip rather than silently degrading to unknown.
    #[cfg(test)]
    pub(crate) fn retained_committed_at_for_test(
        &self,
        branch_id: BranchId,
        commit_version: CommitVersion,
    ) -> StorageApiResult<Option<Timestamp>> {
        let entries = match &self.inner {
            StorageRuntimeInner::Cache(slot) => slot
                .lock()
                .branch_catalog()
                .branch_state(branch_id)
                .map_err(map_lifecycle_error)?
                .retained_timeline()
                .materialized_entries(None),
            StorageRuntimeInner::DurableOwned(slot) => slot
                .lock()
                .branch_catalog()
                .branch_state(branch_id)
                .map_err(map_lifecycle_error)?
                .retained_timeline()
                .materialized_entries(None),
            StorageRuntimeInner::Closed => {
                return Err(StorageApiError::InvalidRuntimeState {
                    reason: "timeline inspection requires an open runtime",
                })
            }
        };
        Ok(entries.and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry.commit_version() == commit_version)
                .and_then(|entry| entry.committed_at())
        }))
    }

    #[cfg(test)]
    pub(crate) fn set_timestamp_coverage_for_test(
        &mut self,
        branch_id: BranchId,
        coverage: crate::branch::read::BranchTimestampCoverage,
    ) -> StorageApiResult<()> {
        match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                let generation = runtime
                    .branch_catalog()
                    .registry()
                    .lookup(branch_id)
                    .map_err(commit_error)?
                    .generation();
                runtime
                    .branch_catalog_mut_for_test()
                    .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))
                    .map_err(map_lifecycle_error)?
                    .set_timestamp_coverage(coverage);
                // BS2.3: coverage is part of the read view; republish after this test-only mutation.
                runtime.publish_branch_snapshot_for_test(branch_id);
                Ok(())
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let generation = runtime
                    .branch_catalog()
                    .registry()
                    .lookup(branch_id)
                    .map_err(commit_error)?
                    .generation();
                runtime
                    .branch_catalog_mut_for_test()
                    .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))
                    .map_err(map_lifecycle_error)?
                    .set_timestamp_coverage(coverage);
                // BS2.3: coverage is part of the read view; republish after this test-only mutation.
                runtime.publish_branch_snapshot_for_test(branch_id);
                Ok(())
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "timestamp coverage update requires an open runtime",
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn fork_default_branch_for_test(
        &mut self,
        destination: BranchId,
    ) -> StorageApiResult<()> {
        let destination_generation =
            crate::commit::CommitBranchGeneration::new(DEFAULT_BRANCH_GENERATION)
                .map_err(commit_error)?;
        let created_at = current_visible(self);
        match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => slot
                .lock()
                .fork_current(
                    DEFAULT_BRANCH_ID,
                    destination,
                    destination_generation,
                    created_at,
                )
                .map(|_| ())
                .map_err(map_lifecycle_error),
            StorageRuntimeInner::DurableOwned(slot) => slot
                .lock()
                .fork_current(
                    DEFAULT_BRANCH_ID,
                    destination,
                    destination_generation,
                    created_at,
                )
                .map(|_| ())
                .map_err(map_lifecycle_error),
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "fork requires an open runtime",
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn flush_default_branch_for_test(&mut self) -> StorageApiResult<()> {
        self.flush_branch_for_test(DEFAULT_BRANCH_ID)
    }

    #[cfg(test)]
    pub(crate) fn rotate_default_branch_for_test(&mut self) -> StorageApiResult<()> {
        self.rotate_branch_for_test(DEFAULT_BRANCH_ID)
    }

    /// Drop the table-object block cache so the next lazy read hits the backend (#3047 cold-read
    /// exercise). Cache mode is non-durable and has no such cache, so it is a no-op there.
    #[cfg(test)]
    pub(crate) fn clear_block_cache_for_test(&mut self) {
        match &mut self.inner {
            StorageRuntimeInner::DurableOwned(slot) => slot.lock().clear_block_cache_for_test(),
            StorageRuntimeInner::Cache(_) | StorageRuntimeInner::Closed => {}
        }
    }

    #[cfg(test)]
    pub(crate) fn rotate_branch_for_test(&mut self, branch_id: BranchId) -> StorageApiResult<()> {
        match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                runtime
                    .rotate_active_for_branch_for_maintenance(branch_id)
                    .map(|_| ())
                    .map_err(map_lifecycle_error)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                runtime
                    .rotate_active_for_branch_for_maintenance(branch_id)
                    .map(|_| ())
                    .map_err(map_lifecycle_error)
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "rotation requires an open runtime",
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn flush_branch_for_test(&mut self, branch_id: BranchId) -> StorageApiResult<()> {
        match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                runtime
                    .rotate_active_for_maintenance()
                    .map_err(map_lifecycle_error)?;
                runtime
                    .flush_frozen(&flush_request_for_boundary(branch_id)?)
                    .map(|_| ())
                    .map_err(map_lifecycle_error)
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                runtime
                    .rotate_active_for_maintenance()
                    .map_err(map_lifecycle_error)?;
                runtime
                    .flush_frozen(&flush_request_for_boundary(branch_id)?)
                    .map(|_| ())
                    .map_err(map_lifecycle_error)
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "flush requires an open runtime",
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn pin_branch_reachability_for_test(
        &mut self,
        branch_id: BranchId,
    ) -> StorageApiResult<()> {
        match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => slot
                .lock()
                .branch_catalog_mut_for_test()
                .pin_reachability(branch_id)
                .map(|_| ())
                .map_err(map_lifecycle_error),
            StorageRuntimeInner::DurableOwned(slot) => slot
                .lock()
                .branch_catalog_mut_for_test()
                .pin_reachability(branch_id)
                .map(|_| ())
                .map_err(map_lifecycle_error),
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "pin requires an open runtime",
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn append_raw_row_for_test(&mut self, row: StorageRow) -> StorageApiResult<()> {
        self.append_row_for_test_inner(row, true)
    }

    /// Append a committed row **above** the visible frontier without advancing it — the
    /// `applied_not_visible` shape (a commit whose visible publish failed after apply). Off-lock
    /// reads bounded by the visible version must hide the row (BS2.4 interleaving seam).
    #[cfg(test)]
    pub(crate) fn append_unacked_row_for_test(&mut self, row: StorageRow) -> StorageApiResult<()> {
        self.append_row_for_test_inner(row, false)
    }

    #[cfg(test)]
    fn append_row_for_test_inner(
        &mut self,
        row: StorageRow,
        advance_visible: bool,
    ) -> StorageApiResult<()> {
        let branch_id = row.physical_key().branch_id();
        // Raw appends bypass the commit executor, so this seam maintains the executor's
        // invariant itself: with `advance_visible` the visible frontier covers every committed row
        // (BS2.2 bounded Latest reads would otherwise hide the injected rows); without it the row
        // stays above the frontier to reproduce `applied_not_visible`.
        let commit_version = row.commit_version();
        let commit_timestamp = row.commit_timestamp();
        match &mut self.inner {
            StorageRuntimeInner::Cache(slot) => {
                let mut runtime = slot.lock();
                let generation = runtime
                    .branch_catalog()
                    .registry()
                    .lookup(branch_id)
                    .map_err(commit_error)?
                    .generation();
                runtime
                    .branch_catalog_mut_for_test()
                    .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))
                    .map_err(map_lifecycle_error)?
                    .append_committed_row(row)
                    .map(|_| ())
                    .map_err(branch_error)?;
                if advance_visible && commit_version > runtime.visible_version() {
                    runtime.catch_up_commit_frontier_for_test(commit_version, commit_timestamp);
                }
                // Model 2 commits do not republish, but this seam bypasses the commit path, so
                // refresh the snapshot's facts explicitly (harmless: the live active already sees
                // the append).
                runtime.publish_branch_snapshot_for_test(branch_id);
                Ok(())
            }
            StorageRuntimeInner::DurableOwned(slot) => {
                let mut runtime = slot.lock();
                let generation = runtime
                    .branch_catalog()
                    .registry()
                    .lookup(branch_id)
                    .map_err(commit_error)?
                    .generation();
                runtime
                    .branch_catalog_mut_for_test()
                    .branch_state_mut(branch_id, CommitBranchGenerationGuard::exact(generation))
                    .map_err(map_lifecycle_error)?
                    .append_committed_row(row)
                    .map(|_| ())
                    .map_err(branch_error)?;
                if advance_visible && commit_version > runtime.visible_version() {
                    runtime.catch_up_commit_frontier_for_test(commit_version, commit_timestamp);
                }
                // Model 2 commits do not republish, but this seam bypasses the commit path, so
                // refresh the snapshot's facts explicitly (harmless: the live active already sees
                // the append).
                runtime.publish_branch_snapshot_for_test(branch_id);
                Ok(())
            }
            StorageRuntimeInner::Closed => Err(StorageApiError::InvalidRuntimeState {
                reason: "raw row append requires an open runtime",
            }),
        }
    }
}

/// BS3.4b: the per-commit write-throttle delay for a durable runtime, dispatched on the admission
/// mode. Graded → the debt-adaptive token bucket (paced by the batch's active bytes); legacy → the
/// quadratic P-controller below. Default is legacy until BS3.4c bakes graded after the out-of-band
/// A/B.
fn durable_commit_throttle_delay_millis<S>(
    runtime: &LifecycleDurableLocalRuntime<'_, S>,
    batch: &crate::commit::CommitBatch,
) -> u64 {
    let batch_bytes = if runtime.is_graded_admission() {
        // Pacing is best-effort and the commit has already succeeded; a byte-estimate error
        // (row-size overflow) falls back to 0 bytes → no pacing for this commit, never a failure.
        estimate_commit_batch_active_bytes(batch).unwrap_or(0)
    } else {
        0
    };
    durable_commit_throttle_delay_millis_from_bytes(
        runtime,
        batch_bytes,
        runtime.last_write_admission(),
    )
}

/// Throttle-delay computation from pre-captured inputs (BS5.1): group members' batches are
/// consumed by the group execution, so the leader captures each member's byte estimate before
/// executing and each member's admission snapshot as it completes, then computes the delay
/// from the post-group runtime state — the same inputs a solo commit reads under the lock.
fn durable_commit_throttle_delay_millis_from_bytes<S>(
    runtime: &LifecycleDurableLocalRuntime<'_, S>,
    batch_bytes: u64,
    admission: Option<LifecycleWriteAdmissionOutcome>,
) -> u64 {
    if runtime.is_graded_admission() {
        return runtime.graded_write_throttle_delay_millis(batch_bytes);
    }
    write_throttle_delay_millis(
        admission.map_or(0, |admission| {
            admission.pressure().throttle_ratio_permille()
        }),
        runtime
            .open_plan()
            .lifecycle_config()
            .write_throttle_policy(),
    )
}

/// The durable-commit under-lock section (BS5.1): every commit joins the slot's group queue.
/// With no leader active the caller leads immediately (a drained-empty queue = the exact solo
/// path); otherwise it waits on its own joiner — never on the runtime lock — so served members
/// wake the moment the leader publishes their responses and immediately re-join, keeping the
/// next group full. Leadership hands off by promotion when the leader finishes.
fn execute_durable_commit_grouped<S>(
    slot: &RuntimeSlot<LifecycleDurableLocalRuntime<'static, S>>,
    batch: crate::commit::CommitBatch,
    original_batch: &crate::commit::CommitBatch,
    generation_guard: CommitBranchGenerationGuard,
) -> commit_group::CommitGroupResponse
where
    S: CommitTimestampSource,
{
    let request = commit_group::CommitGroupRequest {
        batch,
        generation_guard,
    };
    match slot.commit_groups().join(request) {
        commit_group::JoinPath::Lead(request) => {
            lead_commit_group_as_leader(slot, request, original_batch)
        }
        commit_group::JoinPath::Wait(joiner) => {
            match slot.commit_groups().await_service(&joiner) {
                commit_group::WaitOutcome::Done(response) => *response,
                commit_group::WaitOutcome::Lead(request) => {
                    lead_commit_group_as_leader(slot, request, original_batch)
                }
                // Leaders complete every taken member before handing off, so this is
                // only reachable if a leader died mid-group. Fail closed; never
                // re-execute.
                commit_group::WaitOutcome::Abandoned => commit_group::CommitGroupResponse {
                    outcome: Err(LifecycleError::InvalidLifecycleState {
                        reason: "write group leader did not complete this commit; \
                                 durability is uncertain",
                    }),
                    admission: None,
                    pending_tasks: 0,
                    wal_growth: None,
                    throttle_delay_millis: 0,
                },
            }
        }
    }
}

/// Acquire the runtime lock and lead one write group. `Always` runtimes take
/// the pipelined path (fsync outside the lock); `Standard` executes in a
/// single hold. The leadership guard promotes the next leader even if
/// execution panics — the pipelined path hands off EARLY (right after phase 1
/// releases the lock) so the next group forms and appends during this group's
/// covering fsync.
fn lead_commit_group_as_leader<S>(
    slot: &RuntimeSlot<LifecycleDurableLocalRuntime<'static, S>>,
    request: commit_group::CommitGroupRequest,
    original_batch: &crate::commit::CommitBatch,
) -> commit_group::CommitGroupResponse
where
    S: CommitTimestampSource,
{
    let leadership = commit_group::CommitGroupLeadership::new(slot.commit_groups());
    let mut runtime = slot.lock_for_commit();
    if matches!(
        runtime.open_plan().storage_mode(),
        LifecycleStorageMode::DurableLocalAlways
    ) {
        // Phase 1 under this hold: formation + admission + appends + applies
        // + sync-ticket capture (~µs-scale next to the fsync).
        let formed = form_commit_group(slot, &*runtime, request, original_batch);
        let joiners = formed.joiners;
        let member_bytes = formed.member_bytes;
        let mut in_flight = runtime.execute_durable_commit_group_begin(formed.batches);
        // Parallel branch applies (BS5.4c) inside this hold: checked-out
        // states must be back before the lock drops, or the NEXT group
        // (forming during our fsync) would fail closed on those branches.
        let apply_fatal = run_group_applies_parallel(
            &mut runtime,
            &mut in_flight,
            &joiners,
            &formed.member_branches,
        );
        let durable_seq = runtime.wal_durable_seq_handle();
        drop(runtime);
        // Hand leadership off NOW: the next group forms, appends, and applies
        // while this group's covering fsync runs.
        drop(leadership);
        // Off-lock durability: prove coverage by a completed sync, or run one
        // through the chain (one sync in flight at a time — the device flush
        // is the serial resource, and each completed sync covers everyone who
        // appended before its capture).
        let batching_beat = in_flight.concurrent_spans() > 1;
        let sync_result = in_flight.ticket().and_then(|ticket| {
            slot.wal_sync()
                .sync_or_wait_covered(&durable_seq, ticket, batching_beat, || {
                    slot.lock_for_commit().wal_group_sync_ticket()
                })
        });
        // Phase 2 under a fresh hold: redeem the sync, publish the group's
        // visibility, run post-commit bookkeeping.
        let mut runtime = slot.lock_for_commit();
        let results = runtime.execute_durable_commit_group_finish_with_apply_outcome(
            in_flight,
            sync_result,
            apply_fatal,
        );
        distribute_group_responses(&mut runtime, results, member_bytes, joiners)
    } else {
        lead_commit_group(slot, &mut runtime, request, original_batch)
    }
}

/// Today's exact solo under-lock section, shared by the uncontended fast path and a leader
/// whose queue drained empty.
fn solo_commit_response<S>(
    runtime: &mut LifecycleDurableLocalRuntime<'_, S>,
    batch: crate::commit::CommitBatch,
    original_batch: &crate::commit::CommitBatch,
    generation_guard: CommitBranchGenerationGuard,
) -> commit_group::CommitGroupResponse
where
    S: CommitTimestampSource,
{
    let result = runtime.execute_durable_commit(batch, generation_guard);
    let post_timer = perf_trace::start_timer();
    let throttle_delay_millis = durable_commit_throttle_delay_millis(runtime, original_batch);
    let response = commit_group::CommitGroupResponse {
        outcome: result,
        admission: runtime.last_write_admission(),
        pending_tasks: runtime.maintenance_status().pending_tasks(),
        wal_growth: runtime.last_wal_growth_outcome().cloned(),
        throttle_delay_millis,
    };
    perf_trace::record_commit_api_post_elapsed(post_timer);
    response
}

/// Group-leader under-lock section (BS5.1): drain queued joiners, execute the group with the
/// leader's request last (member order is version order and WAL order), then publish every
/// member's response BEFORE the runtime lock is released — a member waking under the lock must
/// find its response, never an in-flight group.
fn lead_commit_group<S>(
    slot: &RuntimeSlot<LifecycleDurableLocalRuntime<'static, S>>,
    runtime: &mut LifecycleDurableLocalRuntime<'static, S>,
    leader_request: commit_group::CommitGroupRequest,
    leader_original_batch: &crate::commit::CommitBatch,
) -> commit_group::CommitGroupResponse
where
    S: CommitTimestampSource,
{
    let formed = form_commit_group(slot, runtime, leader_request, leader_original_batch);
    if formed.joiners.is_empty() {
        // Nothing actually joined: the exact solo path. Only the Standard
        // leader reaches here (Always pipelines every group, including
        // groups of one).
        let mut batches = formed.batches;
        // The leader's request is the sole (last) entry by construction.
        let Some((batch, generation_guard)) = batches.pop() else {
            return commit_group::CommitGroupResponse {
                outcome: Err(LifecycleError::InvalidLifecycleState {
                    reason: "write group formed without its leader's request",
                }),
                admission: None,
                pending_tasks: 0,
                wal_growth: None,
                throttle_delay_millis: 0,
            };
        };
        return solo_commit_response(runtime, batch, leader_original_batch, generation_guard);
    }
    let mut in_flight = runtime.execute_durable_commit_group_begin(formed.batches);
    // Standard captures no sync ticket; the group's applies run in parallel
    // across the deferring members' parked threads (BS5.4c) inside this
    // single hold.
    let apply_fatal = run_group_applies_parallel(
        runtime,
        &mut in_flight,
        &formed.joiners,
        &formed.member_branches,
    );
    let results = runtime.execute_durable_commit_group_finish_with_apply_outcome(
        in_flight,
        None,
        apply_fatal,
    );
    distribute_group_responses(runtime, results, formed.member_bytes, formed.joiners)
}

/// BS5.4c: run the group's deferred branch applies in parallel — each
/// deferring member's parked thread applies its own branch's rows on an
/// owned, checked-out state (no locks held) while the leader applies the
/// remainder and then collects the barrier. Runs entirely under the leader's
/// runtime-lock hold, so checked-out states are never observable across
/// groups. Returns whether the group must go fatal (an apply failed, or an
/// outcome went missing).
fn run_group_applies_parallel<S>(
    runtime: &mut LifecycleDurableLocalRuntime<'static, S>,
    in_flight: &mut crate::lifecycle::DurableGroupInFlight<'static>,
    joiners: &[commit_group::CommitGroupJoinerHandle],
    member_branches: &[BranchId],
) -> bool
where
    S: CommitTimestampSource,
{
    let work = runtime.take_deferred_apply_work(in_flight);
    if work.is_empty() {
        return false;
    }
    let exchange = commit_group::CommitGroupExchangeHandle::default();
    let mut expected = 0_usize;
    let mut leader_work = Vec::new();
    for unit in work {
        let branch = unit.branch_id();
        // Route to the branch's first member thread; work is self-contained
        // (owned state + rows), so WHICH thread runs it is only a
        // load-balancing choice. The leader (last, unlisted) keeps its own.
        let Some(index) = member_branches.iter().position(|member| *member == branch) else {
            leader_work.push(unit);
            continue;
        };
        match joiners[index].request_apply(unit, &exchange) {
            Ok(()) => expected += 1,
            Err(work) => leader_work.push(work),
        }
    }
    // The leader's own share overlaps the members' applies.
    let leader_done: Vec<_> = leader_work
        .into_iter()
        .map(crate::lifecycle::DurableGroupApplyWork::apply)
        .collect();
    let mut done = exchange.wait_for(expected);
    // A missing outcome means a member thread died mid-apply (panic-class):
    // group-fatal, and that branch stays checked out — every later access
    // fails closed until reopen.
    let missing = expected.saturating_sub(done.len());
    done.extend(leader_done);
    let apply_fatal = runtime.finish_deferred_apply_work(done, in_flight.group_state());
    apply_fatal || missing > 0
}

/// One formed write group: queued joiners plus the leader's own request
/// (always LAST — member order is version order), with per-member byte
/// estimates captured before the batches are consumed.
struct FormedCommitGroup {
    joiners: Vec<commit_group::CommitGroupJoinerHandle>,
    batches: Vec<(crate::commit::CommitBatch, CommitBranchGenerationGuard)>,
    member_bytes: Vec<u64>,
    /// Branch of each joiner member (aligned with `joiners`; the leader is
    /// not listed), for routing deferred applies back to member threads.
    member_branches: Vec<BranchId>,
}

fn form_commit_group<S>(
    slot: &RuntimeSlot<LifecycleDurableLocalRuntime<'static, S>>,
    runtime: &LifecycleDurableLocalRuntime<'static, S>,
    leader_request: commit_group::CommitGroupRequest,
    leader_original_batch: &crate::commit::CommitBatch,
) -> FormedCommitGroup
where
    S: CommitTimestampSource,
{
    const MAX_DRAIN: usize = commit_group::COMMIT_GROUP_MAX_MEMBERS - 1;
    let members = slot.commit_groups().drain_members(MAX_DRAIN);
    let graded = runtime.is_graded_admission();
    let estimate_bytes = |batch: &crate::commit::CommitBatch| {
        if graded {
            // Same best-effort fallback as the solo throttle path.
            estimate_commit_batch_active_bytes(batch).unwrap_or(0)
        } else {
            0
        }
    };
    let mut joiners = Vec::with_capacity(members.len());
    let mut batches = Vec::with_capacity(members.len() + 1);
    let mut member_bytes = Vec::with_capacity(members.len() + 1);
    let mut member_branches = Vec::with_capacity(members.len());
    for (joiner, request) in members {
        member_bytes.push(estimate_bytes(&request.batch));
        member_branches.push(request.batch.branch_id());
        batches.push((request.batch, request.generation_guard));
        joiners.push(joiner);
    }
    member_bytes.push(estimate_bytes(leader_original_batch));
    batches.push((leader_request.batch, leader_request.generation_guard));
    FormedCommitGroup {
        joiners,
        batches,
        member_bytes,
        member_branches,
    }
}

/// Build per-member responses from settled group results (the same snapshots
/// each member would read under the lock after a solo commit), publish every
/// member's response into its joiner, and return the leader's own (last).
fn distribute_group_responses<S>(
    runtime: &mut LifecycleDurableLocalRuntime<'static, S>,
    results: Vec<crate::lifecycle::DurableGroupMemberResult>,
    member_bytes: Vec<u64>,
    joiners: Vec<commit_group::CommitGroupJoinerHandle>,
) -> commit_group::CommitGroupResponse
where
    S: CommitTimestampSource,
{
    let pending_tasks = runtime.maintenance_status().pending_tasks();
    let wal_growth = runtime.last_wal_growth_outcome().cloned();
    let mut responses: Vec<commit_group::CommitGroupResponse> = results
        .into_iter()
        .zip(member_bytes)
        .map(|(result, batch_bytes)| commit_group::CommitGroupResponse {
            throttle_delay_millis: durable_commit_throttle_delay_millis_from_bytes(
                runtime,
                batch_bytes,
                result.admission,
            ),
            outcome: result.outcome,
            admission: result.admission,
            pending_tasks,
            wal_growth: wal_growth.clone(),
        })
        .collect();
    // The leader's request was the last member; the group runtime returns one result
    // per member, so the pop cannot miss.
    let leader_response = responses
        .pop()
        .unwrap_or_else(|| commit_group::CommitGroupResponse {
            outcome: Err(LifecycleError::InvalidLifecycleState {
                reason: "write group returned no result for its leader",
            }),
            admission: None,
            pending_tasks,
            wal_growth,
            throttle_delay_millis: 0,
        });
    for (joiner, response) in joiners.into_iter().zip(responses) {
        joiner.complete(response);
    }
    leader_response
}

/// Per-commit proportional write-throttle delay in milliseconds. Returns 0 below the policy's
/// soft fullness threshold; above it the delay ramps quadratically toward `max_delay_millis` as
/// pool fullness approaches the hard memory budget (a well-damped P-controller: gentle at the
/// knee, strong as the budget nears). `RocksDB`'s debt-driven token-bucket rate ramp is a
/// deliberate out-of-scope follow-up (stateful integral controller; RC1/fix #3 territory).
fn write_throttle_delay_millis(ratio_permille: u16, policy: LifecycleWriteThrottlePolicy) -> u64 {
    if !policy.enabled() {
        return 0;
    }
    let soft = u64::from(policy.soft_ratio_permille());
    let ratio = u64::from(ratio_permille).min(1000);
    if ratio <= soft {
        return 0;
    }
    // `validate()` guarantees soft in 1..1000, so the denominator is >= 1.
    let excess_permille = (ratio - soft).saturating_mul(1000) / (1000 - soft);
    policy
        .max_delay_millis()
        .saturating_mul(excess_permille)
        .saturating_mul(excess_permille)
        / 1_000_000
}

const fn default_timestamp_source() -> ApiTimestampSource {
    ApiTimestampSource::new(DEFAULT_TIMESTAMP)
}

#[cfg(test)]
mod write_throttle_tests {
    use super::{write_throttle_delay_millis, LifecycleWriteThrottlePolicy};

    #[test]
    fn delay_is_zero_at_or_below_the_soft_threshold() {
        let policy = LifecycleWriteThrottlePolicy::new(700, 20);
        assert_eq!(write_throttle_delay_millis(0, policy), 0);
        assert_eq!(write_throttle_delay_millis(699, policy), 0);
        assert_eq!(write_throttle_delay_millis(700, policy), 0);
    }

    #[test]
    fn delay_ramps_monotonically_and_caps_at_max() {
        let policy = LifecycleWriteThrottlePolicy::new(700, 20);
        let mut previous = 0;
        for ratio in 700u16..=1000 {
            let delay = write_throttle_delay_millis(ratio, policy);
            assert!(
                delay >= previous,
                "delay must be non-decreasing in fullness"
            );
            assert!(delay <= 20, "delay must never exceed max_delay_millis");
            previous = delay;
        }
        // Full fullness reaches the cap; over-full (clamped) stays at the cap.
        assert_eq!(write_throttle_delay_millis(1000, policy), 20);
        assert_eq!(write_throttle_delay_millis(u16::MAX, policy), 20);
    }

    #[test]
    fn delay_is_zero_when_the_policy_is_disabled() {
        assert_eq!(
            write_throttle_delay_millis(1000, LifecycleWriteThrottlePolicy::disabled()),
            0
        );
    }
}
