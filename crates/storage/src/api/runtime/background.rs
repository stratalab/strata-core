use super::{
    fmt, perf_trace, Arc, AtomicBool, AtomicUsize, BackgroundBackpressureError,
    BackgroundTaskPriority, Duration, InlineMaintenanceExecutor, LifecycleConfig,
    LifecycleMaintenanceSchedulingPolicy, LifecycleStoragePressure, LifecycleStoragePressureReason,
    LifecycleStoragePressureSeverity, LifecycleWalGrowthTrigger, MaintenanceClock,
    MaintenanceExecutor, MaintenanceExecutorStats, MaintenanceInstant, ManualMaintenanceClock,
    ModeLifecyclePolicy, Ordering, ParkingMutex, ParkingMutexGuard, RealMaintenanceClock,
    StorageBackgroundMaintenanceOptions, ThreadedMaintenanceExecutor, WalGrowthFacts,
    DEFAULT_BACKGROUND_BLOCK_NO_RELIEF_ROUNDS, DEFAULT_BACKGROUND_BLOCK_STALL_DEADLINE,
    DEFAULT_BACKGROUND_BLOCK_WAIT_SLICE,
};

use std::sync::atomic::AtomicU64;

use strata_core::BranchId;

use crate::branch::read::BranchReadView;
use crate::branch::snapshot::{load_from_registry, BranchSnapshotRegistry};
use crate::lifecycle::RuntimeReadHandles;

pub(super) struct RuntimeSlot<R> {
    runtime: Arc<ParkingMutex<R>>,
    /// Off-lock read handles (BS2.4): the visible-version bound `V` and the published-snapshot
    /// registry, cloned out of the runtime at construction so a read never takes the runtime lock.
    visible: Arc<AtomicU64>,
    snapshot_registry: Arc<BranchSnapshotRegistry>,
    background: Option<BackgroundRuntimeController>,
    background_drain: Option<BackgroundDrainFn>,
    /// Write-group join queue (BS5.1): contended durable commits enqueue here
    /// and are executed in groups by whichever caller holds the runtime lock.
    commit_groups: super::commit_group::CommitGroupQueue,
    /// Covering-fsync chain (BS5.2): serializes the pipelined groups' off-lock
    /// syncs so the device flush stays fat (one sync in flight, everyone it
    /// covers skips their own).
    wal_sync: super::commit_group::WalSyncChain,
    /// Commits currently blocked on (or about to take) the runtime lock
    /// (BS5.3): background drain rounds yield between steps while this is
    /// nonzero — measured at 10-18 µs of writer lock-wait PER COMMIT when
    /// drains interleave freely. Bounded by the drains' one-task fairness
    /// floor so maintenance never starves into the admission stall wall.
    commit_waiters: Arc<AtomicUsize>,
    #[cfg(test)]
    pub(super) background_block_wait: BackgroundBlockWaitConfig,
}

pub(super) type BackgroundDrainFn = Arc<
    dyn Fn(BackgroundDrainLimits, Arc<dyn MaintenanceClock>) -> BackgroundDrainRound + Send + Sync,
>;
pub(super) type BackgroundArcDrain<R> = fn(
    &Arc<ParkingMutex<R>>,
    &AtomicUsize,
    BackgroundDrainLimits,
    &Arc<dyn MaintenanceClock>,
) -> BackgroundDrainRound;

impl<R> fmt::Debug for RuntimeSlot<R>
where
    R: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("RuntimeSlot");
        debug
            .field("runtime", &self.runtime)
            .field("visible", &self.visible.load(Ordering::Relaxed))
            .field("published_branches", &self.snapshot_registry.load().len())
            .field("background", &self.background)
            .field("background_drain", &self.background_drain.is_some())
            .field("commit_groups", &self.commit_groups)
            .field("wal_sync", &self.wal_sync)
            .field(
                "commit_waiters",
                &self.commit_waiters.load(Ordering::Relaxed),
            );
        #[cfg(test)]
        debug.field("background_block_wait", &self.background_block_wait);
        debug.finish()
    }
}

pub(super) struct BackgroundRuntimeController {
    executor: Arc<dyn MaintenanceExecutor>,
    clock: Arc<dyn MaintenanceClock>,
    close_requested: Arc<AtomicBool>,
    active_drains: Arc<AtomicUsize>,
    max_concurrent_drains: usize,
    wake_requested_mask: Arc<AtomicUsize>,
    max_tasks_per_wake: usize,
    max_runtime_per_wake: Duration,
    drain_immediately: bool,
}

impl fmt::Debug for BackgroundRuntimeController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BackgroundRuntimeController")
            .field("stats", &self.stats())
            .field("close_requested", &self.close_requested)
            .field("active_drains", &self.active_drains)
            .field("max_concurrent_drains", &self.max_concurrent_drains)
            .field("wake_requested_mask", &self.wake_requested_mask)
            .field("max_tasks_per_wake", &self.max_tasks_per_wake)
            .field("max_runtime_per_wake", &self.max_runtime_per_wake)
            .field("drain_immediately", &self.drain_immediately)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BackgroundDrainRound {
    pub(super) tasks_completed: usize,
    pub(super) pending_tasks: usize,
    pub(super) made_progress: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BackgroundDrainLimits {
    pub(super) max_tasks: usize,
    pub(super) max_runtime: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct BackgroundShutdownStats {
    pub(super) stats: MaintenanceExecutorStats,
    pub(super) first_shutdown: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BackgroundExecutorMode {
    Threaded,
    Inline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BackgroundBlockWaitConfig {
    pub(super) wait_slice: Duration,
    pub(super) stall_deadline: Duration,
    pub(super) no_relief_rounds: usize,
}

impl Default for BackgroundBlockWaitConfig {
    fn default() -> Self {
        Self {
            wait_slice: DEFAULT_BACKGROUND_BLOCK_WAIT_SLICE,
            stall_deadline: DEFAULT_BACKGROUND_BLOCK_STALL_DEADLINE,
            no_relief_rounds: DEFAULT_BACKGROUND_BLOCK_NO_RELIEF_ROUNDS,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BackgroundPressureSnapshot {
    severity: LifecycleStoragePressureSeverity,
    active_bytes: u64,
    frozen_tables: usize,
    frozen_bytes: u64,
    level_zero_tables: usize,
    owned_tables: usize,
    table_rewrite_bytes: u64,
    inherited_layers: usize,
}

impl BackgroundPressureSnapshot {
    pub(super) fn from_pressure(pressure: LifecycleStoragePressure) -> Self {
        Self {
            severity: pressure.severity(),
            active_bytes: pressure.active_bytes(),
            frozen_tables: pressure.frozen_tables(),
            frozen_bytes: pressure.frozen_bytes(),
            level_zero_tables: pressure.level_zero_tables(),
            owned_tables: pressure.owned_tables(),
            table_rewrite_bytes: pressure.table_rewrite_bytes(),
            inherited_layers: pressure.inherited_layers(),
        }
    }

    pub(super) fn relieved_since(
        self,
        before: Self,
        reason: LifecycleStoragePressureReason,
    ) -> bool {
        if reason == LifecycleStoragePressureReason::FrozenBacklog {
            return self.active_bytes < before.active_bytes
                || self.frozen_tables < before.frozen_tables
                || self.frozen_bytes < before.frozen_bytes;
        }
        lifecycle_storage_pressure_severity_rank(self.severity)
            < lifecycle_storage_pressure_severity_rank(before.severity)
            || self.active_bytes < before.active_bytes
            || self.frozen_tables < before.frozen_tables
            || self.frozen_bytes < before.frozen_bytes
            || self.level_zero_tables < before.level_zero_tables
            || self.owned_tables < before.owned_tables
            || self.table_rewrite_bytes < before.table_rewrite_bytes
            || self.inherited_layers < before.inherited_layers
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BackgroundWalGrowthSnapshot {
    pub(super) retained_bytes: u64,
    pub(super) retained_segments: usize,
    pub(super) commits_since_checkpoint: u64,
    pub(super) exceeds_backpressure: bool,
}

impl BackgroundWalGrowthSnapshot {
    pub(super) fn from_parts(
        facts: WalGrowthFacts,
        commits_since_checkpoint: u64,
        trigger: Option<LifecycleWalGrowthTrigger>,
    ) -> Self {
        Self {
            retained_bytes: facts.retained_bytes(),
            retained_segments: facts.retained_segments(),
            commits_since_checkpoint,
            exceeds_backpressure: trigger.is_some(),
        }
    }

    pub(super) fn relieved_since(self, before: Self) -> bool {
        !self.exceeds_backpressure
            || self.retained_bytes < before.retained_bytes
            || self.retained_segments < before.retained_segments
            || self.commits_since_checkpoint < before.commits_since_checkpoint
    }
}

pub(super) const fn lifecycle_storage_pressure_severity_rank(
    severity: LifecycleStoragePressureSeverity,
) -> u8 {
    match severity {
        LifecycleStoragePressureSeverity::None => 0,
        LifecycleStoragePressureSeverity::Background => 1,
        LifecycleStoragePressureSeverity::Urgent => 2,
        LifecycleStoragePressureSeverity::BlockMutatingAdmission => 3,
    }
}

impl<R> RuntimeSlot<R> {
    /// The current visible-commit-version bound `V`, loaded off-lock (Acquire).
    pub(super) fn visible(&self) -> u64 {
        self.visible.load(Ordering::Acquire)
    }

    /// The published snapshot for `branch_id`, loaded off-lock from the shared registry.
    pub(super) fn load_snapshot(&self, branch_id: BranchId) -> Option<Arc<BranchReadView>> {
        load_from_registry(&self.snapshot_registry, branch_id)
    }

    pub(super) fn lock(&self) -> ParkingMutexGuard<'_, R> {
        let started = perf_trace::start_timer();
        let guard = self.runtime.lock();
        perf_trace::record_lifecycle_foreground_wait_background_lock(perf_trace::timer_elapsed(
            started,
        ));
        guard
    }

    /// The write-group join queue (BS5.1).
    pub(super) fn commit_groups(&self) -> &super::commit_group::CommitGroupQueue {
        &self.commit_groups
    }

    /// The covering-fsync chain (BS5.2).
    pub(super) fn wal_sync(&self) -> &super::commit_group::WalSyncChain {
        &self.wal_sync
    }

    /// Runtime lock acquisition for the COMMIT path (BS5.3): registers as a
    /// commit waiter for the blocked span so background drain rounds yield
    /// between steps instead of interleaving lock holds into every commit.
    pub(super) fn lock_for_commit(&self) -> ParkingMutexGuard<'_, R> {
        self.commit_waiters.fetch_add(1, Ordering::Release);
        let guard = self.lock();
        self.commit_waiters.fetch_sub(1, Ordering::Release);
        guard
    }

    #[allow(
        dead_code,
        reason = "background lifecycle drains submit work through this handle"
    )]
    pub(super) fn runtime_handle(&self) -> Arc<ParkingMutex<R>> {
        Arc::clone(&self.runtime)
    }

    #[allow(
        dead_code,
        reason = "background lifecycle queue wakeups are wired through this hook"
    )]
    pub(super) fn submit_background(
        &self,
        priority: BackgroundTaskPriority,
        work: impl FnOnce(Arc<ParkingMutex<R>>) + Send + 'static,
    ) -> Result<(), BackgroundBackpressureError>
    where
        R: Send + 'static,
    {
        let runtime = self.runtime_handle();
        self.background
            .as_ref()
            .ok_or(BackgroundBackpressureError)?
            .submit(priority, move || work(runtime))
    }

    pub(super) fn notify_background_drain(&self, priority: BackgroundTaskPriority) {
        if let (Some(background), Some(drain)) = (&self.background, &self.background_drain) {
            background.notify_drain(priority, Arc::clone(drain));
        }
    }

    pub(super) fn wait_background_progress_until(
        &self,
        completed_before_wait: u64,
        deadline: MaintenanceInstant,
    ) -> bool {
        self.background.as_ref().is_some_and(|background| {
            background.wait_for_progress_until(completed_before_wait, deadline)
        })
    }

    pub(super) fn background_now(&self) -> Option<MaintenanceInstant> {
        self.background
            .as_ref()
            .map(BackgroundRuntimeController::now)
    }

    #[cfg(all(any(test, feature = "fault-injection"), feature = "localfs"))]
    pub(super) fn advance_maintenance_clock(&self, by: Duration) -> bool {
        match self.background.as_ref() {
            Some(background) => {
                background.advance_clock(by);
                true
            }
            None => false,
        }
    }

    pub(super) fn has_background(&self) -> bool {
        self.background.is_some()
    }

    pub(super) fn background_stats(&self) -> Option<MaintenanceExecutorStats> {
        self.background
            .as_ref()
            .map(BackgroundRuntimeController::stats)
    }

    pub(super) fn shutdown_background(
        &self,
        timeout: Option<Duration>,
    ) -> Option<BackgroundShutdownStats> {
        self.background
            .as_ref()
            .map(|background| background.shutdown(timeout))
    }

    #[cfg(test)]
    pub(super) fn background_shutdown_requested_flag(&self) -> Option<Arc<AtomicBool>> {
        self.background
            .as_ref()
            .map(BackgroundRuntimeController::close_requested_flag)
    }

    #[cfg(test)]
    pub(super) fn wait_background_idle(&self) {
        if let Some(background) = &self.background {
            background.drain_scheduler();
        }
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(super) fn wait_background_idle_until(
        &self,
        timeout: Duration,
    ) -> Option<MaintenanceExecutorStats> {
        let background = self.background.as_ref()?;
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(std::time::Instant::now);
        loop {
            if background.drain_immediately {
                let completed_before_wait = background.stats().tasks_completed;
                let _ = background
                    .executor
                    .wait_for_progress(completed_before_wait, Duration::from_millis(1));
            }
            let stats = background.stats();
            if stats.queue_depth == 0 && stats.active_tasks == 0 {
                return Some(stats);
            }
            if std::time::Instant::now() >= deadline {
                return Some(stats);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(super) fn set_background_drain_limits(
        &mut self,
        max_tasks: usize,
        max_runtime: Duration,
    ) -> bool {
        let Some(background) = &mut self.background else {
            return false;
        };
        background.max_tasks_per_wake = max_tasks;
        background.max_runtime_per_wake = max_runtime;
        true
    }

    #[cfg(test)]
    #[cfg_attr(not(feature = "perf-trace"), allow(dead_code))]
    pub(super) fn set_background_block_wait_for_test(
        &mut self,
        wait_slice: Duration,
        stall_deadline: Duration,
        no_relief_rounds: usize,
    ) -> bool {
        if self.background.is_none() {
            return false;
        }
        self.background_block_wait = BackgroundBlockWaitConfig {
            wait_slice,
            stall_deadline,
            no_relief_rounds,
        };
        true
    }
}

impl<R> RuntimeSlot<R>
where
    R: Send + RuntimeReadHandles + 'static,
{
    pub(super) fn new_with_background_arc_drain(
        runtime: R,
        config: LifecycleConfig,
        background_config: StorageBackgroundMaintenanceOptions,
        executor_mode: BackgroundExecutorMode,
        mode_policy: ModeLifecyclePolicy,
        drain: BackgroundArcDrain<R>,
    ) -> Self {
        let visible = runtime.visible_handle();
        let snapshot_registry = runtime.snapshot_registry();
        let runtime = Arc::new(ParkingMutex::new(runtime));
        // The maintenance scheduling policy selects the executor flavor, but the
        // mode policy is authoritative: volatile modes (cache) never run a
        // background maintenance executor, so no worker thread, condvar, or
        // clock is created for them.
        let background = if config.maintenance_scheduling_policy()
            == LifecycleMaintenanceSchedulingPolicy::Background
            && mode_policy.may_run_background_maintenance()
        {
            Some(BackgroundRuntimeController::new(
                background_config,
                executor_mode,
            ))
        } else {
            None
        };
        let commit_waiters = Arc::new(AtomicUsize::new(0));
        let background_drain = background.as_ref().map(|_| {
            let runtime = Arc::clone(&runtime);
            let waiters = Arc::clone(&commit_waiters);
            Arc::new(move |limits, clock| drain(&runtime, &waiters, limits, &clock))
                as BackgroundDrainFn
        });
        Self {
            runtime,
            visible,
            snapshot_registry,
            background,
            background_drain,
            commit_groups: super::commit_group::CommitGroupQueue::default(),
            wal_sync: super::commit_group::WalSyncChain::default(),
            commit_waiters,
            #[cfg(test)]
            background_block_wait: BackgroundBlockWaitConfig::default(),
        }
    }
}

impl<R> Drop for RuntimeSlot<R> {
    fn drop(&mut self) {
        // Join the workers (bounded, same policy as close), don't just signal:
        // an in-flight background task holds the runtime Arc — and with it the
        // backend writer lock — so a signal-only drop leaves a window where an
        // immediate reopen of the same database fails EWOULDBLOCK (post-delete
        // GC publishes a pending-releases manifest through several fsyncs).
        // Drop must release the lock deterministically; the timeout preserves
        // liveness by detaching workers that fail to quiesce, exactly like the
        // close path.
        // Rationale: shutdown stats are close()'s diagnostics concern; a drop
        // site has no caller to report them to.
        let _ = self.shutdown_background(Some(super::DEFAULT_BACKGROUND_CLOSE_SHUTDOWN_TIMEOUT));
    }
}

impl BackgroundRuntimeController {
    pub(super) fn new(
        background_config: StorageBackgroundMaintenanceOptions,
        executor_mode: BackgroundExecutorMode,
    ) -> Self {
        let (executor, clock, worker_count, drain_immediately): (
            Arc<dyn MaintenanceExecutor>,
            Arc<dyn MaintenanceClock>,
            usize,
            bool,
        ) = match executor_mode {
            BackgroundExecutorMode::Threaded => (
                Arc::new(ThreadedMaintenanceExecutor::new(
                    background_config.worker_count(),
                    background_config.scheduler_queue_depth(),
                )),
                Arc::new(RealMaintenanceClock::new()),
                background_config.worker_count(),
                false,
            ),
            BackgroundExecutorMode::Inline => {
                let clock: Arc<dyn MaintenanceClock> = Arc::new(ManualMaintenanceClock::default());
                (
                    Arc::new(InlineMaintenanceExecutor::new(
                        background_config.scheduler_queue_depth(),
                        Arc::clone(&clock),
                    )) as Arc<dyn MaintenanceExecutor>,
                    clock,
                    0,
                    true,
                )
            }
        };
        perf_trace::record_lifecycle_background_runtime_created(worker_count);
        Self {
            executor,
            clock,
            close_requested: Arc::new(AtomicBool::new(false)),
            active_drains: Arc::new(AtomicUsize::new(0)),
            // Allow up to one concurrent drain per worker thread; the inline
            // (deterministic) executor reports zero workers and stays
            // single-flight via the `max(1)` floor.
            max_concurrent_drains: worker_count.max(1),
            wake_requested_mask: Arc::new(AtomicUsize::new(0)),
            max_tasks_per_wake: background_config.max_tasks_per_wake(),
            max_runtime_per_wake: background_config.max_runtime_per_wake(),
            drain_immediately,
        }
    }

    fn stats(&self) -> MaintenanceExecutorStats {
        self.executor.stats()
    }

    fn now(&self) -> MaintenanceInstant {
        self.clock.now()
    }

    #[cfg(all(any(test, feature = "fault-injection"), feature = "localfs"))]
    fn advance_clock(&self, by: Duration) {
        self.clock.advance(by);
    }

    fn submit(
        &self,
        priority: BackgroundTaskPriority,
        work: impl FnOnce() + Send + 'static,
    ) -> Result<(), BackgroundBackpressureError> {
        let capture_enabled = perf_trace::test_capture_enabled_for_current_thread();
        self.executor.submit(
            priority,
            Box::new(move || {
                perf_trace::with_test_capture_enabled_for_current_thread(capture_enabled, work);
            }),
        )
    }

    fn wait_for_progress_until(
        &self,
        completed_before_wait: u64,
        deadline: MaintenanceInstant,
    ) -> bool {
        let now = self.clock.now();
        if now >= deadline {
            return false;
        }
        let timeout = deadline.saturating_duration_since(now);
        let progressed = self
            .executor
            .wait_for_progress(completed_before_wait, timeout);
        if self.drain_immediately {
            let elapsed = self.clock.now().saturating_duration_since(now);
            let simulated_wait = timeout.min(self.max_runtime_per_wake);
            if elapsed < simulated_wait {
                self.clock.sleep(simulated_wait.saturating_sub(elapsed));
            }
        }
        progressed
    }

    #[cfg(test)]
    fn drain_scheduler(&self) {
        self.executor.wait_for_idle();
    }

    fn notify_drain(&self, priority: BackgroundTaskPriority, drain: BackgroundDrainFn) {
        if self.close_requested.load(Ordering::Acquire) {
            perf_trace::record_lifecycle_background_submit_after_shutdown_rejected();
            perf_trace::record_lifecycle_background_wake_rejected();
            return;
        }
        let wake_bit = background_priority_bit(priority);
        // Claim one of up to `max_concurrent_drains` drain slots so the worker
        // pool can run several drains at once. Each drain claims a different
        // maintenance lane under the runtime lock (same-lane collisions are
        // impossible — see `next_startable_task_index`/`task_lane_is_active`), so
        // flush, compaction, checkpoint, and WAL maintenance overlap. If we are
        // already at the concurrency cap, record the wake so a finishing drain
        // re-arms it instead of dropping it.
        let claimed =
            self.active_drains
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |in_flight| {
                    (in_flight < self.max_concurrent_drains).then_some(in_flight + 1)
                });
        if claimed.is_err() {
            self.wake_requested_mask
                .fetch_or(wake_bit, Ordering::AcqRel);
            perf_trace::record_lifecycle_background_wake_coalesced();
            return;
        }
        self.submit_drain(priority, drain);
    }

    fn submit_drain(&self, priority: BackgroundTaskPriority, drain: BackgroundDrainFn) {
        let controller = self.clone();
        let capture_enabled = perf_trace::test_capture_enabled_for_current_thread();
        let submit = self.executor.submit(
            priority,
            Box::new(move || {
                perf_trace::with_test_capture_enabled_for_current_thread(capture_enabled, || {
                    let limits = BackgroundDrainLimits {
                        max_tasks: controller.max_tasks_per_wake,
                        max_runtime: controller.max_runtime_per_wake,
                    };
                    let round = drain(limits, Arc::clone(&controller.clock));
                    perf_trace::record_lifecycle_background_drain_round(round.tasks_completed);
                    if round.tasks_completed > 0 {
                        perf_trace::record_lifecycle_pressure_clear_wake();
                    }
                    // Release this drain's concurrency slot, then re-arm: if work
                    // remains after making progress, or a wake arrived while we
                    // were at the concurrency cap, kick another drain. Clearing
                    // every requested bit is safe because a drain services all
                    // lanes regardless of the priority that triggered it.
                    controller.active_drains.fetch_sub(1, Ordering::AcqRel);
                    let requested = controller.wake_requested_mask.swap(0, Ordering::AcqRel) != 0;
                    if (round.pending_tasks > 0 && round.made_progress) || requested {
                        controller.notify_drain(priority, drain);
                    }
                });
            }),
        );
        if let Ok(()) = submit {
            perf_trace::record_lifecycle_background_wake_submitted();
            if self.drain_immediately && self.executor.stats().active_tasks == 0 {
                let completed_before_wait = self.executor.stats().tasks_completed;
                let _ = self
                    .executor
                    .wait_for_progress(completed_before_wait, self.max_runtime_per_wake);
            }
        } else {
            // The executor rejected the submission (e.g. shutdown): release the
            // slot claimed in `notify_drain` so the counter stays balanced.
            self.active_drains.fetch_sub(1, Ordering::AcqRel);
            perf_trace::record_lifecycle_background_wake_rejected();
        }
    }

    fn shutdown(&self, timeout: Option<Duration>) -> BackgroundShutdownStats {
        let first_shutdown = !self.close_requested.swap(true, Ordering::AcqRel);
        if first_shutdown {
            self.executor.shutdown(timeout);
        }
        BackgroundShutdownStats {
            stats: self.executor.stats(),
            first_shutdown,
        }
    }

    #[cfg(test)]
    fn close_requested_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.close_requested)
    }
}

impl Clone for BackgroundRuntimeController {
    fn clone(&self) -> Self {
        Self {
            executor: Arc::clone(&self.executor),
            clock: Arc::clone(&self.clock),
            close_requested: Arc::clone(&self.close_requested),
            active_drains: Arc::clone(&self.active_drains),
            max_concurrent_drains: self.max_concurrent_drains,
            wake_requested_mask: Arc::clone(&self.wake_requested_mask),
            max_tasks_per_wake: self.max_tasks_per_wake,
            max_runtime_per_wake: self.max_runtime_per_wake,
            drain_immediately: self.drain_immediately,
        }
    }
}

pub(super) const fn background_priority_value(priority: BackgroundTaskPriority) -> usize {
    match priority {
        BackgroundTaskPriority::Low => 0,
        BackgroundTaskPriority::Normal => 1,
        BackgroundTaskPriority::High => 2,
    }
}

pub(super) const fn background_priority_bit(priority: BackgroundTaskPriority) -> usize {
    1usize << background_priority_value(priority)
}

#[cfg(test)]
mod background_controller_tests {
    use super::*;
    use std::sync::atomic::{
        AtomicBool as TestAtomicBool, AtomicUsize as TestAtomicUsize, Ordering as TestOrdering,
    };
    use std::sync::{Arc as TestArc, Barrier};
    use std::time::Instant;

    #[test]
    fn controller_submits_high_priority_wake_while_normal_drain_is_active() {
        let controller = BackgroundRuntimeController::new(
            StorageBackgroundMaintenanceOptions::product_default()
                .with_worker_count(2)
                .with_scheduler_queue_depth(8),
            BackgroundExecutorMode::Threaded,
        );
        let normal_started = TestArc::new(Barrier::new(2));
        let normal_release = TestArc::new(Barrier::new(2));
        let high_ran = TestArc::new(TestAtomicBool::new(false));

        let normal_drain: BackgroundDrainFn = {
            let normal_started = TestArc::clone(&normal_started);
            let normal_release = TestArc::clone(&normal_release);
            TestArc::new(move |_limits, _clock| {
                normal_started.wait();
                normal_release.wait();
                BackgroundDrainRound {
                    tasks_completed: 1,
                    pending_tasks: 0,
                    made_progress: true,
                }
            })
        };
        controller.notify_drain(BackgroundTaskPriority::Normal, normal_drain);
        normal_started.wait();

        let high_drain: BackgroundDrainFn = {
            let high_ran = TestArc::clone(&high_ran);
            TestArc::new(move |_limits, _clock| {
                high_ran.store(true, TestOrdering::Release);
                BackgroundDrainRound {
                    tasks_completed: 1,
                    pending_tasks: 0,
                    made_progress: true,
                }
            })
        };
        controller.notify_drain(BackgroundTaskPriority::High, high_drain);

        let deadline = Instant::now() + Duration::from_secs(1);
        while !high_ran.load(TestOrdering::Acquire) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        let ran_while_normal_active = high_ran.load(TestOrdering::Acquire);
        normal_release.wait();
        controller.shutdown(Some(Duration::from_secs(1)));

        assert!(
            ran_while_normal_active,
            "high-priority wake should be submitted while normal-priority drain is still active"
        );
    }

    #[test]
    fn controller_runs_multiple_drains_concurrently_under_worker_pool() {
        let controller = BackgroundRuntimeController::new(
            StorageBackgroundMaintenanceOptions::product_default()
                .with_worker_count(2)
                .with_scheduler_queue_depth(8),
            BackgroundExecutorMode::Threaded,
        );
        // The prior single-flight gating ran one drain at a time. The concurrency
        // counter must let up to `worker_count` drains run simultaneously, so two
        // same-priority wakes should both be in flight at once.
        let active = TestArc::new(TestAtomicUsize::new(0));
        let peak = TestArc::new(TestAtomicUsize::new(0));
        let release = TestArc::new(TestAtomicBool::new(false));

        let make_drain = || -> BackgroundDrainFn {
            let active = TestArc::clone(&active);
            let peak = TestArc::clone(&peak);
            let release = TestArc::clone(&release);
            TestArc::new(move |_limits, _clock| {
                let now_active = active.fetch_add(1, TestOrdering::AcqRel) + 1;
                peak.fetch_max(now_active, TestOrdering::AcqRel);
                let deadline = Instant::now() + Duration::from_secs(2);
                while !release.load(TestOrdering::Acquire) && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(1));
                }
                active.fetch_sub(1, TestOrdering::AcqRel);
                BackgroundDrainRound {
                    tasks_completed: 1,
                    pending_tasks: 0,
                    made_progress: true,
                }
            })
        };

        controller.notify_drain(BackgroundTaskPriority::Normal, make_drain());
        controller.notify_drain(BackgroundTaskPriority::Normal, make_drain());

        let deadline = Instant::now() + Duration::from_secs(2);
        while peak.load(TestOrdering::Acquire) < 2 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        let observed_peak = peak.load(TestOrdering::Acquire);
        release.store(true, TestOrdering::Release);
        controller.shutdown(Some(Duration::from_secs(1)));

        assert_eq!(
            observed_peak, 2,
            "two same-priority drains must run concurrently under the worker pool"
        );
    }
}
