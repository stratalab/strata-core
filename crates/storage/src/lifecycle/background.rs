//! Background maintenance scheduler.
//!
//! This module is a storage port of `crates/engine/src/background.rs`.
//! It keeps the old scheduler's core semantics: priority ordering, FIFO
//! ordering within a priority, bounded queue backpressure, drain, idempotent
//! shutdown, task-panic containment, and the submit/shutdown race fix.

use parking_lot::{Condvar, Mutex as ParkingMutex};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::observability::perf_trace;

/// Priority levels for background lifecycle work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BackgroundTaskPriority {
    /// Deferred health, retention, purge, and repair work.
    Low = 0,
    /// Compaction and materialization table rewrites.
    Normal = 1,
    /// Flushes, checkpoints, and pressure-clearing work.
    High = 2,
}

/// Error returned when background work cannot be accepted.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct BackgroundBackpressureError;

impl std::fmt::Display for BackgroundBackpressureError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("background maintenance scheduler queue is full")
    }
}

impl std::error::Error for BackgroundBackpressureError {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) struct MaintenanceInstant {
    elapsed: Duration,
}

impl MaintenanceInstant {
    pub(crate) const fn from_elapsed(elapsed: Duration) -> Self {
        Self { elapsed }
    }

    pub(crate) fn saturating_add(self, duration: Duration) -> Self {
        Self {
            elapsed: self.elapsed.saturating_add(duration),
        }
    }

    pub(crate) fn saturating_duration_since(self, earlier: Self) -> Duration {
        self.elapsed.saturating_sub(earlier.elapsed)
    }
}

pub(crate) trait MaintenanceClock: Send + Sync + std::fmt::Debug {
    fn now(&self) -> MaintenanceInstant;

    fn sleep(&self, duration: Duration);

    /// Advance simulated time without blocking. The manual clock moves its elapsed
    /// counter; the real clock is a deliberate no-op (an explicit advance must never
    /// block a background runtime). Distinct from `sleep`, which the real clock honors.
    fn advance(&self, duration: Duration);

    /// Space-reclamation contract §3.1 (slice 8): run `on_fire` once, `delay`
    /// from now, without blocking the caller. The real clock does it on a
    /// detached one-shot thread; the manual clock does nothing (its owner
    /// fires due timers from `advance`). Never load-bearing (DUR-006): a timer
    /// that never fires costs idle latency, not correctness.
    fn schedule(&self, delay: Duration, on_fire: Box<dyn FnOnce() + Send + 'static>);
}

#[derive(Debug)]
pub(crate) struct RealMaintenanceClock {
    origin: Instant,
}

impl RealMaintenanceClock {
    pub(crate) fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl MaintenanceClock for RealMaintenanceClock {
    fn now(&self) -> MaintenanceInstant {
        MaintenanceInstant::from_elapsed(self.origin.elapsed())
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }

    fn advance(&self, _duration: Duration) {
        // Real time advances on its own; an explicit advance is a no-op so it can
        // never block a background runtime running on the real clock.
    }

    fn schedule(&self, delay: Duration, on_fire: Box<dyn FnOnce() + Send + 'static>) {
        // Rationale: a timer thread that cannot be created only loses one idle
        // wake; the next ordinary wake re-evaluates the debt.
        let _ = std::thread::Builder::new()
            .name("strata-idle-wake".to_owned())
            .spawn(move || {
                std::thread::sleep(delay);
                on_fire();
            });
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ManualMaintenanceClock {
    elapsed_nanos: Arc<AtomicU64>,
}

impl MaintenanceClock for ManualMaintenanceClock {
    fn now(&self) -> MaintenanceInstant {
        MaintenanceInstant::from_elapsed(Duration::from_nanos(
            self.elapsed_nanos.load(AtomicOrdering::Acquire),
        ))
    }

    fn sleep(&self, duration: Duration) {
        self.advance(duration);
    }

    fn advance(&self, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        self.elapsed_nanos
            .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |current| {
                Some(current.saturating_add(nanos))
            })
            .expect("manual maintenance clock update is infallible");
    }

    fn schedule(&self, _delay: Duration, _on_fire: Box<dyn FnOnce() + Send + 'static>) {
        // Simulated time never fires on its own: the controller that armed the
        // timer checks the deadline when the clock is advanced.
    }
}

/// Maintenance executor metrics snapshot.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaintenanceExecutorStats {
    /// Number of tasks waiting in the background queue.
    pub(crate) queue_depth: usize,
    /// Number of tasks currently being executed by workers.
    pub(crate) active_tasks: usize,
    /// Total number of tasks completed since scheduler creation.
    pub(crate) tasks_completed: u64,
    /// Total number of tasks completed after shutdown was requested.
    pub(crate) tasks_completed_after_shutdown: u64,
    /// Total number of task panics caught by the executor.
    pub(crate) worker_panics: u64,
    /// Total number of task panics caught after shutdown was requested.
    pub(crate) worker_panics_after_shutdown: u64,
    /// Number of worker threads.
    pub(crate) worker_count: usize,
}

pub(crate) trait MaintenanceExecutor: Send + Sync {
    fn submit(
        &self,
        priority: BackgroundTaskPriority,
        work: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), BackgroundBackpressureError>;

    fn wait_for_idle(&self);

    fn wait_for_progress(&self, completed_before_wait: u64, timeout: Duration) -> bool;

    fn request_shutdown(&self) -> bool;

    fn shutdown(&self, timeout: Option<Duration>);

    fn stats(&self) -> MaintenanceExecutorStats;
}

struct TaskEnvelope {
    priority: BackgroundTaskPriority,
    sequence: u64,
    work: Box<dyn FnOnce() + Send>,
    capture_enabled: bool,
}

impl Eq for TaskEnvelope {}

impl PartialEq for TaskEnvelope {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Ord for TaskEnvelope {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then(other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for TaskEnvelope {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// # Memory Ordering
///
/// The metric counters use mixed ordering depending on their role:
///
/// - `queue_depth` and `active_tasks` use `Release` on writes and `Acquire` on
///   correctness-critical reads because they gate backpressure and drain waits.
///   `stats()` reads them with `Relaxed` because approximate monitoring
///   snapshots are acceptable.
/// - `tasks_completed` is purely observational and uses `Relaxed`.
/// - `active_tasks.fetch_sub` uses `AcqRel` because the previous value decides
///   whether drain waiters should be woken.
/// - `sequence` uses `Relaxed`; fetch-add atomicity is enough for uniqueness.
struct SchedulerInner {
    queue: ParkingMutex<BinaryHeap<TaskEnvelope>>,
    work_ready: Condvar,
    drain_cond: Condvar,
    shutdown: AtomicBool,
    sequence: AtomicU64,
    queue_depth: AtomicUsize,
    active_tasks: AtomicUsize,
    max_queue_depth: usize,
    tasks_completed: AtomicU64,
    tasks_completed_after_shutdown: AtomicU64,
    worker_panics: AtomicU64,
    worker_panics_after_shutdown: AtomicU64,
}

/// Priority scheduler for background lifecycle maintenance work.
///
/// Tasks are executed by a fixed worker pool. Higher-priority tasks run first;
/// tasks with the same priority run in FIFO submission order.
pub(crate) struct BackgroundScheduler {
    inner: Arc<SchedulerInner>,
    workers: ParkingMutex<Vec<JoinHandle<()>>>,
    worker_count: usize,
}

impl std::fmt::Debug for BackgroundScheduler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BackgroundScheduler")
            .field("stats", &self.stats())
            .finish()
    }
}

impl BackgroundScheduler {
    /// Creates a scheduler using the default storage maintenance thread
    /// name prefix.
    pub(crate) fn new(worker_count: usize, max_queue_depth: usize) -> Self {
        Self::with_thread_name_prefix(worker_count, max_queue_depth, "strata-storage-maint-bg")
    }

    /// Creates a scheduler using a caller-provided thread name prefix.
    pub(crate) fn with_thread_name_prefix(
        worker_count: usize,
        max_queue_depth: usize,
        thread_name_prefix: impl Into<String>,
    ) -> Self {
        assert!(
            worker_count > 0,
            "background maintenance scheduler requires at least one worker thread"
        );
        let inner = Arc::new(SchedulerInner {
            queue: ParkingMutex::new(BinaryHeap::new()),
            work_ready: Condvar::new(),
            drain_cond: Condvar::new(),
            shutdown: AtomicBool::new(false),
            sequence: AtomicU64::new(0),
            queue_depth: AtomicUsize::new(0),
            active_tasks: AtomicUsize::new(0),
            max_queue_depth,
            tasks_completed: AtomicU64::new(0),
            tasks_completed_after_shutdown: AtomicU64::new(0),
            worker_panics: AtomicU64::new(0),
            worker_panics_after_shutdown: AtomicU64::new(0),
        });

        let thread_name_prefix = thread_name_prefix.into();
        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let inner = Arc::clone(&inner);
            let thread_name = format!("{thread_name_prefix}-{index}");
            let handle = std::thread::Builder::new()
                .name(thread_name)
                .spawn(move || worker_loop(&inner))
                .expect("failed to spawn background maintenance worker thread");
            workers.push(handle);
        }

        Self {
            inner,
            workers: ParkingMutex::new(workers),
            worker_count,
        }
    }

    /// Submits a task to the background scheduler.
    ///
    /// Returns `Err` when the queue is at capacity or the scheduler has shut
    /// down.
    pub(crate) fn submit(
        &self,
        priority: BackgroundTaskPriority,
        work: impl FnOnce() + Send + 'static,
    ) -> Result<(), BackgroundBackpressureError> {
        if self.inner.shutdown.load(AtomicOrdering::Acquire) {
            perf_trace::record_lifecycle_background_submit_after_shutdown_rejected();
            return Err(BackgroundBackpressureError);
        }

        if self.inner.queue_depth.load(AtomicOrdering::Acquire) >= self.inner.max_queue_depth {
            return Err(BackgroundBackpressureError);
        }

        let capture_enabled = perf_trace::test_capture_enabled_for_current_thread();
        let sequence = self.inner.sequence.fetch_add(1, AtomicOrdering::Relaxed);
        let envelope = TaskEnvelope {
            priority,
            sequence,
            work: Box::new(work),
            capture_enabled,
        };

        {
            let mut queue = self.inner.queue.lock();
            // Authoritative shutdown check under lock. `shutdown()` takes this
            // same lock before notifying and joining, so accepted work cannot
            // land in a dead queue.
            if self.inner.shutdown.load(AtomicOrdering::Acquire) {
                perf_trace::record_lifecycle_background_submit_after_shutdown_rejected();
                return Err(BackgroundBackpressureError);
            }
            // Storage preserves the old scheduler's early backpressure
            // check and adds this lock-held recheck so concurrent submitters
            // cannot push the queue past its configured depth.
            if self.inner.queue_depth.load(AtomicOrdering::Acquire) >= self.inner.max_queue_depth {
                return Err(BackgroundBackpressureError);
            }
            queue.push(envelope);
            self.inner.queue_depth.fetch_add(1, AtomicOrdering::Release);
        }

        self.inner.work_ready.notify_one();
        Ok(())
    }

    /// Blocks until all queued and in-flight tasks have completed.
    ///
    /// Workers remain alive after drain returns.
    pub(crate) fn drain(&self) {
        let mut queue = self.inner.queue.lock();
        while self.inner.queue_depth.load(AtomicOrdering::Acquire) > 0
            || self.inner.active_tasks.load(AtomicOrdering::Acquire) > 0
        {
            self.inner.drain_cond.wait(&mut queue);
        }
    }

    /// Blocks until at least one accepted task completes, the scheduler becomes
    /// idle, or `deadline` is reached.
    pub(crate) fn wait_for_progress_until(
        &self,
        completed_before_wait: u64,
        deadline: Instant,
    ) -> bool {
        let mut queue = self.inner.queue.lock();
        loop {
            if self.inner.tasks_completed.load(AtomicOrdering::Acquire) > completed_before_wait {
                return true;
            }
            if self.inner.queue_depth.load(AtomicOrdering::Acquire) == 0
                && self.inner.active_tasks.load(AtomicOrdering::Acquire) == 0
            {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let timeout = deadline.saturating_duration_since(now);
            if self
                .inner
                .drain_cond
                .wait_for(&mut queue, timeout)
                .timed_out()
            {
                return self.inner.tasks_completed.load(AtomicOrdering::Acquire)
                    > completed_before_wait
                    || (self.inner.queue_depth.load(AtomicOrdering::Acquire) == 0
                        && self.inner.active_tasks.load(AtomicOrdering::Acquire) == 0);
            }
        }
    }

    fn request_shutdown(&self) -> bool {
        let first_shutdown = !self.inner.shutdown.swap(true, AtomicOrdering::AcqRel);
        if first_shutdown {
            perf_trace::record_lifecycle_background_shutdown();
        }

        {
            let _queue = self.inner.queue.lock();
            self.inner.work_ready.notify_all();
        }
        first_shutdown
    }

    fn wait_for_quiescence(&self, timeout: Option<Duration>, active_task_allowance: usize) -> bool {
        let started = Instant::now();
        let mut queue = self.inner.queue.lock();
        loop {
            if self.inner.queue_depth.load(AtomicOrdering::Acquire) == 0
                && self.inner.active_tasks.load(AtomicOrdering::Acquire) <= active_task_allowance
            {
                return true;
            }
            let Some(timeout) = timeout else {
                self.inner.drain_cond.wait(&mut queue);
                continue;
            };
            let elapsed = started.elapsed();
            if elapsed >= timeout {
                return false;
            }
            let remaining = timeout.saturating_sub(elapsed);
            let _ = self
                .inner
                .drain_cond
                .wait_for(&mut queue, remaining.min(Duration::from_millis(100)));
        }
    }

    /// Signals workers to drain accepted work and exit.
    ///
    /// Repeated shutdown calls are allowed. When `timeout` is supplied, worker
    /// threads that do not reach quiescence before the deadline are detached so
    /// public runtime close/drop paths cannot hang indefinitely.
    pub(crate) fn shutdown(&self, timeout: Option<Duration>) {
        let first_shutdown = self.request_shutdown();

        let (current_worker, workers_to_join) = {
            let current_thread_id = std::thread::current().id();
            let mut current_worker = false;
            let mut current_worker_handle = None;
            let mut workers_to_join = Vec::new();
            let mut workers = self.workers.lock();
            for handle in workers.drain(..) {
                if handle.thread().id() == current_thread_id {
                    current_worker = true;
                    current_worker_handle = Some(handle);
                } else {
                    workers_to_join.push(handle);
                }
            }
            if let Some(handle) = current_worker_handle {
                workers.push(handle);
            }
            (current_worker, workers_to_join)
        };

        if current_worker {
            drain_ready_tasks_on_current_thread(&self.inner);
        }
        let active_task_allowance = usize::from(current_worker);
        let quiesced = self.wait_for_quiescence(timeout, active_task_allowance);
        let joined_workers = if quiesced {
            let joined_workers = workers_to_join.len();
            for handle in workers_to_join {
                let _ = handle.join();
            }
            joined_workers
        } else {
            let detached_workers = workers_to_join.len();
            perf_trace::record_lifecycle_background_shutdown_detached_workers(detached_workers);
            0
        };
        if first_shutdown {
            perf_trace::record_lifecycle_background_shutdown_joined_workers(joined_workers);
        }
    }

    /// Returns an observational metrics snapshot.
    pub(crate) fn stats(&self) -> MaintenanceExecutorStats {
        MaintenanceExecutorStats {
            queue_depth: self.inner.queue_depth.load(AtomicOrdering::Relaxed),
            active_tasks: self.inner.active_tasks.load(AtomicOrdering::Relaxed),
            tasks_completed: self.inner.tasks_completed.load(AtomicOrdering::Relaxed),
            tasks_completed_after_shutdown: self
                .inner
                .tasks_completed_after_shutdown
                .load(AtomicOrdering::Relaxed),
            worker_panics: self.inner.worker_panics.load(AtomicOrdering::Relaxed),
            worker_panics_after_shutdown: self
                .inner
                .worker_panics_after_shutdown
                .load(AtomicOrdering::Relaxed),
            worker_count: self.worker_count,
        }
    }
}

/// Threaded executor implementation backed by the ported old-engine scheduler.
pub(crate) struct ThreadedMaintenanceExecutor {
    scheduler: BackgroundScheduler,
}

impl std::fmt::Debug for ThreadedMaintenanceExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadedMaintenanceExecutor")
            .field("stats", &self.stats())
            .finish()
    }
}

impl ThreadedMaintenanceExecutor {
    pub(crate) fn new(worker_count: usize, max_queue_depth: usize) -> Self {
        Self {
            scheduler: BackgroundScheduler::new(worker_count, max_queue_depth),
        }
    }
}

impl MaintenanceExecutor for ThreadedMaintenanceExecutor {
    fn submit(
        &self,
        priority: BackgroundTaskPriority,
        work: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), BackgroundBackpressureError> {
        self.scheduler.submit(priority, work)
    }

    fn wait_for_idle(&self) {
        self.scheduler.drain();
    }

    fn wait_for_progress(&self, completed_before_wait: u64, timeout: Duration) -> bool {
        self.scheduler.wait_for_progress_until(
            completed_before_wait,
            Instant::now()
                .checked_add(timeout)
                .unwrap_or_else(Instant::now),
        )
    }

    fn request_shutdown(&self) -> bool {
        self.scheduler.request_shutdown()
    }

    fn shutdown(&self, timeout: Option<Duration>) {
        self.scheduler.shutdown(timeout);
    }

    fn stats(&self) -> MaintenanceExecutorStats {
        self.scheduler.stats()
    }
}

struct InlineExecutorInner {
    queue: BinaryHeap<TaskEnvelope>,
    shutdown: bool,
    sequence: u64,
    active_tasks: usize,
    max_queue_depth: usize,
    tasks_completed: u64,
    tasks_completed_after_shutdown: u64,
    worker_panics: u64,
    worker_panics_after_shutdown: u64,
}

/// Single-threaded executor that runs the production maintenance drive path
/// synchronously when asked to drain.
pub(crate) struct InlineMaintenanceExecutor {
    inner: ParkingMutex<InlineExecutorInner>,
    clock: Arc<dyn MaintenanceClock>,
}

impl std::fmt::Debug for InlineMaintenanceExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InlineMaintenanceExecutor")
            .field("stats", &self.stats())
            .finish()
    }
}

impl InlineMaintenanceExecutor {
    pub(crate) fn new(max_queue_depth: usize, clock: Arc<dyn MaintenanceClock>) -> Self {
        Self {
            inner: ParkingMutex::new(InlineExecutorInner {
                queue: BinaryHeap::new(),
                shutdown: false,
                sequence: 0,
                active_tasks: 0,
                max_queue_depth,
                tasks_completed: 0,
                tasks_completed_after_shutdown: 0,
                worker_panics: 0,
                worker_panics_after_shutdown: 0,
            }),
            clock,
        }
    }

    fn run_one(&self) -> bool {
        let task = {
            let mut inner = self.inner.lock();
            let Some(task) = inner.queue.pop() else {
                return false;
            };
            inner.active_tasks = inner.active_tasks.saturating_add(1);
            task
        };

        perf_trace::with_test_capture_enabled_for_current_thread(task.capture_enabled, || {
            let running_after_shutdown = self.inner.lock().shutdown;
            if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task.work)) {
                let mut inner = self.inner.lock();
                inner.worker_panics = inner.worker_panics.saturating_add(1);
                if running_after_shutdown {
                    inner.worker_panics_after_shutdown =
                        inner.worker_panics_after_shutdown.saturating_add(1);
                }
                drop(inner);
                perf_trace::record_lifecycle_background_worker_panic();
                tracing::error!(
                    "inline maintenance task panicked: {:?}",
                    error
                        .downcast_ref::<&str>()
                        .copied()
                        .unwrap_or("(non-string panic)")
                );
            }

            let mut inner = self.inner.lock();
            inner.active_tasks = inner.active_tasks.saturating_sub(1);
            inner.tasks_completed = inner.tasks_completed.saturating_add(1);
            if running_after_shutdown {
                inner.tasks_completed_after_shutdown =
                    inner.tasks_completed_after_shutdown.saturating_add(1);
                perf_trace::record_lifecycle_background_shutdown_executor_tasks_completed(1);
            }
        });
        true
    }

    fn idle(&self) -> bool {
        let inner = self.inner.lock();
        inner.queue.is_empty() && inner.active_tasks == 0
    }
}

impl MaintenanceExecutor for InlineMaintenanceExecutor {
    fn submit(
        &self,
        priority: BackgroundTaskPriority,
        work: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), BackgroundBackpressureError> {
        let mut inner = self.inner.lock();
        if inner.shutdown || inner.queue.len() >= inner.max_queue_depth {
            return Err(BackgroundBackpressureError);
        }
        let sequence = inner.sequence;
        inner.sequence = inner.sequence.saturating_add(1);
        inner.queue.push(TaskEnvelope {
            priority,
            sequence,
            work,
            capture_enabled: perf_trace::test_capture_enabled_for_current_thread(),
        });
        Ok(())
    }

    fn wait_for_idle(&self) {
        while self.run_one() {}
    }

    fn wait_for_progress(&self, completed_before_wait: u64, timeout: Duration) -> bool {
        if self.stats().tasks_completed > completed_before_wait || self.idle() {
            return true;
        }
        if timeout.is_zero() {
            return false;
        }
        let ran_task = self.run_one();
        let progressed = self.stats().tasks_completed > completed_before_wait || self.idle();
        if !progressed && !ran_task {
            self.clock.sleep(timeout);
        }
        progressed
    }

    fn request_shutdown(&self) -> bool {
        {
            let mut inner = self.inner.lock();
            if inner.shutdown {
                return false;
            }
            inner.shutdown = true;
        }
        perf_trace::record_lifecycle_background_shutdown();
        true
    }

    fn shutdown(&self, _timeout: Option<Duration>) {
        let first_shutdown = self.request_shutdown();
        self.wait_for_idle();
        if first_shutdown {
            perf_trace::record_lifecycle_background_shutdown_joined_workers(0);
        }
    }

    fn stats(&self) -> MaintenanceExecutorStats {
        let inner = self.inner.lock();
        MaintenanceExecutorStats {
            queue_depth: inner.queue.len(),
            active_tasks: inner.active_tasks,
            tasks_completed: inner.tasks_completed,
            tasks_completed_after_shutdown: inner.tasks_completed_after_shutdown,
            worker_panics: inner.worker_panics,
            worker_panics_after_shutdown: inner.worker_panics_after_shutdown,
            worker_count: 0,
        }
    }
}

struct ActiveTaskGuard<'a> {
    inner: &'a SchedulerInner,
}

impl Drop for ActiveTaskGuard<'_> {
    fn drop(&mut self) {
        self.inner.active_tasks.fetch_sub(1, AtomicOrdering::AcqRel);
        self.inner
            .tasks_completed
            .fetch_add(1, AtomicOrdering::Relaxed);
        if self.inner.shutdown.load(AtomicOrdering::Acquire) {
            self.inner
                .tasks_completed_after_shutdown
                .fetch_add(1, AtomicOrdering::Relaxed);
            perf_trace::record_lifecycle_background_shutdown_executor_tasks_completed(1);
        }

        let _queue = self.inner.queue.lock();
        self.inner.drain_cond.notify_all();
    }
}

fn worker_loop(inner: &SchedulerInner) {
    loop {
        let task = {
            let mut queue = inner.queue.lock();
            loop {
                if let Some(task) = queue.pop() {
                    inner.queue_depth.fetch_sub(1, AtomicOrdering::Release);
                    inner.active_tasks.fetch_add(1, AtomicOrdering::Release);
                    break task;
                }
                if inner.shutdown.load(AtomicOrdering::Acquire) {
                    return;
                }
                inner.work_ready.wait(&mut queue);
            }
        };

        perf_trace::with_test_capture_enabled_for_current_thread(task.capture_enabled, || {
            let _guard = ActiveTaskGuard { inner };
            if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task.work)) {
                record_worker_panic(inner);
                tracing::error!(
                    "background maintenance task panicked: {:?}",
                    error
                        .downcast_ref::<&str>()
                        .copied()
                        .unwrap_or("(non-string panic)")
                );
            }
        });
    }
}

fn drain_ready_tasks_on_current_thread(inner: &SchedulerInner) {
    loop {
        let task = {
            let mut queue = inner.queue.lock();
            queue.pop().inspect(|_| {
                inner.queue_depth.fetch_sub(1, AtomicOrdering::Release);
                inner.active_tasks.fetch_add(1, AtomicOrdering::Release);
            })
        };

        let Some(task) = task else {
            return;
        };

        perf_trace::with_test_capture_enabled_for_current_thread(task.capture_enabled, || {
            let _guard = ActiveTaskGuard { inner };
            if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task.work)) {
                record_worker_panic(inner);
                tracing::error!(
                    "background maintenance task panicked: {:?}",
                    error
                        .downcast_ref::<&str>()
                        .copied()
                        .unwrap_or("(non-string panic)")
                );
            }
        });
    }
}

fn record_worker_panic(inner: &SchedulerInner) {
    inner.worker_panics.fetch_add(1, AtomicOrdering::Relaxed);
    if inner.shutdown.load(AtomicOrdering::Acquire) {
        inner
            .worker_panics_after_shutdown
            .fetch_add(1, AtomicOrdering::Relaxed);
    }
    perf_trace::record_lifecycle_background_worker_panic();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier};

    #[test]
    fn background_submit_and_drain() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..10 {
            let counter = Arc::clone(&counter);
            scheduler
                .submit(BackgroundTaskPriority::Normal, move || {
                    counter.fetch_add(1, AtomicOrdering::Relaxed);
                })
                .expect("submit task");
        }

        scheduler.drain();
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 10);
        scheduler.shutdown(None);
    }

    #[test]
    fn background_priority_ordering() {
        let scheduler = BackgroundScheduler::new(1, 4096);
        let barrier = Arc::new(Barrier::new(2));
        let task_barrier = Arc::clone(&barrier);
        scheduler
            .submit(BackgroundTaskPriority::Low, move || {
                task_barrier.wait();
            })
            .expect("submit barrier");

        std::thread::sleep(std::time::Duration::from_millis(50));

        let order = Arc::new(ParkingMutex::new(Vec::new()));

        let observed = Arc::clone(&order);
        scheduler
            .submit(BackgroundTaskPriority::Low, move || {
                observed.lock().push("low");
            })
            .expect("submit low");

        let observed = Arc::clone(&order);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                observed.lock().push("normal");
            })
            .expect("submit normal");

        let observed = Arc::clone(&order);
        scheduler
            .submit(BackgroundTaskPriority::High, move || {
                observed.lock().push("high");
            })
            .expect("submit high");

        barrier.wait();
        scheduler.drain();

        assert_eq!(order.lock().clone(), vec!["high", "normal", "low"]);
        scheduler.shutdown(None);
    }

    #[test]
    fn background_fifo_within_same_priority() {
        let scheduler = BackgroundScheduler::new(1, 4096);
        let barrier = Arc::new(Barrier::new(2));
        let task_barrier = Arc::clone(&barrier);
        scheduler
            .submit(BackgroundTaskPriority::Low, move || {
                task_barrier.wait();
            })
            .expect("submit barrier");

        std::thread::sleep(std::time::Duration::from_millis(50));

        let order = Arc::new(ParkingMutex::new(Vec::new()));
        for value in 0..5 {
            let observed = Arc::clone(&order);
            scheduler
                .submit(BackgroundTaskPriority::Normal, move || {
                    observed.lock().push(value);
                })
                .expect("submit normal");
        }

        barrier.wait();
        scheduler.drain();

        assert_eq!(order.lock().clone(), vec![0, 1, 2, 3, 4]);
        scheduler.shutdown(None);
    }

    #[test]
    fn inline_executor_runs_priority_then_fifo_order() {
        let executor = InlineMaintenanceExecutor::new(
            4096,
            Arc::new(ManualMaintenanceClock::default()) as Arc<dyn MaintenanceClock>,
        );
        let order = Arc::new(ParkingMutex::new(Vec::new()));

        for (priority, value) in [
            (BackgroundTaskPriority::Low, "low-0"),
            (BackgroundTaskPriority::Normal, "normal-0"),
            (BackgroundTaskPriority::High, "high-0"),
            (BackgroundTaskPriority::High, "high-1"),
            (BackgroundTaskPriority::Normal, "normal-1"),
        ] {
            let observed = Arc::clone(&order);
            executor
                .submit(
                    priority,
                    Box::new(move || {
                        observed.lock().push(value);
                    }),
                )
                .expect("submit inline task");
        }

        executor.wait_for_idle();

        assert_eq!(
            order.lock().clone(),
            vec!["high-0", "high-1", "normal-0", "normal-1", "low-0"]
        );
        let stats = executor.stats();
        assert_eq!(stats.queue_depth, 0);
        assert_eq!(stats.active_tasks, 0);
        assert_eq!(stats.tasks_completed, 5);
        assert_eq!(stats.worker_count, 0);
    }

    #[test]
    fn inline_executor_wait_timeout_advances_manual_clock_without_progress() {
        let clock = Arc::new(ManualMaintenanceClock::default());
        let executor =
            InlineMaintenanceExecutor::new(4096, Arc::clone(&clock) as Arc<dyn MaintenanceClock>);
        {
            let mut inner = executor.inner.lock();
            inner.active_tasks = 1;
        }
        let start = clock.now();

        assert!(!executor.wait_for_progress(0, Duration::from_millis(25)));

        assert_eq!(
            clock.now().saturating_duration_since(start),
            Duration::from_millis(25)
        );
        let mut inner = executor.inner.lock();
        inner.active_tasks = 0;
    }

    #[test]
    fn manual_maintenance_clock_sleep_advances_simulated_time() {
        let clock = ManualMaintenanceClock::default();
        let start = clock.now();

        clock.sleep(Duration::from_millis(7));

        assert_eq!(
            clock.now().saturating_duration_since(start),
            Duration::from_millis(7)
        );
    }

    #[test]
    fn background_backpressure() {
        let scheduler = BackgroundScheduler::new(1, 2);
        let barrier = Arc::new(Barrier::new(2));
        let task_barrier = Arc::clone(&barrier);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                task_barrier.wait();
            })
            .expect("submit barrier");

        std::thread::sleep(std::time::Duration::from_millis(50));

        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..2 {
            let counter = Arc::clone(&counter);
            scheduler
                .submit(BackgroundTaskPriority::Normal, move || {
                    counter.fetch_add(1, AtomicOrdering::Relaxed);
                })
                .expect("submit queued task");
        }

        assert!(scheduler
            .submit(BackgroundTaskPriority::Normal, || {})
            .is_err());

        barrier.wait();
        scheduler.drain();
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 2);
        scheduler.shutdown(None);
    }

    #[test]
    fn background_concurrent_backpressure_does_not_exceed_queue_depth() {
        for _ in 0..25 {
            let scheduler = Arc::new(BackgroundScheduler::new(1, 1));
            let worker_barrier = Arc::new(Barrier::new(2));
            let task_barrier = Arc::clone(&worker_barrier);
            scheduler
                .submit(BackgroundTaskPriority::Normal, move || {
                    task_barrier.wait();
                })
                .expect("submit barrier");

            std::thread::sleep(std::time::Duration::from_millis(50));

            let submitter_count = 16;
            let start = Arc::new(Barrier::new(submitter_count));
            let accepted = Arc::new(AtomicUsize::new(0));
            let mut handles = Vec::new();
            for _ in 0..submitter_count {
                let scheduler = Arc::clone(&scheduler);
                let start = Arc::clone(&start);
                let accepted = Arc::clone(&accepted);
                handles.push(std::thread::spawn(move || {
                    start.wait();
                    if scheduler
                        .submit(BackgroundTaskPriority::Normal, || {})
                        .is_ok()
                    {
                        accepted.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                }));
            }

            for handle in handles {
                handle.join().expect("submitter joined");
            }

            assert!(
                accepted.load(AtomicOrdering::Relaxed) <= 1,
                "accepted more queued tasks than max queue depth"
            );
            worker_barrier.wait();
            scheduler.shutdown(None);
        }
    }

    #[test]
    #[should_panic(
        expected = "background maintenance scheduler requires at least one worker thread"
    )]
    fn background_zero_workers_rejected() {
        let _scheduler = BackgroundScheduler::new(0, 1);
    }

    #[test]
    fn background_shutdown_drains_remaining() {
        let scheduler = BackgroundScheduler::new(1, 4096);
        let barrier = Arc::new(Barrier::new(2));
        let task_barrier = Arc::clone(&barrier);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                task_barrier.wait();
            })
            .expect("submit barrier");

        std::thread::sleep(std::time::Duration::from_millis(50));

        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..5 {
            let counter = Arc::clone(&counter);
            scheduler
                .submit(BackgroundTaskPriority::Normal, move || {
                    counter.fetch_add(1, AtomicOrdering::Relaxed);
                })
                .expect("submit queued task");
        }

        barrier.wait();
        scheduler.shutdown(None);

        assert_eq!(counter.load(AtomicOrdering::Relaxed), 5);
    }

    #[test]
    fn background_drain_returns_when_idle() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        scheduler.drain();
        scheduler.shutdown(None);
    }

    #[test]
    fn background_stats() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..5 {
            let counter = Arc::clone(&counter);
            scheduler
                .submit(BackgroundTaskPriority::Normal, move || {
                    counter.fetch_add(1, AtomicOrdering::Relaxed);
                })
                .expect("submit task");
        }

        scheduler.drain();

        let stats = scheduler.stats();
        assert_eq!(stats.tasks_completed, 5);
        assert_eq!(stats.queue_depth, 0);
        assert_eq!(stats.active_tasks, 0);
        assert_eq!(stats.worker_count, 2);
        scheduler.shutdown(None);
    }

    #[test]
    fn background_wait_for_progress_observes_task_completion() {
        let scheduler = BackgroundScheduler::new(1, 4096);
        let completed_before_wait = scheduler.stats().tasks_completed;
        let counter = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&counter);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                observed.fetch_add(1, AtomicOrdering::Relaxed);
            })
            .expect("submit task");

        assert!(scheduler.wait_for_progress_until(
            completed_before_wait,
            Instant::now() + std::time::Duration::from_secs(5)
        ));
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 1);
        scheduler.shutdown(None);
    }

    #[test]
    fn background_wait_for_progress_times_out_without_progress() {
        let scheduler = BackgroundScheduler::new(1, 4096);
        let worker_started = Arc::new(Barrier::new(2));
        let worker_release = Arc::new(Barrier::new(2));
        let started = Arc::clone(&worker_started);
        let release = Arc::clone(&worker_release);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                started.wait();
                release.wait();
            })
            .expect("submit barrier task");
        worker_started.wait();

        let completed_before_wait = scheduler.stats().tasks_completed;
        assert!(!scheduler.wait_for_progress_until(
            completed_before_wait,
            Instant::now() + std::time::Duration::from_millis(25)
        ));

        worker_release.wait();
        scheduler.drain();
        scheduler.shutdown(None);
    }

    #[test]
    fn background_stats_report_active_and_queued_tasks_before_drain() {
        let scheduler = BackgroundScheduler::new(1, 4096);
        let worker_started = Arc::new(Barrier::new(2));
        let worker_release = Arc::new(Barrier::new(2));
        let started = Arc::clone(&worker_started);
        let release = Arc::clone(&worker_release);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                started.wait();
                release.wait();
            })
            .expect("submit barrier");

        worker_started.wait();
        let active_stats = scheduler.stats();
        assert_eq!(active_stats.active_tasks, 1);
        assert_eq!(active_stats.queue_depth, 0);

        scheduler
            .submit(BackgroundTaskPriority::Normal, || {})
            .expect("submit queued one");
        scheduler
            .submit(BackgroundTaskPriority::Normal, || {})
            .expect("submit queued two");

        let queued_stats = scheduler.stats();
        assert_eq!(queued_stats.active_tasks, 1);
        assert_eq!(queued_stats.queue_depth, 2);

        worker_release.wait();
        scheduler.drain();
        let drained_stats = scheduler.stats();
        assert_eq!(drained_stats.active_tasks, 0);
        assert_eq!(drained_stats.queue_depth, 0);
        assert_eq!(drained_stats.tasks_completed, 3);
        scheduler.shutdown(None);
    }

    #[test]
    fn background_default_worker_thread_uses_storage_next_prefix() {
        let scheduler = BackgroundScheduler::new(1, 4096);
        let (sender, receiver) = mpsc::channel();

        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                let name = std::thread::current().name().map(str::to_owned);
                sender.send(name).expect("send thread name");
            })
            .expect("submit thread-name task");
        scheduler.drain();

        let name = receiver
            .recv()
            .expect("receive thread name")
            .expect("worker thread is named");
        assert!(
            name.starts_with("strata-storage-maint-bg-"),
            "unexpected background worker thread name: {name}"
        );
        scheduler.shutdown(None);
    }

    #[test]
    fn background_submit_after_shutdown_rejected() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        scheduler.shutdown(None);

        let result = scheduler.submit(BackgroundTaskPriority::Normal, || {});
        assert!(result.is_err());
    }

    #[cfg(feature = "perf-trace")]
    #[test]
    fn background_submit_after_shutdown_records_counter() {
        let _capture = crate::observability::perf_trace::begin_test_capture();
        let scheduler = BackgroundScheduler::new(1, 4096);
        scheduler.shutdown(None);

        let result = scheduler.submit(BackgroundTaskPriority::Normal, || {});

        assert!(result.is_err());
        let perf = crate::observability::perf_trace::snapshot();
        assert_eq!(perf.lifecycle_background_shutdowns(), 1);
        assert_eq!(
            perf.lifecycle_background_submit_after_shutdown_rejected(),
            1
        );
    }

    #[cfg(feature = "perf-trace")]
    #[test]
    fn background_shutdown_records_joined_workers_and_drained_tasks() {
        let _capture = crate::observability::perf_trace::begin_test_capture();
        let scheduler = Arc::new(BackgroundScheduler::new(1, 4096));
        let first_started = Arc::new(Barrier::new(2));
        let first_release = Arc::new(Barrier::new(2));
        let completed = Arc::new(AtomicUsize::new(0));

        let started = Arc::clone(&first_started);
        let release = Arc::clone(&first_release);
        let observed = Arc::clone(&completed);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                started.wait();
                release.wait();
                observed.fetch_add(1, AtomicOrdering::Relaxed);
            })
            .expect("submit blocking task");
        first_started.wait();

        let observed = Arc::clone(&completed);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                observed.fetch_add(1, AtomicOrdering::Relaxed);
            })
            .expect("submit queued task");

        let closing = Arc::clone(&scheduler);
        let capture_enabled =
            crate::observability::perf_trace::test_capture_enabled_for_current_thread();
        let shutdown = std::thread::spawn(move || {
            crate::observability::perf_trace::with_test_capture_enabled_for_current_thread(
                capture_enabled,
                || closing.shutdown(None),
            );
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while crate::observability::perf_trace::snapshot().lifecycle_background_shutdowns() == 0 {
            assert!(
                Instant::now() < deadline,
                "shutdown did not start while first task was blocked"
            );
            std::thread::yield_now();
        }
        first_release.wait();
        shutdown.join().expect("shutdown joins cleanly");

        assert_eq!(completed.load(AtomicOrdering::Relaxed), 2);
        let perf = crate::observability::perf_trace::snapshot();
        assert_eq!(perf.lifecycle_background_shutdowns(), 1);
        assert_eq!(perf.lifecycle_background_shutdown_joined_workers(), 1);
        assert_eq!(perf.lifecycle_background_shutdown_drained_tasks(), 0);
        assert_eq!(
            scheduler.stats().tasks_completed_after_shutdown,
            2,
            "all accepted tasks drained by shutdown should be counted after shutdown"
        );
        assert_eq!(
            perf.lifecycle_background_shutdown_executor_tasks_completed(),
            2
        );
    }

    #[cfg(feature = "perf-trace")]
    #[test]
    fn background_shutdown_timeout_detaches_active_worker() {
        let _capture = crate::observability::perf_trace::begin_test_capture();
        let scheduler = BackgroundScheduler::new(1, 4096);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                started_tx.send(()).expect("signal task start");
                release_rx.recv().expect("wait for release");
            })
            .expect("submit blocking task");
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("task starts");

        scheduler.shutdown(Some(Duration::from_millis(1)));

        let perf = crate::observability::perf_trace::snapshot();
        assert_eq!(perf.lifecycle_background_shutdowns(), 1);
        assert_eq!(perf.lifecycle_background_shutdown_joined_workers(), 0);
        assert_eq!(perf.lifecycle_background_shutdown_detached_workers(), 1);
        assert_eq!(
            perf.lifecycle_background_shutdown_executor_tasks_completed(),
            0
        );

        release_tx.send(()).expect("release detached task");
        scheduler.drain();
    }

    #[test]
    fn background_task_panic_does_not_hang_drain() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        let counter = Arc::new(AtomicUsize::new(0));

        scheduler
            .submit(BackgroundTaskPriority::Normal, || {
                panic!("intentional background test panic");
            })
            .expect("submit panic task");

        for _ in 0..5 {
            let counter = Arc::clone(&counter);
            scheduler
                .submit(BackgroundTaskPriority::Normal, move || {
                    counter.fetch_add(1, AtomicOrdering::Relaxed);
                })
                .expect("submit normal task");
        }

        scheduler.drain();

        assert_eq!(counter.load(AtomicOrdering::Relaxed), 5);
        assert_eq!(scheduler.stats().tasks_completed, 6);
        scheduler.shutdown(None);
    }

    #[cfg(feature = "perf-trace")]
    #[test]
    fn background_task_panic_records_counter() {
        let _capture = crate::observability::perf_trace::begin_test_capture();
        let scheduler = BackgroundScheduler::new(1, 4096);

        scheduler
            .submit(BackgroundTaskPriority::Normal, || {
                panic!("intentional background test panic");
            })
            .expect("submit panic task");
        scheduler.drain();
        scheduler.shutdown(None);

        let perf = crate::observability::perf_trace::snapshot();
        assert_eq!(perf.lifecycle_background_worker_panics(), 1);
    }

    #[cfg(feature = "perf-trace")]
    #[test]
    fn background_task_panic_after_shutdown_is_distinct_in_stats() {
        let _capture = crate::observability::perf_trace::begin_test_capture();
        let scheduler = Arc::new(BackgroundScheduler::new(1, 4096));
        let started = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));

        let task_started = Arc::clone(&started);
        let task_release = Arc::clone(&release);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                task_started.wait();
                task_release.wait();
                panic!("intentional post-shutdown background test panic");
            })
            .expect("submit blocking panic task");
        started.wait();

        let closing = Arc::clone(&scheduler);
        let capture_enabled =
            crate::observability::perf_trace::test_capture_enabled_for_current_thread();
        let shutdown = std::thread::spawn(move || {
            crate::observability::perf_trace::with_test_capture_enabled_for_current_thread(
                capture_enabled,
                || closing.shutdown(None),
            );
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while crate::observability::perf_trace::snapshot().lifecycle_background_shutdowns() == 0 {
            assert!(
                Instant::now() < deadline,
                "shutdown did not start while panic task was blocked"
            );
            std::thread::yield_now();
        }
        release.wait();
        shutdown.join().expect("shutdown joins after caught panic");

        let stats = scheduler.stats();
        assert_eq!(stats.worker_panics, 1);
        assert_eq!(stats.worker_panics_after_shutdown, 1);
        let perf = crate::observability::perf_trace::snapshot();
        assert_eq!(perf.lifecycle_background_worker_panics(), 1);
        assert_eq!(
            perf.lifecycle_background_shutdown_executor_tasks_completed(),
            1
        );
    }

    #[test]
    fn background_concurrent_submits() {
        let scheduler = Arc::new(BackgroundScheduler::new(2, 4096));
        let counter = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let scheduler = Arc::clone(&scheduler);
            let counter = Arc::clone(&counter);
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let counter = Arc::clone(&counter);
                    scheduler
                        .submit(BackgroundTaskPriority::Normal, move || {
                            counter.fetch_add(1, AtomicOrdering::Relaxed);
                        })
                        .expect("submit concurrent task");
                }
            }));
        }

        for handle in handles {
            handle.join().expect("submitter joined");
        }

        scheduler.drain();
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 400);
        assert_eq!(scheduler.stats().tasks_completed, 400);
        scheduler.shutdown(None);
    }

    #[test]
    fn background_shutdown_is_idempotent() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        scheduler
            .submit(BackgroundTaskPriority::Normal, || {})
            .expect("submit task");
        scheduler.drain();

        scheduler.shutdown(None);
        scheduler.shutdown(None);
        scheduler.shutdown(None);
    }

    #[test]
    fn background_shutdown_from_worker_does_not_join_itself() {
        let scheduler = Arc::new(BackgroundScheduler::new(1, 4096));
        let task_started = Arc::new(Barrier::new(2));
        let release_task = Arc::new(Barrier::new(2));
        let (sender, receiver) = mpsc::channel();
        let trailing_counter = Arc::new(AtomicUsize::new(0));

        let task_scheduler = Arc::clone(&scheduler);
        let started = Arc::clone(&task_started);
        let release = Arc::clone(&release_task);
        let observed = Arc::clone(&trailing_counter);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                started.wait();
                release.wait();
                task_scheduler.shutdown(None);
                sender
                    .send(observed.load(AtomicOrdering::Relaxed))
                    .expect("send shutdown completion");
            })
            .expect("submit shutdown task");

        task_started.wait();
        for _ in 0..2 {
            let observed = Arc::clone(&trailing_counter);
            scheduler
                .submit(BackgroundTaskPriority::Normal, move || {
                    observed.fetch_add(1, AtomicOrdering::Relaxed);
                })
                .expect("submit trailing task");
        }
        release_task.wait();

        assert_eq!(
            receiver
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("worker shutdown task completed"),
            2
        );
        assert!(scheduler
            .submit(BackgroundTaskPriority::Normal, || {})
            .is_err());
        scheduler.shutdown(None);
    }

    #[test]
    fn background_shutdown_from_worker_contains_trailing_task_panic() {
        let scheduler = Arc::new(BackgroundScheduler::new(1, 4096));
        let task_started = Arc::new(Barrier::new(2));
        let release_task = Arc::new(Barrier::new(2));
        let (sender, receiver) = mpsc::channel();

        let task_scheduler = Arc::clone(&scheduler);
        let started = Arc::clone(&task_started);
        let release = Arc::clone(&release_task);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                started.wait();
                release.wait();
                task_scheduler.shutdown(None);
                sender.send(()).expect("send shutdown completion");
            })
            .expect("submit shutdown task");

        task_started.wait();
        scheduler
            .submit(BackgroundTaskPriority::Normal, || {
                panic!("intentional trailing shutdown panic");
            })
            .expect("submit trailing panic task");
        release_task.wait();

        receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("worker shutdown task completed");
        assert!(scheduler
            .submit(BackgroundTaskPriority::Normal, || {})
            .is_err());
        scheduler.shutdown(None);
    }

    #[test]
    fn background_submit_shutdown_toctou() {
        for _ in 0..50 {
            let scheduler = Arc::new(BackgroundScheduler::new(1, 4096));
            let executed = Arc::new(AtomicUsize::new(0));
            let submitted = Arc::new(AtomicUsize::new(0));
            let barrier = Arc::new(Barrier::new(2));

            let submit_scheduler = Arc::clone(&scheduler);
            let submit_executed = Arc::clone(&executed);
            let submit_count = Arc::clone(&submitted);
            let submit_barrier = Arc::clone(&barrier);
            let submit_thread = std::thread::spawn(move || {
                submit_barrier.wait();
                for _ in 0..100 {
                    let executed = Arc::clone(&submit_executed);
                    if submit_scheduler
                        .submit(BackgroundTaskPriority::Normal, move || {
                            executed.fetch_add(1, AtomicOrdering::Relaxed);
                        })
                        .is_ok()
                    {
                        submit_count.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                }
            });

            let shutdown_scheduler = Arc::clone(&scheduler);
            let shutdown_barrier = Arc::clone(&barrier);
            let shutdowner = std::thread::spawn(move || {
                shutdown_barrier.wait();
                shutdown_scheduler.shutdown(None);
            });

            submit_thread.join().expect("submitter joined");
            shutdowner.join().expect("shutdowner joined");

            let submitted = submitted.load(AtomicOrdering::Relaxed);
            let executed = executed.load(AtomicOrdering::Relaxed);
            assert_eq!(
                executed, submitted,
                "dropped accepted tasks: submitted={submitted}, executed={executed}",
            );
        }
    }

    #[test]
    fn background_drain_then_submit_then_drain() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        let counter = Arc::new(AtomicUsize::new(0));

        let submitted = Arc::clone(&counter);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                submitted.fetch_add(1, AtomicOrdering::Relaxed);
            })
            .expect("submit first task");
        scheduler.drain();
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 1);

        let submitted = Arc::clone(&counter);
        scheduler
            .submit(BackgroundTaskPriority::Normal, move || {
                submitted.fetch_add(1, AtomicOrdering::Relaxed);
            })
            .expect("submit second task");
        scheduler.drain();
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 2);

        scheduler.shutdown(None);
    }
}
