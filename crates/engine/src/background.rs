//! General-purpose background operations scheduler.
//!
//! Provides a priority-based task queue with configurable worker threads.
//! Designed for deferred work like embedding, GC, compaction, and index rebuilds.

use parking_lot::Mutex as ParkingMutex;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread::JoinHandle;
use tracing::error;

/// Priority levels for background work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    /// GC, compaction, index rebuilds
    Low = 0,
    /// Embedding batches, checkpointing
    Normal = 1,
    /// User-initiated flushes
    High = 2,
}

/// Error returned when the task queue is full.
#[derive(Debug)]
pub struct BackpressureError;

impl std::fmt::Display for BackpressureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "background scheduler queue is full")
    }
}

impl std::error::Error for BackpressureError {}

/// Scheduler metrics snapshot.
pub struct SchedulerStats {
    /// Number of tasks waiting in the queue.
    pub queue_depth: usize,
    /// Number of tasks currently being executed by workers.
    pub active_tasks: usize,
    /// Total number of tasks completed since scheduler creation.
    pub tasks_completed: u64,
    /// Number of worker threads.
    pub worker_count: usize,
}

struct TaskEnvelope {
    priority: TaskPriority,
    sequence: u64,
    work: Box<dyn FnOnce() + Send>,
}

impl Eq for TaskEnvelope {}

impl PartialEq for TaskEnvelope {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

// Higher priority first, then lower sequence (older) first
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

struct SchedulerInner {
    queue: ParkingMutex<BinaryHeap<TaskEnvelope>>,
    work_ready: parking_lot::Condvar,
    drain_cond: parking_lot::Condvar,
    shutdown: AtomicBool,
    sequence: AtomicU64,
    queue_depth: AtomicUsize,
    active_tasks: AtomicUsize,
    max_queue_depth: usize,
    tasks_completed: AtomicU64,
}

/// A general-purpose background task scheduler with priority ordering.
///
/// Tasks are executed by a fixed pool of worker threads. Higher-priority tasks
/// run first; within the same priority, tasks are executed in FIFO order.
pub struct BackgroundScheduler {
    inner: Arc<SchedulerInner>,
    workers: ParkingMutex<Vec<JoinHandle<()>>>,
    num_threads: usize,
}

impl BackgroundScheduler {
    /// Create a new scheduler with the given number of worker threads.
    ///
    /// Workers are named `strata-bg-0`, `strata-bg-1`, etc.
    pub fn new(num_threads: usize, max_queue_depth: usize) -> Self {
        let inner = Arc::new(SchedulerInner {
            queue: ParkingMutex::new(BinaryHeap::new()),
            work_ready: parking_lot::Condvar::new(),
            drain_cond: parking_lot::Condvar::new(),
            shutdown: AtomicBool::new(false),
            sequence: AtomicU64::new(0),
            queue_depth: AtomicUsize::new(0),
            active_tasks: AtomicUsize::new(0),
            max_queue_depth,
            tasks_completed: AtomicU64::new(0),
        });

        let mut workers = Vec::with_capacity(num_threads);
        for i in 0..num_threads {
            let inner_clone = Arc::clone(&inner);
            let handle = std::thread::Builder::new()
                .name(format!("strata-bg-{}", i))
                .spawn(move || worker_loop(&inner_clone))
                .expect("failed to spawn background worker thread");
            workers.push(handle);
        }

        Self {
            inner,
            workers: ParkingMutex::new(workers),
            num_threads,
        }
    }

    /// Submit a task to the background scheduler.
    ///
    /// Returns `Err(BackpressureError)` if the queue is at capacity or
    /// the scheduler has been shut down.
    pub fn submit(
        &self,
        priority: TaskPriority,
        work: impl FnOnce() + Send + 'static,
    ) -> Result<(), BackpressureError> {
        // Reject after shutdown — workers have been joined, task would never run
        if self.inner.shutdown.load(AtomicOrdering::Acquire) {
            return Err(BackpressureError);
        }

        // Check backpressure before acquiring the lock
        if self.inner.queue_depth.load(AtomicOrdering::Acquire) >= self.inner.max_queue_depth {
            return Err(BackpressureError);
        }

        let sequence = self.inner.sequence.fetch_add(1, AtomicOrdering::Relaxed);
        let envelope = TaskEnvelope {
            priority,
            sequence,
            work: Box::new(work),
        };

        {
            let mut queue = self.inner.queue.lock();
            queue.push(envelope);
            self.inner.queue_depth.fetch_add(1, AtomicOrdering::Release);
        }

        self.inner.work_ready.notify_one();
        Ok(())
    }

    /// Block until all queued and in-flight tasks have completed.
    ///
    /// Workers remain running after drain completes — this does NOT signal shutdown.
    pub fn drain(&self) {
        let mut queue = self.inner.queue.lock();
        while self.inner.queue_depth.load(AtomicOrdering::Acquire) > 0
            || self.inner.active_tasks.load(AtomicOrdering::Acquire) > 0
        {
            self.inner.drain_cond.wait(&mut queue);
        }
    }

    /// Shut down the scheduler: signal workers to exit and join all threads.
    ///
    /// Workers drain remaining tasks before exiting.
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, AtomicOrdering::Release);

        // Lock the queue before notifying to prevent lost-wakeup:
        // a worker between its shutdown check and condvar wait holds this lock,
        // so acquiring it guarantees the worker is either already in wait()
        // (and our notify will wake it) or hasn't checked shutdown yet
        // (and will see it's true when it does).
        {
            let _queue = self.inner.queue.lock();
            self.inner.work_ready.notify_all();
        }

        let mut workers = self.workers.lock();
        for handle in workers.drain(..) {
            let _ = handle.join();
        }
    }

    /// Return a snapshot of scheduler metrics.
    pub fn stats(&self) -> SchedulerStats {
        SchedulerStats {
            queue_depth: self.inner.queue_depth.load(AtomicOrdering::Relaxed),
            active_tasks: self.inner.active_tasks.load(AtomicOrdering::Relaxed),
            tasks_completed: self.inner.tasks_completed.load(AtomicOrdering::Relaxed),
            worker_count: self.num_threads,
        }
    }
}

/// RAII guard that decrements `active_tasks` and notifies drain waiters on drop.
///
/// This ensures correct bookkeeping even if a task panics — without this guard,
/// a panic would leave `active_tasks` permanently inflated, causing `drain()` to
/// hang forever.
struct ActiveTaskGuard<'a> {
    inner: &'a SchedulerInner,
}

impl<'a> Drop for ActiveTaskGuard<'a> {
    fn drop(&mut self) {
        let prev_active = self
            .inner
            .active_tasks
            .fetch_sub(1, AtomicOrdering::AcqRel);
        self.inner
            .tasks_completed
            .fetch_add(1, AtomicOrdering::Relaxed);

        // If we just became idle and queue is empty, notify drain waiters.
        // Lock the queue before notifying to prevent lost-wakeup: drain()
        // holds this lock while checking the condition and calling wait(),
        // so acquiring it ensures drain is either already in wait() (and
        // our notify will wake it) or will re-check the condition after
        // acquiring the lock.
        if prev_active == 1 && self.inner.queue_depth.load(AtomicOrdering::Acquire) == 0 {
            let _queue = self.inner.queue.lock();
            self.inner.drain_cond.notify_all();
        }
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

        // Guard ensures active_tasks is decremented even if the task panics
        let _guard = ActiveTaskGuard { inner };

        // Execute outside lock. catch_unwind prevents a panicking task from
        // killing the worker thread — the guard handles bookkeeping either way.
        if let Err(e) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task.work)) {
            error!(
                "background task panicked: {:?}",
                e.downcast_ref::<&str>()
                    .copied()
                    .unwrap_or("(non-string panic)")
            );
        }

        // _guard drops here → decrements active_tasks, notifies drain waiters
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Arc, Barrier};

    #[test]
    fn test_submit_and_drain() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..10 {
            let c = Arc::clone(&counter);
            scheduler
                .submit(TaskPriority::Normal, move || {
                    c.fetch_add(1, AtomicOrdering::Relaxed);
                })
                .unwrap();
        }

        scheduler.drain();
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 10);
        scheduler.shutdown();
    }

    #[test]
    fn test_priority_ordering() {
        // Use 1 thread so tasks execute sequentially after we release them
        let scheduler = BackgroundScheduler::new(1, 4096);

        // Block the single worker so we can queue tasks
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        scheduler
            .submit(TaskPriority::Low, move || {
                b.wait();
            })
            .unwrap();

        // Wait until the worker picks up the barrier task
        std::thread::sleep(std::time::Duration::from_millis(50));

        let order = Arc::new(ParkingMutex::new(Vec::new()));

        // Submit Low, Normal, High — should run High, Normal, Low
        let o = Arc::clone(&order);
        scheduler
            .submit(TaskPriority::Low, move || {
                o.lock().push("low");
            })
            .unwrap();

        let o = Arc::clone(&order);
        scheduler
            .submit(TaskPriority::Normal, move || {
                o.lock().push("normal");
            })
            .unwrap();

        let o = Arc::clone(&order);
        scheduler
            .submit(TaskPriority::High, move || {
                o.lock().push("high");
            })
            .unwrap();

        // Release the barrier task so the worker can process queued tasks
        barrier.wait();
        scheduler.drain();

        let result = order.lock().clone();
        assert_eq!(result, vec!["high", "normal", "low"]);
        scheduler.shutdown();
    }

    #[test]
    fn test_fifo_within_same_priority() {
        // Verify that tasks with the same priority run in submission order
        let scheduler = BackgroundScheduler::new(1, 4096);

        // Block the single worker
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        scheduler
            .submit(TaskPriority::Low, move || {
                b.wait();
            })
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));

        let order = Arc::new(ParkingMutex::new(Vec::new()));

        for i in 0..5 {
            let o = Arc::clone(&order);
            scheduler
                .submit(TaskPriority::Normal, move || {
                    o.lock().push(i);
                })
                .unwrap();
        }

        barrier.wait();
        scheduler.drain();

        let result = order.lock().clone();
        assert_eq!(result, vec![0, 1, 2, 3, 4]);
        scheduler.shutdown();
    }

    #[test]
    fn test_backpressure() {
        let scheduler = BackgroundScheduler::new(1, 2);

        // Block the worker so submitted tasks stay in queue
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        scheduler
            .submit(TaskPriority::Normal, move || {
                b.wait();
            })
            .unwrap();

        // Wait for the worker to pick up the barrier task (removing it from queue)
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Now queue 2 tasks to fill the queue
        let counter = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&counter);
        scheduler
            .submit(TaskPriority::Normal, move || {
                c.fetch_add(1, AtomicOrdering::Relaxed);
            })
            .unwrap();

        let c = Arc::clone(&counter);
        scheduler
            .submit(TaskPriority::Normal, move || {
                c.fetch_add(1, AtomicOrdering::Relaxed);
            })
            .unwrap();

        // Third submit should fail — queue is full
        let result = scheduler.submit(TaskPriority::Normal, || {});
        assert!(result.is_err());

        // Release barrier, drain, verify the 2 queued tasks ran
        barrier.wait();
        scheduler.drain();
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 2);
        scheduler.shutdown();
    }

    #[test]
    fn test_shutdown_drains_remaining() {
        let scheduler = BackgroundScheduler::new(1, 4096);

        // Block the worker
        let barrier = Arc::new(Barrier::new(2));
        let b = Arc::clone(&barrier);
        scheduler
            .submit(TaskPriority::Normal, move || {
                b.wait();
            })
            .unwrap();

        // Wait for worker to pick up the barrier task
        std::thread::sleep(std::time::Duration::from_millis(50));

        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..5 {
            let c = Arc::clone(&counter);
            scheduler
                .submit(TaskPriority::Normal, move || {
                    c.fetch_add(1, AtomicOrdering::Relaxed);
                })
                .unwrap();
        }

        // Release the barrier and immediately shutdown
        barrier.wait();
        scheduler.shutdown();

        // All 5 tasks should have run before shutdown completed
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 5);
    }

    #[test]
    fn test_drain_returns_when_idle() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        // No tasks submitted — drain should return immediately
        scheduler.drain();
        scheduler.shutdown();
    }

    #[test]
    fn test_stats() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..5 {
            let c = Arc::clone(&counter);
            scheduler
                .submit(TaskPriority::Normal, move || {
                    c.fetch_add(1, AtomicOrdering::Relaxed);
                })
                .unwrap();
        }

        scheduler.drain();

        let stats = scheduler.stats();
        assert_eq!(stats.tasks_completed, 5);
        assert_eq!(stats.queue_depth, 0);
        assert_eq!(stats.active_tasks, 0);
        assert_eq!(stats.worker_count, 2);
        scheduler.shutdown();
    }

    #[test]
    fn test_submit_after_shutdown_rejected() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        scheduler.shutdown();

        let result = scheduler.submit(TaskPriority::Normal, || {});
        assert!(result.is_err());
    }

    #[test]
    fn test_task_panic_does_not_hang_drain() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        let counter = Arc::new(AtomicUsize::new(0));

        // Submit a task that panics
        scheduler
            .submit(TaskPriority::Normal, || {
                panic!("intentional test panic");
            })
            .unwrap();

        // Submit normal tasks after the panicking one
        for _ in 0..5 {
            let c = Arc::clone(&counter);
            scheduler
                .submit(TaskPriority::Normal, move || {
                    c.fetch_add(1, AtomicOrdering::Relaxed);
                })
                .unwrap();
        }

        // drain() must not hang — the panicking task's active_tasks must be decremented
        scheduler.drain();

        // All non-panicking tasks should still complete
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 5);

        // Stats should count the panicked task as completed too
        let stats = scheduler.stats();
        assert_eq!(stats.tasks_completed, 6);
        scheduler.shutdown();
    }

    #[test]
    fn test_concurrent_submits() {
        let scheduler = Arc::new(BackgroundScheduler::new(2, 4096));
        let counter = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let s = Arc::clone(&scheduler);
            let c = Arc::clone(&counter);
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let c = Arc::clone(&c);
                    s.submit(TaskPriority::Normal, move || {
                        c.fetch_add(1, AtomicOrdering::Relaxed);
                    })
                    .unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        scheduler.drain();
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 400);

        let stats = scheduler.stats();
        assert_eq!(stats.tasks_completed, 400);
        scheduler.shutdown();
    }

    #[test]
    fn test_shutdown_is_idempotent() {
        let scheduler = BackgroundScheduler::new(2, 4096);
        scheduler.submit(TaskPriority::Normal, || {}).unwrap();
        scheduler.drain();

        // Multiple shutdowns should not panic or deadlock
        scheduler.shutdown();
        scheduler.shutdown();
        scheduler.shutdown();
    }

    #[test]
    fn test_drain_then_submit_then_drain() {
        // Verify scheduler is still usable after drain()
        let scheduler = BackgroundScheduler::new(2, 4096);
        let counter = Arc::new(AtomicUsize::new(0));

        let c = Arc::clone(&counter);
        scheduler
            .submit(TaskPriority::Normal, move || {
                c.fetch_add(1, AtomicOrdering::Relaxed);
            })
            .unwrap();
        scheduler.drain();
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 1);

        // Submit more work after drain — workers should still be alive
        let c = Arc::clone(&counter);
        scheduler
            .submit(TaskPriority::Normal, move || {
                c.fetch_add(1, AtomicOrdering::Relaxed);
            })
            .unwrap();
        scheduler.drain();
        assert_eq!(counter.load(AtomicOrdering::Relaxed), 2);

        scheduler.shutdown();
    }
}
