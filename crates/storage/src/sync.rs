//! Swappable synchronization seam for concurrency-schedule exploration
//! (TCP4.3).
//!
//! Modules whose interleavings are model-checked import their primitives
//! from here instead of `std::sync`. A normal build re-exports std — the
//! seam is zero-cost. Under `RUSTFLAGS="--cfg loom"` the same names
//! resolve to [loom](https://docs.rs/loom) primitives, so the in-crate
//! `#[cfg(all(loom, test))]` models explore every schedule of the real
//! protocol code, not a hand-copied abstraction.
//!
//! Two deliberate semantic notes for the loom side:
//!
//! - loom's `Condvar::wait_timeout` never times out (upstream TODO): timed
//!   fallback branches are unreachable under exploration. That is the
//!   correct posture for verification — a schedule that NEEDS a timeout to
//!   make progress is a lost-wakeup bug, and loom reports it as a detected
//!   deadlock instead of letting the fallback mask it.
//! - [`beat_pause`] (a pure batching heuristic in the product) becomes a
//!   scheduler yield: wall-clock pauses are schedule-irrelevant under
//!   model checking.

#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(loom)]
pub(crate) use loom::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::time::Duration;

/// Published-snapshot swap cell (TCP4.3b): [`arc_swap::ArcSwap`] in
/// production (single-atomic lock-free load on the off-lock read path); a
/// loom `RwLock<Arc<T>>` under exploration. The substitution is
/// ordering-equivalent — both are a Release store / Acquire load on one
/// location — and trades only `ArcSwap`'s lock-freedom, a liveness property
/// loom's deadlock detector covers trivially.
#[derive(Debug)]
pub(crate) struct SwapCell<T> {
    #[cfg(not(loom))]
    inner: arc_swap::ArcSwap<T>,
    #[cfg(loom)]
    inner: loom::sync::RwLock<std::sync::Arc<T>>,
}

impl<T> SwapCell<T> {
    pub(crate) fn new(value: std::sync::Arc<T>) -> Self {
        #[cfg(not(loom))]
        {
            Self {
                inner: arc_swap::ArcSwap::from(value),
            }
        }
        #[cfg(loom)]
        {
            Self {
                inner: loom::sync::RwLock::new(value),
            }
        }
    }

    /// Publish a new value (release ordering); old values stay valid for
    /// readers still holding them and die by `Arc` drop.
    pub(crate) fn store(&self, value: std::sync::Arc<T>) {
        #[cfg(not(loom))]
        self.inner.store(value);
        #[cfg(loom)]
        {
            *self.inner.write().expect("swap cell write lock poisoned") = value;
        }
    }

    /// Load the current value with one atomic refcount bump.
    pub(crate) fn load_full(&self) -> std::sync::Arc<T> {
        #[cfg(not(loom))]
        {
            self.inner.load_full()
        }
        #[cfg(loom)]
        {
            std::sync::Arc::clone(&self.inner.read().expect("swap cell read lock poisoned"))
        }
    }
}

/// Batching-beat pause: real sleep in production, scheduler yield under
/// loom (time is not modeled; the beat is a throughput heuristic with no
/// correctness role).
pub(crate) fn beat_pause(duration: Duration) {
    #[cfg(not(loom))]
    std::thread::sleep(duration);
    #[cfg(loom)]
    {
        let _ = duration;
        loom::thread::yield_now();
    }
}

/// Bounded deadline for lost-wakeup safety nets. Real clock in production;
/// under loom it never expires — a schedule that cannot finish without the
/// deadline is a liveness bug loom must surface as a deadlock, not one the
/// net may absorb.
#[derive(Debug)]
pub(crate) struct Deadline {
    #[cfg(not(loom))]
    at: std::time::Instant,
}

impl Deadline {
    pub(crate) fn after(duration: Duration) -> Self {
        #[cfg(not(loom))]
        {
            Self {
                at: std::time::Instant::now() + duration,
            }
        }
        #[cfg(loom)]
        {
            let _ = duration;
            Self {}
        }
    }

    #[cfg_attr(
        loom,
        allow(clippy::unused_self, reason = "loom has no clock to consult")
    )]
    pub(crate) fn expired(&self) -> bool {
        #[cfg(not(loom))]
        {
            std::time::Instant::now() >= self.at
        }
        #[cfg(loom)]
        false
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::Deadline;
    use std::time::Duration;

    #[test]
    fn deadline_expiry_truth_table() {
        assert!(Deadline::after(Duration::ZERO).expired());
        assert!(!Deadline::after(Duration::from_secs(3600)).expired());
    }
}
