//! One writer per branch (#3782, `write-isolation-contract.md`).
//!
//! The capability services borrow `&self`, so threads sharing one `Database`
//! run operations at once. Strata's model is that reads run concurrently and
//! writes are exclusive per branch. A write operation holds its branch's
//! [`BranchWriter`] from its first read to its last commit, so nothing else
//! commits to the branch in between. A read takes no writer and answers from
//! one snapshot.
//!
//! Writers on different branches never wait for each other. The writer is
//! per-database, in-process state, and that is enough: the storage writer
//! lock guarantees one engine process per store.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, ThreadId};

use strata_core::BranchId;

/// The writer locks of a database's branches, created on first use.
#[derive(Default)]
pub(crate) struct BranchWriters {
    locks: Mutex<HashMap<BranchId, Arc<BranchLock>>>,
}

/// One branch's writer lock: the thread that holds it, if any.
#[derive(Default)]
struct BranchLock {
    holder: Mutex<Option<ThreadId>>,
    released: Condvar,
}

/// The right to write one branch, held from a write operation's first read
/// to its last commit. Committing requires one (`StoragePersistence::commit`),
/// so a write cannot reach storage without holding its branch. Dropping it
/// lets the next writer of the branch in.
///
/// It is neither `Send` nor `Sync`: a writer belongs to the thread that took
/// it, which is how re-entry is told apart from contention.
pub(crate) struct BranchWriter {
    branch_id: BranchId,
    lock: Arc<BranchLock>,
    _thread_bound: PhantomData<*const ()>,
}

impl BranchWriter {
    /// The branch this writer holds.
    pub(crate) const fn branch_id(&self) -> BranchId {
        self.branch_id
    }
}

impl Drop for BranchWriter {
    fn drop(&mut self) {
        *lock_holder(&self.lock.holder) = None;
        self.lock.released.notify_one();
    }
}

impl BranchWriters {
    /// Waits until no other thread holds `branch_id`'s writer, then holds it.
    /// `on_wait` runs once if the writer was held when asked for, so a test
    /// can tell a queued write from one still running.
    ///
    /// # Panics
    ///
    /// If the calling thread already holds this branch's writer. A write
    /// operation takes its branch once, at its boundary, and hands the writer
    /// down; taking it again would wait for itself forever.
    pub(crate) fn acquire(&self, branch_id: BranchId, on_wait: impl FnOnce()) -> BranchWriter {
        let lock = Arc::clone(lock_map(&self.locks).entry(branch_id).or_default());
        let me = thread::current().id();
        let mut holder = lock_holder(&lock.holder);
        assert!(
            *holder != Some(me),
            "a write re-entered branch {branch_id}'s writer it already holds"
        );
        if holder.is_some() {
            on_wait();
        }
        while holder.is_some() {
            holder = lock
                .released
                .wait(holder)
                .unwrap_or_else(PoisonError::into_inner);
        }
        *holder = Some(me);
        drop(holder);
        BranchWriter {
            branch_id,
            lock,
            _thread_bound: PhantomData,
        }
    }

    /// Whether some thread holds `branch_id`'s writer right now.
    #[cfg(test)]
    pub(crate) fn is_held(&self, branch_id: BranchId) -> bool {
        lock_map(&self.locks)
            .get(&branch_id)
            .is_some_and(|lock| lock_holder(&lock.holder).is_some())
    }
}

/// The lock map guards no invariant a panic could break, so a poisoned lock
/// is recovered rather than propagated.
fn lock_map(
    locks: &Mutex<HashMap<BranchId, Arc<BranchLock>>>,
) -> MutexGuard<'_, HashMap<BranchId, Arc<BranchLock>>> {
    locks.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A holder slot is only ever `Some(thread)` or `None`, both valid, so a
/// poisoned lock is recovered rather than propagated.
fn lock_holder(holder: &Mutex<Option<ThreadId>>) -> MutexGuard<'_, Option<ThreadId>> {
    holder.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use strata_core::BranchId;

    use super::BranchWriters;

    fn branch(byte: u8) -> BranchId {
        BranchId::from_bytes([byte; 16])
    }

    #[test]
    fn a_branch_has_one_writer_at_a_time_and_branches_do_not_wait_for_each_other() {
        let writers = BranchWriters::default();
        let held = writers.acquire(branch(1), || panic!("an unheld branch does not wait"));
        assert!(writers.is_held(branch(1)));
        // Another branch is free while the first is held.
        drop(writers.acquire(branch(2), || panic!("another branch does not wait")));
        assert!(
            !writers.is_held(branch(2)),
            "a writer lets its branch go when it is dropped"
        );

        let (waited_tx, waited_rx) = mpsc::channel();
        let (acquired_tx, acquired_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let second = scope.spawn(|| {
                let writer = writers.acquire(branch(1), move || {
                    waited_tx.send(()).expect("the test is listening");
                });
                acquired_tx.send(()).expect("the test is listening");
                writer.branch_id()
            });
            waited_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("a second writer of a held branch waits");
            // Correct code never hands the writer over while it is held, so
            // this wait cannot fail spuriously; it only bounds how long a
            // broken lock gets to show itself.
            assert!(
                acquired_rx
                    .recv_timeout(Duration::from_millis(200))
                    .is_err(),
                "the second writer does not get the branch while the first holds it"
            );
            drop(held);
            acquired_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("the second writer gets the branch once the first lets go");
            assert_eq!(second.join().expect("the second writer"), branch(1));
        });
        assert!(
            !writers.is_held(branch(1)),
            "the writer is released on drop"
        );
    }

    #[test]
    #[should_panic(expected = "re-entered")]
    fn a_write_that_takes_its_branch_twice_panics_instead_of_waiting_for_itself() {
        let writers = BranchWriters::default();
        let _held = writers.acquire(branch(1), || {});
        let _again = writers.acquire(branch(1), || {});
    }
}
