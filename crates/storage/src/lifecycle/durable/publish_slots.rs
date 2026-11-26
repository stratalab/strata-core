//! Per-branch table-manifest publish slots, with a release signal (#3747).
//!
//! A background publish claims its branch's slot under the runtime lock,
//! persists the manifest with the lock released, and gives the slot back only
//! after recording the result under the lock again (`finish_publish_phase`).
//! Every other holder claims and releases its slot within one lock hold. So
//! every claim happens under the runtime lock, and a slot seen held from
//! outside it is always an off-lock publish in flight.
//!
//! The release signal is for the close. The close-time background shutdown
//! detaches workers that may still be persisting a manifest, or still building
//! one and about to claim a slot. If the close held the runtime lock while a
//! publish was in flight, the worker could not record its manifest, and the
//! close checkpoint would copy that branch's rows as volatile (DUR-010,
//! DUR-019). [`BranchPublishSlots::lock_once_released`] takes the runtime lock
//! only at a moment no slot is held; since a claim needs that lock, none starts
//! while the close holds it. Each release wakes the wait (DUR-006); its
//! deadline is only a safety net.

use crate::sync::{Arc, Condvar, Deadline, Mutex};
use std::collections::HashSet;
use std::sync::PoisonError;
use std::time::Duration;
use strata_core::BranchId;

/// How long one condvar wait lasts before the deadline is checked again. Only
/// the deadline needs it: each release wakes the wait at once.
const WAIT_SLICE: Duration = Duration::from_millis(20);

/// The branches whose publish slot is held. Shared between the runtime, which
/// claims slots under its lock, and the close, which waits for them outside it.
#[derive(Debug)]
pub(crate) struct BranchPublishSlots {
    state: Mutex<SlotState>,
    released: Condvar,
}

#[derive(Debug, Default)]
struct SlotState {
    held: HashSet<BranchId>,
    /// Slots released while their holder was unwinding from a panic.
    released_by_panic: u64,
}

/// How [`BranchPublishSlots::lock_once_released`] ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublishSlotsWait {
    /// No slot is held.
    Released,
    /// The deadline passed with a slot still held.
    TimedOut,
    /// A publish released its slot by panicking after the caller's mark.
    Panicked,
}

/// How many publishes had released their slot by panicking when it was taken.
/// The close takes one before its background shutdown, so any later panic of
/// a slot holder fails the close.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PublishPanicMark(u64);

/// A claim on one branch's publish slot. Dropping it releases the slot and
/// wakes a waiting close, including on early return or panic.
pub(crate) struct BranchPublishGuard {
    slots: Arc<BranchPublishSlots>,
    branch_id: BranchId,
}

impl BranchPublishSlots {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(SlotState::default()),
            released: Condvar::new(),
        }
    }

    /// Claim `branch_id`'s slot, or `None` when another publish for the branch
    /// holds it. Never waits: callers hold the runtime lock, which must never
    /// block on a slot.
    pub(crate) fn try_acquire(
        slots: &Arc<Self>,
        branch_id: BranchId,
    ) -> Option<BranchPublishGuard> {
        let mut state = slots.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.held.insert(branch_id).then(|| BranchPublishGuard {
            slots: Arc::clone(slots),
            branch_id,
        })
    }

    /// The panics so far, for a later [`Self::lock_once_released`].
    pub(crate) fn panic_mark(&self) -> PublishPanicMark {
        PublishPanicMark(
            self.state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .released_by_panic,
        )
    }

    /// Take the runtime lock through `lock` at a moment no slot is held, or
    /// once `deadline` has passed or a slot holder has panicked since `since`.
    /// A claim needs the runtime lock, so slots found free under it stay free
    /// while the caller holds the returned guard.
    pub(crate) fn lock_once_released<G>(
        &self,
        since: PublishPanicMark,
        deadline: Duration,
        mut lock: impl FnMut() -> G,
    ) -> (G, PublishSlotsWait) {
        let deadline = Deadline::after(deadline);
        let PublishPanicMark(panics_before) = since;
        loop {
            let guard = lock();
            let outcome = self
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .outcome(panics_before, &deadline);
            if let Some(outcome) = outcome {
                return (guard, outcome);
            }
            drop(guard);
            self.wait_for_outcome(panics_before, &deadline);
        }
    }

    /// Block, with the runtime lock released, until the slots have an outcome;
    /// each release wakes the wait.
    fn wait_for_outcome(&self, panics_before: u64, deadline: &Deadline) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while state.outcome(panics_before, deadline).is_none() {
            state = match self.released.wait_timeout(state, WAIT_SLICE) {
                Ok((state, _)) => state,
                Err(poisoned) => poisoned.into_inner().0,
            };
        }
    }
}

impl SlotState {
    fn outcome(&self, panics_before: u64, deadline: &Deadline) -> Option<PublishSlotsWait> {
        close_wait_outcome(
            self.released_by_panic != panics_before,
            !self.held.is_empty(),
            deadline.expired(),
        )
    }
}

/// When the close stops waiting for the slots: on a publish's panic first, then
/// once no slot is held, then at the deadline. `None` keeps it waiting.
const fn close_wait_outcome(
    panicked: bool,
    any_held: bool,
    expired: bool,
) -> Option<PublishSlotsWait> {
    if panicked {
        Some(PublishSlotsWait::Panicked)
    } else if !any_held {
        Some(PublishSlotsWait::Released)
    } else if expired {
        Some(PublishSlotsWait::TimedOut)
    } else {
        None
    }
}

impl Drop for BranchPublishGuard {
    fn drop(&mut self) {
        let mut state = self
            .slots
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.held.remove(&self.branch_id);
        if std::thread::panicking() {
            state.released_by_panic = state.released_by_panic.saturating_add(1);
        }
        drop(state);
        self.slots.released.notify_all();
    }
}

/// DUR-006: the close's wait must finish on the slots' releases alone. Under
/// loom the deadline never expires and `wait_timeout` never times out, so a
/// lost wake-up is reported as a deadlock. Only compiled with
/// `RUSTFLAGS="--cfg loom"`.
#[cfg(all(loom, test))]
mod publish_slots_loom {
    use super::{BranchPublishSlots, PublishSlotsWait};
    use loom::sync::{Arc, Mutex};
    use std::time::Duration;
    use strata_core::BranchId;

    fn branch(byte: u8) -> BranchId {
        BranchId::from_bytes([byte; 16])
    }

    /// The close (the model's main thread) against two detached workers. The
    /// first is persisting branch 1's manifest with its slot held. The second
    /// is still building branch 2's: it claims its slot under the runtime
    /// lock, persists with the lock released, then records under it. Each
    /// worker records and releases its slot under the runtime lock, as
    /// `finish_publish_phase` does. In every schedule the close must hold the
    /// runtime lock only while no slot is held, and find branch 1 recorded.
    #[test]
    fn publish_slots_loom_close_locks_only_while_no_publish_is_in_flight() {
        loom::model(|| {
            let slots = Arc::new(BranchPublishSlots::new());
            let runtime = Arc::new(Mutex::new(Vec::new()));
            let persisting = BranchPublishSlots::try_acquire(&slots, branch(1)).expect("free slot");
            let first = {
                let runtime = Arc::clone(&runtime);
                loom::thread::spawn(move || {
                    let mut recorded = runtime.lock().expect("runtime lock");
                    recorded.push(1_u8);
                    drop(persisting);
                })
            };
            let second = {
                let (runtime, slots) = (Arc::clone(&runtime), Arc::clone(&slots));
                loom::thread::spawn(move || {
                    let claimed = {
                        let _runtime = runtime.lock().expect("runtime lock");
                        BranchPublishSlots::try_acquire(&slots, branch(2)).expect("free slot")
                    };
                    let mut recorded = runtime.lock().expect("runtime lock");
                    recorded.push(2);
                    drop(claimed);
                })
            };
            let mark = slots.panic_mark();
            let (recorded, outcome) =
                slots.lock_once_released(mark, Duration::from_secs(30), || {
                    runtime.lock().expect("runtime lock")
                });
            assert_eq!(outcome, PublishSlotsWait::Released);
            assert!(
                slots.state.lock().expect("slot state").held.is_empty(),
                "no publish is in flight while the close holds the runtime lock"
            );
            assert!(
                recorded.contains(&1),
                "the in-flight publish recorded first"
            );
            drop(recorded);
            first.join().expect("first worker");
            second.join().expect("second worker");
        });
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::{close_wait_outcome, BranchPublishGuard, BranchPublishSlots, PublishSlotsWait};
    use std::sync::{mpsc, Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};
    use strata_core::BranchId;

    fn branch(byte: u8) -> BranchId {
        BranchId::from_bytes([byte; 16])
    }

    /// A holder that unwinds from a panic, as a publish whose persist panics
    /// does. `resume_unwind` skips the panic hook, which keeps the test output
    /// quiet.
    fn panic_while_holding(guard: BranchPublishGuard) -> JoinHandle<()> {
        std::thread::spawn(move || {
            let _guard = guard;
            std::panic::resume_unwind(Box::new("publish panicked"));
        })
    }

    #[test]
    fn close_wait_outcome_truth_table() {
        use PublishSlotsWait::{Panicked, Released, TimedOut};
        for (panicked, any_held, expired, expected) in [
            (false, false, false, Some(Released)),
            (false, false, true, Some(Released)),
            (false, true, false, None),
            (false, true, true, Some(TimedOut)),
            (true, false, false, Some(Panicked)),
            (true, false, true, Some(Panicked)),
            (true, true, false, Some(Panicked)),
            (true, true, true, Some(Panicked)),
        ] {
            assert_eq!(
                close_wait_outcome(panicked, any_held, expired),
                expected,
                "panicked={panicked} any_held={any_held} expired={expired}"
            );
        }
    }

    #[test]
    fn a_held_slot_refuses_a_second_claim_until_it_is_released() {
        let slots = Arc::new(BranchPublishSlots::new());
        let guard = BranchPublishSlots::try_acquire(&slots, branch(1)).expect("free slot");
        assert!(BranchPublishSlots::try_acquire(&slots, branch(1)).is_none());
        assert!(
            BranchPublishSlots::try_acquire(&slots, branch(2)).is_some(),
            "another branch's slot is independent"
        );
        drop(guard);
        assert!(BranchPublishSlots::try_acquire(&slots, branch(1)).is_some());
    }

    #[test]
    fn the_close_locks_at_once_when_no_slot_is_held() {
        let slots = Arc::new(BranchPublishSlots::new());
        let runtime = Mutex::new(());
        drop(BranchPublishSlots::try_acquire(&slots, branch(1)));
        let mut locks = 0;
        let mark = slots.panic_mark();
        let (_runtime, outcome) = slots.lock_once_released(mark, Duration::from_secs(10), || {
            locks += 1;
            runtime.lock().expect("runtime lock")
        });
        assert_eq!(outcome, PublishSlotsWait::Released);
        assert_eq!(locks, 1);
    }

    #[test]
    fn the_close_locks_once_the_last_held_slot_is_released() {
        let slots = Arc::new(BranchPublishSlots::new());
        let runtime = Mutex::new(());
        let first = BranchPublishSlots::try_acquire(&slots, branch(1)).expect("free slot");
        let second = BranchPublishSlots::try_acquire(&slots, branch(2)).expect("free slot");
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drop(first);
            std::thread::sleep(Duration::from_millis(50));
            drop(second);
        });
        let started = Instant::now();
        let mut locks = 0;
        let mark = slots.panic_mark();
        let (_runtime, outcome) = slots.lock_once_released(mark, Duration::from_secs(10), || {
            locks += 1;
            runtime.lock().expect("runtime lock")
        });
        assert_eq!(outcome, PublishSlotsWait::Released);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the release wakes the wait; the deadline does not"
        );
        assert!(
            locks <= 2,
            "the close waits on the releases instead of retrying the lock: {locks} locks"
        );
        assert!(
            BranchPublishSlots::try_acquire(&slots, branch(1)).is_some()
                && BranchPublishSlots::try_acquire(&slots, branch(2)).is_some(),
            "the close locks only once both slots are free"
        );
        releaser.join().expect("releaser thread");
    }

    /// A worker that finishes building while the close waits for the runtime
    /// lock claims its slot under that lock. The close must not keep the lock
    /// while that publish is in flight: it waits until the publish records.
    #[test]
    fn a_slot_claimed_while_the_close_waits_for_the_lock_is_waited_for_too() {
        let slots = Arc::new(BranchPublishSlots::new());
        let runtime = Arc::new(Mutex::new(Vec::new()));
        let (locked_tx, locked_rx) = mpsc::channel();
        let worker = {
            let (runtime, slots) = (Arc::clone(&runtime), Arc::clone(&slots));
            std::thread::spawn(move || {
                let claimed = {
                    let _runtime = runtime.lock().expect("runtime lock");
                    locked_tx.send(()).expect("signal the lock is held");
                    std::thread::sleep(Duration::from_millis(50));
                    BranchPublishSlots::try_acquire(&slots, branch(2)).expect("free slot")
                };
                std::thread::sleep(Duration::from_millis(50));
                let mut recorded = runtime.lock().expect("runtime lock");
                recorded.push(2_u8);
                drop(claimed);
            })
        };
        locked_rx.recv().expect("the worker holds the runtime lock");
        let mark = slots.panic_mark();
        let (recorded, outcome) = slots.lock_once_released(mark, Duration::from_secs(10), || {
            runtime.lock().expect("runtime lock")
        });
        assert_eq!(outcome, PublishSlotsWait::Released);
        assert_eq!(
            *recorded,
            [2],
            "the publish claimed under the lock records before the close holds it"
        );
        drop(recorded);
        worker.join().expect("worker thread");
    }

    #[test]
    fn the_close_gives_up_at_its_deadline_while_a_slot_stays_held() {
        let slots = Arc::new(BranchPublishSlots::new());
        let runtime = Mutex::new(());
        let _held = BranchPublishSlots::try_acquire(&slots, branch(1)).expect("free slot");
        for deadline in [Duration::from_millis(20), Duration::ZERO] {
            let mark = slots.panic_mark();
            let (_runtime, outcome) =
                slots.lock_once_released(mark, deadline, || runtime.lock().expect("runtime lock"));
            assert_eq!(outcome, PublishSlotsWait::TimedOut, "deadline {deadline:?}");
        }
        assert!(BranchPublishSlots::try_acquire(&slots, branch(1)).is_none());
    }

    /// The close takes its mark before the shutdown; a slot holder that panics
    /// any time after it, before or during the wait, fails the wait.
    #[test]
    fn a_slot_holder_that_panics_after_the_mark_fails_the_wait() {
        let slots = Arc::new(BranchPublishSlots::new());
        let runtime = Mutex::new(());
        let guard = BranchPublishSlots::try_acquire(&slots, branch(1)).expect("free slot");
        let mark = slots.panic_mark();
        let holder = panic_while_holding(guard);
        let (runtime_guard, outcome) =
            slots.lock_once_released(mark, Duration::from_secs(10), || {
                runtime.lock().expect("runtime lock")
            });
        drop(runtime_guard);
        assert_eq!(outcome, PublishSlotsWait::Panicked);
        assert!(holder.join().is_err(), "the holder panicked");
    }

    #[test]
    fn a_panic_before_the_mark_does_not_fail_the_wait() {
        let slots = Arc::new(BranchPublishSlots::new());
        let runtime = Mutex::new(());
        let guard = BranchPublishSlots::try_acquire(&slots, branch(1)).expect("free slot");
        assert!(
            panic_while_holding(guard).join().is_err(),
            "the holder panicked"
        );
        let mark = slots.panic_mark();
        let (_runtime, outcome) = slots.lock_once_released(mark, Duration::from_secs(10), || {
            runtime.lock().expect("runtime lock")
        });
        assert_eq!(outcome, PublishSlotsWait::Released);
    }
}
