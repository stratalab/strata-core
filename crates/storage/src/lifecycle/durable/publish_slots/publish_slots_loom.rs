//! DUR-006: the close's wait must finish on the slots' releases alone. Under
//! loom the deadline never expires and `wait_timeout` never times out, so a
//! lost wake-up is reported as a deadlock. Only compiled with
//! `RUSTFLAGS="--cfg loom"`; run via `cargo test -p strata-storage --lib
//! publish_slots_loom`.
//!
//! A file of its own, not an inline module: the mutation gate excludes loom
//! models by the `*_loom.rs` name, because no lane compiles them (#3747).

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
        let (recorded, outcome) = slots.lock_once_released(mark, Duration::from_secs(30), || {
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
