#[cfg(test)]
use std::cell::Cell;
use std::io;

#[cfg(not(test))]
use std::sync::OnceLock;

#[cfg(not(test))]
use std::sync::Mutex;

#[cfg(test)]
thread_local! {
    static MANIFEST_PUBLISH_FAILURE: Cell<Option<io::ErrorKind>> = const { Cell::new(None) };
    static MANIFEST_DIR_FSYNC_FAILURE: Cell<Option<io::ErrorKind>> = const { Cell::new(None) };
}

// ---------------------------------------------------------------------------
// SE4 install-pause hook (test-only)
// ---------------------------------------------------------------------------
//
// Coordination primitive for SE4 regression tests: lets a test thread park
// compaction / materialize just after the I/O phase and just before the
// atomic install, so the race against `clear_branch` is deterministic rather
// than barrier-probabilistic.
//
// Entirely behind `#[cfg(test)]` — production builds contain no reference to
// this state and pay zero runtime cost.

#[cfg(test)]
pub(crate) use install_pause::{arm, maybe_pause, release, wait_until_entered};

#[cfg(test)]
mod install_pause {
    use std::collections::HashMap;
    use std::sync::{Arc, Condvar, Mutex, OnceLock};
    use strata_core::types::BranchId;

    // Keyed by (op_tag, branch_id) so parallel tests on distinct branches
    // don't collide on a shared global slot. Production callers pass their
    // own (branch_id, op_tag); if no test has registered that exact key, the
    // lookup is a HashMap miss and the call returns immediately.
    type Key = (&'static str, BranchId);

    struct PauseState {
        entered: bool,
        released: bool,
    }

    pub(crate) struct ArmedPause {
        key: Key,
        inner: Arc<Inner>,
    }

    struct Inner {
        lock: Mutex<PauseState>,
        cv: Condvar,
    }

    static SLOTS: OnceLock<Mutex<HashMap<Key, Arc<Inner>>>> = OnceLock::new();

    fn slots() -> &'static Mutex<HashMap<Key, Arc<Inner>>> {
        SLOTS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub(crate) fn arm(op_tag: &'static str, branch_id: BranchId) -> ArmedPause {
        let inner = Arc::new(Inner {
            lock: Mutex::new(PauseState {
                entered: false,
                released: false,
            }),
            cv: Condvar::new(),
        });
        let key = (op_tag, branch_id);
        let prev = slots().lock().unwrap().insert(key, Arc::clone(&inner));
        assert!(
            prev.is_none(),
            "install_pause: already armed for ({}, {:?}); use a distinct \
             branch_id per test to keep parallel tests independent",
            key.0,
            key.1,
        );
        ArmedPause { key, inner }
    }

    impl Drop for ArmedPause {
        fn drop(&mut self) {
            // Remove our entry from the global slot so later calls to
            // `maybe_pause` with the same key pass through.
            slots().lock().unwrap().remove(&self.key);
            // Release any thread that is currently blocked on our condvar.
            // Without this, a test that panics between `arm()` and `release()`
            // would leak a compaction thread stuck in `maybe_pause` forever
            // (the thread holds its own `Arc<Inner>` cloned out of the map,
            // so removal alone does not wake it).
            let mut state = self.inner.lock.lock().unwrap();
            state.released = true;
            self.inner.cv.notify_all();
        }
    }

    pub(crate) fn wait_until_entered(pause: &ArmedPause) {
        let mut state = pause.inner.lock.lock().unwrap();
        while !state.entered {
            state = pause.inner.cv.wait(state).unwrap();
        }
    }

    pub(crate) fn release(pause: &ArmedPause) {
        let mut state = pause.inner.lock.lock().unwrap();
        state.released = true;
        pause.inner.cv.notify_all();
    }

    pub(crate) fn maybe_pause(op_tag: &'static str, branch_id: BranchId) {
        let entry = slots().lock().unwrap().get(&(op_tag, branch_id)).cloned();
        if let Some(inner) = entry {
            let mut state = inner.lock.lock().unwrap();
            state.entered = true;
            inner.cv.notify_all();
            while !state.released {
                state = inner.cv.wait(state).unwrap();
            }
        }
    }
}

/// Op tags for install-pause coordination. Keyed jointly with a `BranchId`
/// so parallel tests on distinct branches don't collide.
#[cfg(test)]
pub(crate) mod pause_tag {
    pub(crate) const COMPACT_BRANCH: &str = "compact_branch";
    pub(crate) const COMPACT_TIER: &str = "compact_tier";
    pub(crate) const COMPACT_L0_TO_L1: &str = "compact_l0_to_l1";
    pub(crate) const COMPACT_LEVEL: &str = "compact_level";
    pub(crate) const MATERIALIZE_LAYER: &str = "materialize_layer";
}

#[cfg(not(test))]
fn manifest_publish_failure_slot() -> &'static Mutex<Option<io::ErrorKind>> {
    static MANIFEST_PUBLISH_FAILURE: OnceLock<Mutex<Option<io::ErrorKind>>> = OnceLock::new();
    MANIFEST_PUBLISH_FAILURE.get_or_init(|| Mutex::new(None))
}

#[cfg(not(test))]
fn manifest_dir_fsync_failure_slot() -> &'static Mutex<Option<io::ErrorKind>> {
    static MANIFEST_DIR_FSYNC_FAILURE: OnceLock<Mutex<Option<io::ErrorKind>>> = OnceLock::new();
    MANIFEST_DIR_FSYNC_FAILURE.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(crate) fn inject_manifest_publish_failure(kind: io::ErrorKind) {
    MANIFEST_PUBLISH_FAILURE.with(|slot| slot.set(Some(kind)));
}

#[cfg(test)]
pub(crate) fn clear_manifest_publish_failure() {
    MANIFEST_PUBLISH_FAILURE.with(|slot| slot.set(None));
}

pub(crate) fn maybe_inject_manifest_publish_failure() -> Option<io::Error> {
    #[cfg(test)]
    let kind = MANIFEST_PUBLISH_FAILURE.with(|slot| slot.take());
    #[cfg(not(test))]
    let kind = manifest_publish_failure_slot().lock().unwrap().take();

    kind.map(|kind| io::Error::new(kind, "injected manifest publish failure"))
}

#[cfg(test)]
pub(crate) fn inject_manifest_dir_fsync_failure(kind: io::ErrorKind) {
    MANIFEST_DIR_FSYNC_FAILURE.with(|slot| slot.set(Some(kind)));
}

#[cfg(test)]
pub(crate) fn clear_manifest_dir_fsync_failure() {
    MANIFEST_DIR_FSYNC_FAILURE.with(|slot| slot.set(None));
}

pub(crate) fn maybe_inject_manifest_dir_fsync_failure() -> Option<io::Error> {
    #[cfg(test)]
    let kind = MANIFEST_DIR_FSYNC_FAILURE.with(|slot| slot.take());
    #[cfg(not(test))]
    let kind = manifest_dir_fsync_failure_slot().lock().unwrap().take();

    kind.map(|kind| io::Error::new(kind, "injected manifest dir fsync failure"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_publish_failure_hook_consumes_one_failure() {
        clear_manifest_publish_failure();
        assert!(maybe_inject_manifest_publish_failure().is_none());

        inject_manifest_publish_failure(io::ErrorKind::Other);
        let err =
            maybe_inject_manifest_publish_failure().expect("expected injected publish failure");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(maybe_inject_manifest_publish_failure().is_none());
    }

    #[test]
    fn test_manifest_dir_fsync_failure_hook_consumes_one_failure() {
        clear_manifest_dir_fsync_failure();
        assert!(maybe_inject_manifest_dir_fsync_failure().is_none());

        inject_manifest_dir_fsync_failure(io::ErrorKind::Other);
        let err =
            maybe_inject_manifest_dir_fsync_failure().expect("expected injected fsync failure");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(maybe_inject_manifest_dir_fsync_failure().is_none());
    }
}
