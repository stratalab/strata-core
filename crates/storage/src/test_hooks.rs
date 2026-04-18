//! Test-only fault-injection hooks for the storage publish path.
//!
//! Mirrors the engine-side `test_hooks` module pattern (see
//! `crates/engine/src/database/test_hooks.rs`), but scoped to the caller's
//! thread so that parallel test execution does not leak injections across
//! tests. Each test runs on its own thread in the cargo test runner, and
//! the operations that consume these hooks (`write_branch_manifest` and
//! `write_manifest`) always run synchronously on the caller's thread.
//!
//! Hooks are consumed on read — injecting one failure arms it for the next
//! matching call site on the same thread; the call site clears the slot
//! after consuming.
//!
//! The hooks compile into release builds but are only ever armed by tests:
//! production code never calls `inject_*` functions, so the slots stay
//! `None` and the consuming `maybe_inject_*` calls are cheap no-ops.

use std::cell::Cell;
use std::io;

thread_local! {
    static MANIFEST_PUBLISH_FAILURE: Cell<Option<io::ErrorKind>> = const { Cell::new(None) };
    static DIR_FSYNC_FAILURE: Cell<Option<io::ErrorKind>> = const { Cell::new(None) };
}

/// Arm one manifest-publish failure for the next call to
/// `SegmentedStore::write_branch_manifest` on the current thread.
/// Consumed exactly once.
pub(crate) fn inject_manifest_publish_failure(kind: io::ErrorKind) {
    MANIFEST_PUBLISH_FAILURE.with(|slot| slot.set(Some(kind)));
}

/// Clear any pending manifest-publish failure on the current thread.
pub(crate) fn clear_manifest_publish_failure() {
    MANIFEST_PUBLISH_FAILURE.with(|slot| slot.set(None));
}

/// Consume an armed manifest-publish failure on the current thread, if any.
pub(crate) fn maybe_inject_manifest_publish_failure() -> Option<io::Error> {
    MANIFEST_PUBLISH_FAILURE
        .with(|slot| slot.take())
        .map(|kind| io::Error::new(kind, "injected manifest publish failure"))
}

/// Arm one directory-fsync failure for the next call to
/// `manifest::write_manifest` on the current thread. Consumed exactly once.
pub(crate) fn inject_dir_fsync_failure(kind: io::ErrorKind) {
    DIR_FSYNC_FAILURE.with(|slot| slot.set(Some(kind)));
}

/// Clear any pending dir-fsync failure on the current thread.
pub(crate) fn clear_dir_fsync_failure() {
    DIR_FSYNC_FAILURE.with(|slot| slot.set(None));
}

/// Consume an armed dir-fsync failure on the current thread, if any.
pub(crate) fn maybe_inject_dir_fsync_failure() -> Option<io::Error> {
    DIR_FSYNC_FAILURE
        .with(|slot| slot.take())
        .map(|kind| io::Error::new(kind, "injected dir fsync failure"))
}
