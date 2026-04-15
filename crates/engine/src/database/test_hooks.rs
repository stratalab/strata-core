//! Test hooks for fault injection in WAL sync operations.
//!
//! These hooks are only available in test builds and allow injecting
//! errors to verify error handling and recovery behavior.

use std::cell::RefCell;
use std::io;

thread_local! {
    /// Injected sync failure for testing.
    /// When set, `maybe_inject_sync_failure()` returns the error.
    static INJECT_SYNC_FAILURE: RefCell<Option<io::ErrorKind>> = const { RefCell::new(None) };
}

/// Inject a sync failure for the next `sync_all()` call.
///
/// The failure is consumed after one use. Call this before triggering
/// a background sync to test error handling.
pub fn inject_sync_failure(kind: io::ErrorKind) {
    INJECT_SYNC_FAILURE.with(|cell| {
        *cell.borrow_mut() = Some(kind);
    });
}

/// Clear any pending injected sync failure.
pub fn clear_sync_failure() {
    INJECT_SYNC_FAILURE.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// Check if a sync failure should be injected.
///
/// Returns `Some(io::Error)` if a failure was injected, consuming it.
/// Returns `None` if no failure is pending.
pub fn maybe_inject_sync_failure() -> Option<io::Error> {
    INJECT_SYNC_FAILURE.with(|cell| {
        cell.borrow_mut().take().map(|kind| {
            io::Error::new(kind, "injected test failure")
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inject_and_consume() {
        // Initially no failure
        assert!(maybe_inject_sync_failure().is_none());

        // Inject a failure
        inject_sync_failure(io::ErrorKind::Other);

        // First call returns the error
        let err = maybe_inject_sync_failure().expect("should have error");
        assert_eq!(err.kind(), io::ErrorKind::Other);

        // Second call returns None (consumed)
        assert!(maybe_inject_sync_failure().is_none());
    }

    #[test]
    fn test_clear_failure() {
        inject_sync_failure(io::ErrorKind::Other);
        clear_sync_failure();
        assert!(maybe_inject_sync_failure().is_none());
    }
}
