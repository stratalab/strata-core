//! Bounded retry for test-harness reopens that race a detached background
//! worker from the previous runtime.
//!
//! Dropping a `StorageRuntime` shuts its background scheduler down with a
//! bounded (250 ms) quiesce window; a worker that misses the window is
//! detached, not joined, and keeps the writer-lock file descriptor alive until
//! it finishes. An immediate same-process reopen then fails with the flock's
//! `EWOULDBLOCK`/`EAGAIN`, surfaced as `BackendErrorKind::Unavailable`
//! (issue #2727). Under a loaded parallel test run the window is blown far
//! more often, which made every unprotected `reopen` in the testkit a flake.
//!
//! This is deliberately a **test-harness policy, not a product one**: the
//! product open path must keep failing fast, because writer-lock contention is
//! also the legitimate "another live opener holds this database" signal.

use std::error::Error;
use std::thread;
use std::time::Duration;

use crate::backend::{BackendError, BackendErrorKind};

/// Retry attempts before the final call. Backoff doubles from 2 ms and caps
/// at 64 ms; the cumulative sleep (~382 ms) comfortably outlasts the 250 ms
/// worker-detach window the retry exists to absorb.
const RETRY_ATTEMPTS: u32 = 10;
const INITIAL_BACKOFF: Duration = Duration::from_millis(2);
const MAX_BACKOFF: Duration = Duration::from_millis(64);

/// Runs `open` until it succeeds, retrying **only** failures whose error
/// chain bottoms out in a transient `BackendErrorKind::Unavailable`. Every
/// other error — and exhaustion of the retry budget — returns the original
/// error unchanged, so real failures stay loud.
pub(crate) fn open_with_retry_on_unavailable<T, E, F>(mut open: F) -> Result<T, E>
where
    E: Error + 'static,
    F: FnMut() -> Result<T, E>,
{
    let mut backoff = INITIAL_BACKOFF;
    for _ in 0..RETRY_ATTEMPTS {
        match open() {
            Ok(value) => return Ok(value),
            Err(err) if is_transient_unavailable(&err) => {
                thread::sleep(backoff);
                backoff = next_backoff(backoff);
            }
            Err(err) => return Err(err),
        }
    }
    open()
}

/// The doubling-capped backoff schedule. Pure so its shape is assertable: the
/// cumulative sleep across `RETRY_ATTEMPTS` must outlast the 250 ms
/// detach window, which a shrinking or non-growing schedule would silently
/// break while every retry-count assertion still passed.
fn next_backoff(backoff: Duration) -> Duration {
    (backoff * 2).min(MAX_BACKOFF)
}

/// True when the error, or anything on its `source()` chain, is a backend
/// signal a harness may absorb: `Unavailable` (resource pressure) or
/// `AlreadyExists` (another opener holds the writer lock). The check is
/// structural (downcast + `kind()`), never display text.
///
/// Contention became its own kind in #3005 / #3167 so the PRODUCT path can stop
/// reporting it as a transient outage. This harness is the one place the header
/// of `backend/local_fs.rs` sanctions retrying it, so it must recognise the new
/// kind — otherwise a briefly-held lock fails a sweep that used to survive it.
fn is_transient_unavailable<E: Error + 'static>(err: &E) -> bool {
    let mut current: Option<&(dyn Error + 'static)> = Some(err);
    while let Some(inner) = current {
        // Contention is now a MAPPED condition at both the lifecycle and API
        // layers, and neither variant carries the BackendError as a source, so
        // the downcast-to-BackendError walk below can no longer see it. Match
        // the mapped variants directly (#3005, #3167).
        if matches!(
            inner.downcast_ref::<crate::lifecycle::LifecycleError>(),
            Some(crate::lifecycle::LifecycleError::WriterLockHeld)
        ) || matches!(
            inner.downcast_ref::<crate::api::StorageApiError>(),
            Some(crate::api::StorageApiError::WriterLockHeld)
        ) {
            return true;
        }
        if let Some(backend) = inner.downcast_ref::<BackendError>() {
            return matches!(
                backend.kind(),
                BackendErrorKind::Unavailable | BackendErrorKind::AlreadyExists
            );
        }
        current = inner.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::api::{StorageApiError, StorageApiLowerLayer};
    use crate::lifecycle::{LifecycleError, LifecycleLowerLayer};

    fn unavailable() -> BackendError {
        BackendError::new(
            BackendErrorKind::Unavailable,
            "Resource temporarily unavailable (os error 11)",
        )
    }

    #[test]
    fn transient_unavailable_recovers_after_bounded_retries() {
        let calls = Cell::new(0_u32);
        let result: Result<u32, BackendError> = open_with_retry_on_unavailable(|| {
            calls.set(calls.get() + 1);
            if calls.get() <= 3 {
                Err(unavailable())
            } else {
                Ok(42)
            }
        });
        assert_eq!(result.expect("recovers"), 42);
        assert_eq!(calls.get(), 4);
    }

    #[test]
    fn persistent_unavailable_gives_up_loudly_after_the_budget() {
        let calls = Cell::new(0_u32);
        let result: Result<u32, BackendError> = open_with_retry_on_unavailable(|| {
            calls.set(calls.get() + 1);
            Err(unavailable())
        });
        let err = result.expect_err("budget exhausted");
        assert_eq!(err.kind(), BackendErrorKind::Unavailable);
        assert_eq!(calls.get(), RETRY_ATTEMPTS + 1);
    }

    #[test]
    fn the_backoff_schedule_doubles_capped_and_outlasts_the_detach_window() {
        // Pin the exact schedule (catches a *->/ or cap regression) ...
        assert_eq!(
            next_backoff(Duration::from_millis(2)),
            Duration::from_millis(4)
        );
        assert_eq!(
            next_backoff(Duration::from_millis(32)),
            Duration::from_millis(64)
        );
        assert_eq!(
            next_backoff(Duration::from_millis(64)),
            Duration::from_millis(64)
        );
        // ... and the property the schedule exists for: cumulative sleep across
        // the retry budget outlasts the 250 ms worker-detach window.
        let mut backoff = INITIAL_BACKOFF;
        let mut total = Duration::ZERO;
        for _ in 0..RETRY_ATTEMPTS {
            total += backoff;
            backoff = next_backoff(backoff);
        }
        assert!(
            total > Duration::from_millis(250),
            "cumulative backoff {total:?} must outlast the 250ms detach window"
        );
    }

    #[test]
    fn non_transient_errors_return_immediately_without_retry() {
        let calls = Cell::new(0_u32);
        let result: Result<u32, BackendError> = open_with_retry_on_unavailable(|| {
            calls.set(calls.get() + 1);
            Err(BackendError::new(BackendErrorKind::Corruption, "bad bytes"))
        });
        let err = result.expect_err("fails fast");
        assert_eq!(err.kind(), BackendErrorKind::Corruption);
        assert_eq!(calls.get(), 1);
    }

    fn api_over_lifecycle_over(backend: BackendError) -> StorageApiError {
        // The exact shape from #2727: StorageApiError::LowerLayer(Lifecycle)
        // -> LifecycleError::LowerLayer(Backend) -> BackendError.
        let lifecycle = LifecycleError::lower_layer_with(
            LifecycleLowerLayer::Backend,
            "backend failed",
            backend,
        );
        StorageApiError::LowerLayer {
            layer: StorageApiLowerLayer::Lifecycle,
            inner_code: Some("io.lifecycle.backend"),
            reason: "lifecycle runtime failed",
            source: Some(std::sync::Arc::new(lifecycle)),
        }
    }

    #[test]
    fn the_real_nested_open_error_chain_is_recognized_as_transient() {
        assert!(is_transient_unavailable(&api_over_lifecycle_over(
            unavailable()
        )));

        // A corruption at the same depth must not read as transient.
        assert!(!is_transient_unavailable(&api_over_lifecycle_over(
            BackendError::new(BackendErrorKind::Corruption, "bad")
        )));
    }
}
