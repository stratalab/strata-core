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
//! The retry is bounded by a wall-clock deadline, not an attempt count
//! (#2837 family). The detached worker holds the lock until its *task*
//! finishes, which on a loaded runner can be seconds past the 250 ms window —
//! an attempt-count budget sized to the window (the original ~382 ms) still
//! flaked. The deadline matches the engine-side helper merged in #3693.
//!
//! This is deliberately a **test-harness policy, not a product one**: the
//! product open path must keep failing fast, because writer-lock contention is
//! also the legitimate "another live opener holds this database" signal
//! (the product-contract question is #3694).

use std::error::Error;
use std::thread;
use std::time::{Duration, Instant};

use crate::backend::{BackendError, BackendErrorKind};

/// Wall-clock budget for absorbing a released-late writer lock. Generous on
/// purpose: a healthy reopen succeeds on its first attempt and never sleeps,
/// so the budget is only ever spent by a genuinely stuck lock, where the cost
/// is a slow failure rather than a flaky one.
const RETRY_DEADLINE: Duration = Duration::from_secs(30);
/// Backoff doubles from 2 ms and caps at 64 ms, so a released lock is noticed
/// within one capped sleep however long the wait ran.
const INITIAL_BACKOFF: Duration = Duration::from_millis(2);
const MAX_BACKOFF: Duration = Duration::from_millis(64);

/// Runs `open` until it succeeds, retrying **only** failures whose error
/// chain carries a transient writer-lock / `Unavailable` signal, for up to
/// [`RETRY_DEADLINE`] of wall-clock time. Every other error returns
/// immediately and unchanged — without sleeping — and a transient error that
/// outlasts the deadline returns unchanged too, so real failures stay loud.
pub(crate) fn open_with_retry_on_unavailable<T, E, F>(open: F) -> Result<T, E>
where
    E: Error + 'static,
    F: FnMut() -> Result<T, E>,
{
    open_with_retry_within(RETRY_DEADLINE, open)
}

/// [`open_with_retry_on_unavailable`] with an explicit budget, so the
/// give-up path is testable without spending the real deadline.
fn open_with_retry_within<T, E, F>(budget: Duration, mut open: F) -> Result<T, E>
where
    E: Error + 'static,
    F: FnMut() -> Result<T, E>,
{
    let deadline = Instant::now() + budget;
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match open() {
            Ok(value) => return Ok(value),
            Err(err) if is_transient_unavailable(&err) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(err);
                }
                thread::sleep(backoff.min(remaining));
                backoff = next_backoff(backoff);
            }
            Err(err) => return Err(err),
        }
    }
}

/// The doubling-capped backoff schedule. Pure so its shape is assertable: the
/// cap bounds how late a released lock is noticed, and the doubling keeps a
/// long wait from spinning.
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
    fn persistent_unavailable_gives_up_loudly_at_the_deadline() {
        let budget = Duration::from_millis(60);
        let calls = Cell::new(0_u32);
        let started = Instant::now();
        let result: Result<u32, BackendError> = open_with_retry_within(budget, || {
            calls.set(calls.get() + 1);
            Err(unavailable())
        });
        let elapsed = started.elapsed();
        let err = result.expect_err("deadline exhausted");
        assert_eq!(err.kind(), BackendErrorKind::Unavailable);
        assert!(calls.get() > 1, "a transient error is retried");
        assert!(
            elapsed >= budget,
            "gave up after {elapsed:?}, before the {budget:?} deadline"
        );
    }

    #[test]
    fn the_backoff_schedule_doubles_capped_and_the_deadline_outlasts_a_loaded_detach() {
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
        // ... and the properties the design exists for: the deadline is
        // seconds, not the ~382 ms attempt budget that still flaked under a
        // loaded runner (#2837 family, #3693 uses the same 30 s), and the cap
        // keeps the release-to-notice latency small against it.
        assert!(
            RETRY_DEADLINE >= Duration::from_secs(30),
            "deadline {RETRY_DEADLINE:?} must absorb a seconds-long detached task"
        );
        assert!(MAX_BACKOFF <= Duration::from_millis(100));
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
        // One call: the only sleep sits between retries, so none happened.
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
