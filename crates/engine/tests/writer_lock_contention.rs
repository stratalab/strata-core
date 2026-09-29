//! A database held by another opener is a precondition failure, not an outage.
//!
//! `acquire_writer_lock` is fail-fast by contract — `local_fs.rs`'s own module
//! header says contention "is the *another live opener* signal and must never be
//! retried here". But the `WouldBlock` it returns ran through the generic
//! `map_io_error`, which folds `WouldBlock | TimedOut` into `Unavailable`, so
//! contention arrived as a transient backend outage with
//! `RetryPolicy::SameRequest`: retry the identical request, forever, against a
//! lock only another process can release (#3005, #3167).
//!
//! The right policy is `AfterStateChange` — unlike a size refusal this *does*
//! clear, but only once the holder lets go, which no amount of retrying the
//! same request achieves.

mod common;

use strata_engine::{ErrorClass, ErrorDetail, RetryPolicy};

use common::open_durable_database;

#[test]
fn a_database_held_by_another_opener_is_not_reported_as_an_outage() {
    let tempdir = tempfile::tempdir().expect("tempdir");

    let _holder = open_durable_database(tempdir.path()).expect("first open takes the writer lock");

    let Err(error) = open_durable_database(tempdir.path()) else {
        panic!("a second opener must not take the writer lock")
    };
    let status = error.status();

    // NOTE: assert on the PUBLIC class, not `error.class()`. The legacy
    // `EngineErrorClass` deliberately files `failed_precondition.engine.*`
    // under its `Unavailable` group, so `error.class()` still reports
    // `Unavailable` here and always will — that divergence is #3139. What a
    // CLI, JSON or SDK consumer sees is `status.class()`.
    assert_eq!(
        status.class(),
        ErrorClass::FailedPrecondition,
        "contention is reported as a backend outage on the wire (code {})",
        status.code()
    );
    assert_eq!(
        status.retry_policy(),
        RetryPolicy::AfterStateChange,
        "contention clears when the holder releases it, never by repeating the \
         same request (code {})",
        status.code()
    );
    // Its own code, not the FailedPrecondition class default. The IPC broker
    // keys on this exact string to decide whether to hand the command to a
    // running host, and `failed_precondition.engine.persistence` is shared with
    // branch-not-writable, guard-unavailable and quiesce-unavailable — brokering
    // on that would fire on unrelated preconditions.
    assert_eq!(status.code(), "failed_precondition.engine.writer_lock");
    assert!(
        status.message().contains("another process or handle"),
        "the error must name the condition, not just its class: {:?}",
        status.message()
    );
    // The adapter's site detail names the condition in structured form, so a
    // consumer does not have to read prose. Asserted HERE, in the engine, and
    // not only in the executor's error-contract test: the mutation lane that
    // judges `storage_error_details` runs engine tests, so executor-side
    // coverage leaves "delete this match arm" a surviving mutant.
    assert!(
        status.details().iter().any(|detail| {
            *detail
                == ErrorDetail::new(
                    "reason",
                    "the database writer lock is held by another opener",
                )
        }),
        "the writer-lock condition must reach the wire as a structured detail: {:?}",
        status.details()
    );
}

/// The test-side reopen policy for a DROPPED handle (#2837, #3546): the
/// helper outlasts a lock the previous holder releases a moment later — the
/// detached-worker window — instead of failing on the first contention.
#[test]
fn a_reopen_after_drop_outlasts_a_briefly_held_writer_lock() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let holder = open_durable_database(tempdir.path()).expect("holder takes the writer lock");
    let release = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(100));
        drop(holder);
    });

    let reopened = common::reopen_durable_database_after_drop(tempdir.path());
    release.join().expect("release thread");
    reopened.expect("the reopen waits for the holder's release");
}

/// ... and it retries nothing else: a non-contention refusal returns on the
/// first attempt with its own code, so a real failure is never delayed or
/// masked by the lock policy.
#[test]
// The counting closure returns the engine's own open result; its error size is
// the product's, not this test's to shrink.
#[allow(clippy::result_large_err)]
fn a_reopen_after_drop_returns_any_other_refusal_immediately() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    open_durable_database(tempdir.path())
        .expect("create the database")
        .close()
        .expect("clean close");

    let mut attempts = 0_u32;
    let Err(error) = common::reopen_after_drop(|| {
        attempts += 1;
        strata_engine::Database::open_local(
            tempdir.path(),
            strata_engine::DurableLocalOpenOptions::new()
                .with_default_branch("other")
                .expect("valid default branch"),
        )
    }) else {
        panic!("a conflicting default branch must be refused");
    };
    assert_eq!(error.code(), "failed_precondition.engine.default_branch");
    assert_eq!(attempts, 1, "only writer-lock contention is retried");
}
