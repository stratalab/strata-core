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
