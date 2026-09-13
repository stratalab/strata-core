//! A write too large to encode is a caller error, not an availability failure.
//!
//! The WAL commit payload caps a single row at 16 MiB
//! (`MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES`). Exceeding it is deterministic and
//! permanent: no amount of retrying, and no change of backend or branch state,
//! will ever let the same write through. It must therefore never arrive as
//! `Unavailable` with `RetryPolicy::SameRequest`, which tells an honest caller
//! — and, increasingly, an agent reading `suggested_fix` — to retry forever.
//!
//! The per-primitive size guards do not close this: they cap the *payload*,
//! while the WAL caps the *encoded row*, so any value between
//! (cap - row overhead) and cap passes the engine and dies in storage (#3383).

mod common;

use strata_engine::{
    Database, EngineError, EngineErrorClass, EventPayload, EventType, RetryPolicy,
};
use strata_engine::{KvKey, KvValue};

use common::{branch, open_cache_database, open_durable_database, space};

/// Neither `Unavailable` nor a retry-me policy is an acceptable answer to
/// "this write is too big to encode".
fn assert_refused_as_caller_error(what: &str, error: &EngineError) {
    let status = error.status();
    assert_ne!(
        error.class(),
        EngineErrorClass::Unavailable,
        "{what}: a permanent size refusal is reported as an availability failure \
         (code {}, fix {:?})",
        status.code(),
        status.suggested_fix()
    );
    assert_eq!(
        status.retry_policy(),
        RetryPolicy::Never,
        "{what}: a permanent size refusal must not invite a retry (code {})",
        status.code()
    );
}

/// Cache mode has no WAL (hard rule 14), so the row cap does not apply there:
/// the same 16 MiB value that durable refuses is accepted. That divergence is
/// recorded in #3391 as its own concern; this test pins the durable contract.
fn exercise(database: &mut Database) {
    let over = 16 * 1024 * 1024;

    // KV has no engine-level value guard at all, so this reaches the WAL.
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("kv service");
        let error = kv
            .put(
                KvKey::new("oversized").expect("key"),
                KvValue::new(vec![b'x'; over]),
            )
            .expect_err("a 16 MiB value cannot be encoded into one WAL row");
        assert_refused_as_caller_error("kv put", &error);
    }

    // The event payload guard admits this (it is under MAX_EVENT_PAYLOAD_BYTES),
    // but the encoded row carries the type, hash and envelope too, so storage
    // still refuses it. The guard moved the boundary; it did not close it.
    {
        let payload = EventPayload::new(serde_json::json!({ "d": "x".repeat(over - 64) }))
            .expect("payload is inside the engine's own cap");
        let mut events = database
            .event(branch("default"), space("default"))
            .expect("event service");
        let error = events
            .append(EventType::new("oversized").expect("type"), payload)
            .expect_err("the encoded row exceeds the WAL row cap");
        assert_refused_as_caller_error("event append", &error);
    }

    // Direction control: a write comfortably inside the limit still succeeds.
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("kv service");
        kv.put(
            KvKey::new("modest").expect("key"),
            KvValue::new(vec![b'x'; 1024]),
        )
        .expect("a small value is unaffected");
    }
}

#[test]
fn a_write_too_large_to_encode_is_refused_as_a_caller_error() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut durable = open_durable_database(tempdir.path()).expect("durable open");
    exercise(&mut durable);
}

/// Documents today's divergence rather than blessing it: cache mode has no WAL,
/// so it accepts a row durable mode cannot encode. Code proven in cache can
/// therefore fail on its first durable write (#3391).
#[test]
fn cache_mode_accepts_a_row_durable_mode_cannot_encode() {
    let cache = open_cache_database().expect("cache open");
    let mut kv = cache
        .kv(branch("default"), space("default"))
        .expect("kv service");
    kv.put(
        KvKey::new("oversized").expect("key"),
        KvValue::new(vec![b'x'; 16 * 1024 * 1024]),
    )
    .expect("cache mode has no WAL row cap");
}
