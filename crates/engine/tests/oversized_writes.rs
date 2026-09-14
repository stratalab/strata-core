//! A write too large to encode is a caller error, in every durability mode.
//!
//! The WAL commit payload caps a single row at 16 MiB
//! (`MAX_WAL_COMMIT_PAYLOAD_ROW_BYTES`). Exceeding it is deterministic and
//! permanent: no amount of retrying, and no change of backend or branch state,
//! will ever let the same write through. It must therefore never arrive as
//! `Unavailable` with `RetryPolicy::SameRequest`, which tells an honest caller
//! — and, increasingly, an agent reading `suggested_fix` — to retry forever.
//!
//! The per-primitive size guards do not close this: they cap the *payload*,
//! while the cap is on the *encoded row*, so any value between
//! (cap - row overhead) and cap passes the engine and dies in storage (#3383).
//!
//! Cache mode has no WAL at all (hard rule 14), so nothing downstream of it
//! enforces the cap. Commit admission does, in both modes, because cache is
//! the mode the quickstart, the compiled rustdoc examples and the browser
//! playground all run: a write cache accepts has to be a write durable
//! accepts, or code is proven against one contract and shipped against
//! another (#3391).

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

fn exercise(mode: &str, database: &mut Database) {
    let over = 16 * 1024 * 1024;

    // KV has no engine-level value guard at all, so this reaches admission
    // carrying the whole value.
    {
        let mut kv = database
            .kv(branch("default"), space("default"))
            .expect("kv service");
        let error = kv
            .put(
                KvKey::new("oversized").expect("key"),
                KvValue::new(vec![b'x'; over]),
            )
            .expect_err("a 16 MiB value cannot be encoded into one commit row");
        assert_refused_as_caller_error(&format!("{mode} kv put"), &error);
    }

    // The event payload guard admits this (it is under MAX_EVENT_PAYLOAD_BYTES),
    // but the encoded row carries the type, hash and envelope too, so the row
    // is still over. The guard moved the boundary; it did not close it.
    {
        let payload = EventPayload::new(serde_json::json!({ "d": "x".repeat(over - 64) }))
            .expect("payload is inside the engine's own cap");
        let mut events = database
            .event(branch("default"), space("default"))
            .expect("event service");
        let error = events
            .append(EventType::new("oversized").expect("type"), payload)
            .expect_err("the encoded row exceeds the row cap");
        assert_refused_as_caller_error(&format!("{mode} event append"), &error);
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
    exercise("durable", &mut durable);
}

/// The same contract in cache mode. Before #3391 this suite could only record
/// the divergence, because cache accepted every one of these writes.
#[test]
fn cache_mode_refuses_the_same_writes_durable_mode_refuses() {
    let mut cache = open_cache_database().expect("cache open");
    exercise("cache", &mut cache);
}

/// Boxed because `EngineError` is large and `clippy::result_large_err` denies
/// it by value; the tests only ever inspect it behind a reference.
fn put_value_of(
    database: &mut Database,
    key: &str,
    value_len: usize,
) -> Result<(), Box<EngineError>> {
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("kv service");
    kv.put(
        KvKey::new(key).expect("key"),
        KvValue::new(vec![b'x'; value_len]),
    )
    .map(|_| ())
    .map_err(Box::new)
}

/// Sweep across the encodable boundary and assert the two modes agree at every
/// size, without either side hard-coding where the boundary falls: the point of
/// #3391 is that one answer holds in both modes, not that the answer is any
/// particular number. The sweep spans the row overhead (key, commit facts,
/// framing — on the order of a hundred bytes), so it necessarily crosses.
#[test]
fn cache_and_durable_agree_at_the_encodable_boundary() {
    let cap = 16 * 1024 * 1024;
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut durable = open_durable_database(tempdir.path()).expect("durable open");
    let mut cache = open_cache_database().expect("cache open");

    let mut admitted = 0usize;
    let mut refused = 0usize;
    for offset in [512, 256, 128, 96, 80, 74, 73, 64, 32, 1, 0] {
        let value_len = cap - offset;
        let key = format!("sweep-{offset}");
        let durable_result = put_value_of(&mut durable, &key, value_len);
        let cache_result = put_value_of(&mut cache, &key, value_len);

        assert_eq!(
            durable_result.is_ok(),
            cache_result.is_ok(),
            "cache and durable disagree on a {value_len}-byte value: \
             durable {durable_result:?}, cache {cache_result:?}"
        );
        match (durable_result, cache_result) {
            (Ok(()), Ok(())) => admitted += 1,
            (Err(durable_error), Err(cache_error)) => {
                refused += 1;
                assert_refused_as_caller_error("durable sweep", &durable_error);
                assert_refused_as_caller_error("cache sweep", &cache_error);
                assert_eq!(
                    durable_error.status().code(),
                    cache_error.status().code(),
                    "the same refusal must carry the same code in both modes"
                );
            }
            (durable_result, cache_result) => unreachable!(
                "agreement was already asserted: {durable_result:?} / {cache_result:?}"
            ),
        }
    }

    // Without both, the sweep never crossed the boundary and agreement is
    // vacuous — it would hold just as well if neither mode enforced anything.
    assert!(
        admitted > 0 && refused > 0,
        "sweep did not straddle the boundary ({admitted} admitted, {refused} refused)"
    );
}
