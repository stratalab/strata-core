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
    BranchName, Database, EmbeddingModelId, EngineError, EngineErrorClass, ErrorDetail,
    EventPayload, EventType, GraphName, GraphProperties, JsonDocumentId, JsonValue, ProductSpace,
    RetryPolicy, VectorCollectionName, VectorMetadata,
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

fn put_key_of(database: &mut Database, key_len: usize) -> Result<(), Box<EngineError>> {
    let mut kv = database
        .kv(branch("default"), space("default"))
        .expect("kv service");
    kv.put(
        KvKey::new("k".repeat(key_len)).expect("key"),
        KvValue::new(vec![b'x'; 64]),
    )
    .map(|_| ())
    .map_err(Box::new)
}

/// A key too long to encode into a table entry is refused at write time, in
/// both modes.
///
/// Before #3396 the WAL took it happily, so the write was ACKNOWLEDGED and the
/// branch's next rotation then could not build its table — after which the
/// branch refused every write, small unrelated ones included, until the
/// database was reopened. Refusing at admission is what makes that
/// unreachable: an oversized key never reaches a memtable, so no rotation can
/// inherit one.
#[test]
fn a_key_too_large_to_build_is_refused_in_both_modes() {
    let cap = 64 * 1024;
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut durable = open_durable_database(tempdir.path()).expect("durable open");
    let mut cache = open_cache_database().expect("cache open");

    for (mode, database) in [("durable", &mut durable), ("cache", &mut cache)] {
        // Direction control: a long-but-buildable key is still accepted, so the
        // refusal is about the cap and not about long keys in general.
        put_key_of(database, cap / 2).unwrap_or_else(|error| {
            panic!("{mode}: a key inside the cap must still be accepted: {error}")
        });

        let error = put_key_of(database, cap * 2)
            .expect_err("a key twice the cap can never be built into a table");
        assert_refused_as_caller_error(&format!("{mode} oversized key"), &error);
    }
}

/// The two caps are independent, and the refusals must stay distinguishable:
/// an oversized VALUE is fixed by sending less data, an oversized KEY is not.
/// Collapsing them would send a caller to trim a payload that was never the
/// problem.
#[test]
fn an_oversized_key_and_an_oversized_value_are_different_refusals() {
    let cache = open_cache_database().expect("cache open");
    let mut cache = cache;

    let key_error = put_key_of(&mut cache, 128 * 1024).expect_err("key over the table cap");
    let value_error =
        put_value_of(&mut cache, "modest", 16 * 1024 * 1024).expect_err("value over the row cap");

    assert_ne!(
        key_error.status().details(),
        value_error.status().details(),
        "an oversized key and an oversized value report identically, so neither \
         names the field a caller has to change"
    );
}

/// One structured detail of an error, parsed as the byte count it carries.
fn detail_bytes(error: &EngineError, key: &str) -> u64 {
    let value = detail_text(error, key).unwrap_or_else(|| {
        panic!(
            "{}: no `{key}` detail in {:?}",
            error.code(),
            error.details()
        )
    });
    value
        .parse()
        .unwrap_or_else(|_| panic!("{}: `{key}` is not a byte count: {value:?}", error.code()))
}

fn detail_text<'a>(error: &'a EngineError, key: &str) -> Option<&'a str> {
    error
        .details()
        .iter()
        .find(|detail| detail.key() == key)
        .map(ErrorDetail::value)
}

/// #3397: a size refusal names the size it refused and the limit it refused
/// against, as typed details, so the caller knows whether to trim 40 bytes or
/// 4 MiB. The row cap in particular is not a number a caller can guess: it is
/// 16 MiB minus an overhead that depends on the key, so a value trimmed to
/// exactly 16 MiB is still refused. Both modes, both storage caps.
#[test]
fn a_storage_size_refusal_names_the_size_and_the_limit() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut durable = open_durable_database(tempdir.path()).expect("durable open");
    let mut cache = open_cache_database().expect("cache open");

    for (mode, database) in [("durable", &mut durable), ("cache", &mut cache)] {
        let value =
            put_value_of(database, "sized", 16 * 1024 * 1024).expect_err("row over the cap");
        assert_eq!(
            value.code(),
            "invalid_argument.engine.persistence",
            "{mode}"
        );
        assert_eq!(detail_text(&value, "field"), Some("row"), "{mode}");
        assert_eq!(
            detail_bytes(&value, "limit_bytes"),
            16 * 1024 * 1024,
            "{mode}"
        );
        // The value alone is 16 MiB; the encoded row is that plus the key and
        // framing, so the measured size is strictly more than the value.
        assert!(
            detail_bytes(&value, "actual_bytes") > 16 * 1024 * 1024,
            "{mode}: the encoded row size must include the value and its overhead"
        );

        let key = put_key_of(database, 128 * 1024).expect_err("key over the table cap");
        assert_eq!(key.code(), "invalid_argument.engine.persistence", "{mode}");
        assert_eq!(detail_text(&key, "field"), Some("key"), "{mode}");
        assert_eq!(detail_bytes(&key, "limit_bytes"), 64 * 1024, "{mode}");
        assert!(
            detail_bytes(&key, "actual_bytes") >= 128 * 1024,
            "{mode}: the encoded key is at least the caller's key"
        );
    }
}

/// #3397: the engine's own per-primitive guards carry the same two details
/// under the same keys as the storage caps, so a client reads one pair of
/// keys whichever layer refused. Each row: what was refused, the refusal, the
/// size the caller sent, and the documented cap.
#[test]
fn every_engine_size_guard_names_the_size_and_the_limit() {
    let cases: Vec<(&str, EngineError, u64, u64)> = vec![
        (
            "event type",
            EventType::new("t".repeat(300)).expect_err("event type over 256 bytes"),
            300,
            256,
        ),
        (
            "branch name",
            BranchName::new("b".repeat(300)).expect_err("branch name over 255 bytes"),
            300,
            255,
        ),
        (
            "product space",
            ProductSpace::new("s".repeat(70_000)).expect_err("space over 65535 bytes"),
            70_000,
            65_535,
        ),
        (
            "vector collection name",
            VectorCollectionName::new("c".repeat(300)).expect_err("collection over 256 bytes"),
            300,
            256,
        ),
        (
            "embedding model",
            EmbeddingModelId::new("m".repeat(300)).expect_err("model id over 256 bytes"),
            300,
            256,
        ),
        (
            "graph name",
            GraphName::new("g".repeat(300)).expect_err("graph name over 256 bytes"),
            300,
            256,
        ),
        (
            "JSON document id",
            JsonDocumentId::new("d".repeat(70_000)).expect_err("document id over 65535 bytes"),
            70_000,
            65_535,
        ),
    ];
    for (what, error, sent, limit) in cases {
        assert_eq!(error.class(), EngineErrorClass::InvalidInput, "{what}");
        assert_eq!(detail_bytes(&error, "actual_bytes"), sent, "{what}");
        assert_eq!(detail_bytes(&error, "limit_bytes"), limit, "{what}");
    }

    // The encoded-size guards measure the value's JSON encoding — the string
    // plus its object framing — so the size is strictly over the cap, and the
    // cap is exact.
    let over = || serde_json::json!({ "d": "x".repeat(16 * 1024 * 1024) });
    let encoded: Vec<(&str, EngineError)> = vec![
        (
            "event payload",
            EventPayload::new(over()).expect_err("payload over the cap"),
        ),
        (
            "JSON document",
            JsonValue::new(over()).expect_err("document over the cap"),
        ),
        (
            "vector metadata",
            VectorMetadata::new(over()).expect_err("metadata over the cap"),
        ),
        (
            "graph properties",
            GraphProperties::new(over()).expect_err("properties over the cap"),
        ),
    ];
    for (what, error) in encoded {
        assert_eq!(error.class(), EngineErrorClass::InvalidInput, "{what}");
        assert_eq!(
            detail_bytes(&error, "limit_bytes"),
            16 * 1024 * 1024,
            "{what}"
        );
        assert!(
            detail_bytes(&error, "actual_bytes") > 16 * 1024 * 1024,
            "{what}: the encoded size is over the cap it was refused against"
        );
    }
}
