//! Event and space validation-refusal code coverage (TCP3.5d).
//!
//! Pins the reachable event/space refusal codes by their literal
//! `<class>.engine.<detail>` string. As in the graph/json/vector batches, the
//! genuinely user-reachable refusals are a subset: `event_batch`,
//! `event_metadata`, `event_record` are defensive/short-circuited (#2651), and
//! `space_catalog` (a `u16::try_from(spaces.len())` overflow needing >65535
//! spaces) is practically unreachable — each recorded in the workspace guard
//! allowlist with a reason.

mod common;

use serde_json::json;
use strata_engine::EventPayload;

use common::{branch, open_cache_database, space};

/// An event payload exceeding the 16 MiB encoded ceiling is rejected.
#[test]
fn event_payload_too_large_is_rejected() {
    let oversized = json!({ "blob": "x".repeat(17 * 1024 * 1024) });
    let error = EventPayload::new(oversized).expect_err("oversized payload must reject");
    assert_eq!(
        error.code(),
        "invalid_argument.engine.event_payload_too_large"
    );
}

/// Deleting the default product space is refused with a stable code.
#[test]
fn deleting_the_default_space_is_rejected() {
    let mut database = open_cache_database().expect("cache open");
    let mut spaces = database.spaces(branch("default")).expect("space service");

    let error = spaces
        .delete(&space("default"), false)
        .expect_err("deleting the default space must reject");
    assert_eq!(error.code(), "invalid_argument.engine.space_delete_default");
}

/// #3574: a force-delete of a space holding more rows than one commit
/// admits is no longer refused; it sweeps the rows in commits the budget
/// allows. The rows are written in bounded batches under the per-commit cap
/// (4096) — the exact shape that used to make the space undeletable.
#[test]
fn deleting_an_oversized_space_succeeds_in_chunks() {
    use strata_engine::{KvKey, KvValue};

    let mut database = open_cache_database().expect("cache open");
    let target = space("bulk");
    database
        .spaces(branch("default"))
        .expect("space service")
        .create(target.clone())
        .expect("space create");

    // Just over the old 10,000 atomic space-delete limit.
    for chunk in 0..3u32 {
        let entries = (0..3_400u32).map(|i| {
            let n = chunk * 3_400 + i;
            (
                KvKey::new(format!("k{n:05}")).expect("key"),
                KvValue::new(b"v".to_vec()),
            )
        });
        database
            .kv(branch("default"), target.clone())
            .expect("kv service")
            .put_batch(entries)
            .expect("bulk put");
    }

    let outcome = database
        .spaces(branch("default"))
        .expect("space service")
        .delete(&target, true)
        .expect("an oversized force-delete sweeps in chunks");
    assert!(outcome.deleted());
    assert_eq!(outcome.deleted_rows(), 10_200);
    assert!(!database
        .spaces(branch("default"))
        .expect("space service")
        .exists(&target)
        .expect("exists reads"));
    assert_eq!(
        database
            .kv(branch("default"), target)
            .expect("kv service")
            .count(None)
            .expect("count"),
        0
    );
}
