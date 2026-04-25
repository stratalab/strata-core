//! Cross-primitive time-travel consistency tests (Phase 5 of the MVCC
//! verification plan).
//!
//! These tests prove that every primitive's `_at` / `getv` surface honors
//! the same commit-time semantic: a single `as_of_ts` observed across KV,
//! JSON, Vector, and Graph returns a consistent historical snapshot. Event
//! is append-only so it uses `event_len_at` / `event_list_types_at` as the
//! time-travel assertion rather than a point read.
//!
//! The four test categories mirror the design doc:
//! 1. Same-timestamp visibility — seed, capture ts, update, read at old ts.
//! 2. Tombstone consistency — delete each primitive's record, verify old
//!    value visible before delete and None after.
//! 3. Branch isolation — fork, mutate on the fork, verify the source
//!    branch's history is unaffected.
//! 4. Edge cases — `as_of = 0`, `as_of = u64::MAX`, and same-microsecond
//!    writes (newest-wins via storage version-chain ordering).

mod common;

use common::*;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use strata_core::Value;
use strata_executor_legacy::{DistanceMetric, Strata};

// =============================================================================
// Helpers
// =============================================================================

const COLLECTION: &str = "xp_vecs";
const GRAPH: &str = "xp_graph";
const DIM: u64 = 3;

/// Current wall-clock time in microseconds since the Unix epoch.
fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

/// Capture a timestamp with sleep fences on either side to guarantee
/// monotonic separation from adjacent writes.
fn capture_ts_with_fence() -> u64 {
    std::thread::sleep(Duration::from_millis(5));
    let t = now_micros();
    std::thread::sleep(Duration::from_millis(5));
    t
}

/// JSON object helper — wraps a single `key: value` pair.
fn obj(key: &str, value: Value) -> Value {
    Value::object(HashMap::from([(key.to_string(), value)]))
}

/// Initialize vector collection and graph on the current branch.
fn init_schema(db: &Strata) {
    db.vector_create_collection(COLLECTION, DIM, DistanceMetric::Cosine)
        .unwrap();
    db.graph_create(GRAPH).unwrap();
}

/// Seed one round of writes across KV / JSON / Event / Vector / Graph
/// using the same suffix-derived IDs so each round overwrites the
/// previous one (except for Event, which always appends).
fn seed_round(db: &Strata, suffix: &str, vec_vals: [f32; 3], marker: i64) {
    db.kv_put(&format!("kv:{suffix}"), Value::Int(marker))
        .unwrap();
    db.json_set(&format!("doc:{suffix}"), "$", obj("v", Value::Int(marker)))
        .unwrap();
    db.event_append("audit", obj("i", Value::Int(marker)))
        .unwrap();
    db.vector_upsert(
        COLLECTION,
        &format!("vec:{suffix}"),
        vec_vals.to_vec(),
        None,
    )
    .unwrap();
    db.graph_add_node(
        GRAPH,
        &format!("n:{suffix}"),
        None,
        Some(obj("p", Value::Int(marker))),
    )
    .unwrap();
}

/// Pull the integer `i` out of a KV value written by `seed_round`.
fn assert_kv_marker(db: &Strata, key: &str, as_of: u64, expected: i64) {
    let got = db.kv_get_at(key, Some(as_of)).unwrap();
    match got {
        Some(Value::Int(n)) => assert_eq!(
            n, expected,
            "kv {} at {} expected {}, got {}",
            key, as_of, expected, n
        ),
        other => panic!(
            "kv {} at {}: expected Int({}), got {:?}",
            key, as_of, expected, other
        ),
    }
}

/// Pull the integer `v` out of a JSON doc written by `seed_round`.
fn assert_json_marker(db: &Strata, key: &str, as_of: u64, expected: i64) {
    let got = db.json_get_at(key, "$.v", Some(as_of)).unwrap();
    match got {
        Some(Value::Int(n)) => assert_eq!(
            n, expected,
            "json {} at {} expected {}, got {}",
            key, as_of, expected, n
        ),
        other => panic!(
            "json {} at {}: expected Int({}), got {:?}",
            key, as_of, expected, other
        ),
    }
}

/// Pull the integer `p` out of a graph node written by `seed_round`.
fn assert_graph_marker(db: &Strata, graph: &str, node_id: &str, as_of: u64, expected: i64) {
    let got = db.graph_get_node_at(graph, node_id, as_of).unwrap();
    match got {
        Some(node) => {
            // Graph node data is returned as a JSON object; pull properties.p
            let properties = match &node {
                Value::Object(map) => map.get("properties"),
                _ => None,
            };
            let p = properties.and_then(|props| match props {
                Value::Object(m) => match m.get("p") {
                    Some(Value::Int(n)) => Some(*n),
                    _ => None,
                },
                _ => None,
            });
            assert_eq!(
                p,
                Some(expected),
                "graph {}/{} at {}: expected properties.p={}, got {:?}",
                graph,
                node_id,
                as_of,
                expected,
                node
            );
        }
        None => panic!(
            "graph {}/{} at {}: expected node with p={}, got None",
            graph, node_id, as_of, expected
        ),
    }
}

/// Assert vector embedding at a past timestamp.
fn assert_vector_embedding(
    db: &Strata,
    collection: &str,
    key: &str,
    as_of: u64,
    expected: [f32; 3],
) {
    let got = db.vector_get_at(collection, key, Some(as_of)).unwrap();
    match got {
        Some(data) => {
            assert_eq!(
                data.data.embedding, expected,
                "vector {}/{} at {}: expected {:?}, got {:?}",
                collection, key, as_of, expected, data.data.embedding
            );
        }
        None => panic!(
            "vector {}/{} at {}: expected {:?}, got None",
            collection, key, as_of, expected
        ),
    }
}

// =============================================================================
// Test 1 — same-timestamp visibility across all primitives
// =============================================================================

#[test]
fn cross_primitive_same_timestamp_visibility() {
    let db = create_strata();
    init_schema(&db);

    // Round 1: marker = 1, embedding = [1, 0, 0]
    seed_round(&db, "a", [1.0, 0.0, 0.0], 1);
    let ts_r1 = capture_ts_with_fence();

    // Round 2: marker = 2, embedding = [0, 1, 0]
    seed_round(&db, "a", [0.0, 1.0, 0.0], 2);
    let ts_r2 = capture_ts_with_fence();

    // At ts_r1 every primitive sees the round-1 value.
    assert_kv_marker(&db, "kv:a", ts_r1, 1);
    assert_json_marker(&db, "doc:a", ts_r1, 1);
    assert_vector_embedding(&db, COLLECTION, "vec:a", ts_r1, [1.0, 0.0, 0.0]);
    assert_graph_marker(&db, GRAPH, "n:a", ts_r1, 1);
    // Event is append-only — at ts_r1 exactly one event is visible.
    assert_eq!(db.event_len_at(ts_r1).unwrap(), 1);

    // At ts_r2 every primitive sees the round-2 value.
    assert_kv_marker(&db, "kv:a", ts_r2, 2);
    assert_json_marker(&db, "doc:a", ts_r2, 2);
    assert_vector_embedding(&db, COLLECTION, "vec:a", ts_r2, [0.0, 1.0, 0.0]);
    assert_graph_marker(&db, GRAPH, "n:a", ts_r2, 2);
    assert_eq!(db.event_len_at(ts_r2).unwrap(), 2);
}

// =============================================================================
// Test 2 — tombstone consistency (delete → None after, Some before)
// =============================================================================

#[test]
fn cross_primitive_tombstone_consistency() {
    let db = create_strata();
    init_schema(&db);

    seed_round(&db, "t", [1.0, 0.0, 0.0], 42);
    let ts_alive = capture_ts_with_fence();

    // Delete from each primitive that supports deletion. Event is
    // deliberately omitted — events are immutable by design.
    db.kv_delete("kv:t").unwrap();
    db.json_delete("doc:t", "$").unwrap();
    db.vector_delete(COLLECTION, "vec:t").unwrap();
    db.graph_remove_node(GRAPH, "n:t").unwrap();

    let ts_dead = capture_ts_with_fence();

    // Before deletion: every record is visible.
    assert!(db.kv_get_at("kv:t", Some(ts_alive)).unwrap().is_some());
    assert!(db
        .json_get_at("doc:t", "$", Some(ts_alive))
        .unwrap()
        .is_some());
    assert!(db
        .vector_get_at(COLLECTION, "vec:t", Some(ts_alive))
        .unwrap()
        .is_some());
    assert!(db
        .graph_get_node_at(GRAPH, "n:t", ts_alive)
        .unwrap()
        .is_some());

    // After deletion: every record is gone.
    assert!(db.kv_get_at("kv:t", Some(ts_dead)).unwrap().is_none());
    assert!(db
        .json_get_at("doc:t", "$", Some(ts_dead))
        .unwrap()
        .is_none());
    assert!(db
        .vector_get_at(COLLECTION, "vec:t", Some(ts_dead))
        .unwrap()
        .is_none());
    assert!(db
        .graph_get_node_at(GRAPH, "n:t", ts_dead)
        .unwrap()
        .is_none());
}

// =============================================================================
// Test 3 — branch isolation with time-travel
// =============================================================================

#[test]
fn cross_primitive_branch_isolation_time_travel() {
    // fork_branch requires a disk-backed database, so this test opens a
    // temp directory instead of using the in-memory `create_strata()`
    // helper.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cross_primitive_branch.strata");
    let mut db = Strata::open(path.to_str().unwrap()).unwrap();
    init_schema(&db);

    // Seed on the default branch.
    seed_round(&db, "b", [1.0, 0.0, 0.0], 10);
    let ts_seed = capture_ts_with_fence();

    // Fork default → experiment.
    db.branches().fork("default", "experiment").unwrap();

    // Switch to the fork and overwrite every record with marker = 99.
    db.set_branch("experiment").unwrap();
    seed_round(&db, "b", [0.0, 0.0, 1.0], 99);
    let _ts_after_fork = capture_ts_with_fence();

    // Back to default — its history must be unchanged by the fork.
    db.set_branch("default").unwrap();

    // Historical reads on default must return the round-1 seed values
    // across every primitive.
    assert_kv_marker(&db, "kv:b", ts_seed, 10);
    assert_json_marker(&db, "doc:b", ts_seed, 10);
    assert_vector_embedding(&db, COLLECTION, "vec:b", ts_seed, [1.0, 0.0, 0.0]);
    assert_graph_marker(&db, GRAPH, "n:b", ts_seed, 10);

    // Current values on default must also still be the round-1 seed —
    // verify for every primitive (not just KV), so any fork leak is
    // caught regardless of which primitive it happens in.
    assert_eq!(
        db.kv_get("kv:b").unwrap(),
        Some(Value::Int(10)),
        "default KV should not see fork mutations"
    );
    assert_eq!(
        db.json_get("doc:b", "$.v").unwrap(),
        Some(Value::Int(10)),
        "default JSON should not see fork mutations"
    );
    let default_vec = db
        .vector_get(COLLECTION, "vec:b")
        .unwrap()
        .expect("default should still have vec:b");
    assert_eq!(
        default_vec.data.embedding,
        vec![1.0, 0.0, 0.0],
        "default vector embedding should not see fork mutations"
    );
    let default_node = db
        .graph_get_node(GRAPH, "n:b")
        .unwrap()
        .expect("default should still have n:b");
    let default_p = match &default_node {
        Value::Object(map) => match map.get("properties") {
            Some(Value::Object(props)) => props.get("p").cloned(),
            _ => None,
        },
        _ => None,
    };
    assert_eq!(
        default_p,
        Some(Value::Int(10)),
        "default graph node should not see fork mutations, got {:?}",
        default_node
    );

    // And the fork has its own mutated state across every primitive.
    db.set_branch("experiment").unwrap();
    assert_eq!(
        db.kv_get("kv:b").unwrap(),
        Some(Value::Int(99)),
        "experiment KV should see its own mutations"
    );
    assert_eq!(
        db.json_get("doc:b", "$.v").unwrap(),
        Some(Value::Int(99)),
        "experiment JSON should see its own mutations"
    );
    let fork_vec = db
        .vector_get(COLLECTION, "vec:b")
        .unwrap()
        .expect("experiment should have vec:b");
    assert_eq!(
        fork_vec.data.embedding,
        vec![0.0, 0.0, 1.0],
        "experiment vector embedding should see its own mutations"
    );
    let fork_node = db
        .graph_get_node(GRAPH, "n:b")
        .unwrap()
        .expect("experiment should have n:b");
    let fork_p = match &fork_node {
        Value::Object(map) => match map.get("properties") {
            Some(Value::Object(props)) => props.get("p").cloned(),
            _ => None,
        },
        _ => None,
    };
    assert_eq!(
        fork_p,
        Some(Value::Int(99)),
        "experiment graph node should see its own mutations, got {:?}",
        fork_node
    );
}

// =============================================================================
// Test 4 — edge cases (as_of=0, u64::MAX, same-microsecond writes)
// =============================================================================

#[test]
fn cross_primitive_as_of_zero_returns_empty() {
    let db = create_strata();
    init_schema(&db);

    // Seed after-the-fact; as_of=0 should still see nothing since every
    // write has a timestamp > 0.
    seed_round(&db, "z", [1.0, 0.0, 0.0], 1);

    assert!(db.kv_get_at("kv:z", Some(0)).unwrap().is_none());
    assert!(db.json_get_at("doc:z", "$", Some(0)).unwrap().is_none());
    assert!(db
        .vector_get_at(COLLECTION, "vec:z", Some(0))
        .unwrap()
        .is_none());
    assert!(db.graph_get_node_at(GRAPH, "n:z", 0).unwrap().is_none());
    assert_eq!(db.event_len_at(0).unwrap(), 0);
    assert!(db.event_list_types_at(0).unwrap().is_empty());
}

#[test]
fn cross_primitive_as_of_max_returns_current() {
    let db = create_strata();
    init_schema(&db);

    seed_round(&db, "m", [1.0, 0.0, 0.0], 1);
    seed_round(&db, "m", [0.0, 1.0, 0.0], 2);

    // as_of=u64::MAX must match the current state for every primitive.
    assert_kv_marker(&db, "kv:m", u64::MAX, 2);
    assert_json_marker(&db, "doc:m", u64::MAX, 2);
    assert_vector_embedding(&db, COLLECTION, "vec:m", u64::MAX, [0.0, 1.0, 0.0]);
    assert_graph_marker(&db, GRAPH, "n:m", u64::MAX, 2);
    assert_eq!(db.event_len_at(u64::MAX).unwrap(), 2);
    let mut types = db.event_list_types_at(u64::MAX).unwrap();
    types.sort();
    assert_eq!(types, vec!["audit"]);
}

#[test]
fn cross_primitive_rapid_writes_ordering() {
    // Write a tight burst of KV updates to the same key. Multiple writes
    // MAY share a microsecond timestamp on fast hardware; this test
    // verifies that:
    //   1. kv_getv returns all writes regardless of timestamp collisions
    //   2. current read returns the last write (newest-first ordering
    //      wins even when timestamps tie)
    //   3. Ranging back by history index yields writes in reverse order
    // If no two writes happen to share a microsecond on this run, the
    // test still passes — but it logs whether any collisions occurred
    // so regressions in the storage layer's tie-breaker are detectable.
    let db = create_strata();

    const N: i64 = 200;
    for i in 0..N {
        db.kv_put("rapid:k", Value::Int(i)).unwrap();
    }

    // The current read must return the last value written.
    let current = db.kv_get("rapid:k").unwrap();
    assert_eq!(current, Some(Value::Int(N - 1)));

    // Full history must contain every write (no version-chain drops even
    // when timestamps collide).
    let history = db
        .kv_getv("rapid:k")
        .unwrap()
        .expect("history should exist");
    assert_eq!(
        history.len(),
        N as usize,
        "kv_getv should return all {} versions regardless of timestamp ties",
        N
    );

    // Newest-first ordering: history[0] must be the last write regardless
    // of any timestamp collisions. This is the core invariant the test
    // is checking — version-chain order wins over timestamp ties.
    assert_eq!(
        history[0].value,
        Value::Int(N - 1),
        "newest history entry should be the last write"
    );
    assert_eq!(
        history[N as usize - 1].value,
        Value::Int(0),
        "oldest history entry should be the first write"
    );

    // Ensure every intermediate version is also present and in reverse
    // order, so a version-chain bug that scrambles ordering would fail.
    for (idx, entry) in history.iter().enumerate() {
        let expected = N - 1 - (idx as i64);
        assert_eq!(
            entry.value,
            Value::Int(expected),
            "history[{}] expected Int({}), got {:?}",
            idx,
            expected,
            entry.value
        );
    }

    // Count how many adjacent pairs share a timestamp — documents whether
    // the test actually exercised the same-microsecond branch on this
    // hardware. On fast CI we typically see many collisions. This is
    // informational only (no assertion), since it depends on clock
    // resolution and scheduler timing.
    let collisions = history
        .windows(2)
        .filter(|w| w[0].timestamp == w[1].timestamp)
        .count();
    eprintln!(
        "rapid_writes: {}/{} adjacent pairs shared a microsecond timestamp",
        collisions,
        N as usize - 1
    );

    // With as_of = u64::MAX, the filter picks the newest version whose
    // timestamp <= u64::MAX — which is the last write.
    assert_kv_marker(&db, "rapid:k", u64::MAX, N - 1);
}
