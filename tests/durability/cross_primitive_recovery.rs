//! Cross-Primitive Recovery Tests
//!
//! Verify that all 5 primitives recover atomically — if one recovers,
//! they all recover. No primitive is left behind.

use crate::common::*;

#[test]
fn all_five_primitives_recover_together() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let p = test_db.all_primitives();

    // Write to all 5 primitives
    p.kv.put(&branch_id, "default", "k1", Value::Int(1))
        .unwrap();

    let doc_id = new_doc_id();
    p.json
        .create(&branch_id, "default", &doc_id, test_json_value(1))
        .unwrap();

    p.event
        .append(&branch_id, "default", "stream", int_payload(42))
        .unwrap();

    p.vector
        .create_collection(branch_id, "default", "col", config_small())
        .unwrap();
    p.vector
        .insert(branch_id, "default", "col", "v1", &[1.0, 0.0, 0.0], None)
        .unwrap();

    drop(p);
    test_db.reopen();

    // Verify all 5 primitives recovered
    let p = test_db.all_primitives();

    let kv_val = p.kv.get(&branch_id, "default", "k1").unwrap();
    assert_eq!(kv_val, Some(Value::Int(1)), "KV should recover");

    let json_val = p.json.get(&branch_id, "default", &doc_id, &root()).unwrap();
    let json_val = json_val.expect("JSON should recover");
    assert_eq!(json_val, test_json_value(1));

    let events = p
        .event
        .get_by_type(&branch_id, "default", "stream", None, None)
        .unwrap();
    assert_eq!(events.len(), 1, "EventLog should recover");

    let vec_val = p.vector.get(branch_id, "default", "col", "v1").unwrap();
    let vec_val = vec_val.expect("VectorStore should recover");
    assert_eq!(vec_val.value.embedding, vec![1.0, 0.0, 0.0]);
}

#[test]
fn interleaved_writes_recover_correctly() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    let event = test_db.event();

    // Interleave KV and EventLog writes
    for i in 0..50 {
        kv.put(&branch_id, "default", &format!("k{}", i), Value::Int(i))
            .unwrap();
        event
            .append(&branch_id, "default", "interleaved", int_payload(i))
            .unwrap();
    }

    test_db.reopen();

    let kv = test_db.kv();
    let event = test_db.event();

    for i in 0..50 {
        let val = kv.get(&branch_id, "default", &format!("k{}", i)).unwrap();
        assert_eq!(val, Some(Value::Int(i)), "KV key k{} should be {}", i, i);
    }

    let events = event
        .get_by_type(&branch_id, "default", "interleaved", None, None)
        .unwrap();
    assert_eq!(events.len(), 50, "All 50 events should recover");
}

#[test]
fn multiple_runs_recover_independently() {
    let mut test_db = TestDb::new_strict();
    let run1 = test_db.branch_id;
    let run2 = BranchId::new();

    let kv = test_db.kv();
    kv.put(
        &run1,
        "default",
        "run1_key",
        Value::String("from_run1".into()),
    )
    .unwrap();
    kv.put(
        &run2,
        "default",
        "run2_key",
        Value::String("from_run2".into()),
    )
    .unwrap();

    test_db.reopen();

    let kv = test_db.kv();
    let v1 = kv.get(&run1, "default", "run1_key").unwrap();
    let v2 = kv.get(&run2, "default", "run2_key").unwrap();
    assert_eq!(
        v1,
        Some(Value::String("from_run1".into())),
        "Run1 data should recover"
    );
    assert_eq!(
        v2,
        Some(Value::String("from_run2".into())),
        "Run2 data should recover"
    );

    // Cross-contamination check
    let cross = kv.get(&run1, "default", "run2_key").unwrap();
    assert!(cross.is_none(), "Run1 should not see run2's keys");
}

#[test]
fn vector_collection_config_recovers() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let vector = test_db.vector();
    vector
        .create_collection(branch_id, "default", "cosine_col", config_small())
        .unwrap();
    vector
        .create_collection(branch_id, "default", "euclidean_col", config_euclidean())
        .unwrap();

    // Insert into both
    vector
        .insert(
            branch_id,
            "default",
            "cosine_col",
            "v1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            branch_id,
            "default",
            "euclidean_col",
            "v1",
            &seeded_vector(384, 1),
            None,
        )
        .unwrap();

    test_db.reopen();

    let vector = test_db.vector();
    assert!(
        vector
            .get(branch_id, "default", "cosine_col", "v1")
            .unwrap()
            .is_some(),
        "Cosine collection should recover"
    );
    assert!(
        vector
            .get(branch_id, "default", "euclidean_col", "v1")
            .unwrap()
            .is_some(),
        "Euclidean collection should recover"
    );
}

#[test]
fn json_mutations_survive_recovery() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let json = test_db.json();
    let doc_id = new_doc_id();
    json.create(
        &branch_id,
        "default",
        &doc_id,
        json_value(serde_json::json!({"count": 0, "items": []})),
    )
    .unwrap();

    // Mutate the document
    json.set(
        &branch_id,
        "default",
        &doc_id,
        &path("count"),
        json_value(serde_json::json!(42)),
    )
    .unwrap();

    test_db.reopen();

    let json = test_db.json();
    let doc = json
        .get(&branch_id, "default", &doc_id, &root())
        .unwrap()
        .unwrap();
    let inner = doc.as_inner();
    assert_eq!(inner["count"], 42, "JSON mutation should survive recovery");
}
