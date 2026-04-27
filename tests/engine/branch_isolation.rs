//! Branch Isolation Tests
//!
//! Tests that verify data isolation between different branches.

use crate::common::*;
use std::collections::HashMap;
use strata_engine::JsonPath;

/// Helper to create an event payload object
fn event_payload(data: Value) -> Value {
    Value::object(HashMap::from([("data".to_string(), data)]))
}

// ============================================================================
// KVStore Isolation
// ============================================================================

#[test]
fn kv_branches_are_isolated() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // Same key, different branches
    kv.put(&branch_a, "default", "key", Value::Int(1)).unwrap();
    kv.put(&branch_b, "default", "key", Value::Int(2)).unwrap();

    // Each branch sees its own value
    assert_eq!(
        kv.get(&branch_a, "default", "key").unwrap().unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        kv.get(&branch_b, "default", "key").unwrap().unwrap(),
        Value::Int(2)
    );
}

#[test]
fn kv_delete_doesnt_affect_other_branch() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    kv.put(&branch_a, "default", "key", Value::Int(1)).unwrap();
    kv.put(&branch_b, "default", "key", Value::Int(2)).unwrap();

    kv.delete(&branch_a, "default", "key").unwrap();

    // Branch A's key is gone
    assert!(kv.get(&branch_a, "default", "key").unwrap().is_none());

    // Branch B's key still exists
    assert_eq!(
        kv.get(&branch_b, "default", "key").unwrap().unwrap(),
        Value::Int(2)
    );
}

#[test]
fn kv_list_only_shows_branch_keys() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    kv.put(&branch_a, "default", "a1", Value::Int(1)).unwrap();
    kv.put(&branch_a, "default", "a2", Value::Int(2)).unwrap();
    kv.put(&branch_b, "default", "b1", Value::Int(3)).unwrap();

    let keys_a = kv.list(&branch_a, "default", None).unwrap();
    let keys_b = kv.list(&branch_b, "default", None).unwrap();

    assert_eq!(keys_a.len(), 2);
    assert_eq!(keys_b.len(), 1);

    assert!(keys_a.contains(&"a1".to_string()));
    assert!(keys_a.contains(&"a2".to_string()));
    assert!(keys_b.contains(&"b1".to_string()));
}

// ============================================================================
// EventLog Isolation
// ============================================================================

#[test]
fn eventlog_branches_are_isolated() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    event
        .append(
            &branch_a,
            "default",
            "type",
            event_payload(Value::String("branch_a".into())),
        )
        .unwrap();
    event
        .append(
            &branch_a,
            "default",
            "type",
            event_payload(Value::String("branch_a_2".into())),
        )
        .unwrap();
    event
        .append(
            &branch_b,
            "default",
            "type",
            event_payload(Value::String("branch_b".into())),
        )
        .unwrap();

    assert_eq!(event.len(&branch_a, "default").unwrap(), 2);
    assert_eq!(event.len(&branch_b, "default").unwrap(), 1);
}

#[test]
fn eventlog_sequence_numbers_per_branch() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // Both branches start sequence at 0
    let seq_a = event
        .append(&branch_a, "default", "type", event_payload(Value::Int(1)))
        .unwrap();
    let seq_b = event
        .append(&branch_b, "default", "type", event_payload(Value::Int(1)))
        .unwrap();

    assert_eq!(seq_a.as_u64(), 0);
    assert_eq!(seq_b.as_u64(), 0);
}

#[test]
fn eventlog_independent_per_branch() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    for i in 0..5 {
        event
            .append(&branch_a, "default", "type", event_payload(Value::Int(i)))
            .unwrap();
        event
            .append(
                &branch_b,
                "default",
                "type",
                event_payload(Value::Int(i * 10)),
            )
            .unwrap();
    }

    // Both branches should have 5 events independently
    assert_eq!(event.len(&branch_a, "default").unwrap(), 5);
    assert_eq!(event.len(&branch_b, "default").unwrap(), 5);

    // Events should be readable independently
    let a_event = event.get(&branch_a, "default", 0).unwrap();
    let b_event = event.get(&branch_b, "default", 0).unwrap();
    assert_eq!(a_event.as_ref().unwrap().value.event_type, "type");
    assert_eq!(b_event.as_ref().unwrap().value.event_type, "type");
}

// ============================================================================
// JsonStore Isolation
// ============================================================================

#[test]
fn jsonstore_branches_are_isolated() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    json.create(
        &branch_a,
        "default",
        "doc",
        serde_json::json!({"branch": "a"}).into(),
    )
    .unwrap();
    json.create(
        &branch_b,
        "default",
        "doc",
        serde_json::json!({"branch": "b"}).into(),
    )
    .unwrap();

    let a_doc = json
        .get(&branch_a, "default", "doc", &JsonPath::root())
        .unwrap()
        .unwrap();
    let b_doc = json
        .get(&branch_b, "default", "doc", &JsonPath::root())
        .unwrap()
        .unwrap();

    assert_eq!(a_doc["branch"], "a");
    assert_eq!(b_doc["branch"], "b");
}

#[test]
fn jsonstore_count_per_branch() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    json.create(&branch_a, "default", "doc1", serde_json::json!({}).into())
        .unwrap();
    json.create(&branch_a, "default", "doc2", serde_json::json!({}).into())
        .unwrap();
    json.create(&branch_b, "default", "doc1", serde_json::json!({}).into())
        .unwrap();

    assert_eq!(
        json.list(&branch_a, "default", None, None, 1000)
            .unwrap()
            .doc_ids
            .len(),
        2
    );
    assert_eq!(
        json.list(&branch_b, "default", None, None, 1000)
            .unwrap()
            .doc_ids
            .len(),
        1
    );
}

// ============================================================================
// VectorStore Isolation
// ============================================================================

#[test]
fn vectorstore_collections_per_branch() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    let config = config_small();

    // Same collection name, different branches
    vector
        .create_collection(branch_a, "default", "coll", config.clone())
        .unwrap();
    vector
        .create_collection(branch_b, "default", "coll", config.clone())
        .unwrap();

    // Both exist independently
    let a_colls = vector.list_collections(branch_a, "default").unwrap();
    let b_colls = vector.list_collections(branch_b, "default").unwrap();
    assert!(a_colls.iter().any(|c| c.name == "coll"));
    assert!(b_colls.iter().any(|c| c.name == "coll"));

    // Delete from branch A doesn't affect branch B
    vector
        .delete_collection(branch_a, "default", "coll")
        .unwrap();

    let a_colls = vector.list_collections(branch_a, "default").unwrap();
    let b_colls = vector.list_collections(branch_b, "default").unwrap();
    assert!(!a_colls.iter().any(|c| c.name == "coll"));
    assert!(b_colls.iter().any(|c| c.name == "coll"));
}

#[test]
fn vectorstore_vectors_per_branch() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    let config = config_small();
    vector
        .create_collection(branch_a, "default", "coll", config.clone())
        .unwrap();
    vector
        .create_collection(branch_b, "default", "coll", config.clone())
        .unwrap();

    // Insert different vectors with same key
    vector
        .insert(
            branch_a,
            "default",
            "coll",
            "vec",
            &[1.0f32, 0.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            branch_b,
            "default",
            "coll",
            "vec",
            &[0.0f32, 1.0, 0.0],
            None,
        )
        .unwrap();

    let a_vec = vector
        .get(branch_a, "default", "coll", "vec")
        .unwrap()
        .unwrap();
    let b_vec = vector
        .get(branch_b, "default", "coll", "vec")
        .unwrap()
        .unwrap();

    assert_eq!(a_vec.value.embedding, vec![1.0f32, 0.0, 0.0]);
    assert_eq!(b_vec.value.embedding, vec![0.0f32, 1.0, 0.0]);
}

// ============================================================================
// Cross-Primitive Branch Isolation
// ============================================================================

#[test]
fn all_primitives_isolated_by_branch() {
    let test_db = TestDb::new();
    let prims = test_db.all_primitives();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // Write to all primitives in branch A
    prims
        .kv
        .put(&branch_a, "default", "key", Value::Int(1))
        .unwrap();
    prims
        .event
        .append(&branch_a, "default", "type", event_payload(Value::Int(1)))
        .unwrap();
    prims
        .json
        .create(
            &branch_a,
            "default",
            "doc",
            serde_json::json!({"n": 1}).into(),
        )
        .unwrap();

    // Branch B should see nothing
    assert!(prims.kv.get(&branch_b, "default", "key").unwrap().is_none());
    assert_eq!(prims.event.len(&branch_b, "default").unwrap(), 0);
    assert!(!prims.json.exists(&branch_b, "default", "doc").unwrap());
}

#[test]
fn many_branches_no_interference() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    // Create 10 branches, each with its own data
    let branches: Vec<BranchId> = (0..10).map(|_| BranchId::new()).collect();

    for (i, branch_id) in branches.iter().enumerate() {
        kv.put(branch_id, "default", "data", Value::Int(i as i64))
            .unwrap();
    }

    // Each branch sees only its own data
    for (i, branch_id) in branches.iter().enumerate() {
        let val = kv.get(branch_id, "default", "data").unwrap().unwrap();
        assert_eq!(val, Value::Int(i as i64));
    }
}
