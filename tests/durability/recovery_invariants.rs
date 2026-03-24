//! Recovery Invariant Tests
//!
//! Tests the five fundamental recovery guarantees:
//! 1. No committed data is lost
//! 2. Uncommitted data may be dropped
//! 3. No data is invented
//! 4. Recovery is idempotent
//! 5. Recovery is deterministic

use crate::common::*;

// ============================================================================
// Invariant 1: Committed data survives restart
// ============================================================================

#[test]
fn committed_kv_data_survives_restart() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    for i in 0..100 {
        kv.put(&branch_id, "default", &format!("key_{}", i), Value::Int(i))
            .unwrap();
    }

    let state_before = CapturedState::capture(&test_db.db, &branch_id);

    test_db.reopen();

    let state_after = CapturedState::capture(&test_db.db, &branch_id);
    assert_states_equal(&state_before, &state_after, "KV data lost after restart");
}

#[test]
fn committed_json_data_survives_restart() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let json = test_db.json();
    let doc_id = new_doc_id();
    json.create(&branch_id, "default", &doc_id, test_json_value(42))
        .unwrap();

    test_db.reopen();

    let json = test_db.json();
    let doc = json.get(&branch_id, "default", &doc_id, &root()).unwrap();
    let doc = doc.expect("JSON document should survive restart");
    assert_eq!(doc, test_json_value(42));
}

#[test]
fn committed_event_data_survives_restart() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let event = test_db.event();
    for i in 0..10 {
        event
            .append(&branch_id, "default", "test_stream", int_payload(i))
            .unwrap();
    }

    test_db.reopen();

    let event = test_db.event();
    let events = event
        .get_by_type(&branch_id, "default", "test_stream", None, None)
        .unwrap();
    assert_eq!(events.len(), 10, "All events should survive restart");
}

#[test]
fn committed_vector_data_survives_restart() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let vector = test_db.vector();
    vector
        .create_collection(branch_id, "default", "embeddings", config_small())
        .unwrap();
    vector
        .insert(
            branch_id,
            "default",
            "embeddings",
            "vec_1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            branch_id,
            "default",
            "embeddings",
            "vec_2",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();

    test_db.reopen();

    let vector = test_db.vector();
    let v1 = vector
        .get(branch_id, "default", "embeddings", "vec_1")
        .unwrap();
    let v2 = vector
        .get(branch_id, "default", "embeddings", "vec_2")
        .unwrap();
    let v1 = v1.expect("Vector vec_1 should survive restart");
    assert_eq!(v1.value.embedding, vec![1.0, 0.0, 0.0]);
    let v2 = v2.expect("Vector vec_2 should survive restart");
    assert_eq!(v2.value.embedding, vec![0.0, 1.0, 0.0]);
}

// ============================================================================
// Invariant 2: Recovery does not invent data
// ============================================================================

#[test]
fn recovery_does_not_invent_keys() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    kv.put(&branch_id, "default", "real_key", Value::Int(1))
        .unwrap();

    test_db.reopen();

    let kv = test_db.kv();
    let phantom = kv.get(&branch_id, "default", "never_written").unwrap();
    assert!(phantom.is_none(), "Recovery must not invent data");

    let real = kv.get(&branch_id, "default", "real_key").unwrap();
    assert_eq!(real, Some(Value::Int(1)));
}

// ============================================================================
// Invariant 3: Recovery is idempotent
// ============================================================================

#[test]
fn double_reopen_produces_same_state() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    for i in 0..50 {
        kv.put(&branch_id, "default", &format!("k{}", i), Value::Int(i))
            .unwrap();
    }

    test_db.reopen();
    let state_after_first = CapturedState::capture(&test_db.db, &branch_id);

    test_db.reopen();
    let state_after_second = CapturedState::capture(&test_db.db, &branch_id);

    assert_states_equal(
        &state_after_first,
        &state_after_second,
        "Double reopen should produce identical state",
    );
}

#[test]
fn triple_reopen_is_stable() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    for i in 0..20 {
        kv.put(&branch_id, "default", &format!("k{}", i), Value::Int(i))
            .unwrap();
    }

    test_db.reopen();
    test_db.reopen();
    test_db.reopen();

    let state = CapturedState::capture(&test_db.db, &branch_id);
    assert_eq!(
        state.kv_entries.len(),
        20,
        "All keys must survive 3 reopens"
    );
}

// ============================================================================
// Invariant 4: Recovery is deterministic
// ============================================================================

#[test]
fn recovery_produces_deterministic_state() {
    let mut db1 = TestDb::new_strict();
    let mut db2 = TestDb::new_strict();
    let branch_id = db1.branch_id;

    for i in 0..30 {
        let key = format!("det_{}", i);
        let val = Value::Int(i * 7);
        db1.kv()
            .put(&branch_id, "default", &key, val.clone())
            .unwrap();
        db2.kv().put(&branch_id, "default", &key, val).unwrap();
    }

    db1.reopen();
    db2.reopen();

    let s1 = CapturedState::capture(&db1.db, &branch_id);
    let s2 = CapturedState::capture(&db2.db, &branch_id);

    assert_eq!(s1.kv_entries.len(), s2.kv_entries.len());
    for (key, val1) in &s1.kv_entries {
        let val2 = s2
            .kv_entries
            .get(key)
            .unwrap_or_else(|| panic!("Key {} missing in second db", key));
        assert_eq!(
            val1, val2,
            "Value for key {} differs between databases",
            key
        );
    }
}

// ============================================================================
// Invariant 5: Last-write-wins after recovery
// ============================================================================

#[test]
fn overwrites_are_respected_after_recovery() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    kv.put(&branch_id, "default", "overwrite_key", Value::Int(1))
        .unwrap();
    kv.put(&branch_id, "default", "overwrite_key", Value::Int(2))
        .unwrap();
    kv.put(&branch_id, "default", "overwrite_key", Value::Int(3))
        .unwrap();

    test_db.reopen();

    let kv = test_db.kv();
    let val = kv
        .get(&branch_id, "default", "overwrite_key")
        .unwrap()
        .unwrap();
    assert_eq!(val, Value::Int(3), "Last write should win after recovery");
}

#[test]
fn deletes_are_respected_after_recovery() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    kv.put(&branch_id, "default", "delete_me", Value::Int(1))
        .unwrap();
    kv.delete(&branch_id, "default", "delete_me").unwrap();

    test_db.reopen();

    let kv = test_db.kv();
    let val = kv.get(&branch_id, "default", "delete_me").unwrap();
    assert!(
        val.is_none(),
        "Deleted key should remain deleted after recovery"
    );
}
