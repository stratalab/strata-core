//! Durability Mode Equivalence Tests
//!
//! Verifies that in-memory, standard, and always modes produce
//! semantically identical results for the same workload.

use crate::common::*;

#[test]
fn kv_operations_equivalent_across_modes() {
    test_across_modes("kv_put_get", |db| {
        let branch_id = BranchId::new();
        let kv = KVStore::new(db);

        kv.put(&branch_id, "default", "a", Value::Int(1)).unwrap();
        kv.put(&branch_id, "default", "b", Value::Int(2)).unwrap();
        kv.put(&branch_id, "default", "c", Value::Int(3)).unwrap();

        let a = kv.get(&branch_id, "default", "a").unwrap();
        let b = kv.get(&branch_id, "default", "b").unwrap();
        let c = kv.get(&branch_id, "default", "c").unwrap();
        (a, b, c)
    });
}

#[test]
fn json_operations_equivalent_across_modes() {
    test_across_modes("json_create_get", |db| {
        let branch_id = BranchId::new();
        let json = JsonStore::new(db);
        let doc_id = "mode_test_doc";

        json.create(
            &branch_id,
            "default",
            doc_id,
            json_value(serde_json::json!({"x": 1})),
        )
        .unwrap();

        let doc = json.get(&branch_id, "default", doc_id, &root()).unwrap();
        doc.map(|v| v.as_inner().clone())
    });
}

#[test]
fn event_operations_equivalent_across_modes() {
    test_across_modes("event_append_read", |db| {
        let branch_id = BranchId::new();
        let event = EventLog::new(db);

        event
            .append(&branch_id, "default", "stream", int_payload(1))
            .unwrap();
        event
            .append(&branch_id, "default", "stream", int_payload(2))
            .unwrap();
        event
            .append(&branch_id, "default", "stream", int_payload(3))
            .unwrap();

        let events = event
            .get_by_type(&branch_id, "default", "stream", None, None)
            .unwrap();
        events.len() as u64
    });
}

#[test]
fn overwrite_semantics_equivalent_across_modes() {
    test_across_modes("overwrite", |db| {
        let branch_id = BranchId::new();
        let kv = KVStore::new(db);

        kv.put(&branch_id, "default", "key", Value::Int(1)).unwrap();
        kv.put(&branch_id, "default", "key", Value::Int(2)).unwrap();
        kv.put(&branch_id, "default", "key", Value::Int(3)).unwrap();

        kv.get(&branch_id, "default", "key").unwrap()
    });
}

#[test]
fn delete_semantics_equivalent_across_modes() {
    test_across_modes("delete", |db| {
        let branch_id = BranchId::new();
        let kv = KVStore::new(db);

        kv.put(&branch_id, "default", "ephemeral", Value::Int(1))
            .unwrap();
        kv.delete(&branch_id, "default", "ephemeral").unwrap();

        kv.get(&branch_id, "default", "ephemeral")
            .unwrap()
            .is_none()
    });
}

#[test]
fn standard_mode_recovers_after_restart() {
    let mut test_db = TestDb::new(); // standard mode
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    for i in 0..20 {
        kv.put(&branch_id, "default", &format!("buf_{}", i), Value::Int(i))
            .unwrap();
    }

    let state_before = CapturedState::capture(&test_db.db, &branch_id);

    // Reopen (triggers flush + recovery)
    test_db.reopen();

    let state_after = CapturedState::capture(&test_db.db, &branch_id);
    assert_states_equal(
        &state_before,
        &state_after,
        "Standard mode should recover all data",
    );
}

#[test]
fn always_mode_recovers_after_restart() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    for i in 0..20 {
        kv.put(
            &branch_id,
            "default",
            &format!("strict_{}", i),
            Value::Int(i),
        )
        .unwrap();
    }

    let state_before = CapturedState::capture(&test_db.db, &branch_id);

    test_db.reopen();

    let state_after = CapturedState::capture(&test_db.db, &branch_id);
    assert_states_equal(
        &state_before,
        &state_after,
        "Always mode should recover all data",
    );
}
