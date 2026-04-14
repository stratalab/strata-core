//! Durability Mode Tests
//!
//! Tests that operations produce the same results across all durability modes.
//! Only persistence behavior should differ.
//! Modes: Cache (no sync), Standard (periodic sync), Always (immediate sync).

use crate::common::*;
use std::collections::HashMap;
use strata_core::primitives::json::JsonPath;
use strata_engine::database::OpenSpec;
use strata_engine::KVStoreExt;

/// Helper to create an event payload object
fn event_payload(data: Value) -> Value {
    Value::object(HashMap::from([("data".to_string(), data)]))
}

// ============================================================================
// Mode Equivalence
// ============================================================================

#[test]
fn kv_put_get_same_across_modes() {
    test_across_modes("kv_put_get", |db| {
        let branch_id = BranchId::new();
        let kv = KVStore::new(db);

        kv.put(&branch_id, "default", "key", Value::Int(42))
            .unwrap();

        kv.get(&branch_id, "default", "key").unwrap()
    });
}

#[test]
fn kv_delete_same_across_modes() {
    test_across_modes("kv_delete", |db| {
        let branch_id = BranchId::new();
        let kv = KVStore::new(db);

        kv.put(&branch_id, "default", "key", Value::Int(1)).unwrap();
        let deleted = kv.delete(&branch_id, "default", "key").unwrap();

        (
            deleted,
            kv.get(&branch_id, "default", "key").unwrap().is_none(),
        )
    });
}

#[test]
fn eventlog_append_same_across_modes() {
    test_across_modes("eventlog_append", |db| {
        let branch_id = BranchId::new();
        let event = EventLog::new(db);

        event
            .append(
                &branch_id,
                "default",
                "test_type",
                event_payload(Value::String("payload".into())),
            )
            .unwrap();
        let len = event.len(&branch_id, "default").unwrap();
        let first = event.get(&branch_id, "default", 0).unwrap();

        (len, first.map(|e| e.value.event_type.clone()))
    });
}

#[test]
fn json_create_get_same_across_modes() {
    test_across_modes("json_create_get", |db| {
        let branch_id = BranchId::new();
        let json = JsonStore::new(db);

        let doc_value = serde_json::json!({"name": "test", "count": 42});
        json.create(&branch_id, "default", "doc1", doc_value.clone().into())
            .unwrap();

        let result = json
            .get(&branch_id, "default", "doc1", &JsonPath::root())
            .unwrap();

        // Return serialized JSON for comparison
        result.map(|v| serde_json::to_string(&v).unwrap_or_default())
    });
}

// ============================================================================
// Mode-Specific Behavior
// ============================================================================

#[test]
fn cache_mode_is_cache() {
    // Database::open_runtime(OpenSpec::cache()) creates a truly in-memory database with no files
    let db = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem))
        .expect("cache database");
    assert!(db.is_cache());
}

#[test]
fn cache_create_test_db_is_cache() {
    // create_test_db() uses Database::cache() which is truly in-memory
    let db = create_test_db();
    assert!(db.is_cache());
}

#[test]
fn standard_mode_is_persistent() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path()).with_subsystem(SearchSubsystem),
    )
    .expect("standard database");

    // Standard mode is NOT cache (has durability)
    assert!(!db.is_cache());
}

#[test]
fn always_mode_is_persistent() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path())
            .with_config(always_config())
            .with_subsystem(SearchSubsystem),
    )
    .expect("always database");

    assert!(!db.is_cache());
}

// ============================================================================
// Cross-Mode Transaction Semantics
// ============================================================================

#[test]
fn transaction_atomicity_in_memory() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;

    // Atomic transaction using extension trait
    test_db
        .db
        .transaction(branch_id, |txn| {
            txn.kv_put("a", Value::Int(1))?;
            txn.kv_put("b", Value::Int(2))?;
            Ok(())
        })
        .unwrap();

    let kv = test_db.kv();
    assert_eq!(
        kv.get(&branch_id, "default", "a").unwrap(),
        Some(Value::Int(1))
    );
    assert_eq!(
        kv.get(&branch_id, "default", "b").unwrap(),
        Some(Value::Int(2))
    );
}

#[test]
fn transaction_atomicity_standard() {
    let test_db = TestDb::new(); // TestDb::new() uses temp dir with durability
    let branch_id = test_db.branch_id;

    test_db
        .db
        .transaction(branch_id, |txn| {
            txn.kv_put("a", Value::Int(1))?;
            txn.kv_put("b", Value::Int(2))?;
            Ok(())
        })
        .unwrap();

    let kv = test_db.kv();
    assert_eq!(
        kv.get(&branch_id, "default", "a").unwrap(),
        Some(Value::Int(1))
    );
    assert_eq!(
        kv.get(&branch_id, "default", "b").unwrap(),
        Some(Value::Int(2))
    );
}

#[test]
fn transaction_atomicity_always() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path())
            .with_config(always_config())
            .with_subsystem(SearchSubsystem),
    )
    .expect("always database");
    let branch_id = BranchId::new();

    db.transaction(branch_id, |txn| {
        txn.kv_put("a", Value::Int(1))?;
        txn.kv_put("b", Value::Int(2))?;
        Ok(())
    })
    .unwrap();

    let kv = KVStore::new(db);
    assert_eq!(
        kv.get(&branch_id, "default", "a").unwrap(),
        Some(Value::Int(1))
    );
    assert_eq!(
        kv.get(&branch_id, "default", "b").unwrap(),
        Some(Value::Int(2))
    );
}

// ============================================================================
// Multi-Primitive Consistency
// ============================================================================

#[test]
fn all_primitives_work_in_all_modes() {
    test_across_modes("all_primitives", |db| {
        let branch_id = BranchId::new();

        let kv = KVStore::new(db.clone());
        let event = EventLog::new(db.clone());
        let json = JsonStore::new(db.clone());
        let branch_idx = BranchIndex::new(db.clone());

        // KV
        kv.put(&branch_id, "default", "k", Value::Int(1)).unwrap();

        // Event
        event
            .append(&branch_id, "default", "e", event_payload(Value::Int(2)))
            .unwrap();

        // JSON
        json.create(
            &branch_id,
            "default",
            "j",
            serde_json::json!({"x": 4}).into(),
        )
        .unwrap();

        // BranchIndex
        branch_idx.create_branch("test_branch").unwrap();

        // All succeeded
        true
    });
}
