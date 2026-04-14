//! Cross-Primitive Transaction Tests
//!
//! Tests verifying that multiple primitives can participate in atomic transactions.

use std::collections::HashMap;
use std::sync::Arc;
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_engine::database::OpenSpec;
use strata_engine::Database;
use strata_engine::SearchSubsystem;
use strata_engine::{EventLog, EventLogExt, KVStore, KVStoreExt};
use tempfile::TempDir;

/// Helper to create an empty object payload for EventLog
fn empty_payload() -> Value {
    Value::object(HashMap::new())
}

/// Helper to create an object payload with a string value
fn string_payload(s: &str) -> Value {
    Value::object(HashMap::from([(
        "data".to_string(),
        Value::String(s.into()),
    )]))
}

/// Helper to create an object payload with an integer value
fn int_payload(v: i64) -> Value {
    Value::object(HashMap::from([("value".to_string(), Value::Int(v))]))
}

fn setup() -> (Arc<Database>, TempDir, BranchId) {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path()).with_subsystem(SearchSubsystem),
    )
    .unwrap();
    let branch_id = BranchId::new();
    (db, temp_dir, branch_id)
}

/// Test that KV and Event operations work atomically in a single transaction
#[test]
fn test_kv_event_atomic() {
    let (db, _temp, branch_id) = setup();

    // Perform atomic transaction with KV and Event primitives
    let result = db.transaction(branch_id, |txn| {
        // KV operation
        txn.kv_put("task/status", Value::String("running".into()))?;

        // Event operation (sequences start at 0)
        let seq = txn.event_append("task_started", string_payload("payload"))?;
        assert_eq!(seq, 0);

        Ok(())
    });

    assert!(result.is_ok());

    // Verify all operations succeeded
    let kv = KVStore::new(db.clone());
    let event_log = EventLog::new(db.clone());

    assert_eq!(
        kv.get(&branch_id, "default", "task/status").unwrap(),
        Some(Value::String("running".into()))
    );
    assert_eq!(event_log.len(&branch_id, "default").unwrap(), 1);
}

/// Test that a failed operation causes full rollback of all primitives
#[test]
fn test_cross_primitive_rollback() {
    let (db, _temp, branch_id) = setup();

    // Attempt transaction that fails partway through
    let result: Result<(), strata_core::StrataError> = db.transaction(branch_id, |txn| {
        // KV put (should succeed alone)
        txn.kv_put("key_to_rollback", Value::Int(42))?;

        // Event append (should succeed alone)
        txn.event_append("event_to_rollback", empty_payload())?;

        // Force a failure
        Err(strata_core::StrataError::invalid_input("forced failure"))
    });

    // Transaction should have failed
    assert!(result.is_err());

    // Verify KV was NOT written (rollback affected all)
    let kv = KVStore::new(db.clone());
    assert!(kv
        .get(&branch_id, "default", "key_to_rollback")
        .unwrap()
        .is_none());

    // Verify Event was NOT written
    let event_log = EventLog::new(db.clone());
    assert_eq!(event_log.len(&branch_id, "default").unwrap(), 0);
}

/// Test that extension traits compose correctly in single transaction
#[test]
fn test_all_extension_traits_compose() {
    let (db, _temp, branch_id) = setup();

    // Use extension traits in single transaction
    let result = db.transaction(branch_id, |txn| {
        // KVStoreExt::kv_put()
        txn.kv_put("config", Value::String("enabled".into()))?;

        // EventLogExt::event_append() - sequences start at 0
        let seq = txn.event_append("config_changed", empty_payload())?;
        assert_eq!(seq, 0);

        Ok(())
    });

    assert!(result.is_ok());

    // Verify all succeeded
    let kv = KVStore::new(db.clone());
    let event_log = EventLog::new(db.clone());

    assert!(kv.get(&branch_id, "default", "config").unwrap().is_some());
    assert_eq!(event_log.len(&branch_id, "default").unwrap(), 1);
}

/// Test nested/chained primitive operations within single transaction
#[test]
fn test_nested_primitive_operations() {
    let (db, _temp, branch_id) = setup();

    // Pre-populate some KV data
    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "initial_value", Value::Int(42))
        .unwrap();

    // Chain operations: read KV -> use in Event
    let result = db.transaction(branch_id, |txn| {
        // Read KV -> use value in Event payload (wrapped in object)
        let kv_value = txn.kv_get("initial_value")?;
        let inner_value = kv_value.unwrap_or(Value::Null);
        // EventLog requires object payloads, so wrap the KV value
        let payload = Value::object(HashMap::from([("from_kv".to_string(), inner_value)]));

        // Append Event with payload from KV (sequence starts at 0)
        let seq = txn.event_append("chained_event", payload)?;

        Ok(seq)
    });

    assert!(result.is_ok());
    let seq = result.unwrap();
    assert_eq!(seq, 0); // Sequences start at 0

    // Verify causal chain worked
    let event_log = EventLog::new(db.clone());
    let event = event_log.get(&branch_id, "default", 0).unwrap().unwrap();
    // Payload is now wrapped: {"from_kv": 42}
    let expected_payload = Value::object(HashMap::from([("from_kv".to_string(), Value::Int(42))]));
    assert_eq!(event.value.payload, expected_payload);
}

/// Test multiple sequential transactions with all primitives
#[test]
fn test_multiple_transactions_consistency() {
    let (db, _temp, branch_id) = setup();

    // Run 10 sequential transactions
    for i in 1..=10 {
        let result = db.transaction(branch_id, |txn| {
            txn.kv_put(&format!("key_{}", i), Value::Int(i))?;
            txn.event_append("iteration", int_payload(i))?;
            Ok(())
        });
        assert!(result.is_ok());
    }

    // Verify final state
    let kv = KVStore::new(db.clone());
    let event_log = EventLog::new(db.clone());

    // All 10 KV entries exist
    for i in 1..=10 {
        assert_eq!(
            kv.get(&branch_id, "default", &format!("key_{}", i))
                .unwrap(),
            Some(Value::Int(i))
        );
    }

    // 10 events
    assert_eq!(event_log.len(&branch_id, "default").unwrap(), 10);
}

/// Test read operations within transaction see uncommitted writes
#[test]
fn test_read_your_writes_in_transaction() {
    let (db, _temp, branch_id) = setup();

    let result = db.transaction(branch_id, |txn| {
        // Write KV
        txn.kv_put("test_key", Value::String("test_value".into()))?;

        // Read back within same transaction (read-your-writes)
        let value = txn.kv_get("test_key")?;
        assert_eq!(value, Some(Value::String("test_value".into())));

        // Append event (sequences start at 0)
        let seq1 = txn.event_append("event1", empty_payload())?;
        assert_eq!(seq1, 0);

        // Append another - sequence should continue
        let seq2 = txn.event_append("event2", empty_payload())?;
        assert_eq!(seq2, 1);

        Ok(())
    });

    assert!(result.is_ok());
}

/// Test transaction with only reads doesn't modify anything
#[test]
fn test_read_only_transaction() {
    let (db, _temp, branch_id) = setup();

    // Pre-populate data
    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "existing", Value::Int(100))
        .unwrap();

    // Read-only transaction
    let result = db.transaction(branch_id, |txn| {
        let kv_val = txn.kv_get("existing")?;
        assert_eq!(kv_val, Some(Value::Int(100)));

        Ok(())
    });

    assert!(result.is_ok());

    // Data unchanged
    assert_eq!(
        kv.get(&branch_id, "default", "existing").unwrap(),
        Some(Value::Int(100))
    );
}
