//! Cross-Primitive Transaction Tests
//!
//! Tests verifying that multiple primitives can participate in atomic transactions.

use std::collections::HashMap;
use std::sync::Arc;
use strata_core::contract::Version;
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_engine::Database;
use strata_engine::{EventLog, EventLogExt, KVStore, KVStoreExt, StateCell, StateCellExt};
use tempfile::TempDir;

/// Helper to create an empty object payload for EventLog
fn empty_payload() -> Value {
    Value::Object(Box::new(HashMap::new()))
}

/// Helper to create an object payload with a string value
fn string_payload(s: &str) -> Value {
    Value::Object(Box::new(HashMap::from([(
        "data".to_string(),
        Value::String(s.into()),
    )])))
}

/// Helper to create an object payload with an integer value
fn int_payload(v: i64) -> Value {
    Value::Object(Box::new(HashMap::from([(
        "value".to_string(),
        Value::Int(v),
    )])))
}

fn setup() -> (Arc<Database>, TempDir, BranchId) {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();
    let branch_id = BranchId::new();
    (db, temp_dir, branch_id)
}

/// Test that KV, Event, and State operations work atomically in a single transaction
#[test]
fn test_kv_event_state_atomic() {
    let (db, _temp, branch_id) = setup();

    // Initialize state cell first (needed for CAS)
    let state_cell = StateCell::new(db.clone());
    state_cell
        .init(&branch_id, "default", "workflow", Value::Int(0))
        .unwrap();

    // Perform atomic transaction with all 3 primitives
    let result = db.transaction(branch_id, |txn| {
        // KV operation
        txn.kv_put("task/status", Value::String("running".into()))?;

        // Event operation (sequences start at 0)
        let seq = txn.event_append("task_started", string_payload("payload"))?;
        assert_eq!(seq, 0);

        // State operation (CAS from version 1 after init)
        let new_version = txn.state_cas(
            "workflow",
            Version::counter(1),
            Value::String("step1".into()),
        )?;
        assert_eq!(new_version, Version::counter(2));

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

    let state = state_cell
        .get(&branch_id, "default", "workflow")
        .unwrap()
        .unwrap();
    assert_eq!(state, Value::String("step1".into()));
}

/// Test that a failed operation causes full rollback of all primitives
#[test]
fn test_cross_primitive_rollback() {
    let (db, _temp, branch_id) = setup();

    // Initialize state cell with version 1
    let state_cell = StateCell::new(db.clone());
    state_cell
        .init(&branch_id, "default", "cell", Value::Int(100))
        .unwrap();

    // Attempt transaction with wrong CAS version - should fail and rollback
    let result = db.transaction(branch_id, |txn| {
        // KV put (should succeed alone)
        txn.kv_put("key_to_rollback", Value::Int(42))?;

        // Event append (should succeed alone)
        txn.event_append("event_to_rollback", empty_payload())?;

        // StateCell CAS with WRONG version (should fail)
        // State is at version 1, but we try version 999
        txn.state_cas("cell", Version::counter(999), Value::Int(200))?;

        Ok(())
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

    // Verify StateCell unchanged
    let state = state_cell
        .get(&branch_id, "default", "cell")
        .unwrap()
        .unwrap();
    assert_eq!(state, Value::Int(100));
}

/// Test that all 3 extension traits compose correctly in single transaction
#[test]
fn test_all_extension_traits_compose() {
    let (db, _temp, branch_id) = setup();

    // Pre-initialize state cell
    let state_cell = StateCell::new(db.clone());
    state_cell
        .init(&branch_id, "default", "counter", Value::Int(0))
        .unwrap();

    // Use all 3 extension traits in single transaction
    let result = db.transaction(branch_id, |txn| {
        // KVStoreExt::kv_put()
        txn.kv_put("config", Value::String("enabled".into()))?;

        // EventLogExt::event_append() - sequences start at 0
        let seq = txn.event_append("config_changed", empty_payload())?;
        assert_eq!(seq, 0);

        // StateCellExt::state_set() (unconditional) - version 2 after init
        let version = txn.state_set("counter", Value::Int(1))?;
        assert_eq!(version, Version::counter(2));

        Ok(())
    });

    assert!(result.is_ok());

    // Verify all succeeded
    let kv = KVStore::new(db.clone());
    let event_log = EventLog::new(db.clone());

    assert!(kv.get(&branch_id, "default", "config").unwrap().is_some());
    assert_eq!(event_log.len(&branch_id, "default").unwrap(), 1);
    assert!(state_cell
        .get(&branch_id, "default", "counter")
        .unwrap()
        .is_some());
}

/// Test that partial failure in any primitive causes full rollback
#[test]
fn test_partial_failure_full_rollback() {
    let (db, _temp, branch_id) = setup();

    // Initialize state cell
    let state_cell = StateCell::new(db.clone());
    state_cell
        .init(&branch_id, "default", "state", Value::Int(0))
        .unwrap();

    // Write successfully to 2 primitives, then fail on 3rd
    let result = db.transaction(branch_id, |txn| {
        // 1. KV - success
        txn.kv_put("partial_key", Value::Int(1))?;

        // 2. Event - success
        txn.event_append("partial_event", empty_payload())?;

        // 3. State CAS with wrong version - FAILURE
        txn.state_cas("state", Version::counter(999), Value::Int(100))?;

        Ok(())
    });

    // Should fail
    assert!(result.is_err());

    // Verify ALL operations rolled back
    let kv = KVStore::new(db.clone());
    let event_log = EventLog::new(db.clone());

    assert!(kv
        .get(&branch_id, "default", "partial_key")
        .unwrap()
        .is_none());
    assert_eq!(event_log.len(&branch_id, "default").unwrap(), 0);

    let state = state_cell
        .get(&branch_id, "default", "state")
        .unwrap()
        .unwrap();
    assert_eq!(state, Value::Int(0)); // Unchanged
}

/// Test nested/chained primitive operations within single transaction
#[test]
fn test_nested_primitive_operations() {
    let (db, _temp, branch_id) = setup();

    // Pre-populate some KV data
    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "initial_value", Value::Int(42))
        .unwrap();

    // Initialize state
    let state_cell = StateCell::new(db.clone());
    state_cell
        .init(&branch_id, "default", "sequence_tracker", Value::Int(0))
        .unwrap();

    // Chain operations: read KV -> use in Event -> update State
    let result = db.transaction(branch_id, |txn| {
        // Read KV -> use value in Event payload (wrapped in object)
        let kv_value = txn.kv_get("initial_value")?;
        let inner_value = kv_value.unwrap_or(Value::Null);
        // EventLog requires object payloads, so wrap the KV value
        let payload = Value::Object(Box::new(HashMap::from([(
            "from_kv".to_string(),
            inner_value,
        )])));

        // Append Event with payload from KV (sequence starts at 0)
        let seq = txn.event_append("chained_event", payload)?;

        // Update State with sequence number
        let _version = txn.state_set("sequence_tracker", Value::Int(seq as i64))?;

        Ok(seq)
    });

    assert!(result.is_ok());
    let seq = result.unwrap();
    assert_eq!(seq, 0); // Sequences start at 0

    // Verify causal chain worked
    let event_log = EventLog::new(db.clone());
    let event = event_log.get(&branch_id, "default", 0).unwrap().unwrap();
    // Payload is now wrapped: {"from_kv": 42}
    let expected_payload = Value::Object(Box::new(HashMap::from([(
        "from_kv".to_string(),
        Value::Int(42),
    )])));
    assert_eq!(event.value.payload, expected_payload);

    let state = state_cell
        .get(&branch_id, "default", "sequence_tracker")
        .unwrap()
        .unwrap();
    assert_eq!(state, Value::Int(0)); // Sequence number (starts at 0)
}

/// Test multiple sequential transactions with all primitives
#[test]
fn test_multiple_transactions_consistency() {
    let (db, _temp, branch_id) = setup();

    // Initialize state
    let state_cell = StateCell::new(db.clone());
    state_cell
        .init(&branch_id, "default", "counter", Value::Int(0))
        .unwrap();

    // Run 10 sequential transactions
    for i in 1..=10 {
        let result = db.transaction(branch_id, |txn| {
            txn.kv_put(&format!("key_{}", i), Value::Int(i))?;
            txn.event_append("iteration", int_payload(i))?;
            txn.state_set("counter", Value::Int(i))?;
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

    // Counter at 10
    let state = state_cell
        .get(&branch_id, "default", "counter")
        .unwrap()
        .unwrap();
    assert_eq!(state, Value::Int(10));
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

    let state_cell = StateCell::new(db.clone());
    state_cell
        .init(&branch_id, "default", "cell", Value::Int(50))
        .unwrap();

    // Read-only transaction
    let result = db.transaction(branch_id, |txn| {
        let kv_val = txn.kv_get("existing")?;
        assert_eq!(kv_val, Some(Value::Int(100)));

        let state_val = txn.state_get("cell")?;
        assert!(state_val.is_some());

        Ok(())
    });

    assert!(result.is_ok());

    // Data unchanged
    assert_eq!(
        kv.get(&branch_id, "default", "existing").unwrap(),
        Some(Value::Int(100))
    );
    let state = state_cell
        .get(&branch_id, "default", "cell")
        .unwrap()
        .unwrap();
    assert_eq!(state, Value::Int(50));
}
