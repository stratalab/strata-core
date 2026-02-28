//! Cross-Primitive Transaction Tests
//!
//! Tests that verify atomic transactions spanning multiple primitives.

use crate::common::*;
use std::collections::HashMap;
use strata_engine::{EventLogExt, KVStoreExt, StateCellExt};

/// Helper to create an event payload object
fn event_payload(data: Value) -> Value {
    Value::Object(Box::new(HashMap::from([("data".to_string(), data)])))
}

// ============================================================================
// Multi-Primitive Transactions
// ============================================================================

#[test]
fn kv_and_eventlog_atomic() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    test_db
        .db
        .transaction(branch_id, |txn| {
            txn.kv_put("order_id", Value::String("ORD-123".into()))?;
            txn.event_append(
                "order_created",
                event_payload(Value::String("ORD-123".into())),
            )?;
            Ok(())
        })
        .unwrap();

    let kv = test_db.kv();
    let event = test_db.event();

    assert_eq!(
        kv.get(&branch_id, "default", "order_id").unwrap(),
        Some(Value::String("ORD-123".into()))
    );
    assert_eq!(event.len(&branch_id, "default").unwrap(), 1);
}

#[test]
fn kv_and_statecell_atomic() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let state = test_db.state();

    // Init state first
    state
        .init(
            &branch_id,
            "default",
            "status",
            Value::String("pending".into()),
        )
        .unwrap();

    test_db
        .db
        .transaction(branch_id, |txn| {
            txn.kv_put("processed_at", Value::Int(1234567890))?;
            txn.state_set("status", Value::String("completed".into()))?;
            Ok(())
        })
        .unwrap();

    let kv = test_db.kv();
    assert_eq!(
        kv.get(&branch_id, "default", "processed_at").unwrap(),
        Some(Value::Int(1234567890))
    );

    let current = state.get(&branch_id, "default", "status").unwrap().unwrap();
    assert_eq!(current, Value::String("completed".into()));
}

#[test]
fn three_primitives_atomic() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let state = test_db.state();

    state
        .init(&branch_id, "default", "counter", Value::Int(0))
        .unwrap();

    test_db
        .db
        .transaction(branch_id, |txn| {
            // KV
            txn.kv_put("data", Value::String("value".into()))?;

            // Event
            txn.event_append("log", event_payload(Value::String("action".into())))?;

            // State
            txn.state_set("counter", Value::Int(1))?;

            Ok(())
        })
        .unwrap();

    let kv = test_db.kv();
    let event = test_db.event();

    assert_eq!(
        kv.get(&branch_id, "default", "data").unwrap(),
        Some(Value::String("value".into()))
    );
    assert_eq!(event.len(&branch_id, "default").unwrap(), 1);

    let counter = state
        .get(&branch_id, "default", "counter")
        .unwrap()
        .unwrap();
    assert_eq!(counter, Value::Int(1));
}

// ============================================================================
// Cross-Primitive Rollback
// ============================================================================

#[test]
fn cross_primitive_failure_rolls_back_all() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let result: Result<(), _> = test_db.db.transaction(branch_id, |txn| {
        txn.kv_put("rollback_key", Value::Int(1))?;
        txn.event_append("rollback_event", event_payload(Value::Int(2)))?;

        // Force failure
        Err(strata_core::StrataError::invalid_input("forced rollback"))
    });

    assert!(result.is_err());

    // Neither primitive should have data
    let kv = test_db.kv();
    let event = test_db.event();

    assert!(kv
        .get(&branch_id, "default", "rollback_key")
        .unwrap()
        .is_none());
    assert_eq!(event.len(&branch_id, "default").unwrap(), 0);
}

#[test]
fn partial_writes_not_visible() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();

    // Pre-existing data
    kv.put(&branch_id, "default", "existing", Value::Int(100))
        .unwrap();

    let result: Result<(), _> = test_db.db.transaction(branch_id, |txn| {
        // Multiple writes
        txn.kv_put("new_1", Value::Int(1))?;
        txn.kv_put("new_2", Value::Int(2))?;
        txn.kv_put("existing", Value::Int(999))?; // Overwrite

        // Abort
        Err(strata_core::StrataError::invalid_input("abort"))
    });

    assert!(result.is_err());

    // New keys don't exist
    assert!(kv.get(&branch_id, "default", "new_1").unwrap().is_none());
    assert!(kv.get(&branch_id, "default", "new_2").unwrap().is_none());

    // Existing key unchanged
    assert_eq!(
        kv.get(&branch_id, "default", "existing").unwrap().unwrap(),
        Value::Int(100)
    );
}

// ============================================================================
// Read-Modify-Write Patterns
// ============================================================================

#[test]
fn read_modify_write_single_primitive() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();

    kv.put(&branch_id, "default", "counter", Value::Int(10))
        .unwrap();

    test_db
        .db
        .transaction(branch_id, |txn| {
            let val = txn.kv_get("counter")?;
            if let Some(Value::Int(n)) = val {
                txn.kv_put("counter", Value::Int(n + 5))?;
            }
            Ok(())
        })
        .unwrap();

    let result = kv.get(&branch_id, "default", "counter").unwrap().unwrap();
    assert_eq!(result, Value::Int(15));
}

#[test]
fn read_from_one_write_to_another() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();
    let state = test_db.state();

    // Source data
    kv.put(&branch_id, "default", "source", Value::Int(42))
        .unwrap();

    // Copy to state cell using transaction
    test_db
        .db
        .transaction(branch_id, |txn| {
            let val = txn.kv_get("source")?.unwrap();
            txn.state_set("copied", val)?;
            Ok(())
        })
        .unwrap();

    let copied = state.get(&branch_id, "default", "copied").unwrap().unwrap();
    assert_eq!(copied, Value::Int(42));
}

// ============================================================================
// Workflow Patterns
// ============================================================================

#[test]
fn saga_pattern_all_steps_complete() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    // Simulate a multi-step workflow
    test_db
        .db
        .transaction(branch_id, |txn| {
            // Step 1: Create order
            txn.kv_put("order:1", Value::String("created".into()))?;

            // Step 2: Reserve inventory
            txn.kv_put("inventory:item1", Value::Int(99))?;

            // Step 3: Log event
            txn.event_append(
                "order_event",
                event_payload(Value::String("order:1 created".into())),
            )?;

            // Step 4: Update status via state_set
            txn.state_set("order:1:status", Value::String("processing".into()))?;

            Ok(())
        })
        .unwrap();

    let kv = test_db.kv();
    let event = test_db.event();
    let state = test_db.state();

    assert_eq!(
        kv.get(&branch_id, "default", "order:1").unwrap(),
        Some(Value::String("created".into()))
    );
    assert_eq!(
        kv.get(&branch_id, "default", "inventory:item1").unwrap(),
        Some(Value::Int(99))
    );
    assert_eq!(event.len(&branch_id, "default").unwrap(), 1);
    assert_eq!(
        state
            .get(&branch_id, "default", "order:1:status")
            .unwrap()
            .unwrap(),
        Value::String("processing".into())
    );
}

// ============================================================================
// Isolation Between Branches
// ============================================================================

#[test]
fn cross_primitive_branch_isolation() {
    let test_db = TestDb::new();
    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // Write to branch A
    test_db
        .db
        .transaction(branch_a, |txn| {
            txn.kv_put("shared_key", Value::String("branch_a".into()))?;
            txn.event_append("log", event_payload(Value::String("branch_a".into())))?;
            Ok(())
        })
        .unwrap();

    // Write to branch B
    test_db
        .db
        .transaction(branch_b, |txn| {
            txn.kv_put("shared_key", Value::String("branch_b".into()))?;
            txn.event_append("log", event_payload(Value::String("branch_b".into())))?;
            Ok(())
        })
        .unwrap();

    let kv = test_db.kv();
    let event = test_db.event();

    // Each branch sees only its own data
    let a_key = kv.get(&branch_a, "default", "shared_key").unwrap().unwrap();
    let b_key = kv.get(&branch_b, "default", "shared_key").unwrap().unwrap();

    assert_eq!(a_key, Value::String("branch_a".into()));
    assert_eq!(b_key, Value::String("branch_b".into()));

    // Each branch has its own event log
    assert_eq!(event.len(&branch_a, "default").unwrap(), 1);
    assert_eq!(event.len(&branch_b, "default").unwrap(), 1);
}
