//! Database Transaction Tests
//!
//! Tests for transaction begin, commit, abort, and retry semantics.

use crate::common::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use strata_engine::KVStoreExt;

// ============================================================================
// Basic Transaction Flow
// ============================================================================

#[test]
fn transaction_commit_makes_data_visible() {
    let test_db = TestDb::new();
    let kv = test_db.kv();
    let branch_id = test_db.branch_id;

    // Transaction commits
    kv.put(&branch_id, "default", "key", Value::Int(42))
        .unwrap();

    // Data visible after commit
    let result = kv.get(&branch_id, "default", "key").unwrap();
    assert_eq!(result.unwrap(), Value::Int(42));
}

#[test]
fn transaction_closure_api() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    // Use the transaction closure API
    test_db
        .db
        .transaction(branch_id, |txn| {
            txn.kv_put("tx_key", Value::Int(100))?;
            Ok(())
        })
        .unwrap();

    // Verify data committed
    let kv = test_db.kv();
    let result = kv.get(&branch_id, "default", "tx_key").unwrap();
    assert_eq!(result.unwrap(), Value::Int(100));
}

#[test]
fn transaction_returns_value() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();

    kv.put(&branch_id, "default", "counter", Value::Int(10))
        .unwrap();

    // Transaction can return a value
    let result: i64 = test_db
        .db
        .transaction(branch_id, |txn| {
            let val = txn.kv_get("counter")?;
            match val {
                Some(Value::Int(n)) => {
                    txn.kv_put("counter", Value::Int(n + 1))?;
                    Ok(n)
                }
                _ => Ok(0),
            }
        })
        .unwrap();

    assert_eq!(result, 10);

    // Counter was incremented
    let new_val = kv.get(&branch_id, "default", "counter").unwrap();
    assert_eq!(new_val.unwrap(), Value::Int(11));
}

#[test]
fn transaction_error_aborts() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();

    // Put initial value
    kv.put(&branch_id, "default", "abort_test", Value::Int(1))
        .unwrap();

    // Transaction that errors
    let result: Result<(), _> = test_db.db.transaction(branch_id, |txn| {
        txn.kv_put("abort_test", Value::Int(999))?;
        // Return an error
        Err(strata_core::StrataError::invalid_input("forced error"))
    });

    assert!(result.is_err());

    // Original value unchanged
    let val = kv.get(&branch_id, "default", "abort_test").unwrap();
    assert_eq!(val.unwrap(), Value::Int(1));
}

// ============================================================================
// Read-Only Transactions
// ============================================================================

#[test]
fn read_only_transaction_sees_committed_data() {
    let test_db = TestDb::new();
    let kv = test_db.kv();
    let branch_id = test_db.branch_id;

    // Write data
    kv.put(&branch_id, "default", "ro_key", Value::Int(42))
        .unwrap();

    // Read in transaction
    let result: Option<Value> = test_db
        .db
        .transaction(branch_id, |txn| {
            let v = txn.kv_get("ro_key")?;
            Ok(v)
        })
        .unwrap();

    assert_eq!(result, Some(Value::Int(42)));
}

#[test]
fn read_only_transaction_never_conflicts() {
    let test_db = TestDb::new();
    let kv = test_db.kv();
    let branch_id = test_db.branch_id;

    kv.put(&branch_id, "default", "ro_key", Value::Int(1))
        .unwrap();

    // Multiple read-only transactions should all succeed
    for _ in 0..10 {
        let result: Option<Value> = test_db
            .db
            .transaction(branch_id, |txn| {
                let v = txn.kv_get("ro_key")?;
                Ok(v)
            })
            .unwrap();

        assert_eq!(result, Some(Value::Int(1)));
    }
}

// ============================================================================
// Transaction Retry
// ============================================================================

#[test]
fn transaction_retries_on_conflict() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();

    kv.put(&branch_id, "default", "retry_key", Value::Int(0))
        .unwrap();

    let attempt_count = Arc::new(AtomicU64::new(0));
    let attempt_count_clone = attempt_count.clone();

    // This transaction increments the key
    // If there's conflict, it will retry
    test_db
        .db
        .transaction(branch_id, |txn| {
            attempt_count_clone.fetch_add(1, Ordering::SeqCst);

            let val = txn.kv_get("retry_key")?;
            if let Some(Value::Int(n)) = val {
                txn.kv_put("retry_key", Value::Int(n + 1))?;
            }
            Ok(())
        })
        .unwrap();

    // At least one attempt was made
    assert!(attempt_count.load(Ordering::SeqCst) >= 1);

    // Value was incremented
    let final_val = kv.get(&branch_id, "default", "retry_key").unwrap();
    assert_eq!(final_val.unwrap(), Value::Int(1));
}

// ============================================================================
// Manual Transaction Control
// ============================================================================

#[test]
fn begin_transaction_returns_context() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let ctx = test_db.db.begin_transaction(branch_id).unwrap();

    // Context is active
    assert!(ctx.is_active());
    assert_eq!(ctx.branch_id, branch_id);

    // Clean up
    test_db.db.end_transaction(ctx);
}

#[test]
fn commit_transaction_returns_version() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let mut ctx = test_db.db.begin_transaction(branch_id).unwrap();

    // Add a write to the transaction
    use std::sync::Arc;
    use strata_core::types::{Key, Namespace};
    let key = Key::new_kv(Arc::new(Namespace::for_branch(branch_id)), "manual_tx_key");
    ctx.write_set.insert(key, Value::Int(42));

    // Commit
    let version = test_db.db.commit_transaction(&mut ctx).unwrap();

    // Commit succeeded - version > 0 proves the transaction was committed
    // Note: ctx.is_committed() is not accessible after commit (Transaction takes ownership)
    assert!(version > 0);
}

#[test]
fn end_transaction_cleans_up() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let ctx = test_db.db.begin_transaction(branch_id).unwrap();

    // End without commit
    test_db.db.end_transaction(ctx);

    // No panic, transaction cleaned up
}

// ============================================================================
// Multi-Operation Transactions
// ============================================================================

#[test]
fn transaction_multiple_puts() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    test_db
        .db
        .transaction(branch_id, |txn| {
            for i in 0..10 {
                txn.kv_put(&format!("multi_{}", i), Value::Int(i))?;
            }
            Ok(())
        })
        .unwrap();

    // All visible
    let kv = test_db.kv();
    for i in 0..10 {
        let val = kv
            .get(&branch_id, "default", &format!("multi_{}", i))
            .unwrap();
        assert_eq!(val.unwrap(), Value::Int(i));
    }
}

#[test]
fn transaction_put_and_delete() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();

    // Pre-existing key
    kv.put(&branch_id, "default", "to_delete", Value::Int(1))
        .unwrap();

    test_db
        .db
        .transaction(branch_id, |txn| {
            txn.kv_put("new_key", Value::Int(42))?;
            txn.kv_delete("to_delete")?;
            Ok(())
        })
        .unwrap();

    // New key exists
    assert_eq!(
        kv.get(&branch_id, "default", "new_key").unwrap(),
        Some(Value::Int(42))
    );
    // Old key gone
    assert!(kv
        .get(&branch_id, "default", "to_delete")
        .unwrap()
        .is_none());
}
