// Tests temporarily commented out during engine re-architecture.
// These tests use internal engine methods (wal, flush, transaction_with_version,
// transaction_with_retry) that are now pub(crate). Uncomment once the new API
// surface exposes equivalent functionality.

/*
//! Database Transaction API Integration Tests
//!
//! Validates the complete transaction lifecycle including:
//! - Closure API
//! - Implicit transactions
//! - Conflict detection and retry
//! - WAL durability
//! - Recovery
//!
//! Per spec Section 4: These tests validate legacy compatibility and transaction semantics.

use strata_core::StrataError;
use strata_core::BranchId;
use strata_storage::{Key, Namespace, Storage};
use strata_core::value::Value;
use strata_engine::{Database, RetryConfig};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

fn create_ns(branch_id: BranchId) -> Namespace {
    Namespace::new(
        branch_id,
        "default".to_string(),
    )
}

// ============================================================================
// End-to-End Transaction Scenarios
// ============================================================================

#[test]
fn test_e2e_read_modify_write() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key = Key::new_kv(ns, "counter");

    // Initialize counter
    db.put(branch_id, key.clone(), Value::Int(0)).unwrap();

    // Read-modify-write in transaction
    db.transaction(branch_id, |txn| {
        let val = txn.get(&key)?.unwrap();
        if let Value::Int(n) = val {
            txn.put(key.clone(), Value::Int(n + 1))?;
        }
        Ok(())
    })
    .unwrap();

    // Verify incremented
    let result = db.get(&key).unwrap().unwrap();
    assert_eq!(result.value, Value::Int(1));
}

#[test]
fn test_e2e_multi_key_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Transaction with multiple keys
    db.transaction(branch_id, |txn| {
        txn.put(Key::new_kv(ns.clone(), "a"), Value::Int(1))?;
        txn.put(Key::new_kv(ns.clone(), "b"), Value::Int(2))?;
        txn.put(Key::new_kv(ns.clone(), "c"), Value::Int(3))?;
        Ok(())
    })
    .unwrap();

    // Verify all keys
    assert_eq!(
        db.get(&Key::new_kv(ns.clone(), "a"))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );
    assert_eq!(
        db.get(&Key::new_kv(ns.clone(), "b"))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert_eq!(
        db.get(&Key::new_kv(ns.clone(), "c"))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
}

#[test]
fn test_e2e_transaction_abort_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key = Key::new_kv(ns, "rollback_key");

    // Pre-populate
    db.put(branch_id, key.clone(), Value::Int(100)).unwrap();

    // Failing transaction
    let result: Result<(), StrataError> = db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(999))?;
        Err(StrataError::invalid_input("rollback".to_string()))
    });

    assert!(result.is_err());

    // Original value should be preserved
    let val = db.get(&key).unwrap().unwrap();
    assert_eq!(val.value, Value::Int(100));
}

#[test]
fn test_e2e_transaction_with_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key1 = Key::new_kv(ns.clone(), "key1");
    let key2 = Key::new_kv(ns.clone(), "key2");

    // Pre-populate
    db.put(branch_id, key1.clone(), Value::Int(1)).unwrap();
    db.put(branch_id, key2.clone(), Value::Int(2)).unwrap();

    // Transaction that deletes one key and updates another
    db.transaction(branch_id, |txn| {
        txn.delete(key1.clone())?;
        txn.put(key2.clone(), Value::Int(20))?;
        Ok(())
    })
    .unwrap();

    // Verify changes
    assert!(db.get(&key1).unwrap().is_none());
    assert_eq!(db.get(&key2).unwrap().unwrap(), Value::Int(20));
}

#[test]
fn test_e2e_nested_value_types() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    db.transaction(branch_id, |txn| {
        txn.put(
            Key::new_kv(ns.clone(), "string"),
            Value::String("hello".to_string()),
        )?;
        txn.put(Key::new_kv(ns.clone(), "int"), Value::Int(42))?;
        txn.put(Key::new_kv(ns.clone(), "float"), Value::Float(3.14))?;
        txn.put(Key::new_kv(ns.clone(), "bool"), Value::Bool(true))?;
        txn.put(
            Key::new_kv(ns.clone(), "bytes"),
            Value::Bytes(vec![1, 2, 3]),
        )?;
        Ok(())
    })
    .unwrap();

    // Verify all value types
    assert_eq!(
        db.get(&Key::new_kv(ns.clone(), "string"))
            .unwrap()
            .unwrap()
            .value,
        Value::String("hello".to_string())
    );
    assert_eq!(
        db.get(&Key::new_kv(ns.clone(), "int"))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(42)
    );
    assert_eq!(
        db.get(&Key::new_kv(ns.clone(), "float"))
            .unwrap()
            .unwrap()
            .value,
        Value::Float(3.14)
    );
    assert_eq!(
        db.get(&Key::new_kv(ns.clone(), "bool"))
            .unwrap()
            .unwrap()
            .value,
        Value::Bool(true)
    );
    assert_eq!(
        db.get(&Key::new_kv(ns.clone(), "bytes"))
            .unwrap()
            .unwrap()
            .value,
        Value::Bytes(vec![1, 2, 3])
    );
}

// ============================================================================
// Multi-threaded Conflict Tests
// ============================================================================

#[test]
fn test_concurrent_transactions_different_keys() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    let mut handles = vec![];

    // 10 threads, each writing to its own key
    for i in 0..10 {
        let db = Arc::clone(&db);
        let ns = ns.clone();

        handles.push(thread::spawn(move || {
            let key = Key::new_kv(ns, &format!("thread_{}", i));
            db.put(branch_id, key, Value::Int(i as i64)).unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // All keys should exist
    for i in 0..10 {
        let key = Key::new_kv(ns.clone(), &format!("thread_{}", i));
        let val = db.get(&key).unwrap().unwrap();
        assert_eq!(val.value, Value::Int(i as i64));
    }
}

#[test]
fn test_concurrent_transactions_same_key_blind_write() {
    // Per spec Section 3.2: Blind writes don't conflict
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key = Key::new_kv(ns, "contested");

    let mut handles = vec![];

    // 10 threads writing to the same key (blind writes)
    for i in 0..10 {
        let db = Arc::clone(&db);
        let key = key.clone();

        handles.push(thread::spawn(move || {
            db.put(branch_id, key, Value::Int(i as i64)).unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Key should exist (last writer wins)
    let val = db.get(&key).unwrap();
    assert!(val.is_some());
}

#[test]
fn test_concurrent_writes_different_keys_no_conflicts() {
    // Multiple threads writing to different keys should never conflict
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let mut handles = vec![];

    // 5 threads, each doing 10 operations on their own key
    for thread_id in 0..5 {
        let db = Arc::clone(&db);
        let ns = ns.clone();

        handles.push(thread::spawn(move || {
            let key = Key::new_kv(ns, &format!("thread_{}_key", thread_id));

            // Initialize
            db.put(branch_id, key.clone(), Value::Int(0)).unwrap();

            // Increment 10 times
            for _ in 0..10 {
                db.transaction(branch_id, |txn| {
                    let val = txn.get(&key)?.unwrap();
                    if let Value::Int(n) = val {
                        txn.put(key.clone(), Value::Int(n + 1))?;
                        Ok(())
                    } else {
                        Err(StrataError::invalid_input("wrong type".to_string()))
                    }
                })
                .unwrap();
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // All keys should be 10
    for thread_id in 0..5 {
        let key = Key::new_kv(ns.clone(), &format!("thread_{}_key", thread_id));
        let val = db.get(&key).unwrap().unwrap();
        assert_eq!(val.value, Value::Int(10));
    }
}

#[test]
fn test_retry_simulated_conflict() {
    // Test retry with simulated conflicts (using atomic counter to control behavior)
    use std::sync::atomic::{AtomicU64, Ordering};

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key = Key::new_kv(ns, "retry_key");

    // Pre-populate
    db.put(branch_id, key.clone(), Value::Int(0)).unwrap();

    let attempts = AtomicU64::new(0);

    // Retry config with 3 retries
    let config = RetryConfig::new()
        .with_max_retries(3)
        .with_base_delay_ms(1)
        .with_max_delay_ms(5);

    // Fail first 2 times, succeed on 3rd
    let result = db.transaction_with_retry(branch_id, config, |txn| {
        let count = attempts.fetch_add(1, Ordering::Relaxed);

        if count < 2 {
            Err(StrataError::conflict("simulated".to_string()))
        } else {
            txn.put(key.clone(), Value::Int(42))?;
            Ok(())
        }
    });

    assert!(result.is_ok());
    assert_eq!(attempts.load(Ordering::Relaxed), 3); // Tried 3 times
    assert_eq!(db.get(&key).unwrap().unwrap(), Value::Int(42));
}

// ============================================================================
// Recovery Tests
// ============================================================================

#[test]
fn test_recovery_preserves_transactions() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Write data and close
    {
        let db = Database::open(&db_path).unwrap();

        db.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), "key1"), Value::Int(1))?;
            txn.put(Key::new_kv(ns.clone(), "key2"), Value::Int(2))?;
            Ok(())
        })
        .unwrap();

        db.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), "key3"), Value::Int(3))?;
            Ok(())
        })
        .unwrap();
    }

    // Reopen and verify
    {
        let db = Database::open(&db_path).unwrap();

        assert_eq!(
            db.get(&Key::new_kv(ns.clone(), "key1"))
                .unwrap()
                .unwrap()
                .value,
            Value::Int(1)
        );
        assert_eq!(
            db.get(&Key::new_kv(ns.clone(), "key2"))
                .unwrap()
                .unwrap()
                .value,
            Value::Int(2)
        );
        assert_eq!(
            db.get(&Key::new_kv(ns.clone(), "key3"))
                .unwrap()
                .unwrap()
                .value,
            Value::Int(3)
        );
    }
}

#[test]
fn test_recovery_version_continuity() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    let version_before;

    // Write data and capture version
    {
        let db = Database::open(&db_path).unwrap();
        db.put(branch_id, Key::new_kv(ns.clone(), "key"), Value::Int(1))
            .unwrap();
        version_before = db.storage().current_version();
    }

    // Reopen
    {
        let db = Database::open(&db_path).unwrap();
        let version_after = db.storage().current_version();

        // Version should be preserved
        assert_eq!(version_before, version_after);

        // New writes should get higher versions
        db.put(branch_id, Key::new_kv(ns.clone(), "key2"), Value::Int(2))
            .unwrap();
        assert!(db.storage().current_version() > version_before);
    }
}

#[test]
fn test_recovery_aborted_transaction_not_visible() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Write and abort
    {
        let db = Database::open(&db_path).unwrap();

        // This will abort
        let _result: Result<(), StrataError> = db.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), "aborted_key"), Value::Int(999))?;
            Err(StrataError::invalid_input("abort".to_string()))
        });
    }

    // Reopen and verify key doesn't exist
    {
        let db = Database::open(&db_path).unwrap();
        assert!(db
            .get(&Key::new_kv(ns.clone(), "aborted_key"))
            .unwrap()
            .is_none());
    }
}

#[test]
fn test_recovery_with_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Write, then delete
    {
        let db = Database::open(&db_path).unwrap();

        db.put(branch_id, Key::new_kv(ns.clone(), "to_delete"), Value::Int(1))
            .unwrap();
        db.put(branch_id, Key::new_kv(ns.clone(), "to_keep"), Value::Int(2))
            .unwrap();
        db.delete(branch_id, Key::new_kv(ns.clone(), "to_delete"))
            .unwrap();
    }

    // Reopen and verify
    {
        let db = Database::open(&db_path).unwrap();
        assert!(db
            .get(&Key::new_kv(ns.clone(), "to_delete"))
            .unwrap()
            .is_none());
        assert_eq!(
            db.get(&Key::new_kv(ns.clone(), "to_keep"))
                .unwrap()
                .unwrap()
                .value,
            Value::Int(2)
        );
    }
}

// ============================================================================
// Legacy Compatibility Tests
// ============================================================================

#[test]
fn test_m1_api_compatibility() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key = Key::new_kv(ns, "m1_key");

    // legacy-style operations
    db.put(branch_id, key.clone(), Value::String("value1".to_string()))
        .unwrap();
    let val = db.get(&key).unwrap().unwrap();
    assert_eq!(val.value, Value::String("value1".to_string()));

    db.delete(branch_id, key.clone()).unwrap();
    assert!(db.get(&key).unwrap().is_none());
}

#[test]
fn test_m1_cas_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key = Key::new_kv(ns, "cas_key");

    // Create new key (version 0 = must not exist)
    db.cas(branch_id, key.clone(), 0, Value::Int(1)).unwrap();

    // Get version
    let val = db.get(&key).unwrap().unwrap();
    assert_eq!(val.value, Value::Int(1));
    let version = val.version.as_u64();

    // Update with correct version
    db.cas(branch_id, key.clone(), version, Value::Int(2)).unwrap();

    // Verify
    assert_eq!(db.get(&key).unwrap().unwrap(), Value::Int(2));

    // Try with wrong version - should fail
    let result = db.cas(branch_id, key.clone(), version, Value::Int(3));
    assert!(result.is_err());
}

#[test]
fn test_m1_sequence_of_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Create 100 keys
    for i in 0..100 {
        let key = Key::new_kv(ns.clone(), &format!("key_{}", i));
        db.put(branch_id, key, Value::Int(i as i64)).unwrap();
    }

    // Verify all exist
    for i in 0..100 {
        let key = Key::new_kv(ns.clone(), &format!("key_{}", i));
        let val = db.get(&key).unwrap().unwrap();
        assert_eq!(val.value, Value::Int(i as i64));
    }

    // Delete even keys
    for i in (0..100).step_by(2) {
        let key = Key::new_kv(ns.clone(), &format!("key_{}", i));
        db.delete(branch_id, key).unwrap();
    }

    // Verify odd keys exist, even keys don't
    for i in 0..100 {
        let key = Key::new_kv(ns.clone(), &format!("key_{}", i));
        if i % 2 == 0 {
            assert!(db.get(&key).unwrap().is_none());
        } else {
            assert_eq!(db.get(&key).unwrap().unwrap(), Value::Int(i as i64));
        }
    }
}

// ============================================================================
// Transaction Metrics Tests
// ============================================================================

#[test]
fn test_transaction_metrics() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Execute some transactions
    for i in 0..5 {
        db.put(
            branch_id,
            Key::new_kv(ns.clone(), &format!("key{}", i)),
            Value::Int(i as i64),
        )
        .unwrap();
    }

    let metrics = db.metrics();
    assert!(metrics.total_committed >= 5);
    assert!(metrics.commit_rate > 0.0);
}

#[test]
fn test_transaction_metrics_with_aborts() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Successful transaction
    db.put(branch_id, Key::new_kv(ns.clone(), "success"), Value::Int(1))
        .unwrap();

    // Failed transaction
    let _result: Result<(), StrataError> = db.transaction(branch_id, |txn| {
        txn.put(Key::new_kv(ns.clone(), "fail"), Value::Int(2))?;
        Err(StrataError::invalid_input("intentional".to_string()))
    });

    let metrics = db.metrics();
    assert!(metrics.total_committed >= 1);
    assert!(metrics.total_aborted >= 1);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_empty_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();

    // Transaction with no operations
    let result = db.transaction(branch_id, |_txn| Ok(42));
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_large_value() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key = Key::new_kv(ns, "large_value");

    // 1MB value
    let large_data = vec![42u8; 1024 * 1024];
    db.put(branch_id, key.clone(), Value::Bytes(large_data.clone()))
        .unwrap();

    let stored = db.get(&key).unwrap().unwrap();
    assert_eq!(stored.value, Value::Bytes(large_data));
}

#[test]
fn test_many_keys_in_single_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Single transaction with 100 keys
    db.transaction(branch_id, |txn| {
        for i in 0..100 {
            txn.put(
                Key::new_kv(ns.clone(), &format!("batch_key_{}", i)),
                Value::Int(i as i64),
            )?;
        }
        Ok(())
    })
    .unwrap();

    // Verify all exist
    for i in 0..100 {
        let key = Key::new_kv(ns.clone(), &format!("batch_key_{}", i));
        assert_eq!(db.get(&key).unwrap().unwrap(), Value::Int(i as i64));
    }
}

#[test]
fn test_overwrite_in_same_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key = Key::new_kv(ns, "overwrite");

    // Multiple writes to same key in same transaction
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(1))?;
        txn.put(key.clone(), Value::Int(2))?;
        txn.put(key.clone(), Value::Int(3))?;
        Ok(())
    })
    .unwrap();

    // Should have final value
    assert_eq!(db.get(&key).unwrap().unwrap(), Value::Int(3));
}

#[test]
fn test_delete_then_write_in_same_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key = Key::new_kv(ns, "delete_write");

    // Pre-populate
    db.put(branch_id, key.clone(), Value::Int(100)).unwrap();

    // Delete then write
    db.transaction(branch_id, |txn| {
        txn.delete(key.clone())?;
        txn.put(key.clone(), Value::Int(200))?;
        Ok(())
    })
    .unwrap();

    // Should have new value
    assert_eq!(db.get(&key).unwrap().unwrap(), Value::Int(200));
}

#[test]
fn test_write_then_delete_in_same_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key = Key::new_kv(ns, "write_delete");

    // Write then delete
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(100))?;
        txn.delete(key.clone())?;
        Ok(())
    })
    .unwrap();

    // Should be deleted
    assert!(db.get(&key).unwrap().is_none());
}

*/
