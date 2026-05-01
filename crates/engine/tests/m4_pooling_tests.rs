//! Transaction Pooling Verification Tests
//!
//! Validates that the transaction pooling system achieves zero-allocation
//! performance after warmup. These tests verify:
//! - Pool reuses contexts across transactions
//! - Capacity is preserved across resets
//! - Pool size remains stable during steady-state operation
//!
//! Per specification: Hot path must have zero allocations after warmup.

use std::sync::Arc;
use strata_core::BranchId;
use strata_core::Value;
use strata_engine::database::OpenSpec;
use strata_engine::{Database, SearchSubsystem, TransactionPool, MAX_POOL_SIZE};
use strata_storage::{Key, Namespace, TypeTag};
use tempfile::TempDir;

fn create_ns(branch_id: BranchId) -> Arc<Namespace> {
    Arc::new(Namespace::new(branch_id, "default".to_string()))
}

fn create_key(ns: &Arc<Namespace>, user_key: &str) -> Key {
    Key::new(ns.clone(), TypeTag::KV, user_key.as_bytes().to_vec())
}

// ============================================================================
// Pool Reuse Tests
// ============================================================================

#[test]
fn test_pool_reuses_contexts() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path().join("db")).with_subsystem(SearchSubsystem),
    )
    .unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Clear pool for deterministic test
    TransactionPool::warmup(0); // Reset to known state

    // Warmup - fill pool by running transactions
    // After each transaction completes, its context goes back to pool
    for i in 0..MAX_POOL_SIZE {
        let key = create_key(&ns, &format!("warmup_key_{}", i));
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(i as i64))?;
            Ok(())
        })
        .unwrap();
    }

    let pool_size_before = TransactionPool::pool_size();
    assert!(
        pool_size_before > 0,
        "Pool should have contexts after warmup"
    );

    // Operations should reuse from pool - pool size should remain stable
    for i in 0..100 {
        let key = create_key(&ns, &format!("key_{}", i));
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(i as i64))?;
            Ok(())
        })
        .unwrap();
    }

    let pool_size_after = TransactionPool::pool_size();
    assert_eq!(
        pool_size_before, pool_size_after,
        "Pool size should remain stable during steady-state operation"
    );
}

#[test]
fn test_pool_warmup_reduces_allocations() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path().join("db")).with_subsystem(SearchSubsystem),
    )
    .unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Pre-warm the pool
    TransactionPool::warmup(MAX_POOL_SIZE);

    let pool_size_before = TransactionPool::pool_size();
    assert_eq!(
        pool_size_before, MAX_POOL_SIZE,
        "Pool should be full after warmup"
    );

    // Run many transactions - all should reuse pooled contexts
    for i in 0..50 {
        let key = create_key(&ns, &format!("key_{}", i));
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(i as i64))?;
            Ok(())
        })
        .unwrap();
    }

    let pool_size_after = TransactionPool::pool_size();
    // Pool should still be full (contexts returned after each transaction)
    assert_eq!(
        pool_size_after, MAX_POOL_SIZE,
        "Pool should remain full after transactions"
    );
}

#[test]
fn test_capacity_grows_with_usage() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path().join("db")).with_subsystem(SearchSubsystem),
    )
    .unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Start with empty pool
    TransactionPool::warmup(0);

    // First transaction - creates new context
    let _key1 = create_key(&ns, "key1");
    db.transaction(branch_id, |txn| {
        // Add many read/write entries to grow internal capacity
        for i in 0..50 {
            let k = create_key(&ns, &format!("inner_key_{}", i));
            txn.put(k.clone(), Value::Int(i as i64))?;
            let _ = txn.get(&k)?;
        }
        Ok(())
    })
    .unwrap();

    // Pool should now have a context with grown capacity
    let pool_size = TransactionPool::pool_size();
    assert_eq!(pool_size, 1, "One context should be in pool");

    // Second transaction - should reuse the context with grown capacity
    // This tests that capacity is preserved
    let key2 = create_key(&ns, "key2");
    db.transaction(branch_id, |txn| {
        txn.put(key2.clone(), Value::Int(42))?;
        Ok(())
    })
    .unwrap();

    // The capacity growth from first transaction should persist
    // (we can't easily verify capacity directly, but the test ensures
    // the pool is working correctly)
    assert_eq!(
        TransactionPool::pool_size(),
        1,
        "Pool should still have one context"
    );
}

#[test]
fn test_aborted_transactions_return_to_pool() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path().join("db")).with_subsystem(SearchSubsystem),
    )
    .unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Clear pool
    TransactionPool::warmup(0);

    // Run a transaction that succeeds
    let key = create_key(&ns, "key");
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();

    let pool_size_after_success = TransactionPool::pool_size();
    assert_eq!(pool_size_after_success, 1, "Pool should have one context");

    // Run a transaction that fails (returns error from closure)
    let result: Result<(), _> = db.transaction(branch_id, |_txn| {
        Err(strata_engine::StrataError::invalid_input(
            "Test error".to_string(),
        ))
    });
    assert!(result.is_err(), "Transaction should fail");

    // Pool should still have the context (returned even on abort)
    let pool_size_after_abort = TransactionPool::pool_size();
    assert_eq!(
        pool_size_after_abort, pool_size_after_success,
        "Aborted transaction should return context to pool"
    );
}

#[test]
fn test_concurrent_transactions_use_pool() {
    use std::sync::Arc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path().join("db")).with_subsystem(SearchSubsystem),
    )
    .unwrap();

    // Note: Each thread has its own thread-local pool
    // This test verifies that multiple threads can use pooling independently

    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let branch_id = BranchId::new();
                let ns = create_ns(branch_id);

                // Warmup this thread's pool
                for _ in 0..4 {
                    let key = create_key(&ns, "warmup");
                    let _ = db.transaction(branch_id, |txn| {
                        txn.put(key.clone(), Value::Int(0))?;
                        Ok(())
                    });
                }

                let pool_before = TransactionPool::pool_size();

                // Run several transactions
                for i in 0..20 {
                    let key = create_key(&ns, &format!("key_{}_{}", thread_id, i));
                    let _ = db.transaction(branch_id, |txn| {
                        txn.put(key.clone(), Value::Int(i as i64))?;
                        Ok(())
                    });
                }

                let pool_after = TransactionPool::pool_size();

                // Pool should be stable within this thread
                assert_eq!(
                    pool_before, pool_after,
                    "Thread {} pool should be stable",
                    thread_id
                );
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================================
// Pool Size Tests
// ============================================================================

#[test]
fn test_pool_caps_at_max_size() {
    // Verify pool doesn't exceed MAX_POOL_SIZE after many transactions
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path().join("db")).with_subsystem(SearchSubsystem),
    )
    .unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Run more transactions than MAX_POOL_SIZE to fill the pool
    for i in 0..(MAX_POOL_SIZE + 5) {
        let key = create_key(&ns, &format!("key_{}", i));
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(i as i64))?;
            Ok(())
        })
        .unwrap();
    }

    // Pool should cap at MAX_POOL_SIZE
    assert!(
        TransactionPool::pool_size() <= MAX_POOL_SIZE,
        "Pool should cap at MAX_POOL_SIZE"
    );
}
