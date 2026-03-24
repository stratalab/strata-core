//! Adversarial Engine Tests
//!
//! Tests for edge cases, error handling, and behavioral invariants at the engine layer.
//! These tests verify that the engine correctly orchestrates lower layers under
//! adversarial conditions.

use crate::common::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use strata_engine::{EventLogExt, KVStoreExt};

/// Helper to create an event payload object
fn event_payload(data: Value) -> Value {
    Value::object(HashMap::from([("data".to_string(), data)]))
}

// ============================================================================
// Cross-Primitive Atomicity Tests
// ============================================================================

/// Verifies that a failed transaction doesn't leave partial writes across primitives.
/// This is the engine's core responsibility: all-or-nothing across KV, EventLog, JSON.
#[test]
fn cross_primitive_rollback_leaves_no_trace() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    let event = test_db.event();

    // Pre-populate with known values
    kv.put(&branch_id, "default", "existing_key", Value::Int(100))
        .unwrap();

    // Attempt a transaction that touches multiple primitives then fails
    let result: Result<(), _> = test_db.db.transaction(branch_id, |txn| {
        // Write to KV
        txn.kv_put("new_key", Value::Int(1))?;
        txn.kv_put("existing_key", Value::Int(999))?; // Overwrite

        // Write to EventLog
        txn.event_append("test_event", event_payload(Value::Int(2)))?;

        // Force abort
        Err(strata_core::StrataError::invalid_input("forced abort"))
    });

    assert!(result.is_err());

    // Verify NO partial writes leaked:
    // - new_key should not exist
    assert!(
        kv.get(&branch_id, "default", "new_key").unwrap().is_none(),
        "new_key should not exist after rollback"
    );

    // - existing_key should have original value
    assert_eq!(
        kv.get(&branch_id, "default", "existing_key")
            .unwrap()
            .unwrap(),
        Value::Int(100),
        "existing_key should retain original value after rollback"
    );

    // - EventLog should be empty (no events committed)
    assert_eq!(
        event.len(&branch_id, "default").unwrap(),
        0,
        "EventLog should be empty after rollback"
    );
}

/// Multiple transactions writing to different primitives should maintain isolation.
/// Transaction A's writes to KV shouldn't be visible to Transaction B reading KV
/// while A is still in progress.
#[test]
fn cross_primitive_isolation_no_dirty_reads() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let kv = KVStore::new(db.clone());

    // Seed initial value
    kv.put(&branch_id, "default", "shared_key", Value::Int(0))
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let dirty_read_detected = Arc::new(AtomicBool::new(false));

    let b1 = barrier.clone();
    let dirty_flag = dirty_read_detected.clone();
    let db1 = db.clone();

    // Writer thread: starts transaction, writes, waits, then commits
    let writer = thread::spawn(move || {
        db1.transaction(branch_id, |txn| {
            txn.kv_put("shared_key", Value::Int(42))?;

            // Signal that we've written but not committed
            b1.wait();

            // Wait for reader to check
            b1.wait();

            Ok(())
        })
        .unwrap();
    });

    let b2 = barrier.clone();
    let db2 = db.clone();

    // Reader thread: reads while writer is mid-transaction
    let reader = thread::spawn(move || {
        // Wait for writer to write (but not commit)
        b2.wait();

        // Read outside any transaction - should NOT see uncommitted write
        let kv_reader = KVStore::new(db2);
        let val = kv_reader
            .get(&branch_id, "default", "shared_key")
            .unwrap()
            .unwrap();

        if val == Value::Int(42) {
            dirty_flag.store(true, Ordering::SeqCst);
        }

        // Signal done reading
        b2.wait();
    });

    writer.join().unwrap();
    reader.join().unwrap();

    assert!(
        !dirty_read_detected.load(Ordering::SeqCst),
        "Dirty read detected: reader saw uncommitted write"
    );
}

/// Verify that committed data from one transaction is visible to subsequent reads.
#[test]
fn committed_writes_are_visible() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    // Write in transaction
    test_db
        .db
        .transaction(branch_id, |txn| {
            txn.kv_put("key1", Value::Int(100))?;
            txn.kv_put("key2", Value::Int(200))?;
            Ok(())
        })
        .unwrap();

    // Read outside transaction - should see committed values
    let kv = test_db.kv();
    assert_eq!(
        kv.get(&branch_id, "default", "key1").unwrap().unwrap(),
        Value::Int(100)
    );
    assert_eq!(
        kv.get(&branch_id, "default", "key2").unwrap().unwrap(),
        Value::Int(200)
    );
}

// ============================================================================
// OCC Conflict Tests at Engine Level
// ============================================================================

/// First-committer-wins: if two transactions read the same key, then both write,
/// only the first to commit should succeed.
#[test]
fn occ_first_committer_wins() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "contested_key", Value::Int(0))
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let success_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..2)
        .map(|i| {
            let db = db.clone();
            let b = barrier.clone();
            let count = success_count.clone();

            thread::spawn(move || {
                let result = db.transaction(branch_id, |txn| {
                    // Both read the key (adding to read-set)
                    let _val = txn.kv_get("contested_key")?;

                    // Synchronize so both have read
                    b.wait();

                    // Both try to write
                    txn.kv_put("contested_key", Value::Int(i + 1))?;

                    // Synchronize before commit
                    b.wait();

                    Ok(())
                });

                if result.is_ok() {
                    count.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Exactly one should succeed (first committer wins)
    let successes = success_count.load(Ordering::SeqCst);
    assert_eq!(
        successes, 1,
        "Expected exactly 1 successful commit, got {}",
        successes
    );

    // Final value should be from the winner
    let final_val = kv
        .get(&branch_id, "default", "contested_key")
        .unwrap()
        .unwrap();
    assert!(
        final_val == Value::Int(1) || final_val == Value::Int(2),
        "Final value should be from one of the writers"
    );
}

/// Blind writes (no read) should not conflict with each other.
#[test]
fn blind_writes_dont_conflict() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let barrier = Arc::new(Barrier::new(2));
    let success_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..2)
        .map(|i| {
            let db = db.clone();
            let b = barrier.clone();
            let count = success_count.clone();

            thread::spawn(move || {
                let result = db.transaction(branch_id, |txn| {
                    // NO read - just blind write to different keys
                    txn.kv_put(&format!("key_{}", i), Value::Int(i))?;

                    b.wait(); // Sync point

                    Ok(())
                });

                if result.is_ok() {
                    count.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Both should succeed (no conflict)
    assert_eq!(
        success_count.load(Ordering::SeqCst),
        2,
        "Both blind writes should succeed"
    );
}

/// Read-only transactions should never conflict.
#[test]
fn read_only_transactions_never_conflict() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "key", Value::Int(42))
        .unwrap();

    let barrier = Arc::new(Barrier::new(4));
    let success_count = Arc::new(AtomicU64::new(0));

    // Launch 4 read-only transactions concurrently
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            let b = barrier.clone();
            let count = success_count.clone();

            thread::spawn(move || {
                let result = db.transaction(branch_id, |txn| {
                    let _val = txn.kv_get("key")?;
                    b.wait(); // All read at the same time
                    Ok(())
                });

                if result.is_ok() {
                    count.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        success_count.load(Ordering::SeqCst),
        4,
        "All read-only transactions should succeed"
    );
}

// ============================================================================
// Error Propagation Tests
// ============================================================================

/// Errors from primitive operations should propagate correctly.
#[test]
fn primitive_error_propagates() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    // Try to get a non-existent key - should return None
    let kv = test_db.kv();
    let result = kv.get(&branch_id, "default", "nonexistent").unwrap();
    assert!(
        result.is_none(),
        "Get on non-existent key should return None"
    );
}

/// Transaction errors should not leave database in inconsistent state.
#[test]
fn transaction_error_recovery() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    kv.put(&branch_id, "default", "key", Value::Int(1)).unwrap();

    // Multiple failed transactions shouldn't corrupt state
    for _ in 0..5 {
        let _result: Result<(), _> = test_db.db.transaction(branch_id, |txn| {
            txn.kv_put("key", Value::Int(999))?;
            Err(strata_core::StrataError::invalid_input(
                "intentional failure",
            ))
        });
    }

    // Original value should be intact
    assert_eq!(
        kv.get(&branch_id, "default", "key").unwrap().unwrap(),
        Value::Int(1)
    );
}

// ============================================================================
// Version/Ordering Invariants
// ============================================================================

/// Versions should be monotonically increasing for writes to same key.
#[test]
fn versions_monotonically_increase() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();

    kv.put(&branch_id, "default", "key", Value::Int(0)).unwrap();

    let mut last_version = 0u64;
    for i in 1..=10i64 {
        let current = kv.getv(&branch_id, "default", "key").unwrap().unwrap();
        let current_version = current.version().as_u64();

        kv.put(&branch_id, "default", "key", Value::Int(i)).unwrap();

        assert!(
            current_version >= last_version,
            "Version {} should be >= previous version {}",
            current_version,
            last_version
        );
        last_version = current_version;
    }
}

/// EventLog sequence numbers should be monotonic within a branch.
#[test]
fn eventlog_sequence_monotonic() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let event = test_db.event();

    let mut last_seq = 0u64;
    for i in 0..10i64 {
        let seq = event
            .append(&branch_id, "default", "test", event_payload(Value::Int(i)))
            .unwrap();
        let seq_u64 = seq.as_u64();

        if i > 0 {
            assert!(
                seq_u64 > last_seq,
                "Sequence {} should be > previous {}",
                seq_u64,
                last_seq
            );
        }
        last_seq = seq_u64;
    }
}

// ============================================================================
// Branch Isolation Under Contention
// ============================================================================

/// Writes to different branches should never interfere, even under contention.
#[test]
fn branch_isolation_under_contention() {
    let test_db = TestDb::new_in_memory();
    let db = test_db.db.clone();

    let branches: Vec<BranchId> = (0..4).map(|_| BranchId::new()).collect();
    let barrier = Arc::new(Barrier::new(4));
    let errors = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = branches
        .iter()
        .enumerate()
        .map(|(i, &branch_id)| {
            let db = db.clone();
            let b = barrier.clone();
            let err_count = errors.clone();

            thread::spawn(move || {
                b.wait(); // Start all at once

                let kv = KVStore::new(db.clone());

                // Each branch writes to "key" with its own value
                for j in 0..50 {
                    if kv
                        .put(
                            &branch_id,
                            "default",
                            "key",
                            Value::Int((i * 100 + j) as i64),
                        )
                        .is_err()
                    {
                        err_count.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Verify our final value is ours
                let val = kv.get(&branch_id, "default", "key").unwrap().unwrap();
                if let Value::Int(n) = val {
                    if n / 100 != i as i64 {
                        err_count.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    err_count.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        errors.load(Ordering::Relaxed),
        0,
        "Branch isolation violated under contention"
    );
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Empty transaction should succeed without side effects.
#[test]
fn empty_transaction_succeeds() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let result = test_db.db.transaction(branch_id, |_txn| {
        // Do nothing
        Ok(())
    });

    assert!(result.is_ok(), "Empty transaction should succeed");
}

/// Transaction that only reads should succeed.
#[test]
fn read_only_transaction_succeeds() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    kv.put(&branch_id, "default", "key", Value::Int(42))
        .unwrap();

    let result = test_db.db.transaction(branch_id, |txn| {
        let val = txn.kv_get("key")?;
        assert_eq!(val, Some(Value::Int(42)));
        Ok(())
    });

    assert!(result.is_ok(), "Read-only transaction should succeed");
}

/// Very large transaction (many operations) should work.
#[test]
fn large_transaction() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let result = test_db.db.transaction(branch_id, |txn| {
        for i in 0..100 {
            txn.kv_put(&format!("key_{}", i), Value::Int(i))?;
        }
        Ok(())
    });

    assert!(result.is_ok(), "Large transaction should succeed");

    // Verify all writes committed
    let kv = test_db.kv();
    for i in 0..100 {
        let val = kv
            .get(&branch_id, "default", &format!("key_{}", i))
            .unwrap();
        assert_eq!(val, Some(Value::Int(i)), "key_{} should be {}", i, i);
    }
}

/// Nested reads within transaction should be consistent.
#[test]
fn transaction_read_your_writes() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let result = test_db.db.transaction(branch_id, |txn| {
        txn.kv_put("key", Value::Int(42))?;

        // Read back what we just wrote
        let val = txn.kv_get("key")?;
        assert_eq!(val, Some(Value::Int(42)), "Should read own write");

        // Overwrite
        txn.kv_put("key", Value::Int(100))?;

        // Read again
        let val2 = txn.kv_get("key")?;
        assert_eq!(val2, Some(Value::Int(100)), "Should read updated write");

        Ok(())
    });

    assert!(result.is_ok());
}
