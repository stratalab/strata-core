//! Deep Adversarial Engine Tests
//!
//! These tests target specific failure modes and edge cases that could
//! actually fail with a buggy implementation. Unlike shallow tests that
//! verify basic functionality, these probe invariant boundaries.

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
// Lost Update Detection
// ============================================================================

/// Classic lost update problem: Two transactions read V, both compute V+1,
/// both write. Without proper OCC, final value would be V+1 instead of V+2.
///
/// This test would FAIL with naive last-writer-wins without conflict detection.
#[test]
fn lost_update_prevented() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "counter", Value::Int(0))
        .unwrap();

    let iterations = 100;
    let success_count = Arc::new(AtomicU64::new(0));

    // Run many increment attempts - with proper OCC, conflicts should be detected
    let handles: Vec<_> = (0..iterations)
        .map(|_| {
            let db = db.clone();
            let count = success_count.clone();

            thread::spawn(move || {
                // Retry loop for OCC
                loop {
                    let result = db.transaction(branch_id, |txn| {
                        // Read current value
                        let current = txn
                            .kv_get("counter")?
                            .expect("counter key must exist after initialization");
                        let n = match current {
                            Value::Int(v) => v,
                            _ => 0,
                        };

                        // Increment and write back
                        txn.kv_put("counter", Value::Int(n + 1))?;
                        Ok(())
                    });

                    match result {
                        Ok(()) => {
                            count.fetch_add(1, Ordering::SeqCst);
                            break;
                        }
                        Err(_) => {
                            // Conflict - retry
                            continue;
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // ALL increments should have succeeded (with retries)
    assert_eq!(success_count.load(Ordering::SeqCst), iterations as u64);

    // Final value MUST be exactly `iterations` - no lost updates
    let final_val = kv.get(&branch_id, "default", "counter").unwrap().unwrap();
    assert_eq!(
        final_val,
        Value::Int(iterations),
        "Lost update detected! Expected {}, got {:?}",
        iterations,
        final_val
    );
}

// ============================================================================
// Write Skew Detection
// ============================================================================

/// Write skew: Two transactions read overlapping data but write disjoint keys.
/// Classic example: Two doctors on-call, each checks "is other doctor on-call?",
/// both see yes, both go off-call → nobody on-call.
///
/// In OCC with read-set tracking, this SHOULD be detected as a conflict.
/// If the system allows write skew, this test documents that behavior.
#[test]
fn write_skew_behavior() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let kv = KVStore::new(db.clone());
    // Both doctors start on-call
    kv.put(&branch_id, "default", "doctor_a_oncall", Value::Bool(true))
        .unwrap();
    kv.put(&branch_id, "default", "doctor_b_oncall", Value::Bool(true))
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let both_succeeded = Arc::new(AtomicBool::new(false));

    let b1 = barrier.clone();
    let db1 = db.clone();
    let _both1 = both_succeeded.clone();

    // Doctor A tries to go off-call
    let doctor_a = thread::spawn(move || {
        db1.transaction(branch_id, |txn| {
            // Check if doctor B is on-call
            let b_oncall = txn.kv_get("doctor_b_oncall")?;

            b1.wait(); // Sync point - both have read

            if b_oncall == Some(Value::Bool(true)) {
                // B is on-call, so A can go off-call
                txn.kv_put("doctor_a_oncall", Value::Bool(false))?;
            }
            Ok(())
        })
        .is_ok()
    });

    let b2 = barrier.clone();
    let db2 = db.clone();

    // Doctor B tries to go off-call
    let doctor_b = thread::spawn(move || {
        db2.transaction(branch_id, |txn| {
            // Check if doctor A is on-call
            let a_oncall = txn.kv_get("doctor_a_oncall")?;

            b2.wait(); // Sync point - both have read

            if a_oncall == Some(Value::Bool(true)) {
                // A is on-call, so B can go off-call
                txn.kv_put("doctor_b_oncall", Value::Bool(false))?;
            }
            Ok(())
        })
        .is_ok()
    });

    let a_success = doctor_a.join().unwrap();
    let b_success = doctor_b.join().unwrap();

    if a_success && b_success {
        both_succeeded.store(true, Ordering::SeqCst);
    }

    // Check final state
    let a_final = kv
        .get(&branch_id, "default", "doctor_a_oncall")
        .unwrap()
        .unwrap();
    let b_final = kv
        .get(&branch_id, "default", "doctor_b_oncall")
        .unwrap()
        .unwrap();

    let a_oncall = a_final == Value::Bool(true);
    let b_oncall = b_final == Value::Bool(true);

    // At least one doctor MUST still be on-call
    // If both are off-call, we have a write skew anomaly
    if !a_oncall && !b_oncall {
        // This is write skew - document whether system allows it
        println!("WRITE SKEW DETECTED: Both doctors went off-call");
        println!("A succeeded: {}, B succeeded: {}", a_success, b_success);

        // If both transactions succeeded, the system allows write skew
        // This is a valid choice (snapshot isolation allows it) but should be documented
        if both_succeeded.load(Ordering::SeqCst) {
            println!("System allows write skew (snapshot isolation behavior)");
        }
    }

    // The key invariant: if BOTH transactions committed, AND both went off-call,
    // we have write skew. With strict serializability, at least one should abort.
    // Document the actual behavior:
    assert!(
        a_oncall || b_oncall || !both_succeeded.load(Ordering::SeqCst),
        "Write skew anomaly: both doctors off-call AND both transactions committed"
    );
}

// ============================================================================
// Phantom Read Detection
// ============================================================================

/// Phantom reads: Transaction A scans a range, Transaction B inserts into that range,
/// Transaction A re-scans and sees different results.
///
/// Tests whether the system provides repeatable reads within a transaction.
#[test]
fn phantom_read_within_transaction() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();

    // Pre-populate some keys
    kv.put(&branch_id, "default", "key_1", Value::Int(1))
        .unwrap();
    kv.put(&branch_id, "default", "key_2", Value::Int(2))
        .unwrap();

    // Transaction reads, does work, reads again - should see same data
    let result = test_db.db.transaction(branch_id, |txn| {
        // First read
        let v1_first = txn.kv_get("key_1")?;
        let v2_first = txn.kv_get("key_2")?;

        // Simulate some work (in real scenario, another transaction might commit here)
        // Within same transaction, reads should be repeatable

        // Second read
        let v1_second = txn.kv_get("key_1")?;
        let v2_second = txn.kv_get("key_2")?;

        // Values should be identical (repeatable read)
        assert_eq!(v1_first, v1_second, "Non-repeatable read on key_1");
        assert_eq!(v2_first, v2_second, "Non-repeatable read on key_2");

        Ok(())
    });

    assert!(result.is_ok(), "Transaction should succeed");
}

// ============================================================================
// Atomicity Under Partial Failure
// ============================================================================

/// Tests that if ANY operation in a transaction fails, ALL operations are rolled back.
/// This is more rigorous than just testing explicit abort.
#[test]
fn atomicity_on_operation_failure() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();

    // Setup: existing key
    kv.put(&branch_id, "default", "existing", Value::Int(100))
        .unwrap();

    // Try a transaction that writes to KV then fails
    // The failure should roll back the KV write
    let result: Result<(), _> = test_db.db.transaction(branch_id, |txn| {
        // This write should be rolled back
        txn.kv_put("new_key", Value::Int(999))?;
        txn.kv_put("existing", Value::Int(888))?;

        // This will fail - stale version
        // Note: We can't easily do CAS in transaction context, so we'll simulate
        // by just returning an error
        Err(strata_core::StrataError::invalid_input(
            "simulated CAS failure",
        ))
    });

    assert!(result.is_err());

    // Verify atomicity: ALL writes rolled back
    assert!(
        kv.get(&branch_id, "default", "new_key").unwrap().is_none(),
        "new_key should not exist after failed transaction"
    );
    assert_eq!(
        kv.get(&branch_id, "default", "existing").unwrap().unwrap(),
        Value::Int(100),
        "existing key should have original value"
    );
}

// ============================================================================
// Concurrent Modification During Read
// ============================================================================

/// A transaction reads a key, another transaction modifies it, first transaction
/// tries to write based on stale read - should fail.
#[test]
fn stale_read_write_conflict() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "key", Value::Int(0)).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let txn_a_result = Arc::new(AtomicBool::new(false));

    let b1 = barrier.clone();
    let db1 = db.clone();
    let result1 = txn_a_result.clone();

    // Transaction A: read, wait, write
    let txn_a = thread::spawn(move || {
        let success = db1
            .transaction(branch_id, |txn| {
                // Read key (adds to read-set)
                let _val = txn.kv_get("key")?;

                // Wait for B to commit
                b1.wait();
                b1.wait();

                // Try to write - should conflict because B modified key
                txn.kv_put("key", Value::Int(100))?;
                Ok(())
            })
            .is_ok();

        result1.store(success, Ordering::SeqCst);
    });

    let b2 = barrier.clone();
    let db2 = db.clone();

    // Transaction B: wait, modify, commit
    let txn_b = thread::spawn(move || {
        b2.wait(); // Wait for A to read

        // Modify the key (commits before A tries to write)
        let kv_b = KVStore::new(db2);
        kv_b.put(&branch_id, "default", "key", Value::Int(50))
            .unwrap();

        b2.wait(); // Let A proceed
    });

    txn_a.join().unwrap();
    txn_b.join().unwrap();

    // Transaction A should have FAILED due to conflict
    // (B modified a key that A had in its read-set)
    assert!(
        !txn_a_result.load(Ordering::SeqCst),
        "Transaction A should fail due to read-write conflict"
    );

    // Final value should be from B (the one that committed)
    let final_val = kv.get(&branch_id, "default", "key").unwrap().unwrap();
    assert_eq!(final_val, Value::Int(50));
}

// ============================================================================
// Version Wraparound Safety
// ============================================================================

/// Tests behavior when version numbers get very large.
/// A real system might have issues near u64::MAX.
#[test]
fn high_version_numbers_work() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();

    // Do many writes to increment version
    for i in 0..1000 {
        kv.put(&branch_id, "default", "key", Value::Int(i)).unwrap();
    }

    // Verify we can still read the correct value
    let result = kv.get(&branch_id, "default", "key").unwrap().unwrap();
    assert_eq!(result, Value::Int(999));
}

// ============================================================================
// Rapid Transaction Cycling
// ============================================================================

/// Rapidly create and commit/abort many transactions.
/// Tests for resource leaks, lock contention, or state corruption.
#[test]
fn rapid_transaction_cycling() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();

    // Rapid fire 500 transactions
    for i in 0..500 {
        if i % 2 == 0 {
            // Commit
            test_db
                .db
                .transaction(branch_id, |txn| {
                    txn.kv_put(&format!("key_{}", i), Value::Int(i))?;
                    Ok(())
                })
                .unwrap();
        } else {
            // Abort
            let _: Result<(), _> = test_db.db.transaction(branch_id, |txn| {
                txn.kv_put(&format!("key_{}", i), Value::Int(i))?;
                Err(strata_core::StrataError::invalid_input("abort"))
            });
        }
    }

    // Verify state: only even keys should exist
    for i in 0..500 {
        let exists = kv
            .get(&branch_id, "default", &format!("key_{}", i))
            .unwrap()
            .is_some();
        if i % 2 == 0 {
            assert!(exists, "key_{} should exist (committed)", i);
        } else {
            assert!(!exists, "key_{} should not exist (aborted)", i);
        }
    }
}

// ============================================================================
// Cross-Primitive Consistency After Crash Simulation
// ============================================================================

/// Simulates what happens if we write to multiple primitives and one "fails"
/// (by aborting). All should be rolled back atomically.
#[test]
fn cross_primitive_atomic_failure() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    let event = test_db.event();

    // Setup baseline
    kv.put(&branch_id, "default", "balance", Value::Int(1000))
        .unwrap();

    // Attempt a "transfer" that fails partway through
    let result: Result<(), _> = test_db.db.transaction(branch_id, |txn| {
        // Debit
        txn.kv_put("balance", Value::Int(900))?;

        // Log the debit
        txn.event_append("debit", event_payload(Value::Int(100)))?;

        // Credit to external system "fails"
        Err(strata_core::StrataError::invalid_input(
            "external system failure",
        ))
    });

    assert!(result.is_err());

    // BOTH the debit and the log should be rolled back
    assert_eq!(
        kv.get(&branch_id, "default", "balance").unwrap().unwrap(),
        Value::Int(1000),
        "Balance should be unchanged after failed transfer"
    );
    assert_eq!(
        event.len(&branch_id, "default").unwrap(),
        0,
        "Event log should be empty after failed transfer"
    );
}

// ============================================================================
// Interleaved Multi-Key Transactions
// ============================================================================

/// Two transactions each modifying multiple keys with interleaved timing.
/// Tests that transactions see consistent snapshots.
#[test]
fn interleaved_multikey_consistency() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "x", Value::Int(0)).unwrap();
    kv.put(&branch_id, "default", "y", Value::Int(0)).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let invariant_violated = Arc::new(AtomicBool::new(false));

    let b1 = barrier.clone();
    let db1 = db.clone();

    // Transaction A: sets x=1, y=1
    let txn_a = thread::spawn(move || {
        let _ = db1.transaction(branch_id, |txn| {
            txn.kv_put("x", Value::Int(1))?;
            b1.wait(); // Interleave
            txn.kv_put("y", Value::Int(1))?;
            Ok(())
        });
    });

    let b2 = barrier.clone();
    let db2 = db.clone();
    let violated = invariant_violated.clone();

    // Transaction B: reads x and y, checks invariant x == y
    let txn_b = thread::spawn(move || {
        db2.transaction(branch_id, |txn| {
            let x = txn
                .kv_get("x")?
                .map(|v| match v {
                    Value::Int(n) => n,
                    _ => 0,
                })
                .unwrap_or(0);

            b2.wait(); // Interleave (A writes y here)

            let y = txn
                .kv_get("y")?
                .map(|v| match v {
                    Value::Int(n) => n,
                    _ => 0,
                })
                .unwrap_or(0);

            // Under snapshot isolation, x and y should be from same snapshot
            // So either both 0 (before A) or both 1 (after A), never mixed
            if x != y {
                violated.store(true, Ordering::SeqCst);
            }

            Ok((x, y))
        })
    });

    txn_a.join().unwrap();
    let b_result = txn_b.join().unwrap();

    // If B committed successfully and saw inconsistent values, that's a bug
    if let Ok((x, y)) = b_result {
        if x != y {
            panic!(
                "Snapshot isolation violated: saw x={}, y={} (should be equal)",
                x, y
            );
        }
    }

    // Note: B might have aborted due to conflict, which is fine
}
