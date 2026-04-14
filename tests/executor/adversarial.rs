//! Adversarial Tests for Executor Layer
//!
//! These tests probe real invariants under stress and edge conditions:
//! - Concurrent session isolation
//! - Transaction conflict detection
//! - Session crash/drop safety
//! - Command atomicity
//! - Edge cases in serialization and value handling

use crate::common::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use strata_core::Value;
use strata_executor::{Command, Executor, Output, Session};

// ============================================================================
// Concurrent Session Isolation
// ============================================================================

/// Two sessions with transactions should see isolated views
/// Session A's uncommitted writes should NOT be visible to Session B
#[test]
fn concurrent_sessions_isolated_views() {
    let db = create_db();

    let mut session_a = Session::new(db.clone());
    let mut session_b = Session::new(db.clone());

    // Session A begins transaction and writes
    session_a
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session_a
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "isolated_key".into(),
            value: Value::String("session_a_value".into()),
        })
        .unwrap();

    // Session B should NOT see Session A's uncommitted write
    let output = session_b
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "isolated_key".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(None) | Output::Maybe(None) => {
            // Correct: uncommitted writes are not visible
        }
        Output::Maybe(Some(_)) => {
            panic!("ISOLATION VIOLATION: Session B saw Session A's uncommitted write!");
        }
        _ => panic!("Unexpected output"),
    }

    // Session A commits
    session_a.execute(Command::TxnCommit).unwrap();

    // Now Session B should see it
    let output = session_b
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "isolated_key".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::String("session_a_value".into()));
        }
        _ => panic!("Committed value should be visible"),
    }
}

/// Multiple concurrent sessions incrementing a counter
/// Final value must equal number of successful commits
#[test]
fn concurrent_session_increments() {
    let db = create_db();
    let executor = Executor::new(db.clone());

    // Initialize counter
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "counter".into(),
            value: Value::Int(0),
        })
        .unwrap();

    let num_threads = 8;
    let increments_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));
    let successful_commits = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            let successful_commits = successful_commits.clone();

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..increments_per_thread {
                    let mut session = Session::new(db.clone());

                    // Begin transaction
                    if session
                        .execute(Command::TxnBegin {
                            branch: None,
                            options: None,
                        })
                        .is_err()
                    {
                        continue;
                    }

                    // Read current value
                    let current = match session.execute(Command::KvGet {
                        branch: None,
                        space: None,
                        key: "counter".into(),
                        as_of: None,
                    }) {
                        Ok(Output::MaybeVersioned(Some(vv))) => match vv.value {
                            Value::Int(n) => n,
                            _ => continue,
                        },
                        _ => continue,
                    };

                    // Write incremented value
                    if session
                        .execute(Command::KvPut {
                            branch: None,
                            space: None,
                            key: "counter".into(),
                            value: Value::Int(current + 1),
                        })
                        .is_err()
                    {
                        continue;
                    }

                    // Try to commit
                    if session.execute(Command::TxnCommit).is_ok() {
                        successful_commits.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Read final counter value
    let final_value = match executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "counter".into(),
            as_of: None,
        })
        .unwrap()
    {
        Output::MaybeVersioned(Some(vv)) => match vv.value {
            Value::Int(n) => n,
            _ => panic!("Counter should be Int"),
        },
        _ => panic!("Counter should exist"),
    };

    let commits = successful_commits.load(Ordering::SeqCst);

    // INVARIANT: Final value must equal number of successful commits
    assert_eq!(
        final_value as u64, commits,
        "ATOMICITY VIOLATION: final_value={} but successful_commits={}",
        final_value, commits
    );
}

// ============================================================================
// Session Drop Safety
// ============================================================================

/// Dropping a session with an active transaction must rollback
#[test]
fn session_drop_rolls_back_transaction() {
    let db = create_db();

    // Write in a transaction, then drop session without commit
    {
        let mut session = Session::new(db.clone());
        session
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .unwrap();
        session
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: "drop_test".into(),
                value: Value::String("should_not_persist".into()),
            })
            .unwrap();
        // Session dropped here - transaction should be rolled back
    }

    // Verify the write was NOT persisted
    let executor = Executor::new(db);
    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "drop_test".into(),
            as_of: None,
        })
        .unwrap();

    assert!(
        matches!(output, Output::MaybeVersioned(None) | Output::Maybe(None)),
        "DROP SAFETY VIOLATION: Uncommitted write persisted after session drop"
    );
}

/// Dropping a session after commit should preserve the data
#[test]
fn session_drop_after_commit_preserves_data() {
    let db = create_db();

    {
        let mut session = Session::new(db.clone());
        session
            .execute(Command::TxnBegin {
                branch: None,
                options: None,
            })
            .unwrap();
        session
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: "commit_drop_test".into(),
                value: Value::String("should_persist".into()),
            })
            .unwrap();
        session.execute(Command::TxnCommit).unwrap();
        // Session dropped after commit
    }

    // Verify the write WAS persisted
    let executor = Executor::new(db);
    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "commit_drop_test".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::String("should_persist".into()));
        }
        _ => panic!("Committed data should persist after session drop"),
    }
}

// ============================================================================
// Transaction State Machine
// ============================================================================

/// Double begin should fail
#[test]
fn double_begin_fails() {
    let mut session = create_session();

    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    let result = session.execute(Command::TxnBegin {
        branch: None,
        options: None,
    });

    assert!(result.is_err(), "Double begin should fail");
}

/// Commit without begin should fail
#[test]
fn commit_without_begin_fails() {
    let mut session = create_session();

    let result = session.execute(Command::TxnCommit);

    assert!(result.is_err(), "Commit without begin should fail");
}

/// Rollback without begin should fail
#[test]
fn rollback_without_begin_fails() {
    let mut session = create_session();

    let result = session.execute(Command::TxnRollback);

    assert!(result.is_err(), "Rollback without begin should fail");
}

/// After commit, session should allow new transaction
#[test]
fn new_transaction_after_commit() {
    let mut session = create_session();

    // First transaction
    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "txn1".into(),
            value: Value::Int(1),
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();

    // Second transaction on same session
    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "txn2".into(),
            value: Value::Int(2),
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();

    // Both should be visible
    let output = session
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "txn1".into(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(
        output,
        Output::MaybeVersioned(Some(_)) | Output::Maybe(Some(_))
    ));

    let output = session
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "txn2".into(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(
        output,
        Output::MaybeVersioned(Some(_)) | Output::Maybe(Some(_))
    ));
}

/// After rollback, session should allow new transaction
#[test]
fn new_transaction_after_rollback() {
    let mut session = create_session();

    // First transaction - rolled back
    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "rolled_back".into(),
            value: Value::Int(1),
        })
        .unwrap();
    session.execute(Command::TxnRollback).unwrap();

    // Second transaction - committed
    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    session
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "committed".into(),
            value: Value::Int(2),
        })
        .unwrap();
    session.execute(Command::TxnCommit).unwrap();

    // Only committed should be visible
    let output = session
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "rolled_back".into(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(
        output,
        Output::MaybeVersioned(None) | Output::Maybe(None)
    ));

    let output = session
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "committed".into(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(
        output,
        Output::MaybeVersioned(Some(_)) | Output::Maybe(Some(_))
    ));
}

// ============================================================================
// Value Edge Cases
// ============================================================================

/// Empty string should be preserved exactly
#[test]
fn empty_string_preserved() {
    let executor = create_executor();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "empty".into(),
            value: Value::String("".into()),
        })
        .unwrap();

    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "empty".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::String("".into()));
        }
        _ => panic!("Empty string should be retrievable"),
    }
}

/// Null values stored via KvPut are preserved (not treated as deletion)
#[test]
fn null_value_is_storable() {
    let executor = create_executor();

    // First store a real value
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "null_key".into(),
            value: Value::Int(42),
        })
        .unwrap();

    // Verify it exists
    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "null_key".into(),
            as_of: None,
        })
        .unwrap();
    assert!(matches!(
        output,
        Output::MaybeVersioned(Some(_)) | Output::Maybe(Some(_))
    ));

    // Overwrite with null
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "null_key".into(),
            value: Value::Null,
        })
        .unwrap();

    // Null is a real value, not a deletion
    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "null_key".into(),
            as_of: None,
        })
        .unwrap();

    assert!(
        matches!(output, Output::MaybeVersioned(Some(ref vv)) if vv.value == Value::Null)
            || matches!(output, Output::Maybe(Some(Value::Null))),
        "Storing Null should preserve the value, not delete the key"
    );

    // Missing key still returns None
    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "missing_key".into(),
            as_of: None,
        })
        .unwrap();

    assert!(matches!(
        output,
        Output::MaybeVersioned(None) | Output::Maybe(None)
    ));
}

/// Integer boundary values should be preserved
#[test]
fn integer_boundaries_preserved() {
    let executor = create_executor();

    let test_values = [
        ("max", i64::MAX),
        ("min", i64::MIN),
        ("zero", 0),
        ("neg_one", -1),
    ];

    for (key, value) in test_values {
        executor
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: key.into(),
                value: Value::Int(value),
            })
            .unwrap();

        let output = executor
            .execute(Command::KvGet {
                branch: None,
                space: None,
                key: key.into(),
                as_of: None,
            })
            .unwrap();

        match output {
            Output::MaybeVersioned(Some(vv)) => {
                let val = vv.value;
                assert_eq!(
                    val,
                    Value::Int(value),
                    "Integer {} should be preserved",
                    key
                );
            }
            _ => panic!("Integer {} should be retrievable", key),
        }
    }
}

/// Float special values should be handled correctly
#[test]
fn float_special_values() {
    let executor = create_executor();

    // Normal floats
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "pi".into(),
            value: Value::Float(std::f64::consts::PI),
        })
        .unwrap();

    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "pi".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => match vv.value {
            Value::Float(f) => {
                assert!((f - std::f64::consts::PI).abs() < 1e-10);
            }
            _ => panic!("Expected Float"),
        },
        _ => panic!("Float should be retrievable"),
    }

    // Infinity
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "inf".into(),
            value: Value::Float(f64::INFINITY),
        })
        .unwrap();

    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "inf".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => match vv.value {
            Value::Float(f) => {
                assert!(f.is_infinite() && f.is_sign_positive());
            }
            _ => panic!("Expected Float"),
        },
        _ => panic!("Infinity should be retrievable"),
    }

    // NaN - Note: NaN != NaN by IEEE-754
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "nan".into(),
            value: Value::Float(f64::NAN),
        })
        .unwrap();

    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "nan".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => match vv.value {
            Value::Float(f) => {
                assert!(f.is_nan(), "NaN should be preserved");
            }
            _ => panic!("Expected Float"),
        },
        _ => panic!("NaN should be retrievable"),
    }
}

/// Large nested object should be preserved
#[test]
fn large_nested_object() {
    let executor = create_executor();

    // Build a nested object
    let mut inner = std::collections::HashMap::new();
    for i in 0..100 {
        inner.insert(format!("field_{}", i), Value::Int(i));
    }

    let mut outer = std::collections::HashMap::new();
    outer.insert("nested".to_string(), Value::object(inner));
    outer.insert(
        "array".to_string(),
        Value::array((0..100).map(Value::Int).collect()),
    );

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "large_object".into(),
            value: Value::object(outer.clone()),
        })
        .unwrap();

    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "large_object".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::object(outer));
        }
        _ => panic!("Large object should be retrievable"),
    }
}

// ============================================================================
// Concurrent Executor Operations
// ============================================================================

/// Multiple executors reading same key concurrently
#[test]
fn concurrent_reads_consistent() {
    let db = create_db();
    let executor = Executor::new(db.clone());

    // Write initial value
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "concurrent_read".into(),
            value: Value::Int(42),
        })
        .unwrap();

    let num_threads = 16;
    let reads_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                let executor = Executor::new(db);
                barrier.wait();

                for _ in 0..reads_per_thread {
                    let output = executor
                        .execute(Command::KvGet {
                            branch: None,
                            space: None,
                            key: "concurrent_read".into(),
                            as_of: None,
                        })
                        .unwrap();

                    match output {
                        Output::MaybeVersioned(Some(vv)) => {
                            let val = vv.value;
                            assert_eq!(val, Value::Int(42), "Read inconsistency detected!");
                        }
                        _ => panic!("Key should exist"),
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

/// Concurrent writes to different keys should all succeed
#[test]
fn concurrent_writes_different_keys() {
    let db = create_db();

    let num_threads = 16;
    let writes_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = db.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                let executor = Executor::new(db);
                barrier.wait();

                for i in 0..writes_per_thread {
                    let key = format!("thread_{}_key_{}", thread_id, i);
                    executor
                        .execute(Command::KvPut {
                            branch: None,
                            space: None,
                            key,
                            value: Value::Int((thread_id * 1000 + i) as i64),
                        })
                        .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify all writes
    let executor = Executor::new(db);
    for thread_id in 0..num_threads {
        for i in 0..writes_per_thread {
            let key = format!("thread_{}_key_{}", thread_id, i);
            let output = executor
                .execute(Command::KvGet {
                    branch: None,
                    space: None,
                    key,
                    as_of: None,
                })
                .unwrap();

            match output {
                Output::MaybeVersioned(Some(vv)) => {
                    let val = vv.value;
                    assert_eq!(val, Value::Int((thread_id * 1000 + i) as i64));
                }
                _ => panic!(
                    "Write should have succeeded for thread {} key {}",
                    thread_id, i
                ),
            }
        }
    }
}

// ============================================================================
// Command Atomicity
// ============================================================================

/// KvPut should be atomic - either fully succeeds or fails
#[test]
fn kv_put_atomic() {
    let db = create_db();
    let executor = Executor::new(db.clone());

    // Write initial value
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "atomic_test".into(),
            value: Value::Int(1),
        })
        .unwrap();

    // Concurrent updates - each should fully succeed
    let num_threads = 8;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = db.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                let executor = Executor::new(db);
                barrier.wait();

                for _ in 0..100 {
                    // Each write should fully succeed (no partial writes)
                    executor
                        .execute(Command::KvPut {
                            branch: None,
                            space: None,
                            key: "atomic_test".into(),
                            value: Value::Int(thread_id as i64),
                        })
                        .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Final value should be one of the thread IDs (0 to num_threads-1)
    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "atomic_test".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => match vv.value {
            Value::Int(n) => {
                assert!(
                    n >= 0 && n < num_threads as i64,
                    "Final value {} should be a valid thread ID",
                    n
                );
            }
            _ => panic!("Expected Int"),
        },
        _ => panic!("Key should exist"),
    }
}

// ============================================================================
// Branch Isolation at Executor Level
// ============================================================================

/// Operations in different branches must be isolated
#[test]
fn executor_branch_isolation() {
    let executor = create_executor();

    // Create two branches with human-readable names
    let branch_a = match executor
        .execute(Command::BranchCreate {
            branch_id: Some("isolation-test-a".into()),
            metadata: None,
        })
        .unwrap()
    {
        Output::BranchWithVersion { info, .. } => info.id,
        _ => panic!("Expected BranchWithVersion"),
    };

    let branch_b = match executor
        .execute(Command::BranchCreate {
            branch_id: Some("isolation-test-b".into()),
            metadata: None,
        })
        .unwrap()
    {
        Output::BranchWithVersion { info, .. } => info.id,
        _ => panic!("Expected BranchWithVersion"),
    };

    // Write same key to both branches with different values
    executor
        .execute(Command::KvPut {
            branch: Some(branch_a.clone()),
            space: None,
            key: "shared_key".into(),
            value: Value::String("value_in_branch_a".into()),
        })
        .unwrap();

    executor
        .execute(Command::KvPut {
            branch: Some(branch_b.clone()),
            space: None,
            key: "shared_key".into(),
            value: Value::String("value_in_branch_b".into()),
        })
        .unwrap();

    // Each branch should see its own value
    let output_a = executor
        .execute(Command::KvGet {
            branch: Some(branch_a),
            space: None,
            key: "shared_key".into(),
            as_of: None,
        })
        .unwrap();

    let output_b = executor
        .execute(Command::KvGet {
            branch: Some(branch_b),
            space: None,
            key: "shared_key".into(),
            as_of: None,
        })
        .unwrap();

    let va = extract_maybe_value(output_a).expect("Branch A should have a value");
    let vb = extract_maybe_value(output_b).expect("Branch B should have a value");
    assert_eq!(va, Value::String("value_in_branch_a".into()));
    assert_eq!(vb, Value::String("value_in_branch_b".into()));
}

// ============================================================================
// Strata API Thread Safety
// ============================================================================

/// Strata API should be safe to use from multiple threads
#[test]
fn strata_api_thread_safe() {
    // Use Strata::cache() directly for ephemeral test database
    let strata = Arc::new(strata_executor::Strata::cache().unwrap());

    let num_threads = 8;
    let ops_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let strata = strata.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                barrier.wait();

                for i in 0..ops_per_thread {
                    let key = format!("strata_thread_{}_key_{}", thread_id, i);

                    // Write
                    strata.kv_put(&key, Value::Int(i as i64)).unwrap();

                    // Read back
                    let value = strata.kv_get(&key).unwrap();
                    assert_eq!(value, Some(Value::Int(i as i64)));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================================
// Error Recovery
// ============================================================================

/// After an error, the executor should still be usable
#[test]
fn error_recovery() {
    let executor = create_executor();

    // Cause an error (search on nonexistent vector collection)
    let result = executor.execute(Command::VectorQuery {
        branch: None,
        space: None,
        collection: "nonexistent".into(),
        query: vec![1.0, 0.0, 0.0, 0.0],
        k: 10,
        filter: None,
        metric: None,
        as_of: None,
    });
    assert!(result.is_err());

    // Executor should still work
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "recovery_test".into(),
            value: Value::Int(123),
        })
        .unwrap();

    let output = executor
        .execute(Command::KvGet {
            branch: None,
            space: None,
            key: "recovery_test".into(),
            as_of: None,
        })
        .unwrap();

    match output {
        Output::MaybeVersioned(Some(vv)) => {
            let val = vv.value;
            assert_eq!(val, Value::Int(123));
        }
        _ => panic!("Executor should recover from errors"),
    }
}

/// After a transaction error, session should still be usable
#[test]
fn session_error_recovery() {
    let mut session = create_session();

    // Start transaction
    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();

    // Cause an error (try to begin again)
    let result = session.execute(Command::TxnBegin {
        branch: None,
        options: None,
    });
    assert!(result.is_err());

    // Session should still have the original transaction active
    assert!(session.in_transaction());

    // Should be able to commit
    session.execute(Command::TxnCommit).unwrap();

    // Should be able to start a new transaction
    session
        .execute(Command::TxnBegin {
            branch: None,
            options: None,
        })
        .unwrap();
    assert!(session.in_transaction());
    session.execute(Command::TxnRollback).unwrap();
}
