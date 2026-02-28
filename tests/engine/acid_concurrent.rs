//! Concurrent ACID Tests (#802, #803, #804, #811)
//!
//! Tests that verify ACID properties hold under concurrent access:
//! - Conservation invariants across concurrent transfers
//! - Cross-primitive transaction atomicity under contention
//! - Conflict atomicity (all-or-nothing across KV+Event+State)
//! - Durability after concurrent writes and transaction restarts

use crate::common::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use strata_engine::{EventLogExt, KVStoreExt, StateCellExt};

fn event_payload(data: Value) -> Value {
    Value::Object(Box::new(HashMap::from([("data".to_string(), data)])))
}

// ============================================================================
// #802: Concurrent Transfers Conserve Total
// ============================================================================

/// 2 accounts (A=1000, B=1000), 4 threads, 25 transfers each A->B.
/// Retry loop on OCC conflict.
/// Assert: all 100 transfers committed, A=0, B=2000, conservation A+B=2000.
#[test]
fn concurrent_transfers_conserve_total() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "account_a", Value::Int(1000))
        .unwrap();
    kv.put(&branch_id, "default", "account_b", Value::Int(1000))
        .unwrap();

    let num_threads = 4;
    let transfers_per_thread = 25;
    let transfer_amount = 10;
    let total_transfers = num_threads * transfers_per_thread;

    let success_count = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = db.clone();
            let success = success_count.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..transfers_per_thread {
                    loop {
                        let result = db.transaction(branch_id, |txn| {
                            let a_val = txn.kv_get("account_a")?.expect("account_a must exist");
                            let b_val = txn.kv_get("account_b")?.expect("account_b must exist");

                            let a = match a_val {
                                Value::Int(v) => v,
                                _ => panic!("account_a must be Int"),
                            };
                            let b = match b_val {
                                Value::Int(v) => v,
                                _ => panic!("account_b must be Int"),
                            };

                            txn.kv_put("account_a", Value::Int(a - transfer_amount))?;
                            txn.kv_put("account_b", Value::Int(b + transfer_amount))?;
                            Ok(())
                        });

                        match result {
                            Ok(()) => {
                                success.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                            Err(_) => continue, // OCC conflict, retry
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        success_count.load(Ordering::SeqCst),
        total_transfers as u64,
        "All {} transfers must succeed (with retries)",
        total_transfers
    );

    let final_a = kv
        .get(&branch_id, "default", "account_a")
        .unwrap()
        .expect("account_a must exist");
    let final_b = kv
        .get(&branch_id, "default", "account_b")
        .unwrap()
        .expect("account_b must exist");

    let expected_a = 1000 - (total_transfers as i64 * transfer_amount);
    let expected_b = 1000 + (total_transfers as i64 * transfer_amount);

    assert_eq!(
        final_a,
        Value::Int(expected_a),
        "account_a should be {}",
        expected_a
    );
    assert_eq!(
        final_b,
        Value::Int(expected_b),
        "account_b should be {}",
        expected_b
    );

    // Conservation invariant
    let (a_int, b_int) = match (&final_a, &final_b) {
        (Value::Int(a), Value::Int(b)) => (*a, *b),
        _ => panic!("Expected Int values"),
    };
    assert_eq!(
        a_int + b_int,
        2000,
        "Total must be conserved: A({}) + B({}) != 2000",
        a_int,
        b_int
    );
}

// ============================================================================
// #803: Cross-Primitive Transactions Under Contention
// ============================================================================

/// Shared counter in KV, initialized to 0.
/// 4 threads x 25 ops each: read counter, increment, event_append, state_set — all in one txn.
/// Retry loop on conflict.
/// Assert: counter == 100, event count == 100.
#[test]
fn concurrent_cross_primitive_transactions() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "counter", Value::Int(0))
        .unwrap();

    let num_threads = 4;
    let ops_per_thread = 25;
    let total_ops = num_threads * ops_per_thread;

    let success_count = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = db.clone();
            let success = success_count.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..ops_per_thread {
                    loop {
                        let result = db.transaction(branch_id, |txn| {
                            let current = txn.kv_get("counter")?.expect("counter must exist");
                            let n = match current {
                                Value::Int(v) => v,
                                _ => panic!("counter must be Int"),
                            };

                            txn.kv_put("counter", Value::Int(n + 1))?;
                            txn.event_append("increment", event_payload(Value::Int(n + 1)))?;
                            txn.state_set("last_value", Value::Int(n + 1))?;
                            Ok(())
                        });

                        match result {
                            Ok(()) => {
                                success.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                            Err(_) => continue,
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        success_count.load(Ordering::SeqCst),
        total_ops as u64,
        "All {} operations must succeed",
        total_ops
    );

    let final_counter = kv
        .get(&branch_id, "default", "counter")
        .unwrap()
        .expect("counter must exist");
    assert_eq!(
        final_counter,
        Value::Int(total_ops as i64),
        "Counter should be {}",
        total_ops
    );

    let event = EventLog::new(db.clone());
    let event_count = event.len(&branch_id, "default").unwrap();
    assert_eq!(
        event_count, total_ops as u64,
        "Event count should be {}",
        total_ops
    );
}

// ============================================================================
// #803: Conflict Atomicity (All-or-Nothing Across Primitives)
// ============================================================================

/// Shared key + Barrier(2) forcing simultaneous conflict.
/// 2 threads: both read "shared", write KV + event + state.
/// Assert: at most 1 winner; winner's primitives are all-or-nothing.
///
/// The barrier is placed INSIDE the transaction closure so both transactions
/// have already acquired their snapshots before either begins reading.
/// Without this, one transaction could fully commit before the other even
/// starts, allowing the second to read the updated value without conflict.
#[test]
fn concurrent_cross_primitive_conflict_atomicity() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let kv = KVStore::new(db.clone());
    kv.put(&branch_id, "default", "shared", Value::Int(0))
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));

    let handles: Vec<_> = (0..2)
        .map(|i| {
            let db = db.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                let result = db.transaction(branch_id, |txn| {
                    // Barrier inside the closure: both transactions have snapshots
                    // before either reads, guaranteeing they see the same version.
                    barrier.wait();
                    let _current = txn.kv_get("shared")?;
                    txn.kv_put("shared", Value::Int(1))?;
                    txn.event_append("conflict_test", event_payload(Value::Int(i)))?;
                    txn.state_set("conflict_state", Value::Int(i))?;
                    Ok(())
                });

                result.is_ok()
            })
        })
        .collect();

    let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let winners: usize = results.iter().filter(|&&r| r).count();

    assert!(
        winners <= 1,
        "At most one transaction should win, got {}",
        winners
    );

    let final_shared = kv
        .get(&branch_id, "default", "shared")
        .unwrap()
        .expect("shared must exist");
    let event = EventLog::new(db.clone());
    let event_count = event.len(&branch_id, "default").unwrap();

    if winners == 1 {
        // Winner committed: all primitives must reflect the write
        assert_eq!(
            final_shared,
            Value::Int(1),
            "Shared should be 1 if a winner committed"
        );
        assert_eq!(event_count, 1, "Exactly 1 event if a winner committed");
    } else {
        // Both lost: no changes at all
        assert_eq!(
            final_shared,
            Value::Int(0),
            "Shared should be 0 if both lost"
        );
        assert_eq!(event_count, 0, "No events if both lost");
    }
}

// ============================================================================
// #804: Durability Under Concurrent Load
// ============================================================================

/// TestDb::new_strict(), 4 threads x 50 KV writes each (unique keys).
/// Barrier start, then shutdown + reopen.
/// Assert: all 200 keys survive with correct values.
#[test]
fn durability_under_concurrent_load() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let num_threads = 4;
    let writes_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let db = test_db.db.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                barrier.wait();

                let kv = KVStore::new(db);
                for i in 0..writes_per_thread {
                    let key = format!("t{}_{}", t, i);
                    kv.put(
                        &branch_id,
                        "default",
                        &key,
                        Value::Int((t * 1000 + i) as i64),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Shutdown and reopen
    test_db.db.shutdown().unwrap();
    test_db.reopen();

    // Verify all keys survived
    let kv = test_db.kv();
    for t in 0..num_threads {
        for i in 0..writes_per_thread {
            let key = format!("t{}_{}", t, i);
            let expected = Value::Int((t * 1000 + i) as i64);
            let actual = kv.get(&branch_id, "default", &key).unwrap();
            assert_eq!(
                actual,
                Some(expected.clone()),
                "Key {} should be {:?} after restart, got {:?}",
                key,
                expected,
                actual
            );
        }
    }
}

// ============================================================================
// #804: Durability of Concurrent Transactions Survives Restart
// ============================================================================

/// TestDb::new_strict(), shared counter, 4 threads x 25 transaction increments.
/// Retry loop, then shutdown + reopen.
/// Assert: counter == 100 before and after restart.
#[test]
fn durability_concurrent_transactions_survive_restart() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    {
        let kv = KVStore::new(test_db.db.clone());
        kv.put(&branch_id, "default", "counter", Value::Int(0))
            .unwrap();
    }

    let num_threads = 4;
    let ops_per_thread = 25;
    let total_ops = num_threads * ops_per_thread;

    let success_count = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = test_db.db.clone();
            let success = success_count.clone();
            let barrier = barrier.clone();

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..ops_per_thread {
                    loop {
                        let result = db.transaction(branch_id, |txn| {
                            let current = txn.kv_get("counter")?.expect("counter must exist");
                            let n = match current {
                                Value::Int(v) => v,
                                _ => panic!("counter must be Int"),
                            };
                            txn.kv_put("counter", Value::Int(n + 1))?;
                            Ok(())
                        });

                        match result {
                            Ok(()) => {
                                success.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                            Err(_) => continue,
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        success_count.load(Ordering::SeqCst),
        total_ops as u64,
        "All {} increments must succeed",
        total_ops
    );

    // Verify before restart
    {
        let kv = test_db.kv();
        let counter_before = kv
            .get(&branch_id, "default", "counter")
            .unwrap()
            .expect("counter must exist");
        assert_eq!(
            counter_before,
            Value::Int(total_ops as i64),
            "Counter should be {} before restart",
            total_ops
        );
    }

    // Shutdown and reopen
    test_db.db.shutdown().unwrap();
    test_db.reopen();

    // Verify after restart
    let kv_after = test_db.kv();
    let counter_after = kv_after
        .get(&branch_id, "default", "counter")
        .unwrap()
        .expect("counter must exist after restart");
    assert_eq!(
        counter_after,
        Value::Int(total_ops as i64),
        "Counter should be {} after restart",
        total_ops
    );
}
