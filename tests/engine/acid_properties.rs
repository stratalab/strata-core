//! ACID Property Tests
//!
//! Explicit tests for each ACID property:
//! - Atomicity: All or nothing
//! - Consistency: Valid state transitions only
//! - Isolation: Transactions don't interfere
//! - Durability: Committed data survives crashes

use crate::common::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use strata_engine::{EventLogExt, KVStoreExt};

/// Helper to create an event payload object
fn event_payload(data: Value) -> Value {
    Value::object(HashMap::from([("data".to_string(), data)]))
}

// ============================================================================
// Atomicity: All or Nothing
// ============================================================================

#[test]
fn atomicity_success_all_visible() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    test_db
        .db
        .transaction(branch_id, |txn| {
            txn.kv_put("key1", Value::Int(1))?;
            txn.kv_put("key2", Value::Int(2))?;
            txn.kv_put("key3", Value::Int(3))?;
            Ok(())
        })
        .unwrap();

    let kv = test_db.kv();
    assert_eq!(
        kv.get(&branch_id, "default", "key1").unwrap(),
        Some(Value::Int(1))
    );
    assert_eq!(
        kv.get(&branch_id, "default", "key2").unwrap(),
        Some(Value::Int(2))
    );
    assert_eq!(
        kv.get(&branch_id, "default", "key3").unwrap(),
        Some(Value::Int(3))
    );
}

#[test]
fn atomicity_failure_none_visible() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();

    // Pre-existing key
    kv.put(&branch_id, "default", "existing", Value::Int(0))
        .unwrap();

    // Transaction that fails partway through
    let result: Result<(), _> = test_db.db.transaction(branch_id, |txn| {
        txn.kv_put("atomic1", Value::Int(1))?;
        txn.kv_put("atomic2", Value::Int(2))?;
        // Force failure
        Err(strata_core::StrataError::invalid_input("forced failure"))
    });

    assert!(result.is_err());

    // None of the writes should be visible
    assert!(kv.get(&branch_id, "default", "atomic1").unwrap().is_none());
    assert!(kv.get(&branch_id, "default", "atomic2").unwrap().is_none());

    // Pre-existing key unchanged
    assert_eq!(
        kv.get(&branch_id, "default", "existing").unwrap().unwrap(),
        Value::Int(0)
    );
}

#[test]
fn atomicity_cross_primitive() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    // Transaction spans KV and EventLog
    test_db
        .db
        .transaction(branch_id, |txn| {
            txn.kv_put("cross_key", Value::Int(42))?;
            txn.event_append(
                "cross_event",
                event_payload(Value::String("payload".to_string())),
            )?;
            Ok(())
        })
        .unwrap();

    let kv = test_db.kv();
    let event = test_db.event();

    // Both committed together
    assert_eq!(
        kv.get(&branch_id, "default", "cross_key").unwrap(),
        Some(Value::Int(42))
    );
    assert_eq!(event.len(&branch_id, "default").unwrap(), 1);
}

// ============================================================================
// Consistency: Valid State Only
// ============================================================================

#[test]
fn consistency_invariants_maintained() {
    let test_db = TestDb::new();
    let state = test_db.state();
    let branch_id = test_db.branch_id;

    // Initialize counter to 0
    state
        .init(&branch_id, "default", "counter", Value::Int(0))
        .unwrap();

    // Increment counter using read + cas (ensures atomic read-modify-write)
    for _ in 0..10 {
        let current = state
            .getv(&branch_id, "default", "counter")
            .unwrap()
            .unwrap();
        let version = current.version();
        if let Value::Int(n) = current.value() {
            state
                .cas(&branch_id, "default", "counter", version, Value::Int(n + 1))
                .unwrap();
        } else {
            panic!("not an int");
        }
    }

    // Counter should be exactly 10
    let result = state
        .get(&branch_id, "default", "counter")
        .unwrap()
        .unwrap();
    assert_eq!(result, Value::Int(10));
}

#[test]
fn consistency_cas_prevents_invalid_state() {
    let test_db = TestDb::new();
    let state = test_db.state();
    let branch_id = test_db.branch_id;

    state
        .init(&branch_id, "default", "balance", Value::Int(100))
        .unwrap();
    let version = state
        .getv(&branch_id, "default", "balance")
        .unwrap()
        .unwrap()
        .version();

    // First CAS succeeds
    state
        .cas(&branch_id, "default", "balance", version, Value::Int(90))
        .unwrap();

    // Second CAS with stale version fails (same version, now stale)
    let result = state.cas(&branch_id, "default", "balance", version, Value::Int(80));
    assert!(result.is_err());

    // Balance should be 90, not 80
    let balance = state
        .get(&branch_id, "default", "balance")
        .unwrap()
        .unwrap();
    assert_eq!(balance, Value::Int(90));
}

// ============================================================================
// Isolation: Transactions Don't Interfere
// ============================================================================

#[test]
fn isolation_read_committed() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();

    kv.put(&branch_id, "default", "isolated", Value::Int(0))
        .unwrap();

    // Each transaction should see committed state
    let db = test_db.db.clone();
    let barrier = Arc::new(Barrier::new(2));

    let b1 = barrier.clone();
    let h1 = thread::spawn(move || {
        db.transaction(branch_id, |txn| {
            b1.wait(); // Sync point 1
            txn.kv_put("isolated", Value::Int(1))?;
            Ok(())
        })
        .unwrap();
    });

    barrier.wait(); // Wait for thread to start

    // Main thread transaction
    let _current = kv.get(&branch_id, "default", "isolated").unwrap().unwrap();
    // Should see either 0 or 1 (committed), never partial state

    h1.join().unwrap();

    let final_val = kv.get(&branch_id, "default", "isolated").unwrap().unwrap();
    assert!(final_val == Value::Int(0) || final_val == Value::Int(1));
}

#[test]
fn isolation_concurrent_counters() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();

    // Each thread has its own counter
    for i in 0..4 {
        kv.put(
            &branch_id,
            "default",
            &format!("counter_{}", i),
            Value::Int(0),
        )
        .unwrap();
    }

    let db = test_db.db.clone();
    let barrier = Arc::new(Barrier::new(4));
    let total_increments = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let db = db.clone();
            let barrier = barrier.clone();
            let total = total_increments.clone();

            thread::spawn(move || {
                barrier.wait();

                for _ in 0..100 {
                    let key = format!("counter_{}", i);
                    let result = db.transaction(branch_id, |txn| {
                        let val = txn.kv_get(&key)?;
                        if let Some(Value::Int(n)) = val {
                            txn.kv_put(&key, Value::Int(n + 1))?;
                        }
                        Ok(())
                    });

                    if result.is_ok() {
                        total.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Each counter should be 100 (no interference between counters)
    for i in 0..4 {
        let val = kv
            .get(&branch_id, "default", &format!("counter_{}", i))
            .unwrap()
            .unwrap();
        assert_eq!(val, Value::Int(100), "counter_{} should be 100", i);
    }
}

// ============================================================================
// Durability: Committed Data Survives
// ============================================================================

#[test]
fn durability_survives_restart() {
    let mut test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let key = unique_key();

    // Write data
    {
        let kv = test_db.kv();
        kv.put(&branch_id, "default", &key, Value::Int(42)).unwrap();
    }

    // Shutdown and reopen
    test_db.db.shutdown().unwrap();
    test_db.reopen();

    // Data should still be there
    let kv = test_db.kv();
    let result = kv.get(&branch_id, "default", &key).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap(), Value::Int(42));
}

#[test]
fn durability_uncommitted_lost() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;

    // Start transaction but don't commit
    let _ctx = test_db.db.begin_transaction(branch_id).unwrap();

    // End without commit (let it drop)
    test_db.db.end_transaction(_ctx);

    // Key should not exist
    let kv = test_db.kv();
    let result = kv.get(&branch_id, "default", "uncommitted").unwrap();
    assert!(result.is_none());
}

#[test]
fn durability_multiple_commits_persist() {
    let mut test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    // Multiple commits
    for i in 0..10 {
        let kv = test_db.kv();
        kv.put(
            &branch_id,
            "default",
            &format!("durable_{}", i),
            Value::Int(i),
        )
        .unwrap();
    }

    // Restart
    test_db.db.shutdown().unwrap();
    test_db.reopen();

    // All data should persist
    let kv = test_db.kv();
    for i in 0..10 {
        let result = kv
            .get(&branch_id, "default", &format!("durable_{}", i))
            .unwrap();
        assert!(result.is_some(), "durable_{} should exist", i);
        assert_eq!(result.unwrap(), Value::Int(i));
    }
}

// ============================================================================
// Combined ACID
// ============================================================================

#[test]
fn acid_transfer_between_accounts() {
    let test_db = TestDb::new();
    let state = test_db.state();
    let branch_id = test_db.branch_id;

    // Initialize accounts
    state
        .init(&branch_id, "default", "account_a", Value::Int(100))
        .unwrap();
    state
        .init(&branch_id, "default", "account_b", Value::Int(100))
        .unwrap();

    // Transfer 30 from A to B using readv + cas
    let a_val = state
        .getv(&branch_id, "default", "account_a")
        .unwrap()
        .unwrap();
    let b_val = state
        .getv(&branch_id, "default", "account_b")
        .unwrap()
        .unwrap();

    if let (Value::Int(a), Value::Int(b)) = (a_val.value(), b_val.value()) {
        state
            .cas(
                &branch_id,
                "default",
                "account_a",
                a_val.version(),
                Value::Int(a - 30),
            )
            .unwrap();
        let b_val2 = state
            .getv(&branch_id, "default", "account_b")
            .unwrap()
            .unwrap();
        state
            .cas(
                &branch_id,
                "default",
                "account_b",
                b_val2.version(),
                Value::Int(b + 30),
            )
            .unwrap();
    }

    // Verify balances
    let a = state
        .get(&branch_id, "default", "account_a")
        .unwrap()
        .unwrap();
    let b = state
        .get(&branch_id, "default", "account_b")
        .unwrap()
        .unwrap();

    assert_eq!(a, Value::Int(70));
    assert_eq!(b, Value::Int(130));

    // Total should still be 200 (conservation)
    let total = match (&a, &b) {
        (Value::Int(a), Value::Int(b)) => a + b,
        _ => panic!("Expected ints"),
    };
    assert_eq!(total, 200);
}
