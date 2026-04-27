//! Multi-Threaded OCC Conflict Tests
//!
//! Validates optimistic concurrency control behavior
//! with 2-thread scenarios per transaction semantics documentation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_engine::database::OpenSpec;
use strata_engine::Database;
use strata_engine::SearchSubsystem;
use strata_storage::{Key, Namespace};
use tempfile::TempDir;

fn create_ns(branch_id: BranchId) -> Arc<Namespace> {
    Arc::new(Namespace::new(branch_id, "default".to_string()))
}

// ============================================================================
// Write-Write Conflict Tests
// ============================================================================

/// Test: Blind writes (no read) - both can commit, last write wins
#[test]
fn test_blind_writes_no_conflict() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path().join("db")).with_subsystem(SearchSubsystem),
    )
    .unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key = Key::new_kv(ns, "blind_key");

    let db1 = Arc::clone(&db);
    let db2 = Arc::clone(&db);
    let key1 = key.clone();
    let key2 = key.clone();

    let barrier = Arc::new(Barrier::new(2));
    let barrier1 = Arc::clone(&barrier);
    let barrier2 = Arc::clone(&barrier);

    // T1: Blind write (no read first)
    let h1 = thread::spawn(move || {
        db1.transaction(branch_id, |txn| {
            barrier1.wait();
            txn.put(key1.clone(), Value::Int(1))?;
            Ok(())
        })
    });

    // T2: Blind write (no read first)
    let h2 = thread::spawn(move || {
        db2.transaction(branch_id, |txn| {
            barrier2.wait();
            txn.put(key2.clone(), Value::Int(2))?;
            Ok(())
        })
    });

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

    // Both should succeed (blind writes don't conflict )
    assert!(r1.is_ok(), "Blind write T1 should succeed");
    assert!(r2.is_ok(), "Blind write T2 should succeed");
}

// ============================================================================
// No Conflict Tests
// ============================================================================

/// Test: T1 and T2 write different keys -> both commit
#[test]
fn test_no_conflict_different_keys() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path().join("db")).with_subsystem(SearchSubsystem),
    )
    .unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);
    let key_a = Key::new_kv(ns.clone(), "key_a");
    let key_b = Key::new_kv(ns, "key_b");

    let db1 = Arc::clone(&db);
    let db2 = Arc::clone(&db);

    let barrier = Arc::new(Barrier::new(2));
    let barrier1 = Arc::clone(&barrier);
    let barrier2 = Arc::clone(&barrier);

    // T1: Write key_a
    let h1 = thread::spawn(move || {
        db1.transaction(branch_id, |txn| {
            barrier1.wait();
            txn.put(key_a.clone(), Value::Int(1))?;
            Ok(())
        })
    });

    // T2: Write key_b
    let h2 = thread::spawn(move || {
        db2.transaction(branch_id, |txn| {
            barrier2.wait();
            txn.put(key_b.clone(), Value::Int(2))?;
            Ok(())
        })
    });

    let r1 = h1.join().unwrap();
    let r2 = h2.join().unwrap();

    // Both should succeed
    assert!(r1.is_ok(), "T1 should succeed");
    assert!(r2.is_ok(), "T2 should succeed");
}

// ============================================================================
// Multi-threaded Contention Tests
// ============================================================================

/// Test: Multi-threaded contention - validates database handles concurrent access
#[test]
fn test_multi_threaded_contention() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path().join("db")).with_subsystem(SearchSubsystem),
    )
    .unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    let num_threads = 4;
    let ops_per_thread = 10;
    let completed_ops = Arc::new(AtomicU64::new(0));

    // Each thread writes to its own key (no contention)
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let ns = ns.clone();
            let completed = Arc::clone(&completed_ops);

            thread::spawn(move || {
                for op in 0..ops_per_thread {
                    let key = Key::new_kv(ns.clone(), format!("t{}_op{}", thread_id, op));
                    let result = db.transaction(branch_id, |txn| {
                        txn.put(key.clone(), Value::Int((thread_id * 100 + op) as i64))?;
                        Ok(())
                    });

                    if result.is_ok() {
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let completed = completed_ops.load(Ordering::Relaxed);

    // All operations on disjoint keys should succeed
    assert_eq!(
        completed,
        (num_threads * ops_per_thread) as u64,
        "All operations should complete successfully"
    );

    // Verify some values are readable via transaction
    let val = db
        .transaction(branch_id, |txn| {
            let key = Key::new_kv(ns.clone(), "t0_op0");
            txn.get(&key)
        })
        .unwrap()
        .unwrap();
    assert_eq!(val, Value::Int(0));
}

/// Test: Read-only transactions never conflict
#[test]
fn test_read_only_transactions_no_conflict() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path().join("db")).with_subsystem(SearchSubsystem),
    )
    .unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Pre-populate with data
    for i in 0..10 {
        let key = Key::new_kv(ns.clone(), format!("key_{}", i));
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(i))?;
            Ok(())
        })
        .unwrap();
    }

    // Run multiple read-only transactions concurrently
    let num_threads = 10;
    let success_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = Arc::clone(&db);
            let ns = ns.clone();
            let success = Arc::clone(&success_count);

            thread::spawn(move || {
                let result = db.transaction(branch_id, |txn| {
                    // Just read, no writes
                    for i in 0..10 {
                        let key = Key::new_kv(ns.clone(), format!("key_{}", i));
                        let _val = txn.get(&key)?;
                    }
                    Ok(())
                });

                if result.is_ok() {
                    success.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // All read-only transactions should succeed
    assert_eq!(
        success_count.load(Ordering::Relaxed),
        num_threads as u64,
        "All read-only transactions should succeed"
    );
}

/// Test: Concurrent transactions on different keys succeed
#[test]
fn test_concurrent_disjoint_transactions() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open_runtime(
        OpenSpec::primary(temp_dir.path().join("db")).with_subsystem(SearchSubsystem),
    )
    .unwrap();

    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    let num_threads = 8;
    let success_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let ns = ns.clone();
            let success = Arc::clone(&success_count);

            thread::spawn(move || {
                // Each thread works on its own set of keys
                let result = db.transaction(branch_id, |txn| {
                    for i in 0..5 {
                        let key = Key::new_kv(ns.clone(), format!("t{}_{}", thread_id, i));
                        txn.put(key, Value::Int((thread_id * 10 + i) as i64))?;
                    }
                    Ok(())
                });

                if result.is_ok() {
                    success.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // All transactions should succeed (no conflicts on disjoint keys)
    assert_eq!(
        success_count.load(Ordering::Relaxed),
        num_threads as u64,
        "All disjoint transactions should succeed"
    );
}
