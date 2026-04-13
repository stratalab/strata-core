//! Concurrent Transaction Tests
//!
//! Tests for concurrent transaction scenarios:
//! - Parallel commits on different branches
//! - Serial commits on same branch
//! - High contention scenarios

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use strata_concurrency::manager::TransactionManager;
use strata_concurrency::transaction::TransactionContext;
use strata_concurrency::validation::validate_transaction;
use strata_core::id::{CommitVersion, TxnId};
use strata_core::traits::{Storage, WriteMode};
use strata_core::types::{Key, Namespace};
use strata_core::value::Value;
use strata_core::BranchId;
use strata_storage::SegmentedStore;

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    let ns = Arc::new(Namespace::for_branch(branch_id));
    Key::new_kv(ns, name)
}

// ============================================================================
// Parallel Commits on Different Runs
// ============================================================================

#[test]
fn parallel_commits_different_runs_no_contention() {
    let store = Arc::new(SegmentedStore::new());
    let manager = Arc::new(TransactionManager::new(CommitVersion(1)));
    let barrier = Arc::new(Barrier::new(4));
    let commits = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let store = Arc::clone(&store);
            let manager = Arc::clone(&manager);
            let barrier = Arc::clone(&barrier);
            let commits = Arc::clone(&commits);

            thread::spawn(move || {
                let branch_id = BranchId::new(); // Each thread gets unique branch
                let key = create_test_key(branch_id, "data");

                // Setup initial value
                store
                    .put_with_version_mode(
                        key.clone(),
                        Value::Int(0),
                        CommitVersion(1),
                        None,
                        WriteMode::Append,
                    )
                    .unwrap();

                barrier.wait();

                // Each thread commits 10 transactions
                for i in 0..10 {
                    let txn_id = manager.next_txn_id().unwrap();
                    let mut txn = TransactionContext::new(
                        txn_id,
                        branch_id,
                        manager.allocate_version().unwrap(),
                    );

                    // Read and write
                    let v = store
                        .get_versioned(&key, CommitVersion::MAX)
                        .unwrap()
                        .unwrap()
                        .version
                        .as_u64();
                    txn.read_set.insert(key.clone(), CommitVersion(v));
                    txn.write_set.insert(key.clone(), Value::Int(i));

                    // Validate
                    let result = validate_transaction(&txn, &*store);
                    if result.unwrap().is_valid() {
                        let commit_ver = manager.allocate_version().unwrap();
                        store
                            .put_with_version_mode(
                                key.clone(),
                                Value::Int(i),
                                commit_ver,
                                None,
                                WriteMode::Append,
                            )
                            .unwrap();
                        commits.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // All 40 commits should succeed (no cross-branch contention)
    assert_eq!(commits.load(Ordering::Relaxed), 40);
}

#[test]
fn different_branches_have_independent_namespaces() {
    let store = Arc::new(SegmentedStore::new());
    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    let key1 = create_test_key(branch1, "shared_name");
    let key2 = create_test_key(branch2, "shared_name");

    // Write different values to same logical name in different branches
    store
        .put_with_version_mode(
            key1.clone(),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            key2.clone(),
            Value::Int(200),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // They should be independent
    let val1 = store
        .get_versioned(&key1, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .value;
    let val2 = store
        .get_versioned(&key2, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .value;

    assert_eq!(val1, Value::Int(100));
    assert_eq!(val2, Value::Int(200));
}

// ============================================================================
// High Contention Single Key
// ============================================================================

#[test]
fn high_contention_single_key() {
    let store = Arc::new(SegmentedStore::new());
    let manager = Arc::new(TransactionManager::new(CommitVersion(1)));
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "contested");

    // Initial value
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(0),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let barrier = Arc::new(Barrier::new(8));
    let commits = Arc::new(AtomicU64::new(0));
    let retries = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..8)
        .map(|thread_id| {
            let store = Arc::clone(&store);
            let manager = Arc::clone(&manager);
            let barrier = Arc::clone(&barrier);
            let commits = Arc::clone(&commits);
            let retries = Arc::clone(&retries);
            let key = key.clone();

            thread::spawn(move || {
                barrier.wait();

                for i in 0..10 {
                    loop {
                        // Read current value
                        let current = store
                            .get_versioned(&key, CommitVersion::MAX)
                            .unwrap()
                            .unwrap();
                        let read_version = CommitVersion(current.version.as_u64());

                        // Create transaction
                        let txn_id = manager.next_txn_id().unwrap();
                        let mut txn =
                            TransactionContext::new(txn_id, branch_id, read_version);
                        txn.read_set.insert(key.clone(), read_version);
                        txn.write_set
                            .insert(key.clone(), Value::Int((thread_id * 100 + i) as i64));

                        // Validate
                        let result = validate_transaction(&txn, &*store);
                        if result.unwrap().is_valid() {
                            let commit_ver = manager.allocate_version().unwrap();
                            store
                                .put_with_version_mode(
                                    key.clone(),
                                    Value::Int((thread_id * 100 + i) as i64),
                                    commit_ver,
                                    None,
                                    WriteMode::Append,
                                )
                                .unwrap();
                            commits.fetch_add(1, Ordering::Relaxed);
                            break;
                        } else {
                            retries.fetch_add(1, Ordering::Relaxed);
                            // Retry with fresh read
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let total_commits = commits.load(Ordering::Relaxed);

    // All 80 operations must eventually commit (with retries)
    assert_eq!(
        total_commits, 80,
        "All 80 operations must eventually commit"
    );
}

// ============================================================================
// Interleaved Operations (No Conflict)
// ============================================================================

#[test]
fn interleaved_disjoint_operations_both_commit() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key_a = create_test_key(branch_id, "A");
    let key_b = create_test_key(branch_id, "B");

    // Initial values
    store
        .put_with_version_mode(
            key_a.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            key_b.clone(),
            Value::Int(2),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let va = CommitVersion(store
        .get_versioned(&key_a, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64());
    let vb = CommitVersion(store
        .get_versioned(&key_b, CommitVersion::MAX)
        .unwrap()
        .unwrap()
        .version
        .as_u64());

    // T1: reads A, writes B
    let mut t1 = TransactionContext::new(TxnId(1), branch_id, CommitVersion(1));
    t1.read_set.insert(key_a.clone(), va);
    t1.write_set.insert(key_b.clone(), Value::Int(20));

    // T2: reads B, writes A
    let mut t2 = TransactionContext::new(TxnId(2), branch_id, CommitVersion(1));
    t2.read_set.insert(key_b.clone(), vb);
    t2.write_set.insert(key_a.clone(), Value::Int(10));

    // Both should validate (disjoint read/write sets)
    let result1 = validate_transaction(&t1, &*store);
    let result2 = validate_transaction(&t2, &*store);

    assert!(
        result1.unwrap().is_valid(),
        "T1 should commit (reads A, writes B)"
    );
    assert!(
        result2.unwrap().is_valid(),
        "T2 should commit (reads B, writes A)"
    );
}

// ============================================================================
// Version Allocation
// ============================================================================

#[test]
fn version_allocation_is_unique() {
    let manager = Arc::new(TransactionManager::new(CommitVersion(1)));
    let barrier = Arc::new(Barrier::new(8));

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let manager = Arc::clone(&manager);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();
                let mut versions = Vec::with_capacity(1000);
                for _ in 0..1000 {
                    versions.push(manager.allocate_version().unwrap());
                }
                versions
            })
        })
        .collect();

    let mut all_versions: Vec<CommitVersion> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    all_versions.sort();

    // Check for duplicates
    for i in 1..all_versions.len() {
        assert_ne!(
            all_versions[i],
            all_versions[i - 1],
            "Duplicate version: {}",
            all_versions[i]
        );
    }
}

#[test]
fn txn_id_allocation_is_unique() {
    let manager = Arc::new(TransactionManager::new(CommitVersion(1)));
    let barrier = Arc::new(Barrier::new(4));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let manager = Arc::clone(&manager);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();
                let mut ids = Vec::with_capacity(100);
                for _ in 0..100 {
                    ids.push(manager.next_txn_id().unwrap());
                }
                ids
            })
        })
        .collect();

    let mut all_ids: Vec<TxnId> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    all_ids.sort();

    // Check for duplicates
    for i in 1..all_ids.len() {
        assert_ne!(
            all_ids[i],
            all_ids[i - 1],
            "Duplicate txn_id: {}",
            all_ids[i]
        );
    }
}

// ============================================================================
// Transaction Manager State
// ============================================================================

#[test]
fn manager_version_monotonically_increases() {
    let manager = TransactionManager::new(CommitVersion(100));

    let v1 = manager.allocate_version().unwrap();
    let v2 = manager.allocate_version().unwrap();
    let v3 = manager.allocate_version().unwrap();

    assert!(v2 > v1);
    assert!(v3 > v2);
}

#[test]
fn manager_with_initial_version() {
    let manager = TransactionManager::new(CommitVersion(1000));

    let v1 = manager.allocate_version().unwrap();
    assert!(
        v1 >= CommitVersion(1000),
        "First version should be >= initial"
    );
}

#[test]
fn manager_with_txn_id_recovery() {
    // Simulating recovery where we need to continue from a known max txn_id
    let manager = TransactionManager::with_txn_id(CommitVersion(100), TxnId(500));

    let txn_id = manager.next_txn_id().unwrap();
    assert!(txn_id > TxnId(500), "Txn ID should continue from max");
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn concurrent_empty_transactions() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let barrier = Arc::new(Barrier::new(4));

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                // Empty transaction (read-only)
                let txn = TransactionContext::new(TxnId(i as u64), branch_id, CommitVersion(1));
                let result = validate_transaction(&txn, &*store);
                result.unwrap().is_valid()
            })
        })
        .collect();

    for handle in handles {
        assert!(
            handle.join().unwrap(),
            "Empty transactions should always commit"
        );
    }
}

#[test]
fn rapid_transaction_creation() {
    let manager = TransactionManager::new(CommitVersion(1));
    let branch_id = BranchId::new();

    // Create many transactions rapidly
    let txns: Vec<_> = (0..1000)
        .map(|_| {
            let txn_id = manager.next_txn_id().unwrap();
            let start_version = manager.allocate_version().unwrap();
            TransactionContext::new(txn_id, branch_id, start_version)
        })
        .collect();

    // All should have unique IDs
    let mut ids: Vec<TxnId> = txns.iter().map(|t| t.txn_id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 1000);
}
