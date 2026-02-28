//! Branch Isolation Tests
//!
//! Tests that different branches are properly isolated in the storage layer.

use std::sync::Arc;
use std::thread;
use strata_core::traits::Storage;
use strata_core::types::{Key, Namespace};
use strata_core::value::Value;
use strata_core::BranchId;
use strata_storage::sharded::ShardedStore;

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    let ns = Arc::new(Namespace::for_branch(branch_id));
    Key::new_kv(ns, name)
}

// ============================================================================
// Basic Isolation
// ============================================================================

#[test]
fn different_branches_have_separate_namespaces() {
    let store = ShardedStore::new();
    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    // Same key name, different branches
    let key1 = create_test_key(branch1, "shared_name");
    let key2 = create_test_key(branch2, "shared_name");

    Storage::put(&store, key1.clone(), Value::Int(100), None).unwrap();
    Storage::put(&store, key2.clone(), Value::Int(200), None).unwrap();

    let val1 = Storage::get(&store, &key1).unwrap().unwrap().value;
    let val2 = Storage::get(&store, &key2).unwrap().unwrap().value;

    assert_eq!(val1, Value::Int(100));
    assert_eq!(val2, Value::Int(200));
}

#[test]
fn clear_branch_only_affects_target_branch() {
    let store = ShardedStore::new();
    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    // Put keys in both branches
    for i in 0..5 {
        let key1 = create_test_key(branch1, &format!("key_{}", i));
        let key2 = create_test_key(branch2, &format!("key_{}", i));
        Storage::put(&store, key1, Value::Int(i), None).unwrap();
        Storage::put(&store, key2, Value::Int(i + 100), None).unwrap();
    }

    // Clear branch1
    store.clear_branch(&branch1);

    // Branch1 should be empty
    for i in 0..5 {
        let key1 = create_test_key(branch1, &format!("key_{}", i));
        assert!(Storage::get(&store, &key1).unwrap().is_none());
    }

    // Branch2 should still have data
    for i in 0..5 {
        let key2 = create_test_key(branch2, &format!("key_{}", i));
        let val = Storage::get(&store, &key2).unwrap();
        assert!(val.is_some());
        assert_eq!(val.unwrap().value, Value::Int(i + 100));
    }
}

#[test]
fn delete_in_one_branch_doesnt_affect_other() {
    let store = ShardedStore::new();
    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    let key1 = create_test_key(branch1, "shared");
    let key2 = create_test_key(branch2, "shared");

    Storage::put(&store, key1.clone(), Value::Int(1), None).unwrap();
    Storage::put(&store, key2.clone(), Value::Int(2), None).unwrap();

    // Delete in branch1
    Storage::delete(&store, &key1).unwrap();

    // Branch1 deleted
    assert!(Storage::get(&store, &key1).unwrap().is_none());

    // Branch2 unaffected
    assert_eq!(
        Storage::get(&store, &key2).unwrap().unwrap().value,
        Value::Int(2)
    );
}

// ============================================================================
// Concurrent Access Across Branches
// ============================================================================

#[test]
fn concurrent_writes_to_different_branches() {
    let store = Arc::new(ShardedStore::new());
    let num_branches = 8;
    let keys_per_branch = 100;

    let handles: Vec<_> = (0..num_branches)
        .map(|_| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let branch_id = BranchId::new();
                for i in 0..keys_per_branch {
                    let key = create_test_key(branch_id, &format!("key_{}", i));
                    Storage::put(&*store, key, Value::Int(i), None).unwrap();
                }
                branch_id
            })
        })
        .collect();

    let branch_ids: Vec<BranchId> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // Verify all branches have their data
    for branch_id in branch_ids {
        for i in 0..keys_per_branch {
            let key = create_test_key(branch_id, &format!("key_{}", i));
            let val = Storage::get(&*store, &key).unwrap();
            assert!(val.is_some(), "Branch {:?} key {} missing", branch_id, i);
            assert_eq!(val.unwrap().value, Value::Int(i));
        }
    }
}

#[test]
fn concurrent_reads_and_writes_different_branches() {
    let store = Arc::new(ShardedStore::new());
    let read_branch = BranchId::new();
    let write_branch = BranchId::new();

    // Pre-populate read branch
    for i in 0..100 {
        let key = create_test_key(read_branch, &format!("key_{}", i));
        Storage::put(&*store, key, Value::Int(i), None).unwrap();
    }

    let store_read = Arc::clone(&store);
    let store_write = Arc::clone(&store);

    // Reader thread
    let reader = thread::spawn(move || {
        let mut reads = 0u64;
        for _ in 0..1000 {
            for i in 0..100 {
                let key = create_test_key(read_branch, &format!("key_{}", i));
                let val = Storage::get(&*store_read, &key).unwrap();
                assert!(val.is_some());
                assert_eq!(val.unwrap().value, Value::Int(i));
                reads += 1;
            }
        }
        reads
    });

    // Writer thread (different branch)
    let writer = thread::spawn(move || {
        for i in 0..1000 {
            for j in 0..10 {
                let key = create_test_key(write_branch, &format!("key_{}", j));
                Storage::put(&*store_write, key, Value::Int(i), None).unwrap();
            }
        }
    });

    let reads = reader.join().unwrap();
    writer.join().unwrap();

    assert_eq!(reads, 1000 * 100);
}

// ============================================================================
// Branch Listing
// ============================================================================

#[test]
fn branch_ids_lists_all_active_branches() {
    let store = ShardedStore::new();
    let branch1 = BranchId::new();
    let branch2 = BranchId::new();
    let branch3 = BranchId::new();

    // Put one key in each branch
    let key1 = create_test_key(branch1, "k");
    let key2 = create_test_key(branch2, "k");
    let key3 = create_test_key(branch3, "k");

    Storage::put(&store, key1, Value::Int(1), None).unwrap();
    Storage::put(&store, key2, Value::Int(2), None).unwrap();
    Storage::put(&store, key3, Value::Int(3), None).unwrap();

    let branches = store.branch_ids();
    assert_eq!(branches.len(), 3);
    assert!(branches.contains(&branch1));
    assert!(branches.contains(&branch2));
    assert!(branches.contains(&branch3));
}

#[test]
fn branch_entry_count() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    // Put 10 keys
    for i in 0..10 {
        let key = create_test_key(branch_id, &format!("key_{}", i));
        Storage::put(&store, key, Value::Int(i), None).unwrap();
    }

    let count = store.branch_entry_count(&branch_id);
    assert_eq!(count, 10);
}

#[test]
fn list_branch_keys() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Put 5 keys
    for i in 0..5 {
        let key = Key::new_kv(ns.clone(), &format!("key_{}", i));
        Storage::put(&store, key, Value::Int(i), None).unwrap();
    }

    let keys = store.list_branch(&branch_id);
    assert_eq!(keys.len(), 5);
}

// ============================================================================
// Empty Branch Handling
// ============================================================================

#[test]
fn get_from_nonexistent_branch_returns_none() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "never_written");

    let result = Storage::get(&store, &key).unwrap();
    assert!(result.is_none());
}

#[test]
fn clear_nonexistent_branch_succeeds() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    // Should not panic
    store.clear_branch(&branch_id);
}

#[test]
fn branch_entry_count_for_empty_branch() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    let count = store.branch_entry_count(&branch_id);
    assert_eq!(count, 0);
}
