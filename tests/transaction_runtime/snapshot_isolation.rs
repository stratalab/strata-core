//! Snapshot Isolation Tests
//!
//! Tests the snapshot isolation guarantees:
//! - Point-in-time consistency
//! - Repeatable reads
//! - Read-your-writes semantics

use std::sync::Arc;
use strata_core::id::{CommitVersion, TxnId};
use strata_core::BranchId;
use strata_core::Value;
use strata_storage::{Key, Namespace, SegmentedStore, Storage, TransactionContext, WriteMode};

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    let ns = Arc::new(Namespace::for_branch(branch_id));
    Key::new_kv(ns, name)
}

// ============================================================================
// Point-in-Time Consistency
// ============================================================================

#[test]
fn snapshot_captures_state_at_creation() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "captured");

    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(42),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let result = store.get_versioned(&key, CommitVersion(1)).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Value::Int(42));
}

#[test]
fn snapshot_is_immutable() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "immutable");

    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Capture current version as our "snapshot"
    let snapshot_version = store.version();

    // Write a new version (simulating concurrent write)
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(200),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Reading at snapshot version should still see original value
    let result = store
        .get_versioned(&key, CommitVersion(snapshot_version))
        .unwrap();
    assert_eq!(result.unwrap().value, Value::Int(100));
}

#[test]
fn snapshot_version_reflects_creation_time() {
    let store = Arc::new(SegmentedStore::new());
    store.set_version(CommitVersion(42));
    assert_eq!(store.version(), 42);
}

// ============================================================================
// Repeatable Reads
// ============================================================================

#[test]
fn repeated_reads_return_same_value() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "repeat");

    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(42),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let version = store.version();

    // Read multiple times at the same version
    let read1 = store.get_versioned(&key, CommitVersion(version)).unwrap();
    let read2 = store.get_versioned(&key, CommitVersion(version)).unwrap();
    let read3 = store.get_versioned(&key, CommitVersion(version)).unwrap();

    assert_eq!(read1, read2);
    assert_eq!(read2, read3);
}

#[test]
fn missing_key_consistently_returns_none() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "missing");

    let read1 = store.get_versioned(&key, CommitVersion(1)).unwrap();
    let read2 = store.get_versioned(&key, CommitVersion(1)).unwrap();

    assert!(read1.is_none());
    assert!(read2.is_none());
}

// ============================================================================
// Read-Your-Writes (in TransactionContext)
// ============================================================================

#[test]
fn transaction_sees_own_uncommitted_writes() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "ryw");

    // Transaction without snapshot (for testing)
    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(1));

    // Write a value
    txn.write_set.insert(key.clone(), Value::Int(42));

    // Transaction should see its own write
    if let Some(value) = txn.write_set.get(&key) {
        assert_eq!(*value, Value::Int(42));
    } else {
        panic!("Should see own write");
    }
}

#[test]
fn transaction_sees_own_deletes() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "deleted");

    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(1));

    // Delete a key
    txn.delete_set.insert(key.clone());

    // Transaction should know about the delete
    assert!(txn.delete_set.contains(&key));
}

#[test]
fn write_then_delete_sees_delete() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "write_del");

    let mut txn = TransactionContext::new(TxnId(1), branch_id, CommitVersion(1));

    // Write then delete
    txn.write_set.insert(key.clone(), Value::Int(42));
    txn.delete_set.insert(key.clone());

    // Delete should take precedence
    assert!(txn.delete_set.contains(&key));
}

// ============================================================================
// Scan Operations
// ============================================================================

#[test]
fn snapshot_scan_prefix_returns_matching_keys() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    for i in 0..10 {
        let key = Key::new_kv(ns.clone(), format!("prefix_{}", i));
        store
            .put_with_version_mode(
                key,
                Value::Int(i),
                CommitVersion((i + 1) as u64),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }

    // Add some non-matching keys
    let other_key = Key::new_kv(ns.clone(), "other_key");
    store
        .put_with_version_mode(
            other_key,
            Value::Int(999),
            CommitVersion(11),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let version = CommitVersion(store.version());
    let prefix = Key::new_kv(ns.clone(), "prefix_");
    let results = Storage::scan_prefix(&*store, &prefix, version).unwrap();

    assert_eq!(results.len(), 10);
}

#[test]
fn snapshot_scan_empty_prefix() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    let prefix = Key::new_kv(ns.clone(), "anything");
    let results = Storage::scan_prefix(&*store, &prefix, CommitVersion(1)).unwrap();

    assert!(results.is_empty());
}

// ============================================================================
// Snapshot Sharing
// ============================================================================

#[test]
fn store_can_be_shared_via_arc() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "shared");

    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(42),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let version = store.version();

    // Share via Arc clone
    let store2 = Arc::clone(&store);
    let result = store2.get_versioned(&key, CommitVersion(version)).unwrap();
    assert_eq!(result.unwrap().value, Value::Int(42));
}

#[test]
fn versioned_reads_are_independent() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "independent");

    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(42),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let version1 = store.version();

    // Write a new version
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(100),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let version2 = store.version();

    // Both versions retain their values
    assert_eq!(
        store
            .get_versioned(&key, CommitVersion(version1))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(42)
    );
    assert_eq!(
        store
            .get_versioned(&key, CommitVersion(version2))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(100)
    );
}

// ============================================================================
// Thread Safety
// ============================================================================

#[test]
fn sharded_store_is_send_and_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<SegmentedStore>();
    assert_sync::<SegmentedStore>();
}

#[test]
fn snapshot_concurrent_reads() {
    use std::thread;

    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "concurrent");

    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(42),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let version = store.version();

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let store = Arc::clone(&store);
            let key = key.clone();
            thread::spawn(move || {
                for _ in 0..1000 {
                    let result = store.get_versioned(&key, CommitVersion(version)).unwrap();
                    assert_eq!(result.unwrap().value, Value::Int(42));
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

// ============================================================================
// Empty Store
// ============================================================================

#[test]
fn empty_store_creation() {
    let store = SegmentedStore::new();
    assert_eq!(store.version(), 0);
}

#[test]
fn empty_store_get_returns_none() {
    let store = SegmentedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "any");

    let result = store.get_versioned(&key, CommitVersion(0)).unwrap();
    assert!(result.is_none());
}

#[test]
fn empty_store_scan_returns_empty() {
    let store = SegmentedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let prefix = Key::new_kv(ns, "any");

    let results = Storage::scan_prefix(&store, &prefix, CommitVersion(0)).unwrap();
    assert!(results.is_empty());
}

// ============================================================================
// TransactionContext Snapshot Isolation
// ============================================================================

/// Core MVCC invariant: a transaction must not see writes that occur after
/// it began, even when reading the same key multiple times through the
/// TransactionContext API.
#[test]
fn transaction_context_ignores_concurrent_store_writes() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "isolation");

    // Write initial value at version 1
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Begin transaction — captures start_version
    let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));
    assert_eq!(txn.start_version, CommitVersion(1));

    // First read through transaction sees initial value
    let read1 = txn.get(&key).unwrap();
    assert_eq!(read1, Some(Value::Int(100)));

    // Concurrent write to the SAME key at a higher version
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(999),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Second read through transaction MUST still see the old value
    let read2 = txn.get(&key).unwrap();
    assert_eq!(
        read2,
        Some(Value::Int(100)),
        "Transaction must not see writes after start_version"
    );
}

/// Verify that scan_prefix through TransactionContext also respects the
/// snapshot version boundary.
#[test]
fn transaction_context_scan_ignores_concurrent_writes() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Write two keys at version 1
    let key_a = Key::new_kv(ns.clone(), "scan_a");
    let key_b = Key::new_kv(ns.clone(), "scan_b");
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
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Begin transaction
    let mut txn = TransactionContext::with_store(TxnId(1), branch_id, Arc::clone(&store));

    // Concurrent write: add a third key at version 2
    let key_c = Key::new_kv(ns.clone(), "scan_c");
    store
        .put_with_version_mode(
            key_c,
            Value::Int(3),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Scan through transaction should only see keys at version <= 1
    let prefix = Key::new_kv(ns, "scan_");
    let results = txn.scan_prefix(&prefix).unwrap();

    assert_eq!(
        results.len(),
        2,
        "Scan must not include key written after start_version"
    );
}
