//! Snapshot Isolation Tests
//!
//! Tests the snapshot isolation guarantees:
//! - Point-in-time consistency
//! - Repeatable reads
//! - Read-your-writes semantics

use std::collections::BTreeMap;
use std::sync::Arc;
use strata_concurrency::snapshot::ClonedSnapshotView;
use strata_concurrency::transaction::TransactionContext;
use strata_core::traits::SnapshotView;
use strata_core::types::{Key, Namespace};
use strata_core::value::Value;
use strata_core::{BranchId, Versioned};

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    let ns = Arc::new(Namespace::for_branch(branch_id));
    Key::new_kv(ns, name)
}

type VersionedValue = Versioned<Value>;

// ============================================================================
// Point-in-Time Consistency
// ============================================================================

#[test]
fn snapshot_captures_state_at_creation() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "captured");

    let mut data = BTreeMap::new();
    data.insert(
        key.clone(),
        VersionedValue::new(Value::Int(42), strata_core::Version::seq(1)),
    );

    let snapshot = ClonedSnapshotView::new(1, data);

    let result = SnapshotView::get(&snapshot, &key).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Value::Int(42));
}

#[test]
fn snapshot_is_immutable() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "immutable");

    let mut data = BTreeMap::new();
    data.insert(
        key.clone(),
        VersionedValue::new(Value::Int(100), strata_core::Version::seq(1)),
    );

    let snapshot = ClonedSnapshotView::new(1, data.clone());

    // Modify original data (simulating concurrent write)
    data.insert(
        key.clone(),
        VersionedValue::new(Value::Int(200), strata_core::Version::seq(2)),
    );

    // Snapshot should still see original value
    let result = SnapshotView::get(&snapshot, &key).unwrap();
    assert_eq!(result.unwrap().value, Value::Int(100));
}

#[test]
fn snapshot_version_reflects_creation_time() {
    let snapshot = ClonedSnapshotView::new(42, BTreeMap::new());
    assert_eq!(snapshot.version(), 42);
}

// ============================================================================
// Repeatable Reads
// ============================================================================

#[test]
fn repeated_reads_return_same_value() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "repeat");

    let mut data = BTreeMap::new();
    data.insert(
        key.clone(),
        VersionedValue::new(Value::Int(42), strata_core::Version::seq(1)),
    );

    let snapshot = ClonedSnapshotView::new(1, data);

    // Read multiple times
    let read1 = SnapshotView::get(&snapshot, &key).unwrap();
    let read2 = SnapshotView::get(&snapshot, &key).unwrap();
    let read3 = SnapshotView::get(&snapshot, &key).unwrap();

    assert_eq!(read1, read2);
    assert_eq!(read2, read3);
}

#[test]
fn missing_key_consistently_returns_none() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "missing");

    let snapshot = ClonedSnapshotView::new(1, BTreeMap::new());

    let read1 = SnapshotView::get(&snapshot, &key).unwrap();
    let read2 = SnapshotView::get(&snapshot, &key).unwrap();

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
    let mut txn = TransactionContext::new(1, branch_id, 1);

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

    let mut txn = TransactionContext::new(1, branch_id, 1);

    // Delete a key
    txn.delete_set.insert(key.clone());

    // Transaction should know about the delete
    assert!(txn.delete_set.contains(&key));
}

#[test]
fn write_then_delete_sees_delete() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "write_del");

    let mut txn = TransactionContext::new(1, branch_id, 1);

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
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    let mut data = BTreeMap::new();
    for i in 0..10 {
        let key = Key::new_kv(ns.clone(), &format!("prefix_{}", i));
        data.insert(
            key,
            VersionedValue::new(Value::Int(i), strata_core::Version::seq(1)),
        );
    }

    // Add some non-matching keys
    let other_key = Key::new_kv(ns.clone(), "other_key");
    data.insert(
        other_key,
        VersionedValue::new(Value::Int(999), strata_core::Version::seq(1)),
    );

    let snapshot = ClonedSnapshotView::new(1, data);

    let prefix = Key::new_kv(ns.clone(), "prefix_");
    let results = SnapshotView::scan_prefix(&snapshot, &prefix).unwrap();

    assert_eq!(results.len(), 10);
}

#[test]
fn snapshot_scan_empty_prefix() {
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    let snapshot = ClonedSnapshotView::new(1, BTreeMap::new());

    let prefix = Key::new_kv(ns.clone(), "anything");
    let results = SnapshotView::scan_prefix(&snapshot, &prefix).unwrap();

    assert!(results.is_empty());
}

// ============================================================================
// Snapshot Sharing
// ============================================================================

#[test]
fn snapshot_can_be_shared_via_arc() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "shared");

    let mut data = BTreeMap::new();
    data.insert(
        key.clone(),
        VersionedValue::new(Value::Int(42), strata_core::Version::seq(1)),
    );

    let snapshot = ClonedSnapshotView::new(1, data);
    let snapshot = Arc::new(snapshot);

    let result = SnapshotView::get(&*snapshot, &key).unwrap();
    assert_eq!(result.unwrap().value, Value::Int(42));
}

#[test]
fn cloned_snapshots_are_independent() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "independent");

    let mut data = BTreeMap::new();
    data.insert(
        key.clone(),
        VersionedValue::new(Value::Int(42), strata_core::Version::seq(1)),
    );

    let snapshot1 = ClonedSnapshotView::new(1, data.clone());

    // Modify and create another snapshot
    data.insert(
        key.clone(),
        VersionedValue::new(Value::Int(100), strata_core::Version::seq(2)),
    );
    let snapshot2 = ClonedSnapshotView::new(2, data);

    // Both snapshots retain their values
    assert_eq!(
        SnapshotView::get(&snapshot1, &key).unwrap().unwrap().value,
        Value::Int(42)
    );
    assert_eq!(
        SnapshotView::get(&snapshot2, &key).unwrap().unwrap().value,
        Value::Int(100)
    );
}

// ============================================================================
// Thread Safety
// ============================================================================

#[test]
fn snapshot_is_send_and_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<ClonedSnapshotView>();
    assert_sync::<ClonedSnapshotView>();
}

#[test]
fn snapshot_concurrent_reads() {
    use std::thread;

    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "concurrent");

    let mut data = BTreeMap::new();
    data.insert(
        key.clone(),
        VersionedValue::new(Value::Int(42), strata_core::Version::seq(1)),
    );

    let snapshot = Arc::new(ClonedSnapshotView::new(1, data));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let snapshot = Arc::clone(&snapshot);
            let key = key.clone();
            thread::spawn(move || {
                for _ in 0..1000 {
                    let result = SnapshotView::get(&*snapshot, &key).unwrap();
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
// Empty Snapshot
// ============================================================================

#[test]
fn empty_snapshot_creation() {
    let snapshot = ClonedSnapshotView::empty(0);
    assert_eq!(snapshot.version(), 0);
}

#[test]
fn empty_snapshot_get_returns_none() {
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "any");

    let snapshot = ClonedSnapshotView::empty(0);
    let result = SnapshotView::get(&snapshot, &key).unwrap();
    assert!(result.is_none());
}

#[test]
fn empty_snapshot_scan_returns_empty() {
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let prefix = Key::new_kv(ns, "any");

    let snapshot = ClonedSnapshotView::empty(0);
    let results = SnapshotView::scan_prefix(&snapshot, &prefix).unwrap();
    assert!(results.is_empty());
}
