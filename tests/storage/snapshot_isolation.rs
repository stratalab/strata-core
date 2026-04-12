//! Snapshot Isolation Tests
//!
//! Tests the snapshot isolation guarantees:
//! - Snapshots capture a consistent point in time
//! - Concurrent snapshots don't interfere
//! - Snapshot ignores later modifications

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use strata_core::id::CommitVersion;
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
// Snapshot Acquisition
// ============================================================================

#[test]
fn snapshot_captures_current_version() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "capture");

    // Put value
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(42),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Capture version as our "snapshot"
    let version = CommitVersion(store.version());

    // Should see the value at this version
    let result = store.get_versioned(&key, version).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Value::Int(42));
}

#[test]
fn snapshot_acquisition_is_fast() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();

    // Populate store with data
    for i in 0..1000 {
        let key = create_test_key(branch_id, &format!("key_{}", i));
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

    // Measure version capture time (analogous to snapshot acquisition)
    let start = Instant::now();
    for _ in 0..10000 {
        let _version = store.version();
    }
    let elapsed = start.elapsed();

    // Should be very fast (< 1ms per version read on average)
    let avg_ns = elapsed.as_nanos() / 10000;
    assert!(
        avg_ns < 1_000_000, // < 1ms
        "Version read should be O(1), took {} ns average",
        avg_ns
    );
}

#[test]
fn multiple_snapshots_independent() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "multi_snap");

    // Initial value
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let v1 = CommitVersion(store.version());

    // Update value
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(2),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let v2 = CommitVersion(store.version());

    // Update again
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(3),
            CommitVersion(3),
            None,
            WriteMode::Append,
        )
        .unwrap();
    let v3 = CommitVersion(store.version());

    // Each version sees its value
    assert_eq!(
        store.get_versioned(&key, v1).unwrap().unwrap().value,
        Value::Int(1)
    );
    assert_eq!(
        store.get_versioned(&key, v2).unwrap().unwrap().value,
        Value::Int(2)
    );
    assert_eq!(
        store.get_versioned(&key, v3).unwrap().unwrap().value,
        Value::Int(3)
    );
}

// ============================================================================
// Isolation Guarantees
// ============================================================================

#[test]
fn snapshot_ignores_concurrent_writes() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "concurrent");

    // Initial value
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Capture version
    let version = CommitVersion(store.version());

    // Modify after version capture
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(200),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(300),
            CommitVersion(3),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Reading at captured version should still see original value
    let result = store.get_versioned(&key, version).unwrap();
    assert_eq!(result.unwrap().value, Value::Int(100));

    // Current store sees latest
    let current = store.get_versioned(&key, CommitVersion::MAX).unwrap();
    assert_eq!(current.unwrap().value, Value::Int(300));
}

#[test]
fn snapshot_sees_pre_delete_value() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "pre_delete");

    // Put value
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(42),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Capture version
    let version = CommitVersion(store.version());

    // Delete after version capture
    store.delete_with_version(&key, CommitVersion(2)).unwrap();

    // Reading at captured version should still see value
    let result = store.get_versioned(&key, version).unwrap();
    assert!(
        result.is_some(),
        "Should see pre-delete value at captured version"
    );
    assert_eq!(result.unwrap().value, Value::Int(42));

    // Current should not see value
    let current = store.get_versioned(&key, CommitVersion::MAX).unwrap();
    assert!(current.is_none());
}

#[test]
fn repeated_reads_return_same_value() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "repeated");

    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(42),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let version = CommitVersion(store.version());

    // Read multiple times at same version
    let read1 = store.get_versioned(&key, version).unwrap();
    let read2 = store.get_versioned(&key, version).unwrap();
    let read3 = store.get_versioned(&key, version).unwrap();

    assert_eq!(read1, read2);
    assert_eq!(read2, read3);
}

#[test]
fn multi_key_consistency_within_snapshot() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();

    // Put related values
    let key_a = create_test_key(branch_id, "balance_a");
    let key_b = create_test_key(branch_id, "balance_b");

    store
        .put_with_version_mode(
            key_a.clone(),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            key_b.clone(),
            Value::Int(200),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Capture version
    let version = CommitVersion(store.version());

    // Transfer in current store
    store
        .put_with_version_mode(
            key_a.clone(),
            Value::Int(50),
            CommitVersion(3),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            key_b.clone(),
            Value::Int(250),
            CommitVersion(4),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Reading at captured version should see consistent pre-transfer state
    let a = store.get_versioned(&key_a, version).unwrap().unwrap().value;
    let b = store.get_versioned(&key_b, version).unwrap().unwrap().value;

    match (a, b) {
        (Value::Int(a_val), Value::Int(b_val)) => {
            assert_eq!(a_val + b_val, 300, "Should see consistent state at version");
        }
        _ => panic!("Expected Int values"),
    }
}

// ============================================================================
// Concurrent Access
// ============================================================================

#[test]
fn concurrent_readers_dont_block() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();

    // Populate
    for i in 0..100 {
        let key = create_test_key(branch_id, &format!("key_{}", i));
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

    let version = CommitVersion(store.version());
    let barrier = Arc::new(Barrier::new(8));

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut reads = 0u64;
                for _ in 0..1000 {
                    for i in 0..100 {
                        let key = create_test_key(branch_id, &format!("key_{}", i));
                        let _ = store.get_versioned(&key, version);
                        reads += 1;
                    }
                }
                reads
            })
        })
        .collect();

    let total_reads: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(total_reads, 8 * 1000 * 100);
}

#[test]
fn snapshot_survives_store_modifications() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();

    // Initial population
    for i in 0..10 {
        let key = create_test_key(branch_id, &format!("key_{}", i));
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

    let version = CommitVersion(store.version());
    let stop = Arc::new(AtomicBool::new(false));

    // Spawn writer
    let store_clone = Arc::clone(&store);
    let stop_clone = Arc::clone(&stop);
    let writer = thread::spawn(move || {
        let mut counter = 0i64;
        while !stop_clone.load(Ordering::Relaxed) {
            for i in 0..10 {
                let key = create_test_key(branch_id, &format!("key_{}", i));
                let v = CommitVersion(store_clone.next_version());
                let _ = store_clone.put_with_version_mode(
                    key,
                    Value::Int(counter),
                    v,
                    None,
                    WriteMode::Append,
                );
            }
            counter += 1;
            if counter > 100 {
                break;
            }
        }
    });

    // Read at captured version while writer is active
    for _ in 0..100 {
        for i in 0..10 {
            let key = create_test_key(branch_id, &format!("key_{}", i));
            let result = store.get_versioned(&key, version).unwrap();
            // Should see original value (0-9)
            let value = result.unwrap().value;
            if let Value::Int(v) = value {
                assert!(
                    v < 10,
                    "Should see original values at captured version, got {}",
                    v
                );
            }
        }
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
}

#[test]
fn versioned_read_provides_isolation() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cached");

    // Put value
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();

    // Capture version and read (verifies value exists)
    let version = CommitVersion(store.version());
    let first_read = store.get_versioned(&key, version).unwrap();
    assert_eq!(first_read.unwrap().value, Value::Int(100));

    // Delete in store
    store.delete_with_version(&key, CommitVersion(2)).unwrap();

    // Reading at captured version should still see value
    let second_read = store.get_versioned(&key, version).unwrap();
    assert!(
        second_read.is_some(),
        "Value at captured version should survive delete"
    );
    assert_eq!(second_read.unwrap().value, Value::Int(100));
}

// ============================================================================
// Snapshot Scan Operations
// ============================================================================

#[test]
fn snapshot_scan_sees_consistent_state() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Put values with common prefix
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

    let version = CommitVersion(store.version());

    // Modify after version capture
    for i in 0..10 {
        let key = Key::new_kv(ns.clone(), format!("prefix_{}", i));
        store
            .put_with_version_mode(
                key,
                Value::Int(i + 100),
                CommitVersion((i + 11) as u64),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }

    // Scan at captured version should see original values
    let prefix = Key::new_kv(ns.clone(), "prefix_");
    let results = Storage::scan_prefix(&*store, &prefix, version).unwrap();

    // All values should be < 10 (original)
    for (_key, versioned) in results {
        if let Value::Int(v) = versioned.value {
            assert!(v < 10, "Scan should see original values, got {}", v);
        }
    }
}

#[test]
fn snapshot_list_sees_all_keys() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Put 5 values
    for i in 0..5 {
        let key = Key::new_kv(ns.clone(), format!("list_key_{}", i));
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

    let version = CommitVersion(store.version());

    // Delete some in store
    for i in 0..3 {
        let key = Key::new_kv(ns.clone(), format!("list_key_{}", i));
        store
            .delete_with_version(&key, CommitVersion((i + 6) as u64))
            .unwrap();
    }

    // Scan at captured version should still see all 5
    let prefix = Key::new_kv(ns.clone(), "list_key_");
    let results = Storage::scan_prefix(&*store, &prefix, version).unwrap();
    assert_eq!(
        results.len(),
        5,
        "Should see all 5 keys at captured version"
    );
}
