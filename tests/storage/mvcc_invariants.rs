//! MVCC Invariant Tests
//!
//! Tests the core Multi-Version Concurrency Control guarantees:
//! - Version chains store newest-first
//! - get_at_version returns value <= version
//! - TTL expiration semantics
//! - Tombstone semantics

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use strata_core::traits::{Storage, WriteMode};
use strata_core::types::{Key, Namespace};
use strata_core::value::Value;
use strata_core::BranchId;
use strata_storage::sharded::ShardedStore;

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    let ns = Arc::new(Namespace::for_branch(branch_id));
    Key::new_kv(ns, name)
}

// ============================================================================
// Version Chain Semantics
// ============================================================================

#[test]
fn version_chain_stores_newest_first() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "versioned");

    // Put multiple versions
    store.put_with_version_mode(key.clone(), Value::Int(1), 1, None, WriteMode::Append).unwrap();
    store.put_with_version_mode(key.clone(), Value::Int(2), 2, None, WriteMode::Append).unwrap();
    store.put_with_version_mode(key.clone(), Value::Int(3), 3, None, WriteMode::Append).unwrap();

    // Get history - should be newest first
    let history = Storage::get_history(&store, &key, None, None).unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].version.as_u64(), 3);
    assert_eq!(history[1].version.as_u64(), 2);
    assert_eq!(history[2].version.as_u64(), 1);
}

#[test]
fn get_at_version_returns_value_lte_version() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "versioned");

    // Put values at versions 1, 2, 3
    store.put_with_version_mode(key.clone(), Value::Int(10), 1, None, WriteMode::Append).unwrap();
    store.put_with_version_mode(key.clone(), Value::Int(20), 2, None, WriteMode::Append).unwrap();
    store.put_with_version_mode(key.clone(), Value::Int(30), 3, None, WriteMode::Append).unwrap();

    // Get at version 2 - should return value 20
    let result = Storage::get_versioned(&store, &key, 2).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Value::Int(20));

    // Get at version 2.5 (between 2 and 3) - should return value at version 2
    // Since we can't have fractional versions, test with version 2
    let result = Storage::get_versioned(&store, &key, 2).unwrap();
    assert_eq!(result.unwrap().value, Value::Int(20));
}

#[test]
fn get_at_version_before_first_returns_none() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "versioned");

    // Put value at version 5
    store.put_with_version_mode(key.clone(), Value::Int(50), 5, None, WriteMode::Append).unwrap();

    // Get at version 1 (before first) - should return None
    let result = Storage::get_versioned(&store, &key, 1).unwrap();
    assert!(
        result.is_none(),
        "Should not find value before first version"
    );
}

#[test]
fn version_chain_preserves_all_versions() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "preserved");

    // Put 10 versions
    for i in 1..=10 {
        store.put_with_version_mode(key.clone(), Value::Int(i), i as u64, None, WriteMode::Append).unwrap();
    }

    // All 10 should be in history
    let history = Storage::get_history(&store, &key, None, None).unwrap();
    assert_eq!(history.len(), 10, "All versions should be preserved");

    // Verify each version is accessible
    for i in 1..=10 {
        let result = Storage::get_versioned(&store, &key, i as u64).unwrap();
        assert!(result.is_some(), "Version {} should exist", i);
        assert_eq!(result.unwrap().value, Value::Int(i));
    }
}

// ============================================================================
// TTL Semantics
// ============================================================================

#[test]
fn expired_values_filtered_at_read_time() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "ttl_test");

    // Put value with very short TTL
    let ttl = Some(Duration::from_millis(1));
    store.put_with_version_mode(key.clone(), Value::Int(42), 1, ttl, WriteMode::Append).unwrap();

    // Wait for expiration
    thread::sleep(Duration::from_millis(10));

    // Should not be returned (expired)
    let result = store.get_versioned(&key, u64::MAX).unwrap();
    assert!(result.is_none(), "Expired value should not be returned");
}

#[test]
fn non_expired_values_returned() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "ttl_valid");

    // Put value with long TTL
    let ttl = Some(Duration::from_secs(3600)); // 1 hour
    store.put_with_version_mode(key.clone(), Value::Int(42), 1, ttl, WriteMode::Append).unwrap();

    // Should be returned (not expired)
    let result = store.get_versioned(&key, u64::MAX).unwrap();
    assert!(result.is_some(), "Non-expired value should be returned");
    assert_eq!(result.unwrap().value, Value::Int(42));
}

#[test]
fn no_ttl_never_expires() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "no_ttl");

    // Put value without TTL
    store.put_with_version_mode(key.clone(), Value::Int(42), 1, None, WriteMode::Append).unwrap();

    // Should always be returned
    let result = store.get_versioned(&key, u64::MAX).unwrap();
    assert_eq!(
        result.unwrap().value,
        Value::Int(42),
        "Value without TTL should be returned"
    );
}

// ============================================================================
// Tombstone Semantics
// ============================================================================

#[test]
fn tombstone_preserves_snapshot_isolation() {
    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "tombstone_iso");

    // Put a value
    store.put_with_version_mode(key.clone(), Value::Int(100), 1, None, WriteMode::Append).unwrap();

    // Capture version BEFORE delete
    let version = store.version();

    // Delete the key (creates tombstone)
    store.delete_with_version(&key, 2).unwrap();

    // Reading at captured version should still see the value
    let result = store.get_versioned(&key, version).unwrap();
    assert!(
        result.is_some(),
        "Should see value before tombstone at captured version"
    );
    assert_eq!(result.unwrap().value, Value::Int(100));

    // Current store should not see the value
    let current = store.get_versioned(&key, u64::MAX).unwrap();
    assert!(current.is_none(), "Current should not see deleted value");
}

#[test]
fn tombstone_not_returned_to_user() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "tombstone_hidden");

    // Put and delete
    store.put_with_version_mode(key.clone(), Value::Int(42), 1, None, WriteMode::Append).unwrap();
    store.delete_with_version(&key, 2).unwrap();

    // Get should return None, not the tombstone
    let result = store.get_versioned(&key, u64::MAX).unwrap();
    assert!(result.is_none(), "Tombstone should not be returned");
}

#[test]
fn delete_nonexistent_key_succeeds() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "never_existed");

    // Delete should succeed even for nonexistent key
    let result = store.delete_with_version(&key, 1);
    assert!(result.is_ok(), "Delete of nonexistent key should succeed");
}

// ============================================================================
// Version Counter
// ============================================================================

#[test]
fn version_counter_monotonically_increases() {
    let store = ShardedStore::new();

    let v1 = store.next_version();
    let v2 = store.next_version();
    let v3 = store.next_version();

    assert!(v2 > v1, "Versions should increase");
    assert!(v3 > v2, "Versions should increase");
}

#[test]
fn version_counter_wraps_at_u64_max() {
    let store = ShardedStore::new();

    // Set version close to MAX
    store.set_version(u64::MAX - 2);

    let v1 = store.next_version();
    assert_eq!(v1, u64::MAX - 1);

    let v2 = store.next_version();
    assert_eq!(v2, u64::MAX);

    // Should wrap to 0
    let v3 = store.next_version();
    assert_eq!(v3, 0, "Should wrap at u64::MAX");

    let v4 = store.next_version();
    assert_eq!(v4, 1);
}

#[test]
fn concurrent_increments_are_unique() {
    let store = Arc::new(ShardedStore::new());
    let num_threads = 8;
    let increments_per_thread = 1000;

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let mut versions = Vec::with_capacity(increments_per_thread);
                for _ in 0..increments_per_thread {
                    versions.push(store.next_version());
                }
                versions
            })
        })
        .collect();

    let mut all_versions: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    all_versions.sort();

    // Check for duplicates
    for i in 1..all_versions.len() {
        assert_ne!(
            all_versions[i],
            all_versions[i - 1],
            "Duplicate version found: {}",
            all_versions[i]
        );
    }
}

// ============================================================================
// History Pagination
// ============================================================================

#[test]
fn history_pagination_works() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "paginated");

    // Put 5 versions
    for i in 1..=5 {
        store.put_with_version_mode(key.clone(), Value::Int(i), i as u64, None, WriteMode::Append).unwrap();
    }

    // Page 1: first 2
    let page1 = Storage::get_history(&store, &key, Some(2), None).unwrap();
    assert_eq!(page1.len(), 2);
    assert_eq!(page1[0].version.as_u64(), 5);
    assert_eq!(page1[1].version.as_u64(), 4);

    // Page 2: next 2 (before version 4)
    let page2 = Storage::get_history(&store, &key, Some(2), Some(4)).unwrap();
    assert_eq!(page2.len(), 2);
    assert_eq!(page2[0].version.as_u64(), 3);
    assert_eq!(page2[1].version.as_u64(), 2);

    // Page 3: remaining
    let page3 = Storage::get_history(&store, &key, Some(2), Some(2)).unwrap();
    assert_eq!(page3.len(), 1);
    assert_eq!(page3[0].version.as_u64(), 1);
}

#[test]
fn history_of_nonexistent_key_is_empty() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "nonexistent");

    let history = Storage::get_history(&store, &key, None, None).unwrap();
    assert!(history.is_empty());
}
