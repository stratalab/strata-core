use super::*;
use std::sync::Arc;

// ========================================================================
// Test Helpers
// ========================================================================

fn make_sv(version: u64) -> StoredValue {
    StoredValue::new(
        strata_core::value::Value::Int(version as i64),
        Version::txn(version),
        None,
    )
}

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    use strata_core::types::Namespace;
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));
    Key::new_kv(ns, name)
}

fn create_stored_value(value: strata_core::value::Value, version: u64) -> StoredValue {
    StoredValue::new(value, Version::txn(version), None)
}

#[test]
fn test_put_and_get() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "test_key");
    let value = create_stored_value(Value::Int(42), 1);

    // Put
    store.put(key.clone(), value, WriteMode::Append).unwrap();

    // Get via get_versioned (Storage trait)
    let retrieved = store.get_versioned(&key, u64::MAX).unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().value, Value::Int(42));
}

#[test]
fn test_get_nonexistent() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "nonexistent");

    assert!(store.get_versioned(&key, u64::MAX).unwrap().is_none());
}

#[test]
fn test_delete() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "to_delete");
    let value = create_stored_value(Value::Int(42), 1);

    store.put(key.clone(), value, WriteMode::Append).unwrap();
    assert!(store.get_versioned(&key, u64::MAX).unwrap().is_some());

    // Delete with version tombstone
    store.delete_with_version(&key, 2).unwrap();
    assert!(store.get_versioned(&key, u64::MAX).unwrap().is_none());
}

#[test]
fn test_delete_nonexistent() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "nonexistent");

    // delete_with_version on a nonexistent key just creates a tombstone
    store.delete_with_version(&key, 1).unwrap();
    assert!(store.get_versioned(&key, u64::MAX).unwrap().is_none());
}

#[test]
fn test_contains() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "exists");
    let value = create_stored_value(Value::Int(42), 1);

    assert!(!store.contains(&key));
    store.put(key.clone(), value, WriteMode::Append).unwrap();
    assert!(store.contains(&key));
}

#[test]
fn test_overwrite() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "overwrite");

    store
        .put(
            key.clone(),
            create_stored_value(Value::Int(1), 1),
            WriteMode::Append,
        )
        .unwrap();
    store
        .put(
            key.clone(),
            create_stored_value(Value::Int(2), 2),
            WriteMode::Append,
        )
        .unwrap();

    let retrieved = store.get_versioned(&key, u64::MAX).unwrap().unwrap();
    assert_eq!(retrieved.value, Value::Int(2));
    assert_eq!(retrieved.version, Version::txn(2));
}

#[test]
fn test_multiple_branches_isolated() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch1 = BranchId::new();
    let branch2 = BranchId::new();

    let key1 = create_test_key(branch1, "key");
    let key2 = create_test_key(branch2, "key");

    store
        .put(
            key1.clone(),
            create_stored_value(Value::Int(1), 1),
            WriteMode::Append,
        )
        .unwrap();
    store
        .put(
            key2.clone(),
            create_stored_value(Value::Int(2), 1),
            WriteMode::Append,
        )
        .unwrap();

    // Different branches, same key name, different values
    assert_eq!(
        store.get_versioned(&key1, u64::MAX).unwrap().unwrap().value,
        Value::Int(1)
    );
    assert_eq!(
        store.get_versioned(&key2, u64::MAX).unwrap().unwrap().value,
        Value::Int(2)
    );
    assert_eq!(store.shard_count(), 2);
}

#[test]
fn test_apply_batch() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    let key1 = create_test_key(branch_id, "batch1");
    let key2 = create_test_key(branch_id, "batch2");
    let key3 = create_test_key(branch_id, "batch3");

    // First, put key3 so we can delete it
    store
        .put(
            key3.clone(),
            create_stored_value(Value::Int(999), 1),
            WriteMode::Append,
        )
        .unwrap();

    // Apply batch
    let writes = vec![(key1.clone(), Value::Int(1)), (key2.clone(), Value::Int(2))];
    let deletes = vec![key3.clone()];

    store.apply_batch(writes, deletes, 2).unwrap();

    assert_eq!(
        store.get_versioned(&key1, u64::MAX).unwrap().unwrap().value,
        Value::Int(1)
    );
    assert_eq!(
        store
            .get_versioned(&key1, u64::MAX)
            .unwrap()
            .unwrap()
            .version,
        Version::txn(2)
    );
    assert_eq!(
        store.get_versioned(&key2, u64::MAX).unwrap().unwrap().value,
        Value::Int(2)
    );
    assert!(store.get_versioned(&key3, u64::MAX).unwrap().is_none());
}

#[test]
fn test_branch_entry_count() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    assert_eq!(store.branch_entry_count(&branch_id), 0);

    for i in 0..5 {
        let key = create_test_key(branch_id, &format!("key{}", i));
        store
            .put(
                key,
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            )
            .unwrap();
    }

    assert_eq!(store.branch_entry_count(&branch_id), 5);
    assert_eq!(store.total_entries(), 5);
}

#[test]
fn test_concurrent_writes_different_branches() {
    use std::thread;
    use strata_core::value::Value;

    let store = Arc::new(ShardedStore::new());

    // 10 threads, each with its own branch, writing 100 keys
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let branch_id = BranchId::new();
                for i in 0..100 {
                    let key = create_test_key(branch_id, &format!("key{}", i));
                    store
                        .put(
                            key,
                            create_stored_value(Value::Int(i), 1),
                            WriteMode::Append,
                        )
                        .unwrap();
                }
                branch_id
            })
        })
        .collect();

    let branch_ids: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // Verify each branch has 100 entries
    for branch_id in &branch_ids {
        assert_eq!(store.branch_entry_count(branch_id), 100);
    }

    assert_eq!(store.shard_count(), 10);
    assert_eq!(store.total_entries(), 1000);
}

// ========================================================================
// List Operations Tests
// ========================================================================

#[test]
fn test_list_branch_empty() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    let results = store.list_branch(&branch_id);
    assert!(results.is_empty());
}

#[test]
fn test_list_branch() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    // Insert some keys
    for i in 0..5 {
        let key = create_test_key(branch_id, &format!("key{}", i));
        store
            .put(
                key,
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            )
            .unwrap();
    }

    let results = store.list_branch(&branch_id);
    assert_eq!(results.len(), 5);

    // Verify sorted order
    for i in 0..results.len() - 1 {
        assert!(results[i].0 < results[i + 1].0);
    }
}

#[test]
fn test_list_by_prefix() {
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    // Insert keys with different prefixes
    store.put(
        Key::new_kv(ns.clone(), "user:alice"),
        create_stored_value(Value::Int(1), 1),
        WriteMode::Append,
    );
    store.put(
        Key::new_kv(ns.clone(), "user:bob"),
        create_stored_value(Value::Int(2), 1),
        WriteMode::Append,
    );
    store.put(
        Key::new_kv(ns.clone(), "config:timeout"),
        create_stored_value(Value::Int(3), 1),
        WriteMode::Append,
    );

    // Query with "user:" prefix
    let prefix = Key::new_kv(ns.clone(), "user:");
    let results = store.list_by_prefix(&prefix);

    assert_eq!(results.len(), 2);
    // Should be alice, bob in sorted order
    assert!(results[0].0.user_key_string().unwrap().contains("alice"));
    assert!(results[1].0.user_key_string().unwrap().contains("bob"));
}

#[test]
fn test_list_by_prefix_empty() {
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    store.put(
        Key::new_kv(ns.clone(), "data:foo"),
        create_stored_value(Value::Int(1), 1),
        WriteMode::Append,
    );

    // Query with non-matching prefix
    let prefix = Key::new_kv(ns.clone(), "user:");
    let results = store.list_by_prefix(&prefix);

    assert!(results.is_empty());
}

#[test]
fn test_list_by_type() {
    use strata_core::types::{Namespace, TypeTag};
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    // Insert KV entries
    store.put(
        Key::new_kv(ns.clone(), "kv1"),
        create_stored_value(Value::Int(1), 1),
        WriteMode::Append,
    );
    store.put(
        Key::new_kv(ns.clone(), "kv2"),
        create_stored_value(Value::Int(2), 1),
        WriteMode::Append,
    );

    // Insert Event entries
    store.put(
        Key::new_event(ns.clone(), 1),
        create_stored_value(Value::Int(10), 1),
        WriteMode::Append,
    );

    // Insert State entries
    store.put(
        Key::new_state(ns.clone(), "state1"),
        create_stored_value(Value::Int(20), 1),
        WriteMode::Append,
    );

    // Query by type
    let kv_results = store.list_by_type(&branch_id, TypeTag::KV);
    assert_eq!(kv_results.len(), 2);

    let event_results = store.list_by_type(&branch_id, TypeTag::Event);
    assert_eq!(event_results.len(), 1);

    let state_results = store.list_by_type(&branch_id, TypeTag::State);
    assert_eq!(state_results.len(), 1);
}

#[test]
fn test_count_by_type() {
    use strata_core::types::{Namespace, TypeTag};
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    // Insert mixed types
    for i in 0..5 {
        store.put(
            Key::new_kv(ns.clone(), format!("kv{}", i)),
            create_stored_value(Value::Int(i), 1),
            WriteMode::Append,
        );
    }
    for i in 0..3 {
        store.put(
            Key::new_event(ns.clone(), i as u64),
            create_stored_value(Value::Int(i), 1),
            WriteMode::Append,
        );
    }

    assert_eq!(store.count_by_type(&branch_id, TypeTag::KV), 5);
    assert_eq!(store.count_by_type(&branch_id, TypeTag::Event), 3);
    assert_eq!(store.count_by_type(&branch_id, TypeTag::State), 0);
}

#[test]
fn test_branch_ids() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch1 = BranchId::new();
    let branch2 = BranchId::new();
    let branch3 = BranchId::new();

    // Insert data for 3 branches
    store.put(
        create_test_key(branch1, "k1"),
        create_stored_value(Value::Int(1), 1),
        WriteMode::Append,
    );
    store.put(
        create_test_key(branch2, "k1"),
        create_stored_value(Value::Int(2), 1),
        WriteMode::Append,
    );
    store.put(
        create_test_key(branch3, "k1"),
        create_stored_value(Value::Int(3), 1),
        WriteMode::Append,
    );

    let branch_ids = store.branch_ids();
    assert_eq!(branch_ids.len(), 3);
    assert!(branch_ids.contains(&branch1));
    assert!(branch_ids.contains(&branch2));
    assert!(branch_ids.contains(&branch3));
}

#[test]
fn test_clear_branch() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    // Insert some data
    for i in 0..5 {
        let key = create_test_key(branch_id, &format!("key{}", i));
        store
            .put(
                key,
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            )
            .unwrap();
    }

    assert_eq!(store.branch_entry_count(&branch_id), 5);
    assert!(store.has_branch(&branch_id));

    // Clear the branch
    assert!(store.clear_branch(&branch_id));

    assert_eq!(store.branch_entry_count(&branch_id), 0);
    assert!(!store.has_branch(&branch_id));
}

#[test]
fn test_clear_branch_nonexistent() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    // Clear non-existent branch returns false
    assert!(!store.clear_branch(&branch_id));
}

#[test]
fn test_list_sorted_order() {
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    // Insert in random order
    let keys = vec!["zebra", "apple", "mango", "banana"];
    for k in &keys {
        store.put(
            Key::new_kv(ns.clone(), *k),
            create_stored_value(Value::String(k.to_string()), 1),
            WriteMode::Append,
        );
    }

    let results = store.list_branch(&branch_id);
    let result_keys: Vec<_> = results
        .iter()
        .map(|(k, _)| k.user_key_string().unwrap())
        .collect();

    // Should be sorted: apple, banana, mango, zebra
    assert_eq!(result_keys, vec!["apple", "banana", "mango", "zebra"]);
}

// ========================================================================
// Version-Based Read Tests (replaces Snapshot Acquisition Tests)
// ========================================================================

#[test]
fn test_version_based_read_operations() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    // Put some data (version=1)
    let key = create_test_key(branch_id, "test_key");
    store
        .put(
            key.clone(),
            create_stored_value(Value::Int(42), 1),
            WriteMode::Append,
        )
        .unwrap();
    // Update store version so reads can see data at version 1
    store.set_version(1);

    let v = store.version();

    // Read through get_versioned with captured version
    let value = store.get_versioned(&key, v).unwrap();
    assert!(value.is_some());
    assert_eq!(value.unwrap().value, Value::Int(42));

    // contains works
    assert!(store.get_versioned(&key, v).unwrap().is_some());
}

#[test]
fn test_version_capture_for_mvcc() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    // Capture version before writes
    let v0 = store.version();
    assert_eq!(v0, 0);

    // Add data and increment version
    let key1 = create_test_key(branch_id, "key1");
    store
        .put(
            key1.clone(),
            create_stored_value(Value::Int(1), 1),
            WriteMode::Append,
        )
        .unwrap();
    store.next_version();

    let v1 = store.version();
    assert_eq!(v1, 1);

    // Add more data
    let key2 = create_test_key(branch_id, "key2");
    store
        .put(
            key2.clone(),
            create_stored_value(Value::Int(2), 2),
            WriteMode::Append,
        )
        .unwrap();
    store.next_version();

    let v2 = store.version();
    assert_eq!(v2, 2);

    // Captured versions enable MVCC reads at different points in time
    // v0: nothing visible
    assert!(store.get_versioned(&key1, v0).unwrap().is_none());
    assert!(store.get_versioned(&key2, v0).unwrap().is_none());

    // v1: key1 visible, key2 not yet
    assert!(store.get_versioned(&key1, v1).unwrap().is_some());
    assert!(store.get_versioned(&key2, v1).unwrap().is_none());

    // v2: both visible
    assert!(store.get_versioned(&key1, v2).unwrap().is_some());
    assert!(store.get_versioned(&key2, v2).unwrap().is_some());
}

#[test]
fn test_version_thread_safety_with_reads() {
    use std::thread;

    let store = Arc::new(ShardedStore::new());

    // Spawn threads that capture versions and increment concurrently
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                // Capture version
                let v1 = store.version();

                // Increment version
                store.next_version();

                // Capture again
                let v2 = store.version();

                // Second version should be higher or equal
                assert!(v2 >= v1);

                (v1, v2)
            })
        })
        .collect();

    for h in handles {
        let (v1, v2) = h.join().unwrap();
        assert!(v2 >= v1);
    }

    // Final version should be 10 (each thread incremented once)
    assert_eq!(store.version(), 10);
}

// ========================================================================
// Storage Trait Implementation Tests
// ========================================================================

#[test]
fn test_storage_trait_get_put() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns, "test_key");

    // Put via Storage trait
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(42),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Get via Storage trait
    let result = Storage::get_versioned(&store, &key, u64::MAX).unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Value::Int(42));
}

#[test]
fn test_storage_trait_get_versioned() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns, "test_key");

    // Put with version 1
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(42),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Get with max_version 1 - should return value
    let result = Storage::get_versioned(&store, &key, 1).unwrap();
    assert!(result.is_some());

    // Get with max_version 0 - should return None (version 1 > 0)
    let result = Storage::get_versioned(&store, &key, 0).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_storage_trait_delete() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns, "test_key");

    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(42),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    assert!(Storage::get_versioned(&store, &key, u64::MAX)
        .unwrap()
        .is_some());

    // Delete via Storage trait
    Storage::delete_with_version(&store, &key, 2).unwrap();
    assert!(Storage::get_versioned(&store, &key, u64::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn test_storage_trait_scan_prefix() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Insert keys with different prefixes
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "user:alice"),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "user:bob"),
        Value::Int(2),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "config:timeout"),
        Value::Int(3),
        3,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Scan with "user:" prefix
    let prefix = Key::new_kv(ns.clone(), "user:");
    let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_storage_trait_put_with_version() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns, "test_key");

    // Put with specific version 42
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(100),
        42,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Verify version is 42
    let result = Storage::get_versioned(&store, &key, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.version, Version::txn(42));

    // current_version should be updated
    assert!(Storage::current_version(&store) >= 42);
}

#[test]
fn test_storage_trait_delete_with_version() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns, "versioned_delete");

    // Put a value
    Storage::put_with_version_mode(
        &*store,
        key.clone(),
        Value::Int(42),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    assert!(Storage::get_versioned(&*store, &key, u64::MAX)
        .unwrap()
        .is_some());

    // Delete with specific version
    Storage::delete_with_version(&*store, &key, 100).unwrap();

    // Key should no longer be visible
    assert!(Storage::get_versioned(&*store, &key, u64::MAX)
        .unwrap()
        .is_none());

    // Global version should be updated
    assert!(Storage::current_version(&*store) >= 100);

    // Version 1 (before delete) should still be visible via MVCC
    let snap_at_1 = store.shards.get(&branch_id).and_then(|shard| {
        shard.data.get(&key).and_then(|chain| {
            chain.get_at_version(1).and_then(|sv| {
                if !sv.is_tombstone() {
                    Some(sv.versioned())
                } else {
                    None
                }
            })
        })
    });
    assert!(
        snap_at_1.is_some(),
        "Version 1 should still be visible via MVCC"
    );
}

#[test]
fn test_storage_trait_get_version_only_existing_key() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns, "version_check");

    // Put a value — version 1
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::String("hello".to_string()),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // get_version_only should return the version
    let version = Storage::get_version_only(&store, &key).unwrap();
    assert_eq!(version, Some(1));

    // Should match what get() returns
    let full = Storage::get_versioned(&store, &key, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(version.unwrap(), full.version.as_u64());
}

#[test]
fn test_storage_trait_get_version_only_nonexistent() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns, "missing");

    assert_eq!(Storage::get_version_only(&store, &key).unwrap(), None);
}

#[test]
fn test_storage_trait_get_version_only_tombstoned() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns, "to_delete");

    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::delete_with_version(&store, &key, 2).unwrap();

    // get_version_only should return None for tombstoned key (same as get())
    assert_eq!(Storage::get_version_only(&store, &key).unwrap(), None);
    assert!(Storage::get_versioned(&store, &key, u64::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn test_storage_trait_get_version_only_expired() {
    use std::time::Duration;
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns, "expiring");

    // Put with an already-expired TTL (use put_with_version + manual StoredValue)
    let sv = StoredValue::with_timestamp(
        Value::Int(1),
        Version::txn(1),
        strata_core::Timestamp::from_micros(0), // epoch = definitely expired
        Some(Duration::from_secs(1)),
    );
    ShardedStore::put(&store, key.clone(), sv, WriteMode::Append).unwrap();

    // get_version_only should return None for expired key (same as get())
    assert_eq!(Storage::get_version_only(&store, &key).unwrap(), None);
    assert!(Storage::get_versioned(&store, &key, u64::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn test_storage_trait_get_version_only_matches_get() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Write multiple keys with different versions
    for i in 0..5 {
        let key = Key::new_kv(ns.clone(), &format!("key{}", i));
        Storage::put_with_version_mode(
            &store,
            key,
            Value::Int(i),
            (i + 1) as u64,
            None,
            WriteMode::Append,
        )
        .unwrap();
    }

    // Verify get_version_only matches get_versioned() for all keys
    for i in 0..5 {
        let key = Key::new_kv(ns.clone(), &format!("key{}", i));
        let version_only = Storage::get_version_only(&store, &key).unwrap();
        let full_get = Storage::get_versioned(&store, &key, u64::MAX).unwrap();
        assert_eq!(
            version_only,
            full_get.map(|vv| vv.version.as_u64()),
            "get_version_only must match get_versioned() for key{}",
            i
        );
    }

    // Also check a nonexistent key
    let missing = Key::new_kv(ns, "nonexistent");
    assert_eq!(
        Storage::get_version_only(&store, &missing).unwrap(),
        Storage::get_versioned(&store, &missing, u64::MAX)
            .unwrap()
            .map(|vv| vv.version.as_u64())
    );
}

#[test]
fn test_versioned_read_mvcc_filtering() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Put at version 1
    let key1 = Key::new_kv(ns.clone(), "key1");
    Storage::put_with_version_mode(
        &store,
        key1.clone(),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Capture version at 1
    let v = store.version();
    assert_eq!(v, 1);

    // Put at version 2
    let key2 = Key::new_kv(ns.clone(), "key2");
    Storage::put_with_version_mode(
        &store,
        key2.clone(),
        Value::Int(2),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Reading at captured version 1 should only see key1 via MVCC filtering
    let read_key1 = store.get_versioned(&key1, v).unwrap();
    assert!(read_key1.is_some());

    // key2 has version 2, but we're reading at version 1
    let read_key2 = store.get_versioned(&key2, v).unwrap();
    assert!(
        read_key2.is_none(),
        "key2 should not be visible at version 1"
    );
}

#[test]
fn test_versioned_scan_prefix_mvcc() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Put two keys at version 1 and 2
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "user:alice"),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "user:bob"),
        Value::Int(2),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Capture version before third write
    let v = store.version();

    // Put another key at version 3
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "user:charlie"),
        Value::Int(3),
        3,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Scan prefix at captured version - should only see 2 keys
    let prefix = Key::new_kv(ns.clone(), "user:");
    let results = Storage::scan_prefix(&store, &prefix, v).unwrap();

    assert_eq!(results.len(), 2, "Should only see keys at version <= 2");
}

// ========================================================================
// VersionChain::history() Tests
// ========================================================================

#[test]
fn test_version_chain_history_all_versions() {
    use strata_core::value::Value;

    let mut chain = VersionChain::new(create_stored_value(Value::Int(1), 1));
    chain.push(create_stored_value(Value::Int(2), 2));
    chain.push(create_stored_value(Value::Int(3), 3));

    // Get all versions (newest first)
    let history = chain.history(None, None);
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].version().as_u64(), 3);
    assert_eq!(history[1].version().as_u64(), 2);
    assert_eq!(history[2].version().as_u64(), 1);
}

#[test]
fn test_version_chain_history_with_limit() {
    use strata_core::value::Value;

    let mut chain = VersionChain::new(create_stored_value(Value::Int(1), 1));
    chain.push(create_stored_value(Value::Int(2), 2));
    chain.push(create_stored_value(Value::Int(3), 3));
    chain.push(create_stored_value(Value::Int(4), 4));
    chain.push(create_stored_value(Value::Int(5), 5));

    // Get only 2 versions
    let history = chain.history(Some(2), None);
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].version().as_u64(), 5); // Newest
    assert_eq!(history[1].version().as_u64(), 4);
}

#[test]
fn test_version_chain_history_with_before() {
    use strata_core::value::Value;

    let mut chain = VersionChain::new(create_stored_value(Value::Int(1), 1));
    chain.push(create_stored_value(Value::Int(2), 2));
    chain.push(create_stored_value(Value::Int(3), 3));
    chain.push(create_stored_value(Value::Int(4), 4));
    chain.push(create_stored_value(Value::Int(5), 5));

    // Get versions before version 4 (should get 1, 2, 3)
    let history = chain.history(None, Some(4));
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].version().as_u64(), 3);
    assert_eq!(history[1].version().as_u64(), 2);
    assert_eq!(history[2].version().as_u64(), 1);
}

#[test]
fn test_version_chain_history_with_limit_and_before() {
    use strata_core::value::Value;

    let mut chain = VersionChain::new(create_stored_value(Value::Int(1), 1));
    chain.push(create_stored_value(Value::Int(2), 2));
    chain.push(create_stored_value(Value::Int(3), 3));
    chain.push(create_stored_value(Value::Int(4), 4));
    chain.push(create_stored_value(Value::Int(5), 5));

    // Get 2 versions before version 5
    let history = chain.history(Some(2), Some(5));
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].version().as_u64(), 4);
    assert_eq!(history[1].version().as_u64(), 3);
}

#[test]
fn test_version_chain_history_before_first() {
    use strata_core::value::Value;

    let chain = VersionChain::new(create_stored_value(Value::Int(1), 5));

    // Before version 5 returns empty (only version is 5)
    let history = chain.history(None, Some(5));
    assert!(history.is_empty());

    // Before version 6 returns the one version
    let history = chain.history(None, Some(6));
    assert_eq!(history.len(), 1);
}

#[test]
fn test_version_chain_is_empty() {
    use strata_core::value::Value;

    let chain = VersionChain::new(create_stored_value(Value::Int(1), 1));
    assert!(!chain.is_empty());
    assert_eq!(chain.version_count(), 1);
}

// ========================================================================
// VersionChain::get_at_version() Tests (MVCC)
// ========================================================================

#[test]
fn test_version_chain_get_at_version_single() {
    use strata_core::value::Value;

    let chain = VersionChain::new(create_stored_value(Value::Int(42), 5));

    // Exact version match
    let result = chain.get_at_version(5);
    assert!(result.is_some());
    assert_eq!(result.unwrap().version().as_u64(), 5);

    // Higher version should still return the value (latest <= max_version)
    let result = chain.get_at_version(10);
    assert!(result.is_some());
    assert_eq!(result.unwrap().version().as_u64(), 5);

    // Lower version should return None
    let result = chain.get_at_version(4);
    assert!(result.is_none());
}

#[test]
fn test_version_chain_get_at_version_multiple() {
    use strata_core::value::Value;

    // Create chain with versions 1, 2, 3 (newest first after pushes)
    let mut chain = VersionChain::new(create_stored_value(Value::Int(100), 1));
    chain.push(create_stored_value(Value::Int(200), 2));
    chain.push(create_stored_value(Value::Int(300), 3));

    // Chain should have 3 versions
    assert_eq!(chain.version_count(), 3);

    // Query at version 3 should return version 3
    let result = chain.get_at_version(3);
    assert!(result.is_some());
    assert_eq!(result.unwrap().version().as_u64(), 3);
    assert_eq!(result.unwrap().versioned().value, Value::Int(300));

    // Query at version 2 should return version 2
    let result = chain.get_at_version(2);
    assert!(result.is_some());
    assert_eq!(result.unwrap().version().as_u64(), 2);
    assert_eq!(result.unwrap().versioned().value, Value::Int(200));

    // Query at version 1 should return version 1
    let result = chain.get_at_version(1);
    assert!(result.is_some());
    assert_eq!(result.unwrap().version().as_u64(), 1);
    assert_eq!(result.unwrap().versioned().value, Value::Int(100));

    // Query at version 0 should return None
    let result = chain.get_at_version(0);
    assert!(result.is_none());

    // Query at version 100 should return latest (version 3)
    let result = chain.get_at_version(100);
    assert!(result.is_some());
    assert_eq!(result.unwrap().version().as_u64(), 3);
}

#[test]
fn test_version_chain_get_at_version_between_versions() {
    use strata_core::value::Value;

    // Create chain with versions 10, 20, 30 (sparse)
    let mut chain = VersionChain::new(create_stored_value(Value::Int(1), 10));
    chain.push(create_stored_value(Value::Int(2), 20));
    chain.push(create_stored_value(Value::Int(3), 30));

    // Query at version 25 should return version 20 (latest <= 25)
    let result = chain.get_at_version(25);
    assert!(result.is_some());
    assert_eq!(result.unwrap().version().as_u64(), 20);

    // Query at version 15 should return version 10
    let result = chain.get_at_version(15);
    assert!(result.is_some());
    assert_eq!(result.unwrap().version().as_u64(), 10);

    // Query at version 5 should return None (no version <= 5)
    let result = chain.get_at_version(5);
    assert!(result.is_none());
}

#[test]
fn test_version_chain_get_at_version_snapshot_isolation() {
    use strata_core::value::Value;

    // Simulates snapshot isolation: reader sees consistent view
    let mut chain = VersionChain::new(create_stored_value(Value::String("v1".into()), 1));

    // Snapshot taken at version 1
    let snapshot_version = 1;

    // Writer adds new versions
    chain.push(create_stored_value(Value::String("v2".into()), 2));
    chain.push(create_stored_value(Value::String("v3".into()), 3));

    // Snapshot reader should still see version 1
    let result = chain.get_at_version(snapshot_version);
    assert!(result.is_some());
    assert_eq!(result.unwrap().version().as_u64(), 1);
    assert_eq!(
        result.unwrap().versioned().value,
        Value::String("v1".into())
    );

    // Current reader sees version 3
    let result = chain.get_at_version(u64::MAX);
    assert!(result.is_some());
    assert_eq!(result.unwrap().version().as_u64(), 3);
}

// ========================================================================
// Storage::get_history() Tests
// ========================================================================

#[test]
fn test_storage_get_history() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns.clone(), "test-key");

    // Put multiple versions of the same key
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(2),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(3),
        3,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Get full history
    let history = Storage::get_history(&store, &key, None, None).unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].version.as_u64(), 3);
    assert_eq!(history[1].version.as_u64(), 2);
    assert_eq!(history[2].version.as_u64(), 1);
}

#[test]
fn test_storage_get_history_pagination() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns.clone(), "paginated-key");

    // Put 5 versions
    for i in 1..=5 {
        Storage::put_with_version_mode(
            &store,
            key.clone(),
            Value::Int(i),
            i as u64,
            None,
            WriteMode::Append,
        )
        .unwrap();
    }

    // Page 1: Get first 2
    let page1 = Storage::get_history(&store, &key, Some(2), None).unwrap();
    assert_eq!(page1.len(), 2);
    assert_eq!(page1[0].version.as_u64(), 5);
    assert_eq!(page1[1].version.as_u64(), 4);

    // Page 2: Get next 2 (before version 4)
    let page2 = Storage::get_history(&store, &key, Some(2), Some(4)).unwrap();
    assert_eq!(page2.len(), 2);
    assert_eq!(page2[0].version.as_u64(), 3);
    assert_eq!(page2[1].version.as_u64(), 2);

    // Page 3: Get remaining
    let page3 = Storage::get_history(&store, &key, Some(2), Some(2)).unwrap();
    assert_eq!(page3.len(), 1);
    assert_eq!(page3[0].version.as_u64(), 1);
}

#[test]
fn test_storage_get_history_nonexistent_key() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));
    let key = Key::new_kv(ns.clone(), "nonexistent");

    let history = Storage::get_history(&store, &key, None, None).unwrap();
    assert!(history.is_empty());
}

// ========================================================================
// ADVERSARIAL TESTS - Bug Hunting
// These tests probe edge cases and potential bugs.
// ========================================================================

/// MVCC delete with tombstones preserves version-based isolation
///
/// Delete now creates a tombstone at a new version instead of removing all versions.
/// This ensures reads at an earlier version can still see the value.
#[test]
fn test_delete_uses_tombstone_preserving_mvcc() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "mvcc_uncached");

    // Put a value at version 1
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(100),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Capture version at 1
    let v = store.version();
    assert_eq!(v, 1);

    // Delete the key — creates a tombstone at version 2
    Storage::delete_with_version(&store, &key, 2).unwrap();

    // Read at captured version 1 — should see value before the tombstone
    let result = store.get_versioned(&key, v).unwrap();

    // FIX VERIFIED: Reading at version 1 sees the value because
    // delete() creates a tombstone at version 2, leaving version 1 intact.
    assert!(
        result.is_some(),
        "Read at version 1 should see value - tombstone is at version 2"
    );
    assert_eq!(
        result.unwrap().value,
        Value::Int(100),
        "Should see the original value"
    );
}

/// Verify version-based MVCC provides isolation for reads
///
/// Reading at a captured version before a delete still sees the value,
/// because the tombstone has a higher version number.
#[test]
fn test_versioned_read_provides_isolation_across_deletes() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cached_test");

    // Put a value at version 1
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(100),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Capture version and read the key
    let v = store.version();
    let before = store.get_versioned(&key, v).unwrap();
    assert!(before.is_some());
    assert_eq!(before.unwrap().value, Value::Int(100));

    // Delete the key
    Storage::delete_with_version(&store, &key, 2).unwrap();

    // Reading at captured version should still see the value
    let after = store.get_versioned(&key, v).unwrap();
    assert!(
        after.is_some(),
        "Value at version 1 should survive delete at version 2"
    );
    assert_eq!(after.unwrap().value, Value::Int(100));
}

/// Version counter wraps at u64::MAX boundary (fixed)
///
/// The version counter now uses wrapping arithmetic to prevent panic.
/// At ~584 years of continuous operation at 1 billion versions/second,
/// overflow is extremely unlikely but now handled gracefully.
#[test]
fn test_version_counter_wraps_at_max_boundary() {
    let store = ShardedStore::new();

    // Set version close to MAX
    store.set_version(u64::MAX - 2);
    assert_eq!(store.version(), u64::MAX - 2);

    // Allocate versions up to MAX
    let v1 = store.next_version();
    assert_eq!(v1, u64::MAX - 1);

    let v2 = store.next_version();
    assert_eq!(v2, u64::MAX);

    // Cross the boundary - should wrap to 0 without panic
    let v3 = store.next_version();
    assert_eq!(v3, 0, "Version should wrap to 0 at u64::MAX");

    // Continue allocating
    let v4 = store.next_version();
    assert_eq!(v4, 1);

    // Note: After wrapping, versions are no longer monotonically increasing.
    // This is documented behavior for the extremely unlikely overflow case.
}

/// SAFETY: TTL expiration with clock going backward
///
/// If system clock goes backward after storing a value,
/// `duration_since()` returns `None` and `is_expired()` returns `false`.
/// This is correct: the value persists rather than expiring prematurely.
/// Clock-backward means we cannot reliably measure elapsed time, so the
/// safe default is to treat the value as unexpired. Items will expire
/// normally once the clock catches up past `timestamp + ttl`.
#[test]
fn test_ttl_expiration_with_old_timestamp() {
    use strata_core::value::Value;

    // Create a value with a timestamp in the "future" relative to epoch
    let future_ts = Timestamp::from_micros(u64::MAX / 2);
    let stored = StoredValue::with_timestamp(
        Value::Int(42),
        Version::txn(1),
        future_ts,
        Some(std::time::Duration::from_secs(1)), // 1 second TTL
    );

    // If current system time is before the timestamp,
    // duration_since returns None and is_expired returns false
    // This means the value appears NOT expired even though TTL passed
    let is_expired = stored.is_expired();

    // This test documents the edge case:
    // With a future timestamp, the value never expires
    // This is INTENDED behavior (safe default), but worth documenting
    assert!(
        !is_expired,
        "Value with future timestamp doesn't expire - clock backward protection"
    );
}

/// SAFETY: Concurrent versioned reads during store modifications
///
/// `get_versioned()` acquires a DashMap read lock and walks the version
/// chain for entries <= the specified version. New versions are only
/// prepended (push_front) and old versions are never mutated in place,
/// so concurrent writes cannot corrupt a versioned read. DashMap's
/// per-shard RwLock ensures structural safety.
#[test]
fn test_versioned_read_isolation_under_concurrent_writes() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();

    // Put initial values
    for i in 0..10 {
        let key = create_test_key(branch_id, &format!("key{}", i));
        Storage::put_with_version_mode(
            &*store,
            key,
            Value::Int(i),
            (i + 1) as u64,
            None,
            WriteMode::Append,
        )
        .unwrap();
    }

    // Capture version for MVCC reads
    let read_version = store.version();

    // Flag to stop writers
    let stop = Arc::new(AtomicBool::new(false));

    // Spawn writers that continuously modify the store
    let writer_handles: Vec<_> = (0..4)
        .map(|t| {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                let mut counter = 0;
                while !stop.load(Ordering::Relaxed) {
                    for i in 0..10 {
                        let key = create_test_key(branch_id, &format!("key{}", i));
                        let _ = Storage::put_with_version_mode(
                            &*store,
                            key,
                            Value::Int(t * 1000 + counter),
                            11 + counter as u64,
                            None,
                            WriteMode::Append,
                        );
                    }
                    counter += 1;
                    if counter > 100 {
                        break;
                    }
                }
            })
        })
        .collect();

    // Read at captured version while writes are happening
    for _ in 0..100 {
        for i in 0..10 {
            let key = create_test_key(branch_id, &format!("key{}", i));
            let result = store.get_versioned(&key, read_version).unwrap();

            // Should see SOME value (the one at or before captured version)
            assert!(
                result.is_some(),
                "Lost visibility to key{} during concurrent writes",
                i
            );

            // The version should be <= captured version
            let version = result.as_ref().unwrap().version.as_u64();
            assert!(
                version <= read_version,
                "Saw version {} but reading at {} - MVCC violation",
                version,
                read_version
            );
        }
    }

    // Stop writers
    stop.store(true, Ordering::Relaxed);
    for h in writer_handles {
        let _ = h.join();
    }
}

/// SAFETY: Concurrent get_versioned access from multiple threads
///
/// `get_versioned()` only acquires DashMap read locks. Multiple threads
/// calling it concurrently each acquire independent read locks, so there
/// is no contention or race condition.
#[test]
fn test_concurrent_versioned_reads() {
    use std::sync::Barrier;
    use std::thread;
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "cached_key");

    // Put a value
    Storage::put_with_version_mode(
        &*store,
        key.clone(),
        Value::Int(42),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Capture version for reads
    let read_version = store.version();

    // Use barrier to ensure all threads hit get_versioned() simultaneously
    let num_threads = 10;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let store = Arc::clone(&store);
            let key = key.clone();
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();
                // All threads try to get_versioned() at the same time
                let result = store.get_versioned(&key, read_version).unwrap();
                result.map(|v| v.value.clone())
            })
        })
        .collect();

    // Collect results
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All threads should see the same value
    for result in &results {
        assert_eq!(
            *result,
            Some(Value::Int(42)),
            "get_versioned returned inconsistent value under concurrent access"
        );
    }
}

/// SAFETY: Version chain GC during concurrent reads
///
/// `gc()` requires `&mut VersionChain`, which means the caller must hold
/// a DashMap write lock on the shard (via `entry()` or `get_mut()`).
/// `get_at_version()` takes `&self` and is reached through a DashMap read
/// lock (via `get()`). DashMap's per-shard RwLock guarantees mutual
/// exclusion between readers and writers, so GC cannot run while any
/// read is in progress on the same shard, and vice versa.
#[test]
fn test_version_chain_gc_concurrent_reads() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = Arc::new(ShardedStore::new());
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "gc_test");

    // Build up a version chain with many versions
    for i in 1..=100 {
        Storage::put_with_version_mode(
            &*store,
            key.clone(),
            Value::Int(i),
            i as u64,
            None,
            WriteMode::Append,
        )
        .unwrap();
    }

    // Verify all versions exist
    let history = Storage::get_history(&*store, &key, None, None).unwrap();
    assert_eq!(history.len(), 100);

    // Run concurrent reads and simulated GC pressure
    let stop = Arc::new(AtomicBool::new(false));

    // Reader threads
    let reader_handles: Vec<_> = (0..4)
        .map(|_| {
            let store = Arc::clone(&store);
            let key = key.clone();
            let stop = Arc::clone(&stop);

            thread::spawn(move || {
                let mut reads = 0;
                while !stop.load(Ordering::Relaxed) && reads < 1000 {
                    // Read at various versions
                    for v in [1, 25, 50, 75, 100] {
                        let result = Storage::get_versioned(&*store, &key, v).unwrap();
                        if let Some(vv) = result {
                            assert!(
                                vv.version.as_u64() <= v,
                                "Read version {} but requested max {}",
                                vv.version.as_u64(),
                                v
                            );
                        }
                    }
                    reads += 1;
                }
                reads
            })
        })
        .collect();

    // Writer thread that causes potential GC
    let writer_handle = {
        let store = Arc::clone(&store);
        let key = key.clone();
        let stop = Arc::clone(&stop);

        thread::spawn(move || {
            let mut version = 101;
            while !stop.load(Ordering::Relaxed) && version < 200 {
                Storage::put_with_version_mode(
                    &*store,
                    key.clone(),
                    Value::Int(version),
                    version as u64,
                    None,
                    WriteMode::Append,
                )
                .unwrap();
                version += 1;
            }
            version
        })
    };

    // Let it run for a bit
    thread::sleep(std::time::Duration::from_millis(100));
    stop.store(true, Ordering::Relaxed);

    // Collect results - no panics means no data races detected
    for h in reader_handles {
        let reads = h.join().unwrap();
        assert!(reads > 0, "Reader thread did some work");
    }
    let final_version = writer_handle.join().unwrap();
    assert!(final_version > 101, "Writer thread did some work");
}

/// SAFETY: apply_batch atomicity under failure
///
/// `apply_batch` groups operations by branch_id and holds the DashMap
/// shard write lock for each branch's entire batch, making writes
/// atomic within a single branch. Across branches, partial application
/// is theoretically possible, but acceptable: branches are independent
/// domains and each branch's batch is self-consistent. In practice the
/// individual `insert`/`push` operations are infallible (no I/O, no
/// allocation failure on these small structures), so mid-batch failure
/// does not occur.
#[test]
fn test_apply_batch_partial_application() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    // Create batch with many keys
    let keys: Vec<_> = (0..10)
        .map(|i| create_test_key(branch_id, &format!("batch_key_{}", i)))
        .collect();

    let writes: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k.clone(), Value::Int(i as i64)))
        .collect();

    // Apply batch
    store.apply_batch(writes, vec![], 1).unwrap();

    // All keys should be written with same version
    for (i, key) in keys.iter().enumerate() {
        let result = Storage::get_versioned(&store, key, u64::MAX).unwrap();
        assert!(result.is_some(), "Key {} should exist after batch", i);
        let vv = result.unwrap();
        assert_eq!(vv.version.as_u64(), 1, "All keys should have version 1");
    }
}

/// SAFETY: fetch_max semantics for version updates
///
/// `apply_batch` uses `AtomicU64::fetch_max(version, AcqRel)` to advance
/// the global version counter. This is a single atomic operation that
/// correctly handles out-of-order batch application: the version only
/// moves forward, never backward, regardless of arrival order.
#[test]
fn test_version_fetch_max_out_of_order() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "out_of_order");

    // Apply batch at version 100 first
    store
        .apply_batch(vec![(key.clone(), Value::Int(100))], vec![], 100)
        .unwrap();
    assert_eq!(store.version(), 100);

    // Apply batch at version 50 (older) - version should stay at 100
    store
        .apply_batch(vec![(key.clone(), Value::Int(50))], vec![], 50)
        .unwrap();
    assert_eq!(
        store.version(),
        100,
        "Version should not decrease with fetch_max"
    );

    // Apply batch at version 150 - version should update
    store
        .apply_batch(vec![(key.clone(), Value::Int(150))], vec![], 150)
        .unwrap();
    assert_eq!(store.version(), 150);
}

/// SAFETY: Empty version chain after multiple deletes
///
/// Deleting a nonexistent key is handled gracefully: `Storage::delete`
/// returns `Ok(None)` when the key is not found. No version chain is
/// created for a key that was never written, so there is no risk of
/// an empty or corrupted chain.
#[test]
fn test_delete_nonexistent_key_no_panic() {
    use strata_core::traits::Storage;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "never_existed");

    // Delete should not panic
    Storage::delete_with_version(&store, &key, 1).unwrap();

    // Multiple deletes should be safe
    Storage::delete_with_version(&store, &key, 2).unwrap();
}

/// SAFETY: Scan operations with expired TTL values
///
/// `Storage::scan_prefix` explicitly
/// check `!sv.is_expired() && !sv.is_tombstone()` before including a
/// value in results. Expired TTL values are correctly filtered out.
#[test]
fn test_scan_filters_expired_ttl() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    // Create a mix of expired and non-expired values
    let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

    // Non-expired key
    let key1 = Key::new_kv(ns.clone(), "fresh");
    Storage::put_with_version_mode(
        &store,
        key1.clone(),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // "Expired" key - we'll simulate by using internal API
    let key2 = Key::new_kv(ns.clone(), "expired");
    let old_ts = Timestamp::from_micros(0); // epoch
    let expired_value = StoredValue::with_timestamp(
        Value::Int(2),
        Version::txn(2),
        old_ts,
        Some(std::time::Duration::from_secs(1)), // expired long ago
    );
    store
        .put(key2.clone(), expired_value, WriteMode::Append)
        .unwrap();

    // Update store version
    store.set_version(2);

    // Scan should only return the fresh key
    let prefix = Key::new_kv(ns.clone(), "");
    let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

    assert_eq!(results.len(), 1, "Scan should filter out expired values");
    assert!(
        results[0].0.user_key_string().unwrap().contains("fresh"),
        "Only fresh key should be returned"
    );
}

// ========================================================================
// BTreeSet Index Tests
// ========================================================================

#[test]
fn test_ordered_keys_consistent_with_data() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();

    // Insert keys
    for i in 0..20 {
        let key = create_test_key(branch_id, &format!("key_{:03}", i));
        Storage::put_with_version_mode(&store, key, Value::Int(i), 1, None, WriteMode::Append)
            .unwrap();
    }

    // Overwrite some keys (should not duplicate in BTreeSet)
    for i in 0..10 {
        let key = create_test_key(branch_id, &format!("key_{:03}", i));
        Storage::put_with_version_mode(
            &store,
            key,
            Value::Int(i + 100),
            2,
            None,
            WriteMode::Append,
        )
        .unwrap();
    }

    // Delete some keys (tombstone — key stays in both structures)
    for i in 15..20 {
        let key = create_test_key(branch_id, &format!("key_{:03}", i));
        Storage::delete_with_version(&store, &key, 3).unwrap();
    }

    // Ensure ordered_keys is rebuilt lazily, then verify consistency
    let mut shard = store.shards.get_mut(&branch_id).unwrap();
    shard.ensure_ordered_keys();
    let ordered = shard.ordered_keys.as_ref().unwrap();
    assert_eq!(
        ordered.len(),
        shard.data.len(),
        "ordered_keys and data must have the same number of keys"
    );

    // Every key in ordered_keys must exist in data
    for k in ordered.iter() {
        assert!(
            shard.data.contains_key(k),
            "ordered_keys has key not in data"
        );
    }
    // Every key in data must exist in ordered_keys
    for k in shard.data.keys() {
        assert!(ordered.contains(k), "data has key not in ordered_keys");
    }
}

#[test]
fn test_prefix_scan_sorted_without_explicit_sort() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Insert keys in reverse order
    let names = vec![
        "user:zara",
        "user:mike",
        "user:alice",
        "user:bob",
        "user:charlie",
    ];
    for name in &names {
        Storage::put_with_version_mode(
            &store,
            Key::new_kv(ns.clone(), name),
            Value::Int(1),
            1,
            None,
            WriteMode::Append,
        )
        .unwrap();
    }

    let prefix = Key::new_kv(ns.clone(), "user:");
    let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

    assert_eq!(results.len(), 5);
    // Verify sorted order
    for i in 0..results.len() - 1 {
        assert!(
            results[i].0 < results[i + 1].0,
            "Results must be sorted: {:?} should be < {:?}",
            results[i].0.user_key_string(),
            results[i + 1].0.user_key_string(),
        );
    }
}

#[test]
fn test_prefix_scan_with_tombstones() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Insert 3 keys at version 1, 2, 3
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "item:a"),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "item:b"),
        Value::Int(2),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "item:c"),
        Value::Int(3),
        3,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Capture version before delete (version = 3)
    let v_before = store.version();

    // Delete item:b (tombstone at version 4)
    Storage::delete_with_version(&store, &Key::new_kv(ns.clone(), "item:b"), 4).unwrap();

    // Capture version after delete (version = 4)
    let v_after = store.version();

    let prefix = Key::new_kv(ns.clone(), "item:");

    // Reading at version before delete should see all 3
    let results_before = Storage::scan_prefix(&store, &prefix, v_before).unwrap();
    assert_eq!(
        results_before.len(),
        3,
        "Pre-delete version sees all 3 items"
    );

    // Reading at version after delete should see 2 (item:b is tombstoned)
    let results_after = Storage::scan_prefix(&store, &prefix, v_after).unwrap();
    assert_eq!(results_after.len(), 2, "Post-delete version sees 2 items");
    let keys_after: Vec<_> = results_after
        .iter()
        .map(|(k, _)| k.user_key_string().unwrap())
        .collect();
    assert!(keys_after.contains(&"item:a".to_string()));
    assert!(keys_after.contains(&"item:c".to_string()));
}

#[test]
fn test_prefix_scan_scales_with_matches() {
    use strata_core::traits::Storage;
    use strata_core::types::Namespace;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Insert 5000 keys with prefix "alpha:" and 5000 with prefix "beta:"
    for i in 0..5000 {
        Storage::put_with_version_mode(
            &store,
            Key::new_kv(ns.clone(), format!("alpha:{:05}", i)),
            Value::Int(i),
            1,
            None,
            WriteMode::Append,
        )
        .unwrap();
        Storage::put_with_version_mode(
            &store,
            Key::new_kv(ns.clone(), format!("beta:{:05}", i)),
            Value::Int(i),
            2,
            None,
            WriteMode::Append,
        )
        .unwrap();
    }

    // Scan "alpha:" should return exactly 5000
    let prefix_a = Key::new_kv(ns.clone(), "alpha:");
    let results_a = Storage::scan_prefix(&store, &prefix_a, u64::MAX).unwrap();
    assert_eq!(
        results_a.len(),
        5000,
        "alpha: prefix should match 5000 keys"
    );

    // Scan "beta:" should return exactly 5000
    let prefix_b = Key::new_kv(ns.clone(), "beta:");
    let results_b = Storage::scan_prefix(&store, &prefix_b, u64::MAX).unwrap();
    assert_eq!(results_b.len(), 5000, "beta: prefix should match 5000 keys");

    // Scan non-existent prefix should return 0
    let prefix_none = Key::new_kv(ns.clone(), "gamma:");
    let results_none = Storage::scan_prefix(&store, &prefix_none, u64::MAX).unwrap();
    assert_eq!(results_none.len(), 0, "gamma: prefix should match 0 keys");
}

// ========================================================================
// Direct-path and Lazy BTreeSet Tests
// ========================================================================

#[test]
fn test_get_direct_basic() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "direct_key");

    // Should return None for missing keys
    assert!(store.get_direct(&key).is_none());

    // Insert a value
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(42),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Should return the latest value
    let result = store.get_direct(&key);
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Value::Int(42));
}

#[test]
fn test_get_direct_filters_tombstones() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "tombstone_key");

    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    assert!(store.get_direct(&key).is_some());

    // Delete creates a tombstone
    Storage::delete_with_version(&store, &key, 2).unwrap();
    assert!(
        store.get_direct(&key).is_none(),
        "get_direct must filter tombstones"
    );
}

#[test]
fn test_get_direct_filters_expired_ttl() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "ttl_key");

    // Insert with TTL that's already expired
    let expired_value = StoredValue::with_timestamp(
        Value::Int(99),
        Version::txn(1),
        Timestamp::from_micros(1),               // ancient timestamp
        Some(std::time::Duration::from_secs(1)), // expired long ago
    );
    store
        .put(key.clone(), expired_value, WriteMode::Append)
        .unwrap();

    assert!(
        store.get_direct(&key).is_none(),
        "get_direct must filter expired values"
    );
}

#[test]
fn test_get_direct_returns_latest_version() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "multi_version");

    // Insert multiple versions
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(2),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(3),
        3,
        None,
        WriteMode::Append,
    )
    .unwrap();

    let result = store.get_direct(&key).unwrap();
    assert_eq!(
        result.value,
        Value::Int(3),
        "get_direct must return the latest version"
    );
}

#[test]
fn test_lazy_ordered_keys_invalidation_and_rebuild() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

    // Insert keys — ordered_keys starts as None (never built yet)
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "c"),
        Value::Int(3),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "a"),
        Value::Int(1),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "b"),
        Value::Int(2),
        3,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // ordered_keys should be None (not yet built)
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ordered_keys.is_none(),
            "ordered_keys should be None after insertions"
        );
    }

    // scan_prefix forces a rebuild via ensure_ordered_keys
    let prefix = Key::new_kv(ns.clone(), "");
    let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
    assert_eq!(results.len(), 3);

    // Verify sorted order after lazy rebuild
    let keys: Vec<String> = results
        .iter()
        .filter_map(|(k, _)| k.user_key_string())
        .collect();
    assert_eq!(
        keys,
        vec!["a", "b", "c"],
        "Keys must be sorted after lazy rebuild"
    );

    // ordered_keys should now be Some (rebuilt by the scan)
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ordered_keys.is_some(),
            "ordered_keys should be Some after scan triggered rebuild"
        );
    }

    // Insert a NEW key — should be incrementally added to ordered_keys
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "aa"),
        Value::Int(4),
        4,
        None,
        WriteMode::Append,
    )
    .unwrap();
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ordered_keys.is_some(),
            "ordered_keys should remain Some after incremental insert"
        );
    }

    // Scan again — should see all 4 keys in order
    let results2 = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
    let keys2: Vec<String> = results2
        .iter()
        .filter_map(|(k, _)| k.user_key_string())
        .collect();
    assert_eq!(keys2, vec!["a", "aa", "b", "c"]);
}

#[test]
fn test_lazy_ordered_keys_update_existing_no_invalidation() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

    // Insert initial key
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "k"),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Force a rebuild by scanning
    let prefix = Key::new_kv(ns.clone(), "");
    let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

    // ordered_keys should now be Some
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(shard.ordered_keys.is_some());
    }

    // Update EXISTING key — should NOT invalidate ordered_keys
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "k"),
        Value::Int(2),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ordered_keys.is_some(),
            "Updating an existing key must not invalidate the ordered index"
        );
    }
}

#[test]
fn test_incremental_ordered_keys_interleaved_insert_scan() {
    // Reproduces the Workload E pattern: interleaved inserts and scans.
    // Before the fix, each insert invalidated ordered_keys (set to None),
    // forcing an O(N) rebuild on every subsequent scan.
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

    // Seed with initial keys
    for i in 0..10 {
        Storage::put_with_version_mode(
            &store,
            Key::new_kv(ns.clone(), &format!("key_{:03}", i)),
            Value::Int(i),
            1,
            None,
            WriteMode::Append,
        )
        .unwrap();
    }

    // First scan — builds ordered_keys from scratch
    let prefix = Key::new_kv(ns.clone(), "");
    let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
    assert_eq!(results.len(), 10);
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(shard.ordered_keys.is_some());
    }

    // Interleaved insert-scan pattern (simulates Workload E)
    for i in 10..20 {
        // Insert a new key
        Storage::put_with_version_mode(
            &store,
            Key::new_kv(ns.clone(), &format!("key_{:03}", i)),
            Value::Int(i),
            2,
            None,
            WriteMode::Append,
        )
        .unwrap();

        // ordered_keys must remain Some (incremental insert, no invalidation)
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ordered_keys.is_some(),
                "ordered_keys invalidated on insert {i} — should be incrementally maintained"
            );
        }

        // Scan must return all keys so far, in sorted order
        let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
        assert_eq!(
            results.len(),
            i as usize + 1,
            "Expected {} keys after insert {i}",
            i + 1
        );

        // Verify sorted order
        let keys: Vec<String> = results
            .iter()
            .filter_map(|(k, _)| k.user_key_string())
            .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "Keys not sorted after insert {i}");
    }
}

#[test]
fn test_incremental_ordered_keys_btreeset_content_after_inserts() {
    // Verifies that the BTreeSet content is exactly correct after
    // incremental inserts — not just that it's Some.
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

    // Insert initial keys and build ordered_keys via scan
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "b"),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "d"),
        Value::Int(2),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();
    let prefix = Key::new_kv(ns.clone(), "");
    let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

    // Incrementally insert new keys
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "a"),
        Value::Int(3),
        3,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "c"),
        Value::Int(4),
        4,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "e"),
        Value::Int(5),
        5,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Verify BTreeSet matches data keys exactly
    {
        let shard = store.shards.get(&branch_id).unwrap();
        let btree = shard.ordered_keys.as_ref().expect("should be Some");
        assert_eq!(
            btree.len(),
            shard.data.len(),
            "BTreeSet length must match data length"
        );
        for k in shard.data.keys() {
            assert!(btree.contains(k), "BTreeSet missing key from data: {:?}", k);
        }
        for k in btree.iter() {
            assert!(
                shard.data.contains_key(k),
                "BTreeSet has key not in data: {:?}",
                k
            );
        }

        // Verify BTreeSet iteration order
        let btree_keys: Vec<String> = btree.iter().filter_map(|k| k.user_key_string()).collect();
        assert_eq!(btree_keys, vec!["a", "b", "c", "d", "e"]);
    }
}

#[test]
fn test_incremental_ordered_keys_via_apply_batch() {
    // Verifies that apply_batch (commit path) incrementally maintains
    // ordered_keys instead of invalidating it.
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

    // Insert initial keys and build ordered_keys via scan
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "a"),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "c"),
        Value::Int(2),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();
    let prefix = Key::new_kv(ns.clone(), "");
    let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ordered_keys.is_some(),
            "precondition: ordered_keys built"
        );
    }

    // apply_batch with new keys
    let writes = vec![
        (Key::new_kv(ns.clone(), "b"), Value::Int(3)),
        (Key::new_kv(ns.clone(), "d"), Value::Int(4)),
    ];
    store.apply_batch(writes, vec![], 10).unwrap();

    // ordered_keys must remain Some
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ordered_keys.is_some(),
            "apply_batch should not invalidate ordered_keys"
        );
        let btree = shard.ordered_keys.as_ref().unwrap();
        assert_eq!(btree.len(), 4, "BTreeSet should have all 4 keys");
    }

    // Scan must return all keys in sorted order
    let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
    let keys: Vec<String> = results
        .iter()
        .filter_map(|(k, _)| k.user_key_string())
        .collect();
    assert_eq!(keys, vec!["a", "b", "c", "d"]);
}

#[test]
fn test_incremental_ordered_keys_via_apply_batch_deletes() {
    // Verifies that apply_batch deletes of nonexistent keys (which create
    // tombstone entries) also incrementally maintain ordered_keys.
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

    // Insert initial key and build ordered_keys via scan
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "a"),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    let prefix = Key::new_kv(ns.clone(), "");
    let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(shard.ordered_keys.is_some());
    }

    // apply_batch: delete a key that doesn't exist yet (creates tombstone entry)
    let deletes = vec![Key::new_kv(ns.clone(), "z")];
    store.apply_batch(vec![], deletes, 10).unwrap();

    // ordered_keys must remain Some and include the new tombstone key
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ordered_keys.is_some(),
            "apply_batch delete should not invalidate ordered_keys"
        );
        let btree = shard.ordered_keys.as_ref().unwrap();
        assert_eq!(
            btree.len(),
            shard.data.len(),
            "BTreeSet length must match data length after tombstone insert"
        );
    }
}

#[test]
fn test_incremental_ordered_keys_delete_nonexistent_key() {
    // Deleting a nonexistent key goes through put() to create a tombstone.
    // This must incrementally update ordered_keys.
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

    // Insert initial key and build ordered_keys via scan
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "a"),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    let prefix = Key::new_kv(ns.clone(), "");
    let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(shard.ordered_keys.is_some());
    }

    // Delete a key that doesn't exist — creates tombstone via put()
    let nonexistent = Key::new_kv(ns.clone(), "z");
    Storage::delete_with_version(&store, &nonexistent, 2).unwrap();

    // ordered_keys must remain Some and include the tombstone key
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ordered_keys.is_some(),
            "delete of nonexistent key should not invalidate ordered_keys"
        );
        let btree = shard.ordered_keys.as_ref().unwrap();
        assert_eq!(
            btree.len(),
            shard.data.len(),
            "BTreeSet must include the tombstone key"
        );
    }
}

#[test]
fn test_incremental_ordered_keys_apply_batch_mixed() {
    // apply_batch with a mix of: updates to existing keys, new key writes,
    // and new key deletes. Only new keys should be added to the BTreeSet.
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

    // Insert initial keys and build ordered_keys
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "a"),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "c"),
        Value::Int(2),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "e"),
        Value::Int(3),
        3,
        None,
        WriteMode::Append,
    )
    .unwrap();
    let prefix = Key::new_kv(ns.clone(), "");
    let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

    // apply_batch: update "a" (existing), insert "b" (new), delete "d" (new tombstone)
    let writes = vec![
        (Key::new_kv(ns.clone(), "a"), Value::Int(10)), // existing
        (Key::new_kv(ns.clone(), "b"), Value::Int(20)), // new
    ];
    let deletes = vec![
        Key::new_kv(ns.clone(), "d"), // new (tombstone)
    ];
    store.apply_batch(writes, deletes, 10).unwrap();

    // ordered_keys must remain Some
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ordered_keys.is_some(),
            "mixed apply_batch should not invalidate ordered_keys"
        );
        let btree = shard.ordered_keys.as_ref().unwrap();
        // data should have: a, b, c, d (tombstone), e = 5 keys
        assert_eq!(btree.len(), 5);
        assert_eq!(shard.data.len(), 5);
        assert_eq!(btree.len(), shard.data.len());
    }

    // Scan returns non-tombstoned keys in sorted order
    let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
    let keys: Vec<String> = results
        .iter()
        .filter_map(|(k, _)| k.user_key_string())
        .collect();
    assert_eq!(keys, vec!["a", "b", "c", "e"]);
}

// ========================================================================
// Dead Key GC Tests (Issue #1304)
// ========================================================================

#[test]
fn test_version_chain_is_dead() {
    // Single tombstone → dead
    let chain = VersionChain::new(StoredValue::tombstone(Version::txn(1)));
    assert!(chain.is_dead());

    // Single expired value → dead
    let expired = StoredValue::with_timestamp(
        strata_core::value::Value::Int(1),
        Version::txn(1),
        Timestamp::from_micros(0),
        Some(std::time::Duration::from_secs(1)),
    );
    let chain = VersionChain::new(expired);
    assert!(chain.is_dead());

    // Single live value → not dead
    let chain = VersionChain::new(make_sv(1));
    assert!(!chain.is_dead());

    // Multi with tombstone latest + live older → not dead
    let mut chain = VersionChain::new(make_sv(1));
    chain.push(StoredValue::tombstone(Version::txn(10)));
    assert!(!chain.is_dead());

    // After GC reduces to single tombstone → dead
    let mut chain = VersionChain::new(make_sv(1));
    chain.push(StoredValue::tombstone(Version::txn(10)));
    assert_eq!(chain.version_count(), 2);
    chain.gc(5); // Prunes v1, leaves v10 tombstone
    assert_eq!(chain.version_count(), 1);
    assert!(chain.is_dead());
}

#[test]
fn test_gc_branch_removes_dead_keys_from_hashmap() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::new(
        branch_id,
        "default".to_string(),
    ));

    // Insert a key, then tombstone it
    let key = Key::new_kv(ns.clone(), "victim");
    store.put(
        key.clone(),
        StoredValue::new(strata_core::value::Value::Int(1), Version::txn(1), None),
        WriteMode::Append,
    );
    store
        .put(
            key.clone(),
            StoredValue::tombstone(Version::txn(10)),
            WriteMode::Append,
        )
        .unwrap();

    // BTreeSet not built yet — verify GC handles the None case
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ordered_keys.is_none(),
            "BTreeSet should not exist yet"
        );
    }

    // GC with min_version high enough to prune the live version
    let pruned = store.gc_branch(branch_id, 5);
    assert_eq!(pruned, 1, "should report 1 version pruned (v1 live value)");

    // Key should be removed from HashMap
    let shard = store.shards.get(&branch_id).unwrap();
    assert!(
        !shard.data.contains_key(&key),
        "dead key should be removed from HashMap after GC"
    );
    assert!(
        shard.ordered_keys.is_none(),
        "BTreeSet should remain None (never built)"
    );
}

#[test]
fn test_gc_branch_removes_single_tombstone_no_prior_value() {
    // Key inserted directly as tombstone (no prior live value) —
    // exercises the Single(tombstone) → gc() noop → is_dead() true path
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::new(
        branch_id,
        "default".to_string(),
    ));

    let key = Key::new_kv(ns.clone(), "orphan_tombstone");
    store
        .put(
            key.clone(),
            StoredValue::tombstone(Version::txn(5)),
            WriteMode::Append,
        )
        .unwrap();

    // gc() on Single is a noop (returns 0), but is_dead() should still
    // detect the single tombstone and remove it
    let pruned = store.gc_branch(branch_id, 100);
    assert_eq!(pruned, 0, "gc on Single is always 0");

    let shard = store.shards.get(&branch_id).unwrap();
    assert!(
        !shard.data.contains_key(&key),
        "single tombstone with no prior value should be removed"
    );
}

#[test]
fn test_gc_branch_removes_dead_keys_from_btreeset() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::new(
        branch_id,
        "default".to_string(),
    ));

    let key = Key::new_kv(ns.clone(), "victim");
    store.put(
        key.clone(),
        StoredValue::new(strata_core::value::Value::Int(1), Version::txn(1), None),
        WriteMode::Append,
    );
    store
        .put(
            key.clone(),
            StoredValue::tombstone(Version::txn(10)),
            WriteMode::Append,
        )
        .unwrap();

    // Trigger BTreeSet build before GC
    {
        let mut shard = store.shards.get_mut(&branch_id).unwrap();
        shard.ensure_ordered_keys();
        assert!(shard.ordered_keys.as_ref().unwrap().contains(&key));
    }

    store.gc_branch(branch_id, 5);

    let shard = store.shards.get(&branch_id).unwrap();
    let btree = shard.ordered_keys.as_ref().unwrap();
    assert!(
        !btree.contains(&key),
        "dead key should be removed from BTreeSet after GC"
    );
}

#[test]
fn test_gc_branch_keeps_alive_keys() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::new(
        branch_id,
        "default".to_string(),
    ));

    let key_a = Key::new_kv(ns.clone(), "alive");
    let key_b = Key::new_kv(ns.clone(), "dead");

    // Key A: live value
    store.put(
        key_a.clone(),
        StoredValue::new(strata_core::value::Value::Int(1), Version::txn(1), None),
        WriteMode::Append,
    );

    // Key B: value then tombstone
    store.put(
        key_b.clone(),
        StoredValue::new(strata_core::value::Value::Int(2), Version::txn(2), None),
        WriteMode::Append,
    );
    store
        .put(
            key_b.clone(),
            StoredValue::tombstone(Version::txn(10)),
            WriteMode::Append,
        )
        .unwrap();

    // Build BTreeSet
    {
        let mut shard = store.shards.get_mut(&branch_id).unwrap();
        shard.ensure_ordered_keys();
    }

    let pruned = store.gc_branch(branch_id, 5);
    assert_eq!(pruned, 1, "only v2 of key_b should be pruned");

    let shard = store.shards.get(&branch_id).unwrap();
    // A remains in HashMap and BTreeSet with correct value
    assert!(shard.data.contains_key(&key_a));
    assert!(shard.ordered_keys.as_ref().unwrap().contains(&key_a));
    let chain_a = shard.data.get(&key_a).unwrap();
    assert_eq!(chain_a.version_count(), 1);
    assert!(!chain_a.is_dead());
    // B removed from both
    assert!(!shard.data.contains_key(&key_b));
    assert!(!shard.ordered_keys.as_ref().unwrap().contains(&key_b));
}

#[test]
fn test_gc_branch_keeps_multi_reduced_to_live() {
    // Critical negative case: Multi chain reduced to a single LIVE value
    // by GC must NOT be treated as dead. Guards against is_dead() regressing
    // to just `version_count() == 1`.
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::new(
        branch_id,
        "default".to_string(),
    ));

    let key = Key::new_kv(ns.clone(), "survivor");
    // v5: old value, v10: updated value (both live)
    store.put(
        key.clone(),
        StoredValue::new(strata_core::value::Value::Int(1), Version::txn(5), None),
        WriteMode::Append,
    );
    store.put(
        key.clone(),
        StoredValue::new(strata_core::value::Value::Int(2), Version::txn(10), None),
        WriteMode::Append,
    );

    // Build BTreeSet before GC
    {
        let mut shard = store.shards.get_mut(&branch_id).unwrap();
        shard.ensure_ordered_keys();
    }

    // GC with min_version=8 → prunes v5, leaves v10 (live)
    let pruned = store.gc_branch(branch_id, 8);
    assert_eq!(pruned, 1, "v5 should be pruned");

    let shard = store.shards.get(&branch_id).unwrap();
    // Key MUST survive — single live value is not dead
    assert!(
        shard.data.contains_key(&key),
        "key reduced to single LIVE value must NOT be removed"
    );
    assert!(shard.ordered_keys.as_ref().unwrap().contains(&key));
    let chain = shard.data.get(&key).unwrap();
    assert_eq!(chain.version_count(), 1);
    assert!(!chain.is_dead());
    assert_eq!(chain.latest().unwrap().version(), Version::txn(10));
}

#[test]
fn test_gc_branch_keeps_key_with_multiple_versions() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::new(
        branch_id,
        "default".to_string(),
    ));

    let key = Key::new_kv(ns.clone(), "multi");
    // v10: live value, v20: tombstone
    store.put(
        key.clone(),
        StoredValue::new(strata_core::value::Value::Int(1), Version::txn(10), None),
        WriteMode::Append,
    );
    store
        .put(
            key.clone(),
            StoredValue::tombstone(Version::txn(20)),
            WriteMode::Append,
        )
        .unwrap();

    // Build BTreeSet to verify it's maintained across progressive GC
    {
        let mut shard = store.shards.get_mut(&branch_id).unwrap();
        shard.ensure_ordered_keys();
    }

    // GC with min_version=5 → too low to prune v10, key stays
    let pruned = store.gc_branch(branch_id, 5);
    assert_eq!(
        pruned, 0,
        "nothing to prune when min_version < all versions"
    );
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.data.contains_key(&key),
            "key with multiple versions should survive low min_version GC"
        );
        assert!(
            shard.ordered_keys.as_ref().unwrap().contains(&key),
            "key should remain in BTreeSet after surviving GC"
        );
        assert_eq!(shard.data.get(&key).unwrap().version_count(), 2);
    }

    // GC with min_version=15 → prunes v10, leaves only v20 tombstone → dead
    let pruned = store.gc_branch(branch_id, 15);
    assert_eq!(pruned, 1, "v10 should be pruned");
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            !shard.data.contains_key(&key),
            "key reduced to single tombstone should be removed from HashMap"
        );
        assert!(
            !shard.ordered_keys.as_ref().unwrap().contains(&key),
            "key reduced to single tombstone should be removed from BTreeSet"
        );
    }
}

#[test]
fn test_ensure_ordered_keys_skips_dead_keys() {
    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::new(
        branch_id,
        "default".to_string(),
    ));

    let key_live = Key::new_kv(ns.clone(), "live");
    let key_dead = Key::new_kv(ns.clone(), "dead");

    // Live key
    store.put(
        key_live.clone(),
        StoredValue::new(strata_core::value::Value::Int(1), Version::txn(1), None),
        WriteMode::Append,
    );

    // Dead key (tombstone only)
    store
        .put(
            key_dead.clone(),
            StoredValue::tombstone(Version::txn(2)),
            WriteMode::Append,
        )
        .unwrap();

    // Build BTreeSet — should skip the dead key
    {
        let mut shard = store.shards.get_mut(&branch_id).unwrap();
        shard.ensure_ordered_keys();
        let btree = shard.ordered_keys.as_ref().unwrap();
        assert!(btree.contains(&key_live));
        assert!(
            !btree.contains(&key_dead),
            "ensure_ordered_keys should skip dead (tombstone-only) keys"
        );
        // HashMap must still contain the dead key — filtering is BTreeSet-only
        assert!(
            shard.data.contains_key(&key_dead),
            "ensure_ordered_keys must NOT remove from HashMap"
        );
        assert_eq!(btree.len(), 1, "BTreeSet should only contain the live key");
        assert_eq!(shard.data.len(), 2, "HashMap should contain both keys");
    }
}

#[test]
fn test_scan_prefix_after_gc_removes_dead_keys() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let ns = Arc::new(strata_core::types::Namespace::new(
        branch_id,
        "default".to_string(),
    ));

    // Insert keys: alpha (live), beta (will be tombstoned), gamma (live)
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "alpha"),
        Value::Int(1),
        1,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "beta"),
        Value::Int(2),
        2,
        None,
        WriteMode::Append,
    )
    .unwrap();
    Storage::put_with_version_mode(
        &store,
        Key::new_kv(ns.clone(), "gamma"),
        Value::Int(3),
        3,
        None,
        WriteMode::Append,
    )
    .unwrap();

    // Tombstone beta
    let beta_key = Key::new_kv(ns.clone(), "beta");
    store
        .put(
            beta_key.clone(),
            StoredValue::tombstone(Version::txn(100)),
            WriteMode::Append,
        )
        .unwrap();

    // Scan before GC — beta should not appear (tombstoned)
    let prefix = Key::new_kv(ns.clone(), "");
    let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
    let keys: Vec<String> = results
        .iter()
        .filter_map(|(k, _)| k.user_key_string())
        .collect();
    assert_eq!(keys, vec!["alpha", "gamma"]);

    // Verify beta is still in internal structures before GC
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.data.contains_key(&beta_key),
            "beta should still be in HashMap before GC"
        );
        assert_eq!(shard.data.len(), 3, "all 3 keys in HashMap before GC");
    }

    // Run GC to actually remove dead keys
    store.gc_branch(branch_id, 50);

    // Verify beta is gone from internal structures
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(!shard.data.contains_key(&beta_key));
        assert!(!shard.ordered_keys.as_ref().unwrap().contains(&beta_key));
        assert_eq!(shard.data.len(), 2, "only live keys remain after GC");
    } // Drop read guard before scan_prefix needs get_mut

    // Scan after GC — same result, live keys still present with correct values
    let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.user_key_string().unwrap(), "alpha");
    assert_eq!(results[0].1.value, Value::Int(1));
    assert_eq!(results[1].0.user_key_string().unwrap(), "gamma");
    assert_eq!(results[1].1.value, Value::Int(3));
}

// ========================================================================
// TTL Index Wiring Tests (S-4)
// ========================================================================

fn make_sv_with_ttl(version: u64, ttl_secs: u64) -> StoredValue {
    StoredValue::with_timestamp(
        strata_core::value::Value::Int(version as i64),
        Version::txn(version),
        Timestamp::from_micros(1_000_000), // fixed ts=1s for predictable expiry
        Some(std::time::Duration::from_secs(ttl_secs)),
    )
}

fn make_expired_sv(version: u64) -> StoredValue {
    // Timestamp at epoch + 1ms TTL → definitely expired
    StoredValue::with_timestamp(
        strata_core::value::Value::Int(version as i64),
        Version::txn(version),
        Timestamp::from_micros(0),
        Some(std::time::Duration::from_millis(1)),
    )
}

fn test_ns_and_key(suffix: &str) -> Key {
    use strata_core::types::Namespace;
    let branch_id = BranchId::from_bytes([1u8; 16]);
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));
    Key::new_kv(ns, suffix)
}

#[test]
fn test_put_with_ttl_updates_ttl_index() {
    let store = ShardedStore::new();
    let key = test_ns_and_key("ttl_key");
    let sv = make_sv_with_ttl(1, 60);
    let expected_expiry = sv.expiry_timestamp().unwrap();

    store.put(key.clone(), sv, WriteMode::Append).unwrap();

    // Verify TTL index has the entry
    let shard = store.shards.get(&key.namespace.branch_id).unwrap();
    assert_eq!(shard.ttl_index.len(), 1);
    let expired = shard.ttl_index.find_expired(expected_expiry);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0], key);
}

#[test]
fn test_put_overwrite_removes_old_ttl() {
    let store = ShardedStore::new();
    let key = test_ns_and_key("overwrite_key");

    // First put with TTL=60s
    let sv1 = make_sv_with_ttl(1, 60);
    let old_expiry = sv1.expiry_timestamp().unwrap();
    store.put(key.clone(), sv1, WriteMode::Append).unwrap();

    // Overwrite with TTL=120s
    let sv2 = make_sv_with_ttl(2, 120);
    let new_expiry = sv2.expiry_timestamp().unwrap();
    store.put(key.clone(), sv2, WriteMode::Append).unwrap();

    let shard = store.shards.get(&key.namespace.branch_id).unwrap();
    // Should have exactly 1 TTL entry (the new one), not 2
    assert_eq!(shard.ttl_index.len(), 1);
    // Old expiry should find nothing
    let at_old = shard.ttl_index.find_expired(old_expiry);
    assert!(at_old.is_empty(), "Old TTL entry should be removed");
    // New expiry should find the key
    let at_new = shard.ttl_index.find_expired(new_expiry);
    assert_eq!(at_new.len(), 1);
}

#[test]
fn test_put_overwrite_ttl_with_no_ttl_cleans_index() {
    let store = ShardedStore::new();
    let key = test_ns_and_key("ttl_to_none");

    // First put with TTL
    store
        .put(key.clone(), make_sv_with_ttl(1, 60), WriteMode::Append)
        .unwrap();
    {
        let shard = store.shards.get(&key.namespace.branch_id).unwrap();
        assert_eq!(shard.ttl_index.len(), 1);
    }

    // Overwrite without TTL
    store
        .put(key.clone(), make_sv(2), WriteMode::Append)
        .unwrap();
    {
        let shard = store.shards.get(&key.namespace.branch_id).unwrap();
        assert!(
            shard.ttl_index.is_empty(),
            "TTL index should be empty after overwrite with non-TTL value"
        );
    }
}

#[test]
fn test_put_without_ttl_no_index_overhead() {
    let store = ShardedStore::new();
    let key = test_ns_and_key("no_ttl");

    store
        .put(key.clone(), make_sv(1), WriteMode::Append)
        .unwrap();

    let shard = store.shards.get(&key.namespace.branch_id).unwrap();
    assert!(
        shard.ttl_index.is_empty(),
        "Non-TTL put should not touch TTL index"
    );
}

#[test]
fn test_delete_over_ttl_key_cleans_index() {
    let store = ShardedStore::new();
    let key = test_ns_and_key("delete_ttl");

    // Put with TTL
    store
        .put(key.clone(), make_sv_with_ttl(1, 60), WriteMode::Append)
        .unwrap();
    {
        let shard = store.shards.get(&key.namespace.branch_id).unwrap();
        assert_eq!(shard.ttl_index.len(), 1);
    }

    // Delete (tombstone) — should clean the TTL entry
    store
        .put(
            key.clone(),
            StoredValue::tombstone(Version::txn(2)),
            WriteMode::Append,
        )
        .unwrap();
    {
        let shard = store.shards.get(&key.namespace.branch_id).unwrap();
        assert!(
            shard.ttl_index.is_empty(),
            "Tombstone over TTL key should clean TTL index"
        );
    }
}

#[test]
fn test_expire_ttl_keys_cleans_index() {
    let store = ShardedStore::new();
    let key1 = test_ns_and_key("exp1");
    let key2 = test_ns_and_key("exp2");

    // Insert two keys with TTLs that are already expired (epoch + 1ms)
    store
        .put(key1.clone(), make_expired_sv(1), WriteMode::Append)
        .unwrap();
    store
        .put(key2.clone(), make_expired_sv(2), WriteMode::Append)
        .unwrap();

    {
        let shard = store.shards.get(&key1.namespace.branch_id).unwrap();
        assert_eq!(shard.ttl_index.len(), 2);
    }

    // Expire — should clean both index entries
    let removed = store.expire_ttl_keys(Timestamp::now());
    assert_eq!(removed, 2);

    {
        let shard = store.shards.get(&key1.namespace.branch_id).unwrap();
        assert!(shard.ttl_index.is_empty());
    }
}

#[test]
fn test_gc_branch_cleans_ttl_entries_for_dead_keys() {
    let store = ShardedStore::new();
    let key = test_ns_and_key("gc_ttl");
    let branch_id = key.namespace.branch_id;

    // Version 1: value with TTL (already expired)
    store
        .put(key.clone(), make_expired_sv(1), WriteMode::Append)
        .unwrap();
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert_eq!(shard.ttl_index.len(), 1);
    }

    // Version 2: tombstone (makes it dead after GC)
    store
        .put(
            key.clone(),
            StoredValue::tombstone(Version::txn(2)),
            WriteMode::Append,
        )
        .unwrap();

    // Tombstone should have cleaned the TTL entry from the put() path
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ttl_index.is_empty(),
            "TTL index should be clean after tombstone"
        );
    }

    // GC with min_version=3 should remove the dead key entirely
    let pruned = store.gc_branch(branch_id, 3);
    assert!(pruned > 0);

    // Key should be gone from data
    let shard = store.shards.get(&branch_id).unwrap();
    assert!(shard.data.is_empty());
}

#[test]
fn test_gc_branch_cleans_ttl_for_expired_only_key() {
    let store = ShardedStore::new();
    let key = test_ns_and_key("gc_exp_only");
    let branch_id = key.namespace.branch_id;

    // Single expired version (no tombstone, just expired TTL)
    store
        .put(key.clone(), make_expired_sv(1), WriteMode::Append)
        .unwrap();
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert_eq!(shard.ttl_index.len(), 1);
    }

    // GC with min_version=2 — the expired entry is dead, should be removed
    let pruned = store.gc_branch(branch_id, 2);
    // The chain is dead (expired), so key removed from data AND ttl index
    let shard = store.shards.get(&branch_id).unwrap();
    assert!(shard.data.is_empty(), "Dead expired key should be removed");
    assert!(
        shard.ttl_index.is_empty(),
        "TTL entry for dead expired key should be cleaned"
    );
    let _ = pruned;
}

#[test]
fn test_apply_batch_cleans_ttl_on_overwrite() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let key = test_ns_and_key("batch_ttl");
    let branch_id = key.namespace.branch_id;

    // First: put with TTL via the normal path
    store
        .put(key.clone(), make_sv_with_ttl(1, 60), WriteMode::Append)
        .unwrap();
    {
        let shard = store.shards.get(&branch_id).unwrap();
        assert_eq!(shard.ttl_index.len(), 1);
    }

    // Then: apply_batch overwrites the same key (no TTL in batch writes)
    store
        .apply_batch(vec![(key.clone(), Value::Int(99))], vec![], 2)
        .unwrap();

    // TTL entry should have been cleaned up
    let shard = store.shards.get(&branch_id).unwrap();
    assert!(
        shard.ttl_index.is_empty(),
        "apply_batch overwrite should clean stale TTL entry"
    );
}

// ========================================================================
// Branch Limit Tests
// ========================================================================

#[test]
fn test_branch_limit_allows_up_to_max() {
    use strata_core::value::Value;

    let store = ShardedStore::with_limits(3);
    for i in 0..3 {
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, &format!("k{}", i));
        store
            .put(
                key,
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            )
            .unwrap();
    }
    assert_eq!(store.shard_count(), 3);
}

#[test]
fn test_branch_limit_rejects_at_max() {
    use strata_core::value::Value;

    let store = ShardedStore::with_limits(2);
    for i in 0..2 {
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, &format!("k{}", i));
        store
            .put(
                key,
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            )
            .unwrap();
    }
    // Third branch should fail
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "overflow");
    let err = store
        .put(
            key,
            create_stored_value(Value::Int(99), 1),
            WriteMode::Append,
        )
        .unwrap_err();
    assert!(
        format!("{}", err).contains("branches"),
        "Error should mention branches: {}",
        err
    );
}

#[test]
fn test_branch_limit_allows_write_to_existing() {
    use strata_core::value::Value;

    let store = ShardedStore::with_limits(1);
    let branch_id = BranchId::new();
    let key1 = create_test_key(branch_id, "k1");
    let key2 = create_test_key(branch_id, "k2");
    store
        .put(
            key1,
            create_stored_value(Value::Int(1), 1),
            WriteMode::Append,
        )
        .unwrap();
    // Same branch — should succeed even though limit is 1
    store
        .put(
            key2,
            create_stored_value(Value::Int(2), 2),
            WriteMode::Append,
        )
        .unwrap();
    assert_eq!(store.shard_count(), 1);
}

#[test]
fn test_branch_limit_zero_unlimited() {
    use strata_core::value::Value;

    let store = ShardedStore::new(); // max_branches = 0 (unlimited)
    for i in 0..100 {
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, &format!("k{}", i));
        store
            .put(
                key,
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            )
            .unwrap();
    }
    assert_eq!(store.shard_count(), 100);
}

#[test]
fn test_branch_limit_apply_batch() {
    use strata_core::value::Value;

    let store = ShardedStore::with_limits(1);
    let b1 = BranchId::new();
    let key1 = create_test_key(b1, "k1");
    // First branch via apply_batch
    store
        .apply_batch(vec![(key1, Value::Int(1))], vec![], 1)
        .unwrap();

    // Second branch should fail
    let b2 = BranchId::new();
    let key2 = create_test_key(b2, "k2");
    let err = store
        .apply_batch(vec![(key2, Value::Int(2))], vec![], 2)
        .unwrap_err();
    assert!(format!("{}", err).contains("branches"));
}

// ========================================================================
// Memory Stats Tests
// ========================================================================

#[test]
fn test_memory_stats_empty() {
    let store = ShardedStore::new();
    let stats = store.memory_stats();
    assert_eq!(stats.total_branches, 0);
    assert_eq!(stats.total_entries, 0);
    assert_eq!(stats.estimated_bytes, 0);
    assert!(stats.per_branch.is_empty());
}

#[test]
fn test_memory_stats_with_data() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let b1 = BranchId::new();
    let b2 = BranchId::new();

    // Insert 10 entries in branch 1
    for i in 0..10 {
        let key = create_test_key(b1, &format!("k{}", i));
        store
            .put(
                key,
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            )
            .unwrap();
    }

    // Insert 5 entries in branch 2
    for i in 0..5 {
        let key = create_test_key(b2, &format!("k{}", i));
        store
            .put(
                key,
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            )
            .unwrap();
    }

    let stats = store.memory_stats();
    assert_eq!(stats.total_branches, 2);
    assert_eq!(stats.total_entries, 15);
    assert!(stats.estimated_bytes > 0);
    assert_eq!(stats.per_branch.len(), 2);

    // Find branch stats
    let b1_stats = stats.per_branch.iter().find(|s| s.branch_id == b1).unwrap();
    assert_eq!(b1_stats.entry_count, 10);
    assert!(b1_stats.estimated_bytes > 0);
}

#[test]
fn test_memory_stats_byte_formula() {
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch = BranchId::new();

    // Insert exactly 10 entries
    for i in 0..10 {
        let key = create_test_key(branch, &format!("k{}", i));
        store
            .put(
                key,
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            )
            .unwrap();
    }

    let stats = store.memory_stats();
    assert_eq!(stats.total_entries, 10);
    // 10 entries * 120 bytes/entry = 1200, no btree index by default
    assert_eq!(stats.estimated_bytes, 10 * 120);

    let branch_stats = &stats.per_branch[0];
    assert_eq!(branch_stats.estimated_bytes, 10 * 120);
    assert!(!branch_stats.has_btree_index);
}

#[test]
fn test_set_max_branches_dynamic() {
    use strata_core::value::Value;

    let store = ShardedStore::new(); // unlimited
                                     // Create 5 branches
    let mut branches = Vec::new();
    for i in 0..5 {
        let branch = BranchId::new();
        branches.push(branch);
        let key = create_test_key(branch, &format!("k{}", i));
        store
            .put(
                key,
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            )
            .unwrap();
    }
    assert_eq!(store.shard_count(), 5);

    // Now set a limit of 5 — existing branches still writable
    store.set_max_branches(5);
    let key = create_test_key(branches[0], "extra");
    store
        .put(
            key,
            create_stored_value(Value::Int(99), 2),
            WriteMode::Append,
        )
        .unwrap();

    // But a 6th branch should fail
    let new_branch = BranchId::new();
    let key = create_test_key(new_branch, "overflow");
    let err = store
        .put(
            key,
            create_stored_value(Value::Int(100), 3),
            WriteMode::Append,
        )
        .unwrap_err();
    assert!(format!("{}", err).contains("branches"));
}

#[test]
fn test_branch_limit_delete_new_branch() {
    use strata_core::value::Value;

    let store = ShardedStore::with_limits(1);
    let b1 = BranchId::new();
    let key1 = create_test_key(b1, "k1");
    store
        .put(
            key1.clone(),
            create_stored_value(Value::Int(1), 1),
            WriteMode::Append,
        )
        .unwrap();

    // delete_with_version() on an existing branch is fine
    store.delete_with_version(&key1, 2).unwrap();

    // delete_with_version() on a NEW branch should be rejected — delete inserts a
    // tombstone, which creates the shard if it doesn't exist.
    let b2 = BranchId::new();
    let key2 = create_test_key(b2, "phantom");
    let result = store.delete_with_version(&key2, 3);
    // delete_with_version() calls put() internally, which checks the branch limit
    assert!(result.is_err());
}

#[test]
fn test_branch_limit_via_storage_trait() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::with_limits(1);
    let b1 = BranchId::new();
    let key1 = create_test_key(b1, "k1");
    // Storage::put_with_version_mode via trait
    Storage::put_with_version_mode(&store, key1, Value::Int(1), 1, None, WriteMode::Append)
        .unwrap();

    // Second branch via trait should fail
    let b2 = BranchId::new();
    let key2 = create_test_key(b2, "k2");
    let err =
        Storage::put_with_version_mode(&store, key2, Value::Int(2), 1, None, WriteMode::Append)
            .unwrap_err();
    assert!(format!("{}", err).contains("branches"));
}

// ========================================================================
// KeepLast and max_versions_per_key Tests (Issue #1389)
// ========================================================================

#[test]
fn test_store_put_keep_last_1_replaces() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "adj_list");

    // Write 3 versions with Append
    for v in 1..=3u64 {
        store
            .put(
                key.clone(),
                create_stored_value(Value::Int(v as i64), v),
                WriteMode::Append,
            )
            .unwrap();
    }
    assert_eq!(
        store.get_history(&key, None, None).unwrap().len(),
        3,
        "should have 3 versions before KeepLast"
    );

    // Write with KeepLast(1) — should prune all old versions
    store
        .put(
            key.clone(),
            create_stored_value(Value::Int(99), 4),
            WriteMode::KeepLast(1),
        )
        .unwrap();

    let latest = store.get_versioned(&key, u64::MAX).unwrap().unwrap();
    assert_eq!(latest.value, Value::Int(99));
    assert_eq!(
        store.get_history(&key, None, None).unwrap().len(),
        1,
        "KeepLast(1) should leave exactly 1 version"
    );
}

#[test]
fn test_store_put_keep_last_3_prunes_oldest() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "bounded");

    // Write 5 versions with KeepLast(3)
    for v in 1..=5u64 {
        store
            .put(
                key.clone(),
                create_stored_value(Value::Int(v as i64), v),
                WriteMode::KeepLast(3),
            )
            .unwrap();
    }

    // Should have exactly 3 versions
    let history = store.get_history(&key, None, None).unwrap();
    assert_eq!(
        history.len(),
        3,
        "KeepLast(3) should keep exactly 3 versions"
    );
    assert_eq!(history[0].value, Value::Int(5)); // newest
    assert_eq!(history[1].value, Value::Int(4));
    assert_eq!(history[2].value, Value::Int(3)); // oldest kept

    // Snapshot read at version 2 should find nothing (pruned)
    assert!(
        store.get_versioned(&key, 2).unwrap().is_none(),
        "versions 1 and 2 should be pruned by KeepLast(3)"
    );
}

#[test]
fn test_global_max_versions_promotes_append() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    store.set_max_versions_per_key(3);

    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "global_limited");

    // Write 5 versions with Append — global max should kick in
    for v in 1..=5u64 {
        store
            .put(
                key.clone(),
                create_stored_value(Value::Int(v as i64), v),
                WriteMode::Append,
            )
            .unwrap();
    }

    let history = store.get_history(&key, None, None).unwrap();
    assert_eq!(
        history.len(),
        3,
        "global max_versions_per_key=3 should limit to 3 versions"
    );
    // Newest 3 should be kept
    assert_eq!(history[0].value, Value::Int(5));
    assert_eq!(history[1].value, Value::Int(4));
    assert_eq!(history[2].value, Value::Int(3));
}

#[test]
fn test_global_max_versions_overridden_by_explicit_keep_last() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    store.set_max_versions_per_key(10); // generous global limit

    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "override");

    // Write 5 versions with explicit KeepLast(2) — should override global 10
    for v in 1..=5u64 {
        store
            .put(
                key.clone(),
                create_stored_value(Value::Int(v as i64), v),
                WriteMode::KeepLast(2),
            )
            .unwrap();
    }

    let history = store.get_history(&key, None, None).unwrap();
    assert_eq!(
        history.len(),
        2,
        "explicit KeepLast(2) should override global max=10"
    );
    assert_eq!(history[0].value, Value::Int(5));
    assert_eq!(history[1].value, Value::Int(4));
}

#[test]
fn test_global_max_versions_zero_means_unlimited() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    // max_versions_per_key defaults to 0 (unlimited)

    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "unlimited");

    for v in 1..=20u64 {
        store
            .put(
                key.clone(),
                create_stored_value(Value::Int(v as i64), v),
                WriteMode::Append,
            )
            .unwrap();
    }

    let history = store.get_history(&key, None, None).unwrap();
    assert_eq!(
        history.len(),
        20,
        "max_versions_per_key=0 should keep all versions"
    );
}

#[test]
fn test_keep_last_via_storage_trait_put_with_version_mode() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "trait_path");

    // Write 3 versions via trait with Append
    for v in 1..=3u64 {
        Storage::put_with_version_mode(
            &store,
            key.clone(),
            Value::Int(v as i64),
            v,
            None,
            WriteMode::Append,
        )
        .unwrap();
    }
    assert_eq!(store.get_history(&key, None, None).unwrap().len(), 3);

    // Write via trait with KeepLast(1)
    Storage::put_with_version_mode(
        &store,
        key.clone(),
        Value::Int(99),
        4,
        None,
        WriteMode::KeepLast(1),
    )
    .unwrap();

    let history = store.get_history(&key, None, None).unwrap();
    assert_eq!(
        history.len(),
        1,
        "KeepLast(1) via Storage trait should prune to 1 version"
    );
    assert_eq!(history[0].value, Value::Int(99));
}

#[test]
fn test_keep_last_preserves_newest_versions() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    store.set_max_versions_per_key(3);

    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "newest_check");

    for v in 1..=5u64 {
        store
            .put(
                key.clone(),
                create_stored_value(Value::Int(v as i64 * 100), v),
                WriteMode::Append,
            )
            .unwrap();
    }

    // Latest should be v5
    let latest = store.get_versioned(&key, u64::MAX).unwrap().unwrap();
    assert_eq!(latest.value, Value::Int(500));
    assert_eq!(latest.version.as_u64(), 5);

    // Snapshot at v4 should still see v4 (kept)
    let at_v4 = store.get_versioned(&key, 4).unwrap().unwrap();
    assert_eq!(at_v4.value, Value::Int(400));

    // Snapshot at v3 should still see v3 (kept)
    let at_v3 = store.get_versioned(&key, 3).unwrap().unwrap();
    assert_eq!(at_v3.value, Value::Int(300));

    // Snapshot at v2 should see nothing (pruned)
    assert!(
        store.get_versioned(&key, 2).unwrap().is_none(),
        "version 2 should be pruned by max_versions=3"
    );
}

#[test]
fn test_set_max_versions_per_key_dynamic() {
    use strata_core::traits::Storage;
    use strata_core::value::Value;

    let store = ShardedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "dynamic_limit");

    // Write 5 versions with no limit
    for v in 1..=5u64 {
        store
            .put(
                key.clone(),
                create_stored_value(Value::Int(v as i64), v),
                WriteMode::Append,
            )
            .unwrap();
    }
    assert_eq!(store.get_history(&key, None, None).unwrap().len(), 5);

    // Now set global limit to 2 — existing data is NOT retroactively pruned
    store.set_max_versions_per_key(2);

    // But the next write WILL prune
    store
        .put(
            key.clone(),
            create_stored_value(Value::Int(60), 6),
            WriteMode::Append,
        )
        .unwrap();

    let history = store.get_history(&key, None, None).unwrap();
    assert_eq!(
        history.len(),
        2,
        "after set_max_versions_per_key(2), next Append write should prune to 2"
    );
    assert_eq!(history[0].value, Value::Int(60));
    assert_eq!(history[1].value, Value::Int(5));
}
