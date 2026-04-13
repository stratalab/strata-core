use super::*;

// ========================================================================
// Batch apply tests (Epic 8b)
// ========================================================================

#[test]
fn apply_batch_equivalent_to_individual() {
    let store = SegmentedStore::new();
    let _b = branch();

    // Write some keys individually
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 1);

    // Write same keys via apply_batch
    let store2 = SegmentedStore::new();
    let writes = vec![
        (kv_key("a"), Value::Int(1), WriteMode::Append),
        (kv_key("b"), Value::Int(2), WriteMode::Append),
    ];
    store2.apply_batch(writes, CommitVersion(1)).unwrap();

    // Both stores should produce the same results
    for key_name in &["a", "b"] {
        let k = kv_key(key_name);
        let v1 = store
            .get_versioned(&k, CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value;
        let v2 = store2
            .get_versioned(&k, CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value;
        assert_eq!(v1, v2);
    }
}

#[test]
fn apply_batch_cross_branch() {
    let store = SegmentedStore::new();
    let b1 = BranchId::from_bytes([1; 16]);
    let b2 = BranchId::from_bytes([2; 16]);
    let ns1 = Arc::new(Namespace::new(b1, "default".to_string()));
    let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));

    let k1 = Key::new(ns1, TypeTag::KV, b"x".to_vec());
    let k2 = Key::new(ns2, TypeTag::KV, b"y".to_vec());

    let writes = vec![
        (k1.clone(), Value::Int(10), WriteMode::Append),
        (k2.clone(), Value::Int(20), WriteMode::Append),
    ];
    store.apply_batch(writes, CommitVersion(5)).unwrap();

    assert_eq!(
        store
            .get_versioned(&k1, CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
    assert_eq!(
        store
            .get_versioned(&k2, CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(20)
    );
}

#[test]
fn apply_batch_with_deletes() {
    let store = SegmentedStore::new();
    let _b = branch();

    // Write first
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 1);

    // Delete via batch
    let deletes = vec![kv_key("a")];
    store.delete_batch(deletes, CommitVersion(2)).unwrap();

    assert!(store
        .get_versioned(&kv_key("a"), CommitVersion::MAX)
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
}

#[test]
fn apply_batch_empty() {
    let store = SegmentedStore::new();
    store.apply_batch(vec![], CommitVersion(1)).unwrap();
    store.delete_batch(vec![], CommitVersion(1)).unwrap();
    assert_eq!(store.current_version(), CommitVersion(0));
}

// ========================================================================
// Issue #1706: apply_writes_atomic does not advance version until all
// puts AND deletes are installed.
// ========================================================================

#[test]
fn test_issue_1706_apply_writes_atomic_no_partial_visibility() {
    // Setup: K1 and K2 exist at version 1.
    let store = SegmentedStore::new();
    seed(&store, kv_key("K1"), Value::Int(1), 1);
    seed(&store, kv_key("K2"), Value::Int(2), 1);
    assert_eq!(store.version(), 1);

    // Transaction at version 10: put K1="new", delete K2.
    let writes = vec![(kv_key("K1"), Value::from("new"), WriteMode::Append)];
    let deletes = vec![kv_key("K2")];

    // apply_writes_atomic should install everything BEFORE advancing version.
    store
        .apply_writes_atomic(writes, deletes, CommitVersion(10), &[])
        .unwrap();

    // After the atomic call, version should be 10.
    assert_eq!(store.version(), 10);

    // A reader at version 10 must see BOTH the put AND the delete.
    // K1 should be "new" at version 10.
    let k1 = store
        .get_versioned(&kv_key("K1"), CommitVersion(10))
        .unwrap()
        .unwrap();
    assert_eq!(k1.value, Value::from("new"));

    // K2 should be deleted (tombstone) at version 10 — returns None.
    let k2 = store
        .get_versioned(&kv_key("K2"), CommitVersion(10))
        .unwrap();
    assert!(k2.is_none(), "K2 should be deleted at version 10");

    // A reader at version 9 (before the commit) must see old state.
    let k1_old = store
        .get_versioned(&kv_key("K1"), CommitVersion(9))
        .unwrap()
        .unwrap();
    assert_eq!(k1_old.value, Value::Int(1));
    let k2_old = store
        .get_versioned(&kv_key("K2"), CommitVersion(9))
        .unwrap()
        .unwrap();
    assert_eq!(k2_old.value, Value::Int(2));
}

#[test]
fn test_issue_1706_version_not_advanced_before_deletes_installed() {
    // This test verifies the core invariant: version must not be visible
    // while deletes are still pending.
    //
    // With the OLD split apply_batch/delete_batch, apply_batch alone
    // advances the version. With the fix, only apply_writes_atomic
    // advances it after ALL entries are installed.
    let store = SegmentedStore::new();
    seed(&store, kv_key("A"), Value::Int(100), 1);

    // Simulate a transaction that both puts and deletes.
    let writes = vec![(kv_key("B"), Value::Int(200), WriteMode::Append)];
    let deletes = vec![kv_key("A")];

    store
        .apply_writes_atomic(writes, deletes, CommitVersion(5), &[])
        .unwrap();

    // Version advanced to 5 only after both writes and deletes are in.
    assert_eq!(store.version(), 5);

    // Reader at version 5: B exists, A is deleted.
    assert!(store
        .get_versioned(&kv_key("B"), CommitVersion(5))
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("A"), CommitVersion(5))
        .unwrap()
        .is_none());
}

#[test]
fn test_issue_1706_apply_writes_atomic_empty() {
    let store = SegmentedStore::new();
    // Both empty — should not advance version.
    store
        .apply_writes_atomic(vec![], vec![], CommitVersion(5), &[])
        .unwrap();
    assert_eq!(store.version(), 0);

    // Only writes, no deletes.
    let writes = vec![(kv_key("X"), Value::Int(1), WriteMode::Append)];
    store
        .apply_writes_atomic(writes, vec![], CommitVersion(3), &[])
        .unwrap();
    assert_eq!(store.version(), 3);

    // Only deletes, no writes.
    let deletes = vec![kv_key("X")];
    store
        .apply_writes_atomic(vec![], deletes, CommitVersion(7), &[])
        .unwrap();
    assert_eq!(store.version(), 7);
    assert!(store
        .get_versioned(&kv_key("X"), CommitVersion(7))
        .unwrap()
        .is_none());
}

// ========================================================================
// Bulk load mode tests (Epic 8d)
// ========================================================================

#[test]
fn bulk_load_data_readable() {
    let store = SegmentedStore::new();
    let b = branch();

    store.begin_bulk_load(&b);
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 2);

    // Data should be readable even during bulk load
    assert_eq!(
        store
            .get_versioned(&kv_key("a"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );

    store.end_bulk_load(&b).unwrap();

    assert_eq!(
        store
            .get_versioned(&kv_key("b"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
}

#[test]
fn bulk_load_defers_rotation() {
    let dir = tempfile::tempdir().unwrap();
    // Tiny write buffer (64 bytes) — normally would rotate after 1 entry
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 64);
    let b = branch();

    store.begin_bulk_load(&b);
    assert!(store.is_bulk_loading(&b));

    // Write many entries — should NOT rotate during bulk load
    for i in 0..100 {
        seed(
            &store,
            kv_key(&format!("k{}", i)),
            Value::Int(i),
            (i + 1) as u64,
        );
    }

    // No frozen memtables — everything in active
    assert_eq!(store.branch_frozen_count(&b), 0);

    store.end_bulk_load(&b).unwrap();
    assert!(!store.is_bulk_loading(&b));

    // After end_bulk_load, data is still readable
    for i in 0..100 {
        assert!(store
            .get_versioned(&kv_key(&format!("k{}", i)), CommitVersion::MAX)
            .unwrap()
            .is_some());
    }
}

#[test]
fn bulk_load_normal_writes_after() {
    let store = SegmentedStore::new();
    let b = branch();

    store.begin_bulk_load(&b);
    seed(&store, kv_key("bulk"), Value::Int(1), 1);
    store.end_bulk_load(&b).unwrap();

    // Normal writes should work after bulk load
    seed(&store, kv_key("normal"), Value::Int(2), 2);

    assert_eq!(
        store
            .get_versioned(&kv_key("bulk"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("normal"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
}

// ===== Engine-facing API tests =====

#[test]
fn get_value_direct_returns_latest() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(10), 1);
    seed(&store, kv_key("k"), Value::Int(20), 2);
    assert_eq!(
        store.get_value_direct(&kv_key("k")).unwrap(),
        Some(Value::Int(20))
    );
}

#[test]
fn get_value_direct_skips_tombstone() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(10), 1);
    store
        .delete_with_version(&kv_key("k"), CommitVersion(2))
        .unwrap();
    assert_eq!(store.get_value_direct(&kv_key("k")).unwrap(), None);
}

#[test]
fn get_value_direct_nonexistent() {
    let store = SegmentedStore::new();
    assert_eq!(store.get_value_direct(&kv_key("k")).unwrap(), None);
}

#[test]
fn get_versioned_direct_returns_latest() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(10), 1);
    seed(&store, kv_key("k"), Value::Int(20), 2);
    let vv = store.get_versioned_direct(&kv_key("k")).unwrap().unwrap();
    assert_eq!(vv.value, Value::Int(20));
    assert_eq!(vv.version, strata_core::Version::txn(2));
}

#[test]
fn get_versioned_direct_skips_tombstone() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(10), 1);
    store
        .delete_with_version(&kv_key("k"), CommitVersion(2))
        .unwrap();
    assert!(store.get_versioned_direct(&kv_key("k")).unwrap().is_none());
}

#[test]
fn get_versioned_direct_nonexistent() {
    let store = SegmentedStore::new();
    assert!(store.get_versioned_direct(&kv_key("k")).unwrap().is_none());
}

#[test]
fn get_versioned_direct_preserves_timestamp() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::String("hello".into()), 5);
    let vv = store.get_versioned_direct(&kv_key("k")).unwrap().unwrap();
    assert_eq!(vv.value, Value::String("hello".into()));
    assert_eq!(vv.version, strata_core::Version::txn(5));
    // Timestamp should be non-zero (set by Memtable::put)
    assert!(vv.timestamp > 0.into());
}

#[test]
fn get_versioned_direct_moves_bytes_value() {
    let store = SegmentedStore::new();
    let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    seed(&store, kv_key("bin"), Value::Bytes(data.clone()), 3);
    let vv = store.get_versioned_direct(&kv_key("bin")).unwrap().unwrap();
    assert_eq!(vv.value, Value::Bytes(data));
    assert_eq!(vv.version, strata_core::Version::txn(3));
}

mod estimate_num_keys {
    use super::*;

    #[test]
    fn tracks_inserts() {
        let store = SegmentedStore::new();
        assert_eq!(
            store.count_prefix(&kv_key(""), CommitVersion::MAX).unwrap(),
            0
        );
        seed(&store, kv_key("a"), Value::Int(1), 1);
        seed(&store, kv_key("b"), Value::Int(2), 2);
        seed(&store, kv_key("c"), Value::Int(3), 3);
        assert_eq!(
            store.count_prefix(&kv_key(""), CommitVersion::MAX).unwrap(),
            3
        );
    }

    #[test]
    fn accounts_for_deletes() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("a"), Value::Int(1), 1);
        seed(&store, kv_key("b"), Value::Int(2), 2);
        assert_eq!(
            store.count_prefix(&kv_key(""), CommitVersion::MAX).unwrap(),
            2
        );
        store
            .delete_with_version(&kv_key("a"), CommitVersion(3))
            .unwrap();
        // estimate = 2 entries - 1 deletion = 1
        assert_eq!(
            store.count_prefix(&kv_key(""), CommitVersion::MAX).unwrap(),
            1
        );
    }

    #[test]
    fn does_not_underflow() {
        let store = SegmentedStore::new();
        // Delete without any prior insert
        store
            .delete_with_version(&kv_key("ghost"), CommitVersion(1))
            .unwrap();
        assert_eq!(
            store.count_prefix(&kv_key(""), CommitVersion::MAX).unwrap(),
            0
        );
    }

    #[test]
    fn prefix_query_uses_exact_iterator() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("apple"), Value::Int(1), 1);
        seed(&store, kv_key("apricot"), Value::Int(2), 2);
        seed(&store, kv_key("banana"), Value::Int(3), 3);
        // Unprefixed: uses O(1) estimate
        assert_eq!(
            store.count_prefix(&kv_key(""), CommitVersion::MAX).unwrap(),
            3
        );
        // Prefixed: uses exact O(N) iterator
        assert_eq!(
            store
                .count_prefix(&kv_key("ap"), CommitVersion::MAX)
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .count_prefix(&kv_key("b"), CommitVersion::MAX)
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .count_prefix(&kv_key("z"), CommitVersion::MAX)
                .unwrap(),
            0
        );
    }
}

#[test]
fn list_by_type_filters_correctly() {
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k1"), Value::Int(1), 1);
    let json_key = Key::new(ns(), TypeTag::Json, "d1".as_bytes().to_vec());
    seed(&store, json_key, Value::Int(2), 2);

    let kv_entries = store.list_by_type(&b, TypeTag::KV);
    assert_eq!(kv_entries.len(), 1);
    assert_eq!(kv_entries[0].1.value, Value::Int(1));

    let json_entries = store.list_by_type(&b, TypeTag::Json);
    assert_eq!(json_entries.len(), 1);
    assert_eq!(json_entries[0].1.value, Value::Int(2));
}

#[test]
fn get_at_timestamp_sees_old_version() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(10), 1);
    // Record timestamp after first write
    let ts_after_v1 = Timestamp::now().as_micros();
    // Small sleep to ensure timestamp ordering
    std::thread::sleep(std::time::Duration::from_millis(5));
    seed(&store, kv_key("k"), Value::Int(20), 2);

    // Query at ts_after_v1 should see v1 (Int(10))
    let result = store.get_at_timestamp(&kv_key("k"), ts_after_v1).unwrap();
    assert!(
        result.is_some(),
        "should find version at snapshot timestamp"
    );
    assert_eq!(result.unwrap().value, Value::Int(10));

    // Query at current time should see v2 (Int(20))
    let result_now = store
        .get_at_timestamp(&kv_key("k"), Timestamp::now().as_micros())
        .unwrap();
    assert_eq!(result_now.unwrap().value, Value::Int(20));
}

#[test]
fn get_at_timestamp_nonexistent_branch() {
    let store = SegmentedStore::new();
    let result = store.get_at_timestamp(&kv_key("k"), u64::MAX).unwrap();
    assert!(result.is_none());
}

#[test]
fn get_at_timestamp_respects_tombstone() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(10), 1);
    std::thread::sleep(std::time::Duration::from_millis(5));
    store
        .delete_with_version(&kv_key("k"), CommitVersion(2))
        .unwrap();

    // Query at current time should return None (tombstone)
    let result = store
        .get_at_timestamp(&kv_key("k"), Timestamp::now().as_micros())
        .unwrap();
    assert!(result.is_none());
}

#[test]
fn scan_prefix_at_timestamp_filters() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("user:1"), Value::Int(1), 1);
    let ts_after = Timestamp::now().as_micros();
    std::thread::sleep(std::time::Duration::from_millis(5));
    seed(&store, kv_key("user:2"), Value::Int(2), 2);

    // At ts_after, only user:1 should be visible
    let prefix = kv_key("user:");
    let results = store.scan_prefix_at_timestamp(&prefix, ts_after).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1.value, Value::Int(1));
}

#[test]
fn time_range_returns_min_max() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("a"), Value::Int(1), 1);
    std::thread::sleep(std::time::Duration::from_millis(5));
    seed(&store, kv_key("b"), Value::Int(2), 2);

    let range = store.time_range(branch()).unwrap();
    assert!(range.is_some());
    let (min_ts, max_ts) = range.unwrap();
    assert!(min_ts <= max_ts);
    assert!(min_ts > 0);
}

#[test]
fn time_range_empty_branch() {
    let store = SegmentedStore::new();
    assert!(store.time_range(branch()).unwrap().is_none());
}

#[test]
fn gc_branch_is_noop() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    assert_eq!(store.gc_branch(&branch(), CommitVersion(100)), 0);
}

#[test]
fn set_max_branches_stores_value() {
    let store = SegmentedStore::new();
    store.set_max_branches(42);
    assert_eq!(store.max_branches.load(Ordering::Relaxed), 42);
}

#[test]
fn memory_stats_basic() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k1"), Value::Int(1), 1);
    seed(&store, kv_key("k2"), Value::Int(2), 2);

    let stats = store.memory_stats();
    assert_eq!(stats.total_branches, 1);
    assert_eq!(stats.total_entries, 2);
    assert!(stats.estimated_bytes > 0);
    assert_eq!(stats.per_branch.len(), 1);
    assert_eq!(stats.per_branch[0].entry_count, 2);
}

#[test]
fn shard_stats_detailed_counts() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    seed(&store, kv_key("k"), Value::Int(2), 2);

    let (entries, versions, btree) = store.shard_stats_detailed(&branch()).unwrap();
    assert_eq!(entries, 1, "1 logical key");
    assert_eq!(versions, 2, "2 versions of that key");
    assert!(!btree);
}

#[test]
fn shard_stats_detailed_missing_branch() {
    let store = SegmentedStore::new();
    assert!(store.shard_stats_detailed(&BranchId::new()).is_none());
}

// ===== Write stalling / backpressure tests =====

#[test]
fn write_stalling_skips_rotation_when_frozen_limit_reached() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().join("segments"), 1024); // 1KB threshold
    store.set_max_immutable_memtables(2);

    let bid = branch();

    // Write enough to trigger rotation 3 times (well over 1KB each time)
    for round in 0..3 {
        for i in 0..20 {
            let key_name = format!("round{}_{}", round, i);
            seed(
                &store,
                kv_key(&key_name),
                Value::String("x".repeat(100)),
                (round * 20 + i + 1) as u64,
            );
        }
    }

    // Should have exactly 2 frozen (the limit), 3rd rotation was blocked
    assert_eq!(store.branch_frozen_count(&bid), 2);

    // Flush one frozen memtable
    store.flush_oldest_frozen(&bid).unwrap();

    // Now rotation should work again on next write batch
    for i in 0..20 {
        seed(
            &store,
            kv_key(&format!("after_{}", i)),
            Value::String("y".repeat(100)),
            100 + i as u64,
        );
    }

    // Data should still be readable
    assert!(store
        .get_versioned(&kv_key("round0_0"), CommitVersion::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn write_stalling_disabled_when_max_is_zero() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().join("segments"), 1024);
    store.set_max_immutable_memtables(0); // unlimited

    let _bid = branch();

    // Write enough to trigger many rotations
    for i in 0..100 {
        seed(
            &store,
            kv_key(&format!("k{}", i)),
            Value::String("x".repeat(100)),
            i as u64 + 1,
        );
    }

    // Should have rotated freely — more than 2 frozen
    assert!(store.branch_frozen_count(&branch()) > 2);
}
