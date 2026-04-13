use super::*;

// ===== Basic Storage trait tests =====

#[test]
fn put_then_get() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(42), 1);
    let result = store
        .get_versioned(&kv_key("k"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(42));
    assert_eq!(result.version.as_u64(), 1);
}

#[test]
fn get_nonexistent_returns_none() {
    let store = SegmentedStore::new();
    assert!(store
        .get_versioned(&kv_key("k"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn delete_creates_tombstone() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(42), 1);
    store
        .delete_with_version(&kv_key("k"), CommitVersion(2))
        .unwrap();
    assert!(store
        .get_versioned(&kv_key("k"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn versioned_read_respects_snapshot() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(10), 1);
    seed(&store, kv_key("k"), Value::Int(20), 2);
    seed(&store, kv_key("k"), Value::Int(30), 3);

    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(1))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(2))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(20)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(3))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(30)
    );
    assert!(store
        .get_versioned(&kv_key("k"), CommitVersion(0))
        .unwrap()
        .is_none());
}

#[test]
fn tombstone_snapshot_isolation() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(10), 1);
    store
        .delete_with_version(&kv_key("k"), CommitVersion(2))
        .unwrap();
    seed(&store, kv_key("k"), Value::Int(30), 3);

    // Snapshot at 1: see original value
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(1))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
    // Snapshot at 2: tombstone → None
    assert!(store
        .get_versioned(&kv_key("k"), CommitVersion(2))
        .unwrap()
        .is_none());
    // Snapshot at 3: see re-written value
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(3))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(30)
    );
}

#[test]
fn get_history_returns_newest_first() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    seed(&store, kv_key("k"), Value::Int(2), 2);
    seed(&store, kv_key("k"), Value::Int(3), 3);

    let history = store.get_history(&kv_key("k"), None, None).unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].value, Value::Int(3));
    assert_eq!(history[1].value, Value::Int(2));
    assert_eq!(history[2].value, Value::Int(1));
}

#[test]
fn get_history_with_limit() {
    let store = SegmentedStore::new();
    for i in 1..=10 {
        seed(&store, kv_key("k"), Value::Int(i), i as u64);
    }
    let history = store.get_history(&kv_key("k"), Some(3), None).unwrap();
    assert_eq!(history.len(), 3);
    // Must be the 3 newest versions
    assert_eq!(history[0].value, Value::Int(10));
    assert_eq!(history[1].value, Value::Int(9));
    assert_eq!(history[2].value, Value::Int(8));
}

#[test]
fn get_history_with_before_version() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    seed(&store, kv_key("k"), Value::Int(2), 2);
    seed(&store, kv_key("k"), Value::Int(3), 3);

    let history = store
        .get_history(&kv_key("k"), None, Some(CommitVersion(3)))
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].value, Value::Int(2));
}

#[test]
fn get_history_includes_tombstones() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store
        .delete_with_version(&kv_key("k"), CommitVersion(2))
        .unwrap();
    seed(&store, kv_key("k"), Value::Int(3), 3);

    let history = store.get_history(&kv_key("k"), None, None).unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].value, Value::Int(3));
    // Tombstone at v2 appears as Value::Null
    assert_eq!(history[1].value, Value::Null);
    assert_eq!(history[1].version.as_u64(), 2);
    assert_eq!(history[2].value, Value::Int(1));
}

#[test]
fn scan_prefix_returns_matching_keys() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("user/alice"), Value::Int(1), 1);
    seed(&store, kv_key("user/bob"), Value::Int(2), 2);
    seed(&store, kv_key("config/x"), Value::Int(3), 3);

    let prefix = Key::new(ns(), TypeTag::KV, "user/".as_bytes().to_vec());
    let results = store.scan_prefix(&prefix, CommitVersion::MAX).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn scan_prefix_filters_tombstones() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k1"), Value::Int(1), 1);
    seed(&store, kv_key("k2"), Value::Int(2), 2);
    store
        .delete_with_version(&kv_key("k1"), CommitVersion(3))
        .unwrap();

    let prefix = Key::new(ns(), TypeTag::KV, "k".as_bytes().to_vec());
    let results = store.scan_prefix(&prefix, CommitVersion::MAX).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1.value, Value::Int(2));
}

#[test]
fn scan_prefix_mvcc_snapshot() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k1"), Value::Int(10), 1);
    seed(&store, kv_key("k1"), Value::Int(20), 3);
    seed(&store, kv_key("k2"), Value::Int(30), 2);

    let prefix = Key::new(ns(), TypeTag::KV, "k".as_bytes().to_vec());

    // Snapshot at 2: k1@1 and k2@2
    let results = store.scan_prefix(&prefix, CommitVersion(2)).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1.value, Value::Int(10)); // k1@1
    assert_eq!(results[1].1.value, Value::Int(30)); // k2@2
}

#[test]
fn current_version_tracks_writes() {
    let store = SegmentedStore::new();
    assert_eq!(store.current_version(), CommitVersion(0));
    seed(&store, kv_key("k"), Value::Int(1), 5);
    assert!(store.current_version() >= CommitVersion(5));
}

#[test]
fn version_next_version_set_version() {
    let store = SegmentedStore::new();
    assert_eq!(store.version(), 0);
    assert_eq!(store.next_version(), 1);
    assert_eq!(store.version(), 1);
    store.set_version(CommitVersion(100));
    assert_eq!(store.version(), 100);
}

#[test]
#[should_panic(expected = "version counter overflow")]
fn test_issue_1718_next_version_overflow_panics() {
    // M-11: next_version() must detect u64::MAX overflow instead of
    // silently wrapping to 0, which would corrupt MVCC ordering.
    // This is consistent with TransactionManager::allocate_version()
    // which returns Err(CounterOverflow) at u64::MAX.
    let store = SegmentedStore::new();
    store.set_version(CommitVersion(u64::MAX - 1));
    // Advances from MAX-1 to MAX — should succeed
    assert_eq!(store.next_version(), u64::MAX);
    // Advances from MAX — should panic (overflow)
    store.next_version();
}

#[test]
fn branch_ids_and_clear() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    assert_eq!(store.branch_ids().len(), 1);
    assert!(store.clear_branch(&branch()));
    assert!(store.branch_ids().is_empty());
    assert!(!store.clear_branch(&branch())); // already cleared
}

#[test]
fn branch_entry_count() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 2);
    seed(&store, kv_key("a"), Value::Int(3), 3); // overwrites a
    assert_eq!(store.branch_entry_count(&branch()), 2); // a, b
}

#[test]
fn list_branch_returns_live_entries() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 2);
    store
        .delete_with_version(&kv_key("a"), CommitVersion(3))
        .unwrap();

    let entries = store.list_branch(&branch());
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1.value, Value::Int(2));
}

#[test]
fn multiple_branches_isolated() {
    let b1 = BranchId::from_bytes([1; 16]);
    let b2 = BranchId::from_bytes([2; 16]);
    let ns1 = Arc::new(Namespace::new(b1, "default".to_string()));
    let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));
    let k1 = Key::new(ns1, TypeTag::KV, "k".as_bytes().to_vec());
    let k2 = Key::new(ns2, TypeTag::KV, "k".as_bytes().to_vec());

    let store = SegmentedStore::new();
    store
        .put_with_version_mode(
            k1.clone(),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            k2.clone(),
            Value::Int(2),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();

    assert_eq!(
        store
            .get_versioned(&k1, CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );
    assert_eq!(
        store
            .get_versioned(&k2, CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert_eq!(store.branch_ids().len(), 2);
}

#[test]
fn compaction_does_not_cross_branches() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    let b1 = BranchId::from_bytes([1; 16]);
    let b2 = BranchId::from_bytes([2; 16]);
    let ns1 = Arc::new(Namespace::new(b1, "default".to_string()));
    let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));

    let key_b1 = |name: &str| Key::new(ns1.clone(), TypeTag::KV, name.as_bytes().to_vec());
    let key_b2 = |name: &str| Key::new(ns2.clone(), TypeTag::KV, name.as_bytes().to_vec());

    // Write and flush data to both branches
    for i in 0..20u64 {
        store
            .put_with_version_mode(
                key_b1(&format!("k{:04}", i)),
                Value::Int(i as i64),
                CommitVersion(i + 1),
                None,
                WriteMode::Append,
            )
            .unwrap();
        store
            .put_with_version_mode(
                key_b2(&format!("k{:04}", i)),
                Value::Int((i as i64) * 100),
                CommitVersion(i + 1),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    store.rotate_memtable(&b1);
    store.rotate_memtable(&b2);
    store.flush_oldest_frozen(&b1).unwrap();
    store.flush_oldest_frozen(&b2).unwrap();

    assert_eq!(store.l0_segment_count(&b1), 1);
    assert_eq!(store.l0_segment_count(&b2), 1);

    // Compact only branch 1 L0→L1
    store.compact_l0_to_l1(&b1, CommitVersion(0)).unwrap();

    // Branch 1: compacted to L1
    assert_eq!(store.l0_segment_count(&b1), 0);
    assert!(store.l1_segment_count(&b1) > 0);

    // Branch 2: untouched — still in L0
    assert_eq!(store.l0_segment_count(&b2), 1);
    assert_eq!(store.l1_segment_count(&b2), 0);

    // Verify data integrity on both branches
    for i in 0..20u64 {
        let e1 = store
            .get_versioned(&key_b1(&format!("k{:04}", i)), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e1.value, Value::Int(i as i64));

        let e2 = store
            .get_versioned(&key_b2(&format!("k{:04}", i)), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e2.value, Value::Int((i as i64) * 100));
    }

    // Values are distinct: branch 1 stores i, branch 2 stores i*100
    // Verify a non-zero key to confirm no cross-contamination
    let e1 = store
        .get_versioned(&key_b1("k0005"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(e1.value, Value::Int(5), "branch 1 must have its own value");
    let e2 = store
        .get_versioned(&key_b2("k0005"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(
        e2.value,
        Value::Int(500),
        "branch 2 must have its own value"
    );
}

#[test]
fn ttl_expiration_at_read_time() {
    let store = SegmentedStore::new();
    // Insert with 1ms TTL using a timestamp from the past
    let branch_id = branch();
    let key = kv_key("ttl_key");

    let branch = store
        .branches
        .entry(branch_id)
        .or_insert_with(BranchState::new);

    let entry = MemtableEntry {
        value: Value::Int(42),
        is_tombstone: false,
        timestamp: Timestamp::from_micros(0), // ancient
        ttl_ms: 1,                            // 1ms TTL — definitely expired
        raw_value: None,
    };
    branch.active.put_entry(&key, CommitVersion(1), entry);
    drop(branch);

    // Should be expired
    assert!(store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn concurrent_readers_and_writer() {
    use std::sync::Arc;
    let store = Arc::new(SegmentedStore::new());

    // Seed some data
    for i in 0..100u64 {
        store
            .put_with_version_mode(
                kv_key(&format!("k{}", i)),
                Value::Int(i as i64),
                CommitVersion(i + 1),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }

    // Spawn readers
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let s = Arc::clone(&store);
            std::thread::spawn(move || {
                for i in 0..100u64 {
                    let result = s.get_versioned(&kv_key(&format!("k{}", i)), CommitVersion::MAX);
                    assert!(result.unwrap().is_some());
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

// ===== Missing coverage tests =====

#[test]
fn get_history_nonexistent_key() {
    let store = SegmentedStore::new();
    let history = store.get_history(&kv_key("nope"), None, None).unwrap();
    assert!(history.is_empty());
}

#[test]
fn get_version_only_existing() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(1), 5);
    seed(&store, kv_key("k"), Value::Int(2), 10);
    assert_eq!(
        store.get_version_only(&kv_key("k")).unwrap(),
        Some(CommitVersion(10))
    );
}

#[test]
fn get_version_only_nonexistent() {
    let store = SegmentedStore::new();
    assert_eq!(store.get_version_only(&kv_key("k")).unwrap(), None);
}

#[test]
fn get_version_only_tombstoned() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store
        .delete_with_version(&kv_key("k"), CommitVersion(2))
        .unwrap();
    assert_eq!(store.get_version_only(&kv_key("k")).unwrap(), None);
}

/// Issue #1700: WriteMode::KeepLast is a compaction-time retention hint, NOT
/// a write-time pruning directive.  The memtable always appends — pruning
/// happens only during compaction via `max_versions_per_key`.
#[test]
fn test_issue_1700_keep_last_does_not_prune_at_write_time() {
    let store = SegmentedStore::new();
    let key = kv_key("adj_list");

    // Write 3 versions with Append
    for v in 1..=3u64 {
        store
            .put_with_version_mode(
                key.clone(),
                Value::Int(v as i64),
                CommitVersion(v),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    assert_eq!(store.get_history(&key, None, None).unwrap().len(), 3);

    // Write a 4th version with KeepLast(1) — memtable must still keep all 4.
    store
        .put_with_version_mode(
            key.clone(),
            Value::Int(99),
            CommitVersion(4),
            None,
            WriteMode::KeepLast(1),
        )
        .unwrap();

    let history = store.get_history(&key, None, None).unwrap();
    assert_eq!(
        history.len(),
        4,
        "KeepLast(1) must not prune at write time; all 4 versions should be present"
    );
    // Latest version is the KeepLast write
    assert_eq!(history[0].value, Value::Int(99));
    assert_eq!(history[0].version.as_u64(), 4);
}

/// Issue #1700: apply_batch with KeepLast entries preserves all versions.
#[test]
fn test_issue_1700_apply_batch_keep_last_preserves_versions() {
    let store = SegmentedStore::new();
    let key = kv_key("batch_adj");

    // Seed 2 versions
    seed(&store, key.clone(), Value::Int(1), 1);
    seed(&store, key.clone(), Value::Int(2), 2);

    // Batch write with KeepLast(1)
    store
        .apply_batch(
            vec![(key.clone(), Value::Int(3), WriteMode::KeepLast(1))],
            CommitVersion(3),
        )
        .unwrap();

    let history = store.get_history(&key, None, None).unwrap();
    assert_eq!(
        history.len(),
        3,
        "apply_batch with KeepLast must not prune; all 3 versions should be present"
    );
}

#[test]
fn delete_nonexistent_key() {
    let store = SegmentedStore::new();
    // Deleting a key that never existed should succeed (creates tombstone)
    store
        .delete_with_version(&kv_key("ghost"), CommitVersion(1))
        .unwrap();
    assert!(store
        .get_versioned(&kv_key("ghost"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn scan_prefix_results_are_sorted() {
    let store = SegmentedStore::new();
    // Insert in reverse order to verify sorting
    seed(&store, kv_key("k3"), Value::Int(3), 3);
    seed(&store, kv_key("k1"), Value::Int(1), 1);
    seed(&store, kv_key("k2"), Value::Int(2), 2);

    let prefix = Key::new(ns(), TypeTag::KV, "k".as_bytes().to_vec());
    let results = store.scan_prefix(&prefix, CommitVersion::MAX).unwrap();
    assert_eq!(results.len(), 3);
    // Must be sorted by key
    assert!(results[0].0 < results[1].0);
    assert!(results[1].0 < results[2].0);
}

#[test]
fn put_with_ttl_via_public_api() {
    let store = SegmentedStore::new();
    store
        .put_with_version_mode(
            kv_key("ttl"),
            Value::Int(1),
            CommitVersion(1),
            Some(Duration::from_secs(3600)), // 1 hour — should not expire
            WriteMode::Append,
        )
        .unwrap();
    // Should be readable (not expired yet)
    assert!(store
        .get_versioned(&kv_key("ttl"), CommitVersion::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn branch_entry_count_nonexistent_branch() {
    let store = SegmentedStore::new();
    assert_eq!(store.branch_entry_count(&BranchId::from_bytes([99; 16])), 0);
}

#[test]
fn list_branch_nonexistent_branch() {
    let store = SegmentedStore::new();
    assert!(store
        .list_branch(&BranchId::from_bytes([99; 16]))
        .is_empty());
}

/// Issue #1749: scan_prefix must return an error (not silently truncated results)
/// when a segment data block is corrupt.
#[test]
fn test_issue_1749_scan_prefix_returns_error_on_corruption() {
    use crate::segment_builder::HEADER_SIZE;

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Insert entries and flush to a segment on disk.
    seed(&store, kv_key("item_a"), Value::Int(1), 1);
    seed(&store, kv_key("item_b"), Value::Int(2), 2);
    seed(&store, kv_key("item_c"), Value::Int(3), 3);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    // Find the .sst file on disk.
    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let sst_files: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
        .collect();
    assert_eq!(sst_files.len(), 1, "expected exactly one segment file");
    let sst_path = sst_files[0].path();

    // Read the segment file and corrupt the first data block's CRC.
    // Frame layout: type(1) + codec(1) + reserved(2) + data_len(4) + data(N) + crc(4)
    let data = std::fs::read(&sst_path).unwrap();
    let data_len =
        u32::from_le_bytes(data[HEADER_SIZE + 4..HEADER_SIZE + 8].try_into().unwrap()) as usize;
    let crc_offset = HEADER_SIZE + 8 + data_len;
    let mut corrupt = data.clone();
    corrupt[crc_offset] ^= 0xFF; // flip a CRC byte
    std::fs::write(&sst_path, &corrupt).unwrap();

    // Re-open the store so it loads the corrupt segment file.
    drop(store);
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    store2.recover_segments().unwrap();

    // scan_prefix MUST return Err, not a silently truncated Vec.
    let prefix = Key::new(ns(), TypeTag::KV, "item_".as_bytes().to_vec());
    let result = store2.scan_prefix(&prefix, CommitVersion::MAX);
    assert!(
        result.is_err(),
        "scan_prefix must return Err on corrupt segment, got {} entries",
        result.unwrap().len(),
    );
}

/// Issue #1749: get_history must return an error on segment corruption.
#[test]
fn test_issue_1749_get_history_returns_error_on_corruption() {
    use crate::segment_builder::HEADER_SIZE;

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("mykey"), Value::Int(10), 1);
    seed(&store, kv_key("mykey"), Value::Int(20), 2);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let sst_files: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
        .collect();
    assert_eq!(sst_files.len(), 1);
    let sst_path = sst_files[0].path();

    let data = std::fs::read(&sst_path).unwrap();
    let data_len =
        u32::from_le_bytes(data[HEADER_SIZE + 4..HEADER_SIZE + 8].try_into().unwrap()) as usize;
    let crc_offset = HEADER_SIZE + 8 + data_len;
    let mut corrupt = data.clone();
    corrupt[crc_offset] ^= 0xFF;
    std::fs::write(&sst_path, &corrupt).unwrap();

    drop(store);
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    store2.recover_segments().unwrap();

    // get_history MUST return Err, not silently truncated results.
    let result = store2.get_history(&kv_key("mykey"), None, None);
    assert!(
        result.is_err(),
        "get_history must return Err on corrupt segment, got {} entries",
        result.unwrap().len(),
    );
}

// ===== count_prefix tests =====

#[test]
fn count_prefix_matches_scan_prefix() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 2);
    seed(&store, kv_key("c"), Value::Int(3), 3);

    let prefix = Key::new(ns(), TypeTag::KV, vec![]);
    let scan_count = store
        .scan_prefix(&prefix, CommitVersion::MAX)
        .unwrap()
        .len() as u64;
    let direct_count = store.count_prefix(&prefix, CommitVersion::MAX).unwrap();
    assert_eq!(scan_count, direct_count);
    assert_eq!(direct_count, 3);
}

#[test]
fn count_prefix_excludes_tombstones() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 2);
    seed(&store, kv_key("c"), Value::Int(3), 3);
    store
        .delete_with_version(&kv_key("b"), CommitVersion(4))
        .unwrap();

    let prefix = Key::new(ns(), TypeTag::KV, vec![]);
    let count = store.count_prefix(&prefix, CommitVersion::MAX).unwrap();
    assert_eq!(count, 2, "deleted key must not be counted");
}

#[test]
fn count_prefix_respects_version_snapshot() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k1"), Value::Int(10), 1);
    seed(&store, kv_key("k2"), Value::Int(20), 3);

    // At version 2, only k1 exists
    let prefix = Key::new(ns(), TypeTag::KV, vec![]);
    assert_eq!(store.count_prefix(&prefix, CommitVersion(2)).unwrap(), 1);
    // At version 3, both exist
    assert_eq!(store.count_prefix(&prefix, CommitVersion::MAX).unwrap(), 2);
}

#[test]
fn count_prefix_empty_store() {
    let store = SegmentedStore::new();
    let prefix = Key::new(ns(), TypeTag::KV, vec![]);
    assert_eq!(store.count_prefix(&prefix, CommitVersion::MAX).unwrap(), 0);
}

#[test]
fn count_prefix_with_key_filter() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("user:1"), Value::Int(1), 1);
    seed(&store, kv_key("user:2"), Value::Int(2), 2);
    seed(&store, kv_key("task:1"), Value::Int(3), 3);

    let user_prefix = Key::new(ns(), TypeTag::KV, "user:".as_bytes().to_vec());
    assert_eq!(
        store
            .count_prefix(&user_prefix, CommitVersion::MAX)
            .unwrap(),
        2
    );

    let task_prefix = Key::new(ns(), TypeTag::KV, "task:".as_bytes().to_vec());
    assert_eq!(
        store
            .count_prefix(&task_prefix, CommitVersion::MAX)
            .unwrap(),
        1
    );
}

// ===== BranchSnapshot tests =====

#[test]
fn snapshot_survives_rotation() {
    use crate::merge_iter::MvccIterator;

    let store = SegmentedStore::new();
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 2);
    seed(&store, kv_key("c"), Value::Int(3), 3);

    // Capture snapshot while keys are in active memtable
    let snapshot = store.snapshot_branch(&branch()).unwrap();

    // Rotate memtable and write new keys
    store.rotate_memtable(&branch());
    seed(&store, kv_key("d"), Value::Int(4), 4);

    // Snapshot should still see original keys (a, b, c) but NOT d
    let prefix = Key::new(ns(), TypeTag::KV, vec![]);
    let (merge, flags) =
        SegmentedStore::build_snapshot_merge_iter(&snapshot, &prefix, &prefix).unwrap();
    let mvcc = MvccIterator::new(merge, CommitVersion(3)); // snapshot at version 3
    let count = mvcc
        .filter(|(_, entry)| !entry.is_tombstone)
        .filter(|(ik, _)| ik.decode().is_some())
        .count();
    super::check_corruption(&flags).unwrap();
    assert_eq!(count, 3, "snapshot should see a, b, c");
}

#[test]
fn snapshot_branch_returns_none_for_missing() {
    let store = SegmentedStore::new();
    let missing = BranchId::from_bytes([99; 16]);
    assert!(store.snapshot_branch(&missing).is_none());
}

// ===== StorageIterator tests =====

#[test]
fn storage_iterator_seek_next() {
    let store = SegmentedStore::new();
    for i in 0..10u64 {
        seed(
            &store,
            kv_key(&format!("key_{:02}", i)),
            Value::Int(i as i64),
            i + 1,
        );
    }

    let prefix = Key::new(ns(), TypeTag::KV, vec![]);
    let mut iter = store
        .new_storage_iterator(&branch(), prefix, CommitVersion::MAX)
        .unwrap();

    // Seek to key_05
    iter.seek(&kv_key("key_05")).unwrap();
    let mut keys = Vec::new();
    while let Some((key, _)) = iter.next() {
        keys.push(key.user_key_string().unwrap());
    }
    iter.check_corruption().unwrap();
    assert_eq!(keys, vec!["key_05", "key_06", "key_07", "key_08", "key_09"]);
}

#[test]
fn storage_iterator_reseek() {
    let store = SegmentedStore::new();
    for c in ['a', 'b', 'c', 'd', 'e'] {
        let v = c as i64;
        seed(&store, kv_key(&c.to_string()), Value::Int(v), v as u64);
    }

    let prefix = Key::new(ns(), TypeTag::KV, vec![]);
    let mut iter = store
        .new_storage_iterator(&branch(), prefix, CommitVersion::MAX)
        .unwrap();

    // First seek to "c", read 2
    iter.seek(&kv_key("c")).unwrap();
    let k1 = iter.next().unwrap().0.user_key_string().unwrap();
    let k2 = iter.next().unwrap().0.user_key_string().unwrap();
    assert_eq!(k1, "c");
    assert_eq!(k2, "d");

    // Re-seek to "a", should restart from beginning
    iter.seek(&kv_key("a")).unwrap();
    let k3 = iter.next().unwrap().0.user_key_string().unwrap();
    assert_eq!(k3, "a");
    iter.check_corruption().unwrap();
}

#[test]
fn storage_iterator_seek_past_end() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 2);

    let prefix = Key::new(ns(), TypeTag::KV, vec![]);
    let mut iter = store
        .new_storage_iterator(&branch(), prefix, CommitVersion::MAX)
        .unwrap();

    iter.seek(&kv_key("z")).unwrap();
    assert!(iter.next().is_none(), "seek past end should yield nothing");
    iter.check_corruption().unwrap();
}

#[test]
fn storage_iterator_pagination() {
    let store = SegmentedStore::new();
    // 15 keys: key_00 through key_14
    for i in 0..15u64 {
        seed(
            &store,
            kv_key(&format!("key_{:02}", i)),
            Value::Int(i as i64),
            i + 1,
        );
    }

    let prefix = Key::new(ns(), TypeTag::KV, vec![]);
    let mut iter = store
        .new_storage_iterator(&branch(), prefix, CommitVersion::MAX)
        .unwrap();

    // Paginate: 5 pages of 3 entries
    let mut all_keys = Vec::new();
    let mut cursor = kv_key("");
    for _ in 0..5 {
        iter.seek(&cursor).unwrap();
        let mut page = Vec::new();
        for _ in 0..3 {
            if let Some((key, _)) = iter.next() {
                page.push(key);
            }
        }
        if page.is_empty() {
            break;
        }
        // Next cursor is one past the last key in this page.
        // For simplicity, use the last key's user_key + "\0" as next cursor.
        let last = page.last().unwrap();
        let mut next_user_key = last.user_key.to_vec();
        next_user_key.push(0);
        cursor = Key::new_kv(ns(), next_user_key);
        all_keys.extend(page.iter().filter_map(|k| k.user_key_string()));
    }
    iter.check_corruption().unwrap();
    assert_eq!(all_keys.len(), 15, "should paginate through all 15 keys");
    assert_eq!(all_keys[0], "key_00");
    assert_eq!(all_keys[14], "key_14");
}
