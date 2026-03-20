use super::compaction::*;
use super::*;
use strata_core::types::{Namespace, TypeTag};

fn branch() -> BranchId {
    BranchId::from_bytes([1; 16])
}

fn ns() -> Arc<Namespace> {
    Arc::new(Namespace::new(branch(), "default".to_string()))
}

fn kv_key(name: &str) -> Key {
    Key::new(ns(), TypeTag::KV, name.as_bytes().to_vec())
}

fn seed(store: &SegmentedStore, key: Key, value: Value, version: u64) {
    store
        .put_with_version_mode(key, value, version, None, WriteMode::Append)
        .unwrap();
}

// ===== Basic Storage trait tests =====

#[test]
fn put_then_get() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(42), 1);
    let result = store
        .get_versioned(&kv_key("k"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(42));
    assert_eq!(result.version.as_u64(), 1);
}

#[test]
fn get_nonexistent_returns_none() {
    let store = SegmentedStore::new();
    assert!(store
        .get_versioned(&kv_key("k"), u64::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn delete_creates_tombstone() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(42), 1);
    store.delete_with_version(&kv_key("k"), 2).unwrap();
    assert!(store
        .get_versioned(&kv_key("k"), u64::MAX)
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
        store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
        Value::Int(10)
    );
    assert_eq!(
        store.get_versioned(&kv_key("k"), 2).unwrap().unwrap().value,
        Value::Int(20)
    );
    assert_eq!(
        store.get_versioned(&kv_key("k"), 3).unwrap().unwrap().value,
        Value::Int(30)
    );
    assert!(store.get_versioned(&kv_key("k"), 0).unwrap().is_none());
}

#[test]
fn tombstone_snapshot_isolation() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(10), 1);
    store.delete_with_version(&kv_key("k"), 2).unwrap();
    seed(&store, kv_key("k"), Value::Int(30), 3);

    // Snapshot at 1: see original value
    assert_eq!(
        store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
        Value::Int(10)
    );
    // Snapshot at 2: tombstone → None
    assert!(store.get_versioned(&kv_key("k"), 2).unwrap().is_none());
    // Snapshot at 3: see re-written value
    assert_eq!(
        store.get_versioned(&kv_key("k"), 3).unwrap().unwrap().value,
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

    let history = store.get_history(&kv_key("k"), None, Some(3)).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].value, Value::Int(2));
}

#[test]
fn get_history_includes_tombstones() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.delete_with_version(&kv_key("k"), 2).unwrap();
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
    let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn scan_prefix_filters_tombstones() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k1"), Value::Int(1), 1);
    seed(&store, kv_key("k2"), Value::Int(2), 2);
    store.delete_with_version(&kv_key("k1"), 3).unwrap();

    let prefix = Key::new(ns(), TypeTag::KV, "k".as_bytes().to_vec());
    let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
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
    let results = store.scan_prefix(&prefix, 2).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1.value, Value::Int(10)); // k1@1
    assert_eq!(results[1].1.value, Value::Int(30)); // k2@2
}

#[test]
fn current_version_tracks_writes() {
    let store = SegmentedStore::new();
    assert_eq!(store.current_version(), 0);
    seed(&store, kv_key("k"), Value::Int(1), 5);
    assert!(store.current_version() >= 5);
}

#[test]
fn version_next_version_set_version() {
    let store = SegmentedStore::new();
    assert_eq!(store.version(), 0);
    assert_eq!(store.next_version(), 1);
    assert_eq!(store.version(), 1);
    store.set_version(100);
    assert_eq!(store.version(), 100);
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
    store.delete_with_version(&kv_key("a"), 3).unwrap();

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
        .put_with_version_mode(k1.clone(), Value::Int(1), 1, None, WriteMode::Append)
        .unwrap();
    store
        .put_with_version_mode(k2.clone(), Value::Int(2), 2, None, WriteMode::Append)
        .unwrap();

    assert_eq!(
        store.get_versioned(&k1, u64::MAX).unwrap().unwrap().value,
        Value::Int(1)
    );
    assert_eq!(
        store.get_versioned(&k2, u64::MAX).unwrap().unwrap().value,
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
                i + 1,
                None,
                WriteMode::Append,
            )
            .unwrap();
        store
            .put_with_version_mode(
                key_b2(&format!("k{:04}", i)),
                Value::Int((i as i64) * 100),
                i + 1,
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
    store.compact_l0_to_l1(&b1, 0).unwrap();

    // Branch 1: compacted to L1
    assert_eq!(store.l0_segment_count(&b1), 0);
    assert!(store.l1_segment_count(&b1) > 0);

    // Branch 2: untouched — still in L0
    assert_eq!(store.l0_segment_count(&b2), 1);
    assert_eq!(store.l1_segment_count(&b2), 0);

    // Verify data integrity on both branches
    for i in 0..20u64 {
        let e1 = store
            .get_versioned(&key_b1(&format!("k{:04}", i)), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e1.value, Value::Int(i as i64));

        let e2 = store
            .get_versioned(&key_b2(&format!("k{:04}", i)), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(e2.value, Value::Int((i as i64) * 100));
    }

    // Values are distinct: branch 1 stores i, branch 2 stores i*100
    // Verify a non-zero key to confirm no cross-contamination
    let e1 = store
        .get_versioned(&key_b1("k0005"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(e1.value, Value::Int(5), "branch 1 must have its own value");
    let e2 = store
        .get_versioned(&key_b2("k0005"), u64::MAX)
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
    };
    branch.active.put_entry(&key, 1, entry);
    drop(branch);

    // Should be expired
    assert!(store.get_versioned(&key, u64::MAX).unwrap().is_none());
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
                i + 1,
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
                    let result = s.get_versioned(&kv_key(&format!("k{}", i)), u64::MAX);
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
    assert_eq!(store.get_version_only(&kv_key("k")).unwrap(), Some(10));
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
    store.delete_with_version(&kv_key("k"), 2).unwrap();
    assert_eq!(store.get_version_only(&kv_key("k")).unwrap(), None);
}

#[test]
fn delete_nonexistent_key() {
    let store = SegmentedStore::new();
    // Deleting a key that never existed should succeed (creates tombstone)
    store.delete_with_version(&kv_key("ghost"), 1).unwrap();
    assert!(store
        .get_versioned(&kv_key("ghost"), u64::MAX)
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
    let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
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
            1,
            Some(Duration::from_secs(3600)), // 1 hour — should not expire
            WriteMode::Append,
        )
        .unwrap();
    // Should be readable (not expired yet)
    assert!(store
        .get_versioned(&kv_key("ttl"), u64::MAX)
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

// ===== Flush pipeline tests =====

#[test]
fn flush_moves_data_to_segment() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    for i in 1..=100u64 {
        seed(
            &store,
            kv_key(&format!("k{:04}", i)),
            Value::Int(i as i64),
            i,
        );
    }

    assert!(store.rotate_memtable(&branch()));
    assert!(store.has_frozen(&branch()));
    assert_eq!(store.branch_frozen_count(&branch()), 1);

    let flushed = store.flush_oldest_frozen(&branch()).unwrap();
    assert!(flushed);
    assert_eq!(store.branch_frozen_count(&branch()), 0);
    assert_eq!(store.branch_segment_count(&branch()), 1);

    for i in 1..=100u64 {
        let result = store
            .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }
}

#[test]
fn flush_produces_valid_segment_file() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // Segments are now in a branch subdirectory
    let branch_hex = super::hex_encode_branch(&branch());
    let branch_dir = dir.path().join(&branch_hex);
    let sst_files: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "sst"))
        .collect();
    assert!(!sst_files.is_empty());

    let seg = crate::segment::KVSegment::open(&sst_files[0].path()).unwrap();
    assert_eq!(seg.entry_count(), 1);
}

#[test]
fn flush_empty_frozen_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("a"), Value::Int(1), 1);
    let flushed = store.flush_oldest_frozen(&branch()).unwrap();
    assert!(!flushed);
}

#[test]
fn flush_without_dir_is_noop() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    assert!(store.has_frozen(&branch()));

    let flushed = store.flush_oldest_frozen(&branch()).unwrap();
    assert!(!flushed);
}

#[test]
fn rotation_triggers_at_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 1);

    seed(&store, kv_key("k"), Value::Int(42), 1);

    assert!(
        store.branch_frozen_count(&branch()) >= 1,
        "rotation should have triggered at 1-byte threshold"
    );
}

#[test]
fn rotation_creates_fresh_active() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 1);

    seed(&store, kv_key("old"), Value::Int(1), 1);
    seed(&store, kv_key("new"), Value::Int(2), 2);

    assert_eq!(
        store
            .get_versioned(&kv_key("old"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1),
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("new"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2),
    );
}

#[test]
fn no_rotation_when_threshold_zero() {
    let store = SegmentedStore::new();

    for i in 1..=100u64 {
        seed(&store, kv_key(&format!("k{}", i)), Value::Int(i as i64), i);
    }

    assert_eq!(store.branch_frozen_count(&branch()), 0);
}

#[test]
fn mvcc_correct_across_flush() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    seed(&store, kv_key("k"), Value::Int(2), 2);

    assert_eq!(
        store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
        Value::Int(1),
    );
    assert_eq!(
        store.get_versioned(&kv_key("k"), 2).unwrap().unwrap().value,
        Value::Int(2),
    );
}

#[test]
fn prefix_scan_spans_memtable_and_segment() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("item/a"), Value::Int(1), 1);
    seed(&store, kv_key("item/b"), Value::Int(2), 2);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    seed(&store, kv_key("item/c"), Value::Int(3), 3);
    seed(&store, kv_key("item/d"), Value::Int(4), 4);

    let prefix = Key::new(ns(), TypeTag::KV, "item/".as_bytes().to_vec());
    let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
    assert_eq!(results.len(), 4);
}

#[test]
fn get_history_spans_memtable_and_segment() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    seed(&store, kv_key("k"), Value::Int(2), 2);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    seed(&store, kv_key("k"), Value::Int(3), 3);

    let history = store.get_history(&kv_key("k"), None, None).unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].value, Value::Int(3));
    assert_eq!(history[1].value, Value::Int(2));
    assert_eq!(history[2].value, Value::Int(1));
}

#[test]
fn multiple_flushes_produce_multiple_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    for cycle in 0..3u64 {
        let base = cycle * 10 + 1;
        for i in 0..10u64 {
            seed(
                &store,
                kv_key(&format!("c{}k{}", cycle, i)),
                Value::Int((base + i) as i64),
                base + i,
            );
        }
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();
    }

    assert_eq!(store.branch_segment_count(&branch()), 3);

    for cycle in 0..3u64 {
        let base = cycle * 10 + 1;
        for i in 0..10u64 {
            let result = store
                .get_versioned(&kv_key(&format!("c{}k{}", cycle, i)), u64::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(result.value, Value::Int((base + i) as i64));
        }
    }
}

#[test]
fn newest_segment_wins_for_same_key() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    seed(&store, kv_key("k"), Value::Int(2), 2);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    assert_eq!(store.branch_segment_count(&branch()), 2);

    let result = store
        .get_versioned(&kv_key("k"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(2));
}

#[test]
fn reads_dont_block_during_flush() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    for i in 1..=50u64 {
        store
            .put_with_version_mode(
                kv_key(&format!("k{:04}", i)),
                Value::Int(i as i64),
                i,
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    store.rotate_memtable(&branch());

    let store_reader = Arc::clone(&store);
    let reader = std::thread::spawn(move || {
        for i in 1..=50u64 {
            let result = store_reader
                .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
                .unwrap();
            assert!(result.is_some(), "key k{:04} should be readable", i);
        }
    });

    store.flush_oldest_frozen(&branch()).unwrap();
    reader.join().unwrap();
}

#[test]
fn rotate_nonexistent_branch_returns_false() {
    let store = SegmentedStore::new();
    assert!(!store.rotate_memtable(&branch()));
}

#[test]
fn flush_nonexistent_branch_returns_false() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let flushed = store.flush_oldest_frozen(&branch()).unwrap();
    assert!(!flushed);
}

#[test]
fn delete_across_flush_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    store.delete_with_version(&kv_key("k"), 2).unwrap();

    assert!(store.get_versioned(&kv_key("k"), 2).unwrap().is_none());
    assert_eq!(
        store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
        Value::Int(1),
    );
}

#[test]
fn tombstone_survives_flush() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write value then delete, so the frozen memtable contains both
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.delete_with_version(&kv_key("k"), 2).unwrap();
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // Tombstone must survive the flush — key is deleted at snapshot 2
    assert!(store.get_versioned(&kv_key("k"), 2).unwrap().is_none());
    // Value is still visible at snapshot 1
    assert_eq!(
        store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
        Value::Int(1),
    );
    // History shows both versions from the segment
    let history = store.get_history(&kv_key("k"), None, None).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].value, Value::Null); // tombstone at v2
    assert_eq!(history[1].value, Value::Int(1)); // value at v1
}

// ===== Recovery tests =====

#[test]
fn recover_segments_loads_flushed_data() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    for i in 1..=50u64 {
        seed(
            &store,
            kv_key(&format!("k{:04}", i)),
            Value::Int(i as i64),
            i,
        );
    }
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.branches_recovered, 1);
    assert_eq!(info.segments_loaded, 1);
    assert_eq!(info.errors_skipped, 0);

    for i in 1..=50u64 {
        let result = store2
            .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }
}

#[test]
fn recover_segments_multiple_branches() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    let b1 = BranchId::from_bytes([1; 16]);
    let b2 = BranchId::from_bytes([2; 16]);
    let ns1 = Arc::new(Namespace::new(b1, "default".to_string()));
    let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));

    for i in 1..=10u64 {
        let key = Key::new(ns1.clone(), TypeTag::KV, format!("k{}", i).into_bytes());
        store
            .put_with_version_mode(key, Value::Int(i as i64), i, None, WriteMode::Append)
            .unwrap();
    }
    store.rotate_memtable(&b1);
    store.flush_oldest_frozen(&b1).unwrap();

    for i in 11..=20u64 {
        let key = Key::new(ns2.clone(), TypeTag::KV, format!("k{}", i).into_bytes());
        store
            .put_with_version_mode(key, Value::Int(i as i64), i, None, WriteMode::Append)
            .unwrap();
    }
    store.rotate_memtable(&b2);
    store.flush_oldest_frozen(&b2).unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.branches_recovered, 2);
    assert_eq!(info.segments_loaded, 2);

    let k1 = Key::new(ns1, TypeTag::KV, "k1".as_bytes().to_vec());
    let k11 = Key::new(ns2, TypeTag::KV, "k11".as_bytes().to_vec());
    assert!(store2.get_versioned(&k1, u64::MAX).unwrap().is_some());
    assert!(store2.get_versioned(&k11, u64::MAX).unwrap().is_some());
}

#[test]
fn recover_segments_skips_corrupt_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let branch_hex = super::hex_encode_branch(&branch());
    let corrupt_path = dir.path().join(&branch_hex).join("corrupt.sst");
    std::fs::write(&corrupt_path, b"not a valid segment").unwrap();

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.segments_loaded, 1);
    assert_eq!(info.errors_skipped, 1);
    assert!(store2
        .get_versioned(&kv_key("k"), u64::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn recover_segments_empty_dir_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store.recover_segments().unwrap();
    assert_eq!(
        info,
        super::RecoverSegmentsInfo {
            branches_recovered: 0,
            segments_loaded: 0,
            errors_skipped: 0
        }
    );
}

#[test]
fn recover_segments_no_dir_is_noop() {
    let store = SegmentedStore::new();
    let info = store.recover_segments().unwrap();
    assert_eq!(info.branches_recovered, 0);
}

#[test]
fn recover_segments_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    for cycle in 0..3u64 {
        let base = cycle * 10 + 1;
        for i in 0..5u64 {
            seed(
                &store,
                kv_key(&format!("c{}k{}", cycle, i)),
                Value::Int((base + i) as i64),
                base + i,
            );
        }
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();
    }

    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.segments_loaded, 3);

    let branch = store2.branches.get(&branch()).unwrap();
    let ver = branch.version.load();
    assert!(ver.l0_segments()[0].commit_range().1 >= ver.l0_segments()[1].commit_range().1);
    assert!(ver.l0_segments()[1].commit_range().1 >= ver.l0_segments()[2].commit_range().1);
}

#[test]
fn max_flushed_commit_returns_correct_value() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    assert_eq!(store.max_flushed_commit(&branch()), None);

    seed(&store, kv_key("k1"), Value::Int(1), 5);
    seed(&store, kv_key("k2"), Value::Int(2), 10);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();
    assert_eq!(store.max_flushed_commit(&branch()), Some(10));

    seed(&store, kv_key("k3"), Value::Int(3), 20);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();
    assert_eq!(store.max_flushed_commit(&branch()), Some(20));
}

#[test]
fn flush_writes_to_branch_subdirectory() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    let branch_hex = super::hex_encode_branch(&branch());
    let branch_dir = dir.path().join(&branch_hex);
    assert!(branch_dir.exists());

    let sst_files: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|e| e.to_str()) == Some("sst"))
        .collect();
    assert_eq!(sst_files.len(), 1);
}

#[test]
fn frozen_memtable_reads_correct_order() {
    let store = SegmentedStore::new();

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    seed(&store, kv_key("k"), Value::Int(2), 2);
    store.rotate_memtable(&branch());
    seed(&store, kv_key("k"), Value::Int(3), 3);

    assert_eq!(store.branch_frozen_count(&branch()), 2);
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3),
    );
    assert_eq!(
        store.get_versioned(&kv_key("k"), 2).unwrap().unwrap().value,
        Value::Int(2),
    );
    assert_eq!(
        store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
        Value::Int(1),
    );
}

// ===== Compaction tests =====

#[test]
fn compact_merges_two_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed(&store, kv_key("b"), Value::Int(2), 2);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    assert_eq!(store.branch_segment_count(&b), 2);

    let result = store.compact_branch(&b, 0).unwrap().unwrap();
    assert_eq!(result.segments_merged, 2);
    assert_eq!(result.output_entries, 2);
    assert_eq!(result.entries_pruned, 0);
    assert_eq!(store.branch_segment_count(&b), 1);

    assert_eq!(
        store
            .get_versioned(&kv_key("a"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
}

#[test]
fn compact_merges_overlapping_versions() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed(&store, kv_key("k"), Value::Int(2), 2);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    let result = store.compact_branch(&b, 0).unwrap().unwrap();
    assert_eq!(result.output_entries, 2);
    assert_eq!(result.entries_pruned, 0);

    assert_eq!(
        store
            .get_versioned(&kv_key("k"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert_eq!(
        store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
        Value::Int(1)
    );
}

#[test]
fn compact_prunes_old_versions() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    for commit in 1..=3u64 {
        seed(&store, kv_key("k"), Value::Int(commit as i64), commit);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();
    }
    assert_eq!(store.branch_segment_count(&b), 3);

    // floor=3: commit 3 (above floor) + commit 2 (newest below floor) survive, commit 1 pruned
    let result = store.compact_branch(&b, 3).unwrap().unwrap();
    assert_eq!(result.segments_merged, 3);
    assert_eq!(result.output_entries, 2);
    assert_eq!(result.entries_pruned, 1);

    // Verify the correct versions survived
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
    assert_eq!(
        store.get_versioned(&kv_key("k"), 2).unwrap().unwrap().value,
        Value::Int(2)
    );
    // Version 1 was pruned — reading at snapshot 1 should return nothing
    assert!(store.get_versioned(&kv_key("k"), 1).unwrap().is_none());
}

#[test]
fn compact_removes_dead_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    store.delete_with_version(&kv_key("k"), 2).unwrap();
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    let result = store.compact_branch(&b, 5).unwrap().unwrap();
    assert_eq!(result.output_entries, 0);
    assert_eq!(result.entries_pruned, 2);
}

#[test]
fn compact_preserves_tombstone_above_floor() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    store.delete_with_version(&kv_key("k"), 3).unwrap();
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    let result = store.compact_branch(&b, 2).unwrap().unwrap();
    assert_eq!(result.output_entries, 2);
    assert_eq!(result.entries_pruned, 0);
}

#[test]
fn compact_noop_zero_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    assert!(store.compact_branch(&b, 0).unwrap().is_none());
}

#[test]
fn compact_noop_one_segment() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();
    assert_eq!(store.branch_segment_count(&b), 1);
    assert!(store.compact_branch(&b, 0).unwrap().is_none());
}

#[test]
fn compact_noop_ephemeral() {
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    assert!(store.compact_branch(&b, 0).unwrap().is_none());
}

#[test]
fn compact_deletes_old_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    for commit in 1..=3u64 {
        seed(&store, kv_key("k"), Value::Int(commit as i64), commit);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();
    }

    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let files_before: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
        .collect();
    assert_eq!(files_before.len(), 3);

    store.compact_branch(&b, 0).unwrap();

    let files_after: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
        .collect();
    assert_eq!(files_after.len(), 1);
}

#[test]
fn compact_reads_correct_after() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(10), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed(&store, kv_key("a"), Value::Int(2), 2);
    seed(&store, kv_key("c"), Value::Int(20), 2);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    store.compact_branch(&b, 0).unwrap();

    assert_eq!(
        store
            .get_versioned(&kv_key("a"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("c"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(20)
    );

    let prefix_key = Key::new(ns(), TypeTag::KV, Vec::new());
    let results = store.scan_prefix(&prefix_key, u64::MAX).unwrap();
    assert_eq!(results.len(), 3);

    let history = store.get_history(&kv_key("a"), None, None).unwrap();
    assert_eq!(history.len(), 2);
}

#[test]
fn compact_result_counts() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Two keys, multiple versions across segments
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(10), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed(&store, kv_key("a"), Value::Int(2), 2);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed(&store, kv_key("a"), Value::Int(3), 3);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    // 4 total entries: a@3, a@2, a@1, b@1
    // floor=3: a keeps 3 (above) + 2 (floor entry), prunes 1. b keeps 1 (floor entry).
    let result = store.compact_branch(&b, 3).unwrap().unwrap();
    assert_eq!(result.segments_merged, 3);
    assert_eq!(result.output_entries, 3); // a@3, a@2, b@1
    assert_eq!(result.entries_pruned, 1); // a@1

    // Verify reads are correct
    assert_eq!(
        store
            .get_versioned(&kv_key("a"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
    assert_eq!(
        store.get_versioned(&kv_key("a"), 2).unwrap().unwrap().value,
        Value::Int(2)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
}

// ===== Memory pressure tests =====

#[test]
fn total_memtable_bytes_empty() {
    let store = SegmentedStore::new();
    assert_eq!(store.total_memtable_bytes(), 0);
}

#[test]
fn total_memtable_bytes_active_only() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    assert!(store.total_memtable_bytes() > 0);
}

#[test]
fn total_memtable_bytes_includes_frozen() {
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    let before_rotate = store.total_memtable_bytes();
    store.rotate_memtable(&b);
    seed(&store, kv_key("k2"), Value::Int(2), 2);
    let after = store.total_memtable_bytes();
    assert!(after > before_rotate);
}

#[test]
fn total_memtable_bytes_multiple_branches() {
    let store = SegmentedStore::new();
    let b1 = branch();
    let b2 = BranchId::from_bytes([2; 16]);
    let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));

    seed(&store, kv_key("a"), Value::Int(1), 1);
    let bytes1 = store.total_memtable_bytes();
    assert!(bytes1 > 0);

    seed(
        &store,
        Key::new(ns2, TypeTag::KV, b"x".to_vec()),
        Value::Int(2),
        2,
    );
    let bytes2 = store.total_memtable_bytes();
    assert!(bytes2 > bytes1);

    let b1_bytes = store.branches.get(&b1).unwrap().active.approx_bytes();
    let b2_bytes = store.branches.get(&b2).unwrap().active.approx_bytes();
    assert_eq!(bytes2, b1_bytes + b2_bytes);
}

#[test]
fn pressure_level_with_disabled() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::String("x".repeat(10000)), 1);
    assert_eq!(store.pressure_level(), PressureLevel::Normal);
}

#[test]
fn pressure_level_tracks_growth() {
    use crate::pressure::MemoryPressure;
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir_and_pressure(
        dir.path().to_path_buf(),
        0,
        MemoryPressure::new(1000, 0.7, 0.9),
    );

    assert_eq!(store.pressure_level(), PressureLevel::Normal);

    for i in 0..20u64 {
        seed(
            &store,
            kv_key(&format!("key_{}", i)),
            Value::String("x".repeat(30)),
            i + 1,
        );
    }
    let level = store.pressure_level();
    assert!(
        level >= PressureLevel::Warning,
        "expected at least Warning, got {:?}",
        level
    );
}

#[test]
fn branches_needing_flush_prioritization() {
    let store = SegmentedStore::new();
    let b1 = branch();
    let b2 = BranchId::from_bytes([2; 16]);
    let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));

    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&b1);

    for i in 0..3u64 {
        seed(
            &store,
            Key::new(ns2.clone(), TypeTag::KV, format!("k{}", i).into_bytes()),
            Value::Int(i as i64),
            i + 1,
        );
        store.rotate_memtable(&b2);
    }

    let needing = store.branches_needing_flush();
    assert_eq!(needing.len(), 2);
    assert_eq!(needing[0], b2);
    assert_eq!(needing[1], b1);
}

#[test]
fn should_compact_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    assert!(!store.should_compact(&b, 2));

    seed(&store, kv_key("b"), Value::Int(2), 2);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    assert!(store.should_compact(&b, 2));
}

#[test]
fn compact_after_flush_integration() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    for commit in 1..=3u64 {
        seed(
            &store,
            kv_key(&format!("k{}", commit)),
            Value::Int(commit as i64),
            commit,
        );
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();
    }

    assert!(store.should_compact(&b, 3));

    store.compact_branch(&b, 0).unwrap();
    assert_eq!(store.branch_segment_count(&b), 1);
    assert!(!store.should_compact(&b, 2));

    for commit in 1..=3u64 {
        assert_eq!(
            store
                .get_versioned(&kv_key(&format!("k{}", commit)), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(commit as i64),
        );
    }
}

#[test]
fn compact_with_active_memtable_data() {
    // Verify memtable data coexists correctly with compacted segments.
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create 2 segments
    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed(&store, kv_key("b"), Value::Int(2), 2);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    // Write to active memtable (NOT flushed)
    seed(&store, kv_key("c"), Value::Int(3), 3);
    // Update "a" in memtable (newer version than segment)
    seed(&store, kv_key("a"), Value::Int(10), 4);

    // Compact segments — memtable data must survive
    store.compact_branch(&b, 0).unwrap();
    assert_eq!(store.branch_segment_count(&b), 1);

    // Memtable data visible
    assert_eq!(
        store
            .get_versioned(&kv_key("c"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
    // Memtable update shadows segment version
    assert_eq!(
        store
            .get_versioned(&kv_key("a"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
    // Old segment version still readable at old snapshot
    assert_eq!(
        store.get_versioned(&kv_key("a"), 1).unwrap().unwrap().value,
        Value::Int(1)
    );
    // Segment-only data still readable
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
}

#[test]
fn compact_concurrent_flush_preserves_new_segment() {
    // Simulate: compact snapshots 2 segments, then a flush adds a 3rd
    // segment before the swap. The 3rd segment must survive.
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create 2 segments
    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed(&store, kv_key("b"), Value::Int(2), 2);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    // Compact — this merges the 2 segments into 1
    store.compact_branch(&b, 0).unwrap();
    assert_eq!(store.branch_segment_count(&b), 1);

    // Now create 2 more segments and write a frozen memtable
    seed(&store, kv_key("c"), Value::Int(3), 3);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed(&store, kv_key("d"), Value::Int(4), 4);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    // Now we have 3 segments (1 from compaction + 2 new)
    assert_eq!(store.branch_segment_count(&b), 3);

    // Compact again
    store.compact_branch(&b, 0).unwrap();
    assert_eq!(store.branch_segment_count(&b), 1);

    // All data still readable
    assert_eq!(
        store
            .get_versioned(&kv_key("a"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("c"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("d"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(4)
    );
}

// ========================================================================
// Batch apply tests (Epic 8b)
// ========================================================================

#[test]
fn apply_batch_equivalent_to_individual() {
    let store = SegmentedStore::new();
    let b = branch();

    // Write some keys individually
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 1);

    // Write same keys via apply_batch
    let store2 = SegmentedStore::new();
    let writes = vec![
        (kv_key("a"), Value::Int(1), WriteMode::Append),
        (kv_key("b"), Value::Int(2), WriteMode::Append),
    ];
    store2.apply_batch(writes, 1).unwrap();

    // Both stores should produce the same results
    for key_name in &["a", "b"] {
        let k = kv_key(key_name);
        let v1 = store.get_versioned(&k, u64::MAX).unwrap().unwrap().value;
        let v2 = store2.get_versioned(&k, u64::MAX).unwrap().unwrap().value;
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
    store.apply_batch(writes, 5).unwrap();

    assert_eq!(
        store.get_versioned(&k1, u64::MAX).unwrap().unwrap().value,
        Value::Int(10)
    );
    assert_eq!(
        store.get_versioned(&k2, u64::MAX).unwrap().unwrap().value,
        Value::Int(20)
    );
}

#[test]
fn apply_batch_with_deletes() {
    let store = SegmentedStore::new();
    let b = branch();

    // Write first
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 1);

    // Delete via batch
    let deletes = vec![kv_key("a")];
    store.delete_batch(deletes, 2).unwrap();

    assert!(store
        .get_versioned(&kv_key("a"), u64::MAX)
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
}

#[test]
fn apply_batch_empty() {
    let store = SegmentedStore::new();
    store.apply_batch(vec![], 1).unwrap();
    store.delete_batch(vec![], 1).unwrap();
    assert_eq!(store.current_version(), 0);
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
            .get_versioned(&kv_key("a"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );

    store.end_bulk_load(&b).unwrap();

    assert_eq!(
        store
            .get_versioned(&kv_key("b"), u64::MAX)
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
            .get_versioned(&kv_key(&format!("k{}", i)), u64::MAX)
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
            .get_versioned(&kv_key("bulk"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("normal"), u64::MAX)
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
    assert_eq!(store.get_value_direct(&kv_key("k")), Some(Value::Int(20)));
}

#[test]
fn get_value_direct_skips_tombstone() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("k"), Value::Int(10), 1);
    store.delete_with_version(&kv_key("k"), 2).unwrap();
    assert_eq!(store.get_value_direct(&kv_key("k")), None);
}

#[test]
fn get_value_direct_nonexistent() {
    let store = SegmentedStore::new();
    assert_eq!(store.get_value_direct(&kv_key("k")), None);
}

#[test]
fn list_by_type_filters_correctly() {
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k1"), Value::Int(1), 1);
    let state_key = Key::new(ns(), TypeTag::State, "s1".as_bytes().to_vec());
    seed(&store, state_key, Value::Int(2), 2);

    let kv_entries = store.list_by_type(&b, TypeTag::KV);
    assert_eq!(kv_entries.len(), 1);
    assert_eq!(kv_entries[0].1.value, Value::Int(1));

    let state_entries = store.list_by_type(&b, TypeTag::State);
    assert_eq!(state_entries.len(), 1);
    assert_eq!(state_entries[0].1.value, Value::Int(2));
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
    store.delete_with_version(&kv_key("k"), 2).unwrap();

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
    assert_eq!(store.gc_branch(&branch(), 100), 0);
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
        .get_versioned(&kv_key("round0_0"), u64::MAX)
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

// ===== Tiered compaction tests =====

#[test]
fn segment_file_sizes_returns_correct_values() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // No segments → empty
    assert!(store.segment_file_sizes(&b).is_empty());

    // Create 3 segments with varying amounts of data
    for commit in 1..=3u64 {
        for i in 0..(commit * 10) {
            seed(
                &store,
                kv_key(&format!("c{}k{}", commit, i)),
                Value::Int(i as i64),
                commit * 100 + i,
            );
        }
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();
    }

    let sizes = store.segment_file_sizes(&b);
    assert_eq!(sizes.len(), 3);
    // All sizes should be non-zero
    for &sz in &sizes {
        assert!(sz > 0, "segment file size should be > 0");
    }
}

#[test]
fn compact_tier_merges_subset() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create 6 segments with distinct keys
    for commit in 1..=6u64 {
        seed(
            &store,
            kv_key(&format!("k{}", commit)),
            Value::Int(commit as i64),
            commit,
        );
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();
    }
    assert_eq!(store.branch_segment_count(&b), 6);

    // Compact first 4 segments (indices 0..4)
    let result = store.compact_tier(&b, &[0, 1, 2, 3], 0).unwrap().unwrap();
    assert_eq!(result.segments_merged, 4);
    assert_eq!(result.output_entries, 4);
    assert_eq!(result.entries_pruned, 0);

    // 2 untouched + 1 merged = 3 segments remaining
    assert_eq!(store.branch_segment_count(&b), 3);

    // All data still readable
    for commit in 1..=6u64 {
        let val = store
            .get_versioned(&kv_key(&format!("k{}", commit)), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(val.value, Value::Int(commit as i64));
    }
}

#[test]
fn compact_tier_too_few_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("k1"), Value::Int(1), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    // Only 1 index → Ok(None)
    assert!(store.compact_tier(&b, &[0], 0).unwrap().is_none());

    // Empty indices → Ok(None)
    assert!(store.compact_tier(&b, &[], 0).unwrap().is_none());
}

#[test]
fn compact_tier_ephemeral_returns_none() {
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    assert!(store.compact_tier(&b, &[0, 1], 0).unwrap().is_none());
}

#[test]
fn compact_tier_missing_branch_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = BranchId::from_bytes([99; 16]);
    assert!(store.compact_tier(&b, &[0, 1], 0).unwrap().is_none());
}

#[test]
fn compact_tier_prunes_versions() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create 3 segments with same key at different versions
    for commit in 1..=3u64 {
        seed(&store, kv_key("k"), Value::Int(commit as i64), commit);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();
    }

    // Compact all 3 with prune_floor=3
    // Above floor: commit 3. Below floor: commit 2 (newest below), commit 1 (pruned).
    let result = store.compact_tier(&b, &[0, 1, 2], 3).unwrap().unwrap();
    assert_eq!(result.segments_merged, 3);
    assert_eq!(result.output_entries, 2); // commit 3 + commit 2
    assert_eq!(result.entries_pruned, 1); // commit 1

    // Latest version still readable
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
}

// ===== O(1) time_range tests =====

#[test]
fn time_range_empty_branch_returns_none() {
    let store = SegmentedStore::new();
    assert_eq!(store.time_range(branch()).unwrap(), None);
}

#[test]
fn time_range_o1_tracks_writes() {
    let store = SegmentedStore::new();

    // No data -> None
    assert_eq!(store.time_range(branch()).unwrap(), None);

    // Write some data
    seed(&store, kv_key("a"), Value::Int(1), 1);
    seed(&store, kv_key("b"), Value::Int(2), 2);

    let range = store.time_range(branch()).unwrap().unwrap();
    // min and max should be close to now (within last second)
    assert!(range.0 > 0);
    assert!(range.1 >= range.0);
}

#[test]
fn time_range_o1_includes_deletes() {
    let store = SegmentedStore::new();
    seed(&store, kv_key("a"), Value::Int(1), 1);
    let range_before = store.time_range(branch()).unwrap().unwrap();

    // Short sleep to ensure delete timestamp is strictly later
    std::thread::sleep(std::time::Duration::from_millis(1));
    store.delete_with_version(&kv_key("a"), 2).unwrap();

    let range_after = store.time_range(branch()).unwrap().unwrap();
    // max should have advanced to include the delete timestamp
    assert!(range_after.1 >= range_before.1);
}

#[test]
fn time_range_o1_nonexistent_branch_returns_none() {
    let store = SegmentedStore::new();
    // Write to one branch, query another
    seed(&store, kv_key("a"), Value::Int(1), 1);
    let other_branch = BranchId::from_bytes([99; 16]);
    assert_eq!(store.time_range(other_branch).unwrap(), None);
}

#[test]
fn compact_tier_deletes_old_segment_files() {
    let dir = tempfile::tempdir().unwrap();
    let seg_dir = dir.path().join("segments");
    let store = SegmentedStore::with_dir(seg_dir.clone(), 4096);
    let b = branch();

    for i in 0..500u64 {
        seed(
            &store,
            kv_key(&format!("key_{:06}", i)),
            Value::String("x".repeat(80)),
            i + 1,
        );
    }
    while store.flush_oldest_frozen(&b).unwrap() {}
    assert!(store.branch_segment_count(&b) > 4);

    let sizes = store.segment_file_sizes(&b);
    let scheduler = crate::compaction::CompactionScheduler::default();
    let candidates = scheduler.pick_candidates(&sizes);
    store
        .compact_tier(&b, &candidates[0].segment_indices, 0)
        .unwrap();

    // SST files on disk should match in-memory segment count (no orphans)
    let branch_hex = format!("{:032x}", u128::from_be_bytes(*b.as_bytes()));
    let branch_dir = seg_dir.join(&branch_hex);
    let file_count = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .ok()
                .and_then(|e| e.path().extension().map(|ext| ext == "sst"))
                .unwrap_or(false)
        })
        .count();
    assert_eq!(file_count, store.branch_segment_count(&b));
}

#[test]
fn diagnostic_segment_layout_at_scale() {
    let dir = tempfile::tempdir().unwrap();
    // 4KB write buffer to force frequent rotation at small scale
    let store = SegmentedStore::with_dir(dir.path().join("segments"), 4096);
    let b = branch();

    // Write 1000 keys (~100 bytes each → ~100KB total → ~25 rotations)
    for i in 0..1000u64 {
        seed(
            &store,
            kv_key(&format!("key_{:06}", i)),
            Value::String("x".repeat(80)),
            i + 1,
        );
    }

    let frozen_before = store.branch_frozen_count(&b);
    // Flush all frozen
    while store.flush_oldest_frozen(&b).unwrap() {}
    let segments_after_flush = store.branch_segment_count(&b);
    let sizes_after_flush = store.segment_file_sizes(&b);

    eprintln!("=== Diagnostic: Segment Layout ===");
    eprintln!("Frozen before flush: {}", frozen_before);
    eprintln!("Segments after flush: {}", segments_after_flush);
    eprintln!("Segment sizes: {:?}", sizes_after_flush);

    // Now simulate what the engine does: tiered compaction
    let scheduler = crate::compaction::CompactionScheduler::default();
    let candidates = scheduler.pick_candidates(&sizes_after_flush);
    eprintln!("Compaction candidates: {} tiers eligible", candidates.len());
    for c in &candidates {
        eprintln!(
            "  Tier {}: {} segments (indices {:?})",
            c.tier,
            c.segment_indices.len(),
            c.segment_indices
        );
    }

    // Run tier compaction if available
    if let Some(candidate) = candidates.first() {
        let result = store
            .compact_tier(&b, &candidate.segment_indices, 0)
            .unwrap();
        eprintln!(
            "After tier compaction: segments={}, result={:?}",
            store.branch_segment_count(&b),
            result
        );
    }

    // Check final state
    let final_sizes = store.segment_file_sizes(&b);
    eprintln!("Final segments: {}", final_sizes.len());
    eprintln!("Final sizes: {:?}", final_sizes);

    // Verify data is intact
    for i in [0u64, 500, 999] {
        assert!(store
            .get_versioned(&kv_key(&format!("key_{:06}", i)), u64::MAX)
            .unwrap()
            .is_some());
    }

    // The real question: how many segments accumulate?
    // With proper compaction, should be O(log(N)) not O(N)
    assert!(
        final_sizes.len() <= 20,
        "Too many segments: {} (expected <= 20 for 1000 keys)",
        final_sizes.len()
    );
}

#[test]
fn l0_segment_count_tracks_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 64);
    let bid = branch();

    assert_eq!(store.l0_segment_count(&bid), 0);

    // Write enough data to trigger rotation, then flush.
    for i in 0..10 {
        seed(
            &store,
            kv_key(&format!("k{i}")),
            Value::Int(i as i64),
            (i + 1) as u64,
        );
    }
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    assert_eq!(store.l0_segment_count(&bid), 1);
    assert_eq!(store.branch_segment_count(&bid), 1);

    // Flush a second segment.
    for i in 10..20 {
        seed(
            &store,
            kv_key(&format!("k{i}")),
            Value::Int(i as i64),
            (i + 1) as u64,
        );
    }
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    assert_eq!(store.l0_segment_count(&bid), 2);
    assert_eq!(store.branch_segment_count(&bid), 2);
}

// ========================================================================
// L0 → L1 compaction tests (Epic 22)
// ========================================================================

/// Helper: create a store, write entries, flush to L0 segments.
fn flush_data(
    store: &SegmentedStore,
    b: &BranchId,
    keys: &[(&str, i64, u64)], // (key_name, value, commit)
) {
    for &(key_name, val, commit) in keys {
        seed(store, kv_key(key_name), Value::Int(val), commit);
    }
    store.rotate_memtable(b);
    store.flush_oldest_frozen(b).unwrap();
}

#[test]
fn compact_l0_to_l1_basic() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 1)]);
    flush_data(&store, &b, &[("c", 3, 2), ("d", 4, 2)]);
    flush_data(&store, &b, &[("e", 5, 3)]);

    assert_eq!(store.l0_segment_count(&b), 3);
    assert_eq!(store.l1_segment_count(&b), 0);

    let result = store.compact_l0_to_l1(&b, 0).unwrap().unwrap();
    assert_eq!(result.segments_merged, 3);
    assert_eq!(result.output_entries, 5);
    assert_eq!(result.entries_pruned, 0);

    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 1);
    assert_eq!(store.branch_segment_count(&b), 1);
}

#[test]
fn compact_l0_to_l1_merges_overlapping_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // First L0→L1: keys a,b
    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 1)]);
    store.compact_l0_to_l1(&b, 0).unwrap();
    assert_eq!(store.l1_segment_count(&b), 1);

    // New L0 with overlapping keys
    flush_data(&store, &b, &[("a", 10, 2), ("c", 3, 2)]);
    let result = store.compact_l0_to_l1(&b, 0).unwrap().unwrap();

    // 1 L0 + 1 overlapping L1 merged; prune_floor=0 keeps all versions
    assert_eq!(result.segments_merged, 2);
    assert_eq!(result.output_entries, 4); // a@2, a@1, b@1, c@2
    assert_eq!(result.entries_pruned, 0);

    // Overlapping L1 was merged — still just 1 L1 segment
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 1);

    // All data correct
    assert_eq!(
        store
            .get_versioned(&kv_key("a"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("c"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
}

#[test]
fn compact_l0_to_l1_preserves_non_overlapping_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // First L0→L1: keys x, y, z (high range)
    flush_data(&store, &b, &[("x", 1, 1), ("y", 2, 1), ("z", 3, 1)]);
    store.compact_l0_to_l1(&b, 0).unwrap();
    assert_eq!(store.l1_segment_count(&b), 1);

    // New L0 with disjoint keys a, b (low range, no overlap with x-z)
    flush_data(&store, &b, &[("a", 10, 2), ("b", 20, 2)]);
    let result = store.compact_l0_to_l1(&b, 0).unwrap().unwrap();

    // Only 1 L0 segment merged (no overlapping L1)
    assert_eq!(result.segments_merged, 1);
    assert_eq!(result.output_entries, 2);
    assert_eq!(result.entries_pruned, 0);

    // Non-overlapping L1 preserved + new L1 = 2 L1 segments
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 2);

    // All data correct
    for (key, val) in [("a", 10), ("b", 20), ("x", 1), ("y", 2), ("z", 3)] {
        assert_eq!(
            store
                .get_versioned(&kv_key(key), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(val),
            "key {key}"
        );
    }
}

#[test]
fn compact_l0_to_l1_prunes_versions() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("k", 1, 1)]);
    flush_data(&store, &b, &[("k", 2, 2)]);
    flush_data(&store, &b, &[("k", 3, 3)]);

    // prune_floor=3: keep v3 (above floor) + v2 (floor entry), prune v1
    let result = store.compact_l0_to_l1(&b, 3).unwrap().unwrap();
    assert_eq!(result.output_entries, 2);
    assert_eq!(result.entries_pruned, 1);

    assert_eq!(
        store
            .get_versioned(&kv_key("k"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
    assert_eq!(
        store.get_versioned(&kv_key("k"), 2).unwrap().unwrap().value,
        Value::Int(2)
    );
    assert!(store.get_versioned(&kv_key("k"), 1).unwrap().is_none());
}

#[test]
fn compact_l0_to_l1_empty_l0_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    // No rotation/flush → L0 is empty
    assert!(store.compact_l0_to_l1(&b, 0).unwrap().is_none());
}

#[test]
fn compact_l0_to_l1_ephemeral_returns_none() {
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    assert!(store.compact_l0_to_l1(&b, 0).unwrap().is_none());
}

#[test]
fn compact_l0_to_l1_deletes_old_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1)]);
    flush_data(&store, &b, &[("b", 2, 2)]);

    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let count_sst = || -> usize {
        std::fs::read_dir(&branch_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
            .count()
    };
    assert_eq!(count_sst(), 2);

    store.compact_l0_to_l1(&b, 0).unwrap();
    assert_eq!(count_sst(), 1); // 2 old deleted, 1 new created
}

#[test]
fn compact_l0_to_l1_concurrent_flush_preserves_new_l0() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1)]);
    flush_data(&store, &b, &[("b", 2, 2)]);

    // Compact L0→L1
    store.compact_l0_to_l1(&b, 0).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 1);

    // New L0 flush
    flush_data(&store, &b, &[("c", 3, 3)]);
    assert_eq!(store.l0_segment_count(&b), 1);

    // All data still readable
    assert!(store
        .get_versioned(&kv_key("a"), u64::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("b"), u64::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("c"), u64::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn compact_l0_to_l1_data_correct_after() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1), ("b", 10, 1)]);
    flush_data(&store, &b, &[("a", 2, 2), ("c", 20, 2)]);

    store.compact_l0_to_l1(&b, 0).unwrap();

    // Point lookups
    assert_eq!(
        store
            .get_versioned(&kv_key("a"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert_eq!(
        store.get_versioned(&kv_key("a"), 1).unwrap().unwrap().value,
        Value::Int(1)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("c"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(20)
    );

    // Prefix scan
    let prefix_key = Key::new(ns(), TypeTag::KV, Vec::new());
    let results = store.scan_prefix(&prefix_key, u64::MAX).unwrap();
    assert_eq!(results.len(), 3);

    // History
    let history = store.get_history(&kv_key("a"), None, None).unwrap();
    assert_eq!(history.len(), 2);
}

#[test]
fn compact_l0_to_l1_l1_sorted_invariant() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create two non-overlapping L1 segments
    flush_data(&store, &b, &[("x", 1, 1), ("y", 2, 1)]);
    store.compact_l0_to_l1(&b, 0).unwrap();

    flush_data(&store, &b, &[("a", 3, 2), ("b", 4, 2)]);
    store.compact_l0_to_l1(&b, 0).unwrap();

    assert_eq!(store.l1_segment_count(&b), 2);

    // Verify L1 segments are sorted by key range
    let branch_state = store.branches.get(&b).unwrap();
    let ver = branch_state.version.load();
    for i in 1..ver.l1_segments().len() {
        let prev_max = ver.l1_segments()[i - 1].key_range().1;
        let cur_min = ver.l1_segments()[i].key_range().0;
        assert!(
            prev_max <= cur_min,
            "L1 segments should be sorted by key range"
        );
    }
}

#[test]
fn compact_l0_to_l1_repeated() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // First round
    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 1)]);
    store.compact_l0_to_l1(&b, 0).unwrap();
    assert_eq!(store.l1_segment_count(&b), 1);

    // Second round — new L0 overlaps L1
    flush_data(&store, &b, &[("a", 10, 2), ("c", 3, 2)]);
    store.compact_l0_to_l1(&b, 0).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 1);

    // Third round — new disjoint L0
    flush_data(&store, &b, &[("z", 99, 3)]);
    store.compact_l0_to_l1(&b, 0).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 2);

    // All data correct
    assert_eq!(
        store
            .get_versioned(&kv_key("a"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("z"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(99)
    );
}

#[test]
fn compact_l0_to_l1_writes_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1)]);
    flush_data(&store, &b, &[("b", 2, 2)]);
    store.compact_l0_to_l1(&b, 0).unwrap();

    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let manifest = crate::manifest::read_manifest(&branch_dir).unwrap();
    assert!(manifest.is_some(), "manifest should exist after compaction");
    let manifest = manifest.unwrap();
    assert!(!manifest.entries.is_empty());

    // All entries should be L1 (no L0 left)
    for entry in &manifest.entries {
        assert_eq!(entry.level, 1, "all segments should be L1 after compaction");
    }
}

// ========================================================================
// Read path L1 tests
// ========================================================================

#[test]
fn point_lookup_reads_l1_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("k1", 1, 1), ("k2", 2, 2)]);
    store.compact_l0_to_l1(&b, 0).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 1);

    // Data in L1 only
    assert_eq!(
        store
            .get_versioned(&kv_key("k1"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("k2"), u64::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert!(store
        .get_versioned(&kv_key("nonexistent"), u64::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn scan_prefix_includes_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("item/a", 1, 1), ("item/b", 2, 1)]);
    store.compact_l0_to_l1(&b, 0).unwrap();

    // Add to memtable (L0)
    seed(&store, kv_key("item/c"), Value::Int(3), 2);

    let prefix = Key::new(ns(), TypeTag::KV, "item/".as_bytes().to_vec());
    let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
    assert_eq!(results.len(), 3); // 2 from L1 + 1 from memtable
}

#[test]
fn list_branch_includes_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 2)]);
    store.compact_l0_to_l1(&b, 0).unwrap();

    let entries = store.list_branch(&b);
    assert_eq!(entries.len(), 2);
}

#[test]
fn l1_binary_search_correct_segment() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create 3 non-overlapping L1 segments
    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 1)]);
    store.compact_l0_to_l1(&b, 0).unwrap();

    flush_data(&store, &b, &[("m", 3, 2), ("n", 4, 2)]);
    store.compact_l0_to_l1(&b, 0).unwrap();

    flush_data(&store, &b, &[("x", 5, 3), ("y", 6, 3)]);
    store.compact_l0_to_l1(&b, 0).unwrap();

    assert_eq!(store.l1_segment_count(&b), 3);

    // Each key should find the correct L1 segment
    for (key, val) in [("a", 1), ("b", 2), ("m", 3), ("n", 4), ("x", 5), ("y", 6)] {
        assert_eq!(
            store
                .get_versioned(&kv_key(key), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(val),
            "key {key}"
        );
    }

    // Keys not in any segment
    assert!(store
        .get_versioned(&kv_key("c"), u64::MAX)
        .unwrap()
        .is_none());
    assert!(store
        .get_versioned(&kv_key("p"), u64::MAX)
        .unwrap()
        .is_none());
    assert!(store
        .get_versioned(&kv_key("zzz"), u64::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn max_flushed_commit_includes_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 5), ("b", 2, 10)]);
    store.compact_l0_to_l1(&b, 0).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);

    // max_flushed_commit should still work with L1 only
    assert_eq!(store.max_flushed_commit(&b), Some(10));
}

#[test]
fn shard_stats_includes_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 2), ("c", 3, 3)]);
    store.compact_l0_to_l1(&b, 0).unwrap();

    let (entry_count, total_versions, _) = store.shard_stats_detailed(&b).unwrap();
    assert_eq!(entry_count, 3); // 3 live entries
    assert_eq!(total_versions, 3); // 3 versions in L1
}

// ========================================================================
// Recovery with manifest tests
// ========================================================================

#[test]
fn recover_with_manifest_restores_levels() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create L0 and compact to L1
    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 2)]);
    flush_data(&store, &b, &[("c", 3, 3)]);
    store.compact_l0_to_l1(&b, 0).unwrap();

    // Add another L0 segment
    flush_data(&store, &b, &[("d", 4, 4)]);

    assert_eq!(store.l0_segment_count(&b), 1);
    assert_eq!(store.l1_segment_count(&b), 1);

    // Recover into a fresh store
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.segments_loaded, 2);

    // Levels restored from manifest
    assert_eq!(store2.l0_segment_count(&b), 1);
    assert_eq!(store2.l1_segment_count(&b), 1);

    // All data correct
    for (key, val) in [("a", 1), ("b", 2), ("c", 3), ("d", 4)] {
        assert_eq!(
            store2
                .get_versioned(&kv_key(key), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(val),
            "key {key}"
        );
    }
}

#[test]
fn recover_without_manifest_all_l0() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1)]);
    flush_data(&store, &b, &[("b", 2, 2)]);

    // Delete the manifest file
    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let manifest_path = branch_dir.join("segments.manifest");
    if manifest_path.exists() {
        std::fs::remove_file(&manifest_path).unwrap();
    }

    // Recover — all segments go to L0 (backward compat)
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    store2.recover_segments().unwrap();

    assert_eq!(store2.l0_segment_count(&b), 2);
    assert_eq!(store2.l1_segment_count(&b), 0);

    // Data still correct
    assert!(store2
        .get_versioned(&kv_key("a"), u64::MAX)
        .unwrap()
        .is_some());
    assert!(store2
        .get_versioned(&kv_key("b"), u64::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn recover_manifest_corrupt_falls_back_to_l0() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1)]);
    store.compact_l0_to_l1(&b, 0).unwrap();

    // Corrupt the manifest
    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let manifest_path = branch_dir.join("segments.manifest");
    std::fs::write(&manifest_path, b"corrupted data!").unwrap();

    // Recover — corrupt manifest falls back to all-L0
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    store2.recover_segments().unwrap();

    assert_eq!(store2.l0_segment_count(&b), 1);
    assert_eq!(store2.l1_segment_count(&b), 0);

    // Data still correct
    assert!(store2
        .get_versioned(&kv_key("a"), u64::MAX)
        .unwrap()
        .is_some());
}

// ========================================================================
// Streaming compaction + multi-segment output tests (Epic 23)
// ========================================================================

#[test]
fn compact_l0_to_l1_streaming_correctness() {
    // Verify streaming compaction produces same results as before
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Write enough data across multiple flushes
    for batch in 0..5u64 {
        for i in 0..50u64 {
            let key = kv_key(&format!("key_{:06}", batch * 50 + i));
            seed(
                &store,
                key,
                Value::String(format!("val_{}", batch * 50 + i)),
                batch * 50 + i + 1,
            );
        }
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();
    }

    assert_eq!(store.l0_segment_count(&b), 5);
    store.compact_l0_to_l1(&b, 0).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert!(store.l1_segment_count(&b) >= 1);

    // All 250 entries should be readable
    for i in 0..250u64 {
        let result = store
            .get_versioned(&kv_key(&format!("key_{:06}", i)), u64::MAX)
            .unwrap();
        assert!(result.is_some(), "key_{:06} should be readable", i);
    }

    // Prefix scan should find all entries
    let prefix = Key::new(ns(), TypeTag::KV, "key_".as_bytes().to_vec());
    let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
    assert_eq!(results.len(), 250);
}

#[test]
fn compact_l0_to_l1_with_splitting_builder() {
    // Verify compact_l0_to_l1 works with the SplittingSegmentBuilder path.
    // With 400 entries × ~250B = ~100KB (well under 64MB target),
    // this produces 1 L1 segment. Splitting is tested directly in
    // segment_builder::tests::splitting_builder_respects_target_size.
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    for batch in 0..4u64 {
        for i in 0..100u64 {
            let key = kv_key(&format!("k{:06}", batch * 100 + i));
            seed(
                &store,
                key,
                Value::String("x".repeat(200)),
                batch * 100 + i + 1,
            );
        }
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();
    }

    assert_eq!(store.l0_segment_count(&b), 4);

    let result = store.compact_l0_to_l1(&b, 0).unwrap().unwrap();
    assert_eq!(result.output_entries, 400);
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 1);

    // All data correct
    for i in 0..400u64 {
        assert!(
            store
                .get_versioned(&kv_key(&format!("k{:06}", i)), u64::MAX)
                .unwrap()
                .is_some(),
            "k{:06} missing",
            i
        );
    }
}

// ========================================================================
// compact_level tests
// ========================================================================

#[test]
fn compact_level_0_equivalent_to_compact_l0_to_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 1)]);
    flush_data(&store, &b, &[("c", 3, 2), ("d", 4, 2)]);
    flush_data(&store, &b, &[("e", 5, 3)]);

    assert_eq!(store.l0_segment_count(&b), 3);

    let result = store.compact_level(&b, 0, 0).unwrap().unwrap();
    assert_eq!(result.segments_merged, 3);
    assert_eq!(result.output_entries, 5);
    assert_eq!(result.entries_pruned, 0);

    assert_eq!(store.l0_segment_count(&b), 0);
    assert!(store.l1_segment_count(&b) >= 1);

    // All data is findable with correct values
    let expected = [("a", 1i64), ("b", 2), ("c", 3), ("d", 4), ("e", 5)];
    for (key_name, expected_val) in &expected {
        let val = store
            .get_versioned(&kv_key(key_name), u64::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("key {} missing after compact_level(0)", key_name));
        assert_eq!(val.value, Value::Int(*expected_val));
    }
}

#[test]
fn compact_level_1_moves_to_l2() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create data in L1 via compact_level(0)
    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 2)]);
    flush_data(&store, &b, &[("c", 3, 3), ("d", 4, 4)]);
    store.compact_level(&b, 0, 0).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert!(store.l1_segment_count(&b) >= 1);

    // Now compact L1 → L2
    let result = store.compact_level(&b, 1, 0).unwrap().unwrap();
    assert_eq!(result.output_entries, 4);

    // L1 should be empty, L2 should have data
    assert_eq!(store.level_segment_count(&b, 1), 0);
    assert!(store.level_segment_count(&b, 2) >= 1);

    // All data still findable with correct values
    let expected = [("a", 1i64), ("b", 2), ("c", 3), ("d", 4)];
    for (key_name, expected_val) in &expected {
        let val = store
            .get_versioned(&kv_key(key_name), u64::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("key {} missing after compact_level(1)", key_name));
        assert_eq!(val.value, Value::Int(*expected_val));
    }
}

#[test]
fn compact_level_picks_round_robin() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create 3 L1 segments with distinct key ranges
    flush_data(&store, &b, &[("aaa", 1, 1)]);
    store.compact_level(&b, 0, 0).unwrap();
    flush_data(&store, &b, &[("mmm", 2, 2)]);
    store.compact_level(&b, 0, 0).unwrap();
    flush_data(&store, &b, &[("zzz", 3, 3)]);
    store.compact_level(&b, 0, 0).unwrap();

    assert_eq!(store.level_segment_count(&b, 1), 3);
    assert_eq!(store.level_segment_count(&b, 2), 0);

    // First compaction: picks file 0 (trivial move), pointer advances past "aaa"
    let r1 = store.compact_level(&b, 1, 0).unwrap().unwrap();
    assert_eq!(r1.segments_merged, 1);
    assert_eq!(store.level_segment_count(&b, 1), 2);
    assert_eq!(store.level_segment_count(&b, 2), 1);

    // Second compaction: picks file after pointer (should be "mmm")
    let r2 = store.compact_level(&b, 1, 0).unwrap().unwrap();
    assert_eq!(r2.segments_merged, 1);
    assert_eq!(store.level_segment_count(&b, 1), 1);
    assert_eq!(store.level_segment_count(&b, 2), 2);

    // Third compaction: picks remaining file ("zzz")
    let r3 = store.compact_level(&b, 1, 0).unwrap().unwrap();
    assert_eq!(r3.segments_merged, 1);
    assert_eq!(store.level_segment_count(&b, 1), 0);
    assert_eq!(store.level_segment_count(&b, 2), 3);

    // L1 empty, nothing to compact
    assert!(store.compact_level(&b, 1, 0).unwrap().is_none());

    // All data still findable
    for key_name in &["aaa", "mmm", "zzz"] {
        assert!(
            store
                .get_versioned(&kv_key(key_name), u64::MAX)
                .unwrap()
                .is_some(),
            "key {} missing after round-robin compact",
            key_name
        );
    }
}

#[test]
fn compact_level_trivial_move() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create 1 L1 segment with no overlap in L2
    flush_data(&store, &b, &[("x", 1, 1)]);
    store.compact_level(&b, 0, 0).unwrap();
    assert_eq!(store.level_segment_count(&b, 1), 1);
    assert_eq!(store.level_segment_count(&b, 2), 0);

    // Record L1 file size before move
    let l1_bytes_before = store.level_bytes(&b, 1);
    assert!(l1_bytes_before > 0);

    // Compact L1 → L2: should be a trivial move (1 input, 0 overlap)
    let result = store.compact_level(&b, 1, 0).unwrap().unwrap();
    assert_eq!(result.segments_merged, 1);
    assert_eq!(result.entries_pruned, 0);
    // Trivial move: output size equals input (no rewrite)
    assert_eq!(result.output_file_size, l1_bytes_before);

    assert_eq!(store.level_segment_count(&b, 1), 0);
    assert_eq!(store.level_segment_count(&b, 2), 1);
    // Bytes moved, not duplicated
    assert_eq!(store.level_bytes(&b, 1), 0);
    assert_eq!(store.level_bytes(&b, 2), l1_bytes_before);

    // Data still findable with correct value
    let val = store
        .get_versioned(&kv_key("x"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(1));
}

#[test]
fn compact_level_expands_l0_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create overlapping L0 segments
    flush_data(&store, &b, &[("a", 1, 1), ("c", 3, 1)]);
    flush_data(&store, &b, &[("b", 2, 2), ("d", 4, 2)]);
    assert_eq!(store.l0_segment_count(&b), 2);

    // compact_level(0) should expand to both L0 files
    let result = store.compact_level(&b, 0, 0).unwrap().unwrap();
    assert_eq!(result.segments_merged, 2); // both L0 segments included
    assert_eq!(result.output_entries, 4);
    assert_eq!(store.l0_segment_count(&b), 0);
}

#[test]
fn compact_level_finds_overlapping_next() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create L1 data covering "a" to "d"
    flush_data(
        &store,
        &b,
        &[("a", 1, 1), ("b", 2, 1), ("c", 3, 1), ("d", 4, 1)],
    );
    store.compact_level(&b, 0, 0).unwrap();

    // Flush new L0 data that overlaps with part of L1
    flush_data(&store, &b, &[("b", 20, 2)]);
    assert_eq!(store.l0_segment_count(&b), 1);

    // compact_level(0) should merge L0 with overlapping L1 segment
    let result = store.compact_level(&b, 0, 0).unwrap().unwrap();
    assert_eq!(result.segments_merged, 2); // 1 L0 + 1 L1
    assert_eq!(store.l0_segment_count(&b), 0);

    // Verify updated value for "b"
    let val = store
        .get_versioned(&kv_key("b"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(20));

    // Verify other keys survived the merge
    let expected = [("a", 1i64), ("c", 3), ("d", 4)];
    for (key_name, expected_val) in &expected {
        let val = store
            .get_versioned(&kv_key(key_name), u64::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("key {} missing after overlap merge", key_name));
        assert_eq!(val.value, Value::Int(*expected_val));
    }
}

#[test]
fn point_lookup_finds_data_in_l3() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Push data down to L3: flush → L0→L1 → L1→L2 → L2→L3
    flush_data(&store, &b, &[("deep", 42, 1)]);
    store.compact_level(&b, 0, 0).unwrap(); // L0 → L1
    store.compact_level(&b, 1, 0).unwrap(); // L1 → L2
    store.compact_level(&b, 2, 0).unwrap(); // L2 → L3

    assert_eq!(store.level_segment_count(&b, 0), 0);
    assert_eq!(store.level_segment_count(&b, 1), 0);
    assert_eq!(store.level_segment_count(&b, 2), 0);
    assert!(store.level_segment_count(&b, 3) >= 1);

    // Point lookup must find data at L3
    let val = store
        .get_versioned(&kv_key("deep"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(42));
}

#[test]
fn scan_includes_all_levels() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Put data at different levels
    // L2: "p/a"
    flush_data(&store, &b, &[("p/a", 1, 1)]);
    store.compact_level(&b, 0, 0).unwrap();
    store.compact_level(&b, 1, 0).unwrap();
    assert!(store.level_segment_count(&b, 2) >= 1);

    // L1: "p/b"
    flush_data(&store, &b, &[("p/b", 2, 2)]);
    store.compact_level(&b, 0, 0).unwrap();
    assert!(store.level_segment_count(&b, 1) >= 1);

    // L0: "p/c"
    flush_data(&store, &b, &[("p/c", 3, 3)]);
    assert!(store.l0_segment_count(&b) >= 1);

    // memtable: "p/d"
    seed(&store, kv_key("p/d"), Value::Int(4), 4);

    // Prefix scan should find all four
    let results = store.scan_prefix(&kv_key("p/"), u64::MAX).unwrap();
    assert_eq!(results.len(), 4, "scan should merge data across all levels");
}

#[test]
fn recover_restores_multi_level() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create data across multiple levels
    flush_data(&store, &b, &[("a", 1, 1)]);
    store.compact_level(&b, 0, 0).unwrap(); // L1
    flush_data(&store, &b, &[("b", 2, 2)]);
    store.compact_level(&b, 0, 0).unwrap(); // L1
    store.compact_level(&b, 1, 0).unwrap(); // L1 → L2
    flush_data(&store, &b, &[("c", 3, 3)]);
    store.compact_level(&b, 0, 0).unwrap(); // L1

    // Record exact level counts before recovery
    let l1_before = store.level_segment_count(&b, 1);
    let l2_before = store.level_segment_count(&b, 2);
    assert!(l1_before >= 1, "L1 should have segments");
    assert!(l2_before >= 1, "L2 should have segments");

    // Recover from fresh store
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert_eq!(info.segments_loaded, l1_before + l2_before);

    // Verify exact level counts restored
    assert_eq!(store2.level_segment_count(&b, 1), l1_before);
    assert_eq!(store2.level_segment_count(&b, 2), l2_before);

    // Verify data accessible with correct values
    let expected = [("a", 1i64), ("b", 2), ("c", 3)];
    for (key_name, expected_val) in &expected {
        let val = store2
            .get_versioned(&kv_key(key_name), u64::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("key {} missing after recovery", key_name));
        assert_eq!(val.value, Value::Int(*expected_val));
    }
}

#[test]
fn level_bytes_reports_correct_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    assert_eq!(store.level_bytes(&b, 0), 0);
    assert_eq!(store.level_bytes(&b, 1), 0);

    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 2)]);
    let l0_bytes = store.level_bytes(&b, 0);
    assert!(l0_bytes > 0, "L0 should have bytes after flush");

    store.compact_level(&b, 0, 0).unwrap();
    assert_eq!(store.level_bytes(&b, 0), 0);
    let l1_bytes = store.level_bytes(&b, 1);
    assert!(l1_bytes > 0, "L1 should have bytes after compaction");
}

#[test]
fn compact_level_last_level_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Can't compact past the last level
    let result = store.compact_level(&b, NUM_LEVELS - 1, 0).unwrap();
    assert!(result.is_none());
}

#[test]
fn compact_level_empty_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // No data at level 0
    let result = store.compact_level(&b, 0, 0).unwrap();
    assert!(result.is_none());
}

#[test]
fn compact_level_prunes_old_versions() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Write 4 versions of the same key, then flush.
    // CompactionIterator with prune_floor=3 keeps: commits 3,4 (above floor)
    // + 1 survivor below floor (commit 2). Commits 1 is pruned.
    seed(&store, kv_key("k"), Value::Int(1), 1);
    seed(&store, kv_key("k"), Value::Int(2), 2);
    seed(&store, kv_key("k"), Value::Int(3), 3);
    seed(&store, kv_key("k"), Value::Int(4), 4);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();
    assert_eq!(store.l0_segment_count(&b), 1);

    // Compact L0 → L1 with prune_floor = 3 (prunes commits < 3, keeping 1 survivor)
    let result = store.compact_level(&b, 0, 3).unwrap().unwrap();
    assert_eq!(result.segments_merged, 1);
    assert_eq!(
        result.entries_pruned, 1,
        "should prune 1 old version (commit 1)"
    );

    // Latest version should be visible
    let val = store
        .get_versioned(&kv_key("k"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(4));

    // History should have 3 versions (4, 3, 2) — commit 1 pruned
    let history = store.get_history(&kv_key("k"), None, None).unwrap();
    assert_eq!(history.len(), 3, "should have 3 versions after pruning");
    assert_eq!(history[0].value, Value::Int(4));
    assert_eq!(history[1].value, Value::Int(3));
    assert_eq!(history[2].value, Value::Int(2));
}

#[test]
fn compact_level_non_bottommost_preserves_tombstones() {
    // Regression test for zombie key bug (#1600):
    // When a tombstone exists in L1 and an older value exists in L3,
    // compacting L0→L1 with prune_floor must NOT drop the tombstone
    // (since L3 holds data the tombstone covers).
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Step 1: Write value@1, flush, compact to L3 (deep level)
    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();
    store.compact_level(&b, 0, 0).unwrap(); // L0 → L1
    store.compact_level(&b, 1, 0).unwrap(); // L1 → L2
    store.compact_level(&b, 2, 0).unwrap(); // L2 → L3

    // Step 2: Write tombstone@3, flush to L0
    seed(&store, kv_key("k"), Value::Null, 3);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    // Step 3: Compact L0→L1 with prune_floor=5 (tombstone@3 is below floor).
    // This is NOT bottommost (L3 has data), so tombstone MUST be preserved.
    store.compact_level(&b, 0, 5).unwrap();

    // The key should still be deleted (tombstone covers value@1 in L3)
    let result = store.get_versioned(&kv_key("k"), u64::MAX).unwrap();
    assert!(
        result.is_none() || result.as_ref().map_or(false, |v| v.value == Value::Null),
        "Key should be deleted, but got {:?} — zombie key bug!",
        result,
    );
}

#[test]
fn compact_level_ephemeral_returns_none() {
    // Ephemeral store (no segments_dir) — compact_level should return None
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    let result = store.compact_level(&b, 0, 0).unwrap();
    assert!(result.is_none());
}

// ===== Dynamic level targets tests =====

#[test]
fn recalculate_targets_empty_db() {
    let level_bytes = [0u64; NUM_LEVELS];
    let targets = recalculate_level_targets(&level_bytes);
    // All levels derive from MIN_BASE_BYTES when no non-L0 data exists
    assert_eq!(targets.max_bytes[0], 0); // L0 unused (count-based)
    assert_eq!(targets.max_bytes[1], MIN_BASE_BYTES); // 1MB
    assert_eq!(targets.max_bytes[2], MIN_BASE_BYTES * 10);
    assert_eq!(targets.max_bytes[3], MIN_BASE_BYTES * 100);
    assert_eq!(targets.max_bytes[4], MIN_BASE_BYTES * 1_000);
    assert_eq!(targets.max_bytes[5], MIN_BASE_BYTES * 10_000);
    assert_eq!(targets.max_bytes[6], MIN_BASE_BYTES * 100_000);
}

#[test]
fn recalculate_targets_small_db() {
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = 100 << 20; // 100MB in L1
    let targets = recalculate_level_targets(&level_bytes);
    // Dynamic: base = 100MB (derived from bottom level L1)
    assert_eq!(targets.max_bytes[0], 0);
    assert_eq!(targets.max_bytes[1], 100 << 20);
    assert_eq!(targets.max_bytes[2], (100 << 20) * 10);
    assert_eq!(targets.max_bytes[3], (100 << 20) * 100);
    assert_eq!(targets.max_bytes[4], (100 << 20) * 1_000);
    assert_eq!(targets.max_bytes[5], (100 << 20) * 10_000);
    assert_eq!(targets.max_bytes[6], (100 << 20) * 100_000);
}

#[test]
fn recalculate_targets_scales_up() {
    let data: u64 = 30 * (1 << 30); // 30 GiB in L3
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[3] = data;
    let targets = recalculate_level_targets(&level_bytes);
    // base = 30GiB / 10^2 = ~307MB → clamped to MAX_BASE_BYTES (256MB)
    assert_eq!(targets.max_bytes[1], MAX_BASE_BYTES);
    assert_eq!(targets.max_bytes[2], MAX_BASE_BYTES * 10);
    assert_eq!(targets.max_bytes[3], MAX_BASE_BYTES * 100);
    assert_eq!(targets.max_bytes[4], MAX_BASE_BYTES * 1_000);
    assert_eq!(targets.max_bytes[5], MAX_BASE_BYTES * 10_000);
    assert_eq!(targets.max_bytes[6], MAX_BASE_BYTES * 100_000);
}

#[test]
fn recalculate_targets_anchor_at_high_level() {
    // Data at L5 — verifies backward chain across 4 divisions and forward to L6.
    let data: u64 = 3 * (1u64 << 40); // 3 TiB in L5
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[5] = data;
    let targets = recalculate_level_targets(&level_bytes);
    // base = 3TiB / 10^4 = ~322MB → clamped to MAX_BASE_BYTES (256MB)
    assert_eq!(targets.max_bytes[0], 0);
    assert_eq!(targets.max_bytes[1], MAX_BASE_BYTES);
    assert_eq!(targets.max_bytes[2], MAX_BASE_BYTES * 10);
    assert_eq!(targets.max_bytes[3], MAX_BASE_BYTES * 100);
    assert_eq!(targets.max_bytes[4], MAX_BASE_BYTES * 1_000);
    assert_eq!(targets.max_bytes[5], MAX_BASE_BYTES * 10_000);
    assert_eq!(targets.max_bytes[6], MAX_BASE_BYTES * 100_000);
}

#[test]
fn recalculate_targets_tiny_db() {
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = 10 << 20; // 10MB in L1
    let targets = recalculate_level_targets(&level_bytes);
    // base = 10MB — within [MIN, MAX], full geometric chain
    assert_eq!(targets.max_bytes[0], 0);
    assert_eq!(targets.max_bytes[1], 10 << 20);
    assert_eq!(targets.max_bytes[2], (10 << 20) * 10);
    assert_eq!(targets.max_bytes[3], (10 << 20) * 100);
    assert_eq!(targets.max_bytes[4], (10 << 20) * 1_000);
    assert_eq!(targets.max_bytes[5], (10 << 20) * 10_000);
    assert_eq!(targets.max_bytes[6], (10 << 20) * 100_000);
}

#[test]
fn recalculate_targets_sub_minimum() {
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = 512 * 1024; // 512KB in L1
    let targets = recalculate_level_targets(&level_bytes);
    // 512KB < MIN_BASE_BYTES → clamped to 1MB, full chain from MIN
    assert_eq!(targets.max_bytes[1], MIN_BASE_BYTES);
    assert_eq!(targets.max_bytes[2], MIN_BASE_BYTES * 10);
    assert_eq!(targets.max_bytes[6], MIN_BASE_BYTES * 100_000);
}

#[test]
fn recalculate_targets_multi_level_data() {
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = 200 << 20; // 200MB
    level_bytes[2] = 2 << 30; // 2GB
    level_bytes[3] = 15 << 30; // 15GB — largest non-L0 level
    let targets = recalculate_level_targets(&level_bytes);
    // base = 15GB / 10^2 = ~153MB → within [MIN, MAX] range
    let expected_base: u64 = (15u64 << 30) / 100;
    assert_eq!(targets.max_bytes[1], expected_base);
    assert_eq!(targets.max_bytes[2], expected_base * 10);
    assert_eq!(targets.max_bytes[3], expected_base * 100);
    assert_eq!(targets.max_bytes[4], expected_base * 1_000);
    assert_eq!(targets.max_bytes[5], expected_base * 10_000);
    assert_eq!(targets.max_bytes[6], expected_base * 100_000);
    // Verify L1 actual (200MB) exceeds target (~153MB) → score > 1
    assert!(level_bytes[1] > targets.max_bytes[1]);
}

#[test]
fn recalculate_targets_only_l0() {
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[0] = 500 << 20; // 500MB in L0 only
    let targets = recalculate_level_targets(&level_bytes);
    // L0 is ignored — no non-L0 data → uses MIN_BASE_BYTES
    assert_eq!(targets.max_bytes[0], 0);
    assert_eq!(targets.max_bytes[1], MIN_BASE_BYTES);
    assert_eq!(targets.max_bytes[6], MIN_BASE_BYTES * 100_000);
}

#[test]
fn recalculate_targets_empty_intermediate() {
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[4] = 100 << 30; // 100GB in L4 only (L1-L3 empty)
    let targets = recalculate_level_targets(&level_bytes);
    // base = 100GB / 10^3 = ~102MB
    let expected_base: u64 = (100u64 << 30) / 1_000;
    assert_eq!(targets.max_bytes[1], expected_base);
    assert_eq!(targets.max_bytes[2], expected_base * 10);
    assert_eq!(targets.max_bytes[3], expected_base * 100);
    assert_eq!(targets.max_bytes[4], expected_base * 1_000);
    assert_eq!(targets.max_bytes[5], expected_base * 10_000);
    assert_eq!(targets.max_bytes[6], expected_base * 100_000);
}

#[test]
fn recalculate_targets_data_at_l6() {
    // L6 is the highest level — requires maximum backward divisions (5).
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[6] = 100u64 << 40; // 100 TiB in L6
    let targets = recalculate_level_targets(&level_bytes);
    // base = 100TiB / 10^5 = ~1.1GB → clamped to MAX_BASE_BYTES (256MB)
    assert_eq!(targets.max_bytes[1], MAX_BASE_BYTES);
    assert_eq!(targets.max_bytes[6], MAX_BASE_BYTES * 100_000);
}

#[test]
fn recalculate_targets_data_at_l6_unclamped() {
    // L6 with a value small enough that base stays within [MIN, MAX]
    // 10^5 * 100MB = 10TB at L6 → base = 10TB / 10^5 = 100MB
    let data: u64 = 10u64 * (1u64 << 40); // 10 TiB
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[6] = data;
    let targets = recalculate_level_targets(&level_bytes);
    let expected_base = data / 100_000;
    assert!(expected_base > MIN_BASE_BYTES && expected_base < MAX_BASE_BYTES);
    assert_eq!(targets.max_bytes[1], expected_base);
    assert_eq!(targets.max_bytes[2], expected_base * 10);
    assert_eq!(targets.max_bytes[6], expected_base * 100_000);
}

#[test]
fn recalculate_targets_lower_level_dominates() {
    // L2 has more data than L5 — the algorithm should pick L2 as the
    // bottom level (largest by bytes), not L5 (highest by index).
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[2] = 50 << 30; // 50GB in L2
    level_bytes[5] = 1 << 30; // 1GB in L5
    let targets = recalculate_level_targets(&level_bytes);
    // base = 50GB / 10^1 = 5GB → clamped to MAX_BASE_BYTES (256MB)
    assert_eq!(targets.max_bytes[1], MAX_BASE_BYTES);
    // L2 actual (50GB) vs target (2.56GB) → score ~19.5 → aggressive compaction
    assert!(level_bytes[2] as f64 / targets.max_bytes[2] as f64 > 10.0);
}

#[test]
fn recalculate_targets_base_exactly_at_min() {
    // Construct a case where the computed base equals MIN_BASE_BYTES exactly.
    // Data at L1 = 1MB → base = 1MB = MIN_BASE_BYTES.
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = MIN_BASE_BYTES;
    let targets = recalculate_level_targets(&level_bytes);
    assert_eq!(targets.max_bytes[1], MIN_BASE_BYTES);
}

#[test]
fn recalculate_targets_base_exactly_at_max() {
    // Data at L1 = 256MB → base = 256MB = MAX_BASE_BYTES.
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = MAX_BASE_BYTES;
    let targets = recalculate_level_targets(&level_bytes);
    assert_eq!(targets.max_bytes[1], MAX_BASE_BYTES);
    assert_eq!(targets.max_bytes[2], MAX_BASE_BYTES * 10);
}

#[test]
fn recalculate_targets_base_just_above_max_clamps() {
    // Data at L1 = 300MB → base = 300MB > MAX_BASE_BYTES → clamped to 256MB.
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = 300 << 20;
    let targets = recalculate_level_targets(&level_bytes);
    assert_eq!(targets.max_bytes[1], MAX_BASE_BYTES);
}

#[test]
fn recalculate_targets_score_meaningful_for_small_db() {
    // The motivating scenario: 10MB in L1 should produce score ~1.0,
    // not 0.04 (which is what the old 256MB static target produced).
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = 10 << 20; // 10MB
    let targets = recalculate_level_targets(&level_bytes);
    let score = level_bytes[1] as f64 / targets.max_bytes[1] as f64;
    assert!(
        (score - 1.0).abs() < 0.01,
        "10MB in L1 should produce score ~1.0, got {}",
        score
    );
}

#[test]
fn compute_scores_l0_count() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // 3 L0 files → score 0.75
    flush_data(&store, &b, &[("a", 1, 1)]);
    flush_data(&store, &b, &[("b", 2, 2)]);
    flush_data(&store, &b, &[("c", 3, 3)]);

    let scores = store.compute_compaction_scores(&b);
    let l0 = scores.iter().find(|s| s.level == 0).unwrap();
    assert!((l0.score - 0.75).abs() < 0.01);
    // L1-L5 scores should be ~0 (no data in L1+ yet, all data is in L0)
    for cs in scores.iter().filter(|s| s.level > 0) {
        assert!(
            cs.score < 0.01,
            "L{} score {} should be ~0",
            cs.level,
            cs.score
        );
    }

    // 5 L0 files → score 1.25
    flush_data(&store, &b, &[("d", 4, 4)]);
    flush_data(&store, &b, &[("e", 5, 5)]);

    let scores = store.compute_compaction_scores(&b);
    let l0 = scores.iter().find(|s| s.level == 0).unwrap();
    assert!((l0.score - 1.25).abs() < 0.01);
}

#[test]
fn pick_and_compact_no_work() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // 1 L0 segment — below trigger
    flush_data(&store, &b, &[("a", 1, 1)]);

    let result = store.pick_and_compact(&b, 0).unwrap();
    assert!(result.is_none());
}

#[test]
fn pick_and_compact_triggers_l0() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // 4 L0 segments → triggers compaction (score = 1.0)
    flush_data(&store, &b, &[("a", 1, 1)]);
    flush_data(&store, &b, &[("b", 2, 2)]);
    flush_data(&store, &b, &[("c", 3, 3)]);
    flush_data(&store, &b, &[("d", 4, 4)]);

    let result = store.pick_and_compact(&b, 0).unwrap().unwrap();
    assert_eq!(result.level, 0);
    assert_eq!(result.compaction.segments_merged, 4);

    // After compaction, no more work
    assert_eq!(store.l0_segment_count(&b), 0);
    assert!(store.pick_and_compact(&b, 0).unwrap().is_none());
}

#[test]
fn pick_and_compact_ephemeral_noop() {
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    let result = store.pick_and_compact(&b, 0).unwrap();
    assert!(result.is_none());
}

#[test]
fn monkey_allocation_levels() {
    use super::bloom_bits_for_level;

    // L0 always gets base bits (flushed from memtable, short-lived)
    assert_eq!(bloom_bits_for_level(0, 10), 10);
    // L1: bonus = floor(3.3 * (6-1-1)) = floor(3.3*4) = 13, 10+13=23 → capped at 20
    assert_eq!(bloom_bits_for_level(1, 10), 20);
    // L2: bonus = floor(3.3 * 3) = 9, so 10+9=19
    assert_eq!(bloom_bits_for_level(2, 10), 19);
    // L3: bonus = floor(3.3 * 2) = 6, so 10+6=16
    assert_eq!(bloom_bits_for_level(3, 10), 16);
    // L4: bonus = floor(3.3 * 1) = 3, so 10+3=13
    assert_eq!(bloom_bits_for_level(4, 10), 13);
    // L5: bonus = floor(3.3 * 0) = 0, so 10
    assert_eq!(bloom_bits_for_level(5, 10), 10);
    // L6: (6-1-6) saturates to 0, so 10
    assert_eq!(bloom_bits_for_level(6, 10), 10);

    // Monotonicity: upper levels always get >= lower levels
    for level in 1..7 {
        assert!(
            bloom_bits_for_level(level, 10) >= bloom_bits_for_level(level + 1, 10),
            "L{} should get >= bits than L{}",
            level,
            level + 1
        );
    }

    // Different base_bits
    assert_eq!(bloom_bits_for_level(0, 8), 8);
    assert_eq!(bloom_bits_for_level(6, 8), 8);
    // L1 with base=8: 8 + 13 = 21 → capped at 20
    assert_eq!(bloom_bits_for_level(1, 8), 20);
}

#[test]
fn compression_for_level_returns_expected_codecs() {
    use super::compression_for_level;
    use crate::segment_builder::CompressionCodec;

    // L0-L2: hot levels, no compression
    assert_eq!(compression_for_level(0), CompressionCodec::None);
    assert_eq!(compression_for_level(1), CompressionCodec::None);
    assert_eq!(compression_for_level(2), CompressionCodec::None);

    // L3-L5: warm levels, Zstd level 3
    assert_eq!(compression_for_level(3), CompressionCodec::Zstd(3));
    assert_eq!(compression_for_level(4), CompressionCodec::Zstd(3));
    assert_eq!(compression_for_level(5), CompressionCodec::Zstd(3));

    // L6+: cold/bottommost, Zstd level 6
    assert_eq!(compression_for_level(6), CompressionCodec::Zstd(6));
    assert_eq!(compression_for_level(7), CompressionCodec::Zstd(6));
}

// ===== Rate limiter integration tests =====

/// Helper: write many keys to produce multi-block segments (each ~64KiB block).
/// Writes `count` keys with 1KiB values so ~64 keys per block.
fn seed_many(store: &SegmentedStore, prefix: &str, count: usize, version: u64) {
    let big_value = Value::Bytes(vec![0xAB; 1024]);
    for i in 0..count {
        seed(
            store,
            kv_key(&format!("{}{:06}", prefix, i)),
            big_value.clone(),
            version,
        );
    }
}

#[test]
fn compaction_with_rate_limiter_multi_block() {
    // Write enough data to span multiple data blocks (64KiB each),
    // so the rate limiter is actually invoked for block transitions on
    // both the read and write paths.
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    store.set_compaction_rate_limit(50 * 1024 * 1024); // 50 MB/s (fast enough to not stall)

    // ~200 keys × 1KiB ≈ 200KiB → ~3 data blocks per segment
    seed_many(&store, "a", 200, 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed_many(&store, "b", 200, 2);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    assert_eq!(store.branch_segment_count(&b), 2);

    let result = store.compact_branch(&b, 0).unwrap().unwrap();
    assert_eq!(result.segments_merged, 2);
    assert_eq!(result.output_entries, 400);
    assert_eq!(result.entries_pruned, 0);
    assert_eq!(store.branch_segment_count(&b), 1);

    // Verify data integrity: spot-check first, last, and middle keys
    assert!(store
        .get_versioned(&kv_key("a000000"), u64::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("a000199"), u64::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("b000100"), u64::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("b000199"), u64::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn rate_limiter_disabled_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // No rate limiter set — compaction should work normally
    seed_many(&store, "x", 100, 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed_many(&store, "y", 100, 2);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    let result = store.compact_branch(&b, 0).unwrap().unwrap();
    assert_eq!(result.segments_merged, 2);
    assert_eq!(result.output_entries, 200);

    assert!(store
        .get_versioned(&kv_key("x000000"), u64::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("y000099"), u64::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn set_compaction_rate_limit_zero_disables() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Enable then disable
    store.set_compaction_rate_limit(1_000_000);
    store.set_compaction_rate_limit(0);

    seed_many(&store, "z", 100, 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed_many(&store, "w", 100, 2);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    // Compaction with no limiter should complete quickly and correctly
    let result = store.compact_branch(&b, 0).unwrap().unwrap();
    assert_eq!(result.segments_merged, 2);
    assert_eq!(result.output_entries, 200);
}

#[test]
fn rate_limiter_l0_to_l1_compaction() {
    // Verify the rate limiter is threaded through the L0→L1 path.
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    store.set_compaction_rate_limit(50 * 1024 * 1024);

    for i in 0..4 {
        seed_many(&store, &format!("l0seg{}", i), 50, (i + 1) as u64);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();
    }

    let result = store.compact_l0_to_l1(&b, 0).unwrap().unwrap();
    assert_eq!(result.segments_merged, 4);
    assert_eq!(result.output_entries, 200);

    // Spot-check data after L0→L1 compaction
    assert!(store
        .get_versioned(&kv_key("l0seg0000000"), u64::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("l0seg3000049"), u64::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn rate_limited_compaction_preserves_prune_semantics() {
    // Verify that version pruning still works correctly with the limiter active.
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    store.set_compaction_rate_limit(50 * 1024 * 1024);

    // Write two versions of the same keys (v1 and v5)
    seed_many(&store, "k", 100, 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    seed_many(&store, "k", 100, 5);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    // prune_floor=6: both v1 and v5 are below floor.
    // Only the newest below-floor version (v5) survives per key; v1 is pruned.
    let result = store.compact_branch(&b, 6).unwrap().unwrap();
    assert_eq!(result.segments_merged, 2);
    assert_eq!(result.output_entries, 100); // only v5 per key survives
    assert_eq!(result.entries_pruned, 100);

    // Version 5 still readable
    let val = store
        .get_versioned(&kv_key("k000050"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.version.as_u64(), 5);

    // Version 1 was pruned
    assert!(store
        .get_versioned(&kv_key("k000050"), 1)
        .unwrap()
        .is_none());
}
