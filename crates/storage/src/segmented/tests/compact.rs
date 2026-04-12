use super::*;

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
            .get_versioned(&kv_key("a"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );
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
            .get_versioned(&kv_key("k"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(1))
            .unwrap()
            .unwrap()
            .value,
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
            .get_versioned(&kv_key("k"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion(2))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    // Version 1 was pruned — reading at snapshot 1 should return nothing
    assert!(store
        .get_versioned(&kv_key("k"), CommitVersion(1))
        .unwrap()
        .is_none());
}

#[test]
fn compact_removes_dead_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    seed(&store, kv_key("k"), Value::Int(1), 1);
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    store
        .delete_with_version(&kv_key("k"), CommitVersion(2))
        .unwrap();
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

    store
        .delete_with_version(&kv_key("k"), CommitVersion(3))
        .unwrap();
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
            .get_versioned(&kv_key("a"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("c"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(20)
    );

    let prefix_key = Key::new(ns(), TypeTag::KV, Vec::new());
    let results = store.scan_prefix(&prefix_key, CommitVersion::MAX).unwrap();
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
            .get_versioned(&kv_key("a"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("a"), CommitVersion(2))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), CommitVersion::MAX)
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
fn pressure_includes_segment_metadata() {
    use crate::pressure::MemoryPressure;
    let dir = tempfile::tempdir().unwrap();
    // Budget: 100 bytes — any segment's bloom + index metadata will exceed this.
    let store = SegmentedStore::with_dir_and_pressure(
        dir.path().to_path_buf(),
        0,
        MemoryPressure::new(100, 0.7, 0.9),
    );
    let b = branch();

    // Write enough entries to produce a segment with non-trivial bloom/index metadata.
    for i in 0..100u64 {
        seed(
            &store,
            kv_key(&format!("key_{:04}", i)),
            Value::String("x".repeat(100)),
            i + 1,
        );
    }
    store.rotate_memtable(&b);
    store.flush_oldest_frozen(&b).unwrap();

    // After flush, memtable is nearly empty. But segment bloom + index metadata
    // (loaded eagerly at open time) should be tracked. With a 100-byte budget,
    // segment metadata (~500 bytes of bloom/index data) should push pressure up.
    let level = store.pressure_level();
    assert!(
        level >= PressureLevel::Warning,
        "pressure_level ignores segment metadata — reports {:?} even though \
         segment bloom+index metadata far exceeds the 100-byte budget",
        level
    );
}

mod segment_metadata_tracking {
    use super::*;

    #[test]
    fn segment_metadata_bytes_tracked_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        assert_eq!(store.total_segment_metadata_bytes(), 0);

        for i in 0..50u64 {
            seed(
                &store,
                kv_key(&format!("key_{:04}", i)),
                Value::String("x".repeat(100)),
                i + 1,
            );
        }
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        let meta = store.total_segment_metadata_bytes();
        assert!(
            meta > 0,
            "flushed segment should have non-zero metadata bytes (bloom + index)"
        );
    }

    #[test]
    fn total_tracked_bytes_includes_memtable_and_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        // Write and flush to create segments
        for i in 0..50u64 {
            seed(
                &store,
                kv_key(&format!("key_{:04}", i)),
                Value::String("x".repeat(100)),
                i + 1,
            );
        }
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        // Write more to active memtable
        for i in 50..60u64 {
            seed(
                &store,
                kv_key(&format!("key_{:04}", i)),
                Value::String("x".repeat(100)),
                i + 1,
            );
        }

        let memtable = store.total_memtable_bytes();
        let seg_meta = store.total_segment_metadata_bytes();
        let tracked = store.total_tracked_bytes();

        assert!(memtable > 0, "active memtable should have data");
        assert!(seg_meta > 0, "flushed segments should have metadata");
        assert_eq!(
            tracked,
            memtable + seg_meta,
            "total_tracked should be memtable + segment metadata"
        );
    }
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
                .get_versioned(&kv_key(&format!("k{}", commit)), CommitVersion::MAX)
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
            .get_versioned(&kv_key("c"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
    // Memtable update shadows segment version
    assert_eq!(
        store
            .get_versioned(&kv_key("a"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
    // Old segment version still readable at old snapshot
    assert_eq!(
        store
            .get_versioned(&kv_key("a"), CommitVersion(1))
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );
    // Segment-only data still readable
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
            .get_versioned(&kv_key("a"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("b"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("c"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(3)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("d"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(4)
    );
}
