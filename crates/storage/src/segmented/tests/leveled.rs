use super::*;

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
    let result = store
        .compact_tier(&b, &[0, 1, 2, 3], CommitVersion::ZERO)
        .unwrap()
        .unwrap();
    assert_eq!(result.segments_merged, 4);
    assert_eq!(result.output_entries, 4);
    assert_eq!(result.entries_pruned, 0);

    // 2 untouched + 1 merged = 3 segments remaining
    assert_eq!(store.branch_segment_count(&b), 3);

    // All data still readable
    for commit in 1..=6u64 {
        let val = store
            .get_versioned(&kv_key(&format!("k{}", commit)), CommitVersion::MAX)
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
    assert!(store
        .compact_tier(&b, &[0], CommitVersion(0))
        .unwrap()
        .is_none());

    // Empty indices → Ok(None)
    assert!(store
        .compact_tier(&b, &[], CommitVersion(0))
        .unwrap()
        .is_none());
}

#[test]
fn compact_tier_ephemeral_returns_none() {
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    assert!(store
        .compact_tier(&b, &[0, 1], CommitVersion::ZERO)
        .unwrap()
        .is_none());
}

#[test]
fn compact_tier_missing_branch_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = BranchId::from_bytes([99; 16]);
    assert!(store
        .compact_tier(&b, &[0, 1], CommitVersion::ZERO)
        .unwrap()
        .is_none());
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
    let result = store
        .compact_tier(&b, &[0, 1, 2], CommitVersion(3))
        .unwrap()
        .unwrap();
    assert_eq!(result.segments_merged, 3);
    assert_eq!(result.output_entries, 2); // commit 3 + commit 2
    assert_eq!(result.entries_pruned, 1); // commit 1

    // Latest version still readable
    assert_eq!(
        store
            .get_versioned(&kv_key("k"), CommitVersion::MAX)
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
    store
        .delete_with_version(&kv_key("a"), CommitVersion(2))
        .unwrap();

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
        .compact_tier(&b, &candidates[0].segment_indices, CommitVersion(0))
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
            .compact_tier(&b, &candidate.segment_indices, CommitVersion(0))
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
            .get_versioned(&kv_key(&format!("key_{:06}", i)), CommitVersion::MAX)
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

    let result = store
        .compact_l0_to_l1(&b, CommitVersion(0))
        .unwrap()
        .unwrap();
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
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();
    assert_eq!(store.l1_segment_count(&b), 1);

    // New L0 with overlapping keys
    flush_data(&store, &b, &[("a", 10, 2), ("c", 3, 2)]);
    let result = store
        .compact_l0_to_l1(&b, CommitVersion(0))
        .unwrap()
        .unwrap();

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
            .get_versioned(&kv_key("a"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
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
}

#[test]
fn compact_l0_to_l1_preserves_non_overlapping_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // First L0→L1: keys x, y, z (high range)
    flush_data(&store, &b, &[("x", 1, 1), ("y", 2, 1), ("z", 3, 1)]);
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();
    assert_eq!(store.l1_segment_count(&b), 1);

    // New L0 with disjoint keys a, b (low range, no overlap with x-z)
    flush_data(&store, &b, &[("a", 10, 2), ("b", 20, 2)]);
    let result = store
        .compact_l0_to_l1(&b, CommitVersion(0))
        .unwrap()
        .unwrap();

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
                .get_versioned(&kv_key(key), CommitVersion::MAX)
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
    let result = store
        .compact_l0_to_l1(&b, CommitVersion(3))
        .unwrap()
        .unwrap();
    assert_eq!(result.output_entries, 2);
    assert_eq!(result.entries_pruned, 1);

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
    assert!(store
        .get_versioned(&kv_key("k"), CommitVersion(1))
        .unwrap()
        .is_none());
}

#[test]
fn compact_l0_to_l1_empty_l0_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    // No rotation/flush → L0 is empty
    assert!(store
        .compact_l0_to_l1(&b, CommitVersion(0))
        .unwrap()
        .is_none());
}

#[test]
fn compact_l0_to_l1_ephemeral_returns_none() {
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    assert!(store
        .compact_l0_to_l1(&b, CommitVersion(0))
        .unwrap()
        .is_none());
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

    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();
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
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 1);

    // New L0 flush
    flush_data(&store, &b, &[("c", 3, 3)]);
    assert_eq!(store.l0_segment_count(&b), 1);

    // All data still readable
    assert!(store
        .get_versioned(&kv_key("a"), CommitVersion::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("b"), CommitVersion::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("c"), CommitVersion::MAX)
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

    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

    // Point lookups
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
            .get_versioned(&kv_key("a"), CommitVersion(1))
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

    // Prefix scan
    let prefix_key = Key::new(ns(), TypeTag::KV, Vec::new());
    let results = store.scan_prefix(&prefix_key, CommitVersion::MAX).unwrap();
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
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

    flush_data(&store, &b, &[("a", 3, 2), ("b", 4, 2)]);
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

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
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();
    assert_eq!(store.l1_segment_count(&b), 1);

    // Second round — new L0 overlaps L1
    flush_data(&store, &b, &[("a", 10, 2), ("c", 3, 2)]);
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 1);

    // Third round — new disjoint L0
    flush_data(&store, &b, &[("z", 99, 3)]);
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 2);

    // All data correct
    assert_eq!(
        store
            .get_versioned(&kv_key("a"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(10)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("z"), CommitVersion::MAX)
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
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

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
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 1);

    // Data in L1 only
    assert_eq!(
        store
            .get_versioned(&kv_key("k1"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(1)
    );
    assert_eq!(
        store
            .get_versioned(&kv_key("k2"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(2)
    );
    assert!(store
        .get_versioned(&kv_key("nonexistent"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn scan_prefix_includes_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("item/a", 1, 1), ("item/b", 2, 1)]);
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

    // Add to memtable (L0)
    seed(&store, kv_key("item/c"), Value::Int(3), 2);

    let prefix = Key::new(ns(), TypeTag::KV, "item/".as_bytes().to_vec());
    let results = store.scan_prefix(&prefix, CommitVersion::MAX).unwrap();
    assert_eq!(results.len(), 3); // 2 from L1 + 1 from memtable
}

#[test]
fn list_branch_includes_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 2)]);
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

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
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

    flush_data(&store, &b, &[("m", 3, 2), ("n", 4, 2)]);
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

    flush_data(&store, &b, &[("x", 5, 3), ("y", 6, 3)]);
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

    assert_eq!(store.l1_segment_count(&b), 3);

    // Each key should find the correct L1 segment
    for (key, val) in [("a", 1), ("b", 2), ("m", 3), ("n", 4), ("x", 5), ("y", 6)] {
        assert_eq!(
            store
                .get_versioned(&kv_key(key), CommitVersion::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(val),
            "key {key}"
        );
    }

    // Keys not in any segment
    assert!(store
        .get_versioned(&kv_key("c"), CommitVersion::MAX)
        .unwrap()
        .is_none());
    assert!(store
        .get_versioned(&kv_key("p"), CommitVersion::MAX)
        .unwrap()
        .is_none());
    assert!(store
        .get_versioned(&kv_key("zzz"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn max_flushed_commit_includes_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 5), ("b", 2, 10)]);
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);

    // max_flushed_commit should still work with L1 only
    assert_eq!(store.max_flushed_commit(&b), Some(CommitVersion(10)));
}

#[test]
fn shard_stats_includes_l1() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1), ("b", 2, 2), ("c", 3, 3)]);
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

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
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

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
                .get_versioned(&kv_key(key), CommitVersion::MAX)
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

    // Recover — backward-compat L0 promotion still happens, but SE2 now
    // emits a `NoManifestFallbackUsed` fault classified as
    // `PolicyDowngrade` so strict callers (D4) can refuse.
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();

    match &info.health {
        super::RecoveryHealth::Degraded { faults, class } => {
            assert_eq!(
                *class,
                super::DegradationClass::PolicyDowngrade,
                "no-manifest fallback must classify as PolicyDowngrade"
            );
            assert!(
                faults.iter().any(|f| matches!(
                    f,
                    super::RecoveryFault::NoManifestFallbackUsed { segments_promoted, .. }
                    if *segments_promoted == 2
                )),
                "should emit NoManifestFallbackUsed fault with 2 segments promoted"
            );
        }
        other => panic!("expected Degraded, got {other:?}"),
    }

    assert_eq!(store2.l0_segment_count(&b), 2);
    assert_eq!(store2.l1_segment_count(&b), 0);

    // Data still correct
    assert!(store2
        .get_versioned(&kv_key("a"), CommitVersion::MAX)
        .unwrap()
        .is_some());
    assert!(store2
        .get_versioned(&kv_key("b"), CommitVersion::MAX)
        .unwrap()
        .is_some());
}

#[test]
fn recover_manifest_corrupt_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    flush_data(&store, &b, &[("a", 1, 1)]);
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();

    // Corrupt the manifest
    let branch_dir = dir.path().join(hex_encode_branch(&b));
    let manifest_path = branch_dir.join("segments.manifest");
    std::fs::write(&manifest_path, b"corrupted data!").unwrap();

    // Recover — corrupt manifest must NOT silently load all as L0 (#1680).
    // The branch is skipped (own segments not loaded), but recovery itself
    // succeeds so other branches are unaffected (#1691). Post-SE2 the
    // defect surfaces as a `CorruptManifest` fault classified as `DataLoss`.
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    match &info.health {
        super::RecoveryHealth::Degraded { faults, class } => {
            assert_eq!(
                *class,
                super::DegradationClass::DataLoss,
                "corrupt manifest must classify as DataLoss"
            );
            assert!(
                faults
                    .iter()
                    .any(|f| matches!(f, super::RecoveryFault::CorruptManifest { .. })),
                "corrupt manifest branch must be reported as CorruptManifest fault"
            );
        }
        other => panic!("expected Degraded, got {other:?}"),
    }
    // The corrupt branch's data must NOT be accessible.
    assert!(
        store2
            .get_versioned(&kv_key("a"), CommitVersion::MAX)
            .unwrap()
            .is_none(),
        "corrupt-manifest branch must not have segments loaded"
    );
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
    store.compact_l0_to_l1(&b, CommitVersion(0)).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert!(store.l1_segment_count(&b) >= 1);

    // All 250 entries should be readable
    for i in 0..250u64 {
        let result = store
            .get_versioned(&kv_key(&format!("key_{:06}", i)), CommitVersion::MAX)
            .unwrap();
        assert!(result.is_some(), "key_{:06} should be readable", i);
    }

    // Prefix scan should find all entries
    let prefix = Key::new(ns(), TypeTag::KV, "key_".as_bytes().to_vec());
    let results = store.scan_prefix(&prefix, CommitVersion::MAX).unwrap();
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

    let result = store
        .compact_l0_to_l1(&b, CommitVersion(0))
        .unwrap()
        .unwrap();
    assert_eq!(result.output_entries, 400);
    assert_eq!(store.l0_segment_count(&b), 0);
    assert_eq!(store.l1_segment_count(&b), 1);

    // All data correct
    for i in 0..400u64 {
        assert!(
            store
                .get_versioned(&kv_key(&format!("k{:06}", i)), CommitVersion::MAX)
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

    let result = store
        .compact_level(&b, 0, CommitVersion(0))
        .unwrap()
        .unwrap();
    assert_eq!(result.segments_merged, 3);
    assert_eq!(result.output_entries, 5);
    assert_eq!(result.entries_pruned, 0);

    assert_eq!(store.l0_segment_count(&b), 0);
    assert!(store.l1_segment_count(&b) >= 1);

    // All data is findable with correct values
    let expected = [("a", 1i64), ("b", 2), ("c", 3), ("d", 4), ("e", 5)];
    for (key_name, expected_val) in &expected {
        let val = store
            .get_versioned(&kv_key(key_name), CommitVersion::MAX)
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
    store.compact_level(&b, 0, CommitVersion(0)).unwrap();
    assert_eq!(store.l0_segment_count(&b), 0);
    assert!(store.l1_segment_count(&b) >= 1);

    // Now compact L1 → L2
    let result = store
        .compact_level(&b, 1, CommitVersion(0))
        .unwrap()
        .unwrap();
    assert_eq!(result.output_entries, 4);

    // L1 should be empty, L2 should have data
    assert_eq!(store.level_segment_count(&b, 1), 0);
    assert!(store.level_segment_count(&b, 2) >= 1);

    // All data still findable with correct values
    let expected = [("a", 1i64), ("b", 2), ("c", 3), ("d", 4)];
    for (key_name, expected_val) in &expected {
        let val = store
            .get_versioned(&kv_key(key_name), CommitVersion::MAX)
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
    store.compact_level(&b, 0, CommitVersion(0)).unwrap();
    flush_data(&store, &b, &[("mmm", 2, 2)]);
    store.compact_level(&b, 0, CommitVersion(0)).unwrap();
    flush_data(&store, &b, &[("zzz", 3, 3)]);
    store.compact_level(&b, 0, CommitVersion(0)).unwrap();

    assert_eq!(store.level_segment_count(&b, 1), 3);
    assert_eq!(store.level_segment_count(&b, 2), 0);

    // First compaction: picks file 0 (trivial move), pointer advances past "aaa"
    let r1 = store
        .compact_level(&b, 1, CommitVersion(0))
        .unwrap()
        .unwrap();
    assert_eq!(r1.segments_merged, 1);
    assert_eq!(store.level_segment_count(&b, 1), 2);
    assert_eq!(store.level_segment_count(&b, 2), 1);

    // Second compaction: picks file after pointer (should be "mmm")
    let r2 = store
        .compact_level(&b, 1, CommitVersion(0))
        .unwrap()
        .unwrap();
    assert_eq!(r2.segments_merged, 1);
    assert_eq!(store.level_segment_count(&b, 1), 1);
    assert_eq!(store.level_segment_count(&b, 2), 2);

    // Third compaction: picks remaining file ("zzz")
    let r3 = store
        .compact_level(&b, 1, CommitVersion(0))
        .unwrap()
        .unwrap();
    assert_eq!(r3.segments_merged, 1);
    assert_eq!(store.level_segment_count(&b, 1), 0);
    assert_eq!(store.level_segment_count(&b, 2), 3);

    // L1 empty, nothing to compact
    assert!(store
        .compact_level(&b, 1, CommitVersion(0))
        .unwrap()
        .is_none());

    // All data still findable
    for key_name in &["aaa", "mmm", "zzz"] {
        assert!(
            store
                .get_versioned(&kv_key(key_name), CommitVersion::MAX)
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
    store.compact_level(&b, 0, CommitVersion(0)).unwrap();
    assert_eq!(store.level_segment_count(&b, 1), 1);
    assert_eq!(store.level_segment_count(&b, 2), 0);

    // Record L1 file size before move
    let l1_bytes_before = store.level_bytes(&b, 1);
    assert!(l1_bytes_before > 0);

    // Compact L1 → L2: should be a trivial move (1 input, 0 overlap)
    let result = store
        .compact_level(&b, 1, CommitVersion(0))
        .unwrap()
        .unwrap();
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
        .get_versioned(&kv_key("x"), CommitVersion::MAX)
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
    let result = store
        .compact_level(&b, 0, CommitVersion(0))
        .unwrap()
        .unwrap();
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
    store.compact_level(&b, 0, CommitVersion(0)).unwrap();

    // Flush new L0 data that overlaps with part of L1
    flush_data(&store, &b, &[("b", 20, 2)]);
    assert_eq!(store.l0_segment_count(&b), 1);

    // compact_level(0) should merge L0 with overlapping L1 segment
    let result = store
        .compact_level(&b, 0, CommitVersion(0))
        .unwrap()
        .unwrap();
    assert_eq!(result.segments_merged, 2); // 1 L0 + 1 L1
    assert_eq!(store.l0_segment_count(&b), 0);

    // Verify updated value for "b"
    let val = store
        .get_versioned(&kv_key("b"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(20));

    // Verify other keys survived the merge
    let expected = [("a", 1i64), ("c", 3), ("d", 4)];
    for (key_name, expected_val) in &expected {
        let val = store
            .get_versioned(&kv_key(key_name), CommitVersion::MAX)
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
    store.compact_level(&b, 0, CommitVersion(0)).unwrap(); // L0 → L1
    store.compact_level(&b, 1, CommitVersion(0)).unwrap(); // L1 → L2
    store.compact_level(&b, 2, CommitVersion(0)).unwrap(); // L2 → L3

    assert_eq!(store.level_segment_count(&b, 0), 0);
    assert_eq!(store.level_segment_count(&b, 1), 0);
    assert_eq!(store.level_segment_count(&b, 2), 0);
    assert!(store.level_segment_count(&b, 3) >= 1);

    // Point lookup must find data at L3
    let val = store
        .get_versioned(&kv_key("deep"), CommitVersion::MAX)
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
    store.compact_level(&b, 0, CommitVersion(0)).unwrap();
    store.compact_level(&b, 1, CommitVersion(0)).unwrap();
    assert!(store.level_segment_count(&b, 2) >= 1);

    // L1: "p/b"
    flush_data(&store, &b, &[("p/b", 2, 2)]);
    store.compact_level(&b, 0, CommitVersion(0)).unwrap();
    assert!(store.level_segment_count(&b, 1) >= 1);

    // L0: "p/c"
    flush_data(&store, &b, &[("p/c", 3, 3)]);
    assert!(store.l0_segment_count(&b) >= 1);

    // memtable: "p/d"
    seed(&store, kv_key("p/d"), Value::Int(4), 4);

    // Prefix scan should find all four
    let results = store
        .scan_prefix(&kv_key("p/"), CommitVersion::MAX)
        .unwrap();
    assert_eq!(results.len(), 4, "scan should merge data across all levels");
}

#[test]
fn recover_restores_multi_level() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // Create data across multiple levels
    flush_data(&store, &b, &[("a", 1, 1)]);
    store.compact_level(&b, 0, CommitVersion(0)).unwrap(); // L1
    flush_data(&store, &b, &[("b", 2, 2)]);
    store.compact_level(&b, 0, CommitVersion(0)).unwrap(); // L1
    store.compact_level(&b, 1, CommitVersion(0)).unwrap(); // L1 → L2
    flush_data(&store, &b, &[("c", 3, 3)]);
    store.compact_level(&b, 0, CommitVersion(0)).unwrap(); // L1

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
            .get_versioned(&kv_key(key_name), CommitVersion::MAX)
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

    store.compact_level(&b, 0, CommitVersion(0)).unwrap();
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
    let result = store
        .compact_level(&b, NUM_LEVELS - 1, CommitVersion(0))
        .unwrap();
    assert!(result.is_none());
}

#[test]
fn compact_level_empty_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let b = branch();

    // No data at level 0
    let result = store.compact_level(&b, 0, CommitVersion(0)).unwrap();
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
    let result = store
        .compact_level(&b, 0, CommitVersion(3))
        .unwrap()
        .unwrap();
    assert_eq!(result.segments_merged, 1);
    assert_eq!(
        result.entries_pruned, 1,
        "should prune 1 old version (commit 1)"
    );

    // Latest version should be visible
    let val = store
        .get_versioned(&kv_key("k"), CommitVersion::MAX)
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
fn compact_level_ephemeral_returns_none() {
    // Ephemeral store (no segments_dir) — compact_level should return None
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    let result = store.compact_level(&b, 0, CommitVersion(0)).unwrap();
    assert!(result.is_none());
}

// ===== Dynamic level targets tests =====

#[test]
fn recalculate_targets_empty_db() {
    let level_bytes = [0u64; NUM_LEVELS];
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
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
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
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
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
    // base = 30GiB / 10^2 = ~307MB → clamped to LEVEL_BASE_BYTES (256MB)
    assert_eq!(targets.max_bytes[1], LEVEL_BASE_BYTES);
    assert_eq!(targets.max_bytes[2], LEVEL_BASE_BYTES * 10);
    assert_eq!(targets.max_bytes[3], LEVEL_BASE_BYTES * 100);
    assert_eq!(targets.max_bytes[4], LEVEL_BASE_BYTES * 1_000);
    assert_eq!(targets.max_bytes[5], LEVEL_BASE_BYTES * 10_000);
    assert_eq!(targets.max_bytes[6], LEVEL_BASE_BYTES * 100_000);
}

#[test]
fn recalculate_targets_anchor_at_high_level() {
    // Data at L5 — verifies backward chain across 4 divisions and forward to L6.
    let data: u64 = 3 * (1u64 << 40); // 3 TiB in L5
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[5] = data;
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
    // base = 3TiB / 10^4 = ~322MB → clamped to LEVEL_BASE_BYTES (256MB)
    assert_eq!(targets.max_bytes[0], 0);
    assert_eq!(targets.max_bytes[1], LEVEL_BASE_BYTES);
    assert_eq!(targets.max_bytes[2], LEVEL_BASE_BYTES * 10);
    assert_eq!(targets.max_bytes[3], LEVEL_BASE_BYTES * 100);
    assert_eq!(targets.max_bytes[4], LEVEL_BASE_BYTES * 1_000);
    assert_eq!(targets.max_bytes[5], LEVEL_BASE_BYTES * 10_000);
    assert_eq!(targets.max_bytes[6], LEVEL_BASE_BYTES * 100_000);
}

#[test]
fn recalculate_targets_tiny_db() {
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = 10 << 20; // 10MB in L1
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
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
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
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
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
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
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
    // L0 is ignored — no non-L0 data → uses MIN_BASE_BYTES
    assert_eq!(targets.max_bytes[0], 0);
    assert_eq!(targets.max_bytes[1], MIN_BASE_BYTES);
    assert_eq!(targets.max_bytes[6], MIN_BASE_BYTES * 100_000);
}

#[test]
fn recalculate_targets_empty_intermediate() {
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[4] = 100 << 30; // 100GB in L4 only (L1-L3 empty)
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
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
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
    // base = 100TiB / 10^5 = ~1.1GB → clamped to LEVEL_BASE_BYTES (256MB)
    assert_eq!(targets.max_bytes[1], LEVEL_BASE_BYTES);
    assert_eq!(targets.max_bytes[6], LEVEL_BASE_BYTES * 100_000);
}

#[test]
fn recalculate_targets_data_at_l6_unclamped() {
    // L6 with a value small enough that base stays within [MIN, MAX]
    // 10^5 * 100MB = 10TB at L6 → base = 10TB / 10^5 = 100MB
    let data: u64 = 10u64 * (1u64 << 40); // 10 TiB
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[6] = data;
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
    let expected_base = data / 100_000;
    assert!(expected_base > MIN_BASE_BYTES && expected_base < LEVEL_BASE_BYTES);
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
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
    // base = 50GB / 10^1 = 5GB → clamped to LEVEL_BASE_BYTES (256MB)
    assert_eq!(targets.max_bytes[1], LEVEL_BASE_BYTES);
    // L2 actual (50GB) vs target (2.56GB) → score ~19.5 → aggressive compaction
    assert!(level_bytes[2] as f64 / targets.max_bytes[2] as f64 > 10.0);
}

#[test]
fn recalculate_targets_base_exactly_at_min() {
    // Construct a case where the computed base equals MIN_BASE_BYTES exactly.
    // Data at L1 = 1MB → base = 1MB = MIN_BASE_BYTES.
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = MIN_BASE_BYTES;
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
    assert_eq!(targets.max_bytes[1], MIN_BASE_BYTES);
}

#[test]
fn recalculate_targets_base_exactly_at_max() {
    // Data at L1 = 256MB → base = 256MB = LEVEL_BASE_BYTES.
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = LEVEL_BASE_BYTES;
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
    assert_eq!(targets.max_bytes[1], LEVEL_BASE_BYTES);
    assert_eq!(targets.max_bytes[2], LEVEL_BASE_BYTES * 10);
}

#[test]
fn recalculate_targets_base_just_above_max_clamps() {
    // Data at L1 = 300MB → base = 300MB > LEVEL_BASE_BYTES → clamped to 256MB.
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = 300 << 20;
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
    assert_eq!(targets.max_bytes[1], LEVEL_BASE_BYTES);
}

#[test]
fn recalculate_targets_score_meaningful_for_small_db() {
    // The motivating scenario: 10MB in L1 should produce score ~1.0,
    // not 0.04 (which is what the old 256MB static target produced).
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[1] = 10 << 20; // 10MB
    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);
    let score = level_bytes[1] as f64 / targets.max_bytes[1] as f64;
    assert!(
        (score - 1.0).abs() < 0.01,
        "10MB in L1 should produce score ~1.0, got {}",
        score
    );
}

#[test]
fn test_issue_1683_dynamic_base_level_deep_small_data() {
    // Bug: when data is concentrated in a deep level at a small size,
    // the algorithm divides by multiplier^(level-1) producing a tiny base
    // that clamps to MIN_BASE_BYTES. This gives L1 a target of just 1MB,
    // causing any 2MB flush to trigger unnecessary compaction cascading
    // through empty intermediate levels.
    //
    // Fix: raise base_level dynamically so levels below it get passive
    // LEVEL_BASE_BYTES targets, preventing the cascading.
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[4] = 5 << 20; // 5MB in L4

    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);

    // Without dynamic base_level: base = 5MB / 10^3 = 5KB → clamped to 1MB.
    // L1 target = 1MB — trivially exceeded by any normal flush.
    //
    // With the fix: base_level raised to L4.
    // L1-L3 get passive LEVEL_BASE_BYTES (256MB) targets.
    assert_eq!(
        targets.max_bytes[1], LEVEL_BASE_BYTES,
        "L1 should have passive LEVEL_BASE_BYTES target when base_level > 1"
    );
    assert_eq!(
        targets.max_bytes[2], LEVEL_BASE_BYTES,
        "L2 should have passive LEVEL_BASE_BYTES target"
    );
    assert_eq!(
        targets.max_bytes[3], LEVEL_BASE_BYTES,
        "L3 should have passive LEVEL_BASE_BYTES target"
    );

    // L4 is the base_level — its target is derived from bottom_bytes via
    // integer division and multiplication (5MB / 10^3 * 10^3), which may
    // truncate slightly.
    let expected_base: u64 = {
        let mut b = 5u64 << 20;
        for _ in 1..4 {
            b /= LEVEL_MULTIPLIER;
        }
        for _ in 1..4 {
            b = b.saturating_mul(LEVEL_MULTIPLIER);
        }
        b
    };
    assert_eq!(targets.max_bytes[4], expected_base);

    // L5-L6 continue geometric progression from base_level
    assert_eq!(targets.max_bytes[5], expected_base * 10);
    assert_eq!(targets.max_bytes[6], expected_base * 100);
}

#[test]
fn test_issue_1683_deep_level_small_data_l6() {
    // Same issue but with data at L6 (maximum backward divisions).
    // 500MB in L6: base = 500MB / 10^5 = 5242 → below MIN_BASE_BYTES.
    // Multiply up: 5242 → 52420 → 524200 → 5242000 (bl=4).
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[6] = 500 << 20; // 500MB in L6

    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);

    let expected_base: u64 = {
        let mut b = 500u64 << 20;
        for _ in 1..6 {
            b /= LEVEL_MULTIPLIER;
        }
        // multiply back up until >= MIN_BASE_BYTES
        while b < MIN_BASE_BYTES {
            b = b.saturating_mul(LEVEL_MULTIPLIER);
        }
        b
    };

    // L1-L3 get passive LEVEL_BASE_BYTES targets (below base_level=4).
    for level in 1..=3 {
        assert_eq!(
            targets.max_bytes[level], LEVEL_BASE_BYTES,
            "L{level} should have passive LEVEL_BASE_BYTES target"
        );
    }

    // L4 is the base_level with the data-derived target.
    assert_eq!(targets.max_bytes[4], expected_base);
    assert_eq!(targets.max_bytes[5], expected_base * 10);
    assert_eq!(targets.max_bytes[6], expected_base * 100);
}

#[test]
fn test_issue_1683_transition_empty_to_populated() {
    // During transitions from empty to populated states, data may
    // land in a deep level while shallow levels are empty. The
    // shallow levels should not get tiny targets.
    let mut level_bytes = [0u64; NUM_LEVELS];
    level_bytes[5] = 1 << 30; // 1GB in L5

    let targets = recalculate_level_targets(&level_bytes, LEVEL_BASE_BYTES);

    // base = 1GB / 10^4 = 107374 (~100KB) → below MIN_BASE_BYTES.
    // Multiply up: 107374 → 1073740 (bl=2). base_level = 2.
    let expected_base: u64 = {
        let mut b = 1u64 << 30;
        for _ in 1..5 {
            b /= LEVEL_MULTIPLIER;
        }
        for _ in 1..2 {
            b = b.saturating_mul(LEVEL_MULTIPLIER);
        }
        b
    };

    // L1 gets passive LEVEL_BASE_BYTES target, not tiny 1MB.
    assert_eq!(
        targets.max_bytes[1], LEVEL_BASE_BYTES,
        "L1 should have passive LEVEL_BASE_BYTES target during empty→populated transition"
    );

    // L2 is the base_level with the data-derived target.
    assert_eq!(targets.max_bytes[2], expected_base);
    assert_eq!(targets.max_bytes[3], expected_base * 10);
    assert_eq!(targets.max_bytes[4], expected_base * 100);
    assert_eq!(targets.max_bytes[5], expected_base * 1_000);
    assert_eq!(targets.max_bytes[6], expected_base * 10_000);

    // L5 target should not exceed actual data size
    assert!(
        targets.max_bytes[5] <= 1 << 30,
        "L5 target should not exceed actual data size"
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

    let result = store.pick_and_compact(&b, CommitVersion(0)).unwrap();
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

    let result = store
        .pick_and_compact(&b, CommitVersion(0))
        .unwrap()
        .unwrap();
    assert_eq!(result.level, 0);
    assert_eq!(result.compaction.segments_merged, 4);

    // After compaction, no more work
    assert_eq!(store.l0_segment_count(&b), 0);
    assert!(store
        .pick_and_compact(&b, CommitVersion(0))
        .unwrap()
        .is_none());
}

#[test]
fn pick_and_compact_ephemeral_noop() {
    let store = SegmentedStore::new();
    let b = branch();
    seed(&store, kv_key("k"), Value::Int(1), 1);
    let result = store.pick_and_compact(&b, CommitVersion(0)).unwrap();
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

    let result = store.compact_branch(&b, CommitVersion(0)).unwrap().unwrap();
    assert_eq!(result.segments_merged, 2);
    assert_eq!(result.output_entries, 400);
    assert_eq!(result.entries_pruned, 0);
    assert_eq!(store.branch_segment_count(&b), 1);

    // Verify data integrity: spot-check first, last, and middle keys
    assert!(store
        .get_versioned(&kv_key("a000000"), CommitVersion::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("a000199"), CommitVersion::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("b000100"), CommitVersion::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("b000199"), CommitVersion::MAX)
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

    let result = store.compact_branch(&b, CommitVersion(0)).unwrap().unwrap();
    assert_eq!(result.segments_merged, 2);
    assert_eq!(result.output_entries, 200);

    assert!(store
        .get_versioned(&kv_key("x000000"), CommitVersion::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("y000099"), CommitVersion::MAX)
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
    let result = store.compact_branch(&b, CommitVersion(0)).unwrap().unwrap();
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

    let result = store
        .compact_l0_to_l1(&b, CommitVersion(0))
        .unwrap()
        .unwrap();
    assert_eq!(result.segments_merged, 4);
    assert_eq!(result.output_entries, 200);

    // Spot-check data after L0→L1 compaction
    assert!(store
        .get_versioned(&kv_key("l0seg0000000"), CommitVersion::MAX)
        .unwrap()
        .is_some());
    assert!(store
        .get_versioned(&kv_key("l0seg3000049"), CommitVersion::MAX)
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
    let result = store.compact_branch(&b, CommitVersion(6)).unwrap().unwrap();
    assert_eq!(result.segments_merged, 2);
    assert_eq!(result.output_entries, 100); // only v5 per key survives
    assert_eq!(result.entries_pruned, 100);

    // Version 5 still readable
    let val = store
        .get_versioned(&kv_key("k000050"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.version.as_u64(), 5);

    // Version 1 was pruned
    assert!(store
        .get_versioned(&kv_key("k000050"), CommitVersion(1))
        .unwrap()
        .is_none());
}
