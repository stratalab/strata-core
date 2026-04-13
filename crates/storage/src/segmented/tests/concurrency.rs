use super::*;

// =========================================================================
// Issue #1679 — fork_branch() not serialized against concurrent writes
// =========================================================================

/// Concurrent stress test: continuously write to a parent branch while calling
/// fork_branch(). The child must see every version <= fork_version with
/// no omissions.
///
/// Writers hold a shared (read) barrier across next_version() + put() to
/// mirror the engine layer's quiesce_commits() protocol (see branch_ops.rs
/// #2105/#2110).  Without this barrier, a writer can allocate version V,
/// get preempted while another writer commits V+1, then fork captures
/// fork_version = V+1 without V in the snapshot — a version-gap race.
#[test]
fn test_issue_1679_fork_concurrent_write_visibility() {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    // write_buffer_size=0 disables auto-rotation, keeping writes in the
    // memtable where the race is most likely to surface.
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    // Seed initial data so parent branch exists with segments
    seed(&store, parent_kv("init"), Value::Int(0), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    let stop = Arc::new(AtomicBool::new(false));

    // Barrier that mirrors the engine's commit_quiesce RwLock:
    // writers hold read(), fork acquires write() to drain in-flight writers
    // before snapshotting.  This ensures no version <= fork_version is
    // allocated-but-unapplied when the snapshot is taken.
    let barrier = Arc::new(parking_lot::RwLock::new(()));

    // Spawn writer threads that continuously write to the parent branch.
    // Each thread writes sequentially named keys so we can enumerate them.
    let mut writers = Vec::new();
    for t in 0..2u8 {
        let store_c = Arc::clone(&store);
        let stop_c = Arc::clone(&stop);
        let barrier_c = Arc::clone(&barrier);
        writers.push(thread::spawn(move || {
            let mut i = 0u64;
            while !stop_c.load(std::sync::atomic::Ordering::Relaxed) {
                // Hold read barrier across version allocation + write so
                // fork's write barrier drains us before snapshotting.
                let _guard = barrier_c.read();
                let v = store_c.next_version();
                let key_name = format!("w{}_k{}", t, i);
                store_c
                    .put_with_version_mode(
                        parent_kv(&key_name),
                        Value::Int(v as i64),
                        CommitVersion(v),
                        None,
                        WriteMode::Append,
                    )
                    .unwrap();
                drop(_guard);
                i += 1;
                if i % 3 == 0 {
                    thread::yield_now();
                }
            }
        }));
    }

    // Perform multiple forks while writers are active
    let mut failures = Vec::new();
    for fork_idx in 0..20u8 {
        let fork_branch_id = BranchId::from_bytes([50 + fork_idx; 16]);
        store
            .branches
            .entry(fork_branch_id)
            .or_insert_with(BranchState::new);

        // Drain in-flight writers before forking — mirrors
        // db.quiesce_commits() in the engine layer.  After this
        // returns, every allocated version has been applied, so
        // fork_version will be an accurate upper bound.
        let _quiesce = barrier.write();
        let (fork_version, _) = store
            .fork_branch(&parent_branch(), &fork_branch_id)
            .unwrap();
        drop(_quiesce);

        // For each key visible in the parent at fork_version, verify the
        // child also sees it through inheritance.
        //
        // Post-fork writes all have version > fork_version (next_version
        // is monotonically increasing), so get_versioned(parent,
        // fork_version) cannot see them — the comparison is safe even
        // while writers resume concurrently.
        let child_ns = Arc::new(Namespace::new(fork_branch_id, "default".to_string()));

        for t in 0..2u8 {
            for i in 0..50u64 {
                let key_name = format!("w{}_k{}", t, i);
                let parent_val = store
                    .get_versioned(&parent_kv(&key_name), fork_version)
                    .unwrap();
                if let Some(parent_entry) = parent_val {
                    let child_key = Key::new(
                        Arc::clone(&child_ns),
                        TypeTag::KV,
                        key_name.as_bytes().to_vec(),
                    );
                    match store.get_versioned(&child_key, CommitVersion::MAX) {
                        Ok(Some(child_entry)) => {
                            if child_entry.value != parent_entry.value {
                                failures.push(format!(
                                    "fork {}: key {} value mismatch",
                                    fork_idx, key_name
                                ));
                            }
                        }
                        Ok(None) => {
                            failures.push(format!(
                                "fork {}: key {} visible in parent@v{} but MISSING in child",
                                fork_idx, key_name, fork_version
                            ));
                        }
                        Err(e) => {
                            failures
                                .push(format!("fork {}: key {} error: {}", fork_idx, key_name, e));
                        }
                    }
                }
            }
        }

        if failures.len() > 10 {
            break;
        }
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }

    assert!(
        failures.is_empty(),
        "Child branch missed {} entries visible in parent at fork_version:\n{}",
        failures.len(),
        failures
            .iter()
            .take(10)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// =============================================================================
// #1680: Recovery must not load orphaned SSTs when manifest is corrupt
// =============================================================================

/// When a manifest file exists but is corrupt, recovery should return an error
/// rather than silently loading all SSTs (including orphans) as L0.
#[test]
fn test_issue_1680_corrupt_manifest_rejects_orphan_loading() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Flush a real segment so we get a valid manifest + SST on disk.
    seed(&store, kv_key("real"), Value::Int(1), 1);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // Create an orphan SST (simulates a compaction output that crashed
    // before the manifest was updated to include it).
    let branch_hex = super::hex_encode_branch(&branch());
    let branch_dir = dir.path().join(&branch_hex);

    // Write a second real segment to act as the orphan.
    seed(&store, kv_key("orphan"), Value::Int(999), 2);
    store.rotate_memtable(&branch());
    store.flush_oldest_frozen(&branch()).unwrap();

    // Now read the manifest, remove the orphan entry, and write it back
    // so the manifest only knows about the first segment.
    let manifest = crate::manifest::read_manifest(&branch_dir)
        .unwrap()
        .unwrap();
    assert!(
        manifest.entries.len() >= 2,
        "should have at least 2 segments"
    );
    // Keep only the first entry (the "real" segment)
    let kept_entries = vec![manifest.entries[0].clone()];
    crate::manifest::write_manifest(&branch_dir, &kept_entries, &manifest.inherited_layers)
        .unwrap();

    // Now corrupt the manifest file.
    let manifest_path = branch_dir.join("segments.manifest");
    let mut data = std::fs::read(&manifest_path).unwrap();
    // Flip a byte in the middle to cause CRC mismatch.
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    std::fs::write(&manifest_path, &data).unwrap();

    // Recovery must NOT silently load all SSTs as L0 (#1680).
    // The branch is skipped (own segments not loaded) but recovery succeeds
    // so other branches remain functional (#1691).
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert!(
        info.corrupt_manifest_branches > 0,
        "corrupt manifest must be reported, not silently load as L0"
    );
    // The orphan SST must NOT be accessible (no L0 fallback).
    let orphan_val = store2
        .get_versioned(&kv_key("orphan"), CommitVersion::MAX)
        .unwrap();
    assert!(
        orphan_val.is_none(),
        "orphan segment must not be loaded when manifest is corrupt"
    );
    // The real segment must also not be accessible (branch skipped entirely).
    let real_val = store2
        .get_versioned(&kv_key("real"), CommitVersion::MAX)
        .unwrap();
    assert!(
        real_val.is_none(),
        "no segments should be loaded for corrupt-manifest branch"
    );
}

// =====================================================================
// Issue #1682: Shared-segment deletion races fork_branch refcount
// =====================================================================

/// #1682: delete_segment_if_unreferenced must not delete a segment file
/// while a concurrent fork_branch is about to increment its refcount.
///
/// Setup: parent has 2 L0 segments. We fork a child, then clear it
/// (refcount drops to 0), then race fork + compact on the parent.
/// If the deletion barrier is missing, compaction can delete the segment
/// file between is_referenced() returning false and the new fork's
/// increment() call.
#[test]
fn test_issue_1682_segment_deletion_races_fork_refcount() {
    use std::sync::{Arc, Barrier};

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    // Populate parent with 2 L0 segments (needed for compaction).
    for i in 0..50 {
        seed(
            &store,
            parent_kv(&format!("k{:04}", i)),
            Value::Int(i as i64),
            1,
        );
    }
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    for i in 50..100 {
        seed(
            &store,
            parent_kv(&format!("k{:04}", i)),
            Value::Int(i as i64),
            2,
        );
    }
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Verify we have 2 L0 segments (needed for compaction).
    {
        let p = store.branches.get(&parent_branch()).unwrap();
        let ver = p.version.load_full();
        assert_eq!(ver.l0_segments().len(), 2, "should have 2 L0 segments");
    }

    // Fork child1 from parent → refcounts = 1 for parent segments.
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Clear child1 → refcounts back to 0.
    store.clear_branch(&child_branch());

    // Now race: fork a new child (child2) + compact parent concurrently.
    // Without the deletion barrier, compaction can see refcount=0 and
    // delete the segment file before fork increments the refcount.
    let child2 = BranchId::from_bytes([40; 16]);
    store
        .branches
        .entry(child2)
        .or_insert_with(BranchState::new);

    let barrier = Arc::new(Barrier::new(2));

    let s1 = Arc::clone(&store);
    let b1 = Arc::clone(&barrier);
    let fork_handle = std::thread::spawn(move || {
        b1.wait();
        s1.fork_branch(&parent_branch(), &child2)
    });

    let s2 = Arc::clone(&store);
    let b2 = Arc::clone(&barrier);
    let compact_handle = std::thread::spawn(move || {
        b2.wait();
        s2.compact_branch(&parent_branch(), CommitVersion(0))
    });

    let fork_result = fork_handle.join().unwrap().unwrap();
    let _compact_result = compact_handle.join().unwrap();

    assert!(fork_result.1 > 0, "fork should share segments");

    // The critical assertion: child2 must read ALL inherited data.
    // If the race hit, segment files are deleted and reads fail with ENOENT.
    let child2_ns = Arc::new(Namespace::new(
        BranchId::from_bytes([40; 16]),
        "default".to_string(),
    ));
    for i in 0..100 {
        let key = Key::new(
            Arc::clone(&child2_ns),
            TypeTag::KV,
            format!("k{:04}", i).as_bytes().to_vec(),
        );
        let result = store.get_versioned(&key, CommitVersion::MAX).unwrap();
        assert!(
            result.is_some(),
            "child2 should see inherited key {:04} — segment file must not be deleted by race",
            i
        );
    }
}

/// #1682 stress variant: run multiple rounds to increase the probability
/// of hitting the race window between is_referenced() and remove_file().
#[test]
fn test_issue_1682_segment_deletion_races_fork_refcount_concurrent() {
    use std::sync::{Arc, Barrier};

    for round in 0..20 {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

        // Two L0 segments on parent.
        for i in 0..30 {
            seed(
                &store,
                parent_kv(&format!("r{}k{:04}", round, i)),
                Value::Int(i as i64),
                1,
            );
        }
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();

        for i in 30..60 {
            seed(
                &store,
                parent_kv(&format!("r{}k{:04}", round, i)),
                Value::Int(i as i64),
                2,
            );
        }
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();

        // Fork child1 → refcount = 1, then clear → refcount = 0.
        store
            .branches
            .entry(child_branch())
            .or_insert_with(BranchState::new);
        store
            .fork_branch(&parent_branch(), &child_branch())
            .unwrap();
        store.clear_branch(&child_branch());

        // Race fork child2 + compact.
        let child2 = BranchId::from_bytes([40; 16]);
        store
            .branches
            .entry(child2)
            .or_insert_with(BranchState::new);

        let barrier = Arc::new(Barrier::new(2));

        let s1 = Arc::clone(&store);
        let b1 = Arc::clone(&barrier);
        let t1 = std::thread::spawn(move || {
            b1.wait();
            s1.fork_branch(&parent_branch(), &child2)
        });

        let s2 = Arc::clone(&store);
        let b2 = Arc::clone(&barrier);
        let t2 = std::thread::spawn(move || {
            b2.wait();
            s2.compact_branch(&parent_branch(), CommitVersion(0))
        });

        let fork_result = t1.join().unwrap().unwrap();
        let _compact = t2.join().unwrap();

        assert!(
            fork_result.1 > 0,
            "round {}: fork should share segments",
            round
        );

        // Verify all data readable from child2.
        let child2_ns = Arc::new(Namespace::new(child2, "default".to_string()));
        for i in 0..60 {
            let key = Key::new(
                Arc::clone(&child2_ns),
                TypeTag::KV,
                format!("r{}k{:04}", round, i).as_bytes().to_vec(),
            );
            let result = store.get_versioned(&key, CommitVersion::MAX).unwrap();
            assert!(
                result.is_some(),
                "round {}: child2 missing key {:04} — segment file may have been deleted by race",
                round,
                i,
            );
        }
    }
}

// ===== Issue #1698: Time-travel TTL evaluates against wall-clock now =====

/// Insert a MemtableEntry with explicit timestamp and TTL into the store.
fn seed_with_timestamp_and_ttl(
    store: &SegmentedStore,
    key: Key,
    value: Value,
    version: u64,
    timestamp: Timestamp,
    ttl_ms: u64,
) {
    let branch_id = key.namespace.branch_id;
    let branch = store
        .branches
        .entry(branch_id)
        .or_insert_with(BranchState::new);
    let entry = MemtableEntry {
        value,
        is_tombstone: false,
        timestamp,
        ttl_ms,
        raw_value: None,
    };
    branch.active.put_entry(&key, CommitVersion(version), entry);
}

#[test]
fn test_issue_1698_get_at_timestamp_ttl_uses_query_time() {
    // Scenario from issue #1698:
    // - Entry written at t=100s with TTL=60s (expires at t=160s)
    // - Query at t=120s (entry alive at this time)
    // - Query runs at wall-clock now >> 160s
    // Expected: entry returned (alive at t=120s)
    // Bug: is_expired() evaluates against now, skipping the entry
    let store = SegmentedStore::new();
    let key = kv_key("ttl_key");

    let write_ts = Timestamp::from_micros(100_000_000); // 100 seconds
    let ttl_ms = 60_000; // 60 seconds
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(42), 1, write_ts, ttl_ms);

    // Query at t=120s — entry should be alive (written at t=100, TTL=60s, expires at t=160s)
    let query_ts = 120_000_000u64; // 120 seconds in microseconds
    let result = store.get_at_timestamp(&key, query_ts).unwrap();
    assert!(
        result.is_some(),
        "Entry should be visible at t=120s (TTL expires at t=160s, not yet expired)"
    );
    assert_eq!(result.unwrap().value, Value::Int(42));
}

#[test]
fn test_issue_1698_get_at_timestamp_ttl_expired_at_query_time() {
    // Entry written at t=100s with TTL=60s (expires at t=160s)
    // Query at t=200s — entry should be expired at query time
    let store = SegmentedStore::new();
    let key = kv_key("ttl_key_expired");

    let write_ts = Timestamp::from_micros(100_000_000);
    let ttl_ms = 60_000;
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(42), 1, write_ts, ttl_ms);

    let query_ts = 200_000_000u64; // 200 seconds
    let result = store.get_at_timestamp(&key, query_ts).unwrap();
    assert!(
        result.is_none(),
        "Entry should be expired at t=200s (TTL expired at t=160s)"
    );
}

#[test]
fn test_issue_1698_list_by_type_at_timestamp_ttl_uses_query_time() {
    let store = SegmentedStore::new();
    let key = kv_key("ttl_list_key");

    let write_ts = Timestamp::from_micros(100_000_000);
    let ttl_ms = 60_000;
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(99), 1, write_ts, ttl_ms);

    // Query at t=120s — entry alive
    let query_ts = 120_000_000u64;
    let results = store.list_by_type_at_timestamp(&branch(), TypeTag::KV, query_ts);
    assert_eq!(results.len(), 1, "Entry should be visible at t=120s");
    assert_eq!(results[0].1.value, Value::Int(99));
}

#[test]
fn test_issue_1698_scan_prefix_at_timestamp_ttl_uses_query_time() {
    let store = SegmentedStore::new();
    let key = kv_key("ttl_scan_key");

    let write_ts = Timestamp::from_micros(100_000_000);
    let ttl_ms = 60_000;
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(77), 1, write_ts, ttl_ms);

    // Query at t=120s — entry alive
    let query_ts = 120_000_000u64;
    let prefix = kv_key("ttl_scan");
    let results = store.scan_prefix_at_timestamp(&prefix, query_ts).unwrap();
    assert_eq!(results.len(), 1, "Entry should be visible at t=120s");
    assert_eq!(results[0].1.value, Value::Int(77));
}

#[test]
fn test_issue_1698_list_by_type_at_timestamp_ttl_expired_at_query_time() {
    let store = SegmentedStore::new();
    let key = kv_key("ttl_list_exp");

    let write_ts = Timestamp::from_micros(100_000_000);
    let ttl_ms = 60_000;
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(99), 1, write_ts, ttl_ms);

    // Query at t=200s — entry expired at query time
    let query_ts = 200_000_000u64;
    let results = store.list_by_type_at_timestamp(&branch(), TypeTag::KV, query_ts);
    assert_eq!(results.len(), 0, "Entry should be expired at t=200s");
}

#[test]
fn test_issue_1698_scan_prefix_at_timestamp_ttl_expired_at_query_time() {
    let store = SegmentedStore::new();
    let key = kv_key("ttl_scan_exp");

    let write_ts = Timestamp::from_micros(100_000_000);
    let ttl_ms = 60_000;
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(77), 1, write_ts, ttl_ms);

    // Query at t=200s — entry expired at query time
    let query_ts = 200_000_000u64;
    let prefix = kv_key("ttl_scan");
    let results = store.scan_prefix_at_timestamp(&prefix, query_ts).unwrap();
    assert_eq!(results.len(), 0, "Entry should be expired at t=200s");
}

#[test]
fn test_issue_1698_get_at_timestamp_ttl_exact_expiry_boundary() {
    // Entry written at t=100s with TTL=60s → expires at exactly t=160s
    // Query at t=160s (exact boundary) — should be expired (>= semantics)
    let store = SegmentedStore::new();
    let key = kv_key("ttl_boundary");

    let write_ts = Timestamp::from_micros(100_000_000);
    let ttl_ms = 60_000; // 60s = 60_000_000 micros
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(42), 1, write_ts, ttl_ms);

    // Exact boundary: 100s + 60s = 160s
    let query_ts = 160_000_000u64;
    let result = store.get_at_timestamp(&key, query_ts).unwrap();
    assert!(
        result.is_none(),
        "Entry should be expired at exact boundary t=160s (>= semantics)"
    );

    // One microsecond before boundary: still alive
    let query_ts_before = 159_999_999u64;
    let result_before = store.get_at_timestamp(&key, query_ts_before).unwrap();
    assert!(
        result_before.is_some(),
        "Entry should be alive 1µs before expiry"
    );
}

/// Regression test: failed compaction must not leave partial .sst output files on disk.
///
/// When `build_from_iter` succeeds but corruption is detected (or the build itself
/// fails due to disk full / I/O error), the compaction error path must clean up any
/// output .sst or .tmp files. Otherwise, the files become orphans that waste disk
/// space — critical when the disk is already full.
#[test]
fn test_issue_1716_compact_branch_cleans_up_on_failure() {
    use crate::segment_builder::HEADER_SIZE;

    let dir = tempfile::tempdir().unwrap();
    let seg_dir = dir.path().join("segments");
    let store = SegmentedStore::with_dir(seg_dir.clone(), 1024);
    let bid = branch();

    // Flush two L0 segments (compact_branch requires >= 2)
    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    seed(&store, kv_key("b"), Value::Int(2), 2);
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    // Count .sst files before compaction attempt
    let branch_hex = hex_encode_branch(&bid);
    let branch_dir = seg_dir.join(&branch_hex);
    let count_sst_files = |dir: &std::path::Path| -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .is_some_and(|ext| ext == "sst" || ext == "tmp")
            })
            .count()
    };
    let sst_before = count_sst_files(&branch_dir);
    assert_eq!(sst_before, 2, "should have exactly 2 L0 .sst files");

    // Corrupt the first segment's data-block CRC to trigger corruption detection
    let mut sst_files: Vec<std::path::PathBuf> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "sst"))
        .collect();
    sst_files.sort();
    let target = &sst_files[0];
    let data = std::fs::read(target).unwrap();
    let data_len =
        u32::from_le_bytes(data[HEADER_SIZE + 4..HEADER_SIZE + 8].try_into().unwrap()) as usize;
    let crc_offset = HEADER_SIZE + 8 + data_len;
    let mut corrupt = data.clone();
    corrupt[crc_offset] ^= 0xFF;
    std::fs::write(target, &corrupt).unwrap();

    // Attempt compaction — should fail due to corruption
    let result = store.compact_branch(&bid, CommitVersion(0));
    assert!(result.is_err(), "compaction must fail on corrupt segment");

    // After failed compaction, no new .sst or .tmp files should remain
    let sst_after = count_sst_files(&branch_dir);
    assert_eq!(
        sst_after, sst_before,
        "failed compaction must not leave orphan .sst/.tmp files on disk \
         (before={sst_before}, after={sst_after})"
    );
}

/// Regression test: failed compact_l0_to_l1 must not leave partial output files.
///
/// Uses two L0 segments and corrupts only one, so the output file is written
/// from the good segment's data before corruption is detected by check_corruption_flags.
#[test]
fn test_issue_1716_compact_l0_to_l1_cleans_up_on_failure() {
    use crate::segment_builder::HEADER_SIZE;

    let dir = tempfile::tempdir().unwrap();
    let seg_dir = dir.path().join("segments");
    let store = SegmentedStore::with_dir(seg_dir.clone(), 1024);
    let bid = branch();

    // Flush two L0 segments so compact_l0_to_l1 has work to do and produces output
    seed(&store, kv_key("a"), Value::Int(1), 1);
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    seed(&store, kv_key("b"), Value::Int(2), 2);
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    // Count files before
    let branch_hex = hex_encode_branch(&bid);
    let branch_dir = seg_dir.join(&branch_hex);
    let count_files = |dir: &std::path::Path| -> usize {
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .is_some_and(|ext| ext == "sst" || ext == "tmp")
            })
            .count()
    };
    let files_before = count_files(&branch_dir);
    assert_eq!(files_before, 2, "should have exactly 2 L0 .sst files");

    // Corrupt only one segment so data from the other flows through to output
    let mut sst_files: Vec<std::path::PathBuf> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "sst"))
        .collect();
    sst_files.sort();
    let target = &sst_files[0];
    let data = std::fs::read(target).unwrap();
    let data_len =
        u32::from_le_bytes(data[HEADER_SIZE + 4..HEADER_SIZE + 8].try_into().unwrap()) as usize;
    let crc_offset = HEADER_SIZE + 8 + data_len;
    let mut corrupt = data.clone();
    corrupt[crc_offset] ^= 0xFF;
    std::fs::write(target, &corrupt).unwrap();

    // Attempt compact_l0_to_l1 — should fail due to corruption
    let result = store.compact_l0_to_l1(&bid, CommitVersion(0));
    assert!(
        result.is_err(),
        "compact_l0_to_l1 must fail on corrupt segment"
    );

    // No orphan files should remain
    let files_after = count_files(&branch_dir);
    assert_eq!(
        files_after, files_before,
        "failed compact_l0_to_l1 must not leave orphan files \
         (before={files_before}, after={files_after})"
    );
}

// ===== Issue #1722: TTL-expired entries don't shadow older versions in timestamp queries =====

/// When the newest version of a key is TTL-expired at the query timestamp,
/// older non-expired versions must NOT bleed through (ghost resurrection).
#[test]
fn test_issue_1722_get_at_timestamp_expired_shadows_older() {
    let store = SegmentedStore::new();
    let key = kv_key("k");

    // v1: written at t=100s, no TTL
    let ts_v1 = Timestamp::from_micros(100_000_000);
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(1), 5, ts_v1, 0);

    // v2: written at t=200s, TTL=60s (expires at t=260s)
    let ts_v2 = Timestamp::from_micros(200_000_000);
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(2), 10, ts_v2, 60_000);

    // Query at t=230s — v2 still alive, should return v2
    let pre_expiry = 230_000_000u64;
    let alive = store.get_at_timestamp(&key, pre_expiry).unwrap();
    assert_eq!(
        alive.as_ref().map(|v| &v.value),
        Some(&Value::Int(2)),
        "Before expiry, v2 should be returned",
    );

    // Query at t=300s — v2 is expired. Must NOT fall through to v1.
    let query_ts = 300_000_000u64;
    let result = store.get_at_timestamp(&key, query_ts).unwrap();
    assert!(
        result.is_none(),
        "Expired v2 must shadow v1 — got {:?} (ghost resurrection)",
        result,
    );
}

#[test]
fn test_issue_1722_list_by_type_at_timestamp_expired_shadows_older() {
    let store = SegmentedStore::new();
    let key = kv_key("k");

    // v1: written at t=100s, no TTL
    let ts_v1 = Timestamp::from_micros(100_000_000);
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(1), 5, ts_v1, 0);

    // v2: written at t=200s, TTL=60s (expires at t=260s)
    let ts_v2 = Timestamp::from_micros(200_000_000);
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(2), 10, ts_v2, 60_000);

    // Query at t=230s — v2 still alive, should appear
    let pre_expiry = 230_000_000u64;
    let alive = store.list_by_type_at_timestamp(&branch(), TypeTag::KV, pre_expiry);
    assert_eq!(alive.len(), 1, "Before expiry, v2 should appear in list");
    assert_eq!(alive[0].1.value, Value::Int(2));

    // Query at t=300s — v2 is expired. Key must not appear at all.
    let query_ts = 300_000_000u64;
    let results = store.list_by_type_at_timestamp(&branch(), TypeTag::KV, query_ts);
    assert!(
        results.is_empty(),
        "Expired v2 must shadow v1 in list — got {} result(s) (ghost resurrection)",
        results.len(),
    );
}

#[test]
fn test_issue_1722_scan_prefix_at_timestamp_expired_shadows_older() {
    let store = SegmentedStore::new();
    let key = kv_key("k");

    // v1: written at t=100s, no TTL
    let ts_v1 = Timestamp::from_micros(100_000_000);
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(1), 5, ts_v1, 0);

    // v2: written at t=200s, TTL=60s (expires at t=260s)
    let ts_v2 = Timestamp::from_micros(200_000_000);
    seed_with_timestamp_and_ttl(&store, key.clone(), Value::Int(2), 10, ts_v2, 60_000);

    // Query at t=230s — v2 still alive, should appear
    let pre_expiry = 230_000_000u64;
    let prefix = kv_key("k");
    let alive = store.scan_prefix_at_timestamp(&prefix, pre_expiry).unwrap();
    assert_eq!(alive.len(), 1, "Before expiry, v2 should appear in scan");
    assert_eq!(alive[0].1.value, Value::Int(2));

    // Query at t=300s — v2 is expired. Key must not appear at all.
    let query_ts = 300_000_000u64;
    let results = store.scan_prefix_at_timestamp(&prefix, query_ts).unwrap();
    assert!(
        results.is_empty(),
        "Expired v2 must shadow v1 in scan — got {} result(s) (ghost resurrection)",
        results.len(),
    );
}

#[test]
fn test_issue_1720_concurrent_fork_same_dest_rejected() {
    // Issue #1720: Two forks to the same destination must not both succeed.
    // The second fork should fail because the destination already has inherited layers.
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    // Set up two source branches with different data.
    let source_a = BranchId::from_bytes([10; 16]);
    let source_b = BranchId::from_bytes([20; 16]);
    let dest = BranchId::from_bytes([30; 16]);

    let ns_a = Arc::new(Namespace::new(source_a, "default".to_string()));
    let ns_b = Arc::new(Namespace::new(source_b, "default".to_string()));

    // Write data to source_a and flush to segments.
    let key_a = Key::new(ns_a, TypeTag::KV, b"key_a".to_vec());
    seed(&store, key_a, Value::Int(1), 1);
    store.rotate_memtable(&source_a);
    store.flush_oldest_frozen(&source_a).unwrap();

    // Write data to source_b and flush to segments.
    let key_b = Key::new(ns_b, TypeTag::KV, b"key_b".to_vec());
    seed(&store, key_b, Value::Int(2), 2);
    store.rotate_memtable(&source_b);
    store.flush_oldest_frozen(&source_b).unwrap();

    // First fork: source_a → dest. Should succeed.
    store.branches.entry(dest).or_insert_with(BranchState::new);
    let result1 = store.fork_branch(&source_a, &dest);
    assert!(result1.is_ok(), "First fork must succeed");

    // Second fork: source_b → dest. Must fail because dest already has inherited layers.
    let result2 = store.fork_branch(&source_b, &dest);
    assert!(
        result2.is_err(),
        "Second fork to same destination must be rejected"
    );
    assert_eq!(
        result2.unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );

    // Verify refcounts: only source_a's segments should be referenced.
    let dest_state = store.branches.get(&dest).unwrap();
    assert_eq!(
        dest_state.inherited_layers.len(),
        1,
        "Dest should have exactly one inherited layer (from source_a)"
    );
    assert_eq!(
        dest_state.inherited_layers[0].source_branch_id, source_a,
        "Inherited layer must be from the first fork's source"
    );

    // Verify source_a's segment refcounts are 1 (referenced by dest).
    for level in &dest_state.inherited_layers[0].segments.levels {
        for seg in level {
            assert_eq!(
                store.ref_registry.ref_count(seg.file_id()),
                1,
                "Winning fork's segment {} must have refcount 1",
                seg.file_id()
            );
        }
    }
    drop(dest_state);

    // Verify source_b's segments had their refcounts rolled back to 0.
    let source_b_state = store.branches.get(&source_b).unwrap();
    let source_b_ver = source_b_state.version.load();
    for level in &source_b_ver.levels {
        for seg in level {
            assert_eq!(
                store.ref_registry.ref_count(seg.file_id()),
                0,
                "Rejected fork's segment {} must have refcount 0 (rollback)",
                seg.file_id()
            );
        }
    }
}

#[test]
fn test_issue_1720_concurrent_fork_same_dest_concurrent() {
    // Stress test: many threads race to fork different sources to the same dest.
    // Exactly one must succeed; all others must fail.
    use std::sync::Barrier;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    let num_threads = 8;
    let dest = BranchId::from_bytes([99; 16]);

    // Create source branches with flushed segments.
    let sources: Vec<BranchId> = (0..num_threads)
        .map(|i| BranchId::from_bytes([i as u8 + 1; 16]))
        .collect();

    for (i, src) in sources.iter().enumerate() {
        let ns = Arc::new(Namespace::new(*src, "default".to_string()));
        let key = Key::new(ns, TypeTag::KV, format!("key_{i}").into_bytes());
        seed(&store, key, Value::Int(i as i64), (i + 1) as u64);
        store.rotate_memtable(src);
        store.flush_oldest_frozen(src).unwrap();
    }

    // Pre-create the dest entry.
    store.branches.entry(dest).or_insert_with(BranchState::new);

    let barrier = Arc::new(Barrier::new(num_threads));
    let results: Vec<_> = sources
        .iter()
        .map(|src| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let src = *src;
            std::thread::spawn(move || {
                barrier.wait();
                store.fork_branch(&src, &dest)
            })
        })
        .collect();

    let outcomes: Vec<_> = results.into_iter().map(|h| h.join().unwrap()).collect();
    let successes: Vec<_> = outcomes.iter().filter(|r| r.is_ok()).collect();
    let failures: Vec<_> = outcomes.iter().filter(|r| r.is_err()).collect();

    assert_eq!(
        successes.len(),
        1,
        "Exactly one concurrent fork must succeed, got {}",
        successes.len()
    );
    assert_eq!(failures.len(), num_threads - 1, "All other forks must fail");

    // Verify dest has exactly one set of inherited layers.
    let dest_state = store.branches.get(&dest).unwrap();
    assert_eq!(
        dest_state.inherited_layers.len(),
        1,
        "Dest must have exactly one inherited layer"
    );

    // Verify the winning source's segments have refcount 1.
    let winner = dest_state.inherited_layers[0].source_branch_id;
    for level in &dest_state.inherited_layers[0].segments.levels {
        for seg in level {
            assert_eq!(
                store.ref_registry.ref_count(seg.file_id()),
                1,
                "Winner's segment {} must have refcount 1",
                seg.file_id()
            );
        }
    }
    drop(dest_state);

    // Verify all losing sources' segments have refcount 0 (rolled back).
    for src in &sources {
        if *src == winner {
            continue;
        }
        let src_state = store.branches.get(src).unwrap();
        let src_ver = src_state.version.load();
        for level in &src_ver.levels {
            for seg in level {
                assert_eq!(
                    store.ref_registry.ref_count(seg.file_id()),
                    0,
                    "Loser {:?} segment {} must have refcount 0",
                    src,
                    seg.file_id()
                );
            }
        }
    }
}

// ===== Issue #1740: TTL not preserved through recovery =====

#[test]
fn test_issue_1740_put_recovery_entry_preserves_ttl() {
    // Write via put_recovery_entry (simulating WAL replay) with a TTL.
    // The entry should retain its TTL after recovery, not become permanent.
    let store = SegmentedStore::new();
    let key = kv_key("ttl_recovery");
    let ttl_ms = 3_600_000u64; // 1 hour — won't expire during test

    // Use current wall-clock time so the entry is not expired
    let now_us = Timestamp::now().as_micros();

    store
        .put_recovery_entry(key.clone(), Value::Int(42), CommitVersion(1), now_us, ttl_ms)
        .unwrap();

    // Read the entry via store (filters expired) — should still be alive
    let result = store
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(42));

    // Verify TTL was preserved by checking via the memtable directly
    let branch = store.branches.get(&branch()).unwrap();
    let entry = branch
        .active
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert_eq!(
        entry.ttl_ms, ttl_ms,
        "put_recovery_entry must preserve TTL, got ttl_ms={}",
        entry.ttl_ms
    );
}

#[test]
fn test_issue_1740_apply_recovery_atomic_preserves_ttl() {
    // apply_recovery_atomic is used by follower refresh and should preserve TTL.
    let store = SegmentedStore::new();
    let key = kv_key("ttl_atomic");
    let ttl_ms = 3_600_000u64; // 1 hour — won't expire during test

    let now_us = Timestamp::now().as_micros();

    let writes = vec![(key.clone(), Value::Int(99))];
    let put_ttls = vec![ttl_ms];
    let deletes = vec![];

    store
        .apply_recovery_atomic(writes, deletes, CommitVersion(1), now_us, &put_ttls)
        .unwrap();

    // Verify TTL was preserved
    let branch = store.branches.get(&branch()).unwrap();
    let entry = branch
        .active
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert_eq!(
        entry.ttl_ms, ttl_ms,
        "apply_recovery_atomic must preserve TTL, got ttl_ms={}",
        entry.ttl_ms
    );
}

// ===== Issue #1721: fork_branch must not copy Materializing status =====

/// Regression test for issue #1721 (COW-M6).
///
/// When a source branch has an inherited layer with `Materializing` status,
/// `fork_branch` must set the child's copy to `Active`. Otherwise the child
/// can never materialize that layer (the `Materializing` check returns early).
#[test]
fn test_issue_1721_fork_resets_materializing_status() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    let grandparent = BranchId::from_bytes([10; 16]);
    let gp_ns = Arc::new(Namespace::new(grandparent, "default".to_string()));

    // 1. Seed grandparent with data and flush to segments.
    for i in 0..10 {
        let key = Key::new(
            Arc::clone(&gp_ns),
            TypeTag::KV,
            format!("k{}", i).into_bytes(),
        );
        seed(&store, key, Value::Int(i as i64), 1);
    }
    store.rotate_memtable(&grandparent);
    store.flush_oldest_frozen(&grandparent).unwrap();

    // 2. Fork grandparent → parent.  Parent inherits grandparent's segments.
    let parent = BranchId::from_bytes([20; 16]);
    store
        .branches
        .entry(parent)
        .or_insert_with(BranchState::new);
    store.fork_branch(&grandparent, &parent).unwrap();

    // 3. Simulate in-progress materialization on the parent's inherited layer.
    {
        let mut branch = store.branches.get_mut(&parent).unwrap();
        assert!(
            !branch.inherited_layers.is_empty(),
            "parent should have inherited layers"
        );
        branch.inherited_layers[0].status = LayerStatus::Materializing;
    }

    // 4. Fork parent → child (grandchild).
    let child = BranchId::from_bytes([30; 16]);
    store.branches.entry(child).or_insert_with(BranchState::new);
    store.fork_branch(&parent, &child).unwrap();

    // 5. Verify ALL of child's inherited layers are Active.
    {
        let branch = store.branches.get(&child).unwrap();
        assert!(
            branch.inherited_layers.len() >= 2,
            "child should have at least 2 inherited layers"
        );
        for (i, layer) in branch.inherited_layers.iter().enumerate() {
            assert_eq!(
                layer.status,
                LayerStatus::Active,
                "child inherited layer {} should be Active, got {:?}",
                i,
                layer.status
            );
        }
    }

    // 6. Verify child can materialize layer[1] (the one that was Materializing in parent).
    //    Before the fix, materialize_layer returns early with 0 entries because
    //    it sees Materializing status and assumes materialization is already running.
    let result = store.materialize_layer(&child, 1).unwrap();
    assert!(
        result.entries_materialized > 0,
        "child must be able to materialize layer that was Materializing in parent"
    );

    // 7. Verify data is still readable through the child.
    let child_ns = Arc::new(Namespace::new(child, "default".to_string()));
    let child_key = Key::new(Arc::clone(&child_ns), TypeTag::KV, b"k0".to_vec());
    let val = store
        .get_versioned(&child_key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(0));
}

/// Issue #1718 M-13: Two concurrent `flush_oldest_frozen` calls for the same
/// branch must not install duplicate segments from the same frozen memtable.
#[test]
fn test_issue_1718_concurrent_flush_no_duplicate_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));
    let b = branch();

    // Write data and rotate to create exactly one frozen memtable.
    for i in 0..10u64 {
        store
            .put_with_version_mode(
                kv_key(&format!("k{:04}", i)),
                Value::Int(i as i64),
                CommitVersion(i + 1),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }
    store.rotate_memtable(&b);
    assert_eq!(store.branch_frozen_count(&b), 1);

    // Launch two threads that both try to flush the same frozen memtable.
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let s = Arc::clone(&store);
            let bar = Arc::clone(&barrier);
            std::thread::spawn(move || {
                bar.wait();
                s.flush_oldest_frozen(&branch()).unwrap()
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Exactly ONE segment should be installed (not two duplicates).
    let seg_count = store.branch_segment_count(&b);
    assert_eq!(
        seg_count, 1,
        "Expected 1 segment from the single frozen memtable, got {} (duplicate detected)",
        seg_count
    );

    // The frozen list should be empty (memtable was consumed).
    assert_eq!(store.branch_frozen_count(&b), 0);

    // All data is readable.
    for i in 0..10u64 {
        let result = store
            .get_versioned(&kv_key(&format!("k{:04}", i)), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }
}

/// Issue #1718 M-13 stress: Many concurrent flushes must not produce duplicates.
#[test]
fn test_issue_1718_concurrent_flush_no_duplicate_segments_stress() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));
    let b = branch();

    // Create 3 frozen memtables.
    for batch in 0..3u64 {
        for i in 0..5u64 {
            let key_idx = batch * 5 + i;
            store
                .put_with_version_mode(
                    kv_key(&format!("k{:04}", key_idx)),
                    Value::Int(key_idx as i64),
                    CommitVersion(key_idx + 1),
                    None,
                    WriteMode::Append,
                )
                .unwrap();
        }
        store.rotate_memtable(&b);
    }
    assert_eq!(store.branch_frozen_count(&b), 3);

    // Launch 8 threads all racing to flush.
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let s = Arc::clone(&store);
            let bar = Arc::clone(&barrier);
            std::thread::spawn(move || {
                bar.wait();
                while s.flush_oldest_frozen(&branch()).unwrap() {}
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Exactly 3 segments (one per frozen memtable), no duplicates.
    let seg_count = store.branch_segment_count(&b);
    assert_eq!(
        seg_count, 3,
        "Expected 3 segments from 3 frozen memtables, got {} (duplicates detected)",
        seg_count
    );
    assert_eq!(store.branch_frozen_count(&b), 0);

    // All data readable.
    for i in 0..15u64 {
        let result = store
            .get_versioned(&kv_key(&format!("k{:04}", i)), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(i as i64));
    }
}

/// #1694: Materialization shadow detection must consider child memtable writes.
///
/// If a child writes a key to its memtable (not yet flushed) that shadows an
/// inherited key, the materializer should recognize the shadow and skip the
/// inherited entry. Without the fix, the unflushed memtable write is invisible
/// to shadow detection, causing unnecessary materialization.
#[test]
fn test_issue_1694_materialize_shadow_detection_ignores_memtable() {
    let (_dir, store) = setup_parent_with_segments(&[("a", 1, 1), ("b", 2, 2)]);

    // Fork child from parent — child inherits both "a" and "b"
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();
    assert_eq!(store.inherited_layer_count(&child_branch()), 1);

    // Write "a" to child's memtable at a newer version — shadows inherited "a".
    // Crucially, do NOT flush — the write stays in the active memtable.
    seed(&store, child_kv("a"), Value::Int(999), 10);

    // Verify the memtable write is in the active memtable (not flushed)
    assert!(!store.has_frozen(&child_branch()));
    assert_eq!(store.branch_segment_count(&child_branch()), 0);

    // Materialize the inherited layer
    let result = store.materialize_layer(&child_branch(), 0).unwrap();

    // Only "b" should be materialized — "a" is shadowed by the child's memtable write.
    assert_eq!(
        result.entries_materialized, 1,
        "Only 'b' should be materialized; 'a' is shadowed by the child's memtable write"
    );

    // Verify reads return the child's value for "a" and the materialized value for "b"
    let val_a = store
        .get_versioned(&child_kv("a"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val_a.value, Value::Int(999), "child's memtable write wins");

    let val_b = store
        .get_versioned(&child_kv("b"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val_b.value, Value::Int(2), "inherited 'b' is materialized");
}

/// #1694 variant: shadow detection must also see frozen (unflushed) memtables.
#[test]
fn test_issue_1694_materialize_shadow_detection_ignores_frozen_memtable() {
    let (_dir, store) = setup_parent_with_segments(&[("x", 10, 1), ("y", 20, 2)]);

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Write "x" to child, then rotate so it lands in a frozen memtable (not flushed).
    seed(&store, child_kv("x"), Value::Int(777), 10);
    store.rotate_memtable(&child_branch());
    assert!(store.has_frozen(&child_branch()));
    assert_eq!(store.branch_segment_count(&child_branch()), 0);

    let result = store.materialize_layer(&child_branch(), 0).unwrap();

    // Only "y" should be materialized — "x" is shadowed by the frozen memtable.
    assert_eq!(
        result.entries_materialized, 1,
        "Only 'y' should be materialized; 'x' is shadowed by child's frozen memtable"
    );

    let val_x = store
        .get_versioned(&child_kv("x"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val_x.value, Value::Int(777), "child's frozen write wins");
}

/// Issue #1734: apply_recovery_atomic must NOT bump the visible storage version.
///
/// The follower refresh path calls apply_recovery_atomic to install KV entries,
/// then updates secondary indexes (BM25/HNSW), and only THEN should the version
/// advance. If the version advances inside apply_recovery_atomic, a concurrent
/// reader on the follower can see committed vectors via KV get() but miss them
/// in search() because the HNSW backend hasn't been updated yet.
#[test]
fn test_issue_1734_apply_recovery_atomic_does_not_bump_version() {
    let store = SegmentedStore::new();

    // Seed an initial entry so the store has version 1
    seed(&store, kv_key("existing"), Value::Int(1), 1);
    assert_eq!(
        store.version(),
        1,
        "precondition: version should be 1 after seed"
    );

    // apply_recovery_atomic installs entries at version 5.
    // CORRECT BEHAVIOR: the storage version should remain at 1 (caller bumps later).
    let writes = vec![(kv_key("new_key"), Value::String("hello".into()))];
    store
        .apply_recovery_atomic(writes, vec![], CommitVersion(5), 1_000_000, &[0])
        .unwrap();

    assert_eq!(
        store.version(),
        1,
        "apply_recovery_atomic must NOT advance the visible version; \
         the caller (refresh) is responsible for bumping it after secondary indexes are updated"
    );

    // The entry IS in the memtable but should be invisible at the current version (1)
    let invisible = store
        .get_versioned(&kv_key("new_key"), CommitVersion(store.version()))
        .unwrap();
    assert!(
        invisible.is_none(),
        "entry at version 5 should be invisible when storage version is 1"
    );

    // After the caller bumps the version, the entry becomes visible
    store.advance_version(CommitVersion(5));
    assert_eq!(store.version(), 5);

    let visible = store
        .get_versioned(&kv_key("new_key"), CommitVersion(store.version()))
        .unwrap();
    assert!(
        visible.is_some(),
        "entry should be visible after advance_version(5)"
    );
    assert_eq!(visible.unwrap().value, Value::String("hello".into()));
}

// =====================================================================
// Issue #2110 / #2105: Fork version gap — deterministic reproduction
// =====================================================================

/// Deterministic proof that fork_branch can produce a child whose
/// fork_version includes a version whose data was never captured.
///
/// Uses `mpsc` channels to force this exact interleaving:
///
///   1. Writer: next_version()            → allocates V, global counter = V
///   2. Writer: signals "allocated"
///   3. Fork:   receives signal
///   4. Fork:   fork_branch(parent, child)
///      → acquires DashMap lock
///      → inline-flushes memtable (does NOT contain V's data)
///      → fork_version = self.version.load() = V
///      → captures snapshot, releases lock
///   5. Fork:   signals "forked"
///   6. Writer: receives signal
///   7. Writer: put_with_version_mode(V)  → data at V enters parent's memtable
///
/// Result: fork_version >= V but child is missing V's data.
///
/// NOTE: This test asserts the BUG exists at the storage level. When the
/// engine-level fix (#2105) is applied (quiescing commits before fork),
/// this storage-level race is prevented by the engine, but the raw
/// storage API still exhibits it.
#[test]
fn test_issue_2110_fork_version_gap_deterministic() {
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    // Seed parent branch with initial data so it exists with segments
    seed(&store, parent_kv("init"), Value::Int(0), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Channels for deterministic ordering
    let (tx_allocated, rx_allocated) = mpsc::channel::<u64>();
    let (tx_forked, rx_forked) = mpsc::channel::<()>();

    let store_writer = Arc::clone(&store);
    let store_forker = Arc::clone(&store);

    // Writer thread: allocate version, signal, wait for fork, then write data
    let writer = thread::spawn(move || {
        // Step 1: allocate version (advances self.version but no data yet)
        let v = store_writer.next_version();
        // Step 2: signal the forker that version is allocated
        tx_allocated.send(v).unwrap();
        // Step 6: wait for fork to complete
        rx_forked.recv().unwrap();
        // Step 7: NOW write the data (too late for the fork to capture)
        store_writer
            .put_with_version_mode(
                parent_kv("gap_key"),
                Value::Int(999),
                CommitVersion(v),
                None,
                WriteMode::Append,
            )
            .unwrap();
        v
    });

    // Forker thread: wait for version allocation, then fork
    let fork_branch_id = BranchId::from_bytes([77; 16]);
    let forker = thread::spawn(move || {
        store_forker
            .branches
            .entry(fork_branch_id)
            .or_insert_with(BranchState::new);

        // Step 3: wait for writer to allocate version
        let allocated_version = rx_allocated.recv().unwrap();
        // Step 4: fork (captures memtable that does NOT contain gap_key)
        let (fork_version, _) = store_forker
            .fork_branch(&parent_branch(), &fork_branch_id)
            .unwrap();
        // Step 5: signal writer that fork is done
        tx_forked.send(()).unwrap();
        (fork_version, allocated_version)
    });

    let writer_version = writer.join().unwrap();
    let (fork_version, allocated_version) = forker.join().unwrap();

    assert_eq!(writer_version, allocated_version);

    // After the fix: fork_version uses source.max_version (the highest
    // version actually applied to the branch), NOT the global counter.
    // The writer allocated a version via next_version() but hadn't applied
    // it yet, so fork_version correctly excludes it.
    assert!(
        fork_version.as_u64() < allocated_version,
        "fork_version ({}) should be < allocated version ({}) — \
         fork_branch now uses per-branch max_version, not the global counter",
        fork_version.as_u64(),
        allocated_version,
    );

    // Parent has the data (writer wrote it after fork)
    let parent_val = store
        .get_versioned(&parent_kv("gap_key"), CommitVersion::MAX)
        .unwrap();
    assert_eq!(
        parent_val.map(|e| e.value),
        Some(Value::Int(999)),
        "parent must have the gap_key data"
    );

    // Child correctly does NOT include gap_key — fork_version < allocated_version,
    // so the fork doesn't claim to include data it doesn't have.
    let child_ns = Arc::new(Namespace::new(fork_branch_id, "default".to_string()));
    let child_key = Key::new(child_ns, TypeTag::KV, b"gap_key".to_vec());
    let child_val = store.get_versioned(&child_key, CommitVersion::MAX).unwrap();
    assert!(
        child_val.is_none(),
        "child should not have gap_key (fork_version={} < allocated_version={})",
        fork_version,
        allocated_version,
    );
}
