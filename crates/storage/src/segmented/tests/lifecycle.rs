use super::*;

// =====================================================================
// Bug-fix regression tests (#1662, #1663, #1664)
// =====================================================================

/// #1663: clear_branch() must delete own segment files, manifest, and branch dir.
#[test]
fn clear_branch_deletes_own_segments_and_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write data to child and flush to produce own segments
    seed(&store, child_kv("x"), Value::Int(10), 1);
    store.rotate_memtable(&child_branch());
    store.flush_oldest_frozen(&child_branch()).unwrap();

    seed(&store, child_kv("y"), Value::Int(20), 2);
    store.rotate_memtable(&child_branch());
    store.flush_oldest_frozen(&child_branch()).unwrap();

    // Collect own segment file paths
    let child_state = store.branches.get(&child_branch()).unwrap();
    let own_paths: Vec<std::path::PathBuf> = child_state
        .version
        .load()
        .levels
        .iter()
        .flat_map(|level| level.iter().map(|s| s.file_path().to_path_buf()))
        .collect();
    drop(child_state);
    assert!(!own_paths.is_empty(), "should have own segment files");

    // Verify manifest exists
    let branch_hex = hex_encode_branch(&child_branch());
    let branch_dir = dir.path().join(&branch_hex);
    let manifest_path = branch_dir.join("segments.manifest");
    assert!(manifest_path.exists(), "manifest should exist before clear");

    // Clear the branch
    assert!(store.clear_branch(&child_branch()));

    // Own segment files must be deleted
    for path in &own_paths {
        assert!(
            !path.exists(),
            "Own segment {:?} should be deleted by clear_branch",
            path
        );
    }

    // Manifest must be deleted
    assert!(
        !manifest_path.exists(),
        "manifest should be deleted by clear_branch"
    );

    // Branch directory should be removed (was empty after file cleanup)
    assert!(
        !branch_dir.exists(),
        "branch directory should be removed by clear_branch"
    );
}

/// #1663: clear_branch() must clean up inherited segment refcounts AND own segments.
/// Inherited segment files must NOT be deleted when the parent still owns them.
#[test]
fn clear_branch_cleans_up_inherited_and_own_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Set up parent with a segment
    seed(&store, parent_kv("a"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Fork parent → child (increments refcounts on parent segments)
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Write own data to child and flush
    seed(&store, child_kv("b"), Value::Int(2), 2);
    store.rotate_memtable(&child_branch());
    store.flush_oldest_frozen(&child_branch()).unwrap();

    // Collect paths
    let child_state = store.branches.get(&child_branch()).unwrap();
    let own_paths: Vec<std::path::PathBuf> = child_state
        .version
        .load()
        .levels
        .iter()
        .flat_map(|level| level.iter().map(|s| s.file_path().to_path_buf()))
        .collect();
    let inherited_paths: Vec<std::path::PathBuf> = child_state
        .inherited_layers
        .iter()
        .flat_map(|l| l.segments.levels.iter())
        .flat_map(|level| level.iter().map(|s| s.file_path().to_path_buf()))
        .collect();
    let inherited_ids: Vec<u64> = child_state
        .inherited_layers
        .iter()
        .flat_map(|l| l.segments.levels.iter())
        .flat_map(|level| level.iter().map(|s| s.file_id()))
        .collect();
    drop(child_state);

    assert!(!own_paths.is_empty());
    assert!(!inherited_paths.is_empty());

    // Clear child — parent has NOT compacted, so inherited segment files
    // must survive (parent still owns them in its version.levels).
    assert!(store.clear_branch(&child_branch()));

    // Own segments must be gone
    for path in &own_paths {
        assert!(!path.exists(), "Own segment {:?} should be deleted", path);
    }

    // Inherited segment files must still exist (parent owns them)
    for path in &inherited_paths {
        assert!(
            path.exists(),
            "Inherited segment {:?} should still exist (parent owns it)",
            path
        );
    }

    // Refcounts should be released
    for id in &inherited_ids {
        assert!(
            !store.ref_registry.is_referenced(*id),
            "Inherited segment {} should have refcount=0 after clear",
            id
        );
    }

    // Parent can still read its data
    let result = store
        .get_versioned(&parent_kv("a"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(1));
}

/// #1664: Concurrent fork + compaction must not lose segments.
///
/// Validates the #1662 fix: refcount increments happen before the DashMap
/// source guard is dropped, so concurrent parent compaction cannot delete
/// segments that the fork just snapshotted.
#[test]
fn concurrent_fork_and_compaction_no_data_loss() {
    use std::sync::{Arc, Barrier};

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    // Write enough parent data to trigger compaction (need ≥2 L0 segments)
    for i in 0..50 {
        seed(
            &store,
            parent_kv(&format!("key{:04}", i)),
            Value::Int(i as i64),
            1,
        );
    }
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    for i in 50..100 {
        seed(
            &store,
            parent_kv(&format!("key{:04}", i)),
            Value::Int(i as i64),
            2,
        );
    }
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Ensure child branch exists
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);

    // Run fork and compaction concurrently
    let barrier = Arc::new(Barrier::new(2));

    let store_fork = Arc::clone(&store);
    let barrier_fork = Arc::clone(&barrier);
    let fork_handle = std::thread::spawn(move || {
        barrier_fork.wait();
        store_fork
            .fork_branch(&parent_branch(), &child_branch())
            .unwrap()
    });

    let store_compact = Arc::clone(&store);
    let barrier_compact = Arc::clone(&barrier);
    let compact_handle = std::thread::spawn(move || {
        barrier_compact.wait();
        store_compact.compact_branch(&parent_branch(), 0)
    });

    let (_fork_ver, segments_shared) = fork_handle.join().unwrap();
    let _compact_result = compact_handle.join().unwrap();

    assert!(segments_shared > 0, "fork should share segments");

    // The critical check: child must be able to read ALL inherited data
    // without ENOENT errors on deleted segment files.
    for i in 0..100 {
        let key = child_kv(&format!("key{:04}", i));
        let result = store.get_versioned(&key, u64::MAX).unwrap();
        assert!(
            result.is_some(),
            "child should see inherited key {:04} — segment file must not be deleted",
            i
        );
        assert_eq!(result.unwrap().value, Value::Int(i as i64));
    }
}

/// #1664: Run multiple rounds of concurrent fork + compaction to increase
/// the chance of hitting the race window.
#[test]
fn concurrent_fork_and_compaction_stress() {
    use std::sync::{Arc, Barrier};

    for round in 0..10 {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

        // Two L0 segments on parent
        for i in 0..20 {
            seed(
                &store,
                parent_kv(&format!("r{}k{:04}", round, i)),
                Value::Int(i as i64),
                1,
            );
        }
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();

        for i in 20..40 {
            seed(
                &store,
                parent_kv(&format!("r{}k{:04}", round, i)),
                Value::Int(i as i64),
                2,
            );
        }
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();

        store
            .branches
            .entry(child_branch())
            .or_insert_with(BranchState::new);

        let barrier = Arc::new(Barrier::new(2));

        let s1 = Arc::clone(&store);
        let b1 = Arc::clone(&barrier);
        let t1 = std::thread::spawn(move || {
            b1.wait();
            s1.fork_branch(&parent_branch(), &child_branch())
        });

        let s2 = Arc::clone(&store);
        let b2 = Arc::clone(&barrier);
        let t2 = std::thread::spawn(move || {
            b2.wait();
            s2.compact_branch(&parent_branch(), 0)
        });

        let fork_result = t1.join().unwrap().unwrap();
        let _compact_result = t2.join().unwrap();

        assert!(fork_result.1 > 0);

        // Verify all data readable from child
        for i in 0..40 {
            let key = child_kv(&format!("r{}k{:04}", round, i));
            let result = store.get_versioned(&key, u64::MAX).unwrap();
            assert!(
                result.is_some(),
                "round {}: child missing key {:04}",
                round,
                i
            );
        }
    }
}

// =====================================================================
// Performance benchmarks (#1672)
//
// Run with: cargo test -p strata-storage --lib -- segmented::tests::bench_ --ignored --nocapture
// =====================================================================

/// #1672: Fork 1M-key branch — target: < 100ms.
/// Verifies the O(1) fork claim holds in practice.
#[test]
#[ignore = "benchmark — run with --ignored --nocapture"]
fn bench_fork_1m_keys() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Populate parent with 1M keys across multiple segments
    let big_value = Value::Bytes(vec![0xAB; 64]);
    let batch_size = 50_000;
    let total_keys = 1_000_000;
    for batch in 0..(total_keys / batch_size) {
        for i in 0..batch_size {
            let key_idx = batch * batch_size + i;
            seed(
                &store,
                parent_kv(&format!("k{:08}", key_idx)),
                big_value.clone(),
                1,
            );
        }
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();
    }

    let parent_segs: usize = store
        .branches
        .get(&parent_branch())
        .unwrap()
        .version
        .load()
        .levels
        .iter()
        .map(|l| l.len())
        .sum();
    eprintln!("Parent: {} segments, {} keys", parent_segs, total_keys);

    // Fork
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);

    let start = std::time::Instant::now();
    let (_fv, shared) = store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();
    let elapsed = start.elapsed();

    eprintln!(
        "Fork: {} segments shared in {:.2}ms",
        shared,
        elapsed.as_secs_f64() * 1000.0
    );
    assert!(
        elapsed.as_millis() < 500,
        "Fork should be fast (was {}ms)",
        elapsed.as_millis()
    );
    assert!(shared > 0);
}

/// #1672: 100 branches forked from same parent — verify fan-out scalability.
#[test]
#[ignore = "benchmark — run with --ignored --nocapture"]
fn bench_100_branch_fanout() {
    let dir = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    // Populate parent
    for i in 0..1000 {
        seed(
            &store,
            parent_kv(&format!("k{:06}", i)),
            Value::Int(i as i64),
            1,
        );
    }
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Second segment for compaction eligibility
    for i in 1000..2000 {
        seed(
            &store,
            parent_kv(&format!("k{:06}", i)),
            Value::Int(i as i64),
            2,
        );
    }
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Fork 100 children
    let start = std::time::Instant::now();
    for c in 0..100 {
        let child = BranchId::from_bytes([c as u8 + 50; 16]);
        store.branches.entry(child).or_insert_with(BranchState::new);
        store.fork_branch(&parent_branch(), &child).unwrap();
    }
    let fork_elapsed = start.elapsed();
    eprintln!(
        "100 forks in {:.2}ms ({:.2}ms/fork)",
        fork_elapsed.as_secs_f64() * 1000.0,
        fork_elapsed.as_secs_f64() * 10.0
    );

    // Compact parent — should NOT delete shared segments
    store.compact_branch(&parent_branch(), 0).unwrap();

    // Verify all 100 children can still read
    let start = std::time::Instant::now();
    for c in 0..100u8 {
        let child = BranchId::from_bytes([c + 50; 16]);
        for i in 0..2000 {
            let key = Key::new(
                std::sync::Arc::new(strata_core::types::Namespace::new(
                    child,
                    "default".to_string(),
                )),
                strata_core::types::TypeTag::KV,
                format!("k{:06}", i).into_bytes(),
            );
            let val = store.get_versioned(&key, u64::MAX).unwrap();
            assert!(val.is_some(), "child {} missing key {}", c, i);
        }
    }
    let read_elapsed = start.elapsed();
    eprintln!(
        "200K reads across 100 children in {:.2}ms",
        read_elapsed.as_secs_f64() * 1000.0
    );
}

/// #1672: Fork chain depth trigger — fork A→B→C→D→E (4 layers on E).
#[test]
#[ignore = "benchmark — run with --ignored --nocapture"]
fn bench_fork_chain_depth() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Create chain: A → B → C → D → E
    let branches: Vec<BranchId> = (0..5)
        .map(|i| BranchId::from_bytes([i as u8 + 100; 16]))
        .collect();

    // Populate first branch
    let ns_a = std::sync::Arc::new(strata_core::types::Namespace::new(
        branches[0],
        "default".to_string(),
    ));
    for i in 0..1000 {
        seed(
            &store,
            Key::new(
                std::sync::Arc::clone(&ns_a),
                strata_core::types::TypeTag::KV,
                format!("k{:04}", i).into_bytes(),
            ),
            Value::Int(i as i64),
            1,
        );
    }
    store.rotate_memtable(&branches[0]);
    store.flush_oldest_frozen(&branches[0]).unwrap();

    // Fork chain
    for i in 1..5 {
        store
            .branches
            .entry(branches[i])
            .or_insert_with(BranchState::new);
        store.fork_branch(&branches[i - 1], &branches[i]).unwrap();
    }

    // Verify depth
    let last = store.branches.get(&branches[4]).unwrap();
    let depth = last.inherited_layers.len();
    eprintln!("Fork chain depth on E: {} layers", depth);
    assert_eq!(depth, 4, "E should have 4 inherited layers (A,B,C,D)");
    drop(last);

    // Verify E can read all data
    let ns_e = std::sync::Arc::new(strata_core::types::Namespace::new(
        branches[4],
        "default".to_string(),
    ));
    for i in 0..1000 {
        let key = Key::new(
            std::sync::Arc::clone(&ns_e),
            strata_core::types::TypeTag::KV,
            format!("k{:04}", i).into_bytes(),
        );
        let val = store.get_versioned(&key, u64::MAX).unwrap();
        assert!(val.is_some(), "E missing key {}", i);
    }

    // Materialize deepest layer on E
    let start = std::time::Instant::now();
    let result = store.materialize_layer(&branches[4], 0).unwrap();
    let elapsed = start.elapsed();
    eprintln!(
        "Materialize: {} entries in {:.2}ms, {} segments created",
        result.entries_materialized,
        elapsed.as_secs_f64() * 1000.0,
        result.segments_created
    );
    assert!(result.entries_materialized > 0);

    // Depth should be reduced
    let last = store.branches.get(&branches[4]).unwrap();
    assert_eq!(last.inherited_layers.len(), 3);
}

// =============================================================================
// #1701: Recovery skips orphan SSTs not in manifest
// =============================================================================

/// Recovery with a manifest must only load segments listed in the manifest.
/// Files on disk but not in the manifest are orphans and must be skipped.
#[test]
fn recovery_skips_orphan_sst_not_in_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write and flush parent data → creates segment + manifest
    seed(&store, parent_kv("a"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Verify 1 segment exists
    let parent_state = store.branches.get(&parent_branch()).unwrap();
    assert_eq!(parent_state.version.load().total_segment_count(), 1);
    drop(parent_state);

    // Create an orphan .sst in the parent directory (simulates a shared
    // segment kept for a child that has since been deleted)
    let branch_hex = hex_encode_branch(&parent_branch());
    let branch_dir = dir.path().join(&branch_hex);
    let orphan_path = branch_dir.join("999999.sst");
    // Write a valid segment file using the correct builder API
    let typed_key = crate::key_encoding::encode_typed_key(&parent_kv("orphan"));
    let ik = crate::key_encoding::InternalKey::from_typed_key_bytes(&typed_key, 1);
    let me = crate::memtable::MemtableEntry {
        value: Value::Int(999),
        is_tombstone: false,
        timestamp: strata_core::Timestamp::from(0u64),
        ttl_ms: 0,
        raw_value: None,
    };
    let builder = crate::segment_builder::SegmentBuilder::default();
    builder
        .build_from_iter(std::iter::once((ik, me)), &orphan_path)
        .unwrap();
    assert!(orphan_path.exists());

    // Drop store and recover
    drop(store);
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();

    // The orphan should be skipped (not loaded as L0)
    assert!(info.orphans_skipped >= 1, "expected orphan to be counted");
    let parent_state = store2.branches.get(&parent_branch()).unwrap();
    // Should have exactly 1 segment (the real one), not 2
    assert_eq!(
        parent_state.version.load().total_segment_count(),
        1,
        "orphan SST should not be loaded"
    );

    // Data from the real segment should be readable
    drop(parent_state);
    let result = store2
        .get_versioned(&parent_kv("a"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(1));
}

/// #1701: After fork → parent compaction → crash → recovery, the child's
/// inherited layer must still find the orphaned (pre-compaction) segments.
/// Without the fix, inherited layer resolution looks only at the parent's
/// active version (which skips orphans), so the child loses its data.
#[test]
fn test_issue_1701_recovery_inherited_layer_finds_orphan_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // 1. Write parent data in two flushes → 2 L0 segments
    seed(&store, parent_kv("a"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    seed(&store, parent_kv("b"), Value::Int(2), 2);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // 2. Fork parent → child (child inherits both segments)
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    let (_fork_ver, shared) = store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();
    assert!(shared >= 2, "child should share at least 2 segments");

    // Verify child can read inherited data before compaction
    let val_a = store
        .get_versioned(&child_kv("a"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val_a.value, Value::Int(1));

    // 3. Compact parent L0 → L1. Old segments kept on disk (refcount > 0).
    store
        .compact_l0_to_l1(&parent_branch(), 0)
        .unwrap()
        .expect("compaction should produce output");

    // Parent's L0 should be empty, data now in L1
    let parent_state = store.branches.get(&parent_branch()).unwrap();
    assert_eq!(
        parent_state.version.load().l0_segments().len(),
        0,
        "parent L0 should be empty after compaction"
    );
    drop(parent_state);

    // Verify child still reads correctly (in-memory Arcs are alive)
    let val_b = store
        .get_versioned(&child_kv("b"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val_b.value, Value::Int(2));

    // 4. Drop store (simulate crash) and recover
    drop(store);
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();
    assert!(info.branches_recovered >= 2, "both branches should recover");

    // 5. Child must still see inherited data after recovery
    //    This is the bug: without the fix, inherited layer resolution can't
    //    find the orphan segments because they're not in the parent's version.
    let val_a_recovered = store2.get_versioned(&child_kv("a"), u64::MAX).unwrap();
    assert!(
        val_a_recovered.is_some(),
        "child should see inherited key 'a' after recovery (orphan segment)"
    );
    assert_eq!(val_a_recovered.unwrap().value, Value::Int(1));

    let val_b_recovered = store2.get_versioned(&child_kv("b"), u64::MAX).unwrap();
    assert!(
        val_b_recovered.is_some(),
        "child should see inherited key 'b' after recovery (orphan segment)"
    );
    assert_eq!(val_b_recovered.unwrap().value, Value::Int(2));

    // Parent should still read the compacted data
    let parent_a = store2
        .get_versioned(&parent_kv("a"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(parent_a.value, Value::Int(1));
}

// =============================================================================
// #1691: Inherited-layer recovery independent of source branch state
// =============================================================================

/// Recovery of child's inherited layers must not depend on the source branch's
/// manifest being intact.  If the parent's manifest is corrupt, recovery should
/// skip the parent (not load its own segments) but still open the inherited
/// segment files directly so the child branch is unaffected.
#[test]
fn test_issue_1691_inherited_layer_recovery_independent_of_source() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // 1. Write parent data in two flushes → 2 L0 segments
    seed(&store, parent_kv("a"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    seed(&store, parent_kv("b"), Value::Int(2), 2);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // 2. Fork parent → child (child inherits both segments)
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    let (_fork_ver, shared) = store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();
    assert!(shared >= 2, "child should share at least 2 segments");

    // Verify child can read inherited data before crash
    let val_a = store
        .get_versioned(&child_kv("a"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val_a.value, Value::Int(1));

    // 3. Drop store (simulate crash)
    drop(store);

    // 4. Corrupt the PARENT's manifest — the child's manifest is intact.
    //    This simulates a scenario where the source branch's recovery state
    //    is broken, but the child's own data and the segment files are fine.
    let parent_hex = super::hex_encode_branch(&parent_branch());
    let parent_dir = dir.path().join(&parent_hex);
    let manifest_path = parent_dir.join("segments.manifest");
    let mut data = std::fs::read(&manifest_path).unwrap();
    let mid = data.len() / 2;
    data[mid] ^= 0xFF; // Flip a byte to cause CRC mismatch
    std::fs::write(&manifest_path, &data).unwrap();

    // 5. Recovery: should NOT fail entirely just because the parent's
    //    manifest is corrupt.  The child's inherited segments should be
    //    resolved directly from disk, independent of the source branch.
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store2.recover_segments().unwrap();

    // 6. Child must still see inherited data after recovery.
    let val_a_recovered = store2.get_versioned(&child_kv("a"), u64::MAX).unwrap();
    assert!(
        val_a_recovered.is_some(),
        "child should see inherited key 'a' after recovery"
    );
    assert_eq!(val_a_recovered.unwrap().value, Value::Int(1));

    let val_b_recovered = store2.get_versioned(&child_kv("b"), u64::MAX).unwrap();
    assert!(
        val_b_recovered.is_some(),
        "child should see inherited key 'b' after recovery"
    );
    assert_eq!(val_b_recovered.unwrap().value, Value::Int(2));

    // 7. The parent branch should NOT have been loaded with its own
    //    segments (corrupt manifest → skipped).  The parent's segment
    //    files still exist on disk, so the child's direct opens worked.
    assert!(
        info.corrupt_manifest_branches > 0,
        "should report parent's corrupt manifest"
    );
    // Parent branch must not have its own segments loaded.
    assert!(
        store2
            .get_versioned(&parent_kv("a"), u64::MAX)
            .unwrap()
            .is_none(),
        "parent branch own segments must not be loaded (corrupt manifest)"
    );
}

// =============================================================================
// #1703: Concurrent materialize_layer is serialized
// =============================================================================

/// Two concurrent calls to materialize_layer on the same branch should
/// not both proceed — one should return early with 0 entries.
#[test]
fn concurrent_materialize_serialized() {
    use std::sync::{Arc, Barrier};

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

    // Write parent data and flush
    for i in 0..100 {
        seed(
            &store,
            parent_kv(&format!("k{:04}", i)),
            Value::Int(i as i64),
            1,
        );
    }
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Fork → child gets inherited layer
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    assert_eq!(store.inherited_layer_count(&child_branch()), 1);

    // Race two materializations
    let barrier = Arc::new(Barrier::new(2));
    let results: Vec<_> = (0..2)
        .map(|_| {
            let s = Arc::clone(&store);
            let b = Arc::clone(&barrier);
            std::thread::spawn(move || {
                b.wait();
                s.materialize_layer(&child_branch(), 0)
            })
        })
        .collect();

    let mut materialized_count = 0;
    let mut skipped_count = 0;
    for handle in results {
        let result = handle.join().unwrap().unwrap();
        if result.entries_materialized > 0 {
            materialized_count += 1;
        } else {
            skipped_count += 1;
        }
    }

    // Exactly one should have materialized, the other should have been skipped
    assert_eq!(
        materialized_count, 1,
        "exactly one materialization should proceed"
    );
    assert_eq!(skipped_count, 1, "the other should be skipped");

    // Layer should be removed
    assert_eq!(store.inherited_layer_count(&child_branch()), 0);

    // Data should be accessible
    let result = store
        .get_versioned(&child_kv("k0050"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(50));
}

// =============================================================================
// #1702: clear_branch checks refcount before deleting own segments
// =============================================================================

/// When a parent's own segments are referenced by a child, deleting the parent
/// must not remove those segment files.
#[test]
fn clear_branch_preserves_referenced_own_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write and flush parent data
    seed(&store, parent_kv("a"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Fork → child inherits parent's segments
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Collect parent's own segment paths
    let parent_state = store.branches.get(&parent_branch()).unwrap();
    let own_paths: Vec<std::path::PathBuf> = parent_state
        .version
        .load()
        .levels
        .iter()
        .flat_map(|level| level.iter().map(|s| s.file_path().to_path_buf()))
        .collect();
    drop(parent_state);
    assert!(!own_paths.is_empty());

    // Clear (delete) parent — child still references parent's segments
    assert!(store.clear_branch(&parent_branch()));

    // Parent's own segment files should still exist (child references them)
    for path in &own_paths {
        assert!(
            path.exists(),
            "Parent segment {:?} should survive (child references it)",
            path
        );
    }

    // Child should still be able to read inherited data
    let result = store
        .get_versioned(&child_kv("a"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(1));
}

// =============================================================================
// #1705: gc_orphan_segments cleans up leaked files
// =============================================================================

/// After parent compaction removes segment S and child releases its reference,
/// gc_orphan_segments should delete the orphaned S.
#[test]
fn gc_orphan_segments_cleans_leaked_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write TWO batches to parent so compaction can merge them
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

    // Fork → child inherits 2 segments, refcount=1 each
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Collect the inherited segment paths
    let child_state = store.branches.get(&child_branch()).unwrap();
    let inherited_paths: Vec<std::path::PathBuf> = child_state
        .inherited_layers
        .iter()
        .flat_map(|l| l.segments.levels.iter())
        .flat_map(|level| level.iter().map(|s| s.file_path().to_path_buf()))
        .collect();
    drop(child_state);
    assert!(
        inherited_paths.len() >= 2,
        "expected at least 2 inherited segments"
    );

    // Parent compacts: S1 + S2 → S3. Old segments kept (refcount > 0).
    store.compact_branch(&parent_branch(), 0).unwrap();
    for path in &inherited_paths {
        assert!(
            path.exists(),
            "segment should survive compaction (refcount > 0)"
        );
    }

    // Child releases (clear_branch). refcount → 0.
    // clear_branch runs gc_orphan_segments internally, which should clean up
    // the orphaned files (parent already compacted them away, no references remain).
    store.clear_branch(&child_branch());

    // Orphaned segments should be deleted (GC ran as part of clear_branch).
    for path in &inherited_paths {
        assert!(
            !path.exists(),
            "orphaned segment {:?} should be deleted by GC after clear_branch",
            path
        );
    }

    // Calling GC again should find nothing to delete.
    let deleted = store.gc_orphan_segments();
    assert_eq!(deleted, 0, "no orphans should remain after clear_branch GC");
}

/// #1705: materialize_layer must GC orphaned segments after decrementing refcounts.
///
/// Sequence: parent has segments → fork → parent compacts (old segs kept because
/// refcount > 0) → child materializes (refcount → 0) → orphaned .sst files must
/// be cleaned up by gc_orphan_segments called from materialize_layer.
#[test]
fn test_issue_1705_materialize_layer_gc_orphan_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write TWO batches to parent so compaction can merge them
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

    // Fork → child inherits 2 segments, refcount=1 each
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Collect the inherited segment paths
    let child_state = store.branches.get(&child_branch()).unwrap();
    let inherited_paths: Vec<std::path::PathBuf> = child_state
        .inherited_layers
        .iter()
        .flat_map(|l| l.segments.levels.iter())
        .flat_map(|level| level.iter().map(|s| s.file_path().to_path_buf()))
        .collect();
    drop(child_state);
    assert!(
        inherited_paths.len() >= 2,
        "expected at least 2 inherited segments"
    );

    // Parent compacts: S1 + S2 → S3. Old segments kept (refcount > 0).
    store.compact_branch(&parent_branch(), 0).unwrap();
    for path in &inherited_paths {
        assert!(
            path.exists(),
            "segment should survive compaction (refcount > 0)"
        );
    }

    // Child materializes the inherited layer → refcount → 0.
    // materialize_layer should run gc_orphan_segments internally,
    // cleaning up the orphaned files.
    let result = store.materialize_layer(&child_branch(), 0).unwrap();
    assert!(
        result.entries_materialized > 0,
        "should materialize entries"
    );

    // Orphaned segments should be deleted (GC ran as part of materialize_layer).
    for path in &inherited_paths {
        assert!(
            !path.exists(),
            "orphaned segment {:?} should be deleted by GC after materialize_layer",
            path
        );
    }

    // Calling GC again should find nothing to delete.
    let deleted = store.gc_orphan_segments();
    assert_eq!(
        deleted, 0,
        "no orphans should remain after materialize_layer GC"
    );

    // Child should still read materialized data from its own segments
    let val = store
        .get_versioned(&child_kv("k0000"), u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(0));
}

// ===== Issue #1677: Corruption must not resurrect stale data =====

/// Regression test for issue #1677.
///
/// When a newer L0 segment's data block is corrupt, `read_data_block()` must
/// return an error — NOT `None`. Returning `None` causes the read path to
/// silently fall through to an older segment, resurrecting stale data.
#[test]
fn test_issue_1677_corruption_does_not_resurrect_stale_data() {
    use crate::segment_builder::HEADER_SIZE;

    let dir = tempfile::tempdir().unwrap();
    let seg_dir = dir.path().join("segments");
    let store = SegmentedStore::with_dir(seg_dir.clone(), 1024);
    let bid = branch();

    // 1. Write old value and flush to L0 segment
    seed(&store, kv_key("k"), Value::Int(100), 1);
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    // 2. Write newer value and flush to another L0 segment (prepended, i.e. newest-first)
    seed(&store, kv_key("k"), Value::Int(200), 2);
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    // 3. Find the segment files. The newer segment has a higher numeric ID.
    let branch_hex = hex_encode_branch(&bid);
    let branch_dir = seg_dir.join(&branch_hex);
    let mut sst_files: Vec<std::path::PathBuf> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "sst"))
        .collect();
    sst_files.sort();
    assert!(sst_files.len() >= 2, "expected at least 2 segment files");
    let newer_path = sst_files.last().unwrap();

    // 4. Corrupt the newer segment's data-block CRC.
    //    Data block starts at HEADER_SIZE. Frame: type(1) + codec(1) + reserved(2) + data_len(4) + data(N) + crc(4).
    let data = std::fs::read(newer_path).unwrap();
    let data_len =
        u32::from_le_bytes(data[HEADER_SIZE + 4..HEADER_SIZE + 8].try_into().unwrap()) as usize;
    let crc_offset = HEADER_SIZE + 8 + data_len;
    let mut corrupt = data.clone();
    corrupt[crc_offset] ^= 0xFF;
    std::fs::write(newer_path, &corrupt).unwrap();

    // 5. Read must return a corruption error, NEVER the stale value (100).
    //    Before this fix, corruption returned None which fell through to the
    //    older segment and returned Value::Int(100) — a data-correctness violation.
    let result = store.get_versioned(&kv_key("k"), u64::MAX);
    match result {
        Err(e) => {
            assert!(
                e.is_storage_error(),
                "expected a storage/corruption error, got: {e}"
            );
        }
        Ok(Some(v)) => {
            panic!(
                "corruption must not silently return data (got {:?}); \
                 stale value resurrection detected",
                v.value
            );
        }
        Ok(None) => {
            panic!(
                "corruption must not be treated as 'key absent'; \
                 this would cause stale data resurrection in multi-level reads"
            );
        }
    }
}

/// Regression test: compaction must abort when a source segment is corrupt.
///
/// Before this fix, `OwnedSegmentIter` silently returned `None` on corruption,
/// causing the compacted output to permanently lose all entries after the
/// corrupt data block.
#[test]
fn test_issue_1677_compaction_aborts_on_corrupt_segment() {
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

    // Corrupt the first segment's data-block CRC
    let branch_hex = hex_encode_branch(&bid);
    let branch_dir = seg_dir.join(&branch_hex);
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

    // Compaction must return an error, not silently drop entries
    let result = store.compact_branch(&bid, 0);
    assert!(
        result.is_err(),
        "compaction must abort on corrupt segment, not silently drop entries"
    );
}

/// Regression test: materialization shadow check must return true (shadowed)
/// on corruption, not false (unshadowed).
///
/// Returning false when corruption prevents reading a child segment would cause
/// the materializer to copy stale parent entries into the child — data resurrection.
#[test]
fn test_issue_1677_shadow_check_returns_true_on_corruption() {
    use crate::segment_builder::HEADER_SIZE;

    let dir = tempfile::tempdir().unwrap();
    let seg_dir = dir.path().join("segments");
    let store = SegmentedStore::with_dir(seg_dir.clone(), 1024);
    let bid = branch();

    // Write a key and flush to a segment
    seed(&store, kv_key("k"), Value::Int(42), 1);
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    // Corrupt the segment
    let branch_hex = hex_encode_branch(&bid);
    let branch_dir = seg_dir.join(&branch_hex);
    let sst_files: Vec<std::path::PathBuf> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "sst"))
        .collect();
    let target = &sst_files[0];

    let data = std::fs::read(target).unwrap();
    let data_len =
        u32::from_le_bytes(data[HEADER_SIZE + 4..HEADER_SIZE + 8].try_into().unwrap()) as usize;
    let crc_offset = HEADER_SIZE + 8 + data_len;
    let mut corrupt = data.clone();
    corrupt[crc_offset] ^= 0xFF;
    std::fs::write(target, &corrupt).unwrap();

    // key_exists_in_own_segments must return true (conservatively shadowed)
    // when corruption prevents reading. Returning false would cause
    // materialization to copy stale parent data.
    let branch_state = store.branches.get(&bid).unwrap();
    let ver = branch_state.version.load_full();
    let typed_key = crate::key_encoding::encode_typed_key(&kv_key("k"));
    let result = SegmentedStore::key_exists_in_own_segments(&ver, &typed_key);
    assert!(
        result,
        "shadow check must return true on corruption to prevent stale data resurrection"
    );
}

