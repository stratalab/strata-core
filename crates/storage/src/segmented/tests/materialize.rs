use super::*;

// =====================================================================
// Materialization tests
// =====================================================================

#[test]
fn compaction_preserves_shared_segments() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write parent data and flush to 2 segments (to trigger compaction)
    seed(&store, parent_kv("a"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    seed(&store, parent_kv("b"), Value::Int(2), 2);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Fork parent → child
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Collect shared segment file paths
    let child_state = store.branches.get(&child_branch()).unwrap();
    let shared_paths: Vec<std::path::PathBuf> = child_state
        .inherited_layers
        .iter()
        .flat_map(|l| l.segments.levels.iter())
        .flat_map(|level| level.iter().map(|s| s.file_path().to_path_buf()))
        .collect();
    drop(child_state);

    assert!(!shared_paths.is_empty(), "should have shared segment files");

    // Compact parent — should NOT delete shared segments
    store
        .compact_branch(&parent_branch(), CommitVersion(0))
        .unwrap();

    // Verify shared segment files still exist
    for path in &shared_paths {
        assert!(
            path.exists(),
            "Shared segment {:?} should not be deleted by parent compaction",
            path
        );
    }

    // Child can still read inherited data
    let result = store
        .get_versioned(&child_kv("a"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(1));
}

#[test]
fn materialize_collapses_layer() {
    let (_dir, store) = setup_parent_with_segments(&[("a", 1, 1), ("b", 2, 2)]);

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    assert_eq!(store.inherited_layer_count(&child_branch()), 1);

    let result = store.materialize_layer(&child_branch(), 0).unwrap();
    assert_eq!(result.entries_materialized, 2);
    assert_eq!(result.segments_created, 1);

    // Layer removed
    assert_eq!(store.inherited_layer_count(&child_branch()), 0);

    // Data still accessible in own segments
    let val = store
        .get_versioned(&child_kv("a"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(1));

    let val = store
        .get_versioned(&child_kv("b"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(2));
}

#[test]
fn materialize_preserves_commit_ids() {
    let (_dir, store) = setup_parent_with_segments(&[("k", 42, 5)]);

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    store.materialize_layer(&child_branch(), 0).unwrap();

    let val = store
        .get_versioned(&child_kv("k"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(42));
    assert_eq!(val.version.as_u64(), 5, "commit_id should be preserved");
}

#[test]
fn materialize_skips_post_fork_entries() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write entries at versions 1 and 100 into the same segment
    seed(&store, parent_kv("early"), Value::Int(1), 1);
    seed(&store, parent_kv("late"), Value::Int(2), 100);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Manually attach inherited layer with fork_version=50
    // so "early" (version 1 <= 50) passes but "late" (version 100 > 50) is filtered
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 50);

    // Increment refcounts for shared segments
    {
        let child = store.branches.get(&child_branch()).unwrap();
        for layer in &child.inherited_layers {
            for level in &layer.segments.levels {
                for seg in level {
                    store.ref_registry.increment(seg.file_id());
                }
            }
        }
    }

    store.materialize_layer(&child_branch(), 0).unwrap();

    // "early" (version 1 <= 50) should be materialized
    let val = store
        .get_versioned(&child_kv("early"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(1));

    // "late" (version 100 > 50) should NOT be materialized
    assert!(
        store
            .get_versioned(&child_kv("late"), CommitVersion::MAX)
            .unwrap()
            .is_none(),
        "post-fork entry should not be materialized"
    );
}

#[test]
fn materialize_skips_shadowed_by_own() {
    let (_dir, store) = setup_parent_with_segments(&[("k", 1, 1)]);

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Write to child's own data (must be flushed to segments for shadow detection)
    seed(&store, child_kv("k"), Value::Int(999), 11);
    store.rotate_memtable(&child_branch());
    store.flush_oldest_frozen(&child_branch()).unwrap();

    let result = store.materialize_layer(&child_branch(), 0).unwrap();
    assert_eq!(
        result.entries_materialized, 0,
        "inherited entry should be shadowed by own"
    );

    // Value should be child's own
    let val = store
        .get_versioned(&child_kv("k"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(999));
}

#[test]
fn materialize_skips_shadowed_by_closer_layer() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Grandparent has key "k"
    let grandparent = BranchId::from_bytes([30; 16]);
    let gp_ns = Arc::new(Namespace::new(grandparent, "default".to_string()));
    let gp_key = Key::new(gp_ns.clone(), TypeTag::KV, b"k".to_vec());
    seed(&store, gp_key, Value::Int(1), 1);
    store.rotate_memtable(&grandparent);
    store.flush_oldest_frozen(&grandparent).unwrap();

    // Parent also has key "k" (different value)
    seed(&store, parent_kv("k"), Value::Int(2), 2);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Child has two inherited layers:
    // layer 0 (closer): parent with "k"=2
    // layer 1 (deeper): grandparent with "k"=1
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);

    // Attach layers in order: closer first
    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);
    attach_inherited_layer(&store, grandparent, child_branch(), 10);

    // Increment refcounts
    {
        let child = store.branches.get(&child_branch()).unwrap();
        for layer in &child.inherited_layers {
            for level in &layer.segments.levels {
                for seg in level {
                    store.ref_registry.increment(seg.file_id());
                }
            }
        }
    }

    // Materialize deepest layer (index 1 = grandparent)
    let result = store.materialize_layer(&child_branch(), 1).unwrap();
    assert_eq!(
        result.entries_materialized, 0,
        "grandparent's 'k' should be shadowed by closer parent layer"
    );
}

#[test]
fn materialize_manifest_publish_failure_restores_active_layer_status() {
    crate::test_hooks::clear_manifest_publish_failure();

    let (_dir, store) = setup_parent_with_segments(&[("a", 1, 1)]);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    crate::test_hooks::inject_manifest_publish_failure(std::io::ErrorKind::Other);
    let err = store.materialize_layer(&child_branch(), 0).unwrap_err();
    assert!(matches!(err, crate::StorageError::ManifestPublish { .. }));

    let child = store.branches.get(&child_branch()).unwrap();
    assert_eq!(child.inherited_layers.len(), 1);
    assert_eq!(child.inherited_layers[0].status, LayerStatus::Active);
    assert!(store.publish_health().is_none());
}

#[test]
fn materialize_dir_fsync_failure_is_forward_only_and_latches_health() {
    crate::test_hooks::clear_manifest_dir_fsync_failure();

    let (_dir, store) = setup_parent_with_segments(&[("a", 1, 1), ("b", 2, 2)]);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    crate::test_hooks::inject_manifest_dir_fsync_failure(std::io::ErrorKind::Other);
    let result = store.materialize_layer(&child_branch(), 0).unwrap();
    assert_eq!(result.entries_materialized, 2);
    assert_eq!(store.inherited_layer_count(&child_branch()), 0);
    assert!(store.publish_health().is_some());
}

#[test]
fn materialize_deepest_first() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // A → B → C chain
    let branch_a = BranchId::from_bytes([30; 16]);
    let branch_b = parent_branch();
    let branch_c = child_branch();

    let ns_a = Arc::new(Namespace::new(branch_a, "default".to_string()));

    // A has "a_key"
    seed(
        &store,
        Key::new(ns_a.clone(), TypeTag::KV, b"a_key".to_vec()),
        Value::Int(1),
        1,
    );
    store.rotate_memtable(&branch_a);
    store.flush_oldest_frozen(&branch_a).unwrap();

    // B has "b_key"
    seed(&store, parent_kv("b_key"), Value::Int(2), 2);
    store.rotate_memtable(&branch_b);
    store.flush_oldest_frozen(&branch_b).unwrap();

    // C inherits from [B, A] (closer first)
    store
        .branches
        .entry(branch_c)
        .or_insert_with(BranchState::new);
    attach_inherited_layer(&store, branch_b, branch_c, 10);
    attach_inherited_layer(&store, branch_a, branch_c, 10);

    // Increment refcounts
    {
        let child = store.branches.get(&branch_c).unwrap();
        for layer in &child.inherited_layers {
            for level in &layer.segments.levels {
                for seg in level {
                    store.ref_registry.increment(seg.file_id());
                }
            }
        }
    }

    assert_eq!(store.inherited_layer_count(&branch_c), 2);

    // Materialize deepest (index 1 = A)
    let r1 = store.materialize_layer(&branch_c, 1).unwrap();
    assert_eq!(r1.entries_materialized, 1); // "a_key"
    assert_eq!(store.inherited_layer_count(&branch_c), 1);

    // Materialize remaining (index 0 = B)
    let r2 = store.materialize_layer(&branch_c, 0).unwrap();
    assert_eq!(r2.entries_materialized, 1); // "b_key"
    assert_eq!(store.inherited_layer_count(&branch_c), 0);

    // All data accessible
    let val = store
        .get_versioned(&child_kv("a_key"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(1));

    let val = store
        .get_versioned(&child_kv("b_key"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(2));
}

#[test]
fn materialize_empty_layer() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Parent branch with no segments (empty)
    store
        .branches
        .entry(parent_branch())
        .or_insert_with(BranchState::new);
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);

    // Attach empty inherited layer
    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    let result = store.materialize_layer(&child_branch(), 0).unwrap();
    assert_eq!(result.entries_materialized, 0);
    assert_eq!(result.segments_created, 0);
    assert_eq!(store.inherited_layer_count(&child_branch()), 0);
}

#[test]
fn materialize_refcount_decremented() {
    let (_dir, store) = setup_parent_with_segments(&[("a", 1, 1)]);

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Collect segment IDs and check initial refcounts
    let seg_ids: Vec<u64> = {
        let child = store.branches.get(&child_branch()).unwrap();
        child
            .inherited_layers
            .iter()
            .flat_map(|l| l.segments.levels.iter())
            .flat_map(|level| level.iter().map(|s| s.file_id()))
            .collect()
    };

    for &id in &seg_ids {
        assert!(
            store.ref_registry.is_referenced(id),
            "segment {} should be referenced before materialize",
            id
        );
    }

    store.materialize_layer(&child_branch(), 0).unwrap();

    // After materialization, refcounts should be decremented
    // (If no other child references them, they reach 0)
    for &id in &seg_ids {
        assert!(
            !store.ref_registry.is_referenced(id),
            "segment {} refcount should be decremented after materialize",
            id
        );
    }
}

#[test]
fn materialize_preserves_read_path() {
    let (_dir, store) = setup_parent_with_segments(&[("x", 10, 1), ("y", 20, 2), ("z", 30, 3)]);

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Snapshot reads before materialization
    let before: Vec<(String, Value)> = ["x", "y", "z"]
        .iter()
        .filter_map(|k| {
            store
                .get_versioned(&child_kv(k), CommitVersion::MAX)
                .unwrap()
                .map(|v| (k.to_string(), v.value))
        })
        .collect();

    store.materialize_layer(&child_branch(), 0).unwrap();

    // Reads after materialization should return same data
    let after: Vec<(String, Value)> = ["x", "y", "z"]
        .iter()
        .filter_map(|k| {
            store
                .get_versioned(&child_kv(k), CommitVersion::MAX)
                .unwrap()
                .map(|v| (k.to_string(), v.value))
        })
        .collect();

    assert_eq!(
        before, after,
        "reads must be identical before and after materialization"
    );
}

#[test]
fn materialize_crash_recovery_resets_status() {
    let dir = tempfile::tempdir().unwrap();

    // First store: fork and start materializing (write manifest with Materializing status)
    {
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        seed(&store, parent_kv("a"), Value::Int(1), 1);
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();

        store
            .branches
            .entry(child_branch())
            .or_insert_with(BranchState::new);
        store
            .fork_branch(&parent_branch(), &child_branch())
            .unwrap();

        // Manually set status to Materializing and write manifest
        {
            let mut child = store.branches.get_mut(&child_branch()).unwrap();
            child.inherited_layers[0].status = LayerStatus::Materializing;
        }
        store.write_branch_manifest(&child_branch()).unwrap();
    }

    // Second store: recover from manifest — Materializing should reset to Active
    {
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        store.recover_segments().unwrap();

        let child = store.branches.get(&child_branch()).unwrap();
        assert_eq!(
            child.inherited_layers.len(),
            1,
            "layer should still exist after crash"
        );
        assert_eq!(
            child.inherited_layers[0].status,
            LayerStatus::Active,
            "Materializing status should reset to Active on recovery"
        );
    }
}

/// #1668: Recovery after crash during materialization I/O.
///
/// Simulates a crash after the new `.sst` segment has been partially
/// written but before the inherited layer was removed from BranchState.
/// On recovery: Materializing status resets to Active, reads return
/// correct data (dedup handles the duplicate entries).
#[test]
fn materialize_crash_recovery_with_partial_segment() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Set up fork, begin materialization (simulate crash mid-I/O)
    {
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        // Parent with data
        seed(&store, parent_kv("a"), Value::Int(1), 1);
        seed(&store, parent_kv("b"), Value::Int(2), 2);
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();

        // Fork parent → child
        store
            .branches
            .entry(child_branch())
            .or_insert_with(BranchState::new);
        store
            .fork_branch(&parent_branch(), &child_branch())
            .unwrap();

        // Simulate partial materialization:
        // 1. Write an orphan .sst file in the child's branch dir
        //    (as if the segment was written but install step never ran)
        let child_hex = hex_encode_branch(&child_branch());
        let child_dir = dir.path().join(&child_hex);
        let orphan_path = child_dir.join("999999.sst");
        // Write a small dummy file (not a valid segment — recovery should
        // skip invalid files gracefully)
        std::fs::write(&orphan_path, b"partial garbage").unwrap();

        // 2. Mark the layer as Materializing in the manifest
        {
            let mut child = store.branches.get_mut(&child_branch()).unwrap();
            child.inherited_layers[0].status = LayerStatus::Materializing;
        }
        store.write_branch_manifest(&child_branch()).unwrap();

        // "crash" — store drops
    }

    // Phase 2: Recover and verify
    {
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let info = store.recover_segments().unwrap();
        assert!(info.branches_recovered >= 2, "both branches should recover");
        let corrupt_segment_count = match &info.health {
            super::RecoveryHealth::Degraded { faults, .. } => faults
                .iter()
                .filter(|f| matches!(f, super::RecoveryFault::CorruptSegment { .. }))
                .count(),
            super::RecoveryHealth::Healthy => 0,
        };
        assert!(
            corrupt_segment_count >= 1,
            "corrupt orphan .sst should produce a CorruptSegment fault"
        );

        // Materializing status should be reset to Active
        let child = store.branches.get(&child_branch()).unwrap();
        assert_eq!(
            child.inherited_layers.len(),
            1,
            "inherited layer should still exist"
        );
        assert_eq!(
            child.inherited_layers[0].status,
            LayerStatus::Active,
            "Materializing should reset to Active on recovery"
        );
        drop(child);

        // Data should be readable through inherited layers
        let a = store
            .get_versioned(&child_kv("a"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(a.value, Value::Int(1));

        let b = store
            .get_versioned(&child_kv("b"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(b.value, Value::Int(2));

        // Parent data should also be intact
        let pa = store
            .get_versioned(&parent_kv("a"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(pa.value, Value::Int(1));
    }
}

/// #1668: Recovery with a valid orphan segment (crash after segment write,
/// before layer removal). The orphan segment gets loaded into the child's
/// own version during recovery, and reads still succeed via MVCC dedup.
#[test]
fn materialize_crash_recovery_with_valid_orphan_segment() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Create a real scenario where a valid segment exists
    // alongside inherited layers
    {
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        // Parent with data
        seed(&store, parent_kv("x"), Value::Int(10), 1);
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();

        // Fork parent → child
        store
            .branches
            .entry(child_branch())
            .or_insert_with(BranchState::new);
        store
            .fork_branch(&parent_branch(), &child_branch())
            .unwrap();

        // Child writes own data and flushes (creates a real .sst in child dir)
        seed(&store, child_kv("y"), Value::Int(20), 2);
        store.rotate_memtable(&child_branch());
        store.flush_oldest_frozen(&child_branch()).unwrap();

        // Mark Materializing (simulating mid-materialization crash)
        {
            let mut child = store.branches.get_mut(&child_branch()).unwrap();
            child.inherited_layers[0].status = LayerStatus::Materializing;
        }
        store.write_branch_manifest(&child_branch()).unwrap();
    }

    // Phase 2: Recover
    {
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        store.recover_segments().unwrap();

        let child = store.branches.get(&child_branch()).unwrap();
        assert_eq!(child.inherited_layers[0].status, LayerStatus::Active);
        drop(child);

        // Child's own data (from the valid segment) should be readable
        let y = store
            .get_versioned(&child_kv("y"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(y.value, Value::Int(20));

        // Inherited data from parent should also be readable
        let x = store
            .get_versioned(&child_kv("x"), CommitVersion::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(x.value, Value::Int(10));
    }
}

/// #1670: Recovery surfaces dropped inherited layers when source branch is missing.
#[test]
fn recovery_surfaces_missing_source_branch() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Create parent and fork child
    {
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        seed(&store, parent_kv("a"), Value::Int(1), 1);
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();

        store
            .branches
            .entry(child_branch())
            .or_insert_with(BranchState::new);
        store
            .fork_branch(&parent_branch(), &child_branch())
            .unwrap();

        // Write manifest for both branches
        store.write_branch_manifest(&parent_branch()).unwrap();
        store.write_branch_manifest(&child_branch()).unwrap();
    }

    // Sabotage: delete the parent's branch directory (simulating missing source)
    let parent_hex = hex_encode_branch(&parent_branch());
    let parent_dir = dir.path().join(&parent_hex);
    std::fs::remove_dir_all(&parent_dir).unwrap();

    // Phase 2: Recover — child's inherited layer should be dropped
    {
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let info = store.recover_segments().unwrap();

        // An InheritedLayerLost fault should report the missing parent.
        let faults = match &info.health {
            super::RecoveryHealth::Degraded { faults, class } => {
                assert_eq!(
                    *class,
                    super::DegradationClass::DataLoss,
                    "inherited-layer loss classifies as DataLoss"
                );
                &faults[..]
            }
            super::RecoveryHealth::Healthy => &[],
        };
        let dropped = faults.iter().find_map(|f| match f {
            super::RecoveryFault::InheritedLayerLost {
                child,
                source_branch,
            } => Some((*child, *source_branch)),
            _ => None,
        });
        assert_eq!(
            dropped,
            Some((child_branch(), parent_branch())),
            "should report dropped inherited layer"
        );
        assert_eq!(
            info.branches_recovered, 0,
            "child with no own segments and no recoverable inherited entries must not be counted"
        );

        // Child should still exist but with no inherited layers
        let child = store.branches.get(&child_branch()).unwrap();
        assert!(
            child.inherited_layers.is_empty(),
            "inherited layer should be dropped (source missing)"
        );
    }
}

/// SE2 follow-up: if an inherited layer partially loads, recovery must still
/// surface `InheritedLayerLost` rather than silently accepting a truncated
/// child view. The child may keep the surviving inherited entries, but the
/// authoritative loss must be visible in the typed fault set.
#[test]
fn recovery_surfaces_partial_inherited_layer_loss() {
    let dir = tempfile::tempdir().unwrap();

    {
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        seed(&store, parent_kv("a"), Value::Int(1), 1);
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();

        seed(&store, parent_kv("b"), Value::Int(2), 2);
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();

        store
            .branches
            .entry(child_branch())
            .or_insert_with(BranchState::new);
        store
            .fork_branch(&parent_branch(), &child_branch())
            .unwrap();
        store.write_branch_manifest(&parent_branch()).unwrap();
        store.write_branch_manifest(&child_branch()).unwrap();
    }

    let parent_hex = hex_encode_branch(&parent_branch());
    let parent_dir = dir.path().join(&parent_hex);
    let mut sst_paths: Vec<_> = std::fs::read_dir(&parent_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("sst"))
        .collect();
    sst_paths.sort();
    let removed = sst_paths
        .pop()
        .expect("expected parent .sst files to exist");
    std::fs::remove_file(&removed).unwrap();

    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    let info = store.recover_segments().unwrap();

    let faults = match &info.health {
        super::RecoveryHealth::Degraded { faults, class } => {
            assert_eq!(
                *class,
                super::DegradationClass::DataLoss,
                "partial inherited-layer loss is authoritative data loss"
            );
            &faults[..]
        }
        other => panic!("expected Degraded, got {other:?}"),
    };
    assert!(
        faults.iter().any(|f| matches!(
            f,
            super::RecoveryFault::InheritedLayerLost {
                child,
                source_branch,
            } if *child == child_branch() && *source_branch == parent_branch()
        )),
        "partial inherited-layer loss must surface InheritedLayerLost"
    );

    let child = store.branches.get(&child_branch()).unwrap();
    assert!(
        !child.inherited_layers.is_empty(),
        "surviving inherited entries should still be installed for the child"
    );
}

#[test]
fn materialize_manifest_roundtrip() {
    let dir = tempfile::tempdir().unwrap();

    let seg_ids_before: Vec<u64>;

    {
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        seed(&store, parent_kv("a"), Value::Int(1), 1);
        store.rotate_memtable(&parent_branch());
        store.flush_oldest_frozen(&parent_branch()).unwrap();

        store
            .branches
            .entry(child_branch())
            .or_insert_with(BranchState::new);
        store
            .fork_branch(&parent_branch(), &child_branch())
            .unwrap();

        store.materialize_layer(&child_branch(), 0).unwrap();

        // After materialization, child should have own segment and no inherited layers
        let child = store.branches.get(&child_branch()).unwrap();
        assert_eq!(child.inherited_layers.len(), 0);

        seg_ids_before = child
            .version
            .load()
            .levels
            .iter()
            .flat_map(|l| l.iter().map(|s| s.file_id()))
            .collect();
    }

    // Recover and verify manifest reflects removed layer + new own segment
    {
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        store.recover_segments().unwrap();

        let child = store.branches.get(&child_branch()).unwrap();
        assert_eq!(
            child.inherited_layers.len(),
            0,
            "no inherited layers after recovery"
        );

        let seg_ids_after: Vec<u64> = child
            .version
            .load()
            .levels
            .iter()
            .flat_map(|l| l.iter().map(|s| s.file_id()))
            .collect();

        assert_eq!(
            seg_ids_before.len(),
            seg_ids_after.len(),
            "same number of segments after recovery"
        );
    }
}

#[test]
fn branches_needing_materialization_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Create a branch with many inherited layers (> MAX_INHERITED_LAYERS)
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);

    // Initially no branches need materialization
    assert!(store.branches_needing_materialization().is_empty());

    // Add MAX_INHERITED_LAYERS layers (at threshold, not over)
    {
        let mut child = store.branches.get_mut(&child_branch()).unwrap();
        for i in 0..super::MAX_INHERITED_LAYERS {
            child.inherited_layers.push(InheritedLayer {
                source_branch_id: BranchId::from_bytes([i as u8 + 40; 16]),
                fork_version: CommitVersion(10),
                segments: Arc::new(SegmentVersion::new()),
                status: LayerStatus::Active,
            });
        }
    }
    assert!(
        store.branches_needing_materialization().is_empty(),
        "at threshold, not over"
    );

    // Add one more to exceed threshold
    {
        let mut child = store.branches.get_mut(&child_branch()).unwrap();
        child.inherited_layers.push(InheritedLayer {
            source_branch_id: BranchId::from_bytes([99; 16]),
            fork_version: CommitVersion(10),
            segments: Arc::new(SegmentVersion::new()),
            status: LayerStatus::Active,
        });
    }
    let needing = store.branches_needing_materialization();
    assert_eq!(needing.len(), 1);
    assert_eq!(needing[0], child_branch());
}

#[test]
fn materialize_multi_segment_inherited_layer() {
    // Regression test: parent has multiple L0 segments (overlapping).
    // Materialization must sort entries globally before building the output
    // segment, or binary search in the output would break.
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Flush three separate L0 segments with interleaved keys
    seed(&store, parent_kv("b"), Value::Int(2), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    seed(&store, parent_kv("a"), Value::Int(1), 2);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    seed(&store, parent_kv("c"), Value::Int(3), 3);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Parent now has 3 L0 segments with keys b, a, c (NOT globally sorted)
    assert_eq!(store.l0_segment_count(&parent_branch()), 3);

    // Fork
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Materialize — without the sort fix, this produces a corrupt segment
    let result = store.materialize_layer(&child_branch(), 0).unwrap();
    assert_eq!(result.entries_materialized, 3);
    assert_eq!(result.segments_created, 1);

    // Verify ALL keys are readable via point lookup (would fail on corrupt segment)
    let a = store
        .get_versioned(&child_kv("a"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(a.value, Value::Int(1));

    let b = store
        .get_versioned(&child_kv("b"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(b.value, Value::Int(2));

    let c = store
        .get_versioned(&child_kv("c"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(c.value, Value::Int(3));

    // Also verify list_branch works (scan path)
    let all = store.list_branch(&child_branch());
    assert_eq!(
        all.len(),
        3,
        "all 3 entries should be visible after materialize"
    );
}

#[test]
fn materialize_multi_level_inherited_layer() {
    // Parent has data in BOTH L0 and L1 (post-compaction + new writes).
    // Materialization must correctly merge entries across levels.
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Phase 1: write data and compact to L1
    seed(&store, parent_kv("a"), Value::Int(1), 1);
    seed(&store, parent_kv("c"), Value::Int(3), 2);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();
    store
        .compact_l0_to_l1(&parent_branch(), CommitVersion(0))
        .unwrap();
    assert_eq!(store.l0_segment_count(&parent_branch()), 0);
    assert_eq!(store.l1_segment_count(&parent_branch()), 1);

    // Phase 2: write more data to L0 (interleaved keys)
    seed(&store, parent_kv("b"), Value::Int(2), 3);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();
    assert_eq!(store.l0_segment_count(&parent_branch()), 1);

    // Parent now has: L0=[b@3], L1=[a@1, c@2]

    // Fork child
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Materialize — entries come from both L0 and L1
    let result = store.materialize_layer(&child_branch(), 0).unwrap();
    assert_eq!(result.entries_materialized, 3);

    // All keys must be readable (verifies correct sort order in output segment)
    let a = store
        .get_versioned(&child_kv("a"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(a.value, Value::Int(1));

    let b = store
        .get_versioned(&child_kv("b"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(b.value, Value::Int(2));

    let c = store
        .get_versioned(&child_kv("c"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(c.value, Value::Int(3));
}
