use super::*;
use std::collections::HashSet;

// ===== COW Branching Foundation Tests (Epic A) =====

#[test]
fn branch_state_default_no_inherited_layers() {
    let state = BranchState::new();
    assert!(state.inherited_layers.is_empty());
}

#[test]
fn compaction_deletes_when_unreferenced() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 1024);
    let bid = branch();

    // Write enough data to create 2+ segments via flush
    for i in 0..100 {
        seed(
            &store,
            kv_key(&format!("key_{:04}", i)),
            Value::Int(i as i64),
            1,
        );
    }
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    for i in 100..200 {
        seed(
            &store,
            kv_key(&format!("key_{:04}", i)),
            Value::Int(i as i64),
            2,
        );
    }
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    // Count .sst files before compaction
    let branch_hex = super::hex_encode_branch(&bid);
    let branch_dir = dir.path().join(&branch_hex);
    let sst_before: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
        .collect();
    assert!(sst_before.len() >= 2);

    // Compact — segments are unreferenced, so they should be deleted
    store.compact_branch(&bid, CommitVersion(0)).unwrap();

    // Old .sst files should be gone, only the compacted output remains
    let sst_after: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
        .collect();
    // Should have exactly 1 compacted output segment
    assert_eq!(sst_after.len(), 1);
}

#[test]
fn compaction_preserves_when_referenced() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 1024);
    let bid = branch();

    // Write enough data to create 2+ segments via flush
    for i in 0..100 {
        seed(
            &store,
            kv_key(&format!("key_{:04}", i)),
            Value::Int(i as i64),
            1,
        );
    }
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    for i in 100..200 {
        seed(
            &store,
            kv_key(&format!("key_{:04}", i)),
            Value::Int(i as i64),
            2,
        );
    }
    store.rotate_memtable(&bid);
    store.flush_oldest_frozen(&bid).unwrap();

    // Increment refcount on the first segment twice (simulating 2 COW children)
    let branch_ref = store.branches.get(&bid).unwrap();
    let ver = branch_ref.version.load();
    let referenced_seg = &ver.l0_segments()[0];
    let referenced_file_id = referenced_seg.file_id();
    let referenced_path = referenced_seg.file_path().to_path_buf();
    store.ref_registry.increment(referenced_file_id);
    store.ref_registry.increment(referenced_file_id);
    drop(ver);
    drop(branch_ref);

    // Count .sst files before compaction (should be 2 input segments)
    let branch_hex = super::hex_encode_branch(&bid);
    let branch_dir = dir.path().join(&branch_hex);
    let sst_before: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
        .collect();
    assert!(sst_before.len() >= 2);

    // Compact — uses is_referenced() (not decrement) so refcount stays at 2
    store.compact_branch(&bid, CommitVersion(0)).unwrap();

    // The referenced segment file should still exist on disk
    assert!(
        referenced_path.exists(),
        "multiply-referenced segment should not be deleted during compaction"
    );

    // Refcount unchanged — compaction checks is_referenced() but doesn't decrement
    assert_eq!(store.ref_registry.ref_count(referenced_file_id), 2);

    // Verify non-referenced segments were deleted: should have the 1 new compacted
    // output + 1 surviving referenced segment = 2 total
    let sst_after: Vec<_> = std::fs::read_dir(&branch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
        .collect();
    assert_eq!(
        sst_after.len(),
        2,
        "expected 1 compacted output + 1 preserved referenced segment"
    );
}

// =========================================================================
// COW Branching — inherited layer read-path tests
// =========================================================================

#[test]
fn inherited_layer_point_lookup() {
    let (_dir, store) = setup_parent_with_segments(&[("a", 1, 1), ("b", 2, 2), ("c", 3, 3)]);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    // Child should see parent's data
    let result = store
        .get_versioned(&child_kv("a"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(1));

    let result = store
        .get_versioned(&child_kv("c"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(3));

    // Non-existent key still returns None
    assert!(store
        .get_versioned(&child_kv("z"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn inherited_layer_version_filter() {
    // Parent has entries at versions 1, 5, 10.
    // Fork at version 5 → child should only see versions 1 and 5.
    let (_dir, store) = setup_parent_with_segments(&[("k", 10, 1), ("k", 50, 5), ("k", 100, 10)]);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 5);

    let result = store
        .get_versioned(&child_kv("k"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(
        result.value,
        Value::Int(50),
        "should see version 5, not version 10"
    );
}

#[test]
fn inherited_layer_write_shadows() {
    let (_dir, store) = setup_parent_with_segments(&[("k", 100, 1)]);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    // Write to child — this should shadow the inherited value
    store
        .put_with_version_mode(
            child_kv("k"),
            Value::Int(999),
            CommitVersion(11),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let result = store
        .get_versioned(&child_kv("k"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(999));

    // But at snapshot before the child write, we see the inherited value
    let result = store
        .get_versioned(&child_kv("k"), CommitVersion(10))
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(100));
}

#[test]
fn inherited_layer_delete_shadows() {
    let (_dir, store) = setup_parent_with_segments(&[("k", 42, 1)]);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    // Delete on child hides inherited entry
    store
        .delete_with_version(&child_kv("k"), CommitVersion(11))
        .unwrap();

    assert!(store
        .get_versioned(&child_kv("k"), CommitVersion::MAX)
        .unwrap()
        .is_none());

    // Snapshot before the delete still sees inherited data
    let result = store
        .get_versioned(&child_kv("k"), CommitVersion(10))
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(42));
}

#[test]
fn inherited_layer_range_scan() {
    let (_dir, store) = setup_parent_with_segments(&[
        ("user:alice", 1, 1),
        ("user:bob", 2, 2),
        ("user:carol", 3, 3),
        ("order:1", 10, 4),
    ]);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    // Child writes a new user and overwrites bob
    store
        .put_with_version_mode(
            child_kv("user:bob"),
            Value::Int(200),
            CommitVersion(11),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            child_kv("user:dave"),
            Value::Int(4),
            CommitVersion(12),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let results = store
        .scan_prefix(&child_kv("user:"), CommitVersion::MAX)
        .unwrap();
    assert_eq!(results.len(), 4); // alice, bob(200), carol, dave

    let values: Vec<i64> = results
        .iter()
        .map(|(_, vv)| match &vv.value {
            Value::Int(i) => *i,
            _ => panic!("expected Int"),
        })
        .collect();
    assert_eq!(values, vec![1, 200, 3, 4]); // alice=1, bob=200, carol=3, dave=4
}

#[test]
fn inherited_layer_list_branch() {
    let (_dir, store) = setup_parent_with_segments(&[("a", 1, 1), ("b", 2, 2)]);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    // Write one more on child
    store
        .put_with_version_mode(
            child_kv("c"),
            Value::Int(3),
            CommitVersion(11),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let entries = store.list_branch(&child_branch());
    assert_eq!(entries.len(), 3);

    let keys: Vec<String> = entries
        .iter()
        .map(|(k, _)| String::from_utf8(k.user_key.to_vec()).unwrap())
        .collect();
    assert_eq!(keys, vec!["a", "b", "c"]);
}

#[test]
fn inherited_layer_bloom_correct() {
    // Point lookup should succeed through inherited layer even with bloom
    // filters — we rewrite the typed_key to use the source branch_id for probes.
    let (_dir, store) = setup_parent_with_segments(&[("bloom_key", 42, 1)]);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    let result = store
        .get_versioned(&child_kv("bloom_key"), CommitVersion::MAX)
        .unwrap();
    assert!(
        result.is_some(),
        "bloom filter should not reject rewritten key"
    );
    assert_eq!(result.unwrap().value, Value::Int(42));
}

#[test]
fn inherited_layer_two_levels() {
    // A ← B ← C: C reads through B's own segments and A's segments.
    // C has two inherited layers: [parent_segments, grandparent_segments]
    // (nearest ancestor first).
    let grandparent = BranchId::from_bytes([30; 16]);
    let parent = BranchId::from_bytes([31; 16]);
    let child = BranchId::from_bytes([32; 16]);

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    let gp_ns = Arc::new(Namespace::new(grandparent, "default".to_string()));
    let p_ns = Arc::new(Namespace::new(parent, "default".to_string()));
    let c_ns = Arc::new(Namespace::new(child, "default".to_string()));

    // Write to grandparent and flush
    store
        .put_with_version_mode(
            Key::new(gp_ns.clone(), TypeTag::KV, b"from_gp".to_vec()),
            Value::Int(1),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            Key::new(gp_ns.clone(), TypeTag::KV, b"shared".to_vec()),
            Value::Int(10),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store.rotate_memtable(&grandparent);
    store.flush_oldest_frozen(&grandparent).unwrap();

    // Parent writes own data and flushes
    store
        .put_with_version_mode(
            Key::new(p_ns.clone(), TypeTag::KV, b"from_parent".to_vec()),
            Value::Int(2),
            CommitVersion(3),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            Key::new(p_ns.clone(), TypeTag::KV, b"shared".to_vec()),
            Value::Int(20),
            CommitVersion(4),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store.rotate_memtable(&parent);
    store.flush_oldest_frozen(&parent).unwrap();

    // Child inherits from parent (nearest ancestor) then grandparent
    attach_inherited_layer(&store, parent, child, 10);
    // Also attach grandparent's segments (ordered: nearest ancestor first)
    {
        let gp = store.branches.get(&grandparent).unwrap();
        let gp_snapshot = gp.version.load_full();
        drop(gp);
        let mut child_state = store.branches.get_mut(&child).unwrap();
        child_state.inherited_layers.push(InheritedLayer {
            source_branch_id: grandparent,
            fork_version: CommitVersion(10),
            segments: gp_snapshot,
            status: LayerStatus::Active,
        });
    }

    // Child sees parent's own data
    let r = store
        .get_versioned(
            &Key::new(c_ns.clone(), TypeTag::KV, b"from_parent".to_vec()),
            CommitVersion::MAX,
        )
        .unwrap()
        .unwrap();
    assert_eq!(r.value, Value::Int(2));

    // Child sees parent's version of "shared" (parent shadows grandparent)
    let r = store
        .get_versioned(
            &Key::new(c_ns.clone(), TypeTag::KV, b"shared".to_vec()),
            CommitVersion::MAX,
        )
        .unwrap()
        .unwrap();
    assert_eq!(r.value, Value::Int(20));

    // Child sees grandparent's exclusive data
    let r = store
        .get_versioned(
            &Key::new(c_ns.clone(), TypeTag::KV, b"from_gp".to_vec()),
            CommitVersion::MAX,
        )
        .unwrap()
        .unwrap();
    assert_eq!(r.value, Value::Int(1));
}

#[test]
fn inherited_layer_version_clamping() {
    // Time-travel: get_versioned(key, 3) with fork_version=10
    // should respect the user's snapshot version, not fork_version.
    let (_dir, store) = setup_parent_with_segments(&[("k", 10, 1), ("k", 50, 5), ("k", 100, 9)]);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    // Snapshot at version 3: should see version 1
    let result = store
        .get_versioned(&child_kv("k"), CommitVersion(3))
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(10));

    // Snapshot at version 7: should see version 5
    let result = store
        .get_versioned(&child_kv("k"), CommitVersion(7))
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(50));
}

#[test]
fn inherited_layer_get_at_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write entries with specific timestamps via recovery API
    store
        .put_recovery_entry(parent_kv("k"), Value::Int(100), CommitVersion(1), 1000, 0)
        .unwrap();
    store
        .put_recovery_entry(parent_kv("k"), Value::Int(200), CommitVersion(2), 2000, 0)
        .unwrap();
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    // get_at_timestamp with max_ts=1500 should see the first version (ts=1000)
    let result = store
        .get_at_timestamp(&child_kv("k"), 1500)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(100));

    // get_at_timestamp with max_ts=2500 should see the second version (ts=2000)
    let result = store
        .get_at_timestamp(&child_kv("k"), 2500)
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(200));
}

#[test]
fn inherited_layer_get_history() {
    let (_dir, store) = setup_parent_with_segments(&[("k", 10, 1), ("k", 20, 2), ("k", 30, 3)]);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    let history = store.get_history(&child_kv("k"), None, None).unwrap();
    assert_eq!(history.len(), 3);
    // Newest first
    assert_eq!(history[0].value, Value::Int(30));
    assert_eq!(history[1].value, Value::Int(20));
    assert_eq!(history[2].value, Value::Int(10));
}

#[test]
fn inherited_layer_scan_prefix_at_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    store
        .put_recovery_entry(
            parent_kv("user:a"),
            Value::Int(1),
            CommitVersion(1),
            1000,
            0,
        )
        .unwrap();
    store
        .put_recovery_entry(
            parent_kv("user:b"),
            Value::Int(2),
            CommitVersion(2),
            2000,
            0,
        )
        .unwrap();
    store
        .put_recovery_entry(
            parent_kv("user:c"),
            Value::Int(3),
            CommitVersion(3),
            3000,
            0,
        )
        .unwrap();
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    // Scan at timestamp 2500 — should see a and b but not c
    let results = store
        .scan_prefix_at_timestamp(&child_kv("user:"), 2500)
        .unwrap();
    assert_eq!(results.len(), 2);
    let keys: Vec<String> = results
        .iter()
        .map(|(k, _)| String::from_utf8(k.user_key.to_vec()).unwrap())
        .collect();
    assert_eq!(keys, vec!["user:a", "user:b"]);
}

#[test]
fn inherited_layer_materialized_skipped() {
    let (_dir, store) = setup_parent_with_segments(&[("k", 42, 1)]);

    // Manually attach a Materialized layer — should be skipped
    let source = store.branches.get(&parent_branch()).unwrap();
    let snapshot = source.version.load_full();
    drop(source);
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    let mut child = store.branches.get_mut(&child_branch()).unwrap();
    child.inherited_layers.push(InheritedLayer {
        source_branch_id: parent_branch(),
        fork_version: CommitVersion(10),
        segments: snapshot,
        status: LayerStatus::Materialized,
    });
    drop(child);

    // Point lookup should return None — materialized layer is skipped
    assert!(store
        .get_versioned(&child_kv("k"), CommitVersion::MAX)
        .unwrap()
        .is_none());

    // List should be empty
    assert!(store.list_branch(&child_branch()).is_empty());

    // Scan should be empty
    assert!(store
        .scan_prefix(&child_kv(""), CommitVersion::MAX)
        .unwrap()
        .is_empty());
}

#[test]
fn inherited_layer_empty_parent() {
    // Parent branch exists but has no data (no segments).
    // Inherited layer should gracefully return nothing.
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Create parent branch with empty state (no writes, no flush)
    store
        .branches
        .entry(parent_branch())
        .or_insert_with(BranchState::new);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 10);

    assert!(store
        .get_versioned(&child_kv("k"), CommitVersion::MAX)
        .unwrap()
        .is_none());
    assert!(store.list_branch(&child_branch()).is_empty());
    assert!(store
        .scan_prefix(&child_kv(""), CommitVersion::MAX)
        .unwrap()
        .is_empty());
    assert!(store
        .get_history(&child_kv("k"), None, None)
        .unwrap()
        .is_empty());
}

#[test]
fn inherited_layer_fork_version_zero() {
    // fork_version=0 means no parent data is visible (all entries have commit_id >= 1).
    let (_dir, store) = setup_parent_with_segments(&[("a", 1, 1), ("b", 2, 2)]);
    attach_inherited_layer(&store, parent_branch(), child_branch(), 0);

    // Point lookup: nothing visible
    assert!(store
        .get_versioned(&child_kv("a"), CommitVersion::MAX)
        .unwrap()
        .is_none());

    // List: empty
    assert!(store.list_branch(&child_branch()).is_empty());

    // Scan: empty
    assert!(store
        .scan_prefix(&child_kv(""), CommitVersion::MAX)
        .unwrap()
        .is_empty());
}

#[test]
fn inherited_layer_custom_space() {
    // Verify that non-default space names survive the branch_id rewrite.
    let space = "custom_space";
    let parent = parent_branch();
    let child = child_branch();
    let p_ns = Arc::new(Namespace::new(parent, space.to_string()));
    let c_ns = Arc::new(Namespace::new(child, space.to_string()));

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    store
        .put_with_version_mode(
            Key::new(p_ns.clone(), TypeTag::KV, b"key1".to_vec()),
            Value::Int(42),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store.rotate_memtable(&parent);
    store.flush_oldest_frozen(&parent).unwrap();

    attach_inherited_layer(&store, parent, child, 10);

    // Query with child's namespace in the custom space
    let result = store
        .get_versioned(
            &Key::new(c_ns.clone(), TypeTag::KV, b"key1".to_vec()),
            CommitVersion::MAX,
        )
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(42));

    // Query with child's namespace in the DEFAULT space should NOT find it
    assert!(store
        .get_versioned(&child_kv("key1"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn inherited_layer_non_kv_type_tags() {
    // Verify inherited layers work for Json and Event type tags, not just KV.
    let parent = parent_branch();
    let child = child_branch();
    let p_ns = Arc::new(Namespace::new(parent, "default".to_string()));
    let c_ns = Arc::new(Namespace::new(child, "default".to_string()));

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write a Json entry and an Event entry to parent
    store
        .put_with_version_mode(
            Key::new(p_ns.clone(), TypeTag::Json, b"doc1".to_vec()),
            Value::Int(100),
            CommitVersion(1),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store
        .put_with_version_mode(
            Key::new(p_ns.clone(), TypeTag::Event, 42u64.to_be_bytes().to_vec()),
            Value::Int(200),
            CommitVersion(2),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store.rotate_memtable(&parent);
    store.flush_oldest_frozen(&parent).unwrap();

    attach_inherited_layer(&store, parent, child, 10);

    // Child sees the Json entry
    let result = store
        .get_versioned(
            &Key::new(c_ns.clone(), TypeTag::Json, b"doc1".to_vec()),
            CommitVersion::MAX,
        )
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(100));

    // Child sees the Event entry
    let result = store
        .get_versioned(
            &Key::new(c_ns.clone(), TypeTag::Event, 42u64.to_be_bytes().to_vec()),
            CommitVersion::MAX,
        )
        .unwrap()
        .unwrap();
    assert_eq!(result.value, Value::Int(200));

    // KV lookup should NOT find Json/Event entries
    assert!(store
        .get_versioned(&child_kv("doc1"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

// =========================================================================
// COW Fork — SegmentedStore::fork_branch() tests
// =========================================================================

fn grandchild_branch() -> BranchId {
    BranchId::from_bytes([30; 16])
}

fn grandchild_ns() -> Arc<Namespace> {
    Arc::new(Namespace::new(grandchild_branch(), "default".to_string()))
}

fn grandchild_kv(name: &str) -> Key {
    Key::new(grandchild_ns(), TypeTag::KV, name.as_bytes().to_vec())
}

#[test]
fn fork_creates_inherited_layer() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write data to parent and flush to segments
    seed(&store, parent_kv("a"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Create child branch
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);

    // Fork
    let (fork_version, segments_shared) = store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Verify inherited layer
    let child = store.branches.get(&child_branch()).unwrap();
    assert_eq!(child.inherited_layers.len(), 1);
    assert_eq!(child.inherited_layers[0].source_branch_id, parent_branch());
    assert_eq!(child.inherited_layers[0].fork_version, fork_version);
    assert_eq!(child.inherited_layers[0].status, LayerStatus::Active);
    assert!(segments_shared > 0);
}

#[test]
fn fork_no_data_copy() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Write data to parent and flush
    seed(&store, parent_kv("a"), Value::Int(1), 1);
    seed(&store, parent_kv("b"), Value::Int(2), 2);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Create child branch
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);

    // Fork
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Child's own segments should be empty
    let child = store.branches.get(&child_branch()).unwrap();
    assert_eq!(child.version.load().total_segment_count(), 0);
    assert!(child.active.is_empty());
    assert!(child.frozen.is_empty());
}

#[test]
fn fork_read_through() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, parent_kv("x"), Value::Int(42), 1);
    seed(&store, parent_kv("y"), Value::String("hello".into()), 2);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Point lookups on child return parent's data
    let r = store
        .get_versioned(&child_kv("x"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(r.value, Value::Int(42));

    let r = store
        .get_versioned(&child_kv("y"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(r.value, Value::String("hello".into()));

    // Non-existent key
    assert!(store
        .get_versioned(&child_kv("z"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn fork_write_shadows() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, parent_kv("k"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Write to child shadows inherited data
    let child_version = store.next_version();
    store
        .put_with_version_mode(
            child_kv("k"),
            Value::Int(99),
            CommitVersion(child_version),
            None,
            WriteMode::Append,
        )
        .unwrap();

    let r = store
        .get_versioned(&child_kv("k"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(r.value, Value::Int(99));

    // Parent is unchanged
    let r = store
        .get_versioned(&parent_kv("k"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(r.value, Value::Int(1));
}

#[test]
fn fork_parent_write_invisible() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, parent_kv("k"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Write new data to parent after fork — flush to new segment
    let v = store.next_version();
    store
        .put_with_version_mode(
            parent_kv("new_key"),
            Value::Int(999),
            CommitVersion(v),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Child should NOT see the new parent key
    assert!(store
        .get_versioned(&child_kv("new_key"), CommitVersion::MAX)
        .unwrap()
        .is_none());

    // But parent sees it
    assert_eq!(
        store
            .get_versioned(&parent_kv("new_key"), CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .value,
        Value::Int(999)
    );
}

#[test]
fn fork_scan() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, parent_kv("user/alice"), Value::Int(1), 1);
    seed(&store, parent_kv("user/bob"), Value::Int(2), 2);
    seed(&store, parent_kv("config/x"), Value::Int(3), 3);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Prefix scan on child finds inherited data
    let prefix = Key::new(child_ns(), TypeTag::KV, "user/".as_bytes().to_vec());
    let results = store.scan_prefix(&prefix, CommitVersion::MAX).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn fork_chain_3_levels() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Grandparent → parent → child
    seed(&store, parent_kv("gp_key"), Value::Int(100), 1);
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

    // Write something to child, flush
    let v = store.next_version();
    store
        .put_with_version_mode(
            child_kv("child_key"),
            Value::Int(200),
            CommitVersion(v),
            None,
            WriteMode::Append,
        )
        .unwrap();
    store.rotate_memtable(&child_branch());
    store.flush_oldest_frozen(&child_branch()).unwrap();

    // Fork child → grandchild
    store
        .branches
        .entry(grandchild_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&child_branch(), &grandchild_branch())
        .unwrap();

    // Grandchild should see parent's data via flattened layers
    let gc = store.branches.get(&grandchild_branch()).unwrap();
    assert_eq!(
        gc.inherited_layers.len(),
        2,
        "grandchild has 2 inherited layers"
    );

    // Grandchild sees child's own data
    let r = store
        .get_versioned(&grandchild_kv("child_key"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(r.value, Value::Int(200));

    // Grandchild sees grandparent's data (through child's inherited layer)
    let r = store
        .get_versioned(&grandchild_kv("gp_key"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(r.value, Value::Int(100));
}

#[test]
fn fork_refcounts_incremented() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, parent_kv("a"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    // Get segment file_id before fork
    let parent_state = store.branches.get(&parent_branch()).unwrap();
    let seg_ids: Vec<u64> = parent_state
        .version
        .load()
        .levels
        .iter()
        .flat_map(|l| l.iter().map(|s| s.file_id()))
        .collect();
    drop(parent_state);
    assert!(!seg_ids.is_empty());

    // Before fork: segments are untracked
    for &id in &seg_ids {
        assert!(!store.ref_registry.is_referenced(id));
    }

    // Fork
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // After fork: refcounts are incremented
    for &id in &seg_ids {
        assert!(
            store.ref_registry.is_referenced(id),
            "Segment {} should be referenced after fork",
            id
        );
    }
}

#[test]
fn fork_refcounts_on_clear() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, parent_kv("a"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    let parent_state = store.branches.get(&parent_branch()).unwrap();
    let seg_ids: Vec<u64> = parent_state
        .version
        .load()
        .levels
        .iter()
        .flat_map(|l| l.iter().map(|s| s.file_id()))
        .collect();
    drop(parent_state);

    // Fork
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Verify referenced
    for &id in &seg_ids {
        assert!(store.ref_registry.is_referenced(id));
    }

    // Clear child — refcounts should be decremented
    store.clear_branch(&child_branch());

    for &id in &seg_ids {
        assert!(
            !store.ref_registry.is_referenced(id),
            "Segment {} should not be referenced after clear",
            id
        );
    }
}

#[test]
fn fork_manifest_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, parent_kv("a"), Value::Int(1), 1);
    seed(&store, parent_kv("b"), Value::Int(2), 2);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);
    let (fork_version, _) = store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Verify inherited layers are present before recovery
    {
        let child = store.branches.get(&child_branch()).unwrap();
        assert_eq!(child.inherited_layers.len(), 1);
        assert_eq!(child.inherited_layers[0].fork_version, fork_version);
    }

    // Create a new store and recover from the same directory
    let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
    store2.recover_segments().unwrap();

    // Verify child branch recovered with inherited layers
    let child2 = store2.branches.get(&child_branch());
    assert!(
        child2.is_some(),
        "Child branch should survive manifest recovery"
    );
    let child2 = child2.unwrap();
    assert_eq!(
        child2.inherited_layers.len(),
        1,
        "Inherited layers should survive recovery"
    );
    assert_eq!(child2.inherited_layers[0].fork_version, fork_version);

    // Verify data is readable through inherited layers after recovery
    let r = store2
        .get_versioned(&child_kv("a"), CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(r.value, Value::Int(1));
}

#[test]
fn fork_self_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, parent_kv("a"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    let result = store.fork_branch(&parent_branch(), &parent_branch());
    assert!(result.is_err(), "Self-fork must be rejected");
    let err = result.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn fork_ephemeral_succeeds_at_storage_level() {
    // The storage layer allows fork on ephemeral stores (data stays in memtables).
    // The engine layer guards against this with has_segments_dir().
    let store = SegmentedStore::new();
    assert!(!store.has_segments_dir());

    seed(&store, parent_kv("a"), Value::Int(1), 1);

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);

    // Fork succeeds — flush is a no-op, snapshot captures empty SegmentVersion
    let (fork_version, segments_shared) = store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();
    let _ = fork_version;
    assert_eq!(segments_shared, 0, "No segments on ephemeral store");

    // Data written to parent's memtable is NOT visible to child via inherited
    // layers (memtable data is not captured in the segment snapshot).
    // The engine layer prevents this by checking has_segments_dir() first.
    assert!(store
        .get_versioned(&child_kv("a"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn fork_empty_source_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    // Create parent branch with no data (no segments)
    store
        .branches
        .entry(parent_branch())
        .or_insert_with(BranchState::new);
    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);

    let (fork_version, _segments_shared) = store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // fork_version is still valid (captures current version counter)
    let _ = fork_version;

    // Child has one inherited layer, reads return nothing
    let child = store.branches.get(&child_branch()).unwrap();
    assert_eq!(child.inherited_layers.len(), 1);
    assert!(store
        .get_versioned(&child_kv("anything"), CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn fork_double_fork_same_dest_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

    seed(&store, parent_kv("a"), Value::Int(1), 1);
    store.rotate_memtable(&parent_branch());
    store.flush_oldest_frozen(&parent_branch()).unwrap();

    store
        .branches
        .entry(child_branch())
        .or_insert_with(BranchState::new);

    // Fork once
    store
        .fork_branch(&parent_branch(), &child_branch())
        .unwrap();

    // Collect segment IDs after first fork for refcount verification
    let child_state = store.branches.get(&child_branch()).unwrap();
    let first_fork_seg_ids: Vec<u64> = child_state
        .inherited_layers
        .iter()
        .flat_map(|l| l.segments.levels.iter())
        .flat_map(|level| level.iter().map(|s| s.file_id()))
        .collect();
    drop(child_state);

    // After first fork, each inherited segment has refcount 1
    for &id in &first_fork_seg_ids {
        assert_eq!(store.ref_registry.ref_count(id), 1);
    }

    // Second fork to same dest must be rejected (issue #1720)
    let result = store.fork_branch(&parent_branch(), &child_branch());
    assert!(result.is_err(), "Double fork must be rejected");
    assert_eq!(
        result.unwrap_err().kind(),
        std::io::ErrorKind::AlreadyExists
    );

    // Original inherited layers must be preserved unchanged.
    let child = store.branches.get(&child_branch()).unwrap();
    assert_eq!(child.inherited_layers.len(), 1);

    // Refcounts must remain at 1 (no leak, no underflow).
    for &id in &first_fork_seg_ids {
        assert_eq!(
            store.ref_registry.ref_count(id),
            1,
            "Segment {} refcount must remain 1 after rejected fork",
            id
        );
    }
}

// ===== list_own_entries tests (COW-aware diff support) =====

#[test]
fn list_own_entries_excludes_inherited() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 64 * 1024);
    let pid = parent_branch();
    let cid = child_branch();

    // Write keys on parent
    seed(&store, parent_kv("alpha"), Value::Int(1), 1);
    seed(&store, parent_kv("beta"), Value::Int(2), 2);
    store.rotate_memtable(&pid);
    store.flush_oldest_frozen(&pid).unwrap();

    // Fork child from parent at version 2
    attach_inherited_layer(&store, pid, cid, 2);

    // Write one key on the child
    seed(&store, child_kv("gamma"), Value::Int(3), 3);

    // list_own_entries on child should return ONLY gamma (not alpha/beta)
    let own = store.list_own_entries(&cid, None);
    assert_eq!(own.len(), 1);
    assert_eq!(std::str::from_utf8(&own[0].key.user_key).unwrap(), "gamma");
    assert_eq!(own[0].value, Value::Int(3));
    assert!(!own[0].is_tombstone);
}

#[test]
fn list_own_entries_includes_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 64 * 1024);
    let pid = parent_branch();
    let cid = child_branch();

    // Parent has key "alpha"
    seed(&store, parent_kv("alpha"), Value::Int(1), 1);
    store.rotate_memtable(&pid);
    store.flush_oldest_frozen(&pid).unwrap();

    // Fork child
    attach_inherited_layer(&store, pid, cid, 1);

    // Child deletes "alpha" (writes tombstone)
    store
        .delete_with_version(&child_kv("alpha"), CommitVersion(2))
        .unwrap();

    // list_own_entries should include the tombstone
    let own = store.list_own_entries(&cid, None);
    assert_eq!(own.len(), 1);
    assert_eq!(std::str::from_utf8(&own[0].key.user_key).unwrap(), "alpha");
    assert!(own[0].is_tombstone);
}

#[test]
fn list_own_entries_since_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 64 * 1024);
    let pid = parent_branch();

    // Parent writes before fork
    seed(&store, parent_kv("pre_fork"), Value::Int(1), 1);
    seed(&store, parent_kv("at_fork"), Value::Int(2), 2);

    // Parent writes after fork
    seed(&store, parent_kv("post_fork"), Value::Int(3), 3);
    seed(&store, parent_kv("post_fork_2"), Value::Int(4), 4);

    // list_own_entries with min_commit_id=2 should return only post-fork entries
    let own = store.list_own_entries(&pid, Some(CommitVersion(2)));
    assert_eq!(own.len(), 2);
    let keys: HashSet<String> = own
        .iter()
        .map(|e| std::str::from_utf8(&e.key.user_key).unwrap().to_string())
        .collect();
    assert!(keys.contains("post_fork"));
    assert!(keys.contains("post_fork_2"));
    assert!(!keys.contains("pre_fork"));
    assert!(!keys.contains("at_fork"));
}

#[test]
fn list_own_entries_mvcc_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 64 * 1024);
    let pid = parent_branch();

    // Write same key multiple times at different versions
    seed(&store, parent_kv("key"), Value::Int(1), 1);
    seed(&store, parent_kv("key"), Value::Int(2), 2);
    seed(&store, parent_kv("key"), Value::Int(3), 3);

    // list_own_entries should return only the latest version
    let own = store.list_own_entries(&pid, None);
    assert_eq!(own.len(), 1);
    assert_eq!(own[0].value, Value::Int(3));
    assert_eq!(own[0].commit_id, CommitVersion(3));
}
