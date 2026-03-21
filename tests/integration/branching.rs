//! Branching and Branch Isolation Tests
//!
//! Tests branch isolation guarantees, branch management operations,
//! and branch operations (fork, diff, merge).

use crate::common::*;
use strata_engine::branch_ops::{self, MergeStrategy};
use strata_engine::SpaceIndex;

// ============================================================================
// Branch Isolation
// ============================================================================

#[test]
fn data_isolated_between_branches() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // Write to branch A
    kv.put(&branch_a, "default", "key", Value::String("value_a".into()))
        .unwrap();

    // Write to branch B
    kv.put(&branch_b, "default", "key", Value::String("value_b".into()))
        .unwrap();

    // Each branch sees only its own data
    let val_a = kv.get(&branch_a, "default", "key").unwrap().unwrap();
    let val_b = kv.get(&branch_b, "default", "key").unwrap().unwrap();

    assert_eq!(val_a, Value::String("value_a".into()));
    assert_eq!(val_b, Value::String("value_b".into()));
}

#[test]
fn delete_in_one_branch_doesnt_affect_other() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // Write same key to both branches
    kv.put(&branch_a, "default", "shared_key", Value::Int(1))
        .unwrap();
    kv.put(&branch_b, "default", "shared_key", Value::Int(2))
        .unwrap();

    // Delete from branch A
    kv.delete(&branch_a, "default", "shared_key").unwrap();

    // Branch A should be empty, branch B should have data
    assert!(kv
        .get(&branch_a, "default", "shared_key")
        .unwrap()
        .is_none());
    assert_eq!(
        kv.get(&branch_b, "default", "shared_key").unwrap(),
        Some(Value::Int(2))
    );
}

#[test]
fn all_primitives_isolated_between_branches() {
    let test_db = TestDb::new();
    let p = test_db.all_primitives();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // Write to branch A
    p.kv.put(&branch_a, "default", "k", Value::Int(1)).unwrap();
    p.state
        .init(&branch_a, "default", "s", Value::Int(1))
        .unwrap();
    p.event
        .append(&branch_a, "default", "e", int_payload(1))
        .unwrap();
    p.json
        .create(
            &branch_a,
            "default",
            "j",
            json_value(serde_json::json!({"a": 1})),
        )
        .unwrap();
    p.vector
        .create_collection(branch_a, "default", "v", config_small())
        .unwrap();
    p.vector
        .insert(branch_a, "default", "v", "vec", &[1.0, 0.0, 0.0], None)
        .unwrap();

    // Write different values to branch B
    p.kv.put(&branch_b, "default", "k", Value::Int(2)).unwrap();
    p.state
        .init(&branch_b, "default", "s", Value::Int(2))
        .unwrap();
    p.event
        .append(&branch_b, "default", "e", int_payload(2))
        .unwrap();
    p.json
        .create(
            &branch_b,
            "default",
            "j",
            json_value(serde_json::json!({"b": 2})),
        )
        .unwrap();
    p.vector
        .create_collection(branch_b, "default", "v", config_small())
        .unwrap();
    p.vector
        .insert(branch_b, "default", "v", "vec", &[0.0, 1.0, 0.0], None)
        .unwrap();

    // Verify isolation
    assert_eq!(
        p.kv.get(&branch_a, "default", "k").unwrap().unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        p.kv.get(&branch_b, "default", "k").unwrap().unwrap(),
        Value::Int(2)
    );

    assert_eq!(
        p.state.get(&branch_a, "default", "s").unwrap().unwrap(),
        Value::Int(1)
    );
    assert_eq!(
        p.state.get(&branch_b, "default", "s").unwrap().unwrap(),
        Value::Int(2)
    );

    let events_a = p
        .event
        .get_by_type(&branch_a, "default", "e", None, None)
        .unwrap();
    let events_b = p
        .event
        .get_by_type(&branch_b, "default", "e", None, None)
        .unwrap();
    assert_eq!(events_a.len(), 1);
    assert_eq!(events_b.len(), 1);

    let json_a = p
        .json
        .get(&branch_a, "default", "j", &root())
        .unwrap()
        .unwrap();
    let json_b = p
        .json
        .get(&branch_b, "default", "j", &root())
        .unwrap()
        .unwrap();
    assert_eq!(json_a.as_inner().get("a"), Some(&serde_json::json!(1)));
    assert_eq!(json_b.as_inner().get("b"), Some(&serde_json::json!(2)));

    let vec_a = p
        .vector
        .get(branch_a, "default", "v", "vec")
        .unwrap()
        .unwrap();
    let vec_b = p
        .vector
        .get(branch_b, "default", "v", "vec")
        .unwrap()
        .unwrap();
    assert_eq!(vec_a.value.embedding[0], 1.0);
    assert_eq!(vec_b.value.embedding[1], 1.0);
}

#[test]
fn many_concurrent_branches() {
    let test_db = TestDb::new();
    let kv = test_db.kv();

    // Create 100 branches with data
    let branch_ids: Vec<BranchId> = (0..100).map(|_| BranchId::new()).collect();

    for (i, branch_id) in branch_ids.iter().enumerate() {
        kv.put(branch_id, "default", "index", Value::Int(i as i64))
            .unwrap();
    }

    // Verify each branch has correct isolated data
    for (i, branch_id) in branch_ids.iter().enumerate() {
        let val = kv.get(branch_id, "default", "index").unwrap().unwrap();
        assert_eq!(val, Value::Int(i as i64));
    }
}

// ============================================================================
// Branch Lifecycle (via BranchIndex)
// ============================================================================

#[test]
fn create_and_list_branches() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();

    // Create some branches
    branch_index.create_branch("branch_1").unwrap();
    branch_index.create_branch("branch_2").unwrap();
    branch_index.create_branch("branch_3").unwrap();

    // List all branches
    let branches = branch_index.list_branches().unwrap();
    assert!(branches.len() >= 3);

    // Verify our branches exist
    assert!(branches.contains(&"branch_1".to_string()));
    assert!(branches.contains(&"branch_2".to_string()));
    assert!(branches.contains(&"branch_3".to_string()));
}

#[test]
fn branch_with_metadata() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();

    // create_branch creates a branch with default metadata;
    // verify we can retrieve the branch and it has the expected fields.
    branch_index.create_branch("with_metadata").unwrap();

    let branch = branch_index.get_branch("with_metadata").unwrap().unwrap();
    assert_eq!(branch.value.name, "with_metadata");
}

// ============================================================================
// Branch Isolation with Data Operations
// ============================================================================

#[test]
fn vector_collections_isolated_per_branch() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // Same collection name, different branches
    vector
        .create_collection(branch_a, "default", "embeddings", config_small())
        .unwrap();
    vector
        .create_collection(branch_b, "default", "embeddings", config_small())
        .unwrap();

    vector
        .insert(
            branch_a,
            "default",
            "embeddings",
            "vec",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            branch_b,
            "default",
            "embeddings",
            "vec",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();

    // Verify isolation
    let vec_a = vector
        .get(branch_a, "default", "embeddings", "vec")
        .unwrap()
        .unwrap();
    let vec_b = vector
        .get(branch_b, "default", "embeddings", "vec")
        .unwrap()
        .unwrap();

    assert_eq!(vec_a.value.embedding[0], 1.0);
    assert_eq!(vec_b.value.embedding[1], 1.0);
}

#[test]
fn event_streams_isolated_per_branch() {
    let test_db = TestDb::new();
    let event = test_db.event();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // Same stream name, different branches
    event
        .append(&branch_a, "default", "audit", int_payload(100))
        .unwrap();
    event
        .append(&branch_a, "default", "audit", int_payload(101))
        .unwrap();
    event
        .append(&branch_b, "default", "audit", int_payload(200))
        .unwrap();

    assert_eq!(
        event
            .get_by_type(&branch_a, "default", "audit", None, None)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        event
            .get_by_type(&branch_b, "default", "audit", None, None)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn json_documents_isolated_per_branch() {
    let test_db = TestDb::new();
    let json = test_db.json();

    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    // Same doc ID, different branches
    json.create(
        &branch_a,
        "default",
        "config",
        json_value(serde_json::json!({"version": 1})),
    )
    .unwrap();
    json.create(
        &branch_b,
        "default",
        "config",
        json_value(serde_json::json!({"version": 2})),
    )
    .unwrap();

    let doc_a = json
        .get(&branch_a, "default", "config", &path(".version"))
        .unwrap()
        .unwrap();
    let doc_b = json
        .get(&branch_b, "default", "config", &path(".version"))
        .unwrap()
        .unwrap();

    assert_eq!(doc_a.as_inner(), &serde_json::json!(1));
    assert_eq!(doc_b.as_inner(), &serde_json::json!(2));
}

// ============================================================================
// Branch Create vs Fork Behavior
// ============================================================================

/// create_branch creates a blank branch — it does NOT copy parent data.
/// Use fork_branch to copy data from one branch to another.
#[test]
fn child_branch_does_not_inherit_parent_data_currently() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // Create parent branch and get its branch_id
    let parent_meta = branch_index.create_branch("parent").unwrap();
    let parent_branch_id = BranchId::from_string(&parent_meta.value.branch_id).unwrap();

    kv.put(
        &parent_branch_id,
        "default",
        "parent_key",
        Value::String("parent_value".into()),
    )
    .unwrap();

    // Create child branch (create_branch makes a blank branch, not a fork)
    let child_meta = branch_index.create_branch("child").unwrap();
    let child_branch_id = BranchId::from_string(&child_meta.value.branch_id).unwrap();

    // create_branch does NOT inherit parent's data — use fork_branch for that
    let child_value = kv.get(&child_branch_id, "default", "parent_key").unwrap();
    assert!(
        child_value.is_none(),
        "create_branch does not copy data. Use fork_branch to copy data."
    );

    // Parent data should still exist
    let parent_value = kv.get(&parent_branch_id, "default", "parent_key").unwrap();
    assert_eq!(
        parent_value,
        Some(Value::String("parent_value".into())),
        "Parent data should remain"
    );
}

// ============================================================================
// Branch Fork Tests
// ============================================================================

#[test]
fn test_fork_branch() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // Create source branch with data
    branch_index.create_branch("source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");
    kv.put(&source_id, "default", "k1", Value::String("hello".into()))
        .unwrap();
    kv.put(&source_id, "default", "k2", Value::Int(42)).unwrap();

    // Fork it (no data copy)
    let info = branch_ops::fork_branch(&test_db.db, "source", "forked").unwrap();
    assert_eq!(info.source, "source");
    assert_eq!(info.destination, "forked");
    assert_eq!(info.keys_copied, 0, "fork copies zero keys");
    assert!(info.fork_version.is_some(), "fork returns fork_version");

    // Verify forked branch has the data
    let dest_id = strata_engine::primitives::branch::resolve_branch_name("forked");
    assert_eq!(
        kv.get(&dest_id, "default", "k1").unwrap(),
        Some(Value::String("hello".into()))
    );
    assert_eq!(
        kv.get(&dest_id, "default", "k2").unwrap(),
        Some(Value::Int(42))
    );

    // Verify source is unchanged
    assert_eq!(
        kv.get(&source_id, "default", "k1").unwrap(),
        Some(Value::String("hello".into()))
    );
}

#[test]
fn test_fork_with_spaces() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let space_index = SpaceIndex::new(test_db.db.clone());
    let kv = test_db.kv();

    // Create source branch with multiple spaces
    branch_index.create_branch("src").unwrap();
    let src_id = strata_engine::primitives::branch::resolve_branch_name("src");
    space_index.register(src_id, "alpha").unwrap();
    space_index.register(src_id, "beta").unwrap();

    kv.put(&src_id, "default", "d-key", Value::Int(1)).unwrap();
    kv.put(&src_id, "alpha", "a-key", Value::Int(2)).unwrap();
    kv.put(&src_id, "beta", "b-key", Value::Int(3)).unwrap();

    // Fork
    let info = branch_ops::fork_branch(&test_db.db, "src", "dst").unwrap();
    assert!(info.spaces_copied >= 3);

    let dst_id = strata_engine::primitives::branch::resolve_branch_name("dst");

    // Verify all spaces and data
    let dst_spaces = space_index.list(dst_id).unwrap();
    assert!(dst_spaces.contains(&"alpha".to_string()));
    assert!(dst_spaces.contains(&"beta".to_string()));

    assert_eq!(
        kv.get(&dst_id, "default", "d-key").unwrap(),
        Some(Value::Int(1))
    );
    assert_eq!(
        kv.get(&dst_id, "alpha", "a-key").unwrap(),
        Some(Value::Int(2))
    );
    assert_eq!(
        kv.get(&dst_id, "beta", "b-key").unwrap(),
        Some(Value::Int(3))
    );
}

// ============================================================================
// Branch Diff Tests
// ============================================================================

#[test]
fn test_diff_branches() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    branch_index.create_branch("a").unwrap();
    branch_index.create_branch("b").unwrap();
    let id_a = strata_engine::primitives::branch::resolve_branch_name("a");
    let id_b = strata_engine::primitives::branch::resolve_branch_name("b");

    // Shared key with different values
    kv.put(&id_a, "default", "shared", Value::Int(1)).unwrap();
    kv.put(&id_b, "default", "shared", Value::Int(2)).unwrap();

    // Keys unique to each branch
    kv.put(&id_a, "default", "only-a", Value::String("a".into()))
        .unwrap();
    kv.put(&id_b, "default", "only-b", Value::String("b".into()))
        .unwrap();

    let diff = branch_ops::diff_branches(&test_db.db, "a", "b").unwrap();
    assert_eq!(diff.branch_a, "a");
    assert_eq!(diff.branch_b, "b");
    assert_eq!(diff.summary.total_modified, 1, "shared key is modified");
    assert_eq!(
        diff.summary.total_removed, 1,
        "only-a is removed (in A not B)"
    );
    assert_eq!(diff.summary.total_added, 1, "only-b is added (in B not A)");
}

#[test]
fn test_diff_with_all_primitives() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("x").unwrap();
    branch_index.create_branch("y").unwrap();
    let id_x = strata_engine::primitives::branch::resolve_branch_name("x");
    let id_y = strata_engine::primitives::branch::resolve_branch_name("y");

    // Write KV to x only
    p.kv.put(&id_x, "default", "kv-key", Value::Int(1)).unwrap();

    // Write JSON to y only
    p.json
        .create(
            &id_y,
            "default",
            "doc",
            json_value(serde_json::json!({"field": "value"})),
        )
        .unwrap();

    // Write State to both with different values
    p.state
        .init(&id_x, "default", "cell", Value::Int(10))
        .unwrap();
    p.state
        .init(&id_y, "default", "cell", Value::Int(20))
        .unwrap();

    let diff = branch_ops::diff_branches(&test_db.db, "x", "y").unwrap();

    // kv-key is only in x → removed
    // doc is only in y → added
    // cell is in both with different values → modified
    assert!(diff.summary.total_removed >= 1, "KV key should be removed");
    assert!(diff.summary.total_added >= 1, "JSON doc should be added");
    assert!(
        diff.summary.total_modified >= 1,
        "State cell should be modified"
    );
}

// ============================================================================
// Branch Merge Tests
// ============================================================================

#[test]
fn test_merge_branches_lww() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    branch_index.create_branch("target").unwrap();
    branch_index.create_branch("source").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Write conflicting data
    kv.put(&target_id, "default", "shared", Value::Int(1))
        .unwrap();
    kv.put(&source_id, "default", "shared", Value::Int(2))
        .unwrap();
    kv.put(
        &source_id,
        "default",
        "new-key",
        Value::String("new".into()),
    )
    .unwrap();

    let info = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
    )
    .unwrap();
    assert!(info.keys_applied >= 2);

    // Target should have source's value for "shared"
    assert_eq!(
        kv.get(&target_id, "default", "shared").unwrap(),
        Some(Value::Int(2))
    );
    // New key should be present
    assert_eq!(
        kv.get(&target_id, "default", "new-key").unwrap(),
        Some(Value::String("new".into()))
    );
}

#[test]
fn test_merge_branches_strict() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    branch_index.create_branch("target").unwrap();
    branch_index.create_branch("source").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Write conflicting data
    kv.put(&target_id, "default", "shared", Value::Int(1))
        .unwrap();
    kv.put(&source_id, "default", "shared", Value::Int(2))
        .unwrap();

    // Strict merge should fail with conflicts
    let result = branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict);
    assert!(result.is_err());

    // Target should be unchanged
    assert_eq!(
        kv.get(&target_id, "default", "shared").unwrap(),
        Some(Value::Int(1))
    );
}

#[test]
fn test_fork_diff_merge_roundtrip() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // Create original branch with data
    branch_index.create_branch("original").unwrap();
    let original_id = strata_engine::primitives::branch::resolve_branch_name("original");
    kv.put(&original_id, "default", "base", Value::Int(1))
        .unwrap();
    kv.put(&original_id, "default", "shared", Value::Int(10))
        .unwrap();

    // Fork it
    branch_ops::fork_branch(&test_db.db, "original", "fork").unwrap();
    let fork_id = strata_engine::primitives::branch::resolve_branch_name("fork");

    // Diverge: modify both branches
    kv.put(
        &original_id,
        "default",
        "original-only",
        Value::String("orig".into()),
    )
    .unwrap();
    kv.put(
        &fork_id,
        "default",
        "fork-only",
        Value::String("fork".into()),
    )
    .unwrap();
    kv.put(&fork_id, "default", "shared", Value::Int(20))
        .unwrap();

    // Diff: original vs fork
    let diff = branch_ops::diff_branches(&test_db.db, "original", "fork").unwrap();
    assert!(
        diff.summary.total_modified >= 1,
        "shared should be modified"
    );
    assert!(diff.summary.total_removed >= 1, "original-only in A not B");
    assert!(diff.summary.total_added >= 1, "fork-only in B not A");

    // Merge fork → original (LWW)
    let info = branch_ops::merge_branches(
        &test_db.db,
        "fork",
        "original",
        MergeStrategy::LastWriterWins,
    )
    .unwrap();
    assert!(info.keys_applied >= 2, "Should apply fork-only and shared");

    // Verify merge results
    assert_eq!(
        kv.get(&original_id, "default", "base").unwrap(),
        Some(Value::Int(1)),
        "base should be unchanged"
    );
    assert_eq!(
        kv.get(&original_id, "default", "shared").unwrap(),
        Some(Value::Int(20)),
        "shared should have fork's value"
    );
    assert_eq!(
        kv.get(&original_id, "default", "fork-only").unwrap(),
        Some(Value::String("fork".into())),
        "fork-only should be merged in"
    );
    assert_eq!(
        kv.get(&original_id, "default", "original-only").unwrap(),
        Some(Value::String("orig".into())),
        "original-only should still exist (merge doesn't delete)"
    );
}

// ============================================================================
// Branch Isolation Stress Test
// ============================================================================

#[test]
fn concurrent_operations_across_branches() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let test_db = TestDb::new();
    let db = test_db.db.clone();

    let num_branches = 10;
    let ops_per_branch = 100;
    let barrier = Arc::new(Barrier::new(num_branches));

    let handles: Vec<_> = (0..num_branches)
        .map(|r| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                let branch_id = BranchId::new();
                let kv = KVStore::new(db.clone());
                let event = EventLog::new(db);

                barrier.wait();

                for i in 0..ops_per_branch {
                    kv.put(
                        &branch_id,
                        "default",
                        &format!("key_{}", i),
                        Value::Int((r * 1000 + i) as i64),
                    )
                    .unwrap();
                    event
                        .append(
                            &branch_id,
                            "default",
                            "ops",
                            int_payload((r * 1000 + i) as i64),
                        )
                        .unwrap();
                }

                // Verify own data
                for i in 0..ops_per_branch {
                    let val = kv
                        .get(&branch_id, "default", &format!("key_{}", i))
                        .unwrap()
                        .unwrap();
                    assert_eq!(val, Value::Int((r * 1000 + i) as i64));
                }

                branch_id
            })
        })
        .collect();

    let branch_ids: Vec<BranchId> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // Verify all branches have correct isolated data
    let kv = KVStore::new(test_db.db.clone());
    for (r, branch_id) in branch_ids.iter().enumerate() {
        let keys = kv.list(branch_id, "default", Some("key_")).unwrap();
        assert_eq!(keys.len(), ops_per_branch);

        for i in 0..ops_per_branch {
            let val = kv
                .get(branch_id, "default", &format!("key_{}", i))
                .unwrap()
                .unwrap();
            assert_eq!(val, Value::Int((r * 1000 + i) as i64));
        }
    }
}
