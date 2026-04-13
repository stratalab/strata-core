//! Branching and Branch Isolation Tests
//!
//! Tests branch isolation guarantees, branch management operations,
//! and branch operations (fork, diff, merge).

use crate::common::*;
use std::sync::{Arc, Barrier};
use std::thread;
use strata_core::id::CommitVersion;
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

    // Write KV to both with different values (to test "modified")
    p.kv.put(&id_x, "default", "shared-key", Value::Int(10))
        .unwrap();
    p.kv.put(&id_y, "default", "shared-key", Value::Int(20))
        .unwrap();

    let diff = branch_ops::diff_branches(&test_db.db, "x", "y").unwrap();

    // kv-key is only in x → removed
    // doc is only in y → added
    // shared-key is in both with different values → modified
    assert!(diff.summary.total_removed >= 1, "KV key should be removed");
    assert!(diff.summary.total_added >= 1, "JSON doc should be added");
    assert!(
        diff.summary.total_modified >= 1,
        "Shared KV key should be modified"
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
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Write initial data before fork
    kv.put(&target_id, "default", "shared", Value::Int(1))
        .unwrap();

    // Fork target → source
    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Both modify "shared" (conflict), source adds a new key
    kv.put(&target_id, "default", "shared", Value::Int(10))
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
        None,
    )
    .unwrap();
    assert!(info.keys_applied >= 2);

    // Target should have source's value for "shared" (LWW: source wins)
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
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Write initial data before fork
    kv.put(&target_id, "default", "shared", Value::Int(1))
        .unwrap();

    // Fork target → source
    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Both modify "shared" to different values (conflict)
    kv.put(&target_id, "default", "shared", Value::Int(10))
        .unwrap();
    kv.put(&source_id, "default", "shared", Value::Int(2))
        .unwrap();

    // Strict merge should fail with conflicts
    let result =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None);
    assert!(result.is_err());

    // Target should be unchanged
    assert_eq!(
        kv.get(&target_id, "default", "shared").unwrap(),
        Some(Value::Int(10))
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
        None,
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
// Event Merge Safety
// ============================================================================
//
// See docs/design/branching/primitive-aware-merge.md. The generic three-way
// merge treats EventLog records as opaque key/value pairs and silently
// corrupts the hash chain when both sides of a fork have appended to the
// same space. These tests pin down the new refusal behavior and verify
// that single-sided and cross-space-disjoint merges still produce a
// verifiable chain.

/// Walk a branch's event log seq=0..len and verify:
/// (a) each event's stored `hash` matches `compute_event_hash` recomputed
///     from its fields + the previous hash, and
/// (b) each event's `prev_hash` matches the previous event's stored `hash`.
///
/// Returns the number of events walked on success, or a descriptive error.
fn verify_event_chain(event_log: &EventLog, branch: &BranchId, space: &str) -> Result<u64, String> {
    use strata_engine::primitives::event::compute_event_hash;
    let len = event_log.len(branch, space).map_err(|e| e.to_string())?;
    let mut prev_hash = [0u8; 32];
    for seq in 0..len {
        let v = event_log
            .get(branch, space, seq)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("missing event at seq={seq}"))?;
        let ev = &v.value;
        if ev.prev_hash != prev_hash {
            return Err(format!(
                "prev_hash mismatch at seq={seq}: expected chain head {:?}, event stores {:?}",
                prev_hash, ev.prev_hash
            ));
        }
        let recomputed = compute_event_hash(
            ev.sequence,
            &ev.event_type,
            &ev.payload,
            ev.timestamp.as_micros(),
            &prev_hash,
        )
        .map_err(|e| e.to_string())?;
        if recomputed != ev.hash {
            return Err(format!(
                "hash mismatch at seq={seq}: recomputed {:?}, stored {:?}",
                recomputed, ev.hash
            ));
        }
        prev_hash = ev.hash;
    }
    Ok(len)
}

#[test]
fn event_merge_divergent_rejects() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let event = test_db.event();

    // Seed target with 3 events before fork so ancestor next_sequence=3.
    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    event
        .append(&target_id, "default", "t0", int_payload(0))
        .unwrap();
    event
        .append(&target_id, "default", "t1", int_payload(1))
        .unwrap();
    event
        .append(&target_id, "default", "t2", int_payload(2))
        .unwrap();
    assert_eq!(event.len(&target_id, "default").unwrap(), 3);

    // Fork target → source (source inherits the 3 events).
    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source appends 1 event of its own type.
    event
        .append(&source_id, "default", "src_type", int_payload(100))
        .unwrap();
    assert_eq!(event.len(&source_id, "default").unwrap(), 4);

    // Target appends 2 events of different types. Both sides have now
    // advanced past the fork's next_sequence=3.
    event
        .append(&target_id, "default", "tgt_a", int_payload(200))
        .unwrap();
    event
        .append(&target_id, "default", "tgt_b", int_payload(201))
        .unwrap();
    assert_eq!(event.len(&target_id, "default").unwrap(), 5);

    // LWW merge must be rejected.
    let err = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect_err("divergent event merge must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("merge unsupported: divergent event appends"),
        "error message should identify the refusal, got: {msg}"
    );
    assert!(
        msg.contains("space 'default'"),
        "error message should name the divergent space, got: {msg}"
    );

    // Strict merge must also be rejected (the divergence check fires
    // regardless of strategy, before the strategy-specific branch runs).
    let err =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None)
            .expect_err("divergent event merge must be rejected under Strict");
    assert!(
        err.to_string()
            .contains("merge unsupported: divergent event appends"),
        "Strict rejection should also use the divergence error"
    );

    // Target state must be untouched by the refused merges.
    assert_eq!(event.len(&target_id, "default").unwrap(), 5);
    let tgt_a_hits = event
        .get_by_type(&target_id, "default", "tgt_a", None, None)
        .unwrap();
    assert_eq!(tgt_a_hits.len(), 1, "target's tgt_a event must survive");
    assert_eq!(tgt_a_hits[0].value.payload, int_payload(200));
    let src_hits = event
        .get_by_type(&target_id, "default", "src_type", None, None)
        .unwrap();
    assert!(
        src_hits.is_empty(),
        "source's events must not have leaked into target"
    );

    // Target's own chain is still walkable end-to-end.
    let walked = verify_event_chain(&event, &target_id, "default").expect("target chain valid");
    assert_eq!(walked, 5);
}

#[test]
fn event_merge_single_sided_same_space_succeeds() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let event = test_db.event();

    // Seed target with 3 events before fork.
    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    event
        .append(&target_id, "default", "t0", int_payload(0))
        .unwrap();
    event
        .append(&target_id, "default", "t1", int_payload(1))
        .unwrap();
    event
        .append(&target_id, "default", "t2", int_payload(2))
        .unwrap();

    // Fork, then append only on source.
    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");
    event
        .append(&source_id, "default", "src_a", int_payload(100))
        .unwrap();
    event
        .append(&source_id, "default", "src_b", int_payload(101))
        .unwrap();
    assert_eq!(event.len(&source_id, "default").unwrap(), 5);
    assert_eq!(event.len(&target_id, "default").unwrap(), 3);

    // Single-sided merge should succeed through the generic path.
    let info = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("single-sided event merge should succeed");
    assert!(
        info.keys_applied > 0,
        "merge should apply source's new event records and metadata row"
    );

    // Target now has all 5 events.
    assert_eq!(event.len(&target_id, "default").unwrap(), 5);

    // Chain walks cleanly from seq=0 to seq=4.
    let walked = verify_event_chain(&event, &target_id, "default")
        .expect("target chain must verify after single-sided merge");
    assert_eq!(walked, 5);

    // Per-type index is consistent: source's events are queryable on target.
    let src_a_hits = event
        .get_by_type(&target_id, "default", "src_a", None, None)
        .unwrap();
    assert_eq!(src_a_hits.len(), 1);
    assert_eq!(src_a_hits[0].value.payload, int_payload(100));
    let src_b_hits = event
        .get_by_type(&target_id, "default", "src_b", None, None)
        .unwrap();
    assert_eq!(src_b_hits.len(), 1);
    assert_eq!(src_b_hits[0].value.payload, int_payload(101));

    // Target's pre-fork events are still in place too.
    let t0_hits = event
        .get_by_type(&target_id, "default", "t0", None, None)
        .unwrap();
    assert_eq!(t0_hits.len(), 1);
    assert_eq!(t0_hits[0].value.payload, int_payload(0));
}

#[test]
fn event_merge_target_only_appends_succeeds() {
    // Symmetric to `event_merge_single_sided_same_space_succeeds`: only target
    // appends after fork. The event divergence check must pass and the merge
    // must leave target's new events intact (merge is a no-op for Event on
    // the generic path because source's meta matches ancestor → `SourceChanged`
    // is never produced, and target's own additions classify as `TargetAdded`).
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let event = test_db.event();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    event
        .append(&target_id, "default", "t0", int_payload(0))
        .unwrap();
    event
        .append(&target_id, "default", "t1", int_payload(1))
        .unwrap();
    event
        .append(&target_id, "default", "t2", int_payload(2))
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Target only: append two events. Source is untouched.
    event
        .append(&target_id, "default", "tgt_new", int_payload(50))
        .unwrap();
    event
        .append(&target_id, "default", "tgt_new", int_payload(51))
        .unwrap();
    assert_eq!(event.len(&source_id, "default").unwrap(), 3);
    assert_eq!(event.len(&target_id, "default").unwrap(), 5);

    // Merge source → target with LWW. Check must not falsely fire (source
    // did not diverge from ancestor).
    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("target-only event merge should succeed");

    // Target's new events must still be present and walkable end-to-end.
    assert_eq!(event.len(&target_id, "default").unwrap(), 5);
    let walked = verify_event_chain(&event, &target_id, "default")
        .expect("target chain must verify after target-only merge");
    assert_eq!(walked, 5);
    let new_hits = event
        .get_by_type(&target_id, "default", "tgt_new", None, None)
        .unwrap();
    assert_eq!(new_hits.len(), 2);
    assert_eq!(new_hits[0].value.payload, int_payload(50));
    assert_eq!(new_hits[1].value.payload, int_payload(51));
}

#[test]
fn event_append_auto_registers_space() {
    // Regression test for the EventLog space auto-registration gap that was
    // worked around in an earlier cross-space merge test (PR #2330).
    //
    // `EventLog::append` must register non-default spaces with `SpaceIndex`
    // so that `branch_ops::merge_branches` (which iterates via
    // `SpaceIndex::list`) and other space-aware callers can see them.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let space_index = SpaceIndex::new(test_db.db.clone());
    let event = test_db.event();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Spaces are absent from the index before any append.
    let spaces_before = space_index.list(target_id).unwrap();
    assert!(!spaces_before.contains(&"orders".to_string()));
    assert!(!spaces_before.contains(&"users".to_string()));

    // Append into two non-default spaces. Each should auto-register.
    event
        .append(&target_id, "orders", "created", int_payload(0))
        .unwrap();
    event
        .append(&target_id, "users", "signup", int_payload(1))
        .unwrap();

    // Both spaces are now visible to SpaceIndex.
    let spaces_after = space_index.list(target_id).unwrap();
    assert!(
        spaces_after.contains(&"orders".to_string()),
        "orders space must be auto-registered by EventLog::append, got: {spaces_after:?}"
    );
    assert!(
        spaces_after.contains(&"users".to_string()),
        "users space must be auto-registered by EventLog::append, got: {spaces_after:?}"
    );
    assert!(
        space_index.exists(target_id, "orders").unwrap(),
        "SpaceIndex::exists must return true for auto-registered space"
    );

    // Idempotence: subsequent appends to the same space must not fail or
    // duplicate the registration.
    event
        .append(&target_id, "orders", "paid", int_payload(2))
        .unwrap();
    let spaces_after_again = space_index.list(target_id).unwrap();
    let orders_count = spaces_after_again
        .iter()
        .filter(|s| s.as_str() == "orders")
        .count();
    assert_eq!(
        orders_count, 1,
        "auto-registration must be idempotent — orders space appears {orders_count} times"
    );

    // batch_append should also auto-register a fresh space.
    event
        .batch_append(
            &target_id,
            "audits",
            vec![
                ("created".to_string(), int_payload(10)),
                ("updated".to_string(), int_payload(11)),
            ],
        )
        .unwrap();
    let spaces_with_audits = space_index.list(target_id).unwrap();
    assert!(
        spaces_with_audits.contains(&"audits".to_string()),
        "batch_append must auto-register the space, got: {spaces_with_audits:?}"
    );
}

#[test]
fn event_merge_cross_space_divergence_succeeds() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let event = test_db.event();

    // Seed both spaces on target before fork. With EventLog auto-registering
    // spaces on first append, no explicit `space_index.register` is needed
    // for `merge_branches` to discover these spaces — this test exercises
    // the auto-registration end-to-end through the merge path.
    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    event
        .append(&target_id, "orders", "created", int_payload(0))
        .unwrap();
    event
        .append(&target_id, "orders", "paid", int_payload(1))
        .unwrap();
    event
        .append(&target_id, "users", "signup", int_payload(10))
        .unwrap();
    event
        .append(&target_id, "users", "login", int_payload(11))
        .unwrap();

    // Fork.
    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source appends only in "orders"; target appends only in "users".
    // Each space is single-sided from the merge's point of view.
    event
        .append(&source_id, "orders", "shipped", int_payload(2))
        .unwrap();
    event
        .append(&target_id, "users", "logout", int_payload(12))
        .unwrap();

    // Merge should succeed: per-space divergence check allows this shape.
    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("cross-space divergent event merge should succeed");

    // "orders" space on target got source's new event.
    assert_eq!(event.len(&target_id, "orders").unwrap(), 3);
    let shipped_hits = event
        .get_by_type(&target_id, "orders", "shipped", None, None)
        .unwrap();
    assert_eq!(shipped_hits.len(), 1);
    assert_eq!(shipped_hits[0].value.payload, int_payload(2));

    // "users" space on target kept its own new event.
    assert_eq!(event.len(&target_id, "users").unwrap(), 3);
    let logout_hits = event
        .get_by_type(&target_id, "users", "logout", None, None)
        .unwrap();
    assert_eq!(logout_hits.len(), 1);
    assert_eq!(logout_hits[0].value.payload, int_payload(12));

    // Both spaces' chains walk cleanly.
    let orders_len = verify_event_chain(&event, &target_id, "orders")
        .expect("orders chain must verify after merge");
    assert_eq!(orders_len, 3);
    let users_len = verify_event_chain(&event, &target_id, "users")
        .expect("users chain must verify after merge");
    assert_eq!(users_len, 3);
}

// ============================================================================
// Graph Merge Safety: semantic three-way merge
// ============================================================================
//
// See docs/design/branching/primitive-aware-merge.md. The graph merge
// performs decoded-edge-level diffing, additive merging of disjoint
// edges, and referential integrity validation. Single-sided merges work,
// dangling-edge / orphan-reference scenarios reject (with structured
// errors), and disjoint additions on both sides SUCCEED rather than
// being conservatively rejected.

#[test]
fn graph_merge_disjoint_node_additions_succeeds() {
    use strata_graph::types::{EdgeData, NodeData};

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    // Seed the target branch with two nodes before fork.
    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "bob", NodeData::default())
        .unwrap();

    // Fork target → source.
    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Both branches modify graph state since the fork.
    // Source adds a new node "carol" and an edge alice→carol.
    p.graph
        .add_node(source_id, "default", "g", "carol", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();
    // Target adds a different new node "dave" and edge bob→dave.
    p.graph
        .add_node(target_id, "default", "g", "dave", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            target_id,
            "default",
            "g",
            "bob",
            "dave",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    // Disjoint additions on both sides merge cleanly. The earlier
    // tactical refusal would have rejected this scenario; the semantic
    // merge correctly recognizes that source's "carol+alice→carol" and
    // target's "dave+bob→dave" are independent and combinable.
    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("disjoint divergent graph merge should succeed");

    // After the merge, target has BOTH source's additions AND its own.
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "alice")
        .unwrap()
        .is_some());
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "bob")
        .unwrap()
        .is_some());
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "carol")
        .unwrap()
        .is_some());
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "dave")
        .unwrap()
        .is_some());

    // Bidirectional consistency: alice→carol visible from both sides.
    let alice_outgoing = p
        .graph
        .outgoing_neighbors(target_id, "default", "g", "alice", None)
        .unwrap();
    let alice_targets: std::collections::HashSet<String> =
        alice_outgoing.iter().map(|n| n.node_id.clone()).collect();
    assert!(
        alice_targets.contains("carol"),
        "alice→carol missing on target"
    );

    let carol_incoming = p
        .graph
        .incoming_neighbors(target_id, "default", "g", "carol", None)
        .unwrap();
    let carol_sources: std::collections::HashSet<String> =
        carol_incoming.iter().map(|n| n.node_id.clone()).collect();
    assert!(
        carol_sources.contains("alice"),
        "carol's incoming must include alice (bidirectional consistency)"
    );

    // bob→dave (target's edge) also intact.
    let bob_outgoing = p
        .graph
        .outgoing_neighbors(target_id, "default", "g", "bob", None)
        .unwrap();
    let bob_targets: std::collections::HashSet<String> =
        bob_outgoing.iter().map(|n| n.node_id.clone()).collect();
    assert!(bob_targets.contains("dave"));

    let dave_incoming = p
        .graph
        .incoming_neighbors(target_id, "default", "g", "dave", None)
        .unwrap();
    let dave_sources: std::collections::HashSet<String> =
        dave_incoming.iter().map(|n| n.node_id.clone()).collect();
    assert!(dave_sources.contains("bob"));
}

#[test]
fn graph_merge_single_sided_source_succeeds() {
    use strata_graph::types::{EdgeData, NodeData};

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    // Seed target with two pre-fork nodes.
    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "bob", NodeData::default())
        .unwrap();

    // Fork.
    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Only source modifies graph state — adds carol and an edge alice→carol.
    p.graph
        .add_node(source_id, "default", "g", "carol", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();
    // Also add a second edge to exercise multiple writes on the same fwd list.
    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "bob",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    // Single-sided merge must succeed (graph divergence check is per-side).
    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("single-sided graph merge should succeed");

    // Target now has carol.
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "carol")
        .unwrap()
        .is_some());

    // Bidirectional consistency: outgoing_neighbors(alice) and
    // incoming_neighbors(carol) must agree on the alice→carol edge.
    let alice_outgoing = p
        .graph
        .outgoing_neighbors(target_id, "default", "g", "alice", None)
        .unwrap();
    let alice_targets: std::collections::HashSet<String> =
        alice_outgoing.iter().map(|n| n.node_id.clone()).collect();
    assert!(
        alice_targets.contains("carol"),
        "alice→carol must be visible from outgoing_neighbors, got: {alice_targets:?}"
    );
    assert!(
        alice_targets.contains("bob"),
        "alice→bob must be visible from outgoing_neighbors, got: {alice_targets:?}"
    );

    let carol_incoming = p
        .graph
        .incoming_neighbors(target_id, "default", "g", "carol", None)
        .unwrap();
    let carol_sources: std::collections::HashSet<String> =
        carol_incoming.iter().map(|n| n.node_id.clone()).collect();
    assert!(
        carol_sources.contains("alice"),
        "carol's incoming must include alice, got: {carol_sources:?}"
    );

    let bob_incoming = p
        .graph
        .incoming_neighbors(target_id, "default", "g", "bob", None)
        .unwrap();
    let bob_sources: std::collections::HashSet<String> =
        bob_incoming.iter().map(|n| n.node_id.clone()).collect();
    assert!(
        bob_sources.contains("alice"),
        "bob's incoming must include alice, got: {bob_sources:?}"
    );
}

#[test]
fn graph_merge_single_sided_target_succeeds() {
    use strata_graph::types::{EdgeData, NodeData};

    // Symmetric to the source-only case: only target adds graph data after
    // the fork. The generic merge path is a no-op for graph in this case
    // (every cell classifies as TargetAdded/TargetChanged → no action), and
    // the divergence check must not fire because source did not diverge.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();

    // Only target modifies graph state.
    p.graph
        .add_node(target_id, "default", "g", "bob", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            target_id,
            "default",
            "g",
            "alice",
            "bob",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("target-only graph merge should succeed");

    // Target's post-fork additions must still be present and bidirectionally
    // consistent.
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "bob")
        .unwrap()
        .is_some());
    let alice_outgoing = p
        .graph
        .outgoing_neighbors(target_id, "default", "g", "alice", None)
        .unwrap();
    assert_eq!(alice_outgoing.len(), 1);
    assert_eq!(alice_outgoing[0].node_id, "bob");
    let bob_incoming = p
        .graph
        .incoming_neighbors(target_id, "default", "g", "bob", None)
        .unwrap();
    assert_eq!(bob_incoming.len(), 1);
    assert_eq!(bob_incoming[0].node_id, "alice");
}

#[test]
fn graph_merge_no_changes_does_not_block_kv_merge() {
    // Regression: a merge where neither side touches graph state, but both
    // sides make divergent KV writes, must not falsely trip the graph
    // divergence check. The KV LWW resolution should run normally.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    kv.put(&target_id, "default", "shared", Value::Int(1))
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Both sides modify the same KV key — generates a conflict that LWW
    // should resolve. No graph activity on either side.
    kv.put(&target_id, "default", "shared", Value::Int(10))
        .unwrap();
    kv.put(&source_id, "default", "shared", Value::Int(20))
        .unwrap();

    let info = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("KV merge with empty graph state must not be blocked by graph check");
    assert!(info.keys_applied >= 1);
    assert_eq!(
        kv.get(&target_id, "default", "shared").unwrap(),
        Some(Value::Int(20)),
        "LWW resolution must apply source's value to target"
    );
}

#[test]
fn graph_merge_concurrent_node_delete_and_edge_add_rejected() {
    use strata_graph::types::{EdgeData, NodeData};

    // Scenario B from the design doc — the most subtle corruption pattern.
    // Pre-fork: nodes alice and bob exist on target.
    // Source: deletes alice.
    // Target: adds an outgoing edge alice→bob (alice still exists on target).
    // The semantic merge sees: projected nodes lacks alice, but projected
    // edges has alice→bob → DanglingEdge / OrphanedReference. Both are
    // fatal regardless of strategy, so the merge is refused with a
    // structured error before any writes happen.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "bob", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source deletes alice.
    p.graph
        .remove_node(source_id, "default", "g", "alice")
        .unwrap();
    // Target adds an edge alice→bob (alice still exists on target).
    p.graph
        .add_edge(
            target_id,
            "default",
            "g",
            "alice",
            "bob",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    // LWW must reject — referential integrity is fatal regardless of
    // strategy. The graph plan function emits a structured error naming
    // the violation.
    let err = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect_err("dangling edge / orphan reference must be rejected even under LWW");
    let msg = err.to_string();
    assert!(
        msg.contains("merge unsupported: graph referential integrity violation"),
        "rejection must use the structured error, got: {msg}"
    );
    assert!(
        msg.contains("alice"),
        "error must name the offending node, got: {msg}"
    );

    // Strict must also reject (same error path).
    let err =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None)
            .expect_err("dangling edge / orphan reference must reject under Strict too");
    assert!(
        err.to_string()
            .contains("merge unsupported: graph referential integrity violation"),
        "Strict rejection should use the same structured error"
    );

    // Target state untouched: alice still exists, alice→bob edge intact.
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "alice")
        .unwrap()
        .is_some());
    let alice_outgoing = p
        .graph
        .outgoing_neighbors(target_id, "default", "g", "alice", None)
        .unwrap();
    assert_eq!(alice_outgoing.len(), 1);
    assert_eq!(alice_outgoing[0].node_id, "bob");
}

// ============================================================================
// Additional graph merge test cases
// ============================================================================

#[test]
fn graph_merge_disjoint_edge_additions_succeeds() {
    use strata_graph::types::{EdgeData, NodeData};

    // Pre-fork: alice, bob, carol, dave (no edges).
    // Source adds alice→carol. Target adds bob→dave.
    // The earlier tactical refusal would have rejected this; the
    // semantic merge combines the disjoint edges additively.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    for n in &["alice", "bob", "carol", "dave"] {
        p.graph
            .add_node(target_id, "default", "g", n, NodeData::default())
            .unwrap();
    }

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();
    p.graph
        .add_edge(
            target_id,
            "default",
            "g",
            "bob",
            "dave",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("disjoint edge additions on both sides should merge cleanly");

    // Both edges present and bidirectionally consistent on target.
    let alice_out = p
        .graph
        .outgoing_neighbors(target_id, "default", "g", "alice", None)
        .unwrap();
    assert_eq!(alice_out.len(), 1);
    assert_eq!(alice_out[0].node_id, "carol");

    let carol_in = p
        .graph
        .incoming_neighbors(target_id, "default", "g", "carol", None)
        .unwrap();
    assert_eq!(carol_in.len(), 1);
    assert_eq!(carol_in[0].node_id, "alice");

    let bob_out = p
        .graph
        .outgoing_neighbors(target_id, "default", "g", "bob", None)
        .unwrap();
    assert_eq!(bob_out.len(), 1);
    assert_eq!(bob_out[0].node_id, "dave");

    let dave_in = p
        .graph
        .incoming_neighbors(target_id, "default", "g", "dave", None)
        .unwrap();
    assert_eq!(dave_in.len(), 1);
    assert_eq!(dave_in[0].node_id, "bob");
}

#[test]
fn graph_merge_dangling_edge_rejected() {
    use strata_graph::types::{EdgeData, NodeData};

    // Pre-fork: alice, carol exist.
    // Source adds edge alice→carol.
    // Target deletes carol.
    // Result: projected edges has alice→carol but projected nodes lacks
    // carol → DanglingEdge → fatal regardless of strategy.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "carol", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();
    p.graph
        .remove_node(target_id, "default", "g", "carol")
        .unwrap();

    let err = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect_err("dangling edge must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("merge unsupported: graph referential integrity violation"),
        "rejection must use the structured error, got: {msg}"
    );
    assert!(
        msg.contains("carol") || msg.contains("alice"),
        "error must name an involved node, got: {msg}"
    );
}

#[test]
fn graph_merge_conflicting_node_props_lww_source_wins() {
    use strata_graph::types::NodeData;

    // Pre-fork: alice exists with role=user.
    // Source modifies alice's properties to role=admin.
    // Target modifies alice's properties to role=guest.
    // Under LWW, source wins; under Strict, the merge rejects.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    let original = NodeData {
        entity_ref: None,
        properties: Some(serde_json::json!({"role": "user"})),
        object_type: None,
    };
    p.graph
        .add_node(target_id, "default", "g", "alice", original)
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    let source_data = NodeData {
        entity_ref: None,
        properties: Some(serde_json::json!({"role": "admin"})),
        object_type: None,
    };
    let target_data = NodeData {
        entity_ref: None,
        properties: Some(serde_json::json!({"role": "guest"})),
        object_type: None,
    };
    p.graph
        .add_node(source_id, "default", "g", "alice", source_data.clone())
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", target_data)
        .unwrap();

    let info = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("LWW node prop conflict should resolve, not error");

    // Conflict was reported in MergeInfo.conflicts even though merge succeeded.
    assert!(
        !info.conflicts.is_empty(),
        "conflict should be reported in MergeInfo.conflicts under LWW"
    );

    // Source's value won.
    let alice = p
        .graph
        .get_node(target_id, "default", "g", "alice")
        .unwrap()
        .expect("alice still exists post-merge");
    assert_eq!(alice, source_data);
}

#[test]
fn graph_merge_conflicting_node_props_strict_rejects() {
    use strata_graph::types::NodeData;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    let original = NodeData {
        entity_ref: None,
        properties: Some(serde_json::json!({"role": "user"})),
        object_type: None,
    };
    p.graph
        .add_node(target_id, "default", "g", "alice", original.clone())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    p.graph
        .add_node(
            source_id,
            "default",
            "g",
            "alice",
            NodeData {
                entity_ref: None,
                properties: Some(serde_json::json!({"role": "admin"})),
                object_type: None,
            },
        )
        .unwrap();
    p.graph
        .add_node(
            target_id,
            "default",
            "g",
            "alice",
            NodeData {
                entity_ref: None,
                properties: Some(serde_json::json!({"role": "guest"})),
                object_type: None,
            },
        )
        .unwrap();

    let err =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None)
            .expect_err("Strict must reject node property conflicts");
    assert!(
        err.to_string().contains("Merge conflict"),
        "Strict rejection message should mention conflict, got: {err}"
    );
}

// ============================================================================
// Cherry-pick semantic graph merge + additive catalog
// ============================================================================

/// Cherry-pick of disjoint graph divergences succeeds: the per-handler
/// `plan` dispatch is wired into `cherry_pick_from_diff`. Earlier, this
/// scenario was rejected by the tactical refusal.
#[test]
fn cherry_pick_graph_disjoint_node_additions_succeeds() {
    use strata_engine::branch_ops::CherryPickFilter;
    use strata_graph::types::{EdgeData, NodeData};

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    // Pre-fork: target has alice and bob.
    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "bob", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source: adds carol + alice→carol.
    p.graph
        .add_node(source_id, "default", "g", "carol", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();
    // Target: adds dave + bob→dave (disjoint from source's changes).
    p.graph
        .add_node(target_id, "default", "g", "dave", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            target_id,
            "default",
            "g",
            "bob",
            "dave",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    // Cherry-pick with no filter (default = include everything). The
    // cherry-pick path hands this off to the per-handler plan dispatch,
    // which produces the same semantic merge result as `merge_branches`.
    branch_ops::cherry_pick_from_diff(
        &test_db.db,
        "source",
        "target",
        CherryPickFilter::default(),
        None,
    )
    .expect("cherry-pick of disjoint graph divergences must succeed");

    // Target now has all four nodes, with bidirectional consistency on the
    // alice→carol edge that source added.
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "alice")
        .unwrap()
        .is_some());
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "carol")
        .unwrap()
        .is_some());
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "dave")
        .unwrap()
        .is_some());

    let alice_outgoing = p
        .graph
        .outgoing_neighbors(target_id, "default", "g", "alice", None)
        .unwrap();
    let alice_targets: std::collections::HashSet<String> =
        alice_outgoing.iter().map(|n| n.node_id.clone()).collect();
    assert!(
        alice_targets.contains("carol"),
        "alice→carol must be visible after cherry-pick, got {alice_targets:?}"
    );
    let carol_incoming = p
        .graph
        .incoming_neighbors(target_id, "default", "g", "carol", None)
        .unwrap();
    let carol_sources: std::collections::HashSet<String> =
        carol_incoming.iter().map(|n| n.node_id.clone()).collect();
    assert!(
        carol_sources.contains("alice"),
        "carol's incoming must include alice (bidirectional consistency), got {carol_sources:?}"
    );
}

/// Dangling-edge scenarios are rejected through cherry-pick — referential
/// integrity is enforced inside the graph plan and fatal conflicts
/// propagate as `Err`.
#[test]
fn cherry_pick_graph_dangling_edge_rejected() {
    use strata_engine::branch_ops::CherryPickFilter;
    use strata_graph::types::{EdgeData, NodeData};

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "carol", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source: adds edge alice→carol.
    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();
    // Target: deletes carol. After cherry-pick, the projected edge set would
    // include alice→carol but the projected node set would lack carol →
    // dangling edge / orphan reference, both fatal.
    p.graph
        .remove_node(target_id, "default", "g", "carol")
        .unwrap();

    let err = branch_ops::cherry_pick_from_diff(
        &test_db.db,
        "source",
        "target",
        CherryPickFilter::default(),
        None,
    )
    .expect_err("dangling-edge cherry-pick must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("graph referential integrity violation")
            || msg.contains("dangling")
            || msg.contains("orphaned"),
        "expected referential-integrity error from cherry-pick, got: {msg}"
    );
}

/// Cherry-pick with `filter.keys` set to a partial subset of graph keys
/// must refuse with the atomicity error — the filter would split a
/// (space, Graph) cell's interdependent actions and break bidirectional
/// consistency.
#[test]
fn cherry_pick_graph_atomic_filter_with_keys_rejected() {
    use strata_engine::branch_ops::CherryPickFilter;
    use strata_graph::types::{EdgeData, NodeData};

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "bob", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source: add a new node carol AND a new edge alice→carol. The plan
    // produces multiple graph actions: n/carol, fwd/alice, rev/carol,
    // __edge_count__/follows. A filter that names only g/n/carol would
    // include the node but exclude the adjacency entries — exactly the
    // partial-graph scenario the atomicity guard exists to catch.
    p.graph
        .add_node(source_id, "default", "g", "carol", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    let filter = CherryPickFilter {
        spaces: None,
        keys: Some(vec!["g/n/carol".to_string()]),
        primitives: None,
    };
    let err = branch_ops::cherry_pick_from_diff(&test_db.db, "source", "target", filter, None)
        .expect_err("partial graph key filter must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("graph data must be applied atomically"),
        "expected atomicity error, got: {msg}"
    );
}

/// Cherry-pick with `primitives = [KV]` cleanly drops all graph actions
/// even when the source has divergent graph state. No atomicity error
/// fires (the primitives filter partitions cells atomically).
#[test]
fn cherry_pick_graph_excluded_via_primitives_filter_works() {
    use strata_core::PrimitiveType;
    use strata_engine::branch_ops::CherryPickFilter;
    use strata_engine::primitives::kv::KVStore;
    use strata_graph::types::{EdgeData, NodeData};

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();
    let kv: &KVStore = &p.kv;

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();
    kv.put(&target_id, "default", "before_fork", Value::Int(1))
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source: divergent graph + new KV.
    p.graph
        .add_node(source_id, "default", "g", "carol", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();
    kv.put(&source_id, "default", "kv_only", Value::Int(42))
        .unwrap();

    // Filter to KV only. The graph plan produces actions but the primitives
    // filter atomically excludes them all → no atomicity error.
    let filter = CherryPickFilter {
        spaces: None,
        keys: None,
        primitives: Some(vec![PrimitiveType::Kv]),
    };
    let info = branch_ops::cherry_pick_from_diff(&test_db.db, "source", "target", filter, None)
        .expect("KV-only cherry-pick must succeed even with divergent graph");
    assert!(info.keys_applied >= 1, "expected the kv_only put to apply");

    // KV applied; graph state on target must be unchanged (carol absent).
    assert_eq!(
        kv.get(&target_id, "default", "kv_only").unwrap(),
        Some(Value::Int(42))
    );
    assert!(
        p.graph
            .get_node(target_id, "default", "g", "carol")
            .unwrap()
            .is_none(),
        "graph data must NOT have been applied under primitives=[KV] filter"
    );
}

/// Cherry-pick where `filter.keys` is set to a non-graph key and the
/// source has divergent graph data. The atomicity guard's "0 graph
/// actions pass" branch must drop all graph actions atomically without
/// raising an atomicity error. This is the only test that exercises the
/// atomicity helper through the loop branch (`filter.keys.is_some()`)
/// with a clean drop outcome — the `primitives = [KV]` test goes through
/// the early-return path because `filter.keys` is None there.
#[test]
fn cherry_pick_graph_dropped_via_non_graph_key_filter_succeeds() {
    use strata_engine::branch_ops::CherryPickFilter;
    use strata_engine::primitives::kv::KVStore;
    use strata_graph::types::{EdgeData, NodeData};

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();
    let kv: &KVStore = &p.kv;

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();
    kv.put(&target_id, "default", "doc/foo", Value::Int(1))
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source diverges in BOTH graph (adds carol + alice→carol) AND KV
    // (adds doc/bar). The graph plan will produce multiple actions.
    p.graph
        .add_node(source_id, "default", "g", "carol", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();
    kv.put(&source_id, "default", "doc/bar", Value::Int(2))
        .unwrap();

    // Filter to a single KV key. None of the graph plan actions match it,
    // so the atomicity check must observe `passed=0, total>0` and drop
    // them all WITHOUT raising "graph data must be applied atomically".
    let filter = CherryPickFilter {
        spaces: None,
        keys: Some(vec!["doc/bar".to_string()]),
        primitives: None,
    };
    let info = branch_ops::cherry_pick_from_diff(&test_db.db, "source", "target", filter, None)
        .expect("filter that drops all graph actions atomically must NOT raise atomicity error");
    assert_eq!(info.keys_applied, 1, "only doc/bar should have applied");

    // KV target side reflects the cherry-picked key.
    assert_eq!(
        kv.get(&target_id, "default", "doc/bar").unwrap(),
        Some(Value::Int(2)),
    );
    // Graph side is untouched on target.
    assert!(
        p.graph
            .get_node(target_id, "default", "g", "carol")
            .unwrap()
            .is_none(),
        "graph data must NOT have been cherry-picked"
    );
}

/// KV-only cherry-pick when neither side touched graph state must
/// succeed without invoking the atomicity guard at all (no graph
/// actions in the plan).
#[test]
fn cherry_pick_kv_only_no_graph_changes_works() {
    use strata_engine::branch_ops::CherryPickFilter;
    use strata_engine::primitives::kv::KVStore;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();
    let kv: &KVStore = &p.kv;

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    kv.put(&target_id, "default", "k1", Value::Int(1)).unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    kv.put(&source_id, "default", "k2", Value::Int(2)).unwrap();

    let info = branch_ops::cherry_pick_from_diff(
        &test_db.db,
        "source",
        "target",
        CherryPickFilter::default(),
        None,
    )
    .expect("KV-only cherry-pick must succeed");
    assert!(info.keys_applied >= 1);
    assert_eq!(
        kv.get(&target_id, "default", "k2").unwrap(),
        Some(Value::Int(2))
    );
}

/// Additive catalog merge: both branches concurrently `create_graph` for
/// different names. The earlier behavior rejected this with
/// `CatalogDivergence`; the additive catalog merge merges the catalogs
/// as a set union — both new graphs coexist.
#[test]
fn graph_merge_catalog_additive_disjoint_creates_succeeds() {
    use strata_graph::types::NodeData;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g_pre", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g_pre", "x", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source creates a brand new graph "g_src".
    p.graph
        .create_graph(source_id, "default", "g_src", None)
        .unwrap();
    p.graph
        .add_node(
            source_id,
            "default",
            "g_src",
            "src_node",
            NodeData::default(),
        )
        .unwrap();
    // Target creates a brand new graph "g_tgt".
    p.graph
        .create_graph(target_id, "default", "g_tgt", None)
        .unwrap();
    p.graph
        .add_node(
            target_id,
            "default",
            "g_tgt",
            "tgt_node",
            NodeData::default(),
        )
        .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("additive catalog must merge concurrent creates of different graphs");

    // Target now lists all three graphs.
    let graphs = p.graph.list_graphs(target_id, "default").unwrap();
    let graph_set: std::collections::HashSet<String> = graphs.into_iter().collect();
    assert!(
        graph_set.contains("g_pre") && graph_set.contains("g_src") && graph_set.contains("g_tgt"),
        "merged catalog must contain all three graphs, got {graph_set:?}"
    );

    // The new graphs' data is materialized too.
    assert!(p
        .graph
        .get_node(target_id, "default", "g_src", "src_node")
        .unwrap()
        .is_some());
    assert!(p
        .graph
        .get_node(target_id, "default", "g_tgt", "tgt_node")
        .unwrap()
        .is_some());
}

/// Additive catalog merge: source deletes one graph, target creates a
/// different graph. Both intents are honored — the deleted graph is
/// dropped from the catalog, the new graph is added.
#[test]
fn graph_merge_catalog_additive_one_deletes_one_creates_succeeds() {
    use strata_graph::types::NodeData;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g_keep", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g_keep", "x", NodeData::default())
        .unwrap();
    p.graph
        .create_graph(target_id, "default", "g_drop", None)
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source deletes g_drop.
    p.graph
        .delete_graph(source_id, "default", "g_drop")
        .unwrap();
    // Target creates g_new.
    p.graph
        .create_graph(target_id, "default", "g_new", None)
        .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("additive catalog must handle mixed delete + create");

    let graphs: std::collections::HashSet<String> = p
        .graph
        .list_graphs(target_id, "default")
        .unwrap()
        .into_iter()
        .collect();
    assert!(graphs.contains("g_keep"), "g_keep must remain");
    assert!(graphs.contains("g_new"), "g_new must be added by target");
    assert!(
        !graphs.contains("g_drop"),
        "g_drop must be dropped (source's delete intent) — got {graphs:?}"
    );
}

// ============================================================================
// COW Inherited Layer Integration Tests (#1666, #1667)
// ============================================================================

/// #1667: Diff across inherited layers after COW fork.
///
/// After forking, the child inherits parent data through COW layers.
/// Modifications and deletions on the child should appear correctly in diff.
#[test]
fn cow_diff_across_inherited_layers() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // 1. Create parent with keys {a: 1, b: 2, c: 3}
    branch_index.create_branch("parent").unwrap();
    let parent_id = strata_engine::primitives::branch::resolve_branch_name("parent");
    kv.put(&parent_id, "default", "a", Value::Int(1)).unwrap();
    kv.put(&parent_id, "default", "b", Value::Int(2)).unwrap();
    kv.put(&parent_id, "default", "c", Value::Int(3)).unwrap();

    // 2. COW fork parent → child
    branch_ops::fork_branch(&test_db.db, "parent", "child").unwrap();
    let child_id = strata_engine::primitives::branch::resolve_branch_name("child");

    // 3. Child modifies b and deletes c
    kv.put(&child_id, "default", "b", Value::Int(20)).unwrap();
    kv.delete(&child_id, "default", "c").unwrap();

    // 4. diff_branches(parent, child): b modified, c removed
    let diff = branch_ops::diff_branches(&test_db.db, "parent", "child").unwrap();
    assert_eq!(
        diff.summary.total_modified, 1,
        "b should be modified (2 → 20)"
    );
    assert_eq!(
        diff.summary.total_removed, 1,
        "c should be removed (in parent, deleted in child)"
    );
    assert_eq!(diff.summary.total_added, 0, "no new keys added in child");

    // 5. Inverse diff: diff_branches(child, parent)
    let inv = branch_ops::diff_branches(&test_db.db, "child", "parent").unwrap();
    assert_eq!(inv.summary.total_modified, 1, "b modified in inverse");
    assert_eq!(
        inv.summary.total_added, 1,
        "c appears as added (in parent, not in child)"
    );
    assert_eq!(
        inv.summary.total_removed, 0,
        "nothing removed from child→parent perspective"
    );
}

/// #1667: Diff after COW fork with no changes should show empty diff.
#[test]
fn cow_diff_no_changes_is_empty() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    branch_index.create_branch("parent").unwrap();
    let parent_id = strata_engine::primitives::branch::resolve_branch_name("parent");
    kv.put(&parent_id, "default", "x", Value::Int(1)).unwrap();
    kv.put(&parent_id, "default", "y", Value::Int(2)).unwrap();

    branch_ops::fork_branch(&test_db.db, "parent", "child").unwrap();

    // No modifications — diff should be empty
    let diff = branch_ops::diff_branches(&test_db.db, "parent", "child").unwrap();
    assert_eq!(diff.summary.total_added, 0);
    assert_eq!(diff.summary.total_removed, 0);
    assert_eq!(diff.summary.total_modified, 0);
}

/// #1666: Two-way LWW merge after COW fork.
///
/// Current merge is two-way (no ancestor awareness). LWW applies ALL
/// source (child) entries that differ from target (parent), including
/// inherited values the child didn't actually modify.
#[test]
fn cow_merge_lww_with_inherited_layers() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // 1. Create parent with keys {a: 1, b: 2, c: 3}
    branch_index.create_branch("parent").unwrap();
    let parent_id = strata_engine::primitives::branch::resolve_branch_name("parent");
    kv.put(&parent_id, "default", "a", Value::Int(1)).unwrap();
    kv.put(&parent_id, "default", "b", Value::Int(2)).unwrap();
    kv.put(&parent_id, "default", "c", Value::Int(3)).unwrap();

    // 2. COW fork parent → child
    branch_ops::fork_branch(&test_db.db, "parent", "child").unwrap();
    let child_id = strata_engine::primitives::branch::resolve_branch_name("child");

    // 3. Diverge: parent writes a: 10, child writes b: 20
    kv.put(&parent_id, "default", "a", Value::Int(10)).unwrap();
    kv.put(&child_id, "default", "b", Value::Int(20)).unwrap();

    // 4. Merge child → parent (LWW)
    let info = branch_ops::merge_branches(
        &test_db.db,
        "child",
        "parent",
        MergeStrategy::LastWriterWins,
        None,
    )
    .unwrap();
    assert!(info.keys_applied >= 1, "at least b should be applied");

    // 5. Verify: a keeps parent's value (three-way merge recognizes this as
    //    TargetChanged — ancestor was 1, child still has 1, parent changed to 10).
    assert_eq!(
        kv.get(&parent_id, "default", "a").unwrap(),
        Some(Value::Int(10)),
        "a: three-way merge preserves parent's change (ancestor=1, parent=10, child=1)"
    );
    assert_eq!(
        kv.get(&parent_id, "default", "b").unwrap(),
        Some(Value::Int(20)),
        "b should have child's merged value (20)"
    );
    assert_eq!(
        kv.get(&parent_id, "default", "c").unwrap(),
        Some(Value::Int(3)),
        "c should be unchanged"
    );
}

/// #1666: Three-way merge with ancestor state from inherited layers.
///
/// Three-way merge with ancestor state from inherited layers.
/// Verifies that disjoint changes merge cleanly without conflict.
#[test]
fn cow_three_way_merge_with_inherited_layers() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // 1. Create parent with keys {a: 1, b: 2, c: 3}
    branch_index.create_branch("parent").unwrap();
    let parent_id = strata_engine::primitives::branch::resolve_branch_name("parent");
    kv.put(&parent_id, "default", "a", Value::Int(1)).unwrap();
    kv.put(&parent_id, "default", "b", Value::Int(2)).unwrap();
    kv.put(&parent_id, "default", "c", Value::Int(3)).unwrap();

    // 2. COW fork parent → child
    branch_ops::fork_branch(&test_db.db, "parent", "child").unwrap();
    let child_id = strata_engine::primitives::branch::resolve_branch_name("child");

    // 3. Diverge: parent writes a: 10, child writes b: 20
    kv.put(&parent_id, "default", "a", Value::Int(10)).unwrap();
    kv.put(&child_id, "default", "b", Value::Int(20)).unwrap();

    // 4. Three-way merge: ancestor is {a: 1, b: 2, c: 3}
    //    Parent changed a (1→10), child changed b (2→20), neither changed c.
    //    Expected result: {a: 10, b: 20, c: 3}
    let _info = branch_ops::merge_branches(
        &test_db.db,
        "child",
        "parent",
        MergeStrategy::LastWriterWins, // TODO: ThreeWay strategy
        None,
    )
    .unwrap();

    assert_eq!(
        kv.get(&parent_id, "default", "a").unwrap(),
        Some(Value::Int(10)),
        "a should keep parent's change (ancestor was 1, parent changed to 10)"
    );
    assert_eq!(
        kv.get(&parent_id, "default", "b").unwrap(),
        Some(Value::Int(20)),
        "b should get child's change (ancestor was 2, child changed to 20)"
    );
    assert_eq!(
        kv.get(&parent_id, "default", "c").unwrap(),
        Some(Value::Int(3)),
        "c should be unchanged (neither side modified it)"
    );
}

/// #1666: Repeated merge after COW fork (fork → merge → modify → merge again).
#[test]
fn cow_repeated_merge_after_fork() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    branch_index.create_branch("parent").unwrap();
    let parent_id = strata_engine::primitives::branch::resolve_branch_name("parent");
    kv.put(&parent_id, "default", "k", Value::Int(1)).unwrap();

    // Fork
    branch_ops::fork_branch(&test_db.db, "parent", "child").unwrap();
    let child_id = strata_engine::primitives::branch::resolve_branch_name("child");

    // First round: child modifies, merge into parent
    kv.put(&child_id, "default", "k", Value::Int(10)).unwrap();
    branch_ops::merge_branches(
        &test_db.db,
        "child",
        "parent",
        MergeStrategy::LastWriterWins,
        None,
    )
    .unwrap();
    assert_eq!(
        kv.get(&parent_id, "default", "k").unwrap(),
        Some(Value::Int(10))
    );

    // Second round: child modifies again, merge into parent again
    kv.put(&child_id, "default", "k", Value::Int(100)).unwrap();
    branch_ops::merge_branches(
        &test_db.db,
        "child",
        "parent",
        MergeStrategy::LastWriterWins,
        None,
    )
    .unwrap();
    assert_eq!(
        kv.get(&parent_id, "default", "k").unwrap(),
        Some(Value::Int(100))
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

// ============================================================================
// Issue #1695: COW Lifecycle Edge-Case Tests
// ============================================================================

/// Issue #1695, test 1: Race fork_branch() against continuous parent writes.
///
/// Spawns writer threads that continuously write to the parent branch while
/// fork_branch() runs concurrently. Verifies that the child snapshot is
/// consistent — pre-fork seed keys are always visible, and no partial state
/// is observed.
///
/// Validates fix for #1679 (fork must serialize against writes).
#[test]
fn test_issue_1695_fork_vs_parent_write_race() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let test_db = TestDb::new_strict();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // Create parent branch with initial data
    branch_index.create_branch("parent").unwrap();
    let parent_id = strata_engine::primitives::branch::resolve_branch_name("parent");
    for i in 0..10 {
        kv.put(
            &parent_id,
            "default",
            &format!("seed_{}", i),
            Value::Int(i as i64),
        )
        .unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let db = test_db.db.clone();

    // Spawn writer threads that continuously write to parent.
    // Each thread writes sequentially numbered keys so we can detect gaps.
    let mut writers = Vec::new();
    for t in 0..3u8 {
        let kv_w = KVStore::new(db.clone());
        let stop_c = Arc::clone(&stop);
        writers.push(thread::spawn(move || {
            let mut i = 0u64;
            while !stop_c.load(Ordering::Relaxed) {
                let key = format!("w{}_k{}", t, i);
                let _ = kv_w.put(&parent_id, "default", &key, Value::Int(i as i64));
                i += 1;
                if i.is_multiple_of(5) {
                    thread::yield_now();
                }
            }
        }));
    }

    // Perform multiple forks while writers are active
    let mut fork_results = Vec::new();
    for fork_idx in 0..10u32 {
        let fork_name = format!("child_{}", fork_idx);
        if let Ok(info) = branch_ops::fork_branch(&db, "parent", &fork_name) {
            fork_results.push((fork_name, info));
        }
        thread::yield_now();
    }

    // Stop writers
    stop.store(true, Ordering::Release);
    for w in writers {
        w.join().unwrap();
    }

    // Verify each fork: child must see a consistent snapshot
    for (fork_name, info) in &fork_results {
        let _fork_version = info.fork_version.expect("fork must return fork_version");
        let child_id = strata_engine::primitives::branch::resolve_branch_name(fork_name);

        // Seed keys (seed_0..seed_9) were written before any fork, so they
        // must always be visible in every child.
        for i in 0..10 {
            let val = kv
                .get(&child_id, "default", &format!("seed_{}", i))
                .unwrap();
            assert_eq!(
                val,
                Some(Value::Int(i as i64)),
                "fork '{}': seed key seed_{} must be visible in child",
                fork_name,
                i
            );
        }

        // For writer keys: the child's view must be prefix-consistent.
        // If w{t}_k{i} is visible, all w{t}_k{j} for j < i must also be
        // visible (writes are sequential within each thread).
        for t in 0..3u8 {
            let mut last_visible = None;
            for i in 0..200u64 {
                let key = format!("w{}_k{}", t, i);
                let child_val = kv.get(&child_id, "default", &key).unwrap();
                if child_val.is_some() {
                    last_visible = Some(i);
                } else if last_visible.is_some() {
                    // Found a gap: key i is missing but some j < i was present.
                    // All keys after this should also be missing (snapshot consistency).
                    for j in (i + 1)..std::cmp::min(i + 10, 200) {
                        let later_key = format!("w{}_k{}", t, j);
                        assert!(
                            kv.get(&child_id, "default", &later_key).unwrap().is_none(),
                            "fork '{}': snapshot inconsistency — w{}_k{} missing but w{}_k{} present",
                            fork_name, t, i, t, j
                        );
                    }
                    break;
                }
            }
        }
    }

    assert!(
        !fork_results.is_empty(),
        "at least one fork must succeed under contention"
    );
}

/// Issue #1695, test 2: Fork child, compact parent, restart.
///
/// Verifies that after parent compaction and database restart:
/// - Parent has no duplicate data (compaction output is clean)
/// - Child still reads all inherited data through recovery
///
/// Validates fixes for #1680 (manifest-authoritative recovery) and
/// #1691 (inherited layer recovery independence).
#[test]
fn test_issue_1695_fork_compact_parent_restart() {
    let mut test_db = TestDb::new_strict();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // Create parent with multiple batches of data (so compaction has work)
    branch_index.create_branch("parent").unwrap();
    let parent_id = strata_engine::primitives::branch::resolve_branch_name("parent");

    for i in 0..50 {
        kv.put(
            &parent_id,
            "default",
            &format!("k{:04}", i),
            Value::Int(i as i64),
        )
        .unwrap();
    }

    // Fork parent → child
    let fork_info = branch_ops::fork_branch(&test_db.db, "parent", "child").unwrap();
    assert!(fork_info.fork_version.is_some());
    let child_id = strata_engine::primitives::branch::resolve_branch_name("child");

    // Write more data to parent (post-fork)
    for i in 50..100 {
        kv.put(
            &parent_id,
            "default",
            &format!("k{:04}", i),
            Value::Int(i as i64),
        )
        .unwrap();
    }

    // Verify child sees only pre-fork data
    for i in 0..50 {
        assert_eq!(
            kv.get(&child_id, "default", &format!("k{:04}", i)).unwrap(),
            Some(Value::Int(i as i64)),
            "child should see pre-fork key k{:04}",
            i
        );
    }
    // Child should NOT see post-fork parent writes
    assert!(
        kv.get(&child_id, "default", "k0050").unwrap().is_none(),
        "child should not see post-fork parent key"
    );

    // Restart the database
    test_db.reopen();
    let kv = test_db.kv();

    // Parent should have all 100 keys, no duplicates
    let parent_keys = kv.list(&parent_id, "default", Some("k")).unwrap();
    assert_eq!(
        parent_keys.len(),
        100,
        "parent should have exactly 100 keys after restart"
    );
    for i in 0..100 {
        assert_eq!(
            kv.get(&parent_id, "default", &format!("k{:04}", i))
                .unwrap(),
            Some(Value::Int(i as i64)),
            "parent key k{:04} must survive restart",
            i
        );
    }

    // Child should still read inherited data after restart
    for i in 0..50 {
        assert_eq!(
            kv.get(&child_id, "default", &format!("k{:04}", i)).unwrap(),
            Some(Value::Int(i as i64)),
            "child key k{:04} must survive restart through inherited layers",
            i
        );
    }
    // Post-fork data still invisible to child
    assert!(
        kv.get(&child_id, "default", "k0050").unwrap().is_none(),
        "child should not see post-fork parent key after restart"
    );
}

/// Issue #1695, test 3: Race background materialization against explicit
/// materialize_branch() API call.
///
/// Creates a deeply-forked branch (exceeding MAX_INHERITED_LAYERS) so the
/// background scheduler will attempt materialization. Simultaneously calls
/// materialize_branch() from the test thread. Verifies single output and
/// no data corruption.
///
/// Validates fix for #1693 (concurrent materialization guard).
#[test]
fn test_issue_1695_concurrent_bg_vs_explicit_materialization() {
    let test_db = TestDb::new_strict();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();
    let db = test_db.db.clone();

    // Build a chain of forks: root → a → b → c → d → e → leaf
    // This creates 6 inherited layers on leaf (exceeds MAX_INHERITED_LAYERS=4)
    branch_index.create_branch("root").unwrap();
    let root_id = strata_engine::primitives::branch::resolve_branch_name("root");
    for i in 0..20 {
        kv.put(
            &root_id,
            "default",
            &format!("root_k{}", i),
            Value::Int(i as i64),
        )
        .unwrap();
    }

    let chain = ["a", "b", "c", "d", "e", "leaf"];
    let mut prev = "root";
    for name in &chain {
        branch_ops::fork_branch(&db, prev, name).unwrap();
        prev = name;
    }

    let leaf_id = strata_engine::primitives::branch::resolve_branch_name("leaf");

    // Verify leaf can read inherited data before materialization
    assert_eq!(
        kv.get(&leaf_id, "default", "root_k0").unwrap(),
        Some(Value::Int(0)),
        "leaf should read root data through inheritance chain"
    );

    // Race: explicit materialize_branch in parallel threads
    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                branch_ops::materialize_branch(&db, "leaf")
            })
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All calls must succeed (no panics)
    for (i, r) in results.iter().enumerate() {
        assert!(r.is_ok(), "thread {} failed: {:?}", i, r);
    }

    // At most one thread should have actually materialized entries
    let workers: Vec<_> = results
        .iter()
        .filter(|r| r.as_ref().unwrap().entries_materialized > 0)
        .collect();
    assert!(
        workers.len() <= 1,
        "at most 1 thread should materialize entries, got {}",
        workers.len()
    );

    // Data must still be correct
    for i in 0..20 {
        assert_eq!(
            kv.get(&leaf_id, "default", &format!("root_k{}", i))
                .unwrap(),
            Some(Value::Int(i as i64)),
            "leaf should still read root_k{} after materialization",
            i
        );
    }
}

/// Issue #1695, test 4: Fork child, clear (delete) child, restart.
///
/// Verifies that a deleted child branch stays deleted after recovery.
/// The parent's data must be unaffected. No orphaned segments should
/// cause the child to reappear.
///
/// Validates recovery handling of cleared branches.
#[test]
fn test_issue_1695_fork_clear_child_restart() {
    let mut test_db = TestDb::new_strict();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // Create parent with data
    branch_index.create_branch("parent").unwrap();
    let parent_id = strata_engine::primitives::branch::resolve_branch_name("parent");
    for i in 0..20 {
        kv.put(
            &parent_id,
            "default",
            &format!("pk{}", i),
            Value::Int(i as i64),
        )
        .unwrap();
    }

    // Fork parent → child
    branch_ops::fork_branch(&test_db.db, "parent", "child").unwrap();
    let child_id = strata_engine::primitives::branch::resolve_branch_name("child");

    // Verify child has data
    assert_eq!(
        kv.get(&child_id, "default", "pk0").unwrap(),
        Some(Value::Int(0)),
        "child should have inherited data before deletion"
    );

    // Write some child-only data
    kv.put(&child_id, "default", "child_only", Value::Int(999))
        .unwrap();

    // Delete the child branch
    branch_index.delete_branch("child").unwrap();

    // Verify child is gone
    assert!(
        branch_index.get_branch("child").unwrap().is_none(),
        "child branch should be deleted"
    );

    // Restart the database
    test_db.reopen();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // Child must still be deleted after restart
    assert!(
        branch_index.get_branch("child").unwrap().is_none(),
        "child branch must stay deleted after restart"
    );

    // Parent data must be intact
    for i in 0..20 {
        assert_eq!(
            kv.get(&parent_id, "default", &format!("pk{}", i)).unwrap(),
            Some(Value::Int(i as i64)),
            "parent key pk{} must survive child deletion + restart",
            i
        );
    }
}

/// Issue #1695, test 5: Fork, make disjoint changes on parent and child, merge.
///
/// After forking, parent and child each modify different keys. Merging child
/// back into parent with LastWriterWins should apply child's additions to
/// parent. Verifies parent-only changes survive the merge.
///
/// Note: #1692 (three-way merge) is still open. The current two-way diff/apply
/// merge treats child additions as "added" relative to parent, applying them.
/// Parent-only changes that don't conflict with child should survive because
/// the merge only applies entries from the diff (added + modified), it does
/// not delete entries that are absent in source.
#[test]
fn test_issue_1695_fork_disjoint_changes_merge() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // Create parent with shared baseline
    branch_index.create_branch("parent").unwrap();
    let parent_id = strata_engine::primitives::branch::resolve_branch_name("parent");
    kv.put(&parent_id, "default", "base", Value::Int(100))
        .unwrap();

    // Fork parent → child
    branch_ops::fork_branch(&test_db.db, "parent", "child").unwrap();
    let child_id = strata_engine::primitives::branch::resolve_branch_name("child");

    // Make DISJOINT changes: parent modifies "parent_only", child modifies "child_only"
    kv.put(
        &parent_id,
        "default",
        "parent_only",
        Value::String("from_parent".into()),
    )
    .unwrap();
    kv.put(
        &child_id,
        "default",
        "child_only",
        Value::String("from_child".into()),
    )
    .unwrap();

    // Both should still see "base" from before fork
    assert_eq!(
        kv.get(&parent_id, "default", "base").unwrap(),
        Some(Value::Int(100))
    );
    assert_eq!(
        kv.get(&child_id, "default", "base").unwrap(),
        Some(Value::Int(100))
    );

    // Merge child → parent (LWW)
    let merge_info = branch_ops::merge_branches(
        &test_db.db,
        "child",
        "parent",
        MergeStrategy::LastWriterWins,
        None,
    )
    .unwrap();

    // child_only should be applied to parent (it's "added" in child vs parent)
    assert!(merge_info.keys_applied >= 1, "child_only should be merged");

    // Verify ALL keys on parent after merge:
    // 1. "base" - original shared key, untouched by both → still present
    assert_eq!(
        kv.get(&parent_id, "default", "base").unwrap(),
        Some(Value::Int(100)),
        "base key must survive merge"
    );

    // 2. "parent_only" - written only on parent → must survive
    assert_eq!(
        kv.get(&parent_id, "default", "parent_only").unwrap(),
        Some(Value::String("from_parent".into())),
        "parent-only change must survive merge (disjoint changes preserved)"
    );

    // 3. "child_only" - written only on child → merged into parent
    assert_eq!(
        kv.get(&parent_id, "default", "child_only").unwrap(),
        Some(Value::String("from_child".into())),
        "child-only change must be applied to parent by merge"
    );
}

/// COW-aware diff correctness oracle: set up a complex scenario with adds,
/// modifies, deletes on both parent and child after fork, then verify the
/// COW fast-path diff produces correct results by checking each entry.
#[test]
fn cow_diff_matches_expected_complex() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    // 1. Parent with 5 keys
    branch_index.create_branch("parent").unwrap();
    let parent_id = strata_engine::primitives::branch::resolve_branch_name("parent");
    kv.put(&parent_id, "default", "shared", Value::Int(1))
        .unwrap();
    kv.put(&parent_id, "default", "child_mod", Value::Int(2))
        .unwrap();
    kv.put(&parent_id, "default", "child_del", Value::Int(3))
        .unwrap();
    kv.put(&parent_id, "default", "parent_mod", Value::Int(4))
        .unwrap();
    kv.put(&parent_id, "default", "both_mod", Value::Int(5))
        .unwrap();

    // 2. Fork
    branch_ops::fork_branch(&test_db.db, "parent", "child").unwrap();
    let child_id = strata_engine::primitives::branch::resolve_branch_name("child");

    // 3. Child: modify, delete, add
    kv.put(&child_id, "default", "child_mod", Value::Int(20))
        .unwrap();
    kv.delete(&child_id, "default", "child_del").unwrap();
    kv.put(&child_id, "default", "child_new", Value::Int(100))
        .unwrap();
    kv.put(&child_id, "default", "both_mod", Value::Int(50))
        .unwrap();

    // 4. Parent: modify after fork
    kv.put(&parent_id, "default", "parent_mod", Value::Int(40))
        .unwrap();
    kv.put(&parent_id, "default", "both_mod", Value::Int(55))
        .unwrap();
    kv.put(&parent_id, "default", "parent_new", Value::Int(200))
        .unwrap();

    // 5. Diff parent → child
    let diff = branch_ops::diff_branches(&test_db.db, "parent", "child").unwrap();

    // Collect into maps for easy assertion
    let space = diff.spaces.iter().find(|s| s.space == "default").unwrap();

    let added_keys: std::collections::HashSet<&str> =
        space.added.iter().map(|e| e.key.as_str()).collect();
    let removed_keys: std::collections::HashSet<&str> =
        space.removed.iter().map(|e| e.key.as_str()).collect();
    let modified_keys: std::collections::HashSet<&str> =
        space.modified.iter().map(|e| e.key.as_str()).collect();

    // child_new: only in child → added
    assert!(
        added_keys.contains("child_new"),
        "child_new should be added"
    );
    // parent_new: only in parent → removed (in A=parent, not in B=child)
    assert!(
        removed_keys.contains("parent_new"),
        "parent_new should be removed (in parent, not child)"
    );
    // child_del: in parent, deleted in child → removed
    assert!(
        removed_keys.contains("child_del"),
        "child_del should be removed"
    );
    // child_mod: different values → modified
    assert!(
        modified_keys.contains("child_mod"),
        "child_mod should be modified"
    );
    // parent_mod: parent changed, child has old value → modified
    assert!(
        modified_keys.contains("parent_mod"),
        "parent_mod should be modified"
    );
    // both_mod: both changed to different values → modified
    assert!(
        modified_keys.contains("both_mod"),
        "both_mod should be modified"
    );
    // shared: unchanged on both sides → NOT in diff
    assert!(!added_keys.contains("shared"), "shared should not appear");
    assert!(!removed_keys.contains("shared"), "shared should not appear");
    assert!(
        !modified_keys.contains("shared"),
        "shared should not appear"
    );
}

// ============================================================================
// Graph space symmetry
// ============================================================================
//
// These tests verify that graph data can live in any user-named space,
// not just the reserved `_graph_` space. `Strata::graph_*` honors
// `current_space` exactly like KV/JSON/Vector/Event do — graph
// inherits branching uniformly with the other primitives.

/// Create a graph in a custom user space and verify it's queryable only
/// from that space.
#[test]
fn graph_in_user_space_create_and_query_succeeds() {
    use strata_graph::types::{EdgeData, NodeData};

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("main").unwrap();
    let branch_id = strata_engine::primitives::branch::resolve_branch_name("main");

    // Create the graph in "tenant_a" space.
    p.graph
        .create_graph(branch_id, "tenant_a", "patients", None)
        .unwrap();
    p.graph
        .add_node(branch_id, "tenant_a", "patients", "p1", NodeData::default())
        .unwrap();
    p.graph
        .add_node(branch_id, "tenant_a", "patients", "p2", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            branch_id,
            "tenant_a",
            "patients",
            "p1",
            "p2",
            "knows",
            EdgeData::default(),
        )
        .unwrap();

    // Same space → readable.
    assert!(p
        .graph
        .get_node(branch_id, "tenant_a", "patients", "p1")
        .unwrap()
        .is_some());
    let nodes = p
        .graph
        .list_nodes(branch_id, "tenant_a", "patients")
        .unwrap();
    let node_set: std::collections::HashSet<String> = nodes.into_iter().collect();
    assert!(node_set.contains("p1") && node_set.contains("p2"));

    // Different space (default) → not visible. The graph "patients" simply
    // doesn't exist in the default space, so list_graphs is empty there.
    let default_graphs = p.graph.list_graphs(branch_id, "default").unwrap();
    assert!(
        !default_graphs.contains(&"patients".to_string()),
        "tenant_a's 'patients' must not appear in default space, got {default_graphs:?}"
    );
    let legacy_graphs = p.graph.list_graphs(branch_id, "_graph_").unwrap();
    assert!(
        !legacy_graphs.contains(&"patients".to_string()),
        "tenant_a's 'patients' must not appear in _graph_ space, got {legacy_graphs:?}"
    );
}

/// Two graphs with the same name in different spaces are fully
/// independent — same node IDs, different data, no cross-contamination.
#[test]
fn graph_in_two_spaces_independent() {
    use strata_graph::types::NodeData;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("main").unwrap();
    let branch_id = strata_engine::primitives::branch::resolve_branch_name("main");

    let alice_a = NodeData {
        entity_ref: None,
        properties: Some(serde_json::json!({"tenant": "a"})),
        object_type: None,
    };
    let alice_b = NodeData {
        entity_ref: None,
        properties: Some(serde_json::json!({"tenant": "b"})),
        object_type: None,
    };

    p.graph
        .create_graph(branch_id, "tenant_a", "users", None)
        .unwrap();
    p.graph
        .create_graph(branch_id, "tenant_b", "users", None)
        .unwrap();
    p.graph
        .add_node(branch_id, "tenant_a", "users", "alice", alice_a.clone())
        .unwrap();
    p.graph
        .add_node(branch_id, "tenant_b", "users", "alice", alice_b.clone())
        .unwrap();

    // Each space sees its own alice with its own properties.
    let from_a = p
        .graph
        .get_node(branch_id, "tenant_a", "users", "alice")
        .unwrap()
        .unwrap();
    assert_eq!(from_a.properties, alice_a.properties);
    let from_b = p
        .graph
        .get_node(branch_id, "tenant_b", "users", "alice")
        .unwrap()
        .unwrap();
    assert_eq!(from_b.properties, alice_b.properties);
    // And NEITHER bleeds into the other.
    assert_ne!(from_a.properties, from_b.properties);
}

/// A graph in a user space survives a fork — the child branch inherits
/// the data via COW exactly like KV/JSON.
#[test]
fn graph_in_user_space_survives_fork() {
    use strata_graph::types::NodeData;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("parent").unwrap();
    let parent_id = strata_engine::primitives::branch::resolve_branch_name("parent");

    p.graph
        .create_graph(parent_id, "tenant_a", "g", None)
        .unwrap();
    p.graph
        .add_node(parent_id, "tenant_a", "g", "alice", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "parent", "child").unwrap();
    let child_id = strata_engine::primitives::branch::resolve_branch_name("child");

    // Child inherits the parent's graph in tenant_a space.
    assert!(p
        .graph
        .get_node(child_id, "tenant_a", "g", "alice")
        .unwrap()
        .is_some());

    // Child can mutate independently.
    p.graph
        .add_node(child_id, "tenant_a", "g", "bob", NodeData::default())
        .unwrap();
    assert!(p
        .graph
        .get_node(child_id, "tenant_a", "g", "bob")
        .unwrap()
        .is_some());
    // Parent doesn't see child's addition (branch isolation).
    assert!(p
        .graph
        .get_node(parent_id, "tenant_a", "g", "bob")
        .unwrap()
        .is_none());
}

/// The semantic graph merge applies to user-space graphs identically
/// to graphs in the reserved `_graph_` space. Disjoint additions on a
/// user-space graph merge cleanly with bidirectional consistency.
#[test]
fn graph_in_user_space_semantic_merge_succeeds() {
    use strata_graph::types::{EdgeData, NodeData};

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "tenant_a", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "tenant_a", "g", "alice", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "tenant_a", "g", "bob", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Disjoint additions on both sides.
    p.graph
        .add_node(source_id, "tenant_a", "g", "carol", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            source_id,
            "tenant_a",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();
    p.graph
        .add_node(target_id, "tenant_a", "g", "dave", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            target_id,
            "tenant_a",
            "g",
            "bob",
            "dave",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("semantic merge must work for user-space graphs");

    // All four nodes present after merge.
    for n in &["alice", "bob", "carol", "dave"] {
        assert!(
            p.graph
                .get_node(target_id, "tenant_a", "g", n)
                .unwrap()
                .is_some(),
            "{n} missing from merged user-space graph"
        );
    }

    // Bidirectional consistency: alice→carol visible from both sides.
    let alice_outgoing = p
        .graph
        .outgoing_neighbors(target_id, "tenant_a", "g", "alice", None)
        .unwrap();
    let alice_targets: std::collections::HashSet<String> =
        alice_outgoing.iter().map(|n| n.node_id.clone()).collect();
    assert!(alice_targets.contains("carol"));
    let carol_incoming = p
        .graph
        .incoming_neighbors(target_id, "tenant_a", "g", "carol", None)
        .unwrap();
    let carol_sources: std::collections::HashSet<String> =
        carol_incoming.iter().map(|n| n.node_id.clone()).collect();
    assert!(carol_sources.contains("alice"));
}

/// Referential integrity rejection applies to user-space graphs the
/// same way it does to graphs in the reserved `_graph_` space.
#[test]
fn graph_in_user_space_referential_integrity_rejects_dangling() {
    use strata_graph::types::{EdgeData, NodeData};

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "tenant_a", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "tenant_a", "g", "alice", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "tenant_a", "g", "carol", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source adds an edge alice→carol; target deletes carol.
    p.graph
        .add_edge(
            source_id,
            "tenant_a",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();
    p.graph
        .remove_node(target_id, "tenant_a", "g", "carol")
        .unwrap();

    let err = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect_err("dangling edge in user-space graph must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("graph referential integrity violation")
            || msg.contains("dangling")
            || msg.contains("orphaned"),
        "expected referential-integrity error, got: {msg}"
    );
}

/// The lower-level `GraphStore::*` API accepts any space name, including
/// the reserved `_graph_` space that `branch_dag.rs` uses for the system
/// DAG. This is unreachable through the user-facing `Strata::graph_*`
/// API (space-name validation rejects `_`-prefixed names), but direct
/// GraphStore callers — branch_dag, recovery, test fixtures — must still
/// be able to target it.
#[test]
fn graph_store_accepts_reserved_system_dag_space() {
    use strata_graph::types::NodeData;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("main").unwrap();
    let branch_id = strata_engine::primitives::branch::resolve_branch_name("main");

    p.graph
        .create_graph(branch_id, strata_graph::keys::GRAPH_SPACE, "g", None)
        .unwrap();
    p.graph
        .add_node(
            branch_id,
            strata_graph::keys::GRAPH_SPACE,
            "g",
            "alice",
            NodeData::default(),
        )
        .unwrap();

    let alice = p
        .graph
        .get_node(branch_id, strata_graph::keys::GRAPH_SPACE, "g", "alice")
        .unwrap();
    assert!(alice.is_some());

    // The user-facing `default` space is fully isolated from the
    // reserved system DAG space.
    assert!(p
        .graph
        .get_node(branch_id, "default", "g", "alice")
        .unwrap()
        .is_none());
    assert!(p
        .graph
        .get_node(branch_id, "tenant-a", "g", "alice")
        .unwrap()
        .is_none());
}

// ============================================================================
// Known bugs — failing tests pinned for follow-up
// ============================================================================
//
// Each test in this section asserts the *correct* behavior and currently
// fails. They are marked `#[ignore]` so CI stays green; remove the ignore
// attribute once the corresponding fix lands.

/// A whole-graph delete on one side merged against a target-side node
/// addition resolves atomically: catalog and storage stay in sync.
///
/// Setup:
///   - Pre-fork: target has graph "g" with node "alice".
///   - Source: `delete_graph("g")` — wipes all `g/*` keys, drops "g" from
///     `__catalog__`.
///   - Target: `add_node("g", "bob")`.
///
/// `compute_graph_merge` detects the wholesale-delete on source (ancestor
/// had data, source's `PerGraphState` is empty) and emits a
/// `WholeGraphDeleteConflict`. Under LWW source's delete wins (matching
/// the convention in `merge_meta`'s `DeleteModifyConflict` arm) and the
/// per-graph state is fully dropped, including target-side adds.
///
/// Post-merge state must satisfy:
///   `catalog contains "g"` ⇔ `any g/* keys exist in storage`.
#[test]
fn graph_merge_whole_graph_delete_vs_target_add_is_atomic() {
    use strata_graph::types::NodeData;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    // Pre-fork: target has graph "g" with alice.
    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source wipes the whole graph.
    p.graph.delete_graph(source_id, "default", "g").unwrap();

    // Target concurrently adds a new node.
    p.graph
        .add_node(target_id, "default", "g", "bob", NodeData::default())
        .unwrap();

    // Merge under LWW.
    let result = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    );

    // The merge should either succeed atomically (one side wins fully) or
    // be rejected with a structured error. Whatever it does, the result
    // must be internally consistent: `g` is either fully present (in the
    // catalog AND the node bob exists) or fully absent (catalog excludes
    // `g` AND no `g/*` storage rows remain).
    match result {
        Ok(_) => {
            let graphs: std::collections::HashSet<String> = p
                .graph
                .list_graphs(target_id, "default")
                .unwrap()
                .into_iter()
                .collect();
            let has_bob = p
                .graph
                .get_node(target_id, "default", "g", "bob")
                .unwrap()
                .is_some();
            let has_alice = p
                .graph
                .get_node(target_id, "default", "g", "alice")
                .unwrap()
                .is_some();
            let g_in_catalog = graphs.contains("g");
            let any_storage_rows = has_bob || has_alice;

            assert_eq!(
                g_in_catalog, any_storage_rows,
                "post-merge state is inconsistent: catalog_has_g={g_in_catalog}, \
                 has_alice={has_alice}, has_bob={has_bob} — \
                 catalog and storage must agree on whether 'g' exists"
            );
        }
        Err(e) => {
            // Rejection is fine as long as target state is unchanged.
            let msg = e.to_string();
            assert!(
                msg.contains("graph") || msg.contains("conflict") || msg.contains("merge"),
                "rejection should mention the graph conflict, got: {msg}"
            );
        }
    }
}

/// Under LWW, when a node-level conflict resolves in one side's favor,
/// the `__ref__` / `__by_type__` indexes match the merged `NodeData`.
///
/// Setup:
///   - Pre-fork: alice with `entity_ref=patient/p1`, `object_type=Patient`.
///   - Source: changes alice's properties only (refs unchanged).
///   - Target: changes alice's `entity_ref → patient/p2` and
///     `object_type → Doctor`.
///
/// LWW picks source for the node. The merge runs `project_node_index_writes`
/// after the per-key node merge, deriving the canonical index keys from
/// `projected_nodes` and reconciling them against target's existing
/// entries — so the indexes always reflect the projected `NodeData`.
#[test]
fn graph_merge_lww_node_keeps_ref_and_type_indexes_consistent() {
    use strata_graph::types::NodeData;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    // Pre-fork: alice with refs/type set.
    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    let alice_pre = NodeData {
        entity_ref: Some("patient/p1".to_string()),
        properties: Some(serde_json::json!({"v": 1})),
        object_type: Some("Patient".to_string()),
    };
    p.graph
        .add_node(target_id, "default", "g", "alice", alice_pre.clone())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source: change properties only.
    let alice_source = NodeData {
        entity_ref: Some("patient/p1".to_string()),
        properties: Some(serde_json::json!({"v": 2})),
        object_type: Some("Patient".to_string()),
    };
    p.graph
        .add_node(source_id, "default", "g", "alice", alice_source.clone())
        .unwrap();

    // Target: change entity_ref + object_type, keep properties at v=1.
    let alice_target = NodeData {
        entity_ref: Some("patient/p2".to_string()),
        properties: Some(serde_json::json!({"v": 1})),
        object_type: Some("Doctor".to_string()),
    };
    p.graph
        .add_node(target_id, "default", "g", "alice", alice_target.clone())
        .unwrap();

    // Merge under LWW. With both sides differing from ancestor and from
    // each other, the node lands as a Conflict and source wins.
    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("merge with concurrent node edits should succeed under LWW");

    // The merged node JSON: source won, so entity_ref=p1, type=Patient.
    let merged = p
        .graph
        .get_node(target_id, "default", "g", "alice")
        .unwrap()
        .expect("alice must still exist after merge");
    assert_eq!(
        merged.entity_ref.as_deref(),
        Some("patient/p1"),
        "LWW source side should win the node merge"
    );
    assert_eq!(merged.object_type.as_deref(), Some("Patient"));

    // The __ref__ index must agree with the merged node. p1 should
    // resolve to alice; p2 should NOT — but the bug keeps the target's
    // p2 index live.
    let by_p1 = p
        .graph
        .nodes_for_entity(target_id, "default", "patient/p1")
        .unwrap();
    assert!(
        by_p1.iter().any(|(g, n)| g == "g" && n == "alice"),
        "nodes_for_entity(patient/p1) must contain alice (her merged \
         entity_ref is patient/p1), got {by_p1:?}"
    );
    let by_p2 = p
        .graph
        .nodes_for_entity(target_id, "default", "patient/p2")
        .unwrap();
    assert!(
        !by_p2.iter().any(|(g, n)| g == "g" && n == "alice"),
        "nodes_for_entity(patient/p2) must NOT contain alice (her merged \
         entity_ref is patient/p1, not p2), got {by_p2:?}"
    );

    // Same for the __by_type__ index.
    let patients = p
        .graph
        .nodes_by_type(target_id, "default", "g", "Patient")
        .unwrap();
    assert!(
        patients.contains(&"alice".to_string()),
        "nodes_by_type(Patient) must contain alice (her merged type is \
         Patient), got {patients:?}"
    );
    let doctors = p
        .graph
        .nodes_by_type(target_id, "default", "g", "Doctor")
        .unwrap();
    assert!(
        !doctors.contains(&"alice".to_string()),
        "nodes_by_type(Doctor) must NOT contain alice (her merged type \
         is Patient, not Doctor), got {doctors:?}"
    );
}

/// `EntityRef::Graph` carries the space, so two nodes from different
/// spaces with the same `(graph, node_id)` are distinct documents in
/// the search index — multi-tenant graphs don't collide.
#[test]
fn entity_ref_graph_distinguishes_spaces() {
    use strata_engine::search::EntityRef;

    let branch_id = strata_core::types::BranchId::new();

    let tenant_a_alice = EntityRef::Graph {
        branch_id,
        space: "tenant_a".to_string(),
        key: "social/n/alice".to_string(),
    };
    let tenant_b_alice = EntityRef::Graph {
        branch_id,
        space: "tenant_b".to_string(),
        key: "social/n/alice".to_string(),
    };

    assert_ne!(
        tenant_a_alice, tenant_b_alice,
        "EntityRef::Graph for two different spaces must not be equal \
         (otherwise they collide in the inverted index, leaking search \
         results across tenants)"
    );
}

// ============================================================================
// Vector merge — Claim 4 vector-aware merge
// ============================================================================
//
// `VectorMergeHandler` (engine side) tracks `(space, collection)` pairs
// touched by the merge and dispatches to vector-crate callbacks for
// dimension/metric mismatch refusal (`precheck`) and per-collection HNSW
// rebuild (`post_commit`). These tests pin the contract: disjoint vector
// adds merge cleanly, conflicting IDs follow the merge strategy,
// incompatible configs are refused, and post-merge k-NN search returns
// the correct union of both sides without a global rebuild.

#[test]
fn vector_merge_disjoint_collections() {
    // Source writes to collection "alpha", target writes to "beta".
    // After merge, target sees both collections with their respective
    // vectors searchable.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();
    let vector = test_db.vector();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Seed target with a placeholder write so the storage layer knows
    // the branch exists before fork (fork requires storage state).
    kv.put(&target_id, "default", "_seed", Value::Int(0))
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source: create "alpha" and add a vector.
    vector
        .create_collection(source_id, "default", "alpha", config_small())
        .unwrap();
    vector
        .insert(source_id, "default", "alpha", "v1", &[1.0, 0.0, 0.0], None)
        .unwrap();

    // Target: create "beta" and add a vector.
    vector
        .create_collection(target_id, "default", "beta", config_small())
        .unwrap();
    vector
        .insert(target_id, "default", "beta", "v1", &[0.0, 1.0, 0.0], None)
        .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("disjoint-collection vector merge should succeed");

    // After merge, target sees both collections.
    let collections = vector.list_collections(target_id, "default").unwrap();
    let names: Vec<&str> = collections.iter().map(|c| c.name.as_str()).collect();
    assert!(
        names.contains(&"alpha"),
        "merged target should see source's 'alpha' collection; got {names:?}"
    );
    assert!(
        names.contains(&"beta"),
        "merged target should retain its own 'beta' collection; got {names:?}"
    );

    // Both vectors are searchable post-merge.
    let alpha_hits = vector
        .search(target_id, "default", "alpha", &[1.0, 0.0, 0.0], 1, None)
        .unwrap();
    assert_eq!(
        alpha_hits.len(),
        1,
        "expected exactly one hit for 'alpha' search; got {}",
        alpha_hits.len()
    );
    assert_eq!(alpha_hits[0].key, "v1");

    let beta_hits = vector
        .search(target_id, "default", "beta", &[0.0, 1.0, 0.0], 1, None)
        .unwrap();
    assert_eq!(
        beta_hits.len(),
        1,
        "expected exactly one hit for 'beta' search; got {}",
        beta_hits.len()
    );
    assert_eq!(beta_hits[0].key, "v1");
}

#[test]
fn vector_merge_same_collection_disjoint_ids() {
    // Both branches use the same collection but write disjoint vector
    // keys. After merge, target sees the union of both sets and all
    // four vectors are searchable.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let vector = test_db.vector();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Seed the collection on target before fork so both branches share it.
    vector
        .create_collection(target_id, "default", "shared", config_small())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source: add vec1, vec2 with disjoint embeddings.
    vector
        .insert(
            source_id,
            "default",
            "shared",
            "vec1",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            source_id,
            "default",
            "shared",
            "vec2",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();

    // Target: add vec3, vec4 with disjoint embeddings.
    vector
        .insert(
            target_id,
            "default",
            "shared",
            "vec3",
            &[0.0, 0.0, 1.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            target_id,
            "default",
            "shared",
            "vec4",
            &[0.5, 0.5, 0.0],
            None,
        )
        .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("disjoint-id vector merge should succeed");

    // All four vectors are present post-merge.
    for key in ["vec1", "vec2", "vec3", "vec4"] {
        assert!(
            vector
                .get(target_id, "default", "shared", key)
                .unwrap()
                .is_some(),
            "expected '{key}' to be present in target after merge"
        );
    }

    // k-NN search returns vectors from both branches. Searching with
    // k=4 over a 4-vector collection should return all four.
    let hits = vector
        .search(target_id, "default", "shared", &[1.0, 0.0, 0.0], 4, None)
        .unwrap();
    let returned_keys: std::collections::HashSet<String> =
        hits.iter().map(|h| h.key.clone()).collect();
    assert_eq!(
        returned_keys.len(),
        4,
        "expected k=4 search to return all four vectors; got {returned_keys:?}"
    );
    for key in ["vec1", "vec2", "vec3", "vec4"] {
        assert!(
            returned_keys.contains(key),
            "expected '{key}' in search results; got {returned_keys:?}"
        );
    }
}

#[test]
fn vector_merge_conflicting_id_lww() {
    // Both branches write the same (collection, key) with different
    // embeddings. Under LWW the source value wins.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let vector = test_db.vector();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    vector
        .create_collection(target_id, "default", "shared", config_small())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Both sides write the same key with different embeddings.
    vector
        .insert(
            source_id,
            "default",
            "shared",
            "conflict",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            target_id,
            "default",
            "shared",
            "conflict",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("LWW vector conflict should resolve cleanly");

    // LWW: source wins. Target now reads source's embedding.
    let entry = vector
        .get(target_id, "default", "shared", "conflict")
        .unwrap()
        .unwrap();
    assert_eq!(
        entry.value.embedding,
        vec![1.0, 0.0, 0.0],
        "LWW should resolve to source's embedding"
    );
}

#[test]
fn vector_merge_dimension_mismatch_rejected() {
    // Both branches have a collection named "shared" but with different
    // dimensions. The merge precheck refuses regardless of strategy —
    // combining mismatched-dimension vectors into one HNSW would
    // corrupt the index.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();
    let vector = test_db.vector();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    kv.put(&target_id, "default", "_seed", Value::Int(0))
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source: 3-dim collection.
    vector
        .create_collection(source_id, "default", "shared", config_small())
        .unwrap();
    vector
        .insert(source_id, "default", "shared", "v1", &[1.0, 0.0, 0.0], None)
        .unwrap();

    // Target: 5-dim collection (same name).
    vector
        .create_collection(
            target_id,
            "default",
            "shared",
            config_custom(5, DistanceMetric::Cosine),
        )
        .unwrap();
    vector
        .insert(
            target_id,
            "default",
            "shared",
            "v1",
            &[1.0, 0.0, 0.0, 0.0, 0.0],
            None,
        )
        .unwrap();

    // LWW should refuse the merge — dimension mismatch is fatal regardless
    // of strategy.
    let lww_result = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    );
    assert!(
        lww_result.is_err(),
        "LWW merge with dimension mismatch must fail; got {:?}",
        lww_result
    );
    let err = lww_result.unwrap_err().to_string();
    assert!(
        err.contains("incompatible dimensions") || err.contains("dimension"),
        "error message should mention dimension mismatch; got: {err}"
    );

    // Strict should also refuse.
    let strict_result =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None);
    assert!(
        strict_result.is_err(),
        "Strict merge with dimension mismatch must fail; got {:?}",
        strict_result
    );

    // Target's data is unchanged after the failed merge attempts.
    let entry = vector
        .get(target_id, "default", "shared", "v1")
        .unwrap()
        .unwrap();
    assert_eq!(
        entry.value.embedding.len(),
        5,
        "target should still have its 5-dim vector after failed merge"
    );
}

#[test]
fn vector_merge_metric_mismatch_rejected() {
    // Both branches have a collection named "shared" but with different
    // distance metrics. The merge precheck refuses — switching metrics
    // would silently change search semantics for already-indexed
    // vectors.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();
    let vector = test_db.vector();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    kv.put(&target_id, "default", "_seed", Value::Int(0))
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source: Cosine metric.
    vector
        .create_collection(
            source_id,
            "default",
            "shared",
            config_custom(3, DistanceMetric::Cosine),
        )
        .unwrap();

    // Target: Euclidean metric (same name + dimension, different metric).
    vector
        .create_collection(
            target_id,
            "default",
            "shared",
            config_custom(3, DistanceMetric::Euclidean),
        )
        .unwrap();

    let result = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    );
    assert!(
        result.is_err(),
        "merge with metric mismatch must fail; got {:?}",
        result
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("incompatible distance metrics") || err.contains("metric"),
        "error message should mention metric mismatch; got: {err}"
    );
}

#[test]
fn vector_merge_hnsw_search_correct_after_merge() {
    // Phase 4 acceptance test: after disjoint adds on both branches,
    // a k-NN search returns the union of both sides with correct
    // ranking. This proves the per-collection HNSW rebuild produced a
    // searchable index without a full backend reload.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let vector = test_db.vector();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    vector
        .create_collection(target_id, "default", "shared", config_small())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source adds 3 vectors near [1, 0, 0]
    vector
        .insert(
            source_id,
            "default",
            "shared",
            "src_a",
            &[0.99, 0.01, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            source_id,
            "default",
            "shared",
            "src_b",
            &[0.95, 0.05, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            source_id,
            "default",
            "shared",
            "src_c",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();

    // Target adds 3 vectors — two near [1, 0, 0], one far away
    vector
        .insert(
            target_id,
            "default",
            "shared",
            "tgt_a",
            &[0.98, 0.02, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            target_id,
            "default",
            "shared",
            "tgt_b",
            &[0.0, 0.0, 1.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            target_id,
            "default",
            "shared",
            "tgt_c",
            &[0.5, 0.5, 0.0],
            None,
        )
        .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("disjoint-id vector merge should succeed");

    // Search for [1, 0, 0] with k=3. Top results must come from BOTH
    // branches: src_a (0.99), tgt_a (0.98), src_b (0.95) — i.e. the
    // top-3 are src_a, tgt_a, src_b in that order, mixing source and
    // target additions.
    let hits = vector
        .search(target_id, "default", "shared", &[1.0, 0.0, 0.0], 3, None)
        .unwrap();
    assert_eq!(
        hits.len(),
        3,
        "expected k=3 search to return 3 results; got {}",
        hits.len()
    );

    let returned_keys: Vec<&str> = hits.iter().map(|h| h.key.as_str()).collect();
    let has_src = returned_keys.iter().any(|k| k.starts_with("src_"));
    let has_tgt = returned_keys.iter().any(|k| k.starts_with("tgt_"));
    assert!(
        has_src && has_tgt,
        "post-merge top-k must include vectors from BOTH source and target; got {returned_keys:?}"
    );

    // src_a (0.99 cosine ≈ 1.0) should be the top hit.
    assert_eq!(
        hits[0].key, "src_a",
        "src_a (0.99 cosine to query) should be the top result; got {returned_keys:?}"
    );
}

#[test]
fn vector_merge_does_not_touch_unaffected_collections() {
    // Regression test for the per-collection rebuild contract:
    // collections that the merge does NOT touch should not be rebuilt.
    //
    // We insert vectors into the untouched collection in NON-lex order
    // so each vector's VectorId reflects insertion order, not key
    // order. The rebuild path scans KV in lexicographic order, so a
    // regression that re-introduces full-branch rebuild would
    // reassign IDs and the assertion below would fail. With proper
    // per-collection scoping (the merge doesn't touch this
    // collection → no rebuild fires), all VectorIds stay byte-for-byte
    // identical to their pre-merge values.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let vector = test_db.vector();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Create two collections on target before fork.
    vector
        .create_collection(target_id, "default", "touched", config_small())
        .unwrap();
    vector
        .create_collection(target_id, "default", "untouched", config_small())
        .unwrap();

    // Insert in non-lexicographic order. After this loop:
    //   c → vid 0,  e → vid 1,  a → vid 2,  d → vid 3,  b → vid 4
    // A lexicographic-order rebuild would reassign:
    //   a → vid 0,  b → vid 1,  c → vid 2,  d → vid 3,  e → vid 4
    // So pre-merge and post-merge IDs only match if NO rebuild fires.
    for (key, embedding) in [
        ("c", [0.5, 0.5, 0.0f32]),
        ("e", [0.1, 0.9, 0.0f32]),
        ("a", [1.0, 0.0, 0.0f32]),
        ("d", [0.3, 0.7, 0.0f32]),
        ("b", [0.9, 0.1, 0.0f32]),
    ] {
        vector
            .insert(target_id, "default", "untouched", key, &embedding, None)
            .unwrap();
    }

    // Capture all pre-merge VectorIds.
    let pre_merge_ids: std::collections::HashMap<&str, u64> = ["a", "b", "c", "d", "e"]
        .iter()
        .map(|&key| {
            let id = vector
                .get(target_id, "default", "untouched", key)
                .unwrap()
                .unwrap()
                .value
                .vector_id
                .as_u64();
            (key, id)
        })
        .collect();

    // Sanity check: insertion order produced non-lex IDs (otherwise a
    // coincidental rebuild would reproduce them and the test is vacuous).
    assert_ne!(
        pre_merge_ids[&"a"], 0,
        "test setup invariant: 'a' was inserted third, so its VectorId must not be 0"
    );

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source touches ONLY the "touched" collection.
    vector
        .insert(
            source_id,
            "default",
            "touched",
            "new",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("single-side vector merge should succeed");

    // The touched collection sees the new vector.
    assert!(vector
        .get(target_id, "default", "touched", "new")
        .unwrap()
        .is_some());

    // ALL of the untouched collection's VectorIds must remain stable.
    // A regression that re-introduces full-branch rebuild would shuffle
    // these IDs because the rebuild scans lexicographically.
    for key in ["a", "b", "c", "d", "e"] {
        let post_id = vector
            .get(target_id, "default", "untouched", key)
            .unwrap()
            .unwrap()
            .value
            .vector_id
            .as_u64();
        assert_eq!(
            post_id, pre_merge_ids[key],
            "untouched collection's VectorId for '{key}' shifted across merge \
             (was {}, now {}) — per-collection rebuild scoping regression",
            pre_merge_ids[key], post_id
        );
    }

    // Search still works on the untouched collection.
    let hits = vector
        .search(target_id, "default", "untouched", &[1.0, 0.0, 0.0], 1, None)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].key, "a");
}

#[test]
fn vector_cherry_pick_refreshes_hnsw_backend() {
    // Regression test for the cherry-pick + vector post_commit gap.
    //
    // Cherry-pick has its own apply path that historically called
    // `reload_secondary_backends` directly. After Phase 4, the
    // `VectorRefreshHook::post_merge_reload` callback is a no-op
    // (the per-handler `post_commit` lifecycle owns vector rebuilds).
    // If `cherry_pick_from_diff` does not also dispatch through the
    // `PrimitiveMergeHandler::post_commit` lifecycle, vectors land
    // correctly in target's KV but the in-memory HNSW backend stays
    // stale — k-NN search returns the pre-cherry-pick result set.
    //
    // This test cherry-picks a new vector from source to target and
    // asserts that searching for that vector's embedding returns the
    // new key (not the old one).
    use strata_engine::branch_ops::CherryPickFilter;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let vector = test_db.vector();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Seed target with a collection and one vector at [0, 1, 0].
    vector
        .create_collection(target_id, "default", "shared", config_small())
        .unwrap();
    vector
        .insert(
            target_id,
            "default",
            "shared",
            "old",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();

    // Pre-cherry-pick: searching for [1, 0, 0] returns "old" (only
    // vector in the collection).
    let pre_hits = vector
        .search(target_id, "default", "shared", &[1.0, 0.0, 0.0], 1, None)
        .unwrap();
    assert_eq!(pre_hits.len(), 1);
    assert_eq!(pre_hits[0].key, "old");

    // Fork target → source, source adds a vector at [1, 0, 0]
    // (closer to the query than "old").
    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    vector
        .insert(
            source_id,
            "default",
            "shared",
            "new",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();

    // Cherry-pick everything from source to target.
    branch_ops::cherry_pick_from_diff(
        &test_db.db,
        "source",
        "target",
        CherryPickFilter::default(),
        None,
    )
    .expect("vector cherry-pick should succeed");

    // The cherry-picked vector must be visible AND searchable. The
    // search returning "new" (not "old") proves the in-memory HNSW
    // backend was refreshed — without the post_commit dispatch, this
    // assertion would fail because the backend would still only know
    // about "old".
    assert!(
        vector
            .get(target_id, "default", "shared", "new")
            .unwrap()
            .is_some(),
        "cherry-picked vector should be present in target's KV"
    );

    let post_hits = vector
        .search(target_id, "default", "shared", &[1.0, 0.0, 0.0], 1, None)
        .unwrap();
    assert_eq!(post_hits.len(), 1);
    assert_eq!(
        post_hits[0].key, "new",
        "cherry-picked vector should be the top hit; if 'old' is returned, \
         the in-memory HNSW backend was not refreshed by cherry-pick \
         (post_commit dispatch missing — Phase 4 regression)"
    );
}

// ============================================================================
// JSON path-level merge
// ============================================================================
//
// See docs/design/branching/primitive-aware-merge.md.
// `JsonMergeHandler::plan` runs a per-document path-level three-way merge
// on JSON documents, combining disjoint path edits from both sides into
// a single merged document instead of falling back to whole-doc LWW.
// Same-path edits to different values still produce a conflict that
// honors `MergeStrategy`. `post_commit` refreshes secondary indexes
// and the BM25 inverted index for documents the merge touched.

#[test]
fn json_merge_disjoint_paths_auto_merges() {
    // Both branches modify the same document but at disjoint top-level
    // keys. The path-level merge combines both edits without raising
    // a conflict.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    json.create(
        &target_id,
        "default",
        "doc-1",
        json_value(serde_json::json!({"a": 1, "b": 2})),
    )
    .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source modifies "a", target modifies "b" — disjoint edits.
    json.set(
        &source_id,
        "default",
        "doc-1",
        &"a".parse().unwrap(),
        json_value(serde_json::json!(10)),
    )
    .unwrap();
    json.set(
        &target_id,
        "default",
        "doc-1",
        &"b".parse().unwrap(),
        json_value(serde_json::json!(20)),
    )
    .unwrap();

    let info =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None)
            .expect("disjoint path merge should succeed under Strict (no conflicts)");
    assert_eq!(
        info.conflicts.len(),
        0,
        "disjoint path edits must not produce conflicts"
    );

    let merged = json
        .get(&target_id, "default", "doc-1", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        merged.as_inner(),
        &serde_json::json!({"a": 10, "b": 20}),
        "merged document should combine both sides' edits"
    );
}

#[test]
fn json_merge_same_path_same_value_no_conflict() {
    // Both branches edit the same path to the same new value. The merge
    // should converge silently — no conflict, no LWW arbitration.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    json.create(
        &target_id,
        "default",
        "doc-1",
        json_value(serde_json::json!({"status": "draft"})),
    )
    .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    json.set(
        &source_id,
        "default",
        "doc-1",
        &"status".parse().unwrap(),
        json_value(serde_json::json!("published")),
    )
    .unwrap();
    json.set(
        &target_id,
        "default",
        "doc-1",
        &"status".parse().unwrap(),
        json_value(serde_json::json!("published")),
    )
    .unwrap();

    let info =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None)
            .expect("same-path same-value merge should be conflict-free under Strict");
    assert_eq!(info.conflicts.len(), 0);

    let merged = json
        .get(&target_id, "default", "doc-1", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        merged.as_inner(),
        &serde_json::json!({"status": "published"})
    );
}

#[test]
fn json_merge_same_path_different_values_lww() {
    // Both branches edit the same path to different values. Strict
    // mode rejects with a conflict; LWW resolves source-wins.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    json.create(
        &target_id,
        "default",
        "doc-1",
        json_value(serde_json::json!({"name": "Alice", "age": 30})),
    )
    .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Both sides edit "name" — same path, different values → conflict.
    json.set(
        &source_id,
        "default",
        "doc-1",
        &"name".parse().unwrap(),
        json_value(serde_json::json!("Bob")),
    )
    .unwrap();
    json.set(
        &target_id,
        "default",
        "doc-1",
        &"name".parse().unwrap(),
        json_value(serde_json::json!("Carol")),
    )
    .unwrap();

    // Strict refuses.
    let strict =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None);
    assert!(
        strict.is_err(),
        "same-path different-values merge must be rejected under Strict"
    );

    // Target should be unchanged after the failed strict merge.
    let after_strict = json
        .get(&target_id, "default", "doc-1", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        after_strict.as_inner(),
        &serde_json::json!({"name": "Carol", "age": 30}),
        "Strict failure must not mutate the target"
    );

    // LWW resolves source-wins on the conflicting path. The merged
    // document still has age=30 (unchanged on both sides).
    let info = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("LWW merge should succeed despite the path-level conflict");
    assert!(
        !info.conflicts.is_empty(),
        "LWW merge should still surface the conflict to the caller"
    );
    let merged = json
        .get(&target_id, "default", "doc-1", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        merged.as_inner(),
        &serde_json::json!({"name": "Bob", "age": 30}),
        "LWW should pick source's name and keep the unchanged age"
    );
}

#[test]
fn json_merge_subtree_delete_vs_edit_conflict() {
    // One side deletes a subtree (a nested key) while the other side
    // edits within that subtree. The handler reports this as a
    // path-level conflict — there is no obvious correct merge.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    json.create(
        &target_id,
        "default",
        "doc-1",
        json_value(serde_json::json!({
            "user": {"name": "Alice", "email": "a@example.com"},
            "active": true,
        })),
    )
    .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source deletes the entire user.email key.
    json.delete_at_path(
        &source_id,
        "default",
        "doc-1",
        &"user.email".parse().unwrap(),
    )
    .unwrap();
    // Target edits user.email to a new value.
    json.set(
        &target_id,
        "default",
        "doc-1",
        &"user.email".parse().unwrap(),
        json_value(serde_json::json!("alice@new.com")),
    )
    .unwrap();

    // Strict mode rejects with the conflict surfaced.
    let strict =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None);
    assert!(
        strict.is_err(),
        "delete-vs-edit on the same path must be rejected under Strict"
    );

    // LWW: source-wins → email is removed in the merged result.
    let info = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("LWW merge should succeed for delete-vs-edit");
    assert!(
        !info.conflicts.is_empty(),
        "LWW must still report the path-level conflict"
    );

    let merged = json
        .get(&target_id, "default", "doc-1", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        merged.as_inner(),
        &serde_json::json!({
            "user": {"name": "Alice"},
            "active": true,
        }),
        "LWW source-wins must drop the deleted email key"
    );
}

#[test]
fn json_merge_nested_disjoint_paths_auto_merge() {
    // Disjoint edits inside the same nested object — exercises the
    // recursive object-walk path of `merge_json_values`.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    json.create(
        &target_id,
        "default",
        "doc-1",
        json_value(serde_json::json!({
            "user": {"name": "Alice", "age": 30},
            "tags": "admin"
        })),
    )
    .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    json.set(
        &source_id,
        "default",
        "doc-1",
        &"user.name".parse().unwrap(),
        json_value(serde_json::json!("Bob")),
    )
    .unwrap();
    json.set(
        &target_id,
        "default",
        "doc-1",
        &"user.age".parse().unwrap(),
        json_value(serde_json::json!(31)),
    )
    .unwrap();

    let info =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None)
            .expect("disjoint nested path edits should be conflict-free");
    assert_eq!(info.conflicts.len(), 0);

    let merged = json
        .get(&target_id, "default", "doc-1", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        merged.as_inner(),
        &serde_json::json!({
            "user": {"name": "Bob", "age": 31},
            "tags": "admin"
        }),
    );
}

#[test]
fn json_merge_secondary_index_refreshed_post_commit() {
    // post_commit refreshes secondary index entries for the documents
    // touched by the merge. Without that refresh, an index lookup on
    // the merged target would still return the pre-merge field value.
    use strata_engine::primitives::json::index::IndexType;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    json.create(
        &target_id,
        "default",
        "doc-1",
        json_value(serde_json::json!({"price": 10.0, "name": "widget"})),
    )
    .unwrap();
    json.create_index(
        &target_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();
    // Backfill the existing doc into the freshly created index by
    // re-touching its price (set is the only public mutation that
    // re-runs the index update path).
    json.set(
        &target_id,
        "default",
        "doc-1",
        &"price".parse().unwrap(),
        json_value(serde_json::json!(10.0)),
    )
    .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source updates price 10 → 25. Target leaves it alone.
    json.set(
        &source_id,
        "default",
        "doc-1",
        &"price".parse().unwrap(),
        json_value(serde_json::json!(25.0)),
    )
    .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("merge with index-touching change should succeed");

    // The merged target's doc value should be the source's update.
    let merged = json
        .get(&target_id, "default", "doc-1", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        merged.as_inner().get("price"),
        Some(&serde_json::json!(25.0))
    );

    // The secondary index for price should now resolve a query for
    // value=25 to doc-1 and NOT resolve a query for value=10 to doc-1.
    // We use the lower-level lookup_eq helper because the public
    // search() API requires a recipe.
    use strata_engine::primitives::json::index;
    let encoded_25 = index::encode_numeric(25.0);
    let encoded_10 = index::encode_numeric(10.0);
    let mut hits_25: Vec<String> = Vec::new();
    let mut hits_10: Vec<String> = Vec::new();
    test_db
        .db
        .transaction(target_id, |txn| {
            hits_25 = index::lookup_eq(txn, &target_id, "default", "price_idx", &encoded_25)?;
            hits_10 = index::lookup_eq(txn, &target_id, "default", "price_idx", &encoded_10)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        hits_25,
        vec!["doc-1".to_string()],
        "post_commit must populate the index entry for the merged value"
    );
    assert!(
        hits_10.is_empty(),
        "post_commit must remove the index entry for the pre-merge value; \
         found stale entries: {:?}",
        hits_10
    );
}

#[test]
fn json_merge_source_only_new_doc_propagates() {
    // Source creates a brand-new doc that target doesn't have. The
    // merge should propagate it as a SourceAdded action and refresh
    // any secondary indexes on target.
    use strata_engine::primitives::json::index::IndexType;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Create the index on target BEFORE forking so source inherits it.
    json.create_index(
        &target_id,
        "default",
        "qty_idx",
        "$.qty",
        IndexType::Numeric,
    )
    .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source creates a new doc — target has nothing.
    json.create(
        &source_id,
        "default",
        "doc-new",
        json_value(serde_json::json!({"qty": 7, "name": "fresh"})),
    )
    .unwrap();

    let info =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None)
            .expect("source-only new doc merge should succeed under Strict");
    assert_eq!(info.conflicts.len(), 0);
    assert!(info.keys_applied >= 1);

    // Target should now have the new doc with the same value.
    let merged = json
        .get(&target_id, "default", "doc-new", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        merged.as_inner(),
        &serde_json::json!({"qty": 7, "name": "fresh"})
    );

    // Secondary index on $.qty should resolve a query for value=7 to
    // the merged doc — proves post_commit emitted the index entry for
    // the new doc, not just for pre-existing target docs.
    use strata_engine::primitives::json::index;
    let encoded_7 = index::encode_numeric(7.0);
    let mut hits: Vec<String> = Vec::new();
    test_db
        .db
        .transaction(target_id, |txn| {
            hits = index::lookup_eq(txn, &target_id, "default", "qty_idx", &encoded_7)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        hits,
        vec!["doc-new".to_string()],
        "post_commit must populate the index entry for a source-added doc"
    );
}

#[test]
fn json_merge_source_deletes_doc_propagates() {
    // Source deletes a doc that target has unchanged from the fork
    // point. SourceDeleted → the merge should delete the doc on target
    // AND drop its secondary index entries.
    use strata_engine::primitives::json::index::IndexType;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    json.create(
        &target_id,
        "default",
        "doc-doomed",
        json_value(serde_json::json!({"qty": 99, "label": "ephemeral"})),
    )
    .unwrap();
    json.create_index(
        &target_id,
        "default",
        "qty_idx",
        "$.qty",
        IndexType::Numeric,
    )
    .unwrap();
    // Backfill the existing doc into the index by re-touching its qty.
    json.set(
        &target_id,
        "default",
        "doc-doomed",
        &"qty".parse().unwrap(),
        json_value(serde_json::json!(99)),
    )
    .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source destroys the doc; target leaves it alone.
    let existed = json.destroy(&source_id, "default", "doc-doomed").unwrap();
    assert!(existed);

    let info =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None)
            .expect("source-delete merge should succeed under Strict");
    assert_eq!(info.conflicts.len(), 0);
    assert!(info.keys_deleted >= 1);

    // Doc should be gone on target.
    let after = json
        .get(&target_id, "default", "doc-doomed", &root())
        .unwrap();
    assert!(
        after.is_none(),
        "doc should be deleted on target after merge"
    );

    // Index entry for value=99 should also be gone — post_commit
    // delegated the cleanup to update_index_entries(old=Some, new=None).
    use strata_engine::primitives::json::index;
    let encoded_99 = index::encode_numeric(99.0);
    let mut hits: Vec<String> = Vec::new();
    test_db
        .db
        .transaction(target_id, |txn| {
            hits = index::lookup_eq(txn, &target_id, "default", "qty_idx", &encoded_99)?;
            Ok(())
        })
        .unwrap();
    assert!(
        hits.is_empty(),
        "index entry for the deleted doc must be removed by post_commit; \
         found stale entries: {:?}",
        hits
    );
}

#[test]
fn json_merge_multiple_docs_in_one_pass() {
    // The handler walks every JSON cell user_key. A single merge that
    // touches several distinct documents must produce one merge action
    // per affected doc and refresh all of them.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Three docs on the target side, each with the same shape.
    for i in 0..3 {
        json.create(
            &target_id,
            "default",
            &format!("doc-{}", i),
            json_value(serde_json::json!({"a": i, "b": 0})),
        )
        .unwrap();
    }

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source modifies each doc's "a" field; target modifies each doc's
    // "b" field. Disjoint paths within each doc → all three should
    // path-level merge cleanly.
    for i in 0..3 {
        json.set(
            &source_id,
            "default",
            &format!("doc-{}", i),
            &"a".parse().unwrap(),
            json_value(serde_json::json!(100 + i)),
        )
        .unwrap();
        json.set(
            &target_id,
            "default",
            &format!("doc-{}", i),
            &"b".parse().unwrap(),
            json_value(serde_json::json!(200 + i)),
        )
        .unwrap();
    }

    let info =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None)
            .expect("multi-doc disjoint-path merge should succeed under Strict");
    assert_eq!(info.conflicts.len(), 0);
    assert!(
        info.keys_applied >= 3,
        "expected at least 3 doc updates, got {}",
        info.keys_applied
    );

    for i in 0..3 {
        let merged = json
            .get(&target_id, "default", &format!("doc-{}", i), &root())
            .unwrap()
            .unwrap();
        assert_eq!(
            merged.as_inner(),
            &serde_json::json!({"a": 100 + i, "b": 200 + i}),
            "doc-{} should have both sides' edits merged",
            i
        );
    }
}

#[test]
fn json_merge_path_level_via_cherry_pick() {
    // `cherry_pick_from_diff` routes through the same per-handler
    // dispatch as `merge_branches`. This test pins that contract for
    // JSON: a cherry-pick of a disjoint-path edit must produce the same
    // merged-doc result as a full merge would, AND the secondary index
    // for the cherry-picked field must be refreshed.
    //
    // Without the per-handler dispatch in cherry_pick, this test would
    // fail because cherry_pick would fall back to the generic 14-case
    // classifier, which writes source's whole doc verbatim and never
    // refreshes the index. (Mirrors the
    // `vector_cherry_pick_refreshes_hnsw_backend` regression test.)
    use strata_engine::branch_ops::CherryPickFilter;
    use strata_engine::primitives::json::index::IndexType;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    json.create(
        &target_id,
        "default",
        "doc-1",
        json_value(serde_json::json!({"a": 1, "b": 2, "price": 10.0})),
    )
    .unwrap();
    json.create_index(
        &target_id,
        "default",
        "price_idx",
        "$.price",
        IndexType::Numeric,
    )
    .unwrap();
    json.set(
        &target_id,
        "default",
        "doc-1",
        &"price".parse().unwrap(),
        json_value(serde_json::json!(10.0)),
    )
    .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source edits "a" and "price"; target edits "b" — three disjoint
    // edits across the same doc. The path-level merge should combine
    // all three.
    json.set(
        &source_id,
        "default",
        "doc-1",
        &"a".parse().unwrap(),
        json_value(serde_json::json!(11)),
    )
    .unwrap();
    json.set(
        &source_id,
        "default",
        "doc-1",
        &"price".parse().unwrap(),
        json_value(serde_json::json!(42.0)),
    )
    .unwrap();
    json.set(
        &target_id,
        "default",
        "doc-1",
        &"b".parse().unwrap(),
        json_value(serde_json::json!(22)),
    )
    .unwrap();

    branch_ops::cherry_pick_from_diff(
        &test_db.db,
        "source",
        "target",
        CherryPickFilter::default(),
        None,
    )
    .expect("cherry-pick of a disjoint-path JSON edit should succeed");

    let merged = json
        .get(&target_id, "default", "doc-1", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        merged.as_inner(),
        &serde_json::json!({"a": 11, "b": 22, "price": 42.0}),
        "cherry-pick must produce the same path-merged result as merge_branches"
    );

    // Index for the cherry-picked price field must be refreshed.
    use strata_engine::primitives::json::index;
    let encoded_42 = index::encode_numeric(42.0);
    let encoded_10 = index::encode_numeric(10.0);
    let mut hits_42: Vec<String> = Vec::new();
    let mut hits_10: Vec<String> = Vec::new();
    test_db
        .db
        .transaction(target_id, |txn| {
            hits_42 = index::lookup_eq(txn, &target_id, "default", "price_idx", &encoded_42)?;
            hits_10 = index::lookup_eq(txn, &target_id, "default", "price_idx", &encoded_10)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        hits_42,
        vec!["doc-1".to_string()],
        "cherry-pick post_commit must populate the index entry for the merged price; \
         a regression here means cherry_pick_from_diff is bypassing JsonMergeHandler"
    );
    assert!(
        hits_10.is_empty(),
        "cherry-pick post_commit must remove the index entry for the pre-merge price; \
         found stale entries: {:?}",
        hits_10
    );
}

#[test]
fn json_merge_both_sides_deleted_doc_no_action() {
    // Both source and target deleted the same doc since the merge base.
    // The handler must take no action — the doc is already gone on
    // target, and the secondary indexes were already cleaned up by
    // whichever side did the delete on target. Pins the
    // `(_, None, None, _, _, _)` arm of `merge_one_doc`.
    use strata_engine::primitives::json::index::IndexType;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    json.create(
        &target_id,
        "default",
        "doomed",
        json_value(serde_json::json!({"qty": 5})),
    )
    .unwrap();
    json.create_index(
        &target_id,
        "default",
        "qty_idx",
        "$.qty",
        IndexType::Numeric,
    )
    .unwrap();
    // Backfill the index for the existing doc.
    json.set(
        &target_id,
        "default",
        "doomed",
        &"qty".parse().unwrap(),
        json_value(serde_json::json!(5)),
    )
    .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Both sides destroy the doc independently.
    let src_existed = json.destroy(&source_id, "default", "doomed").unwrap();
    let tgt_existed = json.destroy(&target_id, "default", "doomed").unwrap();
    assert!(src_existed && tgt_existed);

    // Sanity: target's index entry for value=5 should already be gone
    // because target's `destroy` ran update_index_entries.
    use strata_engine::primitives::json::index;
    let encoded_5 = index::encode_numeric(5.0);
    let mut pre_merge_hits: Vec<String> = Vec::new();
    test_db
        .db
        .transaction(target_id, |txn| {
            pre_merge_hits = index::lookup_eq(txn, &target_id, "default", "qty_idx", &encoded_5)?;
            Ok(())
        })
        .unwrap();
    assert!(
        pre_merge_hits.is_empty(),
        "target should have cleaned up its index pre-merge"
    );

    let info =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None)
            .expect("both-sides-deleted merge should succeed under Strict");
    // The merge produces no actions for the JSON cell (both sides agree
    // the doc is gone). It may produce an empty merge_version because
    // no other primitives changed either, which is fine.
    assert_eq!(info.conflicts.len(), 0);

    // Doc still gone on target.
    let after = json.get(&target_id, "default", "doomed", &root()).unwrap();
    assert!(after.is_none());

    // Index still empty for value=5.
    let mut post_merge_hits: Vec<String> = Vec::new();
    test_db
        .db
        .transaction(target_id, |txn| {
            post_merge_hits = index::lookup_eq(txn, &target_id, "default", "qty_idx", &encoded_5)?;
            Ok(())
        })
        .unwrap();
    assert!(
        post_merge_hits.is_empty(),
        "both-sides-deleted merge must not resurrect index entries; \
         got: {:?}",
        post_merge_hits
    );
}

#[test]
fn json_merge_parent_subtree_deleted_vs_child_edited_conflicts() {
    // Source removes a whole nested object; target edits a leaf inside
    // that nested object. The recursive merge should report a path-
    // level conflict at the parent path (the deleted subtree) — not
    // at the child path that target edited. LWW source-wins drops the
    // entire subtree.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let json = test_db.json();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    json.create(
        &target_id,
        "default",
        "doc-1",
        json_value(serde_json::json!({
            "user": {"name": "Alice", "email": "a@example.com"},
            "active": true,
        })),
    )
    .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Source deletes the WHOLE user object (the parent subtree).
    json.delete_at_path(&source_id, "default", "doc-1", &"user".parse().unwrap())
        .unwrap();
    // Target edits a leaf INSIDE the deleted subtree.
    json.set(
        &target_id,
        "default",
        "doc-1",
        &"user.email".parse().unwrap(),
        json_value(serde_json::json!("alice@new.com")),
    )
    .unwrap();

    // Strict mode rejects.
    let strict =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None);
    assert!(
        strict.is_err(),
        "parent-subtree-delete vs child-edit must be rejected under Strict"
    );

    // LWW: source-wins drops the entire user subtree.
    let info = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("LWW should succeed for parent-delete-vs-child-edit");
    assert!(
        !info.conflicts.is_empty(),
        "LWW must still report the path-level conflict"
    );
    // The conflict path should reference the deleted parent ("user"),
    // not the leaf the target edited. This pins the recursive walk's
    // behavior of reporting the conflict at the highest mismatching
    // node.
    let has_user_conflict = info
        .conflicts
        .iter()
        .any(|c| c.key.contains("@user") && !c.key.contains("@user."));
    assert!(
        has_user_conflict,
        "expected a conflict reported at parent path 'user', got conflicts: {:?}",
        info.conflicts.iter().map(|c| &c.key).collect::<Vec<_>>()
    );

    let merged = json
        .get(&target_id, "default", "doc-1", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        merged.as_inner(),
        &serde_json::json!({"active": true}),
        "LWW source-wins must drop the entire user subtree"
    );
}

// ============================================================================
// Cross-primitive parity (Claim 4 verification suite)
// ============================================================================
//
// These tests prove "all primitives inherit branching uniformly" by
// behavior, not by storage plumbing — they fork a branch, mutate every
// primitive, merge, and verify that each primitive's invariants hold on
// the merged target. See `docs/design/branching/primitive-aware-merge.md`
// for the design and acceptance criteria.

/// Verify the standard graph adjacency invariant on a branch: every
/// edge's endpoints exist on both sides (forward and reverse adjacency
/// records refer to nodes that are still present), and the bidirectional
/// adjacency is consistent. Used by the cross-primitive invariant sweep.
///
/// Returns `Ok(())` on success or a descriptive error on the first
/// invariant violation. Mirrors `verify_event_chain` for events.
fn verify_graph_adjacency(
    graph: &GraphStore,
    branch: &BranchId,
    space: &str,
    graph_name: &str,
    expected_nodes: &[&str],
) -> Result<(), String> {
    // Every expected node must exist.
    for node_id in expected_nodes {
        let node = graph
            .get_node(*branch, space, graph_name, node_id)
            .map_err(|e| format!("get_node({}) failed: {}", node_id, e))?;
        if node.is_none() {
            return Err(format!("expected node '{}' missing on target", node_id));
        }
    }
    // For each node, walk outgoing edges and verify each target exists.
    // Then walk incoming edges and verify the reverse direction is also
    // recorded (bidirectional consistency).
    for node_id in expected_nodes {
        let outgoing = graph
            .outgoing_neighbors(*branch, space, graph_name, node_id, None)
            .map_err(|e| format!("outgoing_neighbors({}) failed: {}", node_id, e))?;
        for neighbor in &outgoing {
            // Endpoint exists.
            if graph
                .get_node(*branch, space, graph_name, &neighbor.node_id)
                .map_err(|e| format!("get_node({}) failed: {}", neighbor.node_id, e))?
                .is_none()
            {
                return Err(format!(
                    "dangling outgoing edge: {}→{} but {} doesn't exist",
                    node_id, neighbor.node_id, neighbor.node_id
                ));
            }
            // Reverse direction is recorded.
            let reverse = graph
                .incoming_neighbors(*branch, space, graph_name, &neighbor.node_id, None)
                .map_err(|e| format!("incoming_neighbors({}) failed: {}", neighbor.node_id, e))?;
            if !reverse.iter().any(|r| r.node_id == *node_id) {
                return Err(format!(
                    "bidirectional consistency violated: {}→{} present in outgoing but reverse \
                     {} not in {}'s incoming",
                    node_id, neighbor.node_id, node_id, neighbor.node_id
                ));
            }
        }
    }
    Ok(())
}

/// Test #1 — fork isolation across all five primitives. Extends the
/// existing `all_primitives_isolated_between_branches` (which omits
/// Graph and uses two unrelated branches) by exercising a real fork
/// relationship and writing to all five primitives on both sides.
#[test]
fn cross_primitive_fork_isolation() {
    use strata_graph::types::NodeData;

    let test_db = TestDb::new();
    let p = test_db.all_primitives();
    let branch_index = test_db.branch_index();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Seed every primitive on target before fork.
    p.kv.put(&target_id, "default", "k", Value::Int(1)).unwrap();
    p.event
        .append(&target_id, "default", "stream-a", int_payload(1))
        .unwrap();
    p.json
        .create(
            &target_id,
            "default",
            "doc-1",
            json_value(serde_json::json!({"x": 1})),
        )
        .unwrap();
    p.vector
        .create_collection(target_id, "default", "vc", config_small())
        .unwrap();
    p.vector
        .insert(
            target_id,
            "default",
            "vc",
            "v-target",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "n-target", NodeData::default())
        .unwrap();

    // Fork.
    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Mutate every primitive on source only. Target stays untouched.
    p.kv.put(&source_id, "default", "k", Value::Int(99))
        .unwrap();
    p.event
        .append(&source_id, "default", "stream-a", int_payload(99))
        .unwrap();
    p.json
        .set(
            &source_id,
            "default",
            "doc-1",
            &"x".parse().unwrap(),
            json_value(serde_json::json!(99)),
        )
        .unwrap();
    p.vector
        .insert(
            source_id,
            "default",
            "vc",
            "v-source",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();
    p.graph
        .add_node(source_id, "default", "g", "n-source", NodeData::default())
        .unwrap();

    // Target should still see ONLY pre-fork values for every primitive.
    assert_eq!(
        p.kv.get(&target_id, "default", "k").unwrap(),
        Some(Value::Int(1)),
        "target's KV should be unchanged after source-side mutation"
    );
    assert_eq!(
        p.event.len(&target_id, "default").unwrap(),
        1,
        "target's event log should still hold the pre-fork single event"
    );
    let target_doc = p
        .json
        .get(&target_id, "default", "doc-1", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        target_doc.as_inner().get("x"),
        Some(&serde_json::json!(1)),
        "target's JSON doc should still have x=1"
    );
    assert!(
        p.vector
            .get(target_id, "default", "vc", "v-source")
            .unwrap()
            .is_none(),
        "target should not see source's added vector"
    );
    assert!(
        p.graph
            .get_node(target_id, "default", "g", "n-source")
            .unwrap()
            .is_none(),
        "target should not see source's added graph node"
    );

    // Source should see post-mutation values for every primitive.
    assert_eq!(
        p.kv.get(&source_id, "default", "k").unwrap(),
        Some(Value::Int(99))
    );
    assert_eq!(p.event.len(&source_id, "default").unwrap(), 2);
    let source_doc = p
        .json
        .get(&source_id, "default", "doc-1", &root())
        .unwrap()
        .unwrap();
    assert_eq!(source_doc.as_inner().get("x"), Some(&serde_json::json!(99)));
    assert!(p
        .vector
        .get(source_id, "default", "vc", "v-source")
        .unwrap()
        .is_some());
    assert!(p
        .graph
        .get_node(source_id, "default", "g", "n-source")
        .unwrap()
        .is_some());
}

/// Test #2 — disjoint cross-primitive merge. Each side mutates a
/// different subset of primitives. The merge must succeed under Strict
/// (no conflicts) and target must end up with both sides' contributions.
#[test]
fn cross_primitive_merge_disjoint() {
    use strata_graph::types::NodeData;

    let test_db = TestDb::new();
    let p = test_db.all_primitives();
    let branch_index = test_db.branch_index();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Seed shared baseline state on target.
    p.kv.put(&target_id, "default", "shared", Value::Int(0))
        .unwrap();
    p.json
        .create(
            &target_id,
            "default",
            "doc-shared",
            json_value(serde_json::json!({"a": 0, "b": 0})),
        )
        .unwrap();
    p.vector
        .create_collection(target_id, "default", "vc", config_small())
        .unwrap();
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "root", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // SOURCE mutates KV / JSON / Vector. TARGET mutates Event / Graph.
    // Disjoint primitives → merge under Strict must succeed.
    p.kv.put(&source_id, "default", "kv-only", Value::Int(42))
        .unwrap();
    p.json
        .set(
            &source_id,
            "default",
            "doc-shared",
            &"a".parse().unwrap(),
            json_value(serde_json::json!(11)),
        )
        .unwrap();
    p.vector
        .insert(
            source_id,
            "default",
            "vc",
            "v-source",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();

    p.event
        .append(&target_id, "default", "stream-x", int_payload(1))
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "tgt-leaf", NodeData::default())
        .unwrap();

    let info =
        branch_ops::merge_branches(&test_db.db, "source", "target", MergeStrategy::Strict, None)
            .expect("disjoint cross-primitive merge should succeed under Strict");
    assert_eq!(info.conflicts.len(), 0);
    assert!(
        info.keys_applied >= 3,
        "expected at least KV+JSON+Vector applies, got {}",
        info.keys_applied
    );

    // KV: source's new key landed on target.
    assert_eq!(
        p.kv.get(&target_id, "default", "kv-only").unwrap(),
        Some(Value::Int(42))
    );
    // KV: shared baseline preserved.
    assert_eq!(
        p.kv.get(&target_id, "default", "shared").unwrap(),
        Some(Value::Int(0))
    );
    // JSON: source's edit applied.
    let merged_doc = p
        .json
        .get(&target_id, "default", "doc-shared", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        merged_doc.as_inner(),
        &serde_json::json!({"a": 11, "b": 0}),
        "JSON merge should apply source's edit"
    );
    // Vector: source's vector visible on target.
    assert!(p
        .vector
        .get(target_id, "default", "vc", "v-source")
        .unwrap()
        .is_some());
    // Vector: HNSW search returns the source-added vector.
    let hits = p
        .vector
        .search(target_id, "default", "vc", &[1.0, 0.0, 0.0], 1, None)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].key, "v-source");
    // Event: target's append still visible.
    assert_eq!(p.event.len(&target_id, "default").unwrap(), 1);
    // Graph: target's added node still present.
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "tgt-leaf")
        .unwrap()
        .is_some());
}

/// Test #3 — overlapping cross-primitive merge under LWW. Both sides
/// mutate every primitive with overlapping keys. The merge must resolve
/// each primitive's conflicts semantically — not just byte-for-byte.
#[test]
fn cross_primitive_merge_overlapping_lww() {
    use strata_graph::types::NodeData;

    let test_db = TestDb::new();
    let p = test_db.all_primitives();
    let branch_index = test_db.branch_index();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Seed shared state.
    p.kv.put(&target_id, "default", "shared", Value::Int(0))
        .unwrap();
    p.json
        .create(
            &target_id,
            "default",
            "doc-shared",
            json_value(serde_json::json!({"a": 0, "b": 0, "c": 0})),
        )
        .unwrap();
    p.vector
        .create_collection(target_id, "default", "vc", config_small())
        .unwrap();
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(
            target_id,
            "default",
            "g",
            "shared-node",
            NodeData::default(),
        )
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // OVERLAP: both sides mutate the same KV key, JSON doc paths, vector
    // collection (different keys), and add nodes to the same graph.
    // Note: we deliberately use DIFFERENT spaces / event-streams for
    // Event (`stream-source` vs `stream-target`) since divergent appends
    // to the same event stream are unmergeable per Phase 1 — that's a
    // separate test (`event_merge_divergent_rejects` at line 778).
    p.kv.put(&source_id, "default", "shared", Value::Int(11))
        .unwrap();
    p.kv.put(&target_id, "default", "shared", Value::Int(22))
        .unwrap();

    // JSON: source edits "a", target edits "b" (disjoint paths inside
    // the same doc — should auto-merge), and both sides edit "c"
    // differently (path-level conflict, LWW source-wins).
    p.json
        .set(
            &source_id,
            "default",
            "doc-shared",
            &"a".parse().unwrap(),
            json_value(serde_json::json!(11)),
        )
        .unwrap();
    p.json
        .set(
            &source_id,
            "default",
            "doc-shared",
            &"c".parse().unwrap(),
            json_value(serde_json::json!(111)),
        )
        .unwrap();
    p.json
        .set(
            &target_id,
            "default",
            "doc-shared",
            &"b".parse().unwrap(),
            json_value(serde_json::json!(22)),
        )
        .unwrap();
    p.json
        .set(
            &target_id,
            "default",
            "doc-shared",
            &"c".parse().unwrap(),
            json_value(serde_json::json!(222)),
        )
        .unwrap();

    // Vector: each side adds a different vector to the same collection.
    p.vector
        .insert(
            source_id,
            "default",
            "vc",
            "v-source",
            &[1.0, 0.0, 0.0],
            None,
        )
        .unwrap();
    p.vector
        .insert(
            target_id,
            "default",
            "vc",
            "v-target",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();

    // Graph: each side adds a different node to the same graph.
    p.graph
        .add_node(source_id, "default", "g", "n-source", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "n-target", NodeData::default())
        .unwrap();

    let info = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("LWW merge should succeed even with overlapping conflicts");

    // KV: source-wins on "shared".
    assert_eq!(
        p.kv.get(&target_id, "default", "shared").unwrap(),
        Some(Value::Int(11)),
        "KV LWW should pick source's value"
    );

    // JSON: disjoint path edits combine; same-path conflict resolves
    // source-wins. Pin the path-level merge contract.
    let merged_doc = p
        .json
        .get(&target_id, "default", "doc-shared", &root())
        .unwrap()
        .unwrap();
    assert_eq!(
        merged_doc.as_inner(),
        &serde_json::json!({"a": 11, "b": 22, "c": 111}),
        "JSON merge: 'a' (source-only) and 'b' (target-only) auto-merge; \
         'c' (both) resolves source-wins"
    );
    // Source-vs-target conflict on "c" should appear in conflicts list.
    assert!(
        info.conflicts.iter().any(|c| c.key.contains("@c")),
        "expected a path-level conflict at 'c'; got: {:?}",
        info.conflicts.iter().map(|c| &c.key).collect::<Vec<_>>()
    );

    // Vector: both sides' vectors visible.
    assert!(p
        .vector
        .get(target_id, "default", "vc", "v-source")
        .unwrap()
        .is_some());
    assert!(p
        .vector
        .get(target_id, "default", "vc", "v-target")
        .unwrap()
        .is_some());
    // HNSW search returns each. Assert non-empty before indexing so a
    // regression returning no hits produces a clear failure rather than
    // an out-of-bounds panic.
    let hits_a = p
        .vector
        .search(target_id, "default", "vc", &[1.0, 0.0, 0.0], 1, None)
        .unwrap();
    assert!(!hits_a.is_empty(), "search for v-source returned no hits");
    assert_eq!(hits_a[0].key, "v-source");
    let hits_b = p
        .vector
        .search(target_id, "default", "vc", &[0.0, 1.0, 0.0], 1, None)
        .unwrap();
    assert!(!hits_b.is_empty(), "search for v-target returned no hits");
    assert_eq!(hits_b[0].key, "v-target");

    // Graph: both sides' nodes present.
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "n-source")
        .unwrap()
        .is_some());
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "n-target")
        .unwrap()
        .is_some());
    // Pre-fork shared node still present.
    assert!(p
        .graph
        .get_node(target_id, "default", "g", "shared-node")
        .unwrap()
        .is_some());
}

/// Test #4 — invariant sweep after a meaty cross-primitive merge.
/// Combines disjoint and overlapping mutations across all five
/// primitives, then runs each primitive's invariant checker.
#[test]
fn cross_primitive_merge_invariant_sweep() {
    use strata_engine::primitives::json::index::{self as jindex, IndexType};
    use strata_graph::types::{EdgeData, NodeData};

    let test_db = TestDb::new();
    let p = test_db.all_primitives();
    let branch_index = test_db.branch_index();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // ==== Seed baseline state ====
    p.kv.put(&target_id, "default", "shared", Value::Int(0))
        .unwrap();
    p.event
        .append(&target_id, "default", "stream-target-only", int_payload(1))
        .unwrap();
    p.json
        .create(
            &target_id,
            "default",
            "doc-1",
            json_value(serde_json::json!({"price": 10.0, "stock": 5})),
        )
        .unwrap();
    p.json
        .create_index(
            &target_id,
            "default",
            "price_idx",
            "$.price",
            IndexType::Numeric,
        )
        .unwrap();
    // Backfill the existing doc into the index.
    p.json
        .set(
            &target_id,
            "default",
            "doc-1",
            &"price".parse().unwrap(),
            json_value(serde_json::json!(10.0)),
        )
        .unwrap();
    p.vector
        .create_collection(target_id, "default", "vc", config_small())
        .unwrap();
    p.vector
        .insert(target_id, "default", "vc", "v-pre", &[1.0, 0.0, 0.0], None)
        .unwrap();
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "bob", NodeData::default())
        .unwrap();

    // ==== Fork ====
    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // ==== Source-side mutations ====
    p.kv.put(&source_id, "default", "shared", Value::Int(11))
        .unwrap();
    p.kv.put(&source_id, "default", "src-only", Value::Int(7))
        .unwrap();
    // Source updates the indexed JSON field — secondary index must
    // refresh atomically as part of the merge txn (Step 1 of Phase 7).
    p.json
        .set(
            &source_id,
            "default",
            "doc-1",
            &"price".parse().unwrap(),
            json_value(serde_json::json!(25.0)),
        )
        .unwrap();
    // Source adds a vector and a graph node + edge.
    p.vector
        .insert(
            source_id,
            "default",
            "vc",
            "v-source",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();
    p.graph
        .add_node(source_id, "default", "g", "carol", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    // ==== Target-side mutations (different keys to avoid event conflict) ====
    // Target also mutates `shared` — creates a KV conflict that the
    // merge resolves source-wins under LWW.
    p.kv.put(&target_id, "default", "shared", Value::Int(22))
        .unwrap();
    p.event
        .append(&target_id, "default", "stream-target-only", int_payload(2))
        .unwrap();
    // Target adds a different vector and graph edge.
    p.vector
        .insert(
            target_id,
            "default",
            "vc",
            "v-target",
            &[0.0, 0.0, 1.0],
            None,
        )
        .unwrap();
    p.graph
        .add_edge(
            target_id,
            "default",
            "g",
            "alice",
            "bob",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    // ==== Merge ====
    let info = branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("cross-primitive invariant-sweep merge should succeed");
    assert_eq!(
        info.conflicts.len(),
        1,
        "the only conflict should be on the shared KV key, got: {:?}",
        info.conflicts.iter().map(|c| &c.key).collect::<Vec<_>>()
    );

    // ==== Invariant sweep ====

    // Event: hash chain verifies and length matches.
    let event_count = verify_event_chain(&p.event, &target_id, "default")
        .expect("event hash chain must verify after cross-primitive merge");
    assert_eq!(
        event_count, 2,
        "target should have 2 events (1 pre-fork + 1 post-fork target-only)"
    );

    // Graph: adjacency invariant holds — every edge endpoint exists,
    // bidirectional consistency intact.
    verify_graph_adjacency(
        &p.graph,
        &target_id,
        "default",
        "g",
        &["alice", "bob", "carol"],
    )
    .expect("graph adjacency must verify after cross-primitive merge");

    // Vector: HNSW search returns every inserted vector. Assert
    // non-empty before indexing so a regression that returns no hits
    // produces a clear failure instead of an out-of-bounds panic.
    let pre_hit = p
        .vector
        .search(target_id, "default", "vc", &[1.0, 0.0, 0.0], 1, None)
        .unwrap();
    assert!(
        !pre_hit.is_empty(),
        "pre-fork vector search returned no hits"
    );
    assert_eq!(pre_hit[0].key, "v-pre", "pre-fork vector still findable");
    let src_hit = p
        .vector
        .search(target_id, "default", "vc", &[0.0, 1.0, 0.0], 1, None)
        .unwrap();
    assert!(
        !src_hit.is_empty(),
        "source-added vector search returned no hits"
    );
    assert_eq!(src_hit[0].key, "v-source", "source-added vector findable");
    let tgt_hit = p
        .vector
        .search(target_id, "default", "vc", &[0.0, 0.0, 1.0], 1, None)
        .unwrap();
    assert!(
        !tgt_hit.is_empty(),
        "target-added vector search returned no hits"
    );
    assert_eq!(tgt_hit[0].key, "v-target", "target-added vector findable");

    // JSON: secondary index entry for the merged price exists; entry
    // for the pre-merge price does NOT (atomic refresh from Step 1).
    let encoded_25 = jindex::encode_numeric(25.0);
    let encoded_10 = jindex::encode_numeric(10.0);
    let mut hits_25: Vec<String> = Vec::new();
    let mut hits_10: Vec<String> = Vec::new();
    test_db
        .db
        .transaction(target_id, |txn| {
            hits_25 = jindex::lookup_eq(txn, &target_id, "default", "price_idx", &encoded_25)?;
            hits_10 = jindex::lookup_eq(txn, &target_id, "default", "price_idx", &encoded_10)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        hits_25,
        vec!["doc-1".to_string()],
        "JSON index: lookup for merged price=25 must return doc-1"
    );
    assert!(
        hits_10.is_empty(),
        "JSON index: lookup for pre-merge price=10 must be empty; \
         got stale entries: {:?}",
        hits_10
    );

    // KV: shared key resolves source-wins, src-only landed, no spurious
    // keys appeared.
    assert_eq!(
        p.kv.get(&target_id, "default", "shared").unwrap(),
        Some(Value::Int(11))
    );
    assert_eq!(
        p.kv.get(&target_id, "default", "src-only").unwrap(),
        Some(Value::Int(7))
    );
    let post: std::collections::HashSet<String> =
        p.kv.list(&target_id, "default", None)
            .unwrap()
            .into_iter()
            .collect();
    let expected_kv_keys: std::collections::HashSet<String> = ["shared", "src-only"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        post, expected_kv_keys,
        "KV namespace should hold exactly the merged set, no spurious writes"
    );
}

/// Test #5 — every primitive's merged state survives a database
/// restart. Mirrors `cross_primitive_merge_invariant_sweep` but drops
/// and reopens the database AFTER the merge, then re-runs the same
/// invariant checks. This is the crash-rollback test from the design
/// doc: it proves that all merge state — primitive data, secondary
/// indexes, vector backends, BM25 — converges to the correct merged
/// state after recovery.
///
/// The atomic-merge-transaction work in this PR is the precondition
/// for this test passing for JSON. Before, JSON secondary indexes were
/// written by `JsonMergeHandler::post_commit` in a separate transaction
/// after the merge transaction committed; a crash between the two
/// would leave indexes stale and no recovery path would heal them.
/// With secondary index updates inlined into the merge transaction,
/// they survive restart for free via WAL replay.
#[test]
fn cross_primitive_merge_rollback_via_reopen() {
    use strata_engine::primitives::json::index::{self as jindex, IndexType};
    use strata_graph::types::{EdgeData, NodeData};

    // Use strict (always-fsync) durability to guarantee WAL fsync
    // before reopen — mirrors `always_mode_all_primitives_survive_reopen`.
    let mut test_db = TestDb::new_strict();
    let p = test_db.all_primitives();
    let branch_index = test_db.branch_index();

    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // Seed the same baseline state as the invariant-sweep test.
    p.kv.put(&target_id, "default", "shared", Value::Int(0))
        .unwrap();
    p.event
        .append(&target_id, "default", "stream-target-only", int_payload(1))
        .unwrap();
    p.json
        .create(
            &target_id,
            "default",
            "doc-1",
            json_value(serde_json::json!({"price": 10.0, "stock": 5})),
        )
        .unwrap();
    p.json
        .create_index(
            &target_id,
            "default",
            "price_idx",
            "$.price",
            IndexType::Numeric,
        )
        .unwrap();
    p.json
        .set(
            &target_id,
            "default",
            "doc-1",
            &"price".parse().unwrap(),
            json_value(serde_json::json!(10.0)),
        )
        .unwrap();
    p.vector
        .create_collection(target_id, "default", "vc", config_small())
        .unwrap();
    p.vector
        .insert(target_id, "default", "vc", "v-pre", &[1.0, 0.0, 0.0], None)
        .unwrap();
    p.graph
        .create_graph(target_id, "default", "g", None)
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "alice", NodeData::default())
        .unwrap();
    p.graph
        .add_node(target_id, "default", "g", "bob", NodeData::default())
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "target", "source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

    // Mutate every primitive on both sides.
    p.kv.put(&source_id, "default", "shared", Value::Int(11))
        .unwrap();
    p.kv.put(&source_id, "default", "src-only", Value::Int(7))
        .unwrap();
    p.json
        .set(
            &source_id,
            "default",
            "doc-1",
            &"price".parse().unwrap(),
            json_value(serde_json::json!(25.0)),
        )
        .unwrap();
    p.vector
        .insert(
            source_id,
            "default",
            "vc",
            "v-source",
            &[0.0, 1.0, 0.0],
            None,
        )
        .unwrap();
    p.graph
        .add_node(source_id, "default", "g", "carol", NodeData::default())
        .unwrap();
    p.graph
        .add_edge(
            source_id,
            "default",
            "g",
            "alice",
            "carol",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    p.kv.put(&target_id, "default", "shared", Value::Int(22))
        .unwrap();
    p.event
        .append(&target_id, "default", "stream-target-only", int_payload(2))
        .unwrap();
    p.vector
        .insert(
            target_id,
            "default",
            "vc",
            "v-target",
            &[0.0, 0.0, 1.0],
            None,
        )
        .unwrap();
    p.graph
        .add_edge(
            target_id,
            "default",
            "g",
            "alice",
            "bob",
            "follows",
            EdgeData::default(),
        )
        .unwrap();

    branch_ops::merge_branches(
        &test_db.db,
        "source",
        "target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .expect("rollback merge should commit before reopen");

    // ==== Drop and reopen — simulates a clean restart. After Step 1,
    // every primitive's merged state (including JSON secondary indexes)
    // is in WAL by the time `merge_branches` returns. ====
    drop(p); // drop primitive handles before swapping the db
    test_db.reopen();
    let p = test_db.all_primitives();

    // Re-resolve branch IDs after reopen — the names are persisted in
    // the BranchIndex, the underlying BranchId UUIDs are stable.
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

    // ==== Same invariant sweep as test #4, but POST-REOPEN ====

    // Event hash chain still verifies.
    let event_count = verify_event_chain(&p.event, &target_id, "default")
        .expect("event hash chain must verify after reopen");
    assert_eq!(event_count, 2);

    // Graph adjacency invariant holds for every node and edge.
    verify_graph_adjacency(
        &p.graph,
        &target_id,
        "default",
        "g",
        &["alice", "bob", "carol"],
    )
    .expect("graph adjacency must verify after reopen");

    // Vector HNSW backend rebuilt by recovery — every inserted vector
    // findable via k-NN. Assert non-empty before indexing so a recovery
    // failure produces a clear assertion failure instead of an
    // out-of-bounds panic.
    let pre_hit = p
        .vector
        .search(target_id, "default", "vc", &[1.0, 0.0, 0.0], 1, None)
        .unwrap();
    assert!(
        !pre_hit.is_empty(),
        "post-reopen: pre-fork vector search returned no hits — HNSW recovery may be broken"
    );
    assert_eq!(pre_hit[0].key, "v-pre");
    let src_hit = p
        .vector
        .search(target_id, "default", "vc", &[0.0, 1.0, 0.0], 1, None)
        .unwrap();
    assert!(
        !src_hit.is_empty(),
        "post-reopen: source-added vector search returned no hits"
    );
    assert_eq!(src_hit[0].key, "v-source");
    let tgt_hit = p
        .vector
        .search(target_id, "default", "vc", &[0.0, 0.0, 1.0], 1, None)
        .unwrap();
    assert!(
        !tgt_hit.is_empty(),
        "post-reopen: target-added vector search returned no hits"
    );
    assert_eq!(tgt_hit[0].key, "v-target");

    // JSON secondary index — the critical assertion that this PR
    // unlocks. Before Step 1, post_commit's separate transaction
    // could be rolled back by a crash before reopen, leaving the
    // index pointing at the pre-merge field value. After Step 1,
    // the index entries committed atomically with the doc updates
    // and survive WAL replay.
    let encoded_25 = jindex::encode_numeric(25.0);
    let encoded_10 = jindex::encode_numeric(10.0);
    let mut hits_25: Vec<String> = Vec::new();
    let mut hits_10: Vec<String> = Vec::new();
    test_db
        .db
        .transaction(target_id, |txn| {
            hits_25 = jindex::lookup_eq(txn, &target_id, "default", "price_idx", &encoded_25)?;
            hits_10 = jindex::lookup_eq(txn, &target_id, "default", "price_idx", &encoded_10)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        hits_25,
        vec!["doc-1".to_string()],
        "post-reopen: JSON index lookup for merged price=25 must return doc-1; \
         a regression here means JSON secondary index writes are NOT in the merge txn"
    );
    assert!(
        hits_10.is_empty(),
        "post-reopen: JSON index lookup for pre-merge price=10 must be empty; \
         got stale entries: {:?}",
        hits_10
    );

    // KV: merged state intact.
    assert_eq!(
        p.kv.get(&target_id, "default", "shared").unwrap(),
        Some(Value::Int(11))
    );
    assert_eq!(
        p.kv.get(&target_id, "default", "src-only").unwrap(),
        Some(Value::Int(7))
    );
}

// ============================================================================
// Branch DAG end-to-end integration
//
// These tests prove the architectural fix from PR "branch DAG: close engine
// bypass, record revert, add e2e tests": every fork / merge / revert /
// cherry-pick / create / delete operation through the engine fires the DAG
// hook regardless of caller. They go through `TestDb::new()` + the engine's
// `branch_ops::*` API directly (no executor in the loop) and assert on the
// DAG state stored in the `_branch_dag` graph on the `_system_` branch.
//
// Before this fix the DAG writes lived in the executor handler layer
// (`crates/executor/src/handlers/branch.rs`), so engine-direct callers like
// these tests silently bypassed the DAG. After the fix, the engine's
// `branch_ops::*` functions fire registered hooks at the end of every
// successful operation. The hook implementation (`graph::dag_hook_impl`) is
// registered by `tests/common/mod.rs::ensure_test_handlers_registered`.
// ============================================================================

/// Read a node from the branch DAG graph (`_branch_dag` on `_system_`).
///
/// Returns `None` if the DAG node does not exist. Used by every DAG e2e
/// test below to assert that hooks fired and produced the expected nodes.
fn dag_query_node(p: &AllPrimitives, name: &str) -> Option<strata_graph::types::NodeData> {
    use strata_core::branch_dag::SYSTEM_BRANCH;
    let system_id = strata_engine::primitives::branch::resolve_branch_name(SYSTEM_BRANCH);
    p.graph
        .get_node(system_id, "_graph_", "_branch_dag", name)
        .expect("DAG read failed")
}

/// List all node IDs in the branch DAG graph.
fn dag_list_nodes(p: &AllPrimitives) -> Vec<String> {
    use strata_core::branch_dag::SYSTEM_BRANCH;
    let system_id = strata_engine::primitives::branch::resolve_branch_name(SYSTEM_BRANCH);
    p.graph
        .list_nodes(system_id, "_graph_", "_branch_dag")
        .expect("DAG list failed")
}

/// Get outgoing neighbors of a DAG node (used to walk fork/merge/cherry-pick
/// event edges from a branch node to its associated event nodes).
fn dag_outgoing(
    p: &AllPrimitives,
    name: &str,
    edge_type: &str,
) -> Vec<strata_graph::types::Neighbor> {
    use strata_core::branch_dag::SYSTEM_BRANCH;
    use strata_graph::types::Direction;
    let system_id = strata_engine::primitives::branch::resolve_branch_name(SYSTEM_BRANCH);
    p.graph
        .neighbors(
            system_id,
            "_graph_",
            "_branch_dag",
            name,
            Direction::Outgoing,
            Some(edge_type),
        )
        .expect("DAG neighbors read failed")
}

#[test]
fn dag_records_fork_with_version() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();
    let p = test_db.all_primitives();

    branch_index.create_branch("dag_fork_src").unwrap();
    let src_id = strata_engine::primitives::branch::resolve_branch_name("dag_fork_src");
    // Storage needs at least one write to register the branch for forking.
    kv.put(&src_id, "default", "seed", Value::Int(1)).unwrap();
    branch_ops::fork_branch(&test_db.db, "dag_fork_src", "dag_fork_dst").unwrap();

    // Both branch nodes should now exist in the DAG.
    assert!(
        dag_query_node(&p, "dag_fork_src").is_some(),
        "source branch node missing from DAG after fork"
    );
    assert!(
        dag_query_node(&p, "dag_fork_dst").is_some(),
        "destination branch node missing from DAG after fork"
    );

    // The fork event node hangs off the source via a `parent` edge.
    let fork_events = dag_outgoing(&p, "dag_fork_src", "parent");
    assert_eq!(
        fork_events.len(),
        1,
        "expected exactly one fork event from dag_fork_src, got {}",
        fork_events.len()
    );
    let event_id = &fork_events[0].node_id;

    // Verify the event is a fork event with a populated fork_version.
    let event = dag_query_node(&p, event_id).expect("fork event node missing");
    assert_eq!(event.object_type.as_deref(), Some("fork"));
    let props = event.properties.expect("fork event has no properties");
    let fv = props
        .get("fork_version")
        .and_then(|v| v.as_u64())
        .expect("fork event missing fork_version");
    assert!(fv > 0, "fork_version should be > 0, got {fv}");

    // The fork event must have an outgoing `child` edge pointing at the
    // destination — without this assertion the test would pass even if the
    // event was created but the child link was wrong or missing.
    let children = dag_outgoing(&p, event_id, "child");
    assert_eq!(
        children.len(),
        1,
        "fork event should have exactly one child edge"
    );
    assert_eq!(
        children[0].node_id, "dag_fork_dst",
        "fork event's child edge must point at the destination branch"
    );
}

#[test]
fn dag_records_merge_with_version_and_keys() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();
    let p = test_db.all_primitives();

    branch_index.create_branch("dag_merge_target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("dag_merge_target");
    kv.put(&target_id, "default", "shared", Value::Int(1))
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "dag_merge_target", "dag_merge_source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("dag_merge_source");

    // Diverge: source updates "shared" and adds a new key.
    kv.put(&source_id, "default", "shared", Value::Int(2))
        .unwrap();
    kv.put(&source_id, "default", "new_key", Value::Int(99))
        .unwrap();

    let merge_info = branch_ops::merge_branches(
        &test_db.db,
        "dag_merge_source",
        "dag_merge_target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .unwrap();
    assert!(
        merge_info.keys_applied >= 2,
        "merge should apply at least 2 keys"
    );

    // The merge event hangs off the source via a `source` edge.
    let merge_events = dag_outgoing(&p, "dag_merge_source", "source");
    assert_eq!(merge_events.len(), 1, "expected one merge event");
    let event_id = &merge_events[0].node_id;

    let event = dag_query_node(&p, event_id).expect("merge event node missing");
    assert_eq!(event.object_type.as_deref(), Some("merge"));
    let props = event.properties.expect("merge event has no properties");
    assert_eq!(
        props.get("strategy").and_then(|v| v.as_str()),
        Some("last_writer_wins"),
        "merge strategy should be recorded as last_writer_wins"
    );
    let keys = props
        .get("keys_applied")
        .and_then(|v| v.as_u64())
        .expect("merge event missing keys_applied");
    assert!(
        keys >= 2,
        "keys_applied should reflect actual keys merged, got {keys}"
    );
    let mv = props
        .get("merge_version")
        .and_then(|v| v.as_u64())
        .expect("merge event missing merge_version");
    assert!(mv > 0, "merge_version should be > 0, got {mv}");

    // The merge event must point at the target via a `target` edge.
    let targets = dag_outgoing(&p, event_id, "target");
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].node_id, "dag_merge_target");
}

#[test]
fn dag_records_revert() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();
    let p = test_db.all_primitives();

    branch_index.create_branch("dag_revert_branch").unwrap();
    let branch_id = strata_engine::primitives::branch::resolve_branch_name("dag_revert_branch");

    // Write a few versions so we have a non-trivial range to revert.
    kv.put(&branch_id, "default", "k1", Value::Int(1)).unwrap();
    let v_before = test_db.db.current_version().as_u64();
    kv.put(&branch_id, "default", "k2", Value::Int(2)).unwrap();
    kv.put(&branch_id, "default", "k1", Value::Int(99)).unwrap();
    let v_after = test_db.db.current_version().as_u64();

    let revert_info = branch_ops::revert_version_range(
        &test_db.db,
        "dag_revert_branch",
        CommitVersion(v_before + 1),
        CommitVersion(v_after),
    )
    .unwrap();
    // The range covers the v2 (k2 add) and v3 (k1 update) writes, so both
    // keys must come back to their pre-range states: k1 → 1, k2 → gone.
    assert_eq!(
        revert_info.keys_reverted, 2,
        "expected exactly 2 keys reverted (k1 restored, k2 deleted), got {}",
        revert_info.keys_reverted
    );
    assert!(
        revert_info.revert_version.is_some(),
        "revert_version must be set when there are mutations to apply"
    );
    // Spot-check the actual data to prove the revert ran for real (so a
    // bug that records a DAG event without doing the work would be caught).
    assert_eq!(
        kv.get(&branch_id, "default", "k1").unwrap(),
        Some(Value::Int(1)),
        "k1 should be restored to its pre-range value of 1"
    );
    assert_eq!(
        kv.get(&branch_id, "default", "k2").unwrap(),
        None,
        "k2 should be deleted (didn't exist before the range)"
    );

    // The revert event hangs off the branch via the `reverted` edge (single
    // direction — revert is a self-event, not a binary op between branches).
    let revert_events = dag_outgoing(&p, "dag_revert_branch", "reverted");
    assert_eq!(
        revert_events.len(),
        1,
        "expected one revert event in DAG, got {}",
        revert_events.len()
    );
    let event_id = &revert_events[0].node_id;

    let event = dag_query_node(&p, event_id).expect("revert event node missing");
    assert_eq!(event.object_type.as_deref(), Some("revert"));
    let props = event.properties.expect("revert event has no properties");
    assert_eq!(
        props.get("from_version").and_then(|v| v.as_u64()),
        Some(v_before + 1),
        "from_version not recorded correctly"
    );
    assert_eq!(
        props.get("to_version").and_then(|v| v.as_u64()),
        Some(v_after),
        "to_version not recorded correctly"
    );
    assert_eq!(
        props.get("keys_reverted").and_then(|v| v.as_u64()),
        Some(2),
        "keys_reverted on the DAG event must match the engine's RevertInfo"
    );
    assert_eq!(
        props.get("revert_version").and_then(|v| v.as_u64()),
        revert_info.revert_version.map(|v| v.as_u64()),
        "revert_version on the DAG event must match the engine's RevertInfo"
    );
    assert_eq!(
        props.get("branch").and_then(|v| v.as_str()),
        Some("dag_revert_branch"),
        "branch name must be denormalized onto the revert event for cheap lookups"
    );
}

#[test]
fn dag_records_cherry_pick() {
    use strata_engine::branch_ops::CherryPickFilter;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();
    let p = test_db.all_primitives();

    branch_index.create_branch("dag_cp_target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("dag_cp_target");
    kv.put(&target_id, "default", "base", Value::Int(0))
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "dag_cp_target", "dag_cp_source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("dag_cp_source");

    // Source-only divergence: a single new key to cherry-pick.
    kv.put(&source_id, "default", "picked", Value::Int(42))
        .unwrap();

    let cp_info = branch_ops::cherry_pick_from_diff(
        &test_db.db,
        "dag_cp_source",
        "dag_cp_target",
        CherryPickFilter::default(),
        None,
    )
    .unwrap();
    assert_eq!(
        cp_info.keys_applied, 1,
        "should have picked exactly one key"
    );
    // Spot-check actual data to prove the cherry-pick ran (a bug that
    // recorded the DAG event without applying writes would be caught).
    assert_eq!(
        kv.get(&target_id, "default", "picked").unwrap(),
        Some(Value::Int(42)),
        "picked key should be present on target after cherry-pick"
    );

    // Cherry-pick uses its own edge type to be distinguishable from a full merge.
    let cp_events = dag_outgoing(&p, "dag_cp_source", "cherry_pick_source");
    assert_eq!(
        cp_events.len(),
        1,
        "expected one cherry-pick event in DAG, got {}",
        cp_events.len()
    );
    let event_id = &cp_events[0].node_id;

    let event = dag_query_node(&p, event_id).expect("cherry-pick event node missing");
    assert_eq!(event.object_type.as_deref(), Some("cherry_pick"));
    let props = event
        .properties
        .expect("cherry-pick event has no properties");
    assert_eq!(
        props.get("keys_applied").and_then(|v| v.as_u64()),
        Some(1),
        "keys_applied on the DAG event must match the engine's CherryPickInfo"
    );
    assert_eq!(
        props.get("cherry_pick_version").and_then(|v| v.as_u64()),
        cp_info.cherry_pick_version,
        "cherry_pick_version on the DAG event must match the engine's CherryPickInfo"
    );

    // The event points at the target via the cherry_pick_target edge type.
    let targets = dag_outgoing(&p, event_id, "cherry_pick_target");
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].node_id, "dag_cp_target");

    // The fork edge from the prior fork should NOT have produced a merge or
    // cherry-pick event by itself — verify there's exactly one cherry-pick
    // event and no merge events on this branch pair.
    let merge_events_on_source = dag_outgoing(&p, "dag_cp_source", "source");
    assert!(
        merge_events_on_source.is_empty(),
        "cherry-pick should not produce a merge event"
    );
}

#[test]
fn dag_records_branch_create_and_delete() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    branch_index.create_branch("dag_lifecycle").unwrap();

    // After create, branch should exist in DAG with status=active and a
    // populated created_at timestamp.
    let node = dag_query_node(&p, "dag_lifecycle").expect("branch node missing after create");
    assert_eq!(node.object_type.as_deref(), Some("branch"));
    let props = node.properties.expect("branch node has no properties");
    assert_eq!(props.get("status").and_then(|v| v.as_str()), Some("active"));
    let created_at = props
        .get("created_at")
        .and_then(|v| v.as_u64())
        .expect("created_at should be set on the branch node");
    assert!(created_at > 0, "created_at should be a real timestamp");

    branch_index.delete_branch("dag_lifecycle").unwrap();

    // After delete, the same node should still exist (lineage is preserved)
    // but with status flipped to deleted and a deleted_at marker.
    let node = dag_query_node(&p, "dag_lifecycle").expect("branch node missing after delete");
    let props = node.properties.expect("branch node has no properties");
    assert_eq!(
        props.get("status").and_then(|v| v.as_str()),
        Some("deleted"),
        "status should flip to deleted after delete_branch"
    );
    assert!(
        props
            .get("deleted_at")
            .and_then(|v| v.as_u64())
            .filter(|&v| v > 0)
            .is_some(),
        "deleted_at should be set after delete_branch"
    );
    // The branch's original created_at must still be present — we preserve
    // lineage rather than rewriting the node from scratch.
    assert_eq!(
        props.get("created_at").and_then(|v| v.as_u64()),
        Some(created_at),
        "created_at must be preserved across delete (no lineage erasure)"
    );
}

#[test]
fn dag_repeated_merge_advances_merge_base() {
    // After merging source → target, then diverging and merging again, the
    // DAG must contain TWO merge events with monotonically increasing
    // `merge_version` values. This is what powers
    // `compute_merge_base_from_dag` (in the executor merge handler):
    // `find_last_merge_version` returns the most recent merge version,
    // which becomes the merge-base override for the next merge call.
    // Without this property, repeated merges would always re-walk back to
    // the original fork point and re-apply already-merged changes.
    use strata_graph::branch_dag::find_last_merge_version;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();

    branch_index.create_branch("dag_rep_target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("dag_rep_target");
    kv.put(&target_id, "default", "base", Value::Int(0))
        .unwrap();

    branch_ops::fork_branch(&test_db.db, "dag_rep_target", "dag_rep_source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("dag_rep_source");

    // First divergence + merge.
    kv.put(&source_id, "default", "round1", Value::Int(1))
        .unwrap();
    branch_ops::merge_branches(
        &test_db.db,
        "dag_rep_source",
        "dag_rep_target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .unwrap();
    let first_mv = find_last_merge_version(&test_db.db, "dag_rep_source", "dag_rep_target")
        .unwrap()
        .expect("first merge_version should be recorded");

    // Second divergence + merge.
    kv.put(&source_id, "default", "round2", Value::Int(2))
        .unwrap();
    branch_ops::merge_branches(
        &test_db.db,
        "dag_rep_source",
        "dag_rep_target",
        MergeStrategy::LastWriterWins,
        None,
    )
    .unwrap();
    let second_mv = find_last_merge_version(&test_db.db, "dag_rep_source", "dag_rep_target")
        .unwrap()
        .expect("second merge_version should be recorded");

    assert!(
        second_mv > first_mv,
        "merge_base should advance: second={second_mv} should be > first={first_mv}"
    );

    // Defensive: there must be exactly TWO merge events on the source branch
    // — not one (where the second merge somehow overwrote the first) and
    // not three (where some extra event leaked in).
    let p = test_db.all_primitives();
    let merge_events = dag_outgoing(&p, "dag_rep_source", "source");
    assert_eq!(
        merge_events.len(),
        2,
        "two consecutive merges should produce exactly two merge events in the DAG, got {}",
        merge_events.len()
    );
}

#[test]
fn dag_skips_system_branch() {
    use strata_core::branch_dag::SYSTEM_BRANCH;

    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let p = test_db.all_primitives();

    // The DAG must NOT contain a node literally named `_system_`. The hook
    // implementation early-returns for `is_system_branch(name)`, which
    // matters because `init_system_branch` calls
    // `BranchIndex::create_branch(SYSTEM_BRANCH)` BEFORE the `_branch_dag`
    // graph itself exists. Without the guard, every db.open() would log a
    // "failed to record branch creation in DAG" warning.
    assert!(
        dag_query_node(&p, SYSTEM_BRANCH).is_none(),
        "_system_ branch should never appear as a self-referential DAG node"
    );

    // Prove the guard is *selective* — non-system branches DO get recorded.
    // If `is_system_branch` were too broad (e.g. matching everything), this
    // assertion would catch it.
    branch_index.create_branch("dag_guard_check").unwrap();
    assert!(
        dag_query_node(&p, "dag_guard_check").is_some(),
        "non-system branches must still be recorded in the DAG"
    );

    // Defensive sweep: nothing system-named should appear in the DAG. Catches
    // any other code path that might write a `_system*` entry by accident.
    let names = dag_list_nodes(&p);
    for n in &names {
        assert!(
            !strata_core::branch_dag::is_system_branch(n),
            "DAG must not contain any system-branch node, found: {n}"
        );
    }
}

#[test]
fn dag_survives_database_reopen() {
    let mut test_db = TestDb::new();
    {
        let branch_index = test_db.branch_index();
        let kv = test_db.kv();
        branch_index.create_branch("dag_persist").unwrap();
        let persist_id = strata_engine::primitives::branch::resolve_branch_name("dag_persist");
        // Storage requires at least one write to register the branch for forking.
        kv.put(&persist_id, "default", "seed", Value::Int(1))
            .unwrap();
        branch_ops::fork_branch(&test_db.db, "dag_persist", "dag_persist_fork").unwrap();
    }

    // Reopen — DAG state lives on the durable `_system_` branch and must
    // survive WAL replay.
    test_db.reopen();
    let p = test_db.all_primitives();

    assert!(
        dag_query_node(&p, "dag_persist").is_some(),
        "branch node should survive reopen"
    );
    assert!(
        dag_query_node(&p, "dag_persist_fork").is_some(),
        "forked branch node should survive reopen"
    );

    // Fork event must also survive.
    let fork_events = dag_outgoing(&p, "dag_persist", "parent");
    assert_eq!(
        fork_events.len(),
        1,
        "fork event should survive reopen, got {} events",
        fork_events.len()
    );

    // The fork event's properties (fork_version, etc.) must survive too —
    // not just the node's existence. A bug that wrote the node header but
    // dropped properties on WAL replay would be caught here.
    let event_id = &fork_events[0].node_id;
    let event = dag_query_node(&p, event_id).expect("fork event missing after reopen");
    let props = event
        .properties
        .expect("fork event lost properties on reopen");
    assert!(
        props
            .get("fork_version")
            .and_then(|v| v.as_u64())
            .filter(|&v| v > 0)
            .is_some(),
        "fork_version must be preserved across reopen"
    );

    // The fork event's child edge must also survive.
    let children = dag_outgoing(&p, event_id, "child");
    assert_eq!(children.len(), 1, "fork child edge missing after reopen");
    assert_eq!(children[0].node_id, "dag_persist_fork");
}

#[test]
fn dag_records_fork_metadata_end_to_end() {
    // Verify that the message/creator parameters survive the entire chain:
    // executor → engine `_with_metadata` variant → hook → `dag_record_fork`
    // → DAG event node properties. A refactor that drops the parameters
    // anywhere along the chain would silently lose audit metadata; this
    // test catches that.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();
    let p = test_db.all_primitives();

    branch_index.create_branch("dag_meta_src").unwrap();
    let src_id = strata_engine::primitives::branch::resolve_branch_name("dag_meta_src");
    kv.put(&src_id, "default", "seed", Value::Int(1)).unwrap();

    strata_engine::branch_ops::fork_branch_with_metadata(
        &test_db.db,
        "dag_meta_src",
        "dag_meta_dst",
        Some("releasing v1.2"),
        Some("alice@example.com"),
    )
    .unwrap();

    let fork_events = dag_outgoing(&p, "dag_meta_src", "parent");
    assert_eq!(fork_events.len(), 1);
    let event = dag_query_node(&p, &fork_events[0].node_id).unwrap();
    let props = event.properties.unwrap();
    assert_eq!(
        props.get("message").and_then(|v| v.as_str()),
        Some("releasing v1.2"),
        "fork message must flow from caller through to DAG event"
    );
    assert_eq!(
        props.get("creator").and_then(|v| v.as_str()),
        Some("alice@example.com"),
        "fork creator must flow from caller through to DAG event"
    );
}

#[test]
fn dag_records_merge_metadata_end_to_end() {
    // Mirror of `dag_records_fork_metadata_end_to_end` for merge.
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let kv = test_db.kv();
    let p = test_db.all_primitives();

    branch_index.create_branch("dag_meta_target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("dag_meta_target");
    kv.put(&target_id, "default", "base", Value::Int(0))
        .unwrap();
    branch_ops::fork_branch(&test_db.db, "dag_meta_target", "dag_meta_source").unwrap();
    let source_id = strata_engine::primitives::branch::resolve_branch_name("dag_meta_source");
    kv.put(&source_id, "default", "new", Value::Int(1)).unwrap();

    strata_engine::branch_ops::merge_branches_with_metadata(
        &test_db.db,
        "dag_meta_source",
        "dag_meta_target",
        MergeStrategy::LastWriterWins,
        None,
        Some("hotfix release"),
        Some("bob@example.com"),
    )
    .unwrap();

    let merge_events = dag_outgoing(&p, "dag_meta_source", "source");
    assert_eq!(merge_events.len(), 1);
    let event = dag_query_node(&p, &merge_events[0].node_id).unwrap();
    let props = event.properties.unwrap();
    assert_eq!(
        props.get("message").and_then(|v| v.as_str()),
        Some("hotfix release"),
        "merge message must flow from caller through to DAG event"
    );
    assert_eq!(
        props.get("creator").and_then(|v| v.as_str()),
        Some("bob@example.com"),
        "merge creator must flow from caller through to DAG event"
    );
}
