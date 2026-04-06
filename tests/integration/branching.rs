//! Branching and Branch Isolation Tests
//!
//! Tests branch isolation guarantees, branch management operations,
//! and branch operations (fork, diff, merge).

use crate::common::*;
use std::sync::{Arc, Barrier};
use std::thread;
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
// Event Merge Safety (Phase 1 of primitive-aware merge)
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
fn event_merge_cross_space_divergence_succeeds() {
    let test_db = TestDb::new();
    let branch_index = test_db.branch_index();
    let space_index = SpaceIndex::new(test_db.db.clone());
    let event = test_db.event();

    // Seed both spaces on target before fork. Non-default spaces must be
    // registered explicitly so `merge_branches` iterates them via
    // `SpaceIndex::list` — `EventLog::append` does not auto-register.
    branch_index.create_branch("target").unwrap();
    let target_id = strata_engine::primitives::branch::resolve_branch_name("target");
    space_index.register(target_id, "orders").unwrap();
    space_index.register(target_id, "users").unwrap();
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
// Branch Isolation Stress Test
// ============================================================================

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
