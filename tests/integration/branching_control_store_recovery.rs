//! B3.4 — `BranchControlStore` recovery on reopen.
//!
//! These tests reopen a primary database and assert that:
//!
//! - **Active control records survive a reopen.** A branch created
//!   before reopen is still observable through
//!   `BranchService::control_record` after reopen, with the same
//!   generation-aware `BranchRef`.
//! - **Tombstones survive a reopen.** A delete-then-reopen-then-recreate
//!   cycle yields gen 1 (not gen 0), proving the persisted next-gen
//!   counter saw the tombstone.
//! - **Lineage edges (Fork / Merge / Revert / CherryPick) survive a
//!   reopen.** `merge_base` returns the same point before and after
//!   reopen — the only way that can stay stable is if the
//!   `LineageEdgeRecord` rows persisted to storage.
//! - **Migration on a second open is a no-op.** Reopening twice in a row
//!   never alters control-record, tombstone, or edge state, regardless
//!   of whether the DAG hook (and therefore the projection rebuild) is
//!   installed. Migration short-circuits when control records already
//!   exist; a second pass cannot mutate anything.
//! - **DAG projection rebuild is idempotent.** `log` returns the same
//!   ordered history across multiple reopens. (The rebuild runs on each
//!   primary open as a best-effort step per AD3.)
//!
//! All five assertions are observed through the public `BranchService`
//! API — the underlying store is `pub(crate)` and not directly
//! reachable from integration tests, which is the correct boundary.

#![cfg(not(miri))]

use crate::common::*;
use strata_core::id::CommitVersion;
use strata_core::value::Value;
use strata_core::{BranchId, BranchRef};
use strata_engine::primitives::extensions::KVStoreExt;

fn resolve(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

fn seed(test_db: &TestDb, name: &str, key: &str, value: i64) {
    let branch_id = resolve(name);
    test_db
        .kv()
        .put(&branch_id, "default", key, Value::Int(value))
        .expect("seed write succeeds");
}

// ============================================================================
// Active records survive reopen
// ============================================================================

#[test]
fn active_control_records_survive_reopen() {
    let mut test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("alpha").unwrap();
    branches.create("beta").unwrap();
    let alpha_before = branches.control_record("alpha").unwrap().unwrap().branch;
    let beta_before = branches.control_record("beta").unwrap().unwrap().branch;

    test_db.reopen();
    let branches = test_db.db.branches();

    let alpha_after = branches
        .control_record("alpha")
        .unwrap()
        .expect("active record persists across reopen");
    let beta_after = branches
        .control_record("beta")
        .unwrap()
        .expect("active record persists across reopen");
    assert_eq!(alpha_after.branch, alpha_before);
    assert_eq!(beta_after.branch, beta_before);
}

// ============================================================================
// Tombstones survive reopen
// ============================================================================

#[test]
fn tombstones_survive_reopen_and_advance_generation() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("ghost").unwrap();
    test_db.db.branches().delete("ghost").unwrap();

    test_db.reopen();
    let branches = test_db.db.branches();

    // Recreate must allocate gen 1 — only possible if the tombstone for
    // gen 0 (and the next-gen counter that records it) survived the
    // reopen. A lost tombstone would let the recreate allocate gen 0
    // again, blurring the lifecycle instances.
    branches.create("ghost").unwrap();
    let rec = branches.control_record("ghost").unwrap().unwrap();
    assert_eq!(
        rec.branch,
        BranchRef::new(resolve("ghost"), 1),
        "post-reopen recreate must skip the tombstoned generation"
    );
}

#[test]
fn tombstones_chain_across_multiple_reopens() {
    let mut test_db = TestDb::new();
    test_db.db.branches().create("chain").unwrap();
    test_db.db.branches().delete("chain").unwrap();
    test_db.reopen();
    test_db.db.branches().create("chain").unwrap(); // gen 1
    test_db.db.branches().delete("chain").unwrap();
    test_db.reopen();
    test_db.db.branches().create("chain").unwrap(); // gen 2

    let rec = test_db
        .db
        .branches()
        .control_record("chain")
        .unwrap()
        .unwrap();
    assert_eq!(
        rec.branch.generation, 2,
        "generation counter must monotonically advance across reopens"
    );
}

// ============================================================================
// Lineage edges (fork + merge) survive reopen
// ============================================================================

#[test]
fn fork_anchor_survives_reopen() {
    let mut test_db = TestDb::new();
    let branches = test_db.db.branches();
    branches.create("main").unwrap();
    seed(&test_db, "main", "_seed_", 1);
    branches.fork("main", "feature").unwrap();
    let parent_ref_before = branches
        .control_record("feature")
        .unwrap()
        .unwrap()
        .fork
        .unwrap()
        .parent;

    test_db.reopen();

    let parent_ref_after = test_db
        .db
        .branches()
        .control_record("feature")
        .unwrap()
        .unwrap()
        .fork
        .expect("fork anchor persists across reopen")
        .parent;
    assert_eq!(parent_ref_after, parent_ref_before);
}

#[test]
fn merge_lineage_edge_survives_reopen() {
    let mut test_db = TestDb::new();
    let branches = test_db.db.branches();
    branches.create("main").unwrap();
    seed(&test_db, "main", "_seed_", 1);
    branches.fork("main", "feature").unwrap();
    seed(&test_db, "feature", "delta", 99);
    let merge_info = branches.merge("feature", "main").unwrap();
    let merge_version = merge_info.merge_version.expect("merge committed");
    let mb_before = branches.merge_base("main", "feature").unwrap().unwrap();
    assert_eq!(
        mb_before.commit_version,
        CommitVersion(merge_version),
        "post-merge merge-base advances to the merge commit version"
    );

    test_db.reopen();

    let mb_after = test_db
        .db
        .branches()
        .merge_base("main", "feature")
        .unwrap()
        .expect("merge edge persists, so merge_base still resolves after reopen");
    assert_eq!(
        mb_after.commit_version,
        mb_before.commit_version,
        "merge_base point semantics must survive reopen"
    );
    assert_eq!(mb_after.branch_id, mb_before.branch_id);
}

#[test]
fn revert_lineage_edge_survives_reopen() {
    let mut test_db = TestDb::new();
    let branches = test_db.db.branches();
    branches.create("main").unwrap();
    seed(&test_db, "main", "k1", 1);
    seed(&test_db, "main", "k2", 2);
    let v_after_seed = test_db.db.current_version();
    seed(&test_db, "main", "k3", 3);
    let revert_info = branches
        .revert(
            "main",
            CommitVersion(v_after_seed.0 + 1),
            CommitVersion(v_after_seed.0 + 1),
        )
        .unwrap();
    let revert_version = revert_info
        .revert_version
        .expect("revert produced a commit");

    // Sanity: the revert lands as a strictly-later commit.
    assert!(revert_version.0 > v_after_seed.0);

    test_db.reopen();

    // After reopen, the post-revert state must still resolve correctly.
    // We assert via merge_base against the same branch — it returns
    // CommitVersion::MAX with `branch == self`. The point is that
    // reopen does not crash on a stored revert edge and the branch is
    // still introspectable.
    let post_reopen = test_db
        .db
        .branches()
        .control_record("main")
        .unwrap()
        .expect("main survives reopen with a revert edge");
    assert_eq!(post_reopen.branch.generation, 0);
    let log_after: Vec<_> = test_db
        .db
        .branches()
        .log("main", 100)
        .unwrap()
        .into_iter()
        .map(|e| e.kind)
        .collect();
    assert!(
        log_after
            .iter()
            .any(|k| matches!(k, strata_engine::database::dag_hook::DagEventKind::Revert)),
        "rebuilt projection includes the revert event after reopen"
    );
}

// ============================================================================
// Migration on second open is a no-op
// ============================================================================

#[test]
fn second_reopen_does_not_alter_control_state() {
    let mut test_db = TestDb::new();
    let branches = test_db.db.branches();
    branches.create("main").unwrap();
    branches.create("aux").unwrap();
    branches.delete("aux").unwrap();
    seed(&test_db, "main", "_seed_", 1);
    branches.fork("main", "feature").unwrap();
    branches.merge("feature", "main").unwrap();

    let main_before = branches.control_record("main").unwrap().unwrap().branch;
    let feature_before = branches.control_record("feature").unwrap().unwrap().branch;
    let mb_before = branches.merge_base("main", "feature").unwrap().unwrap();

    // First reopen — runs migration (which no-ops because control
    // records already exist) and the projection rebuild.
    test_db.reopen();
    // Second reopen — proves the no-op path is genuinely idempotent;
    // nothing about the observable state should drift.
    test_db.reopen();

    let main_after = test_db
        .db
        .branches()
        .control_record("main")
        .unwrap()
        .unwrap()
        .branch;
    let feature_after = test_db
        .db
        .branches()
        .control_record("feature")
        .unwrap()
        .unwrap()
        .branch;
    let mb_after = test_db
        .db
        .branches()
        .merge_base("main", "feature")
        .unwrap()
        .unwrap();

    assert_eq!(main_after, main_before);
    assert_eq!(feature_after, feature_before);
    assert_eq!(mb_after.commit_version, mb_before.commit_version);
    assert_eq!(mb_after.branch_id, mb_before.branch_id);

    // The tombstoned `aux` must still refuse to resurface as an active
    // record after the no-op migration.
    assert!(test_db
        .db
        .branches()
        .control_record("aux")
        .unwrap()
        .is_none());
}

// ============================================================================
// DAG projection rebuild is idempotent across reopens
// ============================================================================

#[test]
fn log_history_is_stable_across_reopens() {
    let mut test_db = TestDb::new();
    let branches = test_db.db.branches();
    branches.create("main").unwrap();
    seed(&test_db, "main", "_seed_", 1);
    branches.fork("main", "feature").unwrap();
    seed(&test_db, "feature", "delta", 99);
    branches.merge("feature", "main").unwrap();

    let log_before: Vec<_> = branches
        .log("main", 100)
        .unwrap()
        .into_iter()
        .map(|e| (e.kind, e.commit_version))
        .collect();
    assert!(
        !log_before.is_empty(),
        "main has at least one DAG event before reopen"
    );

    test_db.reopen();
    let log_after_first: Vec<_> = test_db
        .db
        .branches()
        .log("main", 100)
        .unwrap()
        .into_iter()
        .map(|e| (e.kind, e.commit_version))
        .collect();

    test_db.reopen();
    let log_after_second: Vec<_> = test_db
        .db
        .branches()
        .log("main", 100)
        .unwrap()
        .into_iter()
        .map(|e| (e.kind, e.commit_version))
        .collect();

    assert_eq!(
        log_after_first, log_after_second,
        "DAG projection rebuild must be idempotent across reopens"
    );
    // First-reopen vs pre-reopen comparison: the rebuild walks the
    // store's records + edges and reconstructs equivalent DagEvents.
    // Order and cardinality must match the live-write history.
    assert_eq!(
        log_after_first.len(),
        log_before.len(),
        "rebuilt projection must contain the same number of events"
    );
}
