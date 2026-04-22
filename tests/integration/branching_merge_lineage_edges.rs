//! B3.3 — lineage edges and store-authoritative merge base.
//!
//! Exercises the cutover landed in B3.3: merge / revert / cherry-pick
//! persist `LineageEdgeRecord`s on the control store, `merge_base`
//! reads from the store with AD8 point semantics, and
//! `MergeOptions::merge_base` is removed from the public surface.
//!
//! Invariants covered (from `docs/design/branching/b3-phasing-plan.md`):
//!
//! - Fork → store-derived merge base is the fork anchor point.
//! - Merge advancement: after each source → target merge, the merge
//!   base advances to `(source_ref, merge_version)` — repeated merges
//!   move forward, they do not snap back to the fork anchor.
//! - Same-lifecycle repeat: merging the same generation twice returns
//!   the *more recent* merge point (point semantics, not bare
//!   `BranchRef`).
//! - Generation wall: after delete + recreate, a merge from the new
//!   generation does not inherit the tombstoned generation's merge
//!   points — lineage is scoped by `BranchRef`, not by name.
//! - Unrelated branches return `None` — no fallback, no fabricated base.

#![cfg(not(miri))]

use crate::common::*;
use strata_core::value::Value;
use strata_engine::{MergeOptions, MergeStrategy};

fn resolve(name: &str) -> BranchId {
    BranchId::from_user_name(name)
}

/// Create `parent`, write an initial KV on it, fork to `child`, write
/// one extra KV on the child. Returns the fork version.
fn setup_fork(test_db: &TestDb, parent: &str, child: &str) -> u64 {
    let branches = test_db.db.branches();
    let kv = test_db.kv();
    let parent_id = resolve(parent);

    branches.create(parent).unwrap();
    kv.put(&parent_id, "default", "parent_seed", Value::Int(1))
        .unwrap();

    let info = branches.fork(parent, child).unwrap();
    let fork_version = info
        .fork_version
        .expect("fork must report fork_version for COW relationship");

    let child_id = resolve(child);
    kv.put(&child_id, "default", "child_seed", Value::Int(2))
        .unwrap();

    fork_version
}

// =============================================================================
// Fork only: merge_base returns the fork anchor
// =============================================================================

#[test]
fn merge_base_after_fork_is_fork_anchor() {
    let test_db = TestDb::new();
    let fork_version = setup_fork(&test_db, "parent", "feature");

    let mb = test_db
        .db
        .branches()
        .merge_base("parent", "feature")
        .expect("store-backed merge_base MUST not error for fresh fork")
        .expect("fresh fork MUST report a merge base");

    assert_eq!(mb.branch_name, "parent");
    assert_eq!(
        mb.commit_version.0, fork_version,
        "fresh fork's merge base MUST equal the fork version"
    );
}

// =============================================================================
// Single merge: merge_base advances past the fork anchor
// =============================================================================

#[test]
fn merge_base_advances_after_first_merge() {
    let test_db = TestDb::new();
    let fork_version = setup_fork(&test_db, "parent_s", "feature_s");

    // Merge feature_s → parent_s. MergeInfo.merge_version is the commit
    // version at which the merge landed on parent_s.
    let info = test_db
        .db
        .branches()
        .merge("feature_s", "parent_s")
        .expect("merge of fresh fork must succeed");
    let merge_version = info
        .merge_version
        .expect("non-empty merge MUST report merge_version");
    assert!(merge_version > fork_version);

    let mb = test_db
        .db
        .branches()
        .merge_base("feature_s", "parent_s")
        .unwrap()
        .expect("post-merge merge_base MUST be Some");

    // After feature_s → parent_s, parent_s has incorporated feature_s's
    // state at merge_version. The new merge base is feature_s at
    // merge_version — strictly advanced past the fork anchor.
    assert_eq!(
        mb.branch_name, "feature_s",
        "post-merge base MUST be the source branch, not the fork parent"
    );
    assert_eq!(
        mb.commit_version.0, merge_version,
        "post-merge base MUST equal the merge commit version"
    );
    assert!(
        mb.commit_version.0 > fork_version,
        "post-merge base MUST advance past the fork anchor"
    );
}

// =============================================================================
// Repeated merge: merge_base advances on each merge (point semantics)
// =============================================================================

#[test]
fn merge_base_advances_on_repeated_merge() {
    let test_db = TestDb::new();
    let _fork_version = setup_fork(&test_db, "parent_r", "feature_r");
    let feature_id = resolve("feature_r");
    let kv = test_db.kv();

    // First merge.
    let first = test_db
        .db
        .branches()
        .merge("feature_r", "parent_r")
        .expect("first merge must succeed");
    let first_version = first.merge_version.expect("first merge_version");

    // Write again on feature_r so the second merge is non-empty.
    kv.put(&feature_id, "default", "post_first", Value::Int(42))
        .unwrap();

    let second = test_db
        .db
        .branches()
        .merge("feature_r", "parent_r")
        .expect("second merge must succeed");
    let second_version = second.merge_version.expect("second merge_version");
    assert!(
        second_version > first_version,
        "second merge MUST commit at a later version than the first"
    );

    let mb = test_db
        .db
        .branches()
        .merge_base("feature_r", "parent_r")
        .unwrap()
        .expect("merge_base MUST be Some after two merges");

    // Point semantics: the store keeps each merge as its own point, so
    // `find_merge_base` returns the most recent shared (branch, version)
    // pair — second_version, not first_version.
    assert_eq!(
        mb.commit_version.0, second_version,
        "repeated-merge MUST return the most recent merge commit version"
    );
    assert_eq!(mb.branch_name, "feature_r");
}

// =============================================================================
// Generation wall: delete + recreate does not inherit tombstoned lineage
// =============================================================================

#[test]
fn merge_base_does_not_cross_generations_after_recreate() {
    let test_db = TestDb::new();
    let fork_version_gen0 = setup_fork(&test_db, "parent_g", "feature_g");

    // Merge once on the gen-0 lifecycle so there's a stale edge on
    // parent_g whose source ref carries generation 0.
    let gen0_merge = test_db
        .db
        .branches()
        .merge("feature_g", "parent_g")
        .unwrap();
    let _gen0_merge_version = gen0_merge.merge_version.unwrap();

    // Delete + recreate feature_g → its new `BranchRef` has generation 1,
    // which is a distinct lifecycle instance from the tombstoned gen 0.
    test_db.db.branches().delete("feature_g").unwrap();
    test_db.db.branches().create("feature_g").unwrap();
    let new_rec = test_db
        .db
        .branches()
        .control_record("feature_g")
        .unwrap()
        .unwrap();
    assert_eq!(
        new_rec.branch.generation, 1,
        "recreate MUST bump generation"
    );

    // Note: feature_g@gen1 is created without forking from parent_g, so
    // there is no shared lineage between feature_g@gen1 and parent_g.
    // The find_merge_base over the new BranchRef must return None —
    // the old gen-0 edges belong to a different lifecycle instance and
    // MUST NOT be surfaced for the new one.
    let mb = test_db
        .db
        .branches()
        .merge_base("feature_g", "parent_g")
        .unwrap();
    assert!(
        mb.is_none(),
        "recreated feature_g@gen1 is unrelated to parent_g; got {mb:?}"
    );

    // Parent's original fork anchor for the gen-0 child stays valid and
    // recoverable via `get_record` of the tombstoned `BranchRef`. We
    // don't assert this here — Scenario A covers the happy path for
    // gen-0 lineage before recreate.
    let _ = fork_version_gen0;
}

// =============================================================================
// Revert then merge: revert edge present but does not advance merge base
// =============================================================================

#[test]
fn revert_edge_does_not_shift_merge_base() {
    let test_db = TestDb::new();
    let _fork_version = setup_fork(&test_db, "parent_v", "feature_v");
    let feature_id = resolve("feature_v");
    let kv = test_db.kv();

    // Write once more on feature_v so we have a commit to revert.
    kv.put(&feature_id, "default", "to_revert", Value::Int(1))
        .unwrap();
    let version_before_revert = test_db.db.current_version();
    kv.put(&feature_id, "default", "post_revert", Value::Int(2))
        .unwrap();
    let version_after = test_db.db.current_version();
    assert!(version_after > version_before_revert);

    // Revert the mid-range. Revert writes a `Revert` edge on feature_v
    // but is a target-only rewind — it has no source and MUST NOT shift
    // the merge base between feature_v and parent_v.
    test_db
        .db
        .branches()
        .revert(
            "feature_v",
            strata_core::id::CommitVersion(version_before_revert.0 + 1),
            version_after,
        )
        .expect("revert must succeed");

    let post_revert = test_db
        .db
        .branches()
        .merge("feature_v", "parent_v")
        .expect("merge after revert must succeed");
    let post_merge_version = post_revert.merge_version.unwrap();

    let mb = test_db
        .db
        .branches()
        .merge_base("feature_v", "parent_v")
        .unwrap()
        .unwrap();
    assert_eq!(
        mb.commit_version.0, post_merge_version,
        "merge base MUST reflect the post-revert merge, not the revert itself"
    );
    assert_eq!(mb.branch_name, "feature_v");
}

// =============================================================================
// Cherry-pick edge present but does not advance merge_base
// =============================================================================

/// Cherry-pick applies a selective subset of a source's state; unlike a
/// full merge, the target has not fully incorporated the source's
/// history at the cherry-pick commit. `ancestor_chain` skips
/// `LineageEdgeKind::CherryPick` for merge-base advancement, so a
/// cherry-pick from source → target does NOT shift merge_base to the
/// cherry-pick commit version. This test locks that behavior so a
/// future refactor that treats cherry-pick as a "weak merge" can't
/// silently change merge-base semantics.
#[test]
fn cherry_pick_edge_does_not_shift_merge_base() {
    let test_db = TestDb::new();
    let fork_version = setup_fork(&test_db, "parent_cp", "feature_cp");
    let feature_id = resolve("feature_cp");
    let kv = test_db.kv();

    // Add a key on feature that cherry-pick will ship over to parent.
    kv.put(&feature_id, "default", "pick_me", Value::Int(7))
        .unwrap();

    // Cherry-pick feature_cp → parent_cp. Writes a CherryPick edge but
    // does not fully "merge" source into target.
    // `cherry_pick` keys are `(space, key)` pairs.
    let cp_info = test_db
        .db
        .branches()
        .cherry_pick(
            "feature_cp",
            "parent_cp",
            &[("default".into(), "pick_me".into())],
        )
        .expect("cherry-pick must succeed");
    assert_eq!(
        cp_info.keys_applied, 1,
        "exactly one key should have been picked"
    );

    let mb = test_db
        .db
        .branches()
        .merge_base("feature_cp", "parent_cp")
        .unwrap()
        .expect("merge_base MUST still report the fork anchor, not the cherry-pick commit");

    // Merge base stays at the fork anchor — cherry-pick edges are
    // skipped in ancestor-chain traversal. This is distinct from the
    // merge-advancement test; even with a later commit on parent via
    // cherry-pick, the shared-visibility point is still the fork.
    assert_eq!(
        mb.branch_name, "parent_cp",
        "cherry-pick MUST NOT flip the merge base to the cherry-pick source"
    );
    assert_eq!(
        mb.commit_version.0, fork_version,
        "cherry-pick MUST NOT advance merge base past the fork anchor"
    );
}

// =============================================================================
// Unrelated branches: no fallback, returns None
// =============================================================================

#[test]
fn unrelated_branches_return_none_for_merge_base() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();
    branches.create("alpha").unwrap();
    branches.create("beta").unwrap();

    let mb = branches.merge_base("alpha", "beta").unwrap();
    assert!(
        mb.is_none(),
        "unrelated branches MUST return None — no DAG fallback, no storage fallback"
    );

    let err = branches
        .merge_with_options(
            "alpha",
            "beta",
            MergeOptions::with_strategy(MergeStrategy::LastWriterWins),
        )
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("no fork or merge relationship found"),
        "unrelated merge MUST refuse with the locked error vocabulary; got: {msg}"
    );
}
