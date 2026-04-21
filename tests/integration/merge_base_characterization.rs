//! Characterization test locking the current externally-observable
//! merge-base contract, per epic B1 of the branching execution plan
//! (`docs/design/branching/branching-execution-plan.md`).
//!
//! ## Finding from B1 exploration
//!
//! The branching execution plan describes two parallel merge-base paths:
//! the canonical executor path (`db.branches().merge`) and a bare-engine
//! path (`branch_ops::merge_branches`). In the current code, the bare-engine
//! path is **not externally reachable** — `branch_ops` is a private module
//! inside `strata_engine` (`crates/engine/src/lib.rs:61`), and only types
//! like `MergeInfo` / `MergeStrategy` are re-exported. `merge_branches` and
//! `compute_merge_base` themselves are reachable only from within the
//! engine crate.
//!
//! That means at the *external* API surface, B2's "one canonical path" is
//! already true. The duplication that B2 will collapse is the
//! `merge_base_override` parameter that flows from
//! `BranchService::merge_with_options(MergeOptions { merge_base: Some(_) })`
//! down into `branch_ops::merge_branches_with_metadata`. This test locks
//! today's behavior on every externally observable axis, and locks the
//! override-takes-priority semantics through the public surface so B3 can
//! prove its removal preserves equivalent behavior on every public path.
//!
//! Internal `branch_ops::merge_branches` characterization already exists
//! as inline `#[cfg(test)]` cases in `crates/engine/src/branch_ops/mod.rs`
//! (search for `merge_branches(&db, ...)`).
//!
//! ## Scenarios covered
//!
//! - **A1**: parent forks child, child→parent merge succeeds with locked
//!   `keys_applied`/`conflicts`/`merge_version` shape.
//! - **A2**: same fork, parent→child merge with one parent-side post-fork
//!   write succeeds and applies the parent's diff to the child.
//! - **B**: unrelated branches refuse merge with the locked error
//!   vocabulary.
//! - **D**: `db.branches().merge_base()` returns a DAG-derived merge base
//!   for fresh forks and a negative result (None or Err) for unrelated
//!   branches.
//! - **E**: `MergeOptions::merge_base` override is honored by
//!   `BranchService::merge_with_options` even between storage-unrelated
//!   branches — the contract B3 will remove.

use crate::common::*;
use strata_engine::{MergeOptions, MergeStrategy};

// =============================================================================
// Helpers
// =============================================================================

/// Resolve a branch name to the engine's deterministic `BranchId`.
fn resolve(name: &str) -> BranchId {
    strata_engine::primitives::branch::resolve_branch_name(name)
}

/// Set up a parent → child fork: creates `parent`, writes `parent_key`,
/// forks to `child`, writes `child_key` to child only. Returns the
/// `fork_version` observed at fork time.
fn setup_parent_child_fork(test_db: &TestDb, parent: &str, child: &str) -> u64 {
    let branches = test_db.db.branches();
    let kv = test_db.kv();
    let parent_id = resolve(parent);

    branches.create(parent).unwrap();
    kv.put(&parent_id, "default", "parent_key", Value::Int(1))
        .unwrap();

    let info = branches.fork(parent, child).unwrap();
    let fork_version = info
        .fork_version
        .expect("fork must report a fork_version for COW relationship");

    let child_id = resolve(child);
    kv.put(&child_id, "default", "child_key", Value::Int(2))
        .unwrap();

    fork_version
}

// =============================================================================
// Scenario A1: child → parent merge across a fresh fork
// =============================================================================

/// Scenario A1: parent forks child, child writes new keys, child merges
/// back into parent. Locks the `keys_applied` shape and the
/// no-conflict / merge_version-present invariants.
#[test]
fn a1_child_to_parent_merge_via_branchservice() {
    let test_db = TestDb::new();
    let _fork_version = setup_parent_child_fork(&test_db, "parent_a1", "child_a1");

    let info = test_db
        .db
        .branches()
        .merge("child_a1", "parent_a1")
        .expect("BranchService merge of fresh fork must succeed");

    assert_eq!(info.source, "child_a1");
    assert_eq!(info.target, "parent_a1");
    // Locked: only `child_key` is new on child since fork. `parent_key`
    // existed at fork base and is unchanged on both sides → not re-applied.
    assert_eq!(
        info.keys_applied, 1,
        "exactly child_key should apply; saw keys_applied={}",
        info.keys_applied
    );
    assert_eq!(info.keys_deleted, 0);
    assert_eq!(info.conflicts.len(), 0);
    assert_eq!(info.spaces_merged, 1);
    assert!(
        info.merge_version.is_some(),
        "successful merge must report a merge_version"
    );
}

// =============================================================================
// Scenario A2: parent → child merge across a fresh fork (reverse direction)
// =============================================================================

/// Scenario A2: parent forks child, parent writes a new key after the fork,
/// parent merges into child. Locks the parent-post-fork → child propagation.
#[test]
fn a2_parent_to_child_merge_with_parent_post_fork_write() {
    let test_db = TestDb::new();
    let _fork_version = setup_parent_child_fork(&test_db, "parent_a2", "child_a2");

    test_db
        .kv()
        .put(
            &resolve("parent_a2"),
            "default",
            "parent_post_fork",
            Value::Int(99),
        )
        .unwrap();

    let info = test_db
        .db
        .branches()
        .merge("parent_a2", "child_a2")
        .expect("parent→child merge of fresh fork must succeed");

    assert_eq!(info.source, "parent_a2");
    assert_eq!(info.target, "child_a2");
    // Locked: only `parent_post_fork` is new on parent since fork.
    // `parent_key` existed at fork base; `child_key` is on child only and
    // does not flow source→target.
    assert_eq!(
        info.keys_applied, 1,
        "exactly parent_post_fork should apply; saw keys_applied={}",
        info.keys_applied
    );
    assert_eq!(info.keys_deleted, 0);
    assert_eq!(info.conflicts.len(), 0);
    assert_eq!(info.spaces_merged, 1);
}

// =============================================================================
// Scenario B: unrelated branches (no fork or merge relationship)
// =============================================================================

/// Scenario B: unrelated branches refuse merge with a "no fork or merge
/// relationship" `StrataError`. Locks today's external error vocabulary.
#[test]
fn b_unrelated_branches_refuse_merge() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();

    branches.create("alpha").unwrap();
    branches.create("beta").unwrap();

    let result = branches.merge("alpha", "beta");
    assert!(
        result.is_err(),
        "unrelated branches MUST refuse merge via BranchService"
    );
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.contains("no fork or merge relationship") || msg.contains("merge base"),
        "expected merge-base-related error; got: {msg}"
    );
}

// =============================================================================
// Scenario D: BranchService::merge_base lookup contract
// =============================================================================

/// `db.branches().merge_base()` on a freshly-forked pair returns Some(_)
/// at the fork version. Locks today's DAG-derived merge-base contract.
#[test]
fn d_branchservice_merge_base_returns_some_for_fresh_fork() {
    let test_db = TestDb::new();
    let fork_version = setup_parent_child_fork(&test_db, "parent_d", "child_d");

    let result = test_db
        .db
        .branches()
        .merge_base("parent_d", "child_d")
        .expect("merge_base lookup MUST not error for fresh fork");

    let mb = result.expect("fresh fork MUST have a DAG-derived merge base");

    // Locked: today the DAG records the fork point at the parent's name.
    // B2 may move this around; if so, update this assertion intentionally
    // and document the change in the per-epic acceptance template.
    assert_eq!(
        mb.branch_name, "parent_d",
        "merge_base branch_name MUST be the parent (the DAG anchor) for fresh forks; got {}",
        mb.branch_name
    );
    assert_eq!(
        mb.commit_version.0, fork_version,
        "merge_base version MUST equal the fork version"
    );
    assert_eq!(
        mb.branch_id,
        resolve("parent_d"),
        "merge_base branch_id MUST resolve to the same UUID as branch_name"
    );
}

/// `db.branches().merge_base()` on unrelated branches reports a negative
/// result (Ok(None) or Err). Both are acceptable today; B2 will normalize.
#[test]
fn d_branchservice_merge_base_negative_for_unrelated_branches() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();
    branches.create("alpha_d").unwrap();
    branches.create("beta_d").unwrap();

    let result = branches.merge_base("alpha_d", "beta_d");
    // Both shapes are acceptable today (B2 will normalize):
    //   Ok(None) — explicit "no merge base"
    //   Err(_)   — DAG raises an error for no path
    // Only Ok(Some(_)) is wrong.
    if let Ok(Some(mb)) = result {
        panic!(
            "unrelated branches MUST NOT report a merge base; got {} @ {:?}",
            mb.branch_name, mb.commit_version
        );
    }
}

// =============================================================================
// Scenario E: MergeOptions.merge_base override is honored
// =============================================================================

/// `BranchService::merge_with_options` honors a caller-supplied
/// `merge_base` override even between storage-unrelated branches.
/// This is the externally-visible form of the
/// `branch_ops::merge_base_override` plumbing that B3 will remove from
/// the engine surface entirely.
#[test]
fn e_branchservice_merge_with_options_honors_override() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();
    branches.create("alpha_e").unwrap();
    branches.create("beta_e").unwrap();
    let alpha_id = resolve("alpha_e");
    test_db
        .kv()
        .put(&alpha_id, "default", "k", Value::Int(1))
        .unwrap();

    // Without override: refused (Scenario B locks this).
    assert!(branches.merge("alpha_e", "beta_e").is_err());

    // With explicit override: merge proceeds. This is the contract B3 removes.
    let opts =
        MergeOptions::with_strategy(MergeStrategy::LastWriterWins).with_merge_base(alpha_id, 0);
    let result = branches.merge_with_options("alpha_e", "beta_e", opts);
    assert!(
        result.is_ok(),
        "BranchService merge_with_options MUST honor merge_base override; got {result:?}"
    );
    let info = result.unwrap();
    assert_eq!(info.source, "alpha_e");
    assert_eq!(info.target, "beta_e");
    assert_eq!(info.conflicts.len(), 0);
    // Locked: the synthetic ancestor at version 0 sees an empty alpha,
    // so alpha's single key `k` flows to beta as a new application.
    assert_eq!(
        info.keys_applied, 1,
        "alpha's key `k` must apply to beta; saw keys_applied={}",
        info.keys_applied
    );
    assert!(info.merge_version.is_some());
}

// =============================================================================
// Scenario F: MergeStrategy::Strict refuses when conflicts exist
// =============================================================================

/// `MergeStrategy::Strict` errors instead of silently picking a winner when
/// both sides changed the same key. Locks today's strict-conflict refusal so
/// B5's rename to `ConflictPolicy::Strict` cannot accidentally change the
/// behavior — only the name.
#[test]
fn f_strict_strategy_refuses_on_conflict() {
    let test_db = TestDb::new();
    let _fork_version = setup_parent_child_fork(&test_db, "parent_f", "child_f");

    // Create a conflict: both sides write the same key with different values.
    test_db
        .kv()
        .put(
            &resolve("parent_f"),
            "default",
            "shared",
            Value::String("from_parent".into()),
        )
        .unwrap();
    test_db
        .kv()
        .put(
            &resolve("child_f"),
            "default",
            "shared",
            Value::String("from_child".into()),
        )
        .unwrap();

    let opts = MergeOptions::with_strategy(MergeStrategy::Strict);
    let result = test_db
        .db
        .branches()
        .merge_with_options("child_f", "parent_f", opts);
    assert!(
        result.is_err(),
        "Strict strategy MUST refuse when conflicts exist; got {result:?}"
    );
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.to_lowercase().contains("conflict"),
        "Strict refusal error MUST mention 'conflict'; got: {msg}"
    );
}

/// `MergeStrategy::Strict` succeeds when there are no conflicts (only
/// disjoint changes). Locks the "Strict ≠ no-op" contract.
#[test]
fn f_strict_strategy_succeeds_when_no_conflicts() {
    let test_db = TestDb::new();
    let _fork_version = setup_parent_child_fork(&test_db, "parent_f2", "child_f2");
    // setup_parent_child_fork wrote `parent_key` only on parent (pre-fork)
    // and `child_key` only on child (post-fork). No shared mutation =
    // no conflict.

    let opts = MergeOptions::with_strategy(MergeStrategy::Strict);
    let info = test_db
        .db
        .branches()
        .merge_with_options("child_f2", "parent_f2", opts)
        .expect("Strict merge with no conflicts MUST succeed");

    assert_eq!(info.conflicts.len(), 0);
    assert_eq!(
        info.keys_applied, 1,
        "Strict merge applies the same single new key as LWW would"
    );
    assert!(info.merge_version.is_some());
}
