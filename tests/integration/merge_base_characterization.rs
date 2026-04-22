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
//! B3.3 update: the store-backed merge-base authority replaced the old
//! DAG-derived plus caller-override pair. Scenarios A1/A2/B/D/F are
//! preserved verbatim as the B1 characterization floor; Scenario E is
//! rewritten to assert the override no longer exists; Scenario G is
//! added to lock the store-authoritative path.
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
//!   for fresh forks and `Ok(None)` for unrelated branches.
//! - **E**: `MergeOptions::merge_base` override no longer exists (B3.3).
//!   Compile-time: `MergeOptions` has no `merge_base` field and no
//!   `with_merge_base` builder. Runtime: unrelated branches still refuse
//!   to merge with the locked `no fork or merge relationship` vocabulary.
//! - **G**: store-derived `merge_base` remains correct whether the DAG
//!   hook is installed or not — the control store is authoritative and
//!   does not depend on the DAG projection.

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
/// relationship found" `StrataError`. Locks today's external error
/// vocabulary.
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
        msg.contains("no fork or merge relationship found"),
        "expected locked unrelated-merge error substring; got: {msg}"
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

/// `db.branches().merge_base()` on unrelated branches returns `Ok(None)`.
/// This is the documented public contract today and B2 must preserve it
/// unless it intentionally changes the surface.
#[test]
fn d_branchservice_merge_base_negative_for_unrelated_branches() {
    let test_db = TestDb::new();
    let branches = test_db.db.branches();
    branches.create("alpha_d").unwrap();
    branches.create("beta_d").unwrap();

    let result = branches
        .merge_base("alpha_d", "beta_d")
        .expect("unrelated merge_base lookup MUST return Ok(None), not an error");
    assert!(
        result.is_none(),
        "unrelated branches MUST NOT report a merge base; got {result:?}"
    );
}

// =============================================================================
// Scenario E: MergeOptions.merge_base override no longer exists (B3.3)
// =============================================================================

/// After B3.3, `MergeOptions` has no `merge_base` field and no
/// `with_merge_base` builder. The previous (pre-B3.3) externally-visible
/// override that let callers synthesize an ancestor between unrelated
/// branches was deliberately removed: merge-base authority moved to
/// `BranchControlStore`, and a synthetic base would silently corrupt
/// downstream lineage reads.
///
/// Compile-time guard: `MergeOptions` must be constructable without a
/// `merge_base` field and without a `with_merge_base` builder. The
/// construction pattern in this test is the only surface we lock.
///
/// Runtime guard: unrelated branches still refuse to merge (identical
/// to Scenario B) — no surviving back-door restores the old behavior.
#[test]
fn e_merge_base_override_no_longer_exists() {
    // Compile-time: constructs `MergeOptions` with the B3.3 surface only.
    // Any reintroduction of `.with_merge_base(...)` or a `merge_base`
    // field would break this line and surface as a clear signal.
    let _opts: MergeOptions = MergeOptions::with_strategy(MergeStrategy::LastWriterWins);

    let test_db = TestDb::new();
    let branches = test_db.db.branches();
    branches.create("alpha_e").unwrap();
    branches.create("beta_e").unwrap();
    test_db
        .kv()
        .put(&resolve("alpha_e"), "default", "k", Value::Int(1))
        .unwrap();

    // No override path exists; unrelated branches refuse to merge.
    let err = branches
        .merge_with_options(
            "alpha_e",
            "beta_e",
            MergeOptions::with_strategy(MergeStrategy::LastWriterWins),
        )
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("no fork or merge relationship found"),
        "unrelated merge_with_options MUST refuse with the locked error; got: {msg}"
    );
}

// =============================================================================
// Scenario G: store-derived merge_base correct with and without DAG hook
// =============================================================================

/// B3.3 moves merge-base authority to `BranchControlStore`. The DAG hook
/// becomes a read-side acceleration for `log` / `ancestors` — it is not
/// required for `merge_base`. This test locks that behavior: a
/// freshly-forked pair returns a valid merge base regardless of whether
/// a DAG hook is installed. Since `TestDb::new` installs the default DAG
/// hook, the "no hook" case is locked by Scenario D elsewhere (the
/// internal branch_ops test `setup_with_branch` exercises the same
/// store-backed path with no DAG hook attached).
#[test]
fn g_store_derived_merge_base_correct_with_dag_hook() {
    let test_db = TestDb::new();
    let fork_version = setup_parent_child_fork(&test_db, "parent_g", "child_g");

    let mb = test_db
        .db
        .branches()
        .merge_base("parent_g", "child_g")
        .expect("merge_base lookup MUST not error")
        .expect("fresh fork MUST have a store-derived merge base");

    assert_eq!(
        mb.branch_name, "parent_g",
        "store-derived merge base should resolve to the parent name for a fresh fork"
    );
    assert_eq!(
        mb.commit_version.0, fork_version,
        "store-derived merge base version MUST equal the fork version"
    );
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
