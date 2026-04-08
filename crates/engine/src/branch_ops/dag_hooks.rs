//! Branch DAG hooks — engine-side surface for the per-branch event log.
//!
//! ## Why this exists
//!
//! Strata records every fork, merge, revert, cherry-pick, branch create, and
//! branch delete as a node in a graph called `_branch_dag` stored on the
//! `_system_` branch. The graph crate (`strata-graph`) implements the actual
//! write logic. The engine needs to call into the graph crate from `fork_branch`,
//! `merge_branches`, and friends — but the engine cannot depend on the graph
//! crate directly (cycle: `strata-graph` already depends on `strata-engine`).
//!
//! The fix mirrors the existing `register_vector_merge` /
//! `register_graph_merge_plan` pattern in `branch_ops/primitive_merge.rs`: the
//! engine declares hook *signatures*, the graph crate registers the
//! *implementations* at startup, and the engine calls the registered hooks
//! from inside its branch-mutating functions. Engine-direct callers and
//! executor-driven callers go through identical code paths, so no one can
//! bypass the DAG by reaching into the engine API directly.
//!
//! ## Failure model
//!
//! Hooks are infallible (`fn(...)` with no `Result`). The implementation
//! handles its own errors by logging warnings — it must never propagate
//! a failure back through the engine, because the underlying branch
//! operation has already committed by the time the hook fires. The DAG is
//! a query-optimization index for `compute_merge_base_from_dag`; staleness
//! degrades gracefully to engine-level fork-info lookup.
//!
//! ## Lifecycle
//!
//! 1. `Database::open()` creates the `_system_` branch and the `_branch_dag`
//!    graph via the `GraphSubsystem` recovery participant.
//! 2. Application startup (or test fixture init) calls
//!    `strata_graph::register_branch_dag_hook_implementation()`, which calls
//!    `register_branch_dag_hooks(...)` from this module.
//! 3. From that point on, every call to `branch_ops::fork_branch`,
//!    `merge_branches`, `revert_version_range`, `cherry_pick_*`,
//!    `BranchIndex::create_branch`, `BranchIndex::delete_branch`, or
//!    `bundle::import_branch` fires the corresponding hook.
//! 4. `OnceCell` ensures the registration is idempotent — the first call
//!    wins, subsequent calls are no-ops.

use std::sync::Arc;

use once_cell::sync::OnceCell;

use super::{CherryPickInfo, ForkInfo, MergeInfo, MergeStrategy, RevertInfo};
use crate::database::Database;

/// Hook fired when a branch is created via `BranchIndex::create_branch` or
/// `bundle::import_branch`. The implementation should add a `branch` node to
/// the DAG with `status = active`.
///
/// Implementations MUST early-return for any name where
/// `strata_core::branch_dag::is_system_branch(name)` is true. Otherwise the
/// hook fires for `_system_` itself during `init_system_branch`, which runs
/// before the `_branch_dag` graph exists — producing a noisy warning on every
/// `Database::open`.
pub type BranchCreateHook = fn(db: &Arc<Database>, branch: &str);

/// Hook fired when a branch is deleted via `BranchIndex::delete_branch`. The
/// implementation should mark the branch's DAG node as `status = deleted`
/// without removing the node (deletion is recorded as a state change so the
/// historical lineage is preserved).
pub type BranchDeleteHook = fn(db: &Arc<Database>, branch: &str);

/// Hook fired when `branch_ops::fork_branch` (or its `_with_metadata`
/// variant) succeeds. The implementation should create a `fork` event node
/// and the parent → event → child edges, recording `fork_version`,
/// `message`, and `creator`.
pub type BranchForkHook =
    fn(db: &Arc<Database>, info: &ForkInfo, message: Option<&str>, creator: Option<&str>);

/// Hook fired when `branch_ops::merge_branches` (or its `_with_metadata`
/// variant) succeeds. The implementation should create a `merge` event node
/// and the source → event → target edges, recording `merge_version`,
/// `keys_applied`, `spaces_merged`, the conflict count, the strategy,
/// `message`, and `creator`.
pub type BranchMergeHook = fn(
    db: &Arc<Database>,
    info: &MergeInfo,
    strategy: MergeStrategy,
    message: Option<&str>,
    creator: Option<&str>,
);

/// Hook fired when `branch_ops::revert_version_range` (or its `_with_metadata`
/// variant) succeeds. The implementation should create a `revert` event node
/// linked from the affected branch, recording `from_version`, `to_version`,
/// `revert_version`, `keys_reverted`, `message`, and `creator`.
pub type BranchRevertHook =
    fn(db: &Arc<Database>, info: &RevertInfo, message: Option<&str>, creator: Option<&str>);

/// Hook fired when `branch_ops::cherry_pick_from_diff` or `cherry_pick_keys`
/// succeeds. The implementation should create a `cherry_pick` event node and
/// source → event → target edges, recording `keys_applied`, `keys_deleted`,
/// and `cherry_pick_version`. Cherry-picks are auditable as a distinct event
/// type from merges because they apply only a filtered subset of the diff.
pub type BranchCherryPickHook =
    fn(db: &Arc<Database>, source: &str, target: &str, info: &CherryPickInfo);

/// Bundle of all six DAG hooks. The graph crate constructs and registers
/// this struct exactly once at startup via [`register_branch_dag_hooks`].
pub struct BranchDagHooks {
    /// Fired by `BranchIndex::create_branch` and `bundle::import_branch`.
    pub on_create: BranchCreateHook,
    /// Fired by `BranchIndex::delete_branch`.
    pub on_delete: BranchDeleteHook,
    /// Fired by `branch_ops::fork_branch` (and the `_with_metadata` variant).
    pub on_fork: BranchForkHook,
    /// Fired by `branch_ops::merge_branches` (and the `_with_metadata` variant).
    pub on_merge: BranchMergeHook,
    /// Fired by `branch_ops::revert_version_range` (and the `_with_metadata` variant).
    pub on_revert: BranchRevertHook,
    /// Fired by `branch_ops::cherry_pick_from_diff` and `cherry_pick_keys`.
    pub on_cherry_pick: BranchCherryPickHook,
}

/// Process-global hook bundle. Set once via [`register_branch_dag_hooks`].
/// `OnceCell` makes the first call win — subsequent registration attempts
/// are no-ops, matching the existing `register_vector_merge` /
/// `register_graph_merge_plan` semantics.
static BRANCH_DAG_HOOKS: OnceCell<BranchDagHooks> = OnceCell::new();

/// Register the branch DAG hook implementation.
///
/// Should be called once at application/test startup, before any branch
/// operation. The standard test fixture in `tests/common/mod.rs` calls this
/// from `ensure_recovery_registered` alongside the existing graph/vector
/// semantic-merge registrations. The executor's recovery init in
/// `crates/executor/src/api/mod.rs` does the same for production startup.
///
/// `strata_graph::register_branch_dag_hook_implementation()` is the
/// canonical caller — it builds the `BranchDagHooks` struct and forwards
/// here.
pub fn register_branch_dag_hooks(hooks: BranchDagHooks) {
    let _ = BRANCH_DAG_HOOKS.set(hooks);
}

/// Engine-internal accessor. Returns the registered hooks if any.
///
/// Engine functions that mutate branch state (`fork_branch`,
/// `merge_branches`, etc.) call this and dispatch to the hook if it exists.
/// If no hook is registered (engine-only unit tests that don't load the
/// graph crate), the call is a no-op and the operation completes normally
/// without DAG bookkeeping.
pub(crate) fn branch_dag_hooks() -> Option<&'static BranchDagHooks> {
    BRANCH_DAG_HOOKS.get()
}
