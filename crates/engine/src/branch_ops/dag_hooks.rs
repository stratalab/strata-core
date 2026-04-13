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
//! The primary DAG path is now per-database:
//!
//! 1. `GraphSubsystem::initialize()` installs a per-database `BranchDagHook`.
//! 2. `GraphSubsystem::bootstrap()` creates the `_system_` branch and the
//!    `_branch_dag` graph if they do not already exist.
//! 3. `BranchService` suppresses these best-effort hooks while it executes the
//!    load-bearing DAG write through its mutation boundary, so the canonical
//!    branch API records exactly once.
//! 4. Direct engine callers (`branch_ops::*`, `BranchIndex::*`, bundle import)
//!    still dispatch through this module. Those calls prefer the per-database
//!    hook and fall back to the legacy process-global registration only when no
//!    per-database hook is installed.
//! 5. `OnceCell` keeps the legacy fallback registration idempotent.

use std::cell::Cell;
use std::sync::Arc;

use once_cell::sync::OnceCell;
use strata_core::id::CommitVersion;
use tracing::warn;

use super::{CherryPickInfo, ForkInfo, MergeInfo, MergeStrategy, RevertInfo};
use crate::database::dag_hook::DagEvent;
use crate::database::Database;
use crate::primitives::branch::resolve_branch_name;

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

std::thread_local! {
    static SUPPRESS_BRANCH_DAG_HOOKS: Cell<usize> = const { Cell::new(0) };
}

/// Register the branch DAG hook implementation.
///
/// Legacy compatibility registration for environments that still rely on a
/// process-global DAG implementation. The canonical production path is the
/// per-database hook installed by `GraphSubsystem::initialize()`.
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

struct BranchDagHookSuppressionGuard;

impl Drop for BranchDagHookSuppressionGuard {
    fn drop(&mut self) {
        SUPPRESS_BRANCH_DAG_HOOKS.with(|depth| {
            depth.set(depth.get().saturating_sub(1));
        });
    }
}

pub(crate) fn with_branch_dag_hooks_suppressed<T>(f: impl FnOnce() -> T) -> T {
    SUPPRESS_BRANCH_DAG_HOOKS.with(|depth| {
        depth.set(depth.get() + 1);
    });
    let _guard = BranchDagHookSuppressionGuard;
    f()
}

fn branch_dag_hooks_suppressed() -> bool {
    SUPPRESS_BRANCH_DAG_HOOKS.with(|depth| depth.get() > 0)
}

fn record_event_best_effort(db: &Arc<Database>, operation: &str, event: DagEvent) -> bool {
    let Some(hook) = db.dag_hook().get() else {
        return false;
    };

    if let Err(error) = hook.record_event(&event) {
        warn!(
            target: "strata::branch_dag",
            hook = hook.name(),
            operation,
            kind = %event.kind,
            branch = %event.branch_name,
            error = %error,
            "failed to record DAG event"
        );
    }

    true
}

pub(crate) fn dispatch_create_hook(db: &Arc<Database>, branch: &str) {
    if branch_dag_hooks_suppressed() {
        return;
    }

    if record_event_best_effort(
        db,
        "create",
        DagEvent::create(resolve_branch_name(branch), branch),
    ) {
        return;
    }

    if let Some(hooks) = branch_dag_hooks() {
        (hooks.on_create)(db, branch);
    }
}

pub(crate) fn dispatch_delete_hook(db: &Arc<Database>, branch: &str) {
    if branch_dag_hooks_suppressed() {
        return;
    }

    if record_event_best_effort(
        db,
        "delete",
        DagEvent::delete(resolve_branch_name(branch), branch),
    ) {
        return;
    }

    if let Some(hooks) = branch_dag_hooks() {
        (hooks.on_delete)(db, branch);
    }
}

pub(crate) fn dispatch_fork_hook(
    db: &Arc<Database>,
    info: &ForkInfo,
    message: Option<&str>,
    creator: Option<&str>,
) {
    if branch_dag_hooks_suppressed() {
        return;
    }

    if let Some(fork_version) = info.fork_version {
        let mut event = DagEvent::fork(
            resolve_branch_name(&info.destination),
            &info.destination,
            resolve_branch_name(&info.source),
            &info.source,
            CommitVersion(fork_version),
        );
        if let Some(message) = message {
            event = event.with_message(message);
        }
        if let Some(creator) = creator {
            event = event.with_creator(creator);
        }
        if record_event_best_effort(db, "fork", event) {
            return;
        }
    } else if db.dag_hook().get().is_some() {
        return;
    }

    if let Some(hooks) = branch_dag_hooks() {
        (hooks.on_fork)(db, info, message, creator);
    }
}

pub(crate) fn dispatch_merge_hook(
    db: &Arc<Database>,
    info: &MergeInfo,
    strategy: MergeStrategy,
    message: Option<&str>,
    creator: Option<&str>,
) {
    if branch_dag_hooks_suppressed() {
        return;
    }

    if let Some(merge_version) = info.merge_version {
        let mut event = DagEvent::merge(
            resolve_branch_name(&info.target),
            &info.target,
            resolve_branch_name(&info.source),
            &info.source,
            CommitVersion(merge_version),
            info.clone(),
            strategy,
        );
        if let Some(message) = message {
            event = event.with_message(message);
        }
        if let Some(creator) = creator {
            event = event.with_creator(creator);
        }
        if record_event_best_effort(db, "merge", event) {
            return;
        }
    } else if db.dag_hook().get().is_some() {
        return;
    }

    if let Some(hooks) = branch_dag_hooks() {
        (hooks.on_merge)(db, info, strategy, message, creator);
    }
}

pub(crate) fn dispatch_revert_hook(
    db: &Arc<Database>,
    info: &RevertInfo,
    message: Option<&str>,
    creator: Option<&str>,
) {
    if branch_dag_hooks_suppressed() {
        return;
    }

    if let Some(revert_version) = info.revert_version {
        let mut event = DagEvent::revert(
            resolve_branch_name(&info.branch),
            &info.branch,
            revert_version,
            info.clone(),
        );
        if let Some(message) = message {
            event = event.with_message(message);
        }
        if let Some(creator) = creator {
            event = event.with_creator(creator);
        }
        if record_event_best_effort(db, "revert", event) {
            return;
        }
    } else if db.dag_hook().get().is_some() {
        return;
    }

    if let Some(hooks) = branch_dag_hooks() {
        (hooks.on_revert)(db, info, message, creator);
    }
}

pub(crate) fn dispatch_cherry_pick_hook(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    info: &CherryPickInfo,
) {
    if branch_dag_hooks_suppressed() {
        return;
    }

    if let Some(cherry_pick_version) = info.cherry_pick_version {
        let event = DagEvent::cherry_pick(
            resolve_branch_name(target),
            target,
            resolve_branch_name(source),
            source,
            CommitVersion(cherry_pick_version),
            info.clone(),
        );
        if record_event_best_effort(db, "cherry_pick", event) {
            return;
        }
    } else if db.dag_hook().get().is_some() {
        return;
    }

    if let Some(hooks) = branch_dag_hooks() {
        (hooks.on_cherry_pick)(db, source, target, info);
    }
}
