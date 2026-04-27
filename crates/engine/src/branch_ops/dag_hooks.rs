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
//! ## Authority (post-B3)
//!
//! After the B3 cutover (see `docs/design/branching/b3-phasing-plan.md`),
//! `BranchControlStore` is the authoritative source for fork + merge
//! lineage and `find_merge_base`. The DAG written through these hooks is a
//! derived **read-side projection** used only for `log` and `ancestors`
//! ordered-history traversals. The store is rebuilt as the projection
//! source on every primary open, so the DAG can be regenerated from
//! authoritative state if it ever falls behind.
//!
//! ## Failure model
//!
//! Best-effort hooks log warnings on failure — they never propagate errors
//! back through the engine. Because the DAG is no longer authoritative for
//! merge-base, hook failure does not corrupt lineage truth: the next open
//! re-runs `BranchControlStore::rebuild_dag_projection` and the projection
//! is restored from the store.
//!
//! ## Lifecycle
//!
//! DAG hooks are per-database:
//!
//! 1. `GraphSubsystem::initialize()` installs a per-database `BranchDagHook`.
//! 2. `GraphSubsystem::bootstrap()` creates the `_system_` branch and the
//!    `_branch_dag` graph if they do not already exist.
//! 3. `BranchService` suppresses these best-effort hooks while it executes the
//!    load-bearing DAG write through its mutation boundary, so the canonical
//!    branch API records exactly once.
//! 4. Direct engine callers (`branch_ops::*`, `BranchIndex::*`, bundle import)
//!    dispatch through this module using the per-database hook. If no hook is
//!    installed (engine-only unit tests), operations complete without DAG
//!    recording.

use std::cell::Cell;
use std::sync::Arc;

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
/// `strata_engine::is_system_branch(name)` is true. Otherwise the
/// hook fires for `_system_` itself during `init_system_branch`, which runs
/// before the `_branch_dag` graph exists — producing a noisy warning on every
/// `Database::open`.
pub type BranchCreateHook = fn(db: &Arc<Database>, branch: &str);

/// Hook fired when a branch is deleted via `BranchIndex::delete_branch`. The
/// implementation should mark the branch's DAG node as `status = deleted`
/// without removing the node (deletion is recorded as a state change so the
/// historical lineage is preserved).
pub type BranchDeleteHook = fn(db: &Arc<Database>, branch: &str);

/// Hook fired when
/// `BranchService::fork` / `BranchService::fork_with_options` succeeds.
/// Internally this is dispatched from the crate-private
/// `branch_ops::fork_branch_with_metadata` helper. The implementation
/// should create a `fork` event node and the parent → event → child
/// edges, recording `fork_version`, `message`, and `creator`.
pub type BranchForkHook =
    fn(db: &Arc<Database>, info: &ForkInfo, message: Option<&str>, creator: Option<&str>);

/// Hook fired when
/// `BranchService::merge` / `BranchService::merge_with_options`
/// succeeds. Internally this is dispatched from the crate-private
/// `branch_ops::merge_branches_with_metadata` helper. The implementation
/// should create a `merge` event node and the source → event → target
/// edges, recording `merge_version`, `keys_applied`, `spaces_merged`,
/// the conflict count, the strategy, `message`, and `creator`.
pub type BranchMergeHook = fn(
    db: &Arc<Database>,
    info: &MergeInfo,
    strategy: MergeStrategy,
    message: Option<&str>,
    creator: Option<&str>,
);

/// Hook fired when `BranchService::revert` succeeds. Internally this is
/// dispatched from the crate-private
/// `branch_ops::revert_version_range_with_metadata` helper. The
/// implementation should create a `revert` event node linked from the
/// affected branch, recording `from_version`, `to_version`,
/// `revert_version`, `keys_reverted`, `message`, and `creator`.
pub type BranchRevertHook =
    fn(db: &Arc<Database>, info: &RevertInfo, message: Option<&str>, creator: Option<&str>);

/// Hook fired when `BranchService::cherry_pick` or
/// `BranchService::cherry_pick_from_diff` succeeds. Internally this is
/// dispatched from the crate-private `branch_ops::cherry_pick_from_diff`
/// or `branch_ops::cherry_pick_keys` helper. The implementation should
/// create a `cherry_pick` event node and source → event → target edges,
/// recording `keys_applied`, `keys_deleted`, and `cherry_pick_version`.
/// Cherry-picks are auditable as a distinct event type from merges
/// because they apply only a filtered subset of the diff.
pub type BranchCherryPickHook =
    fn(db: &Arc<Database>, source: &str, target: &str, info: &CherryPickInfo);

/// Bundle of all six DAG hooks.
///
/// This struct is legacy — the canonical path is now the per-database
/// `BranchDagHook` trait installed by `GraphSubsystem::initialize()`.
pub struct BranchDagHooks {
    /// Fired by `BranchIndex::create_branch` and `bundle::import_branch`.
    pub on_create: BranchCreateHook,
    /// Fired by `BranchIndex::delete_branch`.
    pub on_delete: BranchDeleteHook,
    /// Fired by `BranchService::fork` via the crate-private
    /// `branch_ops::fork_branch_with_metadata` helper.
    pub on_fork: BranchForkHook,
    /// Fired by `BranchService::merge` via the crate-private
    /// `branch_ops::merge_branches_with_metadata` helper.
    pub on_merge: BranchMergeHook,
    /// Fired by `BranchService::revert` via the crate-private
    /// `branch_ops::revert_version_range_with_metadata` helper.
    pub on_revert: BranchRevertHook,
    /// Fired by `BranchService::cherry_pick` /
    /// `BranchService::cherry_pick_from_diff` via the crate-private
    /// `branch_ops::cherry_pick_from_diff` and `cherry_pick_keys` helpers.
    pub on_cherry_pick: BranchCherryPickHook,
}

std::thread_local! {
    static SUPPRESS_BRANCH_DAG_HOOKS: Cell<usize> = const { Cell::new(0) };
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

    record_event_best_effort(
        db,
        "create",
        DagEvent::create(resolve_branch_name(branch), branch),
    );
}

pub(crate) fn dispatch_delete_hook(db: &Arc<Database>, branch: &str) {
    if branch_dag_hooks_suppressed() {
        return;
    }

    record_event_best_effort(
        db,
        "delete",
        DagEvent::delete(resolve_branch_name(branch), branch),
    );
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

    let Some(fork_version) = info.fork_version else {
        return;
    };

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
    record_event_best_effort(db, "fork", event);
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

    let Some(merge_version) = info.merge_version else {
        return;
    };

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
    record_event_best_effort(db, "merge", event);
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

    let Some(revert_version) = info.revert_version else {
        return;
    };

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
    record_event_best_effort(db, "revert", event);
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

    let Some(cherry_pick_version) = info.cherry_pick_version else {
        return;
    };

    let event = DagEvent::cherry_pick(
        resolve_branch_name(target),
        target,
        resolve_branch_name(source),
        source,
        CommitVersion(cherry_pick_version),
        info.clone(),
    );
    record_event_best_effort(db, "cherry_pick", event);
}
