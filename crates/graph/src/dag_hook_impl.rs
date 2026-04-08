//! Branch DAG hook implementation registered with the engine.
//!
//! The engine declares hook *signatures* in `strata_engine::branch_ops::dag_hooks`
//! but cannot implement them itself (the implementation needs `GraphStore`,
//! and the engine cannot depend on the graph crate — cycle). This module
//! provides the implementation.
//!
//! Call [`register_branch_dag_hook_implementation`] once at application or
//! test startup, before any branch operation. The standard test fixture in
//! `tests/common/mod.rs::ensure_recovery_registered` does this alongside
//! the existing graph/vector semantic-merge registrations. The executor's
//! recovery init in `crates/executor/src/api/mod.rs` does the same for
//! production startup.
//!
//! ## Failure model
//!
//! Every hook closure is best-effort: it calls the corresponding
//! `dag_record_*` write helper and logs a warning on failure. The
//! underlying branch operation has already committed by the time the hook
//! fires, so propagating an error back would be confusing — the user's
//! data is intact, only the lineage record is missing. The DAG is a query
//! optimization for `compute_merge_base_from_dag` and degrades gracefully
//! to engine-level fork-info lookup when stale.
//!
//! ## System-branch guard
//!
//! Every hook early-returns for branch names that match
//! `is_system_branch` (anything starting with `_system`). This is critical
//! for `on_create`: `init_system_branch` calls
//! `BranchIndex::create_branch(SYSTEM_BRANCH)` during `Database::open` to
//! create the `_system_` branch *before* the `_branch_dag` graph itself
//! exists. Without the guard, the hook would try to write a node to a
//! graph that doesn't exist yet, logging a noisy warning on every db
//! open. The guard is also semantically correct — the `_system_` branch
//! shouldn't have a self-referential DAG entry.

use strata_core::branch_dag::is_system_branch;
use strata_engine::{register_branch_dag_hooks, BranchDagHooks, MergeStrategy};

use crate::branch_dag::{
    dag_add_branch, dag_mark_deleted, dag_record_cherry_pick, dag_record_fork, dag_record_merge,
    dag_record_revert,
};

/// Register the branch DAG hook implementation with the engine.
///
/// Idempotent: the engine's `OnceCell` slot only accepts the first call.
/// Subsequent calls are no-ops, matching the existing
/// `register_graph_semantic_merge` / `register_vector_semantic_merge`
/// registration pattern.
pub fn register_branch_dag_hook_implementation() {
    register_branch_dag_hooks(BranchDagHooks {
        on_create: |db, branch| {
            // CRITICAL: see the system-branch guard rationale in the
            // module-level docs.
            if is_system_branch(branch) {
                return;
            }
            if let Err(e) = dag_add_branch(db, branch, None, None) {
                tracing::warn!(
                    target: "strata::branch_dag",
                    branch = %branch,
                    error = %e,
                    "failed to record branch creation in DAG"
                );
            }
        },
        on_delete: |db, branch| {
            if is_system_branch(branch) {
                return;
            }
            if let Err(e) = dag_mark_deleted(db, branch) {
                tracing::warn!(
                    target: "strata::branch_dag",
                    branch = %branch,
                    error = %e,
                    "failed to mark branch as deleted in DAG"
                );
            }
        },
        on_fork: |db, info, message, creator| {
            if is_system_branch(&info.source) || is_system_branch(&info.destination) {
                return;
            }
            if let Err(e) = dag_record_fork(
                db,
                &info.source,
                &info.destination,
                info.fork_version,
                message,
                creator,
            ) {
                tracing::warn!(
                    target: "strata::branch_dag",
                    source = %info.source,
                    destination = %info.destination,
                    error = %e,
                    "failed to record fork in DAG"
                );
            }
        },
        on_merge: |db, info, strategy, message, creator| {
            if is_system_branch(&info.source) || is_system_branch(&info.target) {
                return;
            }
            let strategy_str = match strategy {
                MergeStrategy::LastWriterWins => "last_writer_wins",
                MergeStrategy::Strict => "strict",
            };
            if let Err(e) = dag_record_merge(
                db,
                &info.source,
                &info.target,
                info,
                Some(strategy_str),
                message,
                creator,
            ) {
                tracing::warn!(
                    target: "strata::branch_dag",
                    source = %info.source,
                    target = %info.target,
                    error = %e,
                    "failed to record merge in DAG"
                );
            }
        },
        on_revert: |db, info, message, creator| {
            if is_system_branch(&info.branch) {
                return;
            }
            if let Err(e) = dag_record_revert(db, info, message, creator) {
                tracing::warn!(
                    target: "strata::branch_dag",
                    branch = %info.branch,
                    error = %e,
                    "failed to record revert in DAG"
                );
            }
        },
        on_cherry_pick: |db, source, target, info| {
            if is_system_branch(source) || is_system_branch(target) {
                return;
            }
            if let Err(e) = dag_record_cherry_pick(db, source, target, info) {
                tracing::warn!(
                    target: "strata::branch_dag",
                    source = %source,
                    target = %target,
                    error = %e,
                    "failed to record cherry-pick in DAG"
                );
            }
        },
    });
}
