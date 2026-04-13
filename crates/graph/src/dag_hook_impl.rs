//! Branch DAG hook implementation helpers.
//!
//! The actual `dag_record_*` write functions live in `branch_dag.rs`.
//! `GraphSubsystem::initialize()` installs a per-database `BranchDagHook`
//! that dispatches to those functions. This module is kept for backwards
//! compatibility with any code that imports from it.
//!
//! ## Failure model
//!
//! Every hook call is best-effort: it logs a warning on failure. The
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
//! because `init_system_branch` calls `BranchIndex::create_branch(SYSTEM_BRANCH)`
//! during `Database::open` to create the `_system_` branch *before* the
//! `_branch_dag` graph itself exists. Without the guard, the hook would
//! try to write a node to a graph that doesn't exist yet, logging a noisy
//! warning on every db open. The guard is also semantically correct — the
//! `_system_` branch shouldn't have a self-referential DAG entry.

// This module is intentionally empty — the per-database `GraphBranchDagHook`
// implementation lives in `branch_dag.rs`. The module is kept for backwards
// compatibility and documentation purposes.
