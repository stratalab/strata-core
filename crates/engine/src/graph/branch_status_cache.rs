//! In-memory cache for branch DAG statuses.
//!
//! Stored as a database extension (`db.extension::<BranchStatusCache>()`).
//!
//! ## Convergence class
//!
//! `AdvisoryOnly` — see `ConvergenceClass::AdvisoryOnly` in
//! `crates/engine/src/branch_retention/mod.rs` and §"Rule 3.
//! BranchStatusCache is advisory only" in
//! `docs/design/branching/branching-gc/branching-b5-convergence-and-observability.md`.
//!
//! No correctness decision (write gating, lifecycle transitions, branch
//! existence checks, merge eligibility, delete safety) may depend on
//! this cache. The authoritative lifecycle source is
//! `BranchControlStore`. This cache exists only to accelerate read-path
//! introspection for tooling that wants a fast non-authoritative answer.
//!
//! If the cache is missing, stale, or not yet rebuilt:
//! - branch writes still honor the canonical lifecycle gate (they
//!   consult `BranchControlStore`, not this cache)
//! - callers must not infer "writable" or "safe" from cache miss
//!
//! The query surface reflects that: `is_writable_hint` returns
//! `Option<bool>` with `None` for cache miss, so callers are forced to
//! handle the "consult the authority" path at the type level rather
//! than by reading a doc comment.
//!
//! ## Cleanup
//!
//! `GraphSubsystem::cleanup_deleted_branch` removes the deleted
//! branch's entry from this cache so a same-name recreate does not
//! observe the old lifecycle instance's cached status. The cache is
//! keyed by branch name, so without this hook a `main@gen1` → delete
//! → `main@gen2` sequence would leave a stale entry keyed by `"main"`.
//! Even though no correctness decision depends on the cache, leaking
//! old state would violate the convergence contract's "no stale
//! observable state across lifecycle boundaries" rule.

use dashmap::DashMap;

use crate::DagBranchStatus;

/// Cache of branch statuses, populated from the `_branch_dag` graph.
///
/// Implements `Default` so it can be used as a database extension via
/// `db.extension::<BranchStatusCache>()`.
///
/// See the module-level doc for the advisory-only contract.
#[derive(Debug, Default)]
pub struct BranchStatusCache {
    inner: DashMap<String, DagBranchStatus>,
}

impl BranchStatusCache {
    /// Get the cached status of a branch.
    ///
    /// Advisory: the returned status is a point-in-time cache read and
    /// may be stale. Correctness callers must consult `BranchControlStore`
    /// instead.
    pub fn get(&self, name: &str) -> Option<DagBranchStatus> {
        self.inner.get(name).map(|r| *r)
    }

    /// Set the cached status of a branch.
    ///
    /// Write-path plumbing for hydration and best-effort maintenance.
    /// Does not constitute a lifecycle transition; the authoritative
    /// write lives in `BranchControlStore`.
    pub fn set(&self, name: String, status: DagBranchStatus) {
        self.inner.insert(name, status);
    }

    /// Remove a branch from the cache.
    ///
    /// Called by `GraphSubsystem::cleanup_deleted_branch` after a
    /// branch delete so a later same-name recreate does not observe
    /// the prior lifecycle instance's cached entry.
    pub fn remove(&self, name: &str) {
        self.inner.remove(name);
    }

    /// Advisory hint for whether the named branch is currently
    /// writable.
    ///
    /// Returns:
    /// - `Some(true)` — cached status is writable
    /// - `Some(false)` — cached status is archived / merged / deleted
    /// - `None` — cache miss; caller MUST consult the authoritative
    ///   lifecycle source (`BranchControlStore`) instead of assuming
    ///   a default
    ///
    /// This method is `_hint` by design: no correctness path is
    /// allowed to read it. The `None` return forces the "cache miss
    /// means consult the authority" contract at the type level
    /// rather than relying on doc comments to enforce it.
    #[must_use]
    pub fn is_writable_hint(&self, name: &str) -> Option<bool> {
        self.get(name).map(|status| status.is_writable())
    }
}
