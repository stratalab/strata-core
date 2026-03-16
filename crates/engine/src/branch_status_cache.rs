//! In-memory cache for branch DAG statuses.
//!
//! Stored as a database extension (`db.extension::<BranchStatusCache>()`).
//! For Epic 1 this is a minimal struct; richer logic comes in later epics.

use dashmap::DashMap;

use crate::branch_dag::DagBranchStatus;

/// Cache of branch statuses, populated from the `_branch_dag` graph.
///
/// Implements `Default` so it can be used as a database extension via
/// `db.extension::<BranchStatusCache>()`.
#[derive(Debug, Default)]
pub struct BranchStatusCache {
    inner: DashMap<String, DagBranchStatus>,
}

impl BranchStatusCache {
    /// Get the cached status of a branch.
    pub fn get(&self, name: &str) -> Option<DagBranchStatus> {
        self.inner.get(name).map(|r| *r)
    }

    /// Set the cached status of a branch.
    pub fn set(&self, name: String, status: DagBranchStatus) {
        self.inner.insert(name, status);
    }

    /// Remove a branch from the cache.
    pub fn remove(&self, name: &str) {
        self.inner.remove(name);
    }

    /// Returns `true` if the branch is active (or not in the cache, defaulting to writable).
    pub fn is_writable(&self, name: &str) -> bool {
        match self.get(name) {
            Some(status) => status.is_writable(),
            None => true,
        }
    }
}
