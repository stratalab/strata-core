//! Per-database merge handler registry.
//!
//! Replaces the process-global `OnceCell` patterns in `primitive_merge.rs`
//! with per-database registration. This allows different databases to have
//! different merge handlers and avoids first-caller-wins races.
//!
//! ## Usage
//!
//! ```text
//! // During subsystem initialize:
//! db.merge_registry().register_vector(precheck_fn, post_commit_fn);
//! db.merge_registry().register_graph(plan_fn);
//! ```

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::StrataResult;
use parking_lot::RwLock;
use strata_core::types::BranchId;

use crate::database::Database;

// =============================================================================
// Type Aliases for Callbacks
// =============================================================================

/// Vector merge precheck function signature.
///
/// Called before merge to validate dimension/metric compatibility.
pub type VectorMergePrecheckFn =
    fn(db: &Arc<Database>, source_id: BranchId, target_id: BranchId) -> StrataResult<()>;

/// Vector merge post-commit function signature.
///
/// Called after merge commits to rebuild affected HNSW indexes.
pub type VectorMergePostCommitFn = fn(
    db: &Arc<Database>,
    source_id: BranchId,
    target_id: BranchId,
    affected: &BTreeSet<(String, String)>, // (space, collection)
) -> StrataResult<()>;

/// Graph merge plan function signature.
///
/// Called during merge to produce semantic graph merge plan.
pub type GraphMergePlanFn =
    fn(
        ctx: &crate::branch_ops::primitive_merge::MergePlanCtx<'_>,
    ) -> StrataResult<crate::branch_ops::primitive_merge::PrimitiveMergePlan>;

// =============================================================================
// Registry Types
// =============================================================================

/// Vector merge callbacks.
#[derive(Clone)]
pub struct VectorMergeCallbacks {
    /// Precheck function (dimension/metric validation).
    pub precheck: VectorMergePrecheckFn,
    /// Post-commit function (HNSW rebuild).
    pub post_commit: VectorMergePostCommitFn,
}

/// Per-database merge handler registry.
///
/// Stores callbacks for vector and graph merge handlers. Accessed via
/// `Database::merge_registry()`.
pub struct MergeHandlerRegistry {
    /// Vector merge callbacks (optional).
    vector: RwLock<Option<VectorMergeCallbacks>>,
    /// Graph merge plan function (optional).
    graph: RwLock<Option<GraphMergePlanFn>>,
}

impl MergeHandlerRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            vector: RwLock::new(None),
            graph: RwLock::new(None),
        }
    }

    /// Register vector merge callbacks.
    ///
    /// Called by `VectorSubsystem::initialize()`.
    pub fn register_vector(
        &self,
        precheck: VectorMergePrecheckFn,
        post_commit: VectorMergePostCommitFn,
    ) {
        *self.vector.write() = Some(VectorMergeCallbacks {
            precheck,
            post_commit,
        });
    }

    /// Register graph merge plan function.
    ///
    /// Called by `GraphSubsystem::initialize()`.
    pub fn register_graph(&self, plan_fn: GraphMergePlanFn) {
        *self.graph.write() = Some(plan_fn);
    }

    /// Get vector merge callbacks (if registered).
    pub fn vector_callbacks(&self) -> Option<VectorMergeCallbacks> {
        self.vector.read().clone()
    }

    /// Get graph merge plan function (if registered).
    pub fn graph_plan_fn(&self) -> Option<GraphMergePlanFn> {
        *self.graph.read()
    }

    /// Check if vector callbacks are registered.
    pub fn has_vector(&self) -> bool {
        self.vector.read().is_some()
    }

    /// Check if graph plan function is registered.
    pub fn has_graph(&self) -> bool {
        self.graph.read().is_some()
    }
}

impl Default for MergeHandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn dummy_vector_precheck(
        _db: &Arc<Database>,
        _source: BranchId,
        _target: BranchId,
    ) -> StrataResult<()> {
        Ok(())
    }

    fn dummy_vector_post_commit(
        _db: &Arc<Database>,
        _source: BranchId,
        _target: BranchId,
        _affected: &BTreeSet<(String, String)>,
    ) -> StrataResult<()> {
        Ok(())
    }

    #[test]
    fn test_registry_starts_empty() {
        let reg = MergeHandlerRegistry::new();
        assert!(!reg.has_vector());
        assert!(!reg.has_graph());
    }

    #[test]
    fn test_register_vector() {
        let reg = MergeHandlerRegistry::new();
        reg.register_vector(dummy_vector_precheck, dummy_vector_post_commit);
        assert!(reg.has_vector());
        assert!(reg.vector_callbacks().is_some());
    }
}
