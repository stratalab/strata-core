//! Per-database branch DAG hook.
//!
//! The `BranchDagHook` trait provides a fail-fast hook for branch DAG
//! operations. Unlike observers (which are best-effort), DAG hooks are
//! load-bearing: failures propagate and abort the branch operation.
//!
//! ## Why This Exists
//!
//! The branch DAG (`_branch_dag` graph on `_system_` branch) records every
//! fork, merge, revert, and cherry-pick. This data is required for
//! `compute_merge_base` — without it, merge-base computation falls back to
//! expensive engine-level fork-info lookup.
//!
//! The graph crate implements the actual DAG storage, but the engine cannot
//! depend on the graph crate (cycle: graph depends on engine). This trait
//! lets the graph crate install its implementation during subsystem
//! `initialize()`, and the engine calls through the trait from `BranchService`.
//!
//! ## Failure Model
//!
//! DAG hooks are **fail-fast**: if a hook returns `Err`, the branch operation
//! fails and rolls back. This is the opposite of observers, which swallow
//! errors. The DAG is correctness-critical for merge-base computation.
//!
//! ## Per-Database vs Global
//!
//! Previous design used process-global `OnceCell` hooks. This caused state
//! leakage between database instances. The new design uses one hook slot
//! per `Database`, installed by `GraphSubsystem::initialize()`.
//!
//! ## Lifecycle
//!
//! 1. `GraphSubsystem::initialize()` calls `db.install_dag_hook(hook)`
//! 2. `BranchService::fork()` calls `db.dag_hook().record_event(...)`
//! 3. If no hook is installed, DAG-required operations return an error
//!
//! Operations that require a DAG hook:
//! - `fork` - records parent → fork → child edges
//! - `merge` - records source → merge → target edges
//! - `revert` - records revert event
//! - `cherry_pick` - records cherry-pick event
//! - `merge_base` - queries DAG for common ancestor
//! - `log` - queries DAG for branch history
//! - `ancestors` - queries DAG for ancestry chain
//!
//! Operations that work without a DAG hook:
//! - `create`, `delete`, `list`, `exists`, `info`, `diff`, `diff3`
//! - `tag`, `untag`, `list_tags`, `resolve_tag`

use parking_lot::RwLock;
use std::fmt;
use std::sync::Arc;

use strata_core::id::CommitVersion;
use strata_core::types::BranchId;

use crate::branch_ops::{CherryPickInfo, MergeInfo, MergeStrategy, RevertInfo};

// =============================================================================
// Error Types
// =============================================================================

/// Error kind for branch DAG operations.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BranchDagErrorKind {
    /// No DAG hook is installed (required for this operation).
    NoDagHook,
    /// DAG read failed.
    ReadFailed,
    /// DAG write failed.
    WriteFailed,
    /// Branch not found in DAG.
    BranchNotFound,
    /// Merge base computation failed.
    MergeBaseFailed,
    /// DAG is corrupted.
    Corrupted,
    /// Other error.
    Other,
}

impl fmt::Display for BranchDagErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoDagHook => write!(f, "no DAG hook installed"),
            Self::ReadFailed => write!(f, "DAG read failed"),
            Self::WriteFailed => write!(f, "DAG write failed"),
            Self::BranchNotFound => write!(f, "branch not found in DAG"),
            Self::MergeBaseFailed => write!(f, "merge base computation failed"),
            Self::Corrupted => write!(f, "DAG corrupted"),
            Self::Other => write!(f, "other"),
        }
    }
}

/// Error from branch DAG operations.
///
/// Unlike observer errors, DAG errors propagate and fail the operation.
#[derive(Debug)]
#[non_exhaustive]
pub struct BranchDagError {
    /// What kind of failure.
    pub kind: BranchDagErrorKind,
    /// Human-readable message.
    pub message: String,
}

impl BranchDagError {
    /// Create a new DAG error.
    pub fn new(kind: BranchDagErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Create a "no DAG hook" error.
    pub fn no_hook(operation: &str) -> Self {
        Self::new(
            BranchDagErrorKind::NoDagHook,
            format!("operation '{}' requires a DAG hook but none is installed", operation),
        )
    }

    /// Create a "read failed" error.
    pub fn read_failed(message: impl Into<String>) -> Self {
        Self::new(BranchDagErrorKind::ReadFailed, message)
    }

    /// Create a "write failed" error.
    pub fn write_failed(message: impl Into<String>) -> Self {
        Self::new(BranchDagErrorKind::WriteFailed, message)
    }

    /// Create a "branch not found" error.
    pub fn branch_not_found(branch: &str) -> Self {
        Self::new(
            BranchDagErrorKind::BranchNotFound,
            format!("branch '{}' not found in DAG", branch),
        )
    }
}

impl fmt::Display for BranchDagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "branch DAG error: {} - {}", self.kind, self.message)
    }
}

impl std::error::Error for BranchDagError {}

// =============================================================================
// DAG Event Types
// =============================================================================

/// Type of branch DAG event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DagEventKind {
    /// Branch created.
    BranchCreate,
    /// Branch deleted (soft delete — node marked as deleted).
    BranchDelete,
    /// Branch forked from parent.
    Fork,
    /// Branches merged.
    Merge,
    /// Version range reverted.
    Revert,
    /// Cherry-pick from source branch.
    CherryPick,
}

impl fmt::Display for DagEventKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BranchCreate => write!(f, "branch_create"),
            Self::BranchDelete => write!(f, "branch_delete"),
            Self::Fork => write!(f, "fork"),
            Self::Merge => write!(f, "merge"),
            Self::Revert => write!(f, "revert"),
            Self::CherryPick => write!(f, "cherry_pick"),
        }
    }
}

/// Event to record in the branch DAG.
///
/// Created by `BranchService` and passed to `BranchDagHook::record_event`.
#[derive(Debug, Clone)]
pub struct DagEvent {
    /// What kind of event.
    pub kind: DagEventKind,
    /// The primary branch affected.
    pub branch_id: BranchId,
    /// The branch name.
    pub branch_name: String,
    /// Source branch for fork/merge/cherry-pick.
    pub source_branch_id: Option<BranchId>,
    /// Source branch name.
    pub source_branch_name: Option<String>,
    /// The commit version at which the event occurred.
    pub commit_version: CommitVersion,
    /// Optional message.
    pub message: Option<String>,
    /// Optional creator identifier.
    pub creator: Option<String>,
    /// Merge strategy (for Merge events).
    pub strategy: Option<String>,
    /// Merge info (for Merge events).
    pub merge_info: Option<MergeInfo>,
    /// Revert info (for Revert events).
    pub revert_info: Option<RevertInfo>,
    /// Cherry-pick info (for CherryPick events).
    pub cherry_pick_info: Option<CherryPickInfo>,
}

impl DagEvent {
    /// Create a branch create event.
    ///
    /// Use `create()` for the simpler version without commit version.
    pub fn branch_create(
        branch_id: BranchId,
        branch_name: impl Into<String>,
        commit_version: CommitVersion,
    ) -> Self {
        Self {
            kind: DagEventKind::BranchCreate,
            branch_id,
            branch_name: branch_name.into(),
            source_branch_id: None,
            source_branch_name: None,
            commit_version,
            message: None,
            creator: None,
            strategy: None,
            merge_info: None,
            revert_info: None,
            cherry_pick_info: None,
        }
    }

    /// Create a simple branch create event (no commit version).
    ///
    /// Used by `BranchService::create()` where no commit has occurred yet.
    pub fn create(branch_id: BranchId, branch_name: impl Into<String>) -> Self {
        Self::branch_create(branch_id, branch_name, CommitVersion(0))
    }

    /// Create a branch delete event.
    ///
    /// Use `delete()` for the simpler version without commit version.
    pub fn branch_delete(
        branch_id: BranchId,
        branch_name: impl Into<String>,
        commit_version: CommitVersion,
    ) -> Self {
        Self {
            kind: DagEventKind::BranchDelete,
            branch_id,
            branch_name: branch_name.into(),
            source_branch_id: None,
            source_branch_name: None,
            commit_version,
            message: None,
            creator: None,
            strategy: None,
            merge_info: None,
            revert_info: None,
            cherry_pick_info: None,
        }
    }

    /// Create a simple branch delete event (no commit version).
    ///
    /// Used by `BranchService::delete()`.
    pub fn delete(branch_id: BranchId, branch_name: impl Into<String>) -> Self {
        Self::branch_delete(branch_id, branch_name, CommitVersion(0))
    }

    /// Create a fork event.
    pub fn fork(
        branch_id: BranchId,
        branch_name: impl Into<String>,
        source_branch_id: BranchId,
        source_branch_name: impl Into<String>,
        commit_version: CommitVersion,
    ) -> Self {
        Self {
            kind: DagEventKind::Fork,
            branch_id,
            branch_name: branch_name.into(),
            source_branch_id: Some(source_branch_id),
            source_branch_name: Some(source_branch_name.into()),
            commit_version,
            message: None,
            creator: None,
            strategy: None,
            merge_info: None,
            revert_info: None,
            cherry_pick_info: None,
        }
    }

    /// Create a merge event.
    pub fn merge(
        target_branch_id: BranchId,
        target_branch_name: impl Into<String>,
        source_branch_id: BranchId,
        source_branch_name: impl Into<String>,
        commit_version: CommitVersion,
        info: MergeInfo,
        strategy: MergeStrategy,
    ) -> Self {
        let strategy_str = match strategy {
            MergeStrategy::LastWriterWins => "last_writer_wins",
            MergeStrategy::Strict => "strict",
        };
        Self {
            kind: DagEventKind::Merge,
            branch_id: target_branch_id,
            branch_name: target_branch_name.into(),
            source_branch_id: Some(source_branch_id),
            source_branch_name: Some(source_branch_name.into()),
            commit_version,
            message: None,
            creator: None,
            strategy: Some(strategy_str.to_string()),
            merge_info: Some(info),
            revert_info: None,
            cherry_pick_info: None,
        }
    }

    /// Create a revert event.
    pub fn revert(
        branch_id: BranchId,
        branch_name: impl Into<String>,
        commit_version: CommitVersion,
        info: RevertInfo,
    ) -> Self {
        Self {
            kind: DagEventKind::Revert,
            branch_id,
            branch_name: branch_name.into(),
            source_branch_id: None,
            source_branch_name: Some(format!("v{}..v{}", info.from_version.0, info.to_version.0)),
            commit_version,
            message: None,
            creator: None,
            strategy: None,
            merge_info: None,
            revert_info: Some(info),
            cherry_pick_info: None,
        }
    }

    /// Create a cherry-pick event.
    pub fn cherry_pick(
        target_branch_id: BranchId,
        target_branch_name: impl Into<String>,
        source_branch_id: BranchId,
        source_branch_name: impl Into<String>,
        commit_version: CommitVersion,
        info: CherryPickInfo,
    ) -> Self {
        Self {
            kind: DagEventKind::CherryPick,
            branch_id: target_branch_id,
            branch_name: target_branch_name.into(),
            source_branch_id: Some(source_branch_id),
            source_branch_name: Some(source_branch_name.into()),
            commit_version,
            message: None,
            creator: None,
            strategy: None,
            merge_info: None,
            revert_info: None,
            cherry_pick_info: Some(info),
        }
    }

    /// Set the message.
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Set the creator.
    pub fn with_creator(mut self, creator: impl Into<String>) -> Self {
        self.creator = Some(creator.into());
        self
    }
}

/// Merge base result from DAG query.
#[derive(Debug, Clone)]
pub struct MergeBaseResult {
    /// The common ancestor branch.
    pub branch_id: BranchId,
    /// The branch name.
    pub branch_name: String,
    /// The commit version at the merge base.
    pub commit_version: CommitVersion,
}

/// Entry in branch ancestry chain.
#[derive(Debug, Clone)]
pub struct AncestryEntry {
    /// The branch in the ancestry chain.
    pub branch_id: BranchId,
    /// The branch name.
    pub branch_name: String,
    /// How this branch relates to the next (fork, merge, etc.).
    pub relation: DagEventKind,
    /// The commit version at this point.
    pub commit_version: CommitVersion,
}

// =============================================================================
// BranchDagHook Trait
// =============================================================================

/// Per-database hook for branch DAG operations.
///
/// Installed by `GraphSubsystem::initialize()`. Called by `BranchService`
/// for DAG reads (merge-base, log, ancestors) and writes (record_event).
///
/// ## Thread Safety
///
/// Implementations must be `Send + Sync`. The hook may be called
/// concurrently from multiple threads.
///
/// ## Failure Model
///
/// Errors propagate and fail the calling branch operation.
/// This is intentional: the DAG is correctness-critical.
pub trait BranchDagHook: Send + Sync + 'static {
    /// Human-readable name for logging.
    fn name(&self) -> &'static str;

    /// Record a DAG event.
    ///
    /// Called by `BranchService` after the branch mutation succeeds but
    /// before the operation returns. If this fails, the branch operation
    /// rolls back.
    fn record_event(&self, event: &DagEvent) -> Result<(), BranchDagError>;

    /// Find the merge base (common ancestor) of two branches.
    ///
    /// Returns `None` if no common ancestor exists.
    fn find_merge_base(
        &self,
        branch_a: &BranchId,
        branch_b: &BranchId,
    ) -> Result<Option<MergeBaseResult>, BranchDagError>;

    /// Get the history log for a branch.
    ///
    /// Returns events in reverse chronological order (newest first).
    fn log(&self, branch_id: &BranchId, limit: usize) -> Result<Vec<DagEvent>, BranchDagError>;

    /// Get the ancestry chain for a branch.
    ///
    /// Returns the chain from the branch back to its root.
    fn ancestors(&self, branch_id: &BranchId) -> Result<Vec<AncestryEntry>, BranchDagError>;
}

// =============================================================================
// Hook Slot
// =============================================================================

/// Slot for the per-database DAG hook.
///
/// Part of the `Database` struct. Provides install-once semantics.
pub struct DagHookSlot {
    hook: RwLock<Option<Arc<dyn BranchDagHook>>>,
}

impl DagHookSlot {
    /// Create an empty slot.
    pub fn new() -> Self {
        Self {
            hook: RwLock::new(None),
        }
    }

    /// Install a DAG hook.
    ///
    /// Returns `Ok(())` if installed successfully.
    /// Returns `Err` if a hook is already installed (programming error).
    pub fn install(&self, hook: Arc<dyn BranchDagHook>) -> Result<(), BranchDagError> {
        let mut guard = self.hook.write();
        if guard.is_some() {
            return Err(BranchDagError::new(
                BranchDagErrorKind::Other,
                "DAG hook already installed (duplicate install is a programming error)",
            ));
        }
        *guard = Some(hook);
        Ok(())
    }

    /// Get the installed hook.
    ///
    /// Returns `None` if no hook is installed.
    pub fn get(&self) -> Option<Arc<dyn BranchDagHook>> {
        self.hook.read().clone()
    }

    /// Check if a hook is installed.
    pub fn is_installed(&self) -> bool {
        self.hook.read().is_some()
    }

    /// Require the hook for an operation.
    ///
    /// Returns the hook if installed, or an error suitable for propagation.
    pub fn require(&self, operation: &str) -> Result<Arc<dyn BranchDagHook>, BranchDagError> {
        self.get().ok_or_else(|| BranchDagError::no_hook(operation))
    }
}

impl Default for DagHookSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for DagHookSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let installed = self.hook.read().as_ref().map(|h| h.name());
        f.debug_struct("DagHookSlot")
            .field("hook", &installed)
            .finish()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDagHook;

    impl BranchDagHook for TestDagHook {
        fn name(&self) -> &'static str {
            "test"
        }

        fn record_event(&self, _event: &DagEvent) -> Result<(), BranchDagError> {
            Ok(())
        }

        fn find_merge_base(
            &self,
            _a: &BranchId,
            _b: &BranchId,
        ) -> Result<Option<MergeBaseResult>, BranchDagError> {
            Ok(None)
        }

        fn log(&self, _branch_id: &BranchId, _limit: usize) -> Result<Vec<DagEvent>, BranchDagError> {
            Ok(Vec::new())
        }

        fn ancestors(&self, _branch_id: &BranchId) -> Result<Vec<AncestryEntry>, BranchDagError> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn test_dag_hook_slot_install() {
        let slot = DagHookSlot::new();
        assert!(!slot.is_installed());
        assert!(slot.get().is_none());

        let hook = Arc::new(TestDagHook);
        slot.install(hook).unwrap();

        assert!(slot.is_installed());
        assert!(slot.get().is_some());
    }

    #[test]
    fn test_dag_hook_slot_double_install_fails() {
        let slot = DagHookSlot::new();

        let hook1 = Arc::new(TestDagHook);
        slot.install(hook1).unwrap();

        let hook2 = Arc::new(TestDagHook);
        let result = slot.install(hook2);
        assert!(result.is_err());
    }

    #[test]
    fn test_dag_hook_slot_require() {
        let slot = DagHookSlot::new();

        // No hook installed
        let result = slot.require("merge_base");
        assert!(result.is_err());

        // Install hook
        slot.install(Arc::new(TestDagHook)).unwrap();

        // Now require should succeed
        let hook = slot.require("merge_base").unwrap();
        assert_eq!(hook.name(), "test");
    }

    #[test]
    fn test_dag_event_builders() {
        let branch_id = BranchId::new();
        let source_id = BranchId::new();
        let version = CommitVersion(42);

        let event = DagEvent::fork(branch_id, "child", source_id, "parent", version)
            .with_message("Forked for feature")
            .with_creator("user123");

        assert_eq!(event.kind, DagEventKind::Fork);
        assert_eq!(event.branch_name, "child");
        assert_eq!(event.source_branch_name, Some("parent".to_string()));
        assert_eq!(event.message, Some("Forked for feature".to_string()));
        assert_eq!(event.creator, Some("user123".to_string()));
    }

    #[test]
    fn test_dag_error_display() {
        let err = BranchDagError::no_hook("merge_base");
        assert!(err.to_string().contains("merge_base"));
        assert!(err.to_string().contains("no DAG hook"));
    }
}
