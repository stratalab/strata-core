//! Observer traits and per-database registries.
//!
//! Observers are non-load-bearing callbacks that fire after successful
//! operations. They are used for audit logging, metrics, secondary index
//! maintenance, and other derived state updates.
//!
//! ## Failure Model
//!
//! Observers are **best-effort**: failures are logged and counted but never
//! propagate back to the caller. The operation that triggered the observer
//! has already succeeded by the time observers fire.
//!
//! ## Observer Types
//!
//! | Observer | Fires When | Use Cases |
//! |----------|------------|-----------|
//! | `CommitObserver` | After successful commit | Index maintenance, audit log |
//! | `ReplayObserver` | After follower applies record | Index rebuild on replica |
//! | `BranchOpObserver` | After branch operation | DAG audit, branch metrics |
//!
//! ## Thread Safety
//!
//! Registries use `RwLock` for safe concurrent access. Observers themselves
//! must be `Send + Sync` since they may be called from any thread.

use parking_lot::RwLock;
use std::fmt;
use std::sync::Arc;

use strata_core::id::CommitVersion;
use strata_core::types::BranchId;

// =============================================================================
// Error Types
// =============================================================================

/// Error kind for observer failures.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ObserverErrorKind {
    /// Observer callback panicked.
    Panicked,
    /// Observer returned an error.
    Failed,
    /// Observer timed out.
    Timeout,
    /// Other error.
    Other,
}

impl fmt::Display for ObserverErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Panicked => write!(f, "panicked"),
            Self::Failed => write!(f, "failed"),
            Self::Timeout => write!(f, "timeout"),
            Self::Other => write!(f, "other"),
        }
    }
}

/// Error from an observer callback.
///
/// Observer errors are logged but never propagate to the caller.
#[derive(Debug)]
#[non_exhaustive]
pub struct ObserverError {
    /// The observer that failed.
    pub observer_name: String,
    /// What kind of failure.
    pub kind: ObserverErrorKind,
    /// Human-readable message.
    pub message: String,
}

impl ObserverError {
    /// Create a new observer error.
    pub fn new(
        observer_name: impl Into<String>,
        kind: ObserverErrorKind,
        message: impl Into<String>,
    ) -> Self {
        Self {
            observer_name: observer_name.into(),
            kind,
            message: message.into(),
        }
    }

    /// Create a "failed" error.
    pub fn failed(observer_name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(observer_name, ObserverErrorKind::Failed, message)
    }

    /// Create a "panicked" error.
    pub fn panicked(observer_name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(observer_name, ObserverErrorKind::Panicked, message)
    }
}

impl fmt::Display for ObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "observer '{}' {}: {}",
            self.observer_name, self.kind, self.message
        )
    }
}

impl std::error::Error for ObserverError {}

// =============================================================================
// Commit Observer
// =============================================================================

/// Information about a committed transaction.
#[derive(Debug, Clone)]
pub struct CommitInfo {
    /// The branch that was committed to.
    pub branch_id: BranchId,
    /// The commit version assigned.
    pub commit_version: CommitVersion,
    /// Number of entries written.
    pub entry_count: usize,
    /// Whether this was a merge commit.
    pub is_merge: bool,
}

/// Observer called after each successful commit.
///
/// Use for audit logging, metrics, and other cross-cutting concerns that
/// only need generic commit information (branch_id, commit_version, entry_count).
///
/// ## Scope
///
/// `CommitObserver` receives only [`CommitInfo`], not the actual committed data.
/// For operation-specific deferred work (graph node indexing, vector HNSW updates)
/// that requires data captured during command execution, see `PostCommitOp` in
/// the executor crate's session module.
///
/// ## Thread Safety
///
/// Observers may be called concurrently from multiple threads.
/// Implementations must be `Send + Sync`.
///
/// ## Failure Model
///
/// If `on_commit` returns `Err`, the error is logged but the commit
/// is not rolled back (it already succeeded).
pub trait CommitObserver: Send + Sync + 'static {
    /// Human-readable name for logging.
    fn name(&self) -> &'static str;

    /// Called after a transaction commits successfully.
    ///
    /// This is called synchronously after the commit completes.
    /// Keep implementations fast to avoid blocking the commit path.
    fn on_commit(&self, info: &CommitInfo) -> Result<(), ObserverError>;
}

// =============================================================================
// Replay Observer
// =============================================================================

/// Information about a replayed WAL record.
#[derive(Debug, Clone)]
pub struct ReplayInfo {
    /// The branch the record was applied to.
    pub branch_id: BranchId,
    /// The commit version of the replayed record.
    pub commit_version: CommitVersion,
    /// Number of entries in the record.
    pub entry_count: usize,
}

/// Observer called after a follower applies a WAL record.
///
/// Use for index rebuild on replicas, replication metrics, etc.
///
/// ## When It Fires
///
/// Only on followers, after a WAL record has been fully applied to storage.
/// Does not fire on primaries (they use `CommitObserver` instead).
pub trait ReplayObserver: Send + Sync + 'static {
    /// Human-readable name for logging.
    fn name(&self) -> &'static str;

    /// Called after a WAL record is fully applied.
    fn on_replay(&self, info: &ReplayInfo) -> Result<(), ObserverError>;
}

// =============================================================================
// Branch Operation Observer
// =============================================================================

/// Type of branch operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BranchOpKind {
    /// Branch created.
    Create,
    /// Branch deleted.
    Delete,
    /// Branch forked from another.
    Fork,
    /// Branches merged.
    Merge,
    /// Version range reverted.
    Revert,
    /// Cherry-pick from another branch.
    CherryPick,
    /// Tag created.
    Tag,
    /// Tag removed.
    Untag,
}

impl fmt::Display for BranchOpKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Create => write!(f, "create"),
            Self::Delete => write!(f, "delete"),
            Self::Fork => write!(f, "fork"),
            Self::Merge => write!(f, "merge"),
            Self::Revert => write!(f, "revert"),
            Self::CherryPick => write!(f, "cherry_pick"),
            Self::Tag => write!(f, "tag"),
            Self::Untag => write!(f, "untag"),
        }
    }
}

/// Information about a branch operation.
#[derive(Debug, Clone)]
pub struct BranchOpEvent {
    /// What kind of operation.
    pub kind: BranchOpKind,
    /// The primary branch affected (created, deleted, target of merge, etc.).
    pub branch_id: BranchId,
    /// The branch name (if known).
    pub branch_name: Option<String>,
    /// Source branch for fork/merge/cherry-pick (if applicable).
    pub source_branch_id: Option<BranchId>,
    /// The commit version at which the operation occurred.
    pub commit_version: Option<CommitVersion>,
    /// Optional message (for fork, merge, revert).
    pub message: Option<String>,
    /// Optional creator identifier.
    pub creator: Option<String>,
}

impl BranchOpEvent {
    /// Create a simple event for branch create.
    pub fn create(branch_id: BranchId, branch_name: impl Into<String>) -> Self {
        Self {
            kind: BranchOpKind::Create,
            branch_id,
            branch_name: Some(branch_name.into()),
            source_branch_id: None,
            commit_version: None,
            message: None,
            creator: None,
        }
    }

    /// Create a simple event for branch delete.
    pub fn delete(branch_id: BranchId, branch_name: impl Into<String>) -> Self {
        Self {
            kind: BranchOpKind::Delete,
            branch_id,
            branch_name: Some(branch_name.into()),
            source_branch_id: None,
            commit_version: None,
            message: None,
            creator: None,
        }
    }

    /// Create an event for branch fork.
    pub fn fork(
        branch_id: BranchId,
        branch_name: impl Into<String>,
        source_branch_id: BranchId,
        commit_version: CommitVersion,
    ) -> Self {
        Self {
            kind: BranchOpKind::Fork,
            branch_id,
            branch_name: Some(branch_name.into()),
            source_branch_id: Some(source_branch_id),
            commit_version: Some(commit_version),
            message: None,
            creator: None,
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

/// Observer called after branch operations complete.
///
/// Use for branch audit logging, DAG updates, branch metrics, etc.
///
/// ## When It Fires
///
/// After `BranchService` methods complete successfully. Does not fire
/// if the operation fails.
pub trait BranchOpObserver: Send + Sync + 'static {
    /// Human-readable name for logging.
    fn name(&self) -> &'static str;

    /// Called after a branch operation completes.
    fn on_branch_op(&self, event: &BranchOpEvent) -> Result<(), ObserverError>;
}

// =============================================================================
// Registries
// =============================================================================

/// Registry for commit observers.
///
/// Thread-safe, allows concurrent registration and notification.
pub struct CommitObserverRegistry {
    observers: RwLock<Vec<Arc<dyn CommitObserver>>>,
}

impl CommitObserverRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            observers: RwLock::new(Vec::new()),
        }
    }

    /// Register a commit observer.
    pub fn register(&self, observer: Arc<dyn CommitObserver>) {
        self.observers.write().push(observer);
    }

    /// Notify all observers of a commit.
    ///
    /// Errors are logged but not propagated.
    pub fn notify(&self, info: &CommitInfo) {
        let observers = self.observers.read();
        for observer in observers.iter() {
            if let Err(e) = observer.on_commit(info) {
                tracing::error!(
                    observer = observer.name(),
                    error = %e,
                    "commit observer failed"
                );
            }
        }
    }

    /// Number of registered observers.
    pub fn len(&self) -> usize {
        self.observers.read().len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.observers.read().is_empty()
    }
}

impl Default for CommitObserverRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Registry for replay observers.
pub struct ReplayObserverRegistry {
    observers: RwLock<Vec<Arc<dyn ReplayObserver>>>,
}

impl ReplayObserverRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            observers: RwLock::new(Vec::new()),
        }
    }

    /// Register a replay observer.
    pub fn register(&self, observer: Arc<dyn ReplayObserver>) {
        self.observers.write().push(observer);
    }

    /// Notify all observers of a replay.
    pub fn notify(&self, info: &ReplayInfo) {
        let observers = self.observers.read();
        for observer in observers.iter() {
            if let Err(e) = observer.on_replay(info) {
                tracing::error!(
                    observer = observer.name(),
                    error = %e,
                    "replay observer failed"
                );
            }
        }
    }

    /// Number of registered observers.
    pub fn len(&self) -> usize {
        self.observers.read().len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.observers.read().is_empty()
    }
}

impl Default for ReplayObserverRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Registry for branch operation observers.
pub struct BranchOpObserverRegistry {
    observers: RwLock<Vec<Arc<dyn BranchOpObserver>>>,
}

impl BranchOpObserverRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            observers: RwLock::new(Vec::new()),
        }
    }

    /// Register a branch operation observer.
    pub fn register(&self, observer: Arc<dyn BranchOpObserver>) {
        self.observers.write().push(observer);
    }

    /// Notify all observers of a branch operation.
    pub fn notify(&self, event: &BranchOpEvent) {
        let observers = self.observers.read();
        for observer in observers.iter() {
            if let Err(e) = observer.on_branch_op(event) {
                tracing::error!(
                    observer = observer.name(),
                    error = %e,
                    "branch operation observer failed"
                );
            }
        }
    }

    /// Number of registered observers.
    pub fn len(&self) -> usize {
        self.observers.read().len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.observers.read().is_empty()
    }
}

impl Default for BranchOpObserverRegistry {
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingCommitObserver {
        count: AtomicUsize,
    }

    impl CountingCommitObserver {
        fn new() -> Self {
            Self {
                count: AtomicUsize::new(0),
            }
        }

        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    impl CommitObserver for CountingCommitObserver {
        fn name(&self) -> &'static str {
            "counting"
        }

        fn on_commit(&self, _info: &CommitInfo) -> Result<(), ObserverError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn test_commit_observer_registry() {
        let registry = CommitObserverRegistry::new();
        assert!(registry.is_empty());

        let observer = Arc::new(CountingCommitObserver::new());
        registry.register(observer.clone());
        assert_eq!(registry.len(), 1);

        let info = CommitInfo {
            branch_id: BranchId::new(),
            commit_version: CommitVersion(1),
            entry_count: 10,
            is_merge: false,
        };

        registry.notify(&info);
        assert_eq!(observer.count(), 1);

        registry.notify(&info);
        assert_eq!(observer.count(), 2);
    }

    #[test]
    fn test_branch_op_event_builders() {
        let branch_id = BranchId::new();
        let event = BranchOpEvent::create(branch_id, "main")
            .with_message("Initial branch")
            .with_creator("system");

        assert_eq!(event.kind, BranchOpKind::Create);
        assert_eq!(event.branch_name, Some("main".to_string()));
        assert_eq!(event.message, Some("Initial branch".to_string()));
        assert_eq!(event.creator, Some("system".to_string()));
    }

    #[test]
    fn test_observer_error() {
        let err = ObserverError::failed("test", "something went wrong");
        assert_eq!(err.observer_name, "test");
        assert_eq!(err.kind, ObserverErrorKind::Failed);
        assert!(err.to_string().contains("test"));
        assert!(err.to_string().contains("something went wrong"));
    }
}
