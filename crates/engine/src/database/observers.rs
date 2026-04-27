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
//! | `AbortObserver` | After transaction abort/failure | Cleanup staged runtime state |
//! | `ReplayObserver` | After follower applies record | Index rebuild on replica |
//! | `BranchOpObserver` | After branch operation | DAG audit, branch metrics |
//!
//! ## Thread Safety
//!
//! Registries use `RwLock` for safe concurrent access. Observers themselves
//! must be `Send + Sync` since they may be called from any thread.

use parking_lot::{Mutex, RwLock};
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use strata_core::contract::PrimitiveType;
use strata_core::id::{CommitVersion, TxnId};
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_core::{BranchRef, PrimitiveDegradedReason};
use strata_storage::Key;

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
    /// The transaction that committed.
    pub txn_id: TxnId,
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
/// that requires data captured during command execution, subsystem state should
/// be staged during command execution and drained by commit/abort observers.
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

/// Information about an aborted transaction.
#[derive(Debug, Clone)]
pub struct AbortInfo {
    /// The transaction that aborted.
    pub txn_id: TxnId,
    /// The branch the transaction belonged to.
    pub branch_id: BranchId,
}

/// Observer called after a transaction aborts or fails to commit.
///
/// Use for cleaning up subsystem-owned staged state that should only survive a
/// successful commit.
pub trait AbortObserver: Send + Sync + 'static {
    /// Human-readable name for logging.
    fn name(&self) -> &'static str;

    /// Called after the transaction has been marked aborted.
    fn on_abort(&self, info: &AbortInfo) -> Result<(), ObserverError>;
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
    /// Values written by the replayed record.
    pub puts: Vec<(Key, Value)>,
    /// Values that existed before deleted keys were replayed.
    pub deleted_values: Vec<(Key, Value)>,
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
///
/// B3.4 carries the generation-aware [`BranchRef`] alongside the legacy
/// `BranchId`. `branch_ref` is the canonical identity; `branch_id` is
/// retained as a back-compat shadow populated from `branch_ref.id` (full
/// removal lands in B4+ once observer callers no longer read `branch_id`).
#[derive(Debug, Clone)]
pub struct BranchOpEvent {
    /// What kind of operation.
    pub kind: BranchOpKind,
    /// The primary branch affected (created, deleted, target of merge, etc.).
    ///
    /// Back-compat shadow of `branch_ref.id` — same value, name-erased of
    /// generation. Prefer `branch_ref` in new code.
    pub branch_id: BranchId,
    /// Generation-aware identity of the primary branch (B3.4).
    ///
    /// Same lifecycle instance as `branch_id`, plus the [`BranchGeneration`]
    /// that distinguishes a recreated branch from its earlier instance.
    ///
    /// [`BranchGeneration`]: strata_core::BranchGeneration
    pub branch_ref: BranchRef,
    /// The branch name (if known).
    pub branch_name: Option<String>,
    /// Source branch for fork/merge/cherry-pick (if applicable).
    ///
    /// Back-compat shadow of `source_branch_ref.map(|r| r.id)`.
    pub source_branch_id: Option<BranchId>,
    /// Generation-aware source identity for fork/merge/cherry-pick (B3.4).
    pub source_branch_ref: Option<BranchRef>,
    /// Source branch name (for fork: parent, for merge/cherry-pick: source).
    pub source_branch_name: Option<String>,
    /// The commit version at which the operation occurred.
    pub commit_version: Option<CommitVersion>,
    /// Tag name for tag/untag operations.
    pub tag_name: Option<String>,
    /// Optional message (for fork, merge, revert).
    pub message: Option<String>,
    /// Optional creator identifier.
    pub creator: Option<String>,
    /// Merge strategy (for merge operations).
    pub merge_strategy: Option<String>,
    /// Number of keys applied (for merge, cherry-pick).
    pub keys_applied: Option<u64>,
    /// Number of keys deleted (for merge, cherry-pick).
    pub keys_deleted: Option<u64>,
    /// Number of keys reverted (for revert).
    pub keys_reverted: Option<u64>,
    /// Start version for revert range.
    pub from_version: Option<CommitVersion>,
    /// End version for revert range.
    pub to_version: Option<CommitVersion>,
}

impl BranchOpEvent {
    /// Create a simple event for branch create.
    ///
    /// `branch_ref` carries the generation-aware identity of the new
    /// lifecycle instance; the legacy `branch_id` field is populated from
    /// `branch_ref.id`.
    pub fn create(branch_ref: BranchRef, branch_name: impl Into<String>) -> Self {
        Self {
            kind: BranchOpKind::Create,
            branch_id: branch_ref.id,
            branch_ref,
            branch_name: Some(branch_name.into()),
            source_branch_id: None,
            source_branch_ref: None,
            source_branch_name: None,
            commit_version: None,
            tag_name: None,
            message: None,
            creator: None,
            merge_strategy: None,
            keys_applied: None,
            keys_deleted: None,
            keys_reverted: None,
            from_version: None,
            to_version: None,
        }
    }

    /// Create a simple event for branch delete.
    ///
    /// `branch_ref` is the lifecycle instance being tombstoned.
    pub fn delete(branch_ref: BranchRef, branch_name: impl Into<String>) -> Self {
        Self {
            kind: BranchOpKind::Delete,
            branch_id: branch_ref.id,
            branch_ref,
            branch_name: Some(branch_name.into()),
            source_branch_id: None,
            source_branch_ref: None,
            source_branch_name: None,
            commit_version: None,
            tag_name: None,
            message: None,
            creator: None,
            merge_strategy: None,
            keys_applied: None,
            keys_deleted: None,
            keys_reverted: None,
            from_version: None,
            to_version: None,
        }
    }

    /// Create an event for branch fork.
    ///
    /// `branch_ref` is the freshly-created child; `source_branch_ref` is the
    /// parent's lifecycle instance at the fork point.
    pub fn fork(
        branch_ref: BranchRef,
        branch_name: impl Into<String>,
        source_branch_ref: BranchRef,
        source_branch_name: impl Into<String>,
        commit_version: CommitVersion,
    ) -> Self {
        Self {
            kind: BranchOpKind::Fork,
            branch_id: branch_ref.id,
            branch_ref,
            branch_name: Some(branch_name.into()),
            source_branch_id: Some(source_branch_ref.id),
            source_branch_ref: Some(source_branch_ref),
            source_branch_name: Some(source_branch_name.into()),
            commit_version: Some(commit_version),
            tag_name: None,
            message: None,
            creator: None,
            merge_strategy: None,
            keys_applied: None,
            keys_deleted: None,
            keys_reverted: None,
            from_version: None,
            to_version: None,
        }
    }

    /// Create an event for branch merge.
    pub fn merge(
        target_branch_ref: BranchRef,
        target_branch_name: impl Into<String>,
        source_branch_ref: BranchRef,
        source_branch_name: impl Into<String>,
        strategy: impl Into<String>,
        keys_applied: u64,
        keys_deleted: u64,
        merge_version: CommitVersion,
    ) -> Self {
        Self {
            kind: BranchOpKind::Merge,
            branch_id: target_branch_ref.id,
            branch_ref: target_branch_ref,
            branch_name: Some(target_branch_name.into()),
            source_branch_id: Some(source_branch_ref.id),
            source_branch_ref: Some(source_branch_ref),
            source_branch_name: Some(source_branch_name.into()),
            commit_version: Some(merge_version),
            tag_name: None,
            message: None,
            creator: None,
            merge_strategy: Some(strategy.into()),
            keys_applied: Some(keys_applied),
            keys_deleted: Some(keys_deleted),
            keys_reverted: None,
            from_version: None,
            to_version: None,
        }
    }

    /// Create an event for branch revert.
    pub fn revert(
        branch_ref: BranchRef,
        branch_name: impl Into<String>,
        from_version: CommitVersion,
        to_version: CommitVersion,
        keys_reverted: u64,
    ) -> Self {
        Self {
            kind: BranchOpKind::Revert,
            branch_id: branch_ref.id,
            branch_ref,
            branch_name: Some(branch_name.into()),
            source_branch_id: None,
            source_branch_ref: None,
            source_branch_name: None,
            commit_version: None,
            tag_name: None,
            message: None,
            creator: None,
            merge_strategy: None,
            keys_applied: None,
            keys_deleted: None,
            keys_reverted: Some(keys_reverted),
            from_version: Some(from_version),
            to_version: Some(to_version),
        }
    }

    /// Create an event for cherry-pick.
    pub fn cherry_pick(
        target_branch_ref: BranchRef,
        target_branch_name: impl Into<String>,
        source_branch_ref: BranchRef,
        source_branch_name: impl Into<String>,
        keys_applied: u64,
        keys_deleted: u64,
    ) -> Self {
        Self {
            kind: BranchOpKind::CherryPick,
            branch_id: target_branch_ref.id,
            branch_ref: target_branch_ref,
            branch_name: Some(target_branch_name.into()),
            source_branch_id: Some(source_branch_ref.id),
            source_branch_ref: Some(source_branch_ref),
            source_branch_name: Some(source_branch_name.into()),
            commit_version: None,
            tag_name: None,
            message: None,
            creator: None,
            merge_strategy: None,
            keys_applied: Some(keys_applied),
            keys_deleted: Some(keys_deleted),
            keys_reverted: None,
            from_version: None,
            to_version: None,
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

/// Registry for abort observers.
pub struct AbortObserverRegistry {
    observers: RwLock<Vec<Arc<dyn AbortObserver>>>,
}

impl AbortObserverRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            observers: RwLock::new(Vec::new()),
        }
    }

    /// Register an abort observer.
    pub fn register(&self, observer: Arc<dyn AbortObserver>) {
        self.observers.write().push(observer);
    }

    /// Notify all observers of an abort.
    pub fn notify(&self, info: &AbortInfo) {
        let observers = self.observers.read();
        for observer in observers.iter() {
            if let Err(e) = observer.on_abort(info) {
                tracing::error!(
                    observer = observer.name(),
                    error = %e,
                    "abort observer failed"
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

impl Default for AbortObserverRegistry {
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
// Primitive Degraded Observer (B5.4)
// =============================================================================

/// Event fired when a named primitive is marked as fail-closed degraded.
///
/// Per convergence doc §"Required push events", B5.4 must surface
/// fail-closed degraded primitive events on a push channel so operators
/// can route on the branch contract instead of log text. This event
/// carries the same fields as the registry entry in
/// `primitive_degradation::PrimitiveDegradationEntry`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PrimitiveDegradedEvent {
    /// Generation-aware branch identity at the time of degradation.
    pub branch_ref: BranchRef,
    /// Which primitive subsystem owns the degraded surface.
    pub primitive: PrimitiveType,
    /// Primitive-level name (collection, index space, etc.).
    pub name: String,
    /// Typed reason for the degradation.
    pub reason: PrimitiveDegradedReason,
    /// Operator-facing free-form detail (e.g. decode error message).
    pub detail: String,
    /// Wall-clock time the degradation was first marked.
    pub detected_at: SystemTime,
}

/// Observer called when a primitive is marked fail-closed degraded.
///
/// Fires at most once per `(BranchId, PrimitiveType, name)` key during
/// the lifetime of the `PrimitiveDegradationRegistry` (first-mark
/// wins, consistent with the registry's idempotency).
pub trait PrimitiveDegradedObserver: Send + Sync + 'static {
    /// Human-readable name for logging.
    fn name(&self) -> &'static str;

    /// Called after a primitive is marked fail-closed degraded.
    fn on_primitive_degraded(&self, event: &PrimitiveDegradedEvent) -> Result<(), ObserverError>;
}

/// Registry for primitive-degraded observers.
pub struct PrimitiveDegradedObserverRegistry {
    observers: RwLock<Vec<Arc<dyn PrimitiveDegradedObserver>>>,
}

impl PrimitiveDegradedObserverRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            observers: RwLock::new(Vec::new()),
        }
    }

    /// Register a primitive-degraded observer.
    pub fn register(&self, observer: Arc<dyn PrimitiveDegradedObserver>) {
        self.observers.write().push(observer);
    }

    /// Notify all observers of a primitive-degraded event.
    ///
    /// Errors are logged but not propagated (consistent with other
    /// observer registries at the top of this file).
    pub fn notify(&self, event: &PrimitiveDegradedEvent) {
        let observers = self.observers.read();
        for observer in observers.iter() {
            if let Err(e) = observer.on_primitive_degraded(event) {
                tracing::error!(
                    observer = observer.name(),
                    error = %e,
                    "primitive degraded observer failed"
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

impl Default for PrimitiveDegradedObserverRegistry {
    fn default() -> Self {
        Self::new()
    }
}

type PrimitiveDegradedReplayKey = (BranchRef, PrimitiveType, String);

/// Internal wrapper that deduplicates replayed and live degraded events.
///
/// `Database::register_primitive_degraded_observer()` uses this wrapper so a
/// recovery-time replay and a concurrent live degradation for the same
/// `(BranchId, PrimitiveType, name)` key cannot double-deliver to the caller.
pub(crate) struct ReplayDedupPrimitiveDegradedObserver {
    inner: Arc<dyn PrimitiveDegradedObserver>,
    seen: Mutex<HashSet<PrimitiveDegradedReplayKey>>,
}

impl ReplayDedupPrimitiveDegradedObserver {
    pub(crate) fn new(inner: Arc<dyn PrimitiveDegradedObserver>) -> Self {
        Self {
            inner,
            seen: Mutex::new(HashSet::new()),
        }
    }
}

impl PrimitiveDegradedObserver for ReplayDedupPrimitiveDegradedObserver {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn on_primitive_degraded(&self, event: &PrimitiveDegradedEvent) -> Result<(), ObserverError> {
        let key = (event.branch_ref, event.primitive, event.name.clone());
        if !self.seen.lock().insert(key) {
            return Ok(());
        }
        self.inner.on_primitive_degraded(event)
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

    struct CountingAbortObserver {
        count: AtomicUsize,
    }

    impl CountingAbortObserver {
        fn new() -> Self {
            Self {
                count: AtomicUsize::new(0),
            }
        }

        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    impl AbortObserver for CountingAbortObserver {
        fn name(&self) -> &'static str {
            "counting-abort"
        }

        fn on_abort(&self, _info: &AbortInfo) -> Result<(), ObserverError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct CountingPrimitiveObserver {
        count: AtomicUsize,
    }

    impl CountingPrimitiveObserver {
        fn new() -> Self {
            Self {
                count: AtomicUsize::new(0),
            }
        }

        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    impl PrimitiveDegradedObserver for CountingPrimitiveObserver {
        fn name(&self) -> &'static str {
            "counting-primitive"
        }

        fn on_primitive_degraded(
            &self,
            _event: &PrimitiveDegradedEvent,
        ) -> Result<(), ObserverError> {
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
            txn_id: 7u64.into(),
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
    fn test_abort_observer_registry() {
        let registry = AbortObserverRegistry::new();
        assert!(registry.is_empty());

        let observer = Arc::new(CountingAbortObserver::new());
        registry.register(observer.clone());
        assert_eq!(registry.len(), 1);

        let info = AbortInfo {
            txn_id: 9u64.into(),
            branch_id: BranchId::new(),
        };

        registry.notify(&info);
        assert_eq!(observer.count(), 1);
    }

    #[test]
    fn test_primitive_replay_wrapper_dedupes_duplicate_delivery() {
        let inner = Arc::new(CountingPrimitiveObserver::new());
        let wrapper = ReplayDedupPrimitiveDegradedObserver::new(
            inner.clone() as Arc<dyn PrimitiveDegradedObserver>
        );

        let branch_ref = BranchRef::new(BranchId::new(), 7);
        let event = PrimitiveDegradedEvent {
            branch_ref,
            primitive: PrimitiveType::Vector,
            name: "v1".to_string(),
            reason: PrimitiveDegradedReason::ConfigMismatch,
            detail: "corrupt row".to_string(),
            detected_at: SystemTime::now(),
        };

        wrapper.on_primitive_degraded(&event).unwrap();
        wrapper.on_primitive_degraded(&event).unwrap();
        assert_eq!(
            inner.count(),
            1,
            "same degraded lifecycle key must be delivered at most once"
        );

        let event2 = PrimitiveDegradedEvent {
            branch_ref: BranchRef::new(branch_ref.id, branch_ref.generation + 1),
            ..event
        };
        wrapper.on_primitive_degraded(&event2).unwrap();
        assert_eq!(
            inner.count(),
            2,
            "same-name degrade on a new generation must still be delivered"
        );
    }

    #[test]
    fn test_branch_op_event_builders() {
        let branch_id = BranchId::new();
        let branch_ref = BranchRef::new(branch_id, 0);
        let event = BranchOpEvent::create(branch_ref, "main")
            .with_message("Initial branch")
            .with_creator("system");

        assert_eq!(event.kind, BranchOpKind::Create);
        assert_eq!(event.branch_id, branch_id);
        assert_eq!(event.branch_ref, branch_ref);
        assert_eq!(event.branch_name, Some("main".to_string()));
        assert_eq!(event.message, Some("Initial branch".to_string()));
        assert_eq!(event.creator, Some("system".to_string()));
    }

    #[test]
    fn test_branch_op_event_legacy_branch_id_mirrors_ref() {
        let branch_id = BranchId::new();
        let source_id = BranchId::new();
        let branch_ref = BranchRef::new(branch_id, 4);
        let source_ref = BranchRef::new(source_id, 2);

        let create = BranchOpEvent::create(branch_ref, "feature");
        assert_eq!(create.branch_id, branch_ref.id);
        assert_eq!(create.branch_ref, branch_ref);
        assert!(create.source_branch_ref.is_none());

        let fork = BranchOpEvent::fork(branch_ref, "feature", source_ref, "main", CommitVersion(7));
        assert_eq!(fork.branch_id, branch_ref.id);
        assert_eq!(fork.source_branch_id, Some(source_ref.id));
        assert_eq!(fork.source_branch_ref, Some(source_ref));

        let merge = BranchOpEvent::merge(
            branch_ref,
            "main",
            source_ref,
            "feature",
            "last_writer_wins",
            3,
            1,
            CommitVersion(11),
        );
        assert_eq!(merge.branch_ref, branch_ref);
        assert_eq!(merge.source_branch_ref, Some(source_ref));
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
