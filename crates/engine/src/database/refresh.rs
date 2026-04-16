//! Follower refresh types and traits.
//!
//! This module provides the follower refresh infrastructure:
//!
//! - `RefreshHook` trait: secondary index subsystems implement this to participate
//!   in incremental follower refresh
//! - `RefreshOutcome`: structured result from `Database::refresh()`
//! - `FollowerStatus`: current follower state including watermarks
//! - `ContiguousWatermark`: internal type enforcing contiguous advancement
//!
//! ## Usage
//!
//! 1. Implement `RefreshHook` for your subsystem's state type
//! 2. Register the state as a Database extension (`db.extension::<MyState>()`)
//! 3. Register the hook with `db.register_refresh_hook()`
//! 4. `Database::refresh()` will call your hook automatically

use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use strata_core::id::{CommitVersion, TxnId};
use strata_core::types::Key;
use strata_core::value::Value;
use strata_core::StrataResult;

// =============================================================================
// Error Types
// =============================================================================

/// Error returned by a fallible `RefreshHook::apply_refresh`.
///
/// Implementations should provide enough context for operators to diagnose
/// why secondary index maintenance failed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RefreshHookError {
    /// Hook name (e.g., "vector", "search").
    pub hook_name: String,
    /// Human-readable error message.
    pub message: String,
}

impl RefreshHookError {
    /// Create a new hook error.
    pub fn new(hook_name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            hook_name: hook_name.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for RefreshHookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} hook failed: {}", self.hook_name, self.message)
    }
}

impl std::error::Error for RefreshHookError {}

/// Reason why follower refresh is blocked at a specific transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BlockReason {
    /// WAL record could not be decoded (corrupt or incompatible format).
    Decode {
        /// Human-readable decode failure detail.
        message: String,
    },
    /// Codec mismatch between WAL record and database.
    Codec {
        /// Expected codec identifier.
        expected: String,
        /// Actual codec identifier observed while decoding.
        actual: String,
    },
    /// Storage layer rejected the mutation.
    StorageApply {
        /// Human-readable storage failure detail.
        message: String,
    },
    /// A secondary index hook (vector, search, etc.) failed.
    SecondaryIndex {
        /// Name of the failed refresh hook.
        hook_name: String,
        /// Human-readable hook failure detail.
        message: String,
    },
}

impl fmt::Display for BlockReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockReason::Decode { message } => write!(f, "decode error: {}", message),
            BlockReason::Codec { expected, actual } => {
                write!(f, "codec mismatch: expected {}, got {}", expected, actual)
            }
            BlockReason::StorageApply { message } => write!(f, "storage apply error: {}", message),
            BlockReason::SecondaryIndex { hook_name, message } => {
                write!(f, "{} hook failed: {}", hook_name, message)
            }
        }
    }
}

/// Information about a blocked transaction during follower refresh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockedTxn {
    /// Transaction ID that caused the block.
    pub txn_id: TxnId,
    /// Reason for the block.
    pub reason: BlockReason,
}

impl fmt::Display for BlockedTxn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "blocked at {}: {}", self.txn_id, self.reason)
    }
}

/// Outcome of a follower refresh operation.
///
/// This type is `#[must_use]` — callers must inspect the result to determine
/// whether refresh succeeded, partially succeeded, or made no progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[must_use]
#[non_exhaustive]
pub enum RefreshOutcome {
    /// Refresh caught up with all available records.
    CaughtUp {
        /// Number of records applied in this refresh call.
        applied: usize,
        /// Highest contiguously applied transaction ID.
        applied_through: TxnId,
    },
    /// Refresh applied some records but is now stuck.
    Stuck {
        /// Number of records applied before hitting the block.
        applied: usize,
        /// Highest contiguously applied transaction ID.
        applied_through: TxnId,
        /// Information about the blocked transaction.
        blocked_at: BlockedTxn,
    },
    /// Refresh was already blocked and made no progress.
    NoProgress {
        /// Highest contiguously applied transaction ID.
        applied_through: TxnId,
        /// Information about the blocked transaction.
        blocked_at: BlockedTxn,
    },
}

impl RefreshOutcome {
    /// Returns `true` if refresh caught up with all available records.
    pub fn is_caught_up(&self) -> bool {
        matches!(self, RefreshOutcome::CaughtUp { .. })
    }

    /// Returns the number of records applied in this refresh call.
    pub fn applied_count(&self) -> usize {
        match self {
            RefreshOutcome::CaughtUp { applied, .. } => *applied,
            RefreshOutcome::Stuck { applied, .. } => *applied,
            RefreshOutcome::NoProgress { .. } => 0,
        }
    }

    /// Returns the highest contiguously applied transaction ID.
    pub fn applied_through(&self) -> TxnId {
        match self {
            RefreshOutcome::CaughtUp {
                applied_through, ..
            } => *applied_through,
            RefreshOutcome::Stuck {
                applied_through, ..
            } => *applied_through,
            RefreshOutcome::NoProgress {
                applied_through, ..
            } => *applied_through,
        }
    }

    /// Returns the blocked transaction info if refresh is stuck or made no progress.
    pub fn blocked_at(&self) -> Option<&BlockedTxn> {
        match self {
            RefreshOutcome::CaughtUp { .. } => None,
            RefreshOutcome::Stuck { blocked_at, .. } => Some(blocked_at),
            RefreshOutcome::NoProgress { blocked_at, .. } => Some(blocked_at),
        }
    }
}

impl fmt::Display for RefreshOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RefreshOutcome::CaughtUp {
                applied,
                applied_through,
            } => {
                write!(
                    f,
                    "caught up: {} records applied through {}",
                    applied, applied_through
                )
            }
            RefreshOutcome::Stuck {
                applied,
                applied_through,
                blocked_at,
            } => {
                write!(
                    f,
                    "stuck: {} records applied through {}, {}",
                    applied, applied_through, blocked_at
                )
            }
            RefreshOutcome::NoProgress {
                applied_through,
                blocked_at,
            } => {
                write!(
                    f,
                    "no progress: still at {}, {}",
                    applied_through, blocked_at
                )
            }
        }
    }
}

/// Error returned when trying to unblock a follower.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum UnblockError {
    /// The provided transaction ID doesn't match the blocked transaction.
    Mismatch {
        /// Transaction ID currently blocking refresh.
        expected: TxnId,
        /// Transaction ID supplied by the operator.
        provided: TxnId,
    },
    /// The follower is not currently blocked.
    NotBlocked,
    /// The current blocked record cannot be skipped safely.
    NotSkippable {
        /// Transaction ID that remains non-skippable.
        txn_id: TxnId,
    },
    /// This database is not a follower.
    NotFollower,
}

impl fmt::Display for UnblockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnblockError::Mismatch { expected, provided } => {
                write!(
                    f,
                    "unblock mismatch: blocked at {}, but {} was provided",
                    expected, provided
                )
            }
            UnblockError::NotBlocked => write!(f, "follower is not blocked"),
            UnblockError::NotSkippable { txn_id } => {
                write!(
                    f,
                    "blocked txn {} cannot be skipped safely; manual repair is required",
                    txn_id
                )
            }
            UnblockError::NotFollower => write!(f, "this database is not a follower"),
        }
    }
}

impl std::error::Error for UnblockError {}

/// Error returned when watermark advancement fails.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AdvanceError {
    /// Cannot advance: follower is blocked.
    Blocked {
        /// Transaction currently blocking contiguous advancement.
        blocked_at: TxnId,
    },
    /// Cannot advance: not a follower database.
    NotFollower,
}

impl fmt::Display for AdvanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdvanceError::Blocked { blocked_at } => {
                write!(f, "cannot advance: blocked at {}", blocked_at)
            }
            AdvanceError::NotFollower => write!(f, "cannot advance: not a follower"),
        }
    }
}

impl std::error::Error for AdvanceError {}

// =============================================================================
// Follower Status
// =============================================================================

/// Current status of a follower database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowerStatus {
    /// Highest transaction ID received from the WAL (may include unprocessed records).
    pub received_watermark: TxnId,
    /// Highest transaction ID contiguously applied to storage and secondary indexes.
    pub applied_watermark: TxnId,
    /// If blocked, information about the blocking transaction.
    pub blocked_at: Option<BlockedTxn>,
    /// Whether a refresh is currently in progress.
    pub refresh_in_progress: bool,
}

impl FollowerStatus {
    /// Returns `true` if the follower is blocked.
    pub fn is_blocked(&self) -> bool {
        self.blocked_at.is_some()
    }

    /// Returns `true` if there are pending records to apply.
    pub fn has_pending(&self) -> bool {
        self.received_watermark > self.applied_watermark
    }
}

impl fmt::Display for FollowerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FollowerStatus {{ received: {}, applied: {}",
            self.received_watermark, self.applied_watermark
        )?;
        if let Some(blocked) = &self.blocked_at {
            write!(f, ", blocked: {}", blocked)?;
        }
        if self.refresh_in_progress {
            write!(f, ", refresh_in_progress")?;
        }
        write!(f, " }}")
    }
}

// =============================================================================
// Internal Types
// =============================================================================

/// Internal state for contiguous watermark tracking.
///
/// This type ensures that the applied watermark can only advance contiguously
/// — skipping is only possible through the explicit admin skip API.
pub(crate) struct ContiguousWatermark {
    /// Highest transaction ID received from WAL (telemetry only).
    received: AtomicU64,
    /// Highest contiguously applied transaction ID.
    applied: AtomicU64,
    /// If set, the transaction that is blocking progress.
    blocked: parking_lot::RwLock<Option<BlockedTxnState>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BlockedTxnState {
    pub blocked: BlockedTxn,
    pub visibility_version: Option<CommitVersion>,
    pub skip_allowed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistedFollowerState {
    pub received_watermark: TxnId,
    pub applied_watermark: TxnId,
    pub visible_version: CommitVersion,
    pub blocked: BlockedTxnState,
}

pub(crate) const FOLLOWER_STATE_FILE: &str = "follower_state.json";

pub(crate) fn follower_state_path(data_dir: &Path) -> Option<PathBuf> {
    if data_dir.as_os_str().is_empty() {
        None
    } else {
        Some(data_dir.join(FOLLOWER_STATE_FILE))
    }
}

pub(crate) fn load_persisted_follower_state(
    data_dir: &Path,
) -> std::io::Result<Option<PersistedFollowerState>> {
    let Some(path) = follower_state_path(data_dir) else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub(crate) fn persist_follower_state(
    data_dir: &Path,
    state: &PersistedFollowerState,
) -> std::io::Result<()> {
    let Some(path) = follower_state_path(data_dir) else {
        return Ok(());
    };
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    std::fs::create_dir_all(parent)?;
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

pub(crate) fn clear_persisted_follower_state(data_dir: &Path) -> std::io::Result<()> {
    let Some(path) = follower_state_path(data_dir) else {
        return Ok(());
    };
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

impl ContiguousWatermark {
    /// Create a new watermark initialized to the given value.
    pub(crate) fn new(initial: TxnId) -> Self {
        Self {
            received: AtomicU64::new(initial.as_u64()),
            applied: AtomicU64::new(initial.as_u64()),
            blocked: parking_lot::RwLock::new(None),
        }
    }

    /// Create watermark state from explicit values (used when restoring a blocked follower).
    pub(crate) fn from_state(
        received: TxnId,
        applied: TxnId,
        blocked: Option<BlockedTxnState>,
    ) -> Self {
        Self {
            received: AtomicU64::new(received.as_u64()),
            applied: AtomicU64::new(applied.as_u64()),
            blocked: parking_lot::RwLock::new(blocked),
        }
    }

    /// Get the received watermark.
    pub(crate) fn received(&self) -> TxnId {
        TxnId(self.received.load(Ordering::SeqCst))
    }

    /// Get the applied watermark.
    pub(crate) fn applied(&self) -> TxnId {
        TxnId(self.applied.load(Ordering::SeqCst))
    }

    /// Get the blocked transaction if any.
    pub(crate) fn blocked(&self) -> Option<BlockedTxn> {
        self.blocked
            .read()
            .as_ref()
            .map(|state| state.blocked.clone())
    }

    /// Snapshot the full blocked state for persistence.
    pub(crate) fn blocked_state(&self) -> Option<BlockedTxnState> {
        self.blocked.read().clone()
    }

    /// Update the received watermark (telemetry only, does not affect visibility).
    pub(crate) fn set_received(&self, txn_id: TxnId) {
        self.received.fetch_max(txn_id.as_u64(), Ordering::SeqCst);
    }

    /// Advance the applied watermark after successful processing.
    ///
    /// This should only be called after storage apply AND all hooks succeed.
    pub(crate) fn try_advance(&self, txn_id: TxnId) -> Result<(), AdvanceError> {
        if let Some(blocked) = self.blocked.read().as_ref() {
            return Err(AdvanceError::Blocked {
                blocked_at: blocked.blocked.txn_id,
            });
        }
        self.applied.fetch_max(txn_id.as_u64(), Ordering::SeqCst);
        Ok(())
    }

    /// Mark the watermark as blocked at a specific transaction.
    pub(crate) fn block_at(&self, blocked: BlockedTxnState) {
        *self.blocked.write() = Some(blocked);
    }

    /// Admin skip: advance past the blocked transaction.
    ///
    /// Returns an error if the provided txn_id doesn't match the blocked transaction.
    pub(crate) fn unblock_exact(&self, txn_id: TxnId) -> Result<BlockedTxnState, UnblockError> {
        let mut blocked = self.blocked.write();
        match &*blocked {
            Some(state) if state.blocked.txn_id != txn_id => Err(UnblockError::Mismatch {
                expected: state.blocked.txn_id,
                provided: txn_id,
            }),
            Some(state) if !state.skip_allowed => Err(UnblockError::NotSkippable { txn_id }),
            Some(state) => {
                let state = state.clone();
                self.received.fetch_max(txn_id.as_u64(), Ordering::SeqCst);
                self.applied.fetch_max(txn_id.as_u64(), Ordering::SeqCst);
                *blocked = None;
                Ok(state)
            }
            None => Err(UnblockError::NotBlocked),
        }
    }
}

impl Default for ContiguousWatermark {
    fn default() -> Self {
        Self::new(TxnId::ZERO)
    }
}

/// Single-flight gate for refresh operations.
///
/// Ensures only one refresh can be in progress at a time per follower database.
pub(crate) struct RefreshGate {
    lock: parking_lot::Mutex<()>,
    in_progress: AtomicBool,
}

impl RefreshGate {
    pub(crate) fn new() -> Self {
        Self {
            lock: parking_lot::Mutex::new(()),
            in_progress: AtomicBool::new(false),
        }
    }

    /// Check if refresh is in progress.
    pub(crate) fn is_in_progress(&self) -> bool {
        self.in_progress.load(Ordering::SeqCst)
    }
}

impl Default for RefreshGate {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for the refresh gate.
pub(crate) struct RefreshGuard<'a> {
    gate: &'a RefreshGate,
    _guard: parking_lot::MutexGuard<'a, ()>,
}

impl<'a> RefreshGuard<'a> {
    /// Acquire the gate, blocking until the active refresh completes.
    pub(crate) fn new(gate: &'a RefreshGate) -> Self {
        let guard = gate.lock.lock();
        gate.in_progress.store(true, Ordering::SeqCst);
        Self {
            gate,
            _guard: guard,
        }
    }
}

impl Drop for RefreshGuard<'_> {
    fn drop(&mut self) {
        self.gate.in_progress.store(false, Ordering::SeqCst);
    }
}

// =============================================================================
// Refresh Hook Trait
// =============================================================================

/// Pending refresh work prepared by a [`RefreshHook`].
///
/// Hooks validate and stage derived-state updates before the contiguous
/// watermark advances. The engine publishes the prepared updates only after it
/// has exclusive access to the follower's query/publish barrier, preventing
/// search/vector/graph readers from observing pre-visibility state.
pub trait PreparedRefresh: Send {
    /// Publish the staged derived-state changes.
    fn publish(self: Box<Self>);
}

/// No-op prepared refresh for hooks with nothing to publish.
pub struct NoopPreparedRefresh;

impl PreparedRefresh for NoopPreparedRefresh {
    fn publish(self: Box<Self>) {}
}

/// Trait for secondary index backends that participate in follower refresh.
///
/// Implementations handle incremental updates from WAL replay during
/// `Database::refresh()`. The engine calls hooks in two phases:
///
/// 1. `pre_delete_read`: Before storage mutations, read state needed for deletes
/// 2. `apply_refresh`: After storage mutations, validate and stage puts/deletes
///
/// ## Fallibility
///
/// `apply_refresh` returns staged work as `Result<Box<dyn PreparedRefresh>,
/// RefreshHookError>`. If any hook fails, refresh blocks at that transaction
/// and returns `RefreshOutcome::Stuck`. The contiguous watermark does NOT
/// advance past the failed transaction, and no staged derived-state updates are
/// published.
///
/// Operators can inspect `FollowerStatus::blocked_at` to diagnose the issue
/// and use `Database::admin_skip_blocked_record()` to skip past the problematic
/// transaction after manual intervention.
pub trait RefreshHook: Send + Sync + 'static {
    /// Return the hook's name for error reporting.
    fn name(&self) -> &'static str;

    /// Pre-read any state needed for processing deletes.
    ///
    /// Called BEFORE storage mutations are applied. Returns opaque pre-read
    /// data that will be passed to `apply_refresh`. The storage still has
    /// the old values at this point.
    fn pre_delete_read(&self, db: &super::Database, deletes: &[Key]) -> Vec<(Key, Vec<u8>)>;

    /// Validate and stage puts and deletes from a single WAL record.
    ///
    /// Called AFTER storage mutations are applied. Returns staged publication
    /// work on success, or `Err(RefreshHookError)` if secondary index
    /// maintenance failed.
    ///
    /// On error, the contiguous watermark does NOT advance past this transaction.
    fn apply_refresh(
        &self,
        puts: &[(Key, Value)],
        pre_read_deletes: &[(Key, Vec<u8>)],
    ) -> Result<Box<dyn PreparedRefresh>, RefreshHookError>;

    /// Freeze in-memory state to disk for fast recovery on next open.
    fn freeze_to_disk(&self, db: &super::Database) -> StrataResult<()>;

    /// Reload in-memory state after a branch merge.
    ///
    /// Called after branch merge completes to reload vector backends
    /// for the target branch from KV storage.
    fn post_merge_reload(
        &self,
        _db: &super::Database,
        _target_branch: strata_core::types::BranchId,
        _source_branch: Option<strata_core::types::BranchId>,
    ) -> StrataResult<()> {
        Ok(())
    }
}

/// Container for registered refresh hooks.
///
/// Stored as a Database extension and accessed during `refresh()`.
pub struct RefreshHooks {
    hooks: parking_lot::RwLock<Vec<Arc<dyn RefreshHook>>>,
}

impl Default for RefreshHooks {
    fn default() -> Self {
        Self {
            hooks: parking_lot::RwLock::new(Vec::new()),
        }
    }
}

impl RefreshHooks {
    /// Register a new refresh hook.
    pub fn register(&self, hook: Arc<dyn RefreshHook>) {
        self.hooks.write().push(hook);
    }

    /// Get all registered hooks.
    pub fn hooks(&self) -> Vec<Arc<dyn RefreshHook>> {
        self.hooks.read().clone()
    }
}
