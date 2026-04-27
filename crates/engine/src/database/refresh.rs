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

use crate::StrataResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use strata_core::id::{CommitVersion, TxnId};
use strata_core::value::Value;
use strata_storage::Key;

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
    /// Storage and hook staging succeeded, but the follower could not advance
    /// its own internal invariants to match the applied record.
    PostApplyInvariant {
        /// Human-readable post-apply failure detail.
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
            BlockReason::PostApplyInvariant { message } => {
                write!(f, "post-apply invariant failure: {}", message)
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
    /// The database has been shut down.
    DatabaseClosed,
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
            UnblockError::DatabaseClosed => {
                write!(f, "database has been shut down; admin skip rejected")
            }
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
    /// Cannot advance: provided txn id is not the next contiguous id.
    ///
    /// The watermark advances only through strictly contiguous successful
    /// records. A caller that supplies a gap or a duplicate is rejected.
    NonContiguous {
        /// The next txn id the watermark was ready to accept.
        expected: TxnId,
        /// The txn id the caller tried to advance to.
        provided: TxnId,
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
            AdvanceError::NonContiguous { expected, provided } => {
                write!(
                    f,
                    "cannot advance: non-contiguous txn id (expected {}, provided {})",
                    expected, provided
                )
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

/// Typed rejection reason when a persisted `PersistedFollowerState` fails
/// the semantic coherence check run at follower open.
///
/// A rejection never fails the open — the follower falls back to a fresh
/// watermark at the recovered max txn id — but the reason is surfaced via
/// tracing so an operator can diagnose whether the file was hand-edited,
/// truncated across a restart, or left behind by a previous binary.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum BlockedStateValidationError {
    /// `applied_watermark > received_watermark` — watermark pair is inverted.
    AppliedExceedsReceived { applied: TxnId, received: TxnId },
    /// `received_watermark` is beyond what recovery could reconstruct from
    /// on-disk WAL + snapshot.
    ReceivedExceedsRecovered {
        received: TxnId,
        recovered_max: TxnId,
    },
    /// Persisted visible version is beyond what recovery reconstructed.
    VisibleExceedsRecovered {
        visible: CommitVersion,
        recovered: CommitVersion,
    },
    /// `blocked.txn_id <= applied_watermark` — a blocked record must be
    /// strictly ahead of the last applied record; otherwise refresh has
    /// no resume point.
    BlockedNotAheadOfApplied { blocked: TxnId, applied: TxnId },
    /// `blocked.txn_id > received_watermark` — block references a record
    /// the follower never received from the WAL.
    BlockedBeyondReceived { blocked: TxnId, received: TxnId },
    /// `visibility_version` is `Some(v)` with `v > recovered_final_version`.
    /// When refresh fails mid-record after storage apply, the blocked state
    /// captures the commit version that `admin_skip_blocked_record` will
    /// advance visibility to; that version cannot exceed what recovery
    /// reconstructed, or a later skip would advance visibility past
    /// storage's actual high water mark.
    VisibilityVersionExceedsRecovered {
        visibility_version: CommitVersion,
        recovered: CommitVersion,
    },
    /// `BlockReason` fields are internally inconsistent.
    IncoherentBlockReason(BlockReasonIncoherence),
}

/// Sub-classification of incoherent `BlockReason` fields.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum BlockReasonIncoherence {
    /// `Decode { message }` / `StorageApply { message }` with empty message.
    EmptyMessage,
    /// `SecondaryIndex { hook_name, .. }` with empty hook name.
    EmptyHookName,
    /// A post-apply block reason's visibility floor does not advance readers
    /// beyond the currently persisted visible version.
    VisibilityVersionNotAheadOfVisible {
        reason: &'static str,
        visibility_version: CommitVersion,
        visible_version: CommitVersion,
    },
    /// A block reason that requires a post-apply visibility floor omitted it.
    MissingVisibilityVersion { reason: &'static str },
    /// A block reason that requires a post-apply visibility floor used zero.
    ZeroVisibilityVersion { reason: &'static str },
    /// A pre-apply block reason carried a visibility floor even though no
    /// committed storage state should be made visible on skip.
    UnexpectedVisibilityVersion { reason: &'static str },
    /// A block reason that should remain non-skippable was persisted as skippable.
    UnexpectedSkippableState { reason: &'static str },
    /// A block reason that should be operator-skippable was persisted as non-skippable.
    UnexpectedNonSkippableState { reason: &'static str },
    /// `Codec { expected, actual }` with either side empty.
    EmptyCodecId,
    /// `Codec { expected, actual }` where both are the same non-empty id —
    /// a "mismatch" can never have `expected == actual`.
    CodecExpectedEqualsActual { codec: String },
}

impl fmt::Display for BlockedStateValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockedStateValidationError::AppliedExceedsReceived { applied, received } => {
                write!(
                    f,
                    "applied_watermark {applied} exceeds received_watermark {received}"
                )
            }
            BlockedStateValidationError::ReceivedExceedsRecovered {
                received,
                recovered_max,
            } => write!(
                f,
                "received_watermark {received} exceeds recovered max txn id {recovered_max}"
            ),
            BlockedStateValidationError::VisibleExceedsRecovered { visible, recovered } => write!(
                f,
                "visible_version {} exceeds recovered final version {}",
                visible.as_u64(),
                recovered.as_u64()
            ),
            BlockedStateValidationError::BlockedNotAheadOfApplied { blocked, applied } => write!(
                f,
                "blocked txn {blocked} is not ahead of applied_watermark {applied}"
            ),
            BlockedStateValidationError::BlockedBeyondReceived { blocked, received } => write!(
                f,
                "blocked txn {blocked} exceeds received_watermark {received}"
            ),
            BlockedStateValidationError::VisibilityVersionExceedsRecovered {
                visibility_version,
                recovered,
            } => write!(
                f,
                "blocked visibility_version {} exceeds recovered final version {}",
                visibility_version.as_u64(),
                recovered.as_u64()
            ),
            BlockedStateValidationError::IncoherentBlockReason(inc) => match inc {
                BlockReasonIncoherence::EmptyMessage => write!(f, "block reason has empty message"),
                BlockReasonIncoherence::EmptyHookName => {
                    write!(f, "secondary-index block reason has empty hook name")
                }
                BlockReasonIncoherence::VisibilityVersionNotAheadOfVisible {
                    reason,
                    visibility_version,
                    visible_version,
                } => write!(
                    f,
                    "{reason} block reason has visibility_version {} which is not ahead of visible_version {}",
                    visibility_version.as_u64(),
                    visible_version.as_u64()
                ),
                BlockReasonIncoherence::MissingVisibilityVersion { reason } => {
                    write!(f, "{reason} block reason is missing visibility_version")
                }
                BlockReasonIncoherence::ZeroVisibilityVersion { reason } => {
                    write!(f, "{reason} block reason has zero visibility_version")
                }
                BlockReasonIncoherence::UnexpectedVisibilityVersion { reason } => write!(
                    f,
                    "{reason} block reason unexpectedly carries visibility_version"
                ),
                BlockReasonIncoherence::UnexpectedSkippableState { reason } => {
                    write!(f, "{reason} block reason unexpectedly allows operator skip")
                }
                BlockReasonIncoherence::UnexpectedNonSkippableState { reason } => write!(
                    f,
                    "{reason} block reason unexpectedly rejects operator skip"
                ),
                BlockReasonIncoherence::EmptyCodecId => {
                    write!(f, "codec-mismatch block reason has empty codec id")
                }
                BlockReasonIncoherence::CodecExpectedEqualsActual { codec } => write!(
                    f,
                    "codec-mismatch block reason has expected == actual = {codec}"
                ),
            },
        }
    }
}

/// Validate a persisted `PersistedFollowerState` against the watermarks
/// recovery just reconstructed.
///
/// This enforces the watermark pair invariants and the `BlockedTxnState`
/// semantic invariants that the on-disk file format by itself cannot pin:
/// serde round-trips any numerically-typed field without complaint, so
/// `blocked.txn_id == 0` on a follower that applied 100 records would
/// survive deserialization. Without this helper the first `refresh()`
/// after restart would return `RefreshOutcome::NoProgress { blocked_at: 0 }`
/// and silently pin the follower behind a record it already applied.
///
/// On rejection the caller is expected to fall back to a fresh watermark
/// at the recovered max txn id and warn with the typed reason.
pub(crate) fn validate_blocked_state(
    state: &PersistedFollowerState,
    recovered_max_txn_id: TxnId,
    recovered_final_version: CommitVersion,
) -> Result<(), BlockedStateValidationError> {
    if state.applied_watermark > state.received_watermark {
        return Err(BlockedStateValidationError::AppliedExceedsReceived {
            applied: state.applied_watermark,
            received: state.received_watermark,
        });
    }
    if state.received_watermark > recovered_max_txn_id {
        return Err(BlockedStateValidationError::ReceivedExceedsRecovered {
            received: state.received_watermark,
            recovered_max: recovered_max_txn_id,
        });
    }
    if state.visible_version > recovered_final_version {
        return Err(BlockedStateValidationError::VisibleExceedsRecovered {
            visible: state.visible_version,
            recovered: recovered_final_version,
        });
    }

    let blocked_txn = state.blocked.blocked.txn_id;
    if blocked_txn <= state.applied_watermark {
        return Err(BlockedStateValidationError::BlockedNotAheadOfApplied {
            blocked: blocked_txn,
            applied: state.applied_watermark,
        });
    }
    if blocked_txn > state.received_watermark {
        return Err(BlockedStateValidationError::BlockedBeyondReceived {
            blocked: blocked_txn,
            received: state.received_watermark,
        });
    }
    if let Some(v) = state.blocked.visibility_version {
        if v > recovered_final_version {
            return Err(
                BlockedStateValidationError::VisibilityVersionExceedsRecovered {
                    visibility_version: v,
                    recovered: recovered_final_version,
                },
            );
        }
    }
    validate_block_reason(
        &state.blocked.blocked.reason,
        state.blocked.visibility_version,
        state.blocked.skip_allowed,
        state.visible_version,
    )?;
    Ok(())
}

fn block_reason_label(reason: &BlockReason) -> &'static str {
    match reason {
        BlockReason::Decode { .. } => "decode",
        BlockReason::Codec { .. } => "codec",
        BlockReason::StorageApply { .. } => "storage-apply",
        BlockReason::SecondaryIndex { .. } => "secondary-index",
        BlockReason::PostApplyInvariant { .. } => "post-apply",
    }
}

fn validate_block_reason(
    reason: &BlockReason,
    visibility_version: Option<CommitVersion>,
    skip_allowed: bool,
    visible_version: CommitVersion,
) -> Result<(), BlockedStateValidationError> {
    let visibility_incoherence = match reason {
        BlockReason::SecondaryIndex { .. } | BlockReason::PostApplyInvariant { .. } => {
            match visibility_version {
                None => Some(BlockReasonIncoherence::MissingVisibilityVersion {
                    reason: block_reason_label(reason),
                }),
                Some(CommitVersion::ZERO) => Some(BlockReasonIncoherence::ZeroVisibilityVersion {
                    reason: block_reason_label(reason),
                }),
                Some(version) if version <= visible_version => {
                    Some(BlockReasonIncoherence::VisibilityVersionNotAheadOfVisible {
                        reason: block_reason_label(reason),
                        visibility_version: version,
                        visible_version,
                    })
                }
                Some(_) => None,
            }
        }
        BlockReason::Decode { .. }
        | BlockReason::Codec { .. }
        | BlockReason::StorageApply { .. } => {
            visibility_version.map(|_| BlockReasonIncoherence::UnexpectedVisibilityVersion {
                reason: block_reason_label(reason),
            })
        }
    };
    if let Some(inc) = visibility_incoherence {
        return Err(BlockedStateValidationError::IncoherentBlockReason(inc));
    }

    let skip_incoherence = match reason {
        BlockReason::SecondaryIndex { .. } | BlockReason::StorageApply { .. } => (!skip_allowed)
            .then(|| BlockReasonIncoherence::UnexpectedNonSkippableState {
                reason: block_reason_label(reason),
            }),
        BlockReason::PostApplyInvariant { .. } => {
            skip_allowed.then(|| BlockReasonIncoherence::UnexpectedSkippableState {
                reason: block_reason_label(reason),
            })
        }
        BlockReason::Decode { .. } | BlockReason::Codec { .. } => None,
    };
    if let Some(inc) = skip_incoherence {
        return Err(BlockedStateValidationError::IncoherentBlockReason(inc));
    }

    let incoherence = match reason {
        BlockReason::Decode { message }
        | BlockReason::StorageApply { message }
        | BlockReason::PostApplyInvariant { message } => {
            if message.is_empty() {
                Some(BlockReasonIncoherence::EmptyMessage)
            } else {
                None
            }
        }
        BlockReason::SecondaryIndex { hook_name, message } => {
            if hook_name.is_empty() {
                Some(BlockReasonIncoherence::EmptyHookName)
            } else if message.is_empty() {
                Some(BlockReasonIncoherence::EmptyMessage)
            } else {
                None
            }
        }
        BlockReason::Codec { expected, actual } => {
            if expected.is_empty() || actual.is_empty() {
                Some(BlockReasonIncoherence::EmptyCodecId)
            } else if expected == actual {
                Some(BlockReasonIncoherence::CodecExpectedEqualsActual {
                    codec: expected.clone(),
                })
            } else {
                None
            }
        }
    };
    match incoherence {
        Some(inc) => Err(BlockedStateValidationError::IncoherentBlockReason(inc)),
        None => Ok(()),
    }
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

pub(crate) fn sync_path_parent(path: &Path) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    std::fs::File::open(parent)?.sync_all()
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
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)?;
    std::io::Write::write_all(&mut file, &bytes)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(tmp, &path)?;
    sync_path_parent(&path)?;
    Ok(())
}

pub(crate) fn clear_persisted_follower_state(data_dir: &Path) -> std::io::Result<()> {
    let Some(path) = follower_state_path(data_dir) else {
        return Ok(());
    };
    match std::fs::remove_file(&path) {
        Ok(()) => sync_path_parent(&path),
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
    /// The provided `txn_id` must equal `applied + 1`. A gap or duplicate
    /// returns `AdvanceError::NonContiguous` so the contiguity invariant
    /// advertised by the type is load-bearing on the type itself, not only
    /// on refresh-loop discipline.
    ///
    /// Contention is nil: callers hold `RefreshGate` (single-flight), so the
    /// load-then-CAS dance never races. The CAS is kept so the semantic is
    /// still correct if the gate is ever relaxed.
    pub(crate) fn try_advance(&self, txn_id: TxnId) -> Result<(), AdvanceError> {
        if let Some(blocked) = self.blocked.read().as_ref() {
            return Err(AdvanceError::Blocked {
                blocked_at: blocked.blocked.txn_id,
            });
        }
        let current = self.applied.load(Ordering::SeqCst);
        // `checked_add` guards the practically-impossible case of applied ==
        // u64::MAX. In release builds plain `+` would wrap silently; in debug
        // it panics. Treat overflow as a non-contiguous advance so the error
        // shape stays uniform.
        let expected = current.checked_add(1).map(TxnId).ok_or({
            AdvanceError::NonContiguous {
                expected: TxnId(u64::MAX),
                provided: txn_id,
            }
        })?;
        if txn_id != expected {
            return Err(AdvanceError::NonContiguous {
                expected,
                provided: txn_id,
            });
        }
        match self.applied.compare_exchange(
            current,
            txn_id.as_u64(),
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => Ok(()),
            Err(actual) => {
                let expected = actual.checked_add(1).map(TxnId).unwrap_or(TxnId(u64::MAX));
                Err(AdvanceError::NonContiguous {
                    expected,
                    provided: txn_id,
                })
            }
        }
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

#[cfg(test)]
mod watermark_contiguity_tests {
    use super::*;

    fn blocked_state(txn_id: TxnId) -> BlockedTxnState {
        BlockedTxnState {
            blocked: BlockedTxn {
                txn_id,
                reason: BlockReason::Decode {
                    message: "test".into(),
                },
            },
            visibility_version: None,
            skip_allowed: true,
        }
    }

    #[test]
    fn try_advance_accepts_next_contiguous_id() {
        let wm = ContiguousWatermark::new(TxnId(10));
        assert_eq!(wm.applied(), TxnId(10));
        assert_eq!(wm.try_advance(TxnId(11)), Ok(()));
        assert_eq!(wm.applied(), TxnId(11));
        assert_eq!(wm.try_advance(TxnId(12)), Ok(()));
        assert_eq!(wm.applied(), TxnId(12));
    }

    #[test]
    fn try_advance_rejects_gap() {
        let wm = ContiguousWatermark::new(TxnId(10));
        assert_eq!(
            wm.try_advance(TxnId(12)),
            Err(AdvanceError::NonContiguous {
                expected: TxnId(11),
                provided: TxnId(12),
            })
        );
        // Watermark unchanged after rejection.
        assert_eq!(wm.applied(), TxnId(10));
    }

    #[test]
    fn try_advance_rejects_duplicate() {
        let wm = ContiguousWatermark::new(TxnId(10));
        assert_eq!(
            wm.try_advance(TxnId(10)),
            Err(AdvanceError::NonContiguous {
                expected: TxnId(11),
                provided: TxnId(10),
            })
        );
        assert_eq!(wm.applied(), TxnId(10));
    }

    #[test]
    fn try_advance_rejects_stale_below_applied() {
        let wm = ContiguousWatermark::new(TxnId(10));
        assert_eq!(
            wm.try_advance(TxnId(5)),
            Err(AdvanceError::NonContiguous {
                expected: TxnId(11),
                provided: TxnId(5),
            })
        );
        assert_eq!(wm.applied(), TxnId(10));
    }

    #[test]
    fn blocked_state_takes_precedence_over_contiguity() {
        let wm = ContiguousWatermark::new(TxnId(10));
        wm.block_at(blocked_state(TxnId(11)));
        // Even the perfectly-contiguous next id is rejected when blocked.
        assert_eq!(
            wm.try_advance(TxnId(11)),
            Err(AdvanceError::Blocked {
                blocked_at: TxnId(11),
            })
        );
        assert_eq!(wm.applied(), TxnId(10));
    }

    #[test]
    fn skip_flow_preserves_contiguity() {
        // Setup: applied at 99, blocked at 100.
        let wm = ContiguousWatermark::new(TxnId(99));
        wm.block_at(blocked_state(TxnId(100)));

        // Admin skip the blocked txn.
        let result = wm.unblock_exact(TxnId(100));
        assert!(result.is_ok(), "unblock should succeed on exact match");
        assert_eq!(wm.applied(), TxnId(100));
        assert!(wm.blocked().is_none());

        // Next refresh cycle's first advance is to 101 — still contiguous.
        assert_eq!(wm.try_advance(TxnId(101)), Ok(()));
        assert_eq!(wm.applied(), TxnId(101));
    }

    #[test]
    fn unblock_mismatch_leaves_watermark_untouched() {
        let wm = ContiguousWatermark::new(TxnId(99));
        wm.block_at(blocked_state(TxnId(100)));

        let err = wm.unblock_exact(TxnId(101)).expect_err("mismatch expected");
        assert!(matches!(
            err,
            UnblockError::Mismatch {
                expected: TxnId(100),
                provided: TxnId(101),
            }
        ));
        assert_eq!(wm.applied(), TxnId(99));
        assert!(wm.blocked().is_some());
    }

    #[test]
    fn try_advance_handles_applied_at_u64_max_without_panicking() {
        // Defensive: u64::MAX is practically unreachable (18 quintillion txns)
        // but debug builds would panic on `current + 1` without `checked_add`.
        // A caller at saturation must see a NonContiguous error, not a panic.
        let wm = ContiguousWatermark::new(TxnId(u64::MAX));
        let err = wm
            .try_advance(TxnId(0))
            .expect_err("advance at saturation must fail without panic");
        assert!(matches!(err, AdvanceError::NonContiguous { .. }));
        // Watermark remains at saturation.
        assert_eq!(wm.applied(), TxnId(u64::MAX));
    }

    #[test]
    fn from_state_respects_contiguity_on_next_advance() {
        // Follower restarted with persisted state: received=105, applied=100, blocked at 101.
        let wm = ContiguousWatermark::from_state(
            TxnId(105),
            TxnId(100),
            Some(blocked_state(TxnId(101))),
        );
        assert_eq!(wm.applied(), TxnId(100));

        // Skip, then the first advance from the resume point is to 101.
        wm.unblock_exact(TxnId(101)).unwrap();
        assert_eq!(wm.applied(), TxnId(101));
        assert_eq!(wm.try_advance(TxnId(102)), Ok(()));
    }
}

#[cfg(test)]
mod persisted_state_validation_tests {
    use super::*;

    fn persisted(
        received: u64,
        applied: u64,
        visible: u64,
        blocked_txn: u64,
        reason: BlockReason,
    ) -> PersistedFollowerState {
        PersistedFollowerState {
            received_watermark: TxnId(received),
            applied_watermark: TxnId(applied),
            visible_version: CommitVersion(visible),
            blocked: BlockedTxnState {
                blocked: BlockedTxn {
                    txn_id: TxnId(blocked_txn),
                    reason,
                },
                visibility_version: None,
                skip_allowed: true,
            },
        }
    }

    fn ok_decode() -> BlockReason {
        BlockReason::Decode {
            message: "x".into(),
        }
    }

    fn ok_secondary_index() -> BlockReason {
        BlockReason::SecondaryIndex {
            hook_name: "search".into(),
            message: "x".into(),
        }
    }

    fn ok_post_apply() -> BlockReason {
        BlockReason::PostApplyInvariant {
            message: "x".into(),
        }
    }

    #[test]
    fn accepts_coherent_persisted_state() {
        let s = persisted(10, 5, 5, 6, ok_decode());
        assert_eq!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Ok(())
        );
    }

    #[test]
    fn rejects_applied_exceeds_received() {
        let s = persisted(5, 10, 5, 11, ok_decode());
        assert!(matches!(
            validate_blocked_state(&s, TxnId(20), CommitVersion(10)),
            Err(BlockedStateValidationError::AppliedExceedsReceived {
                applied: TxnId(10),
                received: TxnId(5),
            })
        ));
    }

    #[test]
    fn rejects_received_exceeds_recovered() {
        let s = persisted(100, 5, 5, 6, ok_decode());
        assert!(matches!(
            validate_blocked_state(&s, TxnId(50), CommitVersion(50)),
            Err(BlockedStateValidationError::ReceivedExceedsRecovered {
                received: TxnId(100),
                recovered_max: TxnId(50),
            })
        ));
    }

    #[test]
    fn rejects_visible_exceeds_recovered() {
        let s = persisted(10, 5, 100, 6, ok_decode());
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::VisibleExceedsRecovered { .. })
        ));
    }

    #[test]
    fn rejects_blocked_not_ahead_of_applied() {
        // blocked.txn_id == applied_watermark → must be rejected.
        let s = persisted(10, 5, 5, 5, ok_decode());
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::BlockedNotAheadOfApplied {
                blocked: TxnId(5),
                applied: TxnId(5),
            })
        ));

        // blocked.txn_id < applied_watermark → must be rejected.
        let s = persisted(10, 5, 5, 3, ok_decode());
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::BlockedNotAheadOfApplied { .. })
        ));
    }

    #[test]
    fn rejects_blocked_beyond_received() {
        let s = persisted(10, 5, 5, 11, ok_decode());
        assert!(matches!(
            validate_blocked_state(&s, TxnId(20), CommitVersion(10)),
            Err(BlockedStateValidationError::BlockedBeyondReceived {
                blocked: TxnId(11),
                received: TxnId(10),
            })
        ));
    }

    #[test]
    fn accepts_secondary_index_visibility_version_within_recovered() {
        // visible=4 so visibility=5 is strictly ahead (per
        // VisibilityVersionNotAheadOfVisible rule).
        let mut s = persisted(10, 5, 4, 6, ok_secondary_index());
        s.blocked.visibility_version = Some(CommitVersion(5));
        assert_eq!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Ok(())
        );
    }

    #[test]
    fn rejects_visibility_version_beyond_recovered() {
        let mut s = persisted(10, 5, 5, 6, ok_secondary_index());
        s.blocked.visibility_version = Some(CommitVersion(42));
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(
                BlockedStateValidationError::VisibilityVersionExceedsRecovered {
                    visibility_version: CommitVersion(42),
                    recovered: CommitVersion(5),
                }
            )
        ));
    }

    #[test]
    fn rejects_secondary_index_without_visibility_version() {
        let s = persisted(10, 5, 5, 6, ok_secondary_index());
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::MissingVisibilityVersion {
                    reason: "secondary-index",
                }
            ))
        ));
    }

    #[test]
    fn rejects_secondary_index_zero_visibility_version() {
        let mut s = persisted(10, 5, 5, 6, ok_secondary_index());
        s.blocked.visibility_version = Some(CommitVersion::ZERO);
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::ZeroVisibilityVersion {
                    reason: "secondary-index",
                }
            ))
        ));
    }

    #[test]
    fn accepts_post_apply_visibility_version_within_recovered() {
        // visible=4 so visibility=5 is strictly ahead; skip_allowed=false
        // because post-apply invariant failures are intentionally
        // non-skippable.
        let mut s = persisted(10, 5, 4, 6, ok_post_apply());
        s.blocked.visibility_version = Some(CommitVersion(5));
        s.blocked.skip_allowed = false;
        assert_eq!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Ok(())
        );
    }

    #[test]
    fn rejects_decode_with_visibility_version() {
        let mut s = persisted(10, 5, 5, 6, ok_decode());
        s.blocked.visibility_version = Some(CommitVersion(5));
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::UnexpectedVisibilityVersion { reason: "decode" }
            ))
        ));
    }

    #[test]
    fn rejects_secondary_index_visibility_version_not_ahead_of_visible() {
        let mut s = persisted(10, 5, 5, 6, ok_secondary_index());
        s.blocked.visibility_version = Some(CommitVersion(5));
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::VisibilityVersionNotAheadOfVisible {
                    reason: "secondary-index",
                    visibility_version: CommitVersion(5),
                    visible_version: CommitVersion(5),
                }
            ))
        ));
    }

    #[test]
    fn rejects_post_apply_visibility_version_not_ahead_of_visible() {
        let mut s = persisted(10, 5, 5, 6, ok_post_apply());
        s.blocked.visibility_version = Some(CommitVersion(5));
        s.blocked.skip_allowed = false;
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::VisibilityVersionNotAheadOfVisible {
                    reason: "post-apply",
                    visibility_version: CommitVersion(5),
                    visible_version: CommitVersion(5),
                }
            ))
        ));
    }

    #[test]
    fn rejects_secondary_index_when_skip_disallowed() {
        let mut s = persisted(10, 5, 4, 6, ok_secondary_index());
        s.blocked.visibility_version = Some(CommitVersion(5));
        s.blocked.skip_allowed = false;
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::UnexpectedNonSkippableState {
                    reason: "secondary-index",
                }
            ))
        ));
    }

    #[test]
    fn rejects_storage_apply_when_skip_disallowed() {
        let mut s = persisted(
            10,
            5,
            5,
            6,
            BlockReason::StorageApply {
                message: "x".into(),
            },
        );
        s.blocked.skip_allowed = false;
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::UnexpectedNonSkippableState {
                    reason: "storage-apply",
                }
            ))
        ));
    }

    #[test]
    fn rejects_post_apply_when_skip_allowed() {
        let mut s = persisted(10, 5, 4, 6, ok_post_apply());
        s.blocked.visibility_version = Some(CommitVersion(5));
        s.blocked.skip_allowed = true;
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::UnexpectedSkippableState {
                    reason: "post-apply",
                }
            ))
        ));
    }

    #[test]
    fn rejects_codec_with_visibility_version() {
        let mut s = persisted(
            10,
            5,
            5,
            6,
            BlockReason::Codec {
                expected: "v3".into(),
                actual: "v2".into(),
            },
        );
        s.blocked.visibility_version = Some(CommitVersion(5));
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::UnexpectedVisibilityVersion { reason: "codec" }
            ))
        ));
    }

    #[test]
    fn rejects_codec_expected_equals_actual() {
        let s = persisted(
            10,
            5,
            5,
            6,
            BlockReason::Codec {
                expected: "v3".into(),
                actual: "v3".into(),
            },
        );
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::CodecExpectedEqualsActual { .. }
            ))
        ));
    }

    #[test]
    fn rejects_codec_empty_id() {
        let s = persisted(
            10,
            5,
            5,
            6,
            BlockReason::Codec {
                expected: String::new(),
                actual: "v3".into(),
            },
        );
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::EmptyCodecId
            ))
        ));
    }

    #[test]
    fn rejects_secondary_index_empty_hook_name() {
        // Coherent visibility_version (5 > visible=4) and skip_allowed
        // default (true) so only EmptyHookName can trigger.
        let mut s = persisted(
            10,
            5,
            4,
            6,
            BlockReason::SecondaryIndex {
                hook_name: String::new(),
                message: "x".into(),
            },
        );
        s.blocked.visibility_version = Some(CommitVersion(5));
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::EmptyHookName
            ))
        ));
    }

    #[test]
    fn rejects_empty_decode_message() {
        let s = persisted(
            10,
            5,
            5,
            6,
            BlockReason::Decode {
                message: String::new(),
            },
        );
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::EmptyMessage
            ))
        ));
    }

    #[test]
    fn accepts_coherent_codec_mismatch() {
        let s = persisted(
            10,
            5,
            5,
            6,
            BlockReason::Codec {
                expected: "v3".into(),
                actual: "v2".into(),
            },
        );
        assert_eq!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Ok(())
        );
    }

    #[test]
    fn rejects_post_apply_empty_message() {
        // Coherent visibility_version (5 > visible=4) and skip_allowed=false
        // so only EmptyMessage can trigger.
        let mut s = persisted(
            10,
            5,
            4,
            6,
            BlockReason::PostApplyInvariant {
                message: String::new(),
            },
        );
        s.blocked.visibility_version = Some(CommitVersion(5));
        s.blocked.skip_allowed = false;
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::EmptyMessage
            ))
        ));
    }

    #[test]
    fn rejects_secondary_index_empty_message() {
        // Coherent visibility_version (5 > visible=4), non-empty hook_name,
        // skip_allowed default (true) so only EmptyMessage can trigger.
        let mut s = persisted(
            10,
            5,
            4,
            6,
            BlockReason::SecondaryIndex {
                hook_name: "search".into(),
                message: String::new(),
            },
        );
        s.blocked.visibility_version = Some(CommitVersion(5));
        assert!(matches!(
            validate_blocked_state(&s, TxnId(10), CommitVersion(5)),
            Err(BlockedStateValidationError::IncoherentBlockReason(
                BlockReasonIncoherence::EmptyMessage
            ))
        ));
    }
}
