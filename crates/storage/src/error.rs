use std::fmt;
use std::io;
use std::path::PathBuf;

use strata_core::types::BranchId;
use strata_core::StrataError;
use thiserror::Error;

use crate::segmented::{DegradationClass, RecoveryFault};

/// Result alias for storage-local operations that can raise [`StorageError`].
pub type StorageResult<T> = Result<T, StorageError>;

/// Identifies the storage operation that observed a deleted branch mid-flight.
///
/// Used exclusively as a diagnostic tag on [`StorageError::BranchDeletedDuringOp`].
/// This is **not** a lifecycle primitive and carries no behavior; Tranche 4
/// owns the final branch-lifecycle shape (generation counter, tombstone map,
/// per-branch lock). See `docs/design/architecture-cleanup/branch-primitives-scope.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BranchOp {
    /// `SegmentedStore::compact_branch` (all-L0 merge).
    CompactBranch,
    /// `SegmentedStore::compact_tier` (subset-of-L0 merge).
    CompactTier,
    /// `SegmentedStore::compact_l0_to_l1` (L0 → L1 leveled merge).
    CompactL0ToL1,
    /// `SegmentedStore::compact_level` (level N → N+1 leveled merge,
    /// including the metadata-only trivial-move path).
    CompactLevel,
    /// `SegmentedStore::materialize_layer` (inherited-layer copy-down).
    MaterializeLayer,
}

impl fmt::Display for BranchOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            BranchOp::CompactBranch => "compact_branch",
            BranchOp::CompactTier => "compact_tier",
            BranchOp::CompactL0ToL1 => "compact_l0_to_l1",
            BranchOp::CompactLevel => "compact_level",
            BranchOp::MaterializeLayer => "materialize_layer",
        };
        f.write_str(name)
    }
}

/// Storage-local failures that need finer classification than raw `io::Error`.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StorageError {
    /// Generic storage I/O failure.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// `recover_segments()` was invoked more than once on the same store.
    #[error("recover_segments() can only run once per SegmentedStore instance")]
    RecoveryAlreadyApplied,

    /// `reset_recovery_health()` can only clear the GC refusal after this
    /// store instance successfully completed `recover_segments()` and
    /// installed the recovered branch/refcount view. Hard pre-walk recovery
    /// failures never reach that point, so clearing the health bit on the
    /// same instance would let GC treat every on-disk `.sst` as unseen.
    #[error("reset_recovery_health() requires a successfully applied recover_segments() on this store instance")]
    RecoveryHealthResetRequiresSuccessfulRecovery,

    /// Publishing a branch manifest failed before the atomic rename completed.
    #[error("failed to publish segment manifest for branch {branch_id}: {inner}")]
    ManifestPublish {
        /// Branch whose manifest publication failed.
        branch_id: BranchId,
        /// Underlying filesystem error from temp-file write, sync, or rename.
        #[source]
        inner: io::Error,
    },

    /// The manifest rename may already be visible, but the parent-directory
    /// fsync failed so rename durability is unconfirmed.
    #[error("segment manifest rename may not be durable for directory {dir:?}: {inner}")]
    DirFsync {
        /// Directory that could not be fsynced after rename.
        dir: PathBuf,
        /// Underlying directory-open or directory-fsync error.
        #[source]
        inner: io::Error,
    },

    /// A single [`RecoveryFault`] surfaced through an error boundary.
    ///
    /// Recovery itself returns [`StorageResult<RecoveredState>`] with the full
    /// classified health; this variant exists for call sites that need to
    /// carry exactly one fault across a `Result` boundary without reshaping
    /// the outcome type (e.g. cross-crate conversions into `StrataError`).
    ///
    /// [`RecoveredState`]: crate::segmented::RecoveredState
    #[error(transparent)]
    RecoveryFault(#[from] RecoveryFault),

    /// Orphan-segment GC refused to run because the most recent recovery
    /// produced a degradation class that could make deletion unsafe
    /// (`DataLoss` or `PolicyDowngrade`). Callers log this as a
    /// retention-debt signal and continue; `PolicyDowngrade` can be cleared
    /// in-place via `SegmentedStore::reset_recovery_health()`, while
    /// `DataLoss` requires a fresh reopen.
    #[error("gc refused: last recovery degradation class {class:?} is not safe for deletion")]
    GcRefusedDegradedRecovery {
        /// Classified severity of the most recent recovery. Any class other
        /// than `Telemetry` blocks deletion; `DataLoss` and `PolicyDowngrade`
        /// both route here.
        class: DegradationClass,
    },

    /// `reset_recovery_health()` cannot clear this degraded recovery on the
    /// same store instance. After authoritative loss, operators may restore
    /// manifests or `.sst`s on disk, but this store's in-memory
    /// `self.branches` / refcount view is a one-shot snapshot from the
    /// original reopen. Re-enabling GC without a fresh reopen would let it
    /// delete restored files that recovery never reloaded. Any future
    /// degradation class that is not explicitly allowlisted also routes here.
    #[error("reset_recovery_health() requires a fresh reopen after degraded recovery ({class:?})")]
    RecoveryHealthResetRequiresReopen {
        /// Classified severity that blocks an in-place reset. `PolicyDowngrade`
        /// is the only allowlisted degraded class for in-place reset;
        /// `Telemetry` never refuses GC, and any other current or future class
        /// requires a fresh reopen.
        class: DegradationClass,
    },

    /// An in-flight compaction or materialize observed that `branch_id` was
    /// removed from `self.branches` (via `clear_branch`) during the long-I/O
    /// gap between snapshot and install. The op cleaned up any outputs it
    /// built and refused to re-create the branch entry — a deletion proof
    /// must be stronger than the later install.
    ///
    /// Current-state race fix only; Tranche 4 owns the final lifecycle shape
    /// (generation counter, tombstone map, per-branch lock). Callers that
    /// treat best-effort operations (background compaction, materialize) as
    /// non-fatal should log this and continue.
    #[error("branch {branch_id} was deleted during {op}; operation refused to resurrect")]
    BranchDeletedDuringOp {
        /// Branch that was removed from `self.branches` mid-operation.
        branch_id: BranchId,
        /// Identifies which storage op observed the deletion, for diagnostics.
        op: BranchOp,
    },
}

impl StorageError {
    /// Returns the underlying `io::ErrorKind` for callers and tests that only
    /// need the coarse I/O classification.
    ///
    /// Returns [`io::ErrorKind::Other`] for [`StorageError::RecoveryFault`]
    /// variants that do not wrap an `io::Error`.
    pub fn kind(&self) -> io::ErrorKind {
        match self {
            StorageError::Io(inner) => inner.kind(),
            StorageError::ManifestPublish { inner, .. } => inner.kind(),
            StorageError::DirFsync { inner, .. } => inner.kind(),
            StorageError::RecoveryFault(fault) => recovery_fault_kind(fault),
            StorageError::RecoveryAlreadyApplied
            | StorageError::RecoveryHealthResetRequiresSuccessfulRecovery
            | StorageError::GcRefusedDegradedRecovery { .. }
            | StorageError::RecoveryHealthResetRequiresReopen { .. }
            | StorageError::BranchDeletedDuringOp { .. } => io::ErrorKind::Other,
        }
    }
}

fn recovery_fault_kind(fault: &RecoveryFault) -> io::ErrorKind {
    match fault {
        RecoveryFault::CorruptSegment { inner, .. }
        | RecoveryFault::CorruptManifest { inner, .. }
        | RecoveryFault::Io(inner) => inner.kind(),
        RecoveryFault::MissingManifestListed { .. }
        | RecoveryFault::InheritedLayerLost { .. }
        | RecoveryFault::NoManifestFallbackUsed { .. } => io::ErrorKind::Other,
    }
}

impl From<StorageError> for StrataError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::Io(inner) => StrataError::storage_with_source("storage I/O error", inner),
            StorageError::RecoveryAlreadyApplied => {
                StrataError::storage("segment recovery already applied on this store instance")
            }
            StorageError::RecoveryHealthResetRequiresSuccessfulRecovery => StrataError::storage(
                "reset_recovery_health() requires a successfully applied recovery on this store instance",
            ),
            StorageError::ManifestPublish { branch_id, inner } => StrataError::storage_with_source(
                format!("failed to publish segment manifest for branch {branch_id}"),
                inner,
            ),
            StorageError::DirFsync { dir, inner } => StrataError::storage_with_source(
                format!(
                    "segment manifest rename may not be durable for directory {}",
                    dir.display()
                ),
                inner,
            ),
            StorageError::RecoveryFault(fault) => {
                StrataError::storage_with_source("recovery fault", fault)
            }
            StorageError::GcRefusedDegradedRecovery { class } => StrataError::storage(format!(
                "gc refused under degraded recovery ({class:?})"
            )),
            StorageError::RecoveryHealthResetRequiresReopen { class } => {
                StrataError::storage(format!(
                    "reset_recovery_health() requires a fresh reopen after degraded recovery ({class:?})"
                ))
            }
            StorageError::BranchDeletedDuringOp { branch_id, op } => StrataError::storage(
                format!("branch {branch_id} was deleted during {op}; operation refused to resurrect"),
            ),
        }
    }
}

impl From<StorageError> for io::Error {
    fn from(value: StorageError) -> Self {
        io::Error::other(value)
    }
}
