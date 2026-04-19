use std::io;
use std::path::PathBuf;

use strata_core::types::BranchId;
use strata_core::StrataError;
use thiserror::Error;

use crate::segmented::RecoveryFault;

/// Result alias for storage-local operations that can raise [`StorageError`].
pub type StorageResult<T> = Result<T, StorageError>;

/// Storage-local failures that need finer classification than raw `io::Error`.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StorageError {
    /// Generic storage I/O failure.
    #[error(transparent)]
    Io(#[from] io::Error),

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
        }
    }
}

impl From<StorageError> for io::Error {
    fn from(value: StorageError) -> Self {
        io::Error::other(value)
    }
}
