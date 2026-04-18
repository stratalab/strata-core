use std::io;
use std::path::PathBuf;

use strata_core::types::BranchId;
use strata_core::StrataError;
use thiserror::Error;

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
}

impl StorageError {
    /// Returns the underlying `io::ErrorKind` for callers and tests that only
    /// need the coarse I/O classification.
    pub fn kind(&self) -> io::ErrorKind {
        match self {
            StorageError::Io(inner) => inner.kind(),
            StorageError::ManifestPublish { inner, .. } => inner.kind(),
            StorageError::DirFsync { inner, .. } => inner.kind(),
        }
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
        }
    }
}

impl From<StorageError> for io::Error {
    fn from(value: StorageError) -> Self {
        io::Error::other(value)
    }
}
