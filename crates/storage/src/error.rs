//! Storage-crate typed error surface.
//!
//! [`StorageError`] is the return type for storage operations where the
//! caller needs richer context than [`std::io::Error`] provides (notably,
//! the branch identity behind a failed manifest publish, and the directory
//! path behind a failed `sync_all`).
//!
//! Internal storage code still composes `io::Result<T>` inside storage
//! functions via the `From<io::Error>` conversion; the typed variants are
//! constructed only at the two sites that own the additional context
//! ([`crate::segmented::SegmentedStore::write_branch_manifest`] and
//! [`crate::manifest::write_manifest`]).

use std::io;
use std::path::PathBuf;

use strata_core::types::BranchId;
use strata_core::StrataError;
use thiserror::Error;

/// Typed errors produced by storage-layer publish and durability
/// primitives.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StorageError {
    /// Publishing the on-disk branch manifest failed.
    ///
    /// Returned by `write_branch_manifest` when the branch directory
    /// cannot be created or the manifest file cannot be written/renamed.
    /// Callers must treat this as a refused publish: any deletion of the
    /// old input segments that was gated on a successful publish is not
    /// performed.
    #[error("manifest publish failed for branch {branch_id}: {inner}")]
    ManifestPublish {
        /// Branch whose manifest failed to publish.
        branch_id: BranchId,
        /// Underlying I/O error.
        #[source]
        inner: io::Error,
    },

    /// Fsync on a parent directory failed.
    ///
    /// Used by `write_manifest` and any other durability primitive that
    /// needs to prove a rename is durable on crash. The underlying rename
    /// may have succeeded, so callers should treat this as a durability
    /// failure rather than a content failure.
    #[error("directory fsync failed for {}: {inner}", dir.display())]
    DirFsync {
        /// Directory that failed to fsync.
        dir: PathBuf,
        /// Underlying I/O error.
        #[source]
        inner: io::Error,
    },

    /// An otherwise-uncategorised storage I/O error.
    ///
    /// Preserves the original [`io::Error`] so callers that pattern-match
    /// on [`io::ErrorKind`] continue to work after the migration from
    /// `io::Result<T>`.
    #[error("storage i/o error: {0}")]
    Io(#[from] io::Error),
}

impl StorageError {
    /// Return the underlying [`io::ErrorKind`].
    ///
    /// Preserves the `io::ErrorKind` inspection API that callers had when
    /// storage functions returned `io::Result<T>`. For variants that do not
    /// wrap an [`io::Error`], returns [`io::ErrorKind::Other`].
    pub fn kind(&self) -> io::ErrorKind {
        match self {
            StorageError::ManifestPublish { inner, .. } => inner.kind(),
            StorageError::DirFsync { inner, .. } => inner.kind(),
            StorageError::Io(e) => e.kind(),
        }
    }
}

/// Convenience alias for `Result<T, StorageError>`.
pub type StorageResult<T> = Result<T, StorageError>;

/// Cross-layer conversion so engine-layer code can use `?` to surface
/// storage failures as [`StrataError`].
///
/// - [`StorageError::ManifestPublish`] → [`StrataError::Corruption`] (publish
///   barrier violation is a durable-state correctness failure). The inner
///   [`io::Error`]'s description is embedded in the message because
///   `StrataError::Corruption` has no source field today.
/// - [`StorageError::DirFsync`] → [`StrataError::Storage`] with the original
///   [`io::Error`] preserved as `source` (durability failure, not content
///   corruption).
/// - [`StorageError::Io`] → [`StrataError::Storage`] with source preserved —
///   matches the existing `From<io::Error> for StrataError` behavior.
impl From<StorageError> for StrataError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::ManifestPublish { branch_id, inner } => StrataError::corruption(format!(
                "manifest publish failed for branch {branch_id}: {inner}"
            )),
            StorageError::DirFsync { dir, inner } => StrataError::storage_with_source(
                format!("directory fsync failed for {}", dir.display()),
                inner,
            ),
            StorageError::Io(io) => StrataError::from(io),
        }
    }
}
