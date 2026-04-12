//! BranchBundle error types

use std::io;
use thiserror::Error;

/// Errors that can occur during BranchBundle operations
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum BranchBundleError {
    /// Branch not found in BranchIndex
    #[error("Branch not found: {0}")]
    BranchNotFound(String),

    /// Branch is not in a terminal state (cannot export)
    #[error("Branch is not in terminal state (current: {0}). Only Completed, Failed, Cancelled, or Archived branches can be exported.")]
    NotTerminal(String),

    /// Invalid bundle format or structure
    #[error("Invalid bundle: {0}")]
    InvalidBundle(String),

    /// Required file missing from bundle
    #[error("Missing required file in bundle: {0}")]
    MissingFile(String),

    /// Checksum verification failed
    #[error("Checksum mismatch for {file}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        /// File that failed checksum
        file: String,
        /// Expected checksum value
        expected: String,
        /// Actual computed checksum
        actual: String,
    },

    /// Branch already exists (import conflict)
    #[error("Branch already exists: {0}")]
    BranchAlreadyExists(String),

    /// Unsupported bundle format version
    #[error(
        "Unsupported format version: {version}. Supported: 2 (v1 bundles must be re-exported)"
    )]
    UnsupportedVersion {
        /// The unsupported version number
        version: u32,
    },

    /// Archive operation failed
    #[error("Archive error: {0}")]
    Archive(String),

    /// Compression/decompression failed
    #[error("Compression error: {0}")]
    Compression(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Bincode serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// WAL replay failed during import
    #[error("WAL replay error: {0}")]
    WalReplay(String),

    /// WAL entry is malformed
    #[error("Invalid WAL entry at index {index}: {reason}")]
    InvalidWalEntry {
        /// Index of the problematic entry
        index: usize,
        /// Description of the problem
        reason: String,
    },
}

impl BranchBundleError {
    /// Create an archive error
    pub fn archive(msg: impl Into<String>) -> Self {
        Self::Archive(msg.into())
    }

    /// Create a compression error
    pub fn compression(msg: impl Into<String>) -> Self {
        Self::Compression(msg.into())
    }

    /// Create a serialization error
    pub fn serialization(msg: impl Into<String>) -> Self {
        Self::Serialization(msg.into())
    }

    /// Create a WAL replay error
    pub fn wal_replay(msg: impl Into<String>) -> Self {
        Self::WalReplay(msg.into())
    }

    /// Create an invalid bundle error
    pub fn invalid_bundle(msg: impl Into<String>) -> Self {
        Self::InvalidBundle(msg.into())
    }

    /// Create a missing file error
    pub fn missing_file(path: impl Into<String>) -> Self {
        Self::MissingFile(path.into())
    }
}

/// Result type for BranchBundle operations
pub type BranchBundleResult<T> = Result<T, BranchBundleError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = BranchBundleError::BranchNotFound("abc-123".to_string());
        assert!(err.to_string().contains("abc-123"));

        let err = BranchBundleError::NotTerminal("Active".to_string());
        assert!(err.to_string().contains("Active"));
        assert!(err.to_string().contains("terminal"));

        let err = BranchBundleError::ChecksumMismatch {
            file: "WAL.runlog".to_string(),
            expected: "abc".to_string(),
            actual: "def".to_string(),
        };
        assert!(err.to_string().contains("WAL.runlog"));
        assert!(err.to_string().contains("abc"));
        assert!(err.to_string().contains("def"));
    }

    #[test]
    fn test_error_constructors() {
        let err = BranchBundleError::archive("tar failed");
        assert!(matches!(err, BranchBundleError::Archive(_)));

        let err = BranchBundleError::compression("zstd failed");
        assert!(matches!(err, BranchBundleError::Compression(_)));

        let err = BranchBundleError::missing_file("MANIFEST.json");
        assert!(matches!(err, BranchBundleError::MissingFile(_)));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let bundle_err: BranchBundleError = io_err.into();
        assert!(matches!(bundle_err, BranchBundleError::Io(_)));
    }
}
