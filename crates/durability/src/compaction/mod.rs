//! Database compaction
//!
//! Compaction reclaims disk space by removing WAL segments and old versions.
//! Compaction is user-triggered and deterministic.
//!
//! # Compaction Modes
//!
//! - **WALOnly**: Safely removes WAL segments covered by snapshot watermark.
//!   All version history is preserved.
//!
//! # Key Invariants
//!
//! - Compaction is **user-triggered**: No background compaction
//! - Compaction is **deterministic**: Same input → same output
//! - Compaction is **logically invisible**: Read results unchanged for retained data
//! - **Version IDs never change**: Critical semantic invariant
//!
//! # Example
//!
//! ```text
//! let info = database.compact(CompactMode::WALOnly)?;
//! println!("Reclaimed {} bytes", info.reclaimed_bytes);
//! ```

pub mod tombstone;
pub mod wal_only;

pub use tombstone::{Tombstone, TombstoneError, TombstoneIndex, TombstoneReason};
pub use wal_only::WalOnlyCompactor;

/// Compaction mode
///
/// Determines how aggressively compaction reclaims disk space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactMode {
    /// Remove WAL segments covered by snapshot
    ///
    /// Only removes WAL segments whose transactions
    /// are fully captured in a snapshot. All version history preserved.
    WALOnly,
}

impl CompactMode {
    /// Get the name of this compaction mode for logging/metrics
    pub fn name(&self) -> &'static str {
        match self {
            CompactMode::WALOnly => "wal_only",
        }
    }
}

impl std::fmt::Display for CompactMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Result of a compaction operation
///
/// Contains metrics and details about what was compacted.
#[derive(Debug, Clone)]
pub struct CompactInfo {
    /// Compaction mode used
    pub mode: CompactMode,

    /// Bytes reclaimed from disk
    pub reclaimed_bytes: u64,

    /// Number of WAL segments removed
    pub wal_segments_removed: usize,

    /// Snapshot watermark used for compaction (transaction ID)
    pub snapshot_watermark: Option<u64>,

    /// Duration of compaction operation in milliseconds
    pub duration_ms: u64,

    /// Timestamp when compaction completed (microseconds since epoch)
    pub timestamp: u64,
}

impl CompactInfo {
    /// Create a new CompactInfo for a given mode
    pub fn new(mode: CompactMode) -> Self {
        CompactInfo {
            mode,
            reclaimed_bytes: 0,
            wal_segments_removed: 0,
            snapshot_watermark: None,
            duration_ms: 0,
            timestamp: 0,
        }
    }

    /// Check if any compaction actually occurred
    pub fn did_compact(&self) -> bool {
        self.wal_segments_removed > 0
    }

    /// Get a summary string for logging
    pub fn summary(&self) -> String {
        format!(
            "mode={}, segments_removed={}, bytes_reclaimed={}, duration_ms={}",
            self.mode, self.wal_segments_removed, self.reclaimed_bytes, self.duration_ms
        )
    }
}

impl Default for CompactInfo {
    fn default() -> Self {
        Self::new(CompactMode::WALOnly)
    }
}

/// Compaction error types
#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    /// No snapshot available for compaction
    ///
    /// WAL-only compaction requires a snapshot to determine which
    /// segments are safe to remove.
    #[error("No snapshot available for compaction")]
    NoSnapshot,

    /// Compaction already in progress
    ///
    /// Only one compaction can run at a time to ensure consistency.
    #[error("Compaction already in progress")]
    AlreadyInProgress,

    /// IO error during compaction
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// MANIFEST error during compaction
    #[error("Manifest error: {0}")]
    Manifest(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl CompactionError {
    /// Create a new manifest error
    pub fn manifest(msg: impl Into<String>) -> Self {
        CompactionError::Manifest(msg.into())
    }

    /// Create a new internal error
    pub fn internal(msg: impl Into<String>) -> Self {
        CompactionError::Internal(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_mode_name() {
        assert_eq!(CompactMode::WALOnly.name(), "wal_only");
    }

    #[test]
    fn test_compact_mode_display() {
        assert_eq!(format!("{}", CompactMode::WALOnly), "wal_only");
    }

    #[test]
    fn test_compact_info_new() {
        let info = CompactInfo::new(CompactMode::WALOnly);

        assert_eq!(info.mode, CompactMode::WALOnly);
        assert_eq!(info.reclaimed_bytes, 0);
        assert_eq!(info.wal_segments_removed, 0);
        assert_eq!(info.snapshot_watermark, None);
        assert_eq!(info.duration_ms, 0);
        assert_eq!(info.timestamp, 0);
    }

    #[test]
    fn test_compact_info_did_compact() {
        let mut info = CompactInfo::new(CompactMode::WALOnly);
        assert!(!info.did_compact());

        info.wal_segments_removed = 1;
        assert!(info.did_compact());
    }

    #[test]
    fn test_compact_info_summary() {
        let mut info = CompactInfo::new(CompactMode::WALOnly);
        info.wal_segments_removed = 5;
        info.reclaimed_bytes = 1024;
        info.duration_ms = 250;

        let summary = info.summary();
        assert!(summary.contains("mode=wal_only"));
        assert!(summary.contains("segments_removed=5"));
        assert!(summary.contains("bytes_reclaimed=1024"));
        assert!(summary.contains("duration_ms=250"));
    }

    #[test]
    fn test_compact_info_default() {
        let info = CompactInfo::default();
        assert_eq!(info.mode, CompactMode::WALOnly);
    }

    #[test]
    fn test_compaction_error_display() {
        let err = CompactionError::NoSnapshot;
        assert!(err.to_string().contains("No snapshot"));

        let err = CompactionError::AlreadyInProgress;
        assert!(err.to_string().contains("already in progress"));
    }

    #[test]
    fn test_compaction_error_helpers() {
        let err = CompactionError::manifest("test manifest");
        assert!(matches!(err, CompactionError::Manifest(_)));

        let err = CompactionError::internal("test internal");
        assert!(matches!(err, CompactionError::Internal(_)));
    }

    #[test]
    fn test_compaction_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: CompactionError = io_err.into();
        assert!(matches!(err, CompactionError::Io(_)));
    }

    #[test]
    fn test_compact_mode_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(CompactMode::WALOnly);

        assert!(set.contains(&CompactMode::WALOnly));
        assert_eq!(set.len(), 1);
    }
}
