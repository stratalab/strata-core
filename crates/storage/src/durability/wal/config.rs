//! WAL configuration.
//!
//! This module provides configuration for the Write-Ahead Log.

/// Default maximum segment size: 64 MB.
pub const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Compatibility default for the retained buffered-sync threshold.
pub const DEFAULT_BUFFERED_SYNC_BYTES: u64 = 4 * 1024 * 1024;

/// WAL configuration parameters.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Maximum segment size in bytes (default: 64MB).
    ///
    /// When a segment exceeds this size, a new segment is created.
    pub segment_size: u64,

    /// Retained buffered-sync threshold (default: 4MB).
    ///
    /// The current writer does not use this field as an inline fsync trigger;
    /// Standard-mode sync timing is driven by `DurabilityMode::Standard` and
    /// the engine-owned background flush lifecycle. `validate()` still checks
    /// it for callers that intentionally require a self-consistent full
    /// `WalConfig`.
    pub buffered_sync_bytes: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            segment_size: DEFAULT_SEGMENT_SIZE,
            buffered_sync_bytes: DEFAULT_BUFFERED_SYNC_BYTES,
        }
    }
}

impl WalConfig {
    /// Create a new WAL configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set segment size (builder pattern).
    pub fn with_segment_size(mut self, size: u64) -> Self {
        self.segment_size = size;
        self
    }

    /// Set buffered sync threshold (builder pattern).
    pub fn with_buffered_sync_bytes(mut self, bytes: u64) -> Self {
        self.buffered_sync_bytes = bytes;
        self
    }

    /// Validate all configuration fields for static consistency.
    pub fn validate(&self) -> Result<(), WalConfigError> {
        if self.segment_size < 1024 {
            return Err(WalConfigError::SegmentSizeTooSmall);
        }
        if self.buffered_sync_bytes > self.segment_size {
            return Err(WalConfigError::BufferedSyncExceedsSegment);
        }
        Ok(())
    }

    /// Validate only invariants the current writer runtime actively consumes.
    ///
    /// `buffered_sync_bytes` is retained configuration metadata and is not
    /// currently an inline fsync trigger, so `WalWriter::new` must not reject
    /// a writer solely because that inert field exceeds `segment_size`.
    pub(crate) fn validate_writer_runtime(&self) -> Result<(), WalConfigError> {
        if self.segment_size < 1024 {
            return Err(WalConfigError::SegmentSizeTooSmall);
        }
        Ok(())
    }

    /// Create a configuration optimized for testing (small segments).
    pub fn for_testing() -> Self {
        WalConfig {
            segment_size: 64 * 1024,        // 64KB for faster rotation in tests
            buffered_sync_bytes: 16 * 1024, // 16KB
        }
    }
}

/// WAL configuration errors.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WalConfigError {
    /// Segment size is too small (minimum 1KB).
    #[error("Segment size must be at least 1KB")]
    SegmentSizeTooSmall,

    /// Buffered sync threshold exceeds segment size.
    #[error("Buffered sync threshold cannot exceed segment size")]
    BufferedSyncExceedsSegment,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = WalConfig::default();
        assert_eq!(config.segment_size, 64 * 1024 * 1024);
        assert_eq!(config.buffered_sync_bytes, 4 * 1024 * 1024);
    }

    #[test]
    fn test_builder_pattern() {
        let config = WalConfig::new()
            .with_segment_size(128 * 1024 * 1024)
            .with_buffered_sync_bytes(8 * 1024 * 1024);

        assert_eq!(config.segment_size, 128 * 1024 * 1024);
        assert_eq!(config.buffered_sync_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn test_validation_valid() {
        let config = WalConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validation_segment_too_small() {
        let config = WalConfig::new().with_segment_size(512);
        assert!(matches!(
            config.validate(),
            Err(WalConfigError::SegmentSizeTooSmall)
        ));
    }

    #[test]
    fn test_validation_sync_exceeds_segment() {
        let config = WalConfig::new()
            .with_segment_size(1024)
            .with_buffered_sync_bytes(2048);
        assert!(matches!(
            config.validate(),
            Err(WalConfigError::BufferedSyncExceedsSegment)
        ));
    }

    #[test]
    fn test_testing_config() {
        let config = WalConfig::for_testing();
        assert!(config.validate().is_ok());
        assert!(config.segment_size < WalConfig::default().segment_size);
    }
}
