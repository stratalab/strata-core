//! WAL (Write-Ahead Log) module
//!
//! - `mode`: DurabilityMode (Cache, Always, Standard)
//! - `config`: WAL configuration (WalConfig, WalConfigError)
//! - `writer`: Segmented WAL writer (WalWriter)
//! - `reader`: Segmented WAL reader (WalReader)

pub mod config;
pub mod mode;
pub mod reader;
pub mod writer;

// Canonical DurabilityMode
pub use mode::DurabilityMode;

// Segmented WAL types (primary API)
pub use config::{WalConfig, WalConfigError};
pub use reader::{
    ReadStopReason, TruncateInfo, WalReader, WalReaderError, WalRecordIterator,
    WatermarkBlockedRecord, WatermarkReadResult,
};
pub use writer::{WalCounters, WalDiskUsage, WalWriter};

/// Parse a WAL segment number from a filename like `wal-NNNNNN.seg`.
///
/// Returns `None` if the filename does not match the expected pattern.
pub fn parse_segment_number(filename: &str) -> Option<u64> {
    let stem = filename.strip_prefix("wal-")?.strip_suffix(".seg")?;
    if stem.is_empty() {
        return None;
    }
    stem.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_standard_segment() {
        assert_eq!(parse_segment_number("wal-000001.seg"), Some(1));
        assert_eq!(parse_segment_number("wal-000042.seg"), Some(42));
        assert_eq!(parse_segment_number("wal-999999.seg"), Some(999999));
    }

    #[test]
    fn test_parse_large_segment_number() {
        // Segment numbers > 999999 produce >6 digit filenames
        assert_eq!(parse_segment_number("wal-1000000.seg"), Some(1000000));
    }

    #[test]
    fn test_parse_rejects_non_wal_files() {
        assert_eq!(parse_segment_number("wal-000001.meta"), None);
        assert_eq!(parse_segment_number("snapshot-000001.seg"), None);
        assert_eq!(parse_segment_number("wal-.seg"), None);
        assert_eq!(parse_segment_number("wal-backup.seg"), None);
        assert_eq!(parse_segment_number("random.txt"), None);
        assert_eq!(parse_segment_number(""), None);
    }
}
