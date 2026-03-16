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
pub use reader::{ReadStopReason, TruncateInfo, WalReader, WalReaderError, WalRecordIterator};
pub use writer::{WalCounters, WalDiskUsage, WalWriter};
