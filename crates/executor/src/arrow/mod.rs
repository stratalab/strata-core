//! Arrow interoperability layer for Strata.
//!
//! Provides import/export between Strata primitives and Apache Arrow RecordBatches,
//! with file I/O support for Parquet, CSV, and JSONL formats.

mod writer;

pub use writer::{detect_format, write_file, FileFormat};
