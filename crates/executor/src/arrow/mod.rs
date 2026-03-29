//! Arrow interoperability layer for Strata.
//!
//! Provides import/export between Strata primitives and Apache Arrow RecordBatches,
//! with file I/O support for Parquet, CSV, and JSONL formats.

mod export;
mod reader;
mod writer;

pub use export::{export_to_batches, value_to_string, ExportSource};
pub use reader::read_file;
pub use writer::{detect_format, write_file, FileFormat};
