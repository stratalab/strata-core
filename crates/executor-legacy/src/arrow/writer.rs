//! File writers: RecordBatch -> Parquet, CSV, JSONL files.

use std::fs::File;
use std::path::Path;

use arrow::record_batch::RecordBatch;

use crate::{Error, Result};

/// Supported file formats for Arrow import/export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    /// Apache Parquet (columnar, compressed, schema-preserving).
    Parquet,
    /// CSV with header row.
    Csv,
    /// JSON Lines (one JSON object per line).
    Jsonl,
}

/// Detect file format from extension.
///
/// Mapping: `.parquet` -> Parquet, `.csv` -> CSV, `.jsonl`/`.json` -> JSONL.
pub fn detect_format(path: &Path) -> Result<FileFormat> {
    match path.extension().and_then(|e| e.to_str()) {
        Some("parquet") => Ok(FileFormat::Parquet),
        Some("csv") => Ok(FileFormat::Csv),
        Some("jsonl") | Some("json") => Ok(FileFormat::Jsonl),
        Some(ext) => Err(Error::InvalidInput {
            reason: format!(
                "unrecognized file extension '.{ext}'. Expected .parquet, .csv, .jsonl, or .json"
            ),
            hint: None,
        }),
        None => Err(Error::InvalidInput {
            reason: "file has no extension; cannot detect format".into(),
            hint: Some("Specify --format parquet|csv|jsonl".into()),
        }),
    }
}

/// Write RecordBatches to a file. Returns bytes written.
pub fn write_file(path: &Path, format: FileFormat, batches: &[RecordBatch]) -> Result<u64> {
    if batches.is_empty() {
        return Err(Error::InvalidInput {
            reason: "no data to write".into(),
            hint: None,
        });
    }

    let schema = batches[0].schema();

    match format {
        FileFormat::Parquet => {
            let file = create_file(path)?;
            let props = parquet::file::properties::WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .build();
            let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))
                .map_err(|e| Error::Io {
                    reason: format!("failed to create Parquet writer: {e}"),
                    hint: None,
                })?;
            for batch in batches {
                writer.write(batch).map_err(|e| Error::Io {
                    reason: format!("failed to write Parquet batch: {e}"),
                    hint: None,
                })?;
            }
            writer.close().map_err(|e| Error::Io {
                reason: format!("failed to finalize Parquet file: {e}"),
                hint: None,
            })?;
        }
        FileFormat::Csv => {
            let file = create_file(path)?;
            let mut writer = arrow::csv::WriterBuilder::new()
                .with_header(true)
                .build(file);
            for batch in batches {
                writer.write(batch).map_err(|e| Error::Io {
                    reason: format!("failed to write CSV batch: {e}"),
                    hint: None,
                })?;
            }
        }
        FileFormat::Jsonl => {
            let file = create_file(path)?;
            let mut writer = arrow::json::LineDelimitedWriter::new(file);
            for batch in batches {
                writer.write(batch).map_err(|e| Error::Io {
                    reason: format!("failed to write JSONL batch: {e}"),
                    hint: None,
                })?;
            }
            writer.finish().map_err(|e| Error::Io {
                reason: format!("failed to finalize JSONL file: {e}"),
                hint: None,
            })?;
        }
    }

    let metadata = std::fs::metadata(path).map_err(|e| Error::Io {
        reason: format!("failed to stat output file: {e}"),
        hint: None,
    })?;
    Ok(metadata.len())
}

fn create_file(path: &Path) -> Result<File> {
    File::create(path).map_err(|e| Error::Io {
        reason: format!("failed to create file '{}': {e}", path.display()),
        hint: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
                Arc::new(Int64Array::from(vec![30, 25, 35])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_write_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let batch = sample_batch();

        let bytes = write_file(&path, FileFormat::Parquet, &[batch]).unwrap();
        assert!(bytes > 0);

        // Read back and verify schema + data
        let file = File::open(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].schema().field(0).name(), "name");
        assert_eq!(batches[0].schema().field(1).name(), "age");

        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
        assert_eq!(names.value(2), "Carol");

        let ages = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ages.value(0), 30);
        assert_eq!(ages.value(1), 25);
        assert_eq!(ages.value(2), 35);
    }

    #[test]
    fn test_write_parquet_multiple_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("multi.parquet");
        let batch1 = sample_batch();
        let batch2 = sample_batch();

        let bytes = write_file(&path, FileFormat::Parquet, &[batch1, batch2]).unwrap();
        assert!(bytes > 0);

        let file = File::open(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let total_rows: usize = reader.map(|r| r.unwrap().num_rows()).sum();
        assert_eq!(total_rows, 6);
    }

    #[test]
    fn test_write_csv() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.csv");
        let batch = sample_batch();

        let bytes = write_file(&path, FileFormat::Csv, &[batch]).unwrap();
        assert!(bytes > 0);

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 4); // header + 3 data rows
        assert_eq!(lines[0], "name,age");
        assert_eq!(lines[1], "Alice,30");
        assert_eq!(lines[2], "Bob,25");
        assert_eq!(lines[3], "Carol,35");
    }

    #[test]
    fn test_write_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.jsonl");
        let batch = sample_batch();

        let bytes = write_file(&path, FileFormat::Jsonl, &[batch]).unwrap();
        assert!(bytes > 0);

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 3);

        let row0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(row0["name"], "Alice");
        assert_eq!(row0["age"], 30);

        let row1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(row1["name"], "Bob");
        assert_eq!(row1["age"], 25);

        let row2: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(row2["name"], "Carol");
        assert_eq!(row2["age"], 35);
    }

    #[test]
    fn test_detect_format() {
        assert_eq!(
            detect_format(Path::new("data.parquet")).unwrap(),
            FileFormat::Parquet
        );
        assert_eq!(
            detect_format(Path::new("data.csv")).unwrap(),
            FileFormat::Csv
        );
        assert_eq!(
            detect_format(Path::new("data.jsonl")).unwrap(),
            FileFormat::Jsonl
        );
        assert_eq!(
            detect_format(Path::new("data.json")).unwrap(),
            FileFormat::Jsonl
        );

        // Unknown extension
        assert!(detect_format(Path::new("data.xlsx")).is_err());

        // No extension
        assert!(detect_format(Path::new("data")).is_err());
    }

    #[test]
    fn test_write_empty_batches_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.parquet");
        let result = write_file(&path, FileFormat::Parquet, &[]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no data to write"), "got: {err}");
    }
}
