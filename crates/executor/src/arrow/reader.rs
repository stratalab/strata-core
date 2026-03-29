//! File readers: Parquet, CSV, JSONL files -> RecordBatch iterators.

use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::writer::FileFormat;
use crate::{Error, Result};

/// Open a file and return all RecordBatches plus the schema.
///
/// Returns `(Schema, Vec<RecordBatch>)` — we collect eagerly because Arrow's
/// readers hold borrows on the file, making a trait-object iterator awkward
/// across format-specific reader types.
pub fn read_file(path: &Path, format: FileFormat) -> Result<(Schema, Vec<RecordBatch>)> {
    if !path.exists() {
        return Err(Error::InvalidInput {
            reason: format!("file not found: '{}'", path.display()),
            hint: None,
        });
    }

    match format {
        FileFormat::Parquet => read_parquet(path),
        FileFormat::Csv => read_csv(path),
        FileFormat::Jsonl => read_jsonl(path),
    }
}

fn read_parquet(path: &Path) -> Result<(Schema, Vec<RecordBatch>)> {
    let file = open_file(path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Io {
            reason: format!("failed to open Parquet file: {e}"),
            hint: None,
        })?;

    let schema = builder.schema().as_ref().clone();
    let reader = builder.build().map_err(|e| Error::Io {
        reason: format!("failed to build Parquet reader: {e}"),
        hint: None,
    })?;

    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Io {
            reason: format!("failed to read Parquet batch: {e}"),
            hint: None,
        })?;

    Ok((schema, batches))
}

fn read_csv(path: &Path) -> Result<(Schema, Vec<RecordBatch>)> {
    // Infer schema from first 100 rows.
    let schema = arrow::csv::reader::infer_schema_from_files(
        &[path.to_string_lossy().into_owned()],
        b',',
        Some(100),
        true, // has_header
    )
    .map_err(|e| Error::Io {
        reason: format!("failed to infer CSV schema: {e}"),
        hint: None,
    })?;

    let schema = Arc::new(schema);

    // Re-open file and build reader with inferred schema.
    let file = open_file(path)?;
    let reader = arrow::csv::ReaderBuilder::new(schema.clone())
        .with_header(true)
        .build(file)
        .map_err(|e| Error::Io {
            reason: format!("failed to build CSV reader: {e}"),
            hint: None,
        })?;

    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Io {
            reason: format!("failed to read CSV batch: {e}"),
            hint: None,
        })?;

    Ok((schema.as_ref().clone(), batches))
}

fn read_jsonl(path: &Path) -> Result<(Schema, Vec<RecordBatch>)> {
    // Infer schema from first 100 lines.
    let file = open_file(path)?;
    let mut buf_reader = BufReader::new(file);
    let (schema, _) =
        arrow::json::reader::infer_json_schema_from_seekable(&mut buf_reader, Some(100)).map_err(
            |e| Error::Io {
                reason: format!("failed to infer JSONL schema: {e}"),
                hint: None,
            },
        )?;

    let schema = Arc::new(schema);

    // Rewind and build reader with inferred schema.
    buf_reader
        .seek(std::io::SeekFrom::Start(0))
        .map_err(|e| Error::Io {
            reason: format!("failed to seek JSONL file: {e}"),
            hint: None,
        })?;

    let reader = arrow::json::ReaderBuilder::new(schema.clone())
        .build(buf_reader)
        .map_err(|e| Error::Io {
            reason: format!("failed to build JSONL reader: {e}"),
            hint: None,
        })?;

    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Io {
            reason: format!("failed to read JSONL batch: {e}"),
            hint: None,
        })?;

    Ok((schema.as_ref().clone(), batches))
}

fn open_file(path: &Path) -> Result<File> {
    File::open(path).map_err(|e| Error::Io {
        reason: format!("failed to open file '{}': {e}", path.display()),
        hint: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::writer::{detect_format, write_file};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};

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
    fn test_read_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");

        // Write with v1a writer.
        let batch = sample_batch();
        write_file(&path, FileFormat::Parquet, &[batch]).unwrap();

        // Read back.
        let format = detect_format(&path).unwrap();
        let (schema, batches) = read_file(&path, format).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "age");

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);

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
    fn test_read_csv() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.csv");
        std::fs::write(&path, "name,age\nAlice,30\nBob,25\nCarol,35\n").unwrap();

        let (schema, batches) = read_file(&path, FileFormat::Csv).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "age");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Verify first batch data.
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");

        let ages = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ages.value(0), 30);
    }

    #[test]
    fn test_read_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.jsonl");
        std::fs::write(
            &path,
            r#"{"name":"Alice","age":30}
{"name":"Bob","age":25}
{"name":"Carol","age":35}
"#,
        )
        .unwrap();

        let (schema, batches) = read_file(&path, FileFormat::Jsonl).unwrap();

        // Schema should have name (utf8) and age (int64).
        assert_eq!(schema.fields().len(), 2);

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Find the name column by name (field order may vary with JSONL inference).
        let name_idx = schema.index_of("name").unwrap();
        let age_idx = schema.index_of("age").unwrap();

        let names = batches[0]
            .column(name_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");

        let ages = batches[0]
            .column(age_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ages.value(0), 30);
    }

    #[test]
    fn test_read_file_not_found() {
        let result = read_file(
            Path::new("/tmp/nonexistent_12345.parquet"),
            FileFormat::Parquet,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("file not found"), "got: {err}");
    }

    #[test]
    fn test_read_empty_csv() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.csv");
        std::fs::write(&path, "").unwrap();

        // Empty CSV (no header, no data) — Arrow infers 0 columns, returns 0 batches.
        let (schema, batches) = read_file(&path, FileFormat::Csv).unwrap();
        assert_eq!(schema.fields().len(), 0);
        assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0));
    }

    #[test]
    fn test_read_empty_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.jsonl");
        std::fs::write(&path, "").unwrap();

        // Empty JSONL — schema inference finds 0 fields, reader returns 0 batches.
        let (schema, batches) = read_file(&path, FileFormat::Jsonl).unwrap();
        assert_eq!(schema.fields().len(), 0);
        assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0));
    }
}
