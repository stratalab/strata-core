//! Arrow-compatible file writers.

use std::fs::File;
use std::path::Path;

use arrow::record_batch::RecordBatch;

use crate::error::ExecutorError;
use crate::types::ArrowFileFormat;

use super::{internal_error, invalid_input, io_error};

pub(crate) fn write_file(
    path: &Path,
    format: ArrowFileFormat,
    batches: &[RecordBatch],
) -> Result<u64, ExecutorError> {
    if batches.is_empty() {
        return Err(invalid_input(
            "invalid_argument.executor.arrow_empty_export",
            "no Arrow batches to write",
        ));
    }

    let schema = batches[0].schema();
    for batch in batches {
        if batch.schema() != schema {
            return Err(internal_error(
                "cannot write Arrow batches with different schemas",
            ));
        }
    }

    match format {
        ArrowFileFormat::Parquet => write_parquet(path, batches)?,
        ArrowFileFormat::Csv => write_csv(path, batches)?,
        ArrowFileFormat::Jsonl => write_jsonl(path, batches)?,
    }

    let metadata = std::fs::metadata(path)
        .map_err(|error| io_error(format!("failed to stat '{}': {error}", path.display())))?;
    Ok(metadata.len())
}

fn write_parquet(path: &Path, batches: &[RecordBatch]) -> Result<(), ExecutorError> {
    let file = create_file(path)?;
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, batches[0].schema(), Some(properties))
            .map_err(|error| io_error(format!("failed to create Parquet writer: {error}")))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|error| io_error(format!("failed to write Parquet batch: {error}")))?;
    }
    writer
        .close()
        .map_err(|error| io_error(format!("failed to finalize Parquet file: {error}")))?;
    Ok(())
}

fn write_csv(path: &Path, batches: &[RecordBatch]) -> Result<(), ExecutorError> {
    let file = create_file(path)?;
    let mut writer = arrow::csv::WriterBuilder::new()
        .with_header(true)
        .build(file);
    for batch in batches {
        writer
            .write(batch)
            .map_err(|error| io_error(format!("failed to write CSV batch: {error}")))?;
    }
    Ok(())
}

fn write_jsonl(path: &Path, batches: &[RecordBatch]) -> Result<(), ExecutorError> {
    let file = create_file(path)?;
    let mut writer = arrow::json::LineDelimitedWriter::new(file);
    for batch in batches {
        writer
            .write(batch)
            .map_err(|error| io_error(format!("failed to write JSONL batch: {error}")))?;
    }
    writer
        .finish()
        .map_err(|error| io_error(format!("failed to finalize JSONL file: {error}")))?;
    Ok(())
}

fn create_file(path: &Path) -> Result<File, ExecutorError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|error| {
            io_error(format!(
                "failed to create output directory '{}': {error}",
                parent.display()
            ))
        })?;
    }
    File::create(path).map_err(|error| {
        io_error(format!(
            "failed to create file '{}': {error}",
            path.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tempfile::TempDir;

    use crate::arrow::reader::read_file;
    use crate::ExecutorErrorClass;

    use super::*;

    #[test]
    fn writes_each_format_and_reads_rows_back() {
        let dir = TempDir::new().expect("temp dir");
        for (format, name) in [
            (ArrowFileFormat::Parquet, "rows.parquet"),
            (ArrowFileFormat::Csv, "rows.csv"),
            (ArrowFileFormat::Jsonl, "rows.jsonl"),
        ] {
            let path = dir.path().join(name);
            let batch = sample_batch(&["a", "b"], &[1, 2]);
            let size = write_file(&path, format, &[batch]).expect("write succeeds");
            assert!(size > 0);

            let (schema, batches) = read_file(&path, format).expect("read succeeds");
            assert_eq!(schema.field(0).name(), "key");
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        }
    }

    #[test]
    fn writes_multiple_batches_with_one_schema() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("rows.jsonl");
        write_file(
            &path,
            ArrowFileFormat::Jsonl,
            &[
                sample_batch(&["a", "b"], &[1, 2]),
                sample_batch(&["c", "d"], &[3, 4]),
            ],
        )
        .expect("write succeeds");

        let (_, batches) = read_file(&path, ArrowFileFormat::Jsonl).expect("read succeeds");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    }

    #[test]
    fn rejects_empty_or_mismatched_batches_with_stable_errors() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("rows.jsonl");
        let error = write_file(&path, ArrowFileFormat::Jsonl, &[]).expect_err("empty fails");
        assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
        assert_eq!(error.code(), "invalid_argument.executor.arrow_empty_export");

        let different_schema = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["x"]))],
        )
        .expect("different schema batch");
        let error = write_file(
            &path,
            ArrowFileFormat::Jsonl,
            &[sample_batch(&["a"], &[1]), different_schema],
        )
        .expect_err("mismatched schemas fail");
        assert_eq!(error.class(), ExecutorErrorClass::Internal);
        assert_eq!(error.code(), "internal.executor.arrow");
    }

    #[test]
    fn creates_nested_output_directories() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("nested").join("rows.csv");
        let size = write_file(&path, ArrowFileFormat::Csv, &[sample_batch(&["a"], &[1])])
            .expect("write succeeds");
        assert!(size > 0);
        assert!(path.exists());
    }

    fn sample_batch(keys: &[&str], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .expect("sample batch")
    }
}
