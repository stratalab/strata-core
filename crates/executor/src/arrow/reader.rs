//! Arrow-compatible file readers.

use std::fs::File;
use std::io::{BufReader, Seek};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::ExecutorError;
use crate::types::ArrowFileFormat;

use super::{invalid_input, io_error};

/// Column names Strata's own Arrow exporters declare as `Utf8`. CSV carries no
/// embedded schema, so `infer_schema_from_files` would re-type a numeric-looking
/// text column — a `"00501"` key becomes `Int64 501` and its leading zeros are
/// lost on round-trip (#3077). We force these columns back to `Utf8`, mirroring
/// how `COPY`/`.import` parse against the destination's known column types,
/// while leaving genuinely-numeric columns (graph `weight`, the discarded
/// `version`/`timestamp`/`sequence`/`vector_revision`) to inference. Parquet and
/// JSONL carry real schemas, so only the CSV path needs this.
const STRATA_TEXT_COLUMNS: &[&str] = &[
    "key",
    "key_encoding",
    "value",
    "value_encoding",
    "document",
    "event_type",
    "payload",
    "previous_hash",
    "hash",
    "metadata",
    "graph",
    "node_id",
    "properties",
    "binding",
    "src",
    "edge_type",
    "dst",
];

/// Re-type every column CSV inference may have numified back to `Utf8` when its
/// name is one Strata exports as text. Pure over the schema so it can be
/// truth-tabled directly.
fn force_text_columns(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if STRATA_TEXT_COLUMNS.contains(&field.name().as_str()) {
                Arc::new(Field::new(
                    field.name(),
                    DataType::Utf8,
                    field.is_nullable(),
                ))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    Schema::new(fields)
}

pub(crate) fn read_file(
    path: &Path,
    format: ArrowFileFormat,
) -> Result<(Schema, Vec<RecordBatch>), ExecutorError> {
    if !path.exists() {
        return Err(invalid_input(
            "invalid_argument.executor.arrow_input_missing",
            format!("file not found: '{}'", path.display()),
        ));
    }

    match format {
        ArrowFileFormat::Parquet => read_parquet(path),
        ArrowFileFormat::Csv => read_csv(path),
        ArrowFileFormat::Jsonl => read_jsonl(path),
    }
}

fn read_parquet(path: &Path) -> Result<(Schema, Vec<RecordBatch>), ExecutorError> {
    let file = open_file(path)?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| io_error(format!("failed to open Parquet file: {error}")))?;
    let schema = builder.schema().as_ref().clone();
    let reader = builder
        .build()
        .map_err(|error| io_error(format!("failed to build Parquet reader: {error}")))?;
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io_error(format!("failed to read Parquet batch: {error}")))?;
    Ok((schema, batches))
}

fn read_csv(path: &Path) -> Result<(Schema, Vec<RecordBatch>), ExecutorError> {
    let schema = arrow::csv::reader::infer_schema_from_files(
        &[path.to_string_lossy().into_owned()],
        b',',
        // Infer over the whole file, not a 100-row prefix: a column whose type is
        // only disambiguated by a later row must not be mis-typed (#3080).
        None,
        true,
    )
    .map_err(|error| io_error(format!("failed to infer CSV schema: {error}")))?;
    let schema = Arc::new(force_text_columns(&schema));
    let file = open_file(path)?;
    let reader = arrow::csv::ReaderBuilder::new(schema.clone())
        .with_header(true)
        .build(file)
        .map_err(|error| io_error(format!("failed to build CSV reader: {error}")))?;
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io_error(format!("failed to read CSV batch: {error}")))?;
    Ok((schema.as_ref().clone(), batches))
}

fn read_jsonl(path: &Path) -> Result<(Schema, Vec<RecordBatch>), ExecutorError> {
    let file = open_file(path)?;
    let mut reader = BufReader::new(file);
    // Infer over the whole file, not a 100-row prefix: a field first appearing
    // after row 100 must not be dropped from the schema and silently lost (#3080).
    let (schema, _) = arrow::json::reader::infer_json_schema_from_seekable(&mut reader, None)
        .map_err(|error| io_error(format!("failed to infer JSONL schema: {error}")))?;
    let schema = Arc::new(schema);
    reader
        .seek(std::io::SeekFrom::Start(0))
        .map_err(|error| io_error(format!("failed to seek JSONL file: {error}")))?;
    let reader = arrow::json::ReaderBuilder::new(schema.clone())
        .build(reader)
        .map_err(|error| io_error(format!("failed to build JSONL reader: {error}")))?;
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io_error(format!("failed to read JSONL batch: {error}")))?;
    Ok((schema.as_ref().clone(), batches))
}

fn open_file(path: &Path) -> Result<File, ExecutorError> {
    File::open(path)
        .map_err(|error| io_error(format!("failed to open file '{}': {error}", path.display())))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tempfile::TempDir;

    use crate::arrow::writer::write_file;
    use crate::ExecutorErrorClass;

    use super::*;

    #[test]
    fn reads_each_supported_format_with_schema_and_rows() {
        let dir = TempDir::new().expect("temp dir");
        for (format, name) in [
            (ArrowFileFormat::Parquet, "rows.parquet"),
            (ArrowFileFormat::Csv, "rows.csv"),
            (ArrowFileFormat::Jsonl, "rows.jsonl"),
        ] {
            let path = dir.path().join(name);
            write_file(&path, format, &[sample_batch()]).expect("write fixture");
            let (schema, batches) = read_file(&path, format).expect("read succeeds");
            assert_eq!(
                schema
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>(),
                vec!["key", "value"]
            );
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        }
    }

    #[test]
    fn missing_and_malformed_files_have_stable_error_classes() {
        let dir = TempDir::new().expect("temp dir");
        let missing = dir.path().join("missing.parquet");
        let error = read_file(&missing, ArrowFileFormat::Parquet).expect_err("missing file fails");
        assert_eq!(error.class(), ExecutorErrorClass::InvalidInput);
        assert_eq!(
            error.code(),
            "invalid_argument.executor.arrow_input_missing"
        );

        let malformed = dir.path().join("bad.parquet");
        std::fs::write(&malformed, b"not parquet").expect("write malformed");
        let error =
            read_file(&malformed, ArrowFileFormat::Parquet).expect_err("malformed file fails");
        assert_eq!(error.class(), ExecutorErrorClass::Unavailable);
        assert_eq!(error.code(), "unavailable.executor.arrow_io");
    }

    #[test]
    fn force_text_columns_utf8s_known_string_columns_and_leaves_numerics() {
        // A CSV whose text columns inference numified, plus genuinely-numeric
        // columns that must stay as inferred.
        let inferred = Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("node_id", DataType::Int64, false),
            Field::new("weight", DataType::Float64, false),
            Field::new("version", DataType::UInt64, true),
        ]);

        let forced = force_text_columns(&inferred);
        let by_name = |name: &str| {
            forced
                .field_with_name(name)
                .expect("field present")
                .data_type()
                .clone()
        };

        // Known text columns are forced back to Utf8...
        assert_eq!(by_name("key"), DataType::Utf8);
        assert_eq!(by_name("value"), DataType::Utf8);
        assert_eq!(by_name("node_id"), DataType::Utf8);
        // ...while genuinely-numeric columns are left exactly as inferred.
        assert_eq!(by_name("weight"), DataType::Float64);
        assert_eq!(by_name("version"), DataType::UInt64);
        // Nullability is preserved when a column is re-typed.
        assert!(forced.field_with_name("version").unwrap().is_nullable());
        assert!(!forced.field_with_name("key").unwrap().is_nullable());
    }

    fn sample_batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .expect("sample batch")
    }
}
