//! Handler for the `ArrowImport` command.

use std::sync::Arc;

use crate::bridge::Primitives;
use crate::types::BranchId;
use crate::{Error, Output, Result};

/// Handle ArrowImport command.
#[cfg(feature = "arrow")]
#[allow(clippy::too_many_arguments)]
pub fn arrow_import(
    p: &Arc<Primitives>,
    branch: BranchId,
    space: String,
    file_path: String,
    target: String,
    key_column: Option<String>,
    value_column: Option<String>,
    collection: Option<String>,
    format: Option<String>,
) -> Result<Output> {
    use std::path::Path;

    use crate::arrow::{detect_format, read_file, resolve_mapping, ImportPrimitive};
    use crate::bridge::to_core_branch_id;
    use crate::handlers::require_branch_exists;

    require_branch_exists(p, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;

    let path = Path::new(&file_path);

    // Validate file exists.
    if !path.exists() {
        return Err(Error::InvalidInput {
            reason: format!("file not found: '{}'", path.display()),
            hint: None,
        });
    }

    // Resolve format.
    let fmt = match &format {
        Some(f) => parse_format(f)?,
        None => detect_format(path)?,
    };

    // Parse target primitive.
    let primitive = match target.as_str() {
        "kv" => ImportPrimitive::Kv,
        "json" => ImportPrimitive::Json,
        "vector" => ImportPrimitive::Vector,
        other => {
            return Err(Error::InvalidInput {
                reason: format!("unknown import target '{other}'. Expected: kv, json, vector"),
                hint: None,
            });
        }
    };

    // Vector requires --collection.
    if primitive == ImportPrimitive::Vector && collection.is_none() {
        return Err(Error::InvalidInput {
            reason: "--collection is required for vector import".into(),
            hint: None,
        });
    }

    // Read file.
    let (schema, batches) = read_file(path, fmt)?;

    // Resolve column mapping.
    let mapping = resolve_mapping(
        &schema,
        primitive,
        key_column.as_deref(),
        value_column.as_deref(),
    )?;

    // Dispatch to ingest.
    let result = match primitive {
        ImportPrimitive::Kv => crate::arrow::ingest_kv(p, branch_id, &space, &batches, &mapping)?,
        ImportPrimitive::Json => {
            crate::arrow::ingest_json(p, branch_id, &space, &batches, &mapping)?
        }
        ImportPrimitive::Vector => {
            let coll = collection.unwrap(); // validated above
            crate::arrow::ingest_vector(p, branch_id, &space, &coll, &batches, &mapping)?
        }
    };

    Ok(Output::ArrowImported {
        rows_imported: result.rows_imported,
        rows_skipped: result.rows_skipped,
        target,
        file_path,
    })
}

/// Handle ArrowImport when the arrow feature is not enabled.
#[cfg(not(feature = "arrow"))]
#[allow(clippy::too_many_arguments)]
pub fn arrow_import(
    _p: &Arc<Primitives>,
    _branch: BranchId,
    _space: String,
    _file_path: String,
    _target: String,
    _key_column: Option<String>,
    _value_column: Option<String>,
    _collection: Option<String>,
    _format: Option<String>,
) -> Result<Output> {
    Err(Error::Internal {
        reason: "Import requires the 'arrow' feature".into(),
        hint: Some("Rebuild with: cargo build --features arrow".into()),
    })
}

#[cfg(feature = "arrow")]
fn parse_format(s: &str) -> Result<crate::arrow::FileFormat> {
    match s {
        "parquet" => Ok(crate::arrow::FileFormat::Parquet),
        "csv" => Ok(crate::arrow::FileFormat::Csv),
        "jsonl" => Ok(crate::arrow::FileFormat::Jsonl),
        other => Err(Error::InvalidInput {
            reason: format!("unknown format '{other}'. Expected: parquet, csv, jsonl"),
            hint: None,
        }),
    }
}
