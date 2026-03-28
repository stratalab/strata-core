# Arrow v1b: Import

**Status:** Draft
**Scope:** Import KV, JSON, Vector data from Parquet, CSV, JSONL files
**Estimated size:** ~1,000 lines
**Prerequisite:** v1a (export) — provides deps, feature gate, `arrow/` module, `FileFormat`, `writer.rs`

---

## Goal

A developer with a file can load data into Strata in one command. No code, no SDK.

```bash
strata import users.parquet --into kv --key-column id
strata import orders.csv --into json --key-column order_id
strata import embeddings.parquet --into vector --collection docs --key-column doc_id
```

## Scope

**Primitives:** KV, JSON, Vector (import)
**Formats:** Parquet, CSV, JSONL

**Out of scope:** Event import (append-by-nature, less demand for bulk load), Graph import (complex two-table relationship model), NumPy/Arrow IPC formats, progress bars

---

## Implementation

### Step 1: File readers — `arrow/reader.rs`

Thin wrappers around Arrow's built-in readers. All produce `RecordBatch` iterators so the ingest pipeline is format-agnostic.

```rust
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatchReader;

/// Open a file and return its schema + a RecordBatch iterator.
pub fn open_file(
    path: &Path,
    format: FileFormat,  // reuse from writer.rs (v1a)
) -> Result<(Schema, Box<dyn RecordBatchReader>)>;
```

**Parquet:** `ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?` — schema from file metadata, row groups as batches.

**CSV:** `arrow::csv::ReaderBuilder::new(inferred_schema).build(file)?` — infer schema from header + first 100 rows via `arrow::csv::infer_schema_from_files`. Re-open file after inference.

**JSONL:** `arrow::json::ReaderBuilder::new(inferred_schema).build(file)?` — infer schema from first 100 lines. The `arrow::json` reader handles one JSON object per line natively.

Format detected from extension (reusing `detect_format` from v1a), overridable with `--format`.

---

### Step 2: Column mapping — `arrow/schema.rs`

Resolves which columns in the input file map to which primitive fields.

```rust
pub enum ImportPrimitive {
    Kv,
    Json,
    Vector,
}

pub struct ImportMapping {
    /// Column index for the key field.
    pub key_idx: usize,
    /// Column index for the explicit value/document/embedding field (if any).
    pub value_idx: Option<usize>,
    /// Column indices for remaining fields (become JSON metadata).
    pub extra_indices: Vec<usize>,
    /// Column names for extra fields (for JSON serialization).
    pub extra_names: Vec<String>,
}

/// Resolve column mapping from CLI flags + file schema.
pub fn resolve_mapping(
    schema: &Schema,
    primitive: ImportPrimitive,
    key_column: Option<&str>,
    value_column: Option<&str>,
) -> Result<ImportMapping>;
```

#### Resolution logic

**Key column:**
1. If `--key-column` provided → use it (error if not in schema)
2. Else try in order: `key`, `_id`, `id` → use first match
3. If none found → error: `"No key column found. Specify --key-column <COL>. Available columns: ..."`

**Value column (KV):**
1. If `--value-column` provided → use it
2. Else try: `value`
3. If not found and schema has exactly 2 columns → use the non-key column
4. If not found and schema has >2 columns → no explicit value column; all non-key columns become a JSON object

**Document column (JSON):**
1. If `--value-column` provided → use it
2. Else try: `document`, `value`, `doc`, `body`
3. If not found → no explicit document column; all non-key columns become a JSON object (this is the natural CSV→JSON mapping)

**Embedding column (Vector):**
1. If `--value-column` provided → use it
2. Else try: `embedding`, `vector`, `embeddings`, `emb`
3. Must be a list type (FixedSizeList\<f32\> or List\<f32\>) → error if wrong type
4. Remaining non-key, non-embedding columns → metadata JSON

#### Error messages

Column resolution errors should be actionable:
```
Error: No key column found in 'users.parquet'.
  Available columns: name (utf8), email (utf8), age (int64)
  Hint: specify --key-column <COL>
```

```
Error: No embedding column found in 'data.parquet'.
  Available columns: id (utf8), text (utf8), scores (list<f64>)
  Hint: specify --value-column <COL> pointing to a float list column
```

---

### Step 3: Type coercion — `arrow/schema.rs`

Converts Arrow array values to Strata `Value` types for KV/JSON ingest.

```rust
/// Extract a single value from an Arrow array at the given row index.
pub fn arrow_to_value(array: &dyn Array, row: usize) -> Result<Value>;
```

Mapping:

| Arrow type | Strata Value |
|------------|-------------|
| Utf8, LargeUtf8 | `Value::String` |
| Int8..Int64 | `Value::Int` |
| UInt8..UInt64 | `Value::Int` (cast to i64) |
| Float32, Float64 | `Value::Float` |
| Boolean | `Value::Bool` |
| Binary, LargeBinary | `Value::Bytes` |
| Null | `Value::Null` |
| List, Struct | `Value::String` (JSON-serialized) |

For the "remaining columns → JSON object" path:

```rust
/// Serialize non-key, non-value columns of a row as a JSON object.
pub fn row_to_json(
    batch: &RecordBatch,
    row: usize,
    columns: &[(usize, String)],  // (column index, column name)
) -> Result<String>;
```

---

### Step 4: Ingest pipeline — `arrow/ingest.rs`

Core import logic. Reads RecordBatches from the file reader, maps columns, writes to primitives.

```rust
pub struct ImportResult {
    pub rows_imported: u64,
    pub rows_skipped: u64,
    pub batches_processed: u64,
}

pub fn ingest_kv(
    primitives: &Arc<Primitives>,
    branch_id: CoreBranchId,
    space: &str,
    reader: Box<dyn RecordBatchReader>,
    mapping: &ImportMapping,
) -> Result<ImportResult>;

pub fn ingest_json(
    primitives: &Arc<Primitives>,
    branch_id: CoreBranchId,
    space: &str,
    reader: Box<dyn RecordBatchReader>,
    mapping: &ImportMapping,
) -> Result<ImportResult>;

pub fn ingest_vector(
    primitives: &Arc<Primitives>,
    branch_id: CoreBranchId,
    space: &str,
    collection: &str,
    reader: Box<dyn RecordBatchReader>,
    mapping: &ImportMapping,
) -> Result<ImportResult>;
```

#### KV ingest

Per RecordBatch:
1. Extract key strings from `key_idx` column
2. If `value_idx` is Some → extract values via `arrow_to_value()`
3. If `value_idx` is None → serialize extra columns as JSON object per row
4. Collect `Vec<(String, Value)>`, call `p.kv.batch_put()`
5. On per-row errors (null key, coercion failure), skip the row, increment `rows_skipped`

Uses `batch_put` for efficiency — one engine call per RecordBatch rather than per row.

#### JSON ingest

Per RecordBatch:
1. Extract key strings from `key_idx` column
2. If `value_idx` is Some → use that column as the document string
3. If `value_idx` is None → serialize extra columns as JSON object per row
4. Call `p.json.set()` per document

No batch API for JSON today. Individual `set()` calls. For typical import sizes (10K-100K docs), this is fine. If profiling shows a bottleneck, a `batch_set` can be added to the JSON primitive later.

#### Vector ingest

Per RecordBatch:
1. Extract key strings from `key_idx` column
2. Extract embeddings from `value_idx` column as `Vec<f32>` (downcast to FixedSizeListArray or ListArray, then to Float32Array)
3. Extract optional metadata from extra columns as JSON string
4. On first batch: if collection doesn't exist, create it with dimension inferred from the embedding length, default metric (cosine), default storage dtype (f32)
5. Call `p.vector.insert()` per vector

Individual inserts because the vector store doesn't have a batch API. The embedding dimension is validated against the collection config — mismatches error.

#### Error handling

Row-level errors (null key, type mismatch, dimension mismatch) skip the row and increment `rows_skipped`. The import continues. The final `ImportResult` reports both imported and skipped counts so the user knows if something went wrong.

Batch-level errors (I/O failure reading the file, schema mismatch) abort the import with an error. Already-imported rows from previous batches remain committed — no rollback across batches.

---

### Step 5: Command and CLI

#### New Command variant

```rust
// command.rs
ArrowImport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    branch: Option<BranchId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    space: Option<String>,
    /// Path to the input file.
    file_path: String,
    /// Target primitive.
    target: String,  // "kv", "json", "vector"
    /// Column to use as key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key_column: Option<String>,
    /// Column to use as value/document/embedding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value_column: Option<String>,
    /// Vector collection name (required for vector target).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    collection: Option<String>,
    /// Override file format detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    format: Option<String>,  // "parquet", "csv", "jsonl"
},
```

The Command variant is always present in the enum (not feature-gated) so that serialization/deserialization works across feature configurations. The handler returns a feature-gate error when `arrow` is not enabled.

#### New Output variant

```rust
// output.rs
ArrowImported {
    rows_imported: u64,
    rows_skipped: u64,
    target: String,
    file_path: String,
},
```

#### Handler — `handlers/arrow_import.rs`

```rust
#[cfg(feature = "arrow")]
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
    require_branch_exists(p, &branch)?;
    let branch_id = to_core_branch_id(&branch)?;

    let path = Path::new(&file_path);
    let fmt = match format {
        Some(f) => parse_format(&f)?,
        None => detect_format(path)?,
    };

    let (schema, reader) = open_file(path, fmt)?;
    let primitive = parse_import_primitive(&target)?;
    let mapping = resolve_mapping(&schema, primitive, key_column.as_deref(), value_column.as_deref())?;

    let result = match primitive {
        ImportPrimitive::Kv => ingest_kv(p, branch_id, &space, reader, &mapping)?,
        ImportPrimitive::Json => ingest_json(p, branch_id, &space, reader, &mapping)?,
        ImportPrimitive::Vector => {
            let coll = collection.ok_or_else(|| Error::InvalidArgument {
                reason: "--collection is required for vector import".into(),
            })?;
            ingest_vector(p, branch_id, &space, &coll, reader, &mapping)?
        }
    };

    Ok(Output::ArrowImported {
        rows_imported: result.rows_imported,
        rows_skipped: result.rows_skipped,
        target,
        file_path,
    })
}

#[cfg(not(feature = "arrow"))]
pub fn arrow_import(...) -> Result<Output> {
    Err(Error::Internal {
        reason: "Import requires the 'arrow' feature".into(),
        hint: Some("Rebuild with: cargo build --features arrow".into()),
    })
}
```

#### CLI subcommand — `strata import`

New top-level subcommand in `commands.rs`:

```
strata import <FILE> --into <PRIMITIVE> [OPTIONS]

Arguments:
  <FILE>                     Input file path

Required:
  --into <PRIMITIVE>         Target: kv, json, vector

Options:
  --key-column <COL>         Column to use as key (auto-detected if omitted)
  --value-column <COL>       Column to use as value/document/embedding
  --collection <NAME>        Vector collection name (required for vector)
  --format <FMT>             Override format detection (parquet, csv, jsonl)
  -b, --branch <BRANCH>     Target branch (default: default)
  -s, --space <SPACE>        Target space (default: default)
```

---

### Step 6: Testing

#### Unit tests in `arrow/schema.rs` (~8 tests)

1. `test_resolve_key_column_auto` — auto-detect `key` column
2. `test_resolve_key_column_id_fallback` — fallback to `id` when no `key`
3. `test_resolve_key_column_explicit` — `--key-column` override
4. `test_resolve_key_column_missing` — error with available columns listed
5. `test_resolve_value_column_kv` — auto-detect for KV
6. `test_resolve_embedding_column` — auto-detect `embedding` column
7. `test_resolve_embedding_wrong_type` — error on non-list column
8. `test_resolve_extra_columns_as_json` — remaining columns collected

#### Unit tests in `arrow/schema.rs` — type coercion (~5 tests)

1. `test_arrow_to_value_string` — Utf8 → Value::String
2. `test_arrow_to_value_int` — Int64 → Value::Int
3. `test_arrow_to_value_float` — Float64 → Value::Float
4. `test_arrow_to_value_bool` — Boolean → Value::Bool
5. `test_arrow_to_value_null` — Null → Value::Null

#### Integration tests (~6 tests in `executor/tests/arrow_import.rs`)

1. `test_import_kv_from_parquet` — write Parquet with arrow crate → import → verify via `kv get`
2. `test_import_kv_from_csv` — write CSV → import → verify
3. `test_import_json_from_jsonl` — write JSONL → import → verify via `json get`
4. `test_import_json_remaining_columns_as_document` — CSV with no `document` column → all non-key columns become JSON document
5. `test_import_vector_from_parquet` — write Parquet with embedding column → import → verify via `vector search`
6. `test_import_vector_auto_creates_collection` — collection doesn't exist → created with inferred dimension

#### Round-trip tests (~2 tests)

1. `test_roundtrip_kv_parquet` — populate KV → export Parquet (v1a) → import to new branch → compare
2. `test_roundtrip_json_csv` — populate JSON → export CSV → import to new branch → compare

---

## Implementation order

| Order | File | Lines (est.) | Description |
|-------|------|-------------|-------------|
| 1 | `arrow/reader.rs` | ~80 | File → RecordBatch (Parquet, CSV, JSONL) |
| 2 | `arrow/schema.rs` | ~250 | Column mapping + type coercion |
| 3 | `arrow/ingest.rs` | ~300 | RecordBatch → KV/JSON/Vector writes |
| 4 | `command.rs` + `output.rs` | ~40 | ArrowImport command, ArrowImported output |
| 5 | `handlers/arrow_import.rs` | ~60 | Handler dispatch |
| 6 | `cli/commands.rs` | ~40 | `import` subcommand definition |
| 7 | Tests | ~250 | Unit + integration + round-trip |
| **Total** | | **~1,000** | |

Steps 1-2 can be developed in parallel. Step 3 depends on both.

---

## Decisions

1. **No batch API for JSON/Vector:** Use individual `set()`/`insert()` calls. Acceptable for v1 — most imports are 10K-100K rows. Batch APIs can be added to the engine primitives later without changing the import interface.

2. **Vector collection auto-create:** If the collection doesn't exist, create it with dimension inferred from the first embedding, cosine metric, f32 storage. This is the lowest-friction path for "I have a Parquet file of embeddings and want to search them."

3. **Row-level error tolerance:** Skip bad rows, report count. Don't abort the entire import for one malformed row. The user sees `Imported 9,997 rows (3 skipped)` and can investigate.

4. **Command variant not feature-gated:** The `ArrowImport` variant exists in the Command enum regardless of feature flag. The handler returns a clear error when the feature is disabled. This avoids conditional compilation in the serialization layer.

5. **No `--batch-size` flag for v1b:** RecordBatch sizes are determined by the file reader (Parquet: one batch per row group, CSV/JSONL: configurable in the reader builder, default ~1024 rows). This is sufficient. A user-facing batch size control can be added later if needed.
