# v1b Epics: Arrow Import

Four epics. Requires v1a to be complete (deps, feature gate, `arrow/` module, `FileFormat`, `writer.rs`).

---

## Epic 5: File readers

**Goal:** Can open Parquet, CSV, and JSONL files and iterate over their contents as Arrow RecordBatches.

**Files:**

| File | Change |
|------|--------|
| `crates/executor/src/arrow/mod.rs` | Add `pub mod reader;` |
| `crates/executor/src/arrow/reader.rs` | `open_file()` for Parquet, CSV, JSONL |

**Details:**

`reader.rs` implements three readers, all returning `(Schema, Box<dyn RecordBatchReader>)`:

- **Parquet:** `ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?` — schema from Parquet metadata, row groups as batches.
- **CSV:** Infer schema from header + first 100 rows via `arrow::csv::infer_schema_from_files`. Re-open file, build reader with inferred schema.
- **JSONL:** Infer schema from first 100 lines via `arrow::json::infer_json_schema_from_seekable`. Build `ReaderBuilder` with inferred schema.

Reuses `FileFormat` and `detect_format()` from `writer.rs` (v1a Epic 1).

**Tests (3):**
- `test_read_parquet` — write a Parquet file with known data (using writer from v1a), read back, verify schema + row count + values
- `test_read_csv` — write CSV string to temp file, read, verify schema inference and data
- `test_read_jsonl` — write JSONL string to temp file, read, verify schema inference and data

**Estimated size:** ~100 lines
**Acceptance:** All three formats read successfully, schema inferred correctly, tests green.

---

## Epic 6: Column mapping + type coercion

**Goal:** Given a file's Arrow schema and CLI flags, resolve which columns map to key/value/embedding/metadata. Convert Arrow array values to Strata `Value` types.

**Files:**

| File | Change |
|------|--------|
| `crates/executor/src/arrow/mod.rs` | Add `pub mod schema;` |
| `crates/executor/src/arrow/schema.rs` | `ImportPrimitive`, `ImportMapping`, `resolve_mapping()`, `arrow_to_value()`, `row_to_json()` |

**Details:**

Column resolution (`resolve_mapping`):
- Key column: `--key-column` override, else try `key` → `_id` → `id`
- KV value: `--value-column` override, else try `value`, else 2-column shortcut, else remaining columns → JSON
- JSON document: `--value-column` override, else try `document` → `value` → `doc` → `body`, else remaining → JSON
- Vector embedding: `--value-column` override, else try `embedding` → `vector` → `embeddings` → `emb`, must be list<f32> type

Type coercion (`arrow_to_value`):
- Utf8/LargeUtf8 → `Value::String`
- Int8..Int64 → `Value::Int`
- UInt8..UInt64 → `Value::Int` (cast to i64)
- Float32/Float64 → `Value::Float`
- Boolean → `Value::Bool`
- Binary/LargeBinary → `Value::Bytes`
- Null → `Value::Null`
- List/Struct → `Value::String` (JSON-serialized)

Row-to-JSON helper (`row_to_json`):
- Serializes specified columns of a row as `{"col_name": value, ...}` JSON string
- Used when no explicit value/document column exists

Error messages must be actionable — list available columns on resolution failure.

**Tests (13):**

Column resolution (8):
- `test_resolve_key_column_auto` — finds `key` column
- `test_resolve_key_column_id_fallback` — falls back to `id`
- `test_resolve_key_column_explicit` — `--key-column` override works
- `test_resolve_key_column_missing` — error lists available columns
- `test_resolve_value_column_kv` — auto-detect `value`
- `test_resolve_embedding_column` — auto-detect `embedding`
- `test_resolve_embedding_wrong_type` — error on non-list column
- `test_resolve_extra_columns_as_json` — remaining columns collected correctly

Type coercion (5):
- `test_arrow_to_value_string` — Utf8 → Value::String
- `test_arrow_to_value_int` — Int64 → Value::Int
- `test_arrow_to_value_float` — Float64 → Value::Float
- `test_arrow_to_value_bool` — Boolean → Value::Bool
- `test_arrow_to_value_null` — Null → Value::Null

**Estimated size:** ~250 lines
**Acceptance:** Column resolution works for all three primitives (KV, JSON, Vector) with both auto-detect and explicit overrides. Type coercion covers all Arrow→Value mappings.

---

## Epic 7: Ingest pipeline — KV, JSON, Vector

**Goal:** Can read a file and write its contents into Strata primitives. Core import logic.

**Files:**

| File | Change |
|------|--------|
| `crates/executor/src/arrow/mod.rs` | Add `pub mod ingest;` |
| `crates/executor/src/arrow/ingest.rs` | `ImportResult`, `ingest_kv()`, `ingest_json()`, `ingest_vector()` |

**Details:**

KV ingest:
- Per RecordBatch: extract keys from `key_idx`, extract values via `arrow_to_value()` or serialize extra columns as JSON
- Use `p.kv.batch_put()` per batch for efficiency
- Skip rows with null keys, increment `rows_skipped`

JSON ingest:
- Per RecordBatch: extract keys, extract document string from `value_idx` or serialize extra columns as JSON object
- Use `p.json.set()` per document (no batch API exists)
- Skip rows with null keys

Vector ingest:
- Per RecordBatch: extract keys, extract embeddings as `Vec<f32>` from FixedSizeList/List column, extract metadata from extra columns
- On first batch: auto-create collection if it doesn't exist (dimension from first embedding, cosine metric, f32 storage)
- Use `p.vector.insert()` per vector (no batch API)
- Validate embedding dimension against collection config

Error handling:
- Row-level errors (null key, type mismatch, dimension mismatch) → skip row, increment `rows_skipped`
- Batch-level errors (I/O, schema mismatch) → abort, return error
- Already-committed rows from prior batches are not rolled back

**Tests (6):**
- `test_ingest_kv` — create in-memory DB, build RecordBatch with key/value columns, ingest, verify via `kv get`
- `test_ingest_kv_extra_columns_as_json` — RecordBatch with 3 columns (id, name, email), no `value` column → KV values are JSON objects
- `test_ingest_json` — build RecordBatch with key/document columns, ingest, verify via `json get`
- `test_ingest_json_from_columns` — RecordBatch with no `document` column → extra columns become JSON document
- `test_ingest_vector` — build RecordBatch with key + FixedSizeList<f32> embedding, ingest, verify via `vector get`
- `test_ingest_vector_auto_creates_collection` — collection doesn't exist → created with correct dimension

**Estimated size:** ~300 lines
**Acceptance:** All three primitives ingest from RecordBatches correctly. Auto-create vector collection works. Row-level errors are skipped gracefully.

---

## Epic 8: CLI `strata import` command + integration tests

**Goal:** `strata import users.parquet --into kv --key-column id` works end-to-end.

**Files:**

| File | Change |
|------|--------|
| `crates/executor/src/command.rs` | Add `ArrowImport` variant |
| `crates/executor/src/output.rs` | Add `ArrowImported` variant |
| `crates/executor/src/executor.rs` | Dispatch `ArrowImport` → handler |
| `crates/executor/src/handlers/mod.rs` | Register `arrow_import` module |
| `crates/executor/src/handlers/arrow_import.rs` | Handler: parse args → open file → resolve mapping → ingest |
| `crates/cli/src/commands.rs` | Add `import` top-level subcommand |
| `crates/cli/src/main.rs` (or dispatch) | Route `import` subcommand → `ArrowImport` command |

**Details:**

Command variant (always present, not feature-gated):
```rust
ArrowImport {
    branch: Option<BranchId>,
    space: Option<String>,
    file_path: String,
    target: String,         // "kv", "json", "vector"
    key_column: Option<String>,
    value_column: Option<String>,
    collection: Option<String>,
    format: Option<String>, // override auto-detect
}
```

Output variant:
```rust
ArrowImported {
    rows_imported: u64,
    rows_skipped: u64,
    target: String,
    file_path: String,
}
```

Handler (`arrow_import.rs`):
- Feature-gated: `#[cfg(feature = "arrow")]` for the real implementation, `#[cfg(not(feature = "arrow"))]` returns error with rebuild hint
- Validates branch exists, resolves branch ID
- Opens file with reader, resolves column mapping, dispatches to ingest function
- Returns `ArrowImported` output

CLI subcommand:
```
strata import <FILE> --into <PRIMITIVE> [OPTIONS]
  --key-column <COL>
  --value-column <COL>
  --collection <NAME>
  --format <FMT>
  -b, --branch
  -s, --space
```

Human-readable output:
```
Imported 10,000 rows into kv from users.parquet (0 skipped)
```

**Tests (8):**

Integration (6):
- `test_import_kv_from_parquet` — write Parquet → import → verify `kv get`
- `test_import_kv_from_csv` — write CSV → import → verify
- `test_import_json_from_jsonl` — write JSONL → import → verify `json get`
- `test_import_json_remaining_columns_as_document` — CSV with columns (id, name, email) → JSON documents
- `test_import_vector_from_parquet` — write Parquet with embeddings → import → verify `vector search`
- `test_import_vector_auto_creates_collection` — collection auto-created

Round-trip (2):
- `test_roundtrip_kv_parquet` — populate KV → export Parquet (v1a) → import to new branch → compare
- `test_roundtrip_json_csv` — populate JSON → export CSV → import to new branch → compare

**Estimated size:** ~350 lines
**Acceptance:** `strata import` works for all three primitives, all three formats. Round-trip tests prove import and export are compatible. `cargo test --features arrow` all green.

---

## Summary

| Epic | Scope | Lines | Cumulative (with v1a) |
|------|-------|-------|-----------------------|
| 5 | File readers | ~100 | ~1,070 |
| 6 | Column mapping + type coercion | ~250 | ~1,320 |
| 7 | KV, JSON, Vector ingest | ~300 | ~1,620 |
| 8 | CLI `import` + integration tests | ~350 | ~1,970 |
| **v1b Total** | | **~1,000** | |
