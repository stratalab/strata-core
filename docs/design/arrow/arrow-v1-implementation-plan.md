# Arrow v1 Implementation Plan

**Status:** Draft
**Scope:** CLI import/export with Parquet, CSV, JSONL via Apache Arrow
**Estimated size:** ~2,000 lines (Rust)

---

## Goal

One-command data import and export for Strata. A developer with a file can load data into Strata or extract it out without writing code.

```bash
strata import users.parquet --into kv --key-column id
strata export kv --format parquet --output users.parquet
```

## Scope

### In scope

| | Import | Export |
|---|--------|--------|
| **KV** | Yes | Yes |
| **JSON** | Yes | Yes |
| **Vector** | Yes | Yes |
| **Event** | — | Yes |
| **Graph** | — | Yes (nodes + edges) |

**Formats:** Parquet, CSV, JSONL

### Out of scope

- Graph and Event import (complex, lower demand)
- NumPy, Arrow IPC formats
- JSON document flattening on export (`--flatten`)
- Python SDK Arrow API (Phase 2)
- ADBC driver, DataFusion SQL (Phases 3-4)
- Progress bars / speed reporting

---

## Architecture

```
crates/executor/src/arrow/
├── mod.rs              — Feature gate, public re-exports
├── schema.rs           — Column mapping: well-known names + overrides
├── ingest.rs           — RecordBatch → KV/JSON/Vector batch writes
├── export.rs           — Primitive scan → RecordBatch builders
├── reader.rs           — File → RecordBatch (Parquet, CSV, JSONL)
└── writer.rs           — RecordBatch → File (Parquet, CSV, JSONL)

crates/cli/src/
├── commands.rs         — Add `import` and `export_arrow` subcommands
└── dispatch.rs         — Route new subcommands (or inline in main.rs)
```

Arrow code lives entirely in the executor crate behind a feature gate. The CLI crate forwards the feature.

---

## Step 1: Cargo dependencies and feature gate

### `Cargo.toml` (workspace root)

Add to `[workspace.dependencies]`:

```toml
arrow = { version = "54", default-features = false, features = ["prettyprint"] }
parquet = { version = "54", default-features = false, features = ["arrow", "snap", "zstd", "lz4"] }
```

Pin `arrow` and `parquet` to the same version (they share the `arrow-*` subcrates). Use `default-features = false` to avoid pulling in unnecessary backends.

### `crates/executor/Cargo.toml`

```toml
[features]
arrow = ["dep:arrow", "dep:parquet"]

[dependencies]
arrow = { workspace = true, optional = true }
parquet = { workspace = true, optional = true }
```

### `crates/cli/Cargo.toml`

```toml
[features]
arrow = ["strata-executor/arrow"]
```

All Arrow code is gated with `#[cfg(feature = "arrow")]`. When disabled, the binary size is unaffected.

---

## Step 2: Column mapping conventions — `schema.rs`

This module defines the well-known column names for each primitive and the logic for resolving user overrides.

### Column conventions

**KV:**
- Required: `key` (utf8), `value` (utf8/binary/any)
- No aliases — simple enough.

**JSON:**
- Required: `key` or `_id` (utf8)
- Optional: `value` or `document` (utf8 JSON string or struct)
- If no value/document column: remaining columns are serialized as a JSON object keyed by column name. This is the natural CSV→JSON mapping: `id, name, email` becomes `{name, email}` keyed by `id`.

**Vector:**
- Required: `key` (utf8), `embedding` or `vector` (list\<f32\> or fixed_size_list\<f32\>)
- Optional: `metadata` (utf8 JSON) — if absent, remaining columns become metadata JSON.
- Collection name comes from CLI flag (`--collection`), not from the file.

**Event (export only):**
- Columns: `sequence` (u64), `event_type` (utf8), `payload` (utf8 JSON), `timestamp` (u64)

**Graph (export only):**
- Nodes: `node_id` (utf8), `object_type` (utf8 nullable), `properties` (utf8 JSON)
- Edges: `source` (utf8), `target` (utf8), `edge_type` (utf8), `weight` (f64), `properties` (utf8 JSON)

### Key types

```rust
/// Resolved column mapping for a primitive import.
pub struct ImportMapping {
    pub key_column: String,           // Which column is the key
    pub value_column: Option<String>, // Explicit value/document/embedding column
    pub extra_columns: Vec<String>,   // Remaining columns → JSON metadata
}

/// Resolve column mapping from CLI flags + Arrow schema.
pub fn resolve_mapping(
    schema: &arrow::datatypes::Schema,
    primitive: ImportPrimitive,
    key_column: Option<&str>,
    value_column: Option<&str>,
) -> Result<ImportMapping>;
```

The resolver:
1. If `--key-column` provided, use it; else try `key`, then `_id`, then `id`
2. If `--value-column` provided, use it; else try convention names for the primitive
3. Remaining columns become `extra_columns` (serialized as JSON for metadata)
4. Error if required columns are missing

---

## Step 3: File readers — `reader.rs`

Thin wrappers around Arrow's readers that produce `RecordBatch` iterators.

```rust
pub enum FileFormat {
    Parquet,
    Csv,
    Jsonl,
}

/// Detect format from file extension.
pub fn detect_format(path: &Path) -> Result<FileFormat>;

/// Open a file and return a RecordBatch iterator + schema.
pub fn open_file(path: &Path, format: FileFormat) -> Result<(Schema, Box<dyn RecordBatchReader>)>;
```

**Parquet:** `parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder` — reads row groups as RecordBatches. Schema comes from the Parquet file metadata.

**CSV:** `arrow::csv::ReaderBuilder` — reads with schema inference from header + first N rows. We'll use `infer_schema_from_files` with the full file for accuracy.

**JSONL:** `arrow::json::ReaderBuilder` — reads JSON objects per line. Schema inferred from a sample of lines.

All three produce `impl RecordBatchReader`, so the rest of the pipeline is format-agnostic.

---

## Step 4: Ingest pipeline — `ingest.rs`

Converts RecordBatches into primitive write operations. Processes in configurable batches (default: 10,000 rows).

```rust
/// Import a RecordBatch stream into a primitive.
pub fn ingest(
    primitives: &Arc<Primitives>,
    branch_id: BranchId,
    space: &str,
    target: ImportTarget,
    reader: Box<dyn RecordBatchReader>,
    mapping: &ImportMapping,
    batch_size: usize,
) -> Result<ImportResult>;

pub enum ImportTarget {
    Kv,
    Json,
    Vector { collection: String },
}

pub struct ImportResult {
    pub rows_imported: u64,
    pub batches: u64,
    pub errors: u64,
}
```

### Per-primitive ingest logic

**KV ingest:**
For each row: extract `key` as String, extract `value` as `Value`. Use `p.kv.batch_put()` for the batch.

Value coercion: utf8 → `Value::String`, int → `Value::Int`, float → `Value::Float`, bool → `Value::Bool`, binary → `Value::Bytes`. If no explicit value column and multiple extra columns exist, serialize the row (minus key) as a JSON object → `Value::String`.

**JSON ingest:**
For each row: extract `key` as String. If explicit `document` column exists (utf8), use it directly. Otherwise, serialize all non-key columns as a JSON object. Use `p.json.set()` per document (no batch API exists today — if this is a bottleneck, we can add one later).

**Vector ingest:**
For each row: extract `key`, extract `embedding` as `Vec<f32>` (from FixedSizeList or List column), extract optional `metadata` as JSON string. Use `p.vector.insert()` per vector (requires collection to exist — create it from the first batch's embedding dimension if it doesn't exist).

### Transaction batching

Each batch of N rows is processed as a unit. If a row fails (bad key, type coercion error), skip it and increment `errors`. Do not abort the batch — partial progress is better than all-or-nothing for large files.

This differs from the RFC's "each batch is one transaction" proposal because Strata's primitives don't have an explicit multi-row transaction API for all types. We use `batch_put` for KV (which is transactional) and individual puts for JSON/Vector. If transactional batch APIs are added later, we upgrade seamlessly.

---

## Step 5: Export pipeline — `export.rs`

Converts primitive data into RecordBatches. Replaces the existing `handlers/export.rs` string-based rendering for Arrow-format exports, while keeping the old export path for CSV/JSON/JSONL inline output.

```rust
/// Export primitive data as a stream of RecordBatches.
pub fn export_to_batches(
    primitives: &Arc<Primitives>,
    branch_id: BranchId,
    space: &str,
    source: ExportSource,
    limit: Option<usize>,
) -> Result<(Schema, Vec<RecordBatch>)>;

pub enum ExportSource {
    Kv { prefix: Option<String> },
    Json { prefix: Option<String> },
    Event { event_type: Option<String> },
    Vector { collection: String },
    GraphNodes { graph: String },
    GraphEdges { graph: String },
}
```

### Per-primitive export logic

**KV export:**
Use `p.kv.list()` to get keys, then `p.kv.get_versioned()` for each key. Build Arrow arrays: `key` (StringArray), `value` (StringArray or BinaryArray depending on type), `version` (UInt64Array), `timestamp` (UInt64Array).

Note: this is N+1 queries (list + N gets). The existing `export.rs` does exactly the same thing. For v1 this is fine. A future `scan_with_values()` method would eliminate the N gets.

**JSON export:**
Same pattern as KV. Use `p.json.list()` with cursor pagination, then `p.json.get()` per doc. Columns: `key`, `document` (as JSON string), `version`, `created_at`, `updated_at`.

**Event export:**
Use `p.event.range()` with sequence range 0..MAX. Columns: `sequence`, `event_type`, `payload` (JSON string), `timestamp`.

**Vector export:**
Use `p.vector.list_keys()` then `p.vector.get()` per key. Columns: `key`, `embedding` (FixedSizeList\<f32\>), `metadata` (JSON string), `version`.

**Graph nodes export:**
Use `p.graph.all_nodes()`. Columns: `node_id`, `object_type`, `properties` (JSON string).

**Graph edges export:**
Use `p.graph.all_edges()`. Columns: `source`, `target`, `edge_type`, `weight`, `properties` (JSON string).

---

## Step 6: File writers — `writer.rs`

```rust
pub fn write_file(
    path: &Path,
    format: FileFormat,
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<u64>; // returns bytes written
```

**Parquet:** `parquet::arrow::ArrowWriter` with Snappy compression (good default: fast, reasonable ratio).

**CSV:** `arrow::csv::Writer` — straightforward.

**JSONL:** `arrow::json::LineDelimitedWriter` — one JSON object per line.

---

## Step 7: CLI commands

### `strata import`

```
strata import <FILE> --into <PRIMITIVE> [OPTIONS]

Arguments:
  <FILE>                     Input file (Parquet, CSV, or JSONL)

Required:
  --into <PRIMITIVE>         Target primitive: kv, json, vector

Options:
  --key-column <COL>         Column to use as key (default: auto-detect)
  --value-column <COL>       Column to use as value/document/embedding
  --collection <NAME>        Vector collection name (required for --into vector)
  --format <FMT>             File format override (auto-detected from extension)
  --batch-size <N>           Rows per batch (default: 10000)
  --branch <BRANCH>          Target branch (default: default)
  --space <SPACE>            Target space (default: default)
```

Format auto-detection: `.parquet` → Parquet, `.csv` → CSV, `.jsonl`/`.json` → JSONL.

### `strata export` (enhanced)

The existing `strata export` command supports CSV/JSON/JSONL as inline string output. We extend it with Parquet support and file output:

```
strata export <PRIMITIVE> [OPTIONS]

Arguments:
  <PRIMITIVE>                Source: kv, json, events, vector, graph

Options:
  --format <FMT>             Output format: parquet, csv, jsonl (default: csv)
  --output <FILE>            Output file path (if omitted, prints to stdout for csv/jsonl)
  --prefix <PREFIX>          Key/name prefix filter
  --collection <NAME>        Vector collection (required for vector export)
  --graph <NAME>             Graph name (required for graph export)
  --event-type <TYPE>        Filter events by type
  --limit <N>                Maximum rows
  --branch <BRANCH>          Source branch
  --space <SPACE>            Source space
```

When `--format parquet` is used, `--output` is required (can't print binary to stdout). For CSV/JSONL, if `--output` is omitted, the existing inline string rendering is used (backward compatible).

### Implementation approach for CLI

The existing `export` CLI subcommand is under `branch export` (branch bundle export). The data export is handled via the `DbExport` command variant with `--format csv|json|jsonl`.

For `import`: add a new top-level `import` subcommand in `commands.rs`. This is feature-gated behind `#[cfg(feature = "arrow")]`.

For `export` with Parquet: extend the existing `DbExport` command to accept a `parquet` format variant. When the `arrow` feature is enabled, Parquet export goes through the Arrow pipeline. When disabled, attempting `--format parquet` returns an error with a hint to enable the `arrow` feature.

### New Command variants

```rust
// command.rs
#[cfg(feature = "arrow")]
ArrowImport {
    branch: Option<BranchId>,
    space: Option<String>,
    file_path: String,
    target: ImportTarget,
    key_column: Option<String>,
    value_column: Option<String>,
    collection: Option<String>,
    format: Option<FileFormat>,
    batch_size: Option<usize>,
},
```

Export doesn't need a new Command variant — we extend `ExportFormat` with `Parquet` and route to the Arrow export pipeline when that format is selected.

---

## Step 8: Testing strategy

### Unit tests (~15 tests in `arrow/` modules)

- **schema.rs:** Column resolution for each primitive (auto-detect key column, missing required column errors, override flags)
- **ingest.rs:** RecordBatch → Value coercion for each type (utf8, int, float, bool, binary, list→embedding)
- **export.rs:** Primitive data → RecordBatch schema correctness for each primitive

### Integration tests (~10 tests in `executor/tests/`)

- Round-trip: import a Parquet file → export it back → compare schemas and row counts
- Round-trip: import CSV → export CSV → diff
- Round-trip: import JSONL → export JSONL → diff
- KV import: verify keys and values are accessible via `kv get`
- JSON import: verify documents are accessible via `json get`
- Vector import: verify embeddings are searchable via `vector search`
- Export all 5 primitives to Parquet: verify file is valid Parquet
- Error cases: missing key column, wrong format, empty file, bad types

### CLI tests (~5 tests)

- `strata import file.parquet --into kv --key-column id` end-to-end
- `strata export kv --format parquet --output out.parquet` end-to-end
- Format auto-detection from extension
- Error on `--format parquet` without `--output`

---

## Step 9: Implementation order

| Order | Module | Description | Depends on |
|-------|--------|-------------|------------|
| 1 | Cargo.toml changes | Add arrow/parquet deps, feature gates | — |
| 2 | `arrow/mod.rs` | Module skeleton + feature gate | 1 |
| 3 | `arrow/schema.rs` | Column mapping + tests | 2 |
| 4 | `arrow/reader.rs` | File → RecordBatch (Parquet, CSV, JSONL) | 2 |
| 5 | `arrow/writer.rs` | RecordBatch → File (Parquet, CSV, JSONL) | 2 |
| 6 | `arrow/ingest.rs` | RecordBatch → KV/JSON/Vector writes | 3 |
| 7 | `arrow/export.rs` | All 5 primitives → RecordBatch | 3 |
| 8 | Command/Output types | `ArrowImport` command, `Parquet` format | 6, 7 |
| 9 | CLI subcommands | `import` + enhanced `export` | 8 |
| 10 | Integration tests | Round-trip tests | 6, 7, 9 |

Steps 3-5 can be developed in parallel. Steps 6-7 can be developed in parallel.

---

## Dependency impact

The `arrow` crate (v54) and `parquet` crate (v54) are the only new external dependencies. Both are Apache-licensed, battle-tested, and widely used in the Rust ecosystem.

With `default-features = false`:
- `arrow`: ~15 subcrates (arrow-array, arrow-buffer, arrow-schema, etc.)
- `parquet`: adds thrift, snap, zstd, lz4 compression codecs

These are behind a feature gate, so the default binary is unaffected. When enabled, expect ~2-4MB binary size increase (compressed).

---

## Open decisions (minor, resolvable during implementation)

1. **JSON import: set vs batch_set** — There is no `json.batch_set()` today. For v1, use individual `json.set()` calls. If profiling shows this is a bottleneck on large imports, add a batch API to the JSON primitive.

2. **Vector collection auto-create** — If `--collection docs` doesn't exist, should import create it automatically (inferring dimension from the first embedding)? Recommendation: yes, auto-create with default metric (cosine).

3. **ExportFormat::Parquet without arrow feature** — Return a clear error: `"Parquet export requires the 'arrow' feature. Rebuild with: cargo build --features arrow"`.

4. **Graph export file layout** — Export graph as two RecordBatches (nodes + edges) written to separate files (`graph_nodes.parquet`, `graph_edges.parquet`) or a single file with a discriminator column? Recommendation: separate files, matching the RFC.
