# Arrow v1a: Export

**Status:** Draft
**Scope:** Export all 5 primitives to Parquet, CSV, JSONL files via Apache Arrow
**Estimated size:** ~1,000 lines
**Prerequisite:** None
**Unlocks:** v1b (import), Python SDK Arrow API (Phase 2)

---

## Goal

A developer can extract data from Strata in one command, in a format any tool can read. No code, no SDK, no lock-in.

```bash
strata export kv --format parquet --output users.parquet
strata export json --format csv --output orders.csv
strata export events --format jsonl --output logs.jsonl
strata export vector --collection docs --format parquet --output embeddings.parquet
strata export graph --graph social --format csv --output-dir ./social/
```

## Why export first

1. Export is read-only — no risk of data corruption, simpler error handling
2. Answers the adoption-critical question: "Can I get my data out?"
3. Builds the Arrow adapter layer (deps, feature gate, RecordBatch builders, file writers) that import reuses
4. The existing `handlers/export.rs` already does the hard part (primitive scanning) — we're adding Arrow-native output on top

---

## Scope

**Primitives:** All 5 (KV, JSON, Event, Vector, Graph)
**Formats:** Parquet, CSV, JSONL
**Output:** File only (Parquet requires file; CSV/JSONL also write to file for consistency with the new `--output` flag — existing inline CSV/JSON/JSONL export remains for backward compat)

**Out of scope:** Import, JSON flattening, progress bars, Python SDK, ADBC

---

## Implementation

### Step 1: Cargo dependencies and feature gate

**Workspace root `Cargo.toml`** — add to `[workspace.dependencies]`:

```toml
arrow = { version = "54", default-features = false, features = ["prettyprint"] }
parquet = { version = "54", default-features = false, features = ["arrow", "snap", "zstd", "lz4"] }
```

Pin both to the same version (shared `arrow-*` subcrates). `default-features = false` keeps the dependency footprint minimal.

**`crates/executor/Cargo.toml`:**

```toml
[features]
arrow = ["dep:arrow", "dep:parquet"]

[dependencies]
arrow = { workspace = true, optional = true }
parquet = { workspace = true, optional = true }
```

**`crates/cli/Cargo.toml`:**

```toml
[features]
arrow = ["strata-executor/arrow"]
```

All Arrow code gated with `#[cfg(feature = "arrow")]`. Default binary unaffected.

---

### Step 2: Module skeleton — `arrow/mod.rs`

```
crates/executor/src/arrow/
├── mod.rs       — Feature gate, re-exports
├── export.rs    — Primitive → RecordBatch builders
└── writer.rs    — RecordBatch → File (Parquet, CSV, JSONL)
```

```rust
// src/arrow/mod.rs
#![cfg(feature = "arrow")]

mod export;
mod writer;

pub use export::{export_to_batches, ExportSource};
pub use writer::{write_file, FileFormat};
```

Registered in `src/lib.rs`:

```rust
#[cfg(feature = "arrow")]
pub mod arrow;
```

---

### Step 3: Export pipeline — `arrow/export.rs`

Converts primitive data into Arrow RecordBatches. One function per primitive, unified under `export_to_batches`.

```rust
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

pub enum ExportSource {
    Kv { prefix: Option<String> },
    Json { prefix: Option<String> },
    Event { event_type: Option<String> },
    Vector { collection: String },
    GraphNodes { graph: String },
    GraphEdges { graph: String },
}

pub fn export_to_batches(
    primitives: &Arc<Primitives>,
    branch_id: CoreBranchId,
    space: &str,
    source: ExportSource,
    limit: Option<usize>,
) -> Result<(Schema, Vec<RecordBatch>)>;
```

#### KV export

Schema: `key: utf8, value: utf8, version: uint64, timestamp: uint64`

Data access: `p.kv.list()` → keys, then `p.kv.get_versioned()` per key. This is the same N+1 pattern as the existing `handlers/export.rs`. Build `StringBuilder` for key/value, `UInt64Builder` for version/timestamp.

Value rendering: all Value types are serialized to string (matching existing export behavior). Bytes → base64, Array/Object → JSON string, scalars → to_string().

#### JSON export

Schema: `key: utf8, document: utf8`

Data access: `p.json.list()` with cursor pagination → doc IDs, then `p.json.get()` per doc. Document stored as JSON string column.

No version/timestamp columns for now — the JSON primitive's `get()` returns the document value, not version metadata. Can be added later if there's demand.

#### Event export

Schema: `sequence: uint64, event_type: utf8, payload: utf8, timestamp: uint64`

Data access: `p.event.range()` with `start_seq=0`, `end_seq=None`, optional type filter. Events come back as `Vec<Versioned<Event>>` — map directly to Arrow arrays.

#### Vector export

Schema: `key: utf8, embedding: fixed_size_list<f32>[dim], metadata: utf8`

Data access: `p.vector.list_keys()` → keys, then `p.vector.get()` per key. The get returns `Versioned<VectorEntry>` with embedding Vec<f32> and metadata.

The embedding dimension is known per collection (from `collection_info`). Use `FixedSizeListBuilder<Float32Builder>` for the embedding column.

#### Graph nodes export

Schema: `node_id: utf8, object_type: utf8, properties: utf8`

Data access: `p.graph.all_nodes()` → `HashMap<String, NodeData>`. NodeData contains object_type and properties. Properties serialized as JSON string.

#### Graph edges export

Schema: `source: utf8, target: utf8, edge_type: utf8, weight: f64, properties: utf8`

Data access: `p.graph.all_edges()` → `Vec<Edge>`. Edge contains src, dst, edge_type, and data (which includes weight and properties).

#### Graph: two-batch output

Graph export produces two separate RecordBatches (nodes and edges) with different schemas. The caller handles writing them to separate files.

---

### Step 4: File writers — `arrow/writer.rs`

```rust
pub enum FileFormat {
    Parquet,
    Csv,
    Jsonl,
}

/// Detect format from file extension.
pub fn detect_format(path: &Path) -> Result<FileFormat>;

/// Write RecordBatches to a file. Returns bytes written.
pub fn write_file(
    path: &Path,
    format: FileFormat,
    batches: &[RecordBatch],
) -> Result<u64>;
```

**Parquet:** `parquet::arrow::ArrowWriter` with Snappy compression (fast, reasonable ratio, widely supported). Schema inferred from the first RecordBatch.

**CSV:** `arrow::csv::WriterBuilder` with header row. Straightforward — Arrow handles all type formatting.

**JSONL:** `arrow::json::LineDelimitedWriter`. One JSON object per line, column names as keys.

Format detection from extension: `.parquet` → Parquet, `.csv` → CSV, `.jsonl` → JSONL, `.json` → JSONL (JSONL is the natural per-record format; the existing JSON array export remains via the old path).

---

### Step 5: CLI wiring

#### Extending the existing export command

The existing `DbExport` command supports `ExportFormat::{Csv, Json, Jsonl}`. We add `Parquet` to `ExportFormat`:

```rust
// types.rs
pub enum ExportFormat {
    Csv,
    Json,
    Jsonl,
    Parquet,  // new
}
```

New CLI flags on the existing export subcommand:
- `--output <FILE>` — required for Parquet, optional for CSV/JSONL
- `--collection <NAME>` — required when exporting vectors
- `--graph <NAME>` — required when exporting graph

Add `vector` to `ExportPrimitive`:

```rust
pub enum ExportPrimitive {
    Kv,
    Json,
    Events,
    Graph,
    Vector,  // new
}
```

#### Dispatch routing

In `handlers/export.rs` (or a new `handlers/arrow_export.rs`):

```rust
pub fn db_export(...) -> Result<Output> {
    match format {
        ExportFormat::Parquet => {
            #[cfg(feature = "arrow")]
            {
                // Arrow export path
                let source = to_export_source(primitive, prefix, collection, graph)?;
                let (schema, batches) = arrow::export_to_batches(p, branch_id, &space, source, limit)?;
                let path = path.ok_or_else(|| Error::InvalidArgument {
                    reason: "Parquet export requires --output <FILE>".into(),
                })?;
                let bytes = arrow::write_file(Path::new(&path), FileFormat::Parquet, &batches)?;
                Ok(Output::Exported(ExportResult { ... }))
            }
            #[cfg(not(feature = "arrow"))]
            {
                Err(Error::Internal {
                    reason: "Parquet export requires the 'arrow' feature".into(),
                    hint: Some("Rebuild with: cargo build --features arrow".into()),
                })
            }
        }
        // Existing CSV/JSON/JSONL path: file output through Arrow when feature enabled,
        // inline string output through existing renderer when no --output flag
        ExportFormat::Csv | ExportFormat::Jsonl if path.is_some() => {
            #[cfg(feature = "arrow")]
            {
                // Arrow file export path (better: uses Arrow's CSV/JSONL writers)
                let source = to_export_source(primitive, prefix, collection, graph)?;
                let (_, batches) = arrow::export_to_batches(p, branch_id, &space, source, limit)?;
                let fmt = match format {
                    ExportFormat::Csv => FileFormat::Csv,
                    ExportFormat::Jsonl => FileFormat::Jsonl,
                    _ => unreachable!(),
                };
                let bytes = arrow::write_file(Path::new(path.as_ref().unwrap()), fmt, &batches)?;
                Ok(Output::Exported(ExportResult { ... }))
            }
            #[cfg(not(feature = "arrow"))]
            {
                // Fall through to existing string-based export + write to file
                // (existing behavior)
            }
        }
        // Existing inline rendering path (no --output flag)
        _ => {
            // existing collect_* + render_* code path, unchanged
        }
    }
}
```

This keeps full backward compatibility: the existing inline CSV/JSON/JSONL export works exactly as before. The Arrow path only activates for Parquet format, or when `--output` is provided with the arrow feature enabled.

#### Graph export: two files

When exporting graph with `--output`, we derive two file paths:
- `--output social.parquet` → writes `social_nodes.parquet` and `social_edges.parquet`
- `--output ./social/` (directory) → writes `social/nodes.parquet` and `social/edges.parquet`

For CSV/JSONL graph export with `--output`, same pattern with the appropriate extension.

---

### Step 6: Testing

#### Unit tests in `arrow/export.rs` (~8 tests)

Test RecordBatch construction for each primitive. Create an in-memory database, populate with known data, export to RecordBatch, assert schema and values.

1. `test_kv_export_schema` — verify columns and types
2. `test_kv_export_values` — verify data round-trip
3. `test_json_export` — verify documents as JSON strings
4. `test_event_export` — verify sequence/type/payload/timestamp
5. `test_vector_export` — verify embedding dimension and float values
6. `test_graph_nodes_export` — verify node_id/type/properties
7. `test_graph_edges_export` — verify source/target/edge_type/weight
8. `test_export_with_limit` — verify row limiting

#### Unit tests in `arrow/writer.rs` (~4 tests)

Test file writing with known RecordBatches (no database needed).

1. `test_write_parquet` — write + read back, verify schema and row count
2. `test_write_csv` — write + verify header and row content
3. `test_write_jsonl` — write + verify each line is valid JSON
4. `test_detect_format` — extension → format mapping

#### Integration tests (~3 tests in `executor/tests/arrow_export.rs`)

End-to-end: populate database → export to temp file → read file back → verify.

1. `test_kv_export_parquet_roundtrip` — export to Parquet, read with parquet crate, compare
2. `test_all_primitives_csv` — export each primitive to CSV, verify non-empty valid CSV
3. `test_parquet_without_output_flag_errors` — verify error message

---

## Implementation order

| Order | File | Lines (est.) | Description |
|-------|------|-------------|-------------|
| 1 | Cargo.toml (3 files) | ~10 | Add deps + feature gates |
| 2 | `arrow/mod.rs` | ~10 | Module skeleton |
| 3 | `arrow/writer.rs` | ~100 | RecordBatch → Parquet/CSV/JSONL files |
| 4 | `arrow/export.rs` | ~400 | 5 primitives → RecordBatch |
| 5 | `types.rs` + `command.rs` | ~30 | Add Parquet format, Vector primitive |
| 6 | `handlers/export.rs` | ~60 | Route Parquet to Arrow path |
| 7 | `cli/commands.rs` | ~30 | Add --output, --collection, --graph flags |
| 8 | Tests | ~350 | Unit + integration |
| **Total** | | **~1,000** | |

Steps 3-4 can be developed in parallel. Step 5-7 are sequential (types → handler → CLI).

---

## Decisions

1. **Value rendering in KV export:** Serialize all Value types to utf8 string column (matching existing export behavior). This avoids a union-type column that most tools can't read. Binary values become base64.

2. **JSON export schema:** Single `document` column as JSON string, not flattened. Flattening requires schema inference and produces inconsistent schemas across documents. JSON string is universally readable.

3. **Graph two-file output:** Nodes and edges are separate files with different schemas. This matches how graph data is naturally consumed (load nodes, load edges, build graph).

4. **Backward compatibility:** Existing `strata export kv --format csv` (inline string output) works exactly as before. Arrow path only activates for Parquet format or when `--output` is provided with the feature enabled.

5. **No new Command variant:** We extend the existing `DbExport` command with new format and primitive options. This keeps the command surface clean and avoids duplicating dispatch logic.
