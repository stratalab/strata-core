# v1a Epics: Arrow Export

Four epics, each independently testable. Each builds on the previous.

---

## Epic 1: Arrow scaffold + file writers

**Goal:** `cargo build --features arrow` compiles. Can write a synthetic RecordBatch to Parquet, CSV, and JSONL files.

**Files:**

| File | Change |
|------|--------|
| `Cargo.toml` (workspace root) | Add `arrow` v54 and `parquet` v54 to `[workspace.dependencies]` |
| `crates/executor/Cargo.toml` | Add `arrow` feature gate, optional deps |
| `crates/cli/Cargo.toml` | Add `arrow` feature forwarding to executor |
| `crates/executor/src/lib.rs` | Register `#[cfg(feature = "arrow")] pub mod arrow;` |
| `crates/executor/src/arrow/mod.rs` | Module skeleton, re-exports |
| `crates/executor/src/arrow/writer.rs` | `FileFormat` enum, `detect_format()`, `write_file()` |

**Details:**

`writer.rs` implements three writers:
- **Parquet:** `parquet::arrow::ArrowWriter` with Snappy compression
- **CSV:** `arrow::csv::WriterBuilder` with header
- **JSONL:** `arrow::json::LineDelimitedWriter`

`detect_format()` maps extensions: `.parquet` → Parquet, `.csv` → CSV, `.jsonl`/`.json` → JSONL.

**Tests (4):**
- `test_write_parquet` — write synthetic RecordBatch, read back with Parquet reader, verify schema + row count
- `test_write_csv` — write, read file contents, verify header + data rows
- `test_write_jsonl` — write, verify each line is valid JSON with correct keys
- `test_detect_format` — extension → format mapping, unknown extension → error

**Estimated size:** ~120 lines
**Acceptance:** `cargo test --features arrow` passes, writer tests green.

---

## Epic 2: Export KV, JSON, Event to RecordBatch

**Goal:** Can convert KV, JSON, and Event primitive data into Arrow RecordBatches.

**Files:**

| File | Change |
|------|--------|
| `crates/executor/src/arrow/mod.rs` | Add `pub mod export;` |
| `crates/executor/src/arrow/export.rs` | `ExportSource` enum, `export_to_batches()`, KV/JSON/Event export functions |

**Details:**

`export.rs` scaffolding:
- `ExportSource` enum with variants for all 5 primitives (Vector and Graph variants exist but are unimplemented in this epic)
- `export_to_batches()` dispatch function
- `value_to_string()` helper — serializes any `Value` to a string for Arrow columns (reuses logic from existing `handlers/export.rs`)

KV export:
- Schema: `key: utf8, value: utf8, version: uint64, timestamp: uint64`
- `p.kv.list()` → keys, `p.kv.get_versioned()` per key
- All Value types rendered as utf8 strings

JSON export:
- Schema: `key: utf8, document: utf8`
- `p.json.list()` with cursor pagination, `p.json.get()` per doc
- Document stored as JSON string

Event export:
- Schema: `sequence: uint64, event_type: utf8, payload: utf8, timestamp: uint64`
- `p.event.range(start_seq=0, end_seq=None)` with optional type filter

**Tests (5):**
- `test_kv_export_schema` — in-memory DB, put 3 KV pairs, export, verify Arrow schema
- `test_kv_export_values` — verify key/value/version/timestamp data matches
- `test_json_export` — set 2 JSON docs, export, verify key + document columns
- `test_event_export` — append 3 events, export, verify sequence/type/payload/timestamp
- `test_export_with_limit` — put 10 KV pairs, export with limit=3, verify 3 rows

**Estimated size:** ~300 lines
**Acceptance:** All export tests green. Can programmatically call `export_to_batches(ExportSource::Kv { .. })` and get a valid RecordBatch.

---

## Epic 3: Export Vector + Graph to RecordBatch

**Goal:** All 5 primitives export to RecordBatch. Graph produces two batches (nodes + edges).

**Files:**

| File | Change |
|------|--------|
| `crates/executor/src/arrow/export.rs` | Add Vector, GraphNodes, GraphEdges export functions |

**Details:**

Vector export:
- Schema: `key: utf8, embedding: fixed_size_list<f32>[dim], metadata: utf8`
- `p.vector.list_keys()` → keys, `p.vector.get()` per key
- Dimension from `collection_info()`. Use `FixedSizeListBuilder<Float32Builder>`.
- Metadata as JSON string column (nullable)

Graph nodes export:
- Schema: `node_id: utf8, object_type: utf8, properties: utf8`
- `p.graph.all_nodes()` → `HashMap<String, NodeData>`
- Properties serialized as JSON string

Graph edges export:
- Schema: `source: utf8, target: utf8, edge_type: utf8, weight: f64, properties: utf8`
- `p.graph.all_edges()` → `Vec<Edge>`

Graph two-batch return:
- `export_to_batches` for `GraphNodes` returns nodes only
- `export_to_batches` for `GraphEdges` returns edges only
- The caller (Epic 4) is responsible for invoking both and writing separate files

**Tests (3):**
- `test_vector_export` — create collection, upsert 3 vectors with metadata, export, verify embedding dimension + float values + metadata
- `test_graph_nodes_export` — add nodes with types and properties, export, verify columns
- `test_graph_edges_export` — add edges with weights, export, verify source/target/type/weight

**Estimated size:** ~200 lines
**Acceptance:** All 5 primitives produce valid RecordBatches. `cargo test --features arrow` green.

---

## Epic 4: CLI wiring + integration tests

**Goal:** `strata export kv --format parquet --output users.parquet` works end-to-end. Full backward compatibility with existing export.

**Files:**

| File | Change |
|------|--------|
| `crates/executor/src/types.rs` | Add `ExportFormat::Parquet`, `ExportPrimitive::Vector` |
| `crates/executor/src/command.rs` | Update `DbExport` with `collection` and `graph` fields |
| `crates/executor/src/handlers/export.rs` | Route Parquet format + file output to Arrow path |
| `crates/cli/src/commands.rs` | Add `--output`, `--collection`, `--graph`, `--format parquet` flags |
| `crates/executor/src/arrow/mod.rs` | Re-export anything needed by handlers |

**Details:**

Type changes:
- `ExportFormat`: add `Parquet` variant
- `ExportPrimitive`: add `Vector` variant
- `ExportResult`: no changes needed (already has `path` and `size_bytes`)

Command changes:
- `DbExport`: add optional `collection: Option<String>` and `graph: Option<String>` fields

Handler routing in `db_export()`:
- `ExportFormat::Parquet` → require `--output`, build `ExportSource` from primitive/prefix/collection/graph, call `export_to_batches()` + `write_file()`
- `ExportFormat::Csv | Jsonl` with `--output` and arrow feature → same Arrow path with CSV/JSONL writer
- All other cases → existing string-based rendering (unchanged)
- `ExportFormat::Parquet` without arrow feature → clear error with rebuild hint

Graph export CLI:
- `--output social.parquet` → derive `social_nodes.parquet` and `social_edges.parquet`
- Call `export_to_batches` twice (GraphNodes + GraphEdges), write two files
- Report both files in output

Vector export CLI:
- `--collection` required when primitive is `vector`
- Error if missing: `"--collection is required for vector export"`

CLI flags:
- `--output <FILE>` on the existing export subcommand
- `--collection <NAME>` on the existing export subcommand
- `--graph <NAME>` on the existing export subcommand
- `--format` extended with `parquet` option

**Tests (5):**
- `test_kv_export_parquet_roundtrip` — populate KV → export to temp Parquet → read with parquet crate → compare row count + values
- `test_json_export_csv_file` — populate JSON → export to temp CSV → verify valid CSV with header
- `test_event_export_jsonl_file` — populate events → export to temp JSONL → verify each line valid JSON
- `test_parquet_requires_output_flag` — `--format parquet` without `--output` → error
- `test_vector_requires_collection` — export vector without `--collection` → error

**Estimated size:** ~350 lines
**Acceptance:** Full end-to-end: `strata export <primitive> --format parquet --output file.parquet` for all 5 primitives. Existing `strata export kv --format csv` (inline) unchanged. All tests green.

---

## Summary

| Epic | Scope | Lines | Cumulative |
|------|-------|-------|------------|
| 1 | Scaffold + file writers | ~120 | ~120 |
| 2 | KV, JSON, Event → RecordBatch | ~300 | ~420 |
| 3 | Vector, Graph → RecordBatch | ~200 | ~620 |
| 4 | CLI wiring + integration tests | ~350 | ~970 |
| **Total** | | | **~970** |
