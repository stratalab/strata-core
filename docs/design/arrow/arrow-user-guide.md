# Arrow Interoperability: User Guide

**Status:** Shipped (v1a export + v1b import)
**Feature gate:** `--features arrow`

---

## CLI Commands

### Export: `strata export`

Export data from any Strata primitive to Parquet, CSV, or JSONL files.

```bash
# Key-value
strata export kv --format parquet --output data.parquet
strata export kv --format csv --output data.csv --prefix "user:"

# JSON documents
strata export json --format csv --output docs.csv
strata export json --format jsonl --output docs.jsonl

# Events
strata export events --format jsonl --output events.jsonl

# Vectors (requires --collection)
strata export vector --collection my_col --format parquet --output embeddings.parquet

# Graph (requires --graph, produces two files: *_nodes.ext and *_edges.ext)
strata export graph --graph my_graph --format parquet --output graph.parquet
```

**Options:**

| Flag | Description |
|------|-------------|
| `--format <FMT>` | Output format: `parquet`, `csv`, `jsonl`, `json` (default: `csv`) |
| `--output <FILE>` | Output file path (required for Parquet) |
| `--prefix <PREFIX>` | Key prefix filter (KV, JSON) |
| `--limit <N>` | Maximum rows to export |
| `--collection <NAME>` | Vector collection name (required for vector) |
| `--graph <NAME>` | Graph name (required for graph) |
| `-b, --branch <BRANCH>` | Target branch (default: `default`) |
| `-s, --space <SPACE>` | Target space (default: `default`) |

**Output schemas:**

- **KV:** `key (utf8), value (utf8), version (uint64), timestamp (uint64)`
- **JSON:** `key (utf8), document (utf8)`
- **Event:** `sequence (uint64), event_type (utf8), payload (utf8), timestamp (uint64)`
- **Vector:** `key (utf8), embedding (fixed_size_list<f32>[dim]), metadata (utf8, nullable)`
- **Graph nodes:** `node_id (utf8), object_type (utf8, nullable), properties (utf8, nullable)`
- **Graph edges:** `source (utf8), target (utf8), edge_type (utf8), weight (f64), properties (utf8, nullable)`

---

### Import: `strata import`

Import data from Parquet, CSV, or JSONL files into Strata primitives.

```bash
# Key-value
strata import users.parquet --into kv --key-column id
strata import data.csv --into kv

# JSON documents
strata import orders.csv --into json --key-column order_id
strata import docs.jsonl --into json --key-column key --value-column document

# Vectors (requires --collection)
strata import embeddings.parquet --into vector --collection docs --key-column doc_id
```

**Options:**

| Flag | Description |
|------|-------------|
| `--into <PRIMITIVE>` | Target primitive: `kv`, `json`, `vector` (required) |
| `--key-column <COL>` | Column to use as key (auto-detected if omitted) |
| `--value-column <COL>` | Column to use as value/document/embedding |
| `--collection <NAME>` | Vector collection name (required for vector) |
| `--format <FMT>` | Override format auto-detection: `parquet`, `csv`, `jsonl` |
| `-b, --branch <BRANCH>` | Target branch (default: `default`) |
| `-s, --space <SPACE>` | Target space (default: `default`) |

**Output:**

```
Imported 10,000 row(s) into kv from users.parquet (0 skipped)
```

---

## Smart Defaults

### Key column auto-detection

If `--key-column` is not specified, the importer tries these column names in order:

1. `key`
2. `_id`
3. `id`

If none are found, an error lists all available columns.

### Value column auto-detection

**KV:** tries `value`, then 2-column shortcut (non-key column), then remaining columns as JSON object.

**JSON:** tries `document` → `value` → `doc` → `body`, then remaining columns as JSON object.

**Vector:** tries `embedding` → `vector` → `embeddings` → `emb`. Must be a `FixedSizeList<f32>` or `List<f32>` type.

### Extra columns as JSON

When no explicit value/document column is found, all non-key columns are serialized as a JSON object per row. This is the natural mapping for importing a CSV into JSON documents:

```bash
# CSV: id,name,email,age
# Imported as: key="u1", document={"name":"Alice","email":"alice@example.com","age":30}
strata import users.csv --into json --key-column id
```

### Vector collection auto-creation

If the target collection does not exist, it is created automatically with:
- Dimension inferred from the first embedding
- Cosine distance metric
- F32 storage dtype

### Format auto-detection

File extension determines the format: `.parquet` → Parquet, `.csv` → CSV, `.jsonl`/`.json` → JSONL. Override with `--format`.

---

## Programmatic API

Both commands are available via the `Command` enum for use from any SDK:

```rust
// Export
Command::DbExport {
    branch: None,       // defaults to "default"
    space: None,        // defaults to "default"
    primitive: ExportPrimitive::Kv,
    format: ExportFormat::Parquet,
    prefix: None,
    limit: None,
    path: Some("data.parquet".into()),
    collection: None,
    graph: None,
}
// Returns: Output::Exported(ExportResult { row_count, format, path, size_bytes, ... })

// Import
Command::ArrowImport {
    branch: None,
    space: None,
    file_path: "users.parquet".into(),
    target: "kv".into(),
    key_column: Some("id".into()),
    value_column: None,
    collection: None,
    format: None,
}
// Returns: Output::ArrowImported { rows_imported, rows_skipped, target, file_path }
```

---

## Feature Gate

All Arrow functionality requires the `arrow` feature:

```bash
cargo build --features arrow
```

Without it, export/import commands return a clear error:

```
Error: Import requires the 'arrow' feature
Hint: Rebuild with: cargo build --features arrow
```

---

## Supported Formats

| Format | Extension | Export | Import | Notes |
|--------|-----------|-------|--------|-------|
| Parquet | `.parquet` | Yes | Yes | Snappy compression, schema-preserving |
| CSV | `.csv` | Yes | Yes | Header row, schema inferred from first 100 rows |
| JSONL | `.jsonl` | Yes | Yes | One JSON object per line, schema inferred from first 100 lines |
| JSON | `.json` | Yes (inline only) | Yes (as JSONL) | Export: inline pretty-print; Import: treated as JSONL |

---

## Error Handling

- **Row-level errors** (null key, type mismatch): row is skipped, count reported in output
- **File-level errors** (I/O, invalid format): import aborts with error
- **Duplicate keys on JSON import**: upsert semantics (second row overwrites first)
- **KV import**: upsert semantics via `batch_put`
- **Vector import**: upsert semantics via `insert`
