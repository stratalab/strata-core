# RFC: Apache Arrow Interoperability — Universal Data I/O for Strata

**Status:** Proposal
**Priority:** Critical for adoption
**Author:** Ani Joshi
**Related Issues:** #1898 (bulk import/export), #1879 (hybrid search)

---

## Problem

Strata stores data in custom binary formats (MessagePack, JSON strings, custom segments). Today, the only way to move data into or out of Strata is through the Strata SDK, CLI commands (one-at-a-time puts), or the branch bundle archive format (which is Strata-proprietary).

This creates three adoption blockers:

1. **Getting data in is hard.** A developer with a Parquet file of embeddings, a CSV of user records, or a Postgres table of events must write a custom ingestion script using the Strata SDK. There is no `strata import users.parquet` command.

2. **Getting data out is hard.** Exporting Strata data into a format that Pandas, DuckDB, Spark, or any analytics tool can consume requires writing serialization code. There is no `strata export users --format parquet` command.

3. **No interoperability standard.** Strata has no JDBC, ODBC, REST API, or any standard protocol for external tools to connect to. Data is locked inside Strata unless you use Strata's own SDK.

These three problems reduce to one: **Strata is a data island.** Getting data in requires Strata-specific tooling. Getting data out requires Strata-specific tooling. No external tool can connect natively.

For an embedded database targeting AI agents, this is a critical gap. Agents produce data that must flow to analytics pipelines, dashboards, training jobs, and other databases. If that flow requires custom code per destination, adoption stalls.

---

## Solution

Adopt Apache Arrow as Strata's universal data interchange format and implement an ADBC (Arrow Database Connectivity) driver for standardized bidirectional access.

Arrow is not a storage format — Strata keeps its custom LSM storage internally. Arrow is the **boundary format**: the representation used when data crosses the Strata boundary in either direction. Every external interaction (import, export, SDK data transfer, tool integration) speaks Arrow.

### Design Principles

1. **No lock-in.** A developer can import data from standard formats and export it back to standard formats. Strata never holds data hostage.

2. **Zero-copy where possible.** Arrow's columnar memory layout enables zero-copy transfer to Pandas, Polars, DuckDB, and other Arrow-native tools. No serialization/deserialization overhead at the boundary.

3. **Convention over configuration.** Column mapping between Arrow tables and Strata primitives uses well-known column names (`key`, `value`, `embedding`, `node_id`, `event_type`, `payload`). Overrides available but defaults work for 90% of cases.

4. **Primitive-aware.** Strata has 5 primitives with different semantics. The Arrow layer understands all 5 and maps data appropriately — not a generic "dump to table" export.

5. **Incremental adoption.** Each phase is independently useful. Phase 1 (CLI import/export) delivers value without Phase 2 (Arrow API) or Phase 3 (ADBC driver).

---

## Architecture

```
                    ┌─────────────────────────────────────┐
                    │        External Ecosystem            │
                    │                                      │
                    │  Files:     Parquet, CSV, JSONL, NPY │
                    │  Tools:     Pandas, Polars, DuckDB   │
                    │  Databases: Postgres, Spark, BigQuery│
                    │  Languages: Python, R, Go, Java, Rust│
                    └──────────────┬──────────────────────┘
                                   │
                            Arrow RecordBatch
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                     │
    ┌─────────▼────────┐  ┌───────▼────────┐  ┌────────▼───────┐
    │  Phase 1: CLI     │  │ Phase 2: SDK   │  │ Phase 3: ADBC  │
    │  import/export    │  │ .to_arrow()    │  │ Driver         │
    │  (files)          │  │ .from_arrow()  │  │ (universal)    │
    └─────────┬────────┘  └───────┬────────┘  └────────┬───────┘
              │                    │                     │
              └────────────────────┼────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │      Arrow Adapter Layer             │
                    │                                      │
                    │  RecordBatch → Primitive (ingest)    │
                    │  Primitive → RecordBatch (export)    │
                    │  Column mapping + type coercion      │
                    └──────────────┬──────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────┐
                    │        Strata Engine                  │
                    │                                      │
                    │  KV │ JSON │ Event │ Vector │ Graph  │
                    └─────────────────────────────────────┘
```

---

## Phase 1: CLI Import/Export

**Goal:** A developer with a file and a terminal can load data into Strata or extract it out, in one command. No code, no SDK, no Python.

### Import

```bash
# Parquet → KV (each row becomes a key-value pair)
strata import users.parquet --into kv --key-column id

# CSV → JSON documents (each row becomes a JSON document)
strata import orders.csv --into json --key-column order_id

# JSON Lines → Events (each line becomes an event)
strata import logs.jsonl --into event --type-column level

# Parquet → Vectors (requires embedding column)
strata import embeddings.parquet --into vector --collection docs \
    --key-column doc_id --embedding-column embedding

# CSV → Graph nodes
strata import people.csv --into graph.social.nodes \
    --node-id-column person_id --type person

# CSV → Graph edges
strata import relationships.csv --into graph.social.edges \
    --source-column from_id --target-column to_id --edge-type-column rel_type
```

### Export

```bash
# KV → Parquet
strata export kv --format parquet --output users.parquet
strata export kv --prefix "user:" --format parquet --output users.parquet

# JSON documents → Parquet (flattened) or JSON Lines (nested)
strata export json --format parquet --output orders.parquet
strata export json --format jsonl --output orders.jsonl

# Events → CSV
strata export event --format csv --output events.csv
strata export event --type order_created --format csv --output orders.csv

# Vectors → Parquet (embedding as fixed-size list) or NumPy
strata export vector --collection docs --format parquet --output docs.parquet
strata export vector --collection docs --format npy --output embeddings.npy

# Graph → CSV (separate node and edge files)
strata export graph.social --format csv --output-dir ./social/
# Produces: social/nodes.csv, social/edges.csv
```

### Supported File Formats

| Format | Import | Export | Notes |
|--------|--------|--------|-------|
| **Parquet** | Yes | Yes | Primary format. Columnar, compressed, schema-preserving. |
| **CSV** | Yes | Yes | Universal fallback. Header row defines columns. |
| **JSON Lines** | Yes | Yes | One JSON object per line. Natural for events and documents. |
| **NumPy (.npy)** | Yes (vectors only) | Yes (vectors only) | Dense float arrays. ML pipeline standard. |
| **Arrow IPC (.arrow)** | Yes | Yes | Native Arrow format. Zero-overhead for tool chaining. |

All formats are read/written via the `arrow` and `parquet` Rust crates. Strata never implements its own Parquet/CSV parser — it delegates to Arrow's battle-tested implementations.

### Column Mapping Conventions

Each primitive has well-known column names. If the input file uses these names, no `--column` flags are needed.

**KV:**
| Column | Type | Required | Default |
|--------|------|----------|---------|
| `key` | utf8 | Yes | — |
| `value` | utf8 / binary / any | Yes | — |

**JSON:**
| Column | Type | Required | Default |
|--------|------|----------|---------|
| `key` or `_id` | utf8 | Yes | — |
| `value` or `document` | utf8 (JSON) or struct | No | Entire row as JSON object |

If no `value`/`document` column is specified, the entire row (minus the key column) is stored as a JSON object. This is the natural mapping: a CSV with columns `id, name, email, age` becomes JSON documents with `{name, email, age}` keyed by `id`.

**Event:**
| Column | Type | Required | Default |
|--------|------|----------|---------|
| `event_type` or `type` | utf8 | Yes | — |
| `payload` | utf8 (JSON) or struct | No | Entire row as JSON object |
| `timestamp` | timestamp / uint64 | No | Now |

**Vector:**
| Column | Type | Required | Default |
|--------|------|----------|---------|
| `key` | utf8 | Yes | — |
| `embedding` or `vector` | fixed_size_list\<f32\> or list\<f32\> | Yes | — |
| `metadata` | utf8 (JSON) or struct | No | Remaining columns as JSON |

**Graph nodes:**
| Column | Type | Required | Default |
|--------|------|----------|---------|
| `node_id` or `id` | utf8 | Yes | — |
| `object_type` or `type` | utf8 | No | None |
| `properties` | utf8 (JSON) or struct | No | Remaining columns as JSON |

**Graph edges:**
| Column | Type | Required | Default |
|--------|------|----------|---------|
| `source` or `from` | utf8 | Yes | — |
| `target` or `to` | utf8 | Yes | — |
| `edge_type` or `type` | utf8 | Yes | — |
| `weight` | f64 | No | 1.0 |
| `properties` | utf8 (JSON) or struct | No | Remaining columns as JSON |

### Batch Size and Transactions

Import processes data in configurable batches (default: 10,000 rows per transaction). Each batch is one atomic transaction — if it fails, that batch is rolled back but previous batches are committed.

```bash
strata import large_file.parquet --into kv --key-column id --batch-size 50000
```

Progress reporting:
```
Importing users.parquet → kv (default space)
  Format: Parquet (3 columns: id, name, email)
  Rows: 1,000,000
  Mapping: key=id, value={name, email} as JSON
  Progress: [████████████████████░░░░] 850,000 / 1,000,000 (85%)
  Speed: 125,000 rows/sec
```

### Implementation

```
crates/executor/src/arrow/
├── mod.rs              — Feature gate, public API
├── schema.rs           — Column mapping conventions + type coercion
├── ingest.rs           — RecordBatch → primitive batch operations
├── export.rs           — Primitive scan → RecordBatch builders
├── format_readers.rs   — Parquet/CSV/JSONL/NPY → Arrow RecordBatch
└── format_writers.rs   — Arrow RecordBatch → Parquet/CSV/JSONL/NPY

crates/cli/src/
├── import.rs           — `strata import` command
└── export_arrow.rs     — `strata export` command (Arrow-powered)
```

**Cargo feature:** `arrow` (optional, default off for minimal binary size)
```toml
[features]
arrow = ["dep:arrow", "dep:parquet"]
```

**Estimated scope:** ~2,500 lines

---

## Phase 2: Arrow API in Python SDK

**Goal:** Python developers can move data between Strata and the PyData ecosystem with zero-copy Arrow transfers.

### Read Path

```python
import strata

db = strata.open("./mydata")

# Each primitive exposes .to_arrow()
kv_table = db.kv.to_arrow(prefix="user:")
# → pyarrow.Table(key: utf8, value: utf8, version: uint64, timestamp: uint64)

json_table = db.json.to_arrow(prefix="order:")
# → pyarrow.Table(key: utf8, document: utf8, version: uint64,
#                  created_at: uint64, updated_at: uint64)

event_table = db.events.to_arrow(event_type="order_created")
# → pyarrow.Table(sequence: uint64, event_type: utf8, payload: utf8,
#                  timestamp: uint64, hash: binary)

vector_table = db.vectors.to_arrow("my_collection")
# → pyarrow.Table(key: utf8, embedding: fixed_size_list<f32>[384],
#                  metadata: utf8, version: uint64)

graph_nodes = db.graph.nodes_to_arrow("social")
# → pyarrow.Table(node_id: utf8, object_type: utf8, properties: utf8)

graph_edges = db.graph.edges_to_arrow("social")
# → pyarrow.Table(source: utf8, target: utf8, edge_type: utf8,
#                  weight: f64, properties: utf8)

# Use with any Arrow-compatible tool — zero copy
import polars as pl
df = pl.from_arrow(kv_table)

import duckdb
result = duckdb.query("SELECT * FROM json_table WHERE json_extract(document, '$.total') > 100")
```

### Write Path

```python
import pyarrow as pa

# From PyArrow table
users = pa.table({"key": ["u1", "u2"], "value": ['{"name":"Alice"}', '{"name":"Bob"}']})
db.kv.from_arrow(users)

# From Pandas DataFrame
import pandas as pd
df = pd.DataFrame({"order_id": ["o1", "o2"], "total": [99.99, 149.50], "status": ["shipped", "pending"]})
db.json.from_pandas(df, key_column="order_id")
# → Creates JSON documents: o1 → {"total": 99.99, "status": "shipped"}, etc.

# From numpy (vectors)
import numpy as np
keys = ["doc1", "doc2", "doc3"]
embeddings = np.random.randn(3, 384).astype(np.float32)
db.vectors.from_numpy("my_collection", keys, embeddings)

# From Polars DataFrame
import polars as pl
events = pl.DataFrame({"event_type": ["click", "view"], "payload": ['{"page":"home"}', '{"page":"about"}']})
db.events.from_arrow(events.to_arrow())
```

### Implementation

```
strata-python/src/
├── arrow_export.rs    — Rust → PyArrow RecordBatch (via arrow-rs + pyo3)
├── arrow_ingest.rs    — PyArrow RecordBatch → Rust batch operations
└── arrow_utils.rs     — Schema inference, type coercion, numpy bridge
```

The Python SDK uses `pyo3` with the `arrow` feature of `pyo3-arrow` for zero-copy transfer between Rust `arrow::RecordBatch` and Python `pyarrow.Table`.

**Estimated scope:** ~1,500 lines (Rust) + ~500 lines (Python wrappers)

---

## Phase 3: ADBC Driver

**Goal:** Any tool in any language that speaks ADBC can connect to Strata for both reads and writes. One driver replaces the need for JDBC, ODBC, and REST.

### What is ADBC

Arrow Database Connectivity (ADBC) is the Apache Arrow project's replacement for JDBC/ODBC. It provides a standard API for database access with Arrow as the native data format. It works in-process (no server needed) and is supported by DuckDB, SQLite, PostgreSQL, Snowflake, and Flight SQL.

### Usage

```python
# Python (via adbc_driver_manager)
import adbc_driver_strata.dbapi as strata_dbapi

conn = strata_dbapi.connect("/path/to/data")
cursor = conn.cursor()

# Read
cursor.execute("SCAN kv PREFIX 'user:'")
table = cursor.fetch_arrow_table()

# Write
cursor.adbc_ingest("json.orders", orders_arrow_table, mode="create_append")

# SQL (if DataFusion enabled)
cursor.execute("SELECT key, json_extract(document, '$.total') as total FROM json.orders WHERE total > 100")
table = cursor.fetch_arrow_table()

conn.close()
```

```go
// Go
import "github.com/stratadb/adbc-driver-strata"

drv := strata.NewDriver()
db, _ := drv.NewDatabase(map[string]string{"path": "/path/to/data"})
conn, _ := db.Open(context.Background())

stmt, _ := conn.NewStatement()
stmt.SetSQLQuery("SCAN event TYPE 'order_created' LIMIT 1000")
reader, _, _ := stmt.ExecuteQuery(context.Background())
// reader is an Arrow RecordReader — works with any Go Arrow library
```

```java
// Java (via ADBC JDBC bridge — gets JDBC for free)
AdbcDriver driver = new StrataDriver();
AdbcDatabase db = driver.open("/path/to/data");
AdbcConnection conn = db.connect();
AdbcStatement stmt = conn.createStatement();
stmt.setSqlQuery("SCAN json PREFIX 'order:'");
ArrowReader reader = stmt.executeQuery();
```

### Strata Query Language for ADBC

ADBC requires a statement interface. Since Strata doesn't have SQL (yet), we define a minimal query language for ADBC statements:

```sql
-- Reads
SCAN kv [PREFIX 'prefix'] [LIMIT n]
SCAN json [PREFIX 'prefix'] [LIMIT n]
SCAN event [TYPE 'type'] [AFTER seq] [LIMIT n]
SCAN vector collection [LIMIT n]
SCAN graph.name.nodes [TYPE 'type'] [LIMIT n]
SCAN graph.name.edges [LIMIT n]

-- Writes (alternative to adbc_ingest)
-- Writes are primarily done via adbc_ingest(), not statements

-- Search
SEARCH 'query text' [IN kv,json,event] [LIMIT n]

-- Metadata
DESCRIBE
DESCRIBE kv
DESCRIBE json.orders
DESCRIBE vector.my_collection
DESCRIBE graph.social
```

If DataFusion is enabled (Phase 4), full SQL is available via standard `SELECT` / `INSERT` statements and the Strata query language becomes a compatibility fallback.

### Implementation

The ADBC driver implements three traits from the `adbc_core` Rust crate:

```rust
impl AdbcDriver for StrataDriver {
    fn new_database(&self, options: &HashMap<String, String>) -> Result<Box<dyn AdbcDatabase>>;
}

impl AdbcDatabase for StrataDatabase {
    fn new_connection(&self) -> Result<Box<dyn AdbcConnection>>;
}

impl AdbcConnection for StrataConnection {
    fn new_statement(&self) -> Result<Box<dyn AdbcStatement>>;
    fn get_table_types(&self) -> Result<RecordBatch>;        // Returns primitive types
    fn get_objects(&self, ...) -> Result<RecordBatch>;       // Returns collections/spaces
}

impl AdbcStatement for StrataStatement {
    fn set_sql_query(&mut self, query: &str) -> Result<()>;
    fn execute_query(&mut self) -> Result<ArrowArrayStream>;  // Read path
    fn execute_update(&mut self) -> Result<i64>;              // Write path
    fn bind_stream(&mut self, stream: ArrowArrayStream);      // Bulk ingest
}
```

**Estimated scope:** ~2,000 lines

---

## Phase 4: DataFusion SQL (Optional)

**Goal:** Full SQL access to Strata data via Apache DataFusion, an embeddable Rust query engine.

### Usage

```sql
-- Query JSON documents with SQL
SELECT key, json_extract(document, '$.name') as name,
       json_extract(document, '$.email') as email
FROM json.users
WHERE json_extract(document, '$.age') > 25
ORDER BY key
LIMIT 100;

-- Join KV with JSON
SELECT k.key, j.document
FROM kv.sessions k
JOIN json.users j ON json_extract(j.document, '$.session_id') = k.value;

-- Aggregate events
SELECT event_type, COUNT(*) as count, MIN(timestamp) as first, MAX(timestamp) as last
FROM event.default
GROUP BY event_type
ORDER BY count DESC;

-- Vector search (extension function)
SELECT key, strata_vector_search(embedding, ?, 10) as score
FROM vector.my_collection
ORDER BY score DESC;

-- Insert from query
INSERT INTO json.archive
SELECT * FROM json.orders WHERE json_extract(document, '$.status') = 'completed';
```

### Implementation

DataFusion integration requires implementing the `TableProvider` trait for each primitive:

```rust
impl TableProvider for StrataKvTable {
    fn schema(&self) -> SchemaRef;                    // Arrow schema
    fn scan(&self, ...) -> Result<Arc<dyn ExecutionPlan>>;  // Scan with filter pushdown
}
```

Filter pushdown maps SQL `WHERE` clauses to Strata's native prefix scan and range operations, avoiding full table scans where possible.

**Estimated scope:** ~1,500 lines
**Dependency:** DataFusion crate (~large, feature-gated)

---

## Schema for Each Primitive

### KV Arrow Schema

```
key:        utf8 (not null)
value:      utf8 | binary (depends on Value type)
value_type: utf8 (null, bool, int, float, string, bytes, array, object)
version:    uint64
timestamp:  uint64 (microseconds since epoch)
```

### JSON Arrow Schema

```
key:         utf8 (not null)
document:    utf8 (JSON string) | struct (if flattened)
version:     uint64
created_at:  uint64
updated_at:  uint64
```

**Flatten option:** For Parquet/CSV export, JSON documents can be optionally flattened to columnar form (top-level fields become Arrow columns). This requires schema inference from a sample of documents.

### Event Arrow Schema

```
sequence:    uint64 (not null)
event_type:  utf8 (not null)
payload:     utf8 (JSON string)
timestamp:   uint64
prev_hash:   fixed_size_binary(32)
hash:        fixed_size_binary(32)
```

### Vector Arrow Schema

```
key:         utf8 (not null)
embedding:   fixed_size_list<float32>[dimension] (not null)
metadata:    utf8 (JSON string, nullable)
version:     uint64
created_at:  uint64
updated_at:  uint64
```

### Graph Node Arrow Schema

```
node_id:     utf8 (not null)
object_type: utf8 (nullable)
properties:  utf8 (JSON string)
entity_ref:  utf8 (nullable)
```

### Graph Edge Arrow Schema

```
source:      utf8 (not null)
target:      utf8 (not null)
edge_type:   utf8 (not null)
weight:      float64
properties:  utf8 (JSON string, nullable)
```

---

## What This Enables

### For the Developer Evaluating Strata

```bash
# "Can I get my data in?"
strata import my_existing_data.parquet --into json --key-column id
# Yes. One command.

# "Can I get it out if Strata doesn't work?"
strata export json --format parquet --output my_data_back.parquet
# Yes. One command. No lock-in.
```

### For the Data Engineer

```python
# Stream data from Postgres to Strata
pg_conn = adbc_driver_postgresql.connect("postgres://...")
strata_conn = adbc_driver_strata.connect("./agent_data")

cursor = pg_conn.cursor()
cursor.execute("SELECT * FROM users")
strata_conn.cursor().adbc_ingest("json.users", cursor.fetch_arrow_table())

# Query Strata data with DuckDB
import duckdb
table = strata_conn.cursor().execute("SCAN json").fetch_arrow_table()
duckdb.sql("SELECT * FROM table WHERE json_extract(document, '$.age') > 30")
```

### For the ML Engineer

```python
# Export embeddings for training
vectors = db.vectors.to_arrow("documents")
embeddings = vectors.column("embedding").to_numpy()  # zero-copy
# → Use in scikit-learn, PyTorch, etc.

# Import embeddings from a training pipeline
new_embeddings = model.encode(documents)  # numpy array
db.vectors.from_numpy("documents", keys, new_embeddings)
```

### For the AI Agent

```python
# Agent stores observations
db.event_append("observation", {"entity": "server-1", "cpu": 92.5, "status": "warning"})

# Human analyst queries with SQL
# (via DuckDB connected to Strata through ADBC)
SELECT event_type, COUNT(*), AVG(json_extract(payload, '$.cpu'))
FROM event.default
WHERE event_type = 'observation'
  AND timestamp > now() - interval '1 hour'
GROUP BY event_type
```

---

## Phased Rollout

| Phase | Scope | Lines | Dependency | User Value |
|-------|-------|-------|------------|------------|
| **1: CLI Import/Export** | File-based I/O via CLI | ~2,500 | `arrow`, `parquet` crates | **Adoption unblocked** — no code needed to move data |
| **2: Python Arrow API** | `.to_arrow()` / `.from_arrow()` on SDK | ~2,000 | Phase 1 + `pyo3-arrow` | Zero-copy DataFrame integration |
| **3: ADBC Driver** | Universal database connectivity | ~2,000 | Phase 1 | Every ADBC tool in every language works |
| **4: DataFusion SQL** | Embedded SQL engine | ~1,500 | Phase 1 + `datafusion` | SQL queries over Strata data |
| **Total** | | **~8,000** | | |

**Phase 1 is the critical path.** It delivers the adoption message ("your data is not locked in") and builds the Arrow adapter layer that Phases 2-4 reuse.

Phases 2-4 are independently valuable and can be prioritized based on user demand. Phase 2 matters most for the Python/AI audience. Phase 3 matters most for enterprise/tool integration. Phase 4 matters most for analytics use cases.

---

## Alternatives Considered

### JDBC/ODBC

Rejected. Requires C ABI, complex driver lifecycle, row-oriented data transfer (no zero-copy). Designed for client-server databases, not embedded. ADBC supersedes both for modern use cases, and ADBC includes a JDBC bridge for legacy tools.

### REST API

Rejected. Strata is an embedded database — adding an HTTP server contradicts the core positioning. REST also requires serialization to JSON (no zero-copy), doesn't support bulk ingest efficiently, and adds a network stack dependency.

### Custom Wire Protocol

Rejected. Inventing a protocol means writing clients for every language. ADBC already has clients for Python, R, Go, Java, C/C++, and Rust. Building on ADBC gives us all of them for free.

### Parquet as Storage Format

Rejected. Parquet is a columnar analytics format, not a database storage format. Strata's LSM tree requires byte-level control for MVCC, compaction, bloom filters, and zero-copy iteration. Parquet belongs at the boundary (import/export), not in the storage engine.

---

## Open Questions

1. **JSON document flattening:** When exporting JSON documents to Parquet, should we flatten top-level fields into columns (requires schema inference) or store the entire document as a JSON string column? Recommendation: default to JSON string column, offer `--flatten` flag that infers schema from a sample.

2. **Vector embedding format:** Arrow `FixedSizeList<f32>` vs `List<f32>` for variable-dimension support. Recommendation: `FixedSizeList` within a collection (dimension is fixed per collection), `List<f32>` for cross-collection exports.

3. **Transaction semantics for import:** Should `strata import` be all-or-nothing (one transaction) or batched? Recommendation: batched with configurable batch size. All-or-nothing for small files (< 10K rows), batched for large files.

4. **DataFusion table naming:** How do we map Strata's `primitive.namespace` to SQL table names? Recommendation: `kv`, `json`, `event` for default space; `kv_spacename`, `json_spacename` for non-default spaces. Vector: `vector_collectionname`. Graph: `graph_name_nodes`, `graph_name_edges`.

5. **Feature gating:** Should Arrow support be default-on or default-off? Recommendation: default-off in the Rust crate (`--features arrow`), default-on in the Python SDK (most users want it), and default-on in the CLI binary.
