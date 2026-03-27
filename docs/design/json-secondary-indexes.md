# Secondary Indexes on JSON Document Fields

**Issue:** #1880
**Status:** Design
**Priority:** T1 — Table Stakes
**Primitive:** JsonStore
**Competitor baseline:** RediSearch `FT.CREATE ON JSON`, MongoDB `createIndex`, SurrealDB

## Problem

Every filter/sort query on JsonStore beyond full-text search is an O(n) scan over all documents. Without secondary indexes on JSON fields, JsonStore is impractical for any collection beyond a few thousand documents when queries need to filter by field value, numeric range, or tag.

The primitive-capability-audit identifies three related T1 gaps:
- **#13** Secondary index on JSON field
- **#15** Field-level filtering (`@field:value`)
- **#16** Numeric range filter (`@price:[100 200]`)

## Architecture Decision: Search and Query Are Unified

Strata does not introduce a separate query interface. Secondary indexes are an acceleration layer for the **Searchable trait**, which JsonStore currently stubs (returns empty). The unified model:

```
User search query ("price > 100 AND status:active AND 'wireless'")
    |
    v
Searchable::search(SearchRequest)
    |
    +--> Secondary indexes (narrow candidate set by field predicates)
    +--> Inverted index (BM25 score on text matches)
    |
    v
Merged, ranked SearchResponse
```

This means:
- No new `query()` method on JsonStore
- `SearchRequest` gains structured filter predicates alongside the existing text query
- JsonStore's `Searchable` impl goes from stub to real, using indexes
- Field filters produce binary (match / no-match) scores; BM25 produces relevance scores; results merge naturally

## Architecture Decision: Indexes Live in the KV Layer

Secondary index entries are stored as regular KV entries in the storage layer using a reserved space prefix. This is a deliberate choice driven by Strata's branching model:

**Why not a separate data structure:**
- Every new data structure must be branch-aware (fork, merge, diff, materialization)
- The KV layer already handles all of this correctly via `branch_id` in the key prefix
- Segment-level reference counting, inherited layers, and compaction apply automatically
- The performance difference at embedded scale is negligible (a few ms at most)

**What we get for free:**
- MVCC versioning (index entries are versioned like any other key)
- Branch isolation (index on branch A doesn't affect branch B)
- Time-travel (historical queries see the index state at that timestamp)
- Compaction (stale index entries are garbage-collected normally)
- Durability (WAL, segment persistence, mmap — all apply)

## Index Key Encoding

Index entries use the existing `InternalKey` encoding with a reserved space name pattern:

```
Document key (existing):
  branch_id(16) || space\0 || 0x06(Json) || doc_id(escaped) || 0x00 0x00 || !commit_id(8)

Index entry key:
  branch_id(16) || _idx_{space}_{index_name}\0 || 0x06(Json) || encoded_value ++ 0xFF ++ doc_id(escaped) || 0x00 0x00 || !commit_id(8)
```

Where:
- **Space** = `_idx_{collection_space}_{index_name}` — a reserved space name using the `_` prefix convention (similar to `_system_` for vector collections and `_graph_` for graph data)
- **TypeTag** = `0x06` (Json) — indexes belong to the JSON primitive
- **User key** = `encoded_value ++ 0xFF ++ doc_id` — the indexed value followed by separator and document ID for uniqueness
- **Value** = empty or minimal (the index entry is the key itself; the doc_id in the key is the pointer back to the document)

### Value Encoding for Sort Order

Index entries must sort in the correct order for range scans to work via prefix iteration:

| Index Type | Encoding | Sort Order |
|---|---|---|
| **Numeric** | IEEE 754 f64, sign-bit flipped, big-endian (8 bytes) | Natural numeric order |
| **Tag** | UTF-8 string bytes, lowercased | Lexicographic |
| **Text** | UTF-8 string bytes, lowercased, truncated to 128 bytes | Lexicographic prefix |

**Numeric encoding detail:** Flip the sign bit of the IEEE 754 representation, then for negative numbers also flip all other bits. This maps the f64 total order to unsigned byte order. This is a well-known technique (used by FoundationDB, CockroachDB, etc.).

### Example

Collection `products` in space `default`, index `price_idx` on field `$.price`:

```
Document: { "id": "widget-1", "price": 29.99, "status": "active" }

Document key:
  {branch_id} || default\0 || 0x06 || widget-1 || \0\0 || !commit_id

Index entry key:
  {branch_id} || _idx_default_price_idx\0 || 0x06 || <f64_encoded(29.99)> 0xFF widget-1 || \0\0 || !commit_id

Index entry value:
  (empty — doc_id is embedded in the key)
```

Range query `price:[20 50]`:
1. Encode lower bound: `f64_encode(20.0)`
2. Encode upper bound: `f64_encode(50.0)`
3. Prefix-scan `_idx_default_price_idx` space, iterate from lower to upper bound
4. Extract `doc_id` from each matching key
5. Fetch full documents by doc_id for scoring/returning

## Index Metadata

Index definitions are stored in a metadata space `_idx_meta_{space}` as JSON documents:

```json
{
  "name": "price_idx",
  "collection_space": "default",
  "field_path": "$.price",
  "index_type": "Numeric",
  "created_at": 1711468800000000
}
```

This is also in the KV layer, so it's branch-aware. Creating an index on branch A doesn't create it on branch B.

## Index Maintenance

**Synchronous, same-transaction updates.** When a document is written or deleted, all indexes on that collection are updated in the same transaction:

1. **On document create/update:**
   - Read index metadata for the collection
   - For each index, extract the field value at `field_path` from the new document
   - If updating, delete the old index entry (requires reading the old document to get the old field value)
   - Write the new index entry

2. **On document delete:**
   - Read index metadata for the collection
   - For each index, read the document to extract the current field value
   - Delete the index entry

This adds read amplification on writes (must read old doc on update/delete to compute old index key). At embedded scale this is acceptable. The alternative (async index maintenance) would introduce stale reads, which is worse.

## Filter Predicate Model

`SearchRequest` is extended with structured filter predicates:

```rust
/// A predicate on an indexed JSON field
pub enum FieldPredicate {
    /// Exact match: @status:{active}
    Eq { field: String, value: JsonValue },
    /// Range: @price:[100 200]
    Range {
        field: String,
        lower: Option<JsonValue>,
        upper: Option<JsonValue>,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    /// Prefix match on text: @name:wire*
    Prefix { field: String, prefix: String },
}

/// Compound filter with boolean logic
pub enum FieldFilter {
    Predicate(FieldPredicate),
    And(Vec<FieldFilter>),
    Or(Vec<FieldFilter>),
}
```

`SearchRequest` gains an optional `field_filter: Option<FieldFilter>` field. When present, the JsonStore `Searchable` impl uses indexes to resolve it. When combined with a text query, the field filter narrows the candidate set before BM25 scoring.

---

## Epic 1: Index Primitives

The plumbing. No user-facing query changes. After this epic, indexes exist and are maintained, but cannot be queried through search.

### 1.1 Index Types and Metadata

**File:** `crates/engine/src/primitives/json.rs` (or new submodule `json/index.rs`)

- Define `IndexType` enum: `Numeric`, `Tag`, `Text`
- Define `IndexDef` struct: name, collection_space, field_path, index_type, created_at
- Serialization to/from JSON (stored as metadata docs in KV layer)

### 1.2 Value Encoding

**File:** `crates/engine/src/primitives/json/index.rs`

- `encode_numeric(f64) -> [u8; 8]` — sign-flip IEEE 754 encoding
- `decode_numeric([u8; 8]) -> f64` — inverse
- `encode_tag(str) -> Vec<u8>` — lowercase UTF-8
- `encode_index_key(index_name, space, encoded_value, doc_id) -> Key`
- Round-trip tests for all encodings, including edge cases (NaN, -0, infinity, empty strings)

### 1.3 Index CRUD API

**File:** `crates/engine/src/primitives/json.rs`

Methods on `JsonStore`:
- `create_index(branch_id, space, name, field_path, index_type) -> Result<IndexDef>`
  - Validates field_path syntax
  - Writes index metadata to `_idx_meta_{space}`
  - Returns error if index name already exists on this branch
- `drop_index(branch_id, space, name) -> Result<()>`
  - Deletes index metadata
  - Deletes all index entries (scan + delete on `_idx_{space}_{name}`)
- `list_indexes(branch_id, space) -> Result<Vec<IndexDef>>`
  - Scans `_idx_meta_{space}` for all index definitions

### 1.4 Synchronous Index Maintenance

**File:** `crates/engine/src/primitives/json.rs`

Modify document write and delete paths:
- `set()` / `batch_set()`: after writing document, update all index entries
- `delete()` / `batch_delete()`: before deleting document, remove index entries
- Extract field value at `field_path` from JSON document using existing `JsonPath` machinery
- Handle missing fields gracefully (no index entry if field absent)
- Handle type mismatches (e.g., string value in numeric index → skip, log warning)

### 1.5 Executor Commands

**File:** `crates/executor/src/command.rs`, `crates/executor/src/handlers/json.rs`

- `JsonCreateIndex { space, name, field_path, index_type }`
- `JsonDropIndex { space, name }`
- `JsonListIndexes { space }`
- Handler implementations that delegate to `JsonStore`

### 1.6 Tests

- Create index, write documents, verify index entries exist in KV layer
- Update document, verify old index entry removed and new one written
- Delete document, verify index entry removed
- Drop index, verify all entries cleaned up
- Branch isolation: create index on branch A, verify not visible on branch B
- Missing field: document without indexed field produces no index entry
- Type mismatch: string in numeric index field is skipped

---

## Epic 2: Index-Backed Field Filtering via Searchable

This is where indexes become useful. JsonStore's `Searchable` impl goes from stub to real.

### 2.1 FieldPredicate and FieldFilter Types

**File:** `crates/engine/src/search/types.rs`

- Add `FieldPredicate` enum (Eq, Range, Prefix)
- Add `FieldFilter` enum (Predicate, And, Or)
- Add `field_filter: Option<FieldFilter>` to `SearchRequest`
- Backward compatible — existing callers pass `None`

### 2.2 Index Lookup Engine

**File:** `crates/engine/src/primitives/json/index.rs`

- `lookup_eq(branch_id, space, index_name, value) -> Vec<DocId>` — point lookup via prefix scan
- `lookup_range(branch_id, space, index_name, lower, upper) -> Vec<DocId>` — range scan
- `lookup_prefix(branch_id, space, index_name, prefix) -> Vec<DocId>` — prefix scan on text index
- All operations are prefix scans on the KV layer — no new storage primitives

### 2.3 Filter Resolution

**File:** `crates/engine/src/primitives/json/index.rs`

- `resolve_filter(branch_id, space, filter: &FieldFilter, indexes: &[IndexDef]) -> Result<HashSet<DocId>>`
- Maps each `FieldPredicate` to the appropriate index lookup
- AND = intersection of doc_id sets
- Returns error if a predicate references a field with no index (no silent full-scan fallback)

### 2.4 JsonStore Searchable Implementation

**File:** `crates/engine/src/primitives/json.rs`

Replace the stub with:
1. If `field_filter` is present, resolve it to a candidate doc_id set via index lookups
2. If text `query` is also present, score candidates with BM25 (via inverted index)
3. If only `field_filter`, return matches ordered by doc_id (or indexed value for sorted results)
4. If only text `query`, fall back to inverted index scoring (same as KVStore today)
5. Build `SearchResponse` with hits, snippets, and stats (including `index_used: true`)

### 2.5 Tests

- Numeric range: create index on `price`, insert docs, search with `price:[10 50]`, verify correct results
- Tag exact match: index on `status`, search `status:{active}`, verify filtering
- AND compound: `price:[10 50] AND status:{active}`, verify intersection
- Combined text + filter: text query "wireless" with `price:[0 100]`, verify BM25 scoring within filtered set
- Empty result: filter that matches nothing returns empty SearchResponse
- No index error: predicate on non-indexed field returns error
- Branch isolation: index and docs on branch A, search on branch B returns empty

---

## Epic 3: Sorted Results and OR Logic

### 3.1 Sort-by-Indexed-Field

- Add `sort_by: Option<SortSpec>` to `SearchRequest` where `SortSpec = { field, direction }`
- When sorting by an indexed numeric field, iterate the index in order instead of collecting + sorting
- Integrates with pagination (cursor-based iteration on index)

### 3.2 OR Filter Logic

- `Or(Vec<FieldFilter>)` = union of doc_id sets
- Combine with AND for expressions like `(status:active OR status:pending) AND price:[10 50]`

### 3.3 Richer Scoring Integration

- Field filter match as a scoring signal (boost factor for field matches)
- Combine BM25 text score + field filter signals in unified ranking

---

## Epic 4: Composite Indexes and Aggregation

### 4.1 Composite Indexes

- Index on multiple fields: `create_index(space, name, [(field1, type1), (field2, type2)])`
- Key encoding: concatenate encoded values in field order
- Enables efficient compound equality + range queries (leftmost prefix rule, same as B-tree composite indexes)

### 4.2 Aggregation Primitives

- `Sum`, `Avg`, `Min`, `Max` over indexed numeric fields
- `GroupBy` field + aggregation
- Implemented as index scans — no full document reads needed for numeric aggregations
- Exposed through a new `aggregate()` method on JsonStore (not through Searchable — aggregations aren't search)

---

## Non-Goals

- **Background index building for existing documents** — not needed today (no pre-existing unindexed collections in production)
- **Automatic index selection / query planner** — the caller specifies which indexes exist; predicates that reference non-indexed fields are rejected
- **Partial indexes** (index only documents matching a condition) — future enhancement
- **Full JSONPath filter expressions** (`$[?(@.x>5)]`) — separate issue (#5 in audit), orthogonal to secondary indexes
- **TTL integration** — blocked on branch-aware GC (#1860)

## References

- `crates/engine/src/primitives/json.rs` — JsonStore implementation (2918 lines)
- `crates/engine/src/search/searchable.rs` — Searchable trait, BM25LiteScorer
- `crates/engine/src/search/types.rs` — SearchRequest, SearchResponse, SearchHit
- `crates/engine/src/search/index.rs` — InvertedIndex (segmented, mmap-backed)
- `crates/storage/src/key_encoding.rs` — InternalKey encoding scheme
- `crates/core/src/primitives/json.rs` — JsonPath, JsonValue types
- `docs/design/primitive-capability-audit.md` — Capability gaps #13, #15, #16
- Issue #1860 — Branch-aware GC (blocks TTL, not indexes)
- Issue #1878 — TTL (blocked on #1860)
