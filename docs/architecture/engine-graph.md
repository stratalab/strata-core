# Engine Graph Subsystem Architecture

**Crate:** `strata-engine` (graph module)
**Dependencies:** `strata-core`, `strata-concurrency`, `strata-storage`, `serde_json`, `dashmap`, `once_cell`
**Source files:** 11 modules (~12,089 lines total)

## Overview

The graph subsystem implements a **property graph overlay** on top of Strata's KV storage engine. All graph data (nodes, edges, metadata, ontology, indices) is stored as KV entries under the `_graph_` namespace, inheriting branch isolation, time-travel, and transactional guarantees from the underlying storage layer.

| Subsystem | Files | Lines | Purpose |
|-----------|-------|-------|---------|
| Core CRUD | `mod.rs` | ~3,574 | GraphStore: node/edge CRUD, bulk insert, snapshots |
| Ontology | `ontology.rs` | ~2,529 | Typed schema, object/link types, draft/frozen lifecycle |
| Traversal | `traversal.rs` | ~1,195 | BFS, neighbors, degree, subgraph extraction |
| Analytics | `analytics.rs` | ~1,081 | WCC, CDLP, PageRank, LCC, SSSP |
| Keys | `keys.rs` | ~866 | Key construction, validation, namespace caching |
| Types | `types.rs` | ~741 | NodeData, EdgeData, GraphMeta, result types |
| Packed | `packed.rs` | ~582 | Compact binary adjacency list serialization |
| Adjacency | `adjacency.rs` | ~457 | In-memory AdjacencyIndex for traversals |
| Boost | `boost.rs` | ~386 | Proximity-based search score boosting |
| Snapshot | `snapshot.rs` | ~359 | Graph snapshot tests and algorithm execution |
| Integrity | `integrity.rs` | ~319 | Cascade hooks for entity deletion |

## Module Map

```
engine/src/graph/
├── mod.rs           GraphStore struct, CRUD, bulk_insert, snapshot, adjacency builder
├── ontology.rs      Object/link type CRUD, draft/frozen lifecycle, validation
├── traversal.rs     neighbors(), degree(), bfs(), subgraph()
├── analytics.rs     wcc(), cdlp(), pagerank(), lcc(), sssp()
├── keys.rs          Key construction, validation, namespace cache
├── types.rs         NodeData, EdgeData, GraphMeta, BfsOptions, result types
├── packed.rs        Binary adjacency list codec (encode/decode/append/remove)
├── adjacency.rs     AdjacencyIndex (in-memory materialized view)
├── boost.rs         GraphBoost, compute_proximity_map(), apply_boost()
├── snapshot.rs      Snapshot tests, GraphAlgorithm trait tests
└── integrity.rs     on_entity_deleted() cascade hook
```

---

## 1. KV Storage Mapping

All graph data lives under a single KV namespace per branch:

```
Namespace::for_branch_space(branch_id, "_graph_")
```

The namespace is cached in a global `DashMap<BranchId, Arc<Namespace>>` (one heap allocation per branch, ever). This fixed an OOM issue (#1297) where repeated namespace construction caused unbounded allocations.

### 1.1 Key Schema

All keys use `/` as the path separator. Identifiers (graph names, node IDs, edge types) are validated: non-empty, no `/`, no `__` prefix, max 255 bytes.

```
Graph Data Keys:
  {graph}/__meta__                          Graph metadata (GraphMeta JSON)
  {graph}/n/{node_id}                       Node data (NodeData JSON)
  {graph}/fwd/{node_id}                     Forward adjacency list (packed binary)
  {graph}/rev/{node_id}                     Reverse adjacency list (packed binary)

Index Keys:
  __ref__/{encoded_uri}/{graph}/{node_id}   Entity-ref reverse index
  {graph}/__by_type__/{object_type}/{node_id}  Type index (object_type → nodes)
  {graph}/__edge_count__/{edge_type}        Per-type edge counter

Ontology Keys:
  {graph}/__types__/object/{name}           Object type definition (JSON)
  {graph}/__types__/link/{name}             Link type definition (JSON)

Catalog:
  __catalog__                               JSON array of graph names
```

### 1.2 Key Layout Diagram

```
_graph_ namespace (per branch)
│
├── __catalog__                     ["patients", "knowledge", ...]
│
├── patients/
│   ├── __meta__                    {"cascade_policy":"cascade","ontology_status":"frozen"}
│   ├── n/
│   │   ├── patient-001             {"entity_ref":"kv://main/p001","object_type":"Patient",...}
│   │   ├── patient-002             {"properties":{"name":"Alice"},...}
│   │   └── lab-result-001          {"object_type":"LabResult",...}
│   ├── fwd/
│   │   ├── patient-001             [packed: (lab-result-001, HAS_RESULT, 1.0)]
│   │   └── patient-002             [packed: (patient-001, KNOWS, 0.9)]
│   ├── rev/
│   │   ├── lab-result-001          [packed: (patient-001, HAS_RESULT, 1.0)]
│   │   └── patient-001             [packed: (patient-002, KNOWS, 0.9)]
│   ├── __types__/
│   │   ├── object/Patient          {"name":"Patient","properties":{"name":{"type":"string",...}}}
│   │   ├── object/LabResult        {"name":"LabResult","properties":{...}}
│   │   └── link/HAS_RESULT         {"name":"HAS_RESULT","source":"Patient","target":"LabResult"}
│   ├── __by_type__/
│   │   ├── Patient/patient-001     null
│   │   ├── Patient/patient-002     null
│   │   └── LabResult/lab-result-001 null
│   └── __edge_count__/
│       ├── HAS_RESULT              "1"
│       └── KNOWS                   "1"
│
└── __ref__/
    └── kv:%2F%2Fmain%2Fp001/patients/patient-001   null
```

### 1.3 URI Encoding

Entity-ref URIs in index keys use percent-encoding for characters that would conflict with the key separator: `%` -> `%25`, `/` -> `%2F`, null -> `%00`, control characters -> `%XX`. Round-trip safe.

---

## 2. GraphStore (`mod.rs`)

The central struct wrapping `Arc<Database>`. All operations are transactional via `db.transaction(branch_id, |txn| { ... })`.

### 2.1 Graph Lifecycle

| Operation | Transaction | Side effects |
|-----------|-------------|-------------|
| `create_graph` | Put meta key + update catalog | Catalog is a JSON array of names |
| `list_graphs` | Read catalog (O(1)), fallback to `__meta__` scan | Lazily creates catalog on fallback |
| `delete_graph` | Batched deletion (10K keys/txn) | Cleans ref index, type index, adj lists |
| `get_graph_meta` | Single key read | Returns `Option<GraphMeta>` |

### 2.2 Node CRUD

```
add_node(branch, graph, node_id, data) →
  1. Validate identifiers
  2. If frozen ontology + object_type: validate against schema
  3. Serialize NodeData to JSON
  4. Transaction:
     a. Read old node (if exists) → clean up old ref/type index entries
     b. Put node key
     c. Put ref index key (if entity_ref present, value = Null)
     d. Put type index key (if object_type present, value = Null)

remove_node(branch, graph, node_id) →
  1. Read node → get entity_ref for ref index cleanup
  2. Track edge type decrements from forward adj list
  3. For each outgoing neighbor: remove self from neighbor's reverse adj list
  4. For each incoming neighbor: remove self from neighbor's forward adj list
  5. Decrement edge type counters
  6. Delete node key + both adj lists
```

### 2.3 Edge CRUD

Edges use **packed adjacency lists** (see section 5). Each node has at most two adjacency entries: forward (`fwd/{node_id}`) and reverse (`rev/{node_id}`).

```
add_edge(branch, graph, src, dst, edge_type, data) →
  1. Validate identifiers + frozen ontology
  2. Transaction:
     a. Verify both src and dst nodes exist (prevents TOCTOU)
     b. Read-modify-write forward adj list (remove old + append new = upsert)
     c. Read-modify-write reverse adj list (same upsert)
     d. Increment edge type counter (only for new edges, not updates)
     e. Use put_replace (skip MVCC versioning — adj lists are mutable blobs)
```

### 2.4 Bulk Insert

`bulk_insert(branch, graph, nodes, edges, chunk_size)` provides high-throughput ingestion:

- Default chunk size: 250,000 entries per transaction
- Nodes: serialized in chunks, with upsert cleanup for old index entries
- Edges: grouped by source (fwd) and destination (rev), packed into adjacency lists
- Namespace hoisted once (avoids per-key DashMap lookup)
- Ontology type caches loaded once for bulk validation
- Forward/reverse adj lists merged with existing via `get_value_direct()` (bypasses read-set tracking, no OCC validation overhead)
- Edge type counters accumulated and written in a single final transaction

### 2.5 Graph Stats and Snapshots

- `snapshot_stats()` — node count via prefix scan key count, edge count via `packed::edge_count()` header reads (no full decode). Single transaction for consistency.
- `snapshot()` — materializes all nodes and edges into a `GraphSnapshot` struct
- `for_each_edge()` — streaming edge iteration without full materialization

---

## 3. Ontology System (`ontology.rs`, `types.rs`)

The ontology provides optional typed schema for graphs, following a **draft/frozen lifecycle**.

### 3.1 Type Definitions

**Object types** (node schemas):

```rust
struct ObjectTypeDef {
    name: String,                           // e.g. "Patient"
    properties: HashMap<String, PropertyDef>, // property schemas
}

struct PropertyDef {
    r#type: Option<String>,  // "string", "integer", etc. (hint, not enforced in draft)
    required: bool,          // enforced when frozen
}
```

**Link types** (edge schemas):

```rust
struct LinkTypeDef {
    name: String,           // e.g. "HAS_RESULT"
    source: String,         // source object type name
    target: String,         // target object type name
    cardinality: Option<String>, // hint only
    properties: HashMap<String, PropertyDef>,
}
```

### 3.2 Draft/Frozen Lifecycle

```
Untyped ──(first define_object_type)──→ Draft ──(freeze_ontology)──→ Frozen
                                          │                            │
                                     Types can be                 Types are
                                     added/removed               immutable;
                                                                 validation
                                                                 is active
```

| State | Type CRUD | Node/Edge Validation | Transition |
|-------|-----------|---------------------|------------|
| Untyped | N/A | None | Implicit on first type definition |
| Draft | define/delete object/link types | None | `freeze_ontology()` |
| Frozen | Rejected with error | Active (required props, type matching) | Terminal |

**Freeze validation:**
1. At least one type must be defined
2. All link type `source`/`target` must reference existing object types
3. Atomic read-modify-write of GraphMeta (OCC protected)

### 3.3 Schema Validation (Frozen)

When ontology is frozen, `add_node` and `add_edge` validate:

- **Nodes:** object_type must be declared; required properties must be present; property type hints enforced
- **Edges:** edge_type must be declared as a link type; source/target node object_types must match the link type's source/target; required edge properties checked

**Cached validation** for bulk operations: `load_object_type_cache()` / `load_link_type_cache()` preload all type definitions once, avoiding per-node/edge KV reads.

### 3.4 Introspection

`ontology_summary()` returns an `OntologySummary` with:
- Current status (Draft/Frozen)
- Object type summaries (properties + live node counts from type index)
- Link type summaries (source/target/cardinality + live edge counts from counters)

---

## 4. Adjacency Index (`adjacency.rs`)

In-memory materialized view of a graph's edge structure for O(1) neighbor lookups.

### 4.1 Structure

```rust
struct AdjacencyIndex {
    outgoing: HashMap<String, Vec<(String, String, EdgeData)>>,  // src → [(dst, type, data)]
    incoming: HashMap<String, Vec<(String, String, EdgeData)>>,  // dst → [(src, type, data)]
    nodes: HashSet<String>,                                       // all node IDs
}
```

### 4.2 Operations

- `add_edge(src, dst, type, data)` — dedup on (dst, edge_type) with last-write-wins
- `remove_node(id)` — removes node + cleans up both directions of all incident edges
- `outgoing_neighbor_ids(node, filter)` — zero-alloc iterator returning `(&str, &str)` pairs
- `outgoing_weighted(node)` — zero-alloc iterator returning `(&str, f64)` pairs
- `incoming_neighbor_ids(node, filter)` — zero-alloc iterator

### 4.3 Build Path

```
GraphStore::build_adjacency_index(branch, graph) →
  1. all_edges() — single prefix scan of all fwd/ keys
  2. Decode each packed adjacency list
  3. Populate AdjacencyIndex from edge list
  Return index for reuse across multiple traversals
```

---

## 5. Packed Adjacency Lists (`packed.rs`)

Compact binary format storing all edges from/to a single node in one KV entry, reducing entry count by ~27x compared to one-entry-per-edge.

### 5.1 Wire Format

```
┌────────────────────────────────────────────────────┐
│ Header                                             │
│   edge_count: u32 LE                    (4 bytes)  │
├────────────────────────────────────────────────────┤
│ Edge 0                                             │
│   edge_type_len: u16 LE                 (2 bytes)  │
│   edge_type: [u8; len]                  (UTF-8)    │
│   target_id_len: u16 LE                 (2 bytes)  │
│   target_id: [u8; len]                  (UTF-8)    │
│   weight: f64 LE                        (8 bytes)  │
│   properties_len: u32 LE                (4 bytes)  │
│   properties_json: [u8; len]  (only if len > 0)   │
├────────────────────────────────────────────────────┤
│ Edge 1 ...                                         │
└────────────────────────────────────────────────────┘
```

### 5.2 Operations

| Function | Complexity | Description |
|----------|-----------|-------------|
| `encode(edges)` | O(n) | Batch encode from slice |
| `decode(bytes)` | O(n) | Full decode to Vec |
| `append_edge(buf, target, type, data)` | O(1) amortized | Increment header, append bytes |
| `remove_edge(bytes, target, type)` | O(n) | Linear scan, copy without removed entry |
| `find_edge(bytes, target, type)` | O(n) | Linear scan, return EdgeData |
| `edge_count(bytes)` | O(1) | Read 4-byte header only |
| `merge(existing, new)` | O(1) | Update header, concatenate bytes |
| `empty()` | O(1) | Return `[0, 0, 0, 0]` |

**Size limit:** 16 MB per adjacency list (~500K+ edges per node). `append_edge` checks the limit before mutation to prevent corruption.

### 5.3 Read-Modify-Write Pattern

Edge mutations follow a read-modify-write cycle within a single transaction:

```
1. Read packed bytes from KV (or empty() if absent)
2. remove_edge() if upsert (existing edge with same target+type)
3. append_edge() with new data
4. put_replace() back to KV (KeepLast(1) retention hint — blob is mutable)
```

---

## 6. Traversal Algorithms (`traversal.rs`)

### 6.1 Neighbors and Degree

- `neighbors(branch, graph, node, direction, filter)` — reads packed adj lists, decodes, returns `Vec<Neighbor>`
- `degree(branch, graph, node, direction)` — reads packed header only via `edge_count()` (zero decode)

### 6.2 BFS

```
bfs(branch, graph, start, opts) →
  1. build_adjacency_index() — single bulk scan
  2. bfs_with_index(start, opts, index) — pure in-memory BFS

bfs_with_index(start, opts, index) →
  Queue-based BFS with:
  - max_depth limit
  - max_nodes limit
  - edge_type filter (applied at every hop)
  - direction (Outgoing/Incoming/Both)
  - Zero-alloc neighbor iteration via macro
  Returns BfsResult { visited, depths, edges }
```

`bfs_with_index` is public, enabling multiple traversals on the same graph without rebuilding the index.

### 6.3 Subgraph Extraction

```
subgraph(branch, graph, node_ids) →
  For each node_id: get_node() to collect NodeData
  For each existing node: outgoing_neighbors() to find edges within the set
  Cost: O(sum of degrees) instead of O(total edges)
```

---

## 7. Analytics Algorithms (`analytics.rs`)

All algorithms follow the same pattern: build `AdjacencyIndex` + load isolated nodes, then run a pure function on the index.

### 7.1 Weakly Connected Components (WCC)

**Algorithm:** Union-find with path compression and union by rank.

- Collects all nodes (from edges + explicit node set)
- Processes forward edges only (undirected — each edge counted once)
- Component ID = index of smallest node in sorted order

### 7.2 Community Detection via Label Propagation (CDLP)

**Algorithm:** Iterative label propagation.

- Each node starts with a unique label (sorted index)
- Each iteration: adopt most frequent neighbor label (ties broken by smallest)
- Configurable direction and max_iterations (default 10)
- Early termination on convergence

### 7.3 PageRank

**Algorithm:** Power iteration with dangling node handling.

- Initial rank: 1/N for each node
- Damping factor (default 0.85), max iterations (default 20), tolerance (default 1e-6)
- Dangling node mass redistributed uniformly (preserves rank sum = 1.0)
- Convergence check: L1 norm of rank delta

### 7.4 Local Clustering Coefficient (LCC)

**Algorithm:** O(n * d^2) triangle counting per node.

- Undirected neighbor set (union of outgoing + incoming, excluding self-loops)
- Edge set for O(1) connectivity checks
- Coefficient = actual triangles / possible triangles (0.0 for degree < 2)

### 7.5 Single-Source Shortest Path (SSSP)

**Algorithm:** Dijkstra with binary heap.

- `OrdF64` wrapper for f64 ordering in `BinaryHeap<Reverse<...>>`
- Configurable direction (Outgoing/Incoming/Both)
- Returns distances map (unreachable nodes omitted)

---

## 8. Referential Integrity (`integrity.rs`)

### 8.1 Entity-Ref Binding

Nodes can bind to external entities via `entity_ref` (e.g., `"kv://main/patient-4821"`). A reverse index (`__ref__/` prefix) enables lookup by URI.

### 8.2 Cascade on Entity Deletion

```
on_entity_deleted(branch, entity_ref_uri) →
  1. nodes_for_entity() — scan ref index prefix for all (graph, node_id) bindings
  2. For each binding, read graph's cascade_policy from GraphMeta:
     - Cascade: remove_node() (deletes node + all incident edges)
     - Detach: read node, clear entity_ref, re-write
     - Ignore: no-op
  3. Process ALL bindings (no fail-fast)
  4. Return CascadeResult { succeeded, failed }
```

### 8.3 Cascade Policies

| Policy | Node | Edges | entity_ref |
|--------|------|-------|-----------|
| `Cascade` | Deleted | Deleted | N/A |
| `Detach` | Kept | Kept | Cleared |
| `Ignore` | Kept | Kept | Unchanged |

---

## 9. Graph-Boosted Search (`boost.rs`)

Proximity-based scoring for search results based on graph distance to anchor nodes.

### 9.1 Proximity Computation

```
compute_proximity_map(gs, branch, boost) →
  For each anchor node:
    BFS with max_depth, Direction::Both
    For each visited node with entity_ref:
      proximity = 1.0 / 2^depth  (anchor=1.0, 1-hop=0.5, 2-hop=0.25, ...)
      Keep maximum proximity across multiple anchors
  Return HashMap<entity_ref_uri, proximity>
```

### 9.2 Score Boosting

```
apply_boost(score, weight, proximity) = score * (1.0 + weight * proximity)
```

Default weight: 0.3 (up to 30% boost for direct neighbors).

---

## 10. Branch Isolation

Graph data is branch-isolated by construction. The namespace `Namespace::for_branch_space(branch_id, "_graph_")` prefixes all keys with the branch ID. This means:

- Different branches have completely independent graph data
- Graph operations on one branch cannot see or conflict with another
- Standard OCC transaction semantics apply within a branch
- The `GraphStore` accepts `branch_id` on every operation

```
Branch A: _graph_[branch_A]/patients/n/patient-001 → {...}
Branch B: _graph_[branch_B]/patients/n/patient-001 → {...}  (independent copy)
```

Namespace cache cleanup: `invalidate_namespace_cache(branch_id)` should be called on branch deletion to prevent stale `Arc<Namespace>` entries.

---

## Data Flow: Edge Add

```
┌──────────────────────────────────────────────────────────────────────┐
│ Client: gs.add_edge(branch, "g", "A", "B", "KNOWS", data)          │
└─────────────────────────────┬────────────────────────────────────────┘
                              │
           ┌──────────────────▼──────────────────┐
           │  Validate identifiers + ontology     │
           │  (graph name, node IDs, edge type)   │
           └──────────────────┬──────────────────┘
                              │
           ┌──────────────────▼──────────────────┐
           │  db.transaction(branch, |txn| {      │
           │    1. Verify src node exists          │
           │    2. Verify dst node exists          │
           │    3. Read fwd adj list (packed)      │
           │    4. Remove old edge (if upsert)     │
           │    5. Append new edge                 │
           │    6. put_replace fwd adj list        │
           │    7. Read rev adj list (packed)      │
           │    8. Remove old edge (if upsert)     │
           │    9. Append new edge                 │
           │   10. put_replace rev adj list        │
           │   11. Increment edge type counter     │
           │  })                                   │
           └──────────────────┬──────────────────┘
                              │
               OCC commit via TransactionManager
```

## Data Flow: BFS Traversal

```
┌──────────────────────────────────────────────────────────────────────┐
│ Client: gs.bfs(branch, "g", "A", opts)                              │
└─────────────────────────────┬────────────────────────────────────────┘
                              │
           ┌──────────────────▼──────────────────┐
           │  build_adjacency_index(branch, "g")  │
           │  1. scan_prefix("g/fwd/")            │
           │  2. Decode each packed adj list       │
           │  3. Populate AdjacencyIndex           │
           └──────────────────┬──────────────────┘
                              │
           ┌──────────────────▼──────────────────┐
           │  bfs_with_index("A", opts, index)    │
           │  Pure in-memory BFS:                 │
           │  - VecDeque queue                    │
           │  - HashSet seen                      │
           │  - Zero-alloc neighbor iteration     │
           │  - Edge type filtering per hop       │
           │  - max_depth + max_nodes guards      │
           └──────────────────┬──────────────────┘
                              │
                    BfsResult { visited, depths, edges }
```

---

## Performance Characteristics

| Operation | Cost | Notes |
|-----------|------|-------|
| Node read/write | O(1) KV ops | Single key lookup |
| Edge read/write | O(E_node) | Read-modify-write packed adj list |
| Degree query | O(1) | 4-byte header read, no decode |
| Neighbor query | O(E_node) | Full packed list decode |
| BFS | O(V + E) | Single bulk scan + in-memory traversal |
| PageRank | O(V * E * iterations) | In-memory power iteration |
| WCC | O(V + E) | Union-find with path compression |
| SSSP | O((V + E) log V) | Dijkstra with binary heap |
| Bulk insert | O(N + E) | Chunked transactions, packed adjacency |
| Graph deletion | O(keys / 10K) txns | Batched to bound memory |

---

## Key Design Decisions

### Packed Adjacency Lists vs One-Entry-Per-Edge
All edges from/to a single node are stored in one packed binary KV entry. This reduces KV entry count by ~27x and enables atomic read-modify-write of the entire neighbor list. The tradeoff is O(n) scan for single-edge lookup within a node's adj list, but this is fast for typical graph degrees.

### Adjacency Index: Build Once, Traverse Many
Traversal and analytics algorithms build an in-memory `AdjacencyIndex` from a single bulk scan, then run entirely in-memory. This avoids N per-node KV transactions during traversal. The index is exposed publicly (`bfs_with_index`) so callers can reuse it across multiple traversals.

### put_replace for Adjacency Lists
Adjacency lists use `put_replace` instead of standard `put`, which marks the key with `WriteMode::KeepLast(1)`. The write still appends a new MVCC version; older versions are pruned at compaction time. This signals single-version intent for mutable blobs that are always fully overwritten.

### Catalog for Graph Listing
Graph names are maintained in a `__catalog__` JSON array for O(1) listing. Legacy data without a catalog falls back to scanning for `__meta__` keys and lazily creates the catalog.

### Ontology as Advisory Schema
Property `type` hints are recorded for AI orientation but only `required` is strictly enforced. Type enforcement (matching JSON value types to declared types) was added later (G-16). Cardinality on link types is purely informational.

### Entity-Ref Index is Graph-Agnostic
The `__ref__/` index is flat (not nested under a graph prefix), enabling cross-graph lookups by entity URI. This supports cascade operations that may affect nodes across multiple graphs.
