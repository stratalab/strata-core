## Product Requirements Document

**Product:** `strata-graph`
**Type:** Module in `strata-engine` (like `search/`)
**Status:** Proposed
**Owner:** Strata Core Team

---

# 1. Purpose

`strata-graph` provides **cross-primitive relationship tracking** on top of Strata's existing KV primitive. It lets users express directed, typed relationships between KV entries, JSON documents, events, vectors, and arbitrary string-identified nodes — with branch isolation, versioning, time-travel, and fork/merge inherited for free.

The key insight: relationships between data are as important as the data itself, but users shouldn't need a separate graph database to express them. By encoding edges as KV entries with a dual-index key convention, `strata-graph` turns Strata's existing storage engine into a relationship-aware system without introducing any new primitives or storage backends.

### Motivating Use Case: Healthcare Patient Graph

Consider a clinical data platform where a single patient's care involves data scattered across every Strata primitive:

| Primitive | What It Stores | Example |
|-----------|---------------|---------|
| JSON | Patient records, clinical notes | `json://main/patient-4821` |
| KV | Lab results, vital signs | `kv://main/lab:4821:CBC:2026-02-10` |
| Vector | Embeddings of clinical notes for similarity search | `vector://main/clinical-notes/note-1137` |
| Event | Admissions, discharges, medication administrations | `event://main/50234` |

Without `strata-graph`, these are disconnected silos. A developer who wants to answer "what lab results led to this diagnosis, and which clinical notes support it?" must manually join across primitives with application code. With `strata-graph`, these relationships are first-class:

```
patient-4821 --DIAGNOSED_WITH--> ICD:E11.9 (Type 2 Diabetes)
patient-4821 --HAS_LAB_RESULT--> lab:4821:HbA1c:2026-01  (bound to kv://main/...)
lab:4821:HbA1c:2026-01 --SUPPORTS--> ICD:E11.9
patient-4821 --HAS_NOTE--> note-1137  (bound to json://main/... AND vector://main/...)
note-1137 --MENTIONS--> ICD:E11.9
ICD:E11.9 --TREATED_BY--> med:metformin
med:metformin --CONTRAINDICATES--> ICD:N18.3 (Chronic Kidney Disease)
```

Now branching becomes powerful: fork to `treatment-plan-b`, swap `TREATED_BY` to a different medication, and compare the resulting contraindication subgraphs — all without touching production data.

### Other Use Cases

| Domain | What Gets Linked | Why Branching Matters |
|--------|-----------------|----------------------|
| Social media | User profiles (JSON) ↔ posts (KV) ↔ post embeddings (Vector) ↔ interactions (Event) | A/B test moderation policies on forked content graphs |
| Data lineage | Source datasets → transformations → derived outputs, each bound to real data | Fork to test a pipeline change, diff the lineage graphs |
| Agent reasoning | Hypothesis → evidence → refinement chains across LLM steps | Fork to explore alternative reasoning paths |
| Dependency modeling | Services, packages, or tasks with typed dependencies | Branch to simulate removing a dependency |

---

# 2. Goals

## Primary Goals

1. Enable directed, typed relationships between any combination of Strata primitives and arbitrary nodes.
2. Inherit branch isolation, versioning, time-travel, and merge semantics from KV — zero additional infrastructure.
3. Provide efficient single-hop adjacency queries and bounded traversals (BFS) without external dependencies.
4. Support an extensibility model (`GraphAlgorithm` trait) that allows adding algorithms without modifying storage.
5. Expose a Pythonic API that follows the existing Namespace + Handle pattern (`db.graphs`).

## Non-Goals

* **Not a graph database.** No query language (Cypher, Gremlin, SPARQL). No query optimizer.
* **Not a graph compute engine.** No PageRank, community detection, or shortest-path in v1. Delegate to export.
* **No new primitive.** No new `TypeTag`, no new storage backend. Edges are KV entries.
* **No schema enforcement.** Edge types are strings, not compile-time traits. This is required for Python dynamism.
* **No distributed traversal.** All operations are local to a single Strata instance.

---

# 3. Design Principles

### 3.1 Build On KV, Don't Compete With It

Every graph operation reduces to KV reads and writes. The graph module is a **semantic layer** — key construction helpers + traversal logic — not a storage engine. If KV gets faster, graphs get faster.

### 3.2 Cross-Primitive Linking Is the Value Proposition

A typed KV wrapper that only links strings is ~100 lines of user code. The real value is `EntityRef` integration: a node can be bound to `kv://main/lab:4821:HbA1c`, `json://main/patient-4821`, `vector://main/clinical-notes/note-1137`, or `event://main/50234`, enabling queries like "what lab results, clinical notes, and events are connected to this patient's diagnosis?"

### 3.3 Branch Awareness Is Free, Not Bolted On

Because edges are KV entries, they automatically participate in branching:
- **Fork** → edges are logically copied (CoW)
- **Merge** → edges reconcile via existing KV merge semantics
- **Time-travel** → `as_of` queries work on graph state
- **Diff** → edge changes are visible in branch diffs

### 3.4 Extensible Without Rewriting

The `GraphAlgorithm` trait + `GraphSnapshot` pattern means future algorithms (PageRank, connected components, centrality) are pure functions over an in-memory snapshot. They never touch storage directly, so adding them requires zero changes to the graph module's core.

---

# 4. Architecture

## 4.1 Storage Model: Edges as Dual-Indexed KV Entries

Each edge produces **two KV entries** (forward + reverse) to enable efficient adjacency queries in both directions via prefix scan. Node metadata and graph configuration are stored as additional KV entries.

### Key Schema

```
Forward:  _graph_/{graph_name}/e/{src}/{edge_type}/{dst}  → edge payload (JSON)
Reverse:  _graph_/{graph_name}/r/{dst}/{edge_type}/{src}  → edge payload (JSON)
Node:     _graph_/{graph_name}/n/{node_id}                → node metadata (JSON)
Meta:     _graph_/{graph_name}/__meta__                   → graph config (JSON)
```

The `_graph_/` prefix follows the existing convention of system-internal key prefixes (like `_system_` for vector collections), keeping graph data in the same KV namespace but distinguishable from user data.

### Why Dual Indexes

| Query | Key Prefix Scanned | Complexity |
|-------|--------------------|------------|
| Outgoing neighbors of `src` | `_graph_/{g}/e/{src}/` | O(out-degree) |
| Outgoing neighbors of `src` with type `T` | `_graph_/{g}/e/{src}/{T}/` | O(edges of type T) |
| Incoming neighbors of `dst` | `_graph_/{g}/r/{dst}/` | O(in-degree) |
| Incoming neighbors of `dst` with type `T` | `_graph_/{g}/r/{dst}/{T}/` | O(edges of type T) |
| All edges in graph | `_graph_/{g}/e/` | O(total edges) |
| All nodes in graph | `_graph_/{g}/n/` | O(total nodes) |
| Degree of node (outgoing) | Count of `_graph_/{g}/e/{node}/` | O(out-degree) |

Every adjacency query is a single prefix scan. No joins, no secondary indexes, no additional data structures.

### Edge Payload

```json
{
  "weight": 0.9,
  "properties": {
    "confidence": 0.85,
    "source": "model-v3"
  }
}
```

Both forward and reverse entries store identical payloads. The duplication cost is minimal (edge payloads are typically small) and avoids a cross-reference lookup on every read.

---

## 4.2 Node Model: String IDs with Optional EntityRef Binding

Nodes are identified by **opaque string IDs**. This keeps the API simple — users don't need to construct `EntityRef` objects for basic graph operations.

Optionally, a node can be **bound** to an `EntityRef`, stored as a field in node metadata. This enables cross-primitive linking without forcing it.

### Node Metadata

```json
{
  "entity_ref": "json://main/patient-4821",
  "properties": {
    "label": "Patient Record",
    "department": "endocrinology"
  }
}
```

The `entity_ref` field is optional. When present, it establishes a semantic link between the graph node and a concrete Strata object (KV entry, JSON document, event, vector, state cell, or branch).

### EntityRef Variants (from `crates/core/src/contract/entity_ref.rs`)

| Variant | URI Format | Example |
|---------|------------|---------|
| `Kv` | `kv://{branch}/{key}` | `kv://main/user:alice` |
| `Json` | `json://{branch}/{doc_id}` | `json://main/doc-42` |
| `Event` | `event://{branch}/{sequence}` | `event://main/1042` |
| `Vector` | `vector://{branch}/{collection}/{key}` | `vector://main/embeddings/vec-7` |
| `State` | `state://{branch}/{name}` | `state://main/counter` |
| `Branch` | `branch://{branch}` | `branch://experiment-3` |

### Why Optional Binding

| Scenario | EntityRef Used? | Example |
|----------|-----------------|---------|
| Abstract concepts | No — nodes are identifiers | `"ICD:E11.9"` (diabetes diagnosis code), `"med:metformin"` |
| Concrete data | Yes — nodes represent stored data | `json://main/patient-4821`, `kv://main/lab:4821:HbA1c:2026-01` |
| Mixed graph | Some bound, some not | Patient records (bound to JSON) linked to diagnosis codes (unbound) linked to lab results (bound to KV) |

---

## 4.3 Edge Model: String-Typed, Directed, Weighted

Edges are **directed** and **string-typed**. String types (not compile-time generics) are required for the Python API, where edge types are dynamic values.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `src` | `String` | Yes | Source node ID |
| `dst` | `String` | Yes | Destination node ID |
| `edge_type` | `String` | Yes | Semantic relationship label |
| `weight` | `f64` | No | Numeric weight (default 1.0) |
| `properties` | `Map<String, Value>` | No | Arbitrary key-value metadata |

### Edge Type Conventions

Edge types are user-defined strings. Examples by domain:

```
Healthcare:  DIAGNOSED_WITH, TREATED_BY, CONTRAINDICATES, HAS_LAB_RESULT,
             REFERRED_TO, SUPPORTS, MENTIONS, PRESCRIBED

Social:      FOLLOWS, POSTED, LIKED, REPLIED_TO, SHARED, REPORTED,
             SIMILAR_TO, BLOCKED_BY

General:     DEPENDS_ON, DERIVED_FROM, CONTAINS, PRECEDES
```

No registry or pre-declaration required — any string is valid.

---

## 4.4 Traversal: Enough to Be Useful, Not a Graph Engine

v1 provides the traversals that reduce to prefix scans and iterative neighbor calls. Anything requiring global graph knowledge (shortest path, PageRank) is deferred to `GraphSnapshot` + export.

### v1 Traversals

| Operation | Description | Implementation |
|-----------|-------------|----------------|
| **Neighbors** | Single-hop, directional, filterable by edge type | One prefix scan |
| **BFS** | Bounded depth, max_nodes cap, edge type filter | Iterative neighbor calls |
| **Reverse traversal** | Incoming edges/neighbors | Reverse index prefix scan |
| **Subgraph extraction** | Edges induced by a node set | Filter edge scan by node membership |
| **Degree count** | In-degree, out-degree, total | Prefix scan count (no materialization) |

### BFS Options

```rust
pub struct BfsOptions {
    pub max_depth: usize,            // Stop at this depth (required)
    pub max_nodes: Option<usize>,    // Cap total visited nodes
    pub edge_types: Option<Vec<String>>,  // Filter to these edge types
    pub direction: Direction,        // Outgoing, Incoming, Both
}
```

### Not in v1

| Operation | Why Deferred |
|-----------|-------------|
| Shortest path | Requires priority queue + global state, better in export |
| PageRank | Iterative matrix computation, not a storage concern |
| Community detection | Global clustering, export to NetworkX/igraph |
| Connected components | Union-find over full graph, export path |
| Pattern matching | Subgraph isomorphism is NP-hard in general |

---

## 4.5 Extensibility: GraphSnapshot + GraphAlgorithm Trait

The extensibility model separates **storage** (KV-backed `GraphStore`) from **computation** (`GraphSnapshot` + algorithms). This means future algorithms never touch the storage layer.

### GraphSnapshot

A full in-memory materialization of a graph at a point in time:

```rust
pub struct GraphSnapshot {
    /// Node ID → node metadata
    pub nodes: HashMap<String, NodeData>,

    /// Source → Vec<(destination, edge_type, edge_data)>
    pub outgoing: HashMap<String, Vec<(String, String, EdgeData)>>,

    /// Destination → Vec<(source, edge_type, edge_data)>
    pub incoming: HashMap<String, Vec<(String, String, EdgeData)>>,
}
```

### GraphAlgorithm Trait

```rust
pub trait GraphAlgorithm {
    type Output;
    fn execute(&self, snapshot: &GraphSnapshot) -> Result<Self::Output>;
}
```

Future algorithms implement this trait:

```rust
// Example: future PageRank implementation
pub struct PageRank {
    pub damping: f64,
    pub iterations: usize,
}

impl GraphAlgorithm for PageRank {
    type Output = HashMap<String, f64>;
    fn execute(&self, snapshot: &GraphSnapshot) -> Result<Self::Output> {
        // Pure computation over snapshot — no storage access
    }
}
```

### Export Methods on GraphSnapshot

```rust
impl GraphSnapshot {
    /// Returns Vec<(src, dst, edge_type, weight)>
    pub fn to_edge_list(&self) -> Vec<(String, String, String, f64)>;

    /// Returns HashMap<node_id, Vec<(neighbor_id, edge_type, weight)>>
    pub fn to_adjacency_list(&self) -> HashMap<String, Vec<(String, String, f64)>>;

    /// Returns (nodes_csv, edges_csv) for external tool import
    pub fn to_csv(&self) -> (String, String);
}
```

These enable integration with NetworkX, igraph, PyG, or any external graph tool without Strata runtime dependencies.

---

## 4.6 Referential Integrity: Cascade on Entity Deletion

When a node is bound to an `EntityRef` and the underlying entity is deleted (e.g., `db.json.delete("patient-4821")`), the graph should react. Without this, users must manually track and clean up dangling references — the exact kind of cross-primitive bookkeeping that `strata-graph` exists to eliminate.

### Mechanism: Delete Hook (Follows embed_hook Pattern)

The executor's write handlers (`kv_delete`, `json_delete`, etc.) already call best-effort hooks after mutations (see `embed_hook.rs`). A `graph_integrity_hook` follows the same pattern:

```rust
// Called from executor write handlers after a delete completes
pub fn on_entity_deleted(p: &Arc<Primitives>, branch: &BranchId, entity_ref: &EntityRef) {
    // 1. Look up reverse EntityRef index (see §4.7) for nodes bound to this entity
    // 2. Apply the graph's configured cascade policy
    // 3. Best-effort: log warnings on failure, never block the delete
}
```

### Cascade Policies (Per-Graph, Stored in `__meta__`)

| Policy | Behavior | Use Case |
|--------|----------|----------|
| `cascade` | Delete the node and all incident edges | Cleanup-oriented graphs (temp reasoning traces) |
| `detach` | Clear the `entity_ref` field, keep the node and edges | Lineage graphs where the relationship structure matters more than the data |
| `ignore` | Do nothing (default) | Graphs that are managed externally or don't need integrity |

```json
// _graph_/patient_care/__meta__
{
  "description": "Patient care relationships",
  "on_entity_delete": "detach"
}
```

### Why Best-Effort

Following the embed_hook precedent: integrity hooks log warnings but never propagate errors to the caller. A `kv.delete()` must never fail because a graph hook encountered an issue. The graph module is an observer of primitive mutations, not a gatekeeper.

---

## 4.7 Reverse EntityRef Index

The forward direction — "given a graph node, what entity is it bound to?" — is a single KV read of node metadata. The reverse direction — "given an entity, what graph nodes reference it?" — requires an index.

### Key Schema

```
_graph_/__ref__/{entity_ref_uri}/{graph_name}/{node_id} → ""
```

Examples:
```
_graph_/__ref__/json://main/patient-4821/patient_care/patient-4821  → ""
_graph_/__ref__/kv://main/lab:4821:HbA1c:2026-01/patient_care/lab:HbA1c  → ""
```

### Maintenance

The index is maintained automatically by `GraphStore`:
- **`add_node` with `entity_ref`**: write a ref index entry
- **`remove_node`**: delete the ref index entry (if the node had an `entity_ref`)
- **Update node's `entity_ref`**: delete old entry, write new entry

### Queries Enabled

```rust
impl GraphStore<'_> {
    /// Find all graph nodes (across all graphs) bound to this entity.
    pub fn nodes_for_entity(
        &self, branch: &BranchId, entity_ref: &EntityRef
    ) -> Result<Vec<(String, String)>>;  // Vec<(graph_name, node_id)>
}
```

This is what makes the referential integrity hook (§4.6) efficient: instead of scanning all nodes in all graphs, `on_entity_deleted` does one prefix scan on `_graph_/__ref__/{entity_ref_uri}/`.

### Cost

One additional KV write per node that has an `entity_ref` (at `add_node` time). Nodes without `entity_ref` incur zero overhead.

---

## 4.8 Graph-Boosted Search

Search results become more relevant when they account for how entities are related, not just how well they match a query. A lab result that is 2 hops from a patient's active diagnosis should rank higher than an unrelated lab result with the same BM25 score.

### Mechanism: Post-Fusion Score Injection

The search pipeline (§`crates/engine/src/search/`) produces ranked results in three stages:

1. **Per-primitive BM25 scoring** — each primitive scores candidates independently
2. **Cross-primitive RRF fusion** — `HybridSearch` merges results via Reciprocal Rank Fusion
3. **Optional LLM re-ranking** — an LLM-based re-ranker adjusts final positions

Graph boosting injects **between stages 2 and 3** — after RRF has normalized scores into a single list, but before the optional re-ranker. This is the natural extension point because RRF scores are already comparable across primitives and every hit carries its `EntityRef`.

### Scoring Formula

```
boosted_score = rrf_score * (1.0 + graph_weight * proximity_score)
```

Where `proximity_score` is derived from the shortest graph distance between the hit's `EntityRef` and a set of **anchor nodes** provided by the caller:

| Distance | Proximity Score |
|----------|----------------|
| 0 (is anchor) | 1.0 |
| 1 hop | 0.5 |
| 2 hops | 0.25 |
| 3+ hops or unconnected | 0.0 |

`graph_weight` is a tunable parameter (default 0.3) — enough to break ties and promote connected results without overwhelming text relevance.

### API Surface

```rust
// Added to SearchRequest
pub struct GraphBoost {
    pub graph_name: String,
    pub anchor_nodes: Vec<String>,    // node IDs to boost proximity to
    pub max_hops: usize,              // default 2
    pub weight: f32,                  // default 0.3
}
```

```python
# Python API
results = db.search("HbA1c levels", graph_boost={
    "graph": "patient_care",
    "anchors": ["patient-4821"],
    "max_hops": 2,
    "weight": 0.3,
})
```

### Implementation: Materialized Adjacency (§4.9) Required

Computing graph proximity per search hit requires fast traversal. The materialized adjacency index makes this viable — a 2-hop BFS from anchor nodes over an in-memory adjacency map is microseconds, not milliseconds per KV prefix scan.

### Why This Matters

Without graph boosting, search treats every entity as independent. With it, Strata becomes a system where *what you've connected influences what you find*. This is the kind of integration users cannot build by layering a graph convention on top of KV — it requires deep coupling between the graph and search modules.

---

## 4.9 Materialized Adjacency Index

KV prefix scans are efficient for single-hop neighbor queries, but multi-hop traversals (BFS, graph-boosted search) compound scan latency per hop. A materialized in-memory adjacency index makes traversal O(1) per hop.

### Design: Database Extension (Follows VectorBackendState Pattern)

The adjacency index is stored as a `Database` extension via `db.extension::<AdjacencyIndex>()`, following the same pattern used by `VectorBackendState` (vector index registry) and `InvertedIndex` (BM25 search index).

```rust
pub struct AdjacencyIndex {
    /// graph_name → in-memory adjacency representation
    graphs: DashMap<String, GraphAdjacency>,
    /// Track which graphs have been loaded
    loaded: DashMap<String, bool>,
}

pub struct GraphAdjacency {
    /// node_id → Vec<(neighbor_id, edge_type, weight)>
    outgoing: HashMap<String, Vec<(String, String, f64)>>,
    /// node_id → Vec<(neighbor_id, edge_type, weight)>
    incoming: HashMap<String, Vec<(String, String, f64)>>,
    /// node_id → NodeData (including entity_ref)
    nodes: HashMap<String, NodeData>,
}

impl Default for AdjacencyIndex {
    fn default() -> Self {
        Self {
            graphs: DashMap::new(),
            loaded: DashMap::new(),
        }
    }
}
```

### Lifecycle

| Phase | Behavior |
|-------|----------|
| **First access** | `db.extension::<AdjacencyIndex>()` creates an empty index via `Default` |
| **First query on graph G** | Full prefix scan of `_graph_/{G}/` to populate `GraphAdjacency`. Cached for subsequent queries. |
| **Writes** | `add_node`, `add_edge`, `remove_node`, `remove_edge` update both KV (source of truth) and the in-memory index (if loaded). KV-first — if the KV write fails, the in-memory index is not updated. |
| **Recovery** | A `RecoveryParticipant` (like `register_search_recovery()`) rebuilds the index from KV at startup for any graphs that were loaded in the previous session. The graphs to recover are tracked in a manifest KV entry `_graph_/__loaded__`. |
| **Branch fork** | The in-memory index is per-branch. Forking creates a CoW snapshot in KV; the adjacency index for the child branch is lazily populated on first query. |

### Performance Comparison

| Operation | KV-Only | Materialized |
|-----------|---------|-------------|
| Single-hop neighbors | ~50-200 µs (prefix scan) | ~0.1 µs (HashMap lookup) |
| 3-hop BFS (degree 20) | ~400 ms (8K prefix scans) | ~0.5 ms (8K HashMap lookups) |
| Graph-boosted search (2-hop, 10 anchors) | Not viable | ~0.1 ms |
| Memory overhead per graph | 0 | ~100 bytes/edge + ~200 bytes/node |

### Consistency Model

KV is the source of truth. The in-memory index is a cache:
- **Writes go to KV first**, then update the cache. If the process crashes between KV write and cache update, recovery rebuilds from KV.
- **Reads hit the cache** when loaded, falling back to KV prefix scans when the cache is cold.
- **No dirty reads** — the cache is updated synchronously after KV commit, within the same thread.

---

# 5. Module Structure

The graph module lives in `crates/engine/src/graph/`, following the same pattern as `crates/engine/src/search/`.

```
crates/engine/src/graph/
├── mod.rs          — GraphStore facade, public re-exports
├── types.rs        — NodeId, NodeData, Edge, EdgeData, Direction, BfsOptions, GraphBoost
├── keys.rs         — Key construction/parsing for forward/reverse/node/meta/ref-index
├── traversal.rs    — BFS, subgraph extraction, degree counting
├── snapshot.rs     — GraphSnapshot, GraphAlgorithm trait, export methods
├── adjacency.rs    — AdjacencyIndex (Database extension), GraphAdjacency, recovery participant
├── integrity.rs    — Referential integrity hook, cascade policies
├── ref_index.rs    — Reverse EntityRef index (nodes_for_entity queries)
├── boost.rs        — Graph-boosted search scoring (post-fusion injection)
```

Plus the executor-side hook:
```
crates/executor/src/handlers/graph_integrity_hook.rs  — on_entity_deleted, called from delete handlers
```

### GraphStore (Facade)

```rust
pub struct GraphStore<'a> {
    kv: &'a KvPrimitive,
    db: &'a Database,       // for AdjacencyIndex extension access
    graph_name: String,
}

impl<'a> GraphStore<'a> {
    // Node operations
    pub fn add_node(&self, branch: &BranchId, id: &str, data: NodeData) -> Result<()>;
    pub fn get_node(&self, branch: &BranchId, id: &str) -> Result<Option<NodeData>>;
    pub fn remove_node(&self, branch: &BranchId, id: &str) -> Result<()>;
    pub fn list_nodes(&self, branch: &BranchId) -> Result<Vec<String>>;

    // Edge operations
    pub fn add_edge(&self, branch: &BranchId, edge: &Edge) -> Result<()>;
    pub fn get_edge(&self, branch: &BranchId, src: &str, edge_type: &str, dst: &str) -> Result<Option<EdgeData>>;
    pub fn remove_edge(&self, branch: &BranchId, src: &str, edge_type: &str, dst: &str) -> Result<()>;

    // Query operations (use AdjacencyIndex when loaded, fall back to KV prefix scan)
    pub fn neighbors(&self, branch: &BranchId, node: &str, direction: Direction, edge_type: Option<&str>) -> Result<Vec<Neighbor>>;
    pub fn degree(&self, branch: &BranchId, node: &str, direction: Direction) -> Result<usize>;

    // Traversal
    pub fn bfs(&self, branch: &BranchId, start: &str, options: &BfsOptions) -> Result<BfsResult>;
    pub fn subgraph(&self, branch: &BranchId, node_ids: &[&str]) -> Result<GraphSnapshot>;

    // Snapshot
    pub fn snapshot(&self, branch: &BranchId) -> Result<GraphSnapshot>;

    // Reverse EntityRef index (§4.7)
    pub fn nodes_for_entity(&self, branch: &BranchId, entity_ref: &EntityRef) -> Result<Vec<(String, String)>>;
}
```

---

# 6. Python API

Follows the existing **Namespace + Handle** pattern used by `db.kv`, `db.vectors`, etc.

### Namespace: `db.graphs`

```python
class GraphsNamespace:
    """Namespace for graph operations: db.graphs.create(), db.graphs.get(), etc."""
    __slots__ = ("_db",)

    def __init__(self, db):
        self._db = db

    def create(self, name, *, description=None, on_entity_delete="ignore"):
        """Create a named graph, returns a GraphHandle.

        on_entity_delete: 'cascade' | 'detach' | 'ignore' (default)
        """
        self._db.graph_create(name, description=description,
                              on_entity_delete=on_entity_delete)
        return GraphHandle(self._db, name)

    def get(self, name):
        """Get an existing graph by name, returns a GraphHandle."""
        return GraphHandle(self._db, name)

    def list(self):
        """List all graph names."""
        return self._db.graph_list()

    def delete(self, name):
        """Delete a graph and all its nodes/edges."""
        return self._db.graph_delete(name)
```

### Handle: `GraphHandle`

```python
class GraphHandle:
    """Handle for operating on a specific named graph."""
    __slots__ = ("_db", "_name")

    def __init__(self, db, name):
        self._db = db
        self._name = name

    # ---- Nodes ----
    def add_node(self, node_id, *, entity_ref=None, properties=None):
        """Add a node with optional EntityRef binding and properties."""
        return self._db.graph_add_node(self._name, node_id,
                                        entity_ref=entity_ref,
                                        properties=properties)

    def get_node(self, node_id):
        """Get node metadata."""
        return self._db.graph_get_node(self._name, node_id)

    def remove_node(self, node_id):
        """Remove a node and its incident edges."""
        return self._db.graph_remove_node(self._name, node_id)

    def nodes(self):
        """List all node IDs."""
        return self._db.graph_list_nodes(self._name)

    # ---- Edges ----
    def add_edge(self, src, dst, edge_type, *, weight=None, properties=None):
        """Add a directed edge between two nodes."""
        return self._db.graph_add_edge(self._name, src, dst, edge_type,
                                        weight=weight, properties=properties)

    def remove_edge(self, src, dst, edge_type):
        """Remove a specific edge."""
        return self._db.graph_remove_edge(self._name, src, dst, edge_type)

    # ---- Queries ----
    def neighbors(self, node_id, *, direction="outgoing", edge_type=None):
        """Single-hop neighbors with optional direction and type filter."""
        return self._db.graph_neighbors(self._name, node_id,
                                         direction=direction,
                                         edge_type=edge_type)

    def degree(self, node_id, *, direction="both"):
        """Count edges for a node."""
        return self._db.graph_degree(self._name, node_id, direction=direction)

    # ---- Traversal ----
    def bfs(self, start, *, max_depth=3, max_nodes=None,
            edge_types=None, direction="outgoing"):
        """Bounded breadth-first search."""
        return self._db.graph_bfs(self._name, start,
                                   max_depth=max_depth,
                                   max_nodes=max_nodes,
                                   edge_types=edge_types,
                                   direction=direction)

    def subgraph(self, node_ids):
        """Extract edges induced by a set of nodes."""
        return self._db.graph_subgraph(self._name, node_ids)

    # ---- Reverse EntityRef lookup ----
    def nodes_for_entity(self, entity_ref):
        """Find all nodes in this graph bound to an entity.

        Returns list of node IDs.
        """
        return self._db.graph_nodes_for_entity(self._name, entity_ref)

    # ---- Export ----
    def to_edge_list(self):
        """Export as list of (src, dst, type, weight) tuples."""
        return self._db.graph_to_edge_list(self._name)

    def to_adjacency_list(self):
        """Export as dict of node_id → [(neighbor, type, weight)]."""
        return self._db.graph_to_adjacency_list(self._name)
```

### Usage Example: Patient Care Graph

```python
import stratadb

db = stratadb.open("./clinic-db")

# --- Build a patient care graph linking across all primitives ---

graph = db.graphs.create("patient_care")

# Patient record lives in JSON store
db.json.put("patient-4821", {"name": "Jane Doe", "dob": "1974-03-15", "mrn": "4821"})
graph.add_node("patient-4821", entity_ref="json://main/patient-4821",
               properties={"department": "endocrinology"})

# Lab results live in KV store
db.kv.put("lab:4821:HbA1c:2026-01", "8.1%")
db.kv.put("lab:4821:eGFR:2026-01", "52 mL/min")
graph.add_node("lab:HbA1c", entity_ref="kv://main/lab:4821:HbA1c:2026-01")
graph.add_node("lab:eGFR", entity_ref="kv://main/lab:4821:eGFR:2026-01")

# Clinical note embedded for similarity search
graph.add_node("note-1137", entity_ref="json://main/note-1137",
               properties={"type": "progress_note", "date": "2026-01-20"})

# Diagnosis codes are standalone nodes (no backing data)
graph.add_node("ICD:E11.9", properties={"description": "Type 2 Diabetes Mellitus"})
graph.add_node("ICD:N18.3", properties={"description": "CKD Stage 3"})

# Medications
graph.add_node("med:metformin")
graph.add_node("med:empagliflozin")

# --- Wire up relationships ---

graph.add_edge("patient-4821", "ICD:E11.9", "DIAGNOSED_WITH")
graph.add_edge("patient-4821", "lab:HbA1c", "HAS_LAB_RESULT")
graph.add_edge("patient-4821", "lab:eGFR", "HAS_LAB_RESULT")
graph.add_edge("lab:HbA1c", "ICD:E11.9", "SUPPORTS", weight=0.95)
graph.add_edge("note-1137", "ICD:E11.9", "MENTIONS")
graph.add_edge("ICD:E11.9", "med:metformin", "TREATED_BY")
graph.add_edge("med:metformin", "ICD:N18.3", "CONTRAINDICATES", weight=0.8)
graph.add_edge("patient-4821", "ICD:N18.3", "DIAGNOSED_WITH")

# --- Query: what supports the diabetes diagnosis? ---

supporters = graph.neighbors("ICD:E11.9", direction="incoming", edge_type="SUPPORTS")
# → [{"node_id": "lab:HbA1c", "weight": 0.95, ...}]

# --- Query: contraindication check (2-hop from medication) ---

risks = graph.bfs("med:metformin", max_depth=2, edge_types=["CONTRAINDICATES"],
                   direction="outgoing")
# → {"visited": ["med:metformin", "ICD:N18.3"], ...}
# Patient has CKD — metformin may be contraindicated.

# --- Branch: explore alternative treatment plan ---

db.branches.create("treatment-plan-b", parent="main")
alt = db.graphs.get("patient_care")  # same graph, branched state

# Remove metformin, add empagliflozin (renal-safe SGLT2 inhibitor)
alt.remove_edge("ICD:E11.9", "med:metformin", "TREATED_BY")
alt.add_edge("ICD:E11.9", "med:empagliflozin", "TREATED_BY")
# This change is isolated to treatment-plan-b — main branch is untouched.

# --- Time-travel: what did the care graph look like last month? ---
# (free from KV as_of semantics — no graph-specific code needed)

# --- Export for clinical analytics ---

edges = graph.to_edge_list()
# → [("patient-4821", "ICD:E11.9", "DIAGNOSED_WITH", 1.0),
#     ("lab:HbA1c", "ICD:E11.9", "SUPPORTS", 0.95), ...]
# Feed into NetworkX for centrality analysis, PyG for GNN training, etc.
```

### Referential Integrity in Action

```python
# Create a graph with cascade policy
graph = db.graphs.create("trial_data", on_entity_delete="cascade")
graph.add_node("sample-A", entity_ref="json://main/sample-A")
graph.add_edge("sample-A", "result-1", "PRODUCED")

# When the underlying entity is deleted, the node and edges are cleaned up
db.json.delete("sample-A")
# → graph_integrity_hook fires
# → node "sample-A" and its PRODUCED edge are cascade-deleted from the graph
```

### Reverse EntityRef Lookup

```python
# "What graph nodes reference this patient record?"
refs = graph.nodes_for_entity("json://main/patient-4821")
# → [("patient_care", "patient-4821")]
# Works across all graphs — one prefix scan on the reverse index
```

### Graph-Boosted Search

```python
# Search with graph proximity boosting
results = db.search("HbA1c levels", graph_boost={
    "graph": "patient_care",
    "anchors": ["patient-4821"],   # boost results connected to this patient
    "max_hops": 2,
    "weight": 0.3,
})
# Lab results and clinical notes connected to patient-4821 rank higher
# than identical-scoring results for unrelated patients
```

This example exercises every differentiator: cross-primitive linking (JSON patient record ↔ KV lab results ↔ standalone diagnosis codes), branching for treatment alternatives, traversal for contraindication detection, referential integrity on deletion, reverse entity lookup, graph-boosted search, and export for external analytics.

---

# 7. Performance Characteristics

| Operation | Complexity | Mechanism |
|-----------|-----------|-----------|
| Add node | O(1) | KV put + ref index put (if entity_ref) + cache update |
| Add edge | O(1) | Two KV puts (forward + reverse) + cache update |
| Remove edge | O(1) | Two KV deletes + cache update |
| Neighbors (cold) | O(degree) | KV prefix scan |
| Neighbors (warm) | O(1) | AdjacencyIndex HashMap lookup |
| BFS (cold) | O(visited × avg_degree) | Iterative KV prefix scans |
| BFS (warm) | O(visited) | In-memory traversal |
| Degree count | O(degree) cold, O(1) warm | Prefix scan or cached Vec length |
| Snapshot (full load) | O(V + E) | Full prefix scan |
| Reverse entity lookup | O(graphs × nodes per entity) | Ref index prefix scan |
| Graph-boosted search | O(anchors × degree^hops) | BFS over AdjacencyIndex + score multiply |
| Branch fork | O(1) | KV copy-on-write (adjacency cache lazily rebuilt) |

### Storage Overhead

Each edge consumes two KV entries (forward + reverse). For a graph with E edges:
- Key storage: ~2 × E × (prefix_len + src_len + type_len + dst_len) bytes
- Value storage: ~2 × E × payload_size bytes

For typical edge payloads (weight + 2-3 properties): ~200-400 bytes per edge total.

---

# 8. Branch Semantics (Inherited from KV)

All graph operations occur within a branch context. No special graph-level branching logic is needed.

| Branch Operation | Graph Behavior |
|-----------------|----------------|
| **Fork** | New branch sees parent's graph state (CoW). Modifications are isolated. |
| **Merge** | Edges follow KV merge semantics (last-writer-wins per key). |
| **Time-travel** | `as_of` timestamp on KV scans returns historical graph state. |
| **Diff** | KV diff on `_graph_/` prefix shows added/removed/modified edges. |

### Cross-Branch Queries

Not in v1. Each graph query operates within a single branch. Comparing graphs across branches is done via diff or by taking snapshots of each branch and comparing in user code.

---

# 9. Integration Points

### With EntityRef (`crates/core/src/contract/entity_ref.rs`)

Nodes optionally bind to `EntityRef` variants, enabling queries like:
- "What diagnoses and notes reference this patient's JSON record?"
- "Which diagnosis codes are supported by this lab result KV entry?"
- "What clinical notes (vectors) are semantically similar to this admission event?"

### With Search (`crates/engine/src/search/`)

Graph-boosted search (§4.8) injects proximity scores into the search pipeline after RRF fusion. The `GraphBoost` struct is passed via `SearchRequest` and applied in `HybridSearch` before returning results. This couples the graph and search modules at the scoring level — the deepest integration point in `strata-graph`.

### With KV Primitive (`crates/engine/src/primitives/kv.rs`)

Direct dependency. `GraphStore` holds a reference to `KvPrimitive` and delegates all storage operations to it. Uses `kv.put()`, `kv.get()`, `kv.delete()`, and `kv.list()` (prefix scan).

### With Database Extensions (`crates/engine/src/database/mod.rs`)

The `AdjacencyIndex` (§4.9) is stored as a `Database` extension via `db.extension::<AdjacencyIndex>()`, following the same pattern as `VectorBackendState` and `InvertedIndex`. A `RecoveryParticipant` rebuilds it from KV at startup.

### With Executor Write Handlers (`crates/executor/src/handlers/`)

The referential integrity hook (§4.6) is called from executor delete handlers (`kv_delete`, `json_delete`, etc.), following the same best-effort pattern as `embed_hook.rs`. The hook consults the reverse EntityRef index and applies the graph's cascade policy.

---

# 10. Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Scope creep toward graph DB | Strict non-goals, `GraphAlgorithm` trait for extension without core changes |
| Large graphs slow prefix scans | `AdjacencyIndex` makes warm queries O(1); cold queries fall back to prefix scan with `max_nodes` cap |
| Dual-index write amplification | Edge payloads are small; 2× writes is acceptable for O(1) directional lookups |
| Key collision with user data | `_graph_/` prefix is reserved; validate in `kv.put()` or document as convention |
| Node removal orphans edges | `remove_node` scans and deletes incident edges; `AdjacencyIndex` makes this O(degree) in-memory |
| Integrity hook slows deletes | Best-effort (log + continue); reverse EntityRef index makes lookup O(1), not O(all nodes) |
| AdjacencyIndex memory pressure | Lazy per-graph loading; graphs not queried are never materialized; document size limits |
| Graph boost distorts search relevance | Conservative default weight (0.3); multiplicative not additive; user-tunable; opt-in per query |
| Stale adjacency cache after crash | KV is source of truth; `RecoveryParticipant` rebuilds from KV at startup |
| Users expect algorithms | Clear v1 scope, export methods for external tools, `GraphAlgorithm` for future built-ins |

---

# 11. Success Criteria

`strata-graph` is successful if:

1. Users can model patient care graphs, social content networks, and data lineage DAGs without external infrastructure.
2. Cross-primitive relationships (KV ↔ JSON ↔ Vector ↔ Event) are expressible via `EntityRef` binding, with reverse lookup.
3. Deleting a bound entity triggers automatic cleanup (cascade/detach) — users never manually chase dangling references.
4. Search results improve measurably when graph context is provided via `graph_boost`.
5. Multi-hop traversals (BFS, graph-boosted search) complete in <1ms for warm graphs via the materialized adjacency index.
6. Branch-aware graph operations work identically to other Strata primitives — no surprises.
7. Adding a new algorithm (e.g., PageRank) requires implementing one trait, zero storage changes.
8. Python API is indistinguishable in style from `db.kv` and `db.vectors`.

---

# 12. Future Considerations (Not in v1)

| Feature | Prerequisite | Notes |
|---------|-------------|-------|
| PageRank / centrality | `GraphAlgorithm` trait | Pure computation over `GraphSnapshot` |
| Connected components | `GraphAlgorithm` trait | Union-find over snapshot |
| Shortest path | `GraphAlgorithm` trait | Dijkstra/BFS over snapshot |
| Graph-aware merge | Custom merge strategy | Detect conflicting edge modifications |
| Cross-branch graph diff | Branch diff API | Semantic diff of graph structure |
| Full-text search on graph metadata | Search integration | Index node/edge properties |
| Temporal graph queries | Time-travel API | "Show graph as it was at timestamp T" (partially free from KV `as_of`) |
| Hyperedges | Schema extension | Edges connecting >2 nodes |
| Graph ML export (PyG format) | Export methods | `to_pyg()` for PyTorch Geometric |

These are intentionally deferred. The `GraphAlgorithm` trait and `GraphSnapshot` pattern ensure they can be added without rewriting the storage or traversal layers.

---

**Summary**

`strata-graph` is a **cross-primitive relationship layer**, not a graph database. It provides four capabilities users cannot build by layering a key convention on top of KV:

1. **Referential integrity** — automatic cascade/detach when bound entities are deleted, via hooks into every primitive's delete path.
2. **Reverse EntityRef index** — O(1) lookup of "what graph nodes reference this entity?", maintained automatically.
3. **Graph-boosted search** — search results rank higher when they're graph-connected to user-specified anchors, injected post-RRF fusion.
4. **Materialized adjacency** — in-memory graph index (Database extension) that makes multi-hop traversals sub-millisecond, with KV as source of truth and recovery on restart.

These sit on top of the foundational design: dual-indexed KV edges for storage, `EntityRef` binding for cross-primitive linking, branch isolation inherited for free, and `GraphAlgorithm` + `GraphSnapshot` for extensibility. The Python API follows existing Namespace + Handle patterns so graphs feel native to Strata users.