# strata-graph: Implementation Stories

**PRD:** `docs/architecture/strata-graph.md`
**Scope:** Phase 1 — Graph layer only. Ontology (object types, link types, schema validation) is Phase 2.
**Methodology:** Test-Driven Development. Every story starts with tests that fail, then implementation to make them pass.

---

## Architecture: Why This Works

The graph module adds **zero new infrastructure**. It is a semantic layer inside `strata-engine` — key construction helpers and traversal logic — that reduces every operation to KV reads and writes within existing transactions.

### Module Placement

```
strata-core         → types, contracts, EntityRef
strata-storage      → ShardedStore, VersionChain, MVCC
strata-concurrency  → TransactionContext, conflict detection
strata-durability   → WAL, DurabilityMode, checkpoints
strata-engine       → Database, primitives, search
                       ├── primitives/    (kv, json, events, state, vectors)
                       ├── search/        (BM25, hybrid search, recovery)
                       └── graph/         ← NEW — same level as search/
strata-executor     → Command enum, handlers, Strata API
```

The graph module lives inside `strata-engine` as a peer to `search/`. It holds `Arc<Database>` and calls `db.transaction(branch_id, |txn| { ... })` for all operations — exactly like every other primitive.

### Inherited Guarantees

Every graph operation is a transaction closure over `Database`. This means:

| Guarantee | How the Graph Gets It | What We Must NOT Do |
|-----------|----------------------|---------------------|
| **ACID** | `db.transaction()` provides atomic commit with OCC + snapshot isolation. An `add_edge` writing forward + reverse entries either fully commits or fully aborts. | Never write to KV outside a transaction. Never split a logical operation across multiple transactions. |
| **Durability** | WAL is written before storage apply. The 3 modes (Always/Standard/Cache) apply to all writes including `_graph_/` keys. | Never bypass the transaction commit path. Never write directly to storage. |
| **Versioning** | Every `txn.put()` gets a monotonic commit version. Version chains are maintained automatically by `ShardedStore`. | Never manually manage versions. Never delete version history. |
| **Time-travel** | `db.get_at_timestamp()` and `db.scan_prefix_at_timestamp()` work on any key, including `_graph_/` keys. Graph queries at past timestamps are free. | Never use time-travel APIs that bypass the existing version chain. |
| **Branching** | `BranchId` scopes all transactions. Different branches = different storage shards. Fork = CoW. Graph keys on branch A are invisible to branch B. | Never hardcode branch IDs. Always pass `branch_id` through. Never assume a global namespace. |
| **Conflict detection** | OCC tracks read sets. If two transactions modify the same edge, first-committer-wins. | Never suppress conflict errors. Let the transaction system handle retries. |

### The Contract

**The graph module must not:**
- Introduce new storage backends or bypass KV
- Implement its own transaction, versioning, or durability logic
- Break snapshot isolation by reading outside transaction boundaries
- Assume single-branch operation
- Block primary operations (integrity hooks are best-effort)

**The graph module inherits for free:**
- Atomic multi-key writes (forward + reverse edge entries)
- Time-travel on graph state (`as_of` queries on `_graph_/` keys)
- Branch isolation and CoW forking of entire graphs
- WAL-based durability at the configured mode
- Conflict detection on concurrent edge modifications

### Transaction API Surface

The graph module builds on these `Database` and `TransactionContext` APIs:

```rust
// Database — transaction lifecycle
db.transaction(branch_id, |txn| { ... })?;                    // Auto-commit closure
db.transaction_with_version(branch_id, |txn| { ... })?;       // Returns commit version
db.transaction_with_retry(branch_id, config, |txn| { ... })?; // Auto-retry on conflict

// TransactionContext — reads (snapshot-isolated, read-your-writes)
txn.get(&key)?;                    // → Option<Value>
txn.get_versioned(&key)?;          // → Option<VersionedValue> (value + version + timestamp)
txn.scan_prefix(&prefix)?;         // → Vec<(Key, Value)> sorted

// TransactionContext — writes (buffered until commit)
txn.put(key, value);               // Buffer a write
txn.delete(key);                   // Buffer a delete

// Database — time-travel (non-transactional)
db.get_at_timestamp(&key, micros)?;         // Historical point read
db.scan_prefix_at_timestamp(&prefix, micros)?; // Historical prefix scan
```

Every graph operation maps to these calls. Nothing else is needed.

---

## TDD Methodology

Every story follows the Red-Green-Refactor cycle:

1. **Red:** Write tests first. Tests define the expected behavior. They must fail (or not compile) before implementation.
2. **Green:** Write the minimum implementation to make tests pass.
3. **Refactor:** Clean up without changing behavior. Tests must still pass.

### Test Categories per Story

Each story includes three test categories:

| Category | What It Tests | Example |
|----------|--------------|---------|
| **Unit tests** | Pure functions, type behavior, key construction | Serde round-trips, key parsing, validation |
| **Integration tests** | Operations against a real `Database` instance | CRUD round-trips, traversal correctness, branch isolation |
| **Invariant tests** | Existing Strata guarantees are preserved | Versioning works on graph keys, time-travel works, branch isolation holds, existing tests don't break |

### Invariant Tests (Required for Every Story That Touches Storage)

These tests verify that the graph layer does not break Strata's existing guarantees:

```rust
#[test]
fn graph_writes_are_versioned() {
    // Add a node, add it again with different properties
    // Verify both versions exist via get_versioned
}

#[test]
fn graph_supports_time_travel() {
    // Add edges at time T1, modify at T2
    // Query as_of T1 → see original state
    // Query as_of T2 → see modified state
}

#[test]
fn graph_respects_branch_isolation() {
    // Create graph on branch A
    // Fork to branch B
    // Modify on B
    // Verify A is unchanged
}

#[test]
fn graph_writes_are_atomic() {
    // add_edge must write forward + reverse atomically
    // If one fails, neither should persist
}

#[test]
fn graph_keys_do_not_collide_with_user_keys() {
    // Write a user KV key, write a graph key
    // Verify no interference
}

#[test]
fn existing_kv_tests_still_pass() {
    // Run the full existing test suite — zero regressions
}
```

---

## Dependency Graph

```
Epic 1 (Types, Keys, CRUD)
  ├── Epic 2 (Traversals) ──── Epic 3 (Snapshot/Export)
  │     └── Epic 5 (Adjacency Index) ──── Epic 7 (Graph-Boosted Search)
  ├── Epic 4 (Ref Index) ──── Epic 6 (Integrity Hooks)
  └── Epic 8 (Command Enum) ──── Epic 9 (Strata API)
```

**Recommended order:** 1 → 2 → 4 → 3 → 8 → 5 → 6 → 7 → 9

---

## Codebase Patterns

Every story must follow the established patterns in the codebase:

| Pattern | Reference File | Notes |
|---------|---------------|-------|
| Stateless facade | `crates/engine/src/primitives/kv.rs` | `struct GraphStore { db: Arc<Database> }`, methods take `branch_id` |
| Database extension | `crates/engine/src/database/mod.rs` | `db.extension::<AdjacencyIndex>()` for lazy singletons |
| Command enum | `crates/executor/src/command.rs` | Each graph op = a new variant with `branch: Option<BranchId>` |
| Handler pattern | `crates/executor/src/handlers/kv.rs` | validate → call primitive → post-hook → return Output |
| Post-write hook | `crates/executor/src/handlers/embed_hook.rs` | Best-effort, never blocks caller |
| Recovery participant | `crates/engine/src/search/recovery.rs` | Rebuild index from KV at startup |
| Primitives bridge | `crates/executor/src/bridge.rs` | Add `graph: GraphStore` to `Primitives` struct |
| EntityRef | `crates/core/src/contract/entity_ref.rs` | `kv://branch/key`, `json://branch/doc`, etc. |

---

## Epic 1: Core Graph Types, Keys, and CRUD

**Goal:** Foundation — types, key construction, and the `GraphStore` facade with node/edge CRUD. Produces testable code immediately.

### Story 1.1 — Define graph domain types

**File:** `crates/engine/src/graph/types.rs`

**TDD sequence:**
1. Write tests: serde round-trip for each type, default value assertions
2. Define types to make tests pass

Define all graph domain types as specified in PRD Sections 4.2–4.4:

- `NodeData` — optional `entity_ref` (String URI), `properties` (HashMap<String, Value>)
- `EdgeData` — `weight` (f64, default 1.0), `properties` (HashMap<String, Value>)
- `Edge` — `src`, `dst`, `edge_type` (all String), `data` (EdgeData)
- `Direction` — Outgoing, Incoming, Both
- `BfsOptions` — `max_depth`, `max_nodes`, `edge_types`, `direction`
- `BfsResult` — `visited` (Vec), `edges` (Vec of tuples), `depths` (HashMap)
- `Neighbor` — `node_id`, `edge_type`, `edge_data`
- `GraphMeta` — `description`, `on_entity_delete` (CascadePolicy)
- `CascadePolicy` — Cascade, Detach, Ignore (default)

All types derive `Debug, Clone, Serialize, Deserialize` as appropriate.

**Tests (write first):**
- Serialization round-trip for each type
- Default values (weight = 1.0, policy = Ignore)
- Direction enum coverage

---

### Story 1.2 — Key construction and parsing

**File:** `crates/engine/src/graph/keys.rs`

**TDD sequence:**
1. Write tests: construct a key, parse it back, assert components match
2. Write tests: validate rejects empty strings, slashes, reserved prefixes
3. Implement key functions to make tests pass

Key schema from PRD Section 4.1:

```
Forward:   _graph_/{graph}/e/{src}/{edge_type}/{dst}
Reverse:   _graph_/{graph}/r/{dst}/{edge_type}/{src}
Node:      _graph_/{graph}/n/{node_id}
Meta:      _graph_/{graph}/__meta__
Ref index: _graph_/__ref__/{entity_ref_uri}/{graph}/{node_id}
```

**Functions to implement:**

Constructors:
- `forward_edge_key(graph, src, edge_type, dst) → String`
- `reverse_edge_key(graph, dst, edge_type, src) → String`
- `node_key(graph, node_id) → String`
- `meta_key(graph) → String`
- `ref_index_key(entity_ref_uri, graph, node_id) → String`

Prefix builders (for scan operations):
- `forward_edges_prefix(graph, src) → String`
- `forward_edges_typed_prefix(graph, src, edge_type) → String`
- `reverse_edges_prefix(graph, dst) → String`
- `reverse_edges_typed_prefix(graph, dst, edge_type) → String`
- `all_edges_prefix(graph) → String`
- `all_nodes_prefix(graph) → String`
- `ref_index_entity_prefix(entity_ref_uri) → String`
- `graph_prefix(graph) → String`

Parsers:
- `parse_forward_edge_key(key) → Option<(src, edge_type, dst)>`
- `parse_reverse_edge_key(key) → Option<(dst, edge_type, src)>`
- `parse_node_key(key) → Option<node_id>`
- `parse_ref_index_key(key) → Option<(entity_ref_uri, graph, node_id)>`

Validation:
- `validate_graph_name(name) → Result<()>` — no empty, no `/`, no `__` prefix
- `validate_node_id(id) → Result<()>` — no empty, no `/`, no `__` prefix
- `validate_edge_type(edge_type) → Result<()>` — no empty, no `/`

**Tests (write first):**
- Round-trip: construct key then parse, verify components
- Prefix correctness: forward prefix is a true prefix of all outgoing edge keys from a node
- Validation: reject empty names, slashes, reserved prefixes
- Edge cases: single-character names, unicode

---

### Story 1.3 — GraphStore facade with CRUD

**Files:** `crates/engine/src/graph/mod.rs`, `crates/engine/src/lib.rs` (add `pub mod graph`)

**TDD sequence:**
1. Write tests: create graph, get meta, list, delete — all against `Database::cache()`
2. Write tests: add/get/remove node, list nodes
3. Write tests: add/get/remove edge, verify dual-index
4. Write invariant tests: versioning on graph keys, branch isolation, atomic edge writes
5. Implement `GraphStore` to make all tests pass

`GraphStore { db: Arc<Database> }` following `KvPrimitive` pattern.

**Graph lifecycle:**
- `create_graph(branch_id, name, meta) → Result<()>` — write `__meta__` key in transaction
- `get_graph_meta(branch_id, name) → Result<Option<GraphMeta>>` — read `__meta__` in transaction
- `list_graphs(branch_id) → Result<Vec<String>>` — prefix scan on `_graph_/`, deduplicate
- `delete_graph(branch_id, name) → Result<()>` — delete all keys under `_graph_/{name}/` in transaction

**Node CRUD:**
- `add_node(branch_id, graph, id, data) → Result<()>` — write node key + ref index if entity_ref, single transaction
- `get_node(branch_id, graph, id) → Result<Option<NodeData>>` — read node key in transaction
- `remove_node(branch_id, graph, id) → Result<()>` — delete node + all incident edges + ref index, single transaction
- `list_nodes(branch_id, graph) → Result<Vec<String>>` — prefix scan on `_graph_/{g}/n/`

**Edge CRUD:**
- `add_edge(branch_id, graph, edge) → Result<()>` — write forward + reverse KV entries in single transaction (atomic)
- `get_edge(branch_id, graph, src, edge_type, dst) → Result<Option<EdgeData>>` — read forward key
- `remove_edge(branch_id, graph, src, edge_type, dst) → Result<()>` — delete forward + reverse in single transaction

**Tests (write first):**
- *Unit:* Create graph, get meta, list graphs, delete graph
- *Unit:* Add node, get node, remove node, list nodes
- *Unit:* Add edge, get edge, remove edge
- *Unit:* Dual-index verification (forward + reverse both exist after add_edge)
- *Unit:* remove_node cascades edge deletion
- *Unit:* Error cases: invalid graph name, invalid node ID
- *Invariant:* `graph_writes_are_versioned` — add node, update it, verify both versions via `get_versioned`
- *Invariant:* `graph_supports_time_travel` — add edges at T1, modify at T2, query as_of T1 sees original
- *Invariant:* `graph_respects_branch_isolation` — create on branch A, fork to B, modify B, verify A unchanged
- *Invariant:* `graph_edge_writes_are_atomic` — add_edge writes forward + reverse atomically
- *Invariant:* `graph_keys_do_not_collide_with_user_keys` — user KV and graph KV coexist

---

## Epic 2: Traversal Operations

**Dependency:** Epic 1

### Story 2.1 — Neighbors and degree queries

**File:** `crates/engine/src/graph/traversal.rs`

**TDD sequence:**
1. Write tests for each direction and filter combination
2. Implement `neighbors()` and `degree()` using `txn.scan_prefix()`

- `neighbors(branch_id, graph, node, direction, edge_type) → Result<Vec<Neighbor>>`
  - `Direction::Outgoing`: scan `_graph_/{g}/e/{node}/` (or typed prefix)
  - `Direction::Incoming`: scan `_graph_/{g}/r/{node}/` (or typed prefix)
  - `Direction::Both`: union of both scans
- `degree(branch_id, graph, node, direction) → Result<usize>`
  - Same scan, count only

**Tests (write first):**
- *Unit:* 0, 1, N outgoing edges
- *Unit:* Edge type filter (only returns matching type)
- *Unit:* Direction::Incoming returns nodes pointing TO this node
- *Unit:* Direction::Both returns union (no duplicates)
- *Unit:* Degree counts match neighbor list lengths
- *Unit:* Nonexistent node returns empty vec (not error)
- *Invariant:* Neighbors query on branch B doesn't see branch A's edges

---

### Story 2.2 — BFS traversal

**File:** `crates/engine/src/graph/traversal.rs` (extend)

**TDD sequence:**
1. Write tests for each BFS scenario (chain, fan-out, cycle, caps)
2. Implement iterative BFS using `neighbors()`

`bfs(branch_id, graph, start, options) → Result<BfsResult>`

Algorithm:
1. Initialize queue with `start` at depth 0
2. While queue not empty and `visited.len() < max_nodes`:
   - Dequeue `(node, depth)`
   - If `depth >= max_depth`, skip expansion
   - Call `neighbors()` with direction + edge_type filter
   - For each unvisited neighbor, enqueue at `depth + 1`
3. Return `BfsResult { visited, edges, depths }`

**Tests (write first):**
- *Unit:* Linear chain: A→B→C→D, BFS from A depth 2 visits A,B,C
- *Unit:* Fan-out: A→B, A→C, A→D, BFS depth 1 visits all
- *Unit:* `max_nodes` cap: large graph, BFS with max_nodes=5 stops at 5
- *Unit:* Edge type filter: only follow DEPENDS_ON edges
- *Unit:* Direction::Incoming: reverse traversal
- *Unit:* Cycle handling: A→B→C→A does not loop
- *Unit:* Start node not in graph: returns BfsResult with only start

---

### Story 2.3 — Subgraph extraction

**File:** `crates/engine/src/graph/traversal.rs` (extend)

**TDD sequence:**
1. Write tests for subgraph filtering
2. Implement using edge scans + node set membership check

`subgraph(branch_id, graph, node_ids) → Result<GraphSnapshot>`

**Tests (write first):**
- *Unit:* Full graph → all edges included
- *Unit:* Partial set → only edges where both endpoints are in set
- *Unit:* Empty set → empty snapshot
- *Unit:* Single node → no edges

---

## Epic 3: Snapshot and Extensibility

**Dependency:** Epic 2

### Story 3.1 — GraphSnapshot struct and construction

**File:** `crates/engine/src/graph/snapshot.rs`

**TDD sequence:**
1. Write tests: snapshot of empty graph, snapshot matches state, point-in-time isolation
2. Implement `GraphSnapshot` struct and `GraphStore::snapshot()`

```rust
pub struct GraphSnapshot {
    pub nodes: HashMap<String, NodeData>,
    pub outgoing: HashMap<String, Vec<(String, String, EdgeData)>>,
    pub incoming: HashMap<String, Vec<(String, String, EdgeData)>>,
}
```

`GraphStore::snapshot(branch_id, graph) → Result<GraphSnapshot>` does a full prefix scan. O(V+E).

**Tests (write first):**
- *Unit:* Empty graph → empty maps
- *Unit:* Snapshot matches graph state after multiple adds/removes
- *Invariant:* Snapshot is point-in-time (later modifications don't appear in existing snapshot)

---

### Story 3.2 — GraphAlgorithm trait and export methods

**File:** `crates/engine/src/graph/snapshot.rs` (extend)

**TDD sequence:**
1. Write tests: export methods produce correct output
2. Write a trivial test algorithm implementing the trait
3. Implement trait and export methods

```rust
pub trait GraphAlgorithm {
    type Output;
    fn execute(&self, snapshot: &GraphSnapshot) -> Result<Self::Output>;
}

impl GraphSnapshot {
    pub fn to_edge_list(&self) -> Vec<(String, String, String, f64)>;
    pub fn to_adjacency_list(&self) -> HashMap<String, Vec<(String, String, f64)>>;
    pub fn to_csv(&self) -> (String, String);  // (nodes_csv, edges_csv)
    pub fn node_count(&self) -> usize;
    pub fn edge_count(&self) -> usize;
}
```

**Tests (write first):**
- *Unit:* `to_edge_list` returns correct (src, dst, type, weight) tuples
- *Unit:* `to_adjacency_list` groups neighbors correctly
- *Unit:* `to_csv` produces valid CSV with headers
- *Unit:* `node_count` and `edge_count` match graph
- *Unit:* Trivial test algorithm (node count) via `GraphAlgorithm` trait compiles and returns correct result

---

## Epic 4: Reverse EntityRef Index

**Dependency:** Epic 1

### Story 4.1 — Ref index maintenance and query

**File:** `crates/engine/src/graph/ref_index.rs`

**TDD sequence:**
1. Write tests: add node with entity_ref, query `nodes_for_entity`, verify result
2. Write tests: remove node removes ref index, update entity_ref works
3. Implement ref index functions, wire into `add_node`/`remove_node`

Key: `_graph_/__ref__/{entity_ref_uri}/{graph}/{node_id}` → `""`

Ref index writes happen inside the same transaction as the node write — atomic with the node CRUD.

**Tests (write first):**
- *Unit:* Add node with entity_ref, query `nodes_for_entity`, get back (graph, node_id)
- *Unit:* Multiple graphs bound to same entity → all returned
- *Unit:* Remove node → ref index entry removed
- *Unit:* Node without entity_ref → no ref index entry created
- *Unit:* Update entity_ref on existing node (remove old, add new)
- *Invariant:* Ref index is branch-isolated (entity on branch A not visible from branch B query)

---

## Epic 5: Materialized Adjacency Index

**Dependency:** Epics 1–2

### Story 5.1 — AdjacencyIndex Database extension

**File:** `crates/engine/src/graph/adjacency.rs`

**TDD sequence:**
1. Write tests: cold load from KV, warm update, invalidation
2. Implement `AdjacencyIndex` as Database extension

```rust
pub struct AdjacencyIndex {
    graphs: DashMap<(BranchId, String), GraphAdjacency>,
    loaded: DashMap<(BranchId, String), bool>,
}

pub struct GraphAdjacency {
    pub outgoing: HashMap<String, Vec<(String, String, f64)>>,
    pub incoming: HashMap<String, Vec<(String, String, f64)>>,
    pub nodes: HashMap<String, NodeData>,
}

impl Default for AdjacencyIndex { ... }
```

Registered via `db.extension::<AdjacencyIndex>()`.

**Tests (write first):**
- *Unit:* Cold load: add edges via GraphStore, access adjacency → loads correctly
- *Unit:* Warm update: add edge → adjacency index updated without full reload
- *Unit:* Invalidate → re-access triggers reload
- *Unit:* Multiple graphs: independent caching
- *Invariant:* Adjacency index and KV are always consistent after writes

---

### Story 5.2 — Wire adjacency into GraphStore queries

**File:** `crates/engine/src/graph/mod.rs` (modify)

**TDD sequence:**
1. Write tests: same queries produce identical results on cold vs warm paths
2. Modify `neighbors()`, `degree()`, `bfs()` to use adjacency when loaded

Write operations update both KV (first) and cache (if loaded). KV-first guarantees consistency.

**Tests (write first):**
- *Integration:* Cold path: same results as before adjacency index
- *Integration:* Warm path: same results (verify correctness)
- *Integration:* Write-then-read: add edge, immediately query, see new edge
- *Invariant:* All existing traversal tests still pass

---

### Story 5.3 — Recovery participant

**File:** `crates/engine/src/graph/adjacency.rs` (extend)

**TDD sequence:**
1. Write tests: create graph, shutdown, reopen, verify adjacency populated
2. Implement recovery participant following `search/recovery.rs` pattern

Manifest key `_graph_/__loaded__` tracks materialized graphs. Updated on first load.

```rust
pub fn register_graph_recovery() {
    register_recovery_participant(RecoveryParticipant::new("graph", recover_graph_state));
}
```

**Tests (write first):**
- *Integration:* Create graph, add edges, shutdown, reopen → adjacency index populated
- *Integration:* Cache (ephemeral) database → recovery is no-op
- *Integration:* No loaded graphs → recovery is no-op
- *Invariant:* Recovery does not alter KV data (read-only rebuild)

---

## Epic 6: Referential Integrity Hooks

**Dependency:** Epics 1, 4

### Story 6.1 — Cascade logic

**Files:**
- `crates/engine/src/graph/integrity.rs`
- `crates/executor/src/handlers/graph_integrity_hook.rs`

**TDD sequence:**
1. Write tests for each cascade policy (cascade, detach, ignore)
2. Write test that hook failure doesn't propagate
3. Implement hook and cascade logic

`on_entity_deleted(primitives, branch_id, entity_ref)`:
1. Convert `entity_ref` to URI string
2. Call `graph_store.nodes_for_entity(branch_id, uri)` via ref index
3. For each `(graph, node_id)`: get meta → apply policy
4. Best-effort: log warnings on failure, never propagate errors

**Tests (write first):**
- *Integration:* Cascade: delete entity → node and edges removed
- *Integration:* Detach: delete entity → node remains, entity_ref cleared
- *Integration:* Ignore: delete entity → no graph changes
- *Integration:* Multiple graphs with different policies
- *Invariant:* Hook failure logs but doesn't propagate (primary delete always succeeds)
- *Invariant:* Existing delete handler tests still pass

---

### Story 6.2 — Wire hook into executor delete handlers

**Files:** `crates/executor/src/handlers/kv.rs`, `json.rs`, and other delete handlers

**TDD sequence:**
1. Write end-to-end tests: delete KV/JSON entry with graph binding, verify cleanup
2. Wire `graph_integrity_hook::on_entity_deleted()` into delete handlers

Follows embed_hook pattern — best-effort, post-mutation.

**Tests (write first):**
- *Integration:* Create graph with entity_ref, delete KV entry → graph cleanup
- *Integration:* Create graph with entity_ref, delete JSON doc → graph cleanup
- *Invariant:* All existing delete handler tests still pass (zero regressions)

---

## Epic 7: Graph-Boosted Search

**Dependency:** Epics 1, 4, 5

### Story 7.1 — Proximity scoring

**File:** `crates/engine/src/graph/boost.rs`

**TDD sequence:**
1. Write tests for each proximity scenario
2. Implement scoring functions

```rust
pub struct GraphBoost {
    pub graph_name: String,
    pub anchor_nodes: Vec<String>,
    pub max_hops: usize,    // default 2
    pub weight: f32,        // default 0.3
}
```

Proximity: BFS from anchors. Distance 0 = 1.0, 1 = 0.5, 2 = 0.25, 3+ = 0.0.
Formula: `boosted_score = rrf_score * (1.0 + weight * proximity_score)`

**Tests (write first):**
- *Unit:* Anchor node itself → full boost (1.0)
- *Unit:* 1-hop neighbor → 0.5 boost
- *Unit:* Unconnected node → zero boost
- *Unit:* Multiple anchors → uses closest
- *Unit:* Weight = 0 → no change in scores

---

### Story 7.2 — Integrate into search pipeline

**Files:**
- `crates/engine/src/search/` — add `graph_boost: Option<GraphBoost>` to `SearchRequest`
- `crates/executor/src/handlers/search.rs` — apply boost post-RRF, re-sort

**TDD sequence:**
1. Write tests: search without boost unchanged, search with boost reranks
2. Wire boost into search handler

**Tests (write first):**
- *Integration:* No `graph_boost` → identical results to before (zero regression)
- *Integration:* With `graph_boost` → connected results rank higher
- *Integration:* Empty anchor list → no boost applied
- *Integration:* Nonexistent graph → graceful no-op with logged warning
- *Invariant:* All existing search tests still pass

---

## Epic 8: Command Enum Integration

**Dependency:** Epics 1–4

### Story 8.1 — Graph Command and Output variants

**Files:** `crates/executor/src/command.rs`, `output.rs`, `types.rs`

**TDD sequence:**
1. Write tests: serde round-trip for each variant, `is_write()` correctness
2. Add Command and Output variants

~17 Command variants (all with `branch: Option<BranchId>`):

| Command | Write? | Purpose |
|---------|--------|---------|
| `GraphCreate` | yes | Create a named graph |
| `GraphDelete` | yes | Delete a graph and all data |
| `GraphList` | no | List all graph names |
| `GraphGetMeta` | no | Get graph configuration |
| `GraphAddNode` | yes | Add/update a node |
| `GraphGetNode` | no | Get node metadata |
| `GraphRemoveNode` | yes | Remove node + incident edges |
| `GraphListNodes` | no | List all node IDs |
| `GraphAddEdge` | yes | Add a directed edge |
| `GraphRemoveEdge` | yes | Remove a specific edge |
| `GraphNeighbors` | no | Single-hop neighbor query |
| `GraphDegree` | no | Count edges for a node |
| `GraphBfs` | no | Bounded BFS traversal |
| `GraphSubgraph` | no | Extract induced subgraph |
| `GraphNodesForEntity` | no | Reverse EntityRef lookup |
| `GraphSnapshot` | no | Full graph materialization |
| `GraphToEdgeList` | no | Export as edge tuples |
| `GraphToAdjacencyList` | no | Export as adjacency map |

**Tests (write first):**
- *Unit:* Serde round-trip for each Command variant
- *Unit:* Serde round-trip for each Output variant
- *Unit:* `cmd.is_write()` correct for mutations vs reads
- *Unit:* `cmd.name()` returns correct name string
- *Invariant:* Existing Command/Output serde tests still pass

---

### Story 8.2 — Graph handler functions

**File:** `crates/executor/src/handlers/graph.rs` (new)

**TDD sequence:**
1. Write integration tests via `Executor::execute()` for each command
2. Implement handler functions

Each handler: validate → call GraphStore → return Output.

**Tests (write first):**
- *Integration:* Round-trip via `Executor::execute()` for each command
- *Integration:* Error cases: invalid graph name, nonexistent branch, missing node
- *Invariant:* Existing handler tests still pass

---

### Story 8.3 — Wire into Executor dispatch and Primitives bridge

**Files:** `crates/executor/src/executor.rs`, `crates/executor/src/bridge.rs`

**TDD sequence:**
1. Write tests: full round-trip through Executor, ReadOnly enforcement
2. Add `graph: GraphStore` to `Primitives`, add match arms in `execute()`

**Tests (write first):**
- *Integration:* `Executor::execute(Command::GraphCreate { ... })` returns `Output::Unit`
- *Integration:* Add nodes/edges and query neighbors via Command/Output
- *Integration:* Graph commands respect `AccessMode::ReadOnly`
- *Invariant:* All existing executor tests still pass

---

## Epic 9: Strata API Surface

**Dependency:** Epic 8

### Story 9.1 — Graph methods on Strata API

**Files:** `crates/executor/src/api/mod.rs`, `crates/executor/src/api/graph.rs` (new)

**TDD sequence:**
1. Write the full patient care example from PRD Section 6 as a test
2. Write branch isolation test
3. Implement typed methods on `Strata`

```rust
impl Strata {
    pub fn graph_create(&mut self, name: &str) -> Result<()>;
    pub fn graph_create_with_options(&mut self, name: &str, description: Option<&str>, on_entity_delete: &str) -> Result<()>;
    pub fn graph_delete(&mut self, name: &str) -> Result<()>;
    pub fn graph_list(&self) -> Result<Vec<String>>;

    pub fn graph_add_node(&mut self, graph: &str, node_id: &str, entity_ref: Option<&str>, properties: Option<Value>) -> Result<()>;
    pub fn graph_get_node(&self, graph: &str, node_id: &str) -> Result<Option<NodeInfo>>;
    pub fn graph_remove_node(&mut self, graph: &str, node_id: &str) -> Result<()>;
    pub fn graph_list_nodes(&self, graph: &str) -> Result<Vec<String>>;

    pub fn graph_add_edge(&mut self, graph: &str, src: &str, dst: &str, edge_type: &str, weight: Option<f64>, properties: Option<Value>) -> Result<()>;
    pub fn graph_remove_edge(&mut self, graph: &str, src: &str, dst: &str, edge_type: &str) -> Result<()>;

    pub fn graph_neighbors(&self, graph: &str, node_id: &str, direction: &str, edge_type: Option<&str>) -> Result<Vec<NeighborInfo>>;
    pub fn graph_degree(&self, graph: &str, node_id: &str, direction: &str) -> Result<usize>;
    pub fn graph_bfs(&self, graph: &str, start: &str, max_depth: usize, ...) -> Result<BfsResultInfo>;
}
```

**Tests (write first):**
- *Integration:* Full patient care example from PRD Section 6 works end-to-end
- *Integration:* Branch switching: create graph on one branch, switch, verify isolation
- *Invariant:* Existing Strata API tests still pass

---

### Story 9.2 — Register graph recovery in initialization

**File:** `crates/executor/src/api/mod.rs`

**TDD sequence:**
1. Write test: open, add graph data, close, reopen, query succeeds
2. Add `register_graph_recovery()` to initialization

**Tests (write first):**
- *Integration:* Open database, add graph data, close, reopen → graph queries work
- *Integration:* Recovery participant is registered
- *Invariant:* Existing recovery tests still pass

---

## Summary

| Epic | Stories | Scope |
|------|---------|-------|
| 1. Core Types, Keys, CRUD | 1.1, 1.2, 1.3 | Foundation — all subsequent epics depend on this |
| 2. Traversals | 2.1, 2.2, 2.3 | Neighbors, BFS, subgraph |
| 3. Snapshot/Export | 3.1, 3.2 | In-memory materialization, GraphAlgorithm trait, export |
| 4. Reverse EntityRef Index | 4.1 | Cross-primitive entity lookup |
| 5. Adjacency Index | 5.1, 5.2, 5.3 | In-memory cache for fast multi-hop traversal |
| 6. Integrity Hooks | 6.1, 6.2 | Cascade/detach on entity deletion |
| 7. Graph-Boosted Search | 7.1, 7.2 | Search relevance via graph proximity |
| 8. Command Enum | 8.1, 8.2, 8.3 | Executor integration |
| 9. Strata API | 9.1, 9.2 | Public API surface + recovery |

**Total: 21 stories across 9 epics.**

**TDD rule: Every story starts with failing tests. No implementation without a test that demands it.**

**Invariant rule: Every story that touches storage includes tests proving ACID, versioning, time-travel, branching, and durability still work. `cargo test` on the full workspace must pass after every story — zero regressions.**
