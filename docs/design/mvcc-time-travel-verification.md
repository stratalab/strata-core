# MVCC / Time-Travel Verification Plan

**Status**: Phase 1 & 2 complete | **Date**: 2026-04-05 (Phase 1 & 2 landed 2026-04-06) | **Claim**: #3 — Uniform MVCC / Time-Travel Across Primitives | **Current Rating**: PARTIAL → progressing toward VERIFIED

## Progress Log

- **Phase 1 — Graph time-travel**: Merged as stratalab/strata-core#2322. Added `get_node_at`, `list_nodes_at`, `neighbors_at` on `GraphStore` + `as_of` on `GraphGetNode`/`GraphListNodes`/`GraphNeighbors` commands, plus handlers, session bypass, public API, and 10 unit tests.
- **Phase 2 — Vector `getv`**: Added `VectorStore::getv`, `Command::VectorGetv`, `Output::VectorVersionHistory`, session always-bypass routing, public `vector_getv()` API, plus 6 unit tests and 1 session bypass integration test.
- **Phases 3–5**: Pending.

---

## Problem Statement

Strata claims uniform MVCC and time-travel across all primitives. The storage engine fully supports this — `get_at_timestamp()` and `scan_prefix_at_timestamp()` are type-tag agnostic and work on any key. But the public primitive surfaces are asymmetric: KV and JSON expose full MVCC, Event has append-only temporal filters, Vector has historical reads but no version history, and Graph exposes no historical surface at all despite storing all data in the same versioned key-value chain.

---

## Current Reality

### MVCC Foundation (Storage Layer)

The versioning architecture is two-level:

- **Outer (Storage):** All entries use `Version::Txn(u64)` — a global monotonic counter from `TransactionManager.version` (`AtomicU64`). Enforced by `StoredValue::new()` via `debug_assert!`.
- **Inner (Primitive):** Each primitive maintains its own version counter inside the serialized Value payload (Counter for State/JSON/Vector, Sequence for Event, Txn for KV).

Time-travel resolution uses two strategies:
- **Version-based (`get_at_version`):** Finds the latest `StoredValue` whose `version_raw() <= max_version`. Used for MVCC snapshot isolation inside transactions.
- **Timestamp-based (`get_at_timestamp`):** Finds the latest `StoredValue` whose `timestamp <= max_timestamp`. This is the public time-travel API, implemented at `crates/storage/src/segmented/mod.rs` and exposed through `Database::get_at_timestamp`.

Both methods are **type-tag agnostic** — they work identically on KV, JSON, Event, Vector, and Graph keys. The storage foundation already fully supports time-travel for all primitives.

### Per-Primitive Surface

| Primitive | `get_at` | `list_at` | `getv` | Rating |
|-----------|----------|-----------|--------|--------|
| KV | Yes | Yes | Yes | Full MVCC |
| JSON | Yes | Yes | Yes | Full MVCC |
| Event | Yes (timestamp filter) | Yes | N/A (immutable) | Full (by design) |
| Vector | Yes | N/A | **Yes (Phase 2)** | Full |
| Graph | **Yes (Phase 1)** | **Yes (Phase 1)** | N/A | Full for nodes/neighbors |

### KV and JSON (Gold Standard)

These define the target surface that other primitives should match where applicable:

- **`getv`** (`crates/engine/src/primitives/kv.rs:137`): Calls `self.db.get_history()`, returns `VersionedHistory<Value>` — a newest-first ordered list of `Versioned<Value>`, each with value, version, and timestamp.
- **`get_at`** (`crates/engine/src/primitives/kv.rs:551`): Calls `self.db.get_at_timestamp()` directly (non-transactional), returns `Option<Value>`.
- **`list_at`** (`crates/engine/src/primitives/kv.rs:566`): Calls `self.db.scan_prefix_at_timestamp()`, returns list of keys that existed at the given timestamp.

JSON follows the identical pattern at `crates/engine/src/primitives/json/mod.rs` (lines 404, 1141, 1165).

Both expose `as_of: Option<u64>` on their `Get` and `List` commands in `crates/executor/src/command.rs`. The executor dispatches to `_get_at` / `_list_at` handlers when `as_of` is `Some`. Both have dedicated `KvGetv` / `JsonGetv` commands.

### Event (Intentionally Different)

Event is append-only — events cannot be updated, so each event has exactly one version forever. No `getv` by design.

- **`list_at`** (`crates/engine/src/primitives/event.rs:669`): Iterates all sequences, calling `self.db.get_at_timestamp()` for each event, then filters by `event.timestamp <= as_of_ts`. Supports event_type filter and limit.
- **`event_get_at`** (`crates/executor/src/handlers/event.rs:83`): Gets the event by sequence, returns it only if `event.timestamp <= as_of_ts`.
- **`event_get_by_type_at`** (`crates/executor/src/handlers/event.rs:143`): Delegates to engine `get_by_type_at`.

Event's temporal model is well-defined and correct. Its place in the uniform time-travel story is as an append-only stream with timestamp-based filtering — a valid and intentional specialization.

### Vector (Partial)

- **`get_at`** (`crates/vector/src/store/crud.rs:180`): Calls `self.db.get_at_timestamp()`, deserializes the `VectorRecord`. Returns historical embedding from storage.
- **`search_at`** (`crates/vector/src/store/search.rs:203`): Uses temporal filtering on the HNSW index via `created_at`/`deleted_at` timestamps on each node.
- **Missing `getv`**: Vector records are stored in the MVCC version chain (via `Key::new_vector` with `TypeTag::Vector`), so the storage infrastructure holds all historical versions. There is simply no method that calls `get_history` for a vector key.

Commands `VectorGet` and `VectorQuery` both have `as_of: Option<u64>`.

### Graph (Biggest Gap)

All graph data is stored as versioned KV entries under the `_graph_` space:
- Node data: key = `{graph}/n/{node_id}`, value = JSON-serialized `NodeData`
- Forward adjacency: key = `{graph}/fwd/{node_id}`, value = packed binary `Vec<(dst, edge_type, EdgeData)>`
- Reverse adjacency: key = `{graph}/rev/{node_id}`, value = packed binary

All writes go through `TransactionContext`, creating proper version chains with `Version::Txn`. The data IS versioned — every `txn.put()` creates a new entry in the storage version chain. `GraphStore` holds `Arc<Database>`, which provides `get_at_timestamp()` and `scan_prefix_at_timestamp()`.

**Nothing is missing at the storage level. Everything is missing at the API level:**
- No `get_node_at()`, `list_nodes_at()`, `neighbors_at()` methods on `GraphStore`
- No `as_of` field on any Graph command
- No graph temporal handlers
- No graph temporal API methods in `crates/executor/src/api/graph.rs`

### CLI Gap

The CLI parser at `crates/cli/src/parse.rs` hardcodes `as_of: None` for every command. Time-travel is only accessible via the programmatic API. Not a blocker for VERIFIED status on the architectural claim, but a usability gap.

---

## Implementation Plan

### Phase 1: Graph Time-Travel API

**Priority**: Highest — this is the biggest gap and the primary blocker for VERIFIED.
**Complexity**: Medium (~300-400 lines of new code across 5-6 files)

#### Step 1.1: Add time-travel read methods to `GraphStore`

**Files:**
- `crates/graph/src/nodes.rs` — add `get_node_at()`, `list_nodes_at()`
- `crates/graph/src/edges.rs` — add `neighbors_at()`

Implementation pattern (following KV's `get_at`):

```rust
// nodes.rs
pub fn get_node_at(
    &self,
    branch_id: BranchId,
    graph: &str,
    node_id: &str,
    as_of_ts: u64,
) -> StrataResult<Option<NodeData>> {
    let storage_key = /* build Key::new_graph for {graph}/n/{node_id} */;
    let result = self.db.get_at_timestamp(&storage_key, as_of_ts)?;
    // deserialize NodeData from result
}

pub fn list_nodes_at(
    &self,
    branch_id: BranchId,
    graph: &str,
    as_of_ts: u64,
) -> StrataResult<Vec<String>> {
    let prefix = /* build Key::new_graph for {graph}/n/ prefix */;
    let results = self.db.scan_prefix_at_timestamp(&prefix, as_of_ts)?;
    // extract node_ids from keys
}
```

For `neighbors_at`, read the packed adjacency list from the storage timestamp:

```rust
// edges.rs
pub fn outgoing_neighbors_at(
    &self,
    branch_id: BranchId,
    graph: &str,
    node_id: &str,
    edge_type_filter: Option<&str>,
    as_of_ts: u64,
) -> StrataResult<Vec<Neighbor>> {
    let fwd_key = /* build Key::new_graph for {graph}/fwd/{node_id} */;
    match self.db.get_at_timestamp(&fwd_key, as_of_ts)? {
        Some(vv) => { /* decode packed adjacency from vv.value */ }
        None => Ok(Vec::new()),
    }
}
```

#### Step 1.2: Add `as_of` to Graph commands

**File:** `crates/executor/src/command.rs`

Add `as_of: Option<u64>` to:
- `GraphGetNode`
- `GraphListNodes`
- `GraphNeighbors`

Following the exact pattern of `KvGet` / `KvList`.

#### Step 1.3: Add graph temporal handlers

**File:** `crates/executor/src/handlers/graph.rs`

Add functions:
- `graph_get_node_at()`
- `graph_list_nodes_at()`
- `graph_neighbors_at()`

#### Step 1.4: Wire up executor dispatch

**Files:**
- `crates/executor/src/executor.rs` — add `if let Some(ts) = as_of` branching for `GraphGetNode`, `GraphListNodes`, `GraphNeighbors`
- `crates/executor/src/session.rs` — for `as_of` reads, bypass the transaction and call `GraphStore` directly (same pattern as KV)

#### Step 1.5: Add graph temporal API methods

**File:** `crates/executor/src/api/graph.rs`

Add:
- `graph_get_node_as_of()`
- `graph_list_nodes_as_of()`
- `graph_neighbors_as_of()`

---

### Phase 2: Vector Version-History API

**Priority**: Medium — historical reads exist, just missing `getv`.
**Complexity**: Low (~100-150 lines of new code)

#### Step 2.1: Add `getv` to VectorStore

**File:** `crates/vector/src/store/crud.rs`

```rust
pub fn getv(
    &self,
    branch_id: BranchId,
    collection: &str,
    key: &str,
) -> StrataResult<VersionedHistory<VectorEntry>> {
    let storage_key = /* build vector key */;
    let history = self.db.storage().get_history(&storage_key, None, None)?;
    // deserialize each VectorRecord version
}
```

#### Step 2.2: Add VectorGetv command, handler, and API

**Files:**
- `crates/executor/src/command.rs` — add `VectorGetv { branch, space, collection, key }`
- `crates/executor/src/handlers/vector.rs` — add `vector_getv()` handler
- `crates/executor/src/executor.rs` — wire up `Command::VectorGetv` dispatch
- `crates/executor/src/api/vector.rs` — add `vector_getv()` method

---

### Phase 3: Event Temporal Normalization

**Priority**: Low — Event is already correct, this is documentation.
**Complexity**: Minimal

- Document that Event intentionally has no `getv` because events are immutable (append-only, single version per event).
- Verify `EventListTypes` has `as_of` support. If not, add it — there should be a way to list event types that existed as of a given timestamp.
- Frame Event's temporal model as a first-class specialization (append-only stream with timestamp filtering) rather than a gap.

---

### Phase 4: Naming and Semantics Standardization

**Priority**: Medium — polish work after the functional gaps are closed.
**Complexity**: Low

Current naming is already mostly consistent:
- Command level: `as_of: Option<u64>`
- Engine level: `as_of_ts: u64`
- Storage level: `max_timestamp: u64`

Standardize method naming across all primitives:
- `get_at(key, as_of_ts)` — point-in-time read
- `list_at(prefix, as_of_ts)` — point-in-time listing
- `getv(key)` — full version history (where applicable)

Fix the `VectorStore` version inconsistency noted in `docs/architecture/version-semantics.md` (Problem 1: `insert()` returns `Version::counter`, `get()` returns `Version::txn` for the same record).

---

### Phase 5: Cross-Primitive Consistency Tests

**Priority**: High — required to prove the claim.
**Complexity**: Medium
**Depends on**: Phases 1 and 2

#### Test 1: Same-timestamp visibility

Write to KV, JSON, Vector, Graph at the same logical time. Read all at `as_of = that_timestamp`. Verify all are visible. Update all values. Read at the earlier timestamp. Verify old values for all primitives.

#### Test 2: Tombstone consistency

Delete a KV key, JSON doc, Vector record, Graph node. Verify `get_at` before deletion sees old values. Verify `get_at` after deletion returns None.

#### Test 3: Branch isolation with time-travel

Fork a branch, modify data on the fork. Verify time-travel on the original branch is unaffected.

#### Test 4: Edge cases

- `as_of = 0` (before any data): all primitives return None/empty
- `as_of = u64::MAX`: all primitives return current value
- Multiple rapid writes within the same millisecond

#### Existing test coverage

- Storage: `crates/storage/src/segmented/tests/batch.rs` — `get_at_timestamp_sees_old_version`, `get_at_timestamp_respects_tombstone`, `scan_prefix_at_timestamp_filters`
- Engine: `tests/engine/primitives/eventlog.rs` — `list_at_*` tests
- No cross-primitive consistency tests exist today
- No graph temporal tests at all
- No vector `getv` tests

---

## Dependency Graph

```
Phase 1 (Graph) ──────┐
Phase 2 (Vector getv) ─┼──→ Phase 5 (Cross-primitive tests)
Phase 3 (Event docs)  ─┘
Phase 4 (Naming) ← after Phase 1 design settles
```

Phases 1, 2, and 3 are independent and can be parallelized. Phase 5 requires Phases 1 and 2 to be complete.

---

## Acceptance Criteria for VERIFIED

- Graph has real public `get_node_at`, `list_nodes_at`, `neighbors_at` methods
- Vector has `getv` returning full version history
- Event's temporal contract is documented as an intentional specialization
- A user can ask "what did branch X look like at version/time Y?" across all primitives through predictable APIs
- Cross-primitive consistency tests prove that the same `as_of` timestamp produces correct historical views across KV, JSON, Event, Vector, and Graph
- No primitive silently falls back to current-state reads when `as_of` is specified

---

## What This Plan Does NOT Cover

- **CLI `--as-of` parsing** — usability improvement, not required for the architectural claim
- **Snapshot-isolated retrieval** (Claim 6) — separate, much larger problem about making the inverted index version-aware
- **Transactional Graph/Vector** (Claim 2) — separate problem about the OCC transaction boundary
- **Primitive-aware branch merge** (Claim 4) — separate problem about merge semantics
