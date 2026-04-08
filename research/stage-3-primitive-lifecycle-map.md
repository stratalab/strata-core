# Stage 3: Primitive Lifecycle Map

This pass answers a specific architectural question for each primitive:

- what is the real source of truth
- what state is derived or rebuildable
- who owns recovery
- who owns follower refresh
- who owns merge semantics
- who owns freeze / persistence accelerators

The important distinction is between primitives whose semantics are mostly
engine-native and primitives whose correctness depends on optional overlays or
registration side channels.

## Scope

Audited primitives:

- search
- vector
- graph
- JSON
- event
- branch DAG / branch status

Primary code anchors:

- `crates/engine/src/search/recovery.rs`
- `crates/engine/src/search/index.rs`
- `crates/vector/src/recovery.rs`
- `crates/vector/src/store/mod.rs`
- `crates/vector/src/merge_handler.rs`
- `crates/graph/src/lib.rs`
- `crates/graph/src/branch_dag.rs`
- `crates/graph/src/merge_handler.rs`
- `crates/engine/src/primitives/json/mod.rs`
- `crates/engine/src/primitives/json/index.rs`
- `crates/engine/src/primitives/event.rs`
- `crates/engine/src/branch_ops/primitive_merge.rs`
- `crates/engine/src/branch_ops/mod.rs`

## Lifecycle matrix

| Primitive | Durable source of truth | Derived / in-memory state | Recovery owner | Refresh owner | Merge owner | Freeze / cache owner | Architectural judgment |
|---|---|---|---|---|---|---|---|
| Search | Underlying KV / JSON / Event / Graph rows | `InvertedIndex`, mmap-backed `.sidx` segments, `search.manifest` | `SearchSubsystem` | hardcoded in `Database::refresh()` | none as a standalone primitive; other primitives refresh BM25 opportunistically | `SearchSubsystem` -> `freeze_search_index()` | global derived overlay, not engine-native primitive state |
| Vector | KV rows under `TypeTag::Vector` plus config rows | `VectorBackendState`, HNSW backends, `.vec` heap caches, `.hgr` graph caches | `VectorSubsystem` | `VectorRefreshHook` via `RefreshHooks` | engine handler + vector-registered callbacks | `VectorSubsystem` via refresh-hook freeze | split across four lifecycle channels |
| Graph | KV-backed graph rows and packed adjacency lists | on-demand adjacency indexes; no authoritative long-lived cache | none for graph data itself; `GraphSubsystem` only for branch DAG/status | none | engine handler + graph-registered semantic merge plan | none for graph data | graph data is native, but branch-graph behavior is overlay-driven |
| JSON | `JsonDoc` bytes in KV plus `_idx/...` and `_idx_meta/...` rows in KV | BM25 entries in `InvertedIndex` | no JSON subsystem; primary data is already in storage | search refresh via engine path; no JSON-specific hook | engine-owned `JsonMergeHandler` | none for JSON itself | mostly engine-native |
| Event | event rows + `__meta__` + `__tidx__` style per-type index rows in KV | BM25 entries in `InvertedIndex` | no event subsystem; primary data is already in storage | search refresh via engine path; no event-specific hook | engine-owned refusal precheck only | none | engine-native data model with weak branch-merge semantics |
| Branch DAG / status | `_branch_dag` graph on `_system_` branch plus branch metadata in `BranchIndex` | `BranchStatusCache` extension | optional `GraphSubsystem` or executor-side init | none | process-global branch DAG hooks | none | overlay state, not authoritative branch state |

## Per-primitive analysis

### Search

Search is not a first-class data primitive with its own durable truth.
Its truth is derived from other primitives:

- KV
- Event
- Graph
- JSON

`SearchSubsystem` rebuilds the `InvertedIndex` either from mmap-backed search
files or by scanning underlying storage.

Code anchors:

- `crates/engine/src/search/recovery.rs:426`
- `crates/engine/src/search/index.rs:1377`
- `crates/engine/src/search/index.rs:1438`

Persisted-format surface:

- `data_dir/search/search.manifest`
- `data_dir/search/seg_{id}.sidx`

Code anchors:

- `crates/engine/src/search/manifest.rs:1`
- `crates/engine/src/search/segment.rs:1`
- `crates/engine/src/search/index.rs:564`

Ownership model:

- recovery is declarative through `SearchSubsystem`
- follower refresh is not declarative through `RefreshHook`; it is hardcoded in
  `Database::refresh()`
- merge correctness is not search-owned; search catches up from primitive
  write paths and post-merge refresh hooks

Judgment:

- search is a global derived overlay with a reasonably coherent recovery story
- the seam is that refresh is hardcoded engine logic while freeze is subsystem
  logic

### Vector

Vector's durable truth is KV state, not vector-native WAL or snapshot files.
The live model is:

- collection configs in KV
- vector records in KV
- in-memory backends in `VectorBackendState`
- optional `.vec` and `.hgr` disk caches

Code anchors:

- `crates/vector/src/store/mod.rs:1`
- `crates/vector/src/recovery.rs:464`

Persisted-format surface:

- KV rows under `TypeTag::Vector`
- `data_dir/vectors/{branch_hex}/{space}/{collection}.vec`
- `data_dir/vectors/{branch_hex}/{space}/{collection}_graphs/seg_{id}.hgr`

Code anchors:

- `crates/vector/src/recovery.rs:49`
- `crates/vector/src/lib.rs:72`

Lifecycle ownership is split:

- recovery: `VectorSubsystem`
- follower refresh: `VectorRefreshHook` registered into `RefreshHooks`
- merge: engine `VectorMergeHandler` plus vector-registered callbacks
- freeze: `VectorSubsystem::freeze` calling `db.freeze_vector_heaps()`, which
  loops over refresh hooks

Code anchors:

- `crates/vector/src/recovery.rs:466`
- `crates/vector/src/recovery.rs:487`
- `crates/engine/src/database/lifecycle.rs:368`
- `crates/engine/src/branch_ops/primitive_merge.rs:943`
- `crates/vector/src/merge_handler.rs:60`

This makes vector the most composition-sensitive primitive in the system.
Its semantics are spread across:

- subsystem registration
- database extensions
- refresh-hook registration
- process-global merge callback registration

And on top of that, the codebase still contains dormant vector-native WAL and
snapshot stacks.

Judgment:

- vector is the clearest example of a primitive whose data model is sound but
  whose lifecycle ownership is fragmented

### Graph

Ordinary graph data is stored directly in the engine's KV model.
There is no graph-specific recovery subsystem for normal graph state because
the packed adjacency lists are themselves the durable state.

Code anchors:

- `crates/graph/src/lib.rs:1`
- `crates/engine/src/branch_ops/primitive_merge.rs:1129`

Graph search integration is intrinsic to graph write paths, not a separate
subsystem:

- node writes index graph search text directly
- deletes deindex directly

Code anchors:

- `crates/graph/src/lib.rs:231`
- `crates/graph/src/lifecycle.rs:109`

Merge ownership is overlay-driven:

- if graph semantic merge is registered, `GraphMergeHandler` dispatches to it
- otherwise the engine falls back to a tactical refusal of divergent graph
  merges plus generic classification

Code anchors:

- `crates/graph/src/merge_handler.rs:45`
- `crates/engine/src/branch_ops/primitive_merge.rs:1098`

The important split is that graph data and branch DAG/status are not the same
thing:

- graph data is engine-native and persists in normal storage
- branch DAG/status is graph-powered overlay behavior hanging off the engine

Judgment:

- ordinary graph storage is relatively coherent
- branch-related graph semantics are not

### JSON

JSON is mostly engine-native.
Its durable truth is:

- `JsonDoc` rows in storage
- JSON secondary index metadata in `_idx_meta/{space}`
- JSON secondary index entries in `_idx/{space}/{index}`

Code anchors:

- `crates/engine/src/primitives/json/mod.rs:1`
- `crates/engine/src/primitives/json/index.rs:184`
- `crates/engine/src/primitives/json/index.rs:190`

Write-path behavior:

- JSON document writes happen in transactions
- JSON secondary index entries are updated synchronously in the same transactional
  path
- BM25 entries are updated after commit through the search extension

Code anchors:

- `crates/engine/src/primitives/json/mod.rs:304`
- `crates/engine/src/primitives/json/mod.rs:321`
- `crates/engine/src/primitives/json/mod.rs:1388`

Merge ownership is engine-owned, not externally registered:

- `JsonMergeHandler` performs per-document recursive object merge
- index entries for `_idx/...` are emitted atomically during merge plan
- BM25 refresh is best-effort post-commit and self-heals on next open

Code anchors:

- `crates/engine/src/branch_ops/json_merge.rs`
- `crates/engine/src/branch_ops/primitive_merge.rs:326`
- `crates/engine/src/branch_ops/primitive_merge.rs:502`

There is no JSON subsystem because JSON's primary data is already native engine
storage.

Judgment:

- JSON is one of the cleaner primitives architecturally
- its main seams are around search refresh and coarse transaction semantics,
  not lifecycle ownership

### Event

EventLog is also mostly engine-native.
Its durable truth includes:

- immutable event rows
- per-space event metadata row `__meta__`
- per-type index rows via `Key::new_event_type_idx(...)`

Code anchors:

- `crates/engine/src/primitives/event.rs:35`
- `crates/engine/src/primitives/event.rs:331`
- `crates/engine/src/primitives/event.rs:370`

Write-path behavior:

- append uses transaction-time CAS-like metadata update
- write path ensures space registration
- BM25 search index is updated after successful commit

Code anchors:

- `crates/engine/src/primitives/event.rs:355`
- `crates/engine/src/primitives/event.rs:368`
- `crates/engine/src/primitives/event.rs:398`

There is no event subsystem because the event stream itself is stored in normal
engine storage.

Merge ownership is intentionally minimal:

- `EventMergeHandler` only refuses divergent concurrent appends
- there is no semantic branch merge for event streams

Code anchors:

- `crates/engine/src/branch_ops/primitive_merge.rs:838`
- `crates/engine/src/branch_ops/mod.rs:246`

Judgment:

- event storage is coherent
- branch semantics are the weak point: correctness currently means refusal,
  not merge capability

### Branch DAG and branch status

Branch DAG/status is the least authoritative of the audited primitive-adjacent
systems.

Durable state is split across two different models:

- actual branch existence and metadata in `BranchIndex`
- lineage and richer statuses in `_branch_dag` on the `_system_` branch

Code anchors:

- `crates/engine/src/primitives/branch/index.rs`
- `crates/graph/src/branch_dag.rs`

Derived state:

- `BranchStatusCache` as a database extension

Code anchor:

- `crates/graph/src/branch_status_cache.rs`

Lifecycle ownership is fragmented:

- `GraphSubsystem` can initialize system branch + load status cache
- executor often calls `init_system_branch()` directly instead
- branch lifecycle recording depends on process-global DAG hook registration

Code anchors:

- `crates/graph/src/branch_dag.rs:848`
- `crates/executor/src/api/mod.rs:219`
- `crates/graph/src/dag_hook_impl.rs:51`

Enforcement is weak:

- rich statuses exist in DAG land
- engine branch metadata still only models `Active`
- cache presence does not make branch status authoritative for write blocking

Code anchors:

- `crates/engine/src/primitives/branch/index.rs:75`
- `crates/graph/src/branch_status_cache.rs:36`

Judgment:

- branch DAG/status is overlay state used for history and merge-base assistance
- it should not be described as authoritative branch lifecycle state

## Intrinsic vs overlay classification

### Mostly intrinsic engine primitives

- JSON
- Event
- ordinary Graph data

These primitives store their primary truth directly in engine storage and do
not require a custom recovery subsystem to be logically correct.

### Derived overlay systems

- Search
- Branch DAG / branch status

These systems are useful, but their truth is derived from other data or
best-effort hook execution.

### Split-authority primitive

- Vector

Vector is the only audited primitive whose correctness-adjacent lifecycle is
spread across all of these at once:

- engine storage
- subsystem recovery
- refresh-hook registration
- merge callback registration
- mmap cache management

## Persisted-format census

### Search

- `search/search.manifest`
- `search/seg_{id}.sidx`

### Vector

- KV `TypeTag::Vector` rows
- `vectors/{branch_hex}/{space}/{collection}.vec`
- `vectors/{branch_hex}/{space}/{collection}_graphs/seg_{id}.hgr`
- dormant: vector-native snapshot / WAL code still exists

### Graph

- normal graph rows in KV / packed adjacency storage
- `_system_` branch `_branch_dag` graph for lineage overlay

### JSON

- `TypeTag::Json` document rows
- `_idx/{space}/{index}` secondary index rows
- `_idx_meta/{space}` index metadata rows

### Event

- `TypeTag::Event` event rows
- `__meta__` metadata row
- per-type index rows from `Key::new_event_type_idx`

### Branch status

- no independent persisted format beyond DAG node properties and branch metadata
- `BranchStatusCache` itself is in-memory only

## Gap list from the primitive lens

1. Vector semantics are still split across subsystem, refresh, merge, and dead
   shadow stacks.
2. Search refresh is hardcoded engine behavior rather than part of the same
   subsystem contract that owns search recovery/freeze.
3. Graph semantic merge is not intrinsic; it depends on global registration.
4. Event streams still have no semantic branch merge, only divergence refusal.
5. Branch DAG/status remains advisory overlay state rather than authoritative
   lifecycle state.
6. JSON is the cleanest primitive here, but even it relies on best-effort BM25
   refresh outside its transactional truth.

## Bottom line

From the primitive lens, Strata is not uniformly broken.

It has three different classes of primitive architecture:

- native storage primitives with relatively coherent ownership: JSON, Event,
  ordinary Graph
- derived overlays whose correctness is rebuild-based: Search, branch DAG/status
- one heavily split primitive lifecycle: Vector

That suggests the remediation program should not treat all primitives the same.
The best targets are:

1. make vector single-path
2. make graph merge / branch DAG registration explicit and intrinsic
3. decide whether branch DAG/status is advisory or authoritative
4. unify search lifecycle so recovery, refresh, and freeze follow one contract
