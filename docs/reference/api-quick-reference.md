# API Quick Reference

Every method on the `Strata` struct, grouped by category.

## Database

| Method | Signature | Returns |
|--------|-----------|---------|
| `open` | `(path: impl AsRef<Path>) -> Result<Self>` | New Strata instance |
| `open_with` | `(path: impl AsRef<Path>, opts: OpenOptions) -> Result<Self>` | New Strata instance with options |
| `cache` | `() -> Result<Self>` | Ephemeral in-memory instance |
| `new_handle` | `() -> Result<Self>` | Independent handle to same database |
| `ping` | `() -> Result<String>` | Version string |
| `info` | `() -> Result<DatabaseInfo>` | Database statistics |
| `flush` | `() -> Result<()>` | Flushes pending writes |
| `compact` | `() -> Result<()>` | Triggers compaction |
| `time_range` | `(branch: Option<&str>) -> Result<Option<(u64, u64)>>` | Oldest/latest timestamps |

## Configuration

| Method | Signature | Returns | Notes |
|--------|-----------|---------|-------|
| `config` | `() -> StrataConfig` | Current config snapshot | Read-only clone |
| `configure_model` | `(endpoint: &str, model: &str, api_key: Option<&str>, timeout_ms: Option<u64>) -> Result<()>` | | Persisted to `strata.toml` |
| `auto_embed_enabled` | `() -> bool` | Whether auto-embed is on | |
| `set_auto_embed` | `(enabled: bool) -> Result<()>` | | Persisted to `strata.toml` |
| `access_mode` | `() -> AccessMode` | ReadWrite or ReadOnly | |
| `durability_counters` | `() -> Option<WalCounters>` | WAL stats | `None` for cache databases |

## Branch Context

| Method | Signature | Returns |
|--------|-----------|---------|
| `current_branch` | `() -> &str` | Current branch name |
| `set_branch` | `(name: &str) -> Result<()>` | Switches current branch |
| `create_branch` | `(name: &str) -> Result<()>` | Creates empty branch |
| `list_branches` | `() -> Result<Vec<String>>` | All branch names |
| `delete_branch` | `(name: &str) -> Result<()>` | Deletes branch + data |
| `fork_branch` | `(dest: &str) -> Result<()>` | Copies current branch to dest |
| `branches` | `() -> Branches<'_>` | Power API handle |

## Space Context

| Method | Signature | Returns |
|--------|-----------|---------|
| `current_space` | `() -> &str` | Current space name |
| `set_space` | `(name: &str) -> Result<()>` | Switches current space |
| `list_spaces` | `() -> Result<Vec<String>>` | All space names in current branch |
| `delete_space` | `(name: &str) -> Result<()>` | Deletes empty space |
| `delete_space_force` | `(name: &str) -> Result<()>` | Deletes space and all its data |

> **Note:** All data methods (KV, Event, State, JSON, Vector) operate on the current space set via `set_space`. The default space is `"default"`.

## KV Store

| Method | Signature | Returns | Notes |
|--------|-----------|---------|-------|
| `kv_put` | `(key: &str, value: impl Into<Value>) -> Result<u64>` | Version | Creates or overwrites |
| `kv_get` | `(key: &str) -> Result<Option<Value>>` | Value or None | |
| `kv_get_at` | `(key: &str, as_of_ts: u64) -> Result<Option<Value>>` | Historical value or None | Time-travel read |
| `kv_getv` | `(key: &str) -> Result<Option<Vec<VersionedValue>>>` | Version history or None | Newest first |
| `kv_delete` | `(key: &str) -> Result<bool>` | Whether key existed | |
| `kv_list` | `(prefix: Option<&str>) -> Result<Vec<String>>` | Key names | |
| `kv_list_at` | `(prefix: Option<&str>, as_of_ts: u64) -> Result<Vec<String>>` | Historical key names | Time-travel list |

## Event Log

| Method | Signature | Returns | Notes |
|--------|-----------|---------|-------|
| `event_append` | `(event_type: &str, payload: Value) -> Result<u64>` | Sequence number | Payload must be Object |
| `event_get` | `(sequence: u64) -> Result<Option<VersionedValue>>` | Event or None | |
| `event_get_by_type` | `(event_type: &str) -> Result<Vec<VersionedValue>>` | All events of type | |
| `event_list_at` | `(event_type: Option<&str>, as_of_ts: u64) -> Result<Vec<Event>>` | Events before timestamp | Time-travel list |
| `event_len` | `() -> Result<u64>` | Total event count | |

## State Cell

| Method | Signature | Returns | Notes |
|--------|-----------|---------|-------|
| `state_set` | `(cell: &str, value: impl Into<Value>) -> Result<u64>` | Version | Unconditional write |
| `state_get` | `(cell: &str) -> Result<Option<Value>>` | Value or None | |
| `state_get_at` | `(cell: &str, as_of_ts: u64) -> Result<Option<Value>>` | Historical value or None | Time-travel read |
| `state_getv` | `(cell: &str) -> Result<Option<Vec<VersionedValue>>>` | Version history or None | Newest first |
| `state_init` | `(cell: &str, value: impl Into<Value>) -> Result<u64>` | Version | Only if absent |
| `state_cas` | `(cell: &str, expected: Option<u64>, value: impl Into<Value>) -> Result<Option<u64>>` | New version or None | CAS |

## JSON Store

| Method | Signature | Returns | Notes |
|--------|-----------|---------|-------|
| `json_set` | `(key: &str, path: &str, value: impl Into<Value>) -> Result<u64>` | Version | Use "$" for root |
| `json_get` | `(key: &str, path: &str) -> Result<Option<Value>>` | Value or None | |
| `json_get_at` | `(key: &str, as_of_ts: u64) -> Result<Option<Value>>` | Historical value or None | Time-travel read |
| `json_getv` | `(key: &str) -> Result<Option<Vec<VersionedValue>>>` | Version history or None | Newest first |
| `json_delete` | `(key: &str, path: &str) -> Result<u64>` | Count deleted | |
| `json_list` | `(prefix: Option<String>, cursor: Option<String>, limit: u64) -> Result<(Vec<String>, Option<String>)>` | Keys + cursor | |

## Vector Store

| Method | Signature | Returns | Notes |
|--------|-----------|---------|-------|
| `vector_create_collection` | `(name: &str, dimension: u64, metric: DistanceMetric) -> Result<u64>` | Version | |
| `vector_delete_collection` | `(name: &str) -> Result<bool>` | Whether it existed | |
| `vector_list_collections` | `() -> Result<Vec<CollectionInfo>>` | All collections | |
| `vector_collection_stats` | `(collection: &str) -> Result<CollectionInfo>` | Collection details | Includes `index_type`, `memory_bytes` |
| `vector_upsert` | `(collection: &str, key: &str, vector: Vec<f32>, metadata: Option<Value>) -> Result<u64>` | Version | |
| `vector_batch_upsert` | `(collection: &str, entries: Vec<BatchVectorEntry>) -> Result<Vec<u64>>` | Versions | Atomic bulk insert |
| `vector_get` | `(collection: &str, key: &str) -> Result<Option<VersionedVectorData>>` | Vector data or None | |
| `vector_get_at` | `(collection: &str, key: &str, as_of_ts: u64) -> Result<Option<VectorEntry>>` | Historical vector or None | Time-travel read |
| `vector_delete` | `(collection: &str, key: &str) -> Result<bool>` | Whether it existed | |
| `vector_search` | `(collection: &str, query: Vec<f32>, k: u64) -> Result<Vec<VectorMatch>>` | Top-k matches | 8 metadata filter operators |
| `vector_search_at` | `(collection: &str, query: Vec<f32>, k: u64, as_of_ts: u64) -> Result<Vec<VectorMatch>>` | Historical top-k matches | Temporal HNSW filtering |

## Graph Store

| Method | Signature | Returns | Notes |
|--------|-----------|---------|-------|
| `graph_create` | `(graph: &str) -> Result<()>` | | Creates empty graph |
| `graph_create_with_policy` | `(graph: &str, cascade_policy: Option<&str>) -> Result<()>` | | `"cascade"`, `"detach"`, or `"ignore"` |
| `graph_delete` | `(graph: &str) -> Result<()>` | | Deletes graph + all data |
| `graph_list` | `() -> Result<Vec<String>>` | Graph names | |
| `graph_get_meta` | `(graph: &str) -> Result<Option<Value>>` | Metadata or None | |
| `graph_add_node` | `(graph: &str, node_id: &str, entity_ref: Option<&str>, properties: Option<Value>) -> Result<()>` | | Creates or updates |
| `graph_add_node_typed` | `(graph: &str, node_id: &str, entity_ref: Option<&str>, properties: Option<Value>, object_type: Option<&str>) -> Result<()>` | | With ontology type |
| `graph_get_node` | `(graph: &str, node_id: &str) -> Result<Option<Value>>` | Node data or None | |
| `graph_remove_node` | `(graph: &str, node_id: &str) -> Result<()>` | | Cascades to edges |
| `graph_list_nodes` | `(graph: &str) -> Result<Vec<String>>` | Node IDs | |
| `graph_add_edge` | `(graph: &str, src: &str, dst: &str, edge_type: &str, weight: Option<f64>, properties: Option<Value>) -> Result<()>` | | Directed typed edge |
| `graph_remove_edge` | `(graph: &str, src: &str, dst: &str, edge_type: &str) -> Result<()>` | | |
| `graph_bulk_insert` | `(graph: &str, nodes: &[...], edges: &[...]) -> Result<(u64, u64)>` | (nodes, edges) inserted | Chunked transactions |
| `graph_neighbors` | `(graph: &str, node_id: &str, direction: &str, edge_type: Option<&str>) -> Result<Vec<GraphNeighborHit>>` | Neighbor list | Direction: `"outgoing"`, `"incoming"`, `"both"` |
| `graph_bfs` | `(graph: &str, start: &str, max_depth: usize, max_nodes: Option<usize>, edge_types: Option<Vec<String>>, direction: Option<&str>) -> Result<GraphBfsResult>` | BFS result | visited + depths + edges |

## Graph Ontology

| Method | Signature | Returns | Notes |
|--------|-----------|---------|-------|
| `graph_define_object_type` | `(graph: &str, definition: Value) -> Result<()>` | | Draft mode only |
| `graph_get_object_type` | `(graph: &str, name: &str) -> Result<Option<Value>>` | Definition or None | |
| `graph_list_object_types` | `(graph: &str) -> Result<Vec<String>>` | Type names | |
| `graph_delete_object_type` | `(graph: &str, name: &str) -> Result<()>` | | Draft mode only |
| `graph_define_link_type` | `(graph: &str, definition: Value) -> Result<()>` | | Draft mode only |
| `graph_get_link_type` | `(graph: &str, name: &str) -> Result<Option<Value>>` | Definition or None | |
| `graph_list_link_types` | `(graph: &str) -> Result<Vec<String>>` | Type names | |
| `graph_delete_link_type` | `(graph: &str, name: &str) -> Result<()>` | | Draft mode only |
| `graph_freeze_ontology` | `(graph: &str) -> Result<()>` | | Enables validation |
| `graph_ontology_status` | `(graph: &str) -> Result<Option<Value>>` | Status or None | `"draft"` or `"frozen"` |
| `graph_ontology_summary` | `(graph: &str) -> Result<Option<Value>>` | Full summary or None | Types + counts |
| `graph_nodes_by_type` | `(graph: &str, object_type: &str) -> Result<Vec<String>>` | Node IDs | |

## Search

| Method | Signature | Returns | Notes |
|--------|-----------|---------|-------|
| `search` | `(query: &str, k: Option<u64>, primitives: Option<Vec<String>>, time_range: Option<TimeRangeInput>, mode: Option<String>, expand: Option<bool>, rerank: Option<bool>) -> Result<Vec<SearchResultHit>>` | Search hits | Cross-primitive search |

`TimeRangeInput` has `start` and `end` fields (ISO 8601 strings). `mode` can be `"keyword"` or `"hybrid"` (default). `expand` and `rerank` default to auto (enabled when a model is configured).

## Branch Operations (Low-Level)

| Method | Signature | Returns |
|--------|-----------|---------|
| `branch_create` | `(branch_id: Option<String>, metadata: Option<Value>) -> Result<(BranchInfo, u64)>` | Info + version |
| `branch_get` | `(branch: &str) -> Result<Option<VersionedBranchInfo>>` | Branch info or None |
| `branch_list` | `(state: Option<BranchStatus>, limit: Option<u64>, offset: Option<u64>) -> Result<Vec<VersionedBranchInfo>>` | Branch info list |
| `branch_exists` | `(branch: &str) -> Result<bool>` | Whether branch exists |
| `branch_delete` | `(branch: &str) -> Result<()>` | Deletes branch |

## Bundle Operations

| Method | Signature | Returns |
|--------|-----------|---------|
| `branch_export` | `(branch_id: &str, path: &str) -> Result<BranchExportResult>` | Export info |
| `branch_import` | `(path: &str) -> Result<BranchImportResult>` | Import info |
| `branch_validate_bundle` | `(path: &str) -> Result<BundleValidateResult>` | Validation info |

## Branches Power API

Methods on the `Branches` handle returned by `db.branches()`:

| Method | Signature | Returns |
|--------|-----------|---------|
| `list` | `() -> Result<Vec<String>>` | Branch names |
| `exists` | `(name: &str) -> Result<bool>` | Whether branch exists |
| `create` | `(name: &str) -> Result<()>` | Creates empty branch |
| `delete` | `(name: &str) -> Result<()>` | Deletes branch |
| `fork` | `(source: &str, dest: &str) -> Result<ForkInfo>` | Copies branch data |
| `diff` | `(branch1: &str, branch2: &str) -> Result<BranchDiff>` | Compares two branches |
| `merge` | `(source: &str, target: &str, strategy: MergeStrategy) -> Result<MergeInfo>` | Merges source into target |

## Session

| Method | Signature | Returns |
|--------|-----------|---------|
| `Session::new` | `(db: Arc<Database>) -> Self` | New session |
| `execute` | `(cmd: Command) -> Result<Output>` | Command result |
| `in_transaction` | `() -> bool` | Whether a txn is active |
