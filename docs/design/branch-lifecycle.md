# Branch Lifecycle: DAG Tracking, Status Management, and Metadata

**Status:** Epics 1–2 implemented; remaining epics deferred
**Issue:** #1462
**Date:** 2026-03-15

---

## 1. Problem Statement

Strata branches currently have no lifecycle beyond create/delete. There is no way to:

- **Track lineage** — Which branch was forked from where? What merges happened?
- **Lock branches** — A merged or archived branch can still receive writes, leading to data inconsistency.
- **Attach metadata** — No messages, creator attribution, or comments on branches or operations.
- **Query history** — No way to ask "what happened to this branch?" or "show me all merges into main."
- **Filter branches** — `branch_list` returns all branches with no filtering by status, parent, or time range.

### Why This Matters

Strata is designed as a data engineering workspace where branches represent units of work (cleaning pipelines, feature experiments, staging areas). Without lifecycle management, users must maintain external tracking systems — defeating the purpose of branch-based workflows.

---

## 2. Current State

### 2.1 Branch Infrastructure

**BranchMetadata** (`crates/engine/src/primitives/branch/index.rs:90-149`):
```rust
pub struct BranchMetadata {
    pub name: String,
    pub branch_id: String,              // Random UUID
    pub parent_branch: Option<String>,  // Set on fork, but not queryable
    pub status: BranchStatus,           // Only `Active` variant exists
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub completed_at: Option<Timestamp>,
    pub error: Option<String>,
    pub version: u64,
}
```

**BranchStatus** today has a single variant:
```rust
pub enum BranchStatus { Active }
```

**Branch operations** live in `crates/engine/src/branch_ops.rs`:
- `fork_branch(db, source, destination) -> ForkInfo` — Copies all data, returns key/space counts
- `merge_branches(db, source, target, strategy) -> MergeInfo` — Diffs and applies changes
- `diff_branches_with_options(db, a, b, options) -> BranchDiffResult` — Structured diff

**Key gap**: `fork_branch` sets `parent_branch` on metadata but this relationship is never queryable. `merge_branches` returns `MergeInfo` but it's ephemeral — the merge event is not recorded anywhere.

### 2.2 Graph Primitives

The graph engine (`crates/engine/src/graph/`) provides everything needed for the branch DAG:

- **Nodes** with arbitrary JSON properties and `object_type` field
- **Directed edges** with labels (edge types), weight, and properties
- **Branch-scoped storage** in the `_graph_` namespace
- **Traversal**: BFS with depth/node limits, direction control, edge type filtering
- **Analytics**: WCC, PageRank, SSSP, CDLP, LCC
- **Packed adjacency format** for efficient edge storage (~27x reduction)
- **Bulk insert** for batch loading

Graph data is stored per-branch using the KV layer with keys like `{graph}/n/{node_id}` and packed adjacency lists at `{graph}/fwd/{node_id}` / `{graph}/rev/{node_id}`.

### 2.3 Database Initialization

`Database::open()` flow (`crates/engine/src/database/mod.rs`):
1. Load/create `strata.toml`
2. Check singleton registry
3. Acquire exclusive file lock
4. WAL recovery
5. Open WAL writer
6. Spawn flush thread (Standard mode)
7. Create `TransactionCoordinator`
8. Apply storage limits
9. Register in global registry
10. Recovery participants (vector, search index rebuild)
11. Enable `InvertedIndex` extension

**No "main" branch is auto-created.** The default branch maps to nil UUID `[0u8; 16]` and exists implicitly.

**Extension system**: `DashMap<TypeId, Arc<dyn Any + Send + Sync>>` with lazy `T::default()` initialization via `db.extension::<T>()`.

### 2.4 `_system_` Prefix Convention

Already established for vector collections (`crates/engine/src/database/config.rs:17-24`):
```rust
pub const SHADOW_KV: &str = "_system_embed_kv";
pub const SHADOW_JSON: &str = "_system_embed_json";
pub const SHADOW_EVENT: &str = "_system_embed_event";
pub const SHADOW_STATE: &str = "_system_embed_state";
```

Collection name validation (`crates/engine/src/primitives/vector/collection.rs`):
- `validate_collection_name()` rejects names starting with `_`
- `validate_system_collection_name()` requires `_system_` prefix

### 2.5 Executor Pattern

Commands are dispatched via a `match` on `Command` enum variants in `Executor::execute()` (`crates/executor/src/executor.rs:157+`). Each handler:
1. Calls `require_branch_exists(p, &branch)?`
2. Converts to core types via `to_core_branch_id()`
3. Validates inputs (key, value, limits)
4. Calls engine primitive
5. Runs post-commit hooks (auto-embed, indexing)

**`is_write()` method** on `Command` (`command.rs:1566-1640`) determines read-only mode enforcement.

### 2.6 Write Operations Inventory

All write operations that need branch status guards:

| Primitive | Write Operations | File |
|-----------|-----------------|------|
| **KV** | `put`, `delete`, `batch_put` | `primitives/kv.rs` |
| **JSON** | `create`, `set`, `set_or_create`, `batch_set_or_create`, `delete_at_path`, `destroy` | `primitives/json.rs` |
| **State** | `init`, `set`, `cas`, `batch_set`, `delete` | `primitives/state.rs` |
| **Event** | `append`, `batch_append` | `primitives/event.rs` |
| **Vector** | `create_collection`, `insert`, `batch_insert`, `delete`, `delete_collection` | `primitives/vector/store.rs` |
| **Graph** | `add_node`, `remove_node`, `add_edge`, `remove_edge`, ontology mutations | `graph/mod.rs` |

All writes flow through `Database::transaction(branch_id, |txn| ...)` or `Database::transaction_with_version()`.

### 2.7 BackgroundScheduler

Exists at `crates/engine/src/background.rs` with priority queue, 2 workers, 4096 max queue depth, and backpressure. Currently only used by the embed hook (`embed_hook.rs:162`). GC, compaction, TTL, and checkpointing are all manual-only today. A full maintenance design exists at `docs/design/maintenance.md` but is unimplemented.

### 2.8 Error System

10-code canonical model (`crates/core/src/error.rs`). Key variants for this work:
- `StrataError::InvalidOperation { entity_ref, reason }` — for write-to-non-active-branch
- `StrataError::InvalidInput { message }` — for rejected branch names
- `StrataError::NotFound { entity_ref }` — for missing branches
- `StrataError::ConstraintViolation` via `ConstraintReason::BranchNotActive` — already defined but unused

---

## 3. Design Decisions

### 3.1 Branch DAG on `_system_` Branch

Track branch lineage via a `_branch_dag` graph on a reserved `_system_` branch, using the same graph primitives as user data.

**Why a graph, not metadata fields:**
- Graph primitive already has directed edges, properties, BFS, path finding, analytics
- DAG traversal (ancestors, descendants, merge paths) comes free
- Events-as-nodes model handles repeated merges between same branches
- Future-proof for other internal data (agent context, learned schemas, validation rules)

**Why `_system_` branch, not a global scope:**
- Preserves "everything is branch-scoped" invariant
- Follows existing `_system_` prefix convention from vector collections
- Branch isolation means `_system_` data uses the same storage/transaction machinery

**Security model**: `_system_` must be completely hidden from user-facing APIs. If an AI agent discovers it, it will try to write to it.

### 3.2 Events as DAG Nodes

Forks and merges are **nodes** in the DAG, not just edges. This gives each event:
- A unique ID (referenceable)
- Properties (who, when, why, strategy, results)
- Support for repeated merges between same branches
- Natural audit trail

Three node `object_type` values: `branch`, `fork`, `merge`.

### 3.3 Branch Status on the DAG

Status (`Active`, `Merged`, `Archived`) stored as a property on the branch's DAG node. The DAG is the single source of truth for lifecycle state.

**Why not on `BranchMetadata`:** BranchMetadata is a flat struct with no query capability. Putting status on the DAG means status queries, lineage queries, and audit queries all use the same mechanism.

### 3.4 Status-Based Write Guards

**Interception point**: Executor handler level, before any primitive call. This is the earliest and highest-level point, consistent with existing `require_branch_exists()` pattern.

**Not inside transactions**: A write guard check inside `Database::transaction()` would be too low-level and would require the DAG (which lives on a different branch) to be queried within the user branch's transaction context.

**Caching**: DAG status lookups would be expensive on every write. Use an in-memory `DashMap<String, BranchStatus>` cache (as a database extension) that's populated from the DAG at init and updated on status transitions.

### 3.5 No Squash Merge

MVCC + time-travel means squashing destroys intermediate versions and breaks `as_of` queries. `branch_diff` already provides net-result views.

---

## 4. Implementation Plan

### Phase 1: `_system_` Branch Infrastructure

**Goal**: Reserve the `_system_` namespace, auto-create the branch, hide it from all user-facing APIs, create the `_branch_dag` graph.

#### 1.1 Reserve `_system_` Branch Name

**File: `crates/executor/src/handlers/branch.rs`**

Add to `validate_branch_name()` (line 44):
```rust
if name.starts_with("_system_") {
    return Err(Error::ConstraintViolation {
        reason: "Branch names starting with '_system_' are reserved for internal use".into(),
    });
}
```

**Affected operations** (all in same file):
- `branch_create` (line 93) — already calls `validate_branch_name()`
- `branch_fork` (line 194) — validate both source and destination
- `branch_merge` (line 234) — validate both source and target
- `branch_delete` (line 163) — reject `_system_` deletion

#### 1.2 Auto-Create `_system_` Branch at Database Init

**File: `crates/engine/src/database/mod.rs`**

Add a new method `ensure_system_branch()` called during `open_finish()` (after subsystem recovery, before returning the `Arc<Database>`):

```rust
fn ensure_system_branch(db: &Arc<Database>) -> StrataResult<()> {
    let branch_index = BranchIndex::new(db.clone());
    if !branch_index.exists("_system_")? {
        branch_index.create_branch("_system_")?;
    }
    Ok(())
}
```

**Call sites:**
- `open_finish()` — after subsystem recovery has run (disk databases)
- `cache()` — after `InvertedIndex` setup (ephemeral databases)

**Idempotent**: Safe for existing databases that already have it.

#### 1.3 Hide `_system_` from User-Facing APIs

**File: `crates/executor/src/handlers/branch.rs`**

- `branch_list` (line 127): Filter `_system_` from results before returning
- `branch_get` (line 118): Return `None` for `_system_`
- `branch_exists` (line 153): Return `false` for `_system_`

**File: `crates/executor/src/handlers/mod.rs`**

Add helper:
```rust
pub(crate) fn reject_system_branch(branch: &BranchId) -> Result<()> {
    if branch.as_str().starts_with("_system_") {
        return Err(Error::ConstraintViolation {
            reason: "Cannot access system branches directly".into(),
        });
    }
    Ok(())
}
```

Call from all data handlers (KV, JSON, State, Event, Vector, Graph) before processing writes and reads on `_system_` branches.

#### 1.4 Create `_branch_dag` Graph at Init

**New file: `crates/engine/src/branch_dag.rs`**

```rust
const SYSTEM_BRANCH: &str = "_system_";
const BRANCH_DAG_GRAPH: &str = "_branch_dag";

pub fn ensure_branch_dag(db: &Arc<Database>) -> StrataResult<()> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    // Create graph if it doesn't exist (idempotent)
    match graph.create_graph(branch_id, BRANCH_DAG_GRAPH, None) {
        Ok(()) => Ok(()),
        Err(e) if e.is_already_exists() => Ok(()),
        Err(e) => Err(e),
    }
}
```

Call from `ensure_system_branch()` after branch creation.

#### 1.5 Testing

- `_system_` branch rejected by `branch_create`, `branch_fork`, `branch_merge`, `branch_delete`
- `_system_` hidden from `branch_list`, `branch_get`, `branch_exists`
- Data APIs (kv_set, json_set, etc.) reject `_system_` branch
- `_system_` branch auto-created on `Database::open()` and `Database::cache()`
- `_branch_dag` graph exists after init
- Idempotent: double-init is safe

---

### Phase 2: Branch DAG Tracking

**Goal**: Record branch creation, forks, and merges as nodes/edges in the DAG. Provide internal helper functions for DAG access.

#### 2.1 DAG Node Types

All nodes stored via `GraphStore::add_node()` with `NodeData { object_type, properties, entity_ref: None }`.

**Branch nodes:**
```
node_id: branch name (e.g., "main", "clean/customers-2026-03-14")
object_type: "branch"
properties: {
    "branch_id": "<uuid>",
    "status": "active" | "merged" | "archived",
    "created_at": <timestamp_micros>,
    "updated_at": <timestamp_micros>,
    "message": null | "<comment>",
    "creator": null | "<identifier>"
}
```

**Fork event nodes:**
```
node_id: "fork:<uuid>"
object_type: "fork"
properties: {
    "timestamp": <timestamp_micros>,
    "message": null | "<why>",
    "creator": null | "<who>"
}
```

**Merge event nodes:**
```
node_id: "merge:<uuid>"
object_type: "merge"
properties: {
    "timestamp": <timestamp_micros>,
    "strategy": "last_writer_wins" | "strict",
    "keys_applied": <count>,
    "spaces_merged": <count>,
    "conflicts": <count>,
    "message": null | "<why/review notes>",
    "creator": null | "<who>"
}
```

#### 2.2 DAG Edge Schema

Four edge types connecting branches to events:

```
Fork:   "main" ──[parent]──> "fork:f-<uuid>" ──[child]──> "feature"
Merge:  "feature" ──[source]──> "merge:m-<uuid>" ──[target]──> "main"
```

#### 2.3 Internal DAG Helper Module

**File: `crates/engine/src/branch_dag.rs`** (extend from Phase 1)

Types:
```rust
pub struct DagEventId(String);  // "fork:<uuid>" or "merge:<uuid>"

pub enum BranchStatus {
    Active,
    Merged,
    Archived,
}

pub struct MergeRecord {
    pub event_id: DagEventId,
    pub source: String,
    pub target: String,
    pub timestamp: u64,
    pub strategy: String,
    pub keys_applied: u64,
    pub conflicts: u64,
    pub message: Option<String>,
    pub creator: Option<String>,
}

pub struct ForkRecord {
    pub event_id: DagEventId,
    pub parent: String,
    pub child: String,
    pub timestamp: u64,
    pub message: Option<String>,
    pub creator: Option<String>,
}

pub struct BranchDagInfo {
    pub name: String,
    pub branch_id: String,
    pub status: BranchStatus,
    pub created_at: u64,
    pub message: Option<String>,
    pub creator: Option<String>,
    pub forked_from: Option<ForkRecord>,
    pub merges: Vec<MergeRecord>,
    pub children: Vec<String>,
}
```

Functions:
```rust
/// Add a branch node to the DAG (called on branch_create)
pub fn dag_add_branch(
    db: &Arc<Database>,
    name: &str,
    branch_id: &str,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<()>

/// Record a fork event: parent -> fork_node -> child
pub fn dag_record_fork(
    db: &Arc<Database>,
    parent: &str,
    child: &str,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId>

/// Record a merge event: source -> merge_node -> target
pub fn dag_record_merge(
    db: &Arc<Database>,
    source: &str,
    target: &str,
    info: &MergeInfo,
    message: Option<&str>,
    creator: Option<&str>,
) -> StrataResult<DagEventId>

/// Get/set branch status on DAG node
pub fn dag_set_status(db: &Arc<Database>, name: &str, status: BranchStatus) -> StrataResult<()>
pub fn dag_get_status(db: &Arc<Database>, name: &str) -> StrataResult<BranchStatus>

/// Query lineage
pub fn dag_get_ancestors(db: &Arc<Database>, name: &str) -> StrataResult<Vec<String>>
pub fn dag_get_merge_history(db: &Arc<Database>, name: &str) -> StrataResult<Vec<MergeRecord>>
pub fn dag_get_branch_info(db: &Arc<Database>, name: &str) -> StrataResult<BranchDagInfo>

/// Look up a specific fork or merge event
pub fn dag_get_event(db: &Arc<Database>, event_id: &DagEventId) -> StrataResult<DagEvent>

/// Update message on a branch node
pub fn dag_set_message(db: &Arc<Database>, name: &str, message: &str) -> StrataResult<()>
```

All functions internally:
1. Resolve `_system_` to its core `BranchId`
2. Use `GraphStore` methods to read/write nodes and edges
3. Are `pub(crate)` — not exposed to executor directly

#### 2.4 Wire Branch Operations to DAG

**File: `crates/executor/src/handlers/branch.rs`**

Modify existing handlers to record DAG events after successful operations:

**`branch_create`** (line 93):
```rust
// After successful branch creation:
dag_add_branch(&p.db, &name, &metadata.branch_id, None, None)?;
```

**`branch_fork`** (line 194):
- Add optional `message` and `creator` parameters to `Command::BranchFork`
- After successful fork:
```rust
dag_add_branch(&p.db, &destination, &child_branch_id, message.as_deref(), creator.as_deref())?;
dag_record_fork(&p.db, &source, &destination, message.as_deref(), creator.as_deref())?;
```

**`branch_merge`** (line 234):
- Add optional `message` and `creator` parameters to `Command::BranchMerge`
- After successful merge:
```rust
let event_id = dag_record_merge(&p.db, &source, &target, &merge_info, message.as_deref(), creator.as_deref())?;
dag_set_status(&p.db, &source, BranchStatus::Merged)?;
```

**`branch_delete`** (line 163):
- After successful deletion:
```rust
// Mark on DAG but don't remove the node — preserves lineage
dag_mark_deleted(&p.db, branch.as_str())?;
```

#### 2.5 Seed Default Branch in DAG

During `ensure_branch_dag()`, add a node for the default branch if not already present:

```rust
dag_add_branch(db, "default", &BranchId::from_bytes([0u8; 16]).to_string(), None, None)?;
```

#### 2.6 Testing

- `branch_create` → DAG has branch node with `active` status
- `branch_fork` → DAG has fork event node with parent/child edges
- `branch_merge` → DAG has merge event node with source/target edges, source status → `merged`
- `branch_delete` → DAG node preserved with `deleted_at` property
- `dag_get_ancestors("feature")` returns `["main"]` after fork from main
- `dag_get_merge_history("main")` returns merge records
- `dag_get_branch_info("feature")` returns full info including fork origin
- Multiple forks/merges between same branches each get unique event nodes

---

### Phase 3: Status-Based Write Rejection

**Goal**: Prevent writes to merged/archived branches. Use an in-memory cache for performance.

#### 3.1 Branch Status Cache

**New file: `crates/engine/src/branch_status_cache.rs`**

```rust
#[derive(Default)]
pub struct BranchStatusCache {
    statuses: DashMap<String, BranchStatus>,
}

impl BranchStatusCache {
    pub fn get(&self, name: &str) -> Option<BranchStatus> { ... }
    pub fn set(&self, name: &str, status: BranchStatus) { ... }
    pub fn remove(&self, name: &str) { ... }
}
```

Register as a database extension: `db.extension::<BranchStatusCache>()`.

**Population**: During `ensure_branch_dag()`, scan all branch nodes and populate cache. On status transitions (`dag_set_status`), update cache atomically.

#### 3.2 Write Guard

**File: `crates/executor/src/handlers/mod.rs`**

```rust
pub(crate) fn require_branch_writable(p: &Arc<Primitives>, branch: &BranchId) -> Result<()> {
    if branch.is_default() {
        return Ok(());  // Default branch always writable
    }
    let cache = p.db.extension::<BranchStatusCache>()?;
    if let Some(status) = cache.get(branch.as_str()) {
        if status != BranchStatus::Active {
            return Err(Error::ConstraintViolation {
                reason: format!(
                    "Branch '{}' is {} and cannot be written to",
                    branch.as_str(),
                    status
                ),
            });
        }
    }
    Ok(())
}
```

#### 3.3 Add Write Guards to All Write Handlers

Insert `require_branch_writable(p, &branch)?` into every write handler, after `require_branch_exists` and before any engine call:

**Files to modify:**
| File | Handlers |
|------|----------|
| `handlers/kv.rs` | `kv_put`, `kv_delete`, `kv_batch_put` |
| `handlers/json.rs` | `json_set`, `json_batch_set`, `json_delete`, `json_batch_delete` |
| `handlers/state.rs` | `state_set`, `state_init`, `state_cas`, `state_delete`, `state_batch_set` |
| `handlers/event.rs` | `event_append`, `event_batch_append` |
| `handlers/vector.rs` | `vector_upsert`, `vector_delete`, `vector_batch_upsert`, `vector_create_collection`, `vector_delete_collection` |
| `handlers/graph.rs` | `graph_add_node`, `graph_remove_node`, `graph_add_edge`, `graph_remove_edge`, `graph_create`, `graph_delete`, ontology mutations |

**Alternative approach**: Instead of modifying every handler individually, add the check in `Executor::execute()` for all commands where `cmd.is_write() == true`, before dispatching to the handler. This is a single insertion point:

```rust
// In executor.rs, after resolve_defaults(), before match dispatch:
if cmd.is_write() {
    if let Some(branch) = cmd.branch() {
        self.require_branch_writable(&branch)?;
    }
}
```

This requires adding a `fn branch(&self) -> Option<&BranchId>` method to `Command`.

#### 3.4 Archive/Unarchive API

**New commands:**

```rust
Command::BranchArchive {
    branch: BranchId,
    message: Option<String>,
}

Command::BranchUnarchive {
    branch: BranchId,
}
```

**Handlers** (in `handlers/branch.rs`):

```rust
pub fn branch_archive(p: &Arc<Primitives>, branch: BranchId, message: Option<String>) -> Result<Output> {
    reject_default_branch(&branch, "archive")?;
    require_branch_exists(p, &branch)?;

    let status = dag_get_status(&p.db, branch.as_str())?;
    if status == BranchStatus::Archived {
        return Ok(Output::Unit);  // Idempotent
    }
    if status == BranchStatus::Merged {
        return Err(Error::ConstraintViolation {
            reason: "Cannot archive a merged branch".into(),
        });
    }

    dag_set_status(&p.db, branch.as_str(), BranchStatus::Archived)?;
    if let Some(msg) = message {
        dag_set_message(&p.db, branch.as_str(), &msg)?;
    }

    // Update cache
    let cache = p.db.extension::<BranchStatusCache>()?;
    cache.set(branch.as_str(), BranchStatus::Archived);

    Ok(Output::Unit)
}

pub fn branch_unarchive(p: &Arc<Primitives>, branch: BranchId) -> Result<Output> {
    reject_default_branch(&branch, "unarchive")?;
    require_branch_exists(p, &branch)?;

    let status = dag_get_status(&p.db, branch.as_str())?;
    if status != BranchStatus::Archived {
        return Err(Error::ConstraintViolation {
            reason: format!("Cannot unarchive a branch with status '{}'", status),
        });
    }

    dag_set_status(&p.db, branch.as_str(), BranchStatus::Active)?;

    // Update cache
    let cache = p.db.extension::<BranchStatusCache>()?;
    cache.set(branch.as_str(), BranchStatus::Active);

    Ok(Output::Unit)
}
```

**Deliberate constraint**: `Merged` status cannot be reverted. To un-merge, you'd need to reason about the merge operation itself.

#### 3.5 Testing

- Write to active branch succeeds
- Write to merged branch fails with `ConstraintViolation`
- Write to archived branch fails with `ConstraintViolation`
- `branch_archive` → status becomes `Archived`, writes rejected
- `branch_unarchive` → status back to `Active`, writes allowed
- `branch_archive` on merged branch fails
- `branch_unarchive` on non-archived branch fails
- Merge auto-sets source to `Merged`
- Read operations still work on merged/archived branches
- Status cache consistent with DAG after transitions
- Status cache populated correctly on database reopen

---

### Phase 4: Branch Metadata, Comments, and Listing

**Goal**: Enable metadata on branches and events, filtered listing, and detailed branch info queries.

#### 4.1 Extended `branch_fork` and `branch_merge` Parameters

**File: `crates/executor/src/command.rs`**

Add optional parameters to existing commands:

```rust
Command::BranchFork {
    source: String,
    destination: String,
    message: Option<String>,     // NEW
    creator: Option<String>,     // NEW
}

Command::BranchMerge {
    source: String,
    target: String,
    strategy: MergeStrategy,
    message: Option<String>,     // NEW
    creator: Option<String>,     // NEW
}
```

Add to `resolve_defaults()` and `is_write()` as needed.

#### 4.2 New Commands

```rust
Command::BranchSetMessage {
    branch: BranchId,
    message: String,
}

Command::BranchSetCreator {
    branch: BranchId,
    creator: String,
}

Command::BranchInfo {
    branch: String,
}

Command::BranchEvent {
    event_id: String,
}

Command::BranchArchive {
    branch: BranchId,
    message: Option<String>,
}

Command::BranchUnarchive {
    branch: BranchId,
}
```

#### 4.3 Filtered Branch Listing

**Modify** `Command::BranchList` to add filter parameters:

```rust
Command::BranchList {
    state: Option<BranchStatus>,           // existing (currently unused)
    limit: Option<u64>,                     // existing
    offset: Option<u64>,                    // existing
    parent: Option<String>,                 // NEW: filter by parent
    created_after: Option<u64>,             // NEW: filter by creation date
    created_before: Option<u64>,            // NEW: filter by creation date
}
```

**Implementation**: Read all `branch` nodes from `_branch_dag` via `list_nodes` with `object_type: "branch"`, filter by properties. The graph is small (branches, not data), so a full scan is acceptable.

#### 4.4 Branch Info Output

**New Output variant:**

```rust
Output::BranchInfo {
    name: String,
    branch_id: String,
    status: String,
    created_at: u64,
    message: Option<String>,
    creator: Option<String>,
    forked_from: Option<Value>,    // { parent, event_id, timestamp, message }
    merges: Vec<Value>,            // [{ event_id, target/source, timestamp, ... }]
    children: Vec<String>,
}
```

**Handler**: Calls `dag_get_branch_info()` which uses BFS and neighbor queries on the DAG.

#### 4.5 Event Lookup Output

**New Output variant:**

```rust
Output::BranchEvent {
    event_id: String,
    event_type: String,  // "fork" or "merge"
    properties: Value,   // Full event properties from DAG node
}
```

#### 4.6 Testing

- `branch_fork(source, dest, message: "reason", creator: "agent")` stores message/creator on fork event
- `branch_merge(source, target, strategy, message: "approved")` stores message on merge event
- `branch_set_message` updates branch node message
- `branch_info("feature")` returns full info with fork origin, merges, children
- `branch_event("fork:f-<uuid>")` returns fork event details
- `branch_list(status: Some(Active))` returns only active branches
- `branch_list(parent: Some("main"))` returns only children of main
- `branch_list(created_after: ts)` filters by creation time

---

### Phase 5: Background Maintenance

**Goal**: Wire the existing `BackgroundScheduler` into DAG consistency checks and existing manual maintenance operations.

#### 5.1 DAG Consistency Check

**File: `crates/engine/src/branch_dag.rs`**

```rust
pub fn dag_consistency_check(db: &Arc<Database>) -> StrataResult<DagCheckResult> {
    // 1. List all branch nodes in DAG
    // 2. Cross-reference with BranchIndex::list_branches()
    // 3. Report orphaned DAG nodes (branch deleted without DAG update)
    // 4. Report missing DAG nodes (branch exists without DAG entry)
    // 5. For missing entries, auto-repair by creating DAG nodes
}
```

Schedule at `TaskPriority::Low` on a configurable interval (default: 5 minutes).

#### 5.2 Wire Maintenance Tasks

**File: `crates/engine/src/database/mod.rs`**

After `ensure_branch_dag()` in init, start periodic maintenance:

```rust
fn start_maintenance(db: &Arc<Database>) {
    let db_weak = Arc::downgrade(db);
    db.scheduler().submit(TaskPriority::Low, move || {
        if let Some(db) = db_weak.upgrade() {
            let _ = dag_consistency_check(&db);
        }
    }).ok();
}
```

**Note**: Full periodic maintenance (auto-GC, auto-compaction, TTL) is tracked in the maintenance design doc (`docs/design/maintenance.md`) and issue #1408. This phase only adds DAG-specific maintenance. Wiring the scheduler into general maintenance is out of scope for this issue.

#### 5.3 Testing

- DAG consistency check detects missing branch nodes
- DAG consistency check auto-repairs missing entries
- Consistency check is idempotent
- Scheduler submits and executes maintenance task

---

### Phase 6: Garbage Collection Semantics

**Goal**: Define branch deletion behavior relative to the DAG.

#### 6.1 Soft Delete in DAG

When `branch_delete` is called:
1. All branch data (KV, JSON, graph, events, etc.) is deleted as today — this is destructive and immediate
2. The branch's DAG node is **NOT** removed
3. A `deleted_at` timestamp property is set on the DAG node
4. Status transitions to a terminal state (not revertible)
5. Connected fork and merge event nodes are preserved

This preserves lineage: "branch X was forked from main, merged into main, then deleted."

#### 6.2 Implementation

**File: `crates/engine/src/branch_dag.rs`**

```rust
pub fn dag_mark_deleted(db: &Arc<Database>, name: &str) -> StrataResult<()> {
    let graph = GraphStore::new(db.clone());
    let branch_id = resolve_branch_name(SYSTEM_BRANCH);

    if let Some(mut node) = graph.get_node(branch_id, BRANCH_DAG_GRAPH, name)? {
        node.properties["status"] = json!("deleted");
        node.properties["deleted_at"] = json!(Timestamp::now().as_micros());
        graph.add_node(branch_id, BRANCH_DAG_GRAPH, name, node)?;
    }

    // Remove from status cache
    let cache = db.extension::<BranchStatusCache>()?;
    cache.remove(name);

    Ok(())
}
```

#### 6.3 Testing

- `branch_delete` → DAG node has `deleted_at` and `status: "deleted"`
- Fork/merge nodes connected to deleted branch are preserved
- `branch_info` on deleted branch still returns lineage info
- `branch_list` does not include deleted branches
- `dag_get_ancestors` works through deleted intermediate branches

---

## 5. File Change Summary

### New Files

| File | Purpose |
|------|---------|
| `crates/engine/src/branch_dag.rs` | DAG helper functions, types, consistency check |
| `crates/engine/src/branch_status_cache.rs` | In-memory status cache (database extension) |

### Modified Files

| File | Changes |
|------|---------|
| `crates/engine/src/lib.rs` | Export `branch_dag`, `branch_status_cache` modules |
| `crates/engine/src/database/mod.rs` | Call `ensure_system_branch()` + `ensure_branch_dag()` in init paths |
| `crates/executor/src/command.rs` | Add `BranchArchive`, `BranchUnarchive`, `BranchInfo`, `BranchEvent`, `BranchSetMessage`, `BranchSetCreator` commands; extend `BranchFork`/`BranchMerge` with optional `message`/`creator` fields; add `branch()` accessor method |
| `crates/executor/src/output.rs` | Add `BranchInfo`, `BranchEvent` output variants |
| `crates/executor/src/executor.rs` | Dispatch new commands; add centralized write guard before dispatch |
| `crates/executor/src/handlers/branch.rs` | `validate_branch_name` rejects `_system_`; `branch_list` filters `_system_`; new handlers for archive/unarchive/info/event/set_message/set_creator; wire DAG recording into create/fork/merge/delete |
| `crates/executor/src/handlers/mod.rs` | Add `reject_system_branch()`, `require_branch_writable()` helpers |
| `crates/executor/src/api/branch.rs` | Add public API methods for new commands |
| `crates/executor/src/api/branches.rs` | Add `archive()`, `unarchive()`, `info()` to Branches API |

---

## 6. Risks and Mitigations

### Risk: DAG Writes Fail Mid-Operation

If a branch operation succeeds but the subsequent DAG recording fails, the DAG becomes inconsistent.

**Mitigation**: DAG writes are best-effort with logging (same pattern as auto-embed hooks). The Phase 5 consistency check detects and repairs missing entries. This is acceptable because the DAG is a metadata overlay — branch data integrity is never at risk.

### Risk: Status Cache Stale After Crash

If the process crashes between a status change and cache update, the cache may be stale on restart.

**Mitigation**: Cache is rebuilt from DAG on every database open. The DAG is the source of truth; the cache is a performance optimization.

### Risk: Performance Impact of Write Guards

Every write operation now has an additional cache lookup.

**Mitigation**: `DashMap::get()` is lock-free for reads (sharded, concurrent). The cost is ~10ns per write — negligible compared to WAL append latency (~1-100 microseconds).

### Risk: `_system_` Branch Discovered by AI Agents

AI agents that enumerate internal state may discover and attempt to write to `_system_`.

**Mitigation**: All user-facing APIs reject `_system_` access. The branch is hidden from listing. Even if an agent constructs the name, writes are rejected at the executor layer.

### Risk: Graph Primitives Insufficiency

The graph primitive may lack features needed for DAG operations.

**Mitigation**: Investigation confirms the graph engine has all required capabilities — directed edges with labels, node properties with object_type, BFS traversal, edge type filtering. No graph engine changes are needed.

---

## 7. Dependencies

| Phase | Depends On | Reason |
|-------|-----------|--------|
| Phase 1 | Nothing | Foundation — can start immediately |
| Phase 2 | Phase 1 | Needs `_system_` branch and `_branch_dag` graph |
| Phase 3 | Phase 2 | Write guards check status stored in DAG |
| Phase 4 | Phase 2 | Info/listing queries read from DAG |
| Phase 5 | Phase 2 | Consistency check operates on DAG |
| Phase 6 | Phase 2, Phase 3 | Deletion semantics depend on DAG + status |

Phases 3, 4, 5, and 6 can be developed in parallel once Phase 2 is complete.

```
Phase 1 ──> Phase 2 ──┬──> Phase 3 (write guards)
                       ├──> Phase 4 (metadata/listing)
                       ├──> Phase 5 (background maintenance)
                       └──> Phase 6 (GC semantics)
```

---

## 8. What This Does NOT Cover

- **Data lineage / write log** — #1461 (per-key provenance tracking, not branch-level)
- **Full automatic maintenance** — #1408 / `docs/design/maintenance.md` (auto-GC, compaction, TTL)
- **PerfTrace instrumentation** — #1407 (transaction-level performance tracing)
- **Squash merge** — Explicitly a non-goal (MVCC + time-travel incompatible)
- **RBAC** — Access control beyond visibility hiding is future work
- **Migration of existing databases** — Explicitly excluded per requirements
