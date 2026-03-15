# Engine Transactions, Recovery, and Branch Operations

**Crate:** `strata-engine`
**Layers covered:** Transaction wrapper + pooling, recovery/replay, branch operations (fork/diff/merge), bundle (export/import)
**Source files:** 8 modules (~5,200 lines total)

## Overview

The engine crate sits above `strata-concurrency` and `strata-durability`, providing the application-facing transaction API, primitive-aware recovery, and branch lifecycle operations. This document covers four subsystems:

1. **Transaction** -- `TransactionOps` wrapper over `TransactionContext` with per-primitive key construction and event buffering
2. **Transaction Pool** -- Thread-local `TransactionContext` recycling for zero-allocation hot paths
3. **Recovery** -- Deterministic branch replay (P1-P6 invariants) and a participant registry for primitives with runtime state
4. **Branch Operations** -- Fork, diff, merge, and bundle (export/import) working directly against storage

## Module Map

```
engine/src/
├── transaction/
│   ├── mod.rs              Module re-exports (24 lines)
│   ├── context.rs          Transaction wrapper, TransactionOps impl (1,186 lines)
│   └── pool.rs             Thread-local TransactionContext pooling (372 lines)
├── recovery/
│   ├── mod.rs              Module re-exports (19 lines)
│   ├── replay.rs           BranchIndex, ReadOnlyView, BranchDiff, replay invariants (1,128 lines)
│   └── participant.rs      RecoveryParticipant registry (440 lines)
├── branch_ops.rs           Fork, diff, merge operations (1,546 lines)
└── bundle.rs               Branch export/import via branchbundle archives (483 lines)
```

---

## 1. Transaction Wrapper (`transaction/context.rs`)

The `Transaction` struct wraps `strata-concurrency`'s `TransactionContext` and implements the `TransactionOps` trait, providing a unified, multi-primitive API inside `db.transaction()` closures.

### 1.1 Fields

| Field | Type | Purpose |
|-------|------|---------|
| `ctx` | `&'a mut TransactionContext` | Underlying OCC transaction from concurrency crate |
| `namespace` | `Arc<Namespace>` | Key namespace (tenant/app/agent/branch/space) |
| `pending_events` | `Vec<Event>` | Events buffered in this transaction |
| `base_sequence` | `u64` | Starting sequence from snapshot (events start here) |
| `last_hash` | `[u8; 32]` | Hash chain state for event integrity |

### 1.2 Key Construction

Each primitive type has a dedicated key constructor that combines the namespace with a type tag:

```
kv_key("user:1")     -> Key::new_kv(namespace, "user:1")
event_key(42)         -> Key::new_event(namespace, 42)
state_key("counter")  -> Key::new_state(namespace, "counter")
json_key("doc1")      -> Key::new_json(namespace, "doc1")
```

All keys share the same `TransactionContext` write/delete/read sets. This is how a single transaction can atomically mix KV, event, state, and JSON operations.

### 1.3 Supported Primitives

**KV Operations** (`kv_get`, `kv_put`, `kv_delete`, `kv_exists`, `kv_list`):
- Read-your-writes: check `write_set` then `delete_set` before snapshot
- `kv_list` merges write_set keys matching prefix, excludes delete_set keys, returns sorted
- Delegates to `ctx.put()` / `ctx.delete()` for bookkeeping

**Event Operations** (`event_append`, `event_get`, `event_range`, `event_len`):
- `event_append` builds a hash-chained `Event`, serializes as `Value::String(json)`, writes both the event key and an `EventLogMeta` key
- Hash chaining: each event stores `prev_hash` (previous event's hash), enabling integrity verification
- Sequence continuity: `base_sequence` tracks existing events from snapshot; new events start at `base_sequence + pending_events.len()`
- Cross-Transaction continuity: calls `ctx.set_event_state()` so subsequent `Transaction` instances in the same session continue the chain

**State Operations** (`state_get`, `state_init`, `state_cas`):
- Compare-and-swap with monotonic version numbers
- `state_init` rejects if key already exists in write_set
- `state_cas` checks version match, increments version on success
- State serialized as `Value::String(json)` matching `StateCell` primitive format

**JSON Operations** (`json_create`, `json_get`, `json_get_path`, `json_set`, `json_delete`, `json_exists`, `json_destroy`):
- Delegates to `ctx.json_set()` / `ctx.json_delete()` for path-based operations
- Read-your-writes: scans `ctx.json_writes()` in reverse for most recent write affecting requested path
- Path navigation: if a root Set was written, `json_get_path` navigates into the written value using relative path segments

**Vector and Branch Operations**: Return `StrataError::invalid_input` -- not supported inside transactions. Vector operations require in-memory index backends; branch operations use `BranchIndex` directly.

### 1.4 Event Hash Chain

```
Event 0:  prev_hash = [0; 32]              hash = SHA256(seq, type, payload, ts, prev_hash)
Event 1:  prev_hash = Event_0.hash          hash = SHA256(...)
Event 2:  prev_hash = Event_1.hash          hash = SHA256(...)
```

The chain is maintained across `Transaction` instances within the same session via `TransactionContext.event_sequence_count()` and `event_last_hash()`.

---

## 2. Transaction Pool (`transaction/pool.rs`)

Thread-local pool of `TransactionContext` objects that eliminates allocation overhead on the hot path.

### 2.1 Design

```
thread_local! {
    static TXN_POOL: RefCell<Vec<TransactionContext>>
}
```

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `MAX_POOL_SIZE` | 8 | Sufficient for typical workloads (1-2 active txns per thread) |
| Storage | `RefCell<Vec<...>>` | No cross-thread sharing needed |

### 2.2 Operations

| Method | Behavior |
|--------|----------|
| `acquire(txn_id, branch_id, snapshot)` | Pop from pool and `reset()`, or allocate new |
| `release(ctx)` | Push to pool if room, drop otherwise |
| `warmup(count)` | Pre-allocate contexts to avoid cold-start allocations |
| `pool_size()` | Current pool depth (debugging) |

### 2.3 Why It Works

`TransactionContext::reset()` calls `HashMap::clear()` / `Vec::clear()` on all internal collections, which preserves allocated capacity. After warmup, a typical acquire-use-release cycle involves zero heap allocations.

Collections exceeding 4096 entries are shrunk during reset to prevent unbounded growth from outlier transactions.

### 2.4 Thread Isolation

Each thread has its own pool. No synchronization is needed. This is intentional: `TransactionContext` is `!Sync` (single-threaded use), so cross-thread pooling would require unnecessary locking.

---

## 3. Recovery: Deterministic Replay (`recovery/replay.rs`)

Implements branch lifecycle tracking and deterministic replay producing read-only derived views.

### 3.1 Replay Invariants (P1-P6)

| # | Invariant | Meaning |
|---|-----------|---------|
| P1 | Pure function | Over (Snapshot, WAL, EventLog) |
| P2 | Side-effect free | Does not mutate canonical store |
| P3 | Derived view | Not a new source of truth |
| P4 | Does not persist | Unless explicitly materialized |
| P5 | Deterministic | Same inputs = same view |
| P6 | Idempotent | Running twice produces identical view |

**CRITICAL**: Replay never writes to the canonical store. `ReadOnlyView` is derived, not authoritative.

### 3.2 BranchIndex

Tracks branch metadata and event offsets for O(branch-size) replay.

| Field | Type | Purpose |
|-------|------|---------|
| `branches` | `HashMap<BranchId, BranchMetadata>` | Branch metadata (status, timestamps, event count) |
| `branch_events` | `HashMap<BranchId, BranchEventOffsets>` | WAL event offsets per branch for targeted replay |

Key operations:
- `insert(branch_id, metadata)` -- register a branch, initialize empty event offsets
- `record_event(branch_id, offset)` -- append WAL offset, increment `event_count`
- `find_active()` -- find branches still in Active status (potential orphans after crash)
- `mark_orphaned(branch_ids)` -- transition active branches to Orphaned status

**Orphan detection**: After crash recovery, any branch still in `Active` status was interrupted mid-execution. `find_active()` + `mark_orphaned()` handles this:

```
Recovery:
  1. Replay WAL -> rebuild BranchIndex
  2. find_active() -> [branch_1, branch_3]   (branch_2 was completed before crash)
  3. mark_orphaned([branch_1, branch_3])
```

### 3.3 ReadOnlyView

In-memory derived view produced by replay. Self-contained HashMap-based KV state plus an event list.

| Field | Type | Purpose |
|-------|------|---------|
| `branch_id` | `BranchId` | Which branch this view represents |
| `kv_state` | `HashMap<Key, Value>` | KV state at branch end |
| `events` | `Vec<(String, Value)>` | Events during branch lifetime |
| `operation_count` | `u64` | Total operations applied |

Builder methods: `apply_kv_put`, `apply_kv_delete`, `append_event`. These are used during replay to construct the view incrementally.

### 3.4 Branch Diff

`diff_views(view_a, view_b) -> BranchDiff` compares two `ReadOnlyView`s:

```
BranchDiff
├── added:    Vec<DiffEntry>     Keys in B but not A
├── removed:  Vec<DiffEntry>     Keys in A but not B
└── modified: Vec<DiffEntry>     Keys in both with different values
```

KV comparison is key-by-key with value equality. Event comparison is count-based (append-only nature means extra events in B are "added", fewer are "removed").

`DiffEntry` stores key, primitive type, and optional string-formatted values for both sides.

### 3.5 Error Types

Two error enums handle branch lifecycle and replay failures:

**BranchError**: `AlreadyExists`, `NotFound`, `NotActive`, `Wal`, `Storage`. Converts to `StrataError` via `From` impl.

**ReplayError**: `BranchNotFound`, `EventLog`, `Wal`, `InvalidOperation`. Used for replay-specific failures.

---

## 4. Recovery: Participant Registry (`recovery/participant.rs`)

Extensible recovery mechanism for primitives with runtime state that lives outside `ShardedStore` (e.g., `VectorStore`'s in-memory HNSW backends).

### 4.1 Design

```
static RECOVERY_REGISTRY: Lazy<RwLock<Vec<RecoveryParticipant>>>
```

| Type | Definition |
|------|-----------|
| `RecoveryFn` | `fn(&Database) -> StrataResult<()>` |
| `RecoveryParticipant` | `{ name: &'static str, recover: RecoveryFn }` |

### 4.2 Recovery Flow

```
Database::open_with_mode()
  1. KV recovery via RecoveryCoordinator (durability crate)
     -> ShardedStore now has all committed data
  2. recover_all_participants(&db)
     -> Each participant scans KV for relevant entries
     -> Rebuilds runtime state into Database extensions
  3. Database ready for use
```

### 4.3 Registration Protocol

- `register_recovery_participant(participant)` -- called once at startup, before any Database opens
- Deduplication by name: re-registering the same name is a no-op
- Registration order determines execution order
- Thread-safe: `RwLock` protects the global registry

### 4.4 Error Semantics

- First error stops execution -- subsequent participants are not called
- This is intentional: a failed participant may leave state inconsistent, so continuing could compound errors
- Participants are stateless functions: they use `Database::extension::<T>()` to access shared state

---

## 5. Branch Operations (`branch_ops.rs`)

Engine-level fork, diff, and merge operations working directly against the storage layer.

### 5.1 Data Types Scanned

All branch operations scan these six `TypeTag` values:

```rust
const DATA_TYPE_TAGS: [TypeTag; 6] = [KV, Event, State, Json, Vector, VectorConfig];
```

### 5.2 Fork (`fork_branch`)

Creates a complete copy of a branch including all data and spaces.

```
fork_branch(db, source, destination)

  1. Verify source exists, destination does not
  2. Create destination branch via BranchIndex
  3. List source spaces, register each in destination
  4. For each TypeTag:
     a. list_by_type(source_id, tag) -> all (Key, VersionedValue) pairs
     b. Rewrite keys: replace source namespace with destination namespace
        (preserving space and user_key)
     c. db.transaction(dest_id) { put all rewritten entries }
  5. Reload vector backends for destination
     (post_merge_reload_vectors_from makes HNSW indexes immediately searchable)
  6. Return ForkInfo { keys_copied, spaces_copied }
```

**Key rewriting**: Each key's `Namespace` is replaced with one targeting `dest_id` while preserving the original space name and user key bytes:

```
source: Namespace(branch=src_id, space="alpha") / TypeTag::KV / "user:1"
  ->
dest:   Namespace(branch=dst_id, space="alpha") / TypeTag::KV / "user:1"
```

### 5.3 Diff (`diff_branches`)

Compares two branches across all spaces and data types.

```
diff_branches(db, branch_a, branch_b)

  1. Resolve both branch IDs
  2. List spaces in both branches
     -> spaces_only_in_a, spaces_only_in_b, all_spaces
  3. Scan all data once per TypeTag for both branches
     -> Build maps: HashMap<space, HashMap<(user_key, TypeTag), Value>>
  4. For each space, compare maps:
     - Keys in A not in B -> removed
     - Keys in B not in A -> added
     - Keys in both with different values -> modified
  5. Return BranchDiffResult with per-space SpaceDiff and DiffSummary
```

**Result structure:**

```
BranchDiffResult
├── branch_a, branch_b: String
├── spaces: Vec<SpaceDiff>
│   └── SpaceDiff { space, added, removed, modified: Vec<BranchDiffEntry> }
└── summary: DiffSummary
    └── { total_added, total_removed, total_modified, spaces_only_in_a/b }
```

`BranchDiffEntry` includes both string-formatted key and raw `Vec<u8>` key bytes for programmatic access.

### 5.4 Merge (`merge_branches`)

Merges data from source branch into target branch.

```
merge_branches(db, source, target, strategy)

  1. diff_branches(target, source)     // target=A (base), source=B (incoming)
  2. If Strict strategy and conflicts exist -> return error with conflict list
  3. For each SpaceDiff:
     a. Ensure target has the space (register if needed)
     b. Collect entries to write:
        - Added entries: always applied
        - Modified entries: applied only with LastWriterWins
     c. Re-scan source storage for actual Values
        (diff only stores string representations, not original Values)
     d. db.transaction(target_id) { put all entries }
  4. Reload vector backends for target
  5. Return MergeInfo { keys_applied, conflicts, spaces_merged }
```

**Merge strategies:**

| Strategy | Added | Modified | Removed |
|----------|-------|----------|---------|
| `LastWriterWins` | Applied | Source overwrites target | Left unchanged |
| `Strict` | Applied | Error with conflict list | Left unchanged |

**Design decision -- re-scan for merge**: The diff produces string-formatted values (for display), not original `Value` objects. Merge must re-scan source storage to get actual values. This is a conscious tradeoff: diff is read-only and cheap to display, while merge is a write operation that can afford the extra scan.

**Vector backend reload**: Both fork and merge call `VectorStore::post_merge_reload_vectors_from()` after writing data, ensuring HNSW indexes are immediately searchable. Failures are logged as warnings but do not fail the operation (best-effort).

### 5.5 Result Types

| Type | Key Fields |
|------|-----------|
| `ForkInfo` | source, destination, keys_copied, spaces_copied |
| `BranchDiffResult` | branch_a, branch_b, spaces (Vec<SpaceDiff>), summary |
| `BranchDiffEntry` | key, raw_key, primitive, space, value_a, value_b |
| `MergeInfo` | source, target, keys_applied, conflicts, spaces_merged |
| `ConflictEntry` | key, primitive, space, source_value, target_value |
| `MergeStrategy` | LastWriterWins, Strict |

---

## 6. Bundle: Export/Import (`bundle.rs`)

Engine-level API bridging `Database`/`BranchIndex` with the durability crate's `BranchBundle` format (`.branchbundle.tar.zst`).

### 6.1 Export

```
export_branch(db, branch_id, path)

  1. Verify branch exists via BranchIndex
  2. Build BundleBranchInfo from metadata
  3. scan_branch_data():
     a. list_by_type() for each TypeTag -> all keys
     b. get_history(key) -> full version chain
     c. Group entries into BTreeMap<version, Vec<(Key, Value)>>
     d. Convert to Vec<BranchlogPayload> sorted by version
  4. BranchBundleWriter::write() -> tar.zst archive
  5. Return ExportInfo { entry_count, bundle_size }
```

**Version history preservation**: Export scans the full version history of each key (not just the latest value), grouping entries by commit version. This preserves the complete version chain for import replay.

### 6.2 Import

```
import_branch(db, path)

  1. BranchBundleReader::read_all(path) -> validate + parse
  2. Verify branch does not already exist
  3. Create branch via BranchIndex
  4. For each BranchlogPayload:
     db.transaction(branch_id) {
       put all (key, value) pairs
       delete all delete keys
     }
  5. Return ImportInfo { transactions_applied, keys_written }
```

### 6.3 Validate

`validate_bundle(path)` checks archive structure, checksums, and format version without importing. Returns `BundleInfo { branch_id, format_version, entry_count, checksums_valid }`.

---

## Data Flow: Transaction Lifecycle Through Engine Layer

```
┌─────────────────────────────────────────────────────────────────┐
│  Client code                                                     │
│    db.transaction(branch_id, |txn| {                            │
│        txn.kv_put("key", value)?;      // -> ctx.put(kv_key)   │
│        txn.event_append("type", v)?;   // -> ctx.put(event_key)│
│        txn.state_cas("c", v1, v2)?;    // -> ctx.put(state_key)│
│        txn.json_set("doc", path, v)?;  // -> ctx.json_set()    │
│        Ok(())                                                    │
│    })?;                                                          │
└──────────────────────────┬──────────────────────────────────────┘
                           │
            ┌──────────────▼──────────────┐
            │  TransactionPool::acquire()  │
            │  (thread-local, zero-alloc)  │
            └──────────────┬──────────────┘
                           │
            ┌──────────────▼──────────────┐
            │  Transaction::new(ctx, ns)   │
            │  -> reads base_sequence      │
            │  -> reads last_hash          │
            └──────────────┬──────────────┘
                           │
                    User closure runs
          (all ops buffer in ctx write_set)
                           │
            ┌──────────────▼──────────────┐
            │  TransactionManager::commit  │
            │  (concurrency crate)         │
            │  -> validate -> WAL -> apply │
            └──────────────┬──────────────┘
                           │
            ┌──────────────▼──────────────┐
            │  TransactionPool::release()  │
            │  (return ctx to pool)        │
            └─────────────────────────────┘
```

---

## Data Flow: Recovery

```
Database::open_with_mode()
           │
           ▼
┌─────────────────────────────────┐
│  Phase 1: KV Recovery           │
│  RecoveryCoordinator (durability│
│  crate) replays WAL into        │
│  ShardedStore                    │
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│  Phase 2: Participant Recovery   │
│  recover_all_participants(&db)   │
│                                  │
│  For each registered participant │
│  (in registration order):        │
│    participant.recover(&db)      │
│    -> Scan KV for relevant data  │
│    -> Rebuild runtime state      │
│    -> Store in db.extension()    │
│                                  │
│  Example: VectorStore scans      │
│  TypeTag::Vector entries and     │
│  rebuilds HNSW indexes in memory │
└──────────────┬──────────────────┘
               │
               ▼
        Database ready
```

---

## Data Flow: Fork/Diff/Merge

```
Fork:   source ──scan──> rewrite keys ──txn──> destination
                          (new namespace)

Diff:   branch_a ──scan──┐
                          ├──> compare per-space ──> BranchDiffResult
        branch_b ──scan──┘

Merge:  diff(target, source)
          │
          ├── added entries ────────────> txn.put(target)
          ├── modified (LWW) ──re-scan──> txn.put(target)
          └── removed ──────────────────> (no action)
```

---

## Concurrency Model

| Component | Synchronization | Notes |
|-----------|----------------|-------|
| `Transaction` | Single-threaded (`&'a mut`) | Borrows `TransactionContext` exclusively |
| `TransactionPool` | `RefCell` (thread-local) | No cross-thread contention |
| `RECOVERY_REGISTRY` | `RwLock<Vec<...>>` | Write during startup, read during recovery |
| `BranchIndex` (replay) | Single-threaded | Used during recovery before DB accepts connections |
| `fork_branch` | Serialized via `db.transaction()` | Per-branch commit lock in concurrency crate |
| `diff_branches` | Read-only storage scans | No locks needed |
| `merge_branches` | Serialized via `db.transaction()` | Per-branch commit lock |

---

## Design Decisions and Tradeoffs

### Primitives as KV Entries

All primitive types (events, state cells, JSON documents) are stored as serialized `Value::String(json)` in the same KV write_set. This enables cross-primitive atomicity within a single transaction but means:
- Event and state reads require JSON deserialization
- No type-specific storage optimizations inside the transaction buffer
- Simpler commit path (one write_set, one validation pass)

### Vector Operations Excluded from Transactions

Vector insert/search/delete return errors inside `Transaction`. This is because HNSW indexes are in-memory runtime state that cannot participate in OCC validation. Vector mutations go through `VectorStore` directly, outside the transaction boundary.

### Diff String Formatting Forces Re-scan on Merge

`BranchDiffEntry` stores values as `format!("{:?}", value)` strings for display. Merge cannot reconstruct original values from these strings, so it re-scans source storage. An alternative would be storing `Value` directly in diff entries, but this would increase memory usage for the common diff-without-merge case.

### Event Hash Chaining Across Transaction Instances

The `last_hash` / `base_sequence` state is threaded through `TransactionContext` (via `event_sequence_count` / `event_last_hash`) so multiple `Transaction` instances within one session maintain a continuous hash chain. This is necessary because `Transaction` has a borrow lifetime tied to the closure, but a session may execute multiple transaction closures sequentially.

### Post-Operation Vector Reload

Fork and merge call `post_merge_reload_vectors_from()` after writing data to rebuild HNSW indexes. This is best-effort (warnings on failure, not errors) because the KV data is already committed -- vector indexes can be rebuilt on next database open if the reload fails.

### Global Recovery Registry

The participant registry uses a process-global `static` with `once_cell::Lazy`. This means:
- Registration must happen before any `Database::open()` call
- Tests must serialize access via a mutex and clear the registry between tests
- Duplicate names are silently ignored (idempotent registration)

The alternative (per-Database registry) was rejected because recovery participants are typically registered once during process initialization, not per-database.

### Bundle Export Preserves Full Version History

`scan_branch_data` calls `get_history()` for each key, not just `get()`. This preserves the complete version chain in the exported bundle, enabling faithful replay on import. The tradeoff is larger bundle sizes for keys with many versions.
