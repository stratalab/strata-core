# Engine Primitives Architecture

**Crate:** `strata-engine` (primitives module)
**Dependencies:** `strata-core`, `strata-storage`, `strata-concurrency`, `sha2`, `rmp-serde`, `serde_json`
**Source files:** 10 modules (~7,300 lines of implementation, ~4,500 lines of tests)
**Scope:** KVStore, JsonStore, EventLog, StateCell, SpaceIndex, BranchIndex, BranchHandle, extension traits. Vector, graph, and search primitives are documented separately.

## Overview

The primitives module provides six domain-specific data structures that all follow a single architectural pattern: **stateless facades over `Arc<Database>`**. Each primitive translates domain operations (append event, set JSON path, compare-and-swap cell) into key-value transactions against the unified `ShardedStore`.

| Primitive | TypeTag | Versioning | Serialization | Key Characteristic |
|-----------|---------|------------|---------------|-------------------|
| KVStore | KV (0x01) | Txn-version | Raw `Value` | General-purpose, direct storage reads |
| EventLog | Event (0x02) | Sequence counter | JSON (`serde_json`) | SHA-256 hash chain, append-only |
| StateCell | State (0x03) | Counter version | JSON (`serde_json`) | CAS-based coordination |
| JsonStore | Json (0x06) | Counter version | MessagePack (`rmp-serde`) | Path-based operations, document-level MVCC |
| BranchIndex | Branch (0x05) | Counter version | JSON (`serde_json`) | Global namespace, cascading delete |
| SpaceIndex | Space (0x08) | None | Marker value | Namespace management |

## Module Map

```
engine/src/primitives/
├── mod.rs              Re-exports, module documentation (76 lines)
├── extensions.rs       Cross-primitive transaction traits (148 lines)
├── kv.rs               KVStore + KVStoreExt (1,385 lines)
├── json.rs             JsonStore + JsonStoreExt (2,013 lines)
├── event.rs            EventLog + EventLogExt (1,474 lines)
├── state.rs            StateCell + StateCellExt (1,133 lines)
├── space.rs            SpaceIndex (244 lines)
├── branch/
│   ├── mod.rs          Re-exports (12 lines)
│   ├── index.rs        BranchIndex + BranchMetadata (496 lines)
│   └── handle.rs       BranchHandle + per-primitive handles (312 lines)
└── vector/             (separate document)
```

---

## 1. The Stateless Facade Pattern

Every primitive follows the same structure:

```rust
#[derive(Clone)]
pub struct Primitive {
    db: Arc<Database>,  // ONLY field
}
```

**Consequences:**

- **No warm-up or cache invalidation.** Multiple instances of the same primitive on the same `Database` are safe and equivalent.
- **Thread-safe by construction.** All primitives are `Clone + Send + Sync`. Concurrent access is mediated by the transaction layer.
- **Idempotent retry.** Since no mutable state is held, retrying a failed operation produces the same result.
- **Zero per-instance overhead.** The only cost is the `Arc` reference count increment on clone.

Each primitive constructs typed `Key` values from its inputs (branch, space, user key) and delegates to `db.transaction()` or direct storage reads. The primitive never holds data — it is purely a translation layer from domain semantics to storage operations.

---

## 2. Key Schema and Namespace Isolation

### 2.1 Key Construction

All keys follow the pattern: `<namespace>:<TypeTag>:<user_key_bytes>`

| Primitive | Constructor | User Key |
|-----------|-------------|----------|
| KVStore | `Key::new_kv(ns, key)` | User-provided string |
| EventLog | `Key::new_event(ns, seq)` | Sequence number (8 bytes, big-endian) |
| EventLog meta | `Key::new_event_meta(ns)` | `__meta__` sentinel |
| EventLog type index | `Key::new_event_type_idx(ns, type, seq)` | `type + seq` composite |
| StateCell | `Key::new_state(ns, name)` | Cell name string |
| JsonStore | `Key::new_json(ns, doc_id)` | Document ID string |
| BranchIndex | `Key::new_branch_with_id(global_ns, id)` | Branch ID string |
| SpaceIndex | `Key::new_space(branch_id, name)` | Space name string |

### 2.2 Namespace Hierarchy

```
Namespace::for_branch_space(branch_id, space)
  → "{branch_id_hex}:{space}"

Namespace::for_branch(branch_id)
  → "{branch_id_hex}"   (used by extension traits, defaulting to "default" space)
```

**Branch isolation** is enforced structurally: every key is prefixed with the branch's namespace bytes. Two branches with identical user keys produce different storage keys. No runtime check is needed — isolation is a property of the key encoding.

**Space isolation** adds a second level of namespacing within a branch. The "default" space is implicit — primitives that accept a `space` parameter use `Namespace::for_branch_space`, while extension traits (operating within a transaction that already has a branch_id) use `Namespace::for_branch` which maps to the default space.

### 2.3 BranchIndex: The Exception

BranchIndex uses a **global namespace** (`Namespace::for_branch(nil_uuid)`) because it manages branches themselves. It cannot be scoped to a branch — it must be able to list and delete any branch. All BranchIndex transactions run under the nil UUID branch.

---

## 3. KVStore (`kv.rs`)

The simplest primitive. General-purpose key-value storage with no domain logic beyond key construction.

### 3.1 Data Model

Values are stored as raw `Value` enum variants directly in `ShardedStore`. No wrapper struct, no serialization overhead. This is the only primitive that stores values without a domain-specific envelope.

### 3.2 Read Path — Direct Storage Bypass

```
get(branch_id, space, key) →
  1. Construct Key::new_kv(namespace, key)
  2. db.get_value_direct(&key)  ← bypasses VersionedValue construction
  3. Return Option<Value>
```

KVStore `get()` is the only primitive read that bypasses transactions entirely. It calls `get_value_direct` on the storage layer, which returns only the value without version metadata. This is a performance optimization for the most common read path.

For versioned reads, `get_versioned()` and `getv()` go through transactions or direct storage respectively.

### 3.3 Write Path — Blind Writes

```
put(branch_id, space, key, value) →
  1. Extract text for inverted index (before value is consumed)
  2. Construct storage key
  3. db.transaction_with_version(branch_id, |txn| txn.put(key, value))
  4. Post-commit: update InvertedIndex if enabled
  5. Return Version::Txn(commit_version)
```

KV writes are **blind** — they do not read the key first, so the transaction has an empty read set. Blind writes skip OCC validation entirely (no conflicts possible with other transactions). The commit version comes from the global atomic counter.

### 3.4 Batch API

`batch_put()` writes multiple key-value pairs in a single transaction, sharing one lock acquisition, one WAL record, and one commit. All entries receive the same `Version::Txn(commit_version)`.

### 3.5 Inverted Index Integration

All mutating operations (put, delete, batch_put) update the in-memory `InvertedIndex` **post-commit**. The index is accessed via `db.extension::<InvertedIndex>()`. When the index is disabled (the common case for non-search workloads), the check is a single boolean test with zero overhead.

Text extraction happens **before** the transaction consumes the value (move semantics require this ordering).

### 3.6 Time-Travel

`get_at(key, timestamp)` and `list_at(prefix, timestamp)` read from the storage version chain directly (non-transactional), returning the latest value whose commit timestamp <= the requested timestamp.

---

## 4. EventLog (`event.rs`)

Immutable append-only event stream with causal hash chaining. Designed as a **determinism boundary recorder** — it captures nondeterministic external inputs to enable deterministic replay of agent branches.

### 4.1 Data Model

**Event** (stored per sequence number):

| Field | Type | Purpose |
|-------|------|---------|
| `sequence` | `u64` | Monotonic counter within branch+space |
| `event_type` | `String` | User-defined category (max 256 chars) |
| `payload` | `Value` | Must be JSON object (no primitives/arrays) |
| `timestamp` | `u64` | Microseconds since epoch |
| `prev_hash` | `[u8; 32]` | SHA-256 hash of previous event |
| `hash` | `[u8; 32]` | SHA-256 hash of this event |

**EventLogMeta** (stored at `__meta__` key):

| Field | Type | Purpose |
|-------|------|---------|
| `next_sequence` | `u64` | Next sequence number to assign |
| `head_hash` | `[u8; 32]` | Hash chain head |
| `hash_version` | `u8` | Hash algorithm version (1 = SHA-256) |
| `streams` | `HashMap<String, StreamMeta>` | Per-type statistics |

**StreamMeta** (embedded in EventLogMeta):

| Field | Type | Purpose |
|-------|------|---------|
| `count` | `u64` | Events of this type |
| `first_sequence` / `last_sequence` | `u64` | Sequence range |
| `first_timestamp` / `last_timestamp` | `u64` | Timestamp range |

### 4.2 Hash Chain

Each event's hash is computed as:

```
SHA-256(sequence_le || type_len_le || type_bytes || timestamp_le || payload_len_le || payload_json || prev_hash)
```

All fields use little-endian encoding. The payload is serialized to canonical JSON bytes. The chain starts with `prev_hash = [0u8; 32]` (genesis). This provides tamper-evidence: modifying any event invalidates all subsequent hashes.

### 4.3 Append — Single-Writer Serialization

```
append(branch_id, space, event_type, payload) →
  1. Validate event_type (non-empty, <= 256 chars) and payload (object, no NaN/Inf)
  2. transaction_with_retry(max_retries=50, backoff 1..50ms) →
     a. Read EventLogMeta from __meta__ key
     b. Allocate sequence = meta.next_sequence
     c. Compute SHA-256 hash (prev_hash from meta.head_hash)
     d. Write Event to Key::new_event(ns, sequence)
     e. Write type index key: Key::new_event_type_idx(ns, type, sequence) → Null
     f. Update StreamMeta for this event_type
     g. Write updated EventLogMeta (next_sequence++, head_hash = new hash)
  3. Post-commit: update InvertedIndex
  4. Return Version::Sequence(sequence)
```

**Serialization through metadata CAS:** The metadata key read in step (a) enters the transaction's read set. If another transaction commits a concurrent append between (a) and commit, the read-set validation detects the metadata version change and the transaction retries. This guarantees **total ordering** of events within a branch+space.

The retry config (50 retries, 1-50ms backoff) handles high-contention scenarios where many threads append simultaneously.

### 4.4 Per-Type Index

Each append writes an additional **type index key** (`Key::new_event_type_idx`) with a `Null` value. This enables O(K) lookups by event type (where K = events of that type) instead of O(N) full-scan.

`get_by_type()` scans the type index prefix, extracts sequence numbers from the last 8 bytes of each index key, then fetches the corresponding events. Falls back to O(N) scan for old data written before the index was introduced.

### 4.5 Validation Rules

- Event type: non-empty, max 256 characters
- Payload: must be `Value::Object` (not primitives, arrays, or null)
- Payload floats: must be finite (no NaN or Infinity — these are not valid JSON)

Validation runs **before** entering the transaction to avoid wasting a transaction attempt.

### 4.6 Batch Append

`batch_append()` writes multiple events in a single transaction. Pre-validates all entries outside the transaction. Valid entries get sequential sequence numbers and maintain hash chain continuity within the batch. Invalid entries are reported as errors in the result vector without aborting the transaction for valid entries.

---

## 5. StateCell (`state.rs`)

CAS-based versioned cells for coordination. Each cell holds a value and a monotonically increasing counter version.

### 5.1 Data Model

**State** (from `strata_core::primitives::State`):

| Field | Type | Purpose |
|-------|------|---------|
| `value` | `Value` | User data |
| `version` | `Version` | Counter version (starts at 1, increments per write) |
| `updated_at` | `u64` | Microseconds since epoch |

State is serialized to JSON string (`serde_json::to_string`) and stored as `Value::String`. This differs from JsonStore (MessagePack) — StateCell's values are typically small, so JSON overhead is acceptable and aids debuggability.

### 5.2 Version Semantics

StateCell uses `Version::Counter(n)` — an application-level counter that starts at 1 on `init()` and increments by 1 on each write. This is independent of the storage-layer MVCC version (which tracks commit versions). The counter version is embedded in the `State` struct.

### 5.3 Operations

**init(branch_id, space, name, value)** — Idempotent. If the cell already exists, returns the existing version without modifying the value. Creates with `Version::counter(1)` if new.

**cas(branch_id, space, name, expected_version, new_value)** — Compare-and-swap. Reads the cell, checks `current.version == expected_version`, writes with `version.increment()` if match. Returns `Conflict` error on version mismatch.

Uses `transaction_with_retry` (50 retries, 1-50ms backoff) because the read of current state enters the read set, making OCC conflicts likely under contention.

**set(branch_id, space, name, value)** — Unconditional write. Reads current state to determine next version (or starts at 1 if new). Also uses retry because the read creates an OCC conflict window.

**get(branch_id, space, name)** — Returns only the user value (unwraps from `State` envelope).

**get_versioned(branch_id, space, name)** — Non-transactional read directly from storage, returning the value with its counter version and storage timestamp.

**getv(branch_id, space, name)** — Full version history. Returns `VersionedHistory<Value>` with the `State` wrapper unwrapped — callers see user values with storage-layer timestamps.

### 5.4 Batch API

`batch_set()` writes multiple cells in a single transaction. Each cell is independently read-then-written (existing cells get incremented versions, new cells start at 1). All writes share one commit.

### 5.5 Inverted Index Integration

`set()`, `batch_set()`, and `delete()` update the `InvertedIndex` post-commit. The indexed text is `"{name} {json_value}"`. Deletion removes the document from the index to prevent stale postings.

---

## 6. JsonStore (`json.rs`)

JSON document storage with path-based operations. The most complex data primitive, combining document-level versioning with sub-document mutations.

### 6.1 Data Model

**JsonDoc** (internal document representation):

| Field | Type | Purpose |
|-------|------|---------|
| `id` | `String` | User-provided document ID |
| `value` | `JsonValue` | Root JSON value |
| `version` | `u64` | Document version (increments on any mutation) |
| `created_at` | `u64` | Creation timestamp (microseconds) |
| `updated_at` | `u64` | Last modification timestamp (microseconds) |

### 6.2 Serialization — MessagePack

Unlike other primitives that use `serde_json`, JsonStore serializes documents with **MessagePack** (`rmp_serde`) into `Value::Bytes`. This is a deliberate choice: JSON documents can be large, and MessagePack provides significantly more compact binary encoding. The tradeoff is that documents are not human-readable in raw storage.

```
serialize_doc(doc) → rmp_serde::to_vec(doc) → Value::Bytes(bytes)
deserialize_doc(Value::Bytes(bytes)) → rmp_serde::from_slice(bytes)
```

### 6.3 Path-Based Operations

JsonStore delegates path navigation to `strata_core::primitives::json`:

- `get_at_path(&doc.value, path)` — Read value at path
- `set_at_path(&mut doc.value, path, value)` — Set value at path (creates intermediate objects)
- `delete_at_path(&mut doc.value, path)` — Remove value at path

All path operations validate against `JsonLimitError` limits (depth, key length, array size) before entering transactions.

### 6.4 MVCC Interaction

Document mutations follow the **read-modify-write** pattern:

```
set(branch_id, space, doc_id, path, value) →
  transaction(branch_id, |txn| {
    1. txn.get(&key) → read enters read_set (tracks version)
    2. deserialize_doc(stored)
    3. set_at_path(&mut doc.value, path, value)
    4. doc.touch()  → version++, updated_at = now
    5. txn.put(key, serialize_doc(&doc))
  })
```

The read in step 1 enters the transaction's read set. If a concurrent transaction modifies the same document between read and commit, OCC validation detects the version change and the transaction is retried or fails.

**Document-level granularity:** Two concurrent transactions modifying disjoint paths within the same document will conflict because they both read the same storage key. This is conservative — see the concurrency crate architecture doc for the path-level conflict detection that mitigates this in the `JsonStoreExt` trait.

### 6.5 set_or_create — Atomic Upsert

`set_or_create()` combines existence check, creation, and path-set in a single transaction:

- If document does not exist and path is root: creates document with the value
- If document does not exist and path is non-root: creates with empty object, then sets at path
- If document exists: performs normal set at path

This eliminates the two-transaction pattern of "check exists, then create or set."

### 6.6 Pagination

`list()` implements cursor-based pagination:

1. Scan prefix at storage level
2. Skip entries until past the cursor (cursor = last doc_id from previous page)
3. Collect `limit + 1` entries
4. If more than limit: pop last, use second-to-last as next cursor

### 6.7 Batch API

`batch_set_or_create()` combines multiple upserts in one transaction. Each entry independently creates or updates its document.

---

## 7. SpaceIndex (`space.rs`)

Lightweight namespace management. Tracks which spaces exist within each branch.

### 7.1 Design

SpaceIndex stores metadata keys at the branch level (not space-scoped) to avoid circular dependency — a space can't register itself inside its own namespace.

Key format: `Key::new_space(branch_id, space_name)` — uses `TypeTag::Space` within the branch-level namespace.

Values are marker strings (`"{}"`) — the existence of the key is what matters, not the value.

### 7.2 Operations

| Operation | Behavior |
|-----------|----------|
| `register(branch_id, space)` | Idempotent write. Only first call writes. |
| `exists(branch_id, space)` | Returns `true` for "default" without storage hit. |
| `list(branch_id)` | Scan prefix + always inject "default". |
| `delete(branch_id, space)` | Deletes metadata key only. Caller cleans up data. |
| `is_empty(branch_id, space)` | Scans all data TypeTags (KV, Event, State, Json, Vector, VectorConfig) in the space namespace. |

### 7.3 The "default" Space

"default" is always considered to exist, even without explicit registration. `exists()` short-circuits to `true` for "default" without touching storage. `list()` always includes "default" in results.

---

## 8. BranchIndex (`branch/index.rs`)

Branch lifecycle management. Creates, queries, lists, and deletes branches.

### 8.1 Data Model

**BranchMetadata**:

| Field | Type | Purpose |
|-------|------|---------|
| `name` | `String` | User-provided branch name |
| `branch_id` | `String` | Random UUID (internal identifier) |
| `parent_branch` | `Option<String>` | For forking (post-MVP) |
| `status` | `BranchStatus` | Currently only `Active` |
| `created_at` / `updated_at` | `u64` | Timestamps |
| `completed_at` | `Option<u64>` | Post-MVP |
| `error` | `Option<String>` | Post-MVP |
| `version` | `u64` | Internal counter (default 1) |

### 8.2 Branch ID Resolution

`resolve_branch_name()` maps user-provided names to `BranchId`:

- `"default"` → nil UUID (all zeros)
- Valid UUID string → parsed directly
- Any other string → UUID v5 (deterministic, using a fixed namespace UUID)

This ensures the same name always maps to the same `BranchId` across processes.

### 8.3 Cascading Delete

`delete_branch()` removes both the metadata entry and all branch-scoped data in a single atomic transaction:

```
delete_branch(name) →
  1. Read branch metadata (to get both name-derived and metadata BranchIds)
  2. resolve_branch_name(name) → executor_branch_id
  3. BranchId::from_string(meta.branch_id) → metadata_branch_id
  4. Single transaction:
     a. delete_namespace_data(executor_branch_id) → scan all TypeTags, delete all keys
     b. If metadata_branch_id differs: delete_namespace_data(metadata_branch_id)
     c. Delete metadata key
```

The dual-namespace cleanup handles the case where the executor's deterministic `BranchId` differs from the random UUID stored in `BranchMetadata`. The scan covers all TypeTags including deprecated `Trace` for backward compatibility.

### 8.4 Global Namespace

BranchIndex stores metadata under `Namespace::for_branch(nil_uuid)` — a global namespace not associated with any user branch. All BranchIndex transactions use `global_branch_id()` (nil UUID) as their branch context.

Legacy index keys (`__idx_*`) are filtered out during `list_branches()` for backward compatibility.

---

## 9. BranchHandle (`branch/handle.rs`)

Ergonomic facade that binds a `BranchId` to a `Database`, eliminating the need to pass `branch_id` to every operation.

### 9.1 Structure

```rust
#[derive(Clone)]
pub struct BranchHandle {
    db: Arc<Database>,
    branch_id: BranchId,
}
```

### 9.2 Per-Primitive Handles

BranchHandle provides factory methods that return **scoped handles**:

| Method | Returns | Purpose |
|--------|---------|---------|
| `kv()` | `KvHandle` | KV ops without branch_id |
| `events()` | `EventHandle` | Event ops without branch_id |
| `state()` | `StateHandle` | State ops without branch_id |
| `json()` | `JsonHandle` | JSON ops without branch_id |

Each handle (`KvHandle`, `EventHandle`, etc.) holds `Arc<Database>` + `BranchId` and wraps the corresponding extension trait methods in implicit single-operation transactions.

### 9.3 Transaction Method

`branch.transaction(|txn| { ... })` delegates directly to `db.transaction(branch_id, f)`, enabling atomic cross-primitive operations scoped to the branch.

### 9.4 Thread Safety

All handles are `Clone + Send + Sync`. Multiple threads sharing a `BranchHandle` is the expected usage pattern.

---

## 10. Extension Traits (`extensions.rs`)

Extension traits enable **cross-primitive atomic transactions** — a single transaction can mix operations from different primitives.

### 10.1 Trait Design

```rust
pub trait KVStoreExt {
    fn kv_get(&mut self, key: &str) -> StrataResult<Option<Value>>;
    fn kv_put(&mut self, key: &str, value: Value) -> StrataResult<()>;
    fn kv_delete(&mut self, key: &str) -> StrataResult<()>;
}

pub trait EventLogExt {
    fn event_append(&mut self, event_type: &str, payload: Value) -> StrataResult<u64>;
    fn event_get(&mut self, sequence: u64) -> StrataResult<Option<Value>>;
}

pub trait StateCellExt {
    fn state_get(&mut self, name: &str) -> StrataResult<Option<Value>>;
    fn state_cas(&mut self, name: &str, expected: Version, value: Value) -> StrataResult<Version>;
    fn state_set(&mut self, name: &str, value: Value) -> StrataResult<Version>;
}

pub trait JsonStoreExt {
    fn json_get(&mut self, doc_id: &str, path: &JsonPath) -> StrataResult<Option<JsonValue>>;
    fn json_set(&mut self, doc_id: &str, path: &JsonPath, value: JsonValue) -> StrataResult<Version>;
    fn json_create(&mut self, doc_id: &str, value: JsonValue) -> StrataResult<Version>;
}
```

### 10.2 Implementation Pattern

All traits are implemented on `TransactionContext`. Each method:

1. Constructs a namespace from `self.branch_id` (defaults to the branch-level namespace, i.e., "default" space)
2. Constructs the appropriate typed `Key`
3. Delegates to `self.get()`, `self.put()`, or `self.delete()` on the transaction

The extension trait implementations **do not duplicate logic** — they use the same internal functions as the standalone primitive APIs. For example, `StateCellExt::state_cas` on `TransactionContext` performs the same version check and state construction as `StateCell::cas`, just operating directly on the transaction context instead of creating an implicit transaction.

### 10.3 Object Safety

All traits are object-safe (`fn _accepts_kv_ext(_ext: &dyn KVStoreExt) {}` compiles). This enables dynamic dispatch when needed.

### 10.4 Space Limitation

Extension traits use `Namespace::for_branch(branch_id)` which maps to the default space. Operations on non-default spaces must use the standalone primitive APIs. This is a deliberate simplification — cross-primitive transactions typically operate within the default space.

---

## 11. Serialization Strategies

Each primitive makes a deliberate serialization choice:

| Primitive | Format | Stored As | Rationale |
|-----------|--------|-----------|-----------|
| KVStore | None | Raw `Value` | No envelope needed, maximum throughput |
| EventLog | JSON | `Value::String(json)` | Small per-event, debuggable |
| StateCell | JSON | `Value::String(json)` | Small per-cell, debuggable |
| JsonStore | MessagePack | `Value::Bytes(msgpack)` | Documents can be large, compact encoding |
| BranchIndex | JSON | `Value::String(json)` | Small metadata, debuggable |
| SpaceIndex | None | `Value::String("{}")` | Marker only, value content irrelevant |

The JSON primitives (EventLog, StateCell, BranchIndex) all share identical `to_stored_value`/`from_stored_value` helper functions — these are duplicated per module (not shared) to avoid coupling between primitives.

---

## 12. Transaction and MVCC Interaction

### 12.1 Transaction Usage Patterns

| Pattern | Used By | Read Set | Retries |
|---------|---------|----------|---------|
| Blind write | `KVStore::put` | Empty | No (skips validation) |
| Read-modify-write | `JsonStore::set`, `StateCell::set` | Non-empty | Yes (50 retries) |
| CAS through metadata | `EventLog::append` | Metadata key | Yes (50 retries) |
| Read-only | `KVStore::get` (via `get_value_direct`) | N/A (no txn) | No |
| Read-only (txn) | `*.list`, `*.exists` | Tracked but no writes | No (no commit needed) |

### 12.2 RetryConfig

Primitives that perform read-modify-write use a common retry configuration:

```
max_retries: 50
base_delay_ms: 1
max_delay_ms: 50
```

This handles high-contention scenarios (many concurrent writers to the same key) with exponential backoff. The 50-retry limit handles 50+ concurrent threads reliably for EventLog's serialized appends.

### 12.3 Post-Commit Side Effects

Several primitives perform **post-commit side effects** (inverted index updates). These happen after the transaction commits successfully and are not transactional — a crash between commit and index update leaves the index slightly stale but consistent on next read.

---

## 13. Design Decisions and Tradeoffs

### Stateless Facades vs. Cached State

The stateless facade pattern means every read goes through the storage layer (or transaction snapshot). An alternative would be caching frequently-accessed metadata (e.g., EventLog's `next_sequence`) in-memory. The stateless approach was chosen for simplicity, correctness under concurrent access, and compatibility with multi-process mode. The performance cost is acceptable because `ShardedStore` lookups are O(1) hash table operations.

### EventLog's Single-Writer Serialization

EventLog serializes all appends through a single metadata key, creating a contention bottleneck under parallel writes. This is intentional — total ordering of events is a core requirement for deterministic replay. The alternative (sharded sequences that merge at read time) would break hash chain integrity and complicate replay.

### Document-Level MVCC for JsonStore

Two concurrent transactions modifying disjoint paths in the same JSON document will conflict. Path-level MVCC was considered but rejected as too complex for the engine layer. The concurrency crate provides path-level conflict detection in the `JsonStoreExt` trait on `TransactionContext` for cases where finer granularity is needed.

### JSON vs. MessagePack Serialization Split

StateCell and EventLog use JSON for debuggability (values are human-readable in raw storage). JsonStore uses MessagePack because documents can be arbitrarily large and compact encoding provides meaningful space savings. KVStore stores raw `Value` with no serialization overhead.

### Duplicated Serialization Helpers

The `to_stored_value`/`from_stored_value` helper functions are copy-pasted in `event.rs`, `state.rs`, and `branch/index.rs`. This is intentional — each primitive is self-contained and does not depend on other primitives. A shared utility would create coupling between modules that should remain independent.

### Extension Traits Default to "default" Space

Cross-primitive transaction extension traits operate only on the "default" space. This simplifies the trait interface (no space parameter) but means non-default space operations require the standalone primitive APIs. Since the vast majority of operations target the default space, this is an acceptable limitation.

### SpaceIndex `is_empty` Scans All TypeTags

`SpaceIndex::is_empty()` performs separate prefix scans for each data TypeTag (KV, Event, State, Json, Vector, VectorConfig). This is O(TypeTags) prefix scans, not O(entries). It short-circuits on the first non-empty scan. An alternative would be maintaining a counter, but that would require updating the counter on every write across all primitives.
