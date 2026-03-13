# Concurrency Crate Architecture

**Crate:** `strata-concurrency`
**Dependencies:** `strata-core`, `strata-storage`, `strata-durability`
**Source files:** 7 modules (~3,300 lines of implementation, ~2,500 lines of tests)

## Overview

The concurrency crate implements **Optimistic Concurrency Control (OCC)** with snapshot isolation for Strata. It sits between the storage and engine layers, providing:

1. **TransactionContext** — read/write set tracking with snapshot isolation
2. **TransactionManager** — atomic commit coordination with per-branch serialization
3. **Validation** — multi-phase conflict detection (read-set, CAS, JSON document, JSON path)
4. **TransactionPayload** — MessagePack serialization for WAL records
5. **RecoveryCoordinator** — crash recovery by WAL replay
6. **ClonedSnapshotView** — deep-copy snapshot for testing/recovery
7. **Conflict detection** — JSON region-based write-write conflict analysis

## Module Map

```
concurrency/src/
├── lib.rs              Public API re-exports
├── transaction.rs      TransactionContext + JsonStoreExt (1,800+ lines)
├── manager.rs          TransactionManager commit protocol (650+ lines)
├── validation.rs       OCC validation engine (418 lines)
├── conflict.rs         JSON path conflict detection (268 lines)
├── snapshot.rs         ClonedSnapshotView (480 lines)
├── payload.rs          WAL payload serialization (143 lines)
└── recovery.rs         Crash recovery coordinator (933 lines)
```

---

## 1. TransactionContext (`transaction.rs`)

The central data structure. Each active transaction holds:

### 1.1 Fields

| Field | Type | Purpose |
|-------|------|---------|
| `txn_id` | `u64` | Unique transaction identifier (from `TransactionManager`) |
| `branch_id` | `BranchId` | Branch isolation — no cross-branch conflicts |
| `start_version` | `u64` | Snapshot version; all reads see data as of this version |
| `snapshot` | `Option<Box<dyn SnapshotView>>` | Point-in-time view of storage |
| `read_set` | `HashMap<Key, u64>` | Keys read → version observed (for validation) |
| `write_set` | `HashMap<Key, Value>` | Buffered writes (invisible until commit) |
| `delete_set` | `HashSet<Key>` | Buffered deletes (invisible until commit) |
| `cas_set` | `Vec<CASOperation>` | Buffered compare-and-swap operations |
| `status` | `TransactionStatus` | Lifecycle state machine |
| `read_only` | `bool` | When true, writes rejected, read-set tracking skipped |
| `key_write_modes` | `HashMap<Key, WriteMode>` | Per-key write mode overrides (e.g., `KeepLast(1)` for graph adjacency) |
| `max_write_entries` | `usize` | Write buffer capacity limit (0 = unlimited) |

**JSON fields** (lazily allocated via `Option<...>` — zero overhead when unused):

| Field | Type | Purpose |
|-------|------|---------|
| `json_reads` | `Option<Vec<JsonPathRead>>` | Path-level read tracking |
| `json_writes` | `Option<Vec<JsonPatchEntry>>` | Buffered JSON patches |
| `json_snapshot_versions` | `Option<HashMap<Key, u64>>` | Document versions at read time |

**Event fields** (also lazy):

| Field | Type | Purpose |
|-------|------|---------|
| `event_sequence_count` | `Option<u64>` | Cumulative event sequence across Transaction instances |
| `event_last_hash` | `Option<[u8; 32]>` | Hash chaining state for EventLog continuity |

### 1.2 Read Path — Read-Your-Writes Semantics

```
get(key) →
  1. write_set hit?  → return buffered value      (no read_set entry)
  2. delete_set hit? → return None                 (no read_set entry)
  3. snapshot read   → return value, track in read_set(key → version)
                       version=0 if key absent
```

Key design decisions:
- **Reads of own writes don't enter read_set.** This prevents false self-conflicts.
- **Reads of absent keys track version 0.** If another transaction creates the key before commit, conflict is detected.
- **Read-only mode** (`set_read_only(true)`) skips read_set tracking entirely, saving memory on large scan workloads.

### 1.3 Write Path

- `put(key, value)` — buffer in write_set, clear from delete_set if present
- `put_replace(key, value)` — same as put, but marks key for `WriteMode::KeepLast(1)` (overwrites all previous versions instead of appending MVCC version)
- `delete(key)` — buffer in delete_set, clear from write_set if present
- `cas(key, expected_version, new_value)` — buffer in cas_set (validated at commit, not call time)
- `cas_with_read(key, ...)` — like cas but also reads from snapshot to populate read_set

All writes are **blind** by default — they don't enter the read_set unless you explicitly read the key first. This is fundamental to the OCC model: blind writes never conflict.

### 1.4 Write Buffer Limits

`set_max_write_entries(n)` caps total buffered operations (puts + deletes + CAS). Overwrites of already-tracked keys don't count against the limit. When exceeded, returns `StrataError::capacity_exceeded`.

### 1.5 State Machine

```
Active ──→ Validating ──→ Committed
  │            │
  └──→ Aborted ←──┘
```

- `Active` → `Validating`: `mark_validating()` (first step of commit)
- `Validating` → `Committed`: `mark_committed()` (validation passed)
- `Validating` → `Aborted`: `mark_aborted(reason)` (conflict detected)
- `Active` → `Aborted`: `mark_aborted(reason)` (user abort or error)
- `Committed` and `Aborted` are terminal states

On abort, write_set/delete_set/cas_set are cleared immediately. read_set is preserved for diagnostics.

### 1.6 Scan Prefix

`scan_prefix(prefix)` merges snapshot results with buffered writes/deletes:
1. Scan snapshot → track all observed keys in read_set (including keys we're deleting)
2. Exclude keys in delete_set from results
3. Overlay write_set entries matching prefix
4. Return sorted via BTreeMap

The critical detail: **even deleted keys observed during scan are tracked in read_set.** This prevents a subtle bug where concurrent modification of a scanned-then-deleted key would go undetected.

### 1.7 Apply Writes

`apply_writes(store, commit_version)` drains all three sets into storage:
- write_set → `put_with_version_mode` (Replace for replace_keys, Append otherwise)
- delete_set → `delete_with_version`
- cas_set → `put_with_version` (CAS was already validated; just apply the new value)

Uses `drain()` on all collections to move keys/values without cloning.

### 1.8 Transaction Pooling

`reset(txn_id, branch_id, snapshot)` reuses the TransactionContext without deallocating:
- `HashMap::clear()` / `Vec::clear()` preserve allocated capacity
- Collections exceeding 4096 entries are shrunk to prevent unbounded growth
- Lazy-allocated fields (JSON, events) are set to `None` to reclaim memory

### 1.9 JsonStoreExt Trait

Extends TransactionContext with JSON operations (`json_get`, `json_set`, `json_delete`, `json_get_document`, `json_exists`) enabling **cross-primitive atomic transactions** — a single transaction can mix KV operations and JSON operations.

Read-your-writes for JSON:
- `json_get` scans `json_writes` in reverse for the most recent write affecting the requested path
- Handles ancestor path matching (if `json_set("foo", ...)` was called, `json_get("foo.bar")` navigates into the written value)
- Falls back to snapshot, deserializing from MessagePack

Documents store version snapshots for conflict detection: if any read document changes by commit time, the transaction aborts.

---

## 2. TransactionManager (`manager.rs`)

Coordinates the commit protocol. Owns two atomic counters and per-branch commit locks.

### 2.1 Fields

| Field | Type | Purpose |
|-------|------|---------|
| `version` | `AtomicU64` | Global monotonic version counter (shared across all branches) |
| `next_txn_id` | `AtomicU64` | Next transaction ID (unique per TransactionManager) |
| `commit_locks` | `DashMap<BranchId, Mutex<()>>` | Per-branch commit serialization |

### 2.2 Counter Allocation

Both `allocate_version()` and `next_txn_id()` use `fetch_add(1, AcqRel)` — a single LOCK XADD on x86. No CAS loops.

**Overflow handling:** At `u64::MAX`, `fetch_add` wraps to 0 before a `store(u64::MAX)` repair runs. A theoretical TOCTOU race exists at the overflow boundary, but reaching `u64::MAX` requires 584 years at 1 billion txn/sec — accepted as physically unreachable.

**Version gaps:** If a transaction fails after version allocation (e.g., WAL write failure), the version number is lost. Consumers must not assume contiguous versions.

### 2.3 Commit Protocol

Three commit methods are provided:

#### `commit(&self, txn, store, wal)` — Standard commit

```
1. Read-only fast path: skip everything, return current version
2. Acquire per-branch commit lock
3. Check if blind write (no reads, no CAS, no JSON snapshots)
   3a. Blind write: skip validation, mark committed directly
   3b. Has reads: run txn.commit(store) → validate → mark committed
4. Allocate commit version (fetch_add on global counter)
5. Build TransactionPayload, serialize to WalRecord
6. WAL append (DURABILITY POINT)
7. Apply writes to storage
8. Return commit_version
```

#### `commit_with_wal_arc(&self, txn, store, wal_arc)` — Narrow WAL lock scope

Takes `Arc<Mutex<WalWriter>>` instead of `&mut WalWriter`. Pre-serializes the WAL record outside the lock, then briefly acquires the WAL lock only for the I/O append. Lock ordering: **branch lock → WAL lock**.

**Blind write optimization:** When the transaction has no reads (all blind writes), the per-branch lock is skipped entirely. Blind writes are commutative — concurrent writers to the same key create distinct versions, and the highest version wins.

#### `commit_with_version(&self, txn, store, wal, version)` — External version

Uses an externally-allocated version (from a shared counter file in multi-process mode) instead of incrementing the local counter. Calls `fetch_max` to keep the local counter in sync.

### 2.4 TOCTOU Prevention

The per-branch commit lock prevents this race:

```
Without lock:
  T1: validate (pass, storage at v1)
  T2: validate (pass, storage at v1)    ← races with T1
  T1: apply (storage → v2)
  T2: apply (uses stale validation)     ← INCORRECT

With lock:
  T1: lock → validate → apply → unlock
  T2: lock → validate (sees v2) → may conflict → ...
```

Different branches commit in parallel (different shards, no cross-branch conflicts).

### 2.5 WAL Failure Handling

- **WAL write fails:** Transaction is aborted, `CommitError::WALError` returned
- **Storage apply fails after WAL commit:** Error is **logged but not returned** — the WAL is authoritative, and recovery will replay the transaction. This avoids a split-brain where the WAL says committed but the caller thinks it failed
- **Storage apply fails without WAL:** Transaction is aborted, error returned (data is lost)

### 2.6 Branch Lock Cleanup

`remove_branch_lock(branch_id)` safely removes a commit lock for a deleted branch:
- Tries `try_lock()` to check if a commit is in-flight
- If lock is held → skip removal (stale entry cleaned up later)
- If lock is free → remove entry from DashMap

### 2.7 Multi-Process Support

- `catch_up_version(v)` — advances version counter to at least `v` via `fetch_max`
- `catch_up_txn_id(id)` — advances txn_id counter to at least `id + 1`

Used during multi-process refresh to reflect writes from other processes' WAL entries.

---

## 3. Validation Engine (`validation.rs`)

Four-phase conflict detection, all running against live storage state.

### 3.1 Phase 1: Read-Set Validation

```rust
validate_read_set(read_set: &HashMap<Key, u64>, store: &S) → ValidationResult
```

For each (key, read_version) in read_set:
- Get current version from storage via `get_version_only(key)`
- If `current_version != read_version` → `ReadWriteConflict`
- Missing key = version 0

This implements **first-committer-wins**: the first transaction to commit its writes wins; later transactions that read stale data must abort.

### 3.2 Phase 2: CAS-Set Validation

```rust
validate_cas_set(cas_set: &[CASOperation], store: &S) → ValidationResult
```

For each CAS operation:
- Get current version
- If `current_version != expected_version` → `CASConflict`
- `expected_version = 0` means "key must not exist"

CAS validation is **independent of read-set** — a CAS operation does not automatically protect other reads.

### 3.3 Phase 3: JSON Document-Level Validation

```rust
validate_json_set(json_snapshot_versions: Option<&HashMap<Key, u64>>, store: &S) → ValidationResult
```

Conservative document-level check: if any JSON document's version changed since the transaction read it, conflict detected. This may produce false positives when concurrent transactions modify disjoint paths within the same document.

### 3.4 Phase 4: JSON Path-Level Validation

```rust
validate_json_paths(json_writes: &[JsonPatchEntry]) → ValidationResult
```

Checks for **write-write path overlaps** within the same transaction (semantic error — undefined patch ordering). Uses `JsonPath::overlaps()` which checks ancestor/descendant/equal relationships.

**Read-write path conflicts are intentionally NOT checked** — reading and writing the same path is valid behavior within a single transaction (read-your-writes). Cross-transaction conflicts are handled by document-level version checking in Phase 3.

### 3.5 Orchestration

`validate_transaction(txn, store)` runs all four phases:
1. **Read-only fast path:** if no writes, deletes, CAS ops, or JSON writes → return OK immediately
2. Merge results from all four phases
3. Return accumulated `ValidationResult`

### 3.6 Write Skew

Snapshot isolation intentionally **allows write skew**:

```
T1: read(A), write(B)
T2: read(B), write(A)
Both pass validation → both commit → write skew
```

Mitigation is the application's responsibility (use CAS, combine reads and writes on same key, or application-level locking).

---

## 4. Conflict Detection (`conflict.rs`)

Write-write conflict detection for JSON operations.

### 4.1 Write-Write Detection

`check_write_write_conflicts(writes) → Vec<ConflictType>` — O(n²) pairwise comparison of all JSON patches within a transaction:
- Same document key?
- Paths overlap (ancestor/descendant/equal)?
- If both true → `JsonPathWriteWriteConflict`

Returns `ConflictType` directly (no intermediate representation). Different documents never conflict. Disjoint paths within the same document are fine.

---

## 5. Snapshot Isolation (`snapshot.rs`)

### 5.1 ClonedSnapshotView

Deep-copy snapshot wrapping `Arc<BTreeMap<Key, VersionedValue>>`. O(n) creation, O(1) for multiple Arc clones afterward.

**Used in:** tests and recovery. Production transactions use `ShardedSnapshot` from `strata-storage` which provides O(1) snapshot creation via lock-free DashMap iteration.

Implements `SnapshotView` trait:
- `get(key)` — BTreeMap lookup, returns cloned VersionedValue
- `scan_prefix(prefix)` — BTreeMap iteration with `Key::starts_with` filter
- `version()` — returns snapshot version

Thread-safe: `Send + Sync` (verified by `static_assertions`).

---

## 6. Payload Serialization (`payload.rs`)

### 6.1 TransactionPayload

```rust
struct TransactionPayload {
    version: u64,            // Commit version
    puts: Vec<(Key, Value)>, // From write_set + cas_set
    deletes: Vec<Key>,       // From delete_set
}
```

Serialized with **MessagePack** (`rmp-serde`) for compact binary encoding.

`from_transaction(txn, version)` builds the payload:
- All write_set entries become puts
- All CAS operations become puts (already validated; just store the new value)
- All delete_set entries become deletes

One `TransactionPayload` = one `WalRecord.writeset` blob = one committed transaction.

---

## 7. Recovery Coordinator (`recovery.rs`)

### 7.1 Recovery Procedure

```
1. If WAL directory doesn't exist → return empty result
2. WalReader::read_all(wal_dir) → Vec<WalRecord>
3. For each record:
   a. Deserialize TransactionPayload from record.writeset
   b. Apply all puts via put_with_version
   c. Apply all deletes via delete_with_version
   d. Track max_version and max_txn_id
4. Initialize TransactionManager with (max_version, max_txn_id)
5. Return RecoveryResult { storage, txn_manager, stats }
```

### 7.2 Recovery Invariants

- **No conflict detection on replay.** Replays apply commit decisions, not re-execute logic.
- **Single-threaded.** No concurrent modifications during recovery.
- **Version preservation.** Each replayed write uses the original commit version.
- **Idempotent.** Running recovery twice produces identical results.
- **Partial record tolerance.** Garbage at end of WAL segment is silently skipped by WalReader.

### 7.3 RecoveryStats

Tracks: `txns_replayed`, `writes_applied`, `deletes_applied`, `final_version`, `max_txn_id`, `from_checkpoint` (always false in current implementation — checkpoint-based recovery is stubbed but not implemented).

`RecoveryResult::empty()` provides a fallback for corrupted WAL — database starts with clean state rather than refusing to open.

---

## Data Flow: Transaction Lifecycle

```
┌─────────────────────────────────────────────────────────────────────┐
│ Client Code                                                         │
│   txn = TransactionContext::with_snapshot(id, branch, snapshot)     │
│   txn.get(&key)     → read_set tracks (key, version)               │
│   txn.put(key, val) → write_set buffers (key, value)               │
│   txn.delete(key)   → delete_set buffers key                       │
│   txn.cas(key, v, val) → cas_set buffers CASOperation              │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                    TransactionManager::commit()
                               │
                ┌──────────────▼──────────────┐
                │  Acquire per-branch lock     │
                │  (DashMap<BranchId, Mutex>)  │
                └──────────────┬───────────────┘
                               │
              ┌────────────────▼────────────────┐
              │  Validation (4 phases)           │
              │  1. Read-set vs current storage  │
              │  2. CAS-set vs current storage   │
              │  3. JSON doc versions            │
              │  4. JSON write-write paths       │
              └────────────────┬────────────────┘
                               │
                     conflicts? ──yes──→ Abort (clear write/delete/cas sets)
                               │
                              no
                               │
              ┌────────────────▼────────────────┐
              │  Allocate version (fetch_add)    │
              │  Build TransactionPayload        │
              │  Serialize to WalRecord          │
              │  WAL append (DURABILITY POINT)   │
              └────────────────┬────────────────┘
                               │
              ┌────────────────▼────────────────┐
              │  apply_writes(store, version)    │
              │  - drain write_set → puts        │
              │  - drain delete_set → tombstones │
              │  - drain cas_set → puts          │
              └────────────────┬────────────────┘
                               │
                     Release per-branch lock
                               │
                     Return Ok(commit_version)
```

---

## Concurrency Model

### Thread Safety

| Component | Synchronization |
|-----------|----------------|
| `version` counter | `AtomicU64` (lock-free) |
| `next_txn_id` counter | `AtomicU64` (lock-free) |
| Per-branch commit | `parking_lot::Mutex` per branch in `DashMap` |
| Cross-branch commits | Fully parallel (independent shards) |
| TransactionContext | Single-threaded (not Sync — owned by one thread) |
| Snapshots | `Send + Sync` — can be shared across threads via Arc |

### Contention Profile

- **Low contention:** Different branches commit in parallel with no shared locks
- **Medium contention:** Same-branch commits serialize through per-branch mutex
- **Minimal contention:** Version allocation is a single atomic fetch_add (no CAS loop)
- **WAL bottleneck:** `commit_with_wal_arc` narrows the WAL lock to just the I/O append

---

## Known Issues and Design Decisions

### Write Skew Allowed
Snapshot isolation allows write skew by design. Upgrading to serializable would require predicate locking or SSI (serializable snapshot isolation), which conflicts with the "optimistic, fast-path" design philosophy.

### Document-Level JSON Conflict Detection
JSON conflicts are detected at the document level (any version change triggers conflict), not path level. This is conservative — it may abort transactions that modified disjoint paths within the same document. Path-level cross-transaction detection would add O(reads × writes) overhead per document.

### No Checkpoint-Based Recovery
`RecoveryCoordinator` has a `snapshot_path` field and `with_snapshot_path` builder, but checkpoint-based recovery is not implemented. Recovery always replays the full WAL.

### Event State in TransactionContext
`event_sequence_count` and `event_last_hash` track EventLog state across multiple Transaction instances within the same session. These are vestigial if EventLog is removed (see issue #1391).

### Duplicate Commit Code
`commit()`, `commit_with_wal_arc()`, and `commit_with_version()` share substantial protocol logic but are implemented as three separate methods with significant code duplication (~150 lines each). This could be consolidated with an internal helper.
