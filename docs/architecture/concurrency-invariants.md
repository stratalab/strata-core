# Concurrency Invariant Verification

## 1. Shared Mutable State Inventory

**21 pieces of shared mutable state across 5 crates. Zero unsafe code in production paths.**

### DashMaps (6)

| # | Location | Type | Purpose |
|---|----------|------|---------|
| 1 | `storage/src/sharded.rs:234` | `DashMap<BranchId, Shard>` | Per-branch MVCC storage. Each shard contains `FxHashMap<Key, VersionChain>`. 16-way internal sharding. |
| 2 | `concurrency/src/manager.rs:83` | `DashMap<BranchId, Mutex<()>>` | Per-branch commit locks. Serializes validate+apply within a branch. |
| 3 | `engine/src/database/mod.rs:164` | `DashMap<TypeId, Arc<dyn Any + Send + Sync>>` | Type-erased extension storage (e.g., VectorBackendState). Lazy initialization. |
| 4 | `engine/src/search/index.rs:118` | `DashMap<String, PostingList>` | Inverted index postings for full-text search. |
| 5 | `engine/src/search/index.rs:119` | `DashMap<String, usize>` | Document frequencies per term. |
| 6 | `engine/src/search/index.rs:123` | `DashMap<EntityRef, u32>` | Per-document lengths for BM25 scoring. |

### RwLocks (2 production, 1 test)

| # | Location | Type | Readers | Writers |
|---|----------|------|---------|---------|
| 1 | `engine/src/primitives/vector/store.rs:71` | `RwLock<BTreeMap<CollectionId, Box<dyn VectorIndexBackend>>>` | Concurrent searches | Upsert, delete, create collection |
| 2 | `engine/src/recovery/participant.rs:75` | `RwLock<Vec<RecoveryParticipant>>` | `recover_all_participants()` | `register_recovery_participant()` (startup only) |
| 3 | `core/src/traits.rs:226` | `RwLock<BTreeMap<Key, Vec<VersionedValue>>>` | Test-only MockStorage | — |

### Mutexes (5 production, 3 test)

| # | Location | Type | Risk |
|---|----------|------|------|
| 1 | `engine/src/database/registry.rs:26` | `std::sync::Mutex<HashMap<PathBuf, Weak<Database>>>` | **Poisonable** (issue #855) |
| 2 | `engine/src/database/mod.rs:138` | `parking_lot::Mutex<WalWriter>` | Non-poisoning. Serializes WAL appends. |
| 3 | `durability/src/database/handle.rs:33` | `parking_lot::Mutex<ManifestManager>` | Non-poisoning. Serializes manifest updates. |
| 4 | `durability/src/database/handle.rs:35` | `parking_lot::Mutex<WalWriter>` | Non-poisoning. Serializes WAL writes. |
| 5 | `durability/src/database/handle.rs:37` | `parking_lot::Mutex<CheckpointCoordinator>` | Non-poisoning. Serializes checkpoint creation. |

### Atomics (12)

| # | Location | Type | Ordering | Purpose |
|---|----------|------|----------|---------|
| 1 | `storage/src/sharded.rs:236` | `AtomicU64` | Acquire/AcqRel/Release | Global MVCC version counter |
| 2 | `concurrency/src/manager.rs:65` | `AtomicU64` | SeqCst | Global version counter (transaction manager) |
| 3 | `concurrency/src/manager.rs:70` | `AtomicU64` | SeqCst | Next transaction ID |
| 4 | `engine/src/coordinator.rs:39` | `AtomicU64` | Relaxed | Active transaction count (metric) |
| 5 | `engine/src/coordinator.rs:41` | `AtomicU64` | Relaxed | Total started (metric) |
| 6 | `engine/src/coordinator.rs:43` | `AtomicU64` | Relaxed | Total committed (metric) |
| 7 | `engine/src/coordinator.rs:45` | `AtomicU64` | Relaxed | Total aborted (metric) |
| 8 | `engine/src/database/mod.rs:156` | `AtomicBool` | Relaxed | Shutdown flag (accepting_transactions) |
| 9 | `engine/src/search/index.rs:120` | `AtomicUsize` | Relaxed/SeqCst | Total indexed documents |
| 10 | `engine/src/search/index.rs:121` | `AtomicBool` | Relaxed | Index enabled flag |
| 11 | `engine/src/search/index.rs:122` | `AtomicU64` | Release/Acquire | Index version watermark |
| 12 | `engine/src/search/index.rs:123` | `AtomicUsize` | Relaxed | Total document length (BM25) |

### Global/Static Mutable State (3 production, 1 test-framework)

| # | Location | Type | Purpose |
|---|----------|------|---------|
| 1 | `engine/src/database/registry.rs:26` | `static OPEN_DATABASES: Lazy<Mutex<...>>` | Singleton database per path |
| 2 | `engine/src/recovery/participant.rs:75` | `static RECOVERY_REGISTRY: Lazy<RwLock<...>>` | Recovery participant registration |
| 3 | `engine/src/transaction/pool.rs:29` | `thread_local! { TXN_POOL: RefCell<Vec<...>> }` | Per-thread transaction context reuse |
| 4 | `executor/src/api/mod.rs:61` | `static VECTOR_RECOVERY_INIT: Once` | One-time vector recovery registration |

### Unsafe Code (2 blocks, both in `core`)

| Location | Code | Justification |
|----------|------|---------------|
| `core/src/primitives/json.rs:1028` | `&*(ptr as *const serde_json::Value as *const JsonValue)` | `#[repr(transparent)]` guarantees identical layout |
| `core/src/primitives/json.rs:1088` | `&mut *(ptr as *mut serde_json::Value as *mut JsonValue)` | Same, mutable variant |

Both are pointer casts between `serde_json::Value` and its `#[repr(transparent)]` wrapper `JsonValue`. No concurrency implications.

## 2. Commit Protocol

```
  Thread A (branch X)                Thread B (branch X)
  ───────────────────                ───────────────────
  commit_locks.entry(X)
  lock = branch_lock.lock()  ←───── BLOCKS HERE until A releases
  │
  ├─ 1. Validate read-set
  │     For each key in read_set:
  │       current = store.get(key)
  │       if current.version != read_version → CONFLICT
  │
  ├─ 2. Allocate version
  │     v = version.fetch_add(1, SeqCst) + 1
  │
  ├─ 3. WAL append + flush
  │     Serialize TransactionPayload (puts + deletes)
  │     wal.append(record)
  │     wal.flush()            ← DURABILITY POINT
  │
  ├─ 4. Apply to storage
  │     store.put_with_version(key, value, v)
  │     store.delete_with_version(key, v)
  │                            ← VISIBILITY POINT
  │
  └─ drop(_commit_guard)      ← Thread B unblocks

  Thread C (branch Y)
  ───────────────────
  commit_locks.entry(Y)       ← Different branch, no contention
  lock = branch_lock.lock()   ← Acquires immediately
  │
  └─ ... same steps, parallel with A ...
```

**Location**: `crates/concurrency/src/manager.rs:140-260`

### Key Properties

| Property | Status | Mechanism |
|----------|--------|-----------|
| TOCTOU prevention | Correct | Per-branch Mutex held from validation through apply |
| Version uniqueness | Correct | `AtomicU64::fetch_add(1, SeqCst)` — atomic single-instruction |
| Version gaps | By design | Failed commit after version allocation orphans the version number |
| Durability before visibility | Correct | WAL flush (step 3) precedes storage apply (step 4) |
| Per-branch parallelism | Correct | DashMap entry per BranchId, independent Mutex per branch |

## 3. Transaction Isolation

### Read Path (Snapshot Isolation)

```
  TransactionContext::get(key)
  │
  ├─ Check delete_set         → return None (read-your-deletes)
  ├─ Check write_set           → return buffered value (read-your-writes)
  └─ read_from_snapshot(key)
     │
     ├─ snapshot.get(key)
     │   └─ VersionChain::get_at_version(snapshot_version)
     │       walk chain: [v101, v100, v99, ...]
     │       return first where v <= snapshot_version
     │
     └─ Record in read_set:
        key → version (or 0 if not found)
```

**Snapshot immutability**: The snapshot captures `version` at creation time. Even as concurrent commits append new versions to `VersionChain`, `get_at_version(snapshot_version)` always skips newer entries. Version chains are append-only (push_front), so existing entries are never modified.

**Location**: `crates/concurrency/src/transaction.rs:548-584`

### Write Path (Buffered OCC)

```
  TransactionContext::put(key, value)
  │
  └─ write_set.insert(key, value)   ← Buffered, invisible to others
     delete_set.remove(key)          ← Cancel pending delete if any

  TransactionContext::delete(key)
  │
  └─ delete_set.insert(key)         ← Buffered
     write_set.remove(key)          ← Cancel pending write if any
```

Writes are invisible to all other transactions until commit step 4 (apply to storage).

### Validation (First-Committer-Wins)

```
  validate_read_set(storage)
  │
  For each (key, read_version) in read_set:
  │
  ├─ current = storage.get(key)
  │
  ├─ If read_version == 0 (key didn't exist at read time):
  │   └─ If current exists now → CONFLICT (phantom read)
  │
  └─ If read_version > 0 (key existed at read time):
      └─ If current.version != read_version → CONFLICT (write-write)
```

**Location**: `crates/concurrency/src/validation.rs:148-179`

## 4. Lock Ordering Analysis

### All Locks in Critical Paths

```
  LOCK ACQUISITION ORDER
  ──────────────────────

  Level 0: DashMap shard locks (internal, 16-way)
           ├─ commit_locks DashMap — entry() acquires shard lock briefly
           └─ shards DashMap — entry() acquires shard lock briefly

  Level 1: Per-branch commit Mutex
           └─ commit_locks[branch_id].lock()
              Held during: validate → WAL → apply

  Level 2: WAL writer Mutex
           └─ wal_writer.lock()
              Held during: append + flush (inside commit)

  Level 3: (none — no deeper nesting)
```

### Deadlock Assessment

| Condition | Present? | Reason |
|-----------|----------|--------|
| Mutual exclusion | Yes | Mutex by definition |
| Hold and wait | No | Each commit acquires ONE branch lock; WAL lock is separate scope |
| No preemption | Yes | Mutex by definition |
| Circular wait | **No** | Single lock type per branch, no cross-branch lock acquisition |

**Deadlock verdict: IMPOSSIBLE.** The commit protocol acquires at most one branch lock and one WAL lock, always in the same order (branch lock first, then WAL). No code path acquires two branch locks simultaneously.

## 5. DashMap Race Analysis

### commit_locks (manager.rs:83)

```rust
let commit_mutex = self.commit_locks
    .entry(txn.branch_id)
    .or_insert_with(|| Arc::new(Mutex::new(())))
    .clone(); // clone Arc, drop RefMut → releases shard lock (#1781)
let _commit_guard = commit_mutex.lock();
```

**Safe**: `entry().or_insert_with()` is atomic — DashMap guarantees at-most-once initialization. The `Arc::clone()` drops the `RefMut` (releasing the DashMap shard lock) while keeping the Mutex alive. Two threads on the same branch get the same `Arc<Mutex>`, so one blocks on `lock()`. Threads on different branches no longer contend on DashMap shard locks (#1781).

### shards (sharded.rs:234)

**put()**: Uses `entry(branch_id).or_default()` to get mutable shard reference. DashMap serializes access to the same shard.

**apply_batch()**: Groups all writes by branch, then applies each branch's writes under a single shard lock. No partial visibility within a branch.

**get_versioned()**: Read-only traversal of version chain. DashMap provides concurrent read access without blocking writers on different shards.

**Safe**: No get-then-insert patterns. All mutations use atomic `entry()` API.

### extensions (database/mod.rs:164)

```rust
pub(crate) fn extension<T: Any + Send + Sync + Default>(&self) -> Arc<T> {
    self.extensions.entry(type_id).or_insert_with(|| Arc::new(T::default()));
    // ...
}
```

**Safe**: Atomic entry API. Multiple callers get the same `Arc<T>`. Comment at line 450 confirms "safe to call concurrently".

### Inverted Index DashMaps (search/index.rs:118-123)

**index_document()**: Updates postings, frequencies, and lengths via DashMap entry API.

**remove_document()**: Removes from lengths, iterates and updates postings.

**Safe**: All operations use atomic DashMap APIs. TOCTOU between `contains_key` check and `remove_document` (line 273-275) is benign — DashMap's `remove` is atomic regardless of stale check.

## 6. Memory Ordering Assessment

```
  ORDERING STRATEGY
  ─────────────────

  SeqCst (strongest — total order across all threads)
  ├─ TransactionManager.version          — version ordering across branches
  └─ TransactionManager.next_txn_id      — unique ID allocation

  AcqRel (release on write, acquire on read)
  └─ ShardedStore.version                — MVCC snapshot consistency

  Acquire/Release (paired)
  ├─ ShardedStore.version.load(Acquire)  — snapshot reads see all prior writes
  └─ ShardedStore.version.store(Release) — recovery publishes restored version

  Relaxed (no ordering guarantees)
  ├─ Coordinator metrics (4 counters)    — approximate counts, observational only
  ├─ accepting_transactions flag         — checked outside critical path
  └─ Search index metrics                — approximate counts
```

**Assessment**: Ordering choices are correct. SeqCst is used where total ordering matters (version allocation, transaction IDs). Relaxed is used only for metrics and flags where approximate values are acceptable.

## 7. WAL Concurrency

```
  WAL Writer Access Pattern
  ─────────────────────────

  WalWriter::append(&mut self, ...)    ← requires exclusive reference
  WalWriter::flush(&mut self)          ← requires exclusive reference

  Callers must wrap in Mutex:
    Arc<ParkingMutex<WalWriter>>

  Commit protocol:
    1. Acquire commit_locks[branch] Mutex
    2. Validate
    3. Acquire WAL Mutex → append → flush → release WAL Mutex
    4. Apply to storage
    5. Release commit_locks[branch]
```

**Durability modes**:

| Mode | Behavior | Durability Gap |
|------|----------|---------------|
| Strict | fsync after every append | None — every record durable before return |
| Batched | fsync every N writes or M ms | Up to (batch_size - 1) unfsynced records |
| None | No persistence | All data — in-memory only |

## 8. Recovery Correctness

### WAL Replay Protocol

```
  recover(storage, wal_reader)
  │
  For each WalRecord in WAL:
  │
  ├─ Deserialize TransactionPayload
  │   ├─ version: u64
  │   ├─ puts: Vec<(Key, Value)>
  │   └─ deletes: Vec<Key>
  │
  ├─ Apply puts:
  │   store.put_with_version(key, value, version)
  │   └─ VersionChain::push(StoredValue)     ← append-only
  │
  ├─ Apply deletes:
  │   store.delete_with_version(key, version)
  │   └─ VersionChain::push(tombstone)       ← append, not remove
  │
  ├─ Track max version and max txn_id
  │
  └─ Restore counters:
     version.store(max_version, Release)
     next_txn_id = max_txn_id + 1
```

**Location**: `crates/concurrency/src/recovery.rs:76-134`

### Idempotency Properties

| Property | Status | Mechanism |
|----------|--------|-----------|
| Replay same record twice | Safe | Version chain is append-only; get_at_version picks first match |
| Partial WAL record at EOF | Discarded | CRC checksum validation; `InsufficientData` stops reading |
| Corrupted WAL record | Discarded | `ChecksumMismatch` stops reading |
| Version counter restore | Idempotent | `fetch_max(version, AcqRel)` — max is idempotent |
| Crash during replay | Safe | Replay restarts from beginning; idempotent operations |

### Crash Point Analysis

| Crash Point | State After Recovery |
|-------------|---------------------|
| During validate (step 1) | No WAL record. Transaction lost. Correct — not yet committed. |
| During WAL append (step 3) | Partial record. CRC fails. Discarded on recovery. Transaction lost. Correct. |
| After WAL flush, before apply (step 3→4) | WAL record present. Replayed on recovery. Data restored. Correct. |
| During apply (step 4) | WAL record present. Partial apply. Replay re-applies all. Idempotent. Correct. |
| After apply (step 4) | Fully committed and visible. WAL record is redundant. Replay is idempotent. Correct. |

## 9. Vector Subsystem Concurrency

### Architecture

```
  VectorStore
  │
  ├─ db: Arc<Database>        ← shared database reference
  │
  └─ state: Arc<VectorBackendState>
     └─ backends: RwLock<BTreeMap<CollectionId, Box<dyn VectorIndexBackend>>>
        │
        ├─ Read lock: search, get
        └─ Write lock: upsert, delete, create_collection
```

**Location**: `crates/engine/src/primitives/vector/store.rs:66-72`

### Vector Insert TOCTOU Race

```
  Thread A (insert key K)              Thread B (insert key K)
  ──────────────────────               ──────────────────────
  1. Check KV: key K exists?  NO       1. Check KV: key K exists?  NO
  2. Acquire write lock                     (blocked)
  3. Allocate VectorId = 1
  4. Insert embedding at ID 1
  5. Release write lock
                                       2. Acquire write lock
                                       3. Allocate VectorId = 2
                                       4. Insert embedding at ID 2
                                       5. Release write lock
  6. KV transaction: store              6. KV transaction: store
     record with ID 1                      record with ID 2
     (first-committer wins)                (conflict or overwrites)
```

**Problem**: Between checking existence (step 1, via KV snapshot outside lock) and acquiring the write lock (step 2), another thread can insert the same key. This creates two VectorIds for the same logical key — one becomes orphaned in the heap.

**Location**: `crates/engine/src/primitives/vector/store.rs:410-457`

**Severity**: Medium — the KV transaction at step 6 provides eventual consistency (only one record persists), but the in-memory heap retains the orphaned embedding until collection deletion.

### Vector Backend Not Transactional

```
  vector_upsert(key, embedding)
  │
  ├─ 1. Update in-memory backend     ← OUTSIDE transaction
  │     backends.write()
  │     backend.insert(embedding)
  │
  └─ 2. Store KV record              ← INSIDE transaction
        db.transaction(branch, |txn| {
            txn.put(kv_key, record)
        })

  If step 2 fails:
    In-memory backend has embedding     ← INCONSISTENT
    KV store does not have record       ← No WAL entry
    Recovery will not restore embedding ← Data divergence
```

**Location**: `crates/engine/src/primitives/vector/store.rs:435-457`

**Design limitation**: The vector index backends (`VectorHeap`, `HNSW`) live outside the transaction system. `TransactionContext` cannot access `Database::extension`, so vector operations cannot participate in multi-key transactions. This is explicitly acknowledged in code comments at lines 1231-1286.

**Impact**: A crash between backend update and KV commit leaves the in-memory index with a phantom entry. On recovery, the backend is rebuilt from KV records only, so the phantom is cleared — but during the crash window, searches may return results for non-persisted vectors.

## 10. Cross-Branch Atomicity

### Design: Not Atomic Across Branches

```
  Transaction (branch_id = A)
  │
  ├─ put(key_in_branch_A, v1)     ← In write_set
  ├─ put(key_in_branch_B, v2)     ← In write_set
  │
  └─ commit()
     │
     ├─ Lock: commit_locks[A]      ← Only branch A locked
     ├─ Validate against branch A   ← Only branch A checked
     ├─ Apply all writes            ← Both A and B written
     └─ Unlock: commit_locks[A]

  Concurrent commit (branch_id = B):
     ├─ Lock: commit_locks[B]      ← Independent lock
     └─ Can interleave with above   ← Writes to B not protected
```

**By design**: The transaction model is branch-scoped. Each agent operates on one branch. The per-branch lock prevents TOCTOU within a branch but does not provide cross-branch atomicity. This is consistent with the spec — branches are isolated namespaces.

## 11. Summary

| # | Finding | Severity | Type | Status |
|---|---------|----------|------|--------|
| 1 | Commit protocol TOCTOU prevention | — | Correct | Per-branch Mutex serializes validate+apply |
| 2 | Lock ordering | — | Correct | Single lock type, no cycles, deadlock impossible |
| 3 | Version allocation | — | Correct | SeqCst atomic increment, unique and monotonic |
| 4 | Snapshot isolation | — | Correct | Append-only chains + version filtering = true immutability |
| 5 | WAL recovery | — | Correct | Idempotent replay, CRC validation, crash-safe |
| 6 | DashMap access patterns | — | Correct | Atomic entry API, no get-then-insert races |
| 7 | Memory ordering | — | Correct | SeqCst where needed, Relaxed only for metrics |
| 8 | Vector insert TOCTOU race | Medium | Race | Existence check outside write lock; orphaned heap entries |
| 9 | Vector backend not transactional | Medium | Design | Backend update before KV commit; crash window for phantoms |
| 10 | Cross-branch non-atomicity | Low | Design | By design — branch-scoped transactions per spec |
| 11 | Poisonable OPEN_DATABASES Mutex | Low | Bug | `std::sync::Mutex` in registry.rs (issue #855) |

**Overall**: The core concurrency model is correct. OCC with per-branch commit locks, SeqCst version allocation, append-only version chains, and idempotent WAL recovery form a sound foundation. The two new findings (#8, #9) are both in the vector subsystem, which operates partially outside the transaction system by design.
