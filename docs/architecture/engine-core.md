# Engine Crate: Core Database Layer

**Crate:** `strata-engine`
**Dependencies:** `strata-core`, `strata-storage`, `strata-concurrency`, `strata-durability`, `strata-security`
**Source files:** 12 modules covered here (~9,600 lines total, ~4,300 lines of tests)
**Role:** Top-level orchestrator -- the only crate that knows about all lower layers simultaneously

## Overview

The engine crate's core database layer provides:

1. **Database** -- Main struct: open/close lifecycle, singleton registry, extension mechanism
2. **TransactionCoordinator** -- Version/ID allocation, commit protocol delegation, GC safe point
3. **BackgroundScheduler** -- Priority-based task queue for deferred work
4. **TransactionPool** -- Thread-local context recycling for zero-allocation hot path
5. **TransactionOps** -- Unified trait for cross-primitive atomic operations
6. **StrataConfig** -- TOML-based configuration (`strata.toml`)
7. **PerfTrace** -- Feature-gated per-operation instrumentation

## Module Map

```
engine/src/
├── lib.rs                          Public API re-exports (157 lines)
├── database/
│   ├── mod.rs                      Database struct, open/close, transaction API (3,660 lines)
│   ├── config.rs                   StrataConfig, StorageConfig, ModelConfig (831 lines)
│   ├── transactions.rs             RetryConfig (85 lines)
│   └── registry.rs                 OPEN_DATABASES singleton registry (31 lines)
├── coordinator.rs                  TransactionCoordinator + metrics (1,653 lines)
├── background.rs                   BackgroundScheduler (612 lines)
├── transaction/
│   ├── mod.rs                      Re-exports (24 lines)
│   ├── context.rs                  Transaction wrapper (1,186 lines)
│   └── pool.rs                     Thread-local TransactionPool (372 lines)
├── transaction_ops.rs              TransactionOps trait (689 lines)
└── instrumentation.rs              PerfTrace + perf_time! macro (326 lines)
```

---

## 1. Database (`database/mod.rs`)

The central struct. Owns storage, WAL, coordinator, and background infrastructure.

### 1.1 Fields

| Field | Type | Purpose |
|-------|------|---------|
| `data_dir` | `PathBuf` | Canonical data directory (empty for ephemeral) |
| `storage` | `Arc<ShardedStore>` | Sharded in-memory storage with O(1) snapshots |
| `wal_writer` | `Option<Arc<ParkingMutex<WalWriter>>>` | Segmented WAL (None for ephemeral) |
| `persistence_mode` | `PersistenceMode` | Ephemeral vs Disk |
| `coordinator` | `TransactionCoordinator` | Transaction lifecycle, version allocation, metrics |
| `durability_mode` | `DurabilityMode` | Cache/Standard/Always |
| `accepting_transactions` | `AtomicBool` | Shutdown gate |
| `extensions` | `DashMap<TypeId, Arc<dyn Any + Send + Sync>>` | Type-erased extension storage |
| `config` | `RwLock<StrataConfig>` | Live configuration (hot-reloadable subset) |
| `flush_shutdown` | `Arc<AtomicBool>` | Signal for background WAL flush thread |
| `flush_handle` | `ParkingMutex<Option<JoinHandle>>` | Background flush thread handle |
| `scheduler` | `BackgroundScheduler` | Deferred task executor (2 threads, 4096 queue) |
| `_lock_file` | `Option<File>` | Exclusive/shared file lock (lifetime = Database lifetime) |
| `wal_dir` | `PathBuf` | WAL directory path |
| `wal_watermark` | `AtomicU64` | Max txn_id applied from WAL (follower refresh) |

### 1.2 Persistence vs Durability

These are orthogonal axes:

```
                    DurabilityMode
                    Cache    Standard    Always
PersistenceMode  ┌─────────┬──────────┬─────────┐
  Ephemeral      │ cache() │   N/A    │   N/A   │  No files at all
  Disk           │ fast CI │ default  │ audit   │  WAL + snapshots on disk
                 └─────────┴──────────┴─────────┘
```

`PersistenceMode::Ephemeral` (the `cache()` constructor) creates no directories, no WAL, no lock file. `PersistenceMode::Disk` always creates the directory tree but WAL sync behavior depends on `DurabilityMode`.

### 1.3 Open Protocol

```
Database::open(path)
  │
  ├─ Create directory, read/create strata.toml
  ├─ Parse StrataConfig → DurabilityMode
  │
  └─ open_internal(path, mode, config)
       │
       ├─ Canonicalize path for registry key
       ├─ Lock OPEN_DATABASES registry
       ├─ Check for existing Arc<Database> (Weak upgrade)
       │     → hit: return existing instance
       ├─ Acquire exclusive file lock (.lock)
       ├─ Call open_finish()
       ├─ Register Weak<Database> in OPEN_DATABASES
       ├─ Run subsystem recovery (vector, search)
       ├─ Enable InvertedIndex
       │
       └─ open_finish(path, mode, config, lock_file)
            │
            ├─ Create WAL directory
            ├─ RecoveryCoordinator::recover()
            │    → Ok(result): log stats
            │    → Err + allow_lossy_recovery: start empty
            │    → Err: refuse to open
            ├─ Open WalWriter for appending
            ├─ Spawn WAL flush thread (Standard mode only)
            ├─ Create TransactionCoordinator from recovery
            ├─ Apply storage limits from config
            └─ Return Arc<Database>
```

**Key design decisions:**

- **Singleton per path.** The `OPEN_DATABASES` registry (global `Lazy<Mutex<HashMap<PathBuf, Weak<Database>>>>`) ensures only one instance per canonical path. Multiple `open()` calls return the same `Arc`. This prevents WAL conflicts and ensures consistent in-memory state.
- **Weak references for cleanup.** Registry entries are `Weak<Database>`. When all external `Arc` references are dropped, the `Weak` fails to upgrade and the entry is stale. The `Drop` impl removes it.
- **parking_lot::Mutex everywhere.** Avoids `std::sync::Mutex` poisoning cascades (issue #1047).
- **File lock before anything else.** Prevents two processes from corrupting the same WAL.

### 1.4 Extension Mechanism

```rust
pub fn extension<T: Any + Send + Sync + Default>(&self) -> StrataResult<Arc<T>>
```

Type-erased storage using `DashMap<TypeId, Arc<dyn Any>>`. Extensions are lazily created on first access via `DashMap::entry().or_insert_with()`, guaranteeing at-most-once initialization even under concurrent access.

Used by: `VectorBackendState` (shared HNSW backends), `InvertedIndex` (BM25 search), `AutoEmbedState` (embedding runtime).

### 1.5 Transaction API

Three levels of control:

```
┌──────────────────────────────────────────────────────────────────┐
│  1. Closure API (recommended)                                    │
│     db.transaction(branch_id, |txn| { ... })                    │
│     Automatic commit on Ok, abort on Err                         │
├──────────────────────────────────────────────────────────────────┤
│  2. Retry API                                                    │
│     db.transaction_with_retry(branch_id, config, |txn| { ... }) │
│     Exponential backoff on conflict, up to max_retries           │
├──────────────────────────────────────────────────────────────────┤
│  3. Manual API                                                   │
│     txn = db.begin_transaction(branch_id)                       │
│     ... operations ...                                           │
│     db.commit_transaction(&mut txn)                             │
│     db.end_transaction(txn)  // return to pool                  │
└──────────────────────────────────────────────────────────────────┘
```

All three paths funnel through `commit_internal()`, which delegates to the coordinator. The closure API calls `end_transaction()` automatically to return the context to the thread-local pool.

### 1.6 Commit Path

```
commit_internal(txn, durability)
  │
  └─ coordinator.commit_with_wal_arc(txn, storage, wal_arc)
       → TransactionManager handles:
          validate → version alloc → WAL append → storage apply
```

### 1.7 Follower Refresh

`refresh()` is called on a follower-mode `Database` to bring local storage
up-to-date with writes the primary has appended to the shared WAL directory
since the last refresh:

1. Read WAL records after `wal_watermark` via `WalReader::read_all_after_watermark()`
2. For each record: decode `TransactionPayload`, apply puts/deletes to storage
3. Update BM25 search index incrementally (KV, State, Event entries)
4. Update vector backends incrementally (insert/delete with pre-reads for deletes)
5. Advance coordinator version/txn_id counters
6. Update `wal_watermark`

### 1.8 Checkpoint and Compaction

```
checkpoint()
  1. flush() WAL
  2. Collect all primitive data from storage (KV, Event, State, Branch, JSON)
  3. Load or create MANIFEST
  4. CheckpointCoordinator.checkpoint() → crash-safe snapshot file
  5. Update MANIFEST watermark

compact()
  1. Load MANIFEST
  2. Get writer's active segment number
  3. WalOnlyCompactor removes segments covered by snapshot watermark
  4. Active segment is never removed
```

Checkpoint must precede compaction -- compaction only removes segments whose max_txn_id is at or below the snapshot watermark.

### 1.9 Shutdown Protocol

```
shutdown()
  1. accepting_transactions = false
  2. scheduler.drain()       ← wait for background tasks
  3. Spin-wait for active_count == 0 (30s timeout)
  4. Signal flush thread to stop
  5. Join flush thread (it does a final sync before exiting)
  6. Final WAL flush
  7. Freeze vector heaps to mmap (.vec files)
  8. Freeze search index to disk
```

`Drop` performs a similar sequence but without the transaction drain wait (best-effort).

### 1.10 GC (Garbage Collection)

Version chain pruning is explicit, not automatic:

```rust
gc_safe_point() → u64
  = min(current_version - 1, min_active_version)
  = 0 if no drain has occurred and transactions are active

run_gc() → (safe_point, total_pruned)
  for each branch: storage.gc_branch(branch_id, safe_point)

run_maintenance() → (safe_point, pruned, ttl_expired)
  = run_gc() + expire_ttl_keys()
```

The `gc_safe_version` in the coordinator advances via `fetch_max` only when `active_count` drains to 0. This is a conservative lower bound -- GC never prunes versions that an active transaction might need.

---

## 2. TransactionCoordinator (`coordinator.rs`)

Wraps `TransactionManager` (from concurrency crate) with metrics and GC tracking.

### 2.1 Fields

| Field | Type | Purpose |
|-------|------|---------|
| `manager` | `TransactionManager` | Owns commit protocol, version/ID counters, per-branch locks |
| `active_count` | `AtomicU64` | Currently active transactions (Relaxed ordering) |
| `total_started` | `AtomicU64` | Cumulative started count |
| `total_committed` | `AtomicU64` | Cumulative committed count |
| `total_aborted` | `AtomicU64` | Cumulative aborted count |
| `gc_safe_version` | `AtomicU64` | Advances on active_count drain (0 = no drain yet) |
| `max_write_buffer_entries` | `usize` | Per-transaction write buffer limit (0 = unlimited) |

### 2.2 Memory Ordering Rationale

Metric counters use `Relaxed` ordering because:
- They are purely observational (monitoring/debugging)
- Approximate counts are acceptable
- `fetch_add`/`fetch_sub` guarantee no torn reads even with Relaxed

`active_count` transitions use `AcqRel` for the decrement to ensure the `gc_safe_version` `fetch_max` (with `Release`) is properly sequenced.

### 2.3 GC Safe Point Protocol

```
record_commit(txn_id):
  1. drain_version = manager.current_version()    ← BEFORE decrement
  2. prev = active_count.fetch_sub(1, AcqRel)
  3. total_committed.fetch_add(1, Relaxed)
  4. if prev == 1:  ← all transactions drained
       gc_safe_version.fetch_max(drain_version, Release)
```

Capturing the version BEFORE the decrement ensures gc_safe_version is always <= the snapshot version of any concurrently-starting transaction. A transaction that starts between our version read and the decrement will have a snapshot >= drain_version.

### 2.4 Transaction Timeout

`TRANSACTION_TIMEOUT = 300 seconds (5 minutes)`. Transactions exceeding this duration are rejected at commit time. This prevents long-running transactions from blocking GC indefinitely.

### 2.5 Commit Methods

| Method | WAL interface | Use case |
|--------|--------------|----------|
| `commit()` | `Option<&mut WalWriter>` | Direct WAL access |
| `commit_with_wal_arc()` | `Option<&Arc<Mutex<WalWriter>>>` | Narrow lock scope (default) |
| `commit_with_version()` | `Option<&mut WalWriter>` + external version | Multi-process coordination |

All three record metrics and convert `CommitError` to `StrataError`.

---

## 3. BackgroundScheduler (`background.rs`)

Priority-based task queue with fixed worker thread pool.

### 3.1 Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Producers (any thread)                                  │
│    scheduler.submit(priority, || { ... })                │
└──────────────────────┬──────────────────────────────────┘
                       │
          ┌────────────▼────────────────┐
          │  BinaryHeap<TaskEnvelope>   │
          │  (priority × FIFO order)    │
          │  max_queue_depth = 4096     │
          └────────────┬────────────────┘
                       │  Condvar::notify_one
          ┌────────────▼────────────────┐
          │  Worker threads             │
          │  strata-bg-0, strata-bg-1   │
          │  catch_unwind per task      │
          └─────────────────────────────┘
```

### 3.2 Priority Levels

| Priority | Value | Use cases |
|----------|-------|-----------|
| `Low` | 0 | GC, compaction, index rebuilds |
| `Normal` | 1 | Embedding batches, checkpointing |
| `High` | 2 | User-initiated flushes |

Within the same priority, tasks execute in FIFO order (lower sequence number first).

### 3.3 TaskEnvelope Ordering

```rust
impl Ord for TaskEnvelope {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)              // higher priority first
            .then(other.sequence.cmp(&self.sequence))   // lower sequence first (FIFO)
    }
}
```

### 3.4 Panic Safety

Worker threads use `catch_unwind` to survive panicking tasks. An RAII `ActiveTaskGuard` ensures `active_tasks` is decremented even if a task panics, preventing `drain()` from hanging.

### 3.5 Backpressure

`submit()` returns `Err(BackpressureError)` when `queue_depth >= max_queue_depth` or after shutdown. The check happens before acquiring the queue lock to avoid unnecessary contention.

### 3.6 Shutdown Ordering

```
shutdown():
  1. shutdown.store(true)
  2. Lock queue, notify_all workers
  3. Join all worker threads (they drain remaining tasks first)
```

The queue lock is held during `notify_all` to prevent lost-wakeup: a worker between its shutdown check and condvar wait holds the lock, so our `notify_all` is guaranteed to reach it.

---

## 4. TransactionPool (`transaction/pool.rs`)

Thread-local recycling of `TransactionContext` objects.

### 4.1 Design

```
thread_local! {
    static TXN_POOL: RefCell<Vec<TransactionContext>>
}
```

- **MAX_POOL_SIZE = 8** per thread (sufficient for typical 1-2 concurrent transactions)
- `acquire()`: pop from pool + `reset()`, or allocate new
- `release()`: push to pool if room, drop otherwise
- `reset()` calls `HashMap::clear()` / `Vec::clear()` which preserve allocated capacity

### 4.2 Zero-Allocation Hot Path

After warmup, the typical `begin_transaction()` -> `end_transaction()` cycle performs zero heap allocations. The read_set, write_set, delete_set, and cas_set HashMaps retain their bucket arrays across transactions. Collections exceeding 4096 entries are shrunk during `reset()` to prevent unbounded growth.

---

## 5. Configuration (`database/config.rs`)

### 5.1 StrataConfig

File-based configuration loaded from `strata.toml` in the data directory:

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `durability` | `String` | `"standard"` | `"standard"` / `"always"` / `"cache"` |
| `auto_embed` | `bool` | `false` | Automatic text embedding |
| `model` | `Option<ModelConfig>` | `None` | External inference endpoint |
| `embed_batch_size` | `Option<usize>` | `Some(512)` | Embedding batch size |
| `embed_model` | `String` | `"miniLM"` | Embedding model name |
| `bm25_k1` | `Option<f32>` | `None` (0.9) | BM25 term frequency saturation |
| `bm25_b` | `Option<f32>` | `None` (0.4) | BM25 length normalization |
| `provider` | `String` | `"local"` | Generation provider |
| `default_model` | `Option<String>` | `None` | Default generation model |
| `anthropic_api_key` | `Option<SensitiveString>` | `None` | Claude API key |
| `openai_api_key` | `Option<SensitiveString>` | `None` | GPT API key |
| `google_api_key` | `Option<SensitiveString>` | `None` | Gemini API key |
| `storage` | `StorageConfig` | (below) | Resource limits |
| `allow_lossy_recovery` | `bool` | `false` | Start empty on WAL corruption |

### 5.2 StorageConfig

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `max_branches` | `usize` | 1024 | Branch count limit (0 = unlimited) |
| `max_write_buffer_entries` | `usize` | 500,000 | Per-transaction write buffer limit (0 = unlimited) |

### 5.3 ModelConfig

External inference model endpoint for query expansion and re-ranking:

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `endpoint` | `String` | (required) | OpenAI-compatible API endpoint |
| `model` | `String` | (required) | Model name |
| `api_key` | `Option<SensitiveString>` | `None` | Optional authentication |
| `timeout_ms` | `u64` | 5000 | Request timeout |

### 5.4 Config Lifecycle

```
First open:
  1. write_default_if_missing() → creates strata.toml with defaults + comments
  2. from_file() → parse TOML, validate durability eagerly

open_with_config:
  1. write_to_file() → persist supplied config
  2. Subsequent open() picks up persisted values

Runtime update:
  update_config(|cfg| { ... })
    → Rejects durability changes (requires restart)
    → Persists to strata.toml for disk databases

  persist_config_deferred(|cfg| { ... })
    → Allows durability changes (takes effect on next open)
```

Backward compatibility: all new fields use `#[serde(default)]` so old config files parse correctly with default values.

---

## 6. RetryConfig (`database/transactions.rs`)

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `max_retries` | `usize` | 3 | Maximum retry attempts (0 = no retries) |
| `base_delay_ms` | `u64` | 10 | Base delay for exponential backoff |
| `max_delay_ms` | `u64` | 100 | Delay cap |

Delay calculation: `min(base_delay_ms * 2^attempt, max_delay_ms)`. The shift is capped at 63 to prevent overflow.

Only conflict errors are retried. Non-conflict errors (validation, IO, user errors) propagate immediately.

---

## 7. OPEN_DATABASES Registry (`database/registry.rs`)

```rust
static OPEN_DATABASES: Lazy<Mutex<HashMap<PathBuf, Weak<Database>>>> = ...;
```

Singleton enforcement:
1. **Avoids WAL conflicts** -- two Database instances writing to the same WAL would corrupt it
2. **Shares in-memory state** -- extensions, caches, transaction counters
3. **Consistent behavior** -- all `Arc<Database>` clones share the same instance

Uses `Weak<Database>` so the registry does not prevent cleanup. When all external `Arc` references are dropped, `Database::drop()` removes the registry entry.

Multi-process databases are NOT registered (each process has its own instance, coordinated through the WAL file lock).

---

## 8. TransactionOps (`transaction_ops.rs`)

Unified trait expressing Invariant 3: "Everything is Transactional."

### 8.1 Supported Operations

| Primitive | Read methods (`&self`) | Write methods (`&mut self`) |
|-----------|----------------------|---------------------------|
| KV | `kv_get`, `kv_exists`, `kv_list` | `kv_put`, `kv_delete` |
| Event | `event_get`, `event_range`, `event_len` | `event_append` |
| State | `state_get` | `state_init`, `state_cas` |
| JSON | `json_get`, `json_get_path`, `json_exists` | `json_create`, `json_set`, `json_delete`, `json_destroy` |
| Vector | `vector_get`, `vector_search`, `vector_exists` | `vector_insert`, `vector_delete` |
| Branch | `branch_metadata` | `branch_update_status` |

### 8.2 Design Principles

1. **Reads are `&self`**, writes are `&mut self`** -- compiler-enforced access control
2. **All reads return `Versioned<T>`** -- version information is never lost
3. **All writes return `Version`** -- every mutation produces a version
4. **Object-safe** -- can be used as `&dyn TransactionOps` and `Box<dyn TransactionOps>`

### 8.3 Vector and Branch: Intentional Errors

Vector and Branch operations return `Err(InvalidInput)` with guidance messages when called through `TransactionOps`. These primitives require in-memory backends (HNSW index, BranchIndex) that live outside the transaction context. Users must call `VectorStore` and `BranchIndex` APIs directly.

---

## 9. Performance Instrumentation (`instrumentation.rs`)

Feature-gated (`perf-trace`) per-operation timing.

### 9.1 PerfTrace Fields

| Field | Type | Purpose |
|-------|------|---------|
| `snapshot_acquire_ns` | `u64` | Snapshot creation time |
| `read_set_validate_ns` | `u64` | OCC validation time |
| `write_set_apply_ns` | `u64` | Storage mutation time |
| `wal_append_ns` | `u64` | WAL I/O time |
| `fsync_ns` | `u64` | Disk sync time |
| `commit_total_ns` | `u64` | End-to-end commit time |
| `keys_read` | `usize` | Read set size |
| `keys_written` | `usize` | Write set size |

### 9.2 Zero-Cost Abstraction

Without `perf-trace`, `PerfTrace` is a zero-sized type and the `perf_time!` macro compiles to just the inner expression:

```rust
#[cfg(not(feature = "perf-trace"))]
macro_rules! perf_time {
    ($trace:expr, $field:ident, $expr:expr) => { $expr };
}
```

### 9.3 PerfStats Aggregation

When enabled, `PerfStats` collects traces and provides:
- `mean_commit_ns()` / `p99_commit_ns()` -- latency statistics
- `aggregate_breakdown()` -- percentage breakdown across phases
- `summary()` -- formatted string for logging

---

## Data Flow: Transaction Lifecycle

```
┌───────────────────────────────────────────────────────────────────┐
│  db.transaction(branch_id, |txn| { ... })                        │
└───────────────────────────┬───────────────────────────────────────┘
                            │
              ┌─────────────▼──────────────┐
              │  begin_transaction()        │
              │    Pool::acquire() or alloc │
              │    coordinator.next_txn_id()│
              │    storage.create_snapshot()│
              │    record_start()           │
              └─────────────┬──────────────┘
                            │
              ┌─────────────▼──────────────┐
              │  Execute closure            │
              │    txn.get()  → read_set    │
              │    txn.put()  → write_set   │
              │    txn.cas()  → cas_set     │
              └─────────────┬──────────────┘
                            │
                 Ok(value)  │  Err(e)
              ┌─────────────┤──────────────┐
              │             │              │
   ┌──────────▼───┐        │   ┌──────────▼───────┐
   │ commit_       │        │   │ mark_aborted()   │
   │ internal()    │        │   │ record_abort()   │
   │               │        │   └──────────────────┘
   │ [single-proc] │        │
   │ coordinator.  │        │
   │  commit_with_ │        │
   │  wal_arc()    │        │
   │               │        │
   │ [multi-proc]  │        │
   │ commit_       │        │
   │  coordinated()│        │
   └──────┬───────┘        │
          │                 │
   ┌──────▼──────────────────▼───┐
   │  end_transaction()          │
   │    Pool::release(ctx)       │
   └─────────────────────────────┘
```

---

## Concurrency and Safety Model

### Thread Safety Map

| Component | Synchronization | Pattern |
|-----------|----------------|---------|
| Database fields | Immutable after construction | `Arc<Database>` shared freely |
| `storage` | `Arc<ShardedStore>` (DashMap shards) | Lock-free reads, per-shard write locks |
| `wal_writer` | `Arc<ParkingMutex<WalWriter>>` | Exclusive lock for append |
| `coordinator` | Internal atomics + delegated locks | Metrics: Relaxed; commits: per-branch Mutex |
| `extensions` | `DashMap<TypeId, Arc<...>>` | Lock-free reads after initial insert |
| `config` | `parking_lot::RwLock` | Multiple readers, exclusive writer |
| `accepting_transactions` | `AtomicBool` | Acquire/Release ordering |
| `scheduler` | Internal Mutex + Condvar | Queue lock held only for push/pop |
| `TransactionContext` | Single-threaded (not Sync) | Owned by one thread at a time |
| `TransactionPool` | `thread_local!` | No synchronization needed |

### Lock Ordering

When multiple locks are needed, they are acquired in this order to prevent deadlock:

```
1. OPEN_DATABASES registry (global)
2. Per-branch commit lock (in TransactionManager)
3. WAL writer mutex (narrow scope via commit_with_wal_arc)
4. Extension-specific locks (e.g., VectorBackendState.backends)
```

### Contention Profile

- **Zero contention:** Extension reads (DashMap + Arc clone), metric reads (Relaxed atomics)
- **Low contention:** Different branches commit in parallel (independent per-branch locks)
- **Medium contention:** Same-branch commits serialize through per-branch mutex
- **WAL bottleneck:** `commit_with_wal_arc` pre-serializes outside the WAL lock, holding it only for the I/O append
- **Background flush:** Standard mode defers fsync to background thread, eliminating it from the commit hot path

---

## Design Decisions and Tradeoffs

### Singleton Registry with Weak References

The `OPEN_DATABASES` pattern ensures correctness (one WAL writer per path) at the cost of a global lock during `open()`. The `Weak` reference allows natural cleanup but introduces a TOCTOU window: between the `Weak::upgrade()` check and the `registry.insert()`, another thread could also miss the entry. This is harmless because `open_finish()` is idempotent and the registry insert overwrites.

### Ephemeral vs Disk Split

Rather than using `DurabilityMode::Cache` for in-memory databases (which still creates directory structure), `cache()` uses `PersistenceMode::Ephemeral` to avoid all disk I/O. This matters for unit tests that create hundreds of databases.

### Multi-Process Coordination

Multi-process mode trades throughput for shareability. Every commit acquires a file lock, reads/writes a counter file, and flushes the WAL. This adds ~1ms latency per commit but enables multiple processes to share a database. The `refresh()` method replays new WAL entries into local storage, maintaining eventual consistency.

### Background WAL Flush Thread

In Standard durability mode, a dedicated thread (`strata-wal-flush`) periodically calls `sync_if_overdue()`. This decouples fsync latency from the commit path. A safety net detects stalled background threads: if >3x `interval_ms` since last sync, the commit path performs an inline fsync.

### Config Hot-Reload Restrictions

`update_config()` rejects durability mode changes at runtime because switching from Cache to Always mid-flight would require retroactively persisting buffered data. Other config fields (auto_embed, BM25 params, API keys) can be changed without restart.

### Extension Lazy Initialization

Extensions use `Default::default()` for initialization, avoiding constructor arguments. This means extensions must be self-contained or configure themselves from the `Database` reference after creation. The alternative (factory functions) would require a more complex API.

### TransactionOps Vector/Branch Errors

Returning `Err(InvalidInput)` instead of panicking for unsupported operations in `TransactionOps` was a deliberate choice. Vector operations require in-memory index backends that cannot be serialized into a transaction's write set. The error messages guide users to the correct API (`VectorStore`, `BranchIndex`).
