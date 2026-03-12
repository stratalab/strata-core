# Storage Crate Architecture

Pure in-memory storage layer. No file I/O. Persistence is handled by `strata-durability`.

## File Layout

```
crates/storage/src/
  lib.rs              29 lines   Re-exports
  sharded.rs        5966 lines   ShardedStore, VersionChain, Shard, ShardedSnapshot, Storage impl
  stored_value.rs    631 lines   StoredValue (56-byte compact layout)
  index.rs           328 lines   BranchIndex, TypeIndex (secondary indices)
  ttl.rs             235 lines   TTLIndex (BTreeMap expiry index)
  registry.rs        443 lines   PrimitiveRegistry (unused — see #1392)
  primitive_ext.rs     6 lines   Re-export from core (unused — see #1392)
```

Total: ~7,638 lines. `sharded.rs` is 78% of the crate.

---

## 1. StoredValue — The Storage Unit

**File:** `stored_value.rs`

The atomic unit of storage. Every value in every version chain is a `StoredValue`.

```
StoredValue (56 bytes, compile-time asserted)
+-----------+----------+-----------+---------------+
| value     | version  | timestamp | ttl_and_flags |
| Value     | u64      | Timestamp | u64           |
| (32 bytes)| (8 bytes)| (8 bytes) | (8 bytes)     |
+-----------+----------+-----------+---------------+
                                    ^bit 63 = tombstone
                                    ^bits 0-62 = TTL in ms
```

### Design decisions

- **Version stored as raw `u64`**, not `Version` enum. All storage-layer versions are `Version::Txn` — enforced by `debug_assert!` at construction. Avoids enum tag overhead on every comparison.
- **TTL + tombstone packed into single `u64`**. Bit 63 is the tombstone flag, bits 0-62 are TTL in milliseconds (max ~292 million years). Saves 24 bytes per entry vs separate `Option<Duration>` + `bool` fields (~2.9 GB at 128M entries).
- **Tombstones are explicit**. `Value::Null` with tombstone bit set marks deletion. This separates "deleted" from "value is null" — critical for MVCC correctness.
- **TTL precision is milliseconds**. Sub-millisecond TTLs truncate to zero and read back as `None`. `Duration::ZERO` also becomes `None`.
- **`versioned()` clones the Value**. Returns an owned `VersionedValue` by cloning the inner `Value`. Wrapping in `Arc<Value>` would eliminate this but requires SDK API change (deferred).

### Key methods

| Method | Purpose |
|--------|---------|
| `new(value, version, ttl)` | Standard construction, `Timestamp::now()` |
| `with_timestamp(value, version, ts, ttl)` | Explicit timestamp (recovery, batches) |
| `tombstone(version)` | Deletion marker |
| `is_tombstone()` | Bit 63 check |
| `is_expired()` | `now - timestamp >= ttl` |
| `version_raw()` | Raw `u64` for hot-path comparisons |
| `versioned()` | Reconstruct `VersionedValue` (clones Value) |
| `into_versioned()` | Move-based conversion (no clone) |
| `expiry_timestamp()` | `timestamp + ttl`, `None` on overflow |

---

## 2. VersionChain — MVCC History

**File:** `sharded.rs:46-277`

Stores multiple versions of a single key, newest-first. This is the core MVCC mechanism.

```
VersionStorage enum:
  Single(StoredValue)           <-- most keys (no heap alloc for container)
  Multi(VecDeque<StoredValue>)  <-- keys with >1 version (newest at front)
```

### Single/Multi optimization

Most keys only ever have one version (especially graph edges, configs, metadata). `Single` stores the value inline — no `VecDeque` allocation, saving ~270 bytes per entry. On the second write, `Single` promotes to `Multi` via `VecDeque::with_capacity(2)`.

### Key methods

| Method | Complexity | Purpose |
|--------|-----------|---------|
| `push(value)` | O(1) amortized | Add new version (promotes Single→Multi on 2nd write) |
| `put(value, mode)` | O(1) | Push or replace based on WriteMode |
| `replace_latest(value)` | O(1) | Drop all history, store only this value |
| `get_at_version(max_version)` | O(versions) | MVCC read: first version where `v <= max_version` |
| `get_at_timestamp(max_ts)` | O(versions) | Time-travel: first version where `ts <= max_ts` |
| `latest()` | O(1) | Most recent version (front of deque or single) |
| `gc(min_version)` | O(pruned) | Remove versions older than `min_version`, keep at least 1 |
| `history(limit, before)` | O(versions) | Paginated history, newest-first |
| `is_dead()` | O(1) | Single tombstone/expired remaining — safe to remove |

### WriteMode

`WriteMode::KeepLast(n)` keeps at most `n` versions, pruning oldest after write. `KeepLast(1)` drops all history and stores only the new value. Used by graph adjacency lists to avoid unbounded version chains. `ShardedStore::max_versions_per_key` provides a global default applied to `WriteMode::Append` writes.

---

## 3. Shard — Per-Branch Container

**File:** `sharded.rs:279-349`

Each branch gets its own `Shard`. Branches never contend with each other.

```
Shard
+-------------------------------------------+
| data: FxHashMap<Arc<Key>, VersionChain>   |  O(1) point lookups
| ordered_keys: Option<BTreeSet<Arc<Key>>>  |  O(log n + k) prefix scans
| ttl_index: TTLIndex                       |  O(expired) cleanup
+-------------------------------------------+
```

### Dual index strategy

- **FxHashMap** for point lookups (get, put, delete): O(1) with fast non-cryptographic hash
- **BTreeSet** for ordered scans (prefix scan, list): O(log n + k) range queries

The BTreeSet is **lazily built** on first scan via `ensure_ordered_keys()`. After that, it's maintained incrementally — `put()` and `apply_batch()` insert new `Arc<Key>` into both structures. This avoids paying the sort cost for branches that never scan.

### Arc<Key> sharing

Both `FxHashMap` and `BTreeSet` store `Arc<Key>`, sharing the same allocation. This avoids deep-cloning every `Key` (which contains `Arc<Namespace>`, `TypeTag`, and `Vec<u8>` user_key).

---

## 4. ShardedStore — The Main Store

**File:** `sharded.rs:377-1268`

Top-level storage structure. All data lives here.

```
ShardedStore
+--------------------------------------------------+
| shards: DashMap<BranchId, Shard>                 |  16-way sharded
| version: AtomicU64                               |  global MVCC version
| max_branches: AtomicUsize                        |  capacity limit
+--------------------------------------------------+
```

### DashMap sharding

`DashMap` partitions the branch map into 16 internal shards (by default). Combined with per-branch `Shard` isolation, the contention model is:

- **Different branches**: Never contend (different DashMap shards or different Shard entries)
- **Same branch, reads**: Lock-free via DashMap read guard
- **Same branch, writes**: Only lock the target DashMap shard

### Version management

- `version: AtomicU64` — global monotonic version counter
- `next_version()` — `fetch_update` with `wrapping_add(1)` (safe: ~584 years at 1B/sec)
- `set_version()` — used during recovery to restore version counter
- `fetch_max(version)` — used by `apply_batch()` and `put_with_version()` to advance global version

### Branch limit enforcement

```rust
// Snapshot len() BEFORE entry() to avoid DashMap deadlock:
// DashMap::len() acquires read locks on all internal shards.
// If we called it while holding a write lock via entry(),
// it would deadlock.
let current_len = if max > 0 { self.shards.len() } else { 0 };

let mut shard = match self.shards.entry(branch_id) {
    Occupied(e) => e.into_ref(),        // existing branch — always OK
    Vacant(e) => {
        if max > 0 && current_len >= max {
            return Err(capacity_exceeded(...));
        }
        e.insert(Shard::default())       // new branch — check limit
    }
};
```

This pattern is used in both `put()` and `apply_batch()`. The len-before-entry ordering is critical for DashMap safety.

### Write path

**`put(key, stored_value, mode)`** — Single key write:
1. Extract `branch_id` from key's namespace
2. Check branch limit (snapshot len, then entry)
3. Get-or-create shard via `DashMap::entry()`
4. If key exists: remove old TTL entry, update version chain
5. If key is new: create `Arc<Key>`, insert into FxHashMap + BTreeSet (if built)
6. Register new TTL entry if value has TTL

**`apply_batch(writes, deletes, version)`** — Atomic batch:
1. Capture `Timestamp::now()` once for entire batch (avoids per-write syscalls)
2. Group operations by `branch_id` into `FxHashMap<BranchId, (Vec<writes>, Vec<deletes>)>`
3. Check branch limit once
4. For each branch: acquire shard lock, apply all writes and deletes atomically
5. Advance global version via `fetch_max(version)`

Cross-branch atomicity caveat: if branch A succeeds and branch B fails, A's writes are visible. Callers must validate preconditions before calling.

### Read paths

| Method | Version filter | TTL check | Tombstone check |
|--------|---------------|-----------|-----------------|
| `get()` (Storage trait) | Latest only | Yes | Yes |
| `get_versioned(key, max_v)` | `<= max_version` | Yes | Yes |
| `get_direct(key)` | Latest only | Yes | Yes |
| `get_value_direct(key)` | Latest only | Yes | Yes (returns `Value` only, no `VersionedValue`) |
| `get_at_timestamp(key, ts)` | `ts <= max_ts` | Yes | Yes |
| `get_version_only(key)` | Latest only | Yes | Yes (returns raw `u64`, no clone) |

### Scan paths

All scans use the lazy BTreeSet via `ensure_ordered_keys()`:

| Method | Scope | Version filter |
|--------|-------|---------------|
| `list_branch(branch_id)` | All keys in branch | Latest |
| `list_by_prefix(prefix)` | Keys matching prefix | Latest |
| `list_by_type(branch_id, type_tag)` | Keys of specific TypeTag | Latest |
| `count_by_type(branch_id, type_tag)` | Count of specific TypeTag | Latest |
| `scan_prefix(prefix, max_v)` | Keys matching prefix | `<= max_version` |
| `scan_by_branch(branch_id, max_v)` | All keys in branch | `<= max_version` |
| `scan_prefix_at_timestamp(prefix, ts)` | Keys matching prefix | `ts <= max_ts` |

### Garbage collection

**`gc_branch(branch_id, min_version)`**:
1. Iterate all entries in the shard via `FxHashMap::retain()`
2. For each entry: call `VersionChain::gc(min_version)` to prune old versions
3. If chain becomes dead (single tombstone/expired): collect key, return `false` from retain
4. After retain: remove dead keys from TTL index and BTreeSet
5. Returns total pruned version count

**`expire_ttl_keys(now)`**:
- Iterates all branches, calls `ttl_index.remove_expired(now)` on each
- Only cleans the TTL index — expired keys are detected by `is_expired()` on read and removed by `gc_branch()` via `is_dead()`

---

## 5. ShardedSnapshot — MVCC Read View

**File:** `sharded.rs:1284-1499`

O(1) snapshot creation. No data copying.

```
ShardedSnapshot
+-------------------------------+
| version: u64                  |  captured at creation
| store: Arc<ShardedStore>      |  shared reference
+-------------------------------+
```

### Creation cost

```rust
pub fn snapshot(self: &Arc<Self>) -> ShardedSnapshot {
    ShardedSnapshot {
        version: self.version.load(Ordering::Acquire),  // ~1-5ns
        store: Arc::clone(self),                          // ~20-30ns
    }
}
```

Total: < 50ns. Contract: < 500ns.

### How MVCC reads work

All snapshot reads delegate to `store.get_versioned(key, self.version)`, which walks the version chain to find the first entry where `version_raw() <= snapshot_version`. This means:

- Writes after snapshot creation are invisible (higher version)
- Tombstones at or before snapshot version mark the key as deleted
- Expired entries at any version are filtered out

### SnapshotView trait impl

```rust
impl SnapshotView for ShardedSnapshot {
    fn get(&self, key: &Key) -> StrataResult<Option<VersionedValue>> {
        Storage::get_versioned(&*self.store, key, self.version)
    }
    fn scan_prefix(&self, prefix: &Key) -> StrataResult<Vec<(Key, VersionedValue)>> {
        // BTreeSet range scan with version filter
    }
    fn get_value_and_version(&self, key: &Key) -> StrataResult<Option<(Value, u64)>> {
        // Returns raw u64 version, skips VersionedValue construction
    }
}
```

---

## 6. Secondary Indices

### BranchIndex and TypeIndex

**File:** `index.rs`

Simple `HashMap<K, HashSet<Key>>` indices:

- **BranchIndex**: `BranchId → Set<Key>` — fast branch-scoped queries for replay
- **TypeIndex**: `TypeTag → Set<Key>` — primitive-type-specific queries

Both auto-cleanup empty sets on key removal. These are auxiliary structures used by the durability layer, not by `ShardedStore` itself (which uses its own DashMap-based branch sharding).

### TTLIndex

**File:** `ttl.rs`

```
TTLIndex
+---------------------------------------------+
| index: BTreeMap<Timestamp, HashSet<Key>>    |
+---------------------------------------------+
```

- **Sorted by expiry timestamp** for efficient range queries
- `find_expired(now)` — returns all keys where `expiry <= now`, O(expired count)
- `remove_expired(now)` — bulk cleanup, returns count removed
- Maintained by `ShardedStore::put()` (insert/update) and `gc_branch()` (cleanup)

---

## 7. Storage Trait Implementation

**File:** `sharded.rs:1509-1733`

`ShardedStore` implements `strata_core::traits::Storage`, providing the 12-method interface used by the concurrency layer:

| Method | Used by | Purpose |
|--------|---------|---------|
| `get(key)` | Direct reads | Latest value, filtered |
| `get_versioned(key, max_v)` | Snapshot reads | MVCC-filtered read |
| `get_history(key, limit, before)` | Time-travel API | Paginated version history |
| `put(key, value, ttl)` | Auto-versioning (vestigial) | Allocates version internally |
| `delete(key)` | Auto-versioning (vestigial) | Returns previous value |
| `scan_prefix(prefix, max_v)` | Snapshot prefix scan | BTreeSet range + version filter |
| `scan_by_branch(branch_id, max_v)` | Recovery replay | Full branch scan |
| `current_version()` | Version queries | Atomic load |
| `put_with_version(key, value, v, ttl)` | Transaction commit | External version assignment |
| `put_with_version_mode(key, value, v, ttl, mode)` | Transaction commit (replace) | With WriteMode |
| `delete_with_version(key, v)` | Transaction commit | External version tombstone |
| `get_version_only(key)` | Validation | Raw u64, no Value clone |

Note: `put()`, `delete()` (auto-versioning), and `scan_by_branch()` are vestigial — see #1386.

---

## 8. Data Flow Summary

### Write: Transaction commit

```
TransactionContext::commit()
  → for each write: store.put_with_version(key, value, commit_version, ttl)
    → StoredValue::new(value, Version::txn(v), ttl)
    → ShardedStore::put(key, stored, mode)
      → DashMap::entry(branch_id) → get or create Shard
      → FxHashMap: get_mut(key) → VersionChain::push(stored)
                   or insert(Arc::new(key), VersionChain::new(stored))
      → BTreeSet: insert(Arc::clone(key)) if built
      → TTLIndex: update if TTL present
    → AtomicU64::fetch_max(commit_version)
```

### Read: Snapshot get

```
ShardedSnapshot::get(key)
  → Storage::get_versioned(store, key, snapshot_version)
    → DashMap::get(branch_id) → Shard
    → FxHashMap::get(key) → VersionChain
    → VersionChain::get_at_version(snapshot_version)
      → scan newest-first for first entry where version <= snapshot_version
    → filter: !is_expired() && !is_tombstone()
    → StoredValue::versioned() → clone Value into VersionedValue
```

### Read: Prefix scan

```
ShardedSnapshot::scan_prefix(prefix)
  → DashMap::get_mut(branch_id) → Shard
  → Shard::ensure_ordered_keys() → build BTreeSet if None
  → BTreeSet::range(prefix..) → take_while(starts_with(prefix))
  → for each key: FxHashMap::get(key) → VersionChain::get_at_version()
  → filter + collect
```

---

## 9. Memory Model

### Per-entry cost

| Component | Size | Notes |
|-----------|------|-------|
| StoredValue | 56 bytes | Compile-time asserted |
| Arc\<Key\> in FxHashMap | ~64 bytes | Key + Arc overhead |
| Arc\<Key\> in BTreeSet | ~8 bytes | Shared Arc, just pointer |
| FxHashMap bucket | ~16 bytes | Hash + pointer |
| BTreeSet node overhead | ~64 bytes | B-tree internal nodes |

**Without BTreeSet**: ~136 bytes/entry
**With BTreeSet**: ~200 bytes/entry

BTreeSet only built on first scan — branches that never scan don't pay this cost.

### Per-version cost (multi-version keys)

Each additional version adds 56 bytes (`StoredValue`) plus VecDeque overhead (~24 bytes on promotion from Single to Multi).

### Scaling estimates

| Entries | Without BTreeSet | With BTreeSet |
|---------|-----------------|---------------|
| 1M | ~130 MB | ~190 MB |
| 10M | ~1.3 GB | ~1.9 GB |
| 100M | ~13 GB | ~19 GB |

---

## 10. Concurrency Model

```
Thread 1 (Branch A write)     Thread 2 (Branch B write)     Thread 3 (read)
         │                              │                          │
    DashMap shard 3                DashMap shard 7           DashMap shard 3
    (write lock)                  (write lock)              (read lock)
         │                              │                          │
    Shard A: FxHashMap            Shard B: FxHashMap          Shard A: FxHashMap
    (exclusive via shard lock)    (exclusive via shard lock)  (shared via shard lock)
```

- DashMap's 16-way internal sharding means most branch operations hit different shards
- Same-branch reads are lock-free (DashMap read guards)
- Same-branch writes serialize through the DashMap shard lock
- `apply_batch` holds the shard lock for the entire branch batch — atomic per-branch visibility

---

## 11. Known Issues (Filed)

| Issue | Problem |
|-------|---------|
| #1386 | 5 vestigial auto-versioning methods on Storage trait |
| #1387 | SnapshotView trait unnecessary — ShardedSnapshot is the only production impl |
| #1389 | ~~WriteMode::Replace~~ → Replaced by `WriteMode::KeepLast(usize)` |
| #1390 | Namespace tenant/app/agent fields waste ~93 bytes per Key |
| #1392 | PrimitiveStorageExt + PrimitiveRegistry are unused (~950 lines) |
