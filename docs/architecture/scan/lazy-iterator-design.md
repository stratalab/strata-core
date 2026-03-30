# Lazy Iterator Architecture — RocksDB-Aligned Scan Pipeline

> **Issue**: #2183 — `kv_scan` throughput bottleneck (~20 ops/s at 50K records)
>
> **Status**: Design document. Covers the full scan pipeline redesign: lazy merge,
> `BranchSnapshot`, persistent `StorageIterator`, and all consumer refactors.

---

## Problem

`kv_scan(Some(start_key), Some(10))` runs at ~20 ops/s on 50K records — orders of
magnitude slower than point reads (~650K ops/s). The root cause is architectural:
four separate functions duplicate an eager-collect pattern where every source
(memtable, frozen memtables, segments, inherited layers) is `.collect()`'d into a
`Vec` before being passed to `MergeIterator`.

**Current hot path** (per scan call on 50K records):

1. Storage collects ALL 50K entries from each source into `Vec`s
2. `MergeIterator` + `MvccIterator` processes all 50K
3. Transaction tracks all 50K in `read_set` for OCC conflict detection
4. KV primitive applies `skip_while(start)` + `take(10)` — discards 49,990

**Affected functions** (all share the same eager pattern):

| Function | Location | Purpose |
|---|---|---|
| `scan_prefix_from_branch` | `segmented/mod.rs:2918` | MVCC prefix scan |
| `count_prefix_from_branch` | `segmented/mod.rs:3019` | MVCC prefix count |
| `scan_prefix_at_timestamp` | `segmented/mod.rs:1741` | Timestamp-based scan |
| *(new)* `scan_range_from_branch` | — | Bounded range scan with limit |

---

## Design Goals

1. **O(log N + limit × log K)** per bounded scan (vs. current O(N))
2. **One abstraction, all consumers** — shared lazy merge builder
3. **Persistent iterator API** for cursor-based pagination
4. **RocksDB alignment** — `BranchSnapshot` ≈ SuperVersion, `StorageIterator` ≈ DBIter
5. **Zero invariant violations** — all 50 engine invariants hold

---

## RocksDB Alignment

| RocksDB Concept | Strata Equivalent | Notes |
|---|---|---|
| `SuperVersion` | `BranchSnapshot` | Captures `Arc<Memtable>` + `Arc<SegmentVersion>` via ref counting |
| `ArenaWrappedDBIter` | `StorageIterator` | Persistent Seek/Next API over a snapshot |
| `MergingIterator` (min-heap) | `MergeIterator` (existing) | Heap merge for >4 sources, linear for ≤4 |
| `DBIter` (MVCC filter) | `MvccIterator` (existing) | Deduplicates by logical key, filters by version |
| SST iterator (seeks block index) | `OwnedSegmentIter::new_seek` | Owns `Arc<KVSegment>`, seeks to block position |
| Memtable iterator (live SkipList cursor) | `Memtable::iter_range` | Borrows from SkipMap; collected per-seek for persistent mode |
| `GetReferencedSuperVersion` (thread-local caching) | `snapshot_branch` (short DashMap guard + Arc clone) | Guard released immediately after snapshot capture |
| Snapshot sequence number | `current_version()` | Consistent read boundary without transaction overhead |

**Patterns adopted from RocksDB**:
- Seek-based positioning (binary search to block, not linear scan)
- Lazy k-way merge with limit pushdown
- Snapshot isolation via ref-counted pinning (Arc)
- Source ordering: active → frozen → L0-L6 → inherited (LSM-003)
- Corruption flag propagation (checked after collection)

**Patterns deferred** (future optimization):
- Arena allocation for cache locality
- `IteratorWrapper` caching (eliminate virtual calls on `valid()`/`key()`)
- `max_skip_` reseek (Seek instead of linear scan when many versions skipped)
- Adaptive readahead / block prefetching

---

## Structural Prerequisite: `active: Memtable` → `active: Arc<Memtable>`

RocksDB's `SuperVersion` pins the active memtable via reference counting. Our
`BranchState.active` is currently `Memtable` (owned, not Arc), which prevents
snapshot capture without holding the DashMap guard.

**Change**: `active: Memtable` → `active: Arc<Memtable>`

**Safety**: `Memtable` has zero `&mut self` methods. All writes use interior
mutability (lock-free `crossbeam_skiplist::SkipMap` + atomics). `Arc<Memtable>`
is a transparent, mechanical change.

**Diff** (~15 lines across `segmented/mod.rs`):

```rust
// BranchState::new()
- active: Memtable::new(0),
+ active: Arc::new(Memtable::new(0)),

// Rotation (3 sites: rotate_memtable, maybe_rotate_branch, fork snapshot)
- let old = std::mem::replace(&mut branch.active, Memtable::new(next_id));
- old.freeze();
- branch.frozen.insert(0, Arc::new(old));
+ let old = std::mem::replace(&mut branch.active, Arc::new(Memtable::new(next_id)));
+ old.freeze();
+ branch.frozen.insert(0, old);  // already Arc
```

All `branch.active.foo()` calls auto-deref through Arc — no other changes needed.

**Invariant LSM-004 (memtable rotation atomicity)**: Holds. Rotation still occurs
under DashMap write lock (`get_mut()` / `entry()`). The `Arc::new()` call is a
local allocation that doesn't affect the atomicity of the replace+freeze+insert
sequence.

---

## Core Types

### `BranchSnapshot` — analogous to RocksDB's SuperVersion

Captures the state of a branch at a point in time. Holds Arc references to
prevent memtable rotation and compaction from invalidating the data.

```rust
pub(crate) struct BranchSnapshot {
    /// Active memtable at capture time.
    pub active: Arc<Memtable>,
    /// Frozen memtables at capture time (newest first).
    pub frozen: Vec<Arc<Memtable>>,
    /// Segment version at capture time.
    pub segments: Arc<SegmentVersion>,
    /// Inherited COW layers at capture time.
    pub inherited_layers: Vec<InheritedLayer>,
}
```

**Capture** (under short DashMap read guard):

```rust
pub fn snapshot_branch(&self, branch_id: &BranchId) -> Option<BranchSnapshot> {
    let branch = self.branches.get(branch_id)?;
    Some(BranchSnapshot {
        active: Arc::clone(&branch.active),
        frozen: branch.frozen.clone(),         // Vec<Arc> — cheap
        segments: branch.version.load_full(),  // Arc from ArcSwap
        inherited_layers: branch.inherited_layers.clone(),
    })
    // DashMap guard drops here
}
```

**Lifecycle**: Compaction creates new `SegmentVersion` and tries to delete old
segment files. The `BranchSnapshot` holds `Arc<SegmentVersion>` which holds
`Arc<KVSegment>` for each segment. Segment ref registry (COW-001) prevents
deletion while any Arc reference exists. This is the same safety chain as
RocksDB's SuperVersion → Version → FileMetaData ref counting.

### `StorageIterator` — analogous to RocksDB's ArenaWrappedDBIter

Persistent, seekable iterator over a branch snapshot. Supports multiple
`seek()` + `next()` cycles without re-acquiring the DashMap guard.

```rust
pub struct StorageIterator {
    snapshot: BranchSnapshot,
    prefix: Key,
    snapshot_version: u64,
    /// Current merge pipeline. Fully self-contained (no borrows).
    /// Rebuilt on each seek().
    pipeline: Option<Box<dyn Iterator<Item = (Key, VersionedValue)>>>,
    corruption_flags: Vec<Arc<AtomicBool>>,
}

impl StorageIterator {
    /// Create from a branch snapshot.
    pub fn new(snapshot: BranchSnapshot, prefix: Key, snapshot_version: u64) -> Self;

    /// Seek to first entry >= target within prefix. O(log N) per source.
    pub fn seek(&mut self, target: &Key) -> StrataResult<()>;

    /// Advance to next live entry. O(log K) per call.
    pub fn next(&mut self) -> Option<(Key, VersionedValue)>;

    /// True if the last seek()/next() positioned at a valid entry.
    pub fn valid(&self) -> bool;

    /// Check if any segment reported corruption during iteration.
    pub fn check_corruption(&self) -> StrataResult<()>;
}
```

**Seek implementation**:
1. Drop old pipeline
2. Collect entries from `snapshot.active` and each `snapshot.frozen[i]` starting
   from `target` position (bounded by write_buffer_size)
3. Create `OwnedSegmentIter::new_seek(...)` for each overlapping segment
4. Wrap inherited layer segments in `RewritingIterator`
5. Build `MergeIterator` → `MvccIterator` → tombstone/TTL filter → start_key filter
6. Store as `self.pipeline`

**No self-referential problem**: Memtable entries consumed via `Vec::into_iter()`
(owned). Segment entries via `OwnedSegmentIter` (owns Arc). All iterator sources
are `'static`.

**Next implementation**: `self.pipeline.as_mut()?.next()`

---

## Two Iteration Modes

### Mode 1: One-shot (DashMap guard, fully lazy)

Used by `scan_prefix`, `count_prefix`, `scan_prefix_at_timestamp`, `scan_range`.
Holds DashMap read guard during iteration. Memtable iterators borrow from guard
lifetime — zero collection overhead.

```
DashMap::get() → &BranchState (guard held for function duration)
  → build_branch_merge_iter<'b>(branch, prefix, start_key)
    Sources (all lazy):
      active.iter_range(start_key, prefix)     — borrows 'b
      frozen[i].iter_range(start_key, prefix)  — borrows 'b (through Arc deref)
      OwnedSegmentIter::new_seek(Arc::clone)   — 'static
      RewritingIterator(OwnedSegmentIter)      — 'static
    → MergeIterator<Box<dyn Iterator + 'b>>
  → MvccIterator → filter → .take(limit).collect()
  → check_corruption()
→ guard drops
```

**Lifetime**: `'b` ties to `&BranchState`. `OwnedSegmentIter` is `'static`, which
satisfies any `'b`. Both coexist in `Vec<Box<dyn Iterator<...> + 'b>>`.

**DashMap guard blocks rotation**: The read guard prevents concurrent `get_mut()`
(memtable rotation, writes via `entry()`). For bounded scans (limit=10), the guard
is held for microseconds. For full scans, the guard duration equals iteration time.
This is acceptable — RocksDB's SuperVersion causes the same effect (old memtable
pinned until iterator is destroyed).

### Mode 2: Persistent (BranchSnapshot, Seek/Next API)

Used by `StorageIterator`. Captures `BranchSnapshot` under short guard, releases
immediately. Memtable entries collected per-`seek()` call (bounded).

```
DashMap::get() → BranchSnapshot (Arc clones) → guard drops immediately
StorageIterator::seek(target)
  → collect active/frozen entries from target (Vec::into_iter — owned)
  → OwnedSegmentIter::new_seek per segment (owns Arc)
  → MergeIterator → MvccIterator → pipeline stored in self
StorageIterator::next()
  → pipeline.next() — O(log K) per call
```

**When to use which**:

| Use case | Mode | Why |
|---|---|---|
| `kv_scan(start, limit)` | One-shot | Fastest: fully lazy, no memtable collection |
| `count_prefix` | One-shot | No allocation needed, just count |
| Cursor pagination | Persistent | Multiple seeks, iterator reused |
| Streaming large results | Persistent | Snapshot survives guard release |

---

## Shared Infrastructure

### `build_branch_merge_iter<'b>()` — one-shot mode

```rust
fn build_branch_merge_iter<'b>(
    branch: &'b BranchState,
    prefix: &Key,
    start_key: &Key,
) -> StrataResult<(
    MergeIterator<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)> + 'b>>,
    Vec<Arc<AtomicBool>>,  // corruption flags
)>
```

Builds lazy merge from all sources in LSM-003 order. Corruption flags returned
for post-iteration checking.

### `build_snapshot_merge_iter()` — persistent mode

```rust
fn build_snapshot_merge_iter(
    snapshot: &BranchSnapshot,
    prefix: &Key,
    start_key: &Key,
) -> StrataResult<(
    MergeIterator<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>>,
    Vec<Arc<AtomicBool>>,
)>
```

Same ordering, but memtable entries collected (owned). All sources `'static`.

### `check_corruption()` — shared helper

```rust
fn check_corruption(flags: &[Arc<AtomicBool>]) -> StrataResult<()> {
    for flag in flags {
        if flag.load(Ordering::Relaxed) {
            return Err(StrataError::corruption(
                "segment scan stopped due to data block corruption",
            ));
        }
    }
    Ok(())
}
```

---

## Consumer Refactors

All four consumers delegate to `build_branch_merge_iter`:

### `scan_prefix_from_branch` (refactored)

```rust
fn scan_prefix_from_branch(branch, prefix, max_version) {
    let (merge, flags) = Self::build_branch_merge_iter(branch, prefix, prefix)?;
    let mvcc = MvccIterator::new(merge, max_version);
    let results: Vec<_> = mvcc
        .filter_map(|(ik, entry)| {
            if entry.is_tombstone || entry.is_expired() { return None; }
            let (key, commit_id) = ik.decode()?;
            Some((key, entry.to_versioned(commit_id)))
        })
        .collect();
    check_corruption(&flags)?;
    Ok(results)
}
```

### `count_prefix_from_branch` (refactored)

```rust
fn count_prefix_from_branch(branch, prefix, max_version) {
    let (merge, flags) = Self::build_branch_merge_iter(branch, prefix, prefix)?;
    let mvcc = MvccIterator::new(merge, max_version);
    let count = mvcc
        .filter(|(_, entry)| !entry.is_tombstone && !entry.is_expired())
        .filter(|(ik, _)| ik.decode().is_some())
        .count() as u64;
    check_corruption(&flags)?;
    Ok(count)
}
```

### `scan_prefix_at_timestamp` (refactored)

```rust
pub fn scan_prefix_at_timestamp(branch, prefix, max_timestamp) {
    let (merge, flags) = Self::build_branch_merge_iter(branch, prefix, prefix)?;
    // Existing custom timestamp-based dedup over the lazy merge
    let mut results = Vec::new();
    let mut last_typed_key: Option<Vec<u8>> = None;
    let mut found_for_current = false;
    for (ik, entry) in merge {
        // ... existing timestamp filtering logic unchanged ...
    }
    check_corruption(&flags)?;
    Ok(results)
}
```

### `scan_range_from_branch` (new)

```rust
fn scan_range_from_branch(branch, prefix, start_key, max_version, limit) {
    let (merge, flags) = Self::build_branch_merge_iter(branch, prefix, start_key)?;
    let mvcc = MvccIterator::new(merge, max_version);
    let filtered = mvcc
        .filter_map(|(ik, entry)| {
            if entry.is_tombstone || entry.is_expired() { return None; }
            let (key, commit_id) = ik.decode()?;
            Some((key, entry.to_versioned(commit_id)))
        })
        .filter(|(key, _)| key >= start_key);  // block seek imprecision
    let results: Vec<_> = match limit {
        Some(n) => filtered.take(n).collect(),
        None => filtered.collect(),
    };
    check_corruption(&flags)?;
    Ok(results)
}
```

---

## Block Seek Imprecision

Segment iterators seek to a **block** position, not an exact entry. Entries before
`start_key` within the same data block pass the prefix check and are emitted.

**Fix**: `.filter(|(key, _)| key >= start_key)` applied after MVCC/tombstone
filtering, before `.take(limit)`. `Key` implements `Ord` with ordering consistent
with byte encoding (`types.rs:466`).

Memtable `iter_range` uses SkipMap `range()` which is exact — no imprecision.

---

## Engine Layer Changes

### `Database` (pass-throughs)

```rust
pub(crate) fn scan_range(&self, prefix, start_key, max_version, limit)
    -> StrataResult<Vec<(Key, VersionedValue)>> {
    self.storage.scan_range(prefix, start_key, max_version, limit)
}

pub(crate) fn storage_iterator(&self, branch_id, prefix, max_version)
    -> Option<StorageIterator> {
    let snapshot = self.storage.snapshot_branch(branch_id)?;
    Some(StorageIterator::new(snapshot, prefix, max_version))
}
```

### `KVStore::scan()` (rewritten)

```rust
pub fn scan(&self, branch_id, space, start, limit) -> StrataResult<Vec<(String, Value)>> {
    let ns = self.namespace_for(branch_id, space);
    let prefix = Key::new_kv(Arc::clone(&ns), "");
    let start_key = Key::new_kv(ns, start.unwrap_or(""));
    let snapshot = self.db.current_version();
    let results = self.db.scan_range(&prefix, &start_key, snapshot, limit)?;
    Ok(results.into_iter()
        .filter_map(|(key, vv)| key.user_key_string().map(|k| (k, vv.value)))
        .collect())
}
```

### `KVStore::scan_iter()` (new — persistent iterator)

```rust
pub fn scan_iter(&self, branch_id: &BranchId, space: &str) -> StrataResult<StorageIterator> {
    let ns = self.namespace_for(branch_id, space);
    let prefix = Key::new_kv(ns, "");
    let snapshot_version = self.db.current_version();
    self.db.storage_iterator(branch_id, prefix, snapshot_version)
        .ok_or_else(|| StrataError::branch_not_found(*branch_id))
}
```

---

## Invariant Verification

| Invariant | Result | Rationale |
|---|---|---|
| LSM-003 | HOLDS | Source ordering (active→frozen→L0-L6→inherited) preserved in both build functions |
| LSM-004 | HOLDS | Arc<Memtable> rotation still atomic under DashMap write lock |
| LSM-007 | HOLDS | Segment files unchanged; OwnedSegmentIter reads via pread |
| COW-001 | HOLDS | BranchSnapshot holds Arc<KVSegment> → refcount prevents deletion |
| COW-003 | HOLDS | RewritingIterator applies fork_version gate in both modes |
| MVCC-001 | HOLDS | `current_version()` provides snapshot; MvccIterator filters by it |
| MVCC-002 | HOLDS | Tombstone filter preserved in all consumers |
| MVCC-006 | HOLDS | `is_expired()` check preserved; MvccIterator dedup prevents resurrection |
| ARCH-002 | HOLDS | Snapshot via `current_version()`; BranchSnapshot pins consistent state |
| SCALE-009 | HOLDS | Same MergeIterator algorithm; Box<dyn Iterator + 'b> is transparent |

---

## Performance Expectations

| Scenario | Before | After |
|---|---|---|
| `kv_scan(mid, limit=10)` on 50K keys | O(50K) — ~20 ops/s | O(log 50K + 10) — ~50K+ ops/s |
| `scan_prefix` (full scan, 50K keys) | 2× O(50K) — per-source + merge | O(50K) — merge only, no intermediate Vecs |
| `count_prefix` (50K keys) | 2× O(50K) + Vec alloc | O(50K) — count only, zero allocation |
| Cursor pagination (100 pages × 10) | 100 × O(50K) | 1 snapshot + 100 × O(log N + 10) |

---

## Test Plan

1. **`cargo test`** — all existing tests pass (refactored functions preserve semantics)
2. **New unit tests**:
   - `test_memtable_iter_range_seeks_correctly` — iter_range starts from target, not prefix start
   - `test_owned_segment_iter_new_seek_with_prefix` — seek + prefix matching on OwnedSegmentIter
   - `test_scan_range_with_limit` — bounded scan returns exactly limit entries
   - `test_scan_range_block_imprecision_filtered` — entries before start_key not returned
   - `test_branch_snapshot_survives_rotation` — snapshot valid after memtable rotation
   - `test_storage_iterator_seek_next` — persistent iterator Seek + multiple Next calls
   - `test_storage_iterator_reseek` — re-Seek to different position works correctly
3. **Benchmark**: `cargo bench --bench memory_efficiency -- -q` — scan ops/s: ~20 → thousands
