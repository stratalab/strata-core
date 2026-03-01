# Read/Write Algorithms and Concurrency

**Status:** Proposal
**Author:** Design Team
**Date:** 2026-03-01

> **Part of the Strata Storage Architecture proposal.**
> - [Architecture Overview](tiered-storage.md) — design vision, principles, system model
> - [File Layout Specification](storage-file-layout.md) — on-disk formats for WAL, manifests, and segments
> - **Read/Write Algorithms** (this document) — concurrency model, ACID commit path, lock-free reads
> - [Memtable and Flush Pipeline](memtable-flush-file.md) — memtable design, flush file format, recovery
> - [Industry Research](tiered-storage-research.md) — reference analysis of Redis, RocksDB, LMDB, et al.

This document specifies the runtime algorithms for reads, writes, and concurrency. It covers the ACID commit path, lock-free read algorithms (point lookup, prefix scan), state publication, and background flush coordination.

---

## 1. Concurrency Model

### 1.1 Fundamental Choices

* **Reads are lock-free** — no mutex on the read path.
* **Commits are serialized per branch** — one writer at a time per branch, but the critical section is small.
* Background work (flush, segment build, compaction) runs concurrently and never blocks reads; it publishes new immutable state via atomic pointer swaps and manifest updates.

This preserves existing Strata semantics and keeps correctness tractable.

### 1.2 Snapshot Model

A snapshot is:

* `(branch_id, snapshot_commit_id, manifest_epoch)`
* plus an `Arc` to immutable state roots (segment lists, memtable pointers)

Snapshot creation is O(1): atomic loads + Arc clones. No memory is pinned and no tracker registration is required — the `Arc` reference keeps the state alive until the snapshot is dropped. See [Architecture Overview §13](tiered-storage.md) for the design rationale.

---

## 2. Key Data Structures

### 2.1 BranchState

Per-branch state, stored in a concurrent map keyed by `BranchId`.

```text
BranchState {
  // Writers
  commit_lock: Mutex<()>              // serialize commit per branch

  // MVCC
  head_commit: AtomicU64              // latest committed commit_id

  // In-memory write buffer(s)
  mem_active: ArcSwap<Memtable>       // current mutable memtable
  mem_immut: LockFreeList<Memtable>   // frozen memtables awaiting flush (readable)

  // Visible immutable segments for this branch view
  seg_view: ArcSwap<SegmentView>      // atomically swapped on manifest updates

  // Optional: per-branch delta overlays (for writes after fork)
  overlay_policy: OverlayPolicy

  // GC / retention watermarks
  low_watermark_commit: AtomicU64
}
```

Readers only touch atomics and immutable structures — no mutexes on the read path.

### 2.2 Memtable

The memtable provides Redis-class latency for hot keys. It must be:

* Concurrent for reads (lock-free)
* Cheap for writes (single-writer per branch already serialized)
* Ordered by `(TypedKey ASC, commit_id DESC)` to support prefix scans and flush without transformation

**Design decision: concurrent skiplist.** A lock-free/concurrent skiplist (e.g., `crossbeam_skiplist::SkipMap` or custom with epoch GC) provides:

* Lock-free reads
* Natural prefix scan support (no separate `BTreeSet`)
* Sorted iteration for flush (entries stream directly into segment blocks)

A hybrid hash + ordered structure was considered for faster point lookups but adds complexity and memory overhead. Since `list_by_type` is a core Strata operation, native ordered iteration is worth the small constant-factor cost on point reads.

The memtable stores **multiple versions per key** (full MVCC). Newest versions sort first due to the descending commit_id encoding.

See [Memtable and Flush Pipeline §2](memtable-flush-file.md) for the complete memtable specification including entry layout, memory accounting, and the get/scan algorithms.

### 2.3 SegmentView (immutable state root)

What readers consult after memtables. Published atomically via `ArcSwap`.

```text
SegmentView {
  // ordered from newest → oldest (overlay first)
  kv_segments: Vec<Arc<KVSegmentHandle>>
  bm25_segments: Vec<Arc<BM25SegmentHandle>>
  vec_segments: Vec<Arc<VecSegmentHandle>>
  graph_segments: Vec<Arc<GraphSegmentHandle>>

  // optional acceleration: segment bloom filter pointers, key-range hints
  kv_hints: Vec<KVHint>
}
```

Segment handles are immutable and mmap-backed. Old views remain alive until all readers drop their `Arc` references.

### 2.4 Block Cache

For mmap segments, the OS page cache already handles much of the caching. An application-level block cache provides bounded caching for:

* Index blocks
* Filter (bloom) blocks
* Hot data blocks (when not relying exclusively on mmap)

**Design decision: sharded CLOCK cache** (or segmented LRU). Per-shard mutex (not global). Reads that hit cache take only the shard lock; misses fall back to mmap. Because reads often hit the OS page cache anyway, this cache can be small and contention stays low.

---

## 3. Write Algorithm (ACID Commit Path)

### 3.1 Transaction Object

A transaction accumulates:

* `read_set`: `(key, observed_commit_id)` — for OCC validation
* `write_set`: `(key, op, value, cas_expected?)` — pending mutations

It also records:

* `branch_id`
* `snapshot_commit_id` (start snapshot)

### 3.2 Commit Algorithm (per-branch serialization)

This preserves the existing Strata OCC flow but changes the storage calls.

```text
commit(tx):
  bs = branch_state(tx.branch_id)

  lock(bs.commit_lock)

  // 1) Validate read set (optimistic concurrency control)
  for (key, observed) in tx.read_set:
      current = get_version_only(bs, key)   // lock-free read
      if current != observed:
          unlock(bs.commit_lock)
          return CONFLICT

  // 2) Allocate commit id
  commit_id = global_commit_id.fetch_add(1)

  // 3) WAL append + fsync (durability point)
  wal.append(CommitRecord{commit_id, branch_id, writes...})
  wal.fsync()

  // 4) Apply writes to mem_active (in-memory)
  mt = bs.mem_active.load()
  for write in tx.write_set:
      memtable_put(mt, write.key, commit_id, write)

  // 5) Advance branch head
  bs.head_commit.store(commit_id)

  unlock(bs.commit_lock)
  return OK
```

**ACID properties:**

* **Atomicity:** WAL record defines the commit as one unit; recovery replays it as a unit. Commit completion = WAL fsync. If a crash occurs between WAL fsync and memtable insertion, recovery replays the committed WAL record into a fresh memtable.
* **Durability:** fsync before acknowledging success.
* **Isolation:** Snapshot Isolation (SI). Each transaction reads from a consistent snapshot. OCC read_set validation under the per-branch commit lock detects write-write conflicts. SI prevents dirty reads, non-repeatable reads, and phantom reads. It does not prevent write skew (see [Architecture Overview §6](tiered-storage.md)).
* **Consistency:** enforced by higher layers and invariants.

See [File Layout §3.2](storage-file-layout.md) for the WAL record format.

### 3.3 Hot-Path Performance and Durability Policies

For hot keys, the critical work after WAL fsync is a small number of in-memory skiplist inserts. The CPU cost matches Redis — but **durable Redis (AOF fsync always) is not the same latency as in-memory Redis**. The fsync policy dominates p99 latency for durable writes.

Supported durability policies:

* **fsync always** (strict) — per-commit durability, highest latency
* **fsync every N ms** (group commit) — batches fsync across commits, good latency/durability tradeoff
* **fsync on shutdown** (dev) — lowest latency, data at risk on crash

Same ACID model, different latency profile. Applications choose based on their durability requirements.

---

## 4. Read Algorithms

Reads are lock-free, MVCC-correct, and branch-aware.

### 4.1 Point Lookup

```text
get(branch_id, snapshot_commit, key):
  bs = branch_state(branch_id)

  // 1) Check active memtable
  mt = bs.mem_active.load()
  v = memtable_get_versioned(mt, key, snapshot_commit)
  if v.found: return v

  // 2) Check immutable memtables (frozen, awaiting flush)
  for mt2 in bs.mem_immut.iter():
      v = memtable_get_versioned(mt2, key, snapshot_commit)
      if v.found: return v

  // 3) Check immutable KV segments (newest → oldest)
  view = bs.seg_view.load()
  return kv_segments_get(view.kv_segments, key, snapshot_commit)
```

#### memtable_get_versioned (skiplist-based)

With memtable keys ordered by `(TypedKey ASC, commit_id DESC)`:

* Seek to `(key, +∞)` — i.e., `TypedKeyBytes || EncodeDesc(u64::MAX)`
* Iterate forward while key matches
* Return first entry with `commit_id <= snapshot_commit`

This is O(log n + k) where k is the number of versions in the memtable for that key — usually 1 or 2.

See [Memtable §2.5](memtable-flush-file.md) for the complete get algorithm.

### 4.2 Segment Point Lookup

Each KV segment is sorted by `(TypedKey ASC, commit_id DESC)`. Segments are scanned newest-first to find the most recent version.

```text
kv_segments_get(segments_newest_first, key, snapshot_commit):
  for seg in segments_newest_first:
      if seg.key_range_does_not_cover(key): continue
      if seg.bloom_maybe_contains(key) == false: continue

      v = seg.point_lookup(key, snapshot_commit)
      if v.found: return v

  return NOT_FOUND
```

#### seg.point_lookup(key, snapshot_commit)

* Binary search index block to locate candidate data block
* Binary search within block to first entry with `TypedKey == key`
* Scan within that key's version-run (usually contiguous) until `commit_id <= snapshot_commit`
* Return value or tombstone

All of this is read-only; with mmap it is mostly pointer chasing and OS page faults on cold data. See [File Layout §5](storage-file-layout.md) for the KV segment format.

### 4.3 Prefix Scan Algorithm (`list_by_type`)

Prefix scan is a first-class operation. It is MVCC-correct, stable under concurrent writers, and does not rely on a resident ordered key set.

```text
scan_prefix(branch_id, snapshot_commit, prefix):
  bs = branch_state(branch_id)

  iters = []

  // memtables
  iters.push(memtable_iter_seek(bs.mem_active.load(), prefix))
  for mt in bs.mem_immut.iter():
      iters.push(memtable_iter_seek(mt, prefix))

  // segments
  view = bs.seg_view.load()
  for seg in view.kv_segments:
      iters.push(seg.iter_seek(prefix))

  // k-way merge of sorted streams by (TypedKey ASC, commit_id DESC)
  merged = merge_iters(iters)

  // produce newest visible version per logical key
  last_key = None
  for entry in merged:
      if not entry.key.starts_with(prefix): break

      if entry.key != last_key:
          if entry.commit_id <= snapshot_commit:
              if entry.is_tombstone: skip
              else: yield(entry.key, entry.value)
              last_key = entry.key
          else:
              // commit newer than snapshot, keep scanning within same key
              continue
      else:
          continue
```

Because streams are ordered by `(key ASC, commit_id DESC)`, the merge iterator naturally visits newest versions first for each key. `list_by_type` uses prefix = `(namespace, type_tag)`.

---

## 5. Lock-Free Publication and Memory Reclamation

### 5.1 Memtable Rotation (freeze/rotate)

When the active memtable hits its size threshold:

```text
rotate_memtable(bs):
  old = bs.mem_active.load()

  // create new empty memtable
  new = Memtable::new()
  bs.mem_active.store(new)          // atomic pointer swap

  // mark old as immutable and enqueue for flush
  bs.mem_immut.push(old)
  schedule_flush(old)
```

Readers that already loaded `old` continue safely because they hold an `Arc` reference.

**Implementation:** `ArcSwap<Memtable>` for safe publication. Simple and hard to misuse. A raw-pointer + crossbeam epoch GC approach offers higher performance but significantly more complexity — start with `ArcSwap`.

See [Memtable and Flush Pipeline §3](memtable-flush-file.md) for rotation triggers and the complete rotation algorithm.

### 5.2 SegmentView Publication

When compaction or a new segment build produces a new `SegmentView`:

```text
publish_new_view(bs, new_view):
  bs.seg_view.store(Arc::new(new_view))
```

No locks for readers. Old views remain alive until all readers drop their `Arc` references.

---

## 6. Background Flush (memtable → KV segment)

Flush is the bridge from hot in-memory data to durable immutable segments.

### 6.1 Flush Procedure

1. Iterate memtable entries in sorted order (skiplist is already ordered).
2. Build data blocks (64 KB typical), prefix-compress keys, encode entries.
3. Build index block and bloom filter over TypedKey (without commit_id).
4. Write segment file to `tmp/`, fsync, rename into `segments/` dir.
5. Publish new MANIFEST / SegmentView that includes the segment.
6. Once manifest is published, remove memtable from `mem_immut` and drop it.

Flush does not block reads. Reads consult the memtable until the segment is visible, then the memtable can be safely dropped.

See [Memtable and Flush Pipeline §4-6](memtable-flush-file.md) for the complete flush pipeline including the intermediate flush file format and crash recovery.

---

## 7. CAS Semantics and Conflict Detection

### 7.1 CAS via read_set Validation (recommended)

A CAS operation records `(key, expected_commit)` in the transaction's `read_set`. The commit fails if the latest visible commit for that key does not match `expected_commit`.

This stays consistent with the OCC model — CAS is just a specific read_set entry.

### 7.2 CAS Stored in WAL Record

Including `cas_expected_commit` in the WAL record enables recovery to re-validate before replay. Stronger guarantee but more complex. Usually unnecessary given that OCC validation happens before WAL write.

---

## 8. Performance Contract

### Hot Reads (memtable hit)

Lock-free skiplist seek. O(log M) where M is bounded by `write_buffer_size` (default 128 MB). At ~128K entries, this is ~17 comparisons — sub-microsecond on modern CPUs. This is ~3-5x more comparisons than Redis's O(1) hash lookup but avoids the memory overhead of a separate hash index. See [Architecture Overview §9](tiered-storage.md) for the full latency analysis.

### Hot Writes (memtable insert)

O(log M) skiplist insert where M is bounded by `write_buffer_size` (default 128 MB). Plus WAL append cost.

### Durability Dominates Tail Latency

The fsync policy determines p99 latency for writes. Group commit (fsync every 1-10ms) provides the best latency/durability tradeoff for workloads that need persistence.

### What Is Lock-Free

| Component | Lock-free? |
|-----------|-----------|
| Memtable reads (skiplist) | Yes |
| Segment reads (mmap) | Yes |
| SegmentView publication (ArcSwap) | Yes |
| Commit serialization | Per-branch mutex (small critical section) |
| Memtable rotation | Atomic swap, no global lock |

This is the sweet spot: correctness + high read concurrency + simple failure modes.

---

## 9. Implementation Checklist

1. Use `ArcSwap<Memtable>` and `ArcSwap<SegmentView>` for safe publication.
2. Memtable is ordered (skiplist) so scans do not need a separate ordered structure.
3. Encode KV segments sorted by `(TypedKey ASC, commit_id DESC)` for natural MVCC.
4. Provide merge iterators as a core primitive (point lookup, prefix scan).
5. Make `list_by_type` a first-class prefix scan over encoded keys.
6. Never introduce a permanent global in-memory key directory.
