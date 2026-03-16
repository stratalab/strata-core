# Strata Storage Architecture

**Status:** Proposal
**Author:** Design Team
**Date:** 2026-03-01

> **This is the architecture overview.** Companion specifications:
> - [File Layout Specification](storage-file-layout.md) — on-disk formats for WAL, manifests, and segments
> - [Read/Write Algorithms](read-write-algorithms.md) — concurrency model, ACID commit path, lock-free reads
> - [Memtable and Flush Pipeline](memtable-flush-file.md) — memtable design, flush file format, recovery
> - [Industry Research](tiered-storage-research.md) — reference analysis of Redis, RocksDB, LMDB, et al.

---

## 1. Problem Statement

Strata is **fully in-memory** for KV data. The storage engine (`ShardedStore` at `crates/storage/src/sharded.rs:260`) uses a `DashMap<BranchId, Shard>` where each `Shard` (`sharded.rs:186`) contains:
- `data: FxHashMap<Key, VersionChain>` — O(1) lookups per key
- `ordered_keys: BTreeSet<Key>` — O(log n) prefix scans

Each `VersionChain` (`sharded.rs:56`) holds a `VecDeque<StoredValue>` storing every version of a key. There is **no configurable memory bound** — memory grows linearly with keys × versions.

Similarly, the vector subsystem defaults to `VectorData::InMemory(Vec<f32>)` (`crates/engine/src/primitives/vector/heap.rs:27`), HNSW graphs use heap-allocated `BTreeMap<VectorId, HnswNode>` (`hnsw.rs:152`), and the BM25 active segment uses `DashMap<String, PostingList>` (`crates/engine/src/search/index.rs:238`).

For an embedded database, unbounded memory growth is a correctness problem — the host application should never OOM because its database grew.

### Concrete Scenario: Document Ingestion

A typical document ingestion pipeline (see `docs/design/document-ingestion.md`) produces multiple KV entries per source document: chunks, embeddings, metadata, and BM25 index entries.

Per chunk (384-dimensional embeddings):
```
KV entry:  ~200 bytes (Key namespace + user_key + StoredValue overhead)
           Key: Namespace (~120B) + TypeTag (1B) + user_key (~40B)
           StoredValue: VersionedValue (Value::Bytes ~512B avg) + ttl + tombstone flag
KV index:  ~200 bytes (duplicate Key in BTreeSet<Key>)
Embedding: 384 × 4 = 1,536 bytes (Vec<f32> in VectorHeap)
HNSW node: ~800 bytes (HnswNode with BTreeSet neighbors, ~2 layers × M=16)
BM25:      ~150 bytes (PostingEntry per unique term, ~12B each × ~12 terms/chunk)
Inline metadata: ~80 bytes (BTreeMap<VectorId, InlineMeta> entry)
─────────────────────────────────
Total:     ~2,966 bytes per chunk ≈ 2.9 KB
```

At scale:
```
  50K chunks (small corpus):    ~137 MB
 200K chunks (medium corpus):   ~547 MB
   1M chunks (large corpus):  ~1,483 MB ≈ 1.5 GB
   5M chunks (enterprise):    ~7,415 MB ≈ 7.2 GB
```

This memory is **never released** — there is no eviction, no spill-to-disk for KV data, and the default vector mode keeps all embeddings on the heap. An embedded database sharing an application's process cannot consume this much memory without coordination.

---

## 2. Current State Audit

| # | Data Structure | Location | Growth Pattern | Bounded? | File |
|---|---------------|----------|---------------|----------|------|
| 1 | KV version chains | Heap (`VecDeque<StoredValue>`) | keys × versions | No | `crates/storage/src/sharded.rs:60` |
| 2 | Ordered key index | Heap (`BTreeSet<Key>`) | unique keys | No | `sharded.rs:190` |
| 3 | Vector embeddings (InMemory) | Heap (`Vec<f32>`) | vectors × dimension | No | `crates/engine/src/primitives/vector/heap.rs:27` |
| 4 | Vector embeddings (Mmap) | mmap (`.svec` file) | vectors × dimension | Yes (OS-paged) | `crates/engine/src/primitives/vector/mmap.rs:100` |
| 5 | Vector embeddings (Tiered) | mmap base + heap overlay | overlay bounded | Partially | `heap.rs:34` |
| 6 | HNSW graph (active) | Heap (`BTreeMap<VectorId, HnswNode>`) | vectors × layers × M | No | `crates/engine/src/primitives/vector/hnsw.rs:152` |
| 7 | HNSW graph (sealed/compact) | Heap or mmap (`.shgr` file) | vectors × neighbors | Mmap: Yes | `hnsw.rs:991` |
| 8 | BM25 active segment | Heap (`DashMap<String, PostingList>`) | terms × docs | No (until sealed) | `crates/engine/src/search/index.rs:238` |
| 9 | BM25 sealed segments | mmap (`.sidx` files) | terms × docs | Yes (OS-paged) | `search/index.rs:246` |
| 10 | TTL index | Heap (`BTreeMap<Timestamp, HashSet<Key>>`) | keys with TTL | No | `crates/storage/src/ttl.rs:22` |
| 11 | Tombstone index | Heap (`HashMap<TombstoneKey, Vec<Tombstone>>`) | total deletes | No | `crates/durability/src/compaction/tombstone.rs:274` |
| 12 | Inline metadata | Heap (`BTreeMap<VectorId, InlineMeta>`) | vectors | No | `heap.rs:103` |
| 13 | Doc ID map | Heap (`DashMap` + `RwLock<Vec>`) | indexed docs | No | `search/index.rs:250` |

### Key Observation: Strata Already Has Segments

The vector and BM25 subsystems already implement a segment lifecycle:
- **Vector:** Active `HnswGraph` → sealed `CompactHnswGraph` → mmap-backed `.shgr` files
- **BM25:** Active `DashMap` segment → sealed `.sidx` mmap segments
- **Embeddings:** Heap `Vec<f32>` → mmap `.svec` files with overlay

The gap is **KV data has no segment model**. Version chains grow unbounded on the heap, the ordered key index (`BTreeSet<Key>`) must be fully resident, and there is no sealed/immutable representation for KV data.

---

## 3. Industry Analysis

> **Deep dive:** See [`tiered-storage-research.md`](tiered-storage-research.md) for detailed implementation analysis of Redis, DragonflyDB, RocksDB, redb, sled, LMDB, SQLite, WiredTiger, and TigerBeetle.

| System | Storage Model | Memory Control | Key Insight |
|--------|--------------|---------------|-------------|
| **SQLite** | Page cache (LRU) | `PRAGMA cache_size` (default 2 MB) | Disk is truth; memory is bounded cache |
| **Redis** | In-memory primary | `maxmemory` + eviction | Memory-first; eviction is last resort |
| **RocksDB** | LSM tree (memtable → levels) | `write_buffer_size` + block cache | Memtable is bounded write buffer |
| **LMDB** | mmap-only (CoW B+tree) | Zero config (OS manages paging) | No application-level cache at all |
| **Git** | Content-addressed objects + refs | Pack files + index | Branches are cheap pointers; objects are immutable |
| **Datomic** | Append-only log + segments | Peer memory cache | Immutable segments; index segments cover time ranges |

### Lessons for Strata

1. **Disk should be truth, memory should be cache** (SQLite, RocksDB, LMDB). Strata's current "memory is truth, disk is backup" model is the root cause of unbounded growth.
2. **Immutable segments are the universal unit of storage** (RocksDB SSTs, SQLite pages, LMDB CoW pages, Datomic segments). Strata's vector and BM25 subsystems already use this pattern.
3. **Branches should be metadata, not data copies** (Git). A branch is a pointer/manifest — forking copies only the manifest, not the underlying data.
4. **mmap for immutable data is proven** (LMDB, SQLite mmap mode). Strata already does this for `.svec`, `.shgr`, and `.sidx` files.
5. **No system in production combines branching + embedded queryable segments.** This is Strata's unique position.

### Why Not a Standard LSM or Page Cache

Two approaches were evaluated and rejected:

**Page cache (wrap `FxHashMap` with LRU):** Bounds values but leaves key metadata unbounded. At 100M keys with ~120 bytes per Key, the `FxHashMap<Arc<Key>, PageId>` + `BTreeSet<Arc<Key>>` alone consume ~23GB. Dead end.

**Full LSM engine (rebuild RocksDB in Rust):** Solves the memory problem but ignores Strata's core value proposition — branches, snapshots, and time travel are the product, not an implementation detail. A standard LSM has no branching semantics. Adding branching on top of an LSM creates a "branches as afterthought" architecture where the fundamental unit (SST file) has no awareness of branches. Strata needs branches and segments as co-equal first-class concepts.

---

## 4. Architecture: Branchable Segment Store

### Key Insight

Strata is not a mutable KV database. It is a **branchable, log-structured segment store** — closer to "Git for data segments with database semantics" than to Redis or RocksDB.

The architecture rests on three primitives:
1. **Commit log** — canonical truth, append-only
2. **Segments** — immutable, mmap-able, sealed data structures derived from the log
3. **Branch manifests** — metadata views selecting which segments are visible

### Design Principles

| # | Principle | Implication |
|---|-----------|-------------|
| P1 | **Log is canonical truth** | All mutations append to a durable commit log. No structure is updated in place. |
| P2 | **Segments are immutable** | All indexed data exists in sealed, immutable segments derived from the log. |
| P3 | **Branches are metadata views** | A branch is a manifest selecting which segments are visible. Forking is O(1). |
| P4 | **Memory is a cache, never authority** | Heap usage is bounded by design. Disk structures must be queryable directly. |
| P5 | **Compaction rewrites segments, not state** | GC and optimization produce new segments and update manifests. |
| P6 | **All data models share lifecycle phases and visibility** | KV, ANN, BM25, and graph projections share the same phases (build → seal → adopt → compact → retire) and visibility mechanism (manifest + ArcSwap). Internal algorithms for each phase are type-specific (e.g., KV merge-sort vs. HNSW graph rebuild for compaction). |

### System Model

```
Commit Log (WAL)
     │
     ▼
Segment Builders           ← bounded memtables, active HNSW, active BM25
     │
     ▼
Immutable Segments         ← KVSegment, VectorSegment, TextSegment, GraphSegment
     │
     ▼
Branch Manifests           ← metadata views over segments
     │
     ▼
Query Execution            ← MVCC visibility via snapshot_commit
```

---

## 5. Core Concepts

### 5.1 Commit

A commit represents an atomic transaction. The commit log is the single source of durability.

```rust
struct Commit {
    commit_id: u64,          // monotonic, global
    branch_id: BranchId,
    write_set: Vec<WriteOp>, // puts, deletes, vector inserts, BM25 updates
    timestamp: Timestamp,
}
```

**Durability point:** WAL fsync. Once the commit record is durable, the transaction is committed — even if the process crashes before the memtable is updated. All downstream structures (memtables, segments, manifests) are derived from the log and can be rebuilt on recovery.

**Commit completion semantics:** A commit is logically complete when its WAL record is fsync'd. Memtable insertion and `head_commit` advancement happen after the WAL write but before the commit lock is released. If a crash occurs between WAL fsync and memtable insertion, recovery replays the WAL and applies the committed writes to a fresh memtable. No committed data is lost.

### 5.2 Segment

A segment is an immutable, mmap-able data structure representing a range of commits. Once sealed, a segment is never modified.

**Invariant:** A segment S covers commit range `[c_start, c_end)` and key range K, and is never modified after sealing.

| Segment Type | Purpose | Existing Prior Art |
|-------------|---------|-------------------|
| `KVSegment` | Sorted key/value/version tuples | *New* — replaces `ShardedStore` version chains |
| `VectorSegment` | Embeddings + ANN index | `CompactHnswGraph` + `.shgr` mmap + `.svec` mmap |
| `TextSegment` | BM25 postings | Sealed `.sidx` mmap segments |
| `GraphSegment` | Adjacency structures | *New* — future |

All segment types share the same lifecycle phases and visibility mechanism:

```
build → seal → adopt (into manifest) → compact → retire (ref_count = 0)
```

The phases are universal; the internal algorithms are type-specific:

| Phase | KV | Vector | BM25 |
|-------|-----|--------|------|
| **build** | Skiplist memtable | Active HNSW graph | Active DashMap postings |
| **seal** | Sort + flush to `.sst` | Compact HNSW → `.shgr` | Serialize to `.sidx` |
| **adopt** | Manifest update (shared) | Manifest update (shared) | Manifest update (shared) |
| **compact** | Merge-sort segments | HNSW graph rebuild | Inverted index merge |
| **retire** | ref_count → 0, delete (shared) | ref_count → 0, delete (shared) | ref_count → 0, delete (shared) |

**On-disk format:** Each segment is a single mmap-able file. KVSegments use a block-based format with bloom filters, analogous to SST files — but branch-agnostic, so any branch can reference any segment via its manifest. See [File Layout Specification](storage-file-layout.md) for byte-level formats.

### 5.3 Branch Manifest

The manifest defines a branch's logical database state. It is the only mutable metadata structure (updated atomically on flush, compaction, and merge).

```rust
struct BranchManifest {
    branch_id: BranchId,
    head_commit: u64,
    low_watermark_commit: u64,          // oldest commit retained for time travel
    retention_policy: RetentionPolicy,  // governs low_watermark advancement
    base_segments: Vec<SegmentRef>,     // shared with parent/siblings
    overlay_segments: Vec<SegmentRef>,  // branch-local since fork
    tombstone_ranges: Vec<CommitRange>, // compacted-away commit ranges
}

struct SegmentRef {
    segment_id: u64,
    segment_type: SegmentType,        // KV, Vector, Text, Graph
    commit_range: (u64, u64),         // [start, end)
    key_range: (Key, Key),            // first and last key
    ref_count: Arc<AtomicU32>,        // shared across manifests
    size_bytes: u64,
}
```

**Branch creation is O(1):**

```
fork(branch_a → branch_b):
    manifest_b = clone(manifest_a)
```

No data is copied. The new manifest references the same segments. `ref_count` is incremented on each shared segment.

---

## 6. MVCC Model

Visibility is determined purely by commit ordering within segments. No version chains live in memory.

```
Read(branch, snapshot_commit, key):
    1. Check memtable (hot path)
    2. Check overlay segments newest → oldest
    3. Check base segments newest → oldest
    4. Return first version where commit_id ≤ snapshot_commit
```

**Isolation level: Snapshot Isolation (SI).** Each transaction reads from a consistent snapshot taken at transaction start. Write conflicts are detected via OCC read_set validation under the per-branch commit lock. SI prevents dirty reads, non-repeatable reads, and phantom reads within a transaction. It does not prevent write skew — two concurrent transactions can both read overlapping data and commit non-conflicting writes that together violate an application invariant. If write-skew prevention is needed, applications must use explicit CAS operations on sentinel keys.

**Snapshots are free:** A snapshot is `(branch_id, commit_id)` — no copying, no pinning memory, no registering with a tracker. Time travel queries simply adjust the visibility cutoff.

**Versions live inside segments:** Each key entry in a KVSegment carries its `commit_id`. Multiple versions of the same key may exist across segments (or within a single segment if multiple commits touched the same key before the memtable flushed). The MVCC read picks the newest version ≤ the snapshot commit.

---

## 7. Key Encoding

Branch visibility is handled by manifest selection, not by embedding `branch_id` into the key. This keeps segments shareable across branches — forking never rewrites data.

The internal key encoding used in memtables and KV segments:

```
InternalKey =
    TypedKeyBytes   (variable)  |
    CommitId        (8 bytes, descending via !commit_id)

TypedKeyBytes =
    varint(len(namespace)) | namespace |
    u32_be(type_tag) |
    varint(len(user_key)) | user_key
```

CommitId is stored as `u64_be(!commit_id)` so that larger commit IDs sort first within a key — a forward iterator sees the newest version first.

This encoding enables:
- **`list_by_type(branch, type_tag)`** → prefix scan on `(namespace, type_tag)` across segments selected by the branch manifest
- **MVCC reads** → seek to `(key, +∞)`, return first entry with `commit_id ≤ snapshot_commit`
- **Segment sharing** → segments contain no branch identity, so any branch can reference any segment via its manifest
- **Merge** → deterministic ordering for merge-sort across segments

See [`storage-file-layout.md` §8](storage-file-layout.md#8-typedkey-encoding-critical-for-scans-and-list_by_type) and [`memtable-flush-file.md` §2.2](memtable-flush-file.md#22-key-ordering-contract) for the concrete byte-level encoding.

---

## 8. Write Path

```
put(branch, key, value)
    │
    ▼
┌──────────────────┐
│ Step 1: WAL      │   append(commit_record) → fsync
│ (durability)     │   This is the ACID commit point.
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Step 2: Memtable │   Bounded in-memory buffer (default 128 MB).
│ (hot writes)     │   Concurrent skiplist ordered by (TypedKey, commit_id DESC).
│                  │   Lock-free reads, prefix scans are natural.
└────────┬─────────┘
         │
         ▼
    Memtable full?
    ┌─ Yes ───────────────────────────────────┐
    │  Freeze memtable → immutable            │
    │  Create new active memtable             │
    │  Background: sort → encode → seal       │
    │    → new KVSegment (immutable, mmap-able)│
    │  Update branch manifest                 │
    └─────────────────────────────────────────┘
```

The write path mirrors what vector and BM25 already do:
- **Vector:** inserts accumulate in active `HnswGraph` → sealed `CompactHnswGraph` → `.shgr` mmap
- **BM25:** inserts accumulate in active `DashMap` segment → sealed `.sidx` mmap
- **KV (new):** inserts accumulate in memtable → sealed `KVSegment` → mmap file

All three follow the same lifecycle phases (**build → seal → adopt → compact → retire**) and share the same manifest-based visibility mechanism, while using type-specific internal algorithms at each phase.

See [Read/Write Algorithms](read-write-algorithms.md) for the full ACID commit algorithm and [Memtable and Flush Pipeline](memtable-flush-file.md) for the flush-to-segment pipeline.

---

## 9. Read Path

### Hot Reads — Memtable Hit

```
O(log M) skiplist seek → sub-microsecond for working-set keys
```

The memtable serves the working set. At 128 MB with ~1 KB average entries, M ≈ 128K entries, so log₂(128K) ≈ 17 comparisons — sub-microsecond on modern CPUs. This is a small constant-factor slower than Redis's O(1) hash lookup (~3-5x), but avoids the memory overhead of maintaining a separate hash index alongside the ordered skiplist.

For workloads where the working set fits in the memtable + block cache, Strata achieves sub-microsecond p50 latency because no disk I/O occurs.

### Warm Reads — Block Cache Hit

```
Sharded cache lookup → low microseconds
```

Recently-accessed segment blocks are cached. The block cache is bounded (default 2 GB) and uses sharded LRU eviction. A cache hit requires a per-shard mutex acquire — not lock-free, but contention is low because shards are independent.

### Cold Reads — Segment Scan

```
Bloom filter check → binary search within segment block → OS page fault
```

Cold reads touch disk, but bloom filters (10 bits/key, ~1% false positive rate) skip segments that cannot contain the key. Within a segment, binary search on the block index locates the right block.

See [Read/Write Algorithms §4](read-write-algorithms.md) for the full point lookup and prefix scan algorithms with pseudocode.

### Latency Comparison With Redis

Redis achieves sub-microsecond p50 via: no disk access + O(1) hash lookup.

Strata achieves comparable (not identical) hot-path latency for memtable-resident keys:
- **Memtable hit**: O(log M) skiplist seek — ~3-5x more comparisons than Redis O(1), but still sub-microsecond
- **Block cache hit**: per-shard mutex + memcpy — low microseconds, comparable to Redis for cached data
- **No compaction stalls on reads**: compaction is background-only
- **mmap for zero-copy segment access**: hot data stays memory-resident via OS paging

The gap widens for cold reads (segment scan + possible page fault). Strata's advantage is bounded memory — Redis must hold all data in RAM, while Strata serves 100M+ records from a fixed memory budget.

---

## 10. Memory Model

Memory is bounded by construction. The heap budget is configurable (default 8 GB). No component grows unbounded with dataset size — all hot-path structures are either fixed-size or cached with eviction.

```
Component              │ Default Bound      │ Configurable           │ Scales With
───────────────────────┼────────────────────┼────────────────────────┼──────────────
Active memtable        │ 128 MB             │ write_buffer_size      │ Fixed
Immutable memtables    │ 1-2 × memtable     │ max_write_buffer_number│ Fixed
Block cache (LRU)      │ 2 GB               │ block_cache_size       │ Fixed
Bloom filters*         │ ~120 MB at 100M    │ bloom_bits_per_key     │ Keys†
Manifests              │ ~1 MB at 1K segs   │ —                      │ Segments
Segments               │ Disk (mmap)        │ OS-managed             │ Dataset
mmap RSS               │ OS-managed‡        │ —                      │ Access pattern
───────────────────────┼────────────────────┼────────────────────────┼──────────────
Total heap at 100M keys│ ~2.5 GB            │                        │
Total heap at 1M keys  │ ~145 MB            │                        │
Default budget         │ 8 GB               │ memory_limit           │

*  Bloom filters are cached in the block cache and subject to LRU eviction.
   At 100M keys × 10 bits/key, the full bloom set is ~120 MB. With a 2 GB
   block cache, this leaves ~1.9 GB for index and data blocks.
†  Bloom filters are the only component that scales with key count, not
   dataset size. They are evictable — cold segment blooms are paged out
   under memory pressure.
‡  mmap pages don't count toward heap budget but DO count toward
   container memory limits and OS RSS. Tracked for observability.
```

Compare to current architecture: 100M keys = ~23GB of heap index alone.

### Memory Pressure System

```
┌─────────────────────────────────────────────────────────┐
│ Normal    │ 0% ─────── 70% of budget                     │
│           │ No action. All subsystems operate normally.   │
├───────────┼─────────────────────────────────────────────┤
│ Warning   │ 70% ─────── 90% of budget                     │
│           │ Background: flush memtables, evict cold       │
│           │ blocks, seal BM25/vector segments.            │
├───────────┼─────────────────────────────────────────────┤
│ Critical  │ 90% ─────── 100% of budget                    │
│           │ Aggressive: force-flush all immutable          │
│           │ memtables, shrink block cache, throttle        │
│           │ bulk ingestion. Writes still succeed.          │
└───────────┴─────────────────────────────────────────────┘
```

Eviction priority:
1. **KV immutable memtables** → flush to KVSegment (most impactful)
2. **Block cache** → shrink LRU (immediately frees memory)
3. **Vector overlay** → flush to mmap (`flush_overlay_to_disk()`)
4. **HNSW overlay** → seal to `.shgr` mmap
5. **BM25 active segment** → seal to `.sidx` mmap

---

## 11. Compaction Model

Compaction is segment rewrite, never mutation. Old segments are never modified — new segments are produced and manifests are updated atomically.

### Triggers

- Segment count exceeds threshold (e.g., >10 overlay segments)
- Space amplification exceeds target
- Background maintenance schedule

### Operation

```
merge(S1, S2, … Sn) → S'
    1. Merge-sort entries from input segments
    2. Compute prune_floor = min(low_watermark_commit) across all branches referencing {S1..Sn}
    3. Drop versions with commit_id < prune_floor (respecting retention policy)
    4. Drop tombstoned keys where all versions are below prune_floor
    5. Produce new sealed segment S'
    6. Atomically update manifests: replace {S1..Sn} with S'
    7. Advance low_watermark_commit on affected branches (see §13)
    8. Decrement ref_count on {S1..Sn}
    9. Delete segments where ref_count = 0
```

### Branch Safety

Compaction **never** deletes a segment referenced by any manifest. The `ref_count` on `SegmentRef` guarantees this. A segment is reclaimable only when no manifest references it.

### Budget-Aware Compaction

- **Normal pressure:** Full compaction throughput.
- **Warning pressure:** Prioritize memtable flushes over segment merges.
- **Critical pressure:** Only flush memtables. No background compaction.

---

## 12. Branch Lifecycle and Garbage Collection

### The Problem

Without lifecycle management, branches + immutable segments = infinite disk growth:

```
Branch main:  references segments [1, 2, 3, 4, 5]
Branch dev:   references segments [1, 2, 3, 6]      (forked at segment 3)
Branch stale: references segments [1, 2]             (forked early, never closed)

Compaction wants to merge segments [1, 2] into new segment [7].
But "stale" still references [1, 2].
Compaction is blocked. Disk grows forever.
```

### Solution: Branch Leases + Reference-Counted Segments

**Branch leases:** Every branch has a lease with an activity heartbeat.

```rust
struct BranchLease {
    branch_id: BranchId,
    last_active: AtomicU64,       // updated on any read/write
    lease_duration: Duration,      // default: 24 hours
    status: BranchStatus,         // Active, Archived, Expired
}

enum BranchStatus {
    Active,    // lease current, compaction respects this branch
    Archived,  // explicitly archived, compaction can proceed
    Expired,   // lease expired, eligible for aggressive cleanup
}
```

**Reference-counted segments:** Each segment tracks how many manifests reference it.

**GC rule:** A segment S is reclaimable only when:

```
∀ branches B referencing S:
    S.max_commit < B.min_visible_commit
AND
    S.ref_count = 0
```

This prevents long-lived branches from corrupting visibility — a segment is never deleted while any branch might need it.

### Product-Level Rules

1. **Default lease:** 24 hours, configurable per branch.
2. **Heartbeat:** Any `get()`, `put()`, `scan_prefix()`, or `begin_transaction()` resets the lease.
3. **Expiration warning:** Tracing event emitted when a branch is within 1 hour of expiration.
4. **Explicit archive:** `db.archive_branch(branch_id)` immediately marks a branch as archived.
5. **No silent data loss:** Expired branches are archived, not deleted. Data can be recovered by un-archiving.

---

## 13. Snapshot and Time Travel

Snapshot creation is free:

```rust
let snapshot = Snapshot {
    branch_id: branch,
    commit_id: current_head_commit,
};
```

No copying. No pinning memory. No registering with a snapshot tracker.

Time travel queries simply adjust the visibility cutoff:

```rust
// Read state as of commit 42
let value = db.get_at(branch, key, commit_id: 42);
```

The MVCC read path naturally handles this — it returns the first version with `commit_id ≤ 42` from the segment scan.

### Version Retention and the Retention Horizon

Time travel correctness depends on version retention. Compaction prunes old versions — once pruned, time travel to those commits returns incomplete or missing data.

**Retention horizon:** Each branch has a `low_watermark_commit` that defines the oldest commit guaranteed to be readable via time travel. Compaction may prune versions with `commit_id < low_watermark_commit` for that branch.

```rust
struct RetentionPolicy {
    /// Minimum number of commits to retain. 0 = only latest.
    min_commits: u64,           // default: 1000

    /// Minimum wall-clock duration of history to retain.
    min_duration: Duration,     // default: 24 hours

    /// Maximum versions per key to retain. 0 = unlimited.
    max_versions_per_key: u64,  // default: 0 (unlimited)
}
```

**`low_watermark_commit` advancement:**

1. The retention policy determines the floor: `low_watermark_commit` never advances past the policy's `min_commits` or `min_duration` boundary.
2. After compaction produces a new segment that prunes old versions, the compactor advances `low_watermark_commit` to the oldest surviving commit in the branch's visible segments.
3. `low_watermark_commit` is persisted in the branch manifest (see [File Layout §4.2](storage-file-layout.md)).
4. GC uses `low_watermark_commit` to determine segment reclaimability (see §12).

**Time travel error behavior:**

```rust
/// Returned when a time travel query targets a commit older than the retention horizon.
enum TimeTravelError {
    /// The requested commit has been pruned by compaction.
    CommitPruned {
        requested_commit: u64,
        low_watermark: u64,
    },
}
```

If `requested_commit < low_watermark_commit`, the query returns `CommitPruned` rather than silently returning partial data. This makes retention violations explicit.

---

## 14. Failure Recovery

Recovery replays the WAL to rebuild transient state:

1. **Sealed segments are valid** — immutable files on disk, unaffected by crashes.
2. **Manifests are valid** — atomically updated, crash-safe.
3. **WAL replay rebuilds:**
   - Active memtable (commits after the last flush)
   - Incomplete segment builds (re-derive from WAL entries)
4. **No full re-index required** — only the delta since the last sealed segment.

This is simpler than the current recovery path, which must rebuild `ShardedStore` from the WAL by replaying every operation into heap-allocated data structures.

See [Memtable and Flush Pipeline §7](memtable-flush-file.md) for crash recovery cases (partial flush files, orphaned segments, manifest gaps).

---

## 15. Comparison With Existing Systems

| System | Architecture | vs. Strata |
|--------|-------------|------------|
| **RocksDB** | LSM with levels, no branching | Strata adds branch manifests as first-class concept |
| **Redis** | RAM is truth, disk is backup | Strata inverts: disk is truth, RAM is cache |
| **Datomic** | Append-only log + segments | No embedded locality; requires external storage service |
| **Git** | Content-addressed objects + refs | Branches are cheap pointers, but no query engine |
| **ClickHouse** | Column segments, MergeTree | No MVCC branching; analytical, not transactional |
| **SQLite** | Page cache + WAL | Bounded memory, but no branching or time travel |

**Strata's unique position:** branch manifests + embedded queryable segments + shared lifecycle phases and visibility across KV/vector/text/graph projections.

---

## 16. Configuration

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Memory limit in bytes. 0 = unlimited.
    /// Default: 8 GB. Governs memory pressure thresholds and eviction.
    #[serde(default = "default_memory_limit")]
    pub memory_limit: usize,              // 8_589_934_592 (8 GB)

    /// Memtable size in bytes before flush to segment.
    #[serde(default = "default_write_buffer_size")]
    pub write_buffer_size: usize,         // 134_217_728 (128 MB)

    /// Block cache size in bytes (LRU, holds data/index/bloom blocks).
    #[serde(default = "default_block_cache_size")]
    pub block_cache_size: usize,          // 2_147_483_648 (2 GB)

    /// Bloom filter bits per key (0 = disabled).
    #[serde(default = "default_bloom_bits")]
    pub bloom_bits_per_key: u8,           // 10

    /// Segment data block size in bytes.
    #[serde(default = "default_block_size")]
    pub block_size: usize,               // 65_536 (64 KB)

    /// Default branch lease duration in seconds. 0 = no expiration.
    #[serde(default = "default_branch_lease")]
    pub branch_lease_seconds: u64,        // 86400 (24 hours)

    /// Warning pressure threshold (fraction of memory_limit).
    #[serde(default = "default_warning_threshold")]
    pub warning_threshold: f64,           // 0.7

    /// Critical pressure threshold (fraction of memory_limit).
    #[serde(default = "default_critical_threshold")]
    pub critical_threshold: f64,          // 0.9

    /// Default retention policy for new branches.
    #[serde(default)]
    pub default_retention: RetentionPolicy,
}
```

### Presets

```rust
impl StorageConfig {
    /// Raspberry Pi / low-memory devices (256 MB total)
    pub fn constrained() -> Self {
        Self {
            memory_limit: 256 * 1024 * 1024,
            write_buffer_size: 16 * 1024 * 1024,       // 16 MB memtable
            block_cache_size: 64 * 1024 * 1024,         // 64 MB cache
            bloom_bits_per_key: 10,
            ..Self::default()
        }
    }

    /// Default workload (8 GB total)
    pub fn default() -> Self {
        Self {
            memory_limit: 8 * 1024 * 1024 * 1024,      // 8 GB
            write_buffer_size: 128 * 1024 * 1024,       // 128 MB memtable
            block_cache_size: 2 * 1024 * 1024 * 1024,   // 2 GB cache
            bloom_bits_per_key: 10,
            ..Default::default()
        }
    }

    /// Server workload (32 GB total)
    pub fn server() -> Self {
        Self {
            memory_limit: 32 * 1024 * 1024 * 1024,     // 32 GB
            write_buffer_size: 256 * 1024 * 1024,       // 256 MB memtable
            block_cache_size: 8 * 1024 * 1024 * 1024,   // 8 GB cache
            bloom_bits_per_key: 10,
            ..Default::default()
        }
    }
}
```

---

## 17. Implementation Roadmap

### Phase 1: KV Segments + Manifest Layer

**Goal:** Introduce `KVSegment` and `BranchManifest` as the foundational storage primitives.

**Tasks:**
1. Define `KVSegment` on-disk format (block-based, bloom filters, mmap-able)
2. Implement `SegmentBuilder`: accumulate sorted entries → seal → write to disk
3. Define `BranchManifest` structure with segment references and ref counting
4. Implement memtable with bounded size and flush-to-segment
5. Implement read path: memtable → overlay segments → base segments
6. Implement `Storage` trait for the segment store
7. Key encoding: `[TypedKeyBytes | CommitId(desc)]` — no branch in key, branch via manifest
8. Property tests: Storage trait contract passes for both old and new implementations

**Outcome:** Functional KV segment store with bounded memory. Branching via manifest clone. Not yet compacting.

### Phase 2: Unified Segment Lifecycle

**Goal:** Move existing vector and BM25 segment management under the same manifest and lifecycle API.

**Tasks:**
1. Define `SegmentType` enum and unified `Segment` trait (covering KV, Vector, Text, Graph)
2. Adapt `CompactHnswGraph` sealing to produce `VectorSegment` entries in the manifest
3. Adapt BM25 `.sidx` sealing to produce `TextSegment` entries in the manifest
4. Unify segment ref counting across all types
5. Branch fork now clones the full manifest (all segment types)
6. Add `MemoryTracker` with per-subsystem atomic counters

**Outcome:** All data types (KV, vector, BM25) share the same manifest, lifecycle, and ref counting. Branching is truly O(1) across all projections.

### Phase 3: Branch-Aware Compaction + GC

**Goal:** Background compaction and branch lifecycle management.

**Tasks:**
1. Implement segment merge: `merge(S1, S2, … Sn) → S'` with MVCC-aware version pruning
2. Branch leases with heartbeat and expiration
3. Reference-counted segment deletion (ref_count = 0 → eligible for removal)
4. Budget-aware compaction throttling (pressure levels)
5. Background compaction thread with rate limiting
6. Branch archive and un-archive operations

**Outcome:** Complete segment store with compaction, branch lifecycle, and GC. Disk doesn't grow unbounded.

### Phase 4: Cache Tuning + Redis-Class Latency

**Goal:** Tune memtable, block cache, and flush heuristics for hot-path performance.

**Tasks:**
1. Optimize memtable skiplist for hot-path point lookups (epoch GC tuning, cache-line alignment)
2. Block cache warming strategies (preload bloom filters, hot segment blocks)
3. Adaptive flush scheduling (flush before memtable is 100% full to avoid stalls)
4. Memory pressure integration across all subsystems
5. Benchmark validation: YCSB workloads, sustained ingestion, branch proliferation
6. Feature flag: `storage_engine = "segments"` (default) vs `"memory"` (legacy)

**Outcome:** Hot KV ops match Redis p50 latency. Memory bounded under sustained ingest. 100M records on constrained hardware.

---

## 18. Success Criteria

The system is complete when:

- **Branching cost is constant** regardless of dataset size
- **Heap remains bounded** under sustained ingest (memtable + block cache + bloom filters only)
- **Hot KV ops match Redis p50 latency** for memtable-resident keys (skiplist seek, sub-microsecond)
- **Snapshot reads require no locks or copies** — just `(branch_id, commit_id)`
- **Dataset size is limited only by disk** — 100M+ records within the default 8 GB memory budget
- **All projections share lifecycle phases and visibility** — KV, vector, BM25, graph segments managed by the same manifest, with type-specific internal algorithms
