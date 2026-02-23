# Strata Tiered Storage

**Status:** Proposal
**Author:** Design Team
**Date:** 2026-02-23

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

### Detailed Findings

**1. KV Version Chains (CRITICAL — unbounded heap)**

`VersionChain` at `sharded.rs:56` stores all versions in a `VecDeque<StoredValue>`, newest-first. `gc()` at line 115 prunes versions older than a `min_version`, always keeping at least one. But GC is never called automatically (see `docs/design/maintenance.md`). Each `StoredValue` (`crates/storage/src/stored_value.rs:18`) wraps a `VersionedValue` (Value + Version + Timestamp) plus TTL and tombstone flag. For a 512-byte chunk value, each version consumes ~700 bytes on the heap.

**2. Vector Embeddings — Three Modes**

`VectorData` (`heap.rs:26`) has three variants:
- **InMemory**: Default. Full `Vec<f32>` on the heap — unbounded.
- **Mmap**: Read-only `.svec` file (`mmap.rs:100`, format: `[SVEC magic][header][id_to_offset][free_slots][embeddings]`). OS-managed paging. Zero anonymous memory.
- **Tiered**: mmap base + heap overlay for new inserts (`heap.rs:34`). `anon_data_bytes()` at line 669 reports only overlay size. `flush_overlay_to_disk()` at line 625 merges overlay into base. Bounded to overlay size (~768 MB default for 500K × 384d).

Existing memory tracking: `VectorHeap::anon_data_bytes()` (line 669), `VectorIndexBackend::memory_usage()` (`backend.rs:144`).

**3. HNSW Graph — Two Representations**

Active `HnswGraph` (`hnsw.rs:147`) uses `BTreeMap<VectorId, HnswNode>` with `Vec<BTreeSet<VectorId>>` per node — high overhead (~48 bytes/neighbor via BTreeSet). `memory_usage()` at line 750 estimates this.

Sealed `CompactHnswGraph` (`hnsw.rs:991`) packs neighbors into a contiguous `Vec<u64>` or mmap-backed `NeighborData::Mmap` (line 928) from a `.shgr` file (`mmap_graph.rs:56`). Compact form is ~8 bytes/neighbor vs ~48 bytes — 6× reduction. `memory_usage()` at line 1337 reports 0 for mmap-backed neighbor data.

**4. BM25 Segments — Active vs Sealed**

`InvertedIndex` (`search/index.rs:235`) has an active `DashMap`-based segment that absorbs writes, plus sealed mmap-backed `.sidx` segments (line 246). Active segment is unbounded until seal threshold is reached. Sealed segments are OS-paged. Score merging across all segments at query time (`score_top_k()`) scales linearly with segment count.

---

## 3. Industry Comparison

| System | Storage Model | Memory Control | Disk Format | Key Insight |
|--------|--------------|---------------|-------------|-------------|
| **SQLite** | Page cache (LRU) | `PRAGMA cache_size` (default 2 MB) | B-tree pages | Disk is truth; memory is bounded cache |
| **Redis** | In-memory primary | `maxmemory` + eviction (LRU/LFU/random) | RDB/AOF backup | Memory-first; eviction is last resort |
| **RocksDB** | LSM tree (memtable → levels) | `write_buffer_size` + block cache | SST files (sorted) | Memtable is bounded write buffer |
| **DuckDB** | Buffer pool + spill-to-disk | `memory_limit` (default 80% RAM) | Column segments | Graceful spill when limit hit |
| **LMDB** | mmap-only (copy-on-write B+tree) | Zero config (OS manages paging) | Single mmap file | No application-level cache at all |

### Deep Dive: SQLite — Page Cache as Bounded LRU

SQLite treats **disk as the source of truth** and memory as a bounded read cache. The page cache (`pcache`) holds recently-accessed B-tree pages with a configurable upper bound:

```sql
PRAGMA cache_size = -2000;  -- 2000 KB (negative = KB, positive = pages)
```

Key design properties:
- **Bounded by construction**: The cache never exceeds `cache_size` pages. When full, the LRU page is evicted before a new page is loaded.
- **No warm-up required**: Reads that miss the cache go directly to disk. Cold start is functional (just slower).
- **Dirty page write-back**: Modified pages are written back during checkpoint, not on eviction. The WAL absorbs writes between checkpoints.
- **Zero-copy reads**: Pages are copied from disk into cache buffers. `mmap` mode (`PRAGMA mmap_size`) can bypass the cache entirely, letting the OS manage paging.

**Lesson for Strata**: SQLite demonstrates that a simple page cache with LRU eviction and a hard size bound is sufficient for an embedded database. The default 2 MB cache serves most workloads — applications that need more can increase it.

### Deep Dive: Redis — Memory Limit with Eviction Policies

Redis is memory-first, like current Strata, but provides explicit memory bounds:

```
maxmemory 256mb
maxmemory-policy allkeys-lfu
```

Eviction policies:
- **noeviction** (default): Return errors when limit is reached. Data integrity guaranteed.
- **allkeys-lru**: Evict least-recently-used keys globally.
- **allkeys-lfu**: Evict least-frequently-used keys. Better for skewed access patterns.
- **volatile-ttl**: Evict keys with shortest remaining TTL first.
- **volatile-lru/lfu**: Only evict keys that have an expiry set.

Redis on Flash (Enterprise) extends this with a tiered model: hot data in memory, warm/cold on NVMe SSDs. Keys and metadata always in memory; values can be on disk.

**Lesson for Strata**: The `maxmemory` + policy model is directly applicable. Strata should default to a bounded memory configuration and provide policy options. The "metadata in memory, values on disk" split from Redis on Flash maps well to Strata's Key/VersionChain structure.

### Deep Dive: LMDB — mmap-Only, Zero Configuration

LMDB uses a single memory-mapped file for everything. There is no application-level cache or buffer pool — the OS page cache is the only cache:

- **Read path**: `mmap` + pointer arithmetic. No copies, no cache management.
- **Write path**: Copy-on-write B+tree pages. Modified pages are written to new locations; old pages become free after all readers finish.
- **Memory control**: None at the application level. The OS evicts pages under memory pressure.

**Lesson for Strata**: For read-heavy, immutable data (sealed HNSW graphs, sealed BM25 segments, frozen vector mmap files), LMDB's approach of "just mmap it and let the OS handle paging" is optimal. Strata already does this for `.svec`, `.shgr`, and `.sidx` files. The gap is in the mutable, growing data structures (KV version chains, active segments).

### Briefer: RocksDB and DuckDB

**RocksDB** bounds in-memory data through the memtable (`write_buffer_size`, default 64 MB). Writes fill the memtable, which flushes to an immutable L0 SST file when full. The block cache (`LRUCache`, default 8 MB) caches frequently-read SST blocks. Compaction merges SST files across levels to reclaim space and improve read locality.

**DuckDB** uses a buffer pool manager with `memory_limit` (default 80% of RAM). When the limit is reached, it spills intermediate results and cached column segments to a temporary directory. Operations degrade gracefully rather than failing.

### Key Lessons for Strata

1. **Disk should be truth, memory should be cache** (SQLite, RocksDB, DuckDB). Strata's current "memory is truth, disk is backup" model is the root cause.
2. **Hard memory bounds with configurable limits** (Redis, DuckDB). No embedded database should grow without bound.
3. **mmap for immutable data is proven** (LMDB, SQLite mmap mode). Strata already has this for vectors/graphs/BM25 — extend to KV.
4. **LRU eviction is the simplest effective policy** (SQLite page cache, RocksDB block cache). Start here before considering LFU or adaptive policies.
5. **Eviction policies should be configurable** (Redis). Different workloads have different access patterns.

---

## 4. Proposed Design

### Key Insight

Evolve from **"in-memory with WAL backup"** to **"disk-primary with memory cache."**

Today, Strata writes to heap-allocated structures and backs up to WAL on disk. After tiered storage, Strata writes through a bounded memory cache to disk-backed storage, with the WAL serving its original role for crash recovery. This inverts the relationship between memory and disk.

### Tier Model

```
┌─────────────────────────────────────────────────────────────────┐
│                       Memory Budget                             │
│                     (configurable limit)                        │
├────────────────┬────────────────────┬───────────────────────────┤
│   Tier 0: HOT  │   Tier 1: WARM     │   Tier 2: COLD            │
│   (bounded)    │   (OS-managed)     │   (disk-only)             │
├────────────────┼────────────────────┼───────────────────────────┤
│ KV page cache  │ mmap vector data   │ WAL segments              │
│ Active HNSW    │ mmap HNSW graphs   │ Archived snapshots        │
│ Active BM25    │ mmap BM25 segments │ Compacted data            │
│ Write buffers  │ (.svec/.shgr/.sidx)│                           │
├────────────────┼────────────────────┼───────────────────────────┤
│ Eviction: LRU  │ Eviction: OS       │ Eviction: compaction      │
│ Accounting: ✓  │ Accounting: 0      │ Accounting: 0             │
└────────────────┴────────────────────┴───────────────────────────┘
```

- **Tier 0 (Hot)**: Bounded heap memory under `MemoryTracker` control. Actively evicted when budget is exceeded.
- **Tier 1 (Warm)**: Memory-mapped files. The OS manages paging transparently. Strata reports these as zero anonymous memory (consistent with existing `anon_data_bytes()` at `heap.rs:669`).
- **Tier 2 (Cold)**: On-disk data accessed only through explicit I/O (WAL replay, snapshot restore). Not cached.

### Feature 1: Memory Accounting

**Goal:** Know exactly how much memory each subsystem uses, before changing any behavior.

A centralized `MemoryTracker` with per-subsystem counters, using atomic operations for lock-free updates:

```rust
pub struct MemoryTracker {
    /// Per-subsystem byte counters
    kv_bytes: AtomicUsize,
    vector_bytes: AtomicUsize,
    hnsw_bytes: AtomicUsize,
    bm25_bytes: AtomicUsize,
    metadata_bytes: AtomicUsize,
    /// Total budget (0 = unlimited, for backward compat)
    budget: AtomicUsize,
}
```

Integration points:
- `ShardedStore::put()` (`sharded.rs:341`): increment `kv_bytes` by `StoredValue` size
- `VectorHeap::upsert()`: increment `vector_bytes` by `dimension × 4`
- `HnswGraph::insert()`: increment `hnsw_bytes` by estimated node size
- `InvertedIndex::index_document()`: increment `bm25_bytes`
- GC / eviction paths: decrement corresponding counters

This is **observability only** in Phase 1 — no eviction behavior. Exposed via a new `Command::MemoryStatus` returning per-subsystem breakdowns.

### Feature 2: KV Page Cache

**Goal:** Replace unbounded `FxHashMap<Key, VersionChain>` with a bounded, evictable page-based store.

Today's `Shard` stores all key-value data in a flat hash map. The page cache introduces a structured storage layer:

```rust
/// Fixed-size page holding multiple KV entries
pub struct KvPage {
    /// Page ID
    id: PageId,
    /// Entries on this page (key → version chain)
    entries: FxHashMap<Key, VersionChain>,
    /// Approximate byte size of entries
    size_bytes: usize,
    /// Access tracking for LRU
    last_accessed: AtomicU64,
    /// Dirty flag (modified since last flush)
    dirty: bool,
}

/// Bounded LRU cache of KV pages
pub struct KvPageCache {
    /// Page ID → cached page
    pages: FxHashMap<PageId, KvPage>,
    /// Key → PageId index (always in memory)
    key_index: FxHashMap<Key, PageId>,
    /// LRU tracking
    lru: LruList<PageId>,
    /// Max pages in memory
    capacity: usize,
    /// Backing store for evicted pages
    disk: PageStore,
}
```

The key index remains in memory (similar to Redis on Flash keeping keys in RAM), but **values can be evicted to disk**. On cache miss, the page is loaded from disk. Dirty pages are written back on eviction or checkpoint.

Page size target: **64 KB** (matches SQLite's recommended `page_size` for SSDs, and aligns with OS page granularity for efficient mmap).

### Feature 3: Vector Tiering by Default

**Goal:** Make the existing `VectorData::Tiered` mode the default instead of `InMemory`.

The infrastructure already exists (`heap.rs:34`): Tiered mode uses an mmap base with a heap overlay. `promote_to_tiered()` at line 681 converts an Mmap heap. `flush_overlay_to_disk()` at line 625 merges the overlay back.

Today: `VectorData::InMemory` is the default, keeping all embeddings on the heap.
After: `VectorData::Tiered` is the default for databases opened from disk. New collections start InMemory and promote to Tiered on first `freeze_to_disk()`. The overlay cap ensures bounded anonymous memory.

This is a **configuration change**, not a new mechanism — the Tiered code path is already tested and production-ready.

### Feature 4: HNSW Graph Disk Persistence

**Goal:** Automatically persist sealed HNSW segments to `.shgr` files and load them as mmap-backed `CompactHnswGraph` with `NeighborData::Mmap`.

The file format exists (`mmap_graph.rs`, magic `SHGR`, 48-byte header + node entries + neighbor data). `CompactHnswGraph` at `hnsw.rs:991` already supports `NeighborData::Mmap` (line 928) for zero-copy neighbor access.

Today: Sealed segments build a `CompactHnswGraph` with `NeighborData::Owned(Vec<u64>)` in memory.
After: On seal, the compact graph is written to a `.shgr` file and reopened as mmap-backed. `memory_usage()` at line 1337 already reports 0 for mmap neighbor data, so this change is transparent to accounting.

### Feature 5: Eviction Policy

**Goal:** When memory exceeds the budget, coordinate eviction across subsystems.

```rust
pub enum EvictionPolicy {
    /// Evict least-recently-used pages/entries first
    Lru,
    /// Evict entries with shortest remaining TTL first
    VolatileTtl,
    /// Return errors instead of evicting (Redis noeviction equivalent)
    NoEviction,
}
```

Eviction priority order:
1. **KV pages** — evict LRU pages to disk (most data, easiest to page back)
2. **HNSW overlay** — flush overlay to mmap base (`flush_overlay_to_disk`)
3. **BM25 active segment** — force-seal to mmap (existing `seal_active()` + `freeze_to_disk()`)
4. **Vector overlay** — flush vector overlay to mmap base

The coordinator runs when `MemoryTracker::total()` exceeds `budget × warning_threshold` (default 80%). It evicts from the largest subsystem first until below `budget × target_threshold` (default 70%).

### Feature 6: Memory Pressure System

**Goal:** Provide a system-wide pressure signal that subsystems can react to.

```
┌────────────────────────────────────────────────────┐
│ Normal    │ 0% ─────── 70% of budget               │
│           │ No action. All subsystems operate       │
│           │ normally.                               │
├───────────┼────────────────────────────────────────┤
│ Warning   │ 70% ─────── 90% of budget               │
│           │ Background eviction starts. New         │
│           │ allocations proceed but eviction runs   │
│           │ concurrently.                           │
├───────────┼────────────────────────────────────────┤
│ Critical  │ 90% ─────── 100% of budget              │
│           │ Aggressive eviction. Bulk operations    │
│           │ (ingestion, rebuild) are throttled.     │
│           │ Writes still succeed.                   │
└───────────┴────────────────────────────────────────┘
```

Pressure levels are checked on allocation and periodically by the maintenance coordinator. Transitions emit tracing events:

```rust
pub enum MemoryPressure {
    Normal,
    Warning,   // > warning_threshold (default 70%)
    Critical,  // > critical_threshold (default 90%)
}
```

---

## 5. Architecture

### Key Structs

```rust
/// Global memory budget and tracking
pub struct MemoryBudget {
    /// Total memory limit in bytes (0 = unlimited)
    limit: usize,
    /// Per-subsystem trackers
    tracker: MemoryTracker,
    /// Current pressure level
    pressure: AtomicU8,  // MemoryPressure as u8
}

/// Memory tracker with per-subsystem counters
pub struct MemoryTracker {
    kv_bytes: AtomicUsize,
    vector_bytes: AtomicUsize,
    hnsw_bytes: AtomicUsize,
    bm25_bytes: AtomicUsize,
    metadata_bytes: AtomicUsize,
}

/// Fixed-size page for KV storage
pub struct KvPage {
    id: PageId,
    entries: FxHashMap<Key, VersionChain>,
    size_bytes: usize,
    last_accessed: AtomicU64,
    dirty: bool,
}

/// Bounded page cache with LRU eviction
pub struct KvPageCache {
    pages: FxHashMap<PageId, KvPage>,
    key_index: FxHashMap<Key, PageId>,
    lru: LruList<PageId>,
    capacity: usize,
    disk: PageStore,
}
```

### Data Flow: Write Path

```
put(key, value)
    │
    ▼
┌──────────────┐    ┌───────────────┐
│ WAL append   │───▶│ Durability    │  (unchanged)
└──────┬───────┘    └───────────────┘
       │
       ▼
┌──────────────┐
│ MemoryTracker│──── track(kv_bytes, +size)
│   .allocate()│
└──────┬───────┘
       │
       ▼
┌──────────────┐    ┌───────────────┐
│ KvPageCache  │───▶│ Find/create   │
│   .put()     │    │ target page   │
└──────┬───────┘    └───────┬───────┘
       │                    │
       ▼                    ▼
  Page in cache?       Page on disk?
  ┌─Yes: update ┐     ┌─Yes: load + update─┐
  └─────────────┘     └────────────────────┘
       │
       ▼
  Mark page dirty
```

### Data Flow: Read Path

```
get(key)
    │
    ▼
┌──────────────┐
│ key_index    │──── key → PageId
│   .lookup()  │
└──────┬───────┘
       │
       ▼
  Page in cache?
  ┌─Yes─────────────────────────────────┐
  │  Update LRU position                │
  │  Return entry from page             │
  └─────────────────────────────────────┘
  ┌─No──────────────────────────────────┐
  │  Load page from disk (PageStore)    │
  │  Insert into cache                  │
  │  If cache full: evict LRU page      │
  │    ─ If dirty: write back to disk   │
  │  Return entry from loaded page      │
  └─────────────────────────────────────┘
```

### Data Flow: Eviction Path

```
MemoryTracker.total() > budget × warning_threshold
    │
    ▼
┌──────────────────┐
│ Eviction         │
│ Coordinator      │
└──────┬───────────┘
       │
       ▼
  Find largest subsystem
       │
       ├── KV: evict LRU pages from KvPageCache
       │   └── dirty pages: write back → PageStore
       │
       ├── Vector: flush_overlay_to_disk()
       │   └── overlay → mmap base, reset overlay
       │
       ├── HNSW: persist active graph → .shgr
       │   └── reopen as mmap-backed CompactHnswGraph
       │
       └── BM25: seal_active() + freeze_to_disk()
           └── DashMap → mmap .sidx segment
       │
       ▼
  Update MemoryTracker counters
  Re-evaluate pressure level
  If still > target_threshold: repeat
```

### Memory Accounting Flow

```rust
impl MemoryTracker {
    /// Record an allocation. Returns the new total.
    pub fn allocate(&self, subsystem: Subsystem, bytes: usize) -> usize {
        let counter = self.counter_for(subsystem);
        counter.fetch_add(bytes, Ordering::Relaxed);
        self.total()
    }

    /// Record a deallocation.
    pub fn deallocate(&self, subsystem: Subsystem, bytes: usize) {
        let counter = self.counter_for(subsystem);
        counter.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Total tracked bytes across all subsystems.
    pub fn total(&self) -> usize {
        self.kv_bytes.load(Ordering::Relaxed)
            + self.vector_bytes.load(Ordering::Relaxed)
            + self.hnsw_bytes.load(Ordering::Relaxed)
            + self.bm25_bytes.load(Ordering::Relaxed)
            + self.metadata_bytes.load(Ordering::Relaxed)
    }
}

impl MemoryBudget {
    /// Check pressure level after an allocation.
    pub fn check_pressure(&self) -> MemoryPressure {
        if self.limit == 0 {
            return MemoryPressure::Normal; // unlimited
        }
        let usage = self.tracker.total();
        let ratio = usage as f64 / self.limit as f64;
        if ratio > 0.9 {
            MemoryPressure::Critical
        } else if ratio > 0.7 {
            MemoryPressure::Warning
        } else {
            MemoryPressure::Normal
        }
    }
}
```

---

## 6. Configuration

### StorageConfig

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Memory limit in bytes. 0 = unlimited (backward compatible default).
    /// Recommended: 256 MB for light use, 1 GB for moderate, 4 GB for heavy.
    #[serde(default)]
    pub memory_limit: usize,

    /// Eviction policy when memory limit is reached.
    #[serde(default = "default_eviction_policy")]
    pub eviction_policy: String,  // "lru", "volatile_ttl", "noeviction"

    /// Automatically use Tiered mode for vector collections on disk.
    #[serde(default = "default_true")]
    pub vector_auto_tier: bool,

    /// Auto-persist sealed HNSW graphs to .shgr files.
    #[serde(default = "default_true")]
    pub hnsw_auto_persist: bool,

    /// Warning pressure threshold (fraction of memory_limit).
    #[serde(default = "default_warning_threshold")]
    pub warning_threshold: f64,   // 0.7

    /// Critical pressure threshold (fraction of memory_limit).
    #[serde(default = "default_critical_threshold")]
    pub critical_threshold: f64,  // 0.9

    /// KV page size in bytes.
    #[serde(default = "default_page_size")]
    pub page_size: usize,         // 65536 (64 KB)
}
```

### strata.toml Integration

Nested under `[storage]` in the existing `strata.toml`, alongside the existing top-level settings (`durability`, `auto_embed`, etc.):

```toml
durability = "standard"
auto_embed = false

[storage]
# Memory limit in bytes (0 = unlimited, backward compatible).
# Set to bound memory usage for embedded deployments.
memory_limit = 0

# Eviction policy: "lru" (default), "volatile_ttl", "noeviction"
eviction_policy = "lru"

# Automatically use Tiered (mmap + overlay) mode for vector data.
vector_auto_tier = true

# Auto-persist sealed HNSW graphs to .shgr mmap files.
hnsw_auto_persist = true

# Memory pressure thresholds (fraction of memory_limit).
warning_threshold = 0.7
critical_threshold = 0.9

# KV page size in bytes (64 KB default, aligned with OS page size).
page_size = 65536
```

### Default Justification

| Setting | Default | Rationale |
|---------|---------|-----------|
| `memory_limit` | `0` (unlimited) | Backward compatible. Existing users see no behavior change. |
| `eviction_policy` | `"lru"` | Simplest effective policy (SQLite page cache, RocksDB block cache). |
| `vector_auto_tier` | `true` | Tiered mode is already tested; saves memory with no API change. |
| `hnsw_auto_persist` | `true` | `.shgr` format exists; auto-persist prevents graph loss on crash. |
| `warning_threshold` | `0.7` | Start background eviction at 70% — leaves headroom for bursts. |
| `critical_threshold` | `0.9` | Aggressive eviction at 90% — prevents OOM with 10% buffer. |
| `page_size` | `65536` | 64 KB matches SQLite recommended SSD page size. Good I/O granularity. |

### StrataConfig Integration

Add `StorageConfig` as an optional field in `StrataConfig` (`crates/engine/src/database/config.rs:66`), following the same pattern as the proposed `MaintenanceConfig`:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrataConfig {
    #[serde(default = "default_durability_str")]
    pub durability: String,
    #[serde(default)]
    pub auto_embed: bool,
    // ... existing fields ...

    /// Storage tier configuration. Defaults to unlimited memory (backward compat).
    #[serde(default)]
    pub storage: StorageConfig,
}
```

### Open-Time Override

```rust
let config = StrataConfig {
    storage: StorageConfig {
        memory_limit: 512 * 1024 * 1024,  // 512 MB
        eviction_policy: "lru".to_string(),
        ..StorageConfig::default()
    },
    ..StrataConfig::default()
};
let db = Database::open_with_config("/path/to/data", config)?;
```

---

## 7. Implementation Roadmap

### Phase 1: Memory Accounting + Budget (Observability)

**Goal:** Know how much memory each subsystem uses. No behavior change.

**Tasks:**
1. Add `MemoryTracker` struct with atomic per-subsystem counters
2. Add `MemoryBudget` struct (limit + tracker + pressure level)
3. Instrument `ShardedStore::put()` / `gc()` to track KV bytes
4. Instrument `VectorHeap::upsert()` / `delete()` to track vector bytes
5. Instrument `HnswGraph::insert()` / `compact()` to track HNSW bytes
6. Instrument `InvertedIndex::index_document()` to track BM25 bytes
7. Add `StorageConfig` to `StrataConfig` with serde defaults
8. Add `Command::MemoryStatus` returning per-subsystem breakdown
9. Wire `MemoryBudget` into `Database`, passed through to subsystems
10. Tests: verify counters match actual `memory_usage()` reports

**Outcome:** Operators can see exactly where memory is going. Dashboards and alerts can be built. No user-facing behavior change.

### Phase 2: KV Page Cache with Eviction

**Goal:** Replace unbounded `FxHashMap<Key, VersionChain>` with a bounded page cache.

**Tasks:**
1. Design `KvPage` and `PageStore` (on-disk page format)
2. Implement `KvPageCache` with LRU eviction
3. Implement `PageStore` with page read/write (simple file-backed)
4. Replace `Shard.data` with `KvPageCache` when `memory_limit > 0`
5. Maintain `key_index` in memory (key → PageId mapping)
6. Maintain `ordered_keys` BTreeSet in memory (for prefix scans)
7. Implement dirty page write-back on eviction and checkpoint
8. Implement eviction coordinator with pressure-based triggers
9. Tests: verify eviction respects LRU order, dirty write-back, cache miss loads from disk
10. Benchmarks: measure read latency for cache hit vs cache miss

**Outcome:** KV memory is bounded. Workloads that exceed the budget gracefully page to disk.

### Phase 3: Vector Tier-by-Default + HNSW Disk Persistence

**Goal:** Reduce vector and graph memory by defaulting to mmap-backed storage.

**Tasks:**
1. Change vector collection default from `InMemory` to `Tiered` for disk-backed databases
2. Auto-call `promote_to_tiered()` after initial `freeze_to_disk()` on recovery
3. Implement auto-persist for sealed HNSW segments: write `.shgr` after `CompactHnswGraph::from_graph()`
4. Reopen persisted graphs with `NeighborData::Mmap` instead of `Owned`
5. Add overlay flush to eviction coordinator (flush when pressure > Warning)
6. Tests: verify Tiered default works for new and recovered databases
7. Tests: verify HNSW graphs survive restart via `.shgr` mmap
8. Benchmarks: measure search latency with mmap vs in-memory backends

**Outcome:** Vector memory drops to overlay-only. HNSW sealed graphs consume zero anonymous memory. Typical 200K-chunk workload drops from ~547 MB to ~50 MB (overlay + active structures).

### Phase 4: Unified Memory Pressure System

**Goal:** Coordinate eviction across all subsystems under a single pressure signal.

**Tasks:**
1. Implement `MemoryPressure` enum and pressure-checking logic in `MemoryBudget`
2. Add pressure-aware throttling to bulk operations (ingestion, index rebuild)
3. Implement cross-subsystem eviction coordinator (evict largest subsystem first)
4. Add tracing events for pressure transitions (Normal → Warning → Critical)
5. Integrate with `MaintenanceCoordinator` (from `docs/design/maintenance.md`) — eviction runs alongside GC/compaction
6. Add `Command::StorageStatus` combining memory status + pressure level + eviction stats
7. Tests: verify pressure transitions, throttling behavior, cross-subsystem eviction
8. Stress tests: sustained ingestion under memory limit to verify no OOM
