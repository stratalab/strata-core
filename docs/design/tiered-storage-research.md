# Tiered Storage: Industry Research

**Status:** Reference Document
**Date:** 2026-03-01
**Related:** `docs/design/tiered-storage.md` (design proposal)

This document captures detailed implementation analysis of how production databases solve the hot/cold tiering problem. It serves as a reference for design decisions in Strata's tiered storage implementation (issue #1291).

---

## Table of Contents

1. [Redis](#1-redis)
2. [DragonflyDB](#2-dragonflydb)
3. [RocksDB](#3-rocksdb)
4. [redb](#4-redb)
5. [sled](#5-sled)
6. [LMDB](#6-lmdb)
7. [SQLite](#7-sqlite)
8. [WiredTiger (MongoDB)](#8-wiredtiger-mongodb)
9. [TigerBeetle](#9-tigerbeetle)
10. [Applicability to Strata](#10-applicability-to-strata)

---

## 1. Redis

### 1.1 Enterprise Auto Tiering (formerly Redis on Flash)

Redis Enterprise treats flash as a **transparent RAM extension**, not a traditional disk tier. The application (Redis) decides what lives where, rather than the OS page cache.

**Architecture of a single shard:**

```
Redis Enterprise Flash Shard
├── RAM
│   ├── All keys (always)
│   ├── All key metadata / hash table entries (always)
│   ├── Hot values (frequently accessed)
│   └── Hash table pointers → flash-resident values
└── Flash (NVMe/SATA SSD) via RocksDB/Speedb
    └── Warm/cold values (infrequently accessed)
```

**Key design decisions:**

- RAM-to-flash ratio is configurable (e.g., 20:80 is common). RAM limit cannot be smaller than 10% of total memory.
- A **background evictor task** continuously monitors RAM usage and proactively ejects less-frequently-used values to flash. This runs in the background, not triggered on-demand per request.
- **All keys and metadata remain in RAM at all times.** Only values can be tiered to flash. This is a fundamental architectural constraint — keys are small, operations like `EXISTS`/`TTL`/`SCAN` must be fast, and the hash table structure requires it.

**Flash storage engine:** RocksDB (or its successor Speedb). Key details:
- No WAL/redo log needed — Redis handles durability separately via RDB/AOF/replication.
- Updates to hot keys in RAM generate zero flash I/O. Flash writes only happen during ejection.
- Speedb replaces RocksDB's standard compaction with "Multi-Dimensional Adaptive Compaction" — reduces write amplification by ~80% (WAF from ~30 down to ~5).

**Recovery:** Both RAM and flash must be completely empty at startup. The shard loads from durable copies (RDB/AOF on network-attached storage) or from a replica.

### 1.2 Open-Source Eviction: Sampled LRU/LFU

Redis chose an **approximated LRU** that uses only 24 bits per object and requires no linked list, avoiding the 16+ bytes of pointer overhead per key that true LRU demands.

**The `redisObject` LRU field:** Every Redis object has a 24-bit `lru` field. Under LRU policy, this stores the least-significant 24 bits of the Unix timestamp in seconds (wraps every ~194 days). On each key access, the object's `lru` field is updated to the current clock value.

**Approximated LRU algorithm:**

1. When eviction is needed, randomly sample `maxmemory-samples` keys (default: 5).
2. For each sampled key, compute its idle time (`current_clock - key.lru`).
3. Insert into a **sorted eviction pool** of 16 candidates (the `evictionPoolEntry` array, `EVPOOL_SIZE = 16`, hardcoded in `evict.c`).
4. Evict the candidate with the highest idle time from the pool.
5. If not enough memory freed, repeat.

The eviction pool was the key improvement in Redis 3.0 — instead of just picking the worst of 5 random samples, Redis accumulates candidates across sampling rounds. Performance testing showed:
- **5 samples**: already quite good, much better than random eviction.
- **10 samples**: nearly indistinguishable from true LRU.
- **Beyond 10**: diminishing returns.

**LFU Implementation (Redis 4.0+):**

LFU reuses the same 24-bit `lru` field but splits it differently:

```
[  16 bits: last decay time  |  8 bits: logarithmic counter  ]
     (minutes since epoch)         (Morris counter, 0-255)
```

The Morris counter is a logarithmic probabilistic counter:
```c
double p = 1.0 / (baseval * server.lfu_log_factor + 1);
if (random() < p) counter++;
```

The higher the current count, the lower the probability of incrementing. With default `lfu-log-factor = 10`:

| Factor | 100 hits | 1K hits | 100K hits | 1M hits |
|--------|----------|---------|-----------|---------|
| 0      | 104      | 255     | 255       | 255     |
| 1      | 18       | 49      | 255       | 255     |
| 10     | 10       | 18      | 142       | 255     |
| 100    | 8        | 11      | 49        | 143     |

**Decay mechanism:** The counter is decremented by 1 for each `lfu-decay-time` minutes elapsed since the last access (default 1 minute). This allows adaptation to shifting access patterns.

### 1.3 Performance Numbers

From Redis Enterprise benchmarks (6.2.8-39, 1KiB values, 20:80 RAM:flash ratio):
- With Speedb on I4i.8xlarge: **3.7x** throughput improvement over baseline RocksDB on i3.8xlarge.
- 85% RAM hit ratio: near-pure-RAM latency.
- Cost: ~30% of equivalent DRAM-only deployment.

### 1.4 Persistence with Tiered Data

Flash storage is **not persistent** — it is purely an ephemeral capacity extension. If a node crashes, flash data is gone. Persistence comes from RDB/AOF to network-attached durable storage. RDB snapshots must read cold values back from flash, generating flash read I/O.

---

## 2. DragonflyDB

### 2.1 Dash Table Internals

The Dash table is DragonflyDB's replacement for Redis's `dict`. It is a dynamic array of fixed-size **segments** (not a single large chained hash table).

**Segment structure:**
- 56 regular buckets + 4 stash buckets = 60 total buckets per segment
- 14 slots per bucket → 840 records per segment
- Open-addressing with probing (no linked-list chaining)

**Growth:** When a segment is full, it **splits** — contents are redistributed between old and new segments. Only two segments are affected per split (vs Redis's "big-bang" rehash that doubles the entire table).

**Memory overhead:** ~20 bits of metadata per entry (vs Redis's 64-bit `dictEntry.next` pointer). Per-entry cost at full utilization: ~19 bytes total. Each slot has room for version counters (used for forkless snapshots).

**Eviction integration:** When a segment is full, that is the natural eviction point. Dragonfly evicts items from full segments before they split — **zero additional bookkeeping**, no separate LRU list, no per-entry timestamp.

### 2.2 SSD Tiering Architecture

**Core principle:** Keys, metadata, and the Dash table always in RAM. Only values move to SSD.

**Three-state model:**

| State | Location | Description |
|-------|----------|-------------|
| **Hot** | RAM only | Normal in-memory entry |
| **Cooled** | RAM + Disk | Transitional; value exists in both places |
| **Cold** | Disk only | Value offloaded; metadata still in RAM with file offset |

**The cooling process (mark-sweep):**
1. A background fiber iterates entries and **marks** candidates for offloading.
2. If an entry is accessed before the next iteration, the mark is cleared (it stays hot).
3. Only entries that remain unmarked across **consecutive iterations** are demoted to disk.
4. On write: new values go to disk but maintain an in-memory copy (cooled state). The in-memory copy can be evicted at any time without a disk write since the disk copy is authoritative.
5. On read of a cold entry: if free memory exists, the value is **promoted** back to RAM.

**Memory pressure management:** The `--tiered_offload_threshold` flag (e.g., 0.2 = 20% free) triggers aggressive offloading. When usage exceeds `(1 - threshold)`, Dragonfly throttles incoming writes and accelerates offloading.

### 2.3 SmallBins: I/O Efficiency for Small Values

Since direct I/O requires 4KB-aligned writes but values can be 64 bytes, a middle layer called **SmallBins** packs multiple small values into a single 4KB page before issuing I/O. Without this, write amplification for small values would be catastrophic (64 bytes in a 4KB page = 98.4% waste).

### 2.4 Per-Shard Files

Each shard thread manages its own preallocated file — shared-nothing, no cross-thread contention on I/O. Uses `io_uring` with `O_DIRECT` (bypasses Linux page cache). Reads are single-seek: the in-memory metadata stores the exact file offset.

### 2.5 Compaction

Deleting small values from SmallBins creates fragmentation. A background compaction process scans fragmented pages, identifies valid data, and rewrites into densely packed new pages. Keys are stored alongside values on disk so the compactor can trace values back to their in-memory hash entries.

### 2.6 Snapshot Interaction

Dragonfly's forkless snapshots use per-entry **version counters** in the Dash table. Each shard's snapshot fiber captures the current epoch, then iterates serializing entries where `entry.version <= snapshot_epoch`. For tiered data, the snapshot must read back offloaded values from disk.

**Known issue:** Snapshotting with SmallBins causes read amplification >50x because the same 4KB page may be re-read for each small value it contains.

**Critical limitation:** SSD-tiered data is **not persistent across restarts**. Persistence comes from RDB/AOF snapshots.

---

## 3. RocksDB

### 3.1 LSM-Tree Data Flow

```
Write Path:
  WAL append → Active MemTable (SkipList, 64MB default)
    → Immutable MemTable (when full)
      → Flush to L0 (SST files, overlapping key ranges)
        → Compaction to L1 (non-overlapping from here on)
          → L2 → ... → Ln (each level ~10x larger)
```

**Level sizing (dynamic mode, recommended):**
- Works backwards from the last level's actual size.
- `Target(Ln-1) = Target(Ln) / max_bytes_for_level_multiplier`
- Guarantees ~90% of data in the final level.
- Empty intermediate levels are common.

**Compaction scoring:** Each level gets a score = `total_size / target_size`. The highest-scoring level compacts first.

**Trivial moves:** When a file's key range doesn't overlap with any file in the target level, RocksDB performs a metadata-only move — no I/O rewrite.

### 3.2 Block Cache

The primary read cache for uncompressed data blocks, index blocks, and filter blocks.

**LRU Cache (default):** Per-shard mutex, up to 64 shards. Both lookups and inserts require locking because reads modify LRU metadata.

**Priority pools:** LRU list splits into high-priority (index/filter blocks) and low-priority (data blocks). When high-priority usage exceeds its ratio, overflow competes with data blocks.

**Secondary Cache (two-tier caching):** A non-volatile tier (flash, NVM) behind the DRAM block cache. On eviction from primary, blocks are serialized to secondary. On miss, secondary returns an async handle. Prototyping with CacheLib showed 15% throughput gain and 25-30% reduction in backend reads.

### 3.3 Compaction Strategies

| Strategy | Write Amp | Read Amp | Space Amp | Best For |
|----------|-----------|----------|-----------|----------|
| Leveled | 10-30x | Low (1 I/O with bloom) | ~10-20% | Read-heavy |
| Universal | 2-5x | Medium-High | Up to 2x during compaction | Write-heavy |
| FIFO | 1x | Very High | Bounded by config | Event logs, caching |
| Leveled + BlobDB | 3-6x | Low | Low | Large values |

### 3.4 BlobDB (Key-Value Separation)

Based on the WiscKey paper: large values are stored in separate blob files; the LSM tree stores only keys + blob pointers.

- Values at or above `min_blob_size` go to blob files (e.g., 1 KB threshold).
- GC is integrated into compaction — old blob files are relocated during compaction.
- Write amplification reduction: **36-90%** vs standard leveled compaction.
- Write throughput: **2.1-4.7x** faster.

### 3.5 Native Tiered Storage

RocksDB natively supports hot/cold tiering:
- `last_level_temperature = Temperature::kCold` assigns cold temperature to bottom-level files.
- `preclude_last_level_data_seconds` defines the hot data time window.
- Uses per-key placement during compaction based on estimated write time.
- Temperature metadata flows through the `FileSystem` API, allowing custom routing to different media.

Works best with universal compaction. Leveled + tiered can cause infinite auto-compaction when most data is hot.

### 3.6 MVCC / Snapshots

Every write gets a monotonically increasing **sequence number**. Snapshots capture the current sequence number. During compaction, the `CompactionIterator` retains all versions visible to any active snapshot and only drops versions/tombstones at the bottommost level when no snapshots reference them.

Long-lived snapshots cause space bloat — compaction must retain all referenced versions.

### 3.7 Memory-Constrained Configuration

**Four major memory consumers:** block cache, index/filter blocks, memtables, iterator-pinned blocks.

**`WriteBufferManager`** (since v5.6) enables a single memory budget across memtables and block cache by inserting dummy entries into the block cache to track memtable allocations.

| Knob | Memory-Constrained Setting |
|------|---------------------------|
| `cache_index_and_filter_blocks` | `true` (critical) |
| `write_buffer_size` | Reduce (16-32 MB) |
| `max_write_buffer_number` | 2 (minimum) |
| `optimize_filters_for_hits` | `true` (skip bloom on last level) |
| `block_size` | Increase to 16-32 KB |
| `strict_capacity_limit` | `true` (reject inserts when over capacity) |

---

## 4. redb

### 4.1 Architecture: Copy-on-Write B-Trees

redb is inspired by LMDB but deliberately avoids mmap. All data is stored in **copy-on-write B-trees**.

**Copy-on-write mechanics:**
1. When a write transaction modifies a leaf page, that page is copied to a new location.
2. The parent (which points to the old leaf) is also copied and updated.
3. This propagates bottom-up to the root — every node on the path is copied.
4. The root pointer is updated atomically via dual header slots at commit time.
5. Old pages are added to a freed list.

Crash recovery is free — if the system crashes before the root update, the old root is still valid. No WAL, no replay.

### 4.2 Page Allocation: Buddy Allocator

Database file is organized into **regions**, each managed by a `BuddyAllocator`. Allocations are power-of-2 multiples of the base page size. A `BtreeBitmap` (64-way tree with bit-packed u64 nodes) enables efficient free space queries.

Three tracking states per page during a transaction: `allocated_since_commit`, `freed_pages`, `unpersisted`.

### 4.3 MVCC and Transactions

**Concurrency model:** Single writer + multiple concurrent readers, without blocking (same as LMDB and SQLite WAL mode).

**Free page reclamation:** Pages freed by a write transaction enter a pending-free queue. They become reclaimable only after all read transactions that could reference them have completed, tracked via `TransactionTracker`.

**Savepoints:** Two variants — ephemeral (in-memory, lost on crash) and persistent (stored in `SAVEPOINT_TABLE`, survive crashes). Creating a persistent savepoint internally registers as a read transaction to preserve the snapshot.

### 4.4 Memory Usage and Caching

redb does **not** use mmap. It has a `PagedCachedFile` layer:
- Default total cache: **1 GiB** (configurable via `Builder::set_cache_size()`)
- Split: **90% read cache** (LRU eviction) / **10% write buffer** (dirty pages pending flush)
- Storage backend abstracted behind `StorageBackend` trait (`FileBackend` for production, `InMemoryBackend` for testing)

**Durability options per transaction:**
- `Durability::Immediate`: `fsync()` — full crash safety
- `Durability::None`: skip sync — atomicity/consistency/isolation preserved, not durable across crashes
- Two commit protocols: 1-phase (faster, slower recovery) and 2-phase (fast recovery)

### 4.5 Why redb Over sled

| Dimension | redb | sled |
|-----------|------|------|
| Stability | 1.0 released, stable on-disk format | Beta, format will change |
| Space amplification | Low (COW B-tree, buddy allocator) | High (log-structured) |
| Bulk load | ~1770-2594ms | ~4534-5337ms |
| Random reads | ~975ms | ~1512ms |
| Batch writes | ~2346-2610ms | ~1395-1815ms |
| Data integrity | Stable | Corruption reports exist |

---

## 5. sled

### 5.1 Architecture: Lock-Free Tree on Lock-Free Pagecache on Lock-Free Log

Three-layer stack inspired by LLAMA and Bw-tree papers from Microsoft Research:

1. **The Log:** Fixed-size segments on disk. Pages written as deltas (incremental updates) appended to the log.
2. **The Pagecache:** Maps logical page IDs to physical locations. 256 cache shards with mutex-protected LRU lists.
3. **The Tree:** RCU on entire nodes via `Arc`. `RwLock<Arc<Node>>` with copy-on-write under low contention, full RCU under high contention.

Uses epoch-based reclamation via `crossbeam-epoch` for zero-copy value access.

### 5.2 Known Issues and Lessons Learned

sled explicitly labels itself "the champagne of **beta** embedded databases" and has never reached 1.0.

**Problems:**
- **Space amplification:** Log-structured design means stale data persists until GC. High overhead.
- **Write amplification:** LLAMA-style deltas scatter fragments across segments. Consolidation rewrites everything.
- **Memory:** `cache_capacity` uses on-disk size rather than in-memory representation size, so actual RAM usage can far exceed configuration.
- **Stability:** Database corruption after process interruptions has been reported. On-disk format is not stable.

**The rewrite (komora/marble):** Tyler Neely is rewriting the storage subsystem to fix space/write amplification and memory layout issues.

**Lesson for Strata:** Theoretically elegant architectures (lock-free everything, incremental deltas) can produce practical problems (high amplification, complex GC, OOM). Simpler designs (redb's COW B-tree) ship faster and more reliably.

---

## 6. LMDB

### 6.1 Copy-on-Write B+ Tree with Shadow Paging

**Dual meta pages:** Pages 0 and 1 are meta pages in a double-buffer arrangement. Each contains a transaction ID and a root pointer. On startup, LMDB uses the one with the higher transaction ID.

**CoW update protocol:**
1. Modified leaf → copy to new location.
2. Parent node → copy and update with new leaf pointer.
3. Propagate bottom-up to root (root always written last).
4. Meta page update (single machine word — transaction ID) flushed with `fdatasync()`.

Crash recovery is free — if the system crashes before the meta page update, the old root is still valid. No WAL, no recovery log, no replay.

**Two B+ trees per root:** One for user data, one for the **free page list** (tracking freed pages for reuse).

### 6.2 mmap-Only Design

LMDB's most distinctive choice: **no application-level page cache**.

- `mdb_env_open()` calls `mmap()` on the entire database file.
- All data fetches return **pointers directly into the mapped memory** — no `malloc`, no `memcpy`.
- The OS page cache is the only cache.

**Why this works for trees:** Traversals always start at the root, so root pages are always hot. Modern Linux kernels use Clock-Pro (not simple LRU), which handles database patterns well.

### 6.3 MVCC via Copy-on-Write

- **Readers** record the current transaction ID. Since CoW never overwrites existing pages, the reader's snapshot remains valid indefinitely.
- **Writers** operate on new page copies. Never interfere with readers.
- **Readers never block writers** and vice versa. Only serialization is the single-writer mutex.
- **Reader tracking:** A shared-memory readers table tracks active read transactions (CPU cache-line aligned slots for lockless access). Writers scan this to determine which freed pages can be reclaimed.

### 6.4 Trade-offs

- **Write amplification:** ~1x + tree_depth. Every modified leaf rewrites the entire path to root. Produces random I/O.
- **Max DB size:** Determined by `mapsize` parameter. On 64-bit systems, can be set very large (mmap only allocates physical memory for accessed pages).
- **Single-writer limitation:** One write transaction at a time, globally serialized by mutex.
- **File growth from long-lived readers:** If a read transaction stays open, freed pages can't be reclaimed → file grows.
- **mmap I/O errors:** Trigger SIGBUS/SIGSEGV (process crash) instead of error codes.

### 6.5 Why Some Projects Moved Away

- Write-heavy workloads: LMDB was designed for OpenLDAP's 80-90% read workload.
- Deep learning (Caffe → PyTorch/TensorFlow): single-writer made parallel ingestion difficult.
- Random writes to large databases: pathological CoW amplification (every write touches scattered pages).
- **libmdbx** (fork) was created to address several limitations.

---

## 7. SQLite

### 7.1 Page Cache (Pager Layer)

SQLite's B-tree module talks to the **pager** (`pager.c`), which owns a **page cache** (`pcache.c`). The cache is pluggable.

**Data structure:** Hash table (`PCache1.apHash`) with unordered singly linked list buckets. Each slot contains:
- `PgHdr` metadata (page number, dirty flag, `needSync`, reference count)
- Raw page image (default 4096 bytes, configurable 512–65536)
- Private space for B-tree per-page state

**Pin/unpin protocol:** Pages use reference counting. When `nRef > 0`, the page is pinned and cannot be evicted.

### 7.2 Eviction

**Approximate LRU**. Default limits:
- 2000 pages (~8MB at 4K page size) for main databases
- 500 pages for temp databases

Strict fetch-on-demand — no speculative reads. When the cache is full, the LRU unpinned page is recycled.

### 7.3 WAL Mode and MVCC

**WAL structure:** 32-byte header + frames (24-byte frame header + full page data).

**WAL Index (.shm file):** Memory-mapped shared index. 32KB blocks, each with 4096 page-number slots and 8192-slot hash map. Hash: `(page_number * 383) % 8192` with linear probing.

**Snapshot isolation:** Each reader records an "end mark" in the WAL at transaction start. Readers only see frames up to their end mark. Writers append new page versions while readers see older versions — this is SQLite's MVCC.

**Checkpointing:** Copies latest version of each page from WAL back into the main database file. Four modes: PASSIVE, FULL, RESTART, TRUNCATE.

### 7.4 Memory-Mapped I/O (`PRAGMA mmap_size`)

When set, SQLite memory-maps up to N bytes of the database file:
1. Page requests call `xFetch()` instead of `xRead()`.
2. If in mapped region, returns a direct pointer (no copy).
3. If beyond range, falls back to `xRead()` (copies data).

**Trade-offs:**
- Can nearly double I/O-intensive operation speed.
- I/O errors on mmap'd regions trigger SIGBUS/SIGSEGV (crash) instead of error codes.
- SQLite always uses read-only mmap — copies to heap before modification.

---

## 8. WiredTiger (MongoDB)

### 8.1 Cache Architecture

WiredTiger uses a **dedicated application-level cache** (`WT_CACHE` structure).

**In-memory page format differs from on-disk format.** In memory:
- Row-store leaf pages use `WT_ROW` arrays + **insert skip lists** + **update chains** (linked lists of `WT_UPDATE` for MVCC)
- Internal pages contain `WT_REF` arrays pointing to children

On disk: compact serialized key-value pairs with page and block headers.

**Memory tracking:** Three categories: total cache usage, clean page bytes, dirty page bytes. Drives eviction decisions.

### 8.2 Eviction Thresholds

| Threshold | Default | Behavior |
|-----------|---------|----------|
| `eviction_target` | 80% | Background workers try to stay below this |
| `eviction_trigger` | 95% | **Application threads** forced to participate → latency spikes |
| `eviction_dirty_target` | 5% | Target for dirty data volume |
| `eviction_dirty_trigger` | 20% | Application threads throttled on dirty data |

The gap between `eviction_target` and `eviction_trigger` is the operating window for background eviction.

### 8.3 Eviction Server and Workers

Three components:
1. **Eviction server** (1 thread): Walks page trees across all B-trees. Identifies candidates, sorts by last access time, selects the **oldest one-third** (approximate LRU).
2. **Eviction worker threads** (0 to `threads_max`, dynamically scaled): Pull pages from eviction queues and perform actual eviction.
3. **Three eviction queues:** Two ordinary + one **urgent** queue for high-priority pages.

**Clean vs. dirty eviction:**
- Clean pages: simply freed (no I/O).
- Dirty pages: must go through **reconciliation** first (convert in-memory format → on-disk format), then written before being freed.

### 8.4 Reconciliation

The process of converting rich in-memory pages to compact on-disk format:
1. Walk the page's insert lists and update chains in key order.
2. Select the latest committed value for the on-disk image.
3. Older versions are written to the **history store** (an internal B-tree).
4. When accumulated data reaches `leaf_page_max * split_pct`, mark a split point.
5. Multi-block output if content exceeds a single page.

**History store:** Internal row-store B-tree holding older MVCC versions. Key: `(btree_id, record_key, start_timestamp, counter)`. Current and historical data can be evicted independently.

### 8.5 Hazard Pointers

WiredTiger uses **hazard pointers** instead of read locks for B-tree traversal:

1. Thread publishes a hazard pointer to the page it's reading (writes address to a per-thread array).
2. Page is guaranteed not to be freed while any hazard pointer references it.
3. Thread clears the hazard pointer when done.

**Eviction interaction:**
1. Eviction server identifies a candidate page.
2. Sets page's `WT_REF` state to `WT_REF_LOCKED`.
3. Scans **all threads' hazard pointer arrays**.
4. If any thread holds a pointer → eviction backs off.
5. If clear → eviction proceeds.

**Cost model:** Very cheap for readers (just a store, no atomic CAS, no memory barrier on most architectures). More expensive for exclusive access (must scan all arrays). Ideal for read-heavy workloads.

**WT_REF states:**
- `WT_REF_MEM`: Page in memory, accessible
- `WT_REF_LOCKED`: Locked for exclusive operation
- `WT_REF_SPLIT`: Page has been split; threads restart traversal
- `WT_REF_DISK`: Page on disk, not in memory

### 8.6 Memory-Constrained Configuration

- `cache_size`: set to working set size. MongoDB default: `max(256MB, 50% of RAM - 1GB)`.
- `eviction_target=80, eviction_trigger=90`: tighter window for more headroom.
- `eviction=(threads_max=4)`: more workers to prevent application thread participation.
- `cache_resident=true`: pin hot tables to prevent eviction (increases pressure on others).

---

## 9. TigerBeetle

### 9.1 LSM Forest (Custom Storage Engine)

TigerBeetle implements an **LSM Forest** — ~20-30 individual LSM trees organized into a hierarchy:

```
Forest > Grooves > Trees > Levels
```

- **Groove:** Represents one object type (accounts, transfers). Contains multiple trees.
- **Tree:** One for primary data + several for secondary indexes. Each has 7 levels (0-6), growth factor of 8.

### 9.2 Static Memory Allocation

Most distinctive architectural choice: **all memory allocated once at startup**. No dynamic allocation during operation.

- Fixed-size data types: accounts and transfers are 128 bytes each, cache-line aligned.
- Uses `addOneAssumeCapacity` and `putAssumeCapacityNoClobber` — methods that panic on allocation.
- The storage engine can address **100 TiB of storage using only 1 GiB of RAM**.

**Grid cache (block cache):**
- Fixed-size blocks of **512 KiB** each.
- Set-associative cache with **CLOCK eviction** policy.
- Default 1 GiB, recommended >12 GiB for production.

**Benefits:** No fragmentation, no OOM at runtime, exact resource calculations, bounded memory means better CPU cache behavior.

### 9.3 Compaction (Musical Metaphor)

- One **bar** = `lsm_compaction_ops` beats of work.
- Each beat executes after a commit.
- Selection: pick the table with the **fewest overlapping tables** in the target level.
- **Move table optimization:** Zero-overlap tables are moved by metadata update only.

### 9.4 Data File Format

Single pre-allocated file with three zones:
1. **WAL:** Ring buffer of prepares.
2. **Superblock:** 4 redundant copies. Contains manifest references, free set, consensus state.
3. **Grid:** Elastic array of 512 KiB blocks. Each identified by `(index: u64, checksum: u128)`.

Uses `io_uring` with `O_DIRECT`. Single-threaded event loop. Falls back to kqueue on macOS, IOCP on Windows.

---

## 10. Applicability to Strata

### Pattern Mapping

| Pattern | Source | Fit for Strata |
|---------|--------|----------------|
| Keys always in RAM, values to disk | Redis Enterprise, DragonflyDB | **Direct** — `FxHashMap<Arc<Key>, VersionChain>` stays; cold `VersionChain` contents go to disk |
| Mark-sweep cooling (2 consecutive marks → demote) | DragonflyDB | **Simpler than sampled LRU** — zero per-key overhead, no timestamps needed |
| SmallBins (pack small values into 4KB pages) | DragonflyDB | **Essential** — `Value::Int`/`Value::Bool` are tiny; direct I/O without packing wastes >98% of each page |
| Per-shard/branch files | DragonflyDB | **Direct** — maps to existing `DashMap<BranchId, Shard>` partitioning |
| redb as cold tier backend | redb | **Best pure-Rust option** — stable COW B-tree MVCC, configurable cache, single-writer fits OCC commit lock |
| Tiered eviction thresholds (background + forced) | WiredTiger | **Worth adopting** — prevents both OOM and latency spikes |
| Dual meta page (crash-safe root update) | LMDB | **Nice to have** — simplest crash recovery if building custom append log |
| History store (separate old versions) | WiredTiger | **Relevant** — version chains could separate current value from history |
| Hazard pointers for concurrent eviction | WiredTiger | **Consider** — if read concurrency is high; simpler than rwlocks during eviction |
| Static memory allocation | TigerBeetle | **Aspiration** — pre-allocate cache slots for predictable memory |
| BlobDB value separation | RocksDB | **Relevant** — if using LSM for cold tier, separate large values |

### Closest Architectural Match: DragonflyDB

DragonflyDB's architecture is the closest match to Strata's problem:

1. **Same starting point:** In-memory hash map as primary store.
2. **Same constraint:** API must be transparent (Redis-compatible / Strata API).
3. **Same solution shape:** Keys in RAM, values tiered to SSD with background cooling.
4. **SmallBins:** Essential for Strata's mix of small (`Value::Int`, `Value::Bool`) and large (`Value::Bytes`, `Value::Json`) values.
5. **Per-shard files:** Maps directly to Strata's per-branch `DashMap<BranchId, Shard>`.

### Key Differences from DragonflyDB

1. **MVCC:** Strata has version chains (multiple versions per key). DragonflyDB does not. Cold tier must handle versioned data.
2. **Transactions:** Strata has OCC transactions with read_set/write_set validation. Snapshot reads during transactions may hit disk.
3. **Prefix scans:** Strata uses `BTreeSet<Key>` for ordered prefix scans. The ordered index must remain in memory even when values are cold.
4. **Embeddings/vectors:** Strata has vector data alongside KV — additional memory pressure from a subsystem DragonflyDB doesn't have.

### Architecture Recommendation

```
┌─────────────────────────────────────────────────────┐
│  Always in RAM                                       │
│  ├── Key index: FxHashMap<Arc<Key>, KeyMeta>         │
│  ├── Ordered index: BTreeSet<Arc<Key>>               │
│  └── KeyMeta: { version, tier_state, file_offset }   │
├──────────────────────────────────────────────────────┤
│  Hot Tier (RAM, bounded)                              │
│  └── VersionChain contents for frequently accessed    │
│      keys, managed by mark-sweep cooling              │
├──────────────────────────────────────────────────────┤
│  Cold Tier (disk, per-branch files)                   │
│  ├── Option A: Append-only log + index (simplest)     │
│  ├── Option B: redb (pure Rust, COW B-tree, MVCC)     │
│  └── SmallBins packing for small values               │
├──────────────────────────────────────────────────────┤
│  Frozen Tier (mmap, already implemented)              │
│  ├── Vector embeddings (.svec mmap)                   │
│  ├── HNSW graphs (.shgr mmap)                         │
│  └── BM25 segments (.sidx mmap)                       │
└─────────────────────────────────────────────────────┘
```

### Decision Matrix

| Decision | Recommended | Alternative | Rationale |
|----------|-------------|-------------|-----------|
| Tier boundary | `TieredStore` wrapping `ShardedStore` | Inside `ShardedStore` | Smallest blast radius; `Storage` trait is the natural boundary |
| Disk format | Append-only log (Phase 1) → redb (Phase 2) | RocksDB wrapper | Start simple, graduate to batteries-included if needed |
| Eviction policy | Mark-sweep cooling (DragonflyDB-style) | Sampled LRU (Redis-style) | Zero per-key overhead; simpler than maintaining timestamps |
| Eviction granularity | Key-level | Page-level (64KB pages) | Values are large enough; simplest to reason about |
| Promotion strategy | Eager (promote on read) | Frequency-gated (2nd read) | Simple; mark-sweep handles scan pollution naturally |
| Small value handling | SmallBins (4KB page packing) | Inline in index | Direct I/O requires alignment; packing amortizes I/O cost |
| Memory pressure | Tiered thresholds (WiredTiger-style) | Single threshold | Background (80%) + forced (95%) prevents both OOM and latency spikes |
