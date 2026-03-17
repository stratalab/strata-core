# Billion-Scale Roadmap: Path to 1B Keys

**Status:** Planning
**Date:** 2026-03-17
**Target:** 50-100K random reads/s at 1B keys (1TB on disk), matching RocksDB

---

## The Problem

At 1B keys × 1KB values = ~1TB on disk:
- Data cannot fit in memory — reads MUST come from disk
- With NVMe SSD: ~10-15us per random 4KB read
- With bloom filters: 99% of file checks eliminated → 1-2 actual block reads per lookup
- With leveled compaction (5 levels): at most 5 bloom checks + 1-2 block reads
- **Theoretical max: ~50-100K random reads/s** (matching RocksDB)

Current Strata at 10M keys: 2.3K reads/s. That's **50x below target**.

---

## Root Cause: mmap Is the Wrong I/O Model

The fundamental architectural bottleneck is not missing features — it's **mmap**.

Our segment reader uses `memmap2::Mmap`. Every block read goes through the OS page cache via page faults. At 10M keys:

```
1000 random reads → 3,350 major page faults
Each fault: ~100us (kernel context switch + NVMe read + return to userspace)
3,350 faults × 100us = 335ms for 1000 reads = 335us per read → 3K/s
```

RocksDB uses `pread()` + application-managed block cache, NOT mmap. Why:

1. **No cache priority with mmap** — OS evicts index/bloom blocks (must-have) same as data blocks (nice-to-have)
2. **Double caching** — our `BlockCache` holds decompressed blocks AND OS page cache holds compressed mmap pages = 2x memory
3. **Page fault overhead** — each cold read is a kernel page fault (context switch) vs `pread()` which is a direct syscall
4. **TLB pressure** — thousands of mmap'd segments thrash the TLB at 1TB scale
5. **No I/O scheduling** — can't prioritize user reads over compaction reads

**This is tracked as #1523** and should be done BEFORE or IN PARALLEL with leveled compaction (#1519). Without it, even perfect compaction + perfect bloom filters still hit 100us page faults on every cold block.

The dependency is:
```
#1523 (pread I/O) + #1517 (block cache) = controlled I/O + controlled caching
         ↓
#1519 (leveled compaction) = O(1) file per level
         ↓
50-100K reads/s at 1B keys
```

---

## Architecture Required for 1B Scale

### Read Path (from RocksDB source analysis)

```
Point Lookup at 1B keys:
  1. Check memtable (SkipMap)                    ~1us
  2. Check L0 files (all, with bloom)            ~500ns × 4 files = 2us
  3. Check L1+ files (1 per level, with bloom)
     For each of 4-5 levels:
       a. FileIndexer binary search               ~100ns
       b. Bloom filter check                       ~100ns
       c. If bloom says MAYBE:
          - Block cache lookup                     ~50ns (hit) or
          - Disk read + decompress                 ~15us (miss)
  4. Total (cache hit): ~3-5us → 200-300K/s
  5. Total (cache miss): ~20-30us → 30-50K/s
```

### What's Required

| Component | Purpose | RocksDB Implementation | Strata Status |
|-----------|---------|----------------------|---------------|
| **Leveled compaction** | 1 file per level for L1+ | L0→Lbase, 10x multiplier, non-overlapping | Flat segments (#1519) |
| **Version tracking** | Safe concurrent reads during compaction | Immutable Version + refcount | Mutable DashMap (#1520) |
| **O(1) block cache** | Cache hot blocks, eliminate decompression | Sharded LRU, 64 shards | O(N) HashMap (#1517) |
| **Partitioned index** | Index blocks cached independently | Index-of-indexes | Single index block |
| **Partitioned bloom** | Bloom filters cached independently | Per-SST partitioned filter | Single per-segment |
| **FileIndexer** | O(log N) file lookup within a level | Binary search on key ranges | Linear scan |
| **Dynamic level sizing** | Adaptive level targets as data grows | CalculateBaseBytes | Fixed 64MB base |
| **Rate limiter** | Prevent compaction from starving reads | Token-based, 100MB/s default | None |
| **Streaming segment writer** | Build segments without full-memory buffering | Block-at-a-time output | Full in-memory build |
| **Per-level compression** | Fast compression for hot levels, dense for cold | Snappy L0-L2, Zstd L3+ | Zstd everywhere |

---

## Implementation Phases

### Phase 1: Foundation (unblocks everything else)

**1.1 Version-based segment tracking (#1520)**
- Immutable `SegmentVersion` with Arc refcounting
- Atomic swap on compaction (like LevelDB's `LogAndApply`)
- Readers hold `Arc<SegmentVersion>` throughout their read
- **Why first:** Leveled compaction requires this for correctness
- **Effort:** ~5 days

**1.2 MANIFEST extension for level tracking**
- Append-only log of `VersionEdit` records (add/remove files per level)
- Recovery: replay MANIFEST to rebuild current version
- Track: file number, level, key range, size per segment
- **Why:** Crash recovery must know which level each segment belongs to
- **Effort:** ~3 days

**1.3 O(1) sharded LRU block cache (#1517 Phase 1)**
- Replace `HashMap + min_by_key` with doubly-linked-list LRU
- 16-64 shards to reduce mutex contention
- Auto-size to `max(256MB, available_RAM / 4)`
- **Why:** Large cache must not slow down with O(N) eviction
- **Effort:** ~3 days

### Phase 2: Leveled compaction (#1519)

**2.1 L0/L1 structure**
- `BranchState` holds `l0_segments` (overlapping) and `l1_segments` (non-overlapping, sorted by key range)
- Flush → L0. When L0 count >= 4: compact L0 + overlapping L1 → new L1 segments
- L1 binary search: `partition_point` on key ranges → check 1 segment
- **Why:** This alone reduces read amp from O(N) to O(L0 + 1)
- **Effort:** ~5 days

**2.2 Multi-level (L2+)**
- Each level is 10x the size of the previous
- Compaction picks highest-scoring level: `score = actual_size / target_size`
- Non-overlapping guarantee maintained via key-range-aware merge
- `compact_pointer` per level for round-robin key selection
- **Why:** At 1TB, need 5+ levels to keep per-level size manageable
- **Effort:** ~5 days

**2.3 Dynamic level sizing**
- `CalculateBaseBytes`: work backward from last level's actual size
- Empty intermediate levels are normal (data jumps to base level)
- Recompute on every compaction completion
- **Why:** Static level targets waste space at both small and large scales
- **Effort:** ~3 days

### Phase 3: Read path optimization

**3.1 FileIndexer for L1+ file lookup**
- Binary search on sorted key ranges within each level
- Cache file boundary keys in memory (persistent across compactions)
- **Why:** At 1TB, L5 could have 1000+ files. Linear scan = 1000 comparisons
- **Effort:** ~3 days

**3.2 Restart points in data blocks (#1518)**
- Binary search within blocks (16-entry intervals)
- With MVCC-aware backward scan
- Property-based test suite
- **Why:** 4x fewer comparisons per block read
- **Effort:** ~4 days (with proper testing)

**3.3 Partitioned bloom filters**
- Instead of one bloom per segment, partition into sub-filters per block range
- Cache individual partitions independently (hot partitions stay cached)
- **Why:** At 1TB, a single segment bloom filter can be 10MB. Loading it to check one key wastes cache
- **Effort:** ~4 days

**3.4 Index block caching**
- Cache index blocks separately from data blocks (higher priority)
- RocksDB: `cache_index_and_filter_blocks = true`
- **Why:** Index blocks are accessed on every read. Must never be evicted.
- **Effort:** ~2 days

### Phase 4: Write path + space efficiency

**4.1 Rate limiter for compaction I/O**
- Token-based rate limiter (default 100MB/s)
- Compaction reads/writes throttled, user I/O unthrottled
- **Why:** Prevents compaction from starving user reads at 1TB
- **Effort:** ~3 days

**4.2 Streaming segment builder**
- Write blocks to disk as they're built (not buffer entire segment in memory)
- Flush data blocks incrementally, build index/bloom at the end
- **Why:** At 1TB, compaction output segments can be 1GB+. Can't buffer in memory
- **Effort:** ~5 days

**4.3 Per-level compression strategy**
- L0-L2: No compression or LZ4 (fast, prioritize write speed)
- L3+: Zstd level 3 (dense, data is cold)
- Bottommost level: Zstd level 6 (maximum compression)
- **Why:** Hot data decompression cost matters. Cold data storage cost matters.
- **Effort:** ~2 days

**4.4 Write batch coalescing (#1521)**
- Group concurrent same-branch writers into single WAL write
- Leader-follower model
- **Why:** Reduces WAL I/O for concurrent workloads
- **Effort:** ~4 days

**4.5 Soft write throttle (#1522)**
- Gradual backpressure (1ms delay) before hard stall
- Gives compaction time to catch up
- **Why:** Prevents burst → stall → burst pattern
- **Effort:** ~2 days

### Phase 5: Large-scale robustness

**5.1 Key-value separation (BlobDB-style)**
- Values > 1KB stored in separate blob files
- LSM tree stores only key + blob pointer
- **Why:** At 1B keys with large values, write amplification is dominated by rewriting values during compaction
- **Effort:** ~10 days

**5.2 Max compaction bytes limit**
- Cap single compaction job at 10GB of input
- Split larger compactions into subcompactions
- **Why:** Prevents single compaction from blocking writes for minutes
- **Effort:** ~5 days

**5.3 Parallel subcompactions**
- Single compaction job partitioned by key range
- N threads each process a disjoint range
- **Why:** At 1TB, compaction I/O is the bottleneck. Parallelism = faster compaction = lower read amp
- **Effort:** ~7 days

---

## Timeline Estimate

| Phase | Duration | Cumulative | Scale Unlocked |
|-------|----------|------------|---------------|
| Phase 1 (foundation) | 2-3 weeks | 2-3 weeks | Safe compaction, proper caching |
| Phase 2 (leveled compaction) | 2-3 weeks | 4-6 weeks | **100M keys** |
| Phase 3 (read optimization) | 2-3 weeks | 6-9 weeks | **1B keys (reads)** |
| Phase 4 (write + space) | 2-3 weeks | 8-12 weeks | **1B keys (writes + space)** |
| Phase 5 (robustness) | 3-4 weeks | 11-16 weeks | **Production 1B** |

---

## Expected Performance at Each Milestone

| Scale | Current | After Phase 2 | After Phase 3 | After Phase 4 |
|------:|--------:|--------------:|--------------:|--------------:|
| 1M | 319K/s read | 350K/s | 400K/s | 400K/s |
| 10M | 2.3K/s read | 80K/s | 150K/s | 150K/s |
| 100M | N/A | 30K/s | 80K/s | 100K/s |
| 1B | N/A | N/A | 30K/s | 50-100K/s |

---

## Dependencies

```
Phase 1.1 (version tracking) ──→ Phase 2.1 (L0/L1)
Phase 1.2 (MANIFEST)         ──→ Phase 2.1 (L0/L1) ──→ Phase 2.2 (multi-level)
Phase 1.3 (block cache)      ──→ Phase 3.3 (partitioned bloom)
                                                    ──→ Phase 3.4 (index caching)

Phase 2 (leveled compaction) ──→ Phase 3.1 (FileIndexer)
                              ──→ Phase 4.1 (rate limiter)
                              ──→ Phase 4.2 (streaming builder)
                              ──→ Phase 5 (robustness)
```

---

## RocksDB Read Path Optimizations at 1B (from source audit)

### Cache-Local Blocked Bloom Filter (FastLocalBloom)

RocksDB's bloom filter (`util/bloom_impl.h`) is designed for CPU cache efficiency:
- Divides filter into **512-bit cache-line buckets** — all probes for a key stay within 1 L1 cache line
- AVX2 SIMD path computes **8 probes in parallel** per cycle
- Dynamic probe count: 10 bits/key → 6 probes → 0.95% FP rate
- **Impact:** ~100x reduction in unnecessary block reads at 1B scale

**Strata gap:** Our bloom filter uses standard bit array. At 1B scale, the cache-line-local design matters for throughput.

### Data Block Hash Index

RocksDB optionally appends a hash table to each data block (`data_block_hash_index.h`):
- O(1) hash lookup to find restart interval instead of O(log k) binary search
- ~1.33 bytes per key overhead within the block
- CPU cache friendly: hash table fits in L2 cache
- **Impact:** 5-10% speedup on point lookups, eliminates binary search within blocks entirely

**Strata applicability:** Nice-to-have after restart points are implemented. Alternative approach.

### Partitioned Index and Filters

At 1B keys, index blocks become 1-10MB. Loading the full index wastes cache:
- **Partitioned index** (`partitioned_index_reader.cc`): divide index into 64KB partitions, load on-demand
- **Partitioned bloom**: same idea for bloom filters
- Top-level "index of indexes" enables lazy loading
- `CacheDependencies` prefetches all partitions in single OS call

**Strata gap:** We load entire index and bloom on `KVSegment::open()`. At 1B scale, this consumes GB of memory.

### FileIndexer for Cross-Level Search

`FileIndexer` (`db/file_indexer.cc`) precalculates file range overlaps between adjacent levels:
- When checking L1 after L0, uses L0 comparison results to narrow L1 search range
- Binary search within each level: O(log N) instead of O(N) file comparisons
- At 1B with 1000+ files in bottom levels, this saves 100s of comparisons per read

**Strata gap:** We linearly scan all segments. This is the #1 read path bottleneck at scale.

### MultiGet Batched I/O

`MultiGet` (`block_based_table_reader_sync_and_async.h`) batches multiple key lookups:
- Detects when multiple keys map to the same block → fetch block once
- Issues async cache lookups for all keys, waits once (not N waits)
- 4x faster per key for 32-key batches vs individual Gets

**Strata gap:** No batched read API. Each `kv_get` is independent.

### Clock Cache (replacing LRU)

RocksDB's newer cache (`cache/clock_cache.cc`) uses countdown-based eviction:
- Lock-free reads with atomic countdown
- Scan-resistant: scanned blocks get low initial countdown, evicted faster
- O(1) eviction (clock hand sweep) vs LRU's O(1) with higher constant
- 3 priority levels: HIGH (index/filter), LOW (data), BOTTOM (scan results)

**Strata gap:** Our `HashMap + min_by_key` is O(N). Must be replaced before scaling.

---

## RocksDB Space Efficiency at 1B (from source audit)

### Per-Level Compression

```
Level 0-2: No compression or LZ4 (fast, data is hot)
Level 3-6: Snappy (moderate compression, moderate speed)
Bottommost: Zstd level 6-8 (maximum compression, data is cold)
```

**Strata gap:** We use uniform Zstd level 3. At 1B, hot data decompression wastes CPU. Cold data could be compressed more.

### BlobDB (Key-Value Separation)

Large values (>4KB) stored in separate blob files. LSM tree holds only key + blob pointer:
- Write amplification reduced 36-90% for large-value workloads
- GC integrated into compaction (old blob files relocated)
- `min_blob_size` configurable (typically 1-4KB threshold)

**Strata applicability:** Defer unless workloads have values >100KB regularly. Monitor first.

### Tombstone Optimization

- `optimize_filters_for_hits = true`: skip bloom filter on bottommost level (all data is there, no point filtering)
- Range tombstones fragmented during compaction for efficient cleanup
- Tombstones below prune floor dropped at bottommost level

**Strata quick win:** Skip bloom for cold-tier segments. 2-3% space savings, trivial to implement.

### MANIFEST at Scale

RocksDB uses binary-encoded `VersionEdit` records (~100 bytes per file change):
- Recovery: replay MANIFEST, O(N) where N = number of edits since last checkpoint
- Periodic checkpoint: compact MANIFEST to single snapshot
- `db/repair.cc`: scan all SST file headers to rebuild MANIFEST from scratch

**Strata gap:** Our MANIFEST is a single JSON record with no version history. At 1B, we need:
- Binary MANIFEST format with incremental edits
- Checkpoint/pruning strategy
- Repair utility (scan segment headers, rebuild from scratch)

---

## Complete Optimization Inventory

| # | Optimization | Category | Priority | Effort | Issue |
|---|-------------|----------|----------|--------|-------|
| 1 | O(1) sharded LRU/Clock cache | Read | **P0** | 1 week | #1517 |
| 2 | Version-based segment tracking | Foundation | **P0** | 1 week | #1520 |
| 3 | Leveled compaction (L0/L1) | Read+Write | **P0** | 2 weeks | #1519 |
| 4 | MANIFEST level tracking | Foundation | **P0** | 1 week | #1519 |
| 5 | Multi-level (L2+) compaction | Scale | **P0** | 1 week | #1519 |
| 6 | Dynamic level sizing | Scale | **P1** | 1 week | — |
| 7 | FileIndexer (O(log N) file lookup) | Read | **P1** | 1 week | — |
| 8 | Restart points in blocks | Read | **P1** | 1 week | #1518 |
| 9 | Per-level compression | Space | **P1** | 3 days | — |
| 10 | Rate limiter for compaction I/O | Write | **P1** | 1 week | — |
| 11 | Streaming segment builder | Write | **P1** | 1 week | — |
| 12 | Write batch coalescing | Write | **P1** | 1 week | #1521 |
| 13 | Soft write throttle | Write | **P1** | 3 days | #1522 |
| 14 | Partitioned index/bloom | Read | **P2** | 1 week | — |
| 15 | Cache-local bloom filter | Read | **P2** | 1 week | — |
| 16 | Data block hash index | Read | **P2** | 3 days | — |
| 17 | MultiGet batched I/O | Read | **P2** | 1 week | — |
| 18 | Binary MANIFEST format | Recovery | **P2** | 1 week | — |
| 19 | Skip bloom on bottommost level | Space | **P2** | 1 day | — |
| 20 | BlobDB (key-value separation) | Space | **P3** | 2 weeks | — |
| 21 | Parallel subcompactions | Write | **P3** | 2 weeks | — |
| 22 | Tiered hot/cold storage | Space | **P3** | 2 weeks | — |

---

## Reference

- `docs/design/reference-implementation-audit.md` — full comparison with LevelDB/RocksDB
- `docs/design/leveldb-read-path-reference.md` — LevelDB Block::Iter::Seek analysis
- `docs/design/read-path-optimization-plan.md` — current optimization roadmap
- RocksDB source: cloned from `github.com/facebook/rocksdb`
- LevelDB source: cloned from `github.com/google/leveldb`
- Issues: #1517, #1518, #1519, #1520, #1521, #1522
