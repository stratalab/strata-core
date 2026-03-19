# Read Path Optimization Plan v2: Closing the Gap with RocksDB

**Status:** Planning
**Date:** 2026-03-18
**Target:** 50-100K random reads/s at 1B keys (from current 2.9K/s)
**Reference:** RocksDB achieves 189K reads/s at 900M keys (m5d.2xlarge, Direct I/O)

---

## 1. Where We Are

### Current Performance (post-compaction, post-restart-points)

| Scale | Read ops/s | Read p50 | Write ops/s | RSS | Disk | Space Amp |
|------:|----------:|----------|------------:|----:|-----:|---------:|
| 1K | 1,374K | 0.6us | 82K | 0.1GB | 0.0GB | 1.6x |
| 10K | 1,133K | 0.8us | 87K | 0.1GB | 0.0GB | 1.6x |
| 100K | 783K | 1.1us | 85K | 0.2GB | 0.2GB | 1.6x |
| 1M | 27K | 37.5us | 86K | 1.3GB | 3.3GB | 3.4x |
| 10M | 22K | 43.0us | 80K | 7.5GB | 33.4GB | 3.4x |
| **1B** | **2.9K** | **192.1us** | **265K** | **2.4GB** | **129.7GB** | **5.2x** |

### What We Already Have (shipped)

- Leveled compaction L0-L6 with cascading triggers
- pread + sharded block cache (16 shards, priority tiers, auto-sizing to 4GB)
- Version-based segment tracking with ArcSwap
- Bloom filters (full filter, 10 bits/key, <2% FPR)
- Restart points with binary search within data blocks (16-entry intervals)
- Binary search on L1+ key ranges (1 file per level)
- Zero-copy key comparison (EntryHeaderRef)
- Zstd block compression
- jemalloc allocator

### The Gap

RocksDB on similar hardware (900M keys, NVMe SSD): **189K reads/s**.
Strata at 1B keys: **2.9K reads/s**. That's a **65x gap**.

---

## 2. Anatomy of a RocksDB Point Lookup vs Strata

### RocksDB at 1B keys (warm cache, well-tuned)

```
1. Memtable check                               ~1us
2. L0 bloom filter checks (4 files, pinned)      4 × ~100ns cache hit = 0.4us
3. L1-L5 bloom checks (1 per level, cached)      5 × ~100ns cache hit = 0.5us
4. Bloom says "maybe" at target level             0ns (already computed)
5. Index block lookup (cached, partitioned)       ~200ns
6. Data block pread + decompress                  ~15us (NVMe) or ~50ns (cache hit)
   Total (1 SSD read):  ~17us → 59K/s
   Total (cache hit):   ~3us → 333K/s
```

### Strata at 1B keys (current)

```
1. Memtable check                               ~1us
2. L0 segment checks (bloom + index + block)     N × full lookup (bloom not pinned)
3. L1-L5 checks (1 per level)
   For each level:
     a. Binary search on key ranges               ~100ns
     b. Load full bloom filter (pread if evicted)  ~15us miss or ~50ns hit
     c. Bloom check                                ~100ns
     d. Index block scan (full keys, no sep.)      ~500ns
     e. Data block pread + decompress + scan       ~15us miss or ~1us hit
   Total (1 SSD read):  ~35us → 29K/s
   Total (cache miss):  ~192us → 5K/s (multiple SSD reads)
```

### Why Strata is 65x Slower

The gap breaks down into compounding factors:

| Factor | Strata | RocksDB | Penalty |
|--------|--------|---------|---------|
| **Space amplification** | 5.2x | ~1.1x | 4.7x more data to search/cache |
| **Bloom filter memory** | Full per-segment, evictable | Partitioned, top-level pinned | Cache thrashing |
| **L0 metadata** | Evictable | Pinned in cache | Extra pread per L0 lookup |
| **Block size efficiency** | No prefix compression | 30-50% smaller blocks | Fewer blocks fit in cache |
| **Index block size** | Full keys (~200KB/seg) | Shortened separators (~30KB/seg) | 6x larger indexes |
| **Cache utilization** | Data + full bloom compete | Only needed partition loaded | Wasted cache capacity |

The root cause is not any single feature but **cache inefficiency at scale**: Strata stores more bytes per key (no prefix compression), loads more metadata per lookup (full bloom, full index keys), and doesn't protect critical metadata from eviction (no L0 pinning). At 1B keys where the working set far exceeds RAM, every wasted cache byte means more SSD reads.

---

## 3. The Optimization Plan

Six phases, ordered by impact. Each phase is independent and benchmarkable.

### Phase A: Dynamic Level Sizing (~3 days)

**Problem:** Static level targets waste space. With fixed 256MB L1, a 130GB database at 10x multiplier should have levels sized [256MB, 2.5GB, 25GB, 130GB] but Strata doesn't adapt targets based on actual data volume.

**What RocksDB does:** `CalculateBaseBytes` works backward from the last level's actual size. If last level = 100GB and multiplier = 10, then L4 target = 100GB, L3 = 10GB, L2 = 1GB, L1 = 100MB. Empty intermediate levels are normal — data jumps to the base level.

**Impact:** Reduces space amplification from 5.2x toward ~1.1x. Fewer levels = fewer bloom checks per lookup. Less total data = better cache hit rate.

**Implementation:**
- Add `recalculate_level_targets()` to `BranchState` after each compaction
- Walk backward from last non-empty level: `target[i] = target[i+1] / multiplier`
- `compact_level()` uses dynamic targets for score calculation: `score = actual / target`
- Skip empty intermediate levels in `point_lookup_level()`

**Estimated read improvement:** 2-3x (fewer levels, less data per level, better cache fit)

---

### Phase B: Prefix Compression in Data Blocks (~5 days)

**Problem:** Every entry stores the full InternalKey. Within a 64KB block, adjacent keys typically share 20-50 bytes of namespace + type tag + key prefix. At 1B keys, this wastes 30-50% of block space.

**What LevelDB does:** At restart points, the full key is stored. Between restart points, only the delta is stored: `shared_bytes | unshared_bytes | value_len | key_delta | value`. This is the cornerstone of LevelDB's block format — restart points exist specifically to enable prefix compression.

**Impact:** 30-50% smaller blocks after compression means:
- 30-50% more entries fit in the block cache
- Fewer SSD reads per lookup at scale
- Better zstd compression ratios (less redundant data)

**Implementation:**

Writer (`segment_builder.rs`):
```
encode_entry_v3(prev_key, key, entry, buf, is_restart):
  if is_restart:
    shared = 0
  else:
    shared = common_prefix_len(prev_key, key)
  unshared = key.len() - shared
  buf.push_varint32(shared)
  buf.push_varint32(unshared)
  buf.push_varint32(value_encoded_len)
  buf.push(key[shared..])
  buf.push(encoded_value)
```

Reader (`segment.rs`):
- `decode_entry_header_ref` reconstructs the full key from `shared + delta`
- Binary search at restart points still works (shared = 0 at restarts, full key available)
- Linear scan between restarts must track the previous key to reconstruct current key

**Format:** Bump to v4. Reader accepts v2 (no restarts), v3 (restarts, no prefix), v4 (restarts + prefix).

**Estimated read improvement:** 1.5-2x (better cache utilization)

---

### Phase C: L0 Metadata Pinning + Partitioned Bloom Filters (~5 days)

**Problem 1:** L0 filter and index blocks can be evicted from the block cache under memory pressure. Since every point lookup must check all L0 files, eviction causes repeated SSD reads on the hottest path.

**Problem 2:** Each segment's bloom filter is a single monolithic block (e.g., 5MB for a 500MB segment at 10 bits/key). The entire filter must be in cache to do a single probe. At 1B keys with hundreds of segments, bloom filters alone consume gigabytes of cache.

**What RocksDB does:**
- `pin_l0_filter_and_index_blocks_in_cache`: L0 metadata blocks are marked unpinnable
- `pin_top_level_index_and_filter`: top-level partition index always in cache
- Partitioned filters: split bloom into ~4KB partitions with a small top-level index. Only the needed partition is loaded per lookup.

**Implementation:**

Part 1 — L0 pinning:
- Add `Priority::Pinned` to `BlockCache` (never evicted, separate capacity budget)
- On segment open, if level == 0, insert bloom/index with `Priority::Pinned`
- On L0→L1 compaction, demote to `Priority::High`
- Budget: reserve 10% of cache capacity for pinned blocks

Part 2 — Partitioned bloom:
- Builder: instead of one bloom for all keys, build one bloom per N data blocks (e.g., per 64KB of block data)
- Write partition blooms as a filter meta block with a top-level index mapping key ranges to partition offsets
- Reader: on lookup, binary search top-level index for the relevant partition, load only that ~4KB partition from cache/disk
- Format: new filter block type alongside existing full filter (keyed by `filter.partitioned.bloom` in metaindex)

**Estimated read improvement:** 2-3x (eliminates L0 cache misses, 100x less filter memory per lookup)

---

### Phase D: Index Key Shortening (~2 days)

**Problem:** Index entries store the full first key of each data block (~60-200 bytes). RocksDB stores the shortest separator between adjacent blocks (~10-30 bytes). At 1B keys with thousands of blocks per segment, Strata's index blocks are 3-6x larger.

**What LevelDB does:** `FindShortestSeparator(start, limit)` returns the shortest string >= start and < limit. For example, if the last key in block 0 is `"user:alice:100"` and the first key in block 1 is `"user:bob:1"`, the separator could be `"user:b"`.

**Implementation:**
- Add `shortest_separator(a: &[u8], b: &[u8]) -> Vec<u8>` utility
- In `build_from_iter`, after flushing a block, compute the separator between the block's last key and the next block's first key
- Store the separator (not the full first key) in the index entry
- Index binary search works identically (separators maintain the ordering invariant)

**Estimated read improvement:** 1.2x (smaller index blocks, better cache fit)

---

### Phase E: Improved Bloom Filter (~3 days)

**Problem:** Strata's bloom filter uses CRC32 + FNV-1a hashing (Kirsch-Mitzenmacher construction). These are weaker hash functions than the xxHash/MurmurHash used by RocksDB. Measured FPR is ~1.8% at 10 bits/key vs theoretical ~0.8%.

Additionally, the Monkey paper (SIGMOD 2017) proved that allocating the same bits/key at every level is suboptimal. The largest level should get more bits because it dominates false-positive cost.

**Implementation:**

Part 1 — Better hash function:
- Replace `crc32fast::hash` + `fnv1a` with `xxhash-rust` (xxh3 — 64-bit, SIMD-accelerated)
- Single 64-bit hash → split into two 32-bit halves for Kirsch-Mitzenmacher
- Expected FPR improvement: 1.8% → ~0.9% at 10 bits/key

Part 2 — Per-level bloom allocation (Monkey):
- Store `bloom_bits_per_key` in `SegmentBuilder` (not hardcoded)
- During compaction, compute bits/key based on target level:
  ```
  bits_per_key[level] = base_bits + log2(size_ratio) * (max_level - level)
  ```
  Example with 10 base, ratio 10, 5 levels: L5 = 10, L4 = 13, L3 = 16, L2 = 20, L1 = 23
- Same total memory budget, but concentrated where it matters most

**Estimated read improvement:** 1.3x (fewer false-positive SSD reads)

---

### Phase F: Data Block Hash Index (~3 days)

**Problem:** Even with restart points, a point lookup within a data block does O(log R) binary search on restarts + O(16) linear scan within the interval. RocksDB's data block hash index maps keys directly to their restart interval in O(1).

**What RocksDB does:** Appends a hash table to each data block. Each hash entry maps to a restart interval index (uint8). Point lookups hash the key, find the interval, then do a single linear scan of ≤16 entries. Space overhead: ~4.6% per block.

**Implementation:**
- After entries + restart trailer, append a hash table: `[bucket_0: u8] ... [bucket_N: u8] [num_buckets: u16]`
- Each bucket stores the restart interval index (0-255) or 0xFF for empty
- Hash function: `xxh3(typed_key_prefix) % num_buckets`
- `num_buckets = num_entries / 0.75` (75% load factor)
- Reader: hash the key, read the bucket, jump to that restart offset, linear scan
- Falls back to binary search for: range scans, hash collisions, blocks > 255 restart intervals
- Format: v5 (v4 = prefix compression, v5 = prefix + hash index)

**Estimated read improvement:** 1.1x (marginal — innermost loop, masked by I/O)

---

## 4. Projected Impact

### Conservative Estimates (multiplicative)

| Phase | Improvement | Cumulative | Projected 1B reads/s |
|-------|-----------|-----------|---------------------|
| Current | — | 1x | 2,900 |
| A: Dynamic level sizing | 2-3x | 2.5x | 7,250 |
| B: Prefix compression | 1.5-2x | 4.4x | 12,750 |
| C: L0 pinning + partitioned bloom | 2-3x | 11x | 31,900 |
| D: Index key shortening | 1.2x | 13x | 37,700 |
| E: Improved bloom | 1.3x | 17x | 49,300 |
| F: Block hash index | 1.1x | 19x | 55,100 |

**Target: 50-100K reads/s** — achievable with Phases A through E.

### RocksDB Comparison Point

RocksDB at 900M keys on m5d.2xlarge with Direct I/O: 189K reads/s. That benchmark uses 6.4GB cache on a 300GB dataset. Key advantages RocksDB has beyond our Phase F:

- Ribbon filters (30% less memory than bloom at same FPR)
- HyperClockCache (lock-free, vs our sharded-mutex LRU)
- MultiGet with io_uring batching
- Direct I/O (no OS page cache overhead)
- `kBinarySearchWithFirstKey` (deferred block loads during iteration)
- Compression dictionary (shared context improves zstd ratios on small blocks)

These are Phase 2 optimizations — diminishing returns beyond 50K/s.

---

## 5. Implementation Order and Dependencies

```
Phase A: Dynamic Level Sizing          (3 days, no deps)
    ↓
Phase B: Prefix Compression            (5 days, no deps — but benefits from A's smaller dataset)
    ↓
Phase C: L0 Pinning + Partitioned Bloom (5 days, no deps)
    ↓
Phase D: Index Key Shortening          (2 days, no deps)
    ↓
Phase E: Improved Bloom                (3 days, no deps)
    ↓
Phase F: Block Hash Index              (3 days, depends on B for v4 format)
```

All phases are independently implementable and benchmarkable. Total: ~21 days.

Phase B (prefix compression) is the most complex because it changes the fundamental entry encoding, requires format versioning, and affects both the write and read paths. Everything else is additive.

---

## 6. What We Are NOT Doing (and Why)

| Feature | Why Not |
|---------|---------|
| **Direct I/O** | Our pread + block cache already bypasses double-caching. Direct I/O saves ~10% but adds alignment complexity. Diminishing returns. |
| **MultiGet** | Important for analytics but our primary workload is transactional (1 key at a time). Add when an analytics use case demands it. |
| **Ribbon filters** | 30% space savings but 3-4x CPU cost and complex implementation. Bloom with Monkey allocation gets us close enough. |
| **HyperClockCache** | Our sharded LRU is correct and fast enough. Lock-free clock cache is a month of work for ~10% improvement. |
| **Compression dictionary** | Requires sampling during compaction and dictionary storage. Complex for modest gain (~10% better compression). |
| **PlainTable format** | Only useful for pure in-memory workloads. Our target is SSD-backed 1B keys. |
| **Key-value separation (BlobDB)** | Only helps when values > 1KB. Our default benchmark uses 8-byte values. Add when large-value workloads appear. |

---

## 7. Verification Plan

After each phase, run the full scaling benchmark:

```bash
cd ~/Documents/GitHub/strata-benchmarks
cargo bench --bench scaling -- --scales 1000,10000,100000,1000000,10000000
```

At 10M keys (the fastest feedback loop that still exercises the disk path), verify:
- Read ops/s improves by the expected factor
- No regression in write throughput
- Space amplification decreases (especially after Phase A)
- All existing tests pass

After Phases A-E, run the 1B benchmark to validate the end-to-end target:
```bash
cargo bench --bench scaling -- --scales 1000000000 --tiers kv
```

Target: ≥50K random reads/s at 1B keys.
