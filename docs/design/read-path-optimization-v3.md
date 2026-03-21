# Read Path Optimization — V3

**Date:** 2026-03-21
**Status:** In progress. Three optimizations shipped, five more identified.

---

## What We Did

Starting from 14.8K reads/sec at 1M keys (p50=66us), we diagnosed and
fixed three root causes:

| Change | Root Cause | Fix | Impact at 1M |
|--------|-----------|-----|-------------|
| 4KB blocks | 64KB blocks caused 50us pread on page-cache hit (16 page copies) | Match RocksDB default of 4KB | 14.8K → 63K |
| Pinned index/filter | Bloom + sub-index loaded from block cache on every read (mutex + possible pread) | Load into segment memory at open time | 63K → 90K |
| CLOCK cache | Mutex + LRU list manipulation on every block cache hit | RwLock read + atomic store (no write lock on read path) | 90K → 98K |
| Synchronous compaction | Background compaction task never ran due to priority queue starvation | Flush + compact inline on writer's thread | L0 stays ≤4 |

## Current Performance

```
Scale       ops/sec     p50       p99       Notes
100K        784K        1.1us     2.5us     memtable hit
200K        175K        5.6us     15.6us
500K        112K        9.2us     15.9us
1M           98K       10.0us     20.0us
10M          81K       12.2us     24.3us    8-byte values
100M         52K       19.3us     28.2us    8-byte values
```

Degradation is sublinear (O(log N)). The curve flattens from 500K onwards.
The 100K→200K cliff is structural (memtable → segment transition).

## Target

100K+ ops/sec at 1B keys.

## Diagnosis Method

Instrumented `get_versioned_from_branch()` and `scan_block_for_key()`
with per-step timing, sampled every Nth read. Key findings:

- **encode/active/frozen:** <200ns total. Not a factor.
- **L0 scan with 32 segments:** 11-116us. Dominant cost before compaction fix.
- **Per-segment read:** Bimodal. Cache hit = 80-150ns, cache miss = 39-76us.
- **Cache miss cost came from pread of 64KB blocks** even when data was in page cache.
- **Block scan:** read_data_block dominates. restart_seek and linear_scan are <1us.

After fixing block size and pinning:
- **bloom check:** 300-500ns (zero-copy `maybe_contains_raw`)
- **index search:** 50-100ns
- **data block pread (4KB, page-cache hit):** 1-3us
- **block entry scan + decode:** <1us

Remaining cost at depth is the pread per level × number of levels.

## Remaining Optimizations (Priority Order)

### 1. FileIndexer — Fractional Cascading Across Levels

**Problem:** At 1B keys, L6 has 100K+ files. Binary searching all of them
per read is O(log 100K) ≈ 17 comparisons. With 5+ levels, that's 50+
comparisons per read just to find candidate files.

**Solution:** Pre-compute which files in L(n+1) overlap with each file in
L(n). When descending levels, narrow the binary search range to ~10 files
instead of the full level. This is RocksDB's `FileIndexer` — a form of
fractional cascading.

**Data structure:** Per file per level, store four bounds:
```
smallest_lb: left bound from smallest-key comparison
largest_lb:  left bound from largest-key comparison
smallest_rb: right bound from smallest-key comparison
largest_rb:  right bound from largest-key comparison
```

**Where to add it:** Compute in `refresh_level_targets()` (called after
every compaction). Use in `point_lookup_level_preencoded()` to narrow
the `partition_point()` range.

**Expected impact:** Reduces per-level file search from O(log F) to
O(log ~10). Biggest win at 1B+ keys.

### 2. BinarySearchWithFirstKey Index

**Problem:** After index binary search finds a candidate data block, we
must pread it to check if the key exists. Most of the time (in non-bottom
levels), the key isn't there — the pread was wasted.

**Solution:** Store the first key of each data block in the index entry.
Compare lookup key against the first key before reading the block. If
`lookup_key < first_key`, the key can't be in this block — skip the
pread entirely.

**Where to add it:** Extend `IndexEntry` with optional `first_key`.
Write it in `SegmentBuilder`. Check it in `point_lookup_with_index()`
before calling `scan_block_for_key()`.

**Expected impact:** Eliminates ~50% of data block reads for point
lookups within a file. Saves 1-3us per avoided pread.

### 3. Data Block Hash Index

**Problem:** Within a data block, finding the key requires binary search
over restart points (4-5 comparisons, each reconstructing a prefix-compressed
key) then linear scan within the interval.

**Solution:** Append a small hash table (uint8 buckets) to each data block.
On read, hash the key → jump to the correct restart interval in O(1).

**Current state:** We already have `strip_hash_index()` and hash index
support in the read path. Need to verify it's being written for all new
segments.

**Expected impact:** ~10% throughput improvement on cached workloads.

### 4. Skip Bottom-Level Bloom Filters

**Problem:** At the deepest level, the key (if it exists) is definitely
there. The bloom filter just returns true and wastes space.

**Solution:** Don't build bloom filters for the bottom level. The saved
space means more data blocks fit in the block cache.

**Expected impact:** Modest. ~10% metadata space savings at the bottom
level.

### 5. Verify File Handle Pinning at Scale

**Problem:** At 1B keys with 64MB SST files, there are ~400+ SST files
with 8-byte values, or 4000+ with 1KB values. Each needs an open file
descriptor.

**What to check:** Verify `ulimit -n` is sufficient. Verify no code path
closes and re-opens segment file handles. This should already work but
needs validation at scale.

## Implementation Order

1. FileIndexer (biggest impact at 1B)
2. BinarySearchWithFirstKey (eliminates wasted preads)
3. Data block hash index (incremental, may already be partially working)
4. Skip bottom-level blooms (space optimization)
5. Verify file handles (correctness)

## How We Got Here

The original problem was misdiagnosed as "L0 segment count too high,
needs intra-L0 compaction." Profiling revealed:

1. **Compaction was irrelevant** — it never ran. The background scheduler
   starved compaction tasks behind flush tasks due to priority queue
   FIFO ordering. Fixed by making flush+compact synchronous.

2. **The real cliff was per-segment read cost**, not segment count. A
   single segment read cost 6-68us because:
   - 64KB blocks = 50us pread even on page-cache hit
   - Bloom filter re-parsed from bytes on every check (allocating a Vec)
   - Block cache took a mutex lock + LRU list manipulation on every hit

3. **At scale, the cost is O(levels × pread-per-level)**. With 4KB blocks,
   pinned metadata, and CLOCK cache, each level costs ~10us. Reducing
   levels checked or eliminating preads per level is the path forward.

## Verification

```bash
cargo bench --bench scaling -- --tiers kv \
  --scales 100000,1000000,10000000,100000000,1000000000 \
  --kv-value-size 8 -q
```
