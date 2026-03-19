# Storage Engine Scale-Up: Epics 32-37 — Remaining Read Path Optimizations

## Context

Epics 19-31 built the core storage engine to near-RocksDB parity on architecture:

| Epic | What | Result |
|------|------|--------|
| 19 | O(1) sharded block cache | Eliminated O(N) eviction |
| 20 | Replace mmap with pread | Eliminated page fault overhead |
| 21 | Version-based segment tracking (ArcSwap) | Lock-free reads during compaction |
| 22 | L0→L1 leveled compaction + manifest | O(log N) point lookups via binary search |
| 23 | Streaming compaction + multi-segment output | O(block) compaction memory |
| 24 | Multi-level L0-L6 with leveled merge | Data tiering across 7 levels |
| 25 | Compaction scheduling + engine integration | Cascading L1-L5 triggers |
| 26 | Dynamic level sizing | Adaptive level targets, reduced space amp |
| 27 | Prefix compression (v4) | 30-50% smaller blocks |
| 28 | L0 bloom pinning + partitioned bloom | Cache-friendly bloom, metadata stays resident |
| 29 | Index key shortening | 3-6x smaller index blocks |
| 30 | xxHash bloom + Monkey-optimal level allocation | Lower FPR, optimal bits/key per level |
| 31 | Data block hash index (v6) | O(1) restart interval lookup |

### Remaining Gaps

Six optimizations remain between current Strata and full RocksDB read-path parity at 1B keys. These are ordered by impact-to-effort ratio.

| # | Gap | Impact | Effort |
|---|-----|--------|--------|
| 32 | Uniform Zstd everywhere | Hot data pays unnecessary decompression | ~2 days |
| 33 | Static level sizing at boundaries | Waste at small scale, under-provision at large | ~3 days |
| 34 | No compaction I/O throttling | Compaction starves user reads under load | ~3 days |
| 35 | Full index loaded at open time | At 1B keys, index blocks are 1-10MB in memory | ~5 days |
| 36 | Standard bit-array bloom | No cache-line locality, no SIMD acceleration | ~5 days |
| 37 | No batched point lookup API | Each lookup is independent — can't amortize I/O | ~5 days |

---

## Epic 32: Per-Level Compression Strategy (~2 days)

**Goal:** Use fast compression for hot levels (L0-L2), dense compression for cold levels (L3+). Reduce CPU on the read hot path by ~30% for cache-miss reads at upper levels.

### Problem

All data blocks are compressed with Zstd level 3 regardless of level (`segment_builder.rs:1192`). At 1B keys:

- L0-L2 data is read frequently (hot). Zstd level 3 decompression costs ~1-2μs per 64KB block.
- L5-L6 data is read rarely (cold). The same Zstd level 3 could be Zstd level 6+ for 10-20% better compression with negligible read cost.
- RocksDB default: no compression or LZ4 for L0-L2, Zstd for L3+, Zstd level 6-8 for bottommost.

### Design

Add a `compression_codec` parameter to `SegmentBuilder` and select based on target level:

```
L0-L2:  No compression (codec byte = 0x00)
L3-L5:  Zstd level 3 (current default, codec byte = 0x01)
L6:     Zstd level 6 (codec byte = 0x01, higher level)
```

The block frame already has a codec byte that distinguishes compressed vs uncompressed. The reader already handles both (`parse_framed_block` checks codec). The only change is:

1. `SegmentBuilder::with_compression(codec: CompressionCodec)` — new builder option.
2. `compact_level()` passes `CompressionCodec::None` for L0-L2 outputs, `Zstd(3)` for L3-L5, `Zstd(6)` for L6.
3. `write_framed_block_compressed()` respects the codec setting.

### Files to Modify

| File | Changes |
|------|---------|
| `segment_builder.rs` | Add `CompressionCodec` enum, `with_compression()` builder method, pass to `write_framed_block_compressed` |
| `segmented/compaction.rs` | Select compression codec based on target level |

### Test Plan

- Build segments with each codec, verify round-trip read.
- Verify L0 compaction output uses no compression, L3+ uses Zstd.
- Benchmark: decompression cost per block for each codec.

---

## Epic 33: Dynamic Level Sizing at Boundaries (~3 days)

**Goal:** Adapt level target sizes to actual data volume using RocksDB's `CalculateBaseBytes` algorithm. Reduce space amplification from ~5x to ~1.1x at steady state.

### Problem

Current static targets: L1 = 256MB, each level 10x the previous (`segmented/mod.rs:46-50`). Issues:

- **Small datasets:** 10MB of data still creates L1 target of 256MB, causing premature compaction.
- **Large datasets:** At 1TB, level targets may not match actual data distribution — empty intermediate levels waste compaction work.
- **After bulk loads:** The level tree may be imbalanced, with most data in L0/L1.

### Design (RocksDB `CalculateBaseBytes`)

After every compaction completion, recompute level targets working backward from the largest level:

```
1. actual_bottom = size of largest non-empty level
2. base = actual_bottom / multiplier^(bottom_level - 1)
3. base = clamp(base, MIN_BASE_BYTES, MAX_BASE_BYTES)
4. target[i] = base * multiplier^(i-1) for i in 1..=max_level
```

This means:
- If total data is 10MB, L1 target shrinks proportionally (no wasted compaction).
- If total data is 1TB, L1 target grows to match (proper tiering).
- Empty intermediate levels are expected — data can skip directly to the base level.

### Files to Modify

| File | Changes |
|------|---------|
| `segmented/compaction.rs` | `calculate_base_bytes()` function, call after each compaction |
| `segmented/mod.rs` | Store per-level target sizes in `BranchState`, use in `compaction_score()` |

### Test Plan

- Unit test: `calculate_base_bytes` with various data distributions.
- Integration: bulk-load 1M keys, verify level targets adapt, space amp < 2x.
- Verify empty intermediate levels are handled correctly.

---

## Epic 34: Compaction Rate Limiter (~3 days)

**Goal:** Throttle compaction I/O to prevent it from starving user reads. Target: compaction uses at most 100MB/s of I/O bandwidth by default.

### Problem

Compaction reads and writes large amounts of data (merge-sorting entire levels). At 1TB:

- A single L4→L5 compaction can read/write 10GB+ of data.
- Without throttling, compaction saturates NVMe bandwidth (~3GB/s).
- User reads see 10-100x latency spikes during active compaction.
- RocksDB solves this with a token-based `RateLimiter` (default 100MB/s for compaction, unlimited for user I/O).

### Design

Token-bucket rate limiter integrated into the compaction path:

```rust
pub struct RateLimiter {
    tokens: AtomicI64,          // available bytes
    rate_bytes_per_sec: u64,    // refill rate
    last_refill: AtomicU64,     // timestamp of last refill
}

impl RateLimiter {
    /// Block until `bytes` tokens are available.
    fn acquire(&self, bytes: usize);
}
```

Integration points:
1. `OwnedSegmentIter::next()` — calls `limiter.acquire(block_size)` on each block read during compaction.
2. `SegmentBuilder::write_framed_block_compressed()` — calls `limiter.acquire(framed_size)` on each block write during compaction.
3. User reads (`point_lookup`, `iter_seek`) are NOT throttled.

The limiter is passed as `Option<&RateLimiter>` — `None` for user I/O, `Some(limiter)` for compaction.

### Files to Modify

| File | Changes |
|------|---------|
| `rate_limiter.rs` (new) | `RateLimiter` struct with token-bucket implementation |
| `segmented/compaction.rs` | Pass limiter to compaction iterators and builders |
| `segment_builder.rs` | Accept optional limiter for write throttling |

### Test Plan

- Unit test: rate limiter delivers correct throughput over 1-second window.
- Integration: run compaction with 10MB/s limit, verify wall-clock time matches expected.
- Verify user reads are unaffected (no limiter on read path).

---

## Epic 35: Partitioned Index Blocks (~5 days)

**Goal:** Replace monolithic index block with partitioned sub-indexes loaded on demand through the block cache. Reduce memory footprint at 1B keys from ~10MB per segment to ~64KB resident.

### Problem

Currently, `KVSegment::open()` loads the entire index block into memory (`segment.rs:154-165`). The index maps block boundaries → file offsets. At 1B keys:

- A segment with 100K data blocks has an index block of ~1-3MB (each entry: shortened key + 8-byte offset + 4-byte length).
- With 100+ segments across levels, total index memory can reach 100-300MB.
- RocksDB solves this with a two-level index: a small top-level "index of indexes" kept in memory, with individual index partitions loaded on demand through the block cache.

### Design

**Write path (segment_builder.rs):**

1. After every N data blocks (e.g., 128), emit a sub-index block containing the last N index entries.
2. Build a top-level index that maps `max_key → sub_index_block_offset`.
3. Write: `[data blocks] [sub-index blocks] [top-level index block] [bloom] [filter index] [props] [footer]`.
4. Footer stores the top-level index offset instead of the flat index offset.

**Read path (segment.rs):**

1. `KVSegment::open()` loads only the top-level index (~1-2KB for 100 partitions).
2. `point_lookup` binary-searches the top-level index → identifies the sub-index partition.
3. Sub-index partition is loaded from block cache (pread on miss, cached with `Priority::High`).
4. Binary search within the sub-index → data block offset.

**Backward compatibility:**
- Detect via a new field in footer or properties block: `index_type = 0 (flat) | 1 (partitioned)`.
- Old segments with flat index continue to work unchanged.

### Files to Modify

| File | Changes |
|------|---------|
| `segment_builder.rs` | Emit sub-index blocks, build top-level index, new footer field |
| `segment.rs` | Two-level index lookup, sub-index cache loading |
| `segment_builder.rs` (footer) | Add `index_type` field to footer or properties |

### Test Plan

- Round-trip: build partitioned-index segment, verify all lookups work.
- Memory: open segment, verify only top-level index in memory (not full index).
- Cache: verify sub-index blocks cached with HIGH priority.
- Backward compat: old flat-index segments still readable.
- Iterator: `iter_seek` works across partitioned-index segments.

---

## Epic 36: Cache-Local Blocked Bloom Filter (~5 days)

**Goal:** Replace standard bit-array bloom with RocksDB's `FastLocalBloom` design — all probes for a key stay within one 512-bit (64-byte) cache line. Reduces bloom check latency from ~200ns to ~50ns at 1B scale.

### Problem

Current bloom filter (`bloom.rs`) uses a standard bit array with xxh3 double-hashing. Each probe accesses a random position in the bit array. At 1B keys:

- A single bloom partition covers 16 blocks × ~200 keys = ~3200 keys.
- At 10 bits/key, the bit array is 32Kbit = 4KB — spans 64 cache lines.
- Each of 7 probes hits a random cache line → 7 L1 cache misses per bloom check.
- RocksDB's `FastLocalBloom` confines all probes to a single 64-byte cache line, reducing cache misses from 7 to 1.

### Design (from RocksDB `util/bloom_impl.h`)

**Structure:**
```
[block_0: 512 bits] [block_1: 512 bits] ... [block_N: 512 bits]
```

**Probe algorithm:**
```rust
fn may_contain(filter: &[u8], key: &[u8]) -> bool {
    let h = xxh3_64(key);
    let block_idx = (h >> 32) as usize % num_blocks;
    let block = &filter[block_idx * 64..(block_idx + 1) * 64];
    // All probes within this 64-byte block
    let mut h2 = h as u32;
    for _ in 0..num_probes {
        let bit = h2 % 512;
        if block[bit / 8] & (1 << (bit % 8)) == 0 {
            return false;
        }
        h2 = h2.wrapping_mul(0x9e3779b9).wrapping_add(1); // golden ratio mixing
    }
    true
}
```

**Key properties:**
- All probes for a key touch exactly one 64-byte cache line (L1 cache = 64 bytes on x86/ARM).
- Probe count: `num_probes = ceil(ln(2) * bits_per_key)` — same as standard bloom.
- FPR is ~15% worse than a perfect bloom at the same bits/key, but the cache locality more than compensates at scale.
- Optional: AVX2 SIMD path computes all probes in parallel (future optimization).

**Backward compatibility:**
- New bloom format gets a version byte in the serialized bloom data (currently `bloom.rs` has no version field).
- Old bloom partitions (version 0 or absent) use the existing probe algorithm.
- New bloom partitions (version 1) use cache-local probes.
- Detection: first byte of bloom data = version tag.

### Files to Modify

| File | Changes |
|------|---------|
| `bloom.rs` | `FastLocalBloom` struct with cache-line-blocked probes, version-tagged serialization |
| `segment_builder.rs` | Use `FastLocalBloom::build()` instead of `BloomFilter::build()` |
| `segment.rs` | Detect bloom version, dispatch to correct probe algorithm |

### Test Plan

- FPR test: build filter with 10 bits/key, verify FPR < 1.5% (slightly higher than standard).
- No false negatives: every inserted key must be found.
- Cache-line locality: verify all probes for a key touch the same 64-byte block.
- Backward compat: old bloom partitions (version 0) still work.
- Benchmark: compare probe latency vs standard bloom at 10K, 100K, 1M keys.

---

## Epic 37: MultiGet Batched Point Lookups (~5 days)

**Goal:** Add a `multi_get(keys: &[Key]) -> Vec<Option<Value>>` API that batches lookups to amortize bloom checks, block cache lookups, and I/O. Target: 3-4x throughput improvement for batch sizes of 16-64 keys.

### Problem

Each `point_lookup` is independent:
1. Bloom check → pread bloom partition → decompress → probe.
2. Index search → identify block.
3. Block read → pread data block → decompress → scan.

When multiple keys map to the same bloom partition or data block, each lookup repeats the I/O and decompression independently. At 1B keys:

- 64 random lookups might hit 30-40 distinct data blocks but only 5-10 distinct bloom partitions.
- Without batching: 64 bloom checks + 64 block reads = 128 operations.
- With batching: 10 bloom checks + 40 block reads = 50 operations (~2.5x fewer).

RocksDB's `MultiGet` (`db_impl.cc`) batches lookups per-SST and per-block, issuing one pread per unique block.

### Design

```rust
impl SegmentedStore {
    pub fn multi_get(&self, keys: &[Key], max_version: u64) -> Vec<Option<VersionedValue>> {
        // 1. Check memtables for all keys (cheap, in-memory)
        // 2. Group remaining keys by L0 segment, then by L1+ level
        // 3. For each segment:
        //    a. Batch bloom check: group keys by bloom partition
        //    b. For keys that pass bloom: group by data block
        //    c. Read each unique data block once, scan for all matching keys
        // 4. Merge results
    }
}
```

**Key optimizations:**
- **Bloom batching:** Multiple keys may map to the same bloom partition. Load partition once, check all keys.
- **Block batching:** Multiple keys may map to the same data block. Read and decompress once, scan for all keys.
- **Short-circuit:** Once a key is found in an upper level, skip lower levels for that key.

**API:**
```rust
// Public API
fn multi_get(&self, keys: &[Key], max_version: u64) -> Vec<Option<VersionedValue>>;

// Segment-level batched lookup
fn multi_point_lookup(&self, keys: &[&Key], max_version: u64) -> Vec<Option<SegmentEntry>>;
```

### Files to Modify

| File | Changes |
|------|---------|
| `segmented/mod.rs` | `multi_get()` method with per-level batching logic |
| `segment.rs` | `multi_point_lookup()` with bloom and block batching |

### Test Plan

- Correctness: `multi_get` returns same results as individual `point_lookup` for all keys.
- Batching: verify fewer pread calls than individual lookups (instrument block cache misses).
- Mixed results: some keys found, some not found, some tombstones.
- MVCC: correct version returned per snapshot for each key in batch.
- Benchmark: compare throughput of `multi_get(64 keys)` vs 64× `point_lookup`.

---

## Dependency Graph

```
Epic 32 (per-level compression) ── standalone, no deps
Epic 33 (dynamic level sizing)  ── standalone, no deps
Epic 34 (rate limiter)          ── standalone, no deps
Epic 35 (partitioned index)     ── depends on block cache (done)
Epic 36 (cache-local bloom)     ── depends on partitioned bloom (done)
Epic 37 (MultiGet)              ── best after Epics 35+36 (maximizes batch benefit)
```

**Recommended order:** 32 → 33 → 34 → 35 → 36 → 37

Epics 32-34 are quick wins (2-3 days each, no dependencies). Epics 35-37 are larger structural changes best done sequentially.

## Expected Performance Impact

| Optimization | Cache-hot reads | Cache-miss reads | Write path |
|---|---|---|---|
| Per-level compression | +10-30% (less decompression) | +5% | neutral |
| Dynamic level sizing | +10-20% (less space amp → more fits in cache) | +10% | +20% (less write amp) |
| Rate limiter | +50% p99 (less compaction interference) | +50% p99 | -10% compaction throughput |
| Partitioned index | neutral | +5% (less memory pressure) | neutral |
| Cache-local bloom | +20-40% (fewer cache misses per check) | +20-40% | neutral |
| MultiGet | +200-300% (batch amortization) | +100-200% | N/A |

**Cumulative at 1B keys (cache-miss workload):** Current ~3K reads/s → target 15-30K reads/s with Epics 32-36, plus 50-100K reads/s with MultiGet batching.
