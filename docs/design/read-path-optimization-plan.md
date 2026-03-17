# Read Path Optimization Plan

**Status:** Active Investigation
**Date:** 2026-03-17
**Problem:** 20x read throughput degradation from 100K → 1M keys (532K/s → 24K/s)

---

## 1. Current Performance Profile

### Scaling Behavior (KV, 1KB values, standard durability)

| Scale | Read ops/s | Read p50 | Write ops/s | Scan ops/s | RSS | Disk | Space Amp |
|------:|----------:|----------|------------:|----------:|----:|-----:|----------:|
| 1K | 1,344K | 651ns | 106K | 14K | 8MB | 2MB | 1.6x |
| 10K | 994K | 852ns | 108K | 17K | 19MB | 16MB | 1.6x |
| 100K | 532K | 1.7us | 103K | 15K | 128MB | 157MB | 1.6x |
| 1M | 24K | 41.5us | 110K | 2.4K | 2.7GB | 3.3GB | 3.4x |

**Key observations:**
- **Writes scale perfectly** (103-110K/s across all scales) — the write path is not the bottleneck
- **Reads degrade 22x** from 100K → 1M — sublinear scaling is expected, but 22x for 10x more data is pathological
- **Scans degrade 6x** — less severe than reads but still concerning
- **Space amp jumps** from 1.6x to 3.4x at 1M — WAL not fully truncated, compaction overhead

### Why 100K Is Fast

At 100K keys × 1KB = ~100MB, the entire dataset fits in a single active memtable (write_buffer_size = 128MB). No rotation ever occurs. All reads are SkipMap lookups — O(log N) with excellent cache locality.

### Why 1M Is Slow

At 1M keys × 1KB = ~1GB, the data spans ~8 memtable rotations. After flush + compaction, data lives in 2-4 on-disk segments. Each read must:

1. Check active memtable (SkipMap — fast, ~700ns)
2. Check 0-4 frozen memtables (unlikely to have data — fast)
3. Check 2-4 segments **sequentially** (each: bloom → index → block read → decompress → deserialize)

---

## 2. Hot Path Analysis

### Complete Call Stack (kv_get)

```
kv_get (executor API)
  └─ executor.execute(Command::KvGet)             ~50ns  overhead
      └─ handlers::kv::kv_get()
          └─ KVStore::get_versioned()
              └─ db.transaction(branch_id, |txn| {  ~200ns  txn setup
                  └─ txn.get_versioned(&key)
                      └─ store.get_versioned(key, snapshot)
                          ├─ DashMap::get()           ~30ns   read guard
                          ├─ active.get_versioned()   ~700ns  SkipMap lookup
                          ├─ frozen[0..N]             ~0ns    (usually empty)
                          └─ segments[0..N]           ~10-40us PER SEGMENT
                              ├─ bloom check          ~100ns
                              ├─ index binary search  ~200ns
                              ├─ read_data_block()    ~5-30us (mmap + decompress)
                              └─ decode entries       ~1-5us  (bincode deserialize)
                  })
              └─ commit (read-only fast path)       ~50ns   atomic load
```

### Cost Breakdown at 1M Keys (p50 = 41.5us)

| Phase | Cost | % of Total | Notes |
|-------|------|-----------|-------|
| Transaction overhead | ~300ns | 0.7% | Pool reuse, atomic ops |
| DashMap read guard | ~30ns | 0.1% | Single shard lock |
| Active memtable lookup | ~700ns | 1.7% | SkipMap O(log N), N=128K entries in active |
| Frozen memtable lookup | ~0ns | 0% | Usually empty (flushed) |
| **Segment lookups (total)** | **~40us** | **96%** | **2-4 segments × 10-20us each** |
| Bloom filter check | ~100ns/seg | 0.5% | In-memory bitarray |
| Index binary search | ~200ns/seg | 1% | In-memory Vec |
| **Block read + decompress** | **~8-15us/seg** | **20-35%** | **mmap + zstd decode_all** |
| **Entry decode loop** | **~2-5us/seg** | **5-12%** | **bincode deserialize, scan until match** |
| Commit fast path | ~50ns | 0.1% | Atomic load |

### Dominant Bottleneck: Segment Block Reads

Each segment point lookup does:
1. `read_data_block()` — reads `FRAME_OVERHEAD + compressed_len` bytes from mmap, decompresses with zstd
2. `decode_entry()` loop — parses entries from the decompressed block until the target key is found

With 2-4 segments, this happens 2-4 times per read. At 64KB block size with zstd, each decompression produces ~64KB of data that's scanned linearly for the target key.

### Allocation Hot Spots

Per read operation (measured at 1M scale):

| Allocation | Size | Count | Total |
|-----------|------|-------|-------|
| `key.to_string()` | ~20B | 1 | 20B |
| `InternalKey::encode()` | ~50B | 1-5 | 50-250B |
| `encode_typed_key()` | ~30B | 1-5 | 30-150B |
| `zstd::decode_all()` | ~64KB | 1-4 | 64-256KB |
| `bincode::deserialize(Value)` | ~1KB | 1-N | 1-10KB |
| `data.to_vec()` (uncompressed path) | ~64KB | 0-4 | 0-256KB |

**Total per read: 65KB - 512KB of allocations**, dominated by block decompression.

---

## 3. Root Causes (Ranked by Impact)

### RC-1: No Block Cache (CRITICAL)

**Impact: ~80% of read latency at scale**

Every point lookup decompresses the target data block from scratch. At 1M keys with 2-4 segments, the same hot blocks are decompressed thousands of times. A read of key `K` decompresses a 64KB block, finds `K`, returns — then the next read of a nearby key `K+1` (same block) decompresses the same 64KB block again.

**Evidence:** At 100K keys (single memtable, no segments), reads are 532K/s. At 1M keys (2-4 segments), reads drop to 24K/s. The 22x degradation is entirely from repeated block I/O + decompression.

**Industry comparison:** RocksDB's block cache is 8MB-8GB, caching decompressed blocks with LRU eviction. This makes hot-key reads nearly as fast as in-memory. WiredTiger, SQLite, and every production LSM engine have block caches.

### RC-2: No Bloom Filter Short-Circuit Across Segments (MODERATE)

**Impact: ~10% of read latency at scale**

When a key exists in segment S2, the read path still checks segment S0 and S1 first (newest-first ordering). Each check does a bloom filter test (cheap: ~100ns) followed by a block read (expensive: ~10us) on bloom false positives. With 10 bits/key bloom filters, the false positive rate is ~1%. At 4 segments, the expected number of false-positive block reads per lookup is 0.03 — negligible.

However, bloom filter checks still require `encode_typed_key()` allocation per segment. With 4 segments, that's 4 Vec allocations per read.

**Mitigation priority:** Low. Bloom FPR is already good. The allocation overhead can be fixed by encoding once and reusing.

### RC-3: Sequential Segment Scanning (MODERATE)

**Impact: ~5% of read latency, but limits future scaling**

The read path iterates `branch.segments` linearly: `for seg in &branch.segments`. With tiered compaction, the segment count is bounded (typically 3-8 segments across tiers). At 1M this is manageable, but at 100M+ keys with many tiers, segment count could reach 15-20.

**Industry comparison:** RocksDB uses leveled compaction where L1+ levels have non-overlapping key ranges, allowing binary search to a single file per level. Our tiered compaction produces overlapping segments within a tier.

### RC-4: Per-Read Allocation Overhead (LOW)

**Impact: ~2% of read latency**

`InternalKey::encode()` and `encode_typed_key()` allocate Vec<u8> on every call. These are called per-memtable and per-segment check. A single read at 1M scale triggers 5-10 small heap allocations.

**Mitigation:** Pre-encode the key once and reuse across memtable/segment checks.

### RC-5: Transaction Overhead for Pure Reads (LOW)

**Impact: ~1% of read latency**

Every `kv_get` wraps in a full `db.transaction()` closure — allocating a TransactionContext (from pool), recording txn_id, calling commit. The read-only fast path in the commit layer is well-optimized (just an atomic load), but the transaction setup/teardown is unnecessary overhead for pure point reads.

**Mitigation:** Add a non-transactional read path (`db.get_value_direct()` already exists but isn't exposed through the executor API).

### RC-6: Space Amplification (MODERATE — affects disk I/O)

**Impact: Indirect — larger working set → more page cache pressure**

At 1M keys: 3.4x space amp = 3.3GB on disk for ~1GB of data. Breakdown:
- WAL: ~1GB (not fully truncated — `_system_` branch excluded from watermark, but WAL segments may still accumulate)
- Segments: ~1GB (after compaction)
- Overhead: ~1.3GB (compaction intermediates, segment format overhead, tombstones)

Higher space amp means the working set doesn't fit in the OS page cache as well, increasing mmap page fault rates.

---

## 4. Optimization Roadmap

### Phase 1: Block Cache (Expected: 5-10x read improvement)

**Goal:** Cache decompressed data blocks to eliminate repeated decompression.

**Design:**
```rust
/// Shared block cache across all segments.
///
/// Key: (segment_file_path_hash, block_offset) → decompressed block bytes
/// Eviction: LRU with configurable capacity (default: 64MB)
struct BlockCache {
    cache: parking_lot::Mutex<LruCache<(u64, u64), Arc<Vec<u8>>>>,
    capacity_bytes: usize,
    current_bytes: AtomicUsize,
}
```

**Integration point:** `KVSegment::read_data_block()` — check cache before mmap + decompress. On miss, decompress and insert.

**Expected impact:** Hot keys (Zipf distribution, typical workload) hit the cache >90% of the time. Each cache hit costs ~50ns (mutex + HashMap lookup) vs ~10us (mmap + decompress). For 1M keys with 4 segments: 10us × 4 → 50ns × 4 = 200ns. Total read latency: ~1-2us instead of ~40us.

**Risks:**
- Cache contention under high concurrency (mitigate with sharded cache)
- Memory accounting (need to track cache size accurately)

### Phase 2: Key Encoding Reuse (Expected: 10-20% read improvement)

**Goal:** Encode the typed key and InternalKey once per read, reuse across all memtable/segment checks.

**Design:**
```rust
fn get_versioned_from_branch(branch: &BranchState, key: &Key, max_version: u64) -> ... {
    // Encode once
    let typed_key = encode_typed_key(key);
    let seek_ik = InternalKey::encode(key, u64::MAX);

    // Reuse across all sources
    if let Some(r) = active.get_versioned_with_encoded(&seek_ik, &typed_key, max_version) { ... }
    for frozen in &branch.frozen { ... }
    for seg in &branch.segments {
        seg.point_lookup_with_encoded(&typed_key, &seek_ik, max_version)
    }
}
```

**Integration point:** Add `_with_encoded` variants to memtable and segment lookups.

### Phase 3: Non-Transactional Read Path (Expected: 5-10% read improvement)

**Goal:** Bypass transaction overhead for pure point reads.

**Design:** Expose `Database::get_value_direct()` through the executor for `KvGet` commands that don't need transactional semantics.

**Integration point:** `handlers::kv::kv_get()` — call `p.kv.get_direct()` instead of `p.kv.get_versioned()` when `as_of` is None.

### Phase 4: Leveled Compaction for L1+ (Expected: 2-3x read improvement at 10M+)

**Goal:** Non-overlapping key ranges in compacted segments, allowing binary search to a single segment per level.

**Design:** After tier-0 compaction, promote output to L1 with non-overlapping ranges. L1+ segments are partitioned by key range. Point lookups can skip to the correct segment in O(log S) instead of checking all S segments.

**Complexity:** High. Requires manifest tracking of level assignments and key range boundaries.

### Phase 5: Prefix Bloom Filters (Expected: further read improvement at 100M+)

**Goal:** Eliminate block reads for keys definitely not in a segment's key range.

**Design:** Add a prefix bloom filter covering the first N bytes of each key. This allows skipping entire segments when the key prefix doesn't match.

---

## 5. Priority Matrix

| Phase | Impact | Effort | Risk | Priority |
|-------|--------|--------|------|----------|
| **1. Block Cache** | 5-10x reads | Medium (200 LOC) | Low | **P0 — Do First** |
| **2. Key Encoding Reuse** | 10-20% reads | Low (50 LOC) | Very Low | **P1** |
| **3. Non-Txn Reads** | 5-10% reads | Low (30 LOC) | Low | **P1** |
| **4. Leveled Compaction** | 2-3x at 10M+ | High (500 LOC) | Medium | **P2** |
| **5. Prefix Blooms** | Marginal | Medium | Low | **P3** |

### Expected Outcome After Phase 1-3

With block cache + encoding reuse + non-transactional reads:

| Scale | Current | Expected | Improvement |
|------:|--------:|---------:|------------:|
| 100K | 532K/s | 550K/s | ~1x (already fast, memtable-only) |
| 1M | 24K/s | 200-400K/s | 8-16x |
| 10M | ??? | 100-200K/s | TBD |

---

## 6. Space Amplification Analysis

### Current Breakdown at 1M (3.4x)

| Component | Size | Notes |
|-----------|------|-------|
| WAL | ~1.5GB | Not fully truncated (see below) |
| Segments (live) | ~1GB | Compacted data |
| Segment overhead | ~0.3GB | Headers, bloom filters, index blocks, CRC |
| Compaction waste | ~0.5GB | Orphaned data from overlapping ranges |
| **Total** | **~3.3GB** | |
| **Raw data** | **~1GB** | |
| **Space amp** | **3.4x** | |

### WAL Truncation Gap

The watermark excludes branches without segments (like `_system_`). User branch segments advance the watermark, but the watermark only covers data up to the MIN of max_flushed_commit across all segment-bearing branches. With a single user branch, the watermark equals that branch's max_flushed_commit.

However, the WAL writer uses segmented files. Even with a correct watermark, the active WAL segment (being written to) can't be truncated. And the segment containing records around the watermark boundary survives. This is ~2 WAL segments of overhead minimum.

### Target: <2x Space Amplification

Achievable with:
1. Aggressive WAL truncation (flush all branches before computing watermark — already reverted due to perf impact, needs revisiting with targeted approach)
2. Compaction that eliminates overlapping ranges (leveled compaction Phase 4)
3. Smaller WAL segments (reduces the "active segment can't be truncated" overhead)

---

## 7. Benchmark Methodology

All measurements use the `strata-benchmarks` repo's scaling benchmark:
```bash
cargo bench --bench scaling -- --quick --tiers kv --scales 1000,10000,100000,1000000
```

- **Hardware:** AMD Ryzen 7 7800X3D (8 cores, 16 threads), 61GB RAM, NVMe SSD
- **Durability:** Standard (periodic fsync)
- **Value size:** 1024 bytes
- **Sample ops:** 1000 per operation type
- **Each scale level:** fresh database, load all keys, then measure read/write/scan

### Baseline Commits

| Commit | Description |
|--------|-------------|
| `8b25359` | Pre-tiered-storage (ShardedStore) |
| `9fb8eeb` | SegmentedStore (Epics 1-8) |
| `3ffbf9f` | Full pipeline (Epics 10-18 + fixes) |

---

## 8. Open Questions

1. **Block cache sizing:** What's the optimal default? RocksDB uses 8MB, WiredTiger uses 50% of RAM. For Strata, 64MB seems reasonable as a default with configurability.

2. **Cache sharing:** Should the block cache be per-segment, per-branch, or global? Global is simplest and allows cross-branch cache sharing.

3. **Compressed vs uncompressed cache:** Should we cache compressed blocks (smaller, requires decompression on hit) or decompressed (larger, zero-cost hit)? RocksDB caches uncompressed by default. Decompressed is the right choice for Strata — we optimize for read latency.

4. **mmap vs pread:** Currently segments use mmap. With a block cache, we could switch to pread for data blocks (only mmap index/bloom). This gives more control over I/O and avoids TLB pressure from large mmap regions.

5. **Read-ahead:** Should we prefetch adjacent blocks when reading a block? Useful for scans, harmful for random point reads. Could be adaptive based on access pattern.
