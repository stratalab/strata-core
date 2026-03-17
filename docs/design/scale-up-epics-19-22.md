# Storage Engine Scale-Up: Epics 19-22 for 1B Keys

## Context

Epics 10-18 built the flush pipeline, compaction, compression, WAL truncation, and robustness. The engine works correctly but reads collapse beyond 1M keys:

| Scale | Reads/s | Bottleneck |
|------:|--------:|-----------|
| 1M | 319K | Data fits in memtable — fast |
| 10M | 2.3K | mmap page faults (100us each) + all segments checked |
| 1B | Target: 50-100K | Requires architectural changes |

**Root causes** identified from reading LevelDB/RocksDB source code:
1. `memmap2::Mmap` delegates caching to OS — page faults, double-caching, no priority control
2. Size-tiered compaction produces overlapping segments — reads check ALL segments

**Issues:** #1517 (block cache), #1520 (version tracking), #1523 (pread I/O), #1519 (leveled compaction)

**Reference docs:**
- `docs/design/reference-implementation-audit.md`
- `docs/design/billion-scale-roadmap.md`
- `docs/design/leveldb-read-path-reference.md`
- `docs/design/read-path-optimization-plan.md`

---

## Epic 19: O(1) Sharded Block Cache (#1517 Phase 1)

**Goal:** Replace O(N) HashMap eviction with sharded doubly-linked-list LRU. Auto-size to available RAM.

**Why first:** Epic 20 (pread) depends on a performant cache. Current cache gets SLOWER as it grows.

### Changes

**1. `ShardedLruCache` in `crates/storage/src/block_cache.rs`** (~300 lines, full rewrite of internals)
- 16 shards, each with `parking_lot::Mutex`
- Per-shard: `HashMap<CacheKey, NonNull<LruNode>>` + doubly-linked list
- `LruNode { key, data: Arc<Vec<u8>>, size, prev, next }`
- Insert: O(1) — prepend to list head, evict from tail if over capacity
- Lookup: O(1) — hash lookup + move-to-head
- Evict: O(1) — unlink tail node
- Two priority tiers: HIGH (index/bloom, evicted last) and LOW (data blocks)
- `invalidate_file(file_id)`: walk shard, remove matching entries

**2. Auto-sizing**
- `crates/engine/src/database/config.rs` — add `block_cache_size: usize` (default 0 = auto)
- Auto: read `/proc/meminfo` or `sysinfo` → `max(256MB, available_ram / 4)`
- `crates/storage/src/block_cache.rs` — `set_global_capacity()` called from `open_finish()`

**3. Public API unchanged**
- `global_cache()` still returns `&'static BlockCache`
- `get(file_id, block_offset)` and `insert(file_id, block_offset, data)` signatures unchanged
- New: `insert_with_priority(file_id, block_offset, data, Priority::High)`

### Verification
- Unit test: O(1) eviction timing (insert 100K entries, verify no regression)
- Unit test: HIGH priority blocks survive when LOW blocks are evicted
- Unit test: concurrent access across 16 threads, no deadlocks
- Benchmark: 1M reads with cache sized to working set → verify improvement

### Effort: 1 week

---

## Epic 20: Replace mmap with pread (#1523)

**Goal:** KVSegment uses `pread()` + block cache instead of `Mmap`. Eliminates page fault overhead and double-caching.

**Why:** Root cause of 100us per cold read. RocksDB, WiredTiger, TigerBeetle all use pread.

### Changes

**1. `KVSegment` struct change in `crates/storage/src/segment.rs`** (~100 lines)
```rust
pub struct KVSegment {
    file: std::fs::File,    // was: mmap: Mmap
    header: KVHeader,
    footer: Footer,
    index: Vec<IndexEntry>,  // loaded at open via pread, pinned in cache
    bloom: BloomFilter,      // loaded at open via pread, pinned in cache
    props: PropertiesBlock,
    file_path: std::path::PathBuf,
    file_id: u64,
    file_size: u64,          // NEW: track size without mmap
}
```

**2. `read_data_block` uses pread** (~30 lines)
```rust
fn read_data_block(&self, ie: &IndexEntry) -> Option<Arc<Vec<u8>>> {
    let cache = block_cache::global_cache();
    if let Some(cached) = cache.get(self.file_id, ie.block_offset) {
        return Some(cached);
    }
    // pread from file
    let mut buf = vec![0u8; FRAME_OVERHEAD + ie.block_data_len as usize];
    self.file.read_exact_at(&mut buf, ie.block_offset)?;  // Unix pread
    let (_, data) = parse_framed_block(&buf)?;
    let decompressed = match buf[1] {
        0 => data.to_vec(),
        1 => zstd::decode_all(data).ok()?,
        _ => return None,
    };
    Some(cache.insert(self.file_id, ie.block_offset, decompressed))
}
```

**3. `open()` loads index + bloom via pread** (~40 lines)
- Read footer (last 56 bytes) via pread
- Read index block via pread → parse into `Vec<IndexEntry>`
- Read bloom block via pread → parse into `BloomFilter`
- Insert raw index/bloom into cache with HIGH priority

**4. `SegmentIter` unchanged**
- Already stores `block_data: Option<Arc<Vec<u8>>>` — works with pread or mmap

**5. Remove `memmap2` dependency**
- `crates/storage/Cargo.toml` — remove `memmap2`

### Migration
- No on-disk format change — same .sst files
- All existing tests work unchanged (different read method, same data)

### Verification
- All existing segment tests pass
- Benchmark: 10M reads → verify 20-50K/s (up from 2.3K)
- Test: concurrent reads from same file → no fd contention
- Test: cache stats show hits increasing over repeated reads

### Effort: 1-2 weeks

---

## Epic 21: Version-Based Segment Tracking (#1520)

**Goal:** Immutable `SegmentVersion` snapshots replace mutable segment lists. Readers hold Arc refs. Compaction atomically swaps versions.

**Why:** Leveled compaction (Epic 22) modifies L0 + L1 simultaneously. Without version tracking, readers see partial updates.

### Changes

**1. `SegmentVersion` struct in `crates/storage/src/segmented.rs`** (~40 lines)
```rust
struct SegmentVersion {
    l0_segments: Vec<Arc<KVSegment>>,  // overlapping, newest first
    l1_segments: Vec<Arc<KVSegment>>,  // non-overlapping, sorted by key range
}
```

**2. `BranchState` uses `ArcSwap<SegmentVersion>`** (~20 lines)
```rust
struct BranchState {
    active: Memtable,
    frozen: Vec<Arc<Memtable>>,
    version: arc_swap::ArcSwap<SegmentVersion>,  // atomic swap
    min_timestamp: AtomicU64,
    max_timestamp: AtomicU64,
}
```
- `arc_swap` crate: lock-free atomic `Arc` swap. Readers get `Guard` (no allocation). Writers swap with `store()`.

**3. Read path snapshots version** (~15 lines change in `get_versioned_from_branch`)
```rust
fn get_versioned_from_branch(branch: &BranchState, key, max_version) {
    // Snapshot — immutable for this read
    let version = branch.version.load();
    // 1. Active memtable
    // 2. Frozen memtables
    // 3. L0 segments (from version)
    for seg in &version.l0_segments { ... }
    // 4. L1 segments (from version) — Epic 22 adds binary search
    for seg in &version.l1_segments { ... }
}
```

**4. Flush/compact create new version** (~30 lines each)
```rust
// In flush_oldest_frozen:
let old = branch.version.load();
let mut new_l0 = old.l0_segments.clone();
new_l0.insert(0, Arc::new(new_segment));
branch.version.store(Arc::new(SegmentVersion {
    l0_segments: new_l0,
    l1_segments: old.l1_segments.clone(),
}));
```

**5. MANIFEST VersionEdit** (~100 lines in `crates/durability/src/format/manifest.rs`)
- New record type: `VersionEdit { branch_id, added: [(level, path, key_range)], removed: [path] }`
- Append to MANIFEST after each compaction
- Recovery: replay edits to rebuild `SegmentVersion` per branch
- Fallback: if no edits exist (old database), load from directory scan into L0

**6. Update all segment-accessing methods** (~80 lines)
- `branch_segment_count`, `max_flushed_commit`, `segment_file_sizes`, `list_branch_inner`, `recover_segments`, `compact_branch`, `compact_tier` — all read from `version` instead of `branch.segments`

### New dependency
- `crates/storage/Cargo.toml` — add `arc-swap = "1"`

### Verification
- Test: concurrent read during compaction → reader sees consistent version
- Test: crash recovery → MANIFEST edits replayed → correct segment versions
- Test: old database without edits → falls back to directory scan
- Property test (proptest): random interleaving of reads + flush + compact → no data loss

### Effort: 1-2 weeks

---

## Epic 22: Leveled Compaction (#1519)

**Goal:** L0 segments (overlapping) compact into L1 (non-overlapping). Point lookups binary-search L1 → check 1 segment instead of all.

**Why:** Without non-overlapping levels, every read checks every segment. This is THE scaling bottleneck.

### Changes

**1. Read path: L1 binary search** (~20 lines in `get_versioned_from_branch`)
```rust
// After checking L0 segments:
if !version.l1_segments.is_empty() {
    let typed_key = encode_typed_key(key);
    let idx = version.l1_segments.partition_point(|seg| {
        seg.key_range().1 < typed_key.as_slice()
    });
    if idx < version.l1_segments.len() {
        if let Some(se) = version.l1_segments[idx].point_lookup(key, max_version) {
            return Some((se.commit_id, segment_entry_to_memtable_entry(se)));
        }
    }
}
```

**2. `compact_l0_to_l1` method** (~100 lines in `segmented.rs`)
- Trigger: `l0_segment_count >= 4`
- Merge: all L0 + overlapping L1 → new L1 segments (split at `target_file_size = 256MB`)
- Install: new `SegmentVersion` with empty L0, updated L1
- Delete old segment files, invalidate block cache entries

**3. `key_range()` on KVSegment** (already exists — `props.key_min`, `props.key_max`)

**4. Recovery: classify segments by level**
- MANIFEST VersionEdit (from Epic 21) records level per segment
- On recovery: replay edits → segments in correct level
- No edits (old database): all to L0, first compaction promotes

**5. Engine flush callback** (~10 lines in `database/mod.rs`)
```rust
// Replace tiered compaction:
if storage.l0_segment_count(&branch_id) >= 4 {
    storage.compact_l0_to_l1(&branch_id, 0)?;
}
```

**6. Maintain L1 sorted invariant**
- After L0→L1 compaction, verify `l1_segments` sorted by key_range
- If multiple output segments: ensure non-overlapping (split at key boundaries)

### Files touched
- `crates/storage/src/segmented.rs` (~250 lines)
- `crates/engine/src/database/mod.rs` (~15 lines)
- `crates/storage/src/segment.rs` (~5 lines — key_range() already exists)

### Verification
- Test: flush 8 segments → L0→L1 compaction → 2 L1 segments, 0 L0
- Test: L1 segments have non-overlapping key ranges
- Test: point lookup finds key via L1 binary search (not linear scan)
- Test: recovery loads segments into correct levels
- Benchmark: 10M keys → 50-100K reads/s
- Property test: random writes + compactions → all data recoverable

### Effort: 2 weeks

---

## Dependency Graph & Execution

```
Epic 19 (sharded cache) ──→ Epic 20 (pread I/O)
                                     ↓
Epic 21 (version tracking) ──→ Epic 22 (leveled compaction)
```

**Sprint 1** (week 1-2): Epic 19 + Epic 21 in parallel
**Sprint 2** (week 3-4): Epic 20 + Epic 22 in parallel
**Sprint 3** (week 5-6): Integration testing, benchmarking, bug fixes

### Expected Results

| Scale | Before | After Sprint 1 | After Sprint 2 |
|------:|-------:|---------------:|---------------:|
| 1M | 319K/s | 320K/s | 350K/s |
| 10M | 2.3K/s | 10K/s (cache helps) | 80-150K/s |
| 100M | N/A | N/A | 50-100K/s |

---

## What Comes After (Not in This Plan)

From `docs/design/billion-scale-roadmap.md`:
- Multi-level L2+ (1B scale)
- Dynamic level sizing (CalculateBaseBytes)
- FileIndexer (O(log N) file lookup)
- Restart points (#1518)
- Per-level compression
- Partitioned index/bloom
- Rate limiter, streaming builder
- Write batch coalescing (#1521), soft throttle (#1522)
- BlobDB, parallel subcompactions
