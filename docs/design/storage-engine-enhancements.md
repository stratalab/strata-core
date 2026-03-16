# Storage Engine Enhancement Plan

**Status:** Draft
**Date:** 2026-03-16
**Scope:** SegmentedStore → production stability

This document catalogs every known limitation, deferred decision, and missing feature in the current SegmentedStore implementation. Each item includes the current behavior, the impact, and the proposed enhancement. Items are grouped by priority.

---

## Current Architecture Summary

The storage engine is a per-branch LSM-style design:

```
Write → Active Memtable (SkipMap)
           ↓ rotation (when approx_bytes ≥ write_buffer_size)
        Frozen Memtable(s)
           ↓ flush (background, oldest-first)
        KV Segments (mmap'd .sst files, bloom-filtered, CRC32-checked)
           ↓ compaction (when segment_count ≥ threshold)
        Compacted Segment
```

Read path: active → frozen (newest first) → segments (newest first). First MVCC match wins.

---

## P0 — Correctness & Data Loss

### 1. Segment format v1 does not store timestamps or TTL

**Current:** `SegmentEntry` has three fields: `value`, `is_tombstone`, `commit_id`. When a memtable entry is converted to a segment entry during flush, the timestamp and TTL are discarded (`segment_entry_to_memtable_entry` sets `timestamp: Timestamp::from_micros(0)`, `ttl_ms: 0`).

**Impact:** After a memtable flush to disk:
- **Timestamp queries return wrong results.** `get_at_timestamp`, `scan_prefix_at_timestamp`, and `list_by_type_at_timestamp` treat flushed entries as having timestamp=0 (epoch), so they appear in every point-in-time query regardless of when they were actually written.
- **TTL expiration stops working.** Entries that had a TTL in the memtable lose it after flush. They become permanent until the next compaction (which also can't check TTL because the data isn't there).

**Enhancement:** Add timestamp and TTL fields to the segment entry format. This requires a format version bump (v1 → v2).

```
v2 entry layout:
  ik_len: u32
  ik_bytes: [u8; ik_len]
  value_kind: u8          (PUT=1, DEL=2)
  timestamp: u64           ← NEW (microseconds since epoch)
  ttl_ms: u64              ← NEW (0 = no TTL)
  value_len: u32
  value_bytes: [u8; value_len]
```

Segment reader must handle both v1 (timestamp=0, ttl=0 defaults) and v2 entries based on the header `format_version` field. `segment_entry_to_memtable_entry` then copies the real values instead of using epoch/zero.

### 2. GC is a no-op — old versions accumulate unboundedly

**Current:** `gc_branch()` returns 0. `expire_ttl_keys()` returns 0. The only pruning mechanism is compaction, but compaction requires ≥2 segments and a segments directory. Ephemeral databases (cache mode, no disk) never prune.

**Impact:** For long-running ephemeral databases, memory grows without bound as version chains accumulate. The `set_max_versions_per_key` field is stored but never enforced.

**Enhancement:** Implement in-memtable version pruning. When `max_versions_per_key > 0`, the memtable `put_entry` method (or a periodic sweep) should prune the oldest versions of a key beyond the limit. This is the SegmentedStore equivalent of ShardedStore's `KeepLast` write mode enforcement.

### 3. Compaction does not enforce max_versions_per_key

**Current:** `CompactionIterator` prunes based on `prune_floor` (commit_id threshold) only. The `max_versions_per_key` atomic is stored on `SegmentedStore` but never read by the compaction path.

**Impact:** Even after compaction, a key can retain unlimited versions above the prune floor.

**Enhancement:** Pass `max_versions_per_key` to `CompactionIterator`. After emitting `max_versions_per_key` versions for a logical key (all above floor), skip remaining above-floor versions. Keep the one-below-floor version for snapshot safety.

---

## P1 — Operational Stability

### 4. No write stalling under memory pressure

**Current:** `MemoryPressure` tracks Normal/Warning/Critical levels. `PressureLevel` is computed from `total_memtable_bytes()` / `budget`. But nothing acts on it — writes proceed at full speed even at Critical pressure.

**Impact:** If writes outpace flush, frozen memtables accumulate in memory. The process can OOM before the background scheduler has a chance to flush.

**Enhancement:** Add write stalling at Critical pressure:
- When `pressure_level() == Critical`, `put_with_version_mode` should block (via condvar or yield loop) until level drops to Warning or below.
- `max_immutable_memtables` config field exists but is not enforced — add a check in `maybe_rotate_branch` that blocks if frozen count exceeds this limit.

### 5. Engine never activates the flush pipeline — memory is unbounded

**Current:** The SegmentedStore flush pipeline (rotate memtable → flush to segment → compact segments) is fully implemented and tested in isolation (`flush_pipeline_tests.rs`). However, the engine never activates it:

1. **No segments directory.** `Database::open()` recovery creates `SegmentedStore::new()` (no dir). `Database::cache()` also uses `SegmentedStore::new()`. Neither calls `SegmentedStore::with_dir()`, so `segments_dir` is `None`, `write_buffer_size` is 0, and rotation is disabled.
2. **No flush scheduling.** The `BackgroundScheduler` exists but is only used for embedding tasks. No code path submits `flush_oldest_frozen` as a background job after rotation.
3. **No compaction scheduling.** `compact_branch` is never called from any engine code path.

The entire memtable → segment pipeline exists but is dead code from the engine's perspective.

**Impact:** This is the fundamental reason memory grows without bound. Every write appends to a memtable that never rotates and never flushes. For a disk-backed database with 10M keys at 100 bytes each, the memtable holds ~1 GB in memory with no way to offload it to disk. This defeats the entire purpose of the tiered storage project.

**Enhancement (three wiring changes):**

1. **Pass segments directory at construction.** In `open_finish` and `open_follower_internal`, create `SegmentedStore::with_dir(data_dir.join("DATA/segments"), cfg.storage.write_buffer_size)` instead of `SegmentedStore::new()`. In `RecoveryCoordinator::recover()`, accept a `segments_dir` parameter and pass it through.

2. **Submit flush jobs after rotation.** Add a post-commit hook in the transaction commit path: after `apply_writes` succeeds, check `storage.branches_needing_flush()`. For each branch with frozen memtables, submit a `flush_oldest_frozen` closure to `BackgroundScheduler` at `TaskPriority::High`.

3. **Submit compaction jobs after flush.** In the flush closure, after `flush_oldest_frozen` returns `Ok(true)`, check `storage.should_compact(branch_id, threshold)`. If true, submit a `compact_branch` closure at `TaskPriority::Normal`.

This is the single most important item in this document. Without it, the storage engine is functionally identical to the old in-memory ShardedStore.

### 6. No prefix compression in segment data blocks

**Current:** Each entry stores the full `InternalKey` bytes. For keys that share long prefixes (same branch, space, type tag, similar user keys), this wastes 20-30% of segment file size.

**Impact:** Larger segments mean more disk I/O, more mmap pressure, and slower sequential scans. Not a correctness issue, but affects performance at scale.

**Enhancement:** LevelDB-style prefix compression with restart points. Every `restart_interval` entries (e.g., 16), store the full key; intermediate entries store `shared_prefix_len + suffix`. The `key_restart_interval` field already exists in the header (currently unused). Estimated complexity: ~300 lines in segment_builder + segment reader.

### 7. No block compression

**Current:** The block frame reserves a codec byte (0=none, 1=zstd, 2=lz4) but all blocks are written with codec=0 (uncompressed).

**Impact:** Segment files are 2-4x larger than necessary for compressible data. Increases disk usage and mmap page cache pressure.

**Enhancement:** Add zstd compression (codec=1) as default for data blocks. The `zstd` crate is already in the workspace dependency graph. Index and filter blocks can remain uncompressed (they're small and benefit from random access).

### 8. No block cache

**Current:** Segment reads use mmap, which relies on OS page cache. There is no application-level block cache.

**Impact:** The OS page cache is a shared resource — other processes can evict segment pages. Hot blocks get re-faulted. Also, mmap + application block cache double-caches if both are active (the design review flagged this as issue #13).

**Enhancement:** Deferred. mmap with OS page cache is sufficient for the initial release. If benchmarks show regression from page cache contention, add an LRU block cache that replaces mmap for hot segments. The `block_cache_size` config field should be reserved but the cache itself deferred.

### 9. WAL truncation after flush

**Current:** WAL recovery replays the entire WAL from the beginning. Flushed data is re-applied to the memtable, then immediately visible in segments anyway.

**Impact:** Recovery time grows linearly with total WAL size, even if most data has already been flushed to segments. For a long-running database with many committed transactions, startup can be very slow.

**Enhancement:** Track a `flush_watermark` (the max commit_id that has been fully flushed to segments across all branches). On recovery, skip WAL records with `version ≤ flush_watermark`. Write the watermark to the MANIFEST after each successful flush. The MANIFEST v2 format already exists — add the watermark field.

---

## P2 — Performance & Scalability

### 10. Bloom filter uses crc32 + FNV-1a (not xxhash)

**Current:** Two base hashes from Kirsch-Mitzenmacher optimization: `h1 = crc32fast::hash(key)`, `h2 = fnv1a(key)`. Default 10 bits/key (~1% FPR).

**Impact:** crc32 is hardware-accelerated and fast. FNV-1a is adequate for bloom filters. This is acceptable for v1 but not optimal — xxhash3 would give better hash distribution for the same cost.

**Enhancement:** Low priority. Consider switching to xxhash3 if bloom filter false positive rates are measurably high in production workloads. Not worth the added dependency for v1.

### 11. memory_stats and shard_stats_detailed are O(n)

**Current:** Both methods call `list_branch_inner` which does a full MVCC merge across all sources for every branch. For a database with many branches and entries, this is expensive.

**Impact:** These are diagnostic methods, not hot path. But if called frequently (e.g., from a monitoring loop), they could cause latency spikes.

**Enhancement:** Maintain approximate entry counts per branch as an `AtomicUsize` counter, updated on put/delete. Use the counter for `memory_stats` instead of scanning. Reserve the full scan for an `exact_stats()` method.

### 12. time_range scans all entries

**Current:** `time_range` calls `list_branch` (full MVCC dedup scan) and iterates all results to find min/max timestamps.

**Impact:** O(n) per call for a branch with n live entries. Same concern as #11.

**Enhancement:** Track min/max timestamps per branch as atomics. Update on each write. Return the tracked values in O(1). The tracked values would be approximate (they'd include tombstoned entries' timestamps) but good enough for the time-travel UI.

### 13. No large value handling

**Current:** Values are stored inline in data blocks. A 50 MB value occupies an entire data block (or spans multiple blocks depending on block size), causing massive write amplification during compaction.

**Impact:** Not a problem for typical KV/state/event workloads (sub-KB values). Could be an issue for JSON documents or vector embeddings stored as values.

**Enhancement:** Deferred. For v1, document a recommended max value size (e.g., 1 MB). If needed later, add a blob store: values above a threshold are written to separate blob files and replaced with blob references in the segment.

### 14. Compaction strategy is threshold-only — no background segment reorganization

**Current:** `should_compact` returns true when `segment_count >= threshold`. All segments for a branch are merged into a single output segment. There is no size-tiered or leveled strategy. Compaction is never triggered automatically — it only runs when called explicitly.

**Impact:** At scale this breaks down in two ways:

1. **Read amplification.** Without background reorganization, segments accumulate from flush. At 1B keys with a 128 MB write buffer (~1.3M keys/flush), you'd have ~770 segments. Every point lookup checks the bloom filter of every segment. Every prefix scan merges across all 770 sources. Reads become I/O-bound.

2. **Write amplification.** Merging all 770 segments into one rewrites the entire dataset (~100 GB) in a single pass. This blocks the compaction thread for hours and produces a single massive segment that's expensive to mmap and impossible to compact incrementally.

**Enhancement:** Size-tiered compaction with background scheduling.

**How it works:**

```
Tier 0 (flush):    [64MB] [64MB] [64MB] [64MB]    ← 4 segments from flush
                          ↓ compact (4 → 1)
Tier 1:            [256MB] [256MB] [256MB] [256MB]  ← 4 tier-1 segments
                          ↓ compact (4 → 1)
Tier 2:            [1GB] [1GB] [1GB] [1GB]          ← 4 tier-2 segments
                          ↓ compact (4 → 1)
Tier 3:            [4GB]                             ← single large segment
```

**Rules:**
- Group segments into tiers by size: tier = floor(log₄(file_size / flush_size))
- Within a tier, compact when count ≥ `min_merge_width` (default 4)
- Output segment lands in the next tier up
- Maximum merge width (default 32) caps the I/O for a single compaction job
- Compaction jobs are submitted to `BackgroundScheduler` and run on worker threads
- Read path is unchanged — segments are still scanned newest-first

**Bounds at 1B keys (100 bytes/entry, 128 MB write buffer):**

| Tier | Segment Size | Count | Total |
|------|-------------|-------|-------|
| 0 | 128 MB | 0-3 (pending) | ≤ 384 MB |
| 1 | 512 MB | 0-3 | ≤ 1.5 GB |
| 2 | 2 GB | 0-3 | ≤ 6 GB |
| 3 | 8 GB | 0-3 | ≤ 24 GB |
| 4 | 32 GB | 2-3 | ≤ 96 GB |

At steady state, reads check at most ~15 segments (3-4 per tier × 4 tiers) instead of ~770. Write amplification is bounded to ~4× per tier × 4 tiers = ~16× total (vs unbounded for full compaction).

**Implementation:**
- `CompactionScheduler` struct: scans branch segments, groups into tiers, picks merge candidates
- `compact_tier` method: merges `min_merge_width` same-tier segments into one next-tier segment
- Background loop: after every flush completion, check all tiers for merge-ready groups
- Existing `CompactionIterator` is reused unchanged — tier compaction just selects which segments to merge

---

## Scaling to 1B Keys — Additional Bottlenecks

These items are required for the storage engine to support datasets in the 100M–1B key range. They are separate from the general enhancements above because they only manifest at scale.

### 18. Bloom filters are fully memory-resident

**Current:** `KVSegment::open()` parses the bloom filter block and stores it as `BloomFilter { bits: Vec<u8>, num_hashes: u32 }` in memory. Every open segment holds its full bloom filter in RAM for the lifetime of the process.

**Impact:** At 10 bits/key (default), bloom filter memory scales linearly:

| Keys | Bloom Memory |
|------|-------------|
| 1M | ~1.2 MB |
| 100M | ~120 MB |
| 1B | ~1.2 GB |

At 1B keys, bloom filters alone consume 1.2 GB of heap. With multiple branches this multiplies further. The bloom is not evictable — there is no LRU cache or lazy-load mechanism.

**Enhancement:** Two options (not mutually exclusive):

1. **Lazy bloom loading.** Don't parse the bloom block on `KVSegment::open()`. Instead, mmap the bloom block region and access it directly from the mmap'd file. This moves bloom memory into the OS page cache (evictable under memory pressure) instead of the heap. The bloom filter format is a flat bit array — it's already suitable for direct mmap access. Estimated change: ~50 lines in `segment.rs`.

2. **Partitioned bloom filters.** Split the bloom into per-data-block sub-blooms. On a point lookup, first check the index block to find the candidate data block, then check only that block's sub-bloom. This reduces the working set from the full bloom to a single sub-bloom (~few KB). More complex (~200 lines) and requires segment format v2.

Recommendation: Start with option 1 (lazy/mmap bloom). It's simple, effective, and doesn't require a format change.

### 19. Index blocks are fully memory-resident

**Current:** `KVSegment::open()` parses the index block into `Vec<IndexEntry>` where each entry holds `first_key: InternalKey` and `block_offset: u64`. This entire vector is kept in memory.

**Impact:** With 64 KB data blocks, a 128 MB segment has ~2,000 index entries. At 1B keys across ~15 segments (after tiered compaction), that's ~30,000 index entries — manageable. But without tiered compaction (770 segments), it's ~1.5M index entries holding ~150 MB of key copies.

**Enhancement:** This is primarily solved by tiered compaction (#14) reducing segment count. Additionally, the index block could be mmap'd instead of parsed into a Vec — binary search directly on the mmap'd bytes. This requires a fixed-size index entry format (currently variable due to `InternalKey` length). Lower priority than bloom mmap (#18).

### 20. list_branch and scan methods materialize all results

**Current:** `list_branch`, `list_by_type`, `list_by_type_at_timestamp`, `scan_prefix_at_timestamp`, and `time_range` all collect results into a `Vec` before returning.

**Impact:** At 1B keys, `list_branch` would attempt to allocate a Vec of 1B `(Key, VersionedValue)` tuples — hundreds of GB. This is an instant OOM. Even `list_by_type` for a common type tag (e.g., KV) could return millions of entries.

**Enhancement:** Add iterator-based variants that yield entries lazily:

```rust
// Existing (materializing — keep for small result sets)
fn list_branch(&self, branch_id: &BranchId) -> Vec<(Key, VersionedValue)>

// New (streaming — for large datasets)
fn iter_branch(&self, branch_id: &BranchId) -> impl Iterator<Item = (Key, VersionedValue)>
fn iter_by_type(&self, branch_id: &BranchId, type_tag: TypeTag) -> impl Iterator<...>
fn iter_prefix(&self, prefix: &Key, max_version: u64) -> impl Iterator<...>
```

The engine-layer callers (`branch_ops::diff_branches`, `time_range`, `memory_stats`) should be updated to use the streaming variants and process entries one at a time or in bounded batches.

### 21. Single-segment compaction output produces unbounded file sizes

**Current:** `compact_branch` merges all input segments into a single output segment file. The output size equals the sum of live data across all inputs.

**Impact:** At 1B keys with 100-byte values, the compacted output is ~100 GB in a single file. This has several problems:
- mmap'ing a 100 GB file works on 64-bit but the TLB pressure is severe
- The file cannot be compacted further (it's already one segment)
- If the process crashes mid-compaction, the partial file is wasted I/O
- Concurrent reads on the old segments must be held until the full compaction finishes

**Enhancement:** This is solved by tiered compaction (#14), which bounds output segment size to `flush_size × merge_width^tier`. The largest segment at tier 4 would be ~32 GB. For even larger datasets, add a `max_segment_size` config that splits compaction output into multiple segments when the output exceeds the limit.

---

## Recovery & Durability Blockers

These items affect startup time and crash recovery. At small scale they're invisible; at 100M+ keys they dominate startup latency.

### 22. WAL recovery reads ALL records into memory at once

**Current:** `WalReader::read_all()` (`crates/durability/src/wal/reader.rs:150`) collects every WAL record into a `Vec<WalRecord>` before returning. `RecoveryCoordinator::recover()` then iterates this Vec and replays each record one-by-one into SegmentedStore.

**Impact:** At 1B keys × 10 versions × ~200 bytes/record = **2 TB of WAL data loaded into memory**. Recovery OOMs long before it finishes. Even at 100M keys this is ~20 GB — larger than most machines' RAM.

**Enhancement:** Replace `read_all()` with a streaming iterator that yields records one-by-one from mmap'd WAL segments. The recovery loop already processes records sequentially — it just needs the allocation removed. Estimated: ~100 lines to add `WalReader::iter_all()` returning `impl Iterator<Item = WalRecord>`.

### 23. WAL replay is sequential and single-threaded

**Current:** Recovery replays each WAL record by calling `put_with_version_mode` and `delete_with_version` one at a time (`recovery.rs:108-124`). No batching, no parallelism.

**Impact:** At 1B keys with 10 versions, recovery executes 10B individual memtable inserts. At ~1M inserts/sec, this takes **~3 hours** of pure CPU time. The WAL is read sequentially from a single thread.

**Enhancement:** Two improvements:
1. **Batch replay.** Accumulate 10K-100K records into a batch, then call `apply_batch()` which amortizes DashMap lock acquisition across entries.
2. **Parallel branch replay.** Since branches are independent (no cross-branch contention), records can be partitioned by branch and replayed in parallel across worker threads.

### 24. No WAL segment skipping — watermark doesn't reduce I/O

**Current:** `WalReplayer::replay_after()` (`replayer.rs:83-129`) reads and deserializes every WAL record, then compares `record.txn_id <= watermark` to decide whether to skip. Even when 99% of records are below the watermark, all records are read from disk and deserialized.

**Impact:** The flush watermark saves CPU (skipped records aren't applied) but not I/O or deserialization cost. With 3,000+ WAL segments, recovery reads all of them regardless.

**Enhancement:** Store per-WAL-segment metadata (max_txn_id) so that entire segments below the watermark can be skipped without opening them. The `.meta` files already exist — add `max_txn_id` to the segment metadata and skip segments where `meta.max_txn_id <= watermark`.

### 25. Vector/search recovery scans all KV entries per collection

**Current:** Vector recovery (`vector/recovery.rs:85-188`) and search recovery (`search/recovery.rs:112`) both iterate `branch_ids()`, then for each branch call `scan_prefix()` to enumerate all vector configs and all vector entries. Search recovery calls `list_by_type()` which materializes all entries into a Vec.

**Impact:** With 1B vectors in a single collection, `scan_prefix()` returns 1B entries into a Vec — instant OOM. Even at 100M vectors this is ~10 GB of allocations. Vector recovery also rebuilds the HNSW index from scratch if mmap cache is missing, which is O(n log n) for n vectors.

**Enhancement:**
1. **Mmap-first recovery.** If mmap cache files (`.vec` heap, `.hgr` graph) are intact, skip KV scan entirely. Only fall back to KV scan for collections with missing/corrupt mmap files.
2. **Streaming scan.** Use iterator-based scan (#20) so recovery processes entries one at a time without materializing.
3. **Incremental HNSW rebuild.** Build HNSW graph incrementally during WAL replay rather than full rebuild at the end.

### 26. recover_segments loads all KV segment files into memory at once

**Current:** `SegmentedStore::recover_segments()` (`segmented.rs:675-765`) iterates all branch directories, opens every `.sst` file, and pushes `Arc::new(KVSegment)` into the branch's segment list. Each `KVSegment::open()` loads the full index block and bloom filter into RAM.

**Impact:** At 1B keys after tiered compaction (~15 segments), this is fine. But before compaction (after many flushes), there could be thousands of segments. With 3,000 segments × 64 MB average, the bloom filters + index blocks alone consume several GB of heap.

**Enhancement:** Lazy segment loading — defer `KVSegment::open()` until the segment is actually accessed by a read. Store segment metadata (path, commit range) without opening the file. Combined with mmap'd blooms (#18) and mmap'd index (#19), this bounds startup memory to metadata only.

### 27. write_buffer_size config is defined but never used

**Current:** `StorageConfig` defines `write_buffer_size: usize` (default 128 MiB) in `config.rs:74`. The value is read from `strata.toml`. But `Database::open()` creates `SegmentedStore::new()` which sets `write_buffer_size = 0` (rotation disabled). The config value is never passed to the store.

**Impact:** Memtable rotation is disabled even for disk-backed databases. This is the root cause of unbounded memory growth — the config exists, the rotation code exists, but they're not connected. This is a subset of #5 but worth calling out explicitly because the user-visible config parameter silently does nothing.

**Enhancement:** Part of #5 — use `SegmentedStore::with_dir(segments_dir, cfg.storage.write_buffer_size)` in `open_finish()`.

### 28. MergeIterator uses linear scan instead of binary heap

**Current:** `MergeIterator::next()` (`merge_iter.rs:50-71`) does a linear scan over all sources to find the minimum key. Comment says "with typically 1-3 sources, a linear scan outperforms BinaryHeap."

**Impact:** With tiered compaction, reads merge across ~15 segments + active + frozen memtables ≈ ~18 sources. At 18 sources, linear scan is 18 comparisons per `next()` call. A binary heap would be O(log 18) ≈ 4 comparisons. For a prefix scan returning 100K entries, this is 1.8M comparisons vs 400K.

**Enhancement:** Switch to `BinaryHeap` when source count > 4. Keep linear scan as fast path for ≤4 sources. The `Peekable` iterator interface is already heap-compatible.

### 29. InternalKey panics on malformed input

**Current:** `InternalKey::from_bytes()` (`key_encoding.rs:146-149`) uses `assert!()` for validation. `commit_id()` (`key_encoding.rs:173`) uses `.try_into().unwrap()`. Both panic on malformed input.

**Impact:** A corrupted segment file with truncated key data crashes the entire process instead of returning an error. At scale, segment corruption becomes statistically likely (bit rot, disk errors, interrupted writes).

**Enhancement:** Replace `assert!` with `Result` return. Add `InternalKey::try_from_bytes()` that returns `Option<InternalKey>`. Segment reader should skip corrupted entries with a warning rather than crashing.

### 30. TTLIndex exists but is not integrated

**Current:** `crates/storage/src/ttl.rs` implements a `TTLIndex` with `insert()`, `remove()`, and `find_expired()` methods. But `SegmentedStore` does not maintain a `TTLIndex`. TTL is checked at read time via `is_expired()` and during compaction — there is no proactive expiry.

**Impact:** Expired entries accumulate in memtables and segments until they're either read (and filtered) or compacted away. A branch with 100M expired keys keeps them in memory indefinitely if no reads touch them.

**Enhancement:** Integrate `TTLIndex` into `BranchState`. On each put with TTL, insert into the index. Add a periodic `sweep_expired()` that removes expired entries from the active memtable. This converts TTL from lazy (check-on-read) to eager (background cleanup). Note that `find_expired()` itself materializes into a Vec — needs streaming variant for scale.

---

## P3 — Future / Out of Scope for v1

### 15. Epic 9 — Shared Segments & Branch Manifests

**Goal:** Eliminate disk amplification from per-branch segment duplication. Branches share immutable segments via manifests; only branch-local writes (overlay segments) are unique.

**Current:** Each branch maintains its own independent list of segments in `BranchState.segments: Vec<Arc<KVSegment>>`. Forking a branch copies all data — there is no segment sharing between branches. For a 1 GB branch, fork = 1 GB of new segment files.

**Impact:** Disk usage scales linearly with branch count. A repository with 50 branches of a 1 GB dataset consumes 50 GB instead of ~1.2 GB (shared base + small overlays). This makes branching impractical for large datasets.

**Design:** Segment files are already branch-agnostic (no branch_id in `InternalKey` encoding). The infrastructure for sharing exists — what's missing is the manifest layer.

```
Branch main:     [Seg1] [Seg2] [Seg3] [Seg4]        ← manifest: [1,2,3,4]
                   ↑      ↑      ↑
Branch feature:  [Seg1] [Seg2] [Seg3] [Seg5]        ← manifest: [1,2,3,5]
                                        └── overlay (writes since fork)
```

**Components (~1,000 lines, isolated to storage crate):**

| Component | Est. Lines | Touches |
|-----------|------------|---------|
| `BranchManifest` struct (segment refs + ref counts) | ~200 | New file |
| Manifest persistence (atomic write + CURRENT pointer) | ~200 | New file |
| Fork: flush source memtable → clone manifest → bump refs | ~100 | `segmented.rs` |
| Compaction prune floor coordination across branches | ~400 | `compaction.rs` |
| Segment GC (ref_count=0 → delete file) | ~100 | `segmented.rs` |

**Fork semantics (force-flush approach):**

```
fork(source → dest):
  1. Flush source's active memtable to segment (~100ms for 128MB)
  2. Clone source's manifest → dest manifest
  3. Increment ref_count on all shared segments
  4. dest gets a fresh empty memtable
```

Force-flushing before fork makes unflushed memtable data visible in the new segment, avoiding the correctness issue where memtable data would be invisible to the forked branch. Fork cost is O(flush) not O(1), but O(flush) ≪ O(data copy).

**Compaction with shared segments:**

```
compact(Seg1, Seg2 → Seg6):
  1. prune_floor = min(low_watermark_commit) across ALL branches referencing Seg1, Seg2
  2. Merge-sort Seg1+Seg2, prune versions below prune_floor
  3. Produce Seg6
  4. Atomically update ALL referencing manifests: replace [Seg1,Seg2] with [Seg6]
  5. Decrement ref_counts on Seg1, Seg2
  6. Delete segments where ref_count = 0
```

Cross-branch prune floor coordination is the hardest part (~400 lines) but is entirely isolated to the compaction module. The read path, write path, and segment format are unaffected.

**What doesn't change:** Segment format (v1/v2), memtable, flush pipeline (same files, registers in manifest instead of directory scan), read path (still memtable → frozen → segments newest-first), WAL, recovery.

**Dependencies:** #5 (background flush/compaction) should land first so fork can trigger a flush. #1 (segment format v2) is independent — shared segments work with either format version.

### 16. No multi-process coordination

The design review flagged this as issue #9. The current lock file prevents concurrent access from multiple processes but does not support read-only followers reading segments while the primary writes. The follower mode uses WAL replay, not shared segment access.

### 17. No retention policy integration

The design specifies `RetentionPolicy { min_commits, min_duration, max_versions_per_key }` but it is not wired to compaction. Implementing this requires passing the policy to `CompactionIterator` and using timestamp-aware pruning (which requires P0 item #1 first).

---

## Implementation Priority

| Priority | Item | Effort | Dependency |
|----------|------|--------|------------|
| **P0** | #5 Wire up flush pipeline in engine | M | None |
| **P0** | #27 Connect write_buffer_size config | S | #5 |
| **P0** | #1 Segment format v2 (timestamp + TTL) | M | None |
| **P0** | #2 In-memtable version pruning | S | None |
| **P0** | #3 Compaction max_versions_per_key | S | None |
| **P0** | #22 Streaming WAL reader | M | None |
| **P1** | #4 Write stalling | S | #5 |
| **P1** | #7 Block compression (zstd) | S | None |
| **P1** | #9 WAL truncation | M | #5 |
| **P1** | #14 Size-tiered compaction | L | #5 |
| **P1** | #23 Batched/parallel WAL replay | M | #22 |
| **P1** | #24 WAL segment skipping via metadata | S | #22 |
| **P1** | #28 MergeIterator binary heap fallback | S | None |
| **P1** | #29 InternalKey error handling | S | None |
| **P2** | #6 Prefix compression | M | None |
| **P2** | #11 O(1) memory stats | S | None |
| **P2** | #12 O(1) time_range | S | None |
| **P2** | #18 Mmap'd bloom filters | S | None |
| **P2** | #20 Streaming iterators for list/scan | M | None |
| **P2** | #25 Mmap-first vector/search recovery | M | #20 |
| **P2** | #26 Lazy segment loading | M | #18 |
| **P2** | #30 TTLIndex integration | M | None |
| **P3** | #15 Shared segments & branch manifests (Epic 9) | L | #5, #14 |
| **P3** | #19 Mmap'd index blocks | M | Segment format v2 |
| **P3** | #21 Max segment size limit | S | #14 |

S = small (< 1 day), M = medium (1-3 days), L = large (3+ days)

**#5 is the critical path.** Without it, the engine is functionally in-memory-only — the entire segment pipeline is dead code.

Recommended order: **#5, #27** → #22 → #2, #3 → #1 → #4, #9, #23, #24 → #14, #28, #29 → #7, #20 → #25, #18, #26 → #6 → #15

### Scaling milestones

| Target | Prerequisite Items |
|--------|--------------------|
| **Bounded memory** | **#5, #27** (wire flush pipeline + config — without this, memory grows without bound) |
| **10M keys** | + #2, #3 (version pruning), #22 (streaming WAL reader) |
| **100M keys** | + #14 (tiered compaction), #18 (mmap blooms), #23/#24 (fast recovery) |
| **1B keys** | + #20 (streaming iterators), #7 (compression), #6 (prefix compression), #28 (heap merge) |
| **1B vectors** | + #25 (mmap-first vector recovery), #26 (lazy segment loading), HNSW incremental rebuild |
| **Fast restart at any scale** | + #9 (WAL truncation), #24 (segment skipping), #26 (lazy load) |

---

## Architectural Risks & Mitigations

These are inherent trade-offs of the LSM/segment design. They aren't bugs to fix — they're structural properties that need to be understood, monitored, and mitigated.

### R1. Write amplification

Every piece of data is written multiple times: once to the WAL, once when the memtable flushes to a segment, and again each time that segment participates in compaction. With 4-5 tiers of size-tiered compaction, total write amplification is roughly 16-20x. 100 GB of logical data produces 1.6-2 TB of actual disk writes.

**Why it matters for embedded:** Strata runs on the user's machine. Consumer SSDs have write endurance limits (typically 200-600 TBW). At 20x amplification with heavy write workloads, an SSD could hit its endurance limit within a year. Laptop users may also see battery drain from sustained background I/O.

**Mitigations:**
- Block compression (zstd, item #7) reduces the bytes written at each tier by 2-4x, cutting effective amplification to 5-10x.
- Tuning `min_merge_width` higher (e.g., 8 instead of 4) produces fewer, larger compaction rounds — fewer total rewrites at the cost of more temporary space.
- Rate-limiting compaction I/O (e.g., 50 MB/s cap) prevents compaction from saturating the disk and wearing it unevenly. Not currently implemented but straightforward to add as a `tokio::time::sleep` between block writes.
- For write-heavy workloads, document a recommended `write_buffer_size` increase (e.g., 256-512 MB) to reduce flush frequency.

### R2. Compaction stalls and write latency spikes

When compaction falls behind, frozen memtables accumulate. Once the count exceeds `max_immutable_memtables`, write stalling blocks user writes until a flush completes. This is a binary block/unblock with no gradual degradation — write latency jumps from microseconds to hundreds of milliseconds.

**Why it matters for embedded:** The application embedding Strata experiences unpredictable latency. A UI-driven app might freeze. A server handling requests gets timeouts.

**Mitigations:**
- Gradual backpressure instead of hard stalling. When frozen count reaches 50% of limit, artificially add a small sleep (e.g., 1ms) to writes. At 75%, increase to 10ms. Only hard-block at 100%. This gives compaction time to catch up without a cliff.
- Priority-aware writes. Add a `WriteOptions { priority: High | Normal | Low }` parameter. During backpressure, only Low-priority writes are delayed. High-priority writes (e.g., user-initiated saves) always proceed.
- Compaction thread priority. Use `nice` / `ionice` (on Linux) to keep compaction threads at lower scheduling priority than the write path.

### R3. Space amplification during compaction

During compaction, both old and new segments exist simultaneously until the merge finishes. A tier-4 compaction of 32 GB needs 32 GB of temporary extra space. Combined with WAL segments that haven't been truncated, a 100 GB dataset might need 200-250 GB of disk.

**Why it matters for embedded:** Laptops and desktops have limited disk. A user with 50 GB free might not be able to compact a 40 GB database.

**Mitigations:**
- Pre-check available disk space before starting compaction. Skip compaction if free space is below `2 × largest_input_segment`. Log a warning so the user knows.
- Incremental compaction: process input segments in chunks, writing partial output segments as you go. This caps temporary space to `chunk_size` instead of `total_input_size`. More complex but bounded.
- WAL truncation (item #9) reclaims WAL space after flush, freeing disk for compaction.

### R4. mmap gives up memory control to the OS

Segment data is mmap'd, meaning the OS page cache decides what stays in RAM. Under memory pressure from other processes, segment pages get evicted and reads go from microseconds to milliseconds with no warning. There's no admission control, no priority between segments, and no guarantee that bloom filters stay resident.

**Why it matters for embedded:** The user's machine runs many applications. A browser loading a large page, a video call, or an IDE indexing can evict Strata's working set. The next Strata read is suddenly 100x slower.

**Mitigations:**
- `mlock` critical pages. For bloom filters and index blocks (item #18 moves them to mmap), use `mlock()` to pin them in RAM. This is a few MB total — affordable even on constrained machines.
- Application-level LRU block cache (item #8, currently deferred). For hot data blocks, maintain a fixed-size cache in Strata's own heap. Reads hit the cache first, falling through to mmap only on miss. This gives predictable performance for the working set at the cost of heap memory.
- `madvise(MADV_SEQUENTIAL)` for compaction reads and `MADV_RANDOM` for point lookups. This hints the OS page cache about access patterns so it can make better eviction decisions.

### R5. GC safe point is a global bottleneck

Version pruning during compaction can only remove versions older than the oldest active transaction's snapshot. One long-running read transaction pins the GC safe point and prevents version pruning across the entire database. Old versions accumulate, segments grow, and compaction writes more data.

**Why it matters for embedded:** A slow analytics query or a forgotten open transaction can cause unbounded version accumulation. The user sees disk usage growing with no obvious cause.

**Mitigations:**
- Transaction timeout. Auto-abort read transactions that have been open longer than a configurable duration (e.g., 60 seconds). This prevents stale snapshots from pinning the GC point indefinitely.
- Per-branch GC safe points instead of a global one. A stale reader on branch A shouldn't block GC on branch B. This requires tracking active snapshots per branch instead of globally.
- GC pressure monitoring. Expose a metric: `gc_blocked_seconds` — how long the GC safe point has been pinned. Alert the application so it can log, warn, or abort the offending transaction.

### R6. Point-in-time queries degrade after flush (until segment format v2)

The v1 segment format doesn't store timestamps. Flushed entries appear with timestamp=0 in historical queries. At steady state, most data is in segments, so time-travel queries return incorrect results for all but the most recent writes.

**Why it matters for embedded:** Time-travel is a user-facing feature. If a user queries "show me the state as of yesterday" and gets wrong results, that's a data integrity problem from their perspective.

**Mitigations:**
- Segment format v2 (item #1) is the real fix. Until it ships, document the limitation clearly in the SDK and API docs.
- For ephemeral/cache databases (no flush), timestamps are accurate because all data stays in memtables. Document this as the safe mode for time-travel-heavy workloads.
- As a stopgap, the memtable could persist timestamps to a sidecar file alongside the segment. This avoids changing the segment format but adds I/O complexity.

### R7. Segment corruption is silent data loss

There's no redundancy for segment files. A single bad disk sector in a segment loses the entries in that data block. CRC32 checksums detect corruption on read, but the data is already gone. After WAL truncation, segments are the only copy.

**Why it matters for embedded:** Consumer hardware is less reliable than server-grade storage. Laptops get dropped, power cut unexpectedly, and SSDs have firmware bugs. Silent bit rot is rare but not zero.

**Mitigations:**
- Full-segment checksums. On flush completion, compute a SHA-256 over the entire segment file and store it in the MANIFEST. On recovery, verify the checksum before trusting the segment. If it fails, mark the segment as corrupt and replay from WAL (if WAL hasn't been truncated past it).
- Conservative WAL truncation. Only truncate WAL segments when at least 2 successful compaction rounds have covered the data. This keeps a longer WAL tail as insurance against segment corruption, at the cost of more disk usage.
- `fsync` segment files and directories after write. Already done during flush, but verify the full fsync chain (file → directory → parent) is complete. Missing any link leaves a corruption window on power loss.

### R8. Compaction can't shrink after mass deletion

If you write 1B keys and then delete 900M, the remaining 100M keys are scattered across segments sized for 1B keys. Compaction removes tombstones and dead versions but the output segments are still proportional to the surviving data. However, until enough compaction rounds have consolidated everything, you're paying for fragmented segments.

**Why it matters for embedded:** A user deletes a large dataset expecting disk space to be reclaimed. It isn't — not immediately. Disk usage stays high until background compaction catches up, which could take hours.

**Mitigations:**
- Forced full compaction. Expose a `db.compact()` API that triggers a full branch compaction outside the normal tiered schedule. The user can call it after a large deletion to reclaim space immediately.
- Tombstone density tracking. During compaction, track the ratio of tombstones to live entries in each segment. If a segment is >50% tombstones, prioritize it for compaction regardless of tier scheduling.
- Space reclamation reporting. After compaction, report how much disk was reclaimed. Surface this in the SDK so the application can show progress to the user.

### R9. Branch forking requires a synchronous flush

Forking a branch flushes the source memtable to a segment before cloning the manifest. This is ~100ms for a 128 MB memtable — fast, but synchronous. During that window, the source branch blocks new writes.

**Why it matters for embedded:** Applications that fork frequently (e.g., speculative execution, undo/redo snapshots) pay 100ms per fork. Rapid fork/discard patterns become expensive.

**Mitigations:**
- Copy-on-write memtable sharing. Instead of flushing, let the forked branch share a read-only reference to the source's frozen memtable. Writes to either branch go to new memtables. The shared frozen memtable is flushed by whichever branch next triggers rotation. This makes fork O(1) at the cost of slightly more complex memory management.
- Async fork. Return a `ForkHandle` immediately and do the flush in the background. The forked branch becomes readable once the flush completes. Writes to the fork queue until then. Adds latency to the fork's first write instead of blocking the source.

### R10. Large values cause disproportionate amplification

The segment format stores values inline in 64 KB data blocks. A 50 MB JSON document spans ~800 blocks, gets rewritten during every compaction that touches its segment, and inflates the bloom filter (one entry for a key that's 800x larger than average).

**Why it matters for embedded:** Graph property values, JSON documents, and serialized embeddings can easily exceed 1 MB. A few hundred large values in a dataset of small values can dominate compaction I/O.

**Mitigations:**
- Value size limit enforcement. Reject values above a configurable threshold (e.g., 10 MB) at the API level with a clear error. Document the limit.
- Blob separation (future). Values above a threshold (e.g., 64 KB) are written to a separate blob file and replaced in the segment with a blob reference. Compaction only rewrites the reference, not the blob. This is a significant architectural addition (~1,000 lines) and should be deferred until benchmarks show it's needed.
- For vectors specifically, embedding data already lives in a separate VectorHeap with its own mmap files, so this concern doesn't apply to the vector hot path.
