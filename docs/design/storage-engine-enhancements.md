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

### 5. No background flush or compaction scheduling

**Current:** The engine's `BackgroundScheduler` exists but the storage layer does not submit flush or compaction jobs automatically. Flush only happens when explicitly called (`flush_oldest_frozen`) or during `end_bulk_load`. Compaction only happens when `compact_branch` is called explicitly.

**Impact:** Without background flush, frozen memtables accumulate indefinitely (in disk-backed mode). Without background compaction, segment count grows until reads become slow (linear scan across many segments).

**Enhancement:** Integrate with `BackgroundScheduler`:
- After rotation in `maybe_rotate_branch`, submit a flush job for the branch.
- After flush completes, check `should_compact` and submit a compaction job if needed.
- Respect pressure levels: at Warning, increase flush frequency; at Critical, prioritize flush over compaction.

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

### 14. Compaction strategy is threshold-only

**Current:** `should_compact` returns true when `segment_count >= threshold`. There is no leveled or tiered compaction strategy. All segments for a branch are merged into one.

**Impact:** Full-branch compaction is simple and correct but has high write amplification. A branch with 100 segments of 64 MB each would require reading and rewriting 6.4 GB. This blocks the compaction thread for the entire duration.

**Enhancement:** Implement size-tiered compaction (STCS): group segments by size, compact similar-sized segments together. This limits write amplification to ~10x (vs potentially 100x for full compaction). The trigger changes from "count >= N" to "count of similar-sized segments >= N".

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
| **P0** | #1 Segment format v2 (timestamp + TTL) | M | None |
| **P0** | #2 In-memtable version pruning | S | None |
| **P0** | #3 Compaction max_versions_per_key | S | None |
| **P1** | #4 Write stalling | S | #5 |
| **P1** | #5 Background flush/compaction | M | None |
| **P1** | #6 Prefix compression | M | None |
| **P1** | #7 Block compression (zstd) | S | None |
| **P1** | #9 WAL truncation | M | #5 |
| **P2** | #11 O(1) memory stats | S | None |
| **P2** | #12 O(1) time_range | S | None |
| **P2** | #14 Size-tiered compaction | L | #5 |
| **P3** | #15 Shared segments & branch manifests (Epic 9) | L | #5 |

S = small (< 1 day), M = medium (1-3 days), L = large (3+ days)

Recommended order: #2, #3 → #1 → #5 → #4, #9 → #7, #6 → #14 → #15
