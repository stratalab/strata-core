# Reference Implementation Audit: Strata vs LevelDB/RocksDB

**Status:** Active Reference
**Date:** 2026-03-17
**Sources:** LevelDB (github.com/google/leveldb), RocksDB (github.com/facebook/rocksdb)

This document compares Strata's SegmentedStore with LevelDB and RocksDB across every major subsystem, identifying gaps and prioritized improvements.

---

## 1. Segment/SSTable Format

### Entry Encoding

| Aspect | LevelDB | Strata | Gap |
|--------|---------|--------|-----|
| Entry header | 3 bytes (varints, fast path) | 25 bytes (fixed u32/u64) | 8x larger overhead |
| Key encoding | Prefix-compressed (shared + delta) | Full key per entry | 20-50% larger keys |
| Value storage | Raw bytes (zero-copy Slice) | bincode-serialized Value enum | Deserialization required |
| Restart points | Every 16 entries, offset array at block end | None (deferred to #1518) | No intra-block binary search |

**Impact:** At 64KB blocks with 1KB values, LevelDB fits ~150 entries vs our ~60. More entries per block = fewer blocks = fewer cache misses.

### Block Framing

| Aspect | LevelDB | Strata |
|--------|---------|--------|
| Overhead | 5 bytes (type + CRC) | 12 bytes (type + codec + reserved + len + CRC) |
| CRC scope | Data + type byte | Data only |
| Compression | Snappy (default), Zstd optional | Zstd level 3 |
| Length field | Inferred from index BlockHandle | Explicit in frame header |

### Index Block

| Aspect | LevelDB | Strata |
|--------|---------|--------|
| Keys | Shortened separators (FindShortestSeparator) | Full first key of each block |
| Values | Varint-encoded BlockHandle (offset + size) | Fixed u64 offset + u32 len |
| Format | Same prefix-compressed block format | Custom: count + entries array |

**Key insight:** LevelDB's separator shortening can reduce index size 20-50% for hierarchical keys.

### Bloom Filter

| Aspect | LevelDB | Strata |
|--------|---------|--------|
| Granularity | Per-2KB file region | Single per-segment |
| Key input | Full internal key | Typed key prefix (deduped across versions) |
| Policy | Pluggable (FilterPolicy interface) | Fixed bloom implementation |

---

## 2. Version Management & Concurrency

### The Fundamental Difference

**LevelDB:** Immutable `Version` objects in a refcounted linked list. Compaction creates a new Version; old Versions survive until all readers release them. `LogAndApply` atomically installs new Version.

**Strata:** Mutable `BranchState` behind DashMap. Segment lists are modified in-place. No version history.

### Implications

| Scenario | LevelDB | Strata |
|----------|---------|--------|
| Read during compaction | Reader holds old Version ref → safe | Reader sees segment list mid-mutation → potential issue |
| Compaction output visibility | Atomic via LogAndApply | Segment swap under DashMap guard |
| Crash recovery | Replay MANIFEST to rebuild Version graph | Rescan segment directory + replay WAL |

### File Metadata (per SSTable/Segment)

| Field | LevelDB | RocksDB | Strata |
|-------|---------|---------|--------|
| Level assignment | In VersionEdit | In VersionEdit | **None** |
| Key range | smallest, largest | smallest, largest | key_min, key_max (in PropertiesBlock) |
| Ref count | refs field | refs field | Arc automatic |
| Allowed seeks | Tracked for compaction trigger | Extended | **Not tracked** |
| Being compacted | Implicit (in compaction input set) | being_compacted flag | **Not tracked** |
| Sequence range | N/A | smallest_seqno, largest_seqno | commit_min, commit_max |
| File creation time | N/A | file_creation_time | **Not tracked** |
| Compaction score | Per level (Finalize) | Per level | **Not tracked** |

### MANIFEST

| Aspect | LevelDB/RocksDB | Strata |
|--------|-----------------|--------|
| Format | Append-only log of VersionEdit records | Single record (WAL/snapshot state only) |
| Content | File additions/deletions per level | Active WAL segment, flush watermark |
| Recovery | Replay edits to build current Version | Rescan directory for .sst files |
| Level tracking | Explicit in each edit | **None** |

**Gap:** Strata's MANIFEST doesn't track segment-level assignments, making leveled compaction impossible without extending it.

---

## 3. Compaction

### LevelDB Compaction

```
Level 0: Overlapping files from memtable flush (max 4 before triggering)
Level 1: Non-overlapping (target size ~10MB)
Level 2: Non-overlapping (target size ~100MB)
...
Level N: Non-overlapping (target size ~10^N MB)
```

**Trigger:** `compaction_score = level_size / target_size`. Highest score compacts first.

**Selection (`PickCompaction` in version_set.cc):**
- Size-triggered: pick file overlapping `compact_pointer_[level]` (round-robin)
- Seek-triggered: pick file with `allowed_seeks == 0`
- Choose L+1 files that overlap with chosen L file's key range
- `SetupOtherInputs`: expand L to include all L files overlapping with L+1 range

**Execution:**
- Merge sort L and L+1 inputs → produce L+1 output files (2MB each)
- Trivial move: if L file doesn't overlap any L+1 file, metadata-only move (no I/O)
- `LogAndApply`: atomically install new Version with updated file lists

### RocksDB Extensions

- **Universal compaction:** Size-tiered (our current approach), lower write amp
- **Level0 compaction trigger:** `level0_file_num_compaction_trigger` (default 4)
- **Write stalling:** `level0_slowdown_writes_trigger` (default 20), `level0_stop_writes_trigger` (default 36)
- **Max compaction bytes:** limits how much data is merged per compaction job
- **Subcompactions:** parallelize a single compaction across multiple threads
- **Compaction filters:** drop keys during compaction (TTL, custom logic)

### Strata Compaction

```
All segments: Flat list, newest first (no explicit levels)
Tier 0: size < 64MB base → all flush-sized segments
Tier 1: size 64-256MB
...
```

**Trigger:** `l0_segment_count >= 4` (in flush callback)

**Selection:** `CompactionScheduler::pick_candidates` groups by tier, picks first tier with >= 4 segments.

**Execution:** `compact_tier` merges selected segments → single output → old files deleted.

### Gaps

| Feature | LevelDB/RocksDB | Strata |
|---------|-----------------|--------|
| Level structure | Explicit L0-LN | Flat (size-tiered) |
| Non-overlapping guarantee | L1+ guaranteed | Not enforced |
| Read amplification | 1 file per level (with bloom) | All segments checked |
| Write amplification | 10-30x (leveled) | Lower (size-tiered) |
| Trivial moves | Metadata-only for non-overlapping | Not implemented |
| Compaction scoring | Per-level size ratio | Simple count threshold |
| Round-robin key selection | compact_pointer per level | Not implemented |
| Parallel compaction | Subcompactions | Sequential only |
| Compaction filter | TTL, custom drop logic | Via CompactionIterator pruning |

---

## 4. Write Path

### LevelDB Write Batch

```cpp
// WriteBatch format:
// 8 bytes sequence number
// 4 bytes count
// For each entry:
//   1 byte type (kTypeValue or kTypeDeletion)
//   varint key_length + key_bytes
//   [varint value_length + value_bytes]  (only for kTypeValue)
```

**Key optimization:** `BuildBatchGroup` coalesces multiple concurrent writers into a single WAL write and memtable apply. Leader writer does the work, followers wait.

### Strata Write Path

Each `kv_put` → `db.transaction()` → individual WAL record + storage write. No write coalescing.

**Gap:** LevelDB's `BuildBatchGroup` is a major throughput optimization for concurrent writers. We serialize every write independently.

### WAL Format

| Aspect | LevelDB | Strata |
|--------|---------|--------|
| Record format | 7-byte header (CRC + length + type) | Variable (CRC + txn_id + branch + timestamp + writeset) |
| Segmentation | Fixed 32KB blocks with record spanning | Variable-size segments (configurable) |
| Compression | None (WAL is sequential I/O) | None |
| Write coalescing | BuildBatchGroup combines writes | Individual records per transaction |

---

## 5. Memory Management

### RocksDB Defaults (from source)

```cpp
write_buffer_size = 64 << 20;      // 64MB per memtable
max_write_buffer_number = 2;        // 2 memtables total (~128MB)
block_cache = 32MB (internal);      // If no explicit cache set
db_write_buffer_size = 0;           // Disabled (no cross-CF limit)
```

### WiredTiger (MongoDB)

```
cache_size = max(256MB, 50% of RAM - 1GB)  // Unified cache
```

### Strata Defaults

```rust
write_buffer_size = 128MB           // Per-branch memtable
max_immutable_memtables = 4         // Max frozen before stalling
block_cache = 64MB                  // Global LRU (O(N) eviction!)
// No unified memory budget
```

### Gaps

| Feature | RocksDB | WiredTiger | Strata |
|---------|---------|------------|--------|
| Unified budget | WriteBufferManager | cache_size | **None** |
| Cache implementation | Sharded LRU (64 shards) | Clock eviction + dirty tracking | HashMap + min_by_key O(N) |
| Auto-sizing | Manual but recommended | `50% of RAM - 1GB` | Fixed 64MB |
| Memory accounting | Tracks memtable + cache + index/filter | Unified | Separate atomics |
| Write buffer + cache coordination | Dummy entries in cache | Single pool | Independent |

---

## 6. Prioritized Action Items

### P0 — Required for 10M+ Scale

1. **O(1) LRU block cache** (#1517 Phase 1)
   - Sharded doubly-linked-list LRU (like RocksDB)
   - Auto-sized to `max(256MB, RAM/4)`
   - **Why:** Current O(N) eviction makes large caches slower, not faster

2. **Leveled compaction with MANIFEST** (#1519)
   - Extend MANIFEST to track segment level assignments
   - L0 overlapping → L1+ non-overlapping
   - Adopt LevelDB's `PickCompaction` + `compact_pointer` round-robin
   - **Why:** Without this, reads check all segments (O(N) not O(1))

3. **Version-based segment tracking**
   - Immutable `SegmentVersion` with refcounting (like LevelDB's Version)
   - Atomic `LogAndApply` for compaction output installation
   - **Why:** Current mutable segment list has race conditions during compaction

### P1 — Important for Performance

4. **Restart points** (#1518)
   - With proper MVCC-aware backward scan
   - Property-based testing
   - **Why:** 4x fewer comparisons per block

5. **Write batch coalescing** (new issue needed)
   - Group concurrent writers like LevelDB's BuildBatchGroup
   - Single WAL write + memtable apply for N concurrent transactions
   - **Why:** Major throughput improvement for concurrent workloads

6. **Unified memory budget** (#1517 Phase 2-3)
   - WriteBufferManager-style coordination
   - Auto-size to available RAM
   - **Why:** Prevents OOM and optimizes RAM usage

### P2 — Nice to Have

7. **Prefix compression** in data blocks
8. **Separator key shortening** in index blocks
9. **Varint encoding** for entry headers
10. **Trivial compaction moves** (metadata-only for non-overlapping)
11. **Compaction scoring** (per-level size ratio)
12. **Backward iteration** support

---

## 7. Write Path & WAL

### Write Concurrency

| Aspect | LevelDB | RocksDB | Strata |
|--------|---------|---------|--------|
| Model | Single writer queue | Per-CF + leader-follower | Per-branch + blind-write fast path |
| Batching | `BuildBatchGroup` coalesces N writers into 1 WAL write | WriteThread pipelining | None — individual transactions |
| Lock hold | WAL lock included in critical section | WAL parallel to memtable insert | WAL record pre-serialized outside lock |
| Blind writes | Same cost as read-write | Same cost as read-write | **Lock-free** (skips validation + per-branch lock) |

**Key gap:** LevelDB's `BuildBatchGroup` combines concurrent writers into a single WAL I/O. We serialize every transaction independently. For high-concurrency workloads, this limits throughput.

### WAL Format

| Aspect | LevelDB | RocksDB | Strata |
|--------|---------|---------|--------|
| Record format | 7-byte header per 32KB block fragment | Extended with 2PC markers | Self-contained per-transaction record |
| Fragmentation | Records span 32KB block boundaries | Same as LevelDB | None — records never split |
| Recovery memory | O(total WAL) | Same | **O(largest_segment)** via streaming iterator |
| Segment skipping | Must scan all records | Same | **Skip closed segments by metadata** (`.meta` sidecars) |

**Strata advantage:** Streaming recovery with segment metadata skipping is 10x faster than LevelDB for large WALs.

### Write Stalling

| Aspect | LevelDB | RocksDB | Strata |
|--------|---------|---------|--------|
| Trigger | L0 files >= 4 (slowdown), >= 20 (stop) | Per-CF metrics, heuristic delays | `frozen.len() >= max_immutable_memtables` |
| Mechanism | 1ms sleep per write (slowdown) or block | Configurable delays + queuing | Skip rotation, let active memtable grow |
| Backpressure signal | Implicit (write latency increases) | Explicit (WriteController) | Implicit (memtable grows unbounded) |

**Gap:** No soft-throttle mechanism. LevelDB adds 1ms delay per write when L0 gets large, giving compaction time to catch up. We either rotate freely or hard-stall.

---

## 8. What Strata Does Better

1. **Built-in MVCC** — LevelDB requires app-layer version management
2. **Timestamp + TTL per entry** — LevelDB has no native time support
3. **Per-branch isolation** — DashMap sharding gives better write concurrency than LevelDB's single mutex
4. **Blind-write fast path** — Lock-free commits for write-only transactions (LevelDB/RocksDB serialize all writes)
5. **Zero-copy block scan** — `EntryHeaderRef` borrows from block data, avoiding allocation for non-matching entries
6. **Streaming WAL recovery** — O(largest_segment) memory vs O(total_WAL) for LevelDB/RocksDB
7. **Segment metadata skipping** — Recovery can skip closed WAL segments by txn_id range
8. **Narrow WAL critical section** — Pre-serialization outside lock reduces contention
9. **Format versioning** — explicit version field enables safe evolution
10. **O(1) time_range** — atomic min/max timestamps updated on every write

---

## 9. Complete Gap Analysis

### P0 — Required for 10M+ Scale

| # | Gap | LevelDB/RocksDB Approach | Issue |
|---|-----|--------------------------|-------|
| 1 | O(N) LRU block cache | Sharded doubly-linked-list LRU (RocksDB: 64 shards) | #1517 |
| 2 | No level structure | L0 overlapping → L1+ non-overlapping, `PickCompaction` + scoring | #1519 |
| 3 | No version-based segment tracking | Immutable `Version` with refcounting, atomic `LogAndApply` | New issue needed |
| 4 | MANIFEST doesn't track segment levels | Append-only log of `VersionEdit` records with (level, file) pairs | Part of #1519 |

### P1 — Important for Performance

| # | Gap | LevelDB/RocksDB Approach | Issue |
|---|-----|--------------------------|-------|
| 5 | No restart points in blocks | Binary search on offset array, 16-entry intervals | #1518 |
| 6 | No write batch coalescing | `BuildBatchGroup` / WriteThread leader-follower | New issue needed |
| 7 | No unified memory budget | `WriteBufferManager` (RocksDB) / `cache_size = 50% RAM` (WiredTiger) | #1517 Phase 2-3 |
| 8 | No soft write throttle | 1ms delay per write when L0 large (LevelDB) | New issue needed |

### P2 — Nice to Have

| # | Gap | LevelDB/RocksDB Approach |
|---|-----|--------------------------|
| 9 | No prefix compression | Shared + delta key encoding |
| 10 | No separator key shortening | `FindShortestSeparator` in index |
| 11 | No varint encoding | Variable-length integers for headers |
| 12 | No trivial compaction moves | Metadata-only move for non-overlapping files |
| 13 | No compaction scoring | Per-level `size / target_size` ratio |
| 14 | No backward iteration | Restart point scan (Prev) |
| 15 | No per-segment compaction hints | `allowed_seeks`, `being_compacted` flags |

### Strata Advantages (not in LevelDB/RocksDB)

| # | Feature | Benefit |
|---|---------|---------|
| 1 | Built-in MVCC with commit_id ordering | No app-layer version management |
| 2 | Timestamp + TTL per entry | Native time-based expiration |
| 3 | Per-branch isolation (DashMap) | Better multi-tenant concurrency |
| 4 | Blind-write lock-free fast path | Lower latency for write-only txns |
| 5 | Streaming WAL recovery | 10x faster recovery for large WALs |
| 6 | Segment metadata skipping | Skip closed WAL segments by txn_id |
| 7 | O(1) time_range | Atomic min/max without scanning |
| 8 | Multi-model key types | Branch/space/type grouping in key encoding |
