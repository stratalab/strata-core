# Part 6: Scale-Span Audit — Raspberry Pi Zero to Billion-Key Server

## Context

StrataDB's core product vision is **one binary that scales from a $15 Raspberry Pi Zero (512MB RAM, SD card, single-core ARM) to a bare-metal server (64GB+ RAM, NVMe, 32+ cores) running a billion keys.** Same API, same on-disk format, same ACID guarantees — tuned by `memory_budget` and `StorageConfig`.

This audit verifies that the architecture actually supports both extremes without being over-indexed on either. The question is not "does it work on a Pi Zero today" — it's "is there anything in the core architecture that *fundamentally prevents* it from working, or that would require an architectural rewrite rather than tuning?"

### Target Hardware Spectrum

| | Pi Zero (floor) | Pi Zero 2 W | Laptop (dev) | Server (ceiling) |
|---|---|---|---|---|
| **CPU** | ARM11 (ARMv6), 1 core | Cortex-A53 (ARMv8), 4 cores | x86_64, 8 cores | x86_64, 32+ cores |
| **RAM** | 512MB total (~350MB usable) | 512MB total (~350MB usable) | 16GB | 64-256GB |
| **Storage** | SD card (10-25 MB/s, limited write endurance) | SD card | NVMe SSD (3 GB/s) | NVMe RAID |
| **Word size** | 32-bit | 64-bit | 64-bit | 64-bit |
| **Workload** | 10K keys, 1-3 branches | 50K keys, 5 branches | 1M keys, 10 branches | 1B keys, 1000 branches |

### The `memory_budget` Principle

All memory-consuming structures should be configurable and derive their sizing from a single `memory_budget` parameter (or `StorageConfig` fields). A 64MB budget on a Pi means smaller block cache, smaller memtables, fewer bloom filter bits pinned — but the same algorithms, same format, same correctness.

---

## Audit Categories

### 1. HARDCODED CONSTANTS THAT ASSUME SERVER-CLASS HARDWARE

Find every hardcoded constant in the storage and engine crates. For each, assess whether the default
is appropriate for a Pi Zero, or if it would consume disproportionate resources.

**Known constants to examine:**

| Constant | Value | File | Pi Zero impact |
|----------|-------|------|----------------|
| `DEFAULT_CAPACITY_BYTES` (block cache) | 256 MiB | `block_cache.rs` | 73% of usable RAM |
| `auto_detect_capacity()` minimum | 256 MiB | `block_cache.rs` | Clamps to 256 MiB even if only 350MB available |
| `default_write_buffer_size` | 128 MiB | `config.rs` | 37% of usable RAM for ONE memtable |
| `TARGET_FILE_SIZE` | 64 MiB | `segmented/mod.rs` | A single segment file = 18% of RAM |
| `LEVEL_BASE_BYTES` (L1 target) | 256 MiB | `segmented/mod.rs` | 73% of usable RAM |
| `MAX_GRANDPARENT_OVERLAP` | 640 MiB | `segmented/mod.rs` | 183% of usable RAM |
| `MAX_BASE_BYTES` | 256 MiB | `segmented/mod.rs` | Clamp ceiling = 73% of RAM |
| `NUM_SHARDS` (block cache) | 16 | `block_cache.rs` | 16 shards for 1 core |
| `default_max_immutable_memtables` | 4 | `config.rs` | 4 × 128 MiB = 512 MiB pending |
| `BLOOM_PARTITION_BLOCK_COUNT` | 16 | `segment_builder.rs` | Fine |
| `INDEX_PARTITION_BLOCK_COUNT` | 128 | `segment_builder.rs` | Fine |
| `NUM_LEVELS` | 7 | `segmented/mod.rs` | 7 levels for 10K keys is wasteful but harmless |
| `L0_COMPACTION_TRIGGER` | 4 | `segmented/mod.rs` | Fine |
| `MAX_INHERITED_LAYERS` | 4 | `segmented/mod.rs` | Fine |

**Audit**: For each constant, answer:
1. Is it configurable via `StorageConfig` or `StrataConfig`, or hardcoded?
2. If hardcoded, does the auto-detection or default work at 350MB usable RAM?
3. If configurable, does the default cause OOM on a Pi Zero?
4. What should the Pi Zero value be?

### 2. MEMORY BUDGETING — IS THERE A SINGLE KNOB?

The vision says `memory_budget` controls everything. Verify whether this exists and whether it
actually propagates to all memory-consuming structures.

**Audit**:
- Find the `memory_budget` config field (if it exists). Trace how it flows to:
  - Block cache capacity
  - Write buffer size (memtable rotation threshold)
  - Maximum frozen memtables
  - Bloom filter memory (pinned partitions)
  - Index memory (pinned sub-indexes)
  - HNSW vector index memory
  - Search index memory
  - DashMap overhead (per-branch state)
- If `memory_budget` doesn't exist, find what knobs DO exist (`StorageConfig` fields) and assess
  whether a user on a Pi Zero can set them coherently without expert knowledge.
- Is there a preset/profile system? (e.g., `storage.profile = "embedded"` that sets all fields
  to Pi-friendly values)

### 3. 32-BIT ARCHITECTURE COMPATIBILITY (Pi Zero 1)

The original Pi Zero is 32-bit ARMv6. StrataDB uses `u64` extensively.

**Audit**:
- `AtomicU64`: Available on 32-bit ARM? On ARMv6, `AtomicU64` operations are emulated via locks
  (not lock-free). Every `fetch_add`, `load`, `store` on the version counter, memtable stats,
  block cache counters, and timestamp tracking becomes a mutex operation. Quantify how many
  `AtomicU64` operations occur per read and per write.
- `usize` vs `u64`: On 32-bit, `usize` is 4 bytes. Any code that casts `u64` to `usize` for
  memory allocation sizes could truncate. Find all `as usize` casts on u64 values. Are any of
  them for sizes that could exceed 4GB? (Not on a Pi, but the code should not panic.)
- Address space: 32-bit ARM has a 3GB user-space address space (with 1GB kernel). The block cache
  at 256 MiB + memtables + segment indexes + bloom filters could exceed this. Is there an
  address space budget?
- `mmap`: The vector mmap code (`mmap.rs`, `mmap_graph.rs`) has compile-time checks for little-endian
  (`#[cfg(not(target_endian = "little"))]`). ARM is little-endian, so this is fine. But are there
  any 64-bit alignment assumptions in the mmap code? On 32-bit, pointers are 4 bytes. Verify
  `u64` reinterpretation from mmap'd memory works on 32-bit.
- `crossbeam_skiplist`: Does it compile and work correctly on 32-bit ARM? It uses atomics
  extensively. Verify the Cargo.toml target compatibility.

### 4. SD CARD STORAGE CHARACTERISTICS

SD cards are fundamentally different from SSDs/HDDs:
- Random write: 0.1-1 MB/s (100-1000× slower than SSD)
- Sequential write: 10-25 MB/s
- Write endurance: 10K-100K erase cycles per block
- No TRIM support in most configurations
- Write amplification from the FTL (Flash Translation Layer)

**Audit**:
- **LSM write amplification**: Calculate the theoretical write amplification for the default config
  (L0 trigger=4, multiplier=10, 7 levels). Each byte of user data is written to: memtable → WAL →
  L0 segment → L1 (compaction) → L2 → ... → L6. With multiplier=10, worst-case write amp is
  ~10× per level × 6 levels = 60×. On a device with 100K write cycles and 32GB SD card, this
  translates to approximately how many total user bytes before the card wears out?
- **WAL write pattern**: Every commit writes a WAL record. In `Always` mode, every commit also
  fsyncs. On SD card, fsync can take 50-200ms. At what commit rate does WAL become the bottleneck?
  Is `Standard` mode's default interval (100ms) appropriate for SD card latency?
- **Compaction I/O**: Compaction reads all input segments and writes output segments. On a 10 MB/s
  SD card, compacting a 64MB segment takes ~13 seconds (read + write). During this time, the
  single core is largely blocked on I/O. Is there I/O prioritization? Does the `RateLimiter`
  help, or is it designed for fast storage?
- **Segment file size**: `TARGET_FILE_SIZE` is 64MB. On a Pi with 10K keys (average 100 bytes each),
  total data is ~1MB. A single segment file would be ~1MB, not 64MB. Is the splitting logic
  correct for very small datasets? Does it produce many tiny files instead?
- **Directory fsync**: After creating a new segment file, is the parent directory fsynced?
  On ext4 (common on Pi SD cards), this is required for crash safety. Verify.

### 5. SINGLE-CORE PERFORMANCE

The Pi Zero has ONE core. All concurrency primitives become overhead, not acceleration.

**Audit**:
- **DashMap**: 64 shards by default. On a single core, shard contention is zero but the memory
  overhead of 64 shard locks is wasted. Is the shard count configurable?
- **Block cache shards**: 16 shards with `parking_lot::RwLock`. Same issue — 16 locks for one core.
- **crossbeam SkipMap**: Lock-free data structure. On single core, lock-free is equivalent to
  single-threaded but with higher constant factors (atomic operations instead of plain loads/stores).
  The skip list has ~70 bytes overhead per node. For 10K entries, that's 700KB of overhead.
  Acceptable?
- **Compaction**: Is compaction synchronous (blocking the main thread) or asynchronous (background
  thread)? On a single core, a background compaction thread competes with user operations for the
  single core. Is there priority control?
- **Thread count**: How many threads does StrataDB spawn during normal operation? Background flush,
  compaction scheduler, WAL sync, search indexing? Each thread adds ~8MB stack + scheduling overhead.
  On a Pi Zero, more than 2-3 threads is counterproductive.

### 6. MEMORY OVERHEAD PER ENTITY

On a Pi Zero with 10K keys, per-entry overhead dominates. Calculate the actual memory cost per entry.

**Audit**:
- **Memtable entry**: `InternalKey` size + `MemtableEntry` size + SkipMap node overhead (~70 bytes).
  For a key "user:123" with value "hello", what is the total per-entry memory cost?
- **Segment index (pinned)**: Top-level index entries + sub-index entries are pinned in memory at
  segment open. For a segment with 10K entries, how much memory is pinned?
- **Bloom filter (pinned)**: Bloom partitions are pinned in memory. At 10 bits/key for 10K keys,
  that's ~12KB. Acceptable. But at 20 bits/key (upper levels), it's ~24KB. Still fine.
- **DashMap entry**: Each branch in `DashMap<BranchId, BranchState>` has overhead. BranchState
  includes: Memtable, Vec<Arc<Memtable>>, ArcSwap<SegmentVersion>, 2× AtomicU64, Vec<Option<Vec<u8>>>,
  LevelTargets, Vec<InheritedLayer>. For 3 branches, what's the baseline memory before any data?
- **Block cache entries**: Each cached block is wrapped in `ClockEntry` (Arc<Vec<u8>> + usize +
  Priority + AtomicU8). Per-entry overhead beyond the data itself?
- **Total baseline**: What is the minimum memory footprint of an empty StrataDB instance with
  default settings? (Block cache + DashMap + global structures + thread stacks)

### 7. LARGE-SCALE (BILLION-KEY) CONSTRAINTS

At the other extreme, does the architecture support 1 billion keys?

**Audit**:
- **Segment count**: With `TARGET_FILE_SIZE=64MB` and 1B keys at ~100 bytes each, total data is
  ~100GB. That's ~1600 segments across 7 levels. Each segment has pinned bloom filters and index
  blocks. How much memory is consumed by pinned metadata for 1600 segments?
- **DashMap with 1000 branches**: Each branch has its own BranchState with full segment version.
  1000 branches × 7 levels × average segments per level = how many Arc<KVSegment> references?
- **Block cache sizing**: At 4GB block cache with 4KB blocks, that's ~1M entries in the CLOCK cache.
  Is the HashMap per-shard efficient at ~62K entries per shard?
- **Compaction throughput**: At 100GB total data, L6 could be ~90GB. An L5→L6 compaction reads and
  writes tens of GB. Is there sub-compaction (parallelism within a single compaction)?
- **MergeIterator scalability**: The linear-scan MergeIterator is designed for "1-3 sources." At
  billion scale, a compaction could merge 4 L0 files + overlapping L1 files = potentially 10+
  sources. At what point does linear scan become a bottleneck vs a binary heap?
- **Global version counter**: At 1B keys with average 3 versions each, that's 3B version allocations.
  At `u64::MAX` capacity, this is not a concern numerically, but the `AtomicU64::fetch_add` on the
  version counter becomes a contention point at 32+ cores. Is there per-core batching?
- **File descriptor limits**: 1600 open segment files requires ulimit -n > 1600. Is this documented?
  Does the system degrade gracefully if it runs out of file descriptors?

### 8. CONFIG-DRIVEN SCALING

Verify that a single `strata.toml` (or equivalent) can tune the engine across the full range.

**Audit**:
- Write a `strata.toml` for Pi Zero (64MB budget):
  ```toml
  [storage]
  block_cache_size = 16_777_216    # 16 MiB
  write_buffer_size = 4_194_304    # 4 MiB
  max_immutable_memtables = 2
  max_versions_per_key = 3
  max_branches = 8
  ```
  Does this config actually work? Trace each value through to the storage engine. Are there
  hardcoded minimums that override these values? Does `auto_detect_capacity()` ignore
  `block_cache_size` if it's set?

- Write a `strata.toml` for billion-key server:
  ```toml
  [storage]
  block_cache_size = 17_179_869_184  # 16 GiB
  write_buffer_size = 268_435_456    # 256 MiB
  max_immutable_memtables = 8
  max_versions_per_key = 0           # unlimited
  max_branches = 0                   # unlimited
  ```
  Does this config work? Are there hardcoded maximums that cap these values?

- For each `StorageConfig` field, find where it's consumed in the engine. Is there a field that
  exists in config but is silently ignored in code?

### 9. ON-DISK FORMAT PORTABILITY

The same binary runs on both platforms. The on-disk format must be portable.

**Audit**:
- **Endianness**: Segment files, WAL records, and manifests use `to_le_bytes()` / `from_le_bytes()`.
  ARM is little-endian. x86 is little-endian. No issue. But verify there are no `to_ne_bytes()`
  (native endian) calls that would break cross-platform portability.
- **Pointer-size fields**: Are any fields in the on-disk format `usize`-sized (which would differ
  between 32-bit and 64-bit)? All on-disk sizes should be explicit (`u32`, `u64`), not `usize`.
  Find any `usize` serialization in segment/WAL/manifest formats.
- **Alignment**: The mmap graph format requires 8-byte alignment for u64 reinterpretation. On 32-bit
  ARM, is `mmap` guaranteed to return 8-byte-aligned addresses? (Usually yes, but verify.)
- **File format version**: If a database is created on a 64-bit server and then copied to a 32-bit
  Pi (or vice versa), will it open correctly? The format versions should be the same. Verify no
  platform-specific format differences.

### 10. FEATURE DEGRADATION STRATEGY

Not all features need to work at both extremes. But the degradation should be explicit, not a crash.

**Audit**:
- **Vector search**: HNSW indexes consume significant memory. On a Pi Zero with 10K 384-dim vectors
  (MiniLM), the embedding data alone is 10K × 384 × 4 bytes = ~15MB. The HNSW graph adds overhead.
  Is there a brute-force fallback for small collections that avoids building the graph?
- **Search index (BM25)**: The inverted index uses DashMap + mmap-backed sealed segments. On a Pi,
  is it meaningful to run search on 10K documents? Is there an option to disable the search index?
- **Auto-embed**: MiniLM model loading requires ~100MB of RAM. On a Pi with 350MB usable, this is
  28% of RAM. Is auto-embed configurable/disableable? (It is — `auto_embed: false`.)
- **COW branching**: COW fork is O(segments). On a Pi with few segments, this is cheap. But
  materialization iterates all inherited entries — on a Pi with SD card storage, this could be
  slow. Is there a way to skip materialization and tolerate deeper inherited layer chains?
- **Compaction**: On a Pi, compaction competes with user operations for the single core and the
  slow SD card. Is there a way to throttle compaction more aggressively? The `RateLimiter` exists
  but is it used for all compaction paths?

---

## Deliverables

For each finding, state:

1. **Scale end affected**: Pi Zero / Server / Both
2. **Category**: Which audit category (1-10)
3. **Severity**: Blocker (prevents operation), High (severe degradation), Medium (suboptimal), Low (minor)
4. **Description**: What the issue is
5. **Current behavior at the affected scale**: What happens today
6. **Recommended fix**: Concrete change (config default, code change, feature gate)

At the end, provide:

1. **Pi Zero readiness assessment**: Can StrataDB run on a Pi Zero today with correct config? If not, what are the blockers?

2. **Billion-key readiness assessment**: Can StrataDB handle 1B keys today? If not, what are the blockers?

3. **The scale-span gap**: What is the hardest problem in supporting both extremes simultaneously? Is it a config problem (different defaults) or an architectural problem (different algorithms needed)?

4. **Recommended `StorageConfig` presets**: Provide concrete configs for:
   - `embedded` (Pi Zero, 64MB budget)
   - `desktop` (laptop, 2GB budget)
   - `server` (dedicated, 16GB budget)

5. **Constants that must become configurable**: List every hardcoded constant that prevents scaling and recommend whether it should be a `StorageConfig` field, derived from `memory_budget`, or left hardcoded with a different default.
