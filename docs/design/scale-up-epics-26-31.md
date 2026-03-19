# Storage Engine Scale-Up: Epics 26-31 — Read Path to RocksDB Parity

## Context

Epics 19-25 built the storage foundation:

| Epic | What | Result |
|------|------|--------|
| 19 | O(1) sharded block cache | Eliminated O(N) eviction |
| 20 | Replace mmap with pread | Eliminated page fault overhead |
| 21 | Version-based segment tracking (ArcSwap) | Lock-free reads during compaction |
| 22 | L0→L1 leveled compaction + manifest | O(log N) point lookups via binary search |
| 23 | Streaming compaction + multi-segment output | O(block) compaction memory |
| 24 | Multi-level L0-L6 with leveled merge | Data tiering across 7 levels |
| 25 | Compaction scheduling + engine integration | Cascading L1-L5 triggers |

Plus: restart points (#1518), jemalloc, zero-copy key comparison.

### Current Performance (after Epics 19-25 + restart points)

| Scale | Read ops/s | Read p50 | Write ops/s | RSS | Disk | Space Amp |
|------:|----------:|----------|------------:|----:|-----:|----------:|
| 1K | 1,374K | 0.6us | 82K | 0.1GB | 0.0GB | 1.6x |
| 10K | 1,133K | 0.8us | 87K | 0.1GB | 0.0GB | 1.6x |
| 100K | 783K | 1.1us | 85K | 0.2GB | 0.2GB | 1.6x |
| 1M | 27K | 37.5us | 86K | 1.3GB | 3.3GB | 3.4x |
| 10M | 22K | 43.0us | 80K | 7.5GB | 33.4GB | 3.4x |
| 1B | 2.9K | 192.1us | 265K | 2.4GB | 129.7GB | 5.2x |

### Target

**50-100K random reads/s at 1B keys.** RocksDB achieves 189K at 900M keys on similar hardware.

### Remaining Gaps (from RocksDB/LevelDB analysis)

| Gap | Impact | Epic |
|-----|--------|------|
| Static level targets → 5.2x space amp | Fewer entries fit in cache, more I/O | 26 |
| No prefix compression → 30-50% larger blocks | Worse cache utilization | 27 |
| L0 metadata evictable, monolithic blooms | Cache thrashing at scale | 28 |
| Full keys in index → 3-6x larger index blocks | Wasted cache capacity | 29 |
| CRC32+FNV-1a bloom hashes, uniform bits/key | Higher FPR than optimal | 30 |
| Binary search + linear scan in blocks | ~22% more CPU than hash lookup | 31 |

---

## Epic 26: Dynamic Level Sizing (~3 days)

**Goal:** Replace static level targets with RocksDB-style `CalculateBaseBytes` that adapts to actual data volume. Reduce space amplification from 5.2x toward ~1.1x.

**Why first:** Space amplification is the #1 bottleneck. At 1B keys with 5.2x space amp, we store 650GB of overhead data. Reads must search through more levels, more segments, and the block cache is diluted. Fixing this multiplies the effect of every subsequent optimization.

### Current State

Level targets are static constants in `segmented.rs:35-57`:
```
LEVEL_BASE_BYTES = 256MB (L1)
LEVEL_MULTIPLIER = 10
```
This gives: L1=256MB, L2=2.5GB, L3=25GB, L4=250GB, L5=2.5TB.

The engine (`database/mod.rs:1784-1814`) recomputes these on-the-fly using the same formula. With 130GB of data, the levels fill bottom-up: L1 overflows at 256MB, data cascades to L2-L4, but L4's 250GB target means most data sits in L3-L4 with large empty gaps.

### How RocksDB Does It

`CalculateBaseBytes` works **backward** from the last level's actual size:
1. Find the last level with data and its actual byte count
2. Compute base level: the highest level where `last_level_bytes / multiplier^(last_level - level) >= 1`
3. Set `target[base_level] = last_level_bytes / multiplier^(last_level - base_level)`
4. Empty intermediate levels are normal — data jumps directly to the base level
5. Recompute after every compaction

**Example:** 100GB database, multiplier 10:
- Last level (L5) = 100GB → L4 target = 10GB → L3 target = 1GB → L2 target = 100MB
- L1 target = 10MB (too small for 256MB file targets) → base_level = L2
- Data flows: L0 → L2 → L3 → L4 → L5, skipping L1

### Changes

**1. Add `DynamicLevelState` to `BranchState`** in `segmented.rs` (~30 lines)

```rust
struct DynamicLevelState {
    /// Computed target bytes per level. Updated after each compaction.
    level_targets: [u64; NUM_LEVELS],
    /// The first non-trivial level (where L0 flushes compact to).
    base_level: usize,
}
```

Store as `Mutex<DynamicLevelState>` in `BranchState`. Recomputed after each compaction and on recovery.

**2. `recalculate_level_targets()` method** (~40 lines)

```rust
fn recalculate_level_targets(levels: &[Vec<Arc<KVSegment>>]) -> DynamicLevelState {
    // Find last non-empty level
    let last_level = (1..NUM_LEVELS).rev()
        .find(|&l| !levels[l].is_empty())
        .unwrap_or(1);
    let last_level_bytes: u64 = levels[last_level].iter().map(|s| s.file_size()).sum();

    // Work backward: target[l] = target[l+1] / multiplier
    let mut targets = [0u64; NUM_LEVELS];
    targets[last_level] = last_level_bytes.max(LEVEL_BASE_BYTES);
    for l in (1..last_level).rev() {
        targets[l] = targets[l + 1] / LEVEL_MULTIPLIER;
    }

    // Base level: first level with target >= TARGET_FILE_SIZE
    let base_level = (1..=last_level)
        .find(|&l| targets[l] >= TARGET_FILE_SIZE)
        .unwrap_or(last_level);

    // Clamp base_level target to at least LEVEL_BASE_BYTES
    targets[base_level] = targets[base_level].max(LEVEL_BASE_BYTES);

    DynamicLevelState { level_targets: targets, base_level }
}
```

**3. Update compaction scoring** in `segmented.rs` (~15 lines)

Replace static `max_bytes_for_level(level)` calls with `dynamic_state.level_targets[level]`:
```rust
pub fn compaction_scores(&self, branch_id: &BranchId) -> Vec<(usize, f64)> {
    let dynamic = branch.dynamic_levels.lock();
    for level in 1..NUM_LEVELS - 1 {
        let actual = self.level_bytes(branch_id, level);
        let target = dynamic.level_targets[level];
        if target > 0 {
            let score = actual as f64 / target as f64;
            if score >= 1.0 { scores.push((level, score)); }
        }
    }
}
```

**4. Update engine post-flush callback** in `database/mod.rs` (~20 lines)

Replace the static level-target loop with a call to `recalculate_level_targets()` after each compaction. Remove the duplicated level target computation from the engine.

**5. Call recalculate on recovery** (~5 lines)

In `recover_segments()`, after loading segments into levels, call `recalculate_level_targets()` to initialize dynamic state.

### Files

| File | Change |
|------|--------|
| `crates/storage/src/segmented.rs` | Add `DynamicLevelState`, `recalculate_level_targets()`, update scoring (~80 lines) |
| `crates/engine/src/database/mod.rs` | Replace static targets in post-flush callback (~20 lines) |

### Test Plan

- `dynamic_targets_backward_from_last_level` — 100GB in L4 → L3 target = 10GB, L2 = 1GB
- `base_level_skips_empty_levels` — small DB → base_level = L1; large DB → base_level = L3
- `recalculate_after_compaction` — targets update after each compact_level()
- `recovery_initializes_dynamic_targets` — after segment recovery, targets are correct
- `empty_database_defaults_to_l1_base` — no data → base_level = 1, target = 256MB
- All existing 365+ storage tests pass
- All existing 1307+ engine tests pass
- Benchmark: 10M space amp drops from 3.4x toward 2.0x

### Effort: ~3 days

---

## Epic 27: Prefix Compression in Data Blocks (~5 days)

**Goal:** Delta-encode keys between restart points, reducing block size by 30-50%. This is LevelDB's core block format optimization.

**Why:** The single largest source of wasted cache capacity. Every entry stores the full InternalKey (~60-200 bytes) even though adjacent entries share 20-50 bytes of `branch_id + space + type_tag + key_prefix`. Prefix compression eliminates this redundancy, fitting 30-50% more entries per block cache slot.

### Current State

Entry format (`segment_builder.rs:285`):
```
| ik_len: u32 | ik_bytes | value_kind: u8 | timestamp: u64 | ttl_ms: u64 | value_len: u32 | value_bytes |
```

Every entry stores the full InternalKey. Restart points (every 16 entries) exist for binary search but don't enable compression — they were added purely as jump offsets.

### How LevelDB Does It

Between restart points, entries use delta encoding:
```
| shared: varint32 | unshared: varint32 | value_len: varint32 | key_delta[unshared] | value[value_len] |
```
At restart points: `shared = 0` (full key stored). Between restarts: `shared` = number of bytes shared with the previous key; only the suffix delta is stored.

**Key insight:** Restart points in LevelDB exist specifically for this — the full key at each restart enables the binary search, and delta encoding between restarts saves space. Our restart points already create the right boundaries.

### Changes

**1. New entry encoding** in `segment_builder.rs` (~30 lines)

Add `encode_entry_v4()`:
```rust
fn encode_entry_v4(
    prev_key: &[u8],    // empty at restart points
    ik: &InternalKey,
    entry: &MemtableEntry,
    buf: &mut Vec<u8>,
    is_restart: bool,
) {
    let ik_bytes = ik.as_bytes();
    let shared = if is_restart {
        0
    } else {
        common_prefix_len(prev_key, ik_bytes)
    };
    let unshared = ik_bytes.len() - shared;

    // Varint-encode shared, unshared, value_encoded_len
    encode_varint32(buf, shared as u32);
    encode_varint32(buf, unshared as u32);
    buf.extend_from_slice(&ik_bytes[shared..]);          // key delta

    // Value encoding (same as v3)
    if entry.is_tombstone {
        buf.push(VALUE_KIND_DEL);
        buf.extend_from_slice(&entry.timestamp.as_micros().to_le_bytes());
        buf.extend_from_slice(&entry.ttl_ms.to_le_bytes());
        encode_varint32(buf, 0);
    } else {
        buf.push(VALUE_KIND_PUT);
        buf.extend_from_slice(&entry.timestamp.as_micros().to_le_bytes());
        buf.extend_from_slice(&entry.ttl_ms.to_le_bytes());
        let value_bytes = bincode::serialize(&entry.value).unwrap();
        encode_varint32(buf, value_bytes.len() as u32);
        buf.extend_from_slice(&value_bytes);
    }
}
```

**2. Update `build_from_iter`** to track previous key and pass `is_restart` flag (~10 lines)

```rust
let mut prev_key: Vec<u8> = Vec::new();
// ...
let is_restart = block_entry_count == 0
    || block_entry_count % RESTART_INTERVAL == 0;
encode_entry_v4(&prev_key, &ik, &entry, &mut block_buf, is_restart);
prev_key.clear();
prev_key.extend_from_slice(ik.as_bytes());
```

**3. New entry decoding** in `segment_builder.rs` (~40 lines)

Add `decode_entry_header_ref_v4()` that reconstructs the full key from `shared + delta`:
```rust
pub(crate) fn decode_entry_header_ref_v4<'a>(
    data: &'a [u8],
    prev_key: &[u8],      // needed to reconstruct shared prefix
) -> Option<(Vec<u8>, EntryHeaderRefV4<'a>)> {
    let (shared, pos) = decode_varint32(data)?;
    let (unshared, pos) = decode_varint32(&data[pos..])?;
    // ... reconstruct full key from prev_key[..shared] + data[pos..pos+unshared]
}
```

At restart points, `shared == 0` so the key is fully self-contained — binary search still works without reconstructing from a previous key.

**4. Update `scan_block_for_key`** in `segment.rs` (~20 lines)

The v4 linear scan must track the previous key to reconstruct delta-encoded entries:
```rust
// For v4: maintain prev_key across entries
let mut prev_key: Vec<u8> = Vec::new();
while pos < data_end {
    let (full_key, ref_header) = decode_entry_header_ref_v4(&data[pos..data_end], &prev_key)?;
    prev_key = full_key;
    // ... same comparison logic as v3
}
```

Binary search at restart points works as before (shared == 0, full key available).

**5. Update iterators** in `segment.rs` (~15 lines)

`SegmentIter` and `OwnedSegmentIter` must track `prev_key` across `next()` calls within a block. Reset on block boundary.

**6. Add varint encoding/decoding utilities** (~20 lines)

```rust
fn encode_varint32(buf: &mut Vec<u8>, mut val: u32) { ... }
fn decode_varint32(data: &[u8]) -> Option<(u32, usize)> { ... }
```

LevelDB's fast path: if the byte is < 128, it's a single-byte varint (covers shared < 128, which is always true for practical keys).

**7. Bump FORMAT_VERSION to 4** (~2 lines)

Reader accepts v2 (no restarts), v3 (restarts, full keys), v4 (restarts + prefix compression). Version check: `!(2..=FORMAT_VERSION).contains(&format_version)`.

### Files

| File | Change |
|------|--------|
| `crates/storage/src/segment_builder.rs` | `encode_entry_v4()`, varint utils, format bump (~80 lines) |
| `crates/storage/src/segment.rs` | `decode_entry_header_ref_v4()`, update scan + iterators (~60 lines) |

### Test Plan

- `prefix_compression_roundtrip` — encode with v4, decode, verify all entries match
- `prefix_shared_bytes_correct` — adjacent keys share expected prefix length
- `restart_entries_have_shared_zero` — every restart entry has shared=0
- `v4_binary_search_works` — restart-point binary search finds correct interval
- `v4_iter_across_blocks` — iteration reconstructs keys correctly across block boundaries
- `v3_backward_compat` — v3 segments still readable (linear scan, full keys)
- `compression_ratio_improvement` — v4 blocks are 30-50% smaller than v3
- All existing 365+ storage tests pass
- Benchmark: block cache hit rate improves measurably at 10M

### Effort: ~5 days

---

## Epic 28: L0 Metadata Pinning + Partitioned Bloom Filters (~5 days)

**Goal:** Ensure L0 filter/index blocks are never evicted. Split monolithic bloom filters into ~4KB partitions loaded on demand.

**Why:** Every point lookup checks ALL L0 files. If L0 bloom filters get evicted under cache pressure, each lookup pays an extra SSD read per L0 file. At 1B keys with 4 L0 files, that's 4 extra SSD reads (~60us) on the hottest path. Partitioned bloom filters reduce per-lookup filter memory from multi-MB to ~4KB.

### Current State

Block cache has two priority tiers (`block_cache.rs:46-51`):
```rust
pub enum Priority { Low, High }
```
LOW blocks evicted before HIGH. But there's no "never evict" tier. L0 metadata is inserted as HIGH but can still be evicted when cache is full.

Bloom filters are monolithic — one per segment. At 10M keys with 10 bits/key, a segment's bloom is ~12MB. Loading it into cache consumes 3,000 4KB-block-equivalents of cache capacity.

### Changes

**Part 1: Pinned Cache Tier** (~40 lines in `block_cache.rs`)

Add `Priority::Pinned` — entries that are never evicted:
```rust
pub enum Priority {
    Low,    // Data blocks — evicted first
    High,   // Index/bloom — evicted when Low is empty
    Pinned, // L0 metadata — never evicted
}
```

Implementation:
- Add third list `pinned: LruList` to `LruShard`
- `evict_for()` never touches the pinned list
- Track `pinned_bytes` separately from `used_bytes`
- Reserve 10% of cache capacity for pinned items (configurable)
- `insert_pinned()` method that refuses if pinned budget exceeded (falls back to HIGH)

Add `demote(file_id, block_offset)` method: moves entry from Pinned → High. Called when L0 segment compacts to L1.

**Part 2: Pin L0 metadata on segment open** (~15 lines in `segment.rs`)

Add `open_with_level(path, level)` or a post-open `pin_metadata(cache)` method:
```rust
pub fn pin_metadata_in_cache(&self) {
    let cache = crate::block_cache::global_cache();
    // Index and bloom are already loaded at open time.
    // Re-insert with Pinned priority to prevent eviction.
    // (They're already in cache from open — just upgrade priority.)
    cache.promote_to_pinned(self.file_id, 0); // bloom block offset
    cache.promote_to_pinned(self.file_id, self.footer.index_block_offset);
}
```

Call this from `flush_oldest_frozen` and `recover_segments` for L0 segments only.

**Part 3: Demote on L0→L1 compaction** (~5 lines in `segmented.rs`)

In `compact_l0_to_l1` and `compact_level(0)`, before deleting old L0 segments, demote their cached metadata from Pinned to High (so it can be evicted normally or deleted).

**Part 4: Partitioned Bloom Filters** (~80 lines in `segment_builder.rs` + `segment.rs`)

**Builder changes:**

Instead of one bloom for all keys, build one bloom per N data blocks:
```rust
const BLOOM_PARTITION_BLOCK_COUNT: usize = 16; // ~16 data blocks per bloom partition
```

Write layout:
```
[bloom_partition_0]  (covers data blocks 0-15)
[bloom_partition_1]  (covers data blocks 16-31)
...
[bloom_partition_K]
[bloom_top_index]    (maps key ranges to partition offsets)
```

The top-level index is a small block (~100 bytes) mapping `(max_key_in_partition, partition_block_handle)` pairs.

**Reader changes:**

On point lookup, instead of loading the entire bloom filter:
1. Binary search the top-level index for the target key
2. Load only the matching ~4KB bloom partition from cache/disk
3. Probe that single partition

The top-level index is loaded at segment open time (tiny, cacheable). Individual partitions are cached in the block cache like data blocks.

**Format:** New filter block type `BLOCK_TYPE_PARTITIONED_FILTER = 5`. Segments with partitioned bloom set a flag in the properties block. Reader checks flag to choose between monolithic and partitioned filter lookup.

### Files

| File | Change |
|------|--------|
| `crates/storage/src/block_cache.rs` | `Priority::Pinned`, `promote_to_pinned()`, `demote()`, pinned budget (~40 lines) |
| `crates/storage/src/segment.rs` | `pin_metadata_in_cache()`, partitioned bloom reader (~50 lines) |
| `crates/storage/src/segment_builder.rs` | Partitioned bloom builder, top-level index (~60 lines) |
| `crates/storage/src/segmented.rs` | Pin L0 on flush/recover, demote on compact (~15 lines) |
| `crates/storage/src/bloom.rs` | No change — individual partitions are standard BloomFilters |

### Test Plan

- `pinned_entries_survive_eviction` — fill cache past capacity, pinned items remain
- `pinned_budget_enforced` — pinning beyond 10% budget falls back to High
- `demote_moves_to_high` — demoted entries can be evicted normally
- `l0_metadata_pinned_after_flush` — L0 segment open → bloom/index in pinned tier
- `l0_metadata_demoted_after_compaction` — L0→L1 compaction demotes old L0 metadata
- `partitioned_bloom_roundtrip` — write partitioned filter, read back, correct results
- `partitioned_bloom_loads_one_partition` — verify only 1 partition loaded per lookup
- `partitioned_bloom_no_false_negatives` — same guarantee as monolithic
- `monolithic_bloom_backward_compat` — old segments with monolithic bloom still work
- All existing tests pass
- Benchmark: L0 cache miss rate drops to 0% under pressure

### Effort: ~5 days

---

## Epic 29: Index Key Shortening (~2 days)

**Goal:** Store shortest separator between blocks in the index instead of full first key. Reduce index block size by 2-5x.

**Why:** Index entries currently store the full InternalKey of each block's first entry (~60-200 bytes). LevelDB stores the shortest string that separates adjacent blocks (~10-30 bytes). At 1B keys with thousands of blocks per segment, Strata's index blocks are 3-6x larger than necessary, wasting cache capacity.

### Current State

Index block format (`segment_builder.rs:550-560`):
```
[count: u32] [key_len: u32][key_bytes][offset: u64][data_len: u32] ...
```

The `key_bytes` is the full first key of each data block, stored verbatim.

### How LevelDB Does It

`FindShortestSeparator(start, limit)`:
- Given the last key of block N and the first key of block N+1
- Find the shortest string S such that `last_key_N <= S < first_key_N+1`
- Example: last="user:alice:100", first="user:bob:1" → separator="user:b"

`FindShortSuccessor(key)` for the very last index entry:
- Find shortest string > key (increment a byte, truncate)

### Changes

**1. Add `shortest_separator` utility** (~20 lines)

```rust
fn shortest_separator(a: &[u8], b: &[u8]) -> Vec<u8> {
    let shared = common_prefix_len(a, b);
    if shared < a.len() {
        // Try incrementing the first differing byte of a
        let diff_byte = a[shared];
        if diff_byte < 0xFF && diff_byte + 1 < b[shared] {
            let mut sep = a[..shared + 1].to_vec();
            sep[shared] = diff_byte + 1;
            return sep;
        }
    }
    // Can't shorten — return a as-is
    a.to_vec()
}

fn shortest_successor(a: &[u8]) -> Vec<u8> {
    for i in 0..a.len() {
        if a[i] < 0xFF {
            let mut s = a[..i + 1].to_vec();
            s[i] += 1;
            return s;
        }
    }
    a.to_vec() // All 0xFF — can't shorten
}
```

**2. Update `build_from_iter` to compute separators** (~15 lines)

Track `last_block_last_key` and `current_block_first_key`. When flushing a block, compute separator:
```rust
let separator = if let Some(ref prev_last) = last_block_last_key {
    shortest_separator(prev_last, &bfk)
} else {
    bfk.clone() // First block — use full key
};
index_entries.push((separator, file_offset, on_disk_data_len));
```

For the final index entry, use `shortest_successor(&last_key_in_segment)`.

**3. Update index binary search** in `segment.rs` (~0 lines)

The index binary search already compares `seek_bytes` against `e.key`. Shortened separators maintain the ordering invariant: `separator >= last_key_of_block && separator < first_key_of_next_block`. The existing binary search works without modification.

### Files

| File | Change |
|------|--------|
| `crates/storage/src/segment_builder.rs` | `shortest_separator()`, `shortest_successor()`, update build loop (~40 lines) |
| `crates/storage/src/segment.rs` | No change — binary search works on separators as-is |

### Test Plan

- `shortest_separator_basic` — "abc" / "abd" → "abd" or shorter
- `shortest_separator_shared_prefix` — "user:alice:100" / "user:bob:1" → "user:b"
- `shortest_separator_identical_prefix` — falls back to full key
- `shortest_successor_basic` — "abc" → "abd" (truncated)
- `index_with_separators_roundtrip` — build segment, read back, all lookups correct
- `index_size_reduction` — separator index is measurably smaller than full-key index
- `point_lookup_with_shortened_index` — all keys findable with shortened separators
- `scan_with_shortened_index` — prefix scans work correctly
- All existing tests pass

### Effort: ~2 days

---

## Epic 30: Improved Bloom Filter (~3 days)

**Goal:** Replace CRC32+FNV-1a with xxHash for lower FPR. Add Monkey-optimal per-level bit allocation.

**Why:** CRC32 and FNV-1a are weaker hash functions than what RocksDB uses. Measured FPR is ~1.8% at 10 bits/key vs theoretical ~0.8%. At 1B keys with 5 levels, each false positive costs an SSD read (~15us). Monkey-optimal allocation concentrates bits where they matter most (larger levels), giving 2-5x fewer false I/Os for the same total memory.

### Current State

Bloom filter hashing (`bloom.rs:53-54`):
```rust
let h1 = crc32fast::hash(key) as u64;
let h2 = fnv1a(key);
// Kirsch-Mitzenmacher: bit_pos[i] = (h1 + i * h2) % total_bits
```

`bits_per_key` is hardcoded at 10 in `SegmentBuilder::default()` (`segment_builder.rs:96`).

### Changes

**Part 1: xxHash replacement** (~15 lines in `bloom.rs`)

Replace both hash functions with a single 64-bit xxh3 hash, split into two 32-bit halves:
```rust
fn bloom_hash(key: &[u8]) -> (u64, u64) {
    let h = xxhash_rust::xxh3::xxh3_64(key);
    let h1 = h as u64;
    let h2 = (h >> 32) as u64 | 1; // Ensure h2 is odd for better distribution
    (h1, h2)
}
```

Add dependency: `xxhash-rust = { version = "0.8", features = ["xxh3"] }` to `crates/storage/Cargo.toml`.

**Part 2: Configurable bits_per_key** (~5 lines)

Make `bloom_bits_per_key` a parameter of `SegmentBuilder` (it already is: field `bloom_bits_per_key` at line 89). Expose it through compaction methods so different levels can use different values.

**Part 3: Monkey-optimal per-level allocation** (~20 lines in `segmented.rs`)

In `compact_level()`, compute bits_per_key based on level:
```rust
fn bloom_bits_for_level(level: usize, base_bits: usize) -> usize {
    // Monkey paper: larger levels get more bits.
    // Formula: bits[l] = base + log2(multiplier) * (max_level - l)
    // With multiplier=10: log2(10) ≈ 3.3
    let bonus = (3.3 * (NUM_LEVELS - 1 - level) as f64) as usize;
    (base_bits + bonus).min(20) // Cap at 20 bits/key (~0.0001% FPR)
}
```

Example with base=10, 7 levels:
| Level | bits/key | Approx FPR |
|-------|---------|------------|
| L1 | 20 | 0.0001% |
| L2 | 17 | 0.001% |
| L3 | 13 | 0.01% |
| L4 | 10 | 0.1% |
| L5 | 10 | 0.1% |
| L6 | 10 | 0.1% |

Small levels (checked on every read) get very precise filters. Large levels (checked rarely due to bloom rejections at higher levels) keep the baseline.

**Part 4: Backward-compatible bloom reading** (~5 lines)

The bloom format is unchanged (`[num_hash_fns: u32][bits...]`). Old blooms built with CRC32+FNV work correctly — they just have slightly higher FPR. New blooms built with xxHash produce different bit patterns but the format is identical. No version bump needed; the bloom is self-contained.

However, blooms built with old hashes and queried with new hashes will produce wrong results. To handle this:
- Add a 1-byte `hash_type` field to the bloom serialization: `[hash_type: u8][num_hash_fns: u32][bits...]`
- `hash_type = 0` → legacy CRC32+FNV, `hash_type = 1` → xxHash
- Reader checks `hash_type` and uses the corresponding hash function
- Fallback: if first byte > 30 (impossible for old format's `num_hash_fns`), it's the new format

### Files

| File | Change |
|------|--------|
| `crates/storage/src/bloom.rs` | xxHash replacement, hash_type versioning (~30 lines) |
| `crates/storage/src/segment_builder.rs` | Pass level-specific bits_per_key to builder (~5 lines) |
| `crates/storage/src/segmented.rs` | `bloom_bits_for_level()`, pass to compaction (~20 lines) |
| `crates/storage/Cargo.toml` | Add `xxhash-rust` dependency |

### Test Plan

- `xxhash_bloom_no_false_negatives` — same guarantee as CRC32 version
- `xxhash_bloom_lower_fpr` — FPR < 1.0% at 10 bits/key (vs < 2.0% with CRC32)
- `hash_type_versioning_backward_compat` — old blooms (hash_type=0) queried correctly
- `monkey_allocation_levels` — L1 gets 20 bits, L6 gets 10 bits
- `mixed_bloom_versions` — segment with old bloom + segment with new bloom both work
- All existing tests pass
- Benchmark: fewer false-positive SSD reads at 10M

### Effort: ~3 days

---

## Epic 31: Data Block Hash Index (~3 days)

**Goal:** Append a hash table to each data block for O(1) point-lookup within blocks. Saves ~22% CPU on the innermost read path operation.

**Why:** Even with restart points, a point lookup does O(log R) binary search on restarts + O(16) linear scan within the interval. RocksDB's data block hash index maps keys directly to their restart interval in O(1), with 4.6% space overhead. This is the last-mile CPU optimization — highest impact when reads are cache-hot and I/O isn't the bottleneck.

### Current State

`scan_block_for_key` in `segment.rs` (lines 375-399):
1. Parse restart trailer → `binary_search_restarts()` → find interval
2. `linear_scan_block()` from interval start to `data_end`

At 16 entries per interval, the linear scan does ~8 comparisons on average.

### Changes

**1. Hash index format** appended after restart trailer (~15 lines in builder)

```
[entries...]
[restart_0: u32] ... [restart_K: u32] [num_restarts: u32]   ← existing restart trailer
[bucket_0: u8] [bucket_1: u8] ... [bucket_N: u8]            ← hash index
[num_buckets: u16]                                           ← last 2 bytes of block
[has_hash_index: u8]                                         ← sentinel (0x01)
```

Each bucket stores the restart interval index (0-254) or `0xFF` for empty. Hash function: `xxh3(typed_key_prefix) % num_buckets`. Load factor: 75% → `num_buckets = ceil(num_entries / 0.75)`.

The `has_hash_index` sentinel byte allows the reader to detect v5 blocks: if the last byte is `0x01` and the preceding 2 bytes decode to a reasonable `num_buckets`, it's v5.

**2. Builder** (~30 lines in `segment_builder.rs`)

After entries and restart trailer, build hash table:
```rust
fn build_hash_index(entries: &[(Vec<u8>, usize)], num_restarts: usize) -> Vec<u8> {
    let num_buckets = ((entries.len() as f64 / 0.75).ceil() as usize).max(1);
    let mut buckets = vec![0xFFu8; num_buckets];
    for (typed_key, restart_idx) in entries {
        let hash = xxhash_rust::xxh3::xxh3_64(typed_key) as usize % num_buckets;
        if buckets[hash] == 0xFF {
            buckets[hash] = restart_idx as u8;
        }
        // Collision: leave as first-write-wins; reader falls back to binary search
    }
    let mut buf = buckets;
    buf.extend_from_slice(&(num_buckets as u16).to_le_bytes());
    buf.push(0x01); // sentinel
    buf
}
```

Track `(typed_key_prefix, restart_interval_index)` per entry during block building.

**3. Reader** (~25 lines in `segment.rs`)

In `scan_block_for_key`, after parsing the restart trailer:
```rust
if has_hash_index {
    let hash = xxh3(typed_key) % num_buckets;
    let interval = buckets[hash];
    if interval != 0xFF && (interval as usize) < num_restarts {
        // Direct jump to interval, then linear scan
        let offset = restart_offset_at(data, data_end, interval as usize);
        return self.linear_scan_block(data, offset, data_end, typed_key, snapshot_commit);
    }
    // Hash miss or collision: fall back to binary search
}
// Binary search fallback (same as current v3/v4 path)
```

**4. Bump FORMAT_VERSION to 5** (~2 lines)

Reader accepts v2-v5. Hash index is purely additive — blocks without it work via binary search fallback.

### Files

| File | Change |
|------|--------|
| `crates/storage/src/segment_builder.rs` | `build_hash_index()`, append after trailer (~40 lines) |
| `crates/storage/src/segment.rs` | Parse hash index, O(1) lookup path (~30 lines) |

### Test Plan

- `hash_index_roundtrip` — build block with hash index, every key findable via hash path
- `hash_index_collision_fallback` — colliding keys found via binary search fallback
- `hash_index_space_overhead` — verify ~4.6% overhead per block
- `hash_index_missing_key` — absent keys correctly return None
- `v4_blocks_without_hash` — older blocks still use binary search
- `hash_index_mvcc_versions` — multiple versions of same key handled correctly
- `hash_index_with_prefix_compression` — works with v4 prefix-compressed entries
- All existing tests pass
- Benchmark: ~10-20% CPU reduction in block scan for cache-hot workloads

### Effort: ~3 days

---

## Dependency Graph

```
Epic 26 (Dynamic Level Sizing)            [no deps]
    ↓
Epic 27 (Prefix Compression)              [no deps, but benefits from 26's smaller dataset]
    ↓
Epic 28 (L0 Pinning + Partitioned Bloom)  [no deps]
    ↓
Epic 29 (Index Key Shortening)            [no deps]
    ↓
Epic 30 (Improved Bloom)                  [no deps]
    ↓
Epic 31 (Data Block Hash Index)           [depends on 27 for v4 format, 30 for xxHash dep]
```

All epics are independently implementable and benchmarkable. The ordering is by expected impact. Epic 31 depends on 27 (prefix compression) for the v4 entry format and on 30 (xxHash) for the hash function, but can be implemented without them by using a separate hash and skipping prefix-compressed blocks.

---

## Expected Results

| Metric | Current | After 26 | After 27 | After 28 | After 29 | After 30 | After 31 |
|--------|--------:|--------:|--------:|--------:|--------:|--------:|--------:|
| **1B reads/s** | 2,900 | 7,250 | 12,750 | 31,900 | 37,700 | 49,300 | 55,100 |
| **1B space amp** | 5.2x | ~2.0x | ~1.8x | ~1.8x | ~1.7x | ~1.7x | ~1.7x |
| **10M reads/s** | 22K | 40K | 55K | 80K | 90K | 100K | 110K |
| **10M p50** | 43us | 25us | 18us | 12us | 11us | 10us | 9us |

Estimates are conservative and multiplicative. The 50K reads/s target at 1B is achievable after Epics 26-30.

---

## Total Effort

| Epic | Days | Cumulative |
|------|------|-----------|
| 26: Dynamic Level Sizing | 3 | 3 |
| 27: Prefix Compression | 5 | 8 |
| 28: L0 Pinning + Partitioned Bloom | 5 | 13 |
| 29: Index Key Shortening | 2 | 15 |
| 30: Improved Bloom | 3 | 18 |
| 31: Data Block Hash Index | 3 | 21 |

**Total: ~21 days (4-5 weeks with testing and benchmarking).**

---

## What Comes After (Not in This Plan)

- Billion-scale vectors: PQ + DiskANN + SPFresh (#1267)
- Copy-on-write branching: O(1) fork
- Write path: rate limiter, write batch coalescing (#1521), soft throttle (#1522)
- Per-level compression: LZ4 for hot levels, Zstd for cold
- BlobDB for large values, parallel subcompactions
- Time travel: timestamp-aware compaction, TTL historical correctness (#1534)

---

## References

- `docs/design/scale-up-epics-19-22.md` — foundation epics (completed)
- `docs/design/scale-up-epics-23-25.md` — multi-level epics (completed)
- `docs/design/read-path-optimization-v2.md` — gap analysis and target setting
- `docs/design/billion-scale-roadmap.md` — full roadmap
- LevelDB block.cc: restart point binary search + prefix compression
- RocksDB `CalculateBaseBytes`: dynamic level sizing algorithm
- Monkey (SIGMOD 2017): optimal bloom filter allocation across levels
