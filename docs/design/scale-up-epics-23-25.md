# Storage Engine Scale-Up: Epics 23-25 for 100M Keys

## Context

Epics 19-22 built the foundation for scaled reads:

| Epic | What | Result |
|------|------|--------|
| 19 | O(1) sharded block cache | Eliminated repeated decompression |
| 20 | Replace mmap with pread | Eliminated page fault overhead |
| 21 | Version-based segment tracking (ArcSwap) | Lock-free reads during compaction |
| 22 | L0→L1 leveled compaction + manifest | O(log N) point lookups via binary search |

### Current Performance (after Epics 19-22)

| Scale | Read ops/s | Read p50 | Write ops/s | RSS | Disk | Space Amp |
|------:|----------:|----------|------------:|----:|-----:|----------:|
| 1K | 1,057K | 832ns | 106K | 8MB | 2MB | 1.6x |
| 10K | 898K | 951ns | 110K | 19MB | 16MB | 1.6x |
| 100K | 497K | 1.8us | 99K | 128MB | 157MB | 1.6x |
| 1M | 181K | 5.2us | 79K | 4.7GB | 3.6GB | 3.7x |
| 10M | 100K | 7.4us | 67K | 43.6GB | 38.6GB | 4.0x |

### Remaining Problems

1. **RSS 43.6GB at 10M** — compaction does `.collect()` on all segment entries, materializing 10GB of data in RAM before merging
2. **Space amp 4.0x** — L1 is one giant segment rewritten in full on every compaction; no data tiering
3. **Can't reach 100M** — single L1 would be ~100GB, compaction would OOM

All three trace to the same root cause: **single-level L1 with monolithic segments**. `compact_l0_to_l1` produces ONE output segment. At 10M that's ~10GB. Every subsequent L0→L1 compaction rewrites ALL of L1.

### Target Milestone

**100M keys: 50K+ reads/s, <3x space amp, <16GB RSS**

---

## Reference Implementation Analysis

Key algorithmic details extracted from LevelDB (`/tmp/leveldb`) and RocksDB (`/tmp/rocksdb`) source code.

### LevelDB Core Design

**Constants:**
```
NUM_LEVELS = 7
L0_COMPACTION_TRIGGER = 4 files
MAX_FILE_SIZE = 2MB per output file
Level sizes: L1=10MB, each next 10x (L2=100MB, L3=1GB, L4=10GB, L5=100GB, L6=1TB)
MaxGrandParentOverlapBytes = 10 * max_file_size (20MB)
ExpandedCompactionByteSizeLimit = 25 * max_file_size (50MB)
```

**Compaction scoring (`Finalize`):**
- L0: `score = file_count / L0_COMPACTION_TRIGGER` (file count, not bytes — because L0 files overlap)
- L1+: `score = total_bytes / max_bytes_for_level` (byte ratio — files are non-overlapping)
- Highest-scoring level compacts first
- Score >= 1.0 triggers compaction

**File selection (`PickCompaction`):**
- Size-triggered (score >= 1) takes priority over seek-triggered
- Pick first file AFTER `compact_pointer_[level]` (round-robin across key range)
- Wrap-around when all files are before the pointer
- L0 special case: expand to all overlapping L0 files

**Input gathering (`SetupOtherInputs`):**
1. Add boundary files (same user_key spanning file boundaries — critical for correctness)
2. Find overlapping files in L+1
3. Opportunistic expansion: try adding more L files if L+1 doesn't grow and total < 50MB
4. Track grandparent (L+2) files for output splitting

**Output file splitting (`ShouldStopBefore`):**
- Track cumulative overlap with grandparent (L+2) files as keys are written
- When overlap exceeds `MaxGrandParentOverlapBytes` (20MB) → close current output, start new file
- Prevents cascading compaction amplification at L+2

**Trivial move (`IsTrivialMove`):**
- 1 input file, 0 files overlapping in L+1, bounded grandparent overlap
- Metadata-only: just change the level assignment, no I/O
- Common case for fresh data flowing down through empty levels

**Garbage collection during compaction (`DoCompactionWork`):**
- Rule 1: Older version of same user_key below smallest snapshot → drop
- Rule 2: Deletion tombstone below smallest snapshot AND no data in higher levels (`IsBaseLevelForKey`) → drop
- `IsBaseLevelForKey` uses incremental `level_ptrs_[]` scan for O(N) total, not O(N²)

### RocksDB Extensions (for future reference)

**Calibrated for modern hardware:**
```
target_file_size_base = 64MB (vs LevelDB 2MB)
max_bytes_for_level_base = 256MB (vs LevelDB 10MB)
max_bytes_for_level_multiplier = 10.0
```

**Dynamic level sizing (`CalculateBaseBytes`):**
- Work backward from actual last-level size to compute base level
- `base_level` can float (doesn't have to be L1)
- Empty intermediate levels are normal — data jumps to the appropriate base
- Recomputed after every compaction

**Additional features:**
- Intra-L0 compaction when L0→L1 is blocked
- Grandparent-aware adaptive splitting thresholds (50-90% of target based on boundary frequency)
- Round-robin expansion up to 4 files for trivial moves
- 10x score multiplier for over-target levels (better prioritization)
- TTL-based compaction triggers

### Design Decisions for Strata

| Decision | Choice | Rationale |
|----------|--------|-----------|
| File sizes | **RocksDB scale** (64MB target, 256MB level base) | LevelDB's 2MB files too small for NVMe SSDs |
| Algorithms | **LevelDB simplicity** | No dynamic level sizing, no intra-L0, no TTL triggers yet |
| Grandparent tracking | **Yes** (from LevelDB) | Without it, cascading L+2 compactions kill performance |
| Round-robin | **Yes** (`compact_pointer`) | Without it, same key range gets hammered repeatedly |
| Trivial moves | **Yes** | Common case, saves significant I/O |
| Boundary files | **Yes** | Required for correctness with MVCC keys spanning files |

---

## Epic 23: Streaming Compaction + Multi-Segment Output

**Goal:** Compaction produces multiple non-overlapping output segments (split at 64MB boundaries), using streaming iterators instead of materializing all entries in RAM.

**Why first:** Without this, compaction OOMs at scale and L1 is one monolithic file that gets fully rewritten on every flush cycle.

### Changes

**1. `OwnedSegmentIter` in `crates/storage/src/segment.rs`** (~40 lines)

Currently `SegmentIter<'a>` borrows `&'a KVSegment`, forcing callers to `.collect()` all entries before the segment reference is dropped. `OwnedSegmentIter` holds `Arc<KVSegment>` and implements `Iterator<Item = (InternalKey, MemtableEntry)>` with the same block-by-block streaming logic.

This changes compaction memory from O(total entries) to O(num_segments × block_size).

**2. `SplittingSegmentBuilder` in `crates/storage/src/segment_builder.rs`** (~80 lines)

Wraps `SegmentBuilder` with a `target_file_size` parameter (default 64MB):
- Tracks bytes written to current output segment
- When output exceeds target, finalize current segment and start a new file
- Split only at key boundaries (never between versions of the same key)
- Returns `Vec<SegmentMeta>` with key ranges per output segment
- File naming: `{seg_id}_{split_idx}.sst` (e.g., `42_0.sst`, `42_1.sst`)

**3. Update compaction methods in `crates/storage/src/segmented.rs`** (~60 lines)

- `compact_l0_to_l1`: use `OwnedSegmentIter` instead of `.collect()`, use `SplittingSegmentBuilder`, produce `Vec<Arc<KVSegment>>` for L1
- `compact_branch` / `compact_tier`: same streaming pattern
- All existing compaction tests continue to work (behavior unchanged, just lower memory)

### Files

| File | Change |
|------|--------|
| `crates/storage/src/segment.rs` | Add `OwnedSegmentIter` |
| `crates/storage/src/segment_builder.rs` | Add `SplittingSegmentBuilder` |
| `crates/storage/src/segmented.rs` | Update compaction methods to use streaming + splitting |

### Test Plan

- `streaming_iter_matches_collected` — OwnedSegmentIter produces same entries as collect()
- `splitting_builder_respects_target_size` — output files are ~64MB each
- `splitting_builder_splits_at_key_boundaries` — no key split across files
- `splitting_builder_single_file_if_small` — small input produces 1 file
- `compact_l0_to_l1_produces_multiple_l1_segments` — 200K entries → multiple L1 files
- `compact_l0_to_l1_streaming_correctness` — all data recoverable after streaming compaction
- All existing 332+ storage tests pass unchanged
- Benchmark: 10M RSS drops from 43.6GB to <16GB

### Effort: ~3 days

---

## Epic 24: Generalized Multi-Level Structure (L0-L6)

**Goal:** `SegmentVersion` supports 7 levels. Read path binary-searches each level. `compact_level(n)` picks one file from Ln, finds overlapping Ln+1 files, merges into new Ln+1 files using streaming I/O.

**Why:** Without L2+, L1 holds ALL data and rewrites everything on compaction. With levels, data settles into tiers: L1=256MB, L2=2.5GB, L3=25GB. Each compaction only touches a small slice.

### Changes

**1. Generalize `SegmentVersion`** in `segmented.rs` (~40 lines)

Replace `l0_segments` / `l1_segments` with a single vector of levels:
```rust
struct SegmentVersion {
    /// levels[0] = L0 (overlapping, newest first)
    /// levels[1..] = L1+ (non-overlapping, sorted by key range)
    levels: Vec<Vec<Arc<KVSegment>>>,
}
```

Update all existing code that references `l0_segments` / `l1_segments` to use `levels[0]` / `levels[1]`.

**2. Level configuration** (~20 lines)
```rust
const NUM_LEVELS: usize = 7;
const L0_COMPACTION_TRIGGER: usize = 4;
const TARGET_FILE_SIZE: u64 = 64 << 20;           // 64MB
const LEVEL_BASE_BYTES: u64 = 256 << 20;           // 256MB (L1 target)
const LEVEL_MULTIPLIER: u64 = 10;

fn max_bytes_for_level(level: usize) -> u64 {
    if level == 0 { return u64::MAX; } // L0 uses file count, not bytes
    let mut bytes = LEVEL_BASE_BYTES;  // 256MB for L1
    for _ in 1..level {
        bytes *= LEVEL_MULTIPLIER;     // 10x per level
    }
    bytes
}
// L1=256MB, L2=2.5GB, L3=25GB, L4=250GB, L5=2.5TB
```

**3. `compact_level(level)` method** (~150 lines)

LevelDB's core compaction algorithm, adapted:

```
compact_level(level):
  1. Pick input file from levels[level]:
     - Use compact_pointer[level] for round-robin selection
     - If L0: expand to all overlapping L0 files
  2. Find overlapping files in levels[level+1]
  3. Track grandparent (L+2) files for output splitting
  4. Check for trivial move:
     - 1 input, 0 L+1 overlap, bounded grandparent overlap
     - If trivial: metadata-only level change, no I/O
  5. Merge inputs via streaming MergeIterator + CompactionIterator
  6. Write output via SplittingSegmentBuilder into levels[level+1]
  7. Split output when grandparent overlap > 10 * TARGET_FILE_SIZE
  8. Atomic version swap + manifest write
  9. Update compact_pointer[level] to largest key of input
  10. Delete old files, invalidate block cache
```

**4. Read path generalization** (~30 lines)

Replace separate L0/L1 logic with a loop over all levels:
```rust
// L0: linear scan (overlapping)
for seg in &ver.levels[0] { ... }

// L1+: binary search per level (non-overlapping)
for level in 1..ver.levels.len() {
    if let Some(se) = point_lookup_level(&ver.levels[level], key, max_version) {
        return Some(se);
    }
}
```

Rename `point_lookup_l1` → `point_lookup_level` (works for any non-overlapping level).

For scans: iterate all levels as merge sources (same pattern as current L0+L1).

**5. Manifest extension** (~10 lines)

`ManifestEntry.level` already supports `u8`. Allow values 0-6 instead of just 0-1. Recovery partitions segments into correct levels from manifest.

**6. `compact_l0_to_l1` becomes `compact_level(0)`**

The existing method is replaced by the generalized `compact_level(0)`. Same semantics, but output goes through `SplittingSegmentBuilder` and respects grandparent overlap tracking.

### Files

| File | Change |
|------|--------|
| `crates/storage/src/segmented.rs` | Generalize SegmentVersion, compact_level(), read paths (~300 lines) |
| `crates/storage/src/manifest.rs` | Allow level 0-6 (~5 lines) |

### Test Plan

**Level structure:**
- `data_flows_to_l2` — after loading enough data, L2 has segments
- `level_sizes_respect_targets` — L1 ≤ 256MB, L2 ≤ 2.5GB
- `level_segments_non_overlapping` — all L1+ segments have non-overlapping key ranges

**Compaction mechanics:**
- `compact_level_picks_round_robin` — compact_pointer advances across key range
- `compact_level_finds_overlapping_l_plus_1` — correct L+1 files selected
- `compact_level_grandparent_splits_output` — output split when L+2 overlap too large
- `compact_level_trivial_move_no_io` — metadata-only move when no overlap
- `compact_level_boundary_files_included` — same user_key across files handled correctly

**Read path:**
- `point_lookup_checks_all_levels` — data in L3 is findable
- `point_lookup_one_segment_per_level` — binary search finds correct segment
- `scan_includes_all_levels` — prefix scan merges data across L0-L3

**Recovery:**
- `recover_restores_multi_level` — segments placed in correct levels from manifest
- `recover_without_manifest_all_l0` — backward compat (all to L0, compaction sorts it out)

**Integration:**
- All existing storage tests pass
- Benchmark: 10M space amp drops from 4.0x to <2.5x

### Effort: ~5 days

---

## Epic 25: Compaction Scheduling + Engine Integration

**Goal:** Background compaction picks the highest-scoring level automatically. Replaces ad-hoc `l0_count >= 4` trigger with LevelDB-style per-level scoring.

**Why:** With 7 levels, we can't just trigger on L0 file count. We need to compact whichever level is most over its target size. Without scoring, some levels overflow while others are idle.

### Changes

**1. Per-level scoring** in `segmented.rs` (~40 lines)

```rust
pub fn compaction_scores(&self, branch_id: &BranchId) -> Vec<(usize, f64)> {
    // L0: score = file_count / L0_COMPACTION_TRIGGER
    // L1+: score = total_bytes_in_level / max_bytes_for_level(level)
    // Returns levels with score >= 1.0, sorted by score descending
}
```

**2. `pick_and_compact` method** (~30 lines)

```rust
pub fn pick_and_compact(&self, branch_id: &BranchId, prune_floor: u64)
    -> io::Result<Option<CompactionResult>>
{
    let scores = self.compaction_scores(branch_id);
    if let Some(&(level, _score)) = scores.first() {
        self.compact_level(branch_id, level, prune_floor)
    } else {
        Ok(None) // No level over target
    }
}
```

**3. Engine flush callback** in `database/mod.rs` (~20 lines)

Replace current `l0_count >= 4` trigger:
```rust
// After flush: compact until no level is over target
loop {
    match storage.pick_and_compact(&branch_id, 0) {
        Ok(Some(result)) => { /* log result, continue */ }
        Ok(None) => break,  // All levels within targets
        Err(e) => { /* log error, break */ }
    }
}
```

**4. Compact pointer persistence** (~15 lines)

- Add `compact_pointers: Vec<Vec<u8>>` to manifest (one per level)
- Restore on recovery for round-robin continuity across restarts
- Without this, every restart resets round-robin to the beginning of the key range

### Files

| File | Change |
|------|--------|
| `crates/storage/src/segmented.rs` | `compaction_scores()`, `pick_and_compact()` (~70 lines) |
| `crates/storage/src/manifest.rs` | Add compact_pointer storage (~15 lines) |
| `crates/engine/src/database/mod.rs` | Replace flush callback (~20 lines) |

### Test Plan

- `scoring_l0_uses_file_count` — L0 score = files / 4
- `scoring_l1_plus_uses_byte_ratio` — L1+ score = bytes / target
- `pick_compaction_chooses_highest_score` — highest-scoring level compacts first
- `compaction_loop_drains_all_levels` — repeated pick_and_compact until all scores < 1.0
- `compact_pointer_persists_across_restart` — round-robin resumes after recovery
- All existing engine tests pass (1307+)
- Benchmark: 10M sustained writes show stable space amp, no stalls

### Effort: ~2 days

---

## Dependency Graph

```
Epic 23 (streaming + split) ──→ Epic 24 (multi-level L0-L6)
                                         ↓
                                  Epic 25 (scheduling)
```

Epic 23 is the prerequisite: multi-level compaction requires streaming I/O and multi-segment output. Epic 25 depends on Epic 24 (need levels to score).

---

## Expected Results

| Metric | Current | After Epic 23 | After Epic 24 | After Epic 25 |
|--------|--------:|--------------:|--------------:|--------------:|
| 10M reads/s | 100K | 100K | 120K | 150K |
| 10M RSS | 43.6GB | **<16GB** | <12GB | <12GB |
| 10M space amp | 4.0x | 3.5x | **<2.5x** | <2.0x |
| 10M disk | 38.6GB | 35GB | **<25GB** | <20GB |
| 100M reads/s | OOM | OOM | **50K+** | **80K+** |
| 100M space amp | N/A | N/A | <3x | **<2x** |

---

## What Comes After (Not in This Plan)

From `docs/design/billion-scale-roadmap.md`:
- Dynamic level sizing (`CalculateBaseBytes`) — adaptive level targets
- FileIndexer — O(log N) file lookup within levels with 1000+ files
- Restart points in data blocks (#1518) — binary search within blocks
- Partitioned index/bloom — lazy-load sub-filters for TB-scale segments
- Per-level compression — fast for hot levels, dense for cold
- Rate limiter — prevent compaction from starving reads
- Write batch coalescing (#1521) — leader-follower WAL grouping
- Soft write throttle (#1522) — gradual backpressure
- BlobDB, parallel subcompactions — 1B+ scale

---

## Reference

- `docs/design/scale-up-epics-19-22.md` — previous epic plan (completed)
- `docs/design/billion-scale-roadmap.md` — full roadmap to 1B keys
- `docs/design/reference-implementation-audit.md` — Strata vs LevelDB/RocksDB comparison
- `docs/design/leveldb-read-path-reference.md` — LevelDB block iterator analysis
- `docs/design/read-path-optimization-plan.md` — read path bottleneck analysis
- LevelDB source: `/tmp/leveldb` (cloned from github.com/google/leveldb)
- RocksDB source: `/tmp/rocksdb` (cloned from github.com/facebook/rocksdb)
- Issues: #1517, #1518, #1519, #1520, #1523
