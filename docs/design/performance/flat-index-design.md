# Design: FlatIndex for Cache-Friendly Point Lookups

**Issue:** #2244
**Status:** Proposed
**Author:** Generated from profiling session 2026-04-03

## Problem

At 1M records, L1+ point lookups take 2.72us per read. Profiling shows 70% of the time (1.89us) is spent in the index binary search due to CPU cache misses.

### Root Cause

`IndexEntry.key` is `Vec<u8>` -- each key is a separate heap allocation:

```rust
struct IndexEntry {
    key: Vec<u8>,            // 24-byte header + ~49 bytes on heap (separate allocation)
    block_offset: u64,
    block_data_len: u32,
}
```

At 1M records with 4KB blocks, a segment has ~25K index entries. Binary search over 25K entries requires ~15 comparisons. Each comparison dereferences a `Vec<u8>` pointer to a different heap location, causing an L2 cache miss (~100ns each). 15 x 100ns = 1.5-1.9us.

### Profiling Evidence

YCSB Workload C (pure reads), 1M records, Zipfian distribution:

```
[lookup-prof] bloom=0.36us  index+block=2.36us  total=2.72us
[block-scan]  read_block=0.19us  scan=0.28us   total=0.47us

Index overhead = 2.36 - 0.47 = 1.89us (70% of total)
```

Cache hit rate: 98%. The pread path is not the bottleneck.

### Impact on YCSB

| Scale | Strata | RocksDB | Ratio |
|---|---|---|---|
| 100K (memtable) | 1,144K ops/s | 905K ops/s | 1.26x faster |
| 1M (L1+ disk) | 273K ops/s | 555K ops/s | 0.49x slower |

At 100K, all data fits in the active memtable (0.50us/read). At 1M, 95% of reads hit L1+ segments and pay the index cache-miss penalty, causing a 4x read degradation.

## RocksDB Reference Implementation

RocksDB stores index blocks as a **flat byte buffer** (`const char* data_`) with a restart point array:

```
[key-value pairs with delta encoding...] [restart_offset_0, ..., restart_offset_N] [num_restarts]
```

Key design decisions (from `table/block_based/block.h`, `block.cc`):

1. **Single contiguous buffer** -- all keys packed inline with delta/prefix compression
2. **Restart point array** -- `uint32_t[]` at end of buffer, accessed as `DecodeFixed32(data_ + restarts_ + index * 4)`
3. **Binary search over restart points only** -- `BinarySeekRestartPointIndex` does O(log num_restarts) comparisons, each reading from the same contiguous buffer
4. **Data block hash index** -- `DataBlockHashIndex` provides O(1) restart point lookup for point queries via a tiny hash table (~253 bytes, fits in L1)

The cache efficiency comes from all data living in one allocation with sequential access patterns during binary search.

## Proposed Solution

### Phase 1: FlatIndex (In-Memory Layout Change)

Replace `Vec<IndexEntry>` with a flat structure:

```rust
struct FlatIndex {
    /// All keys packed contiguously: key_0 || key_1 || ... || key_n
    key_data: Vec<u8>,

    /// Byte offset into key_data where each key starts.
    /// Length = num_entries + 1 (sentinel at end = key_data.len())
    key_offsets: Vec<u32>,

    /// Block file offset for each entry.
    block_offsets: Vec<u64>,

    /// Block data length for each entry.
    block_data_lens: Vec<u32>,
}
```

**Key access:** `key_data[key_offsets[i]..key_offsets[i+1]]`

**Binary search:** Over indices `0..len`, comparing `self.key(mid)` against seek bytes. The `key_offsets` array is contiguous (one cache line per ~16 entries), and adjacent keys in `key_data` have good spatial locality.

**SegmentIndex update:**

```rust
enum SegmentIndex {
    Monolithic(FlatIndex),
    Partitioned {
        top_level: FlatIndex,
        sub_indexes: Vec<FlatIndex>,
    },
}
```

**No on-disk format change.** Parse the existing wire format directly into FlatIndex:

```rust
fn parse_flat(data: &[u8]) -> Option<FlatIndex> {
    let count = u32::from_le_bytes(data[..4]) as usize;
    let mut key_data = Vec::new();
    let mut key_offsets = Vec::with_capacity(count + 1);
    let mut block_offsets = Vec::with_capacity(count);
    let mut block_data_lens = Vec::with_capacity(count);
    let mut pos = 4;

    for _ in 0..count {
        let key_len = u32::from_le_bytes(data[pos..pos+4]) as usize;
        pos += 4;
        key_offsets.push(key_data.len() as u32);
        key_data.extend_from_slice(&data[pos..pos+key_len]);
        pos += key_len;
        block_offsets.push(u64::from_le_bytes(data[pos..pos+8]));
        pos += 8;
        block_data_lens.push(u32::from_le_bytes(data[pos..pos+4]));
        pos += 4;
    }
    key_offsets.push(key_data.len() as u32); // sentinel

    Some(FlatIndex { key_data, key_offsets, block_offsets, block_data_lens })
}
```

### Phase 2: On-Disk Delta Encoding (Future)

Add restart-point delta encoding to the on-disk index format, matching RocksDB's approach. This reduces index block size by ~50% (shared prefixes between adjacent keys), further improving cache locality.

### Phase 3: Hash Index on FlatIndex (Future)

Add an optional hash index over the flat key data for O(1) point lookups, similar to RocksDB's `DataBlockHashIndex`. A 256-bucket hash table (~256 bytes) provides direct restart-point lookup without binary search.

## Memory Comparison

At 25K entries, 49-byte keys:

| | Vec\<IndexEntry\> | FlatIndex |
|---|---|---|
| Key storage | 25K separate heap allocs (1,225 KB) | 1 contiguous Vec (1,225 KB) |
| Offsets | -- | 100 KB (1 Vec) |
| Block offsets | In IndexEntry (contiguous) | 200 KB (1 Vec) |
| Block lengths | In IndexEntry (contiguous) | 100 KB (1 Vec) |
| Vec headers | 25K x 24B = 600 KB | 0 |
| **Total** | **~2,025 KB (25K+ allocs)** | **~1,625 KB (4 allocs)** |

20% less memory, but the real win is reducing heap allocations from 25K+ to 4, eliminating cache misses during binary search.

## Expected Performance

| Metric | Current | After FlatIndex | After Hash Index |
|---|---|---|---|
| Index binary search | 1.89us | ~0.2-0.3us | ~0.1us |
| Total point lookup | 2.72us | ~1.0us | ~0.8us |
| Workload C (1M) | 273K ops/s | ~500K ops/s | ~600K ops/s |
| vs RocksDB (555K) | 0.49x | ~0.9x | ~1.1x |

## Files to Modify

| File | Change |
|---|---|
| `crates/storage/src/segment.rs` | Add `FlatIndex`, update `SegmentIndex`, update all consumers |
| `crates/storage/src/segment_builder.rs` | Add `parse_index_block_flat()` |

## Backward Compatibility

- Old segment files: parsed by existing `parse_index_block` then converted to `FlatIndex`
- New segment files: parsed directly by `parse_index_block_flat`
- On-disk format: unchanged in Phase 1
- No migration needed

## Risks

- `FlatIndex::binary_search_by_key` must produce identical results to the current `Vec<IndexEntry>` binary search. A mismatch would cause incorrect block selection and data loss.
- The `scan_block_for_key` function takes `&IndexEntry` today. Need to change its signature to accept offset/len separately, or create a lightweight view struct.
- `SegmentIter` (used by compaction and scans) accesses index entries by partition/block index. Must work with FlatIndex's array-of-arrays layout.

## Testing Strategy

1. Byte-level comparison: parse same index block via old and new paths, verify identical binary search results for 1000 random keys
2. All existing `strata-storage` tests (655+) must pass unchanged
3. All engine recovery tests must pass (segment files still readable)
4. YCSB Workload C at 1M with profiling to verify latency improvement
