# Core Storage Engine (LSM Tree): Focused Analysis

This note is derived from the storage code, not from the existing docs.
Scope for this pass:

- `crates/storage/src/memtable.rs`
- `crates/storage/src/segmented/mod.rs`
- `crates/storage/src/segmented/compaction.rs`
- `crates/storage/src/segment.rs`
- `crates/storage/src/segment_builder.rs`
- `crates/storage/src/bloom.rs`
- `crates/storage/src/block_cache.rs`
- `crates/storage/src/merge_iter.rs`
- `crates/storage/src/seekable.rs`
- `crates/storage/src/key_encoding.rs`

## Executive view

The core storage engine is a real LSM design, not just an in-memory MVCC map with a flush
layer bolted on.

The important architectural choices are:

- MVCC ordering is pushed into the encoded key format itself.
- The write path is `active memtable -> frozen memtables -> immutable SST`.
- The read path is lock-light because it snapshots branch state up front and releases the
  `DashMap` guard before any segment I/O.
- L0 is treated as overlapping and newest-first.
- L1+ is treated as non-overlapping and sorted by key range, which enables binary search and
  lazy per-level iteration.

The engine is strongest where those choices line up:

- point reads and scans share one consistent ordering model
- flushes install atomically without exposing a read gap
- compaction can stream in key order without reconstructing separate MVCC structures

The weakest parts of the storage design are not correctness bugs in the happy path.
They are control-plane and resource-shaping gaps:

- some marketing descriptions overstate concurrency
- metadata residency is aggressively eager and only partially budgeted
- flush outputs do not obey the same sizing policy as compaction outputs
- L0 read amplification remains highly dependent on compaction health

## Inventory validation

| # | Feature | Verdict | Code-derived note |
|:-:|---------|---------|-------------------|
| 1 | Skiplist Memtable | Mostly true | The memtable uses `crossbeam_skiplist::SkipMap` and lock-free reads, but the memtable code itself says writes are serialized by an external per-branch commit lock (`crates/storage/src/memtable.rs:101-104`). |
| 2 | Frozen Memtable Queue | True | Each branch keeps `frozen: Vec<Arc<Memtable>>`, newest first, and rotation freezes the active memtable then pushes it to the front (`crates/storage/src/segmented/mod.rs:306-315`, `crates/storage/src/segmented/mod.rs:2546-2562`). |
| 3 | 7-Level Segment Hierarchy | True | `NUM_LEVELS = 7`, with L0 overlapping and L1+ sorted/non-overlapping in `SegmentVersion` (`crates/storage/src/segmented/mod.rs:90-91`, `crates/storage/src/segmented/mod.rs:233-255`). |
| 4 | KV Segment Files (SST) | True | Segment files are immutable, sorted, block-framed, CRC-protected, and carry bloom/index/properties blocks (`crates/storage/src/segment_builder.rs:1-18`, `crates/storage/src/segment.rs:388-513`). |
| 5 | Block-Based I/O | True | Default data-block size is 4 KiB, blocks are read via `pread_exact`, and decompressed blocks are cached (`crates/storage/src/segment_builder.rs:132`, `crates/storage/src/segment.rs:381-385`, `crates/storage/src/segment.rs:920-1018`). |
| 6 | FlatIndex | True, with nuance | `FlatIndex` is real and clearly designed for cache locality, but the exact "32% faster at 1M+ keys" claim is not in the code comments I inspected (`crates/storage/src/segment.rs:138-166`). |
| 7 | Partitioned Bloom Filters | True | Segments load a filter index plus multiple bloom partitions; the raw partition bytes are pinned in memory at open (`crates/storage/src/segment.rs:125-136`, `crates/storage/src/segment.rs:465-488`). |
| 8 | Cache-Local Bloom | True | `BloomFilter` is a blocked bloom design with all probes confined to one 64-byte block (`crates/storage/src/bloom.rs:1-17`, `crates/storage/src/bloom.rs:22-35`). |
| 9 | Lock-Free Block Cache | True | The cache is a lock-free open-addressed table with atomic metadata and CLOCK eviction, inspired by HyperClockCache (`crates/storage/src/block_cache.rs:1-23`, `crates/storage/src/block_cache.rs:245-259`, `crates/storage/src/block_cache.rs:551-661`). |
| 10 | Per-Level Compression | True, but only after flush | Generic compaction uses `compression_for_level(level + 1)`, but memtable flush to L0 explicitly uses `CompressionCodec::None` (`crates/storage/src/segmented/mod.rs:127-138`, `crates/storage/src/segmented/mod.rs:2599-2602`, `crates/storage/src/segmented/compaction.rs:619-625`, `crates/storage/src/segmented/compaction.rs:894-900`). |
| 11 | Adaptive Bloom Bits/Level | True, but only after flush | `bloom_bits_for_level()` exists and is used by compaction outputs into L1+, while L0 flush uses the base builder settings (`crates/storage/src/segmented/mod.rs:112-125`, `crates/storage/src/segmented/mod.rs:916-921`, `crates/storage/src/segmented/compaction.rs:619-625`, `crates/storage/src/segmented/compaction.rs:894-900`). |
| 12 | MVCC Read Path | True | Point reads go active -> frozen -> L0 -> L1+ -> inherited layers (`crates/storage/src/segmented/mod.rs:3334-3525`). |
| 13 | Merge Iterator | True | There is a real k-way merge plus `MvccIterator` wrapper for logical-key dedup by snapshot version (`crates/storage/src/merge_iter.rs:28-153`, `crates/storage/src/merge_iter.rs:159-206`). |
| 14 | Seekable Iterator | True | The seekable stack is explicit and persistent across seeks, with memtable, per-segment, per-level, merge, MVCC, and rewrite adapters (`crates/storage/src/seekable.rs:1-18`, `crates/storage/src/seekable.rs:39-63`, `crates/storage/src/seekable.rs:276-520`). |
| 15 | Key Encoding | True | `InternalKey = TypedKeyBytes || !commit_id` and the encoding is order-preserving across branch, space, type, user key, then descending commit (`crates/storage/src/key_encoding.rs:1-26`, `crates/storage/src/key_encoding.rs:130-157`). |

## Real execution model

### 1. Write path

### Active memtable

Each branch owns one active `Memtable`, backed by `SkipMap<InternalKey, MemtableEntry>`.
The key format makes MVCC version ordering natural:

- typed key ascending
- commit ID descending inside the same logical key

That lets point lookups seek to `(key, +inf)` and return the first visible version without a
separate per-key version chain.

Code evidence:

- `crates/storage/src/memtable.rs:95-120`
- `crates/storage/src/key_encoding.rs:130-157`

### Rotation

After each write, `maybe_rotate_branch()` checks the active memtable's approximate byte size.
If it exceeds `write_buffer_size`, the store swaps in a fresh memtable, freezes the old one,
and pushes that old memtable into the frozen queue.

If too many frozen memtables already exist, rotation is skipped and the active memtable is
allowed to grow past the configured write-buffer size.

Code evidence:

- `crates/storage/src/segmented/mod.rs:3035-3058`

### Frozen memtables

Frozen memtables remain readable while they await flush. They are stored newest first so the
read path preserves recency ordering.

The memtable bloom filter is built lazily on first frozen read, not eagerly at freeze time.

Code evidence:

- `crates/storage/src/segmented/mod.rs:306-315`
- `crates/storage/src/memtable.rs:223-248`
- `crates/storage/src/memtable.rs:388-400`

### Flush

`flush_oldest_frozen()` clones the oldest frozen memtable, writes an SST while the memtable is
still in the read path, opens the new `KVSegment`, then atomically swaps the branch version so
the new L0 segment appears before the frozen memtable disappears.

That is the key "no read gap" property in this design.

Code evidence:

- `crates/storage/src/segmented/mod.rs:2573-2661`

### 2. Read path

### Snapshotting

Before any segment I/O, the store captures a `BranchSnapshot`:

- `Arc<Memtable>` for active
- cloned `Arc<Memtable>` list for frozen
- `Arc<SegmentVersion>` via `ArcSwap`
- inherited layers

This means the `DashMap` guard is held only long enough to clone the references.

Code evidence:

- `crates/storage/src/segmented/mod.rs:389-405`
- `crates/storage/src/segmented/mod.rs:3963-3989`

### Point lookups

The snapshot-based point lookup order is:

1. active memtable
2. frozen memtables, newest first
3. L0 segments, newest first, linear probe
4. L1+ levels, one segment candidate per level via binary search
5. inherited layers, nearest ancestor first

This is the actual storage read contract.

Code evidence:

- `crates/storage/src/segmented/mod.rs:3334-3525`
- `crates/storage/src/segmented/mod.rs:4603-4625`

### Prefix and range scans

Scans build a merged pipeline from:

- collected memtable entries
- lazy segment iterators
- MVCC deduplication
- tombstone / TTL filtering after MVCC selection

The important detail is that MVCC dedup happens before logical filtering, so the scan emits
at most one visible version per logical key.

Code evidence:

- `crates/storage/src/segmented/mod.rs:3797-3945`
- `crates/storage/src/merge_iter.rs:159-206`

### Seekable iterator stack

For cursor-style iteration, the engine has a separate seekable iterator hierarchy modeled very
explicitly after RocksDB:

- `MemtableSeekableIter`
- `SegmentSeekableIter`
- `LevelSeekableIter`
- `MergeSeekableIter`
- `MvccSeekableIter`
- `RewritingSeekableIter`

The major win is that children survive across seeks; the system re-seeks existing iterators
instead of rebuilding the whole merge stack every time.

Code evidence:

- `crates/storage/src/seekable.rs:1-18`
- `crates/storage/src/seekable.rs:279-409`
- `crates/storage/src/seekable.rs:416-502`

### 3. Segment file design

### Layout

A segment file is not just "sorted values on disk." It has a deliberate block layout:

- fixed header
- data blocks
- optional sub-index blocks
- bloom partitions
- filter index block
- top-level or monolithic index block
- properties block
- footer

Each block is framed and CRC protected.

Code evidence:

- `crates/storage/src/segment_builder.rs:1-18`
- `crates/storage/src/segment_builder.rs:31-71`

### Data blocks

Data blocks use:

- prefix-compressed internal keys
- restart points every 16 entries
- optional hash index appended to the block

That means point lookups do not blindly scan full blocks; they can binary-search restart
intervals and, when the hash bucket is useful, jump directly to a restart interval.

Code evidence:

- `crates/storage/src/segment_builder.rs:40-44`
- `crates/storage/src/segment_builder.rs:587-611`
- `crates/storage/src/segment.rs:1021-1084`
- `crates/storage/src/segment.rs:1193-1299`

### Indexes

`FlatIndex` packs key bytes into one contiguous buffer and stores offsets separately.
For large indexes this is materially better than a vector of heap-allocated keys.

For sufficiently large segments, the on-disk index becomes two-level:

- top-level partition index
- multiple pinned sub-indexes

Code evidence:

- `crates/storage/src/segment.rs:138-166`
- `crates/storage/src/segment.rs:301-359`
- `crates/storage/src/segment_builder.rs:60-65`
- `crates/storage/src/segment_builder.rs:351-417`

### Bloom filters

Bloom filters are two-layered:

- within each bloom partition, the bloom itself is cache-local
- across the segment, a filter index maps ranges to partitions

One important nuance: bloom partitions are preloaded and pinned in heap memory at segment open.
They do not use the block cache.

Code evidence:

- `crates/storage/src/bloom.rs:1-17`
- `crates/storage/src/segment.rs:125-136`
- `crates/storage/src/segment.rs:465-488`
- `crates/storage/src/segment.rs:896-918`

### Data-block caching

Only decompressed data blocks live in the block cache. Metadata does not.

On a cache miss:

- `pread_exact`
- CRC verification
- decompression if needed
- cache insert

On a hit, the reader gets the decompressed bytes directly.

Code evidence:

- `crates/storage/src/segment.rs:920-1018`
- `crates/storage/src/block_cache.rs:326-387`

### 4. Level and compaction policy

### Level sizing

The store has fixed logical levels `L0..L6`, but byte targets for non-L0 levels are dynamic.
The algorithm is patterned after RocksDB's dynamic base-level calculation.

Code evidence:

- `crates/storage/src/segmented/mod.rs:90-110`
- `crates/storage/src/segmented/compaction.rs:13-94`

### L0

L0 is count-triggered and overlapping.
The compaction trigger constant is `4`.

Code evidence:

- `crates/storage/src/segmented/mod.rs:93-97`
- `crates/storage/src/segmented/compaction.rs:114-161`

### L1+

L1 and deeper levels are kept non-overlapping and sorted by key range.
That is what enables:

- one-segment-per-level point lookup
- lazy per-level iterators
- metadata-only trivial moves

Code evidence:

- `crates/storage/src/segment.rs:1763-1900`
- `crates/storage/src/segmented/compaction.rs:823-860`

### Compression and bloom policy

Per-level compression and deeper-level bloom inflation are applied during compaction output
creation, not during raw flush into L0.

Code evidence:

- `crates/storage/src/segmented/mod.rs:112-138`
- `crates/storage/src/segmented/compaction.rs:619-625`
- `crates/storage/src/segmented/compaction.rs:894-900`

## Important unlisted strengths

These are meaningful storage-engine details that were not in the original inventory but matter.

### Atomic segment-version install

Per-branch segment state is wrapped in `ArcSwap<SegmentVersion>`, so flush and compaction
publish a new segment layout with a single pointer swap.

Code evidence:

- `crates/storage/src/segmented/mod.rs:229-267`

### Lazy level iteration

`LevelSegmentIter` opens only the relevant file in a non-overlapping level and opens later
files lazily as iteration advances.

Code evidence:

- `crates/storage/src/segment.rs:1763-1779`
- `crates/storage/src/segment.rs:1800-1831`
- `crates/storage/src/segment.rs:1902-1905`

### Same-file reseek optimization

The level iterator checks whether a target still falls within the current file's key range
before binary-searching the level again.

Code evidence:

- `crates/storage/src/segment.rs:1836-1870`

### Raw-value compaction pass-through

`SegmentEntry` and `MemtableEntry` can carry pre-encoded value bytes so compaction can avoid
the deserialize -> serialize round trip for values.

Code evidence:

- `crates/storage/src/memtable.rs:40-43`
- `crates/storage/src/segment.rs:115-118`
- `crates/storage/src/segment.rs:1678-1709`

## Storage-engine-specific constraints and gaps

### 1. "Lock-free concurrent write buffer" overstates writer concurrency

The memtable data structure is lock-free for storage and reads, but the memtable code itself
says writes are serialized by an external per-branch commit lock.

That means the accurate description is:

- lock-free read buffer
- concurrent skiplist-backed storage
- externally serialized writers

not "many writers mutating the memtable lock-free at once."

Code evidence:

- `crates/storage/src/memtable.rs:101-104`

### 2. Segment metadata is pinned outside the block cache and outside hard cache budgeting

At segment open, the engine eagerly loads:

- all bloom partitions
- all filter-index entries
- all top-level indexes
- all sub-indexes for partitioned indexes

`metadata_bytes()` explicitly says this memory is outside the block cache and contributes to
RSS outside the configured cache budget.

Code evidence:

- `crates/storage/src/segment.rs:465-500`
- `crates/storage/src/segment.rs:839-868`

The store does track this memory for pressure evaluation:

- `total_segment_metadata_bytes()`
- `total_tracked_bytes()`

but that is reactive accounting, not hard budgeting.

Code evidence:

- `crates/storage/src/segmented/mod.rs:3083-3122`

This is a real architectural trade-off:

- very fast read hot path
- but segment-count growth directly increases pinned heap

### 3. Inherited-layer memory accounting likely overcounts shared segment metadata

This is an inference from the code.

`InheritedLayer` stores an `Arc<SegmentVersion>` snapshot of parent segments.
`total_segment_metadata_bytes()` then walks both branch-owned levels and inherited layers and
sums `seg.metadata_bytes()` for every appearance.

Because the loop does not deduplicate by `file_id` or `Arc` identity, shared segments that are
visible through inherited layers appear to be counted multiple times for pressure purposes.

Code evidence:

- `crates/storage/src/segmented/mod.rs:291-300`
- `crates/storage/src/segmented/mod.rs:3087-3104`

Impact:

- memory pressure can be conservative or distorted on branch-heavy workloads
- pressure-triggered decisions can reflect logical visibility fanout, not actual RSS

### 4. Flush output does not honor target file size

Memtable flush uses `SegmentBuilder::build_from_iter()` and writes exactly one L0 file per
frozen memtable.

Compaction output is different: it uses `SplittingSegmentBuilder` with `target_file_size()`.

Code evidence:

- `crates/storage/src/segmented/mod.rs:2599-2602`
- `crates/storage/src/segmented/mod.rs:916-921`
- `crates/storage/src/segmented/compaction.rs:612-630`
- `crates/storage/src/segmented/compaction.rs:892-905`

This means:

- `target_file_size` shapes compaction output
- it does not shape initial L0 flush output

If a memtable grows well past the nominal write-buffer size, or bulk load accumulates a very
large active memtable before rotation, the resulting L0 segment can also be very large.

### 5. L0 read cost is intentionally linear and therefore compaction-sensitive

Point reads check every L0 segment newest-first because L0 is overlapping.
Only after L0 do they get the cheaper one-segment-per-level behavior.

Code evidence:

- `crates/storage/src/segmented/mod.rs:3406-3434`

This is normal LSM behavior, but here it matters operationally because the design assumes L0
will stay small enough for that linear probe to remain cheap.

The storage code itself is efficient inside the probe:

- bloom check
- flat index search
- block scan

but the probe count still scales with L0 fanout.

### 6. Open-time cost scales with segment count because metadata is eagerly loaded

`KVSegment::open()` does real work:

- parse header/footer
- read index blocks
- read filter index
- read every bloom partition
- for partitioned indexes, read every sub-index block

Code evidence:

- `crates/storage/src/segment.rs:388-500`

This makes read-time lookups fast, but it also means database open and segment recovery cost
scale with the number of segment files and partitions, not just with the number of later data
block reads.

## Bottom line

The LSM core is well-structured and coherent.

The most important design win is that the same ordering primitive, `InternalKey`, drives:

- memtable ordering
- SST ordering
- merge iteration
- MVCC visibility
- flush streaming

That is why the system feels internally consistent.

The biggest storage-engine-specific risks are not around key correctness.
They are around resource shape and operational behavior:

- pinned metadata growth
- oversold write concurrency
- oversized flush outputs
- linear L0 sensitivity

Those are the areas I would push on next if we continue the section-by-section deep dive.
