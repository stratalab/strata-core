# Storage Engine

This is the code-level picture of the LSM and branch storage layer.

## 1. Storage state is branch-local

`SegmentedStore` keeps one `BranchState` per branch (`crates/storage/src/segmented/mod.rs:307`).
That state is the real storage unit of the engine:

- `active`: writable memtable
- `frozen`: immutable memtables waiting to flush
- `version`: the current segment hierarchy, wrapped in `ArcSwap`
- `inherited_layers`: copy-on-write ancestry for forked branches
- flush and compaction bookkeeping such as timestamps and `max_flushed_commit`

This matters because almost every storage concern is branch-scoped:
memtable rotation, flushing, compaction, inherited reads, and materialization.

## 2. Internal key order is how MVCC works

The physical sort order is not "user key then oldest version first".
It is "typed key bytes ascending, commit id descending" via `InternalKey::encode`.
That encoding means:

- all versions for a logical key are adjacent
- the newest visible version is encountered first
- memtables and SST segments can use the same ordering

Main source: `crates/storage/src/key_encoding.rs`.

## 3. Memtables are lock-free read/write buffers

The memtable is a `crossbeam_skiplist::SkipMap` (`crates/storage/src/memtable.rs:11`, `crates/storage/src/memtable.rs:105`).
Important behavior:

- writes insert encoded internal keys directly into the skiplist
- reads seek with `InternalKey::encode(key, u64::MAX)` so the first matching entry is the newest version
- `freeze()` makes the memtable immutable before flush (`crates/storage/src/memtable.rs:349`)
- frozen memtables can build a lazy bloom filter to avoid wasted scans

This gives the engine a single in-memory structure for concurrent writers, point lookups, ordered scans, and later SST construction.

## 4. Read path order is explicit and MVCC-aware

`get_versioned_from_snapshot` in `crates/storage/src/segmented/mod.rs:3334` is the core read algorithm.
The search order is:

1. active memtable
2. frozen memtables, newest first
3. L0 segments, newest first, linear scan because they overlap
4. L1+ levels, binary search per level because files are non-overlapping
5. inherited COW layers, nearest ancestor first

Two details matter:

- tombstones are first-class values in the read path, so deletes participate in visibility exactly like puts
- inherited layers are last, so a child branch always sees its own writes before parent state

For iteration, the store constructs source iterators in the same order and merges them with the shared merge iterator stack (`crates/storage/src/segmented/mod.rs:3689`, `crates/storage/src/merge_iter.rs`).

## 5. Flush path converts frozen memtables into L0 segments

The write path is:

1. transaction commit writes into the branch's active memtable
2. `maybe_rotate_memtable` freezes the active memtable when thresholds are exceeded
3. background flush drains the oldest frozen memtable
4. `flush_oldest_frozen` writes a new SST and atomically installs it in L0 (`crates/storage/src/segmented/mod.rs:2573`)
5. the branch segment manifest is persisted before old state is discarded

The key invariant is that the frozen memtable stays visible in the read path while I/O is happening.
Flush does not remove read visibility first and then build the SST later.

`Database::schedule_flush_if_needed` in `crates/engine/src/database/transaction.rs:231` coalesces flush work and drains all pending frozen memtables in the background.

## 6. SST segments are optimized for negative lookups and cache locality

`KVSegment` in `crates/storage/src/segment.rs` is an immutable sorted file with several read-side optimizations:

- 4 KiB block-based I/O
- partitioned bloom filters with a top-level filter index
- `FlatIndex`, a packed in-memory index for cache-friendly binary search (`crates/storage/src/segment.rs:146`)
- eager metadata loading so point lookups do not reopen index/filter structures repeatedly

The bloom implementation is split across two layers:

- `storage/segment.rs` handles partitioned segment blooms
- `storage/bloom.rs` provides the cache-local bloom primitive used inside those filters

Data blocks are cached through the global lock-free block cache in `crates/storage/src/block_cache.rs`.
Segment metadata is not in that cache; it is pinned in process memory, which becomes relevant in the gap analysis.

## 7. Leveled compaction is the real GC path

Storage-level version cleanup happens through compaction, not through an in-place version-chain sweeper.
The public scoring and execution entry points are:

- `compute_compaction_scores` (`crates/storage/src/segmented/compaction.rs:120`)
- `pick_and_compact` (`crates/storage/src/segmented/compaction.rs:167`)

Compaction behavior includes:

- L0 trigger and level score based scheduling
- per-level compression policy
- MVCC pruning through `CompactionIterator`
- tombstone retention rules that differ for bottommost vs non-bottommost compactions
- snapshot-floor awareness so active readers do not lose required versions

This is why `gc_branch()` is currently a no-op stub: old-version removal is expected to happen when files are rewritten, not by mutating in-place structures.

## 8. Branch forks are copy-on-write at the segment layer

`fork_branch` in `crates/storage/src/segmented/mod.rs:1500` creates a child branch by capturing inherited layers instead of copying all keys.
Conceptually the child gets:

- its own active memtable
- its own future L0..L6 files
- read access to a frozen snapshot of the parent's segments up to the fork version

As the inherited chain deepens, `branches_needing_materialization` and `materialize_layer` can collapse older ancestry into local segments (`crates/storage/src/segmented/mod.rs:1116`, `crates/storage/src/segmented/mod.rs:1136`).

## 9. Recovery restores segments from per-branch manifests

On open, segment recovery scans branch directories and reconstructs the on-disk hierarchy via `recover_segments` (`crates/storage/src/segmented/mod.rs:2752`).
It does not blindly load every `.sst` file it finds.
Instead it:

- reads each branch's `segments.manifest`
- restores exact level placement
- preserves inherited-layer relationships
- avoids resurrecting orphaned files left behind by crashes or interrupted rewrites

That per-branch manifest is the storage engine's durable truth for SST placement.

## 10. Operational invariants

The code currently relies on these invariants:

- read order is always own-state first, inherited-state last
- L0 may overlap; L1+ must not
- a version is not globally visible until it is fully installed in storage
- flushed memtables remain readable until the replacement SST is durably integrated
- branch forks do not copy data unless materialization is forced later
