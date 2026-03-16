# Memtable and Flush Pipeline Specification

**Status:** Proposal
**Author:** Design Team
**Date:** 2026-03-01

> **Part of the Strata Storage Architecture proposal.**
> - [Architecture Overview](tiered-storage.md) — design vision, principles, system model
> - [File Layout Specification](storage-file-layout.md) — on-disk formats for WAL, manifests, and segments
> - [Read/Write Algorithms](read-write-algorithms.md) — concurrency model, ACID commit path, lock-free reads
> - **Memtable and Flush Pipeline** (this document) — memtable design, flush file format, recovery
> - [Industry Research](tiered-storage-research.md) — reference analysis of Redis, RocksDB, LMDB, et al.

This document specifies the in-memory write buffer (memtable), the crash-safe flush pipeline that converts a frozen memtable into an immutable KV segment, and the recovery behavior at each crash boundary.

---

## 1. Scope and Non-Goals

### Scope

* In-memory write buffer (memtable) supporting:
  * Fast point `get_versioned(key, snapshot_commit)`
  * Fast prefix scans for `list_by_type` without a global ordered key set
  * MVCC via `commit_id` visibility
* Flush pipeline:
  * Freeze active memtable
  * Write a crash-safe **flush file** to `tmp/`
  * Finalize into an immutable **KV segment (`.sst`)** + publish manifest update
* Recovery behavior when a crash happens mid-flush

### Non-Goals

* Compaction policies (separate spec)
* Bloom filter sizing and block cache details (separate spec)
* Multi-primitive flushing (KV only here; vector and BM25 segment sealing follow the same lifecycle but have their own build pipelines)

---

## 2. Memtable Design

### 2.1 Requirements

* **Writes:** high throughput under per-branch single-writer commit lock (see [Read/Write Algorithms §3.2](read-write-algorithms.md))
* **Reads:** lock-free (no branch lock on the read path)
* **Iteration:** ordered by key to support prefix scans and flush generation
* **MVCC:** multiple versions per key, with "newest version wins" semantics at a snapshot

### 2.2 Key Ordering Contract

Memtable ordering **must** match the KV segment ordering to avoid transform complexity during flush.

Sort order:

1. `TypedKey` ascending (lexicographic bytes)
2. `commit_id` descending (newest first)

The composite internal key is:

```
InternalKey = TypedKeyBytes || EncodeDesc(commit_id)
```

`EncodeDesc(commit_id)` is `u64_be(!commit_id)` so that larger commit_ids sort first.

This encoding is shared across memtables, flush files, and KV segment data blocks. See [Architecture Overview §7](tiered-storage.md) for the design rationale and [File Layout §8](storage-file-layout.md) for the `TypedKeyBytes` byte-level encoding.

This makes:

* **Point reads:** seek to `(key, +∞)` and scan a short run
* **Scans:** merge iterators across sources with correct "newest first" behavior
* **Flush:** straightforward streaming encode into segment blocks

### 2.3 Data Structure

**Design decision: concurrent skiplist** (e.g., `crossbeam_skiplist::SkipMap` or custom with epoch GC).

Properties:

* Readers: lock-free
* Writer: fast inserts under the existing per-branch commit lock
* Iteration: ordered (prefix scans are native, no separate `BTreeSet`)

Publication via `ArcSwap<Memtable>` so readers hold `Arc<Memtable>` references and are never blocked.

### 2.4 Memtable Structure

```
Memtable {
  id: MemtableId (u64)
  state: Active | Frozen
  approx_bytes: AtomicU64
  created_at_commit: u64
  max_commit_seen: AtomicU64

  // Ordered store: InternalKey -> ValueRef
  map: ConcurrentOrderedMap<InternalKey, ValueRef>

  // Optional: per-prefix counters for heuristics
  stats: MemtableStats
}
```

#### ValueRef Encoding

```
ValueRef {
  kind: u8         // 1=PUT, 2=DEL
  len: u32
  bytes: [u8; len] // for PUT only
}
```

Tombstone has `len=0`.

#### Memory Accounting

`approx_bytes` increments on each insert: key bytes + value bytes + structure overhead estimate. This drives flush thresholds.

### 2.5 Get Algorithm (memtable)

`memtable_get_versioned(key, snapshot_commit)`:

1. Compute `seek_key = InternalKey(key, commit_id = +∞)`:
   * `TypedKeyBytes || EncodeDesc(u64::MAX)`
2. Seek iterator to `seek_key`
3. While iterator key matches `TypedKeyBytes`:
   * Decode `commit_id`
   * If `commit_id <= snapshot_commit`: return first PUT or tombstone
   * Else: continue
4. Not found

Because `commit_id` is descending within a key, the result is typically found on the first or second entry.

### 2.6 Prefix Scan Algorithm (memtable)

`memtable_iter_seek(prefix_typed_key_bytes)`:

* Seek to `InternalKey(prefix, u64::MAX)`
* Iterate forward while TypedKey starts with prefix
* Caller does MVCC filtering across merged streams (see [Read/Write Algorithms §4.3](read-write-algorithms.md))

This supports `list_by_type` by choosing prefix = `(namespace, type_tag)`.

---

## 3. Memtable Rotation and Freezing

### 3.1 Rotation Trigger

Rotate the active memtable when any threshold is exceeded:

* `approx_bytes >= memtable_max_bytes` (primary — default 128 MB, see [Architecture §16](tiered-storage.md))
* Entry count exceeds cap (secondary)
* Optional: time-based rotation

### 3.2 Rotation Algorithm (per branch)

Under the branch commit lock, after applying a commit:

1. If the active memtable exceeds threshold:
   * Create new active memtable
   * Atomically swap `mem_active` pointer via `ArcSwap`
   * Mark old as `Frozen`
   * Push old into `mem_immut` list
   * Enqueue flush job with `(branch_id, memtable_id)`

Readers never block — they continue reading the old memtable via their existing `Arc` reference until it is dropped.

---

## 4. Flush Pipeline Overview

Flush converts a **frozen memtable** into durable immutable structures.

Pipeline phases:

1. **Flush file write** — crash-safe staging artifact in `tmp/`
2. **Segment finalize** — KV segment `.sst` written and fsync'd
3. **Manifest publish** — segment becomes visible
4. **Memtable drop** — after visibility is established

Why a flush file at all?

* It provides a durable checkpoint of "these memtable contents have been serialized" so a crash mid-build does not force redoing heavy work or risk partial segments.
* It decouples "serialize entries" (sequential write, fast) from "build segment indexes/filters" (compute-intensive).

Segment build can also be done directly from the memtable. The flush file is an explicit operational boundary that simplifies recovery reasoning.

---

## 5. Flush File Format (`tmp/flush-<branch>-<memtable_id>.flush`)

### 5.1 Purpose

The flush file is a durable, sequential representation of a frozen memtable that is:

* Fast to write (sequential append)
* Fast to scan to build an `.sst` (single pass)
* Easy to validate and recover (per-record checksums)

### 5.2 Layout (append-only, framed records)

```
| FlushHeader |
| Record* |
| FlushFooter |
```

#### FlushHeader (fixed)

```
magic: 8 bytes = "STRAFLU\0"
format_version: u16
branch_id: u128 (or u64)
memtable_id: u64
created_unix_ms: u64
created_at_commit: u64
max_commit_seen: u64
record_count: u64 (0 initially, backfilled optional)
flags: u32
header_crc32c: u32
```

#### Record Framing (repeated)

Each record stores one memtable entry (InternalKey + value):

```
Record:
  record_magic: u32 = 0xF17A5EED
  record_len: u32
  crc32c: u32
  payload: bytes[record_len]
```

Payload:

```
internal_key_len: varint
internal_key_bytes
value_kind: u8        // 1=PUT, 2=DEL
value_len: varint
value_bytes
```

**Ordering guarantee:** Records **must** be written in sorted order by InternalKey (same ordering as the memtable skiplist). This enables single-pass `.sst` build during finalize.

#### FlushFooter

```
magic: 8 bytes = "STRAFEND"
total_records: u64
file_crc64: u64 (optional)
```

### 5.3 Writing Protocol (crash safety)

The flush file is written to a temporary name and atomically renamed only when complete:

1. Create: `tmp/flush-... .partial`
2. Write header (with placeholder counts)
3. Stream sorted entries:
   * Iterate memtable in-order
   * Write framed records
4. Write footer with record count
5. fsync file
6. Rename to `tmp/flush-... .flush`
7. fsync directory (recommended on Linux)

If crash happens:

* `.partial` file exists: discard (or attempt recovery by scanning records until last valid CRC)
* `.flush` file exists: treat as complete and eligible for finalize

See [File Layout §11](storage-file-layout.md) for the atomic publish protocol that the finalize step feeds into.

---

## 6. Finalize: Flush File → KV Segment (`.sst`)

Finalize reads the flush file and produces a KV segment with data blocks, index block, filter block, and properties. See [File Layout §5](storage-file-layout.md) for the complete KV segment format.

### 6.1 Finalize Algorithm (single pass)

Inputs:

* Flush file `.flush`
* Target segment id `seg_id`
* Configured data block size (default 64 KB)
* Restart interval for key prefix compression

Steps:

1. Open flush file, validate header CRC
2. Initialize:
   * Data block builder
   * Index builder
   * Bloom builder over TypedKey (without commit_id)
   * Stats counters
3. For each record:
   * Validate record CRC
   * Decode `internal_key` + value
   * Extract `TypedKeyBytes` and `commit_id`
   * Append to current data block (prefix compress on TypedKey, encode commit_id + value_kind + value)
   * Add TypedKey to bloom builder
4. When block reaches target size:
   * Seal block, write to `.sst.partial`
   * Add index entry pointing to block
   * Start a new block
5. At end:
   * Seal last block
   * Write index block, filter block, properties block
   * Write footer
6. fsync `.sst.partial`, rename to `segments/kv/kvseg-<seg_id>.sst`, fsync directory

### 6.2 Manifest Publish

After the segment file is durable:

* Publish new MANIFEST (or update SegmentView) that includes the new KV segment in the branch's visible segment list.

Only after the manifest update is durable can:

* The frozen memtable be dropped
* The flush file be deleted

### 6.3 Deleting Flush File

Once the segment is visible:

* Delete `tmp/flush-... .flush`
* If crash happens before deletion, it is harmless — recovery detects "segment already published" and deletes stale flush files.

---

## 7. Recovery Behavior

Recovery must handle three crash boundary cases:

### 7.1 WAL Has Commits Not Reflected in Memtables/Segments

Replay WAL commits into a new active memtable (normal recovery path). See [File Layout §3.2](storage-file-layout.md) for the WAL record format.

### 7.2 Flush File Exists but Segment Not Published

* If `.flush` exists:
  * Run finalize to produce `.sst`
  * Publish manifest update
* If only `.partial` exists:
  * Discard `.partial` and rebuild from WAL replay (simplest)
  * Alternatively: salvage by scanning for last valid record CRC and then finalize

### 7.3 Segment Exists but Manifest Not Updated

* Segment file is "orphaned"
* Recovery validates segment integrity, then publishes manifest to adopt it
* This is why segment filenames include `seg_id` and headers include commit ranges — they are self-describing

---

## 8. Performance Notes

### Hot Get/Put

* **Puts:** memtable skiplist insert only, plus WAL cost (controlled by durability mode)
* **Gets:** memtable seek is O(log M) but M is bounded by `memtable_max_bytes`; in practice sub-microsecond for working-set keys

### Flush Cost

Flush is sequential write (flush file) + sequential read (finalize). Optimized for SSD/NVMe. The flush file keeps finalize simple and crash-safe.

### `list_by_type`

Because memtable and segments are both ordered by TypedKey, `list_by_type` is a prefix scan across:

* Active memtable
* Frozen memtables
* Segments (selected by branch manifest)

No resident `BTreeSet` required. See [Read/Write Algorithms §4.3](read-write-algorithms.md) for the complete prefix scan algorithm with merge iterators.

---

## 9. Interfaces

### 9.1 Memtable API

* `put(internal_key, value_ref)`
* `get_versioned(typed_key, snapshot_commit) -> Option<ValueRef>`
* `iter_seek(prefix_typed_key) -> Iterator<InternalEntry>`
* `freeze() -> FrozenMemtable`

### 9.2 Flush Manager API

* `enqueue_flush(branch_id, frozen_memtable)`
* `finalize_flush(flush_file) -> seg_id`
* `publish_segment(branch_id, seg_id, metadata)`

---

## 10. Key Decisions

1. **InternalKey ordering:** `(TypedKey ASC, commit_id DESC)` — shared across memtable, flush file, and segments
2. **Memtable structure:** concurrent skiplist, lock-free reads, single-writer under branch commit lock
3. **Flush file is sorted** and fully checksummed (per-record CRC)
4. **Finalize is single pass** from flush file to `.sst`
5. **Manifest publish is the visibility gate** for dropping memtables and deleting flush files
6. **No branch identity in keys** — branch visibility via manifest selection (see [Architecture §7](tiered-storage.md))
