# Storage File Layout Specification

**Status:** Proposal
**Author:** Design Team
**Date:** 2026-03-01

> **Part of the Strata Storage Architecture proposal.**
> - [Architecture Overview](tiered-storage.md) — design vision, principles, system model
> - **File Layout Specification** (this document) — on-disk formats for WAL, manifests, and segments
> - [Read/Write Algorithms](read-write-algorithms.md) — concurrency model, ACID commit path, lock-free reads
> - [Memtable and Flush Pipeline](memtable-flush-file.md) — memtable design, flush file format, recovery
> - [Industry Research](tiered-storage-research.md) — reference analysis of Redis, RocksDB, LMDB, et al.

This document specifies the on-disk format for every file type in the Strata storage layer: WAL records, manifest snapshots, KV segments, BM25 segments, vector segments, and supporting structures.

---

## 0. Goals and Invariants

**Goals**

* Immutable, mmap-friendly segment files for read paths.
* Append-only WAL for durability and recovery.
* Branches are metadata views (manifests), not data copies.
* MVCC visibility by `(branch_id, snapshot_commit)` without keeping per-key indexes in heap.
* 100M records: metadata scales with segments and blocks, not keys.

**Invariants**

1. **Durability:** a commit is durable when its WAL record is fsync'd.
2. **Atomicity:** each commit is either fully applied to the logical state or not visible at all.
3. **Isolation (snapshot):** reads at snapshot `C` observe the newest version with `commit_id <= C` for that branch view.
4. **Branching:** forking a branch clones a manifest, no data rewrite.
5. **Time travel:** any snapshot `(branch, commit_id)` is readable as long as `commit_id >= branch.low_watermark_commit`. Queries below the retention horizon return an explicit `CommitPruned` error rather than partial data. See [Architecture Overview §13](tiered-storage.md) for retention policy details.
6. **Immutability:** segment and index blocks are never modified in place once sealed.

---

## 1. Directory Layout

Database root: `db/`

```
db/
  CURRENT                      # text file: points to active MANIFEST
  IDENTITY                     # unique db id, format version, created_at
  LOCK                         # optional advisory lock file
  wal/
    wal-0000000000000001.log
    wal-0000000000000002.log
    ...
  manifests/
    MANIFEST-0000000000000007  # immutable snapshot of metadata state
    MANIFEST-0000000000000008
  branches/
    branch-<branch_id>.head     # small file: current head commit id (optional, fast path)
    branch-<branch_id>.ref      # points to manifest revision (optional)
  segments/
    kv/
      kvseg-<seg_id>.sst
      kvseg-<seg_id>.idx        # optional separate index file (or embedded)
    bm25/
      bm25seg-<seg_id>.seg
    vec/
      vecseg-<seg_id>.seg
    graph/
      graphseg-<seg_id>.seg
  tmp/
    build-<seg_id>.partial      # staging files for segment build
  gc/
    tombstones.log              # optional GC bookkeeping (append-only)
```

**Notes**

* All "big" structures are immutable segment files under `segments/`.
* `CURRENT` is the only mutable file that must be updated on manifest publish (atomic rename).
* `tmp/` files are written then atomically renamed into `segments/`.

---

## 2. File Naming and IDs

* `commit_id`: monotonic `u64` assigned at commit time.
* `seg_id`: unique `u128` (or `u64` + random suffix) to avoid collisions across crashes.
* `manifest_id`: monotonic `u64` embedded in MANIFEST filename.
* WAL sequence: monotonic `u64` in filename for ordering.

Rule: **files are never overwritten**, only new files created and pointers advanced.

---

## 3. WAL Format (`wal/wal-*.log`)

WAL is append-only, record-framed, checksummed. Intended for fast sequential write.

### 3.1 WAL Record Framing

Each record:

```
| magic (4) | version (1) | record_type (1) | flags (2) |
| length (4) | crc32c (4) |
| payload (length bytes) |
```

* `magic`: `0x53545257` ("STRW")
* `record_type`: `0x01=COMMIT`, `0x02=CHECKPOINT`, `0x03=SEGMENT_PUBLISHED`
* `crc32c` over `record_type|flags|length|payload`

### 3.2 COMMIT Payload

A commit is the atomic unit. Payload uses a compact binary encoding (varints). One commit can contain multiple writes.

```
CommitRecord {
  commit_id: u64
  branch_id: u128 (or u64)
  parent_commit_id: u64          # for branch head lineage (optional)
  wall_time_unix_ms: u64
  tx_flags: u32                  # e.g., "merge_commit", "system"
  write_count: u32
  writes: [WriteOp; write_count]
  user_crc: u32 (optional)       # higher-level checksum
}
```

Each `WriteOp`:

```
WriteOp {
  op_type: u8                    # PUT=1, DEL=2, CAS_PUT=3, CAS_DEL=4
  key_len: u32
  key_bytes: [u8; key_len]       # encoded TypedKey (see section 8)
  value_len: u32                 # 0 for deletes
  value_bytes: [u8; value_len]
  cas_expected_commit: u64       # only for CAS ops
}
```

**Durability point:** after appending the COMMIT record, fsync WAL.

**Recovery rule:** during recovery, replay COMMIT records in order, applying them to memtables and rebuilding any missing segment publish metadata. See [Memtable and Flush Pipeline §7](memtable-flush-file.md) for recovery cases.

---

## 4. Manifest Format (`manifests/MANIFEST-*`)

A MANIFEST is an immutable snapshot of **metadata state**: branches, segment inventory, reference counts, and visibility rules. Publishing a new MANIFEST is the primary "commit" of background processes (segment builds, compactions, GC), not user writes.

Design choice: **snapshot MANIFEST** (full snapshot each time, chunked and compressed). Easier to reason about than an append-only manifest log, and avoids manifest compaction as a separate concern.

### 4.1 MANIFEST File Structure

```
| FileHeader |
| Chunk[0] ... Chunk[n-1] |
| ChunkIndex |
| Footer |
```

#### FileHeader (fixed)

```
FileHeader {
  magic: [u8; 8] = "STRAMAN\0"
  format_version: u16
  manifest_id: u64
  created_unix_ms: u64
  db_uuid: [u8; 16]
  chunk_count: u32
  flags: u32
}
```

#### Chunks (variable, compressed)

Each chunk is a typed blob:

* `BRANCH_TABLE`
* `SEGMENT_TABLE`
* `REFCOUNTS`
* `GC_STATE`
* `PARAMS`

Chunk framing:

```
Chunk {
  chunk_type: u16
  codec: u8           # 0=none, 1=zstd
  reserved: u8
  uncompressed_len: u32
  compressed_len: u32
  bytes: [u8; compressed_len]
  crc32c: u32
}
```

#### ChunkIndex

An array mapping chunk_type → offset/len for quick access.

#### Footer

```
Footer {
  magic: [u8; 8] = "STRAMEND"
  file_crc64: u64   # optional
}
```

### 4.2 Branch Table Chunk

Defines each branch's logical view. This is the on-disk representation of the `BranchManifest` from the [Architecture Overview §5.3](tiered-storage.md).

```
BranchEntry {
  branch_id: u128
  head_commit_id: u64
  manifest_epoch: u64               # monotonically increases per branch update
  base_segment_ids: [seg_id]        # shared segments
  overlay_segment_ids: [seg_id]     # branch-local delta segments
  tombstone_refs: [tombstone_id]    # optional
  retention_policy: RetentionPolicy
  low_watermark_commit: u64         # oldest commit guaranteed retained for this branch
}
```

**Fork:** clone an existing `BranchEntry` with a new `branch_id`, same segment lists, same head.

**Merge:** create a new commit in WAL (merge commit) and update the `head_commit_id` and segment references accordingly.

### 4.3 Segment Table Chunk

Inventory of all segments and their metadata.

```
SegmentEntry {
  seg_id: u128
  seg_type: u8            # KV=1, BM25=2, VEC=3, GRAPH=4
  created_unix_ms: u64
  commit_min: u64         # inclusive
  commit_max: u64         # exclusive
  keyspace_min: bytes     # optional, for KV/BM25
  keyspace_max: bytes
  file_path: string
  file_size_bytes: u64
  stats: SegmentStats     # type-specific
  state: u8               # 1=ACTIVE, 2=COMPACTED, 3=GC_ELIGIBLE
}
```

**Reference counting** is either stored separately (REFCOUNTS chunk) or derived by scanning branch entries during manifest build.

---

## 5. KV Segment File Format (`segments/kv/kvseg-*.sst`)

KV segments are immutable sorted runs of **versioned entries**. They support:

* point lookups via block index + binary search
* prefix/range scans via iterator across blocks
* MVCC filtering by `commit_id`

### 5.1 High-Level Layout

```
| KVHeader |
| DataBlocks... |
| MetaBlocks... |
| IndexBlock |
| FilterBlock |
| PropertiesBlock |
| Footer |
```

All blocks use a common framing for mmap-friendly access.

#### Block Framing (common to all block types)

```
Block {
  block_type: u8            # DATA=1, INDEX=2, FILTER=3, PROPS=4, META=5
  codec: u8                 # 0=none, 1=zstd, 2=lz4
  reserved: u16
  uncompressed_len: u32
  compressed_len: u32
  bytes: [u8; compressed_len]
  crc32c: u32
}
```

### 5.2 KVHeader

```
KVHeader {
  magic: [u8; 8] = "STRAKV\0\0"
  format_version: u16
  seg_id: u128
  commit_min: u64
  commit_max: u64
  entry_count: u64
  data_block_size: u32        # e.g. 64 KiB
  key_restart_interval: u16   # for prefix compression
  flags: u32
}
```

### 5.3 Data Block Contents

Entries are sorted by `(TypedKey ASC, commit_id DESC)` so that the newest version comes first for a given key. This matches the memtable sort order (see [Memtable §2.2](memtable-flush-file.md)), enabling single-pass flush-to-segment.

Within a data block:

* prefix-compressed keys (LevelDB-style restart points)
* varint-encoded fields

Entry encoding:

```
Entry {
  shared_key_prefix_len: varint
  key_suffix_len: varint
  key_suffix_bytes
  commit_id: u64                 # descending order within a key
  value_kind: u8                 # PUT=1, DEL=2
  value_len: varint              # 0 for DEL
  value_bytes
}
```

**Why commit_id is stored per entry:** MVCC visibility for snapshot `C` is "first entry for key with `commit_id <= C`". See [Read/Write Algorithms §4](read-write-algorithms.md) for the lookup algorithm.

### 5.4 Index Block

Index maps from key-range to data block offsets.

Each index entry:

```
IndexEntry {
  separator_key: bytes      # shortest key >= last key of block
  block_offset: u64
  block_len: u32
}
```

This enables:

* binary search over blocks for point lookup
* seek for scans

### 5.5 Filter Block (Bloom)

Bloom filter is built over **TypedKey (without commit_id)**, so "does key exist at all in this segment" can be tested quickly.

* bits-per-key configurable (default ~10, yielding ~1% false positive rate)
* stored as standard bloom bitset + hash seeds

### 5.6 Properties Block

Contains:

* min/max key
* distinct key count estimate
* tombstone count
* average value size
* build parameters

Useful for compaction planning and segment selection.

### 5.7 Footer

Footer points to the tail blocks for quick access.

```
Footer {
  index_block_offset: u64
  index_block_len: u32
  filter_block_offset: u64
  filter_block_len: u32
  props_block_offset: u64
  props_block_len: u32
  magic: [u8; 8] = "STRAKEND"
}
```

---

## 6. BM25 Segment File Format (`segments/bm25/bm25seg-*.seg`)

BM25 segments are immutable inverted index segments. They follow the same lifecycle as KV segments (build → seal → adopt → compact → retire) but use a domain-specific internal layout.

High-level:

```
| Header |
| LexiconBlocks... |
| PostingsBlocks... |
| DocStoreBlocks... (optional, for snippet retrieval) |
| TermIndexBlock |
| DocIndexBlock |
| Footer |
```

Key points:

* Lexicon: term → postings list pointer
* Postings: docID deltas + tf + positions (optional)
* Doc store: doc metadata, optionally compressed

Visibility:

* Doc versions are bound to commit ranges; deletion handled by tombstones in KV or a doc tombstone bitmap per segment.
* The branch manifest determines which BM25 segments are visible.

BM25 segments use the same block framing, footer conventions, and checksum approach as KV segments for consistency.

---

## 7. Vector Segment File Format (`segments/vec/vecseg-*.seg`)

Vector segments store:

* Vector payloads (optionally quantized)
* ANN index structures (HNSW or disk-friendly)
* Mapping from docID/vectorID to payload offsets
* Optional per-segment routing info (centroids)

High-level:

```
| Header |
| VectorDataBlocks... |
| IdMapBlock |
| AnnGraphBlocks... |
| AnnIndexBlock |
| Footer |
```

Visibility:

* Vectors are materialized from commits; deletions via tombstones or per-segment live bitmap.
* The branch manifest controls visible segments + overlays.

Same block framing, footer style, and checksums as KV and BM25 segments.

---

## 8. TypedKey Encoding

To make `list_by_type(branch_id, type_tag)` a prefix scan without a resident BTree, keys must sort correctly on disk.

### 8.1 Logical Key (what callers see)

```
Key {
  namespace: bytes
  type_tag: u32
  user_key: bytes
}
```

### 8.2 Storage Key (what KV segments store)

```
TypedKeyBytes =
  varint(len(namespace)) | namespace |
  u32_be(type_tag) |
  varint(len(user_key)) | user_key
```

Branching is handled by manifest selection, not by embedding `branch_id` into the key. This keeps segments shareable across branches — forking never rewrites data.

Branch-local key scoping, if needed, is modeled at the namespace level (namespace includes branch scope) or via the branch overlay segment list in the manifest.

This encoding is shared across memtables, flush files, and KV segments. See the [Architecture Overview §7](tiered-storage.md) for the design rationale and [Memtable §2.2](memtable-flush-file.md) for the `InternalKey` composition with descending `commit_id`.

**`list_by_type`** becomes:

* Scan across visible KV segments (selected by manifest) with prefix = `(namespace, type_tag)`.
* Use merge iterators across segments. See [Read/Write Algorithms §4.3](read-write-algorithms.md) for the prefix scan algorithm.

---

## 9. Merge Iterator and Snapshot Visibility

Because data is spread across multiple immutable segments, the read path relies on iterators:

* **Point lookup:**
  * Check memtable (in-memory)
  * Consult per-segment bloom filter to skip segments
  * Binary search into data blocks via index

* **Prefix scans:**
  * Seek iterator in each visible segment to prefix
  * k-way merge of iterators
  * For each logical key, pick newest `commit_id <= snapshot`

No file format change is needed beyond the `(key, commit_id DESC)` ordering in data blocks.

See [Read/Write Algorithms §4](read-write-algorithms.md) for the complete algorithms with pseudocode.

---

## 10. Garbage Collection and Retention

Segments cannot be reclaimed until no branch manifest references them.

To make GC cheap and correct, the manifest tracks:

* per-branch `low_watermark_commit`
* segment `(commit_min, commit_max)`
* segment reference counts (or derivable from branch entries)

A segment is GC-eligible if:

* `seg.commit_max <= min(low_watermark_commit across branches that reference it)`
* AND it is not referenced by any manifest (after compaction adoption)

GC deletes files by unlink, but only after publishing a new MANIFEST that removes references. See the [Architecture Overview §12](tiered-storage.md) for branch lifecycle and lease management.

---

## 11. Atomic Publish Protocols

### 11.1 Publishing a New Segment

1. Write segment to `tmp/build-<seg_id>.partial`
2. fsync file
3. Atomically rename to `segments/.../<seg_id>.*`
4. Publish new MANIFEST referencing it
5. Atomically update `CURRENT` (write temp + rename)

If crash occurs:

* Before manifest publish: orphan segment can be discovered and either adopted or deleted.
* After manifest publish: segment must exist; recovery verifies.

### 11.2 Publishing New Manifest

1. Write `manifests/MANIFEST-<id>.partial`
2. fsync
3. Rename to `manifests/MANIFEST-<id>`
4. Write `CURRENT.partial` with new manifest filename
5. fsync
6. Rename `CURRENT.partial` → `CURRENT`

This gives atomic manifest updates without requiring fsync on the entire directory every time (though fsync on the directory on Linux is recommended for maximal safety).

**Crash between segment rename and CURRENT update:** If a crash occurs after writing the new MANIFEST but before updating CURRENT, recovery opens the CURRENT-referenced manifest (which is stale). Recovery then scans `manifests/` for any MANIFEST with a higher `manifest_id`, validates its integrity, and adopts it as the active manifest. Orphaned segments (files in `segments/` not referenced by any manifest) are discovered during recovery and either adopted (if their commit ranges are valid) or deleted.

See [Memtable and Flush Pipeline §5.3](memtable-flush-file.md) for the crash-safe flush file writing protocol that feeds into this.

---

## 12. Format Versioning and Compatibility

Every file has:

* Magic bytes
* `format_version`
* Feature flags

Rules:

* Readers must reject unknown major format versions.
* Minor version changes can be gated behind feature flags.

---

## 13. What This Layout Provides

Mechanically, KV segments look like SSTables because SSTables are the right immutable building block.

But the Strata-specific value is not the SST format. It is:

* **Branch manifests as first-class state selectors** — not an afterthought on top of a generic LSM
* **Segment sharing across branches** — forking copies a manifest, not data
* **Unified lifecycle phases and visibility across KV/BM25/VEC/GRAPH** — all segment types share the same manifest, ref counting, GC rules, and lifecycle phases, while using type-specific internal algorithms for build and compaction
* **Compaction as manifest-driven segment rewrite** — producing new segments and atomically updating manifests, with multi-primitive coordination

The file layout supports this by:

* Keeping segments immutable and shareable (no branch identity in keys)
* Making manifests authoritative for visibility and GC
* Keeping WAL as the canonical commit history
