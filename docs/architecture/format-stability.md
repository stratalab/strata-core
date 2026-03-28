# Format Stability Guarantee

This document specifies every on-disk format used by StrataDB, their version
numbers, and the compatibility contract for each. It is the authoritative
reference for issue #1556.

## Compatibility Contract

**Backward compatibility (newer code reads older data):** Guaranteed within a
major version. A newer Strata binary can always open databases created by an
older binary, as long as both are in the same major version series.

**Forward compatibility (older code reads newer data):** Rejected with a clear
error. If a database was written by a *newer* Strata version with a higher
format version, the older binary will refuse to open it and print a message
indicating the version mismatch and that an upgrade is required.

**Migration strategy:** Format version bumps within Strata transparently
upgrade older formats on persist (e.g., MANIFEST v1 → v2 upgrade on write).
No manual migration step is required.

## Format Version Registry

| Component | Magic | Current Version | Min Readable | File Pattern |
|-----------|-------|-----------------|--------------|--------------|
| WAL segment header | `STRA` | 2 | 1 | `wal/wal-NNNNNN.seg` |
| WAL record | — | 1 | 1 | (within WAL segments) |
| MANIFEST | `STRM` | 2 | 1 | `MANIFEST` |
| Snapshot | `SNAP` | 1 | 1 | `snap-NNNNNN.chk` |
| Segment metadata | `STAM` | 1 | 1 | `wal/wal-NNNNNN.meta` |
| KV segment | `STRAKV\0\0` | 7 | 4 | `segments/<branch>/*.sst` |
| KV segment footer | `STRAKEND` | — | — | (within KV segments) |
| Segment manifest | `STRAMFST` | 2 | 1 | `segments/<branch>/segments.manifest` |

## Detailed Format Specifications

### WAL Segment Header (32 bytes v1, 36 bytes v2)

```text
Offset  Field              Type      Size  Notes
0-3     magic              [u8;4]    4     "STRA" (0x53545241)
4-7     format_version     u32 LE    4     Currently 2
8-15    segment_number     u64 LE    8     Monotonically increasing
16-31   database_uuid      [u8;16]   16    Integrity check across segments
32-35   header_crc         u32 LE    4     CRC32 of bytes 0-31 (v2+ only)
```

**Version history:**
- v1 (32 bytes): No header CRC. Still readable with a warning.
- v2 (36 bytes): Adds CRC32 of the first 32 bytes for header integrity.

### WAL Record (variable length)

```text
Offset  Field              Type      Size  Notes
0-3     length             u32 LE    4     Byte count of (format_ver..CRC)
4       format_version     u8        1     Currently 1
5-12    txn_id             u64 LE    8     Transaction ID from engine
13-28   branch_id          [u8;16]   16    Branch UUID
29-36   timestamp          u64 LE    8     Microseconds since Unix epoch
37+     writeset           bytes     var   Codec-encoded mutations
last 4  crc32              u32 LE    4     CRC32 of format_version..writeset
```

Records are length-prefixed and self-delimiting. The CRC covers the payload
(format version through writeset), not the length prefix.

### Writeset (within WAL records)

```text
┌──────────────────┬────────────────────┐
│ count (u32 LE)   │ Mutations (var)    │
└──────────────────┴────────────────────┘

Mutation = Tag (u8) + EntityRef + variant fields
  0x01 = PUT    {entity_ref, value: bytes, version: u64}
  0x02 = DELETE {entity_ref}
  0x03 = APPEND {entity_ref, value: bytes, version: u64}

EntityRef = entity_type(u8) + branch_id([u8;16]) + collection(len-prefixed str) + key(len-prefixed str)

Primitive type tags: KV=0x01, Event=0x02, Branch=0x03, Json=0x04, Vector=0x05, Graph=0x06
```

### MANIFEST (variable length)

```text
Offset  Field                      Type      Size  Notes
0-3     magic                      [u8;4]    4     "STRM"
4-7     format_version             u32 LE    4     Currently 2
8-23    database_uuid              [u8;16]   16
24-27   codec_id_len               u32 LE    4
28+     codec_id                   UTF-8     var
?       active_wal_segment         u64 LE    8
?       snapshot_watermark         u64 LE    8     0 = none
?       snapshot_id                u64 LE    8     0 = none
?       flushed_through_commit_id  u64 LE    8     0 = none (v2+)
last 4  crc32                      u32 LE    4     CRC32 of all preceding
```

**Version history:**
- v1: No flushed_through_commit_id field.
- v2: Adds flushed_through_commit_id for WAL truncation and delta replay.

Persisted atomically via write → fsync → rename.

### Snapshot Header (64 bytes)

```text
Offset  Field              Type      Size  Notes
0-3     magic              [u8;4]    4     "SNAP"
4-7     format_version     u32 LE    4     Currently 1
8-15    snapshot_id        u64 LE    8
16-23   watermark_txn      u64 LE    8     All txns <= this are materialized
24-31   created_at         u64 LE    8     Microseconds since epoch
32-47   database_uuid      [u8;16]   16
48      codec_id_len       u8        1
49-63   reserved           [u8;15]   15
```

Followed by codec ID string, then sections (primitive_type u8 + data_len u64 + data),
then a trailing CRC32.

### Segment Metadata Sidecar (60 bytes fixed)

```text
Offset  Field              Type      Size  Notes
0-3     magic              [u8;4]    4     "STAM"
4-7     version            u32 LE    4     Currently 1
8-15    segment_number     u64 LE    8
16-23   min_timestamp      u64 LE    8     u64::MAX = empty
24-31   max_timestamp      u64 LE    8     0 = empty
32-39   min_txn_id         u64 LE    8     u64::MAX = empty
40-47   max_txn_id         u64 LE    8     0 = empty
48-55   record_count       u64 LE    8
56-59   crc32              u32 LE    4     CRC32 of bytes 0-55
```

Optional per-segment metadata for O(1) coverage checks. Missing or corrupted
`.meta` files fall back to full segment scan.

### KV Segment File

```text
| KVHeader (64 bytes)       |
| DataBlock 0..N-1          |
| SubIndexBlock 0..P-1      |  (v7 partitioned only)
| BloomPartition 0..K-1     |
| FilterIndexBlock          |
| TopLevelIndex / IndexBlock|
| PropertiesBlock           |
| Footer (56 bytes)         |
```

**Header (64 bytes):**
```text
Offset  Field              Type      Size  Notes
0-7     magic              [u8;8]    8     "STRAKV\0\0"
8-9     format_version     u16 LE    2     Currently 7 (min readable: 4)
10-15   reserved           [u8;6]    6
16-23   commit_min         u64 LE    8
24-31   commit_max         u64 LE    8
32-39   entry_count        u64 LE    8
40-43   data_block_size    u32 LE    4
44-63   reserved           [u8;20]   20
```

**Block Frame (12 bytes overhead per block):**
```text
block_type(1) + codec(1) + reserved(2) + data_len(u32) + data(var) + crc32(4)
```

Block types: Data=1, Index=2, Filter=3, Props=4, FilterIndex=5, SubIndex=6.

**Footer (56 bytes):**
```text
index_block_offset(u64) + index_block_len(u32) +
filter_block_offset(u64) + filter_block_len(u32) +
props_block_offset(u64) + props_block_len(u32) +
index_type(u8) + reserved(11) + magic("STRAKEND", 8)
```

### Segment Manifest (variable length)

```text
"STRAMFST" (8B magic) | u16 version (=2) | 6B reserved | u32 entry_count
Own entries: [u16 filename_len | filename bytes | u8 level] ...
Inherited layers (v2+):
  u32 layer_count
  Per layer: [u8;16] branch_id | u64 fork_version | u8 status | u32 entry_count
             [u16 filename_len | filename bytes | u8 level] ...
u32 CRC32
```

## Version Validation Summary

Every format reader validates the version field and rejects versions higher
than the current build supports:

| Component | Validation | Error on future version |
|-----------|-----------|------------------------|
| WAL segment header | `version > 2` → reject | `tracing::error` + `None` |
| WAL record | `version != 1` → reject | `WalRecordError::UnsupportedVersion` |
| MANIFEST | `version > 2` → reject | `ManifestError::UnsupportedVersion` |
| Snapshot header | `version > 1` → reject | `SnapshotHeaderError::UnsupportedVersion` |
| Segment metadata | `version != 1` → reject | `SegmentMetaError::UnsupportedVersion` |
| KV segment header | `version > 7` or `< 4` → reject | `tracing::error` + `None` |
| Segment manifest | `version > 2` → reject | `io::Error(InvalidData)` |

## Integrity Guarantees

- **CRC32 checksums** on every format (WAL records, segment headers, MANIFEST,
  snapshots, KV segment blocks, segment manifests, metadata sidecars).
- **Magic bytes** on every file type for quick identification and corruption
  detection.
- **Atomic persistence** for MANIFEST and segment manifests via write → fsync →
  rename → dir fsync.
- **Length-prefixed records** in WAL for self-delimiting corruption recovery.
- **Byte-by-byte scan** in WAL reader after corruption (up to 8MB window) to
  recover subsequent valid records.

## Recovery Guarantees

1. **No committed data is lost** — all committed transactions survive restart.
2. **Uncommitted data may be dropped** — incomplete transactions are discarded.
3. **No data is invented** — recovery only replays what was written.
4. **Recovery is idempotent** — multiple restart cycles produce the same state.
5. **Recovery is deterministic** — same WAL produces same state.

## Upgrade Path (0.6.x → 0.7.x)

- All format readers accept the minimum versions listed above.
- MANIFEST v1 files are transparently upgraded to v2 on next persist.
- WAL v1 segment headers are readable; new segments are written as v2.
- KV segments with format version 4-7 are all readable.
- No manual migration step is required.
- If a future version introduces an incompatible format change, the database
  open path will reject it with a clear error message naming the versions
  involved and instructing the user to upgrade.

## Design Conventions for Future Format Changes

1. **Always bump the version constant** when adding fields or changing layout.
2. **Add new fields at the end** of variable-length formats (MANIFEST, writeset).
3. **Use reserved bytes** in fixed-size headers for future fields.
4. **Read conditionally**: `if version >= N { parse new field }`.
5. **Write the current version**: always emit the latest format on persist.
6. **Reject unknown futures**: return a clear error for `version > CURRENT`.
7. **Test the boundary**: add a test that constructs a future-version file and
   verifies the reader rejects it.
