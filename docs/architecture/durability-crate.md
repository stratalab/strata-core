# Durability Crate Architecture

**Crate:** `strata-durability`
**Dependencies:** `strata-core`, `parking_lot`, `serde`, `rmp-serde`, `crc32fast`, `fs2`, `aes-gcm`, `rand`, `tar`, `zstd`, `xxhash-rust`, `libc` (unix)
**Source files:** ~32 modules, ~15,500 lines total
**Role:** On-disk building blocks — WAL, snapshots, binary formats, codecs, compaction, branch bundles. Recovery orchestration itself lives in `strata_concurrency::RecoveryCoordinator`.

## Overview

The durability crate is the largest in Strata and handles all persistent storage concerns:

| Subsystem | Files | Lines | Purpose |
|-----------|-------|-------|---------|
| WAL | 5 | ~3,800 | Write-ahead log with segmented files |
| Format | 9 | ~3,500 | Binary on-disk formats (WAL, snapshot, MANIFEST) |
| Disk Snapshot | 4 | ~1,450 | Crash-safe snapshot I/O |
| Compaction | 3 | ~1,920 | WAL segment cleanup + tombstone tracking |
| Branch Bundle | 8 | ~2,430 | Portable branch archives (tar.zst) |
| Codec | 4 | ~740 | Encryption/compression abstraction |

## Module Map

```
durability/src/
├── lib.rs                          Re-exports (~160 types)
├── wal/
│   ├── writer.rs                   WalWriter (segment management, durability modes)
│   ├── reader.rs                   WalReader (corruption-resilient parsing)
│   ├── config.rs                   WalConfig (segment size, sync thresholds)
│   └── mode.rs                     DurabilityMode (Cache/Standard/Always)
├── format/
│   ├── wal_record.rs               SegmentHeader, WalSegment, WalRecord
│   ├── writeset.rs                 Mutation, Writeset (transaction mutations)
│   ├── manifest.rs                 Manifest, ManifestManager (atomic persistence)
│   ├── snapshot.rs                 SnapshotHeader, SectionHeader, primitive_tags
│   ├── primitives.rs               SnapshotSerializer (per-primitive binary format)
│   ├── segment_meta.rs             SegmentMeta (optional sidecar files)
│   └── watermark.rs                SnapshotWatermark, CheckpointInfo
├── disk_snapshot/
│   ├── writer.rs                   SnapshotWriter (write-fsync-rename)
│   ├── reader.rs                   SnapshotReader (load + validate)
│   └── checkpoint.rs               CheckpointCoordinator (snapshot + watermark)
├── compaction/
│   ├── wal_only.rs                 WalOnlyCompactor (remove covered segments)
│   └── tombstone.rs                TombstoneIndex (deletion audit trail)
├── branch_bundle/
│   ├── writer.rs                   BranchBundleWriter (tar.zst archive)
│   ├── reader.rs                   BranchBundleReader (validate + extract)
│   ├── wal_log.rs                  WAL.branchlog format (msgpack v2)
│   ├── types.rs                    BundleManifest, BranchExportInfo, etc.
│   └── error.rs                    BranchBundleError
└── codec/
    ├── traits.rs                   StorageCodec trait (object-safe)
    ├── identity.rs                 IdentityCodec (no-op, zero-copy)
    └── aes_gcm.rs                  AesGcmCodec (AES-256-GCM encryption)
```

---

## 1. WAL Subsystem (`wal/`)

The write-ahead log is the durability backbone. One WAL record = one committed transaction.

### 1.1 Durability Modes

```rust
enum DurabilityMode {
    Cache,                                    // No WAL files at all (~250K+ ops/sec)
    Always,                                   // fsync every write (~10ms latency)
    Standard { interval_ms, batch_size },     // Periodic fsync (~30µs latency)
}
```

| Mode | `requires_wal()` | `requires_immediate_fsync()` | Data loss window |
|------|-------------------|------------------------------|------------------|
| Cache | false | false | Everything |
| Always | true | true | Zero |
| Standard | true | false | Up to interval_ms or batch_size writes |

Standard defaults: 100ms interval, 1000 batch size.

### 1.2 WalWriter (`writer.rs`, 1521 lines)

The hot path for durability. Manages segment files and sync behavior.

**State:**

| Field | Type | Purpose |
|-------|------|---------|
| `segment` | `Option<WalSegment>` | Current active segment (None in Cache) |
| `durability` | `DurabilityMode` | Controls sync behavior |
| `config` | `WalConfig` | Segment size (64MB), sync threshold (4MB) |
| `codec` | `Box<dyn StorageCodec>` | Encoding pipeline |
| `bytes_since_sync` | `u64` | Buffered bytes since last fsync |
| `has_unsynced_data` | `bool` | Dirty flag |
| `current_segment_meta` | `Option<SegmentMeta>` | In-memory metadata tracker |

**Append path:**

```
append(record) →
  1. Cache mode? → no-op, return
  2. Serialize record → bytes
  3. Encode through codec
  4. Check segment size → rotate_segment() if over limit
  5. Write to segment file
  6. Track metadata (txn_id, timestamp)
  7. maybe_sync() based on durability mode
```

**Segment rotation:**

```
rotate_segment() →
  1. Close current segment
  2. Write .meta sidecar (SegmentMeta)
  3. Create new segment with incremented number
  4. Warn if >1000 segments (suggests compaction needed)
```

**Pre-serialization optimization:** `append_pre_serialized(record_bytes, txn_id, timestamp)` accepts already-serialized bytes. Used by `TransactionManager::commit_with_wal_arc()` to serialize outside the WAL lock, reducing lock hold time. Uses `encode_cow()` to avoid copies for identity codec.

**Standard mode sync:** Defers to background flush thread. Safety net: if >3× `interval_ms` since last sync, performs inline fsync (detects stalled background thread).

**Multi-process support:** `reopen_if_needed()` detects when another process rotated segments by checking the latest segment number. `append_and_flush()` combines reopen + append + immediate flush for cross-process visibility under WAL file lock.

### 1.3 WalReader (`reader.rs`, 1268 lines)

Stateless, corruption-resilient WAL reader.

**Corruption handling:**

```
For each record in segment:
  1. Read 4-byte length prefix
  2. Read payload + CRC32
  3. Verify CRC32
     ✓ valid → parse record, advance
     ✗ ChecksumMismatch → scan forward byte-by-byte (up to 8MB window)
       looking for next valid record header
     ✗ ParseError (valid CRC, bad payload) → stop immediately
     ✗ InsufficientData → stop (expected partial record at EOF)
```

The byte-by-byte scan after corruption skips damaged regions while preserving subsequent valid records. The 8MB scan window (`MAX_RECOVERY_SCAN_WINDOW`) bounds the cost.

**Watermark-filtered reading:** `read_all_after_watermark(wal_dir, watermark)` uses `.meta` sidecar files for O(1) segment skipping:
- If closed segment's `meta.max_txn_id <= watermark` → skip entire segment
- If `.meta` missing/corrupted → read segment in full (conservative fallback)
- Active segment (latest, no `.meta`) → always read

**Timestamp-based queries** for time-travel:
- `find_segments_for_timestamp(target_ts)` — segments containing records at target timestamp
- `find_segments_for_timestamp_range(min_ts, max_ts)` — overlapping segments
- `find_segments_before_timestamp(target_ts)` — for state reconstruction
- All conservatively include segments with missing metadata

### 1.4 WalConfig (`config.rs`)

| Parameter | Default | Testing | Constraint |
|-----------|---------|---------|------------|
| `segment_size` | 64 MB | 64 KB | >= 1 KB |
| `buffered_sync_bytes` | 4 MB | 16 KB | <= segment_size |

---

## 2. Binary Formats (`format/`)

All formats use little-endian encoding, CRC32 checksums, and magic bytes for identification.

### 2.1 WAL Segment Format (`wal_record.rs`)

```
WAL Segment File (wal-NNNNNN.seg):
┌─────────────────────────────────────────┐
│ SegmentHeader (36 bytes v2)             │
│   magic: "STRA" (4)                     │
│   format_version: u32 (4)               │
│   segment_number: u64 (8)               │
│   database_uuid: [u8; 16] (16)          │
│   header_crc: u32 (4)   ← v2 only      │
├─────────────────────────────────────────┤
│ WalRecord 1                             │
│   length: u32 (4)                       │
│   format_version: u8 (1)                │
│   txn_id: u64 (8)                       │
│   branch_id: [u8; 16] (16)             │
│   timestamp: u64 (8)                    │
│   writeset: [u8; ...] (variable)        │
│   crc32: u32 (4)                        │
├─────────────────────────────────────────┤
│ WalRecord 2 ...                         │
└─────────────────────────────────────────┘
```

Records are self-delimiting (length-prefixed). v1 headers (32 bytes, no CRC) are still readable for backward compatibility.

### 2.2 Writeset Format (`writeset.rs`)

```
Writeset:
  count: u32 (4)
  mutations: [Mutation; count]

Mutation:
  tag: u8 (PUT=0x01, DELETE=0x02, APPEND=0x03)
  entity_ref:
    entity_type: u8 (KV=0x01, EVENT=0x02, STATE=0x03, RUN=0x05, JSON=0x06, VECTOR=0x07)
    branch_id: [u8; 16]
    collection: string (len-prefixed)
    key: string (len-prefixed)
  value: bytes (len-prefixed)  [PUT/APPEND only]
  version: u64 (8)             [PUT/APPEND only]
```

### 2.3 MANIFEST Format (`manifest.rs`)

```
MANIFEST File:
  magic: "STRM" (4)
  format_version: u32 (4)
  database_uuid: [u8; 16] (16)
  codec_id: string (len-prefixed)
  active_wal_segment: u64 (8)
  snapshot_watermark: u64 (8)     ← 0 = none
  snapshot_id: u64 (8)            ← 0 = none
  crc32: u32 (4)
```

`ManifestManager` provides atomic persistence via write-to-temp → fsync → rename pattern. Methods like `set_active_segment()` and `set_snapshot_watermark()` update and persist atomically.

### 2.4 Snapshot Format (`snapshot.rs`, `primitives.rs`)

```
Snapshot File (snap-NNNNNN.chk):
┌──────────────────────────────────────┐
│ SnapshotHeader (64 bytes)            │
│   magic: "SNAP" (4)                  │
│   format_version: u32 (4)            │
│   snapshot_id: u64 (8)               │
│   watermark_txn: u64 (8)             │
│   created_at: u64 (8)                │
│   database_uuid: [u8; 16] (16)       │
│   codec_id_len: u32 (4)              │
│   reserved: [u8; 15] (15)            │
│   (header CRC implicit via footer)   │
├──────────────────────────────────────┤
│ Codec ID (variable)                  │
├──────────────────────────────────────┤
│ Section: primitive_type(1) +         │
│          data_len(8) + data(...)     │
│ Section: ...                         │
├──────────────────────────────────────┤
│ Footer CRC32 (4)                     │
└──────────────────────────────────────┘
```

Primitive types: KV=0x01, EVENT=0x02, STATE=0x03, BRANCH=0x05, JSON=0x06, VECTOR=0x07. (0x04 was TRACE, intentionally skipped.)

`SnapshotSerializer` handles per-primitive binary encoding/decoding through the codec pipeline. Each section starts with a 4-byte entry count, then length-prefixed entries with type-specific fields.

### 2.5 Segment Metadata (`segment_meta.rs`)

```
Sidecar File (wal-NNNNNN.meta, 60 bytes):
  magic: "STAM" (4)
  version: u32 (4)
  segment_number: u64 (8)
  min_timestamp: u64 (8)
  max_timestamp: u64 (8)
  min_txn_id: u64 (8)
  max_txn_id: u64 (8)
  record_count: u64 (8)
  crc32: u32 (4)
```

Optional sidecar files written when segments are closed/rotated. Enable O(1) coverage checks during watermark-filtered recovery and compaction. Graceful fallback to full segment scan if missing or corrupted.

### 2.6 Snapshot Watermark (`watermark.rs`)

```
Watermark (25 bytes when present):
  has_data: u8 (1)
  snapshot_id: u64 (8)
  watermark_txn: u64 (8)
  updated_at: u64 (8)
```

Semantics:
- All transactions with `txn_id <= watermark_txn` are materialized in the snapshot
- WAL entries with `txn_id > watermark_txn` must be replayed during recovery
- WAL entries with `txn_id <= watermark_txn` can be removed by compaction

---

## 3. Recovery

Recovery orchestration lives in `strata_concurrency::RecoveryCoordinator`
(`crates/concurrency/src/recovery.rs`), not in this crate. It consumes the
durability building blocks described here: `WalReader` for segment iteration,
`ManifestManager` for watermarks, `SnapshotReader` for restoring state.

A richer MANIFEST-first recovery planner was prototyped under
`crates/durability/src/recovery/` but was never adopted by the live engine
path. That prototype has been removed; the concepts it explored (explicit
recovery plan, snapshot discovery) can be ported into the concurrency
crate's coordinator as a future improvement.

---

## 4. Disk Snapshots (`disk_snapshot/`)

### 4.1 SnapshotWriter (`writer.rs`)

Crash-safe snapshot creation:

```
1. Write to temporary file (.snap-NNNNNN.tmp)
2. fsync temporary file
3. Atomic rename → snap-NNNNNN.chk
4. fsync parent directory (with single retry)
```

Cleans up stale temp files from previous crashed attempts.

### 4.2 SnapshotReader (`reader.rs`)

Loads and validates snapshots:
- Magic bytes check
- Codec ID match verification
- CRC32 verification
- Section parsing with primitive type validation

### 4.3 CheckpointCoordinator (`checkpoint.rs`)

Coordinates snapshot creation with watermark tracking:

```
checkpoint(data, watermark_txn) →
  1. Determine next snapshot_id (current + 1, or 1)
  2. Serialize all primitive sections via SnapshotSerializer
  3. Write crash-safe snapshot via SnapshotWriter
  4. Update watermark on success
  5. Return SnapshotInfo
```

`CheckpointData` uses builder pattern with optional fields for each primitive type (KV, events, states, branches, JSON, vectors).

---

## 5. Compaction (`compaction/`)

### 5.1 WalOnlyCompactor (`wal_only.rs`, 780 lines)

Safest compaction mode — removes WAL segments fully covered by a snapshot.

**Algorithm:**

```
1. Get snapshot_watermark + active_segment from MANIFEST
2. safe_active = max(manifest_active, writer_active_segment)
3. For each segment < safe_active:
   a. Check if segment covered by watermark:
      - O(1) via .meta sidecar: meta.max_txn_id <= watermark
      - Fallback: full scan to find max_txn_id
   b. If covered → delete segment + .meta sidecar
   c. Track reclaimed bytes
4. Return CompactInfo
```

**Invariant:** Never removes the active segment. Only removes segments where every record is materialized in the snapshot.

### 5.2 TombstoneIndex (`tombstone.rs`, 815 lines)

Audit trail for deletions. HashMap-indexed by `(branch_id, primitive_type, key)`.

```rust
struct Tombstone {
    branch_id: [u8; 16],
    primitive_type: u8,
    key: Vec<u8>,
    version: u64,
    created_at: u64,
    reason: TombstoneReason,  // UserDelete | RetentionPolicy | Compaction
}
```

Supports time-based cleanup via `cleanup_before(timestamp)`. Serializable for inclusion in snapshots. Stored in `_system/` namespace.

---

## 6. Database Lifecycle

This crate does not own a top-level database handle. The engine
(`crates/engine/src/database/open.rs`) wires `WalWriter`, `ManifestManager`,
`CheckpointCoordinator`, and the storage engine together directly and owns
the lifecycle (create/open/recover/close).

An alternate `DatabaseHandle` facade was prototyped under
`crates/durability/src/database/` with its own directory layout
(`WAL/`, `SNAPSHOTS/`, `DATA/`) but was never adopted by the live engine
path. That prototype has been removed; the engine uses lowercase
`wal/`, `segments/`, `snapshots/`.

---

## 7. Codec (`codec/`)

Object-safe `StorageCodec` trait providing an encryption/compression seam.

```rust
trait StorageCodec: Send + Sync {
    fn encode(&self, data: &[u8]) -> Vec<u8>;
    fn encode_cow<'a>(&self, data: &'a [u8]) -> Cow<'a, [u8]>;  // zero-copy for identity
    fn decode(&self, data: &[u8]) -> Result<Vec<u8>, CodecError>;
    fn codec_id(&self) -> &str;
    fn clone_box(&self) -> Box<dyn StorageCodec>;
}
```

**Implementations:**

| Codec | ID | Overhead | Key source |
|-------|----|----------|------------|
| `IdentityCodec` | "identity" | Zero (Cow::Borrowed) | None |
| `AesGcmCodec` | "aes-gcm-256" | +28 bytes/record (12 nonce + 16 tag) | `STRATA_ENCRYPTION_KEY` env var |

Factory: `get_codec(codec_id)` instantiates by ID string. The codec ID is stored in MANIFEST and validated on database reopen to prevent mismatched decoding.

---

## 8. Branch Bundle (`branch_bundle/`)

Portable archive format for exporting/importing branches.

### 10.1 Archive Structure

```
archive.branchbundle.tar.zst
└── branchbundle/
    ├── MANIFEST.json     Format version, checksums (xxh3)
    ├── BRANCH.json       Branch metadata (id, name, state, parent)
    └── WAL.branchlog     Binary log (msgpack v2, per-entry CRC32)
```

### 10.2 WAL.branchlog Format (v2)

```
Header (16 bytes):
  magic: "STRATA_WAL" (10)
  version: u16 LE (2) = 2
  entry_count: u32 LE (4)

Entries:
  length: u32 LE (4)
  data: msgpack(BranchlogPayload) [variable]
  crc32: u32 LE (4)
```

`BranchlogPayload` contains `branch_id`, `version`, `puts: Vec<(Key, Value)>`, `deletes: Vec<Key>`.

### 10.3 Export/Import

- `BranchBundleWriter::write()` — atomic temp/rename, zstd compression (configurable level 1-22, default 3)
- `BranchBundleReader::read_all()` — decompress, validate checksums, parse all components
- Only terminal (closed/errored) branches can be exported
- Import replays WAL entries into target database

---

## Data Flow: Write → Recover

### Normal Write Path

```
Client → TransactionContext.put()
       → TransactionManager.commit()
           → Validate read-set
           → Allocate version
           → TransactionPayload.to_bytes()
           → WalRecord.new(txn_id, branch_id, timestamp, payload)
           → WalWriter.append(record)
               → encode through codec
               → write to segment file
               → maybe_sync() per durability mode
           → apply_writes() to storage
       → Return commit_version
```

### Checkpoint Path

```
Engine checkpoint flow (crates/engine/src/database/compaction.rs) →
  1. CheckpointCoordinator.checkpoint()
     a. Serialize all primitive sections
     b. SnapshotWriter.create_snapshot()
        → write temp file → fsync → rename
     c. Update watermark
  2. ManifestManager.set_snapshot_watermark()
     → write temp → fsync → rename
```

### Recovery Path

Recovery is orchestrated by `strata_concurrency::RecoveryCoordinator`,
not by the durability crate. From the engine open path:

```
strata_concurrency::RecoveryCoordinator::new(wal_dir).recover() →
  1. Load MANIFEST → get snapshot_id, watermark, active_segment
  2. Load snapshot (if exists):
     a. SnapshotReader.load() → validate CRC, decode sections
     b. Restore primitive state from sections
  3. WalReader.read_all_after_watermark(wal_dir, watermark):
     a. Skip closed segments with meta.max_txn_id <= watermark
     b. For each record with txn_id > watermark:
        → Apply puts/deletes to storage
  4. Truncate partial records at WAL tail
  5. Rebuild missing .meta sidecars
  6. Return (manifest, recovery_stats)
```

---

## Concurrency & Safety Model

| Component | Synchronization | Pattern |
|-----------|----------------|---------|
| WalWriter | Single-owner (`&mut self`) | External lock (TransactionManager branch lock or Arc<Mutex>) |
| ManifestManager | `Arc<Mutex<ManifestManager>>` | Write-fsync-rename for atomic persistence |
| SnapshotWriter | Single-owner | Write-fsync-rename (temp → final) |
| WalReader | Stateless | No synchronization needed |

**Crash safety invariants:**
- WAL records are atomic (length-prefixed + CRC32)
- Snapshots use temp-file + atomic-rename
- MANIFEST uses temp-file + atomic-rename
- Partial records at segment tail are silently discarded on recovery
- Watermark ensures WAL replay starts from correct point

