# strata-durability

The durability crate handles persistence and recovery for Strata. It provides write-ahead logging (WAL), snapshots, and portable run export/import (RunBundle).

## Overview

This crate ensures data survives crashes and can be recovered. Key responsibilities:

1. **WAL (Write-Ahead Log)**: Records all changes before they're applied, enabling crash recovery
2. **Snapshots**: Periodic full-state captures for faster recovery
3. **RunBundle**: Export/import runs as portable archive files

## File Guide

### Core WAL Files

#### `wal.rs` - WAL Entry Types and File Operations

This is the main WAL module. It defines:

**WALEntry enum** - All possible log entries:
- `BeginTxn` - Marks the start of a transaction
- `Write` - Records a key-value write with version
- `Delete` - Records a key deletion with version
- `CommitTxn` - Marks successful transaction completion
- `AbortTxn` - Marks transaction rollback
- `Checkpoint` - Marks snapshot boundary for WAL truncation
- JSON operations: `JsonCreate`, `JsonSet`, `JsonDelete`, `JsonDestroy`
- Vector operations: `VectorCollectionCreate`, `VectorCollectionDelete`, `VectorUpsert`, `VectorDelete`

**DurabilityMode enum** - Controls when data syncs to disk:
- `None` - No persistence (fastest, for testing)
- `Batched` - Periodic fsync every N writes or T milliseconds (default: 100ms/1000 writes)
- `Strict` - Immediate fsync after every commit (slowest, safest)

**WAL struct** - File operations:
- `open()` - Open or create WAL file
- `append()` - Write entry to log
- `read_entries()` / `read_all()` - Read entries back
- `read_entries_detailed()` - Read with corruption detection info
- `truncate_to()` - Remove entries after offset (post-snapshot)
- `find_last_checkpoint()` - Locate last checkpoint for safe truncation
- `flush()` / `fsync()` - Ensure data reaches disk

---

#### `encoding.rs` - Entry Encoding with CRC32

Handles binary encoding/decoding of WAL entries with corruption detection.

**Entry format**:
```
[length: 4 bytes][type: 1 byte][payload: N bytes][crc32: 4 bytes]
```

- Length enables reading variable-sized entries
- Type tag enables forward compatibility (skip unknown types)
- CRC32 detects corruption (bit flips, partial writes)
- Payload uses bincode serialization (fast, compact)

**Functions**:
- `encode_entry()` - Serialize WALEntry to bytes with CRC
- `decode_entry()` - Deserialize bytes to WALEntry, validating CRC

**Type tag constants** (for each entry type):
- Core: 1-6 (BeginTxn, Write, Delete, CommitTxn, AbortTxn, Checkpoint)
- JSON: 0x20-0x23 (Create, Set, Delete, Destroy)
- Vector: 0x70-0x73 (CollectionCreate, CollectionDelete, Upsert, Delete)

---

#### `recovery.rs` - WAL Replay for Recovery

Replays WAL entries to restore database state after a crash.

**Replay process**:
1. Read all WAL entries
2. Group entries by transaction ID
3. Apply only committed transactions (those with CommitTxn)
4. Discard incomplete transactions (crashed mid-transaction)
5. Preserve exact version numbers from WAL

**Key types**:
- `ReplayStats` - Statistics about replay (txns applied, writes/deletes, max version)
- `ReplayOptions` - Filtering options (filter by run_id, stop at version, progress callback)
- `ReplayProgress` - Progress info for callbacks
- `ValidationResult` / `ValidationWarning` - Pre-replay validation results

**Functions**:
- `replay_wal()` - Main recovery function
- `replay_wal_with_options()` - Recovery with filtering/progress
- `validate_transactions()` - Pre-validate WAL entries

---

### Snapshot Files

#### `snapshot_types.rs` - Snapshot Format Types

Defines the binary format for snapshot files.

**Snapshot file layout**:
```
+------------------+
| Magic (10 bytes) |  "INMEM_SNAP"
+------------------+
| Version (4)      |  Format version (1)
+------------------+
| Timestamp (8)    |  Microseconds since epoch
+------------------+
| WAL Offset (8)   |  WAL position covered
+------------------+
| Tx Count (8)     |  Transactions included
+------------------+
| Primitive Count  |  Number of sections (1 byte)
+------------------+
| Primitive 1      |  Type (1) + Length (8) + Data
+------------------+
| ...              |
+------------------+
| CRC32 (4)        |  Checksum of everything above
+------------------+
```

**Key types**:
- `SnapshotEnvelope` - Complete parsed snapshot
- `SnapshotHeader` - Header metadata
- `PrimitiveSection` - Data for one primitive type
- `SnapshotError` - Error types for snapshot operations

**primitive_ids module** - Type IDs for each primitive:
- KV=1, JSON=2, EVENT=3, STATE=4, RUN=6, VECTOR=7

---

#### `snapshot.rs` - Snapshot Writer and Reader

Writes and reads snapshot files with CRC32 integrity checking.

**SnapshotWriter**:
- `write()` - Write snapshot to file with CRC32
- `write_atomic()` - Atomic write (temp file + rename)

**SnapshotReader**:
- `validate_checksum()` - Verify snapshot integrity
- `read_header()` - Read just the header
- `read_envelope()` - Read complete snapshot

**SnapshotSerializable trait**:
- Primitives implement this to serialize/deserialize for snapshots

**Helper functions**:
- `serialize_all_primitives()` - Collect all primitives into sections
- `deserialize_primitives()` - Restore primitives from sections

---

### RunBundle Files (Portable Archives)

RunBundle enables exporting a completed run as a portable `.runbundle.tar.zst` archive that can be imported into another database instance.

#### `run_bundle/mod.rs` - Module Exports

Re-exports all public types from the submodules.

---

#### `run_bundle/types.rs` - RunBundle Types

Defines the archive format structures.

**Archive structure**:
```
<run_id>.runbundle.tar.zst
└── runbundle/
    ├── MANIFEST.json    # Format version, checksums
    ├── RUN.json         # Run metadata (id, state, tags)
    └── WAL.runlog       # Run-scoped WAL entries
```

**Key types**:
- `BundleManifest` - Format metadata and file checksums
- `BundleContents` - Summary (entry count, WAL size)
- `BundleRunInfo` - Run metadata (id, name, state, timestamps, tags)
- `RunExportInfo` - Info returned after export
- `BundleVerifyInfo` - Info returned after validation
- `ImportedRunInfo` - Info returned after import
- `ExportOptions` - Compression level (default: 3)

**Constants**:
- `RUNBUNDLE_FORMAT_VERSION` = 1
- `RUNBUNDLE_EXTENSION` = ".runbundle.tar.zst"

---

#### `run_bundle/error.rs` - Error Types

Error enum for RunBundle operations:
- `RunNotFound` - Run doesn't exist
- `NotTerminal` - Can only export completed/failed/cancelled runs
- `InvalidBundle` - Malformed archive
- `MissingFile` - Required file missing from archive
- `ChecksumMismatch` - Integrity check failed
- `RunAlreadyExists` - Import conflict
- `UnsupportedVersion` - Unknown format version
- `Archive` / `Compression` / `Io` / `Json` - Operation errors

---

#### `run_bundle/writer.rs` - Archive Writer

Creates `.runbundle.tar.zst` archives.

**RunBundleWriter**:
- `write()` - Write archive to file (atomic: temp + rename)
- `write_to_vec()` - Write to memory (for testing)

The writer:
1. Serializes run metadata as pretty JSON
2. Writes WAL entries in .runlog format
3. Computes xxh3 checksums for each file
4. Creates zstd-compressed tar archive
5. Atomic rename for crash safety

---

#### `run_bundle/reader.rs` - Archive Reader

Reads and validates `.runbundle.tar.zst` archives.

**RunBundleReader**:
- `validate()` - Check archive integrity without full parse
- `read_manifest()` - Read just the manifest
- `read_run_info()` - Read just the run metadata
- `read_wal_entries()` - Read WAL entries
- `read_wal_entries_validated()` - Read with checksum verification
- `read_all()` - Read complete bundle contents

---

#### `run_bundle/wal_log.rs` - WAL.runlog Format

Binary format for WAL entries within a RunBundle.

**Format**:
```
┌─────────────────────────────────────────────────────────────────┐
│ Header (16 bytes)                                               │
├─────────────────────────────────────────────────────────────────┤
│ Magic: "STRATA_WAL" (10 bytes)                                  │
│ Version: u16 (2 bytes)                                          │
│ Entry Count: u32 (4 bytes)                                      │
├─────────────────────────────────────────────────────────────────┤
│ For each entry:                                                 │
│   Length: u32 (4 bytes)                                         │
│   Data: [u8; length] (bincode-serialized WALEntry)              │
│   CRC32: u32 (4 bytes)                                          │
└─────────────────────────────────────────────────────────────────┘
```

**WalLogWriter**:
- `write()` - Write entries with CRC32 per entry
- `write_to_vec()` - Write to memory

**WalLogReader**:
- `read()` - Read all entries, validating each CRC
- `validate()` - Validate without full deserialization
- `read_header()` - Read just version and count

**WalLogIterator** - Streaming reader for large files

**Helper**:
- `filter_wal_for_run()` - Filter entries for a specific run_id

---

### lib.rs - Crate Entry Point

Re-exports all public types from submodules:

From `encoding`:
- `decode_entry`, `encode_entry`

From `recovery`:
- `replay_wal`, `replay_wal_with_options`, `validate_transactions`
- `ReplayOptions`, `ReplayProgress`, `ReplayStats`, `ValidationResult`, `ValidationWarning`

From `snapshot`:
- `SnapshotWriter`, `SnapshotReader`, `SnapshotSerializable`
- `serialize_all_primitives`, `deserialize_primitives`

From `snapshot_types`:
- `SnapshotEnvelope`, `SnapshotHeader`, `SnapshotInfo`, `SnapshotError`
- `PrimitiveSection`, `primitive_ids`, `now_micros`
- Constants: `SNAPSHOT_MAGIC`, `SNAPSHOT_VERSION_1`, `SNAPSHOT_HEADER_SIZE`

From `wal`:
- `WAL`, `WALEntry`, `DurabilityMode`
- `WalReadResult`, `WalCorruptionInfo`

From `run_bundle`:
- `RunBundleWriter`, `RunBundleReader`
- `BundleManifest`, `BundleRunInfo`, `BundleContents`
- `RunExportInfo`, `ImportedRunInfo`, `BundleVerifyInfo`
- `ExportOptions`, `RunBundleError`, `RunBundleResult`
- `WalLogWriter`, `WalLogReader`, `WalLogIterator`, `WalLogInfo`
- `filter_wal_for_run`
- Constants: `RUNBUNDLE_EXTENSION`, `RUNBUNDLE_FORMAT_VERSION`

## Usage Examples

### Basic WAL Operations

```rust
use strata_durability::{WAL, WALEntry, DurabilityMode};

// Open WAL with batched durability (recommended for production)
let mut wal = WAL::open("data/wal/segment.wal", DurabilityMode::default())?;

// Append entries
wal.append(&WALEntry::BeginTxn { txn_id: 1, run_id, timestamp })?;
wal.append(&WALEntry::Write { run_id, key, value, version: 1 })?;
wal.append(&WALEntry::CommitTxn { txn_id: 1, run_id })?;

// Read back
let entries = wal.read_all()?;
```

### Recovery

```rust
use strata_durability::{replay_wal, ReplayOptions};

let wal = WAL::open("data/wal/segment.wal", DurabilityMode::default())?;
let stats = replay_wal(&wal, &storage)?;
println!("Recovered {} transactions", stats.txns_applied);
```

### Export/Import Runs

```rust
use strata_durability::run_bundle::{RunBundleWriter, RunBundleReader, ExportOptions};

// Export
let writer = RunBundleWriter::new(&ExportOptions::default());
writer.write(&run_info, &wal_entries, Path::new("./my-run.runbundle.tar.zst"))?;

// Import
let contents = RunBundleReader::read_all(Path::new("./my-run.runbundle.tar.zst"))?;
```

## Design Principles

1. **WAL-first**: All changes go through WAL before memory
2. **CRC32 everywhere**: Every entry has checksums for corruption detection
3. **Atomic writes**: Snapshots and bundles use temp file + rename
4. **Run isolation**: All entries include run_id for filtering
5. **Forward compatibility**: Type tags allow skipping unknown entry types
