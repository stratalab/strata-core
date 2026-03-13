# Durability and Recovery

This document describes how StrataDB persists data and recovers from crashes.

## Write-Ahead Log (WAL)

All data changes are first written to the WAL before being applied to the in-memory store. This ensures that committed transactions can be recovered after a crash.

### WAL Entry Types

| Entry | Type Tag | Description |
|-------|----------|-------------|
| `BeginTxn` | 0x01 | Start of a transaction |
| `Write` | 0x02 | Key-value write with version |
| `Delete` | 0x03 | Key deletion with version |
| `CommitTxn` | 0x04 | Transaction committed |
| `AbortTxn` | 0x05 | Transaction aborted |
| `Checkpoint` | 0x06 | Snapshot boundary marker |
| `JsonCreate` | 0x20 | JSON document creation |
| `JsonSet` | 0x21 | JSON path-level update |
| `JsonDelete` | 0x22 | JSON field deletion |
| `JsonDestroy` | 0x23 | Entire JSON document deletion |
| `VectorCollectionCreate` | 0x70 | Vector collection creation |
| `VectorCollectionDelete` | 0x71 | Vector collection deletion |
| `VectorUpsert` | 0x72 | Vector insert/update |
| `VectorDelete` | 0x73 | Vector deletion |

### Binary Format

Each WAL entry is encoded as:

```
+-------------------+-------------------+-------------------+-------------------+
| Length (4 bytes)  | Type Tag (1 byte) | Payload (N bytes) | CRC32 (4 bytes)  |
+-------------------+-------------------+-------------------+-------------------+
```

- **Length** — total size of the entry (type + payload + CRC32)
- **Type tag** — identifies the entry type, enables forward compatibility
- **Payload** — bincode-serialized entry data
- **CRC32** — checksum of type tag + payload, detects corruption

### Durability Modes

| Mode | Behavior |
|------|----------|
| None | No WAL writes (in-memory only) |
| Batched | WAL writes buffered, fsync every ~100ms or ~1000 writes |
| Strict | Immediate fsync after every commit |

## Snapshots

Snapshots are periodic full-state captures written to disk via the `disk_snapshot/` module (magic: `SNAP`, 64-byte header, crash-safe write-fsync-rename pattern).

### Snapshot Benefits

- **Bounded recovery time** — replay only WAL entries after the snapshot
- **WAL truncation** — entries before the snapshot can be removed
- **Atomic writes** — snapshots use temp file + rename for crash safety

## Recovery Flow

When a database opens and finds existing WAL/snapshot files:

```
1. Load latest snapshot (if any)
   └── Restores all primitives to snapshot state

2. Read WAL entries after snapshot's WAL offset
   └── Parses entries, validates CRC32

3. Group entries by transaction ID
   └── Builds per-transaction operation lists

4. Apply committed transactions only
   ├── Transactions with CommitTxn → apply
   └── Transactions without CommitTxn → discard (crashed mid-txn)

5. Restore version counters
   └── Preserves exact version numbers from WAL
```

### Recovery Properties

- **Deterministic** — same WAL + same snapshot always produces the same state
- **Idempotent** — running recovery again produces identical results
- **Prefix-consistent** — no partial transactions are visible
- **All primitives** — KV, JSON, Event, State, Branch, and Vector are all recovered

## Branch Bundles

Branch bundles package a single branch's data into a portable archive.

### Bundle Format

```
<branch_id>.branchbundle.tar.zst
  branchbundle/
    MANIFEST.json     # Format version (1), xxh3 file checksums
    BRANCH.json          # Branch metadata (id, status, tags, timestamps)
    WAL.branchlog     # Binary WAL entries for this branch
```

### WAL.branchlog Format

```
Header (16 bytes):
  Magic: "STRATA_WAL" (10 bytes)
  Version: u16 (2 bytes)
  Entry Count: u32 (4 bytes)

Per entry:
  Length: u32 (4 bytes)
  Data: bincode-serialized WALEntry (N bytes)
  CRC32: u32 (4 bytes)
```

### Import Process

1. Decompress and untar the archive
2. Validate MANIFEST checksums
3. Parse BRANCH.json for branch metadata
4. Read WAL.branchlog entries
5. Replay entries into the target database (same as crash recovery)

## Design Principles

1. **WAL-first** — all changes go through WAL before memory
2. **CRC32 everywhere** — every entry and snapshot has checksums
3. **Atomic writes** — snapshots and bundles use temp file + rename
4. **Branch isolation** — all WAL entries include branch_id for filtering
5. **Forward compatibility** — type tags allow skipping unknown entry types

