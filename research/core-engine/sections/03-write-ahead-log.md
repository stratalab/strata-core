# Write-Ahead Log (WAL): Focused Analysis

This note is derived from the code, not from the existing docs.
Scope for this pass:

- `crates/durability/src/wal/writer.rs`
- `crates/durability/src/wal/reader.rs`
- `crates/durability/src/wal/config.rs`
- `crates/durability/src/wal/mode.rs`
- `crates/durability/src/format/wal_record.rs`
- `crates/durability/src/format/segment_meta.rs`
- `crates/concurrency/src/recovery.rs`
- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/mod.rs`

## Executive view

The WAL subsystem is more substantial than the inventory table suggests.
It is not just a file append wrapper.

It has:

- segmented files with reopen/rotation logic
- a versioned segment header
- a versioned record format with torn-write protection
- per-segment metadata sidecars
- strict and lossy recovery modes
- a background sync path for Standard durability
- post-crash tail truncation before writer reopen

The happy-path architecture is coherent:

- commit serializes one `WalRecord`
- the writer appends to the active segment
- durability mode decides when `sync_all()` happens
- rotation closes the old segment and emits a `.meta` sidecar
- recovery replays records in segment order

The main WAL weaknesses are not in record framing.
They are in policy and layering:

- Standard-mode semantics in the code no longer match several exposed knobs and comments
- background sync state is marked clean before the out-of-lock sync actually succeeds
- the background flush thread is implemented twice with slightly different ordering
- non-identity codecs are still blocked because the writer encodes WAL bytes but the reader does not decode them

## Inventory validation

| # | Feature | Verdict | Code-derived note |
|:-:|---------|---------|-------------------|
| 26 | Segmented WAL | True | `WalWriter` appends to numbered `wal-NNNNNN.seg` files and rotates when `segment.size() + record_len > config.segment_size`. Default segment size is 64 MiB (`crates/durability/src/wal/writer.rs`, `crates/durability/src/wal/config.rs`). |
| 27 | Three Durability Modes | Mostly true | `Cache`, `Always`, and `Standard` are real modes. `Cache` bypasses WAL; `Always` syncs every append; `Standard` uses periodic background sync plus an inline interval fallback. But `Standard` no longer honors the documented batch trigger (`crates/durability/src/wal/mode.rs`, `crates/durability/src/wal/writer.rs`). |
| 28 | Background Fsync | Partially true | There is an out-of-lock background sync path for Standard mode, but it uses `sync_all()` on a cloned file descriptor, not `fdatasync` specifically (`crates/engine/src/database/open.rs`, `crates/durability/src/wal/writer.rs`). |
| 29 | CRC32 Integrity | True, with nuance | WAL v2 has a segment-header CRC, a per-record length CRC, and a main record CRC. `.meta` sidecars also have CRC32 (`crates/durability/src/format/wal_record.rs`, `crates/durability/src/format/segment_meta.rs`). |
| 30 | WAL Record Format v2 | True | v2 records are `length + format_version + length_crc + txn_id + branch_id + timestamp + writeset + main_crc`. Segment headers are 36 bytes in v2 (`crates/durability/src/format/wal_record.rs`). |
| 31 | WAL Counters | True | `WalWriter` tracks cumulative appends, sync calls, bytes written, and sync nanoseconds via `WalCounters` (`crates/durability/src/wal/writer.rs`). |
| 32 | Segment Metadata Sidecars | True | `SegmentMeta` stores min/max timestamp, min/max txn ID, and record count. Readers use it for O(1) watermark skipping and timestamp-range filtering when available (`crates/durability/src/format/segment_meta.rs`, `crates/durability/src/wal/reader.rs`). |
| 33 | Directory Fsync | True | New segment creation fsyncs the parent directory after the header write so the directory entry itself survives power loss (`crates/durability/src/format/wal_record.rs`). |

## Real execution model

### 1. Writer startup

`WalWriter::new()` behaves differently depending on durability mode.

`Cache` mode:

- creates no files
- keeps `segment = None`
- all append operations become no-ops

Persistent modes:

- ensure the WAL directory exists
- find the latest segment number
- try to reopen the last segment for append
- if reopen fails, create a new segment
- rebuild in-memory segment metadata by scanning the reopened segment

That last point matters:

- the `.meta` sidecar is not the source of truth for the active segment
- the writer can reconstruct active-segment metadata from WAL contents

Code evidence:

- `crates/durability/src/wal/writer.rs`

### 2. Append path

The main append flow is:

1. build or receive serialized `WalRecord` bytes
2. encode the bytes through the configured `StorageCodec`
3. rotate first if the current segment would exceed `segment_size`
4. write encoded bytes to the active `WalSegment`
5. update in-memory segment metadata
6. update counters and unsynced state
7. let durability mode decide whether to sync now

Two append entrypoints exist:

- `append(&WalRecord)` for the normal path
- `append_pre_serialized(&[u8], txn_id, timestamp)` for callers that already built the record bytes

`append_pre_serialized()` is a real hot-path optimization because it avoids
record serialization and CRC generation under the WAL mutex.

Code evidence:

- `crates/durability/src/wal/writer.rs`
- `crates/durability/src/format/wal_record.rs`

### 3. Record and segment format

The WAL has two layers of format versioning.

Segment header:

- magic `"STRA"`
- format version
- segment number
- database UUID
- v2 header CRC

Record format v2:

- length field
- record format version
- CRC of the length field
- `txn_id`
- `branch_id`
- `timestamp`
- raw writeset bytes
- main CRC over the full payload

Important nuance:

- v1 headers and v1 records are still readable
- the code is explicitly doing backward-compatible parsing

Code evidence:

- `crates/durability/src/format/wal_record.rs`

### 4. Durability semantics

#### Cache

`Cache` mode bypasses WAL persistence entirely.
No files are created and crash recovery has nothing to replay.

#### Always

`Always` mode does:

- write the record
- `segment.sync()`
- reset unsynced counters

That is the strongest durability mode in the current code.

#### Standard

`Standard` mode is not “sync after N writes or M bytes” in the current implementation.
Its real behavior is:

- writes mark the segment as unsynced
- the background flush thread wakes every `interval_ms`
- under the WAL lock, it flushes the `BufWriter` to the OS page cache and clones the FD
- outside the lock, it calls `sync_all()` on the cloned FD
- `maybe_sync()` provides an inline safety net if the background thread has stalled beyond `interval_ms`

The key runtime property is:

- Standard mode is fundamentally interval-driven now

Code evidence:

- `crates/durability/src/wal/mode.rs`
- `crates/durability/src/wal/writer.rs`
- `crates/engine/src/database/open.rs`

### 5. Rotation and sidecars

Rotation is done before the oversized write lands.

`rotate_segment()`:

- closes the current segment
- writes the closed segment's `.meta` sidecar
- creates the next segment with a v2 header
- resets sync counters

`WalSegment::create()`:

- creates the new file
- writes and flushes the header
- fsyncs the parent directory

That is the important crash-safety point:

- segment rotation protects the directory entry, not just the file contents

Code evidence:

- `crates/durability/src/wal/writer.rs`
- `crates/durability/src/format/wal_record.rs`

### 6. Active segment metadata

Active-segment `.meta` files are best-effort and advisory.

They are flushed:

- on explicit `flush()`
- by the Standard-mode background thread
- via `flush_active_meta()`

Closed-segment `.meta` files are more important because readers and WAL
compaction use them to skip full scans.

The reader is conservative:

- missing or corrupt `.meta` never blocks recovery
- it falls back to full segment scans

Code evidence:

- `crates/durability/src/wal/writer.rs`
- `crates/durability/src/format/segment_meta.rs`
- `crates/durability/src/wal/reader.rs`

### 7. Reader and recovery behavior

The WAL reader is stricter than a simple append-log parser.

Default behavior:

- strict mid-segment corruption handling
- checksum mismatch in the middle of a segment is fatal

Lossy mode:

- scans forward byte-by-byte up to an 8 MiB window
- resumes from the next valid record if found

Recovery also truncates a partial tail on the last segment before the writer reopens it.
That avoids a subtle failure mode where a writer would append after garbage tail bytes
and make later valid records unreachable on the next recovery.

Code evidence:

- `crates/durability/src/wal/reader.rs`
- `crates/concurrency/src/recovery.rs`

## Important unlisted strengths

### Partial-tail truncation before reopen

`RecoveryCoordinator` truncates partial garbage at the tail of the last WAL
segment before normal reopen. That is a real crash-recovery hardening measure,
not just a test artifact.

Code evidence:

- `crates/concurrency/src/recovery.rs`

### Strict and lossy corruption modes

The reader supports two clearly different recovery policies:

- strict corruption rejection
- lossy scan-ahead recovery

That gives the engine a clean safety boundary instead of mixing best-effort
corruption skipping into the default path.

Code evidence:

- `crates/durability/src/wal/reader.rs`

### Backward compatibility for both headers and records

The code explicitly keeps both:

- v1/v2 segment-header compatibility
- v1/v2 record compatibility

That is a stronger compatibility story than a lot of small storage engines have.

Code evidence:

- `crates/durability/src/format/wal_record.rs`

### Pre-serialized append path

`append_pre_serialized()` is a meaningful hot-path optimization because it
lets upper layers build WAL bytes outside the WAL lock.

Code evidence:

- `crates/durability/src/wal/writer.rs`

## WAL-specific constraints and gaps

### 1. Writer and reader codec layers do not match

The writer encodes serialized `WalRecord` bytes through `StorageCodec`.
The reader does not decode through `StorageCodec`; it parses raw bytes
directly with `WalRecord::from_bytes()`.

This is why the open path explicitly blocks non-identity codecs for
WAL-backed durability modes.

Impact:

- encrypted WAL durability is not supported
- codec support exists at the writer boundary but not in the replay path

Code evidence:

- `crates/durability/src/wal/writer.rs`
- `crates/durability/src/wal/reader.rs`
- `crates/engine/src/database/open.rs`

### 2. Standard-mode configuration and implementation have drifted apart

Several exposed Standard-mode knobs are no longer part of the actual sync policy.

Examples:

- `DurabilityMode::Standard { batch_size }` exists, but the writer does not use `batch_size`
- `WalConfig.buffered_sync_bytes` exists, but the writer does not use it for sync decisions
- `mode.rs` still describes Standard as “every N commits OR every T milliseconds”
- `wal/config.rs` still describes bytes-between-fsync behavior

The actual code path is interval-driven background sync plus an inline interval fallback.

Impact:

- config and type surface overstate tuning knobs that do not currently affect behavior
- the documented loss window for Standard mode is stale

Code evidence:

- `crates/durability/src/wal/mode.rs`
- `crates/durability/src/wal/config.rs`
- `crates/durability/src/wal/writer.rs`

### 3. Background sync state is marked clean before the out-of-lock sync succeeds

This is an inference from the code and it matters.

`prepare_background_sync()`:

- flushes the `BufWriter`
- clones the file descriptor
- resets sync counters immediately
- returns the FD for out-of-lock `sync_all()`

If the later `sync_all()` call fails, the writer has already cleared:

- `has_unsynced_data`
- `bytes_since_sync`
- `writes_since_sync`
- `last_sync_time`

The error is only logged in the background thread.

Impact:

- a failed Standard-mode background sync can leave data logically marked as synced
- the writer may not retry until a later write or final shutdown flush
- the effective durability window can become larger than intended after sync failure

Code evidence:

- `crates/durability/src/wal/writer.rs`
- `crates/engine/src/database/open.rs`

### 4. The Standard-mode flush thread is implemented twice with different ordering

There are two background flush-thread implementations:

- one in `open.rs`
- one in `mod.rs` for runtime durability switching

They are not identical.

The `open.rs` version:

- snapshots active `.meta`
- performs `sync_all()` outside the lock
- writes `.meta` afterward

The `mod.rs` version:

- calls `flush_active_meta()` under the lock first
- then performs the out-of-lock `sync_all()`

That means Standard-mode behavior depends on how the thread was created.

Impact:

- duplicated lifecycle code can drift further over time
- ordering around sidecar persistence is inconsistent
- reasoning about Standard-mode guarantees is harder than it should be

Code evidence:

- `crates/engine/src/database/open.rs`
- `crates/engine/src/database/mod.rs`

### 5. The implementation is heavier than the “background fdatasync” description

The code does out-of-lock background syncing, but it uses `sync_all()`, not
`fdatasync`.

That means the actual persistence primitive is heavier than the inventory text suggests.
This is not a correctness bug, but it does affect performance expectations.

Code evidence:

- `crates/durability/src/wal/writer.rs`
- `crates/engine/src/database/open.rs`

## Bottom line

The WAL framing and rotation design are in decent shape.
The strongest parts are:

- record integrity checks
- segment/header versioning
- sidecar-assisted skipping
- crash-tail truncation before reopen

The main architectural gaps are higher-level:

- Standard-mode behavior no longer matches several exposed knobs and docs
- background sync failure handling is too optimistic
- Standard-mode thread logic is duplicated
- codec support is incomplete across write and replay

So the right summary is:

- strong WAL format and crash parsing
- weaker durability-policy cohesion
- incomplete codec integration
