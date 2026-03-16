# Storage Engine Enhancement — Implementation Guide

**Status:** Implementation Reference
**Date:** 2026-03-16
**Scope:** Epics 10–18 — from the plan in `storage-engine-enhancements.md`
**Branch:** `feat/tiered-storage-epic1`

This document specifies the exact code changes for each epic, with file paths, line references, function signatures, and code snippets. It serves as the implementation blueprint — read this before writing code.

---

## Table of Contents

1. [Epic 10: Activate Flush Pipeline](#epic-10-activate-flush-pipeline)
2. [Epic 11: Version Pruning](#epic-11-version-pruning)
3. [Epic 12: Segment Format v2 — Timestamps & TTL](#epic-12-segment-format-v2)
4. [Epic 13: Streaming WAL Recovery](#epic-13-streaming-wal-recovery)
5. [Epic 14: Write Stalling & Backpressure](#epic-14-write-stalling--backpressure)
6. [Epic 15: WAL Truncation After Flush](#epic-15-wal-truncation-after-flush)
7. [Epic 16: Size-Tiered Compaction](#epic-16-size-tiered-compaction)
8. [Epic 17: Block Compression](#epic-17-block-compression)
9. [Epic 18: Robustness & Diagnostics](#epic-18-robustness--diagnostics)
10. [Dependency Graph & Execution Order](#dependency-graph--execution-order)

---

## Epic 10: Activate Flush Pipeline

**Goal:** Wire the SegmentedStore flush pipeline into the engine so memory is bounded.

**This is the most critical epic.** Without it, the storage engine is functionally in-memory-only despite having the full flush/compaction machinery built.

### Current State

- `RecoveryCoordinator::new(wal_dir)` creates a fresh `SegmentedStore::new()` (ephemeral, no segments_dir) — line 76 of `crates/concurrency/src/recovery.rs`
- `open_finish()` at line 502 of `crates/engine/src/database/mod.rs` calls `RecoveryCoordinator::new(wal_dir.clone())` with no segments_dir
- `open_follower_internal()` at line 421 does the same
- `Database::cache()` at line 654 uses `SegmentedStore::new()` — this is correct and stays
- After commit at line 1675, nothing checks for frozen memtables or schedules flushes
- `SegmentedStore::with_dir()`, `flush_oldest_frozen()`, `recover_segments()`, `branches_needing_flush()`, and `should_compact()` all exist but are never called from the engine

### Change 1: Recovery creates SegmentedStore with segments directory

**File:** `crates/concurrency/src/recovery.rs`

Add `segments_dir` and `write_buffer_size` parameters to `RecoveryCoordinator::new()`:

```rust
pub struct RecoveryCoordinator {
    wal_dir: PathBuf,
    #[allow(dead_code)]
    snapshot_path: Option<PathBuf>,
    /// Optional segments directory for disk-backed storage.
    segments_dir: Option<PathBuf>,
    /// Memtable rotation threshold in bytes.
    write_buffer_size: usize,
}

impl RecoveryCoordinator {
    pub fn new(wal_dir: PathBuf) -> Self {
        RecoveryCoordinator {
            wal_dir,
            snapshot_path: None,
            segments_dir: None,
            write_buffer_size: 0,
        }
    }

    /// Configure disk-backed storage with a segments directory and write buffer size.
    pub fn with_segments(mut self, segments_dir: PathBuf, write_buffer_size: usize) -> Self {
        self.segments_dir = Some(segments_dir);
        self.write_buffer_size = write_buffer_size;
        self
    }

    pub fn recover(&self) -> StrataResult<RecoveryResult> {
        // Create storage based on whether segments_dir is configured
        let storage = match &self.segments_dir {
            Some(dir) => SegmentedStore::with_dir(dir.clone(), self.write_buffer_size),
            None => SegmentedStore::new(),
        };
        // ... rest unchanged
    }
}
```

**Impact:** Backward-compatible — existing callers that use `RecoveryCoordinator::new(wal_dir)` continue to get ephemeral storage. Only callers that chain `.with_segments()` get disk-backed storage.

### Change 2: Engine passes segments_dir to recovery

**File:** `crates/engine/src/database/mod.rs`

In `open_finish()` (line ~502), change:

```rust
// Before:
let recovery = RecoveryCoordinator::new(wal_dir.clone());

// After:
let segments_dir = canonical_path.join("segments");
std::fs::create_dir_all(&segments_dir).map_err(StrataError::from)?;
let recovery = RecoveryCoordinator::new(wal_dir.clone())
    .with_segments(segments_dir, cfg.storage.write_buffer_size);
```

In `open_follower_internal()` (line ~421), change similarly:

```rust
// Before:
let recovery = RecoveryCoordinator::new(wal_dir.clone());

// After:
let segments_dir = canonical_path.join("segments");
std::fs::create_dir_all(&segments_dir).map_err(StrataError::from)?;
let recovery = RecoveryCoordinator::new(wal_dir.clone())
    .with_segments(segments_dir, cfg.storage.write_buffer_size);
```

### Change 3: Segment recovery on startup

**File:** `crates/engine/src/database/mod.rs`

After the recovery coordinator returns the `RecoveryResult`, but before constructing the `Database`, call `recover_segments()`:

```rust
// In open_finish(), after recovery completes and before Arc::new(result.storage):
let storage = Arc::new(result.storage);

// Recover any previously flushed segments from disk
if let Err(e) = storage.recover_segments() {
    warn!(target: "strata::db", error = %e, "Segment recovery failed");
    if !cfg.allow_lossy_recovery {
        return Err(StrataError::corruption(format!(
            "Segment recovery failed: {}. Set allow_lossy_recovery=true to force open.", e
        )));
    }
} else {
    // Log segment recovery if segments were found
    if let Ok(info) = storage.recover_segments() {
        // Already recovered above; we should restructure to capture the result
    }
}
```

Better approach — restructure to capture the result:

```rust
let storage = Arc::new(result.storage);

// Recover previously flushed segments from disk
match storage.recover_segments() {
    Ok(seg_info) => {
        if seg_info.segments_loaded > 0 {
            info!(target: "strata::db",
                branches = seg_info.branches_recovered,
                segments = seg_info.segments_loaded,
                errors_skipped = seg_info.errors_skipped,
                "Recovered segments from disk");
        }
    }
    Err(e) => {
        warn!(target: "strata::db", error = %e, "Segment recovery failed");
        if !cfg.allow_lossy_recovery {
            return Err(StrataError::corruption(format!(
                "Segment recovery failed: {}", e
            )));
        }
    }
}
```

### Change 4: Post-commit flush scheduling

**File:** `crates/engine/src/database/mod.rs`

After `commit_internal()` returns successfully, check for branches needing flush and submit background flush tasks. The best place is in `run_single_attempt()` (line ~1666):

```rust
fn run_single_attempt<T>(
    &self,
    txn: &mut TransactionContext,
    result: StrataResult<T>,
    durability: DurabilityMode,
) -> StrataResult<(T, u64)> {
    match result {
        Ok(value) => {
            let commit_version = self.commit_internal(txn, durability)?;

            // Schedule flush for branches with frozen memtables (best-effort)
            self.schedule_flush_if_needed();

            Ok((value, commit_version))
        }
        Err(e) => {
            let _ = txn.mark_aborted(format!("Closure error: {}", e));
            self.coordinator.record_abort(txn.txn_id);
            Err(e)
        }
    }
}
```

Add the helper method:

```rust
/// Schedule background flush tasks for branches with frozen memtables.
///
/// Best-effort: errors are logged, never propagated to the commit caller.
fn schedule_flush_if_needed(&self) {
    let branches = self.storage.branches_needing_flush();
    if branches.is_empty() {
        return;
    }

    for branch_id in branches {
        let storage = Arc::clone(&self.storage);
        let submit_result = self.scheduler.submit(
            crate::background::TaskPriority::High,
            move || {
                match storage.flush_oldest_frozen(&branch_id) {
                    Ok(true) => {
                        tracing::debug!(
                            target: "strata::flush",
                            ?branch_id,
                            "Flushed frozen memtable to segment"
                        );
                        // Post-flush: check if compaction is needed
                        if storage.should_compact(&branch_id, 4) {
                            match storage.compact_branch(&branch_id, 0) {
                                Ok(Some(result)) => {
                                    tracing::debug!(
                                        target: "strata::compact",
                                        ?branch_id,
                                        segments_merged = result.segments_merged,
                                        entries_pruned = result.entries_pruned,
                                        "Compaction complete"
                                    );
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    tracing::warn!(
                                        target: "strata::compact",
                                        ?branch_id,
                                        error = %e,
                                        "Background compaction failed"
                                    );
                                }
                            }
                        }
                    }
                    Ok(false) => {} // Nothing to flush
                    Err(e) => {
                        tracing::warn!(
                            target: "strata::flush",
                            ?branch_id,
                            error = %e,
                            "Background flush failed"
                        );
                    }
                }
            },
        );
        if let Err(e) = submit_result {
            tracing::debug!(
                target: "strata::flush",
                error = %e,
                "Flush task rejected (queue full)"
            );
            break; // Don't spam when queue is full
        }
    }
}
```

### Change 5: Cache databases stay ephemeral

No changes needed — `Database::cache()` already uses `SegmentedStore::new()` (no dir). This is correct.

### Files Touched

| File | Lines Changed |
|------|--------------|
| `crates/concurrency/src/recovery.rs` | ~20 (add fields, builder method, conditional storage creation) |
| `crates/engine/src/database/mod.rs` | ~60 (segments_dir setup, segment recovery, flush scheduling) |

### Verification

```bash
cargo test -p strata-storage -p strata-concurrency -p strata-engine
```

New tests to add:
1. **Disk database creates segments:** Open disk database, write 1000 keys exceeding write_buffer_size, verify `<data_dir>/segments/` has `.sst` files
2. **Frozen memtables are flushed:** Write beyond write_buffer_size, wait for scheduler drain, verify frozen count = 0 and segment count > 0
3. **Existing flush_pipeline_tests pass:** All existing tests in `crates/storage/src/segmented.rs` continue to pass

---

## Epic 11: Version Pruning

**Goal:** Enforce `max_versions_per_key` so old versions don't accumulate unboundedly.

### Current State

- `CompactionIterator` (line 25 of `crates/storage/src/compaction.rs`) has a `prune_floor` for floor-based pruning but no `max_versions` enforcement
- `max_versions_per_key` is stored as `AtomicUsize` on `SegmentedStore` (line 99 of `crates/storage/src/segmented.rs`) and set via `set_max_versions_per_key()`
- `compact_branch()` passes `prune_floor` to `CompactionIterator::new()` but doesn't pass `max_versions`
- `gc_branch()` is a no-op stub (line 464)

### Change 1: Add max_versions to CompactionIterator

**File:** `crates/storage/src/compaction.rs`

```rust
pub struct CompactionIterator<I: Iterator<Item = (InternalKey, MemtableEntry)>> {
    inner: I,
    prune_floor: u64,
    max_versions: usize,  // NEW: 0 = unlimited
    current_prefix: Option<Vec<u8>>,
    emitted_floor_entry: bool,
    versions_emitted: usize,  // NEW: count per key
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> CompactionIterator<I> {
    pub fn new(inner: I, prune_floor: u64) -> Self {
        Self {
            inner,
            prune_floor,
            max_versions: 0,
            current_prefix: None,
            emitted_floor_entry: false,
            versions_emitted: 0,
        }
    }

    /// Set maximum versions to retain per key (0 = unlimited).
    pub fn with_max_versions(mut self, max: usize) -> Self {
        self.max_versions = max;
        self
    }
}
```

Update the `Iterator::next()` implementation:

```rust
fn next(&mut self) -> Option<Self::Item> {
    loop {
        let (ik, entry) = self.inner.next()?;
        let prefix = ik.typed_key_prefix().to_vec();

        // New logical key — reset tracking
        if self.current_prefix.as_ref() != Some(&prefix) {
            self.current_prefix = Some(prefix);
            self.emitted_floor_entry = false;
            self.versions_emitted = 0;
        }

        let commit_id = ik.commit_id();

        if commit_id >= self.prune_floor {
            // Above floor: check max_versions limit
            if self.max_versions > 0 && self.versions_emitted >= self.max_versions {
                continue; // Skip — already emitted enough above-floor versions
            }
            self.versions_emitted += 1;
            return Some((ik, entry));
        }

        // Below floor
        if !self.emitted_floor_entry {
            self.emitted_floor_entry = true;
            if entry.is_tombstone {
                continue; // Dead key cleanup
            }
            // Keep one floor entry, but only if we haven't hit max_versions
            if self.max_versions > 0 && self.versions_emitted >= self.max_versions {
                continue;
            }
            self.versions_emitted += 1;
            return Some((ik, entry));
        }

        // Already emitted a floor entry for this key — prune
    }
}
```

### Change 2: Pass max_versions to compaction

**File:** `crates/storage/src/segmented.rs`

In `compact_branch()` (line ~855):

```rust
// Before:
let compaction_iter = CompactionIterator::new(merge, prune_floor);

// After:
let max_versions = self.max_versions_per_key.load(Ordering::Relaxed);
let compaction_iter = CompactionIterator::new(merge, prune_floor)
    .with_max_versions(max_versions);
```

### Change 3: Implement gc_branch for ephemeral databases

**File:** `crates/storage/src/segmented.rs`

Replace the no-op `gc_branch()` with version pruning for memtable-only branches:

```rust
/// Garbage collect old versions for a branch.
///
/// For disk-backed stores, pruning happens during compaction.
/// For ephemeral stores (no segments_dir), this prunes excess versions
/// from the active memtable directly.
pub fn gc_branch(&self, branch_id: &BranchId, _min_version: u64) -> usize {
    let max_versions = self.max_versions_per_key.load(Ordering::Relaxed);
    if max_versions == 0 || self.segments_dir.is_some() {
        return 0; // Disk stores prune via compaction; unlimited means no pruning
    }

    // For ephemeral stores: iterate the active memtable and prune excess versions.
    // This is O(entries) but only runs when explicitly called.
    let branch = match self.branches.get(branch_id) {
        Some(b) => b,
        None => return 0,
    };

    let mut pruned = 0usize;
    let mut last_prefix: Option<Vec<u8>> = None;
    let mut version_count = 0usize;
    let mut to_remove: Vec<InternalKey> = Vec::new();

    for (ik, _entry) in branch.active.iter_all() {
        let prefix = ik.typed_key_prefix().to_vec();
        if last_prefix.as_ref() != Some(&prefix) {
            last_prefix = Some(prefix);
            version_count = 0;
        }
        version_count += 1;
        if version_count > max_versions {
            to_remove.push(ik);
        }
    }

    // Remove excess versions
    for ik in &to_remove {
        branch.active.remove(ik);
        pruned += 1;
    }

    pruned
}
```

**Note:** This requires `Memtable::remove(ik)` — check if it exists. The `crossbeam_skiplist::SkipMap` supports `remove()`. If `Memtable` doesn't expose it, we may need to add a `remove_internal_key` method, or defer this to compaction-based pruning only and leave gc_branch as a no-op for now.

**Fallback:** If `Memtable::remove()` is too invasive, leave `gc_branch()` as a no-op and rely entirely on compaction-based pruning (which works for disk-backed stores). For ephemeral stores, the `max_versions_per_key` limit at read time is already applied via MVCC dedup.

### Files Touched

| File | Lines Changed |
|------|--------------|
| `crates/storage/src/compaction.rs` | ~30 (add max_versions field, update Iterator impl) |
| `crates/storage/src/segmented.rs` | ~10 (pass max_versions to CompactionIterator) |

### Verification

New tests:
1. **max_versions enforcement:** Write 10 versions of a key, compact with `max_versions=3`, verify only 3 + 1 floor entry survive
2. **max_versions=0 keeps all:** Write 10 versions, compact with `max_versions=0`, verify all 10 survive
3. **max_versions interacts with prune_floor:** Write versions at [1..10], floor=5, max_versions=2 → keep v10, v9 (above floor, max_versions=2), plus v4 (floor entry)

---

## Epic 12: Segment Format v2 — Timestamps & TTL

**Goal:** Preserve timestamps and TTL across memtable flush so point-in-time queries and TTL expiration work correctly for on-disk data.

### Current State

- `encode_entry()` (line 259 of `crates/storage/src/segment_builder.rs`) writes: `ik_len | ik_bytes | value_kind | value_len | value_bytes`
- No timestamp or TTL fields in the on-disk format
- `SegmentEntry` (line 32 of `crates/storage/src/segment.rs`) has only `value`, `is_tombstone`, `commit_id`
- `segment_entry_to_memtable_entry()` (line 1375 of `crates/storage/src/segmented.rs`) hardcodes `timestamp: Timestamp::from_micros(0), ttl_ms: 0`
- `FORMAT_VERSION = 1` (line 36 of `segment_builder.rs`)
- `parse_header()` rejects anything != FORMAT_VERSION (line 535)

### Change 1: Bump format version and extend entry encoding

**File:** `crates/storage/src/segment_builder.rs`

```rust
// Change:
const FORMAT_VERSION: u16 = 1;
// To:
const FORMAT_VERSION: u16 = 2;
```

Update `encode_entry()`:

```rust
fn encode_entry(ik: &InternalKey, entry: &MemtableEntry, buf: &mut Vec<u8>) {
    let ik_bytes = ik.as_bytes();
    buf.extend_from_slice(&(ik_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(ik_bytes);

    if entry.is_tombstone {
        buf.push(VALUE_KIND_DEL);
    } else {
        buf.push(VALUE_KIND_PUT);
    }

    // v2: timestamp and TTL
    buf.extend_from_slice(&entry.timestamp.as_micros().to_le_bytes());
    buf.extend_from_slice(&entry.ttl_ms.to_le_bytes());

    if entry.is_tombstone {
        buf.extend_from_slice(&0u32.to_le_bytes());
    } else {
        let value_bytes =
            bincode::serialize(&entry.value).expect("Value serialization should not fail");
        buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&value_bytes);
    }
}
```

Update `decode_entry()` to handle both v1 and v2. **Problem:** `decode_entry` doesn't know the format version — it's a standalone function. We need to thread the version through.

**Approach:** Add a `decode_entry_v2()` and have the segment reader choose based on header `format_version`. The simplest approach:

```rust
/// Decode a v2 entry with timestamp and TTL fields.
pub(crate) fn decode_entry_v2(data: &[u8]) -> Option<(InternalKey, bool, Value, u64, u64, usize)> {
    if data.len() < 4 {
        return None;
    }
    let mut pos = 0;

    let ik_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;
    if pos + ik_len > data.len() { return None; }
    let ik = InternalKey::from_bytes(data[pos..pos + ik_len].to_vec());
    pos += ik_len;

    if pos >= data.len() { return None; }
    let value_kind = data[pos];
    pos += 1;

    // v2: read timestamp and TTL
    if pos + 16 > data.len() { return None; }
    let timestamp = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let ttl_ms = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
    pos += 8;

    if pos + 4 > data.len() { return None; }
    let value_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;

    if value_kind == VALUE_KIND_DEL {
        return Some((ik, true, Value::Null, timestamp, ttl_ms, pos));
    }
    if value_kind != VALUE_KIND_PUT { return None; }
    if pos + value_len > data.len() { return None; }
    let value: Value = bincode::deserialize(&data[pos..pos + value_len]).ok()?;
    pos += value_len;

    Some((ik, false, value, timestamp, ttl_ms, pos))
}
```

### Change 2: SegmentEntry gains timestamp and TTL

**File:** `crates/storage/src/segment.rs`

```rust
pub struct SegmentEntry {
    pub value: Value,
    pub is_tombstone: bool,
    pub commit_id: u64,
    pub timestamp: u64,   // NEW: microseconds since epoch (0 for v1 segments)
    pub ttl_ms: u64,      // NEW: 0 = no TTL (0 for v1 segments)
}
```

Update all `SegmentEntry` constructions in `segment.rs` to include the new fields. For v1 segments, use defaults (0, 0).

### Change 3: Update segment_entry_to_memtable_entry

**File:** `crates/storage/src/segmented.rs` (line 1375)

```rust
fn segment_entry_to_memtable_entry(se: SegmentEntry) -> MemtableEntry {
    MemtableEntry {
        value: se.value,
        is_tombstone: se.is_tombstone,
        timestamp: Timestamp::from_micros(se.timestamp),
        ttl_ms: se.ttl_ms,
    }
}
```

### Change 4: v1 backward compatibility in parse_header

**File:** `crates/storage/src/segment_builder.rs` (line 535)

```rust
// Before:
if format_version != FORMAT_VERSION {
    return None;
}

// After:
if format_version != 1 && format_version != 2 {
    return None;
}
```

Store the format_version in `KVHeader` so the segment reader can decide which decoder to use.

### Change 5: Segment reader dispatches by format version

**File:** `crates/storage/src/segment.rs`

The `KVSegment` struct already stores `header: KVHeader` which has `format_version`. The `scan_block_for_key()` and `SegmentIter` need to use `decode_entry_v2()` when `format_version == 2` and `decode_entry()` for v1.

### Files Touched

| File | Lines Changed |
|------|--------------|
| `crates/storage/src/segment_builder.rs` | ~40 (encode_entry, decode_entry_v2, format version) |
| `crates/storage/src/segment.rs` | ~20 (SegmentEntry fields, decoder dispatch) |
| `crates/storage/src/segmented.rs` | ~5 (segment_entry_to_memtable_entry) |

### Verification

1. Existing segment tests pass (they now create v2 segments, but the reader handles both)
2. **v1 compat test:** Build a v1 segment (write raw v1 format), verify it opens and returns timestamp=0, ttl_ms=0
3. **v2 roundtrip:** Flush entries with known timestamps and TTL, reopen, verify values survive
4. **get_at_timestamp correctness:** Flush data, verify `get_at_timestamp` returns correct results for flushed data

---

## Epic 13: Streaming WAL Recovery

**Goal:** Recovery doesn't OOM on large databases.

### Current State

- `WalReader::read_all()` (line ~100 of `crates/durability/src/wal/reader.rs`) reads ALL records into a `Vec<WalRecord>` and returns them
- `RecoveryCoordinator::recover()` iterates `read_result.records` (line 96)
- `read_all_after_watermark()` exists (line ~209 of reader.rs) and filters by watermark, but still collects into a Vec
- `ManifestManager` has `set_flush_watermark()` and `flushed_through_commit_id` field

### Change 1: Streaming WAL iterator

**File:** `crates/durability/src/wal/reader.rs`

Add an `iter_all()` method that returns a lazy iterator:

```rust
/// Lazy iterator over all WAL records across all segments.
///
/// Unlike `read_all()`, this does not collect records into a Vec.
/// Records are yielded one at a time, keeping memory usage O(1) per record.
pub fn iter_all(
    &self,
    wal_dir: &Path,
) -> Result<WalRecordIterator, WalReaderError> {
    let mut segments = Self::list_segment_numbers(wal_dir)?;
    segments.sort();
    Ok(WalRecordIterator {
        wal_dir: wal_dir.to_path_buf(),
        segments,
        current_segment_idx: 0,
        current_records: Vec::new(),
        current_record_idx: 0,
    })
}

/// Iterator that yields `WalRecord`s one at a time across all segments.
pub struct WalRecordIterator {
    wal_dir: PathBuf,
    segments: Vec<u64>,
    current_segment_idx: usize,
    current_records: Vec<WalRecord>,
    current_record_idx: usize,
}

impl Iterator for WalRecordIterator {
    type Item = WalRecord;

    fn next(&mut self) -> Option<WalRecord> {
        loop {
            // Yield from current segment's records
            if self.current_record_idx < self.current_records.len() {
                let record = self.current_records[self.current_record_idx].clone();
                self.current_record_idx += 1;
                return Some(record);
            }

            // Move to next segment
            if self.current_segment_idx >= self.segments.len() {
                return None;
            }

            let seg_num = self.segments[self.current_segment_idx];
            self.current_segment_idx += 1;

            let reader = WalReader::new();
            match reader.read_segment(&self.wal_dir, seg_num) {
                Ok((records, _, _, _)) => {
                    self.current_records = records;
                    self.current_record_idx = 0;
                }
                Err(_) => {
                    // Skip corrupt segments
                    self.current_records.clear();
                    self.current_record_idx = 0;
                }
            }
        }
    }
}
```

**Note:** This still reads each segment fully into memory, but only one segment at a time. For true streaming within a segment, we'd need to refactor `read_segment_from()` to return an iterator too — defer that to Epic 23 (batched replay).

### Change 2: Recovery uses streaming reader

**File:** `crates/concurrency/src/recovery.rs`

```rust
// Before:
let reader = WalReader::new();
let read_result = reader.read_all(&self.wal_dir)
    .map_err(|e| ...)?;

for record in &read_result.records {
    // ... apply
}

// After:
let reader = WalReader::new();
let records_iter = reader.iter_all(&self.wal_dir)
    .map_err(|e| strata_core::StrataError::storage(format!("WAL read failed: {}", e)))?;

for record in records_iter {
    // ... same apply logic
}
```

### Change 3: Watermark-based segment skipping

**File:** `crates/durability/src/wal/reader.rs`

Add `iter_all_after_watermark()`:

```rust
pub fn iter_all_after_watermark(
    &self,
    wal_dir: &Path,
    watermark: u64,
) -> Result<impl Iterator<Item = WalRecord>, WalReaderError> {
    let iter = self.iter_all(wal_dir)?;
    Ok(iter.filter(move |r| r.txn_id > watermark))
}
```

**File:** `crates/engine/src/database/mod.rs`

In `open_finish()`, after segment recovery, read the MANIFEST to get the flush watermark and pass it:

```rust
// After segment recovery, before WAL replay:
// Load MANIFEST flush watermark if available
let manifest_path = canonical_path.join("MANIFEST");
let flush_watermark = if manifest_path.exists() {
    match ManifestManager::load(&manifest_path) {
        Ok(manifest) => manifest.flushed_through_commit_id.unwrap_or(0),
        Err(_) => 0,
    }
} else {
    0
};

// Pass watermark to recovery to skip already-flushed WAL segments
// (deferred: recovery currently replays all, segments handle dedup)
```

**Note:** This optimization is only effective after Epic 15 (WAL truncation). For now, we can add the plumbing but not change the recovery behavior — WAL replay with overlapping data is idempotent (memtable just overwrites).

### Files Touched

| File | Lines Changed |
|------|--------------|
| `crates/durability/src/wal/reader.rs` | ~80 (WalRecordIterator struct, iter_all, iter_all_after_watermark) |
| `crates/concurrency/src/recovery.rs` | ~10 (switch from read_all to iter_all) |

### Verification

1. **Streaming recovery:** Write 10K records across multiple WAL segments, recover with iter_all, verify all records replayed
2. **Watermark skipping:** Write records, flush all, set watermark, verify iter_all_after_watermark only returns delta records

---

## Epic 14: Write Stalling & Backpressure

**Goal:** Prevent OOM when writes outpace flush.

### Current State

- `maybe_rotate_branch()` (line 773 of segmented.rs) unconditionally creates frozen memtables
- No limit on the number of frozen memtables per branch
- If writes outpace flush, frozen memtables accumulate without bound
- `max_immutable_memtables` exists in `StorageConfig` (line 78 of config.rs) with default 4

### Change 1: Block writes when frozen count exceeds limit

**File:** `crates/storage/src/segmented.rs`

Add a `Condvar` and `max_immutable_memtables` to `SegmentedStore`:

```rust
pub struct SegmentedStore {
    // ... existing fields ...
    /// Maximum frozen memtables before write stalling. 0 = unlimited.
    max_immutable_memtables: AtomicUsize,
    /// Condvar signaled when a frozen memtable is flushed.
    flush_complete: parking_lot::Condvar,
    /// Mutex for the Condvar (parking_lot Condvar needs a Mutex).
    flush_mutex: parking_lot::Mutex<()>,
}
```

Update `maybe_rotate_branch()`:

```rust
fn maybe_rotate_branch(&self, branch_id: BranchId, branch: &mut BranchState) {
    if self.write_buffer_size > 0
        && branch.active.approx_bytes() >= self.write_buffer_size
        && !self.bulk_load_branches.contains_key(&branch_id)
    {
        // Wait if too many frozen memtables (write stalling)
        let max_frozen = self.max_immutable_memtables.load(Ordering::Relaxed);
        if max_frozen > 0 && branch.frozen.len() >= max_frozen {
            // Drop the DashMap guard before waiting to avoid deadlock
            // NOTE: This is tricky because we're inside a DashMap guard.
            // We can't easily wait here. Instead, we'll just skip rotation
            // and let the caller retry. The write will succeed but the
            // memtable won't rotate until a flush completes.
            tracing::warn!(
                target: "strata::storage",
                ?branch_id,
                frozen_count = branch.frozen.len(),
                max = max_frozen,
                "Write stall: too many frozen memtables, skipping rotation"
            );
            return;
        }

        let next_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let old = std::mem::replace(&mut branch.active, Memtable::new(next_id));
        old.freeze();
        branch.frozen.insert(0, Arc::new(old));
    }
}
```

Update `flush_oldest_frozen()` to signal the condvar:

```rust
// At the end of flush_oldest_frozen(), after removing the frozen memtable:
// Signal any stalled writers
// (No-op if using the "skip rotation" approach above)
```

**Design note:** Blocking inside a DashMap guard is dangerous (deadlock). The safer approach is:
1. Skip rotation when `frozen.len() >= max_immutable_memtables`
2. The active memtable grows beyond `write_buffer_size`
3. The next `flush_oldest_frozen()` drains a frozen memtable
4. The next write's `maybe_rotate_branch()` succeeds

This is simple backpressure without complex synchronization.

Add setter:

```rust
pub fn set_max_immutable_memtables(&self, max: usize) {
    self.max_immutable_memtables.store(max, Ordering::Relaxed);
}
```

### Change 2: Wire config

**File:** `crates/engine/src/database/mod.rs`

In `open_finish()`, after `storage.set_max_versions_per_key(...)`:

```rust
storage.set_max_immutable_memtables(cfg.storage.max_immutable_memtables);
```

### Files Touched

| File | Lines Changed |
|------|--------------|
| `crates/storage/src/segmented.rs` | ~30 (new field, updated maybe_rotate_branch, setter) |
| `crates/engine/src/database/mod.rs` | ~5 (wire config) |

### Verification

1. **Write stalling:** Set max_immutable_memtables=2, disable background flush, write rapidly — verify rotation stops after 2 frozen memtables
2. **Recovery after flush:** Manually flush one, verify rotation resumes

---

## Epic 15: WAL Truncation After Flush

**Goal:** Reclaim WAL disk space after data is safely in segments.

### Current State

- `ManifestManager::set_flush_watermark(commit_id)` exists in `crates/durability/src/format/manifest.rs`
- `WalOnlyCompactor` exists in `crates/durability/src/compaction/wal_only.rs` with `compact()` method
- The `effective_watermark()` function in `wal_only.rs` already combines `snapshot_watermark` and `flushed_through_commit_id`
- Nobody calls `set_flush_watermark()` or triggers WAL compaction after flush

### Change 1: Update flush watermark after flush

**File:** `crates/engine/src/database/mod.rs`

In the `schedule_flush_if_needed()` method's flush closure (from Epic 10), after successful flush:

```rust
// After flush succeeds, update the flush watermark
if let Some(max_commit) = storage.max_flushed_commit(&branch_id) {
    // Compute global minimum across all branches
    let min_flushed = storage.branch_ids()
        .iter()
        .filter_map(|bid| storage.max_flushed_commit(bid))
        .min()
        .unwrap_or(0);

    // Update MANIFEST
    // (Need access to data_dir — capture it in the closure)
    // This is best done from the Database level, not inside SegmentedStore
}
```

**Better approach:** Add a `post_flush_callback` to the flush scheduling in `Database`:

```rust
fn schedule_flush_if_needed(&self) {
    let branches = self.storage.branches_needing_flush();
    if branches.is_empty() {
        return;
    }

    for branch_id in branches {
        let storage = Arc::clone(&self.storage);
        let data_dir = self.data_dir.clone();
        let wal_dir = self.wal_dir.clone();

        let submit_result = self.scheduler.submit(
            crate::background::TaskPriority::High,
            move || {
                match storage.flush_oldest_frozen(&branch_id) {
                    Ok(true) => {
                        // Update flush watermark
                        let min_flushed = storage.branch_ids()
                            .iter()
                            .filter_map(|bid| storage.max_flushed_commit(bid))
                            .min()
                            .unwrap_or(0);

                        if min_flushed > 0 {
                            let manifest_path = data_dir.join("MANIFEST");
                            if let Ok(mut mgr) = ManifestManager::load(&manifest_path) {
                                let _ = mgr.set_flush_watermark(min_flushed);
                            }

                            // Truncate WAL segments below watermark
                            if let Ok(compactor) = WalOnlyCompactor::new(&wal_dir, &manifest_path) {
                                match compactor.compact() {
                                    Ok(info) => {
                                        if info.segments_removed > 0 {
                                            tracing::debug!(
                                                target: "strata::wal",
                                                segments_removed = info.segments_removed,
                                                bytes_reclaimed = info.bytes_reclaimed,
                                                "WAL segments truncated"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            target: "strata::wal",
                                            error = %e,
                                            "WAL compaction failed"
                                        );
                                    }
                                }
                            }
                        }

                        // Post-flush: check compaction
                        // ... (same as Epic 10)
                    }
                    // ... error handling same as Epic 10
                }
            },
        );
        // ...
    }
}
```

**Note:** Need to verify the `ManifestManager` and `WalOnlyCompactor` API signatures. The actual API may differ — check `crates/durability/src/format/manifest.rs` and `crates/durability/src/compaction/wal_only.rs`.

### Files Touched

| File | Lines Changed |
|------|--------------|
| `crates/engine/src/database/mod.rs` | ~30 (update flush callback with watermark + WAL truncation) |

### Verification

1. **WAL truncation:** Write 1000 keys, flush all branches, verify WAL segment files are removed
2. **Recovery after truncation:** Close database, reopen, verify all data present from segments
3. **No data loss:** Ensure no WAL records needed for unflushed data are removed

---

## Epic 16: Size-Tiered Compaction

**Goal:** Bound read amplification and segment count at scale.

### Current State

- `compact_branch()` merges ALL segments into one (line 805 of segmented.rs)
- `should_compact()` uses a simple count threshold (line 934)
- `MergeIterator` uses linear scan for min-selection (adequate for 1-3 sources, but O(n) per `.next()` call for many sources)

### Change 1: CompactionScheduler

**File:** `crates/storage/src/compaction.rs`

```rust
/// Tier-based compaction scheduler.
///
/// Groups segments into tiers by size and picks merge candidates.
/// Tier assignment: `tier = floor(log4(file_size / base_size))`.
pub struct CompactionScheduler {
    /// Minimum segments in a tier to trigger a merge.
    pub min_merge_width: usize,
    /// Base segment size for tier calculation (bytes).
    pub base_size: u64,
}

impl Default for CompactionScheduler {
    fn default() -> Self {
        Self {
            min_merge_width: 4,
            base_size: 64 * 1024 * 1024, // 64 MiB
        }
    }
}

/// A group of segments in the same size tier, ready to merge.
pub struct TierMergeCandidate {
    /// Tier number (0 = smallest segments).
    pub tier: u32,
    /// Segment file sizes in this tier.
    pub segment_sizes: Vec<u64>,
    /// Indices into the branch's segment list.
    pub segment_indices: Vec<usize>,
}

impl CompactionScheduler {
    /// Assign a tier to a segment based on its file size.
    pub fn tier_for_size(&self, file_size: u64) -> u32 {
        if file_size == 0 || self.base_size == 0 {
            return 0;
        }
        let ratio = file_size as f64 / self.base_size as f64;
        if ratio <= 1.0 { 0 } else { ratio.log(4.0).floor() as u32 }
    }

    /// Find merge candidates: tiers with `count >= min_merge_width`.
    ///
    /// Returns candidates sorted by tier (smallest first — merge small segments
    /// before large ones).
    pub fn pick_candidates(&self, segment_sizes: &[u64]) -> Vec<TierMergeCandidate> {
        use std::collections::BTreeMap;

        let mut tiers: BTreeMap<u32, Vec<(usize, u64)>> = BTreeMap::new();
        for (i, &size) in segment_sizes.iter().enumerate() {
            let tier = self.tier_for_size(size);
            tiers.entry(tier).or_default().push((i, size));
        }

        tiers.into_iter()
            .filter(|(_, segments)| segments.len() >= self.min_merge_width)
            .map(|(tier, segments)| TierMergeCandidate {
                tier,
                segment_sizes: segments.iter().map(|(_, s)| *s).collect(),
                segment_indices: segments.iter().map(|(i, _)| *i).collect(),
            })
            .collect()
    }
}
```

### Change 2: Tiered compact_branch

**File:** `crates/storage/src/segmented.rs`

Add `compact_tier()` method:

```rust
/// Compact a specific subset of segments (tier-based compaction).
///
/// Unlike `compact_branch()` which merges all segments, this merges only
/// the segments at the given indices. Returns `Ok(None)` if fewer than 2
/// segments are selected.
pub fn compact_tier(
    &self,
    branch_id: &BranchId,
    segment_indices: &[usize],
    prune_floor: u64,
) -> io::Result<Option<CompactionResult>> {
    if segment_indices.len() < 2 {
        return Ok(None);
    }

    let segments_dir = match &self.segments_dir {
        Some(d) => d,
        None => return Ok(None),
    };

    // Snapshot selected segments under read guard
    let (selected_segments, total_input_entries) = {
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return Ok(None),
        };
        let segs: Vec<Arc<KVSegment>> = segment_indices.iter()
            .filter_map(|&i| branch.segments.get(i).map(Arc::clone))
            .collect();
        if segs.len() < 2 {
            return Ok(None);
        }
        let total: u64 = segs.iter().map(|s| s.entry_count()).sum();
        (segs, total)
    };

    // Build and write compacted segment (same as compact_branch)
    // ... (same merge/build/swap logic, but only for selected_segments)
}
```

### Change 3: Update post-flush compaction to use tiered scheduler

In the flush callback (Epic 10/15):

```rust
// Replace:
if storage.should_compact(&branch_id, 4) {
    storage.compact_branch(&branch_id, 0)?;
}

// With:
let scheduler = CompactionScheduler::default();
let sizes: Vec<u64> = /* get segment file sizes */;
let candidates = scheduler.pick_candidates(&sizes);
if let Some(candidate) = candidates.first() {
    storage.compact_tier(&branch_id, &candidate.segment_indices, 0)?;
}
```

### Change 4: MergeIterator heap fallback

**File:** `crates/storage/src/merge_iter.rs`

When source count > 4, use `BinaryHeap` instead of linear scan:

```rust
impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> Iterator for MergeIterator<I> {
    type Item = (InternalKey, MemtableEntry);

    fn next(&mut self) -> Option<Self::Item> {
        if self.sources.len() <= 4 {
            // Linear scan — fast for small source counts
            self.next_linear()
        } else {
            // Heap-based — O(log n) for large source counts
            self.next_linear() // TODO: implement heap variant
        }
    }
}
```

**Note:** The heap variant is a significant change. For the first iteration, keep linear scan — it's only used during compaction, and tiered compaction limits the number of segments merged at once (typically 4-8). Defer true heap-based merge to Epic 20 (streaming iterators).

### Files Touched

| File | Lines Changed |
|------|--------------|
| `crates/storage/src/compaction.rs` | ~100 (CompactionScheduler, TierMergeCandidate) |
| `crates/storage/src/segmented.rs` | ~80 (compact_tier) |
| `crates/storage/src/merge_iter.rs` | ~0 (defer heap variant) |

### Verification

1. **Tiered grouping:** Create 20 segments of varying sizes, verify CompactionScheduler groups them correctly
2. **Tier compaction:** Flush 20 segments, run tiered compaction, verify segment count reduces
3. **Data integrity:** After tiered compaction, verify all keys are readable

---

## Epic 17: Block Compression

**Goal:** Reduce segment file size 2-4x with zstd compression.

### Current State

- Data blocks are written uncompressed (codec byte = 0 in `write_framed_block`, line 328)
- `parse_framed_block()` reads codec byte but ignores it (line 346: "codec = raw[1], reserved = raw[2..4] — ignored for v1")
- `zstd` crate is already a dependency in `crates/durability/Cargo.toml`

### Change 1: Add zstd compression for data blocks

**File:** `crates/storage/src/segment_builder.rs`

Add a compression codec enum:

```rust
/// Block compression codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    /// No compression (codec byte = 0).
    None,
    /// Zstandard compression (codec byte = 1).
    Zstd,
}
```

Add a `compression` field to `SegmentBuilder`:

```rust
pub struct SegmentBuilder {
    pub data_block_size: usize,
    pub bloom_bits_per_key: usize,
    pub compression: CompressionCodec,
}

impl Default for SegmentBuilder {
    fn default() -> Self {
        Self {
            data_block_size: 64 * 1024,
            bloom_bits_per_key: 10,
            compression: CompressionCodec::Zstd,  // Default to compressed
        }
    }
}
```

Update `write_framed_block` to accept a codec:

```rust
fn write_framed_block_with_codec<W: Write>(
    w: &mut W,
    block_type: u8,
    data: &[u8],
    codec: CompressionCodec,
) -> io::Result<()> {
    let (compressed_data, codec_byte) = match codec {
        CompressionCodec::None => (None, 0u8),
        CompressionCodec::Zstd => {
            let compressed = zstd::encode_all(data, 3)?;  // level 3
            // Only use compression if it actually saves space
            if compressed.len() < data.len() {
                (Some(compressed), 1u8)
            } else {
                (None, 0u8)
            }
        }
    };

    let write_data = compressed_data.as_deref().unwrap_or(data);

    w.write_all(&[block_type])?;
    w.write_all(&[codec_byte])?;
    w.write_all(&[0u8; 2])?;  // reserved
    w.write_all(&(write_data.len() as u32).to_le_bytes())?;
    w.write_all(write_data)?;
    let crc = crc32fast::hash(write_data);
    w.write_all(&crc.to_le_bytes())?;
    Ok(())
}
```

In `build_from_iter()`, use compression for data blocks only:

```rust
// Data blocks: compressed
write_framed_block_with_codec(&mut w, BLOCK_TYPE_DATA, &block_buf, self.compression)?;

// Index, bloom, properties blocks: NOT compressed (must be randomly accessible)
write_framed_block(&mut w, BLOCK_TYPE_INDEX, &index_data)?;
write_framed_block(&mut w, BLOCK_TYPE_FILTER, &bloom_data)?;
write_framed_block(&mut w, BLOCK_TYPE_PROPS, &props_data)?;
```

### Change 2: Decompression in segment reader

**File:** `crates/storage/src/segment.rs`

Update `read_data_block()`:

```rust
fn read_data_block(&self, ie: &IndexEntry) -> Option<Vec<u8>> {
    let start = ie.block_offset as usize;
    let framed_len = FRAME_OVERHEAD + ie.block_data_len as usize;
    let end = start + framed_len;
    if end > self.mmap.len() {
        return None;
    }

    let raw = &self.mmap[start..end];
    let codec_byte = raw[1];
    let (_, data) = parse_framed_block(raw)?;

    match codec_byte {
        0 => Some(data.to_vec()),  // Uncompressed
        1 => {
            // Zstd decompression
            zstd::decode_all(data).ok()
        }
        _ => None,  // Unknown codec
    }
}
```

### Dependency

**File:** `crates/storage/Cargo.toml`

Add zstd dependency:

```toml
[dependencies]
zstd = "0.13"
```

### Files Touched

| File | Lines Changed |
|------|--------------|
| `crates/storage/src/segment_builder.rs` | ~30 (CompressionCodec, write_framed_block_with_codec) |
| `crates/storage/src/segment.rs` | ~15 (read_data_block decompression) |
| `crates/storage/Cargo.toml` | ~1 (add zstd) |

### Verification

1. **Compressed roundtrip:** Build segment with Zstd, reopen, verify all reads correct
2. **Mixed segments:** Uncompressed v1 + compressed v2 segments in same branch — both readable
3. **Size reduction:** Compare file sizes with and without compression on typical workload
4. **Incompressible data:** Verify compression falls back to uncompressed when data doesn't compress

---

## Epic 18: Robustness & Diagnostics

**Goal:** Harden error handling and make diagnostics O(1).

### Current State

- `InternalKey::from_bytes()` panics on short input (line 147 of key_encoding.rs: `assert!(bytes.len() >= 28)`)
- `InternalKey::commit_id()` unwraps (line 173: `.try_into().unwrap()`)
- `memory_stats()` calls `list_branch_inner()` which is O(entries) per branch (line 506)
- `branch_entry_count()` also calls `list_branch_inner()` (line 202)
- `time_range()` calls `list_branch()` which scans all entries (line 448)

### Change 1: Safe InternalKey parsing

**File:** `crates/storage/src/key_encoding.rs`

Add a fallible constructor:

```rust
/// Create an InternalKey from raw bytes, returning None if too short.
pub fn try_from_bytes(bytes: Vec<u8>) -> Option<Self> {
    if bytes.len() < 28 {
        return None;
    }
    Some(InternalKey(bytes))
}
```

Update segment reader to use `try_from_bytes()` and skip corrupt entries:

**File:** `crates/storage/src/segment_builder.rs`

```rust
// In decode_entry():
// Before:
let ik = InternalKey::from_bytes(data[pos..pos + ik_len].to_vec());

// After:
let ik = match InternalKey::try_from_bytes(data[pos..pos + ik_len].to_vec()) {
    Some(ik) => ik,
    None => return None,  // Corrupted entry — skip
};
```

### Change 2: O(1) approximate entry count

**File:** `crates/storage/src/segmented.rs`

Add an approximate entry counter to `BranchState`:

```rust
struct BranchState {
    active: Memtable,
    frozen: Vec<Arc<Memtable>>,
    segments: Vec<Arc<KVSegment>>,
    /// Approximate entry count (incremented on put, not decremented on delete).
    /// Use list_branch() for exact count.
    approx_entry_count: AtomicUsize,
}
```

Increment in `put_with_version_mode()` and `apply_batch()`:

```rust
// In put_with_version_mode, after put_entry:
branch.approx_entry_count.fetch_add(1, Ordering::Relaxed);
```

Update `memory_stats()` to use the approximate count instead of `list_branch_inner()`:

```rust
pub fn memory_stats(&self) -> StorageMemoryStats {
    // ... existing code for bytes ...
    let count = branch.active.len();
    for frozen in &branch.frozen {
        count += frozen.len();
    }
    // Use memtable len() instead of list_branch_inner().len()
    // for O(1) diagnostics instead of O(entries)
}
```

**Note:** `Memtable::len()` likely already exists as a method on SkipMap. If not, use `approx_entry_count`.

### Change 3: O(1) time_range

**File:** `crates/storage/src/segmented.rs`

Add min/max timestamp atomics to `BranchState`:

```rust
struct BranchState {
    // ... existing fields ...
    min_timestamp: AtomicU64,
    max_timestamp: AtomicU64,
}

impl BranchState {
    fn new() -> Self {
        Self {
            active: Memtable::new(0),
            frozen: Vec::new(),
            segments: Vec::new(),
            min_timestamp: AtomicU64::new(u64::MAX),
            max_timestamp: AtomicU64::new(0),
        }
    }
}
```

Update on every write:

```rust
// In put_with_version_mode, after put_entry:
let ts = entry.timestamp.as_micros();
branch.min_timestamp.fetch_min(ts, Ordering::Relaxed);
branch.max_timestamp.fetch_max(ts, Ordering::Relaxed);
```

Update `time_range()`:

```rust
pub fn time_range(&self, branch_id: BranchId) -> StrataResult<Option<(u64, u64)>> {
    let branch = match self.branches.get(&branch_id) {
        Some(b) => b,
        None => return Ok(None),
    };
    let min_ts = branch.min_timestamp.load(Ordering::Relaxed);
    let max_ts = branch.max_timestamp.load(Ordering::Relaxed);
    if min_ts == u64::MAX {
        return Ok(None);  // No data
    }
    Ok(Some((min_ts, max_ts)))
}
```

**Caveat:** This tracks all writes including tombstones and expired entries. The old implementation filtered these out. This is an acceptable trade-off for O(1) — timestamps of deleted entries still bound the range.

### Files Touched

| File | Lines Changed |
|------|--------------|
| `crates/storage/src/key_encoding.rs` | ~10 (try_from_bytes) |
| `crates/storage/src/segment_builder.rs` | ~5 (use try_from_bytes in decode_entry) |
| `crates/storage/src/segmented.rs` | ~40 (BranchState atomics, O(1) methods) |

### Verification

1. **try_from_bytes safety:** `InternalKey::try_from_bytes(vec![1,2,3])` returns None
2. **O(1) memory_stats:** time memory_stats() call, verify it doesn't scale with entry count
3. **O(1) time_range:** time_range() returns approximate range matching actual min/max

---

## Dependency Graph & Execution Order

```
Epic 10 (flush pipeline)  --> Epic 14 (write stalling)
    |                     --> Epic 15 (WAL truncation)
    |                     --> Epic 16 (tiered compaction)
    |
Epic 11 (version pruning)     [independent]
Epic 12 (segment v2)          [independent]
Epic 13 (streaming WAL)       [independent]
Epic 17 (compression)         [independent]
Epic 18 (robustness)          [independent]
```

### Recommended Execution Order

| Sprint | Epics | Rationale |
|--------|-------|-----------|
| 1 | **Epic 10** (flush pipeline), **Epic 11** (version pruning) | Unblocks everything; independent, small |
| 2 | **Epic 12** (segment v2), **Epic 13** (streaming WAL) | Correctness; recovery safety |
| 3 | **Epic 14** (write stalling), **Epic 15** (WAL truncation) | Depends on Epic 10 |
| 4 | **Epic 16** (tiered compaction), **Epic 17** (compression) | 100M scale; disk efficiency |
| 5 | **Epic 18** (robustness) | Hardening |

### Key Design Decisions

1. **No blocking in DashMap guards:** Write stalling (Epic 14) uses "skip rotation" instead of condvar blocking to avoid deadlocks
2. **Streaming WAL is per-segment:** True per-record streaming within a segment is deferred to Epic 23
3. **Compression is data-blocks-only:** Index, bloom, and properties blocks stay uncompressed for random access
4. **O(1) diagnostics are approximate:** time_range() and memory_stats() trade exactness for speed
5. **Format v2 is backward-compatible:** v1 segments remain readable with default timestamp=0, ttl_ms=0
6. **Tiered compaction defers heap-based merge:** Linear scan is adequate when tiered compaction limits merge width to 4-8 segments
