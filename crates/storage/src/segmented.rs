//! Segmented storage — memtable + immutable segments with MVCC.
//!
//! `SegmentedStore` is a `Storage` trait implementation that combines:
//! - Per-branch active memtable (writable, lock-free SkipMap)
//! - Per-branch frozen memtables (immutable, pending flush)
//! - Per-branch KV segments (on-disk, mmap'd)
//!
//! Read path: active → frozen (newest first) → segments (newest first).
//! The first match for a key at commit_id ≤ snapshot wins.

use crate::compaction::CompactionIterator;
use crate::key_encoding::{encode_typed_key, InternalKey};
use crate::memory_stats::{BranchMemoryStats, StorageMemoryStats};
use crate::memtable::{Memtable, MemtableEntry};
use crate::merge_iter::{MergeIterator, MvccIterator};
use crate::pressure::{MemoryPressure, PressureLevel};
use crate::segment::{KVSegment, SegmentEntry};
use crate::segment_builder::SegmentBuilder;

use dashmap::DashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use strata_core::traits::{Storage, WriteMode};
use strata_core::types::{BranchId, Key, TypeTag};
use strata_core::value::Value;
use strata_core::{StrataResult, Timestamp, VersionedValue};

// ---------------------------------------------------------------------------
// CompactionResult
// ---------------------------------------------------------------------------

/// Statistics returned by [`SegmentedStore::compact_branch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionResult {
    /// Number of input segments that were merged.
    pub segments_merged: usize,
    /// Number of entries in the output segment.
    pub output_entries: u64,
    /// Number of entries pruned (input - output).
    pub entries_pruned: u64,
    /// Size of the output segment file in bytes.
    pub output_file_size: u64,
}

// ---------------------------------------------------------------------------
// BranchState
// ---------------------------------------------------------------------------

/// Per-branch state: active memtable, frozen memtables, and on-disk segments.
struct BranchState {
    /// Writable memtable — all new writes go here.
    active: Memtable,
    /// Frozen memtables, newest first.  Immutable, pending flush.
    frozen: Vec<Arc<Memtable>>,
    /// On-disk KV segments, newest first.
    segments: Vec<Arc<KVSegment>>,
}

impl BranchState {
    fn new() -> Self {
        Self {
            active: Memtable::new(0),
            frozen: Vec::new(),
            segments: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// SegmentedStore
// ---------------------------------------------------------------------------

/// Segmented storage engine: memtable + immutable segments.
///
/// Thread safety: per-branch sharding via `DashMap`. Reads are lock-free
/// within a branch (SkipMap). Writes only lock the target branch's shard.
pub struct SegmentedStore {
    /// Per-branch state.
    branches: DashMap<BranchId, BranchState>,
    /// Global monotonic version counter.
    version: AtomicU64,
    /// Directory for segment files.  `None` = ephemeral (memtable-only).
    segments_dir: Option<PathBuf>,
    /// Memtable rotation threshold in bytes.  0 = disabled.
    write_buffer_size: u64,
    /// Monotonic counter for memtable IDs and segment file names.
    next_segment_id: AtomicU64,
    /// Memory pressure tracking.
    pressure: MemoryPressure,
    /// Branches currently in bulk load mode (rotation deferred).
    bulk_load_branches: DashMap<BranchId, ()>,
    /// Maximum number of branches (0 = unlimited, advisory only).
    max_branches: AtomicUsize,
    /// Maximum versions to keep per key (0 = unlimited, pruned at compaction).
    max_versions_per_key: AtomicUsize,
}

impl SegmentedStore {
    /// Create a new ephemeral segmented store (no disk segments).
    ///
    /// Rotation still works (frozen memtables accumulate), but flush is a no-op
    /// because there is no directory to write segments into.
    pub fn new() -> Self {
        Self {
            branches: DashMap::new(),
            version: AtomicU64::new(0),
            segments_dir: None,
            write_buffer_size: 0,
            next_segment_id: AtomicU64::new(1),
            pressure: MemoryPressure::disabled(),
            bulk_load_branches: DashMap::new(),
            max_branches: AtomicUsize::new(0),
            max_versions_per_key: AtomicUsize::new(0),
        }
    }

    /// Create a segmented store backed by a directory for segment files.
    ///
    /// When a memtable exceeds `write_buffer_size` bytes it is automatically
    /// frozen on the next write.  Call [`flush_oldest_frozen`] to persist
    /// frozen memtables to disk as KV segments.
    pub fn with_dir(segments_dir: PathBuf, write_buffer_size: usize) -> Self {
        Self {
            branches: DashMap::new(),
            version: AtomicU64::new(0),
            segments_dir: Some(segments_dir),
            write_buffer_size: write_buffer_size as u64,
            next_segment_id: AtomicU64::new(1),
            pressure: MemoryPressure::disabled(),
            bulk_load_branches: DashMap::new(),
            max_branches: AtomicUsize::new(0),
            max_versions_per_key: AtomicUsize::new(0),
        }
    }

    /// Create a segmented store with a directory, write buffer, and memory pressure tracking.
    pub fn with_dir_and_pressure(
        segments_dir: PathBuf,
        write_buffer_size: usize,
        pressure: MemoryPressure,
    ) -> Self {
        Self {
            branches: DashMap::new(),
            version: AtomicU64::new(0),
            segments_dir: Some(segments_dir),
            write_buffer_size: write_buffer_size as u64,
            next_segment_id: AtomicU64::new(1),
            pressure,
            bulk_load_branches: DashMap::new(),
            max_branches: AtomicUsize::new(0),
            max_versions_per_key: AtomicUsize::new(0),
        }
    }

    // ========================================================================
    // Inherent methods
    // ========================================================================

    /// Get current version.
    #[inline]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Increment version and return new value.
    #[inline]
    pub fn next_version(&self) -> u64 {
        self.version
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                Some(v.wrapping_add(1))
            })
            .unwrap()
            .wrapping_add(1)
    }

    /// Set version (used during recovery).
    pub fn set_version(&self, version: u64) {
        self.version.store(version, Ordering::Release);
    }

    /// Iterate over all branch IDs that have data.
    pub fn branch_ids(&self) -> Vec<BranchId> {
        self.branches.iter().map(|entry| *entry.key()).collect()
    }

    /// Remove all data for a branch.  Returns true if the branch existed.
    pub fn clear_branch(&self, branch_id: &BranchId) -> bool {
        self.branches.remove(branch_id).is_some()
    }

    /// Count distinct live (non-tombstone, non-expired) logical keys in a branch.
    pub fn branch_entry_count(&self, branch_id: &BranchId) -> usize {
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return 0,
        };
        // Collect all entries via MVCC dedup at u64::MAX
        self.list_branch_inner(&branch).len()
    }

    /// List all live entries for a branch (MVCC dedup at latest version).
    pub fn list_branch(&self, branch_id: &BranchId) -> Vec<(Key, VersionedValue)> {
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return Vec::new(),
        };
        self.list_branch_inner(&branch)
    }

    /// Internal: list all live entries for a branch reference.
    fn list_branch_inner(&self, branch: &BranchState) -> Vec<(Key, VersionedValue)> {
        // Build an empty-prefix key to scan all entries.
        // We iterate all entries from all sources, MVCC dedup, filter tombstones/expired.
        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = Vec::new();

        // Active memtable
        let active_entries: Vec<_> = branch.active.iter_all().collect();
        sources.push(Box::new(active_entries.into_iter()));

        // Frozen memtables (newest first)
        for frozen in &branch.frozen {
            let entries: Vec<_> = frozen.iter_all().collect();
            sources.push(Box::new(entries.into_iter()));
        }

        // Segments (newest first) — convert SegmentEntry to MemtableEntry
        for seg in &branch.segments {
            let entries: Vec<_> = seg
                .iter_seek_all()
                .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                .collect();
            sources.push(Box::new(entries.into_iter()));
        }

        let merge = MergeIterator::new(sources);
        let mvcc = MvccIterator::new(merge, u64::MAX);

        mvcc.filter_map(|(ik, entry)| {
            if entry.is_tombstone || entry.is_expired() {
                return None;
            }
            let (key, commit_id) = ik.decode()?;
            Some((key, entry.to_versioned(commit_id)))
        })
        .collect()
    }

    // ========================================================================
    // Engine-facing API
    // ========================================================================

    /// Set maximum number of branches (0 = unlimited, advisory only).
    pub fn set_max_branches(&self, max: usize) {
        self.max_branches.store(max, Ordering::Relaxed);
    }

    /// Set maximum versions to keep per key (0 = unlimited).
    /// Pruning happens at compaction time, not at write time.
    pub fn set_max_versions_per_key(&self, max: usize) {
        self.max_versions_per_key.store(max, Ordering::Relaxed);
    }

    /// Direct single-key read returning only the Value (no VersionedValue).
    pub fn get_value_direct(&self, key: &Key) -> Option<Value> {
        let branch_id = key.namespace.branch_id;
        let branch = self.branches.get(&branch_id)?;
        let (_commit_id, entry) = Self::get_versioned_from_branch(&branch, key, u64::MAX)?;
        if entry.is_tombstone || entry.is_expired() {
            return None;
        }
        Some(entry.value)
    }

    /// List all live entries for a branch filtered by type tag.
    pub fn list_by_type(
        &self,
        branch_id: &BranchId,
        type_tag: TypeTag,
    ) -> Vec<(Key, VersionedValue)> {
        self.list_branch(branch_id)
            .into_iter()
            .filter(|(key, _)| key.type_tag == type_tag)
            .collect()
    }

    /// List entries filtered by type tag with timestamp ≤ max_ts.
    ///
    /// Point-in-time query: for each key, finds the latest version whose
    /// timestamp ≤ max_ts, ignoring newer versions entirely.
    ///
    /// **Limitation:** On-disk segments (v1 format) do not store timestamps.
    /// Flushed entries appear with timestamp=0 and will always match any
    /// `max_ts` query. Results are only fully correct for memtable-resident data.
    pub fn list_by_type_at_timestamp(
        &self,
        branch_id: &BranchId,
        type_tag: TypeTag,
        max_ts: u64,
    ) -> Vec<(Key, VersionedValue)> {
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return Vec::new(),
        };

        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = Vec::new();
        let active_entries: Vec<_> = branch.active.iter_all().collect();
        sources.push(Box::new(active_entries.into_iter()));
        for frozen in &branch.frozen {
            let entries: Vec<_> = frozen.iter_all().collect();
            sources.push(Box::new(entries.into_iter()));
        }
        for seg in &branch.segments {
            let entries: Vec<_> = seg
                .iter_seek_all()
                .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                .collect();
            sources.push(Box::new(entries.into_iter()));
        }

        let merge = MergeIterator::new(sources);
        let mut results: Vec<(Key, VersionedValue)> = Vec::new();
        let mut last_typed_key: Option<Vec<u8>> = None;
        let mut found_for_current = false;

        for (ik, entry) in merge {
            let tk = ik.typed_key_prefix().to_vec();
            if last_typed_key.as_ref() != Some(&tk) {
                last_typed_key = Some(tk);
                found_for_current = false;
            }
            if found_for_current {
                continue;
            }
            if entry.is_expired() || entry.timestamp.as_micros() > max_ts {
                continue;
            }
            found_for_current = true;
            if entry.is_tombstone {
                continue;
            }
            if let Some((key, commit_id)) = ik.decode() {
                if key.type_tag == type_tag {
                    results.push((key, entry.to_versioned(commit_id)));
                }
            }
        }
        results
    }

    /// Get the latest entry for a key where timestamp ≤ max_ts.
    ///
    /// **Limitation:** On-disk segments (v1 format) do not store timestamps.
    /// Flushed entries appear with timestamp=0 and will always match.
    pub fn get_at_timestamp(
        &self,
        key: &Key,
        max_timestamp: u64,
    ) -> StrataResult<Option<VersionedValue>> {
        let branch_id = key.namespace.branch_id;
        let branch = match self.branches.get(&branch_id) {
            Some(b) => b,
            None => return Ok(None),
        };
        let all_versions = Self::get_all_versions_from_branch(&branch, key);
        for (commit_id, entry) in all_versions {
            if entry.is_expired() {
                continue;
            }
            if entry.timestamp.as_micros() <= max_timestamp {
                if entry.is_tombstone {
                    return Ok(None);
                }
                return Ok(Some(entry.to_versioned(commit_id)));
            }
        }
        Ok(None)
    }

    /// Scan keys matching a prefix at or before the given timestamp.
    ///
    /// For each key, finds the latest version whose timestamp ≤ max_timestamp.
    ///
    /// **Limitation:** On-disk segments (v1 format) do not store timestamps.
    /// Flushed entries appear with timestamp=0 and will always match.
    pub fn scan_prefix_at_timestamp(
        &self,
        prefix: &Key,
        max_timestamp: u64,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        let branch_id = prefix.namespace.branch_id;
        let branch = match self.branches.get(&branch_id) {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };

        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = Vec::new();
        let active_entries: Vec<_> = branch.active.iter_prefix(prefix).collect();
        sources.push(Box::new(active_entries.into_iter()));
        for frozen in &branch.frozen {
            let entries: Vec<_> = frozen.iter_prefix(prefix).collect();
            sources.push(Box::new(entries.into_iter()));
        }
        for seg in &branch.segments {
            let entries: Vec<_> = seg
                .iter_seek(prefix)
                .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                .collect();
            sources.push(Box::new(entries.into_iter()));
        }

        let merge = MergeIterator::new(sources);
        let mut results: Vec<(Key, VersionedValue)> = Vec::new();
        let mut last_typed_key: Option<Vec<u8>> = None;
        let mut found_for_current = false;

        for (ik, entry) in merge {
            let tk = ik.typed_key_prefix().to_vec();
            if last_typed_key.as_ref() != Some(&tk) {
                last_typed_key = Some(tk);
                found_for_current = false;
            }
            if found_for_current {
                continue;
            }
            if entry.is_expired() || entry.timestamp.as_micros() > max_timestamp {
                continue;
            }
            found_for_current = true;
            if entry.is_tombstone {
                continue;
            }
            if let Some((key, commit_id)) = ik.decode() {
                results.push((key, entry.to_versioned(commit_id)));
            }
        }
        Ok(results)
    }

    /// Get the available time range for a branch.
    ///
    /// Returns `(oldest_ts, latest_ts)` in microseconds since epoch.
    /// Returns `None` if the branch has no data.
    pub fn time_range(&self, branch_id: BranchId) -> StrataResult<Option<(u64, u64)>> {
        let entries = self.list_branch(&branch_id);
        if entries.is_empty() {
            return Ok(None);
        }
        let mut min_ts = u64::MAX;
        let mut max_ts = 0u64;
        for (_, vv) in &entries {
            let ts = vv.timestamp.as_micros();
            min_ts = min_ts.min(ts);
            max_ts = max_ts.max(ts);
        }
        Ok(Some((min_ts, max_ts)))
    }

    /// Garbage collect old versions for a branch.
    /// SegmentedStore prunes via compaction — this is a no-op stub.
    pub fn gc_branch(&self, _branch_id: &BranchId, _min_version: u64) -> usize {
        0
    }

    /// Expire TTL keys.
    /// SegmentedStore handles TTL at read time and during compaction — this is a no-op stub.
    pub fn expire_ttl_keys(&self, _now: u64) -> usize {
        0
    }

    /// Get `(entry_count, total_version_count, btree_built)` for a branch.
    ///
    /// SegmentedStore does not use BTreeSet indexes, so `btree_built` is always false.
    pub fn shard_stats_detailed(&self, branch_id: &BranchId) -> Option<(usize, usize, bool)> {
        let branch = self.branches.get(branch_id)?;
        let entry_count = self.list_branch_inner(&branch).len();
        let mut total_versions = branch.active.len();
        for frozen in &branch.frozen {
            total_versions += frozen.len();
        }
        for seg in &branch.segments {
            total_versions += seg.entry_count() as usize;
        }
        Some((entry_count, total_versions, false))
    }

    /// Memory usage statistics.
    pub fn memory_stats(&self) -> StorageMemoryStats {
        let mut total_entries = 0usize;
        let mut estimated_bytes = 0usize;
        let mut per_branch = Vec::new();

        for entry in self.branches.iter() {
            let branch_id = *entry.key();
            let branch = entry.value();
            let active_bytes = branch.active.approx_bytes() as usize;
            let frozen_bytes: usize = branch
                .frozen
                .iter()
                .map(|f| f.approx_bytes() as usize)
                .sum();
            let branch_bytes = active_bytes + frozen_bytes;
            let count = self.list_branch_inner(branch).len();
            total_entries += count;
            estimated_bytes += branch_bytes;
            per_branch.push(BranchMemoryStats {
                branch_id,
                entry_count: count,
                has_btree_index: false,
                estimated_bytes: branch_bytes,
            });
        }

        StorageMemoryStats {
            total_branches: self.branches.len(),
            total_entries,
            estimated_bytes,
            per_branch,
        }
    }

    // ========================================================================
    // Bulk Load Mode
    // ========================================================================

    /// Enter bulk load mode for a branch — defers memtable rotation.
    ///
    /// While in bulk load mode, `maybe_rotate` skips this branch so the active
    /// memtable can grow beyond `write_buffer_size`. Call `end_bulk_load()` when
    /// done to flush all accumulated data.
    pub fn begin_bulk_load(&self, branch_id: &BranchId) {
        self.bulk_load_branches.insert(*branch_id, ());
    }

    /// Exit bulk load mode for a branch — rotates and flushes all memtables.
    ///
    /// After this call, all data written during the bulk load is flushed to
    /// frozen memtables (and to disk segments if a directory is configured).
    /// Normal rotation behavior resumes.
    pub fn end_bulk_load(&self, branch_id: &BranchId) -> io::Result<()> {
        self.bulk_load_branches.remove(branch_id);

        // Rotate the active memtable (unconditionally — it may be large)
        self.rotate_memtable(branch_id);

        // Flush all frozen memtables for this branch
        while self.flush_oldest_frozen(branch_id)? {}

        Ok(())
    }

    /// Check if a branch is in bulk load mode.
    pub fn is_bulk_loading(&self, branch_id: &BranchId) -> bool {
        self.bulk_load_branches.contains_key(branch_id)
    }

    // ========================================================================
    // Rotation & Flush
    // ========================================================================

    /// Freeze the active memtable for `branch_id` and swap in a fresh one.
    ///
    /// The old (now frozen) memtable is pushed to the front of `frozen` so
    /// that newest-first read ordering is preserved.  This is cheap (~µs).
    ///
    /// Returns `false` if the branch does not exist.
    pub fn rotate_memtable(&self, branch_id: &BranchId) -> bool {
        let mut branch = match self.branches.get_mut(branch_id) {
            Some(b) => b,
            None => return false,
        };
        let next_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let old = std::mem::replace(&mut branch.active, Memtable::new(next_id));
        old.freeze();
        branch.frozen.insert(0, Arc::new(old));
        true
    }

    /// Flush the oldest frozen memtable for `branch_id` to a KV segment on disk.
    ///
    /// Returns `Ok(true)` if a memtable was flushed, `Ok(false)` if there was
    /// nothing to flush (no frozen memtables, branch missing, or ephemeral mode).
    ///
    /// The frozen memtable stays in the read path during I/O so concurrent
    /// readers never see a gap.  It is only removed when the segment is
    /// installed, in a single DashMap guard.
    pub fn flush_oldest_frozen(&self, branch_id: &BranchId) -> io::Result<bool> {
        let segments_dir = match &self.segments_dir {
            Some(d) => d,
            None => return Ok(false),
        };

        // Clone the Arc to the oldest frozen memtable (keep it in the list
        // so readers can still find it during I/O).
        let frozen_mt = {
            let branch = match self.branches.get(branch_id) {
                Some(b) => b,
                None => return Ok(false),
            };
            match branch.frozen.last() {
                Some(mt) => Arc::clone(mt),
                None => return Ok(false),
            }
            // DashMap guard drops here; data stays alive via Arc
        };

        // Build segment to disk (no locks held — I/O-heavy).
        let seg_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let branch_hex = hex_encode_branch(branch_id);
        let branch_dir = segments_dir.join(&branch_hex);
        std::fs::create_dir_all(&branch_dir)?;
        let seg_path = branch_dir.join(format!("{}.sst", seg_id));
        let builder = SegmentBuilder::default();
        builder.build_from_iter(frozen_mt.iter_all(), &seg_path)?;

        // Open the newly written segment.
        let segment = KVSegment::open(&seg_path)?;

        // Atomically: remove the frozen memtable we just flushed and install
        // the segment, under a single DashMap guard.
        let mut branch = self
            .branches
            .entry(*branch_id)
            .or_insert_with(BranchState::new);
        if let Some(last) = branch.frozen.last() {
            if last.id() == frozen_mt.id() {
                branch.frozen.pop();
            }
        }
        branch.segments.insert(0, Arc::new(segment));

        Ok(true)
    }

    /// Returns `true` if `branch_id` has any frozen memtables pending flush.
    pub fn has_frozen(&self, branch_id: &BranchId) -> bool {
        self.branches
            .get(branch_id)
            .is_some_and(|b| !b.frozen.is_empty())
    }

    /// Number of frozen memtables for a branch (test helper).
    pub fn branch_frozen_count(&self, branch_id: &BranchId) -> usize {
        self.branches.get(branch_id).map_or(0, |b| b.frozen.len())
    }

    /// Number of on-disk segments for a branch (test helper).
    pub fn branch_segment_count(&self, branch_id: &BranchId) -> usize {
        self.branches.get(branch_id).map_or(0, |b| b.segments.len())
    }

    /// Return the maximum commit_id across all flushed segments for a branch.
    ///
    /// Returns `None` if the branch has no segments.
    pub fn max_flushed_commit(&self, branch_id: &BranchId) -> Option<u64> {
        let branch = self.branches.get(branch_id)?;
        branch.segments.iter().map(|s| s.commit_range().1).max()
    }

    /// Get the segments directory (if any).
    pub fn segments_dir(&self) -> Option<&PathBuf> {
        self.segments_dir.as_ref()
    }

    /// Recover flushed segments from disk.
    ///
    /// Scans `segments_dir` for branch subdirectories (hex-encoded BranchId),
    /// opens `.sst` files within each, and installs them into the store.
    ///
    /// Returns `Ok(info)` with recovery statistics, or `Err` on fatal I/O errors.
    /// Individual corrupt `.sst` files are skipped (counted in `errors_skipped`).
    pub fn recover_segments(&self) -> io::Result<RecoverSegmentsInfo> {
        let segments_dir = match &self.segments_dir {
            Some(d) => d,
            None => {
                return Ok(RecoverSegmentsInfo {
                    branches_recovered: 0,
                    segments_loaded: 0,
                    errors_skipped: 0,
                })
            }
        };

        if !segments_dir.exists() {
            return Ok(RecoverSegmentsInfo {
                branches_recovered: 0,
                segments_loaded: 0,
                errors_skipped: 0,
            });
        }

        let mut info = RecoverSegmentsInfo {
            branches_recovered: 0,
            segments_loaded: 0,
            errors_skipped: 0,
        };

        for entry in std::fs::read_dir(segments_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let dir_name = match entry.file_name().to_str() {
                Some(s) => s.to_string(),
                None => continue,
            };

            let branch_id = match hex_decode_branch(&dir_name) {
                Some(id) => id,
                None => continue,
            };

            let mut branch_segments: Vec<Arc<KVSegment>> = Vec::new();

            for seg_entry in std::fs::read_dir(&path)? {
                let seg_entry = seg_entry?;
                let seg_path = seg_entry.path();
                if seg_path.extension().and_then(|e| e.to_str()) != Some("sst") {
                    continue;
                }

                match KVSegment::open(&seg_path) {
                    Ok(seg) => {
                        // Parse segment ID from filename to avoid ID collisions
                        if let Some(stem) = seg_path.file_stem().and_then(|s| s.to_str()) {
                            if let Ok(file_seg_id) = stem.parse::<u64>() {
                                self.next_segment_id
                                    .fetch_max(file_seg_id + 1, Ordering::Relaxed);
                            }
                        }
                        branch_segments.push(Arc::new(seg));
                    }
                    Err(_) => {
                        info.errors_skipped += 1;
                    }
                }
            }

            if branch_segments.is_empty() {
                continue;
            }

            // Sort by commit_max descending (newest first)
            branch_segments.sort_by(|a, b| b.commit_range().1.cmp(&a.commit_range().1));

            let mut branch = self
                .branches
                .entry(branch_id)
                .or_insert_with(BranchState::new);

            info.segments_loaded += branch_segments.len();
            branch.segments.extend(branch_segments);
            // Re-sort after extending in case there were existing segments
            branch
                .segments
                .sort_by(|a, b| b.commit_range().1.cmp(&a.commit_range().1));

            info.branches_recovered += 1;
        }

        Ok(info)
    }

    /// Check whether the active memtable should be rotated and do so inline.
    ///
    /// Called after every write within the DashMap entry guard.
    #[inline]
    fn maybe_rotate_branch(&self, branch_id: BranchId, branch: &mut BranchState) {
        if self.write_buffer_size > 0
            && branch.active.approx_bytes() >= self.write_buffer_size
            && !self.bulk_load_branches.contains_key(&branch_id)
        {
            let next_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
            let old = std::mem::replace(&mut branch.active, Memtable::new(next_id));
            old.freeze();
            branch.frozen.insert(0, Arc::new(old));
        }
    }

    // ========================================================================
    // Compaction
    // ========================================================================

    /// Compact all segments for a branch into a single segment, pruning old versions.
    ///
    /// Returns `Ok(None)` if there is nothing to compact (ephemeral mode,
    /// branch missing, or fewer than 2 segments). Returns `Ok(Some(result))`
    /// with compaction statistics on success.
    ///
    /// Versions with `commit_id < prune_floor` are pruned (at most one per
    /// logical key survives). Dead tombstones below the floor are removed
    /// entirely. When `prune_floor == 0`, no versions are pruned.
    ///
    /// # Concurrent flush safety
    ///
    /// If a concurrent `flush_oldest_frozen` inserts a new segment for the
    /// same branch while compaction is running, the new segment is preserved.
    /// The caller should still serialize flush and compact for a given branch
    /// when possible, to avoid repeated compaction of freshly-flushed data.
    pub fn compact_branch(
        &self,
        branch_id: &BranchId,
        prune_floor: u64,
    ) -> io::Result<Option<CompactionResult>> {
        let segments_dir = match &self.segments_dir {
            Some(d) => d,
            None => return Ok(None),
        };

        // Snapshot current segments under a read guard.
        let (old_segments, total_input_entries) = {
            let branch = match self.branches.get(branch_id) {
                Some(b) => b,
                None => return Ok(None),
            };
            if branch.segments.len() < 2 {
                return Ok(None);
            }
            let segs: Vec<Arc<KVSegment>> = branch.segments.iter().map(Arc::clone).collect();
            let total: u64 = segs.iter().map(|s| s.entry_count()).sum();
            (segs, total)
            // DashMap guard drops here
        };

        let segments_merged = old_segments.len();

        // Build compaction (no lock held — I/O heavy).
        let seg_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let branch_hex = hex_encode_branch(branch_id);
        let branch_dir = segments_dir.join(&branch_hex);
        std::fs::create_dir_all(&branch_dir)?;
        let seg_path = branch_dir.join(format!("{}.sst", seg_id));

        // Build sorted source iterators from each segment (oldest → newest
        // doesn't matter for MergeIterator ordering, but we use the same
        // order as flush: index 0 = newest).
        let sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = old_segments
            .iter()
            .map(|seg| {
                let entries: Vec<_> = seg
                    .iter_seek_all()
                    .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                    .collect();
                Box::new(entries.into_iter())
                    as Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>
            })
            .collect();

        let merge = MergeIterator::new(sources);
        let max_versions = self.max_versions_per_key.load(Ordering::Relaxed);
        let compaction_iter =
            CompactionIterator::new(merge, prune_floor).with_max_versions(max_versions);

        let builder = SegmentBuilder::default();
        let meta = builder.build_from_iter(compaction_iter, &seg_path)?;

        // Open the newly written segment.
        let new_segment = KVSegment::open(&seg_path)?;
        let new_seg_filename = format!("{}.sst", seg_id);

        // Swap: remove only the segments we compacted, insert the new one.
        // Any segments added by concurrent flushes (not in old_segments) are kept.
        let old_seg_count;
        {
            let mut branch = self
                .branches
                .entry(*branch_id)
                .or_insert_with(BranchState::new);
            old_seg_count = branch.segments.len();
            branch
                .segments
                .retain(|s| !old_segments.iter().any(|old| Arc::ptr_eq(s, old)));
            // Compacted segment goes at the end (oldest — covers the range
            // of all old segments). Any concurrent segments are newer and
            // already at the front.
            branch.segments.push(Arc::new(new_segment));
        }

        // File cleanup: only delete .sst files for the segments we compacted.
        // Build a set of filenames to keep (new segment + any concurrent segments).
        // We know old segments used IDs allocated before `seg_id`, so their
        // filenames differ from `new_seg_filename`. We enumerate the directory
        // and only remove files that are NOT the new segment and that existed
        // before compaction started (i.e., have a numeric stem < seg_id).
        if old_seg_count == segments_merged {
            // Fast path: no concurrent segments appeared, safe to delete all
            // .sst files except the new one.
            if let Ok(dir_entries) = std::fs::read_dir(&branch_dir) {
                for entry in dir_entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("sst") {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            if name != new_seg_filename {
                                let _ = std::fs::remove_file(&path);
                            }
                        }
                    }
                }
            }
        } else {
            // Concurrent segments appeared — only delete files whose numeric
            // stem is < seg_id (these belonged to the old segments we merged).
            if let Ok(dir_entries) = std::fs::read_dir(&branch_dir) {
                for entry in dir_entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) != Some("sst") {
                        continue;
                    }
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(file_id) = stem.parse::<u64>() {
                            if file_id < seg_id {
                                let _ = std::fs::remove_file(&path);
                            }
                        }
                    }
                }
            }
        }

        let entries_pruned = total_input_entries.saturating_sub(meta.entry_count);

        Ok(Some(CompactionResult {
            segments_merged,
            output_entries: meta.entry_count,
            entries_pruned,
            output_file_size: meta.file_size,
        }))
    }

    /// Returns `true` if the branch has more than `threshold` segments.
    pub fn should_compact(&self, branch_id: &BranchId, segment_threshold: usize) -> bool {
        self.branches
            .get(branch_id)
            .is_some_and(|b| b.segments.len() >= segment_threshold)
    }

    // ========================================================================
    // Memory pressure
    // ========================================================================

    /// Sum of `approx_bytes()` across all active + frozen memtables.
    pub fn total_memtable_bytes(&self) -> u64 {
        let mut total: u64 = 0;
        for entry in self.branches.iter() {
            let branch = entry.value();
            total += branch.active.approx_bytes();
            for frozen in &branch.frozen {
                total += frozen.approx_bytes();
            }
        }
        total
    }

    /// Check the current memory pressure level.
    pub fn pressure_level(&self) -> PressureLevel {
        self.pressure.level(self.total_memtable_bytes())
    }

    /// Branch IDs with frozen memtables, ordered by frozen count descending.
    ///
    /// Useful for the caller to decide which branches to flush first.
    pub fn branches_needing_flush(&self) -> Vec<BranchId> {
        let mut branches: Vec<(BranchId, usize)> = self
            .branches
            .iter()
            .filter_map(|entry| {
                let count = entry.value().frozen.len();
                if count > 0 {
                    Some((*entry.key(), count))
                } else {
                    None
                }
            })
            .collect();
        branches.sort_by(|a, b| b.1.cmp(&a.1));
        branches.into_iter().map(|(id, _)| id).collect()
    }

    // ========================================================================
    // Read-path helpers
    // ========================================================================

    /// Point read across all sources in a branch.
    ///
    /// Returns `(commit_id, entry)` for the newest version with commit_id ≤ max_version.
    /// Uses `get_versioned_with_commit` to avoid a double-traversal race where a
    /// concurrent write could change results between two separate lookups.
    fn get_versioned_from_branch(
        branch: &BranchState,
        key: &Key,
        max_version: u64,
    ) -> Option<(u64, MemtableEntry)> {
        // 1. Active memtable
        if let Some(result) = branch.active.get_versioned_with_commit(key, max_version) {
            return Some(result);
        }

        // 2. Frozen memtables (newest first)
        for frozen in &branch.frozen {
            if let Some(result) = frozen.get_versioned_with_commit(key, max_version) {
                return Some(result);
            }
        }

        // 3. Segments (newest first)
        for seg in &branch.segments {
            if let Some(se) = seg.point_lookup(key, max_version) {
                let commit_id = se.commit_id;
                return Some((commit_id, segment_entry_to_memtable_entry(se)));
            }
        }

        None
    }

    /// Collect all versions of a key across all sources in a branch.
    fn get_all_versions_from_branch(branch: &BranchState, key: &Key) -> Vec<(u64, MemtableEntry)> {
        let mut all_versions = Vec::new();

        // Active memtable
        all_versions.extend(branch.active.get_all_versions(key));

        // Frozen memtables
        for frozen in &branch.frozen {
            all_versions.extend(frozen.get_all_versions(key));
        }

        // Segments
        let typed_key = encode_typed_key(key);
        for seg in &branch.segments {
            for (ik, se) in seg.iter_seek(key) {
                if ik.typed_key_prefix() != typed_key.as_slice() {
                    break;
                }
                all_versions.push((se.commit_id, segment_entry_to_memtable_entry(se)));
            }
        }

        // Sort descending by commit_id (newest first)
        all_versions.sort_by(|a, b| b.0.cmp(&a.0));
        all_versions
    }

    /// Build an MVCC-deduplicated prefix scan across all sources in a branch.
    fn scan_prefix_from_branch(
        branch: &BranchState,
        prefix: &Key,
        max_version: u64,
    ) -> Vec<(Key, VersionedValue)> {
        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = Vec::new();

        // Active memtable
        let active_entries: Vec<_> = branch.active.iter_prefix(prefix).collect();
        sources.push(Box::new(active_entries.into_iter()));

        // Frozen memtables (newest first)
        for frozen in &branch.frozen {
            let entries: Vec<_> = frozen.iter_prefix(prefix).collect();
            sources.push(Box::new(entries.into_iter()));
        }

        // Segments (newest first)
        for seg in &branch.segments {
            let entries: Vec<_> = seg
                .iter_seek(prefix)
                .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                .collect();
            sources.push(Box::new(entries.into_iter()));
        }

        let merge = MergeIterator::new(sources);
        let mvcc = MvccIterator::new(merge, max_version);

        mvcc.filter_map(|(ik, entry)| {
            if entry.is_tombstone || entry.is_expired() {
                return None;
            }
            let (key, commit_id) = ik.decode()?;
            Some((key, entry.to_versioned(commit_id)))
        })
        .collect()
    }
}

/// Statistics returned by [`SegmentedStore::recover_segments`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverSegmentsInfo {
    /// Number of branch subdirectories successfully loaded.
    pub branches_recovered: usize,
    /// Total number of `.sst` segments loaded across all branches.
    pub segments_loaded: usize,
    /// Number of `.sst` files that failed to open (corrupt/invalid).
    pub errors_skipped: usize,
}

impl Default for SegmentedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for SegmentedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentedStore")
            .field("branch_count", &self.branches.len())
            .field("version", &self.version())
            .finish()
    }
}

// ============================================================================
// Storage Trait Implementation
// ============================================================================

impl Storage for SegmentedStore {
    fn get_versioned(&self, key: &Key, max_version: u64) -> StrataResult<Option<VersionedValue>> {
        let branch_id = key.namespace.branch_id;
        let branch = match self.branches.get(&branch_id) {
            Some(b) => b,
            None => return Ok(None),
        };

        match Self::get_versioned_from_branch(&branch, key, max_version) {
            Some((commit_id, entry)) => {
                if entry.is_tombstone || entry.is_expired() {
                    Ok(None)
                } else {
                    Ok(Some(entry.to_versioned(commit_id)))
                }
            }
            None => Ok(None),
        }
    }

    fn get_history(
        &self,
        key: &Key,
        limit: Option<usize>,
        before_version: Option<u64>,
    ) -> StrataResult<Vec<VersionedValue>> {
        let branch_id = key.namespace.branch_id;
        let branch = match self.branches.get(&branch_id) {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };

        let all_versions = Self::get_all_versions_from_branch(&branch, key);

        let results: Vec<VersionedValue> = all_versions
            .into_iter()
            .filter(|(commit_id, entry)| {
                // Filter by before_version
                if let Some(bv) = before_version {
                    if *commit_id >= bv {
                        return false;
                    }
                }
                // Filter expired (but NOT tombstones — tombstones are included in history)
                !entry.is_expired()
            })
            .map(|(commit_id, entry)| entry.to_versioned(commit_id))
            .collect();

        // Apply limit
        match limit {
            Some(n) => Ok(results.into_iter().take(n).collect()),
            None => Ok(results),
        }
    }

    fn scan_prefix(
        &self,
        prefix: &Key,
        max_version: u64,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        let branch_id = prefix.namespace.branch_id;
        let branch = match self.branches.get(&branch_id) {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };

        Ok(Self::scan_prefix_from_branch(&branch, prefix, max_version))
    }

    fn current_version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    fn put_with_version_mode(
        &self,
        key: Key,
        value: Value,
        version: u64,
        ttl: Option<Duration>,
        _mode: WriteMode,
    ) -> StrataResult<()> {
        let branch_id = key.namespace.branch_id;

        // Get-or-create branch
        let mut branch = self
            .branches
            .entry(branch_id)
            .or_insert_with(BranchState::new);

        let ttl_ms = ttl.map(|d| d.as_millis() as u64).unwrap_or(0);
        let entry = MemtableEntry {
            value,
            is_tombstone: false,
            timestamp: Timestamp::now(),
            ttl_ms,
        };
        branch.active.put_entry(&key, version, entry);

        // Rotate if active memtable exceeds threshold
        self.maybe_rotate_branch(branch_id, &mut branch);

        // Update global version
        self.version.fetch_max(version, Ordering::AcqRel);

        Ok(())
    }

    fn delete_with_version(&self, key: &Key, version: u64) -> StrataResult<()> {
        let branch_id = key.namespace.branch_id;

        let mut branch = self
            .branches
            .entry(branch_id)
            .or_insert_with(BranchState::new);

        let entry = MemtableEntry {
            value: Value::Null,
            is_tombstone: true,
            timestamp: Timestamp::now(),
            ttl_ms: 0,
        };
        branch.active.put_entry(key, version, entry);

        // Rotate if active memtable exceeds threshold
        self.maybe_rotate_branch(branch_id, &mut branch);

        // Update global version
        self.version.fetch_max(version, Ordering::AcqRel);

        Ok(())
    }

    fn apply_batch(&self, writes: Vec<(Key, Value, WriteMode)>, version: u64) -> StrataResult<()> {
        if writes.is_empty() {
            return Ok(());
        }

        // Group by branch_id — acquire each DashMap guard once per branch.
        // WriteMode is ignored (same as put_with_version_mode): memtable appends
        // all versions unconditionally; version chain pruning happens at compaction.
        let mut by_branch: std::collections::HashMap<BranchId, Vec<(Key, Value)>> =
            std::collections::HashMap::new();
        for (key, value, _mode) in writes {
            by_branch
                .entry(key.namespace.branch_id)
                .or_default()
                .push((key, value));
        }

        let timestamp = Timestamp::now();
        for (branch_id, entries) in by_branch {
            let mut branch = self
                .branches
                .entry(branch_id)
                .or_insert_with(BranchState::new);
            for (key, value) in entries {
                let entry = MemtableEntry {
                    value,
                    is_tombstone: false,
                    timestamp,
                    ttl_ms: 0,
                };
                branch.active.put_entry(&key, version, entry);
            }
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        self.version.fetch_max(version, Ordering::AcqRel);
        Ok(())
    }

    fn delete_batch(&self, deletes: Vec<Key>, version: u64) -> StrataResult<()> {
        if deletes.is_empty() {
            return Ok(());
        }

        let mut by_branch: std::collections::HashMap<BranchId, Vec<Key>> =
            std::collections::HashMap::new();
        for key in deletes {
            by_branch
                .entry(key.namespace.branch_id)
                .or_default()
                .push(key);
        }

        let timestamp = Timestamp::now();
        for (branch_id, keys) in by_branch {
            let mut branch = self
                .branches
                .entry(branch_id)
                .or_insert_with(BranchState::new);
            for key in keys {
                let entry = MemtableEntry {
                    value: Value::Null,
                    is_tombstone: true,
                    timestamp,
                    ttl_ms: 0,
                };
                branch.active.put_entry(&key, version, entry);
            }
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        self.version.fetch_max(version, Ordering::AcqRel);
        Ok(())
    }

    fn get_version_only(&self, key: &Key) -> StrataResult<Option<u64>> {
        let branch_id = key.namespace.branch_id;
        let branch = match self.branches.get(&branch_id) {
            Some(b) => b,
            None => return Ok(None),
        };

        match Self::get_versioned_from_branch(&branch, key, u64::MAX) {
            Some((commit_id, entry)) => {
                if entry.is_tombstone || entry.is_expired() {
                    Ok(None)
                } else {
                    Ok(Some(commit_id))
                }
            }
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Hex-encode a BranchId's 16 bytes to a 32-char lowercase hex string.
fn hex_encode_branch(branch_id: &BranchId) -> String {
    let bytes = branch_id.as_bytes();
    let mut s = String::with_capacity(32);
    for &b in bytes.iter() {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// Decode a 32-char hex string back to a BranchId.
/// Returns `None` if the string is not exactly 32 hex chars.
fn hex_decode_branch(hex: &str) -> Option<BranchId> {
    if hex.len() != 32 || !hex.is_ascii() {
        return None;
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(BranchId::from_bytes(bytes))
}

/// Convert a `SegmentEntry` into a `MemtableEntry` for the merge path.
fn segment_entry_to_memtable_entry(se: SegmentEntry) -> MemtableEntry {
    MemtableEntry {
        value: se.value,
        is_tombstone: se.is_tombstone,
        // Segments don't store timestamp/TTL in v1 format.
        // Use epoch as placeholder; TTL is 0 (no expiry).
        timestamp: Timestamp::from_micros(0),
        ttl_ms: 0,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::types::{Namespace, TypeTag};

    fn branch() -> BranchId {
        BranchId::from_bytes([1; 16])
    }

    fn ns() -> Arc<Namespace> {
        Arc::new(Namespace::new(branch(), "default".to_string()))
    }

    fn kv_key(name: &str) -> Key {
        Key::new(ns(), TypeTag::KV, name.as_bytes().to_vec())
    }

    fn seed(store: &SegmentedStore, key: Key, value: Value, version: u64) {
        store
            .put_with_version_mode(key, value, version, None, WriteMode::Append)
            .unwrap();
    }

    // ===== Basic Storage trait tests =====

    #[test]
    fn put_then_get() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(42), 1);
        let result = store
            .get_versioned(&kv_key("k"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(42));
        assert_eq!(result.version.as_u64(), 1);
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let store = SegmentedStore::new();
        assert!(store
            .get_versioned(&kv_key("k"), u64::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn delete_creates_tombstone() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(42), 1);
        store.delete_with_version(&kv_key("k"), 2).unwrap();
        assert!(store
            .get_versioned(&kv_key("k"), u64::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn versioned_read_respects_snapshot() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(10), 1);
        seed(&store, kv_key("k"), Value::Int(20), 2);
        seed(&store, kv_key("k"), Value::Int(30), 3);

        assert_eq!(
            store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
            Value::Int(10)
        );
        assert_eq!(
            store.get_versioned(&kv_key("k"), 2).unwrap().unwrap().value,
            Value::Int(20)
        );
        assert_eq!(
            store.get_versioned(&kv_key("k"), 3).unwrap().unwrap().value,
            Value::Int(30)
        );
        assert!(store.get_versioned(&kv_key("k"), 0).unwrap().is_none());
    }

    #[test]
    fn tombstone_snapshot_isolation() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(10), 1);
        store.delete_with_version(&kv_key("k"), 2).unwrap();
        seed(&store, kv_key("k"), Value::Int(30), 3);

        // Snapshot at 1: see original value
        assert_eq!(
            store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
            Value::Int(10)
        );
        // Snapshot at 2: tombstone → None
        assert!(store.get_versioned(&kv_key("k"), 2).unwrap().is_none());
        // Snapshot at 3: see re-written value
        assert_eq!(
            store.get_versioned(&kv_key("k"), 3).unwrap().unwrap().value,
            Value::Int(30)
        );
    }

    #[test]
    fn get_history_returns_newest_first() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        seed(&store, kv_key("k"), Value::Int(2), 2);
        seed(&store, kv_key("k"), Value::Int(3), 3);

        let history = store.get_history(&kv_key("k"), None, None).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].value, Value::Int(3));
        assert_eq!(history[1].value, Value::Int(2));
        assert_eq!(history[2].value, Value::Int(1));
    }

    #[test]
    fn get_history_with_limit() {
        let store = SegmentedStore::new();
        for i in 1..=10 {
            seed(&store, kv_key("k"), Value::Int(i), i as u64);
        }
        let history = store.get_history(&kv_key("k"), Some(3), None).unwrap();
        assert_eq!(history.len(), 3);
        // Must be the 3 newest versions
        assert_eq!(history[0].value, Value::Int(10));
        assert_eq!(history[1].value, Value::Int(9));
        assert_eq!(history[2].value, Value::Int(8));
    }

    #[test]
    fn get_history_with_before_version() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        seed(&store, kv_key("k"), Value::Int(2), 2);
        seed(&store, kv_key("k"), Value::Int(3), 3);

        let history = store.get_history(&kv_key("k"), None, Some(3)).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].value, Value::Int(2));
    }

    #[test]
    fn get_history_includes_tombstones() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.delete_with_version(&kv_key("k"), 2).unwrap();
        seed(&store, kv_key("k"), Value::Int(3), 3);

        let history = store.get_history(&kv_key("k"), None, None).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].value, Value::Int(3));
        // Tombstone at v2 appears as Value::Null
        assert_eq!(history[1].value, Value::Null);
        assert_eq!(history[1].version.as_u64(), 2);
        assert_eq!(history[2].value, Value::Int(1));
    }

    #[test]
    fn scan_prefix_returns_matching_keys() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("user/alice"), Value::Int(1), 1);
        seed(&store, kv_key("user/bob"), Value::Int(2), 2);
        seed(&store, kv_key("config/x"), Value::Int(3), 3);

        let prefix = Key::new(ns(), TypeTag::KV, "user/".as_bytes().to_vec());
        let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn scan_prefix_filters_tombstones() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k1"), Value::Int(1), 1);
        seed(&store, kv_key("k2"), Value::Int(2), 2);
        store.delete_with_version(&kv_key("k1"), 3).unwrap();

        let prefix = Key::new(ns(), TypeTag::KV, "k".as_bytes().to_vec());
        let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.value, Value::Int(2));
    }

    #[test]
    fn scan_prefix_mvcc_snapshot() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k1"), Value::Int(10), 1);
        seed(&store, kv_key("k1"), Value::Int(20), 3);
        seed(&store, kv_key("k2"), Value::Int(30), 2);

        let prefix = Key::new(ns(), TypeTag::KV, "k".as_bytes().to_vec());

        // Snapshot at 2: k1@1 and k2@2
        let results = store.scan_prefix(&prefix, 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1.value, Value::Int(10)); // k1@1
        assert_eq!(results[1].1.value, Value::Int(30)); // k2@2
    }

    #[test]
    fn current_version_tracks_writes() {
        let store = SegmentedStore::new();
        assert_eq!(store.current_version(), 0);
        seed(&store, kv_key("k"), Value::Int(1), 5);
        assert!(store.current_version() >= 5);
    }

    #[test]
    fn version_next_version_set_version() {
        let store = SegmentedStore::new();
        assert_eq!(store.version(), 0);
        assert_eq!(store.next_version(), 1);
        assert_eq!(store.version(), 1);
        store.set_version(100);
        assert_eq!(store.version(), 100);
    }

    #[test]
    fn branch_ids_and_clear() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        assert_eq!(store.branch_ids().len(), 1);
        assert!(store.clear_branch(&branch()));
        assert!(store.branch_ids().is_empty());
        assert!(!store.clear_branch(&branch())); // already cleared
    }

    #[test]
    fn branch_entry_count() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("a"), Value::Int(1), 1);
        seed(&store, kv_key("b"), Value::Int(2), 2);
        seed(&store, kv_key("a"), Value::Int(3), 3); // overwrites a
        assert_eq!(store.branch_entry_count(&branch()), 2); // a, b
    }

    #[test]
    fn list_branch_returns_live_entries() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("a"), Value::Int(1), 1);
        seed(&store, kv_key("b"), Value::Int(2), 2);
        store.delete_with_version(&kv_key("a"), 3).unwrap();

        let entries = store.list_branch(&branch());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.value, Value::Int(2));
    }

    #[test]
    fn multiple_branches_isolated() {
        let b1 = BranchId::from_bytes([1; 16]);
        let b2 = BranchId::from_bytes([2; 16]);
        let ns1 = Arc::new(Namespace::new(b1, "default".to_string()));
        let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));
        let k1 = Key::new(ns1, TypeTag::KV, "k".as_bytes().to_vec());
        let k2 = Key::new(ns2, TypeTag::KV, "k".as_bytes().to_vec());

        let store = SegmentedStore::new();
        store
            .put_with_version_mode(k1.clone(), Value::Int(1), 1, None, WriteMode::Append)
            .unwrap();
        store
            .put_with_version_mode(k2.clone(), Value::Int(2), 2, None, WriteMode::Append)
            .unwrap();

        assert_eq!(
            store.get_versioned(&k1, u64::MAX).unwrap().unwrap().value,
            Value::Int(1)
        );
        assert_eq!(
            store.get_versioned(&k2, u64::MAX).unwrap().unwrap().value,
            Value::Int(2)
        );
        assert_eq!(store.branch_ids().len(), 2);
    }

    #[test]
    fn ttl_expiration_at_read_time() {
        let store = SegmentedStore::new();
        // Insert with 1ms TTL using a timestamp from the past
        let branch_id = branch();
        let key = kv_key("ttl_key");

        let branch = store
            .branches
            .entry(branch_id)
            .or_insert_with(BranchState::new);

        let entry = MemtableEntry {
            value: Value::Int(42),
            is_tombstone: false,
            timestamp: Timestamp::from_micros(0), // ancient
            ttl_ms: 1,                            // 1ms TTL — definitely expired
        };
        branch.active.put_entry(&key, 1, entry);
        drop(branch);

        // Should be expired
        assert!(store.get_versioned(&key, u64::MAX).unwrap().is_none());
    }

    #[test]
    fn concurrent_readers_and_writer() {
        use std::sync::Arc;
        let store = Arc::new(SegmentedStore::new());

        // Seed some data
        for i in 0..100u64 {
            store
                .put_with_version_mode(
                    kv_key(&format!("k{}", i)),
                    Value::Int(i as i64),
                    i + 1,
                    None,
                    WriteMode::Append,
                )
                .unwrap();
        }

        // Spawn readers
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let s = Arc::clone(&store);
                std::thread::spawn(move || {
                    for i in 0..100u64 {
                        let result = s.get_versioned(&kv_key(&format!("k{}", i)), u64::MAX);
                        assert!(result.unwrap().is_some());
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    // ===== Missing coverage tests =====

    #[test]
    fn get_history_nonexistent_key() {
        let store = SegmentedStore::new();
        let history = store.get_history(&kv_key("nope"), None, None).unwrap();
        assert!(history.is_empty());
    }

    #[test]
    fn get_version_only_existing() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(1), 5);
        seed(&store, kv_key("k"), Value::Int(2), 10);
        assert_eq!(store.get_version_only(&kv_key("k")).unwrap(), Some(10));
    }

    #[test]
    fn get_version_only_nonexistent() {
        let store = SegmentedStore::new();
        assert_eq!(store.get_version_only(&kv_key("k")).unwrap(), None);
    }

    #[test]
    fn get_version_only_tombstoned() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.delete_with_version(&kv_key("k"), 2).unwrap();
        assert_eq!(store.get_version_only(&kv_key("k")).unwrap(), None);
    }

    #[test]
    fn delete_nonexistent_key() {
        let store = SegmentedStore::new();
        // Deleting a key that never existed should succeed (creates tombstone)
        store.delete_with_version(&kv_key("ghost"), 1).unwrap();
        assert!(store
            .get_versioned(&kv_key("ghost"), u64::MAX)
            .unwrap()
            .is_none());
    }

    #[test]
    fn scan_prefix_results_are_sorted() {
        let store = SegmentedStore::new();
        // Insert in reverse order to verify sorting
        seed(&store, kv_key("k3"), Value::Int(3), 3);
        seed(&store, kv_key("k1"), Value::Int(1), 1);
        seed(&store, kv_key("k2"), Value::Int(2), 2);

        let prefix = Key::new(ns(), TypeTag::KV, "k".as_bytes().to_vec());
        let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
        assert_eq!(results.len(), 3);
        // Must be sorted by key
        assert!(results[0].0 < results[1].0);
        assert!(results[1].0 < results[2].0);
    }

    #[test]
    fn put_with_ttl_via_public_api() {
        let store = SegmentedStore::new();
        store
            .put_with_version_mode(
                kv_key("ttl"),
                Value::Int(1),
                1,
                Some(Duration::from_secs(3600)), // 1 hour — should not expire
                WriteMode::Append,
            )
            .unwrap();
        // Should be readable (not expired yet)
        assert!(store
            .get_versioned(&kv_key("ttl"), u64::MAX)
            .unwrap()
            .is_some());
    }

    #[test]
    fn branch_entry_count_nonexistent_branch() {
        let store = SegmentedStore::new();
        assert_eq!(store.branch_entry_count(&BranchId::from_bytes([99; 16])), 0);
    }

    #[test]
    fn list_branch_nonexistent_branch() {
        let store = SegmentedStore::new();
        assert!(store
            .list_branch(&BranchId::from_bytes([99; 16]))
            .is_empty());
    }

    // ===== Flush pipeline tests =====

    #[test]
    fn flush_moves_data_to_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        for i in 1..=100u64 {
            seed(
                &store,
                kv_key(&format!("k{:04}", i)),
                Value::Int(i as i64),
                i,
            );
        }

        assert!(store.rotate_memtable(&branch()));
        assert!(store.has_frozen(&branch()));
        assert_eq!(store.branch_frozen_count(&branch()), 1);

        let flushed = store.flush_oldest_frozen(&branch()).unwrap();
        assert!(flushed);
        assert_eq!(store.branch_frozen_count(&branch()), 0);
        assert_eq!(store.branch_segment_count(&branch()), 1);

        for i in 1..=100u64 {
            let result = store
                .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(result.value, Value::Int(i as i64));
        }
    }

    #[test]
    fn flush_produces_valid_segment_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        seed(&store, kv_key("a"), Value::Int(1), 1);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        // Segments are now in a branch subdirectory
        let branch_hex = super::hex_encode_branch(&branch());
        let branch_dir = dir.path().join(&branch_hex);
        let sst_files: Vec<_> = std::fs::read_dir(&branch_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "sst"))
            .collect();
        assert!(!sst_files.is_empty());

        let seg = crate::segment::KVSegment::open(&sst_files[0].path()).unwrap();
        assert_eq!(seg.entry_count(), 1);
    }

    #[test]
    fn flush_empty_frozen_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        seed(&store, kv_key("a"), Value::Int(1), 1);
        let flushed = store.flush_oldest_frozen(&branch()).unwrap();
        assert!(!flushed);
    }

    #[test]
    fn flush_without_dir_is_noop() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("a"), Value::Int(1), 1);
        store.rotate_memtable(&branch());
        assert!(store.has_frozen(&branch()));

        let flushed = store.flush_oldest_frozen(&branch()).unwrap();
        assert!(!flushed);
    }

    #[test]
    fn rotation_triggers_at_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 1);

        seed(&store, kv_key("k"), Value::Int(42), 1);

        assert!(
            store.branch_frozen_count(&branch()) >= 1,
            "rotation should have triggered at 1-byte threshold"
        );
    }

    #[test]
    fn rotation_creates_fresh_active() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 1);

        seed(&store, kv_key("old"), Value::Int(1), 1);
        seed(&store, kv_key("new"), Value::Int(2), 2);

        assert_eq!(
            store
                .get_versioned(&kv_key("old"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(1),
        );
        assert_eq!(
            store
                .get_versioned(&kv_key("new"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(2),
        );
    }

    #[test]
    fn no_rotation_when_threshold_zero() {
        let store = SegmentedStore::new();

        for i in 1..=100u64 {
            seed(&store, kv_key(&format!("k{}", i)), Value::Int(i as i64), i);
        }

        assert_eq!(store.branch_frozen_count(&branch()), 0);
    }

    #[test]
    fn mvcc_correct_across_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        seed(&store, kv_key("k"), Value::Int(2), 2);

        assert_eq!(
            store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
            Value::Int(1),
        );
        assert_eq!(
            store.get_versioned(&kv_key("k"), 2).unwrap().unwrap().value,
            Value::Int(2),
        );
    }

    #[test]
    fn prefix_scan_spans_memtable_and_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        seed(&store, kv_key("item/a"), Value::Int(1), 1);
        seed(&store, kv_key("item/b"), Value::Int(2), 2);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        seed(&store, kv_key("item/c"), Value::Int(3), 3);
        seed(&store, kv_key("item/d"), Value::Int(4), 4);

        let prefix = Key::new(ns(), TypeTag::KV, "item/".as_bytes().to_vec());
        let results = store.scan_prefix(&prefix, u64::MAX).unwrap();
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn get_history_spans_memtable_and_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        seed(&store, kv_key("k"), Value::Int(1), 1);
        seed(&store, kv_key("k"), Value::Int(2), 2);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        seed(&store, kv_key("k"), Value::Int(3), 3);

        let history = store.get_history(&kv_key("k"), None, None).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].value, Value::Int(3));
        assert_eq!(history[1].value, Value::Int(2));
        assert_eq!(history[2].value, Value::Int(1));
    }

    #[test]
    fn multiple_flushes_produce_multiple_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        for cycle in 0..3u64 {
            let base = cycle * 10 + 1;
            for i in 0..10u64 {
                seed(
                    &store,
                    kv_key(&format!("c{}k{}", cycle, i)),
                    Value::Int((base + i) as i64),
                    base + i,
                );
            }
            store.rotate_memtable(&branch());
            store.flush_oldest_frozen(&branch()).unwrap();
        }

        assert_eq!(store.branch_segment_count(&branch()), 3);

        for cycle in 0..3u64 {
            let base = cycle * 10 + 1;
            for i in 0..10u64 {
                let result = store
                    .get_versioned(&kv_key(&format!("c{}k{}", cycle, i)), u64::MAX)
                    .unwrap()
                    .unwrap();
                assert_eq!(result.value, Value::Int((base + i) as i64));
            }
        }
    }

    #[test]
    fn newest_segment_wins_for_same_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        seed(&store, kv_key("k"), Value::Int(2), 2);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        assert_eq!(store.branch_segment_count(&branch()), 2);

        let result = store
            .get_versioned(&kv_key("k"), u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(result.value, Value::Int(2));
    }

    #[test]
    fn reads_dont_block_during_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SegmentedStore::with_dir(dir.path().to_path_buf(), 0));

        for i in 1..=50u64 {
            store
                .put_with_version_mode(
                    kv_key(&format!("k{:04}", i)),
                    Value::Int(i as i64),
                    i,
                    None,
                    WriteMode::Append,
                )
                .unwrap();
        }
        store.rotate_memtable(&branch());

        let store_reader = Arc::clone(&store);
        let reader = std::thread::spawn(move || {
            for i in 1..=50u64 {
                let result = store_reader
                    .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
                    .unwrap();
                assert!(result.is_some(), "key k{:04} should be readable", i);
            }
        });

        store.flush_oldest_frozen(&branch()).unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn rotate_nonexistent_branch_returns_false() {
        let store = SegmentedStore::new();
        assert!(!store.rotate_memtable(&branch()));
    }

    #[test]
    fn flush_nonexistent_branch_returns_false() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let flushed = store.flush_oldest_frozen(&branch()).unwrap();
        assert!(!flushed);
    }

    #[test]
    fn delete_across_flush_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        store.delete_with_version(&kv_key("k"), 2).unwrap();

        assert!(store.get_versioned(&kv_key("k"), 2).unwrap().is_none());
        assert_eq!(
            store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
            Value::Int(1),
        );
    }

    #[test]
    fn tombstone_survives_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        // Write value then delete, so the frozen memtable contains both
        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.delete_with_version(&kv_key("k"), 2).unwrap();
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        // Tombstone must survive the flush — key is deleted at snapshot 2
        assert!(store.get_versioned(&kv_key("k"), 2).unwrap().is_none());
        // Value is still visible at snapshot 1
        assert_eq!(
            store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
            Value::Int(1),
        );
        // History shows both versions from the segment
        let history = store.get_history(&kv_key("k"), None, None).unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].value, Value::Null); // tombstone at v2
        assert_eq!(history[1].value, Value::Int(1)); // value at v1
    }

    // ===== Recovery tests =====

    #[test]
    fn recover_segments_loads_flushed_data() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        for i in 1..=50u64 {
            seed(
                &store,
                kv_key(&format!("k{:04}", i)),
                Value::Int(i as i64),
                i,
            );
        }
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let info = store2.recover_segments().unwrap();
        assert_eq!(info.branches_recovered, 1);
        assert_eq!(info.segments_loaded, 1);
        assert_eq!(info.errors_skipped, 0);

        for i in 1..=50u64 {
            let result = store2
                .get_versioned(&kv_key(&format!("k{:04}", i)), u64::MAX)
                .unwrap()
                .unwrap();
            assert_eq!(result.value, Value::Int(i as i64));
        }
    }

    #[test]
    fn recover_segments_multiple_branches() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        let b1 = BranchId::from_bytes([1; 16]);
        let b2 = BranchId::from_bytes([2; 16]);
        let ns1 = Arc::new(Namespace::new(b1, "default".to_string()));
        let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));

        for i in 1..=10u64 {
            let key = Key::new(ns1.clone(), TypeTag::KV, format!("k{}", i).into_bytes());
            store
                .put_with_version_mode(key, Value::Int(i as i64), i, None, WriteMode::Append)
                .unwrap();
        }
        store.rotate_memtable(&b1);
        store.flush_oldest_frozen(&b1).unwrap();

        for i in 11..=20u64 {
            let key = Key::new(ns2.clone(), TypeTag::KV, format!("k{}", i).into_bytes());
            store
                .put_with_version_mode(key, Value::Int(i as i64), i, None, WriteMode::Append)
                .unwrap();
        }
        store.rotate_memtable(&b2);
        store.flush_oldest_frozen(&b2).unwrap();

        let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let info = store2.recover_segments().unwrap();
        assert_eq!(info.branches_recovered, 2);
        assert_eq!(info.segments_loaded, 2);

        let k1 = Key::new(ns1, TypeTag::KV, "k1".as_bytes().to_vec());
        let k11 = Key::new(ns2, TypeTag::KV, "k11".as_bytes().to_vec());
        assert!(store2.get_versioned(&k1, u64::MAX).unwrap().is_some());
        assert!(store2.get_versioned(&k11, u64::MAX).unwrap().is_some());
    }

    #[test]
    fn recover_segments_skips_corrupt_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        let branch_hex = super::hex_encode_branch(&branch());
        let corrupt_path = dir.path().join(&branch_hex).join("corrupt.sst");
        std::fs::write(&corrupt_path, b"not a valid segment").unwrap();

        let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let info = store2.recover_segments().unwrap();
        assert_eq!(info.segments_loaded, 1);
        assert_eq!(info.errors_skipped, 1);
        assert!(store2
            .get_versioned(&kv_key("k"), u64::MAX)
            .unwrap()
            .is_some());
    }

    #[test]
    fn recover_segments_empty_dir_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let info = store.recover_segments().unwrap();
        assert_eq!(
            info,
            super::RecoverSegmentsInfo {
                branches_recovered: 0,
                segments_loaded: 0,
                errors_skipped: 0
            }
        );
    }

    #[test]
    fn recover_segments_no_dir_is_noop() {
        let store = SegmentedStore::new();
        let info = store.recover_segments().unwrap();
        assert_eq!(info.branches_recovered, 0);
    }

    #[test]
    fn recover_segments_ordering() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        for cycle in 0..3u64 {
            let base = cycle * 10 + 1;
            for i in 0..5u64 {
                seed(
                    &store,
                    kv_key(&format!("c{}k{}", cycle, i)),
                    Value::Int((base + i) as i64),
                    base + i,
                );
            }
            store.rotate_memtable(&branch());
            store.flush_oldest_frozen(&branch()).unwrap();
        }

        let store2 = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let info = store2.recover_segments().unwrap();
        assert_eq!(info.segments_loaded, 3);

        let branch = store2.branches.get(&branch()).unwrap();
        assert!(branch.segments[0].commit_range().1 >= branch.segments[1].commit_range().1);
        assert!(branch.segments[1].commit_range().1 >= branch.segments[2].commit_range().1);
    }

    #[test]
    fn max_flushed_commit_returns_correct_value() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        assert_eq!(store.max_flushed_commit(&branch()), None);

        seed(&store, kv_key("k1"), Value::Int(1), 5);
        seed(&store, kv_key("k2"), Value::Int(2), 10);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();
        assert_eq!(store.max_flushed_commit(&branch()), Some(10));

        seed(&store, kv_key("k3"), Value::Int(3), 20);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();
        assert_eq!(store.max_flushed_commit(&branch()), Some(20));
    }

    #[test]
    fn flush_writes_to_branch_subdirectory() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);

        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.rotate_memtable(&branch());
        store.flush_oldest_frozen(&branch()).unwrap();

        let branch_hex = super::hex_encode_branch(&branch());
        let branch_dir = dir.path().join(&branch_hex);
        assert!(branch_dir.exists());

        let sst_files: Vec<_> = std::fs::read_dir(&branch_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|e| e.to_str()) == Some("sst"))
            .collect();
        assert_eq!(sst_files.len(), 1);
    }

    #[test]
    fn frozen_memtable_reads_correct_order() {
        let store = SegmentedStore::new();

        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.rotate_memtable(&branch());
        seed(&store, kv_key("k"), Value::Int(2), 2);
        store.rotate_memtable(&branch());
        seed(&store, kv_key("k"), Value::Int(3), 3);

        assert_eq!(store.branch_frozen_count(&branch()), 2);
        assert_eq!(
            store
                .get_versioned(&kv_key("k"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(3),
        );
        assert_eq!(
            store.get_versioned(&kv_key("k"), 2).unwrap().unwrap().value,
            Value::Int(2),
        );
        assert_eq!(
            store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
            Value::Int(1),
        );
    }

    // ===== Compaction tests =====

    #[test]
    fn compact_merges_two_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        seed(&store, kv_key("a"), Value::Int(1), 1);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        seed(&store, kv_key("b"), Value::Int(2), 2);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        assert_eq!(store.branch_segment_count(&b), 2);

        let result = store.compact_branch(&b, 0).unwrap().unwrap();
        assert_eq!(result.segments_merged, 2);
        assert_eq!(result.output_entries, 2);
        assert_eq!(result.entries_pruned, 0);
        assert_eq!(store.branch_segment_count(&b), 1);

        assert_eq!(
            store
                .get_versioned(&kv_key("a"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(1)
        );
        assert_eq!(
            store
                .get_versioned(&kv_key("b"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(2)
        );
    }

    #[test]
    fn compact_merges_overlapping_versions() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        seed(&store, kv_key("k"), Value::Int(2), 2);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        let result = store.compact_branch(&b, 0).unwrap().unwrap();
        assert_eq!(result.output_entries, 2);
        assert_eq!(result.entries_pruned, 0);

        assert_eq!(
            store
                .get_versioned(&kv_key("k"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(2)
        );
        assert_eq!(
            store.get_versioned(&kv_key("k"), 1).unwrap().unwrap().value,
            Value::Int(1)
        );
    }

    #[test]
    fn compact_prunes_old_versions() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        for commit in 1..=3u64 {
            seed(&store, kv_key("k"), Value::Int(commit as i64), commit);
            store.rotate_memtable(&b);
            store.flush_oldest_frozen(&b).unwrap();
        }
        assert_eq!(store.branch_segment_count(&b), 3);

        // floor=3: commit 3 (above floor) + commit 2 (newest below floor) survive, commit 1 pruned
        let result = store.compact_branch(&b, 3).unwrap().unwrap();
        assert_eq!(result.segments_merged, 3);
        assert_eq!(result.output_entries, 2);
        assert_eq!(result.entries_pruned, 1);

        // Verify the correct versions survived
        assert_eq!(
            store
                .get_versioned(&kv_key("k"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(3)
        );
        assert_eq!(
            store.get_versioned(&kv_key("k"), 2).unwrap().unwrap().value,
            Value::Int(2)
        );
        // Version 1 was pruned — reading at snapshot 1 should return nothing
        assert!(store.get_versioned(&kv_key("k"), 1).unwrap().is_none());
    }

    #[test]
    fn compact_removes_dead_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        store.delete_with_version(&kv_key("k"), 2).unwrap();
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        let result = store.compact_branch(&b, 5).unwrap().unwrap();
        assert_eq!(result.output_entries, 0);
        assert_eq!(result.entries_pruned, 2);
    }

    #[test]
    fn compact_preserves_tombstone_above_floor() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        store.delete_with_version(&kv_key("k"), 3).unwrap();
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        let result = store.compact_branch(&b, 2).unwrap().unwrap();
        assert_eq!(result.output_entries, 2);
        assert_eq!(result.entries_pruned, 0);
    }

    #[test]
    fn compact_noop_zero_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        assert!(store.compact_branch(&b, 0).unwrap().is_none());
    }

    #[test]
    fn compact_noop_one_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();
        assert_eq!(store.branch_segment_count(&b), 1);
        assert!(store.compact_branch(&b, 0).unwrap().is_none());
    }

    #[test]
    fn compact_noop_ephemeral() {
        let store = SegmentedStore::new();
        let b = branch();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        assert!(store.compact_branch(&b, 0).unwrap().is_none());
    }

    #[test]
    fn compact_deletes_old_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        for commit in 1..=3u64 {
            seed(&store, kv_key("k"), Value::Int(commit as i64), commit);
            store.rotate_memtable(&b);
            store.flush_oldest_frozen(&b).unwrap();
        }

        let branch_dir = dir.path().join(hex_encode_branch(&b));
        let files_before: Vec<_> = std::fs::read_dir(&branch_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
            .collect();
        assert_eq!(files_before.len(), 3);

        store.compact_branch(&b, 0).unwrap();

        let files_after: Vec<_> = std::fs::read_dir(&branch_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("sst"))
            .collect();
        assert_eq!(files_after.len(), 1);
    }

    #[test]
    fn compact_reads_correct_after() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        seed(&store, kv_key("a"), Value::Int(1), 1);
        seed(&store, kv_key("b"), Value::Int(10), 1);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        seed(&store, kv_key("a"), Value::Int(2), 2);
        seed(&store, kv_key("c"), Value::Int(20), 2);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        store.compact_branch(&b, 0).unwrap();

        assert_eq!(
            store
                .get_versioned(&kv_key("a"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(2)
        );
        assert_eq!(
            store
                .get_versioned(&kv_key("b"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(10)
        );
        assert_eq!(
            store
                .get_versioned(&kv_key("c"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(20)
        );

        let prefix_key = Key::new(ns(), TypeTag::KV, Vec::new());
        let results = store.scan_prefix(&prefix_key, u64::MAX).unwrap();
        assert_eq!(results.len(), 3);

        let history = store.get_history(&kv_key("a"), None, None).unwrap();
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn compact_result_counts() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        // Two keys, multiple versions across segments
        seed(&store, kv_key("a"), Value::Int(1), 1);
        seed(&store, kv_key("b"), Value::Int(10), 1);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        seed(&store, kv_key("a"), Value::Int(2), 2);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        seed(&store, kv_key("a"), Value::Int(3), 3);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        // 4 total entries: a@3, a@2, a@1, b@1
        // floor=3: a keeps 3 (above) + 2 (floor entry), prunes 1. b keeps 1 (floor entry).
        let result = store.compact_branch(&b, 3).unwrap().unwrap();
        assert_eq!(result.segments_merged, 3);
        assert_eq!(result.output_entries, 3); // a@3, a@2, b@1
        assert_eq!(result.entries_pruned, 1); // a@1

        // Verify reads are correct
        assert_eq!(
            store
                .get_versioned(&kv_key("a"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(3)
        );
        assert_eq!(
            store.get_versioned(&kv_key("a"), 2).unwrap().unwrap().value,
            Value::Int(2)
        );
        assert_eq!(
            store
                .get_versioned(&kv_key("b"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(10)
        );
    }

    // ===== Memory pressure tests =====

    #[test]
    fn total_memtable_bytes_empty() {
        let store = SegmentedStore::new();
        assert_eq!(store.total_memtable_bytes(), 0);
    }

    #[test]
    fn total_memtable_bytes_active_only() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        assert!(store.total_memtable_bytes() > 0);
    }

    #[test]
    fn total_memtable_bytes_includes_frozen() {
        let store = SegmentedStore::new();
        let b = branch();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        let before_rotate = store.total_memtable_bytes();
        store.rotate_memtable(&b);
        seed(&store, kv_key("k2"), Value::Int(2), 2);
        let after = store.total_memtable_bytes();
        assert!(after > before_rotate);
    }

    #[test]
    fn total_memtable_bytes_multiple_branches() {
        let store = SegmentedStore::new();
        let b1 = branch();
        let b2 = BranchId::from_bytes([2; 16]);
        let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));

        seed(&store, kv_key("a"), Value::Int(1), 1);
        let bytes1 = store.total_memtable_bytes();
        assert!(bytes1 > 0);

        seed(
            &store,
            Key::new(ns2, TypeTag::KV, b"x".to_vec()),
            Value::Int(2),
            2,
        );
        let bytes2 = store.total_memtable_bytes();
        assert!(bytes2 > bytes1);

        let b1_bytes = store.branches.get(&b1).unwrap().active.approx_bytes();
        let b2_bytes = store.branches.get(&b2).unwrap().active.approx_bytes();
        assert_eq!(bytes2, b1_bytes + b2_bytes);
    }

    #[test]
    fn pressure_level_with_disabled() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::String("x".repeat(10000)), 1);
        assert_eq!(store.pressure_level(), PressureLevel::Normal);
    }

    #[test]
    fn pressure_level_tracks_growth() {
        use crate::pressure::MemoryPressure;
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir_and_pressure(
            dir.path().to_path_buf(),
            0,
            MemoryPressure::new(1000, 0.7, 0.9),
        );

        assert_eq!(store.pressure_level(), PressureLevel::Normal);

        for i in 0..20u64 {
            seed(
                &store,
                kv_key(&format!("key_{}", i)),
                Value::String("x".repeat(30)),
                i + 1,
            );
        }
        let level = store.pressure_level();
        assert!(
            level >= PressureLevel::Warning,
            "expected at least Warning, got {:?}",
            level
        );
    }

    #[test]
    fn branches_needing_flush_prioritization() {
        let store = SegmentedStore::new();
        let b1 = branch();
        let b2 = BranchId::from_bytes([2; 16]);
        let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));

        seed(&store, kv_key("a"), Value::Int(1), 1);
        store.rotate_memtable(&b1);

        for i in 0..3u64 {
            seed(
                &store,
                Key::new(ns2.clone(), TypeTag::KV, format!("k{}", i).into_bytes()),
                Value::Int(i as i64),
                i + 1,
            );
            store.rotate_memtable(&b2);
        }

        let needing = store.branches_needing_flush();
        assert_eq!(needing.len(), 2);
        assert_eq!(needing[0], b2);
        assert_eq!(needing[1], b1);
    }

    #[test]
    fn should_compact_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        seed(&store, kv_key("a"), Value::Int(1), 1);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        assert!(!store.should_compact(&b, 2));

        seed(&store, kv_key("b"), Value::Int(2), 2);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        assert!(store.should_compact(&b, 2));
    }

    #[test]
    fn compact_after_flush_integration() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        for commit in 1..=3u64 {
            seed(
                &store,
                kv_key(&format!("k{}", commit)),
                Value::Int(commit as i64),
                commit,
            );
            store.rotate_memtable(&b);
            store.flush_oldest_frozen(&b).unwrap();
        }

        assert!(store.should_compact(&b, 3));

        store.compact_branch(&b, 0).unwrap();
        assert_eq!(store.branch_segment_count(&b), 1);
        assert!(!store.should_compact(&b, 2));

        for commit in 1..=3u64 {
            assert_eq!(
                store
                    .get_versioned(&kv_key(&format!("k{}", commit)), u64::MAX)
                    .unwrap()
                    .unwrap()
                    .value,
                Value::Int(commit as i64),
            );
        }
    }

    #[test]
    fn compact_with_active_memtable_data() {
        // Verify memtable data coexists correctly with compacted segments.
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        // Create 2 segments
        seed(&store, kv_key("a"), Value::Int(1), 1);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        seed(&store, kv_key("b"), Value::Int(2), 2);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        // Write to active memtable (NOT flushed)
        seed(&store, kv_key("c"), Value::Int(3), 3);
        // Update "a" in memtable (newer version than segment)
        seed(&store, kv_key("a"), Value::Int(10), 4);

        // Compact segments — memtable data must survive
        store.compact_branch(&b, 0).unwrap();
        assert_eq!(store.branch_segment_count(&b), 1);

        // Memtable data visible
        assert_eq!(
            store
                .get_versioned(&kv_key("c"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(3)
        );
        // Memtable update shadows segment version
        assert_eq!(
            store
                .get_versioned(&kv_key("a"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(10)
        );
        // Old segment version still readable at old snapshot
        assert_eq!(
            store.get_versioned(&kv_key("a"), 1).unwrap().unwrap().value,
            Value::Int(1)
        );
        // Segment-only data still readable
        assert_eq!(
            store
                .get_versioned(&kv_key("b"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(2)
        );
    }

    #[test]
    fn compact_concurrent_flush_preserves_new_segment() {
        // Simulate: compact snapshots 2 segments, then a flush adds a 3rd
        // segment before the swap. The 3rd segment must survive.
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 0);
        let b = branch();

        // Create 2 segments
        seed(&store, kv_key("a"), Value::Int(1), 1);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        seed(&store, kv_key("b"), Value::Int(2), 2);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        // Compact — this merges the 2 segments into 1
        store.compact_branch(&b, 0).unwrap();
        assert_eq!(store.branch_segment_count(&b), 1);

        // Now create 2 more segments and write a frozen memtable
        seed(&store, kv_key("c"), Value::Int(3), 3);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        seed(&store, kv_key("d"), Value::Int(4), 4);
        store.rotate_memtable(&b);
        store.flush_oldest_frozen(&b).unwrap();

        // Now we have 3 segments (1 from compaction + 2 new)
        assert_eq!(store.branch_segment_count(&b), 3);

        // Compact again
        store.compact_branch(&b, 0).unwrap();
        assert_eq!(store.branch_segment_count(&b), 1);

        // All data still readable
        assert_eq!(
            store
                .get_versioned(&kv_key("a"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(1)
        );
        assert_eq!(
            store
                .get_versioned(&kv_key("b"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(2)
        );
        assert_eq!(
            store
                .get_versioned(&kv_key("c"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(3)
        );
        assert_eq!(
            store
                .get_versioned(&kv_key("d"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(4)
        );
    }

    // ========================================================================
    // Batch apply tests (Epic 8b)
    // ========================================================================

    #[test]
    fn apply_batch_equivalent_to_individual() {
        let store = SegmentedStore::new();
        let b = branch();

        // Write some keys individually
        seed(&store, kv_key("a"), Value::Int(1), 1);
        seed(&store, kv_key("b"), Value::Int(2), 1);

        // Write same keys via apply_batch
        let store2 = SegmentedStore::new();
        let writes = vec![
            (kv_key("a"), Value::Int(1), WriteMode::Append),
            (kv_key("b"), Value::Int(2), WriteMode::Append),
        ];
        store2.apply_batch(writes, 1).unwrap();

        // Both stores should produce the same results
        for key_name in &["a", "b"] {
            let k = kv_key(key_name);
            let v1 = store.get_versioned(&k, u64::MAX).unwrap().unwrap().value;
            let v2 = store2.get_versioned(&k, u64::MAX).unwrap().unwrap().value;
            assert_eq!(v1, v2);
        }
    }

    #[test]
    fn apply_batch_cross_branch() {
        let store = SegmentedStore::new();
        let b1 = BranchId::from_bytes([1; 16]);
        let b2 = BranchId::from_bytes([2; 16]);
        let ns1 = Arc::new(Namespace::new(b1, "default".to_string()));
        let ns2 = Arc::new(Namespace::new(b2, "default".to_string()));

        let k1 = Key::new(ns1, TypeTag::KV, b"x".to_vec());
        let k2 = Key::new(ns2, TypeTag::KV, b"y".to_vec());

        let writes = vec![
            (k1.clone(), Value::Int(10), WriteMode::Append),
            (k2.clone(), Value::Int(20), WriteMode::Append),
        ];
        store.apply_batch(writes, 5).unwrap();

        assert_eq!(
            store.get_versioned(&k1, u64::MAX).unwrap().unwrap().value,
            Value::Int(10)
        );
        assert_eq!(
            store.get_versioned(&k2, u64::MAX).unwrap().unwrap().value,
            Value::Int(20)
        );
    }

    #[test]
    fn apply_batch_with_deletes() {
        let store = SegmentedStore::new();
        let b = branch();

        // Write first
        seed(&store, kv_key("a"), Value::Int(1), 1);
        seed(&store, kv_key("b"), Value::Int(2), 1);

        // Delete via batch
        let deletes = vec![kv_key("a")];
        store.delete_batch(deletes, 2).unwrap();

        assert!(store
            .get_versioned(&kv_key("a"), u64::MAX)
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .get_versioned(&kv_key("b"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(2)
        );
    }

    #[test]
    fn apply_batch_empty() {
        let store = SegmentedStore::new();
        store.apply_batch(vec![], 1).unwrap();
        store.delete_batch(vec![], 1).unwrap();
        assert_eq!(store.current_version(), 0);
    }

    // ========================================================================
    // Bulk load mode tests (Epic 8d)
    // ========================================================================

    #[test]
    fn bulk_load_data_readable() {
        let store = SegmentedStore::new();
        let b = branch();

        store.begin_bulk_load(&b);
        seed(&store, kv_key("a"), Value::Int(1), 1);
        seed(&store, kv_key("b"), Value::Int(2), 2);

        // Data should be readable even during bulk load
        assert_eq!(
            store
                .get_versioned(&kv_key("a"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(1)
        );

        store.end_bulk_load(&b).unwrap();

        assert_eq!(
            store
                .get_versioned(&kv_key("b"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(2)
        );
    }

    #[test]
    fn bulk_load_defers_rotation() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny write buffer (64 bytes) — normally would rotate after 1 entry
        let store = SegmentedStore::with_dir(dir.path().to_path_buf(), 64);
        let b = branch();

        store.begin_bulk_load(&b);
        assert!(store.is_bulk_loading(&b));

        // Write many entries — should NOT rotate during bulk load
        for i in 0..100 {
            seed(
                &store,
                kv_key(&format!("k{}", i)),
                Value::Int(i),
                (i + 1) as u64,
            );
        }

        // No frozen memtables — everything in active
        assert_eq!(store.branch_frozen_count(&b), 0);

        store.end_bulk_load(&b).unwrap();
        assert!(!store.is_bulk_loading(&b));

        // After end_bulk_load, data is still readable
        for i in 0..100 {
            assert!(store
                .get_versioned(&kv_key(&format!("k{}", i)), u64::MAX)
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn bulk_load_normal_writes_after() {
        let store = SegmentedStore::new();
        let b = branch();

        store.begin_bulk_load(&b);
        seed(&store, kv_key("bulk"), Value::Int(1), 1);
        store.end_bulk_load(&b).unwrap();

        // Normal writes should work after bulk load
        seed(&store, kv_key("normal"), Value::Int(2), 2);

        assert_eq!(
            store
                .get_versioned(&kv_key("bulk"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(1)
        );
        assert_eq!(
            store
                .get_versioned(&kv_key("normal"), u64::MAX)
                .unwrap()
                .unwrap()
                .value,
            Value::Int(2)
        );
    }

    // ===== Engine-facing API tests =====

    #[test]
    fn get_value_direct_returns_latest() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(10), 1);
        seed(&store, kv_key("k"), Value::Int(20), 2);
        assert_eq!(store.get_value_direct(&kv_key("k")), Some(Value::Int(20)));
    }

    #[test]
    fn get_value_direct_skips_tombstone() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(10), 1);
        store.delete_with_version(&kv_key("k"), 2).unwrap();
        assert_eq!(store.get_value_direct(&kv_key("k")), None);
    }

    #[test]
    fn get_value_direct_nonexistent() {
        let store = SegmentedStore::new();
        assert_eq!(store.get_value_direct(&kv_key("k")), None);
    }

    #[test]
    fn list_by_type_filters_correctly() {
        let store = SegmentedStore::new();
        let b = branch();
        seed(&store, kv_key("k1"), Value::Int(1), 1);
        let state_key = Key::new(ns(), TypeTag::State, "s1".as_bytes().to_vec());
        seed(&store, state_key, Value::Int(2), 2);

        let kv_entries = store.list_by_type(&b, TypeTag::KV);
        assert_eq!(kv_entries.len(), 1);
        assert_eq!(kv_entries[0].1.value, Value::Int(1));

        let state_entries = store.list_by_type(&b, TypeTag::State);
        assert_eq!(state_entries.len(), 1);
        assert_eq!(state_entries[0].1.value, Value::Int(2));
    }

    #[test]
    fn get_at_timestamp_sees_old_version() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(10), 1);
        // Record timestamp after first write
        let ts_after_v1 = Timestamp::now().as_micros();
        // Small sleep to ensure timestamp ordering
        std::thread::sleep(std::time::Duration::from_millis(5));
        seed(&store, kv_key("k"), Value::Int(20), 2);

        // Query at ts_after_v1 should see v1 (Int(10))
        let result = store.get_at_timestamp(&kv_key("k"), ts_after_v1).unwrap();
        assert!(
            result.is_some(),
            "should find version at snapshot timestamp"
        );
        assert_eq!(result.unwrap().value, Value::Int(10));

        // Query at current time should see v2 (Int(20))
        let result_now = store
            .get_at_timestamp(&kv_key("k"), Timestamp::now().as_micros())
            .unwrap();
        assert_eq!(result_now.unwrap().value, Value::Int(20));
    }

    #[test]
    fn get_at_timestamp_nonexistent_branch() {
        let store = SegmentedStore::new();
        let result = store.get_at_timestamp(&kv_key("k"), u64::MAX).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn get_at_timestamp_respects_tombstone() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(10), 1);
        std::thread::sleep(std::time::Duration::from_millis(5));
        store.delete_with_version(&kv_key("k"), 2).unwrap();

        // Query at current time should return None (tombstone)
        let result = store
            .get_at_timestamp(&kv_key("k"), Timestamp::now().as_micros())
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn scan_prefix_at_timestamp_filters() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("user:1"), Value::Int(1), 1);
        let ts_after = Timestamp::now().as_micros();
        std::thread::sleep(std::time::Duration::from_millis(5));
        seed(&store, kv_key("user:2"), Value::Int(2), 2);

        // At ts_after, only user:1 should be visible
        let prefix = kv_key("user:");
        let results = store.scan_prefix_at_timestamp(&prefix, ts_after).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.value, Value::Int(1));
    }

    #[test]
    fn time_range_returns_min_max() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("a"), Value::Int(1), 1);
        std::thread::sleep(std::time::Duration::from_millis(5));
        seed(&store, kv_key("b"), Value::Int(2), 2);

        let range = store.time_range(branch()).unwrap();
        assert!(range.is_some());
        let (min_ts, max_ts) = range.unwrap();
        assert!(min_ts <= max_ts);
        assert!(min_ts > 0);
    }

    #[test]
    fn time_range_empty_branch() {
        let store = SegmentedStore::new();
        assert!(store.time_range(branch()).unwrap().is_none());
    }

    #[test]
    fn gc_branch_is_noop() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        assert_eq!(store.gc_branch(&branch(), 100), 0);
    }

    #[test]
    fn set_max_branches_stores_value() {
        let store = SegmentedStore::new();
        store.set_max_branches(42);
        assert_eq!(store.max_branches.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn memory_stats_basic() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k1"), Value::Int(1), 1);
        seed(&store, kv_key("k2"), Value::Int(2), 2);

        let stats = store.memory_stats();
        assert_eq!(stats.total_branches, 1);
        assert_eq!(stats.total_entries, 2);
        assert!(stats.estimated_bytes > 0);
        assert_eq!(stats.per_branch.len(), 1);
        assert_eq!(stats.per_branch[0].entry_count, 2);
    }

    #[test]
    fn shard_stats_detailed_counts() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(1), 1);
        seed(&store, kv_key("k"), Value::Int(2), 2);

        let (entries, versions, btree) = store.shard_stats_detailed(&branch()).unwrap();
        assert_eq!(entries, 1, "1 logical key");
        assert_eq!(versions, 2, "2 versions of that key");
        assert!(!btree);
    }

    #[test]
    fn shard_stats_detailed_missing_branch() {
        let store = SegmentedStore::new();
        assert!(store.shard_stats_detailed(&BranchId::new()).is_none());
    }
}
