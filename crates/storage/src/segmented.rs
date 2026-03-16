//! Segmented storage — memtable + immutable segments with MVCC.
//!
//! `SegmentedStore` is a `Storage` trait implementation that combines:
//! - Per-branch active memtable (writable, lock-free SkipMap)
//! - Per-branch frozen memtables (immutable, pending flush)
//! - Per-branch KV segments (on-disk, mmap'd)
//!
//! Read path: active → frozen (newest first) → segments (newest first).
//! The first match for a key at commit_id ≤ snapshot wins.

use crate::key_encoding::{encode_typed_key, InternalKey};
use crate::memtable::{Memtable, MemtableEntry};
use crate::merge_iter::{MergeIterator, MvccIterator};
use crate::segment::{KVSegment, SegmentEntry};
use crate::segment_builder::SegmentBuilder;

use dashmap::DashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use strata_core::traits::{Storage, WriteMode};
use strata_core::types::{BranchId, Key};
use strata_core::value::Value;
use strata_core::{StrataResult, Timestamp, VersionedValue};

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
        }
    }

    // ========================================================================
    // Inherent methods (match ShardedStore API for test compatibility)
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
        let seg_path = segments_dir.join(format!("{}.sst", seg_id));

        std::fs::create_dir_all(segments_dir)?;
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
        self.branches
            .get(branch_id)
            .map_or(0, |b| b.frozen.len())
    }

    /// Number of on-disk segments for a branch (test helper).
    pub fn branch_segment_count(&self, branch_id: &BranchId) -> usize {
        self.branches
            .get(branch_id)
            .map_or(0, |b| b.segments.len())
    }

    /// Check whether the active memtable should be rotated and do so inline.
    ///
    /// Called after every write within the DashMap entry guard.
    #[inline]
    fn maybe_rotate(&self, branch: &mut BranchState) {
        if self.write_buffer_size > 0
            && branch.active.approx_bytes() >= self.write_buffer_size
        {
            let next_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
            let old = std::mem::replace(&mut branch.active, Memtable::new(next_id));
            old.freeze();
            branch.frozen.insert(0, Arc::new(old));
        }
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
    fn get_all_versions_from_branch(
        branch: &BranchState,
        key: &Key,
    ) -> Vec<(u64, MemtableEntry)> {
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
        self.maybe_rotate(&mut branch);

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
        self.maybe_rotate(&mut branch);

        // Update global version
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
        let result = store.get_versioned(&kv_key("k"), u64::MAX).unwrap().unwrap();
        assert_eq!(result.value, Value::Int(42));
        assert_eq!(result.version.as_u64(), 1);
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let store = SegmentedStore::new();
        assert!(store.get_versioned(&kv_key("k"), u64::MAX).unwrap().is_none());
    }

    #[test]
    fn delete_creates_tombstone() {
        let store = SegmentedStore::new();
        seed(&store, kv_key("k"), Value::Int(42), 1);
        store.delete_with_version(&kv_key("k"), 2).unwrap();
        assert!(store.get_versioned(&kv_key("k"), u64::MAX).unwrap().is_none());
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
            ttl_ms: 1, // 1ms TTL — definitely expired
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
                        let result =
                            s.get_versioned(&kv_key(&format!("k{}", i)), u64::MAX);
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
            seed(&store, kv_key(&format!("k{:04}", i)), Value::Int(i as i64), i);
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

        let sst_files: Vec<_> = std::fs::read_dir(dir.path())
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
            store.get_versioned(&kv_key("old"), u64::MAX).unwrap().unwrap().value,
            Value::Int(1),
        );
        assert_eq!(
            store.get_versioned(&kv_key("new"), u64::MAX).unwrap().unwrap().value,
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
            store.get_versioned(&kv_key("k"), u64::MAX).unwrap().unwrap().value,
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
}
