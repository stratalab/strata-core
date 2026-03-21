//! Segmented storage — memtable + immutable segments with MVCC.
//!
//! `SegmentedStore` is a `Storage` trait implementation that combines:
//! - Per-branch active memtable (writable, lock-free SkipMap)
//! - Per-branch frozen memtables (immutable, pending flush)
//! - Per-branch KV segments (on-disk, pread + block cache)
//!
//! Read path: active → frozen (newest first) → segments (newest first).
//! The first match for a key at commit_id ≤ snapshot wins.

use crate::compaction::CompactionIterator;
use crate::key_encoding::{encode_typed_key, encode_typed_key_prefix, InternalKey};
use crate::memory_stats::{BranchMemoryStats, StorageMemoryStats};
use crate::memtable::{Memtable, MemtableEntry};
use crate::merge_iter::{MergeIterator, MvccIterator};
use crate::pressure::{MemoryPressure, PressureLevel};
use crate::segment::{KVSegment, SegmentEntry};
use crate::segment_builder::SegmentBuilder;

use arc_swap::ArcSwap;
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
// Level configuration constants
// ---------------------------------------------------------------------------

/// Total number of levels (L0 through L6).
const NUM_LEVELS: usize = 7;

/// Number of L0 segments that triggers L0→L1 compaction.
const L0_COMPACTION_TRIGGER: usize = 4;

/// Target size for a single output segment file (64MB).
const TARGET_FILE_SIZE: u64 = 64 << 20;

/// Target total size for L1 (256MB).  L_n = LEVEL_BASE_BYTES × LEVEL_MULTIPLIER^(n-1).
const LEVEL_BASE_BYTES: u64 = 256 << 20;

/// Size multiplier between adjacent levels.
const LEVEL_MULTIPLIER: u64 = 10;

/// Maximum cumulative overlap with grandparent (L+2) files before forcing an
/// output split.  Set to 10 × TARGET_FILE_SIZE (640MB).
const MAX_GRANDPARENT_OVERLAP: u64 = 10 * TARGET_FILE_SIZE;

/// Minimum L1 target size for dynamic level sizing (1MB).
/// Prevents degenerate zero-size targets on tiny embedded datasets.
const MIN_BASE_BYTES: u64 = 1 << 20;

/// Maximum L1 target size for dynamic level sizing.
/// Equals LEVEL_BASE_BYTES (256MB) — the static default.
const MAX_BASE_BYTES: u64 = LEVEL_BASE_BYTES;

/// Monkey-optimal bloom bits per key for a given target level.
///
/// Upper levels (checked on every read) get more bits for near-zero FPR.
/// Lower levels (checked rarely due to upper-level rejections) keep baseline.
/// Formula: bits[l] = base + floor(log2(multiplier) * (max_level - 1 - level))
/// With multiplier=10: log2(10) ≈ 3.3
pub(crate) fn bloom_bits_for_level(level: usize, base_bits: usize) -> usize {
    if level == 0 {
        return base_bits; // L0 uses baseline (flushed from memtable, short-lived)
    }
    let max_level = NUM_LEVELS - 1; // 6
    let bonus = (3.3 * (max_level.saturating_sub(level).saturating_sub(1)) as f64) as usize;
    (base_bits + bonus).min(20) // Cap at 20 bits/key
}

/// Per-level compression strategy.
///
/// - L0–L2 (hot): No compression — eliminates decompression CPU on the read hot path.
/// - L3–L5 (warm): Zstd level 3 — balanced compression ratio vs speed.
/// - L6+ (cold): Zstd level 6 — better compression for rarely-read bottommost data.
pub(crate) fn compression_for_level(level: usize) -> crate::segment_builder::CompressionCodec {
    use crate::segment_builder::CompressionCodec;
    match level {
        0..=2 => CompressionCodec::None,
        3..=5 => CompressionCodec::Zstd(3),
        _ => CompressionCodec::Zstd(6),
    }
}

// ---------------------------------------------------------------------------
// Compaction scheduling types
// ---------------------------------------------------------------------------

/// Compaction score for a single level.
#[derive(Debug, Clone)]
pub struct CompactionScore {
    /// Level index (0–6).
    pub level: usize,
    /// Ratio of actual size to target.  >= 1.0 means compaction is warranted.
    pub score: f64,
}

/// Result of [`SegmentedStore::pick_and_compact`].
#[derive(Debug, Clone)]
pub struct PickAndCompactResult {
    /// The level that was compacted.
    pub level: usize,
    /// Compaction statistics.
    pub compaction: CompactionResult,
}

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
// SegmentVersion
// ---------------------------------------------------------------------------

/// Immutable snapshot of a branch's on-disk segments.
///
/// Wrapped in `ArcSwap` so that segment list mutations (flush, compaction)
/// are atomic single-pointer swaps — readers never see a partial state.
struct SegmentVersion {
    /// Per-level segment lists.
    /// - `levels[0]` (L0): overlapping, newest first.
    /// - `levels[1..=6]` (L1–L6): non-overlapping, sorted by key range.
    levels: Vec<Vec<Arc<KVSegment>>>,
}

impl SegmentVersion {
    fn new() -> Self {
        Self {
            levels: vec![Vec::new(); NUM_LEVELS],
        }
    }

    /// L0 segments accessor (overlapping, newest first).
    fn l0_segments(&self) -> &[Arc<KVSegment>] {
        &self.levels[0]
    }

    /// L1 segments accessor (non-overlapping, sorted by key range).
    fn l1_segments(&self) -> &[Arc<KVSegment>] {
        &self.levels[1]
    }

    /// Iterate over all segments across all levels.
    #[allow(dead_code)]
    fn all_segments(&self) -> impl Iterator<Item = &Arc<KVSegment>> {
        self.levels.iter().flat_map(|level| level.iter())
    }

    /// Total number of segments across all levels.
    fn total_segment_count(&self) -> usize {
        self.levels.iter().map(|l| l.len()).sum()
    }
}

// ---------------------------------------------------------------------------
// COW Branching — inherited layer types
// ---------------------------------------------------------------------------

/// Status of an inherited layer during materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Used in Epic C (COW fork)
enum LayerStatus {
    /// Layer is active — reads fall through to parent segments.
    Active,
    /// Materialization in progress (reset to Active on crash recovery).
    Materializing,
    /// Layer has been fully materialized into own segments.
    Materialized,
}

/// An inherited layer from a parent branch (COW fork).
///
/// When a branch is forked, instead of deep-copying every key-value pair,
/// the child holds an `Arc<SegmentVersion>` pointing at the parent's
/// segments at fork time. Reads fall through to these inherited segments
/// until materialization copies them into the child's own segments.
struct InheritedLayer {
    /// Branch ID of the source (parent) branch.
    source_branch_id: BranchId,
    /// Version counter of the source branch at fork time.
    fork_version: u64,
    /// Snapshot of the parent's segments at fork time.
    segments: Arc<SegmentVersion>,
    /// Current materialization status.
    status: LayerStatus,
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
    /// On-disk segment version (atomic swap for lock-free reads).
    version: ArcSwap<SegmentVersion>,
    /// Minimum timestamp seen across all writes (for O(1) time_range).
    min_timestamp: AtomicU64,
    /// Maximum timestamp seen across all writes (for O(1) time_range).
    max_timestamp: AtomicU64,
    /// Per-level compact pointer for round-robin file selection.
    /// Each entry is the largest typed_key_prefix of the last compaction input.
    compact_pointers: Vec<Option<Vec<u8>>>,
    /// Cached per-level byte targets, recomputed after compaction/flush.
    level_targets: compaction::LevelTargets,
    /// Inherited layers from parent branches (COW fork).
    /// Empty until Epic C enables fork_branch.
    inherited_layers: Vec<InheritedLayer>,
}

impl BranchState {
    fn new() -> Self {
        Self {
            active: Memtable::new(0),
            frozen: Vec::new(),
            version: ArcSwap::from_pointee(SegmentVersion::new()),
            min_timestamp: AtomicU64::new(u64::MAX),
            max_timestamp: AtomicU64::new(0),
            compact_pointers: vec![None; NUM_LEVELS],
            level_targets: compaction::recalculate_level_targets(&[0u64; NUM_LEVELS]),
            inherited_layers: Vec::new(),
        }
    }
}

/// Recompute cached level targets from the current segment version.
fn refresh_level_targets(branch: &mut BranchState) {
    let ver = branch.version.load();
    let mut actual_bytes = [0u64; NUM_LEVELS];
    for (level, segs) in ver.levels.iter().enumerate() {
        actual_bytes[level] = segs.iter().map(|s| s.file_size()).sum();
    }
    branch.level_targets = compaction::recalculate_level_targets(&actual_bytes);
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
    /// Total frozen memtable count across all branches (for O(1) "any frozen?" check).
    total_frozen_count: AtomicUsize,
    /// Maximum frozen memtables per branch before rotation is skipped (0 = unlimited).
    max_immutable_memtables: AtomicUsize,
    /// Optional rate limiter for compaction I/O (read + write).
    compaction_rate_limiter: arc_swap::ArcSwapOption<crate::rate_limiter::RateLimiter>,
    /// Reference count registry for shared segments (COW branching).
    ref_registry: ref_registry::SegmentRefRegistry,
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
            total_frozen_count: AtomicUsize::new(0),
            max_immutable_memtables: AtomicUsize::new(0),
            compaction_rate_limiter: arc_swap::ArcSwapOption::empty(),
            ref_registry: ref_registry::SegmentRefRegistry::new(),
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
            total_frozen_count: AtomicUsize::new(0),
            max_immutable_memtables: AtomicUsize::new(0),
            compaction_rate_limiter: arc_swap::ArcSwapOption::empty(),
            ref_registry: ref_registry::SegmentRefRegistry::new(),
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
            total_frozen_count: AtomicUsize::new(0),
            max_immutable_memtables: AtomicUsize::new(0),
            compaction_rate_limiter: arc_swap::ArcSwapOption::empty(),
            ref_registry: ref_registry::SegmentRefRegistry::new(),
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

    /// Set (or clear) the compaction I/O rate limit.
    ///
    /// When `bytes_per_sec > 0`, compaction reads and writes are throttled to
    /// at most that rate. When `bytes_per_sec == 0`, the rate limiter is
    /// removed and compaction runs at full speed (the default).
    pub fn set_compaction_rate_limit(&self, bytes_per_sec: u64) {
        if bytes_per_sec == 0 {
            self.compaction_rate_limiter.store(None);
        } else {
            self.compaction_rate_limiter.store(Some(std::sync::Arc::new(
                crate::rate_limiter::RateLimiter::new(bytes_per_sec),
            )));
        }
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

        // On-disk segments — all levels
        let ver = branch.version.load();
        for level in &ver.levels {
            for seg in level {
                let entries: Vec<_> = seg
                    .iter_seek_all()
                    .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                    .collect();
                sources.push(Box::new(entries.into_iter()));
            }
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

    /// Set maximum frozen memtables per branch before write stalling (0 = unlimited).
    ///
    /// When the limit is reached, `maybe_rotate_branch` skips rotation and lets
    /// the active memtable grow beyond `write_buffer_size`. Once a frozen
    /// memtable is flushed, rotation resumes on the next write.
    pub fn set_max_immutable_memtables(&self, max: usize) {
        self.max_immutable_memtables.store(max, Ordering::Relaxed);
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
    /// **Note:** v2 segments store timestamps; v1 segments default to timestamp=0.
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
        let ver = branch.version.load();
        for level in &ver.levels {
            for seg in level {
                let entries: Vec<_> = seg
                    .iter_seek_all()
                    .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                    .collect();
                sources.push(Box::new(entries.into_iter()));
            }
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
    /// **Note:** v2 segments store timestamps; v1 segments default to timestamp=0.
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
    /// **Note:** v2 segments store timestamps; v1 segments default to timestamp=0.
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
        let prefix_bytes = encode_typed_key_prefix(prefix);
        let ver = branch.version.load();
        for level in &ver.levels {
            for seg in level {
                let (seg_min, seg_max) = seg.key_range();
                if seg_max.len() >= 8 && seg_min.len() >= 8 {
                    let max_typed = &seg_max[..seg_max.len() - 8];
                    let min_typed = &seg_min[..seg_min.len() - 8];
                    if max_typed < prefix_bytes.as_slice() {
                        continue;
                    }
                    if !min_typed.starts_with(&prefix_bytes) && min_typed > prefix_bytes.as_slice()
                    {
                        continue;
                    }
                }
                let entries: Vec<_> = seg
                    .iter_seek(prefix)
                    .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                    .collect();
                sources.push(Box::new(entries.into_iter()));
            }
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
    ///
    /// O(1) when timestamps have been tracked (normal write path).
    /// Falls back to scanning live entries after recovery from segments
    /// (where WAL was truncated and timestamps weren't replayed).
    ///
    /// The O(1) fast path includes timestamps from all writes (puts,
    /// deletes, tombstones). The fallback path only scans live entries.
    pub fn time_range(&self, branch_id: BranchId) -> StrataResult<Option<(u64, u64)>> {
        let branch = match self.branches.get(&branch_id) {
            Some(b) => b,
            None => return Ok(None),
        };
        let min_ts = branch.min_timestamp.load(Ordering::Relaxed);
        let max_ts = branch.max_timestamp.load(Ordering::Relaxed);
        if min_ts != u64::MAX {
            return Ok(Some((min_ts, max_ts)));
        }

        // Fallback: atomics not populated (e.g. after recovery from segments
        // where WAL was truncated). Scan entries to find actual range.
        let entries = self.list_branch_inner(&branch);
        if entries.is_empty() {
            return Ok(None);
        }
        let mut scan_min = u64::MAX;
        let mut scan_max = 0u64;
        for (_, vv) in &entries {
            let ts = vv.timestamp.as_micros();
            scan_min = scan_min.min(ts);
            scan_max = scan_max.max(ts);
        }
        // Populate atomics so subsequent calls are O(1)
        branch.min_timestamp.fetch_min(scan_min, Ordering::Relaxed);
        branch.max_timestamp.fetch_max(scan_max, Ordering::Relaxed);
        Ok(Some((scan_min, scan_max)))
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

    /// Apply a put during WAL recovery, preserving the original commit timestamp (#1619).
    ///
    /// Normal writes use `Timestamp::now()`, but recovery must use the timestamp
    /// from the WAL record to maintain correct temporal ordering. Without this,
    /// time-travel queries return wrong results after crash recovery.
    pub fn put_recovery_entry(
        &self,
        key: Key,
        value: Value,
        version: u64,
        timestamp_micros: u64,
    ) -> StrataResult<()> {
        let branch_id = key.namespace.branch_id;

        let mut branch = self
            .branches
            .entry(branch_id)
            .or_insert_with(BranchState::new);

        let entry = MemtableEntry {
            value,
            is_tombstone: false,
            timestamp: Timestamp::from_micros(timestamp_micros),
            ttl_ms: 0,
        };
        let ts = entry.timestamp.as_micros();
        branch.active.put_entry(&key, version, entry);
        branch.min_timestamp.fetch_min(ts, Ordering::Relaxed);
        branch.max_timestamp.fetch_max(ts, Ordering::Relaxed);

        self.maybe_rotate_branch(branch_id, &mut branch);
        self.version.fetch_max(version, Ordering::AcqRel);

        Ok(())
    }

    /// Apply a delete during WAL recovery, preserving the original commit timestamp (#1619).
    pub fn delete_recovery_entry(
        &self,
        key: &Key,
        version: u64,
        timestamp_micros: u64,
    ) -> StrataResult<()> {
        let branch_id = key.namespace.branch_id;

        let mut branch = self
            .branches
            .entry(branch_id)
            .or_insert_with(BranchState::new);

        let entry = MemtableEntry {
            value: Value::Null,
            is_tombstone: true,
            timestamp: Timestamp::from_micros(timestamp_micros),
            ttl_ms: 0,
        };
        let ts = entry.timestamp.as_micros();
        branch.active.put_entry(key, version, entry);
        branch.min_timestamp.fetch_min(ts, Ordering::Relaxed);
        branch.max_timestamp.fetch_max(ts, Ordering::Relaxed);

        self.maybe_rotate_branch(branch_id, &mut branch);
        self.version.fetch_max(version, Ordering::AcqRel);

        Ok(())
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
        let ver = branch.version.load();
        for level in &ver.levels {
            for seg in level {
                total_versions += seg.entry_count() as usize;
            }
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
        self.total_frozen_count.fetch_add(1, Ordering::Relaxed);
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
        let builder = SegmentBuilder::default()
            .with_compression(crate::segment_builder::CompressionCodec::None);
        builder.build_from_iter(frozen_mt.iter_all(), &seg_path)?;

        // Open the newly written segment and pin its bloom partitions (L0).
        let segment = KVSegment::open(&seg_path)?;
        segment.pin_bloom_partitions();

        // Atomically: remove the frozen memtable we just flushed and install
        // the segment, under a single DashMap guard.
        let mut branch = self
            .branches
            .entry(*branch_id)
            .or_insert_with(BranchState::new);
        if let Some(last) = branch.frozen.last() {
            if last.id() == frozen_mt.id() {
                branch.frozen.pop();
                self.total_frozen_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
        // Build new version with the new segment prepended (newest first).
        let old_ver = branch.version.load();
        let mut new_l0 = Vec::with_capacity(old_ver.l0_segments().len() + 1);
        new_l0.push(Arc::new(segment));
        new_l0.extend(old_ver.l0_segments().iter().cloned());
        let mut new_levels = old_ver.levels.clone();
        new_levels[0] = new_l0;
        branch
            .version
            .store(Arc::new(SegmentVersion { levels: new_levels }));
        refresh_level_targets(&mut branch);

        // Persist level assignments
        drop(branch);
        self.write_branch_manifest(branch_id);

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
        self.branches
            .get(branch_id)
            .map_or(0, |b| b.version.load().total_segment_count())
    }

    /// Number of L0 segments for a branch.
    ///
    /// Used by compaction triggers to decide when to compact L0 → L1.
    /// Maximum L0 segment count across all branches.
    pub fn max_l0_segment_count(&self) -> usize {
        self.branches
            .iter()
            .map(|b| b.version.load().l0_segments().len())
            .max()
            .unwrap_or(0)
    }

    /// Number of L0 segments for a branch.
    pub fn l0_segment_count(&self, branch_id: &BranchId) -> usize {
        self.branches
            .get(branch_id)
            .map_or(0, |b| b.version.load().l0_segments().len())
    }

    /// Number of L1 segments for a branch.
    pub fn l1_segment_count(&self, branch_id: &BranchId) -> usize {
        self.branches
            .get(branch_id)
            .map_or(0, |b| b.version.load().l1_segments().len())
    }

    /// Number of segments at a given level for a branch.
    pub fn level_segment_count(&self, branch_id: &BranchId, level: usize) -> usize {
        self.branches.get(branch_id).map_or(0, |b| {
            let ver = b.version.load();
            ver.levels.get(level).map_or(0, |l| l.len())
        })
    }

    /// Total bytes of segment files at a given level for a branch.
    pub fn level_bytes(&self, branch_id: &BranchId, level: usize) -> u64 {
        self.branches.get(branch_id).map_or(0, |b| {
            let ver = b.version.load();
            ver.levels
                .get(level)
                .map_or(0, |l| l.iter().map(|s| s.file_size()).sum())
        })
    }

    /// Return the maximum commit_id across all flushed segments for a branch.
    ///
    /// Returns `None` if the branch has no segments.
    pub fn max_flushed_commit(&self, branch_id: &BranchId) -> Option<u64> {
        let branch = self.branches.get(branch_id)?;
        let ver = branch.version.load();
        ver.levels
            .iter()
            .flat_map(|level| level.iter())
            .map(|s| s.commit_range().1)
            .max()
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

        // Collect inherited layer info for deferred resolution (second pass).
        let mut deferred_inherited: Vec<(BranchId, Vec<crate::manifest::ManifestInheritedLayer>)> =
            Vec::new();

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

            // Try to read manifest for level assignments
            let manifest = match crate::manifest::read_manifest(&path) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        branch = %dir_name,
                        error = %e,
                        "corrupt manifest, falling back to all-L0"
                    );
                    None
                }
            };

            // Partition segments into levels based on manifest
            let mut level_segs: Vec<Vec<Arc<KVSegment>>> = vec![Vec::new(); NUM_LEVELS];
            if let Some(ref manifest) = manifest {
                for seg in branch_segments.iter() {
                    let filename = seg
                        .file_path()
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("");
                    let level = manifest
                        .entries
                        .iter()
                        .find(|e| e.filename == filename)
                        .map(|e| (e.level as usize).min(NUM_LEVELS - 1))
                        .unwrap_or(0); // unknown → L0
                    level_segs[level].push(Arc::clone(seg));
                }
            } else {
                // No manifest → all segments go to L0 (backward compat)
                level_segs[0] = branch_segments.clone();
            }

            // L0: sorted by commit_max descending (newest first)
            level_segs[0].sort_by(|a, b| b.commit_range().1.cmp(&a.commit_range().1));
            // Pin L0 bloom partitions so they're never evicted
            for seg in &level_segs[0] {
                seg.pin_bloom_partitions();
            }
            // L1+: sorted by key_range min ascending
            for level in level_segs.iter_mut().skip(1) {
                level.sort_by(|a, b| a.key_range().0.cmp(b.key_range().0));
            }

            let mut branch = self
                .branches
                .entry(branch_id)
                .or_insert_with(BranchState::new);

            info.segments_loaded += branch_segments.len();

            // Build new version: merge existing segments with recovered ones
            let old_ver = branch.version.load();
            let mut new_levels = old_ver.levels.clone();
            // Ensure we have enough levels
            while new_levels.len() < NUM_LEVELS {
                new_levels.push(Vec::new());
            }
            // Merge L0
            new_levels[0].append(&mut level_segs[0]);
            new_levels[0].sort_by(|a, b| b.commit_range().1.cmp(&a.commit_range().1));
            // Merge L1+
            for i in 1..NUM_LEVELS {
                new_levels[i].append(&mut level_segs[i]);
                new_levels[i].sort_by(|a, b| a.key_range().0.cmp(b.key_range().0));
            }

            branch
                .version
                .store(Arc::new(SegmentVersion { levels: new_levels }));
            refresh_level_targets(&mut branch);

            // Collect inherited layer info for second pass (deferred resolution).
            if let Some(manifest) = manifest {
                if !manifest.inherited_layers.is_empty() {
                    deferred_inherited.push((branch_id, manifest.inherited_layers));
                }
            }

            info.branches_recovered += 1;
        }

        // ── Second pass: resolve inherited layers and rebuild refcounts ──
        //
        // After all branches have been loaded with their own segments, we can
        // resolve inherited layer references to source branch segments.
        for (child_id, manifest_layers) in deferred_inherited {
            let mut inherited_layers = Vec::new();

            for ml in &manifest_layers {
                let source_branch = match self.branches.get(&ml.source_branch_id) {
                    Some(b) => b,
                    None => {
                        tracing::warn!(
                            child = %hex_encode_branch(&child_id),
                            source = %hex_encode_branch(&ml.source_branch_id),
                            "inherited layer references missing source branch, skipping"
                        );
                        continue;
                    }
                };

                // Build SegmentVersion from manifest entries using source branch's
                // segments (they are already opened and Arc'd).
                let source_ver = source_branch.version.load();
                let mut layer_levels: Vec<Vec<Arc<KVSegment>>> = vec![Vec::new(); NUM_LEVELS];

                for entry in &ml.entries {
                    let level = (entry.level as usize).min(NUM_LEVELS - 1);
                    // Find the matching segment in the source branch's version
                    let mut found = false;
                    for source_level in &source_ver.levels {
                        for seg in source_level {
                            let seg_name = seg
                                .file_path()
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("");
                            if seg_name == entry.filename {
                                layer_levels[level].push(Arc::clone(seg));
                                found = true;
                            }
                        }
                    }
                    if !found {
                        tracing::warn!(
                            child = %hex_encode_branch(&child_id),
                            source = %hex_encode_branch(&ml.source_branch_id),
                            segment = %entry.filename,
                            "inherited layer segment not found in source branch"
                        );
                    }
                }

                // Sort levels consistently
                layer_levels[0].sort_by(|a, b| b.commit_range().1.cmp(&a.commit_range().1));
                for level in layer_levels.iter_mut().skip(1) {
                    level.sort_by(|a, b| a.key_range().0.cmp(b.key_range().0));
                }

                let seg_version = Arc::new(SegmentVersion {
                    levels: layer_levels,
                });

                // Reset Materializing → Active on crash recovery
                let status = if ml.status == 1 {
                    LayerStatus::Active
                } else if ml.status == 2 {
                    LayerStatus::Materialized
                } else {
                    LayerStatus::Active
                };

                inherited_layers.push(InheritedLayer {
                    source_branch_id: ml.source_branch_id,
                    fork_version: ml.fork_version,
                    segments: seg_version,
                    status,
                });
            }

            if !inherited_layers.is_empty() {
                if let Some(mut branch) = self.branches.get_mut(&child_id) {
                    branch.inherited_layers = inherited_layers;
                }
            }
        }

        // Rebuild refcounts from inherited layers
        for branch in self.branches.iter() {
            for layer in &branch.inherited_layers {
                for seg in layer.segments.all_segments() {
                    self.ref_registry.increment(seg.file_id());
                }
            }
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
            // Write stalling: skip rotation if too many frozen memtables.
            // The active memtable grows beyond write_buffer_size until a
            // frozen memtable is flushed, then rotation resumes.
            let max_frozen = self.max_immutable_memtables.load(Ordering::Relaxed);
            if max_frozen > 0 && branch.frozen.len() >= max_frozen {
                return;
            }

            let next_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
            let old = std::mem::replace(&mut branch.active, Memtable::new(next_id));
            old.freeze();
            branch.frozen.insert(0, Arc::new(old));
            self.total_frozen_count.fetch_add(1, Ordering::Relaxed);
        }
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

    /// Returns `true` if any branch has frozen memtables pending flush.
    ///
    /// O(1) check via atomic counter — no DashMap scan.
    #[inline]
    pub fn has_frozen_memtables(&self) -> bool {
        self.total_frozen_count.load(Ordering::Relaxed) > 0
    }

    /// Branch IDs with frozen memtables, ordered by frozen count descending.
    ///
    /// Useful for the caller to decide which branches to flush first.
    pub fn branches_needing_flush(&self) -> Vec<BranchId> {
        // Fast path: no frozen memtables anywhere
        if !self.has_frozen_memtables() {
            return Vec::new();
        }

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
        // Encode once, reuse everywhere.
        let typed_key = encode_typed_key(key);
        let seek_ik = InternalKey::from_typed_key_bytes(&typed_key, u64::MAX);
        let seek_bytes = seek_ik.as_bytes();

        // 1. Active memtable
        if let Some(result) =
            branch
                .active
                .get_versioned_preencoded(&typed_key, seek_bytes, max_version)
        {
            return Some(result);
        }

        // 2. Frozen memtables (newest first)
        for frozen in &branch.frozen {
            if let Some(result) =
                frozen.get_versioned_preencoded(&typed_key, seek_bytes, max_version)
            {
                return Some(result);
            }
        }

        // 3. L0 segments (newest first, overlapping — linear scan)
        let ver = branch.version.load();
        for seg in ver.l0_segments() {
            if let Some(se) = seg.point_lookup_preencoded(&typed_key, seek_bytes, max_version) {
                let commit_id = se.commit_id;
                return Some((commit_id, segment_entry_to_memtable_entry(se)));
            }
        }

        // 4. L1+ segments (non-overlapping, sorted by key range — binary search per level)
        for level_idx in 1..ver.levels.len() {
            if let Some(se) = point_lookup_level_preencoded(
                &ver.levels[level_idx],
                &typed_key,
                seek_bytes,
                max_version,
            ) {
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

        // On-disk segments — all levels
        let typed_key = encode_typed_key(key);
        let ver = branch.version.load();
        for level in &ver.levels {
            for seg in level {
                for (ik, se) in seg.iter_seek(key) {
                    if ik.typed_key_prefix() != typed_key.as_slice() {
                        break;
                    }
                    all_versions.push((se.commit_id, segment_entry_to_memtable_entry(se)));
                }
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

        // On-disk segments — skip segments whose key range doesn't overlap
        // with the scan prefix.  This avoids loading sub-indexes from hundreds
        // of non-overlapping segments (the old O(total_segments) bottleneck).
        let prefix_bytes = encode_typed_key_prefix(prefix);
        let ver = branch.version.load();
        for level in &ver.levels {
            for seg in level {
                let (seg_min, seg_max) = seg.key_range();
                // key_range returns full InternalKey bytes; strip the
                // trailing 8-byte commit_id to get the typed-key prefix.
                if seg_max.len() >= 8 && seg_min.len() >= 8 {
                    let max_typed = &seg_max[..seg_max.len() - 8];
                    let min_typed = &seg_min[..seg_min.len() - 8];
                    // Segment is entirely before the prefix range.
                    if max_typed < prefix_bytes.as_slice() {
                        continue;
                    }
                    // Segment is entirely after the prefix range: its min key
                    // doesn't start with the prefix AND is lexicographically
                    // greater than the prefix.
                    if !min_typed.starts_with(&prefix_bytes) && min_typed > prefix_bytes.as_slice()
                    {
                        continue;
                    }
                }

                let entries: Vec<_> = seg
                    .iter_seek(prefix)
                    .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                    .collect();
                sources.push(Box::new(entries.into_iter()));
            }
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

    /// Write the manifest file for a branch, reflecting current level assignments
    /// and inherited layers.
    fn write_branch_manifest(&self, branch_id: &BranchId) {
        let segments_dir = match &self.segments_dir {
            Some(d) => d,
            None => return,
        };
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return,
        };
        let ver = branch.version.load();
        let branch_hex = hex_encode_branch(branch_id);
        let branch_dir = segments_dir.join(&branch_hex);

        let mut entries = Vec::new();
        for (level_idx, level) in ver.levels.iter().enumerate() {
            for seg in level {
                if let Some(name) = seg.file_path().file_name().and_then(|n| n.to_str()) {
                    entries.push(crate::manifest::ManifestEntry {
                        filename: name.to_string(),
                        level: level_idx as u8,
                    });
                }
            }
        }

        // Serialize inherited layers
        let inherited: Vec<crate::manifest::ManifestInheritedLayer> = branch
            .inherited_layers
            .iter()
            .map(|layer| {
                let layer_entries: Vec<crate::manifest::ManifestEntry> = layer
                    .segments
                    .levels
                    .iter()
                    .enumerate()
                    .flat_map(|(level_idx, level)| {
                        level.iter().filter_map(move |seg| {
                            seg.file_path()
                                .file_name()
                                .and_then(|n| n.to_str())
                                .map(|name| crate::manifest::ManifestEntry {
                                    filename: name.to_string(),
                                    level: level_idx as u8,
                                })
                        })
                    })
                    .collect();

                let status = match layer.status {
                    LayerStatus::Active => 0,
                    LayerStatus::Materializing => 1,
                    LayerStatus::Materialized => 2,
                };

                crate::manifest::ManifestInheritedLayer {
                    source_branch_id: layer.source_branch_id,
                    fork_version: layer.fork_version,
                    status,
                    entries: layer_entries,
                }
            })
            .collect();

        if let Err(e) = crate::manifest::write_manifest(&branch_dir, &entries, &inherited) {
            tracing::warn!(
                branch = %branch_hex,
                error = %e,
                "failed to write branch manifest"
            );
        }
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
        let ts = entry.timestamp.as_micros();
        branch.active.put_entry(&key, version, entry);
        // Track timestamp for O(1) time_range
        branch.min_timestamp.fetch_min(ts, Ordering::Relaxed);
        branch.max_timestamp.fetch_max(ts, Ordering::Relaxed);

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
        let ts = entry.timestamp.as_micros();
        branch.active.put_entry(key, version, entry);
        // Track timestamp for O(1) time_range
        branch.min_timestamp.fetch_min(ts, Ordering::Relaxed);
        branch.max_timestamp.fetch_max(ts, Ordering::Relaxed);

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
        let ts = timestamp.as_micros();
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
            // Track timestamp for O(1) time_range
            branch.min_timestamp.fetch_min(ts, Ordering::Relaxed);
            branch.max_timestamp.fetch_max(ts, Ordering::Relaxed);
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
        let ts = timestamp.as_micros();
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
            // Track timestamp for O(1) time_range
            branch.min_timestamp.fetch_min(ts, Ordering::Relaxed);
            branch.max_timestamp.fetch_max(ts, Ordering::Relaxed);
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

/// Point lookup on a sorted, non-overlapping level using binary search on key ranges.
///
/// Segments at L1+ are non-overlapping and sorted by key range. For a given key,
/// at most one segment can contain it. We binary search on the
/// `typed_key_prefix` portion of the key range bounds (stripping the trailing
/// 8-byte commit_id).
#[allow(dead_code)]
fn point_lookup_level(
    l1_segments: &[Arc<KVSegment>],
    key: &Key,
    max_version: u64,
) -> Option<SegmentEntry> {
    let typed_key = encode_typed_key(key);
    let seek_ik = InternalKey::from_typed_key_bytes(&typed_key, u64::MAX);
    point_lookup_level_preencoded(l1_segments, &typed_key, seek_ik.as_bytes(), max_version)
}

fn point_lookup_level_preencoded(
    l1_segments: &[Arc<KVSegment>],
    typed_key: &[u8],
    seek_bytes: &[u8],
    max_version: u64,
) -> Option<SegmentEntry> {
    if l1_segments.is_empty() {
        return None;
    }

    let idx = l1_segments.partition_point(|seg| {
        let (_, max_ik) = seg.key_range();
        if max_ik.len() < 8 {
            return true;
        }
        let max_prefix = &max_ik[..max_ik.len() - 8];
        max_prefix < typed_key
    });

    if idx >= l1_segments.len() {
        return None;
    }

    let seg = &l1_segments[idx];
    let (min_ik, _) = seg.key_range();
    if min_ik.len() >= 8 {
        let min_prefix = &min_ik[..min_ik.len() - 8];
        if min_prefix > typed_key {
            return None;
        }
    }

    seg.point_lookup_preencoded(typed_key, seek_bytes, max_version)
}

/// Convert a `SegmentEntry` into a `MemtableEntry` for the merge path.
fn segment_entry_to_memtable_entry(se: SegmentEntry) -> MemtableEntry {
    MemtableEntry {
        value: se.value,
        is_tombstone: se.is_tombstone,
        timestamp: Timestamp::from_micros(se.timestamp),
        ttl_ms: se.ttl_ms,
    }
}

mod compaction;
mod ref_registry;

#[cfg(test)]
mod tests;
