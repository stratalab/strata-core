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
use crate::key_encoding::{
    encode_typed_key, encode_typed_key_prefix, rewrite_branch_id_bytes, InternalKey,
    COMMIT_ID_SUFFIX_LEN,
};
use crate::memory_stats::{BranchMemoryStats, StorageMemoryStats};
use crate::memtable::{Memtable, MemtableEntry};
use crate::merge_iter::{MergeIterator, MvccIterator, RewritingIterator};
use crate::pressure::{MemoryPressure, PressureLevel};
use crate::segment::{KVSegment, SegmentEntry};
use crate::segment_builder::{SegmentBuilder, SplittingSegmentBuilder};

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

/// Maximum inherited layer depth before background materialization is triggered.
const MAX_INHERITED_LAYERS: usize = 4;

/// Minimum L1 target size for dynamic level sizing (1MB).
/// Prevents degenerate zero-size targets on tiny embedded datasets.
const MIN_BASE_BYTES: u64 = 1 << 20;

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
// MaterializeResult
// ---------------------------------------------------------------------------

/// Statistics returned by [`SegmentedStore::materialize_layer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializeResult {
    /// Number of entries copied from the inherited layer into own segments.
    pub entries_materialized: u64,
    /// Number of new segment files created (0 or 1).
    pub segments_created: usize,
}

// ---------------------------------------------------------------------------
// VersionedEntry — version-scoped listing with tombstone status
// ---------------------------------------------------------------------------

/// Entry from version-scoped listing that preserves tombstone status.
/// Used by three-way merge to reconstruct ancestor state including deletions.
#[derive(Debug, Clone)]
pub struct VersionedEntry {
    /// The decoded user key.
    pub key: Key,
    /// The stored value (empty for tombstones).
    pub value: Value,
    /// Whether this entry is a deletion tombstone.
    pub is_tombstone: bool,
    /// The MVCC commit version that wrote this entry.
    pub commit_id: u64,
}

/// Entry from a branch's own sources (excluding inherited layers).
/// Used by COW-aware diff to identify writes since fork.
#[derive(Debug, Clone)]
pub struct OwnEntry {
    /// The decoded user key.
    pub key: Key,
    /// The stored value (empty for tombstones).
    pub value: Value,
    /// Whether this entry is a deletion tombstone.
    pub is_tombstone: bool,
    /// The MVCC commit version that wrote this entry.
    pub commit_id: u64,
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
            level_targets: compaction::recalculate_level_targets(
                &[0u64; NUM_LEVELS],
                LEVEL_BASE_BYTES,
            ),
            inherited_layers: Vec::new(),
        }
    }

    /// Update min/max timestamp tracking for a single write.
    #[inline]
    fn track_timestamp(&self, ts: u64) {
        self.min_timestamp.fetch_min(ts, Ordering::Relaxed);
        self.max_timestamp.fetch_max(ts, Ordering::Relaxed);
    }
}

/// Recompute cached level targets from the current segment version.
fn refresh_level_targets(branch: &mut BranchState, max_base_bytes: u64) {
    let ver = branch.version.load();
    let mut actual_bytes = [0u64; NUM_LEVELS];
    for (level, segs) in ver.levels.iter().enumerate() {
        actual_bytes[level] = segs.iter().map(|s| s.file_size()).sum();
    }
    branch.level_targets = compaction::recalculate_level_targets(&actual_bytes, max_base_bytes);
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
    write_buffer_size: AtomicU64,
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
    /// Snapshot-safe floor (#1697): versions at or above this commit_id are
    /// protected from `max_versions_per_key` pruning during compaction.
    /// Updated by the engine before compaction to reflect `gc_safe_point()`.
    snapshot_floor: AtomicU64,
    /// Total frozen memtable count across all branches (for O(1) "any frozen?" check).
    total_frozen_count: AtomicUsize,
    /// Maximum frozen memtables per branch before rotation is skipped (0 = unlimited).
    max_immutable_memtables: AtomicUsize,
    /// Optional rate limiter for compaction I/O (read + write).
    compaction_rate_limiter: arc_swap::ArcSwapOption<crate::rate_limiter::RateLimiter>,
    /// Reference count registry for shared segments (COW branching).
    ref_registry: ref_registry::SegmentRefRegistry,
    /// Branches currently being materialized (prevents concurrent materialization #1703).
    materializing_branches: DashMap<BranchId, ()>,
    /// Target size for a single output segment file. Default: 64 MiB.
    target_file_size: AtomicU64,
    /// Target total size for L1. Also used as MAX_BASE_BYTES for dynamic level sizing.
    /// Default: 256 MiB.
    level_base_bytes: AtomicU64,
    /// Data block size in bytes for segment files. Default: 4096 (4 KiB).
    data_block_size: AtomicUsize,
    /// Bloom filter bits per key (base value). Default: 10.
    bloom_bits_per_key: AtomicUsize,
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
            write_buffer_size: AtomicU64::new(0),
            next_segment_id: AtomicU64::new(1),
            pressure: MemoryPressure::disabled(),
            bulk_load_branches: DashMap::new(),
            max_branches: AtomicUsize::new(0),
            max_versions_per_key: AtomicUsize::new(0),
            snapshot_floor: AtomicU64::new(0),
            total_frozen_count: AtomicUsize::new(0),
            max_immutable_memtables: AtomicUsize::new(0),
            compaction_rate_limiter: arc_swap::ArcSwapOption::empty(),
            ref_registry: ref_registry::SegmentRefRegistry::new(),
            materializing_branches: DashMap::new(),
            target_file_size: AtomicU64::new(TARGET_FILE_SIZE),
            level_base_bytes: AtomicU64::new(LEVEL_BASE_BYTES),
            data_block_size: AtomicUsize::new(4096),
            bloom_bits_per_key: AtomicUsize::new(10),
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
            write_buffer_size: AtomicU64::new(write_buffer_size as u64),
            next_segment_id: AtomicU64::new(1),
            pressure: MemoryPressure::disabled(),
            bulk_load_branches: DashMap::new(),
            max_branches: AtomicUsize::new(0),
            max_versions_per_key: AtomicUsize::new(0),
            snapshot_floor: AtomicU64::new(0),
            total_frozen_count: AtomicUsize::new(0),
            max_immutable_memtables: AtomicUsize::new(0),
            compaction_rate_limiter: arc_swap::ArcSwapOption::empty(),
            ref_registry: ref_registry::SegmentRefRegistry::new(),
            materializing_branches: DashMap::new(),
            target_file_size: AtomicU64::new(TARGET_FILE_SIZE),
            level_base_bytes: AtomicU64::new(LEVEL_BASE_BYTES),
            data_block_size: AtomicUsize::new(4096),
            bloom_bits_per_key: AtomicUsize::new(10),
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
            write_buffer_size: AtomicU64::new(write_buffer_size as u64),
            next_segment_id: AtomicU64::new(1),
            pressure,
            bulk_load_branches: DashMap::new(),
            max_branches: AtomicUsize::new(0),
            max_versions_per_key: AtomicUsize::new(0),
            snapshot_floor: AtomicU64::new(0),
            total_frozen_count: AtomicUsize::new(0),
            max_immutable_memtables: AtomicUsize::new(0),
            compaction_rate_limiter: arc_swap::ArcSwapOption::empty(),
            ref_registry: ref_registry::SegmentRefRegistry::new(),
            materializing_branches: DashMap::new(),
            target_file_size: AtomicU64::new(TARGET_FILE_SIZE),
            level_base_bytes: AtomicU64::new(LEVEL_BASE_BYTES),
            data_block_size: AtomicUsize::new(4096),
            bloom_bits_per_key: AtomicUsize::new(10),
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

    /// Number of branches currently tracked by the store.
    #[inline]
    pub fn branch_count(&self) -> usize {
        self.branches.len()
    }

    /// Increment version and return new value.
    ///
    /// # Panics
    ///
    /// Panics if the version counter is at `u64::MAX`. This is consistent
    /// with `TransactionManager::allocate_version()` which returns
    /// `Err(CounterOverflow)` at the same boundary.
    #[inline]
    pub fn next_version(&self) -> u64 {
        let prev = self
            .version
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| v.checked_add(1))
            .expect("version counter overflow: u64::MAX versions exhausted");
        // Safe: checked_add succeeded, so prev < u64::MAX and prev + 1 won't overflow.
        prev + 1
    }

    /// Set version (used during recovery).
    pub fn set_version(&self, version: u64) {
        self.version.store(version, Ordering::Release);
    }

    /// Monotonically advance the visible storage version.
    ///
    /// Used by the follower refresh path to bump version AFTER secondary
    /// indexes (BM25/HNSW) have been updated, ensuring readers never see
    /// KV data before the corresponding index entries exist (Issue #1734).
    pub fn advance_version(&self, version: u64) {
        self.version.fetch_max(version, Ordering::AcqRel);
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

    /// Set the target segment file size in bytes.
    pub fn set_target_file_size(&self, bytes: u64) {
        self.target_file_size.store(bytes, Ordering::Relaxed);
    }

    /// Get the configured target segment file size.
    pub fn target_file_size(&self) -> u64 {
        self.target_file_size.load(Ordering::Relaxed)
    }

    /// Set the L1 target size (level_base_bytes) in bytes.
    pub fn set_level_base_bytes(&self, bytes: u64) {
        self.level_base_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Get the configured L1 target size.
    pub fn level_base_bytes(&self) -> u64 {
        self.level_base_bytes.load(Ordering::Relaxed)
    }

    /// Set the data block size in bytes for new segments.
    pub fn set_data_block_size(&self, bytes: usize) {
        self.data_block_size.store(bytes, Ordering::Relaxed);
    }

    /// Get the configured data block size.
    pub fn data_block_size(&self) -> usize {
        self.data_block_size.load(Ordering::Relaxed)
    }

    /// Set the bloom filter bits per key for new segments.
    pub fn set_bloom_bits_per_key(&self, bits: usize) {
        self.bloom_bits_per_key.store(bits, Ordering::Relaxed);
    }

    /// Get the configured bloom bits per key.
    pub fn bloom_bits_per_key(&self) -> usize {
        self.bloom_bits_per_key.load(Ordering::Relaxed)
    }

    /// Create a `SegmentBuilder` pre-configured with this store's
    /// `data_block_size` and `bloom_bits_per_key`.
    fn make_segment_builder(&self) -> SegmentBuilder {
        SegmentBuilder {
            data_block_size: self.data_block_size(),
            bloom_bits_per_key: self.bloom_bits_per_key(),
            ..SegmentBuilder::default()
        }
    }

    /// Iterate over all branch IDs that have data.
    pub fn branch_ids(&self) -> Vec<BranchId> {
        self.branches.iter().map(|entry| *entry.key()).collect()
    }

    /// Returns `true` if this store is backed by a segments directory (disk-backed).
    pub fn has_segments_dir(&self) -> bool {
        self.segments_dir.is_some()
    }

    /// Remove all data for a branch, cleaning up own segments and inherited refcounts.
    /// Own segment files and the branch manifest are deleted immediately.
    /// Inherited segment files are NOT deleted — their refcounts are decremented
    /// and lazy cleanup happens during parent compaction.
    /// Returns true if the branch existed.
    pub fn clear_branch(&self, branch_id: &BranchId) -> bool {
        if let Some((_, branch)) = self.branches.remove(branch_id) {
            // 1. Clean up the branch's own segments.
            // Check refcount before deleting — a child branch may still
            // reference these segments through inherited layers (#1702).
            // Hold deletion write guard to prevent TOCTOU with fork (#1682).
            let ver = branch.version.load();
            {
                let _deletion_guard = self.ref_registry.deletion_write_guard();
                for level in &ver.levels {
                    for seg in level {
                        crate::block_cache::global_cache().invalidate_file(seg.file_id());
                        if !self.ref_registry.is_referenced(seg.file_id()) {
                            let _ = std::fs::remove_file(seg.file_path());
                        }
                    }
                }
            }

            // 2. Decrement refcounts for inherited (shared) segments.
            // Do NOT delete files here — the parent may still own the segment
            // in its version.levels. Orphaned files (parent compacted away the
            // segment before this decrement) are cleaned up by gc_orphan_segments.
            for layer in &branch.inherited_layers {
                for level in &layer.segments.levels {
                    for seg in level {
                        self.ref_registry.decrement(seg.file_id());
                    }
                }
            }

            // 3. Remove manifest file and branch directory from disk.
            if let Some(segments_dir) = &self.segments_dir {
                let branch_hex = hex_encode_branch(branch_id);
                let branch_dir = segments_dir.join(&branch_hex);
                let _ = std::fs::remove_file(branch_dir.join("segments.manifest"));
                // remove_dir only succeeds if empty (safe — won't delete
                // files we missed).
                let _ = std::fs::remove_dir(&branch_dir);
            }

            // 4. Garbage-collect orphan segment files (#1705).
            // Refcount decrements above may have released the last reference to
            // segments whose parent already compacted them away. GC deletes
            // any .sst on disk not referenced by a live branch or the refcount
            // registry.
            self.gc_orphan_segments();

            true
        } else {
            false
        }
    }

    /// Garbage-collect orphaned segment files (#1705).
    ///
    /// Scans all branch directories for `.sst` files that are not referenced
    /// by any branch's current `version.levels` or inherited layers. Such
    /// files arise when a parent compacts away a segment while a child still
    /// references it, and the child later releases its reference (decrement
    /// to 0) without being able to delete the file (the parent might still
    /// own it at that time).
    ///
    /// Returns the number of files deleted.
    pub fn gc_orphan_segments(&self) -> usize {
        let segments_dir = match &self.segments_dir {
            Some(d) => d,
            None => return 0,
        };

        // Build the set of all segment file_id values currently live in any
        // branch. file_id is a hash of the file path (see block_cache::file_path_hash).
        let mut live_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for branch in self.branches.iter() {
            let ver = branch.version.load();
            for level in &ver.levels {
                for seg in level {
                    live_ids.insert(seg.file_id());
                }
            }
            for layer in &branch.inherited_layers {
                for level in &layer.segments.levels {
                    for seg in level {
                        live_ids.insert(seg.file_id());
                    }
                }
            }
        }

        let mut deleted = 0usize;

        // Scan all branch directories.
        let entries = match std::fs::read_dir(segments_dir) {
            Ok(e) => e,
            Err(_) => return 0,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let sst_entries = match std::fs::read_dir(&path) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for sst_entry in sst_entries.flatten() {
                let sst_path = sst_entry.path();
                if sst_path.extension().and_then(|e| e.to_str()) != Some("sst") {
                    continue;
                }
                // file_id is a hash of the full path, matching KVSegment::file_id().
                let file_id = crate::block_cache::file_path_hash(&sst_path);
                if !live_ids.contains(&file_id) {
                    // Hold deletion write guard to prevent TOCTOU with fork (#1682).
                    let _deletion_guard = self.ref_registry.deletion_write_guard();
                    if !self.ref_registry.is_referenced(file_id) {
                        crate::block_cache::global_cache().invalidate_file(file_id);
                        let _ = std::fs::remove_file(&sst_path);
                        deleted += 1;
                    }
                }
            }
        }

        deleted
    }

    /// Number of inherited layers for a branch.
    pub fn inherited_layer_count(&self, branch_id: &BranchId) -> usize {
        self.branches
            .get(branch_id)
            .map_or(0, |b| b.inherited_layers.len())
    }

    /// Get the fork origin of a branch from its nearest active inherited layer.
    ///
    /// Returns the `(source_branch_id, fork_version)` of the first non-materialized
    /// inherited layer, or `None` if the branch has no inherited layers (was never
    /// forked, or all layers have been fully materialized).
    pub fn get_fork_info(&self, branch_id: &BranchId) -> Option<(BranchId, u64)> {
        let branch = self.branches.get(branch_id)?;
        // Find first non-materialized layer (nearest ancestor with live data).
        // Layers are ordered nearest-ancestor-first; skip any that have been
        // fully materialized, as their fork info is no longer useful for reads.
        branch
            .inherited_layers
            .iter()
            .find(|l| l.status != LayerStatus::Materialized)
            .map(|l| (l.source_branch_id, l.fork_version))
    }

    /// Return branch IDs where inherited layer depth exceeds `MAX_INHERITED_LAYERS`.
    pub fn branches_needing_materialization(&self) -> Vec<BranchId> {
        self.branches
            .iter()
            .filter_map(|entry| {
                if entry.value().inherited_layers.len() > MAX_INHERITED_LAYERS {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Materialize a single inherited layer into the child's own segments.
    ///
    /// Copies entries from `inherited_layers[layer_index]` that are not
    /// shadowed by the child's own data or closer layers. Follows the same
    /// snapshot → release → I/O → re-acquire pattern as `flush_oldest_frozen`.
    ///
    /// Returns `Err` on I/O failure, `Ok(result)` on success.
    pub fn materialize_layer(
        &self,
        child_branch_id: &BranchId,
        layer_index: usize,
    ) -> io::Result<MaterializeResult> {
        let segments_dir = match &self.segments_dir {
            Some(d) => d,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "materialize_layer requires a disk-backed store",
                ))
            }
        };

        // Serialize materialization per branch (#1703).
        // Use atomic entry() API to prevent TOCTOU between check and insert.
        {
            use dashmap::mapref::entry::Entry;
            match self.materializing_branches.entry(*child_branch_id) {
                Entry::Occupied(_) => {
                    return Ok(MaterializeResult {
                        entries_materialized: 0,
                        segments_created: 0,
                    });
                }
                Entry::Vacant(e) => {
                    e.insert(());
                }
            }
        }

        // RAII guard to remove entry on all exit paths.
        struct MaterializeGuard<'a> {
            store: &'a SegmentedStore,
            branch_id: BranchId,
        }
        impl Drop for MaterializeGuard<'_> {
            fn drop(&mut self) {
                self.store.materializing_branches.remove(&self.branch_id);
            }
        }
        let _guard = MaterializeGuard {
            store: self,
            branch_id: *child_branch_id,
        };

        // 1b. Flush child memtables to disk so shadow detection sees all
        //     child writes (#1694).  Same pattern as fork_branch.
        {
            let has_active = self
                .branches
                .get(child_branch_id)
                .is_some_and(|b| !b.active.is_empty());
            if has_active {
                self.rotate_memtable(child_branch_id);
            }
        }
        while self.flush_oldest_frozen(child_branch_id)? {}

        // 2a. Snapshot under read guard
        let (layer_segments, source_branch_id, fork_version, own_version, closer_layers) = {
            let branch = match self.branches.get(child_branch_id) {
                Some(b) => b,
                None => return Err(io::Error::new(io::ErrorKind::NotFound, "branch not found")),
            };
            if layer_index >= branch.inherited_layers.len() {
                // Layer was already removed by a concurrent materialization that
                // completed before we acquired the DashMap guard. Not an error.
                return Ok(MaterializeResult {
                    entries_materialized: 0,
                    segments_created: 0,
                });
            }
            let layer = &branch.inherited_layers[layer_index];

            // Reject if already being materialized (#1703)
            if layer.status == LayerStatus::Materializing {
                return Ok(MaterializeResult {
                    entries_materialized: 0,
                    segments_created: 0,
                });
            }

            let layer_segments = Arc::clone(&layer.segments);
            let source_branch_id = layer.source_branch_id;
            let fork_version = layer.fork_version;
            let own_version = branch.version.load_full();

            // Snapshot closer inherited layers (indices 0..layer_index)
            let closer_layers: Vec<(Arc<SegmentVersion>, BranchId, u64)> = branch.inherited_layers
                [..layer_index]
                .iter()
                .map(|l| (Arc::clone(&l.segments), l.source_branch_id, l.fork_version))
                .collect();

            (
                layer_segments,
                source_branch_id,
                fork_version,
                own_version,
                closer_layers,
            )
            // DashMap guard drops here
        };

        // 2b. Set status to Materializing
        {
            let mut branch = match self.branches.get_mut(child_branch_id) {
                Some(b) => b,
                None => return Err(io::Error::new(io::ErrorKind::NotFound, "branch not found")),
            };
            if layer_index < branch.inherited_layers.len() {
                branch.inherited_layers[layer_index].status = LayerStatus::Materializing;
            }
        }
        self.write_branch_manifest(child_branch_id);

        // 2c. Build materialized entries (no locks held — I/O heavy)
        let mut entries = Self::collect_unshadowed_entries(
            &layer_segments,
            child_branch_id,
            fork_version,
            &own_version,
            &closer_layers,
        );

        let entries_materialized = entries.len() as u64;

        // Sort entries by InternalKey — required because entries from multiple
        // segments/levels are not globally sorted (L0 segments overlap, and
        // entries span L0 through L6). SegmentBuilder expects sorted input.
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        // 2d. Build segment file(s) (no locks held).
        // Use SplittingSegmentBuilder to avoid creating oversized L0 segments
        // from large inherited layers (#1671).
        let new_segments: Vec<KVSegment> = if !entries.is_empty() {
            let branch_hex = hex_encode_branch(child_branch_id);
            let branch_dir = segments_dir.join(&branch_hex);
            std::fs::create_dir_all(&branch_dir)?;

            let next_id = &self.next_segment_id;
            let builder = SplittingSegmentBuilder::new(self.target_file_size())
                .with_compression(crate::segment_builder::CompressionCodec::None);
            let built = builder.build_split(entries.into_iter(), |_split_idx| {
                let seg_id = next_id.fetch_add(1, Ordering::Relaxed);
                branch_dir.join(format!("{}.sst", seg_id))
            })?;

            let mut segments = Vec::with_capacity(built.len());
            for (path, _meta) in built {
                let seg = KVSegment::open(&path)?;
                segments.push(seg);
            }
            segments
        } else {
            Vec::new()
        };
        let segments_created = new_segments.len();

        // 2e. Atomic install (under DashMap write guard)
        {
            let mut branch = self
                .branches
                .entry(*child_branch_id)
                .or_insert_with(BranchState::new);

            // Prepend new segments to L0
            if !new_segments.is_empty() {
                let old_ver = branch.version.load();
                let mut new_l0 =
                    Vec::with_capacity(old_ver.l0_segments().len() + new_segments.len());
                for seg in new_segments {
                    new_l0.push(Arc::new(seg));
                }
                new_l0.extend(old_ver.l0_segments().iter().cloned());
                let mut new_levels = old_ver.levels.clone();
                new_levels[0] = new_l0;
                branch
                    .version
                    .store(Arc::new(SegmentVersion { levels: new_levels }));
            }

            // Remove the inherited layer by identity (source_branch_id + fork_version)
            // instead of by index, since the index may have shifted if another
            // layer was removed between snapshot and install (#1703).
            if let Some(idx) = branch.inherited_layers.iter().position(|l| {
                l.source_branch_id == source_branch_id && l.fork_version == fork_version
            }) {
                branch.inherited_layers.remove(idx);
            }

            refresh_level_targets(&mut branch, self.level_base_bytes());
        }

        // 2f. Cleanup
        self.write_branch_manifest(child_branch_id);

        // Decrement refcounts for each segment in the removed layer.
        for level in &layer_segments.levels {
            for seg in level {
                self.ref_registry.decrement(seg.file_id());
            }
        }

        // Garbage-collect orphan segment files (#1705).
        // Refcount decrements above may have released the last reference to
        // segments whose parent already compacted them away.
        self.gc_orphan_segments();

        Ok(MaterializeResult {
            entries_materialized,
            segments_created,
        })
    }

    /// Collect entries from an inherited layer that are not shadowed by the
    /// child's own segments or closer inherited layers.
    fn collect_unshadowed_entries(
        layer_segments: &SegmentVersion,
        child_branch_id: &BranchId,
        fork_version: u64,
        own_version: &SegmentVersion,
        closer_layers: &[(Arc<SegmentVersion>, BranchId, u64)],
    ) -> Vec<(InternalKey, MemtableEntry)> {
        let mut entries = Vec::new();
        let mut last_shadow_key: Option<Vec<u8>> = None;
        let mut last_shadow_result = false;

        for level in &layer_segments.levels {
            for seg in level {
                for (ik, se) in seg.iter_seek_all() {
                    if se.commit_id > fork_version {
                        continue;
                    }

                    let typed_key = ik.typed_key_prefix();
                    let Some(child_typed_key) = rewrite_branch_id_bytes(typed_key, child_branch_id)
                    else {
                        continue;
                    };

                    let is_shadowed = if last_shadow_key.as_deref() == Some(&child_typed_key) {
                        last_shadow_result
                    } else {
                        let shadowed =
                            Self::key_exists_in_own_segments(own_version, &child_typed_key)
                                || closer_layers.iter().any(
                                    |(closer_segs, closer_source, closer_fork)| {
                                        Self::key_exists_in_layer(
                                            closer_segs,
                                            *closer_source,
                                            &child_typed_key,
                                            *closer_fork,
                                        )
                                    },
                                );
                        last_shadow_key = Some(child_typed_key.clone());
                        last_shadow_result = shadowed;
                        shadowed
                    };

                    if is_shadowed {
                        continue;
                    }

                    let Some(child_ik_bytes) =
                        rewrite_branch_id_bytes(ik.as_bytes(), child_branch_id)
                    else {
                        continue;
                    };
                    let child_ik = InternalKey::from_bytes(child_ik_bytes);
                    entries.push((child_ik, segment_entry_to_memtable_entry(se)));
                }
            }
        }
        entries
    }

    /// Check if a key exists in the child's own segments.
    fn key_exists_in_own_segments(own_version: &SegmentVersion, typed_key: &[u8]) -> bool {
        let seek_ik = InternalKey::from_typed_key_bytes(typed_key, u64::MAX);
        let seek_bytes = seek_ik.as_bytes();

        // L0 segments (linear scan)
        for seg in own_version.l0_segments() {
            match seg.point_lookup_preencoded(typed_key, seek_bytes, u64::MAX) {
                Ok(Some(_)) => return true,
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(error = %e, "corruption during materialization shadow check");
                    return true; // Conservative: assume shadowed to prevent stale data resurrection
                }
            }
        }

        // L1+ segments (binary search per level)
        for level_idx in 1..own_version.levels.len() {
            match point_lookup_level_preencoded(
                &own_version.levels[level_idx],
                typed_key,
                seek_bytes,
                u64::MAX,
            ) {
                Ok(Some(_)) => return true,
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(error = %e, "corruption during materialization shadow check");
                    return true; // Conservative: assume shadowed to prevent stale data resurrection
                }
            }
        }

        false
    }

    /// Check if a key exists in a closer inherited layer's segments.
    fn key_exists_in_layer(
        layer_segments: &SegmentVersion,
        source_branch_id: BranchId,
        child_typed_key: &[u8], // in child namespace
        fork_version: u64,
    ) -> bool {
        // Rewrite typed_key from child → source namespace
        let Some(src_typed_key) = rewrite_branch_id_bytes(child_typed_key, &source_branch_id)
        else {
            return false; // Corrupt key can't exist
        };
        let src_seek_ik = InternalKey::from_typed_key_bytes(&src_typed_key, u64::MAX);
        let src_seek_bytes = src_seek_ik.as_bytes();

        // L0 segments
        for seg in layer_segments.l0_segments() {
            match seg.point_lookup_preencoded(&src_typed_key, src_seek_bytes, fork_version) {
                Ok(Some(_)) => return true,
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(error = %e, "corruption during materialization shadow check");
                    return true; // Conservative: assume shadowed to prevent stale data resurrection
                }
            }
        }

        // L1+ segments
        for level_idx in 1..layer_segments.levels.len() {
            match point_lookup_level_preencoded(
                &layer_segments.levels[level_idx],
                &src_typed_key,
                src_seek_bytes,
                fork_version,
            ) {
                Ok(Some(_)) => return true,
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(error = %e, "corruption during materialization shadow check");
                    return true; // Conservative: assume shadowed to prevent stale data resurrection
                }
            }
        }

        false
    }

    /// Fork source branch onto destination via COW inherited layers.
    ///
    /// Flushes source memtables, snapshots segments, attaches as inherited
    /// layer on dest, increments refcounts. Returns `(fork_version, segments_shared)`.
    pub fn fork_branch(
        &self,
        source_id: &BranchId,
        dest_id: &BranchId,
    ) -> io::Result<(u64, usize)> {
        if source_id == dest_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fork_branch: source and destination must be different branches",
            ));
        }

        // 1. Flush bulk memtable data to segments (outside exclusive lock).
        self.rotate_memtable(source_id);
        while self.flush_oldest_frozen(source_id)? {}

        // 2. Acquire exclusive lock on source branch, drain any straggler
        //    writes, then capture fork_version + snapshot atomically.
        //
        //    Between step 1 and this get_mut, concurrent writes may have
        //    committed data to the source's active memtable.  The exclusive
        //    DashMap guard blocks further writes to the same shard, so we
        //    can inline-flush the stragglers and capture a consistent
        //    snapshot (see #1679).
        //
        //    CRITICAL: Increment refcounts BEFORE dropping the source guard.
        //    Without this, concurrent parent compaction can delete segments
        //    between the snapshot and the refcount increment (see #1662).
        let (dest_layers, segments_shared, fork_version, source_manifest_dirty) = {
            let mut source = match self.branches.get_mut(source_id) {
                Some(b) => b,
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "fork_branch: source branch no longer exists",
                    ))
                }
            };

            // Inline-rotate: move straggler writes from active to frozen.
            if !source.active.is_empty() {
                let next_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
                let old = std::mem::replace(&mut source.active, Memtable::new(next_id));
                old.freeze();
                source.frozen.insert(0, Arc::new(old));
                self.total_frozen_count.fetch_add(1, Ordering::Relaxed);
            }

            // Inline-flush: build segments from frozen memtables while
            // holding the exclusive guard.  Typically 0–1 tiny memtables
            // (only the stragglers from above), so I/O is bounded.
            let mut source_manifest_dirty = false;
            if let Some(segments_dir) = &self.segments_dir {
                while let Some(mt) = source.frozen.last().cloned() {
                    let seg_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
                    let branch_hex = hex_encode_branch(source_id);
                    let branch_dir = segments_dir.join(&branch_hex);
                    std::fs::create_dir_all(&branch_dir)?;
                    let seg_path = branch_dir.join(format!("{}.sst", seg_id));
                    let builder = self
                        .make_segment_builder()
                        .with_compression(crate::segment_builder::CompressionCodec::None);
                    builder.build_from_iter(mt.iter_all(), &seg_path)?;
                    let segment = KVSegment::open(&seg_path)?;

                    // Install segment into source's SegmentVersion.
                    let old_ver = source.version.load();
                    let mut new_l0 = Vec::with_capacity(old_ver.l0_segments().len() + 1);
                    new_l0.push(Arc::new(segment));
                    new_l0.extend(old_ver.l0_segments().iter().cloned());
                    let mut new_levels = old_ver.levels.clone();
                    new_levels[0] = new_l0;
                    source
                        .version
                        .store(Arc::new(SegmentVersion { levels: new_levels }));

                    // Remove the flushed frozen memtable.
                    if source
                        .frozen
                        .last()
                        .is_some_and(|last| last.id() == mt.id())
                    {
                        source.frozen.pop();
                        self.total_frozen_count.fetch_sub(1, Ordering::Relaxed);
                    }
                    source_manifest_dirty = true;
                }
                if source_manifest_dirty {
                    refresh_level_targets(&mut source, self.level_base_bytes());
                }
            }

            // All source data is now in segments.  Capture fork_version
            // under the exclusive guard — no concurrent writes can advance
            // it before we snapshot.
            let fork_version = self.version.load(Ordering::Acquire);

            let source_segments = source.version.load_full();
            let source_inherited: Vec<InheritedLayer> = source
                .inherited_layers
                .iter()
                .map(|l| InheritedLayer {
                    source_branch_id: l.source_branch_id,
                    fork_version: l.fork_version,
                    segments: Arc::clone(&l.segments),
                    // Always reset to Active: the child has not started any
                    // materialization.  Copying Materializing would permanently
                    // block the child from materializing this layer (#1721).
                    status: LayerStatus::Active,
                })
                .collect();

            // Build dest layers: [source_own, ...source_inherited]
            let mut layers = Vec::with_capacity(1 + source_inherited.len());
            layers.push(InheritedLayer {
                source_branch_id: *source_id,
                fork_version,
                segments: source_segments,
                status: LayerStatus::Active,
            });
            layers.extend(source_inherited);

            // Increment refcounts while source guard is still held.
            // Hold the deletion barrier read guard to prevent concurrent
            // delete_segment_if_unreferenced from observing a transient
            // zero refcount mid-batch (#1682).
            let mut shared = 0usize;
            {
                let _deletion_guard = self.ref_registry.deletion_read_guard();
                for layer in &layers {
                    for level in &layer.segments.levels {
                        for seg in level {
                            self.ref_registry.increment(seg.file_id());
                            shared += 1;
                        }
                    }
                }
            }

            (layers, shared, fork_version, source_manifest_dirty)
            // source DashMap guard drops here — segments are now protected
        };

        // Persist source manifest if we inline-flushed segments.
        if source_manifest_dirty {
            self.write_branch_manifest(source_id);
        }

        // 6. Attach to dest branch, rejecting if a concurrent fork already installed layers.
        let mut dest = self
            .branches
            .entry(*dest_id)
            .or_insert_with(BranchState::new);
        if !dest.inherited_layers.is_empty() {
            drop(dest);
            // Undo refcount increments from step 2.
            for layer in &dest_layers {
                for level in &layer.segments.levels {
                    for seg in level {
                        self.ref_registry.decrement(seg.file_id());
                    }
                }
            }
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "fork_branch: destination already has inherited layers (concurrent fork race)",
            ));
        }
        dest.inherited_layers = dest_layers;
        drop(dest);

        // 7. Write manifest
        self.write_branch_manifest(dest_id);

        Ok((fork_version, segments_shared))
    }

    /// Count distinct live (non-tombstone, non-expired) logical keys in a branch.
    pub fn branch_entry_count(&self, branch_id: &BranchId) -> usize {
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return 0,
        };
        // Collect all entries via MVCC dedup at u64::MAX
        self.list_branch_inner(&branch, *branch_id).len()
    }

    /// List all live entries for a branch (MVCC dedup at latest version).
    pub fn list_branch(&self, branch_id: &BranchId) -> Vec<(Key, VersionedValue)> {
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return Vec::new(),
        };
        self.list_branch_inner(&branch, *branch_id)
    }

    /// Internal: list all live entries for a branch reference.
    fn list_branch_inner(
        &self,
        branch: &BranchState,
        branch_id: BranchId,
    ) -> Vec<(Key, VersionedValue)> {
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

        // Inherited layers (COW branching)
        for layer in &branch.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            for level in &layer.segments.levels {
                for seg in level {
                    let entries: Vec<_> = seg
                        .iter_seek_all()
                        .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                        .collect();
                    sources.push(Box::new(RewritingIterator::new(
                        entries.into_iter(),
                        branch_id,
                        layer.fork_version,
                    )));
                }
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

    /// Set the snapshot-safe floor for compaction (#1697).
    ///
    /// Versions with `commit_id >= floor` are protected from `max_versions_per_key`
    /// pruning during compaction. The engine should call this with `gc_safe_point()`
    /// before triggering compaction so that active snapshots are not violated.
    pub fn set_snapshot_floor(&self, floor: u64) {
        self.snapshot_floor.store(floor, Ordering::Relaxed);
    }

    /// Set the memtable write buffer size in bytes.
    ///
    /// Changes take effect on the next memtable rotation (the active memtable
    /// continues at its current size; new memtables use the updated threshold).
    pub fn set_write_buffer_size(&self, bytes: usize) {
        self.write_buffer_size
            .store(bytes as u64, Ordering::Relaxed);
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
    pub fn get_value_direct(&self, key: &Key) -> StrataResult<Option<Value>> {
        let branch_id = key.namespace.branch_id;
        let branch = match self.branches.get(&branch_id) {
            Some(b) => b,
            None => return Ok(None),
        };
        match Self::get_versioned_from_branch(&branch, key, u64::MAX)? {
            Some((_commit_id, entry)) => {
                if entry.is_tombstone || entry.is_expired() {
                    Ok(None)
                } else {
                    Ok(Some(entry.value))
                }
            }
            None => Ok(None),
        }
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

    /// List entries from a branch's own sources only (active memtable, frozen
    /// memtables, and own on-disk segments). **Excludes inherited layers.**
    ///
    /// When `min_commit_id` is `Some(v)`, only entries with `commit_id > v` are
    /// returned (for scanning parent's post-fork writes). When `None`, all own
    /// entries are returned (for scanning child's writes since fork).
    ///
    /// Unlike `list_by_type()`, this method preserves tombstones (needed for
    /// COW-aware diff to detect deletions) and does NOT filter expired entries.
    /// Returns MVCC-deduplicated entries (latest version per logical key).
    pub fn list_own_entries(
        &self,
        branch_id: &BranchId,
        min_commit_id: Option<u64>,
    ) -> Vec<OwnEntry> {
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return Vec::new(),
        };

        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = Vec::new();

        // Active memtable
        let active_entries: Vec<_> = branch.active.iter_all().collect();
        sources.push(Box::new(active_entries.into_iter()));

        // Frozen memtables (newest first)
        for frozen in &branch.frozen {
            let entries: Vec<_> = frozen.iter_all().collect();
            sources.push(Box::new(entries.into_iter()));
        }

        // On-disk segments — all levels (own segments only, NO inherited layers)
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

        // NOTE: inherited layers intentionally excluded — this is the whole point.

        let merge = MergeIterator::new(sources);
        let mvcc = MvccIterator::new(merge, u64::MAX);

        mvcc.filter_map(|(ik, entry)| {
            let (key, commit_id) = ik.decode()?;
            // Apply min_commit_id filter if set
            if let Some(min) = min_commit_id {
                if commit_id <= min {
                    return None;
                }
            }
            Some(OwnEntry {
                key,
                value: entry.value.clone(),
                is_tombstone: entry.is_tombstone,
                commit_id,
            })
        })
        .collect()
    }

    /// List entries filtered by type tag at a specific MVCC version, preserving tombstones.
    ///
    /// Unlike `list_by_type()`, this method:
    /// - Uses `max_version` for MVCC visibility instead of `u64::MAX`
    /// - Does NOT filter tombstones -- returns them with `is_tombstone: true`
    /// - Does NOT filter expired entries
    ///
    /// This is required for three-way merge ancestor state reconstruction.
    pub fn list_by_type_at_version(
        &self,
        branch_id: &BranchId,
        type_tag: TypeTag,
        max_version: u64,
    ) -> Vec<VersionedEntry> {
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return Vec::new(),
        };

        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = Vec::new();

        // Active memtable
        let active_entries: Vec<_> = branch.active.iter_all().collect();
        sources.push(Box::new(active_entries.into_iter()));

        // Frozen memtables (newest first)
        for frozen in &branch.frozen {
            let entries: Vec<_> = frozen.iter_all().collect();
            sources.push(Box::new(entries.into_iter()));
        }

        // On-disk segments -- all levels
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

        // Inherited layers (COW branching)
        for layer in &branch.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            for level in &layer.segments.levels {
                for seg in level {
                    let entries: Vec<_> = seg
                        .iter_seek_all()
                        .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                        .collect();
                    sources.push(Box::new(RewritingIterator::new(
                        entries.into_iter(),
                        *branch_id,
                        layer.fork_version,
                    )));
                }
            }
        }

        let merge = MergeIterator::new(sources);
        let mvcc = MvccIterator::new(merge, max_version);

        mvcc.filter_map(|(ik, entry)| {
            let (key, commit_id) = ik.decode()?;
            if key.type_tag != type_tag {
                return None;
            }
            Some(VersionedEntry {
                key,
                value: entry.value.clone(),
                is_tombstone: entry.is_tombstone,
                commit_id,
            })
        })
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

        // Inherited layers (COW branching)
        for layer in &branch.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            for level in &layer.segments.levels {
                for seg in level {
                    let entries: Vec<_> = seg
                        .iter_seek_all()
                        .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                        .collect();
                    sources.push(Box::new(RewritingIterator::new(
                        entries.into_iter(),
                        *branch_id,
                        layer.fork_version,
                    )));
                }
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
            if entry.is_expired_at(max_ts) {
                found_for_current = true;
                continue;
            }
            if entry.timestamp.as_micros() > max_ts {
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
            if entry.timestamp.as_micros() > max_timestamp {
                continue;
            }
            if entry.is_expired_at(max_timestamp) {
                return Ok(None);
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
                if !segment_overlaps_prefix(seg, &prefix_bytes) {
                    continue;
                }
                let entries: Vec<_> = seg
                    .iter_seek(prefix)
                    .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                    .collect();
                sources.push(Box::new(entries.into_iter()));
            }
        }

        // Inherited layers (COW branching)
        for layer in &branch.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            let src_prefix = prefix.with_branch_id(layer.source_branch_id);
            let src_prefix_bytes = encode_typed_key_prefix(&src_prefix);
            for level in &layer.segments.levels {
                for seg in level {
                    if !segment_overlaps_prefix(seg, &src_prefix_bytes) {
                        continue;
                    }
                    let entries: Vec<_> = seg
                        .iter_seek(&src_prefix)
                        .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                        .collect();
                    sources.push(Box::new(RewritingIterator::new(
                        entries.into_iter(),
                        prefix.namespace.branch_id,
                        layer.fork_version,
                    )));
                }
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
            if entry.is_expired_at(max_timestamp) {
                found_for_current = true;
                continue;
            }
            if entry.timestamp.as_micros() > max_timestamp {
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
        let entries = self.list_branch_inner(&branch, branch_id);
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
        branch.track_timestamp(scan_min);
        branch.track_timestamp(scan_max);
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
        ttl_ms: u64,
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
            ttl_ms,
            raw_value: None,
        };
        let ts = entry.timestamp.as_micros();
        branch.active.put_entry(&key, version, entry);
        branch.track_timestamp(ts);

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
            raw_value: None,
        };
        let ts = entry.timestamp.as_micros();
        branch.active.put_entry(key, version, entry);
        branch.track_timestamp(ts);

        self.maybe_rotate_branch(branch_id, &mut branch);
        self.version.fetch_max(version, Ordering::AcqRel);

        Ok(())
    }

    /// Apply puts and deletes atomically during WAL recovery/refresh,
    /// preserving the original commit timestamp (#1699) and deferring the
    /// global version bump until all entries are installed (#1707).
    pub fn apply_recovery_atomic(
        &self,
        writes: Vec<(Key, Value)>,
        deletes: Vec<Key>,
        version: u64,
        timestamp_micros: u64,
        put_ttls: &[u64],
    ) -> StrataResult<()> {
        if writes.is_empty() && deletes.is_empty() {
            return Ok(());
        }

        let timestamp = Timestamp::from_micros(timestamp_micros);
        let ts = timestamp.as_micros();

        // Group puts by branch.
        let mut puts_by_branch: std::collections::HashMap<BranchId, Vec<(Key, Value, u64)>> =
            std::collections::HashMap::new();
        for (i, (key, value)) in writes.into_iter().enumerate() {
            let ttl_ms = put_ttls.get(i).copied().unwrap_or(0);
            puts_by_branch
                .entry(key.namespace.branch_id)
                .or_default()
                .push((key, value, ttl_ms));
        }

        // Group deletes by branch.
        let mut deletes_by_branch: std::collections::HashMap<BranchId, Vec<Key>> =
            std::collections::HashMap::new();
        for key in deletes {
            deletes_by_branch
                .entry(key.namespace.branch_id)
                .or_default()
                .push(key);
        }

        // Install puts.
        for (branch_id, entries) in puts_by_branch {
            let mut branch = self
                .branches
                .entry(branch_id)
                .or_insert_with(BranchState::new);
            for (key, value, ttl_ms) in entries {
                let entry = MemtableEntry {
                    value,
                    is_tombstone: false,
                    timestamp,
                    ttl_ms,
                    raw_value: None,
                };
                branch.active.put_entry(&key, version, entry);
            }
            branch.track_timestamp(ts);
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        // Install tombstones.
        for (branch_id, keys) in deletes_by_branch {
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
                    raw_value: None,
                };
                branch.active.put_entry(&key, version, entry);
            }
            branch.track_timestamp(ts);
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        // Version is NOT advanced here. The caller (Database::refresh) is
        // responsible for calling advance_version() AFTER secondary indexes
        // (BM25/HNSW) have been updated, so readers never observe KV data
        // without corresponding index entries (Issue #1734).
        Ok(())
    }

    /// Get `(entry_count, total_version_count, btree_built)` for a branch.
    ///
    /// SegmentedStore does not use BTreeSet indexes, so `btree_built` is always false.
    pub fn shard_stats_detailed(&self, branch_id: &BranchId) -> Option<(usize, usize, bool)> {
        let branch = self.branches.get(branch_id)?;
        let entry_count = self.list_branch_inner(&branch, *branch_id).len();
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
            let count = self.list_branch_inner(branch, branch_id).len();
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
        let builder = self
            .make_segment_builder()
            .with_compression(crate::segment_builder::CompressionCodec::None);
        builder.build_from_iter(frozen_mt.iter_all(), &seg_path)?;

        let segment = KVSegment::open(&seg_path)?;

        // Atomically: remove the frozen memtable we just flushed and install
        // the segment, under a single DashMap guard.
        let mut branch = self
            .branches
            .entry(*branch_id)
            .or_insert_with(BranchState::new);
        let popped = if let Some(last) = branch.frozen.last() {
            if last.id() == frozen_mt.id() {
                branch.frozen.pop();
                self.total_frozen_count.fetch_sub(1, Ordering::Relaxed);
                true
            } else {
                false
            }
        } else {
            false
        };

        if popped {
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
            refresh_level_targets(&mut branch, self.level_base_bytes());

            // Persist level assignments
            drop(branch);
            self.write_branch_manifest(branch_id);
        } else {
            // Another thread already flushed this memtable; discard the
            // duplicate segment file we just built.
            drop(branch);
            let _ = std::fs::remove_file(&seg_path);
        }

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
            None => return Ok(RecoverSegmentsInfo::default()),
        };

        if !segments_dir.exists() {
            return Ok(RecoverSegmentsInfo::default());
        }

        let mut info = RecoverSegmentsInfo::default();

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
                        // Track max commit_id for version counter restoration (#1726)
                        info.max_commit_id = info.max_commit_id.max(seg.commit_range().1);
                        branch_segments.push(Arc::new(seg));
                    }
                    Err(_) => {
                        info.errors_skipped += 1;
                    }
                }
            }

            // Try to read manifest for level assignments.
            // A corrupt manifest means we cannot safely load this branch's own
            // segments (loading all as L0 would reintroduce orphans — #1680).
            // Skip the branch but do NOT abort: other branches (and children
            // that inherit segments from this branch) can still recover (#1691).
            let manifest = match crate::manifest::read_manifest(&path) {
                Ok(m) => m,
                Err(e) => {
                    tracing::error!(
                        branch = %dir_name,
                        error = %e,
                        "corrupt manifest, skipping branch (own segments not loaded)"
                    );
                    info.corrupt_manifest_branches += 1;
                    continue;
                }
            };

            // Skip branches with no segments and no inherited layers
            let has_inherited = manifest
                .as_ref()
                .is_some_and(|m| !m.inherited_layers.is_empty());
            if branch_segments.is_empty() && !has_inherited {
                continue;
            }

            // Partition segments into levels based on manifest.
            //
            // IMPORTANT (#1701): When a manifest exists, ONLY load segments
            // listed in the manifest. Files on disk but not in the manifest
            // are orphans (e.g. shared segments kept for COW children) and
            // must NOT be promoted to L0, as they could shadow compacted data.
            let mut level_segs: Vec<Vec<Arc<KVSegment>>> = vec![Vec::new(); NUM_LEVELS];
            if let Some(ref manifest) = manifest {
                // Build a map of opened segments by filename for O(1) lookup.
                let seg_by_name: std::collections::HashMap<String, Arc<KVSegment>> =
                    branch_segments
                        .iter()
                        .filter_map(|seg| {
                            seg.file_path()
                                .file_name()
                                .and_then(|n| n.to_str())
                                .map(|name| (name.to_string(), Arc::clone(seg)))
                        })
                        .collect();

                let mut manifest_filenames: std::collections::HashSet<String> =
                    std::collections::HashSet::new();
                for entry in &manifest.entries {
                    manifest_filenames.insert(entry.filename.clone());
                    let level = (entry.level as usize).min(NUM_LEVELS - 1);
                    if let Some(seg) = seg_by_name.get(&entry.filename) {
                        level_segs[level].push(Arc::clone(seg));
                    } else {
                        tracing::warn!(
                            branch = %dir_name,
                            segment = %entry.filename,
                            "manifest references segment not found on disk"
                        );
                    }
                }

                // Log orphan files (on disk but not in manifest — not loaded).
                for seg in branch_segments.iter() {
                    let filename = seg
                        .file_path()
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("");
                    if !manifest_filenames.contains(filename) {
                        tracing::warn!(
                            branch = %dir_name,
                            segment = %filename,
                            "orphan segment on disk (not in manifest), skipping"
                        );
                        info.orphans_skipped += 1;
                    }
                }
            } else {
                // No manifest → all segments go to L0 (backward compat)
                level_segs[0] = branch_segments.clone();
            }

            // L0: sorted by commit_max descending (newest first)
            level_segs[0].sort_by(|a, b| b.commit_range().1.cmp(&a.commit_range().1));
            // L1+: sorted by key_range min ascending
            for level in level_segs.iter_mut().skip(1) {
                level.sort_by(|a, b| a.key_range().0.cmp(b.key_range().0));
            }

            let mut branch = self
                .branches
                .entry(branch_id)
                .or_insert_with(BranchState::new);

            info.segments_loaded += level_segs.iter().map(|l| l.len()).sum::<usize>();

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
            refresh_level_targets(&mut branch, self.level_base_bytes());

            // Collect inherited layer info for second pass (deferred resolution).
            if let Some(manifest) = manifest {
                if !manifest.inherited_layers.is_empty() {
                    deferred_inherited.push((branch_id, manifest.inherited_layers));
                }
            }

            info.branches_recovered += 1;
        }

        // Second pass + refcount rebuild
        self.resolve_inherited_layers(segments_dir, deferred_inherited, &mut info);

        Ok(info)
    }

    /// Second pass of recovery: resolve inherited layers from manifests and
    /// rebuild segment refcounts.
    fn resolve_inherited_layers(
        &self,
        segments_dir: &std::path::Path,
        deferred_inherited: Vec<(BranchId, Vec<crate::manifest::ManifestInheritedLayer>)>,
        info: &mut RecoverSegmentsInfo,
    ) {
        for (child_id, manifest_layers) in deferred_inherited {
            let mut inherited_layers = Vec::new();

            for ml in &manifest_layers {
                let source_dir = segments_dir.join(hex_encode_branch(&ml.source_branch_id));
                let mut layer_levels: Vec<Vec<Arc<KVSegment>>> = vec![Vec::new(); NUM_LEVELS];
                let mut any_found = false;

                for entry in &ml.entries {
                    let level = (entry.level as usize).min(NUM_LEVELS - 1);
                    let seg_path = source_dir.join(&entry.filename);
                    match KVSegment::open(&seg_path) {
                        Ok(seg) => {
                            if let Some(stem) = seg_path.file_stem().and_then(|s| s.to_str()) {
                                if let Ok(file_seg_id) = stem.parse::<u64>() {
                                    self.next_segment_id
                                        .fetch_max(file_seg_id + 1, Ordering::Relaxed);
                                }
                            }
                            info.max_commit_id = info.max_commit_id.max(seg.commit_range().1);
                            layer_levels[level].push(Arc::new(seg));
                            any_found = true;
                        }
                        Err(e) => {
                            tracing::warn!(
                                child = %hex_encode_branch(&child_id),
                                source = %hex_encode_branch(&ml.source_branch_id),
                                segment = %entry.filename,
                                error = %e,
                                "inherited layer segment not found on disk"
                            );
                        }
                    }
                }

                if !any_found {
                    info.layers_dropped.push((child_id, ml.source_branch_id));
                    continue;
                }

                layer_levels[0].sort_by(|a, b| b.commit_range().1.cmp(&a.commit_range().1));
                for level in layer_levels.iter_mut().skip(1) {
                    level.sort_by(|a, b| a.key_range().0.cmp(b.key_range().0));
                }

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
                    segments: Arc::new(SegmentVersion {
                        levels: layer_levels,
                    }),
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
    }

    /// Check whether the active memtable should be rotated and do so inline.
    ///
    /// Called after every write within the DashMap entry guard.
    #[inline]
    fn maybe_rotate_branch(&self, branch_id: BranchId, branch: &mut BranchState) {
        let wbs = self.write_buffer_size.load(Ordering::Relaxed);
        if wbs > 0
            && branch.active.approx_bytes() >= wbs
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
    ///
    /// Currently unused in production — will be wired into the background
    /// flush/compaction scheduler to trigger adaptive write stalling.
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
    ) -> StrataResult<Option<(u64, MemtableEntry)>> {
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
            return Ok(Some(result));
        }

        // 2. Frozen memtables (newest first)
        for frozen in &branch.frozen {
            if let Some(result) =
                frozen.get_versioned_preencoded(&typed_key, seek_bytes, max_version)
            {
                return Ok(Some(result));
            }
        }

        // 3. L0 segments (newest first, overlapping — linear scan)
        let ver = branch.version.load();
        for seg in ver.l0_segments() {
            if let Some(se) = seg.point_lookup_preencoded(&typed_key, seek_bytes, max_version)? {
                let commit_id = se.commit_id;
                return Ok(Some((commit_id, segment_entry_to_memtable_entry(se))));
            }
        }

        // 4. L1+ segments (non-overlapping, sorted by key range — binary search per level)
        for level_idx in 1..ver.levels.len() {
            if let Some(se) = point_lookup_level_preencoded(
                &ver.levels[level_idx],
                &typed_key,
                seek_bytes,
                max_version,
            )? {
                let commit_id = se.commit_id;
                return Ok(Some((commit_id, segment_entry_to_memtable_entry(se))));
            }
        }

        // 5. Inherited layers (COW branching — nearest ancestor first)
        for layer in &branch.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            let effective_version = max_version.min(layer.fork_version);
            // Rewrite typed_key: child branch_id → source branch_id
            let Some(src_typed_key) = rewrite_branch_id_bytes(&typed_key, &layer.source_branch_id)
            else {
                continue; // Skip layer if key is corrupt
            };
            let src_seek_ik = InternalKey::from_typed_key_bytes(&src_typed_key, u64::MAX);
            let src_seek_bytes = src_seek_ik.as_bytes();

            // L0 segments (linear scan)
            for seg in layer.segments.l0_segments() {
                if let Some(se) =
                    seg.point_lookup_preencoded(&src_typed_key, src_seek_bytes, effective_version)?
                {
                    let commit_id = se.commit_id;
                    return Ok(Some((commit_id, segment_entry_to_memtable_entry(se))));
                }
            }

            // L1+ segments (binary search per level)
            for level_idx in 1..layer.segments.levels.len() {
                if let Some(se) = point_lookup_level_preencoded(
                    &layer.segments.levels[level_idx],
                    &src_typed_key,
                    src_seek_bytes,
                    effective_version,
                )? {
                    let commit_id = se.commit_id;
                    return Ok(Some((commit_id, segment_entry_to_memtable_entry(se))));
                }
            }
        }

        Ok(None)
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

        // Inherited layers (COW branching)
        for layer in &branch.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            let src_key = key.with_branch_id(layer.source_branch_id);
            let src_typed_key = encode_typed_key(&src_key);
            for level in &layer.segments.levels {
                for seg in level {
                    for (ik, se) in seg.iter_seek(&src_key) {
                        if ik.typed_key_prefix() != src_typed_key.as_slice() {
                            break;
                        }
                        if se.commit_id <= layer.fork_version {
                            all_versions.push((se.commit_id, segment_entry_to_memtable_entry(se)));
                        }
                    }
                }
            }
        }

        // Sort descending by commit_id (newest first) and deduplicate.
        // After recovery, the same (key, commit_id) may exist in both memtable
        // (from WAL replay) and segments (from disk), producing duplicates (#1733).
        all_versions.sort_by(|a, b| b.0.cmp(&a.0));
        all_versions.dedup_by_key(|entry| entry.0);
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
                if !segment_overlaps_prefix(seg, &prefix_bytes) {
                    continue;
                }
                let entries: Vec<_> = seg
                    .iter_seek(prefix)
                    .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                    .collect();
                sources.push(Box::new(entries.into_iter()));
            }
        }

        // Inherited layers (COW branching)
        for layer in &branch.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            let src_prefix = prefix.with_branch_id(layer.source_branch_id);
            let src_prefix_bytes = encode_typed_key_prefix(&src_prefix);
            for level in &layer.segments.levels {
                for seg in level {
                    if !segment_overlaps_prefix(seg, &src_prefix_bytes) {
                        continue;
                    }
                    let entries: Vec<_> = seg
                        .iter_seek(&src_prefix)
                        .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                        .collect();
                    sources.push(Box::new(RewritingIterator::new(
                        entries.into_iter(),
                        prefix.namespace.branch_id,
                        layer.fork_version,
                    )));
                }
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

        // Ensure the branch directory exists (needed for COW forks with
        // no own segments but inherited layers that need a manifest).
        if let Err(e) = std::fs::create_dir_all(&branch_dir) {
            tracing::warn!(
                branch = %branch_hex,
                error = %e,
                "failed to create branch directory for manifest"
            );
            return;
        }

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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecoverSegmentsInfo {
    /// Number of branch subdirectories successfully loaded.
    pub branches_recovered: usize,
    /// Total number of `.sst` segments loaded across all branches.
    pub segments_loaded: usize,
    /// Number of `.sst` files that failed to open (corrupt/invalid).
    pub errors_skipped: usize,
    /// Inherited layers that were dropped because their source branch
    /// was missing. Each entry is `(child_branch_id, source_branch_id)`.
    /// Non-empty means the child branch may be missing data that was
    /// visible before the crash.
    pub layers_dropped: Vec<(BranchId, BranchId)>,
    /// Number of orphan `.sst` files on disk that were not in the manifest.
    /// These are skipped during recovery to prevent stale data from shadowing
    /// compacted data (#1701).
    pub orphans_skipped: usize,
    /// Number of branches skipped because their manifest was corrupt (#1691).
    /// Own segments for these branches are not loaded, but their segment
    /// files remain on disk for children to open directly.
    pub corrupt_manifest_branches: usize,
    /// Maximum commit_id seen across all loaded segments (#1726).
    /// Used to bump the version counter so new transactions don't collide
    /// with data already persisted in segments.
    pub max_commit_id: u64,
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

        match Self::get_versioned_from_branch(&branch, key, max_version)? {
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
            raw_value: None,
        };
        let ts = entry.timestamp.as_micros();
        branch.active.put_entry(&key, version, entry);
        // Track timestamp for O(1) time_range
        branch.track_timestamp(ts);

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
            raw_value: None,
        };
        let ts = entry.timestamp.as_micros();
        branch.active.put_entry(key, version, entry);
        // Track timestamp for O(1) time_range
        branch.track_timestamp(ts);

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
                    raw_value: None,
                };
                branch.active.put_entry(&key, version, entry);
            }
            // Track timestamp for O(1) time_range
            branch.track_timestamp(ts);
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
                    raw_value: None,
                };
                branch.active.put_entry(&key, version, entry);
            }
            // Track timestamp for O(1) time_range
            branch.track_timestamp(ts);
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        self.version.fetch_max(version, Ordering::AcqRel);
        Ok(())
    }

    fn apply_writes_atomic(
        &self,
        writes: Vec<(Key, Value, WriteMode)>,
        deletes: Vec<Key>,
        version: u64,
        put_ttls: &[u64],
    ) -> StrataResult<()> {
        if writes.is_empty() && deletes.is_empty() {
            return Ok(());
        }

        // Group puts and deletes by branch — acquire each DashMap guard once
        // per branch instead of per entry.
        let mut puts_by_branch: std::collections::HashMap<BranchId, Vec<(Key, Value, u64)>> =
            std::collections::HashMap::new();
        for (i, (key, value, _mode)) in writes.into_iter().enumerate() {
            let ttl_ms = put_ttls.get(i).copied().unwrap_or(0);
            puts_by_branch
                .entry(key.namespace.branch_id)
                .or_default()
                .push((key, value, ttl_ms));
        }
        let mut deletes_by_branch: std::collections::HashMap<BranchId, Vec<Key>> =
            std::collections::HashMap::new();
        for key in deletes {
            deletes_by_branch
                .entry(key.namespace.branch_id)
                .or_default()
                .push(key);
        }

        let timestamp = Timestamp::now();
        let ts = timestamp.as_micros();

        // Install puts.
        for (branch_id, entries) in puts_by_branch {
            let mut branch = self
                .branches
                .entry(branch_id)
                .or_insert_with(BranchState::new);
            for (key, value, ttl_ms) in entries {
                let entry = MemtableEntry {
                    value,
                    is_tombstone: false,
                    timestamp,
                    ttl_ms,
                    raw_value: None,
                };
                branch.active.put_entry(&key, version, entry);
            }
            branch.track_timestamp(ts);
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        // Install tombstones.
        for (branch_id, keys) in deletes_by_branch {
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
                    raw_value: None,
                };
                branch.active.put_entry(&key, version, entry);
            }
            branch.track_timestamp(ts);
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        // Advance version only after ALL entries are installed.
        self.version.fetch_max(version, Ordering::AcqRel);
        Ok(())
    }

    fn get_version_only(&self, key: &Key) -> StrataResult<Option<u64>> {
        let branch_id = key.namespace.branch_id;
        let branch = match self.branches.get(&branch_id) {
            Some(b) => b,
            None => return Ok(None),
        };

        match Self::get_versioned_from_branch(&branch, key, u64::MAX)? {
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
) -> StrataResult<Option<SegmentEntry>> {
    let typed_key = encode_typed_key(key);
    let seek_ik = InternalKey::from_typed_key_bytes(&typed_key, u64::MAX);
    point_lookup_level_preencoded(l1_segments, &typed_key, seek_ik.as_bytes(), max_version)
}

fn point_lookup_level_preencoded(
    l1_segments: &[Arc<KVSegment>],
    typed_key: &[u8],
    seek_bytes: &[u8],
    max_version: u64,
) -> StrataResult<Option<SegmentEntry>> {
    if l1_segments.is_empty() {
        return Ok(None);
    }

    let idx = l1_segments.partition_point(|seg| {
        let (_, max_ik) = seg.key_range();
        if max_ik.len() < COMMIT_ID_SUFFIX_LEN {
            return true;
        }
        let max_prefix = &max_ik[..max_ik.len() - COMMIT_ID_SUFFIX_LEN];
        max_prefix < typed_key
    });

    if idx >= l1_segments.len() {
        return Ok(None);
    }

    let seg = &l1_segments[idx];
    let (min_ik, _) = seg.key_range();
    if min_ik.len() >= COMMIT_ID_SUFFIX_LEN {
        let min_prefix = &min_ik[..min_ik.len() - COMMIT_ID_SUFFIX_LEN];
        if min_prefix > typed_key {
            return Ok(None);
        }
    }

    seg.point_lookup_preencoded(typed_key, seek_bytes, max_version)
}

/// Check whether a segment's key range overlaps a typed-key prefix.
///
/// Returns `true` if the segment might contain entries matching the prefix,
/// `false` if it definitely does not. Used to skip non-overlapping segments
/// during prefix scans and list operations.
fn segment_overlaps_prefix(seg: &KVSegment, prefix_bytes: &[u8]) -> bool {
    let (seg_min, seg_max) = seg.key_range();
    if seg_max.len() >= COMMIT_ID_SUFFIX_LEN && seg_min.len() >= COMMIT_ID_SUFFIX_LEN {
        let max_typed = &seg_max[..seg_max.len() - COMMIT_ID_SUFFIX_LEN];
        let min_typed = &seg_min[..seg_min.len() - COMMIT_ID_SUFFIX_LEN];
        if max_typed < prefix_bytes {
            return false;
        }
        if !min_typed.starts_with(prefix_bytes) && min_typed > prefix_bytes {
            return false;
        }
    }
    true
}

/// Convert a `SegmentEntry` into a `MemtableEntry` for the merge path.
pub(crate) fn segment_entry_to_memtable_entry(se: SegmentEntry) -> MemtableEntry {
    MemtableEntry {
        value: se.value,
        is_tombstone: se.is_tombstone,
        timestamp: Timestamp::from_micros(se.timestamp),
        ttl_ms: se.ttl_ms,
        raw_value: se.raw_value,
    }
}

mod compaction;
mod ref_registry;

#[cfg(test)]
mod tests;
