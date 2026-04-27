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
use crate::seekable::{self, SeekableIterator as _};
use crate::segment::{KVSegment, LevelSegmentIter, OwnedSegmentIter, SegmentEntry};
use crate::segment_builder::{SegmentBuilder, SplittingSegmentBuilder};
use crate::{BranchOp, StorageError, StorageResult};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

// ── Read-path profiling (STRATA_PROFILE_READ=1) ────────────────────────────

static READ_PROFILE_ENABLED: AtomicBool = AtomicBool::new(false);
static READ_PROFILE_CHECKED: AtomicBool = AtomicBool::new(false);

fn read_profile_enabled() -> bool {
    if !READ_PROFILE_CHECKED.load(Ordering::Relaxed) {
        let enabled = std::env::var("STRATA_PROFILE_READ").is_ok();
        READ_PROFILE_ENABLED.store(enabled, Ordering::Relaxed);
        READ_PROFILE_CHECKED.store(true, Ordering::Relaxed);
    }
    READ_PROFILE_ENABLED.load(Ordering::Relaxed)
}

const READ_PROFILE_INTERVAL: u64 = 100_000;

struct ReadProfile {
    count: u64,
    found_active: u64,
    found_frozen: u64,
    found_l0: u64,
    found_l1plus: u64,
    found_inherited: u64,
    not_found: u64,
    l0_probes: u64,
    snapshot_ns: u64,
    key_encode_ns: u64,
    memtable_ns: u64,
    l0_ns: u64,
    l1plus_ns: u64,
    total_ns: u64,
    // Per-level detail
    levels_checked: u64,
    bloom_rejects: u64,
    bloom_passes: u64,
}

thread_local! {
    static READ_PROF: RefCell<ReadProfile> = const { RefCell::new(ReadProfile {
        count: 0,
        found_active: 0, found_frozen: 0, found_l0: 0, found_l1plus: 0,
        found_inherited: 0, not_found: 0, l0_probes: 0,
        snapshot_ns: 0, key_encode_ns: 0, memtable_ns: 0,
        l0_ns: 0, l1plus_ns: 0, total_ns: 0,
        levels_checked: 0, bloom_rejects: 0, bloom_passes: 0,
    }) };
}

use strata_core::id::CommitVersion;
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
/// Used by three-way merge to reconstruct ancestor state including deletions,
/// and by checkpoint collection to carry retention metadata (timestamp, TTL)
/// into snapshot DTOs without a second storage pass.
#[derive(Debug, Clone)]
pub struct VersionedEntry {
    /// The decoded user key.
    pub key: Key,
    /// The stored value (empty for tombstones).
    pub value: Value,
    /// Whether this entry is a deletion tombstone.
    pub is_tombstone: bool,
    /// The MVCC commit version that wrote this entry.
    pub commit_id: CommitVersion,
    /// Original commit timestamp in microseconds.
    pub timestamp_micros: u64,
    /// Per-key TTL in milliseconds (0 = no expiry).
    pub ttl_ms: u64,
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
    pub commit_id: CommitVersion,
}

/// Decoded snapshot payload for a single logical key.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedSnapshotValue {
    /// A live value restored from the snapshot.
    Value(Value),
    /// A tombstone restored from the snapshot.
    Tombstone,
}

/// Decoded snapshot entry installed into storage during recovery.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedSnapshotEntry {
    /// Namespace space name for the logical key.
    pub space: String,
    /// User-key bytes for the logical key.
    pub user_key: Vec<u8>,
    /// Value or tombstone payload.
    pub payload: DecodedSnapshotValue,
    /// MVCC commit version carried by the snapshot.
    pub version: CommitVersion,
    /// Original commit timestamp in microseconds.
    pub timestamp_micros: u64,
    /// Per-key TTL in milliseconds, when known.
    pub ttl_ms: u64,
}

// ---------------------------------------------------------------------------
// SegmentVersion
// ---------------------------------------------------------------------------

/// Immutable snapshot of a branch's on-disk segments.
///
/// Wrapped in `ArcSwap` so that segment list mutations (flush, compaction)
/// are atomic single-pointer swaps — readers never see a partial state.
pub(crate) struct SegmentVersion {
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
pub(crate) enum LayerStatus {
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
#[derive(Clone)]
pub(crate) struct InheritedLayer {
    /// Branch ID of the source (parent) branch.
    source_branch_id: BranchId,
    /// Version counter of the source branch at fork time.
    fork_version: CommitVersion,
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
    /// Wrapped in `Arc` to enable `BranchSnapshot` capture without holding
    /// the DashMap guard (analogous to RocksDB's SuperVersion pinning).
    active: Arc<Memtable>,
    /// Frozen memtables, newest first.  Immutable, pending flush.
    frozen: Vec<Arc<Memtable>>,
    /// On-disk segment version (atomic swap for lock-free reads).
    version: ArcSwap<SegmentVersion>,
    /// Minimum timestamp seen across all writes (for O(1) time_range).
    min_timestamp: AtomicU64,
    /// Maximum timestamp seen across all writes (for O(1) time_range).
    max_timestamp: AtomicU64,
    /// Highest commit_id (version) applied to this branch.
    /// Used by fork_branch to compute fork_version from actual branch data
    /// rather than the global counter (which may include allocated-but-not-yet-applied
    /// versions from other branches).
    max_version: AtomicU64,
    /// Per-level compact pointer for round-robin file selection.
    /// Each entry is the largest typed_key_prefix of the last compaction input.
    compact_pointers: Vec<Option<Vec<u8>>>,
    /// Cached per-level byte targets, recomputed after compaction/flush.
    level_targets: compaction::LevelTargets,
    /// Inherited layers from parent branches (COW fork).
    /// Empty until Epic C enables fork_branch.
    inherited_layers: Vec<InheritedLayer>,
    /// Total non-deletion entries written to this branch (all versions).
    /// Incremented on every put. Used with `num_deletions` to estimate
    /// live key count in O(1) — same algorithm as RocksDB's EstimateNumKeys.
    num_entries: AtomicU64,
    /// Total deletion tombstones written to this branch.
    /// Incremented on every delete. See `num_entries`.
    num_deletions: AtomicU64,
}

impl BranchState {
    fn new() -> Self {
        Self {
            active: Arc::new(Memtable::new(0)),
            frozen: Vec::new(),
            version: ArcSwap::from_pointee(SegmentVersion::new()),
            min_timestamp: AtomicU64::new(u64::MAX),
            max_timestamp: AtomicU64::new(0),
            max_version: AtomicU64::new(0),
            compact_pointers: vec![None; NUM_LEVELS],
            level_targets: compaction::recalculate_level_targets(
                &[0u64; NUM_LEVELS],
                LEVEL_BASE_BYTES,
            ),
            inherited_layers: Vec::new(),
            num_entries: AtomicU64::new(0),
            num_deletions: AtomicU64::new(0),
        }
    }

    /// Estimate the number of live keys.
    ///
    /// `result = entries - deletions` (clamped to 0).
    /// `num_entries` counts only non-tombstone puts; `num_deletions` counts
    /// only tombstone writes. Approximate: overcounts when the same key is
    /// updated multiple times (each update increments `num_entries`).
    /// Inspired by RocksDB's EstimateNumKeys.
    fn estimate_live_keys(&self) -> u64 {
        let entries = self.num_entries.load(Ordering::Relaxed);
        let deletions = self.num_deletions.load(Ordering::Relaxed);
        entries.saturating_sub(deletions)
    }

    /// Update min/max timestamp tracking for a single write.
    #[inline]
    fn track_timestamp(&self, ts: u64) {
        self.min_timestamp.fetch_min(ts, Ordering::Relaxed);
        self.max_timestamp.fetch_max(ts, Ordering::Relaxed);
    }

    /// Update max applied version for this branch.
    #[inline]
    fn track_version(&self, version: CommitVersion) {
        self.max_version
            .fetch_max(version.as_u64(), Ordering::Release);
    }
}

/// Captured state of a branch at a point in time.
///
/// Holds `Arc` references to prevent memtable rotation and compaction from
/// invalidating the data. Analogous to RocksDB's `SuperVersion`.
///
/// Captured under a short DashMap read guard via `Arc` clones. The guard
/// is released immediately — the snapshot remains valid independently.
pub(crate) struct BranchSnapshot {
    /// Active memtable at capture time.
    pub(crate) active: Arc<Memtable>,
    /// Frozen memtables at capture time (newest first).
    pub(crate) frozen: Vec<Arc<Memtable>>,
    /// Segment version at capture time.
    pub(crate) segments: Arc<SegmentVersion>,
    /// Inherited COW layers at capture time.
    pub(crate) inherited_layers: Vec<InheritedLayer>,
}

/// Persistent, seekable iterator over a branch snapshot.
///
/// Analogous to RocksDB's `ArenaWrappedDBIter`. Supports multiple
/// `seek()` + `next()` cycles without re-acquiring the DashMap guard.
/// The [`BranchSnapshot`] pins memtables and segments for the iterator's
/// lifetime.
///
/// On first `seek()`, builds the full seekable pipeline (memtable +
/// segment + merge + MVCC children). On subsequent seeks, re-seeks
/// children in place via [`SeekableIterator::seek()`] — children
/// persist across seeks, matching RocksDB's `MergingIterator` pattern.
pub struct StorageIterator {
    snapshot: BranchSnapshot,
    prefix: Key,
    snapshot_version: CommitVersion,
    /// Seekable pipeline: built on first seek, re-seeked on subsequent seeks.
    pipeline: Option<seekable::MvccSeekableIter>,
    /// Current decoded entry (tombstone/expiry filtering applied).
    current: Option<(Key, VersionedValue)>,
}

impl StorageIterator {
    fn new(snapshot: BranchSnapshot, prefix: Key, snapshot_version: CommitVersion) -> Self {
        Self {
            snapshot,
            prefix,
            snapshot_version,
            pipeline: None,
            current: None,
        }
    }

    /// Seek to first entry >= `target` within the prefix.
    ///
    /// On first call, builds the seekable pipeline from the snapshot.
    /// On subsequent calls, re-seeks existing children in place —
    /// segment iterators are repositioned via in-memory index (O(log B)),
    /// memtable entries are re-collected from the seek position.
    pub fn seek(&mut self, target: &Key) -> StorageResult<()> {
        let target_ik = InternalKey::encode(target, CommitVersion::MAX);

        if let Some(ref mut pipeline) = self.pipeline {
            // Re-seek existing pipeline — children persist
            pipeline.seek(target_ik.as_bytes());
        } else {
            // First seek: build the pipeline
            let mut pipeline = self.build_seekable_pipeline();
            pipeline.seek(target_ik.as_bytes());
            self.pipeline = Some(pipeline);
        }

        // Position at first live entry >= target
        self.advance_to_live(target);
        Ok(())
    }

    /// Build the seekable pipeline from the snapshot.
    fn build_seekable_pipeline(&self) -> seekable::MvccSeekableIter {
        use seekable::*;

        let mut children: Vec<Box<dyn SeekableIterator>> = Vec::new();
        let prefix_bytes = encode_typed_key_prefix(&self.prefix);

        // Active memtable
        children.push(Box::new(MemtableSeekableIter::new(
            Arc::clone(&self.snapshot.active),
            self.prefix.clone(),
        )));

        // Frozen memtables (newest first)
        for frozen in &self.snapshot.frozen {
            children.push(Box::new(MemtableSeekableIter::new(
                Arc::clone(frozen),
                self.prefix.clone(),
            )));
        }

        // L0 segments — individual iterators (overlapping)
        if !self.snapshot.segments.levels.is_empty() {
            for seg in &self.snapshot.segments.levels[0] {
                if !segment_overlaps_prefix(seg, &prefix_bytes) {
                    continue;
                }
                let flag = Arc::new(AtomicBool::new(false));
                children.push(Box::new(SegmentSeekableIter::new(
                    Arc::clone(seg),
                    prefix_bytes.clone(),
                    flag,
                )));
            }
        }

        // L1+ levels — one LevelSeekableIter per level (non-overlapping)
        for level in self.snapshot.segments.levels.iter().skip(1) {
            if level.is_empty() {
                continue;
            }
            let level_segs: Vec<_> = level
                .iter()
                .filter(|s| segment_overlaps_prefix(s, &prefix_bytes))
                .cloned()
                .collect();
            if level_segs.is_empty() {
                continue;
            }
            let flag = Arc::new(AtomicBool::new(false));
            children.push(Box::new(LevelSeekableIter::new(
                level_segs,
                prefix_bytes.clone(),
                flag,
            )));
        }

        // Inherited layers (COW)
        for layer in &self.snapshot.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            let src_prefix = self.prefix.with_branch_id(layer.source_branch_id);
            let src_prefix_bytes = encode_typed_key_prefix(&src_prefix);

            // L0 segments from inherited layer
            if !layer.segments.levels.is_empty() {
                for seg in &layer.segments.levels[0] {
                    if !segment_overlaps_prefix(seg, &src_prefix_bytes) {
                        continue;
                    }
                    let flag = Arc::new(AtomicBool::new(false));
                    let seg_iter = Box::new(SegmentSeekableIter::new(
                        Arc::clone(seg),
                        src_prefix_bytes.clone(),
                        flag,
                    ));
                    children.push(Box::new(RewritingSeekableIter::new(
                        seg_iter,
                        self.prefix.namespace.branch_id,
                        layer.source_branch_id,
                        layer.fork_version,
                    )));
                }
            }

            // L1+ segments from inherited layer
            for level in layer.segments.levels.iter().skip(1) {
                if level.is_empty() {
                    continue;
                }
                let level_segs: Vec<_> = level
                    .iter()
                    .filter(|s| segment_overlaps_prefix(s, &src_prefix_bytes))
                    .cloned()
                    .collect();
                if level_segs.is_empty() {
                    continue;
                }
                let flag = Arc::new(AtomicBool::new(false));
                let level_iter = Box::new(LevelSeekableIter::new(
                    level_segs,
                    src_prefix_bytes.clone(),
                    flag,
                ));
                children.push(Box::new(RewritingSeekableIter::new(
                    level_iter,
                    self.prefix.namespace.branch_id,
                    layer.source_branch_id,
                    layer.fork_version,
                )));
            }
        }

        let merge = seekable::MergeSeekableIter::new(children);
        seekable::MvccSeekableIter::new(merge, self.snapshot_version)
    }

    /// Advance the pipeline to the next live (non-tombstone, non-expired)
    /// entry >= target.
    fn advance_to_live(&mut self, target: &Key) {
        match self.pipeline.as_ref() {
            Some(p) if p.valid() => {}
            _ => {
                self.current = None;
                return;
            }
        }

        let pipeline = self.pipeline.as_mut().unwrap();
        loop {
            if !pipeline.valid() {
                self.current = None;
                return;
            }
            let ik = pipeline.current_key();
            let entry = pipeline.current_entry();

            if entry.is_tombstone || entry.is_expired() {
                pipeline.advance();
                continue;
            }

            if let Some((key, commit_id)) = ik.decode() {
                if &key >= target {
                    self.current = Some((key, entry.to_versioned(commit_id)));
                    pipeline.advance();
                    return;
                }
            }
            pipeline.advance();
        }
    }

    /// Advance to the next live entry. O(log K) per call where K = source count.
    ///
    /// Returns `None` when exhausted. Call `check_corruption()` after iteration
    /// to detect any segment data block corruption encountered during reads.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(Key, VersionedValue)> {
        let result = self.current.take()?;

        // Pre-load next live entry
        let pipeline = self.pipeline.as_mut()?;
        loop {
            if !pipeline.valid() {
                break;
            }
            let ik = pipeline.current_key();
            let entry = pipeline.current_entry();

            if entry.is_tombstone || entry.is_expired() {
                pipeline.advance();
                continue;
            }

            if let Some((key, commit_id)) = ik.decode() {
                self.current = Some((key, entry.to_versioned(commit_id)));
                pipeline.advance();
                break;
            }
            pipeline.advance();
        }

        Some(result)
    }

    /// Check if any segment reported corruption during iteration.
    pub fn check_corruption(&self) -> StorageResult<()> {
        if let Some(ref pipeline) = self.pipeline {
            if pipeline.corruption_detected() {
                return Err(StorageError::corruption(
                    "Segment data block corruption detected during iteration",
                ));
            }
        }
        Ok(())
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
    /// Latched publication durability issue for this process.
    publish_health: Mutex<Option<PublishHealth>>,
    /// Classified health from the most recent `recover_segments()` call.
    /// Initialized to [`RecoveryHealth::Healthy`] — a fresh or ephemeral
    /// store has nothing to have gone wrong. Read by SE3's GC refusal and
    /// D4's engine-side health accessor; written only by `recover_segments`.
    last_recovery_health: ArcSwap<RecoveryHealth>,
    /// `recover_segments()` is a one-shot bootstrap primitive. Once it
    /// successfully installs recovered state, calling it again on the same
    /// store instance would duplicate recovered segments and refcounts.
    recovery_applied: AtomicBool,
}

/// Latched storage publication durability issue for the current process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishHealth {
    /// First time the issue was observed in this process.
    pub first_observed_at: SystemTime,
    /// Human-readable detail for operator/debugging surfaces.
    pub message: String,
}

enum PublishOutcome {
    Durable,
    NotDurable(StorageError),
}

/// Internal stats produced by `resolve_inherited_layers` for its caller to
/// fold into the final `RecoveredState`. Scope is intentionally narrow —
/// only the fields `recover_segments` needs to aggregate.
#[derive(Default)]
struct InheritedResolveStats {
    max_commit_id: CommitVersion,
    segments_loaded: usize,
}

struct RecoveryInvocationGuard<'a> {
    flag: &'a AtomicBool,
    committed: bool,
}

impl<'a> RecoveryInvocationGuard<'a> {
    fn begin(flag: &'a AtomicBool) -> StorageResult<Self> {
        flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| StorageError::RecoveryAlreadyApplied)?;
        Ok(Self {
            flag,
            committed: false,
        })
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for RecoveryInvocationGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.flag.store(false, Ordering::Release);
        }
    }
}

fn missing_segments_dir_error(path: &std::path::Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("segments directory {} is missing", path.display()),
    )
}

/// Outcome of [`SegmentedStore::gc_orphan_segments`].
///
/// A successful call reports how many orphan `.sst` files were actually
/// reaped. A refusal surfaces as [`StorageError::GcRefusedDegradedRecovery`]
/// on the `Err` arm and does not produce a [`GcReport`].
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct GcReport {
    /// Number of orphan segment files removed from disk in this call.
    pub files_deleted: usize,
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
            publish_health: Mutex::new(None),
            last_recovery_health: ArcSwap::from_pointee(RecoveryHealth::Healthy),
            recovery_applied: AtomicBool::new(false),
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
            publish_health: Mutex::new(None),
            last_recovery_health: ArcSwap::from_pointee(RecoveryHealth::Healthy),
            recovery_applied: AtomicBool::new(false),
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
            publish_health: Mutex::new(None),
            last_recovery_health: ArcSwap::from_pointee(RecoveryHealth::Healthy),
            recovery_applied: AtomicBool::new(false),
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

    /// Classified health of the most recent attempted `recover_segments()`
    /// call.
    ///
    /// Returns [`RecoveryHealth::Healthy`] for stores that have never run
    /// recovery. Hard pre-walk I/O failures also publish a degraded snapshot
    /// here before `recover_segments()` returns `Err`. SE3 uses this to refuse
    /// orphan GC under degraded recovery; D4 exposes it through the engine's
    /// public `recovery_health()` accessor. The returned `Arc` is a snapshot
    /// — subsequent recovery calls replace the stored value but do not
    /// invalidate this handle.
    ///
    /// ## B5 retention contract
    ///
    /// Surfaces the §"Recovery-health contract" classification of
    /// `docs/design/branching/branching-gc/branching-retention-contract.md`.
    /// This is the `BarrierKind::RecoveryHealthGate` value the
    /// reclaim path consults: `Healthy` and
    /// `Degraded { class: Telemetry, .. }` permit reclaim;
    /// `DataLoss` and `PolicyDowngrade` block reclaim per Invariant
    /// 5 + KD8. Promoted from storage-local behavior to branch-layer
    /// contract — non-negotiable.
    pub fn last_recovery_health(&self) -> Arc<RecoveryHealth> {
        self.last_recovery_health.load_full()
    }

    /// Reset recovery health back to [`RecoveryHealth::Healthy`] when an
    /// in-place reset is actually safe.
    ///
    /// **Destructive admin.** S4 per the durability-storage-closure plan.
    /// The reset is intentionally narrower than "clear whatever happened":
    ///
    /// - `recover_segments()` must have successfully installed this store's
    ///   branch/refcount view. Hard pre-walk failures never reached an
    ///   authoritative in-memory snapshot, so GC cannot be re-enabled on the
    ///   same instance.
    /// - `DataLoss` degradations require a fresh reopen after reconciliation.
    ///   This store's recovery view is one-shot; if an operator restores a
    ///   manifest or `.sst` after reopen, the same instance cannot reload it,
    ///   and clearing the health bit would let GC delete the restored file.
    ///
    /// `PolicyDowngrade` can be cleared in-place after a successful recovery
    /// because the store already loaded every discovered `.sst` into its live
    /// graph. `Telemetry` does not block GC in the first place.
    pub fn reset_recovery_health(&self) -> StorageResult<()> {
        let health = self.last_recovery_health.load_full();
        match &*health {
            RecoveryHealth::Healthy => Ok(()),
            RecoveryHealth::Degraded {
                class: DegradationClass::PolicyDowngrade,
                ..
            } => {
                if !self.recovery_applied.load(Ordering::Acquire) {
                    return Err(StorageError::RecoveryHealthResetRequiresSuccessfulRecovery);
                }

                self.last_recovery_health
                    .store(Arc::new(RecoveryHealth::Healthy));
                Ok(())
            }
            RecoveryHealth::Degraded { class, .. } => {
                if !self.recovery_applied.load(Ordering::Acquire) {
                    return Err(StorageError::RecoveryHealthResetRequiresSuccessfulRecovery);
                }

                Err(StorageError::RecoveryHealthResetRequiresReopen { class: *class })
            }
        }
    }

    /// Test-only hook: install an arbitrary [`RecoveryHealth`] value so tests
    /// can exercise the GC refusal and telemetry-passthrough paths without
    /// driving a full recovery.
    #[cfg(test)]
    pub(crate) fn set_recovery_health_for_test(&self, health: RecoveryHealth) {
        self.last_recovery_health.store(Arc::new(health));
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
    pub fn set_version(&self, version: CommitVersion) {
        self.version.store(version.as_u64(), Ordering::Release);
    }

    /// Monotonically advance the visible storage version.
    ///
    /// Used by the follower refresh path to bump version AFTER secondary
    /// indexes (BM25/HNSW) have been updated, ensuring readers never see
    /// KV data before the corresponding index entries exist (Issue #1734).
    pub fn advance_version(&self, version: CommitVersion) {
        self.version.fetch_max(version.as_u64(), Ordering::AcqRel);
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

    fn record_publish_health(&self, message: String) {
        let mut slot = self.publish_health.lock();
        if slot.is_none() {
            *slot = Some(PublishHealth {
                first_observed_at: SystemTime::now(),
                message,
            });
        }
    }

    /// Latch a storage publication failure observed by an upper-layer
    /// background path (for example, engine-driven flush) so new writers can
    /// be rejected and health surfaces can report the failure.
    pub fn latch_publish_health(&self, message: impl Into<String>) {
        self.record_publish_health(message.into());
    }

    /// Returns the latched storage publication durability issue, if any.
    pub fn publish_health(&self) -> Option<PublishHealth> {
        self.publish_health.lock().clone()
    }

    fn manifest_payload_from_state(
        ver: &SegmentVersion,
        inherited_layers: &[InheritedLayer],
    ) -> (
        Vec<crate::manifest::ManifestEntry>,
        Vec<crate::manifest::ManifestInheritedLayer>,
    ) {
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

        let inherited = inherited_layers
            .iter()
            .map(|layer| {
                let layer_entries = layer
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

        (entries, inherited)
    }

    fn write_branch_manifest_from_state(
        &self,
        branch_id: &BranchId,
        ver: &SegmentVersion,
        inherited_layers: &[InheritedLayer],
    ) -> StorageResult<()> {
        let segments_dir = match &self.segments_dir {
            Some(d) => d,
            None => return Ok(()),
        };

        if let Some(inner) = crate::test_hooks::maybe_inject_manifest_publish_failure() {
            return Err(StorageError::ManifestPublish {
                branch_id: *branch_id,
                inner,
            });
        }

        let branch_hex = hex_encode_branch(branch_id);
        let branch_dir = segments_dir.join(&branch_hex);
        std::fs::create_dir_all(&branch_dir).map_err(|inner| StorageError::ManifestPublish {
            branch_id: *branch_id,
            inner,
        })?;

        let (entries, inherited) = Self::manifest_payload_from_state(ver, inherited_layers);
        crate::manifest::write_manifest(&branch_dir, &entries, &inherited).map_err(|e| match e {
            StorageError::Io(inner) => StorageError::ManifestPublish {
                branch_id: *branch_id,
                inner,
            },
            other => other,
        })
    }

    fn publish_locked_branch_manifest(
        &self,
        branch_id: &BranchId,
        branch: &BranchState,
    ) -> Result<PublishOutcome, StorageError> {
        let ver = branch.version.load();
        match self.write_branch_manifest_from_state(branch_id, &ver, &branch.inherited_layers) {
            Ok(()) => Ok(PublishOutcome::Durable),
            Err(StorageError::DirFsync { dir, inner }) => {
                let err = StorageError::DirFsync { dir, inner };
                self.record_publish_health(format!("{}", err));
                Ok(PublishOutcome::NotDurable(err))
            }
            Err(e) => Err(e),
        }
    }

    fn cleanup_created_segment_files(&self, paths: &[PathBuf]) {
        let mut parent_dirs = HashSet::new();
        for path in paths {
            if let Some(parent) = path.parent() {
                parent_dirs.insert(parent.to_path_buf());
            }
            let _ = std::fs::remove_file(path);
        }
        for dir in parent_dirs {
            let _ = std::fs::remove_dir(&dir);
        }
    }

    fn rollback_flush_publish_failure_locked(
        &self,
        branch: &mut BranchState,
        frozen_mt: &Arc<Memtable>,
        new_segment: &KVSegment,
    ) {
        crate::block_cache::global_cache().invalidate_file(new_segment.file_id());

        let cur_ver = branch.version.load();
        let mut new_levels = cur_ver.levels.clone();
        let old_l0_len = new_levels[0].len();
        new_levels[0].retain(|seg| seg.file_id() != new_segment.file_id());
        if new_levels[0].len() != old_l0_len {
            branch
                .version
                .store(Arc::new(SegmentVersion { levels: new_levels }));
            refresh_level_targets(branch, self.level_base_bytes());
        }

        if !branch.frozen.iter().any(|mt| mt.id() == frozen_mt.id()) {
            branch.frozen.push(Arc::clone(frozen_mt));
            self.total_frozen_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn rollback_materialize_status_locked(
        branch: &mut BranchState,
        source_branch_id: BranchId,
        fork_version: CommitVersion,
    ) {
        if let Some(layer) = branch.inherited_layers.iter_mut().find(|layer| {
            layer.source_branch_id == source_branch_id && layer.fork_version == fork_version
        }) {
            layer.status = LayerStatus::Active;
        }
    }

    fn rollback_materialize_install_locked(
        &self,
        branch: &mut BranchState,
        layer_index: usize,
        source_branch_id: BranchId,
        fork_version: CommitVersion,
        layer_segments: &Arc<SegmentVersion>,
        new_segments: &[Arc<KVSegment>],
    ) {
        let new_ids: HashSet<u64> = new_segments
            .iter()
            .map(|segment| segment.file_id())
            .collect();

        let cur_ver = branch.version.load();
        let mut new_levels = cur_ver.levels.clone();
        let old_l0_len = new_levels[0].len();
        new_levels[0].retain(|seg| !new_ids.contains(&seg.file_id()));
        if new_levels[0].len() != old_l0_len {
            branch
                .version
                .store(Arc::new(SegmentVersion { levels: new_levels }));
        }

        if let Some(layer) = branch.inherited_layers.iter_mut().find(|layer| {
            layer.source_branch_id == source_branch_id && layer.fork_version == fork_version
        }) {
            layer.status = LayerStatus::Active;
        } else {
            let restore_index = layer_index.min(branch.inherited_layers.len());
            branch.inherited_layers.insert(
                restore_index,
                InheritedLayer {
                    source_branch_id,
                    fork_version,
                    segments: Arc::clone(layer_segments),
                    status: LayerStatus::Active,
                },
            );
        }

        refresh_level_targets(branch, self.level_base_bytes());
    }

    fn rollback_fork_source_publish_failure_locked(
        &self,
        source: &mut BranchState,
        old_active: Arc<Memtable>,
        old_frozen: Vec<Arc<Memtable>>,
        old_version: Arc<SegmentVersion>,
        old_level_targets: compaction::LevelTargets,
    ) {
        let current_frozen = source.frozen.len();
        let restored_frozen = old_frozen.len();
        if restored_frozen > current_frozen {
            self.total_frozen_count
                .fetch_add(restored_frozen - current_frozen, Ordering::Relaxed);
        } else if current_frozen > restored_frozen {
            self.total_frozen_count
                .fetch_sub(current_frozen - restored_frozen, Ordering::Relaxed);
        }

        source.active = old_active;
        source.frozen = old_frozen;
        source.version.store(old_version);
        source.level_targets = old_level_targets;
    }

    fn rollback_fork_dest_publish_failure_locked(
        dest: &mut BranchState,
        dest_was_new: bool,
    ) -> bool {
        dest.inherited_layers.clear();
        dest.max_version.store(0, Ordering::Release);
        dest_was_new
            && dest.active.is_empty()
            && dest.frozen.is_empty()
            && dest.version.load().levels.iter().all(Vec::is_empty)
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
    ///
    /// ## B5.1 retention contract
    ///
    /// Implements §"Parent compaction and parent delete" / "Parent
    /// delete" of
    /// `docs/design/branching/branching-gc/branching-retention-contract.md`.
    /// Parent delete must not invalidate descendant inherited-layer
    /// manifests and must not force descendant materialization (KD4
    /// in `b5-phasing-plan.md`). Per Invariant 1 + §"Reclaimability
    /// rule", segments still referenced by a descendant
    /// inherited-layer manifest are retained — the refcount check
    /// here implements that retention as a runtime accelerator
    /// (`BarrierKind::RuntimeAccelerator`); the durable proof remains
    /// the descendant's manifest entries
    /// (`BarrierKind::PhysicalRetention`).
    pub fn clear_branch(&self, branch_id: &BranchId) -> StorageResult<bool> {
        let branch_dir_to_cleanup = self.segments_dir.as_ref().map(|segments_dir| {
            let branch_hex = hex_encode_branch(branch_id);
            segments_dir.join(&branch_hex)
        });
        if let Some((_, branch)) = self.branches.remove(branch_id) {
            if let Some(branch_dir) = branch_dir_to_cleanup.as_ref() {
                if branch_dir.exists() {
                    // Publish the empty-manifest barrier before any
                    // storage-local mutation. If this fails, callers see a
                    // hard error rather than a partially-cleared branch that
                    // can resurrect on reopen.
                    crate::manifest::write_manifest(branch_dir, &[], &[])?;
                }
            }

            // 1. Route the branch's own segments through the B5.2 reclaim
            //    protocol. Each segment that is no longer referenced by a
            //    descendant inherited layer is quarantined (rename +
            //    durable publish). Shared segments are left in place;
            //    refusal under degraded recovery is logged as retention
            //    debt, not propagated.
            let ver = branch.version.load();
            for level in &ver.levels {
                for seg in level {
                    if let Err(e) = self.quarantine_segment_if_unreferenced_for_cleared_branch(
                        seg.file_path(),
                        seg.file_id(),
                    ) {
                        tracing::warn!(
                            target: "strata::storage::gc",
                            branch_id = %hex_encode_branch(branch_id),
                            segment = %seg.file_path().display(),
                            error = %e,
                            "clear_branch: reclaim refused; retention debt accumulated"
                        );
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

            // 3. Best-effort branch-dir cleanup.
            //
            // In-flight compaction for this branch may still be writing
            // `.tmp` / `.sst` files to this directory even though we've
            // already removed the branch from `self.branches`. `clear_branch`
            // is post-commit cleanup only: it may quarantine candidates and
            // leave cleanup debt behind, but it must not synchronously drain
            // global orphan GC or Stage-5 purge. `remove_dir` (not
            // `remove_dir_all`) preserves the original safety guarantee: if
            // we somehow missed tracking a file, we leak it rather than
            // deleting it.
            if let Some(branch_dir) = branch_dir_to_cleanup.as_deref() {
                self.finish_cleared_branch_dir(branch_id, branch_dir, false)?;
            }

            Ok(true)
        } else {
            if let Some(branch_dir) = branch_dir_to_cleanup.as_deref() {
                if branch_dir.exists() {
                    self.finish_cleared_branch_dir(branch_id, branch_dir, true)?;
                    return Ok(true);
                }
            }

            Ok(false)
        }
    }

    fn finish_cleared_branch_dir(
        &self,
        branch_id: &BranchId,
        branch_dir: &std::path::Path,
        republish_barrier_if_needed: bool,
    ) -> StorageResult<()> {
        if !branch_dir.exists() {
            return Ok(());
        }

        let mut top_level_ssts: Vec<std::path::PathBuf> = std::fs::read_dir(branch_dir)?
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .filter(|path| path.extension().and_then(|x| x.to_str()) == Some("sst"))
            .collect();

        if republish_barrier_if_needed && !top_level_ssts.is_empty() {
            // Retry path: the branch is already absent from memory but
            // top-level files remain on disk. Re-publish the empty-manifest
            // barrier so the directory stays non-live across reopen while we
            // continue branch-local cleanup.
            crate::manifest::write_manifest(branch_dir, &[], &[])?;
        }

        for seg_path in &top_level_ssts {
            let file_id = crate::block_cache::file_path_hash(seg_path);
            if let Err(e) =
                self.quarantine_segment_if_unreferenced_for_cleared_branch(seg_path, file_id)
            {
                tracing::warn!(
                    target: "strata::storage::gc",
                    branch_id = %hex_encode_branch(branch_id),
                    segment = %seg_path.display(),
                    error = %e,
                    "clear_branch retry: reclaim refused; retention debt accumulated"
                );
            }
        }

        top_level_ssts = std::fs::read_dir(branch_dir)?
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.is_file())
            .filter(|path| path.extension().and_then(|x| x.to_str()) == Some("sst"))
            .collect();

        // Any directory that remains on disk is already protected from
        // stale-manifest or no-manifest fallback on reopen. If no top-level
        // `.sst` files remain we can best-effort remove the empty manifest
        // itself before retrying `remove_dir`; otherwise we intentionally keep
        // the empty manifest so deleted-parent orphan storage stays non-live
        // across reopen. `remove_dir` stays non-recursive to preserve the
        // original safety guarantee: unknown files are leaked, not deleted.
        if top_level_ssts.is_empty() {
            match std::fs::remove_file(branch_dir.join("segments.manifest")) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        target: "strata::branch",
                        branch_dir = %branch_dir.display(),
                        error = %e,
                        "Failed to remove empty manifest after branch clear; leaving directory in place",
                    );
                }
            }
        }
        if let Err(e) = std::fs::remove_dir(branch_dir) {
            tracing::warn!(
                target: "strata::branch",
                branch_dir = %branch_dir.display(),
                error = %e,
                "Branch directory not empty after clear_branch; leaving leftover files in place",
            );
        }

        Ok(())
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
    /// # Refusal under degraded recovery (SE3 / SG-009)
    ///
    /// GC refuses when [`SegmentedStore::last_recovery_health`] is
    /// `Degraded` with class [`DegradationClass::DataLoss`] or
    /// [`DegradationClass::PolicyDowngrade`]: in either case the in-memory
    /// branch set may not reflect every branch that still has authoritative
    /// on-disk state, so "not in `live_ids`" is not a safe deletion proof.
    /// [`DegradationClass::Telemetry`] is not a refusal cause — rebuildable
    /// caches do not compromise deletion safety. `PolicyDowngrade` can be
    /// cleared in-place via [`SegmentedStore::reset_recovery_health`] after a
    /// successful recovery; `DataLoss` requires a fresh reopen because the
    /// current store instance cannot reload later-restored files.
    ///
    /// ## B5.1 retention contract
    ///
    /// Implements the §"Recovery-health contract" gate of
    /// `docs/design/branching/branching-gc/branching-retention-contract.md`.
    /// This path now runs the B5.2 quarantine protocol: purge any
    /// durably published quarantine inventory first, then nominate
    /// top-level orphan `.sst` files from the runtime live-set scan and
    /// route each candidate through Stage-2 manifest proof plus
    /// quarantine. The degraded-recovery refusal satisfies Invariants 2
    /// and 5: space leaks are acceptable, false reclaim is not. Barrier
    /// role: `BarrierKind::RecoveryHealthGate` (the refusal) +
    /// manifest-derived `BarrierKind::PhysicalRetention` (the proof).
    pub fn gc_orphan_segments(&self) -> StorageResult<GcReport> {
        // Gate — the B5.2 quarantine protocol uses this same gate before
        // any filesystem mutation. Telemetry-class degradation does not
        // compromise deletion safety.
        self.check_reclaim_allowed()?;

        let Some(segments_dir) = &self.segments_dir else {
            return Ok(GcReport { files_deleted: 0 });
        };

        let mut files_deleted = 0usize;

        // Stage 5 — drain quarantined inventories first. A crashed prior
        // reclaim may have left files in `__quarantine__/` from a
        // previous call; purge what has been durably published.
        let purge = self.purge_all_quarantines()?;
        files_deleted += purge.files_purged;

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

        // Stages 2–4 — scan for orphan `.sst` candidates and route each
        // one through the quarantine protocol. Files inside
        // `__quarantine__/` are skipped: they are already in flight and
        // handled by the purge step above.
        let mut newly_quarantined: Vec<PathBuf> = Vec::new();
        let Ok(entries) = std::fs::read_dir(segments_dir) else {
            return Ok(GcReport { files_deleted });
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
                // Skip subdirectories (in particular `__quarantine__/`,
                // whose contents were drained by `purge_all_quarantines`
                // above — this loop only classifies top-level orphan
                // `.sst` files).
                if sst_path.is_dir() {
                    continue;
                }
                if sst_path.extension().and_then(|e| e.to_str()) != Some("sst") {
                    continue;
                }
                let file_id = crate::block_cache::file_path_hash(&sst_path);
                if !live_ids.contains(&file_id) {
                    match self.quarantine_segment_if_unreferenced(&sst_path, file_id) {
                        Ok(true) => newly_quarantined.push(sst_path),
                        Ok(false) => {} // still referenced by runtime accelerator
                        Err(e) => {
                            // Degraded gate fired mid-scan, or publish failed —
                            // propagate so callers see retention debt.
                            return Err(e);
                        }
                    }
                }
            }
        }

        // Finish purging the freshly-quarantined entries within this
        // call when health still permits — contract §"Stage 5. Final
        // purge" allows this because the inventory publish at Stage 4
        // is the durable-publish precondition.
        if !newly_quarantined.is_empty() {
            let purge2 = self.purge_all_quarantines()?;
            files_deleted += purge2.files_purged;
        }

        Ok(GcReport { files_deleted })
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
    pub fn get_fork_info(&self, branch_id: &BranchId) -> Option<(BranchId, CommitVersion)> {
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
    ///
    /// ## B5.1 retention contract
    ///
    /// Implements §"Materialization contract" of
    /// `docs/design/branching/branching-gc/branching-retention-contract.md`.
    /// Materialization rewrites ownership, not meaning (Invariant 6,
    /// KD4): the child's visible result set, tombstone semantics,
    /// TTL semantics, timestamps, and fork-frontier visibility are
    /// all preserved; only manifest ownership and inherited-layer
    /// membership change. The shadow check at the entry-copy stage
    /// is the conservative path the contract requires (assume
    /// shadowed on corruption rather than risk stale-data
    /// resurrection). Crash-safety: the `Materializing` status flag
    /// in the manifest plus the `Materializing → Active` reset on
    /// reopen satisfy the §"Crash / reopen rule".
    pub fn materialize_layer(
        &self,
        child_branch_id: &BranchId,
        layer_index: usize,
    ) -> StorageResult<MaterializeResult> {
        let segments_dir = match &self.segments_dir {
            Some(d) => d,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "materialize_layer requires a disk-backed store",
                )
                .into());
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
                None => {
                    return Err(io::Error::new(io::ErrorKind::NotFound, "branch not found").into())
                }
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
            let closer_layers: Vec<(Arc<SegmentVersion>, BranchId, CommitVersion)> = branch
                .inherited_layers[..layer_index]
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

        // 2b. Set status to Materializing and publish that state while the
        // branch guard still excludes same-branch mutation.
        let mut initial_not_durable = None;
        {
            let mut branch = match self.branches.get_mut(child_branch_id) {
                Some(b) => b,
                None => {
                    return Err(io::Error::new(io::ErrorKind::NotFound, "branch not found").into())
                }
            };
            if layer_index < branch.inherited_layers.len() {
                branch.inherited_layers[layer_index].status = LayerStatus::Materializing;
            }
            match self.publish_locked_branch_manifest(child_branch_id, &branch) {
                Ok(PublishOutcome::Durable) => {}
                Ok(PublishOutcome::NotDurable(err)) => initial_not_durable = Some(err),
                Err(e) => {
                    Self::rollback_materialize_status_locked(
                        &mut branch,
                        source_branch_id,
                        fork_version,
                    );
                    return Err(e);
                }
            }
        }

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
        let new_segments: Vec<Arc<KVSegment>> = if !entries.is_empty() {
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
                let seg = Arc::new(KVSegment::open(&path)?);
                segments.push(seg);
            }
            segments
        } else {
            Vec::new()
        };
        let segments_created = new_segments.len();

        // 2e. Atomic install (under DashMap write guard)
        //
        // Use get_mut (not entry().or_insert_with) to avoid resurrecting a
        // branch that was concurrently deleted by clear_branch (SE4 / SG-011).
        #[cfg(test)]
        crate::test_hooks::maybe_pause(
            crate::test_hooks::pause_tag::MATERIALIZE_LAYER,
            *child_branch_id,
        );
        {
            let mut branch = match self.branches.get_mut(child_branch_id) {
                Some(b) => b,
                None => {
                    let created_paths: Vec<_> = new_segments
                        .iter()
                        .map(|segment| segment.file_path().to_path_buf())
                        .collect();
                    for segment in &new_segments {
                        crate::block_cache::global_cache().invalidate_file(segment.file_id());
                    }
                    self.cleanup_created_segment_files(&created_paths);
                    return Err(StorageError::BranchDeletedDuringOp {
                        branch_id: *child_branch_id,
                        op: BranchOp::MaterializeLayer,
                    });
                }
            };

            // Prepend new segments to L0
            if !new_segments.is_empty() {
                let old_ver = branch.version.load();
                let mut new_l0 =
                    Vec::with_capacity(old_ver.l0_segments().len() + new_segments.len());
                for seg in &new_segments {
                    new_l0.push(Arc::clone(seg));
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
            match self.publish_locked_branch_manifest(child_branch_id, &branch) {
                Ok(PublishOutcome::Durable) => {}
                Ok(PublishOutcome::NotDurable(err)) => {
                    if initial_not_durable.is_none() {
                        initial_not_durable = Some(err);
                    }
                }
                Err(e) => {
                    self.rollback_materialize_install_locked(
                        &mut branch,
                        layer_index,
                        source_branch_id,
                        fork_version,
                        &layer_segments,
                        &new_segments,
                    );
                    let created_paths: Vec<_> = new_segments
                        .iter()
                        .map(|segment| segment.file_path().to_path_buf())
                        .collect();
                    drop(branch);
                    for segment in &new_segments {
                        crate::block_cache::global_cache().invalidate_file(segment.file_id());
                    }
                    self.cleanup_created_segment_files(&created_paths);
                    return Err(e);
                }
            }
        }

        // Decrement refcounts for each segment in the removed layer.
        for level in &layer_segments.levels {
            for seg in level {
                self.ref_registry.decrement(seg.file_id());
            }
        }

        // Garbage-collect orphan segment files (#1705).
        // Refcount decrements above may have released the last reference to
        // segments whose parent already compacted them away. A degraded
        // recovery (SE3) makes GC refuse; materialize still succeeds and
        // the retention debt is logged.
        if let Err(e) = self.gc_orphan_segments() {
            tracing::warn!(
                target: "strata::storage::gc",
                branch_id = %hex_encode_branch(child_branch_id),
                op = "materialize_layer",
                error = %e,
                "gc_orphan_segments refused; retention debt accumulated",
            );
        }

        if let Some(err) = initial_not_durable {
            tracing::warn!(
                branch = %hex_encode_branch(child_branch_id),
                error = %err,
                "materialize_layer completed with unconfirmed manifest durability"
            );
        }

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
        fork_version: CommitVersion,
        own_version: &SegmentVersion,
        closer_layers: &[(Arc<SegmentVersion>, BranchId, CommitVersion)],
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
        let seek_ik = InternalKey::from_typed_key_bytes(typed_key, CommitVersion::MAX);
        let seek_bytes = seek_ik.as_bytes();

        // L0 segments (linear scan)
        for seg in own_version.l0_segments() {
            match seg.point_lookup_preencoded(typed_key, seek_bytes, CommitVersion::MAX) {
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
                CommitVersion::MAX,
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
        fork_version: CommitVersion,
    ) -> bool {
        // Rewrite typed_key from child → source namespace
        let Some(src_typed_key) = rewrite_branch_id_bytes(child_typed_key, &source_branch_id)
        else {
            return false; // Corrupt key can't exist
        };
        let src_seek_ik = InternalKey::from_typed_key_bytes(&src_typed_key, CommitVersion::MAX);
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
    ///
    /// ## B5.1 retention contract
    ///
    /// Implements §"Fork-frontier semantics" of
    /// `docs/design/branching/branching-gc/branching-retention-contract.md`.
    /// The captured `fork_version` and the inherited-layer entry
    /// installed on the destination together form the durable
    /// fork-frontier barrier (`BarrierKind::ForkFrontier`): they
    /// bound the descendant's inherited reads (read-time clamp to
    /// `commit_id <= fork_version`) and keep the parent's snapshot
    /// segments reachable for the descendant even after subsequent
    /// parent compaction or parent delete. The refcount increments
    /// on shared segments are runtime accelerators only
    /// (`BarrierKind::RuntimeAccelerator`); the durable retention
    /// proof is the destination's manifest entry.
    pub fn fork_branch(
        &self,
        source_id: &BranchId,
        dest_id: &BranchId,
    ) -> StorageResult<(CommitVersion, usize)> {
        if source_id == dest_id {
            return Err(StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fork_branch: source and destination must be different branches",
            )));
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
        let (dest_layers, segments_shared, fork_version) = {
            let mut source = match self.branches.get_mut(source_id) {
                Some(b) => b,
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "fork_branch: source branch no longer exists",
                    )
                    .into())
                }
            };
            let source_old_active = Arc::clone(&source.active);
            let source_old_frozen = source.frozen.clone();
            let source_old_version = source.version.load_full();
            let source_old_level_targets = source.level_targets.clone();
            let mut source_flush_paths = Vec::new();

            // Inline-rotate: move straggler writes from active to frozen.
            if !source.active.is_empty() {
                let next_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
                let old = std::mem::replace(&mut source.active, Arc::new(Memtable::new(next_id)));
                old.freeze();
                source.frozen.insert(0, old);
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
                    source_flush_paths.push(seg_path.clone());
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
                    match self.publish_locked_branch_manifest(source_id, &source) {
                        Ok(PublishOutcome::Durable) => {}
                        Ok(PublishOutcome::NotDurable(err)) => {
                            tracing::warn!(
                                branch = %hex_encode_branch(source_id),
                                error = %err,
                                "fork source manifest rename landed but durability is unconfirmed"
                            );
                        }
                        Err(e) => {
                            self.rollback_fork_source_publish_failure_locked(
                                &mut source,
                                source_old_active,
                                source_old_frozen,
                                source_old_version,
                                source_old_level_targets,
                            );
                            drop(source);
                            self.cleanup_created_segment_files(&source_flush_paths);
                            return Err(e);
                        }
                    }
                }
            }

            // Capture fork_version from the source branch's max applied version,
            // NOT the global counter. The global counter may include versions
            // allocated by concurrent writers (via next_version()) that haven't
            // applied their writes yet. Using the global counter would claim
            // the fork includes data that isn't actually in the snapshot.
            let fork_version = CommitVersion(source.max_version.load(Ordering::Acquire));

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

            // Lock Level 3 (shared): increment refcounts while holding the
            // deletion barrier to prevent concurrent delete_segment_if_unreferenced
            // from observing a transient zero refcount mid-batch (#1682).
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

            (layers, shared, fork_version)
            // source DashMap guard drops here — segments are now protected
        };

        // 6. Attach to dest branch, rejecting if a concurrent fork already installed layers.
        let mut dest_was_new = false;
        let mut dest = self.branches.entry(*dest_id).or_insert_with(|| {
            dest_was_new = true;
            BranchState::new()
        });
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
            return Err(StorageError::Io(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "fork_branch: destination already has inherited layers (concurrent fork race)",
            )));
        }
        dest.inherited_layers = dest_layers.clone();
        // Propagate fork_version so subsequent forks from this child
        // correctly report the inherited data's version range.
        dest.max_version
            .fetch_max(fork_version.as_u64(), Ordering::Release);
        match self.publish_locked_branch_manifest(dest_id, &dest) {
            Ok(PublishOutcome::Durable) => {}
            Ok(PublishOutcome::NotDurable(err)) => {
                tracing::warn!(
                    branch = %hex_encode_branch(dest_id),
                    error = %err,
                    "fork destination manifest rename landed but durability is unconfirmed"
                );
            }
            Err(e) => {
                let remove_branch =
                    Self::rollback_fork_dest_publish_failure_locked(&mut dest, dest_was_new);
                drop(dest);
                for layer in &dest_layers {
                    for level in &layer.segments.levels {
                        for seg in level {
                            self.ref_registry.decrement(seg.file_id());
                        }
                    }
                }
                if remove_branch {
                    self.branches.remove(dest_id);
                }
                return Err(e);
            }
        }
        drop(dest);

        Ok((fork_version, segments_shared))
    }

    /// Count distinct live (non-tombstone, non-expired) logical keys in a branch.
    pub fn branch_entry_count(&self, branch_id: &BranchId) -> usize {
        let snapshot = match self.snapshot_branch(branch_id) {
            Some(s) => s,
            None => return 0,
        };
        Self::list_branch_from_snapshot(&snapshot, *branch_id).len()
    }

    /// List all live entries for a branch (MVCC dedup at latest version).
    pub fn list_branch(&self, branch_id: &BranchId) -> Vec<(Key, VersionedValue)> {
        let snapshot = match self.snapshot_branch(branch_id) {
            Some(s) => s,
            None => return Vec::new(),
        };
        Self::list_branch_from_snapshot(&snapshot, *branch_id)
    }

    /// Internal: list all live entries for a branch reference.
    #[allow(dead_code)]
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
        let mvcc = MvccIterator::new(merge, CommitVersion::MAX);

        mvcc.filter_map(|(ik, entry)| {
            if entry.is_tombstone || entry.is_expired() {
                return None;
            }
            let (key, commit_id) = ik.decode()?;
            Some((key, entry.to_versioned(commit_id)))
        })
        .collect()
    }

    /// List all live entries from a [`BranchSnapshot`] — no DashMap guard held.
    fn list_branch_from_snapshot(
        snapshot: &BranchSnapshot,
        branch_id: BranchId,
    ) -> Vec<(Key, VersionedValue)> {
        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = Vec::new();

        let active_entries: Vec<_> = snapshot.active.iter_all().collect();
        sources.push(Box::new(active_entries.into_iter()));

        for frozen in &snapshot.frozen {
            let entries: Vec<_> = frozen.iter_all().collect();
            sources.push(Box::new(entries.into_iter()));
        }

        for level in &snapshot.segments.levels {
            for seg in level {
                let entries: Vec<_> = seg
                    .iter_seek_all()
                    .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                    .collect();
                sources.push(Box::new(entries.into_iter()));
            }
        }

        for layer in &snapshot.inherited_layers {
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
        let mvcc = MvccIterator::new(merge, CommitVersion::MAX);

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
    pub fn get_value_direct(&self, key: &Key) -> StorageResult<Option<Value>> {
        let snapshot = match self.snapshot_branch(&key.namespace.branch_id) {
            Some(s) => s,
            None => return Ok(None),
        };
        match Self::get_versioned_from_snapshot(&snapshot, key, CommitVersion::MAX)? {
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

    /// Get a versioned value directly from storage, bypassing the transaction layer.
    ///
    /// Like `get_value_direct` but preserves version and timestamp metadata.
    /// Uses `u64::MAX` as the snapshot version to see all committed data.
    ///
    /// This provides per-key read consistency without the overhead of
    /// transaction allocation, coordinator mutex, or read-set tracking.
    /// For multi-key snapshot isolation, use `Database::transaction()`.
    pub fn get_versioned_direct(&self, key: &Key) -> StorageResult<Option<VersionedValue>> {
        let snapshot = match self.snapshot_branch(&key.namespace.branch_id) {
            Some(s) => s,
            None => return Ok(None),
        };
        match Self::get_versioned_from_snapshot(&snapshot, key, CommitVersion::MAX)? {
            Some((commit_id, entry)) => {
                if entry.is_tombstone || entry.is_expired() {
                    Ok(None)
                } else {
                    Ok(Some(entry.into_versioned(commit_id)))
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
        min_commit_id: Option<CommitVersion>,
    ) -> Vec<OwnEntry> {
        let snapshot = match self.snapshot_branch(branch_id) {
            Some(s) => s,
            None => return Vec::new(),
        };

        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = Vec::new();

        // Active memtable
        let active_entries: Vec<_> = snapshot.active.iter_all().collect();
        sources.push(Box::new(active_entries.into_iter()));

        // Frozen memtables (newest first)
        for frozen in &snapshot.frozen {
            let entries: Vec<_> = frozen.iter_all().collect();
            sources.push(Box::new(entries.into_iter()));
        }

        // On-disk segments — all levels (own segments only, NO inherited layers)
        for level in &snapshot.segments.levels {
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
        let mvcc = MvccIterator::new(merge, CommitVersion::MAX);

        mvcc.filter_map(|(ik, entry)| {
            let (key, commit_id) = ik.decode()?;
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
        max_version: CommitVersion,
    ) -> Vec<VersionedEntry> {
        let snapshot = match self.snapshot_branch(branch_id) {
            Some(s) => s,
            None => return Vec::new(),
        };

        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = Vec::new();

        let active_entries: Vec<_> = snapshot.active.iter_all().collect();
        sources.push(Box::new(active_entries.into_iter()));

        for frozen in &snapshot.frozen {
            let entries: Vec<_> = frozen.iter_all().collect();
            sources.push(Box::new(entries.into_iter()));
        }

        for level in &snapshot.segments.levels {
            for seg in level {
                let entries: Vec<_> = seg
                    .iter_seek_all()
                    .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                    .collect();
                sources.push(Box::new(entries.into_iter()));
            }
        }

        for layer in &snapshot.inherited_layers {
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
                timestamp_micros: entry.timestamp.as_micros(),
                ttl_ms: entry.ttl_ms,
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
        let snapshot = match self.snapshot_branch(branch_id) {
            Some(s) => s,
            None => return Vec::new(),
        };

        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = Vec::new();
        let active_entries: Vec<_> = snapshot.active.iter_all().collect();
        sources.push(Box::new(active_entries.into_iter()));
        for frozen in &snapshot.frozen {
            let entries: Vec<_> = frozen.iter_all().collect();
            sources.push(Box::new(entries.into_iter()));
        }
        for level in &snapshot.segments.levels {
            for seg in level {
                let entries: Vec<_> = seg
                    .iter_seek_all()
                    .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)))
                    .collect();
                sources.push(Box::new(entries.into_iter()));
            }
        }

        for layer in &snapshot.inherited_layers {
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
    ) -> StorageResult<Option<VersionedValue>> {
        let snapshot = match self.snapshot_branch(&key.namespace.branch_id) {
            Some(s) => s,
            None => return Ok(None),
        };
        let all_versions = Self::get_all_versions_from_snapshot(&snapshot, key)?;
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
    ) -> StorageResult<Vec<(Key, VersionedValue)>> {
        let snapshot = match self.snapshot_branch(&prefix.namespace.branch_id) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let (merge, flags) = Self::build_snapshot_merge_iter(&snapshot, prefix, prefix)?;

        // Custom timestamp-based dedup — not MvccIterator (which filters by version,
        // not timestamp). For each logical key, find the latest version whose
        // timestamp ≤ max_timestamp.
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
        check_corruption(&flags)?;
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
    pub fn time_range(&self, branch_id: BranchId) -> StorageResult<Option<(u64, u64)>> {
        // O(1) fast path: read atomic timestamps (populated by write path).
        if let Some(branch) = self.branches.get(&branch_id) {
            let min_ts = branch.min_timestamp.load(Ordering::Relaxed);
            let max_ts = branch.max_timestamp.load(Ordering::Relaxed);
            if min_ts != u64::MAX {
                return Ok(Some((min_ts, max_ts)));
            }
        } else {
            return Ok(None);
        }

        // Fallback: atomics not populated (e.g. after recovery from segments
        // where WAL was truncated). Scan entries via snapshot to find range.
        // Uses snapshot to avoid holding DashMap guard during I/O.
        let snapshot = match self.snapshot_branch(&branch_id) {
            Some(s) => s,
            None => return Ok(None),
        };
        let entries = Self::list_branch_from_snapshot(&snapshot, branch_id);
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
        // Populate atomics so subsequent calls are O(1).
        // Re-acquire guard briefly to update the atomic timestamps.
        if let Some(branch) = self.branches.get(&branch_id) {
            branch.track_timestamp(scan_min);
            branch.track_timestamp(scan_max);
        }
        Ok(Some((scan_min, scan_max)))
    }

    /// Garbage collect old versions for a branch.
    /// SegmentedStore prunes via compaction — this is a no-op stub.
    pub fn gc_branch(&self, _branch_id: &BranchId, _min_version: CommitVersion) -> usize {
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
        version: CommitVersion,
        timestamp_micros: u64,
        ttl_ms: u64,
    ) -> StorageResult<()> {
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
        branch.num_entries.fetch_add(1, Ordering::Relaxed);
        branch.track_timestamp(ts);
        branch.track_version(version);

        self.maybe_rotate_branch(branch_id, &mut branch);
        self.version.fetch_max(version.as_u64(), Ordering::AcqRel);

        Ok(())
    }

    /// Apply a delete during WAL recovery, preserving the original commit timestamp (#1619).
    pub fn delete_recovery_entry(
        &self,
        key: &Key,
        version: CommitVersion,
        timestamp_micros: u64,
    ) -> StorageResult<()> {
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
        branch.num_deletions.fetch_add(1, Ordering::Relaxed);
        branch.track_timestamp(ts);
        branch.track_version(version);

        self.maybe_rotate_branch(branch_id, &mut branch);
        self.version.fetch_max(version.as_u64(), Ordering::AcqRel);

        Ok(())
    }

    /// Apply puts and deletes atomically during WAL recovery/refresh,
    /// preserving the original commit timestamp (#1699) and deferring the
    /// global version bump until all entries are installed (#1707).
    pub fn apply_recovery_atomic(
        &self,
        writes: Vec<(Key, Value)>,
        deletes: Vec<Key>,
        version: CommitVersion,
        timestamp_micros: u64,
        put_ttls: &[u64],
    ) -> StorageResult<()> {
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
            let put_count = entries.len() as u64;
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
            branch.num_entries.fetch_add(put_count, Ordering::Relaxed);
            branch.track_timestamp(ts);
            branch.track_version(version);
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        // Install tombstones.
        for (branch_id, keys) in deletes_by_branch {
            let mut branch = self
                .branches
                .entry(branch_id)
                .or_insert_with(BranchState::new);
            let delete_count = keys.len() as u64;
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
            branch
                .num_deletions
                .fetch_add(delete_count, Ordering::Relaxed);
            branch.track_timestamp(ts);
            branch.track_version(version);
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        // Version is NOT advanced here. The caller (Database::refresh) is
        // responsible for calling advance_version() AFTER secondary indexes
        // (BM25/HNSW) have been updated, so readers never observe KV data
        // without corresponding index entries (Issue #1734).
        Ok(())
    }

    /// Install decoded entries for one branch and primitive type from a snapshot.
    ///
    /// The caller provides entries grouped by `(branch_id, type_tag)`. This
    /// method reconstructs full `Key` values internally and preserves the
    /// metadata carried by each decoded entry.
    ///
    /// # Timestamp Invariants
    ///
    /// The original commit timestamps from the snapshot are preserved, ensuring
    /// time-travel queries return correct results after snapshot-based recovery.
    ///
    /// # Version Tracking
    ///
    /// Unlike WAL recovery, this method advances the storage version to the
    /// maximum version seen across the installed snapshot entries.
    pub fn install_snapshot_entries(
        &self,
        branch_id: BranchId,
        type_tag: TypeTag,
        entries: &[DecodedSnapshotEntry],
    ) -> StorageResult<usize> {
        if entries.is_empty() {
            return Ok(0);
        }

        let mut max_version = CommitVersion::ZERO;
        let mut installed = 0usize;

        let mut branch = self
            .branches
            .entry(branch_id)
            .or_insert_with(BranchState::new);

        for snapshot_entry in entries {
            let namespace = Arc::new(strata_core::Namespace::for_branch_space(
                branch_id,
                &snapshot_entry.space,
            ));
            let key = Key::new(namespace, type_tag, snapshot_entry.user_key.clone());
            let timestamp = Timestamp::from_micros(snapshot_entry.timestamp_micros);
            let (value, is_tombstone) = match &snapshot_entry.payload {
                DecodedSnapshotValue::Value(value) => (value.clone(), false),
                DecodedSnapshotValue::Tombstone => (Value::Null, true),
            };
            let entry = MemtableEntry {
                value,
                is_tombstone,
                timestamp,
                ttl_ms: snapshot_entry.ttl_ms,
                raw_value: None,
            };
            branch.active.put_entry(&key, snapshot_entry.version, entry);
            if is_tombstone {
                branch.num_deletions.fetch_add(1, Ordering::Relaxed);
            } else {
                branch.num_entries.fetch_add(1, Ordering::Relaxed);
            }
            branch.track_timestamp(snapshot_entry.timestamp_micros);
            branch.track_version(snapshot_entry.version);
            max_version = max_version.max(snapshot_entry.version);
            installed += 1;
        }

        self.maybe_rotate_branch(branch_id, &mut branch);

        // Advance version to the max seen in the snapshot.
        if max_version > CommitVersion::ZERO {
            self.version
                .fetch_max(max_version.as_u64(), Ordering::AcqRel);
        }

        Ok(installed)
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
    pub fn end_bulk_load(&self, branch_id: &BranchId) -> StorageResult<()> {
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
        let old = std::mem::replace(&mut branch.active, Arc::new(Memtable::new(next_id)));
        old.freeze();
        branch.frozen.insert(0, old);
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
    pub fn flush_oldest_frozen(&self, branch_id: &BranchId) -> StorageResult<bool> {
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

        let segment = Arc::new(KVSegment::open(&seg_path)?);

        // Atomically: remove the frozen memtable we just flushed and install
        // the segment, under a single DashMap guard.
        //
        // Use get_mut (not entry().or_insert_with) to avoid resurrecting a
        // branch that was concurrently deleted by clear_branch (#2108).
        let mut branch = match self.branches.get_mut(branch_id) {
            Some(b) => b,
            None => {
                // Branch was deleted between Phase 1 and Phase 3.
                // Clean up the orphan segment file we just built. Also try
                // to remove the branch directory — `create_dir_all` above
                // may have recreated it after `clear_branch` already tried
                // to remove it, leaving an empty orphan dir behind.
                // Best-effort: `remove_dir` only succeeds if empty, so this
                // is safe against concurrent writes.
                let _ = std::fs::remove_file(&seg_path);
                let _ = std::fs::remove_dir(&branch_dir);
                return Ok(true);
            }
        };
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
            new_l0.push(Arc::clone(&segment));
            new_l0.extend(old_ver.l0_segments().iter().cloned());
            let mut new_levels = old_ver.levels.clone();
            new_levels[0] = new_l0;
            branch
                .version
                .store(Arc::new(SegmentVersion { levels: new_levels }));
            refresh_level_targets(&mut branch, self.level_base_bytes());

            match self.publish_locked_branch_manifest(branch_id, &branch) {
                Ok(PublishOutcome::Durable) => {}
                Ok(PublishOutcome::NotDurable(err)) => {
                    tracing::warn!(
                        branch = %hex_encode_branch(branch_id),
                        error = %err,
                        "flush installed segment but manifest durability is unconfirmed"
                    );
                }
                Err(e) => {
                    self.rollback_flush_publish_failure_locked(&mut branch, &frozen_mt, &segment);
                    drop(branch);
                    let _ = std::fs::remove_file(segment.file_path());
                    if let Some(parent) = segment.file_path().parent() {
                        let _ = std::fs::remove_dir(parent);
                    }
                    return Err(e);
                }
            }
            drop(branch);
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
    pub fn max_flushed_commit(&self, branch_id: &BranchId) -> Option<CommitVersion> {
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

    /// Recover flushed segments from disk into a fresh store.
    ///
    /// Scans `segments_dir` for branch subdirectories (hex-encoded BranchId),
    /// opens `.sst` files within each, and installs them into the store. On
    /// return or hard pre-walk failure, `last_recovery_health()` reflects the
    /// most recent classified recovery attempt.
    ///
    /// Returns [`Ok(RecoveredState)`] for every outcome the walk can reach,
    /// with defects captured inside `RecoveredState::health` as typed
    /// [`RecoveryFault`]s. The `Err` arm is reserved for pre-walk I/O
    /// failures — e.g. the top-level `read_dir(segments_dir)` itself
    /// failing or the authoritative `segments_dir` being missing. Those hard
    /// errors still publish a degraded `last_recovery_health()` snapshot
    /// before returning. Per-branch failures classify into faults and
    /// continue.
    ///
    /// This is a one-shot bootstrap primitive. A second successful call on
    /// the same `SegmentedStore` instance would duplicate recovered state, so
    /// repeated invocations return [`StorageError::RecoveryAlreadyApplied`].
    ///
    /// Callers should pass the returned outcome to
    /// `TransactionCoordinator::apply_storage_recovery` rather than reading
    /// individual fields: that entry point owns version-floor adoption and
    /// future per-branch version wiring.
    ///
    /// ## B5.1 retention contract
    ///
    /// Implements §"Recovery rebuild protocol" of
    /// `docs/design/branching/branching-gc/branching-retention-contract.md`.
    /// Per-branch manifests (own + inherited-layer entries) are the
    /// rebuild source of truth; runtime accelerator state
    /// (`BarrierKind::RuntimeAccelerator`) is rebuilt from them.
    /// `RecoveryHealth` is published before return so subsequent
    /// reclaim respects the rebuild trust rule and the
    /// degraded-recovery refusal gate
    /// (`BarrierKind::RecoveryHealthGate`, KD8). B5.2 extends this
    /// path with the §"Quarantine reconciliation" step against the
    /// chosen persisted quarantine inventory (KD9).
    pub fn recover_segments(&self) -> StorageResult<RecoveredState> {
        let mut invocation = RecoveryInvocationGuard::begin(&self.recovery_applied)?;
        let segments_dir = match &self.segments_dir {
            Some(d) => d,
            None => {
                self.last_recovery_health
                    .store(Arc::new(RecoveryHealth::Healthy));
                invocation.commit();
                return Ok(RecoveredState::empty());
            }
        };

        match std::fs::metadata(segments_dir) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let health = RecoveryHealth::from_faults(vec![RecoveryFault::Io(
                    missing_segments_dir_error(segments_dir),
                )]);
                self.last_recovery_health.store(Arc::new(health));
                return Err(missing_segments_dir_error(segments_dir).into());
            }
            Err(e) => {
                let health = RecoveryHealth::from_faults(vec![RecoveryFault::Io(io::Error::new(
                    e.kind(),
                    format!(
                        "failed to stat segments directory {}: {e}",
                        segments_dir.display()
                    ),
                ))]);
                self.last_recovery_health.store(Arc::new(health));
                return Err(e.into());
            }
        }

        let mut faults: Vec<RecoveryFault> = Vec::new();
        let mut branch_versions: HashMap<BranchId, CommitVersion> = HashMap::new();
        let mut version_floor = CommitVersion::ZERO;
        let mut segments_loaded: usize = 0;
        let mut recovered_branches: HashSet<BranchId> = HashSet::new();

        // Collect inherited layer info for deferred resolution (second pass).
        let mut deferred_inherited: Vec<(BranchId, Vec<crate::manifest::ManifestInheritedLayer>)> =
            Vec::new();

        // Top-level read_dir failure is pre-walk: we cannot even enumerate
        // branches, so surface it as a hard error. Still publish a degraded
        // health snapshot so observers do not retain stale prior state.
        let top_entries = match std::fs::read_dir(segments_dir) {
            Ok(entries) => entries,
            Err(e) => {
                let health = RecoveryHealth::from_faults(vec![RecoveryFault::Io(io::Error::new(
                    e.kind(),
                    format!(
                        "failed to enumerate segments directory {}: {e}",
                        segments_dir.display()
                    ),
                ))]);
                self.last_recovery_health.store(Arc::new(health));
                return Err(e.into());
            }
        };

        for entry in top_entries {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    faults.push(RecoveryFault::Io(e));
                    continue;
                }
            };
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
            let mut sst_names_on_disk: HashSet<String> = HashSet::new();

            // Per-branch read_dir failure is a fault for this branch; other
            // branches can still recover.
            let seg_entries = match std::fs::read_dir(&path) {
                Ok(it) => it,
                Err(e) => {
                    faults.push(RecoveryFault::Io(e));
                    continue;
                }
            };

            for seg_entry in seg_entries {
                let seg_entry = match seg_entry {
                    Ok(e) => e,
                    Err(e) => {
                        faults.push(RecoveryFault::Io(e));
                        continue;
                    }
                };
                let seg_path = seg_entry.path();
                if seg_path.extension().and_then(|e| e.to_str()) != Some("sst") {
                    continue;
                }
                if let Some(name) = seg_path.file_name().and_then(|n| n.to_str()) {
                    sst_names_on_disk.insert(name.to_string());
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
                        version_floor = version_floor.max(seg.commit_range().1);
                        branch_segments.push(Arc::new(seg));
                    }
                    Err(e) => {
                        faults.push(RecoveryFault::CorruptSegment {
                            file: seg_path,
                            inner: e,
                        });
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
                    faults.push(RecoveryFault::CorruptManifest {
                        branch_id,
                        inner: e,
                    });
                    continue;
                }
            };

            // Validate manifest-listed filenames against what is actually on
            // disk. Missing files are authoritative loss and must surface
            // regardless of whether we proceed to install this branch (e.g.
            // even if the branch appears "empty" because the sole listed file
            // was deleted).
            if let Some(ref manifest) = manifest {
                for entry in &manifest.entries {
                    if !sst_names_on_disk.contains(&entry.filename) {
                        faults.push(RecoveryFault::MissingManifestListed {
                            branch_id,
                            file: entry.filename.clone(),
                        });
                    }
                }
            }

            let has_inherited = manifest
                .as_ref()
                .is_some_and(|m| !m.inherited_layers.is_empty());
            if manifest.is_none() && branch_segments.is_empty() && !has_inherited {
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
                        // Missing-file fault was already emitted by the
                        // manifest-vs-disk validation pass above; no action
                        // here beyond the structural tracing::warn.
                        tracing::warn!(
                            branch = %dir_name,
                            segment = %entry.filename,
                            "manifest references segment not found on disk"
                        );
                    }
                }

                // Orphan files (on disk but not in manifest) stay informational
                // only — they are the expected shape of shared segments that
                // live on disk for COW children, and orphan GC (once SE3
                // lands) reclaims them later. Not a fault.
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
                    }
                }
            } else {
                // No manifest → all segments go to L0 (backward compat).
                // Classified as PolicyDowngrade so strict callers can refuse
                // while lossy callers with explicit opt-in continue.
                let promoted = branch_segments.len();
                level_segs[0] = branch_segments.clone();
                faults.push(RecoveryFault::NoManifestFallbackUsed {
                    branch_id,
                    segments_promoted: promoted,
                });
            }

            let own_segments_loaded = level_segs.iter().map(|l| l.len()).sum::<usize>();
            // A branch directory with an explicit manifest but zero own
            // segments loaded is retained orphan storage, not a live branch.
            // This is the deleted-parent-with-live-descendants shape: the
            // empty manifest suppresses legacy no-manifest fallback while
            // descendant inherited layers keep the top-level `.sst` files
            // reachable.
            if own_segments_loaded == 0 && !has_inherited {
                continue;
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

            segments_loaded += own_segments_loaded;
            if own_segments_loaded > 0 {
                recovered_branches.insert(branch_id);
            }

            // Rebuild branch.max_version from the loaded segments before we
            // install the version. Mirrors the snapshot-install pattern: post
            // -restart fork_branch reads this atomic to pick the fork source
            // version. Pre-SE2 the recovery walk skipped this and left it at
            // the constructor default (0).
            let mut branch_max = CommitVersion::ZERO;
            for level in &level_segs {
                for seg in level {
                    let v = seg.commit_range().1;
                    branch.track_version(v);
                    branch_max = branch_max.max(v);
                }
            }

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

            if branch_max > CommitVersion::ZERO {
                let entry = branch_versions
                    .entry(branch_id)
                    .or_insert(CommitVersion::ZERO);
                *entry = (*entry).max(branch_max);
            }

            // Collect inherited layer info for second pass (deferred resolution).
            if let Some(manifest) = manifest {
                if !manifest.inherited_layers.is_empty() {
                    deferred_inherited.push((branch_id, manifest.inherited_layers));
                }
            }
        }

        // Second pass + refcount rebuild. Inherited layers contribute their
        // own segment commit_max to version_floor and to the child branch's
        // branch_versions entry, and per-layer failures append faults.
        let inherited_stats = self.resolve_inherited_layers(
            segments_dir,
            deferred_inherited,
            &mut branch_versions,
            &mut recovered_branches,
            &mut faults,
        );
        version_floor = version_floor.max(inherited_stats.max_commit_id);
        segments_loaded += inherited_stats.segments_loaded;
        let branches_recovered = recovered_branches.len();

        // B5.2 — quarantine reconciliation. Reuse the same branch-dir
        // ledger shape that Stage-2 manifest proof and retention reporting
        // consume rather than reconstructing a second live-filename map
        // from self.branches.
        self.reconcile_quarantine_on_recovery(&mut faults);

        let health = RecoveryHealth::from_faults(faults);
        self.last_recovery_health.store(Arc::new(health.clone()));
        invocation.commit();

        Ok(RecoveredState {
            version_floor,
            branch_versions,
            segments_loaded,
            branches_recovered,
            health,
        })
    }

    /// Second pass of recovery: resolve inherited layers from manifests and
    /// rebuild segment refcounts.
    ///
    /// Folds each inherited layer segment's `commit_range().1` into both the
    /// child branch's entry in `branch_versions` and the returned
    /// `max_commit_id`, so the caller's `version_floor` accounts for versions
    /// visible only via inheritance. Layers whose entire source is
    /// unreachable produce a [`RecoveryFault::InheritedLayerLost`] in
    /// `faults`.
    fn resolve_inherited_layers(
        &self,
        segments_dir: &std::path::Path,
        deferred_inherited: Vec<(BranchId, Vec<crate::manifest::ManifestInheritedLayer>)>,
        branch_versions: &mut HashMap<BranchId, CommitVersion>,
        recovered_branches: &mut HashSet<BranchId>,
        faults: &mut Vec<RecoveryFault>,
    ) -> InheritedResolveStats {
        let mut stats = InheritedResolveStats::default();
        for (child_id, manifest_layers) in deferred_inherited {
            let mut inherited_layers = Vec::new();

            for ml in &manifest_layers {
                let source_dir = segments_dir.join(hex_encode_branch(&ml.source_branch_id));
                let mut layer_levels: Vec<Vec<Arc<KVSegment>>> = vec![Vec::new(); NUM_LEVELS];
                let mut any_found = false;
                let mut missing_entries = false;
                let mut layer_max = CommitVersion::ZERO;

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
                            let seg_max = seg.commit_range().1;
                            stats.max_commit_id = stats.max_commit_id.max(seg_max);
                            layer_max = layer_max.max(seg_max);
                            stats.segments_loaded += 1;
                            layer_levels[level].push(Arc::new(seg));
                            any_found = true;
                        }
                        Err(e) => {
                            missing_entries = true;
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
                    faults.push(RecoveryFault::InheritedLayerLost {
                        child: child_id,
                        source_branch: ml.source_branch_id,
                        fork_version: ml.fork_version,
                    });
                    continue;
                }

                if missing_entries {
                    faults.push(RecoveryFault::InheritedLayerLost {
                        child: child_id,
                        source_branch: ml.source_branch_id,
                        fork_version: ml.fork_version,
                    });
                }

                if layer_max > CommitVersion::ZERO {
                    let child_entry = branch_versions
                        .entry(child_id)
                        .or_insert(CommitVersion::ZERO);
                    *child_entry = (*child_entry).max(layer_max);
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
                recovered_branches.insert(child_id);
            }

            if !inherited_layers.is_empty() {
                if let Some(mut branch) = self.branches.get_mut(&child_id) {
                    // Fold inherited-layer max versions into the child's
                    // own max_version so post-restart fork_branch on the
                    // child picks up inherited data (#SG-008 broadening).
                    if let Some(v) = branch_versions.get(&child_id) {
                        branch.track_version(*v);
                    }
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

        stats
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
            let old = std::mem::replace(&mut branch.active, Arc::new(Memtable::new(next_id)));
            old.freeze();
            branch.frozen.insert(0, old);
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

    /// Total number of frozen memtables across all branches.
    pub fn total_frozen_count(&self) -> usize {
        self.total_frozen_count.load(Ordering::Relaxed)
    }

    /// Sum of `metadata_bytes()` across all open segments in all branches.
    ///
    /// This accounts for eagerly-loaded bloom filter partitions and index
    /// blocks that are pinned in heap memory outside the block cache.
    pub fn total_segment_metadata_bytes(&self) -> u64 {
        let mut total: u64 = 0;
        for entry in self.branches.iter() {
            let version = entry.value().version.load();
            for level in &version.levels {
                for seg in level {
                    total += seg.metadata_bytes();
                }
            }
            // Include inherited COW layers — their segments are also in memory.
            for layer in &entry.value().inherited_layers {
                for level in &layer.segments.levels {
                    for seg in level {
                        total += seg.metadata_bytes();
                    }
                }
            }
        }
        total
    }

    /// Total tracked memory: memtable bytes + segment metadata bytes.
    ///
    /// This is the correct input for memory pressure evaluation — it accounts
    /// for ALL major heap consumers outside the block cache.
    pub fn total_tracked_bytes(&self) -> u64 {
        self.total_memtable_bytes() + self.total_segment_metadata_bytes()
    }

    /// Check the current memory pressure level.
    ///
    /// Includes both memtable bytes and segment metadata (bloom + index) in
    /// the pressure calculation, ensuring segment accumulation is visible.
    pub fn pressure_level(&self) -> PressureLevel {
        self.pressure.level(self.total_tracked_bytes())
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
    #[allow(dead_code)]
    fn get_versioned_from_branch(
        branch: &BranchState,
        key: &Key,
        max_version: CommitVersion,
    ) -> StrataResult<Option<(CommitVersion, MemtableEntry)>> {
        // Encode once, reuse everywhere.
        let typed_key = encode_typed_key(key);
        let seek_ik = InternalKey::from_typed_key_bytes(&typed_key, CommitVersion::MAX);
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
            let src_seek_ik = InternalKey::from_typed_key_bytes(&src_typed_key, CommitVersion::MAX);
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
    #[allow(dead_code)]
    fn get_all_versions_from_branch(
        branch: &BranchState,
        key: &Key,
    ) -> StrataResult<Vec<(CommitVersion, MemtableEntry)>> {
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
                let mut iter = seg.iter_seek(key);
                for (ik, se) in iter.by_ref() {
                    if ik.typed_key_prefix() != typed_key.as_slice() {
                        break;
                    }
                    all_versions.push((se.commit_id, segment_entry_to_memtable_entry(se)));
                }
                if iter.corruption_detected() {
                    return Err(StorageError::corruption(
                        "segment scan stopped due to data block corruption",
                    )
                    .into());
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
                    let mut iter = seg.iter_seek(&src_key);
                    for (ik, se) in iter.by_ref() {
                        if ik.typed_key_prefix() != src_typed_key.as_slice() {
                            break;
                        }
                        if se.commit_id <= layer.fork_version {
                            all_versions.push((se.commit_id, segment_entry_to_memtable_entry(se)));
                        }
                    }
                    if iter.corruption_detected() {
                        return Err(StorageError::corruption(
                            "segment scan stopped due to data block corruption",
                        )
                        .into());
                    }
                }
            }
        }

        // Sort descending by commit_id (newest first) and deduplicate.
        // After recovery, the same (key, commit_id) may exist in both memtable
        // (from WAL replay) and segments (from disk), producing duplicates (#1733).
        all_versions.sort_by(|a, b| b.0.cmp(&a.0));
        all_versions.dedup_by_key(|entry| entry.0);
        Ok(all_versions)
    }

    /// Point lookup from a [`BranchSnapshot`] — no DashMap guard held during I/O.
    ///
    /// Identical to [`get_versioned_from_branch`] but operates on a snapshot
    /// whose fields are already cloned `Arc`s. This eliminates read-compaction
    /// contention: the DashMap shard lock is released before any segment I/O.
    fn get_versioned_from_snapshot(
        snapshot: &BranchSnapshot,
        key: &Key,
        max_version: CommitVersion,
    ) -> StorageResult<Option<(CommitVersion, MemtableEntry)>> {
        let profiling = read_profile_enabled();
        let t_total = if profiling {
            Some(Instant::now())
        } else {
            None
        };

        let t0 = if profiling {
            Some(Instant::now())
        } else {
            None
        };
        let typed_key = encode_typed_key(key);
        let seek_ik = InternalKey::from_typed_key_bytes(&typed_key, CommitVersion::MAX);
        let seek_bytes = seek_ik.as_bytes();
        let key_encode_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);

        // 1. Active memtable
        let t0 = if profiling {
            Some(Instant::now())
        } else {
            None
        };
        if let Some(result) =
            snapshot
                .active
                .get_versioned_preencoded(&typed_key, seek_bytes, max_version)
        {
            if profiling {
                let memtable_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
                let total_ns = t_total.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
                READ_PROF.with(|p| {
                    let mut p = p.borrow_mut();
                    p.count += 1;
                    p.found_active += 1;
                    p.key_encode_ns += key_encode_ns;
                    p.memtable_ns += memtable_ns;
                    p.total_ns += total_ns;
                    Self::maybe_print_read_profile(&mut p);
                });
            }
            return Ok(Some(result));
        }

        // 2. Frozen memtables (newest first)
        for frozen in &snapshot.frozen {
            if let Some(result) =
                frozen.get_versioned_preencoded(&typed_key, seek_bytes, max_version)
            {
                if profiling {
                    let memtable_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
                    let total_ns = t_total.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
                    READ_PROF.with(|p| {
                        let mut p = p.borrow_mut();
                        p.count += 1;
                        p.found_frozen += 1;
                        p.key_encode_ns += key_encode_ns;
                        p.memtable_ns += memtable_ns;
                        p.total_ns += total_ns;
                        Self::maybe_print_read_profile(&mut p);
                    });
                }
                return Ok(Some(result));
            }
        }
        let memtable_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);

        // 3. L0 segments (newest first, overlapping — linear scan)
        let t0 = if profiling {
            Some(Instant::now())
        } else {
            None
        };
        let l0_count = snapshot.segments.l0_segments().len();
        for seg in snapshot.segments.l0_segments() {
            if let Some(se) = seg.point_lookup_preencoded(&typed_key, seek_bytes, max_version)? {
                let commit_id = se.commit_id;
                if profiling {
                    let l0_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
                    let total_ns = t_total.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
                    READ_PROF.with(|p| {
                        let mut p = p.borrow_mut();
                        p.count += 1;
                        p.found_l0 += 1;
                        p.l0_probes += l0_count as u64;
                        p.key_encode_ns += key_encode_ns;
                        p.memtable_ns += memtable_ns;
                        p.l0_ns += l0_ns;
                        p.total_ns += total_ns;
                        Self::maybe_print_read_profile(&mut p);
                    });
                }
                return Ok(Some((commit_id, segment_entry_to_memtable_entry(se))));
            }
        }
        let l0_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);

        // 4. L1+ segments (non-overlapping, sorted by key range — binary search per level)
        let t0 = if profiling {
            Some(Instant::now())
        } else {
            None
        };
        let mut levels_checked = 0u64;
        let mut bloom_rejects = 0u64;
        let bloom_passes = 0u64;

        for level_idx in 1..snapshot.segments.levels.len() {
            let l1_segments = &snapshot.segments.levels[level_idx];
            if l1_segments.is_empty() {
                continue;
            }
            levels_checked += 1;

            if let Some(se) =
                point_lookup_level_preencoded(l1_segments, &typed_key, seek_bytes, max_version)?
            {
                let commit_id = se.commit_id;
                if profiling {
                    let l1plus_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
                    let total_ns = t_total.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
                    READ_PROF.with(|p| {
                        let mut p = p.borrow_mut();
                        p.count += 1;
                        p.found_l1plus += 1;
                        p.l0_probes += l0_count as u64;
                        p.key_encode_ns += key_encode_ns;
                        p.memtable_ns += memtable_ns;
                        p.l0_ns += l0_ns;
                        p.l1plus_ns += l1plus_ns;
                        p.total_ns += total_ns;
                        p.levels_checked += levels_checked;
                        p.bloom_rejects += bloom_rejects;
                        p.bloom_passes += 1;
                        Self::maybe_print_read_profile(&mut p);
                    });
                }
                return Ok(Some((commit_id, segment_entry_to_memtable_entry(se))));
            }
            // If we get here, bloom rejected or key not in this level
            bloom_rejects += 1;
        }

        // 5. Inherited layers (COW branching — nearest ancestor first)
        let l1plus_ns = t0.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
        for layer in &snapshot.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            let effective_version = max_version.min(layer.fork_version);
            let Some(src_typed_key) = rewrite_branch_id_bytes(&typed_key, &layer.source_branch_id)
            else {
                continue;
            };
            let src_seek_ik = InternalKey::from_typed_key_bytes(&src_typed_key, CommitVersion::MAX);
            let src_seek_bytes = src_seek_ik.as_bytes();

            for seg in layer.segments.l0_segments() {
                if let Some(se) =
                    seg.point_lookup_preencoded(&src_typed_key, src_seek_bytes, effective_version)?
                {
                    let commit_id = se.commit_id;
                    if profiling {
                        let total_ns = t_total.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
                        READ_PROF.with(|p| {
                            let mut p = p.borrow_mut();
                            p.count += 1;
                            p.found_inherited += 1;
                            p.l0_probes += l0_count as u64;
                            p.key_encode_ns += key_encode_ns;
                            p.memtable_ns += memtable_ns;
                            p.l0_ns += l0_ns;
                            p.l1plus_ns += l1plus_ns;
                            p.total_ns += total_ns;
                            Self::maybe_print_read_profile(&mut p);
                        });
                    }
                    return Ok(Some((commit_id, segment_entry_to_memtable_entry(se))));
                }
            }

            for level_idx in 1..layer.segments.levels.len() {
                if let Some(se) = point_lookup_level_preencoded(
                    &layer.segments.levels[level_idx],
                    &src_typed_key,
                    src_seek_bytes,
                    effective_version,
                )? {
                    let commit_id = se.commit_id;
                    if profiling {
                        let total_ns = t_total.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
                        READ_PROF.with(|p| {
                            let mut p = p.borrow_mut();
                            p.count += 1;
                            p.found_inherited += 1;
                            p.l0_probes += l0_count as u64;
                            p.key_encode_ns += key_encode_ns;
                            p.memtable_ns += memtable_ns;
                            p.l0_ns += l0_ns;
                            p.l1plus_ns += l1plus_ns;
                            p.total_ns += total_ns;
                            Self::maybe_print_read_profile(&mut p);
                        });
                    }
                    return Ok(Some((commit_id, segment_entry_to_memtable_entry(se))));
                }
            }
        }

        if profiling {
            let total_ns = t_total.map(|t| t.elapsed().as_nanos() as u64).unwrap_or(0);
            READ_PROF.with(|p| {
                let mut p = p.borrow_mut();
                p.count += 1;
                p.not_found += 1;
                p.l0_probes += l0_count as u64;
                p.key_encode_ns += key_encode_ns;
                p.memtable_ns += memtable_ns;
                p.l0_ns += l0_ns;
                p.l1plus_ns += l1plus_ns;
                p.total_ns += total_ns;
                p.levels_checked += levels_checked;
                p.bloom_rejects += bloom_rejects;
                p.bloom_passes += bloom_passes;
                Self::maybe_print_read_profile(&mut p);
            });
        }

        Ok(None)
    }

    fn maybe_print_read_profile(p: &mut ReadProfile) {
        if p.count % READ_PROFILE_INTERVAL == 0 {
            let n = READ_PROFILE_INTERVAL as f64;
            eprintln!(
                "[read-profile] {} reads | avg(us): total={:.2} key_enc={:.2} mem={:.2} l0={:.2} l1+={:.2} | \
                 found: active={:.0}% frozen={:.0}% l0={:.0}% l1+={:.0}% miss={:.0}% | l0_probes={:.1}",
                p.count,
                p.total_ns as f64 / n / 1000.0,
                p.key_encode_ns as f64 / n / 1000.0,
                p.memtable_ns as f64 / n / 1000.0,
                p.l0_ns as f64 / n / 1000.0,
                p.l1plus_ns as f64 / n / 1000.0,
                p.found_active as f64 / n * 100.0,
                p.found_frozen as f64 / n * 100.0,
                p.found_l0 as f64 / n * 100.0,
                p.found_l1plus as f64 / n * 100.0,
                p.not_found as f64 / n * 100.0,
                p.l0_probes as f64 / n,
            );
            if p.levels_checked > 0 {
                eprintln!(
                    "  L1+ detail: levels_checked={:.1}/read  bloom: pass={} reject={} ({:.0}% reject rate)",
                    p.levels_checked as f64 / n,
                    p.bloom_passes,
                    p.bloom_rejects,
                    if p.bloom_passes + p.bloom_rejects > 0 {
                        p.bloom_rejects as f64 / (p.bloom_passes + p.bloom_rejects) as f64 * 100.0
                    } else { 0.0 },
                );
            }
            // Reset
            p.found_active = 0;
            p.found_frozen = 0;
            p.found_l0 = 0;
            p.found_l1plus = 0;
            p.found_inherited = 0;
            p.not_found = 0;
            p.l0_probes = 0;
            p.snapshot_ns = 0;
            p.key_encode_ns = 0;
            p.memtable_ns = 0;
            p.l0_ns = 0;
            p.l1plus_ns = 0;
            p.total_ns = 0;
            p.levels_checked = 0;
            p.bloom_rejects = 0;
            p.bloom_passes = 0;
        }
    }

    /// Collect all versions of a key from a [`BranchSnapshot`] — no DashMap guard held.
    fn get_all_versions_from_snapshot(
        snapshot: &BranchSnapshot,
        key: &Key,
    ) -> StorageResult<Vec<(CommitVersion, MemtableEntry)>> {
        let mut all_versions = Vec::new();

        // Active memtable
        all_versions.extend(snapshot.active.get_all_versions(key));

        // Frozen memtables
        for frozen in &snapshot.frozen {
            all_versions.extend(frozen.get_all_versions(key));
        }

        // On-disk segments — all levels
        let typed_key = encode_typed_key(key);
        for level in &snapshot.segments.levels {
            for seg in level {
                let mut iter = seg.iter_seek(key);
                for (ik, se) in iter.by_ref() {
                    if ik.typed_key_prefix() != typed_key.as_slice() {
                        break;
                    }
                    all_versions.push((se.commit_id, segment_entry_to_memtable_entry(se)));
                }
                if iter.corruption_detected() {
                    return Err(StorageError::corruption(
                        "segment scan stopped due to data block corruption",
                    ));
                }
            }
        }

        // Inherited layers (COW branching)
        for layer in &snapshot.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            let src_key = key.with_branch_id(layer.source_branch_id);
            let src_typed_key = encode_typed_key(&src_key);
            for level in &layer.segments.levels {
                for seg in level {
                    let mut iter = seg.iter_seek(&src_key);
                    for (ik, se) in iter.by_ref() {
                        if ik.typed_key_prefix() != src_typed_key.as_slice() {
                            break;
                        }
                        if se.commit_id <= layer.fork_version {
                            all_versions.push((se.commit_id, segment_entry_to_memtable_entry(se)));
                        }
                    }
                    if iter.corruption_detected() {
                        return Err(StorageError::corruption(
                            "segment scan stopped due to data block corruption",
                        ));
                    }
                }
            }
        }

        all_versions.sort_by(|a, b| b.0.cmp(&a.0));
        all_versions.dedup_by_key(|entry| entry.0);
        Ok(all_versions)
    }

    /// Build a lazy `MergeIterator` over all sources in a branch.
    #[allow(clippy::type_complexity)]
    ///
    /// Sources are added in read-path order (LSM-003): active memtable →
    /// frozen memtables (newest first) → L0-L6 segments → inherited COW layers.
    ///
    /// Memtable iterators borrow from `branch` (lifetime `'b`).
    /// Segment iterators own `Arc<KVSegment>` via `OwnedSegmentIter` (`'static`).
    ///
    /// Returns the merge iterator and a vec of corruption flags — caller must
    /// check flags after consuming the iterator via `check_corruption()`.
    #[allow(dead_code)]
    fn build_branch_merge_iter<'b>(
        branch: &'b BranchState,
        prefix: &Key,
        start_key: &Key,
    ) -> StrataResult<(
        MergeIterator<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)> + 'b>>,
        Vec<Arc<AtomicBool>>,
    )> {
        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)> + 'b>> =
            Vec::new();
        let mut corruption_flags: Vec<Arc<AtomicBool>> = Vec::new();

        // Active memtable — lazy, borrows 'b
        sources.push(Box::new(branch.active.iter_range(start_key, prefix)));

        // Frozen memtables (newest first) — lazy, borrows 'b through Arc deref
        for frozen in &branch.frozen {
            sources.push(Box::new(frozen.iter_range(start_key, prefix)));
        }

        // On-disk segments — lazy iterators
        let prefix_bytes = encode_typed_key_prefix(prefix);
        let ver = branch.version.load();
        for (level_idx, level) in ver.levels.iter().enumerate() {
            if level.is_empty() {
                continue;
            }
            if level_idx == 0 {
                // L0: overlapping files — individual iterators (unavoidable)
                for seg in level {
                    if !segment_overlaps_prefix(seg, &prefix_bytes) {
                        continue;
                    }
                    let flag = Arc::new(AtomicBool::new(false));
                    corruption_flags.push(Arc::clone(&flag));
                    sources.push(Box::new(
                        OwnedSegmentIter::new_seek(
                            Arc::clone(seg),
                            start_key,
                            prefix_bytes.clone(),
                        )
                        .with_corruption_flag(flag)
                        .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se))),
                    ));
                }
            } else {
                // L1+: non-overlapping, sorted — one LevelSegmentIter per level.
                // Binary-searches to the relevant file, opens lazily (#2213).
                let level_segs: Vec<Arc<KVSegment>> = level
                    .iter()
                    .filter(|s| segment_overlaps_prefix(s, &prefix_bytes))
                    .cloned()
                    .collect();
                if level_segs.is_empty() {
                    continue;
                }
                let flag = Arc::new(AtomicBool::new(false));
                corruption_flags.push(Arc::clone(&flag));
                let mut level_iter = LevelSegmentIter::new(level_segs, prefix_bytes.clone(), flag);
                level_iter.seek(start_key);
                sources.push(Box::new(
                    level_iter.map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se))),
                ));
            }
        }

        // Inherited layers (COW branching) — lazy via OwnedSegmentIter + RewritingIterator
        for layer in &branch.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            let src_start = start_key.with_branch_id(layer.source_branch_id);
            let src_prefix = prefix.with_branch_id(layer.source_branch_id);
            let src_prefix_bytes = encode_typed_key_prefix(&src_prefix);
            for level in &layer.segments.levels {
                for seg in level {
                    if !segment_overlaps_prefix(seg, &src_prefix_bytes) {
                        continue;
                    }
                    let flag = Arc::new(AtomicBool::new(false));
                    corruption_flags.push(Arc::clone(&flag));
                    sources.push(Box::new(RewritingIterator::new(
                        OwnedSegmentIter::new_seek(
                            Arc::clone(seg),
                            &src_start,
                            src_prefix_bytes.clone(),
                        )
                        .with_corruption_flag(flag)
                        .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se))),
                        prefix.namespace.branch_id,
                        layer.fork_version,
                    )));
                }
            }
        }

        Ok((MergeIterator::new(sources), corruption_flags))
    }

    /// Build an owned `MergeIterator` from a [`BranchSnapshot`].
    ///
    /// Unlike [`build_branch_merge_iter`] (which borrows from `&BranchState`),
    /// this collects memtable entries into owned `Vec`s so the resulting iterator
    /// has no lifetime ties to the DashMap guard. Used by `StorageIterator` (Epic 5).
    ///
    /// Memtable collection is bounded by write_buffer_size from the seek position.
    /// Segment iteration is lazy via `OwnedSegmentIter`.
    #[allow(clippy::type_complexity, dead_code)]
    pub(crate) fn build_snapshot_merge_iter(
        snapshot: &BranchSnapshot,
        prefix: &Key,
        start_key: &Key,
    ) -> StorageResult<(
        MergeIterator<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>>,
        Vec<Arc<AtomicBool>>,
    )> {
        let mut sources: Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>> = Vec::new();
        let mut corruption_flags: Vec<Arc<AtomicBool>> = Vec::new();

        // Active memtable — collected (owned)
        let active_entries: Vec<_> = snapshot.active.iter_range(start_key, prefix).collect();
        sources.push(Box::new(active_entries.into_iter()));

        // Frozen memtables — collected (owned)
        for frozen in &snapshot.frozen {
            let entries: Vec<_> = frozen.iter_range(start_key, prefix).collect();
            sources.push(Box::new(entries.into_iter()));
        }

        // Segments — lazy via OwnedSegmentIter
        let prefix_bytes = encode_typed_key_prefix(prefix);
        for level in &snapshot.segments.levels {
            for seg in level {
                if !segment_overlaps_prefix(seg, &prefix_bytes) {
                    continue;
                }
                let flag = Arc::new(AtomicBool::new(false));
                corruption_flags.push(Arc::clone(&flag));
                sources.push(Box::new(
                    OwnedSegmentIter::new_seek(Arc::clone(seg), start_key, prefix_bytes.clone())
                        .with_corruption_flag(flag)
                        .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se))),
                ));
            }
        }

        // Inherited layers
        for layer in &snapshot.inherited_layers {
            if layer.status == LayerStatus::Materialized {
                continue;
            }
            let src_start = start_key.with_branch_id(layer.source_branch_id);
            let src_prefix = prefix.with_branch_id(layer.source_branch_id);
            let src_prefix_bytes = encode_typed_key_prefix(&src_prefix);
            for level in &layer.segments.levels {
                for seg in level {
                    if !segment_overlaps_prefix(seg, &src_prefix_bytes) {
                        continue;
                    }
                    let flag = Arc::new(AtomicBool::new(false));
                    corruption_flags.push(Arc::clone(&flag));
                    sources.push(Box::new(RewritingIterator::new(
                        OwnedSegmentIter::new_seek(
                            Arc::clone(seg),
                            &src_start,
                            src_prefix_bytes.clone(),
                        )
                        .with_corruption_flag(flag)
                        .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se))),
                        prefix.namespace.branch_id,
                        layer.fork_version,
                    )));
                }
            }
        }

        Ok((MergeIterator::new(sources), corruption_flags))
    }

    /// Build an MVCC-deduplicated prefix scan across all sources in a branch.
    #[allow(dead_code)]
    fn scan_prefix_from_branch(
        branch: &BranchState,
        prefix: &Key,
        max_version: CommitVersion,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        let (merge, flags) = Self::build_branch_merge_iter(branch, prefix, prefix)?;
        let mvcc = MvccIterator::new(merge, max_version);
        let results: Vec<_> = mvcc
            .filter_map(|(ik, entry)| {
                if entry.is_tombstone || entry.is_expired() {
                    return None;
                }
                let (key, commit_id) = ik.decode()?;
                Some((key, entry.to_versioned(commit_id)))
            })
            .collect();
        check_corruption(&flags)?;
        Ok(results)
    }

    /// Prefix scan from a [`BranchSnapshot`] — no DashMap guard held during I/O.
    fn scan_prefix_from_snapshot(
        snapshot: &BranchSnapshot,
        prefix: &Key,
        max_version: CommitVersion,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        let (merge, flags) = Self::build_snapshot_merge_iter(snapshot, prefix, prefix)?;
        let mvcc = MvccIterator::new(merge, max_version);
        let results: Vec<_> = mvcc
            .filter_map(|(ik, entry)| {
                if entry.is_tombstone || entry.is_expired() {
                    return None;
                }
                let (key, commit_id) = ik.decode()?;
                Some((key, entry.to_versioned(commit_id)))
            })
            .collect();
        check_corruption(&flags)?;
        Ok(results)
    }

    /// Range scan from a [`BranchSnapshot`] — no DashMap guard held during I/O.
    fn scan_range_from_snapshot(
        snapshot: &BranchSnapshot,
        prefix: &Key,
        start_key: &Key,
        max_version: CommitVersion,
        limit: Option<usize>,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        let (merge, flags) = Self::build_snapshot_merge_iter(snapshot, prefix, start_key)?;
        let mvcc = MvccIterator::new(merge, max_version);
        let filtered = mvcc
            .filter_map(|(ik, entry)| {
                if entry.is_tombstone || entry.is_expired() {
                    return None;
                }
                let (key, commit_id) = ik.decode()?;
                Some((key, entry.to_versioned(commit_id)))
            })
            .filter(|(key, _)| key >= start_key);
        let results: Vec<_> = match limit {
            Some(n) => filtered.take(n).collect(),
            None => filtered.collect(),
        };
        check_corruption(&flags)?;
        Ok(results)
    }

    /// Count prefix entries from a [`BranchSnapshot`] — no DashMap guard held during I/O.
    fn count_prefix_from_snapshot(
        snapshot: &BranchSnapshot,
        prefix: &Key,
        max_version: CommitVersion,
    ) -> StorageResult<u64> {
        let (merge, flags) = Self::build_snapshot_merge_iter(snapshot, prefix, prefix)?;
        let mvcc = MvccIterator::new(merge, max_version);
        let count = mvcc
            .filter(|(_, entry)| !entry.is_tombstone && !entry.is_expired())
            .filter(|(ik, _)| ik.decode().is_some())
            .count() as u64;
        check_corruption(&flags)?;
        Ok(count)
    }

    /// Capture a point-in-time snapshot of a branch.
    ///
    /// The DashMap read guard is held only for the duration of the `Arc` clones
    /// (nanoseconds). The returned snapshot is valid independently — memtable
    /// rotation and compaction create new versions without invalidating it.
    pub(crate) fn snapshot_branch(&self, branch_id: &BranchId) -> Option<BranchSnapshot> {
        let branch = self.branches.get(branch_id)?;
        Some(BranchSnapshot {
            active: Arc::clone(&branch.active),
            frozen: branch.frozen.clone(),
            segments: branch.version.load_full(),
            inherited_layers: branch.inherited_layers.clone(),
        })
    }

    /// Create a persistent [`StorageIterator`] for a branch.
    ///
    /// Captures a [`BranchSnapshot`] and returns an iterator that supports
    /// `seek()` + `next()` cycles for cursor-based pagination.
    pub fn new_storage_iterator(
        &self,
        branch_id: &BranchId,
        prefix: Key,
        snapshot_version: CommitVersion,
    ) -> Option<StorageIterator> {
        let snapshot = self.snapshot_branch(branch_id)?;
        Some(StorageIterator::new(snapshot, prefix, snapshot_version))
    }

    /// Scan entries starting from `start_key` within `prefix`, with optional limit.
    ///
    /// Uses the lazy merge pipeline with seek pushdown: sources start from
    /// `start_key` (not the beginning of the prefix), and `.take(limit)` stops
    /// iteration early. For `kv_scan(start, limit=10)` on 50K records, this
    /// reduces work from O(N) to O(log N + limit).
    pub fn scan_range(
        &self,
        prefix: &Key,
        start_key: &Key,
        max_version: CommitVersion,
        limit: Option<usize>,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        let snapshot = match self.snapshot_branch(&prefix.namespace.branch_id) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        Self::scan_range_from_snapshot(&snapshot, prefix, start_key, max_version, limit)
    }

    /// Bounded range scan with seek + limit pushdown.
    #[allow(dead_code)]
    fn scan_range_from_branch(
        branch: &BranchState,
        prefix: &Key,
        start_key: &Key,
        max_version: CommitVersion,
        limit: Option<usize>,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        let (merge, flags) = Self::build_branch_merge_iter(branch, prefix, start_key)?;
        let mvcc = MvccIterator::new(merge, max_version);
        let filtered = mvcc
            .filter_map(|(ik, entry)| {
                if entry.is_tombstone || entry.is_expired() {
                    return None;
                }
                let (key, commit_id) = ik.decode()?;
                Some((key, entry.to_versioned(commit_id)))
            })
            .filter(|(key, _)| key >= start_key); // block seek imprecision
        let results: Vec<_> = match limit {
            Some(n) => filtered.take(n).collect(),
            None => filtered.collect(),
        };
        check_corruption(&flags)?;
        Ok(results)
    }

    /// Count entries matching a prefix without collecting into a Vec.
    ///
    /// Uses the same merge/MVCC pipeline as `scan_prefix` but only counts
    /// the live (non-tombstone, non-expired) entries, avoiding the O(N)
    /// `Vec<(Key, VersionedValue)>` allocation.
    pub fn count_prefix(&self, prefix: &Key, max_version: CommitVersion) -> StorageResult<u64> {
        let branch_id = prefix.namespace.branch_id;

        // O(1) fast path: unprefixed count at latest version uses
        // RocksDB-style EstimateNumKeys (entries - deletions).
        // This only needs a brief DashMap read for the atomic counters.
        if prefix.user_key.is_empty() && max_version == CommitVersion::MAX {
            if let Some(branch) = self.branches.get(&branch_id) {
                return Ok(branch.estimate_live_keys());
            }
            return Ok(0);
        }

        let snapshot = match self.snapshot_branch(&branch_id) {
            Some(s) => s,
            None => return Ok(0),
        };
        Self::count_prefix_from_snapshot(&snapshot, prefix, max_version)
    }

    /// Count MVCC-deduplicated entries in a branch without collecting.
    #[allow(dead_code)]
    fn count_prefix_from_branch(
        branch: &BranchState,
        prefix: &Key,
        max_version: CommitVersion,
    ) -> StrataResult<u64> {
        let (merge, flags) = Self::build_branch_merge_iter(branch, prefix, prefix)?;
        let mvcc = MvccIterator::new(merge, max_version);
        let count = mvcc
            .filter(|(_, entry)| !entry.is_tombstone && !entry.is_expired())
            .filter(|(ik, _)| ik.decode().is_some())
            .count() as u64;
        check_corruption(&flags)?;
        Ok(count)
    }

    /// Write the manifest file for a branch, reflecting current level assignments
    /// and inherited layers.
    fn write_branch_manifest(&self, branch_id: &BranchId) -> StorageResult<()> {
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return Ok(()),
        };
        let ver = branch.version.load();
        self.write_branch_manifest_from_state(branch_id, &ver, &branch.inherited_layers)
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
    fn get_versioned(
        &self,
        key: &Key,
        max_version: CommitVersion,
    ) -> StrataResult<Option<VersionedValue>> {
        let snapshot = match self.snapshot_branch(&key.namespace.branch_id) {
            Some(s) => s,
            None => return Ok(None),
        };

        match Self::get_versioned_from_snapshot(&snapshot, key, max_version)? {
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
        before_version: Option<CommitVersion>,
    ) -> StrataResult<Vec<VersionedValue>> {
        let snapshot = match self.snapshot_branch(&key.namespace.branch_id) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let all_versions = Self::get_all_versions_from_snapshot(&snapshot, key)?;

        let results: Vec<VersionedValue> = all_versions
            .into_iter()
            .filter(|(commit_id, entry)| {
                if let Some(bv) = before_version {
                    if *commit_id >= bv {
                        return false;
                    }
                }
                !entry.is_expired()
            })
            .map(|(commit_id, entry)| entry.to_versioned(commit_id))
            .collect();

        match limit {
            Some(n) => Ok(results.into_iter().take(n).collect()),
            None => Ok(results),
        }
    }

    fn scan_prefix(
        &self,
        prefix: &Key,
        max_version: CommitVersion,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        let snapshot = match self.snapshot_branch(&prefix.namespace.branch_id) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        Self::scan_prefix_from_snapshot(&snapshot, prefix, max_version)
    }

    fn current_version(&self) -> CommitVersion {
        CommitVersion(self.version.load(Ordering::Acquire))
    }

    fn put_with_version_mode(
        &self,
        key: Key,
        value: Value,
        version: CommitVersion,
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

        // Track entry count for O(1) EstimateNumKeys (#2187).
        branch.num_entries.fetch_add(1, Ordering::Relaxed);

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
        branch.track_version(version);

        // Rotate if active memtable exceeds threshold
        self.maybe_rotate_branch(branch_id, &mut branch);

        // Update global version
        self.version.fetch_max(version.as_u64(), Ordering::AcqRel);

        Ok(())
    }

    fn delete_with_version(&self, key: &Key, version: CommitVersion) -> StrataResult<()> {
        let branch_id = key.namespace.branch_id;

        let mut branch = self
            .branches
            .entry(branch_id)
            .or_insert_with(BranchState::new);

        // Track deletion count for O(1) EstimateNumKeys (#2187).
        branch.num_deletions.fetch_add(1, Ordering::Relaxed);

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
        branch.track_version(version);

        // Rotate if active memtable exceeds threshold
        self.maybe_rotate_branch(branch_id, &mut branch);

        // Update global version
        self.version.fetch_max(version.as_u64(), Ordering::AcqRel);

        Ok(())
    }

    fn apply_batch(
        &self,
        writes: Vec<(Key, Value, WriteMode)>,
        version: CommitVersion,
    ) -> StrataResult<()> {
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
            branch.track_version(version);
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        self.version.fetch_max(version.as_u64(), Ordering::AcqRel);
        Ok(())
    }

    fn delete_batch(&self, deletes: Vec<Key>, version: CommitVersion) -> StrataResult<()> {
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
            branch.track_version(version);
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        self.version.fetch_max(version.as_u64(), Ordering::AcqRel);
        Ok(())
    }

    fn apply_writes_atomic(
        &self,
        writes: Vec<(Key, Value, WriteMode)>,
        deletes: Vec<Key>,
        version: CommitVersion,
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
            let put_count = entries.len() as u64;
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
            branch.num_entries.fetch_add(put_count, Ordering::Relaxed);
            branch.track_timestamp(ts);
            branch.track_version(version);
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        // Install tombstones.
        for (branch_id, keys) in deletes_by_branch {
            let mut branch = self
                .branches
                .entry(branch_id)
                .or_insert_with(BranchState::new);
            let del_count = keys.len() as u64;
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
            branch.num_deletions.fetch_add(del_count, Ordering::Relaxed);
            branch.track_timestamp(ts);
            branch.track_version(version);
            self.maybe_rotate_branch(branch_id, &mut branch);
        }

        // Advance version only after ALL entries are installed.
        self.version.fetch_max(version.as_u64(), Ordering::AcqRel);
        Ok(())
    }

    fn get_version_only(&self, key: &Key) -> StrataResult<Option<CommitVersion>> {
        let snapshot = match self.snapshot_branch(&key.namespace.branch_id) {
            Some(s) => s,
            None => return Ok(None),
        };

        match Self::get_versioned_from_snapshot(&snapshot, key, CommitVersion::MAX)? {
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
pub(crate) fn hex_encode_branch(branch_id: &BranchId) -> String {
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
pub(crate) fn hex_decode_branch(hex: &str) -> Option<BranchId> {
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
    max_version: CommitVersion,
) -> StorageResult<Option<SegmentEntry>> {
    let typed_key = encode_typed_key(key);
    let seek_ik = InternalKey::from_typed_key_bytes(&typed_key, CommitVersion::MAX);
    point_lookup_level_preencoded(l1_segments, &typed_key, seek_ik.as_bytes(), max_version)
}

fn point_lookup_level_preencoded(
    l1_segments: &[Arc<KVSegment>],
    typed_key: &[u8],
    seek_bytes: &[u8],
    max_version: CommitVersion,
) -> StorageResult<Option<SegmentEntry>> {
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

/// Check if any segment reported corruption during lazy iteration.
///
/// Called after consuming a `MergeIterator` whose segment sources carry
/// `Arc<AtomicBool>` corruption flags via `OwnedSegmentIter::with_corruption_flag`.
fn check_corruption(flags: &[Arc<AtomicBool>]) -> StorageResult<()> {
    for flag in flags {
        if flag.load(Ordering::Relaxed) {
            return Err(StorageError::corruption(
                "segment scan stopped due to data block corruption",
            ));
        }
    }
    Ok(())
}

mod compaction;
mod quarantine_protocol;
mod recovery;
mod ref_registry;

pub use quarantine_protocol::{PurgeReport, StorageBranchRetention, StorageInheritedLayerInfo};
pub use recovery::{DegradationClass, RecoveredState, RecoveryFault, RecoveryHealth};

#[cfg(test)]
mod tests;
