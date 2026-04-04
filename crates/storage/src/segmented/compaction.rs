//! Compaction scheduling and execution for the segmented store.
//!
//! Contains:
//! - Dynamic level targets (`recalculate_level_targets`)
//! - Compaction scoring (`compute_compaction_scores`, `pick_and_compact`)
//! - Compaction methods (`compact_branch`, `compact_tier`, `compact_l0_to_l1`,
//!   `compact_level`)
//! - Multi-level compaction helpers

use super::*;
use std::sync::atomic::AtomicBool;

/// Per-level byte targets, computed dynamically from actual data sizes.
#[derive(Debug, Clone)]
pub(super) struct LevelTargets {
    pub(super) max_bytes: [u64; NUM_LEVELS],
}

/// Compute adaptive per-level byte targets from actual level sizes.
///
/// Algorithm (RocksDB `CalculateBaseBytes` with dynamic base_level):
/// 1. Find the largest non-empty non-L0 level (the "bottom" level).
/// 2. Compute unclamped base = bottom_bytes / multiplier^(bottom_level - 1).
/// 3. If unclamped base ≥ MIN_BASE_BYTES, use base_level = 1 and clamp base
///    to [MIN_BASE_BYTES, `max_base`].
/// 4. Otherwise, raise base_level until the base ≥ MIN_BASE_BYTES. Levels
///    below base_level get `max_base` as a passive lower-bound clamp,
///    preventing pathological "hourglass" shapes with tiny intermediate targets.
/// 5. Forward-compute targets from base_level: target[i] = base * multiplier^(i - base_level).
///
/// For empty databases (no non-L0 data), uses MIN_BASE_BYTES as base from L1.
pub(super) fn recalculate_level_targets(
    level_bytes: &[u64; NUM_LEVELS],
    max_base: u64,
) -> LevelTargets {
    let max_base = max_base.max(MIN_BASE_BYTES); // safety: never below minimum
    let mut max_bytes = [0u64; NUM_LEVELS];
    max_bytes[0] = 0; // L0 uses count-based trigger

    // 1. Find the largest non-empty non-L0 level
    let mut bottom_level = 0usize;
    let mut bottom_bytes = 0u64;
    for (level, &bytes) in level_bytes.iter().enumerate().skip(1) {
        if bytes > bottom_bytes {
            bottom_level = level;
            bottom_bytes = bytes;
        }
    }

    if bottom_level == 0 {
        // No non-L0 data — forward-compute from MIN_BASE_BYTES at L1.
        let mut target = MIN_BASE_BYTES;
        for slot in max_bytes.iter_mut().skip(1) {
            *slot = target;
            target = target.saturating_mul(LEVEL_MULTIPLIER);
        }
        return LevelTargets { max_bytes };
    }

    // 2. Compute unclamped base (what L1 target would be without clamping)
    let mut unclamped_base = bottom_bytes;
    for _ in 1..bottom_level {
        unclamped_base /= LEVEL_MULTIPLIER;
    }

    // 3. Determine dynamic base_level
    let (base_level, base) = if unclamped_base >= MIN_BASE_BYTES {
        // Base is large enough — use L1 as base_level (no inactive levels)
        (1, unclamped_base.clamp(MIN_BASE_BYTES, max_base))
    } else {
        // Base too small — raise base_level until base ≥ MIN_BASE_BYTES
        let mut bl = 1;
        let mut b = unclamped_base;
        while b < MIN_BASE_BYTES && bl < bottom_level {
            b = b.saturating_mul(LEVEL_MULTIPLIER);
            bl += 1;
        }
        (bl, b.clamp(MIN_BASE_BYTES, max_base))
    };

    // 4. Lower-bound clamp: levels below base_level get passive max_base
    for slot in max_bytes.iter_mut().take(base_level).skip(1) {
        *slot = max_base;
    }

    // 5. Forward-compute targets from base_level
    let mut target = base;
    for slot in max_bytes.iter_mut().skip(base_level) {
        *slot = target;
        target = target.saturating_mul(LEVEL_MULTIPLIER);
    }

    LevelTargets { max_bytes }
}
///
/// Checks if the segment is shared (referenced by child branches via COW
/// inherited layers). Shared segments are skipped — their files are only
impl SegmentedStore {
    /// Delete a segment file if it is not referenced by any inherited layer.
    ///
    /// Checks if the segment is shared (referenced by child branches via COW
    /// inherited layers). Shared segments are skipped — their files are only
    /// deleted when the last child releases via `decrement` (in `clear_branch`
    /// or `materialize_layer`). Untracked segments (not shared) are deleted
    /// immediately.
    fn delete_segment_if_unreferenced(&self, seg: &KVSegment) {
        let _guard = self.ref_registry.deletion_write_guard();
        if !self.ref_registry.is_referenced(seg.file_id()) {
            crate::block_cache::global_cache().invalidate_file(seg.file_id());
            let _ = std::fs::remove_file(seg.file_path());
        }
    }

    /// Compute compaction scores for all levels of a branch.
    ///
    /// - L0: `score = file_count / L0_COMPACTION_TRIGGER`
    /// - L1..L(N-2): `score = actual_bytes / target_bytes` (last level excluded)
    ///
    /// Returns scores sorted by descending score (highest priority first).
    pub fn compute_compaction_scores(&self, branch_id: &BranchId) -> Vec<CompactionScore> {
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return Vec::new(),
        };
        let ver = branch.version.load();

        // Gather actual bytes per level
        let mut actual_bytes = [0u64; NUM_LEVELS];
        for (level, segs) in ver.levels.iter().enumerate() {
            actual_bytes[level] = segs.iter().map(|s| s.file_size()).sum();
        }

        let targets = &branch.level_targets;

        let mut scores = Vec::new();

        // L0: count-based
        let l0_count = ver.l0_segments().len();
        scores.push(CompactionScore {
            level: 0,
            score: l0_count as f64 / L0_COMPACTION_TRIGGER as f64,
        });

        // L1 through L(N-2) — exclude last level (can't compact further)
        for (level, &bytes) in actual_bytes.iter().enumerate().take(NUM_LEVELS - 1).skip(1) {
            let target = targets.max_bytes[level];
            if target > 0 {
                scores.push(CompactionScore {
                    level,
                    score: bytes as f64 / target as f64,
                });
            }
        }

        scores.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scores
    }

    /// Pick the highest-scoring level that needs compaction and compact it.
    ///
    /// Returns `Ok(Some(result))` if a compaction was performed, `Ok(None)` if
    /// no level is over target, or `Err` on I/O failure.
    pub fn pick_and_compact(
        &self,
        branch_id: &BranchId,
        prune_floor: u64,
    ) -> io::Result<Option<PickAndCompactResult>> {
        if self.segments_dir.is_none() {
            return Ok(None);
        }

        let scores = self.compute_compaction_scores(branch_id);

        for cs in &scores {
            if cs.score < 1.0 {
                break; // sorted descending — no more levels over target
            }
            if let Some(compaction) = self.compact_level(branch_id, cs.level, prune_floor)? {
                return Ok(Some(PickAndCompactResult {
                    level: cs.level,
                    compaction,
                }));
            }
        }

        Ok(None)
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
        let (old_segments, total_input_entries, is_bottommost) = {
            let branch = match self.branches.get(branch_id) {
                Some(b) => b,
                None => return Ok(None),
            };
            let ver = branch.version.load_full();
            if ver.l0_segments().len() < 2 {
                return Ok(None);
            }
            let segs: Vec<Arc<KVSegment>> = ver.l0_segments().to_vec();
            let total: u64 = segs.iter().map(|s| s.entry_count()).sum();
            // Output stays in L0; bottommost if L1–L6 are all empty.
            let bottom = (1..ver.levels.len()).all(|l| ver.levels[l].is_empty());
            (segs, total, bottom)
            // DashMap guard drops here
        };

        let segments_merged = old_segments.len();

        // Build compaction (no lock held — I/O heavy).
        let seg_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let branch_hex = hex_encode_branch(branch_id);
        let branch_dir = segments_dir.join(&branch_hex);
        std::fs::create_dir_all(&branch_dir)?;
        let seg_path = branch_dir.join(format!("{}.sst", seg_id));

        // Build streaming source iterators from each segment.
        let limiter = self.compaction_rate_limiter.load_full();
        let (sources, corruption_flags) = streaming_sources(&old_segments, &limiter);

        let merge = MergeIterator::new(sources);
        let max_versions = self.max_versions_per_key.load(Ordering::Relaxed);
        let snap_floor = self.snapshot_floor.load(Ordering::Relaxed);
        let compaction_iter = CompactionIterator::new(merge, prune_floor)
            .with_max_versions(max_versions)
            .with_snapshot_floor(snap_floor)
            .with_drop_expired(is_bottommost)
            .with_is_bottommost(is_bottommost);

        let mut builder = self
            .make_segment_builder()
            .with_compression(crate::segment_builder::CompressionCodec::None);
        if let Some(ref l) = limiter {
            builder = builder.with_rate_limiter(Arc::clone(l));
        }
        let meta = match builder.build_from_iter(compaction_iter, &seg_path) {
            Ok(meta) => meta,
            Err(e) => {
                cleanup_partial_compaction_outputs(&branch_dir, seg_id, seg_id + 1);
                return Err(e);
            }
        };
        if let Err(e) = check_corruption_flags(&corruption_flags) {
            cleanup_partial_compaction_outputs(&branch_dir, seg_id, seg_id + 1);
            return Err(e);
        }

        // Open the newly written segment.
        let new_segment = match KVSegment::open(&seg_path) {
            Ok(seg) => seg,
            Err(e) => {
                cleanup_partial_compaction_outputs(&branch_dir, seg_id, seg_id + 1);
                return Err(e);
            }
        };

        // Swap: remove only the segments we compacted, insert the new one.
        // Any segments added by concurrent flushes (not in old_segments) are kept.
        {
            let mut branch = self
                .branches
                .entry(*branch_id)
                .or_insert_with(BranchState::new);
            let cur_ver = branch.version.load();
            let mut new_l0: Vec<Arc<KVSegment>> = cur_ver
                .l0_segments()
                .iter()
                .filter(|s| !old_segments.iter().any(|old| Arc::ptr_eq(s, old)))
                .cloned()
                .collect();
            // Compacted segment goes at the end (oldest — covers the range
            // of all old segments). Any concurrent segments are newer and
            // already at the front.
            new_l0.push(Arc::new(new_segment));
            let mut new_levels = cur_ver.levels.clone();
            new_levels[0] = new_l0;
            branch
                .version
                .store(Arc::new(SegmentVersion { levels: new_levels }));
            refresh_level_targets(&mut branch, self.level_base_bytes());
        }

        // Persist manifest BEFORE deleting old files — if we crash after
        // delete but before manifest write, recovery would reference missing
        // segments. Writing manifest first ensures crash recovery can always
        // find the new compacted segment.
        self.write_branch_manifest(branch_id);

        // Now safe to delete old segment files (refcount-guarded).
        for seg in &old_segments {
            self.delete_segment_if_unreferenced(seg);
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
            .is_some_and(|b| b.version.load().l0_segments().len() >= segment_threshold)
    }

    /// Get file sizes of all segments for a branch.
    ///
    /// Returns sizes in the same order as the branch's segment list (newest first).
    /// Used by `CompactionScheduler` to assign segments to tiers.
    pub fn segment_file_sizes(&self, branch_id: &BranchId) -> Vec<u64> {
        let branch = match self.branches.get(branch_id) {
            Some(b) => b,
            None => return Vec::new(),
        };
        let ver = branch.version.load();
        ver.l0_segments().iter().map(|s| s.file_size()).collect()
    }

    /// Compact a specific subset of segments for a branch (tier-based compaction).
    ///
    /// Unlike `compact_branch()` which merges all segments, this merges only
    /// the segments at the given indices. Returns `Ok(None)` if fewer than 2
    /// segments are selected or if the branch/segments don't exist.
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

        // Snapshot selected segments under a read guard.
        let (selected_segments, total_input_entries, is_bottommost) = {
            let branch = match self.branches.get(branch_id) {
                Some(b) => b,
                None => return Ok(None),
            };
            let ver = branch.version.load_full();
            let mut segs: Vec<Arc<KVSegment>> = Vec::new();
            for &idx in segment_indices {
                if let Some(seg) = ver.l0_segments().get(idx) {
                    segs.push(Arc::clone(seg));
                }
            }
            if segs.len() < 2 {
                return Ok(None);
            }
            let total: u64 = segs.iter().map(|s| s.entry_count()).sum();
            // Output stays in L0; bottommost if L1–L6 are all empty.
            let bottom = (1..ver.levels.len()).all(|l| ver.levels[l].is_empty());
            (segs, total, bottom)
            // DashMap guard drops here
        };

        let segments_merged = selected_segments.len();

        // Build compaction output (no lock held — I/O heavy)
        let seg_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let branch_hex = hex_encode_branch(branch_id);
        let branch_dir = segments_dir.join(&branch_hex);
        std::fs::create_dir_all(&branch_dir)?;
        let seg_path = branch_dir.join(format!("{}.sst", seg_id));

        let limiter = self.compaction_rate_limiter.load_full();
        let (sources, corruption_flags) = streaming_sources(&selected_segments, &limiter);

        let merge = MergeIterator::new(sources);
        let max_versions = self.max_versions_per_key.load(Ordering::Relaxed);
        let snap_floor = self.snapshot_floor.load(Ordering::Relaxed);
        let compaction_iter = CompactionIterator::new(merge, prune_floor)
            .with_max_versions(max_versions)
            .with_snapshot_floor(snap_floor)
            .with_drop_expired(is_bottommost)
            .with_is_bottommost(is_bottommost);

        let mut builder = self
            .make_segment_builder()
            .with_compression(crate::segment_builder::CompressionCodec::None);
        if let Some(ref l) = limiter {
            builder = builder.with_rate_limiter(Arc::clone(l));
        }
        let meta = match builder.build_from_iter(compaction_iter, &seg_path) {
            Ok(meta) => meta,
            Err(e) => {
                cleanup_partial_compaction_outputs(&branch_dir, seg_id, seg_id + 1);
                return Err(e);
            }
        };
        if let Err(e) = check_corruption_flags(&corruption_flags) {
            cleanup_partial_compaction_outputs(&branch_dir, seg_id, seg_id + 1);
            return Err(e);
        }

        let new_segment = match KVSegment::open(&seg_path) {
            Ok(seg) => seg,
            Err(e) => {
                cleanup_partial_compaction_outputs(&branch_dir, seg_id, seg_id + 1);
                return Err(e);
            }
        };

        // Swap: remove only the segments we compacted, insert the new one.
        // Any segments added by concurrent flushes (not in selected_segments) are kept.
        {
            let mut branch = self
                .branches
                .entry(*branch_id)
                .or_insert_with(BranchState::new);
            let cur_ver = branch.version.load();
            let mut new_l0: Vec<Arc<KVSegment>> = cur_ver
                .l0_segments()
                .iter()
                .filter(|s| !selected_segments.iter().any(|old| Arc::ptr_eq(s, old)))
                .cloned()
                .collect();
            // Compacted segment goes at the end (oldest — covers the range
            // of all compacted segments). Any concurrent segments are newer
            // and already at the front.
            new_l0.push(Arc::new(new_segment));
            let mut new_levels = cur_ver.levels.clone();
            new_levels[0] = new_l0;
            branch
                .version
                .store(Arc::new(SegmentVersion { levels: new_levels }));
            refresh_level_targets(&mut branch, self.level_base_bytes());
        }

        // Persist manifest BEFORE deleting old files (crash safety).
        self.write_branch_manifest(branch_id);

        // Delete old segment files (refcount-guarded).
        for seg in &selected_segments {
            self.delete_segment_if_unreferenced(seg);
        }

        let entries_pruned = total_input_entries.saturating_sub(meta.entry_count);

        Ok(Some(CompactionResult {
            segments_merged,
            output_entries: meta.entry_count,
            entries_pruned,
            output_file_size: meta.file_size,
        }))
    }

    /// Compact all L0 segments into L1, merging with overlapping L1 segments.
    ///
    /// Returns `Ok(None)` if there is nothing to compact (ephemeral mode,
    /// branch missing, or L0 is empty). Returns `Ok(Some(result))` with
    /// compaction statistics on success.
    ///
    /// The output L1 is non-overlapping because we merge ALL L0 segments
    /// plus all overlapping L1 segments into a single output segment.
    pub fn compact_l0_to_l1(
        &self,
        branch_id: &BranchId,
        prune_floor: u64,
    ) -> io::Result<Option<CompactionResult>> {
        let segments_dir = match &self.segments_dir {
            Some(d) => d,
            None => return Ok(None),
        };

        // Snapshot current version.
        let (l0_segs, l1_segs, is_bottommost) = {
            let branch = match self.branches.get(branch_id) {
                Some(b) => b,
                None => return Ok(None),
            };
            let ver = branch.version.load_full();
            if ver.l0_segments().is_empty() {
                return Ok(None);
            }
            let l0: Vec<Arc<KVSegment>> = ver.l0_segments().to_vec();
            let l1: Vec<Arc<KVSegment>> = ver.l1_segments().to_vec();
            // Output goes to L1; bottommost if L2–L6 are all empty.
            let bottom = (2..ver.levels.len()).all(|l| ver.levels[l].is_empty());
            (l0, l1, bottom)
        };

        // Compute overall L0 key range.
        let mut l0_min: Option<Vec<u8>> = None;
        let mut l0_max: Option<Vec<u8>> = None;
        for seg in &l0_segs {
            let (seg_min, seg_max) = seg.key_range();
            if seg_min.is_empty() && seg_max.is_empty() {
                continue; // empty segment
            }
            let seg_min_prefix = if seg_min.len() >= COMMIT_ID_SUFFIX_LEN {
                &seg_min[..seg_min.len() - COMMIT_ID_SUFFIX_LEN]
            } else {
                seg_min
            };
            let seg_max_prefix = if seg_max.len() >= COMMIT_ID_SUFFIX_LEN {
                &seg_max[..seg_max.len() - COMMIT_ID_SUFFIX_LEN]
            } else {
                seg_max
            };
            match &l0_min {
                None => l0_min = Some(seg_min_prefix.to_vec()),
                Some(cur) => {
                    if seg_min_prefix < cur.as_slice() {
                        l0_min = Some(seg_min_prefix.to_vec());
                    }
                }
            }
            match &l0_max {
                None => l0_max = Some(seg_max_prefix.to_vec()),
                Some(cur) => {
                    if seg_max_prefix > cur.as_slice() {
                        l0_max = Some(seg_max_prefix.to_vec());
                    }
                }
            }
        }

        // Partition L1 into overlapping vs non-overlapping with L0 range.
        let (overlapping_l1, non_overlapping_l1): (Vec<_>, Vec<_>) =
            if let (Some(l0_min), Some(l0_max)) = (&l0_min, &l0_max) {
                l1_segs.iter().cloned().partition(|seg| {
                    let (seg_min, seg_max) = seg.key_range();
                    if seg_min.is_empty() && seg_max.is_empty() {
                        return false;
                    }
                    let seg_min_prefix = if seg_min.len() >= COMMIT_ID_SUFFIX_LEN {
                        &seg_min[..seg_min.len() - COMMIT_ID_SUFFIX_LEN]
                    } else {
                        seg_min
                    };
                    let seg_max_prefix = if seg_max.len() >= COMMIT_ID_SUFFIX_LEN {
                        &seg_max[..seg_max.len() - COMMIT_ID_SUFFIX_LEN]
                    } else {
                        seg_max
                    };
                    // Overlapping if ranges intersect
                    seg_min_prefix <= l0_max.as_slice() && seg_max_prefix >= l0_min.as_slice()
                })
            } else {
                // No valid L0 key range (all empty segments)
                (Vec::new(), l1_segs.clone())
            };

        // Count actual input segments and entries (L0 + overlapping L1 only).
        let segments_merged = l0_segs.len() + overlapping_l1.len();
        let total_input_entries: u64 = l0_segs
            .iter()
            .chain(overlapping_l1.iter())
            .map(|s| s.entry_count())
            .sum();

        // Build streaming merge sources: L0 + overlapping L1
        let mut all_inputs: Vec<Arc<KVSegment>> = Vec::new();
        all_inputs.extend(l0_segs.iter().cloned());
        all_inputs.extend(overlapping_l1.iter().cloned());

        let limiter = self.compaction_rate_limiter.load_full();
        let (sources, corruption_flags) = streaming_sources(&all_inputs, &limiter);
        let merge = MergeIterator::new(sources);
        let max_versions = self.max_versions_per_key.load(Ordering::Relaxed);
        let snap_floor = self.snapshot_floor.load(Ordering::Relaxed);
        let compaction_iter = CompactionIterator::new(merge, prune_floor)
            .with_max_versions(max_versions)
            .with_snapshot_floor(snap_floor)
            .with_drop_expired(is_bottommost)
            .with_is_bottommost(is_bottommost);

        // Build output segments, splitting at target file size
        let branch_hex = hex_encode_branch(branch_id);
        let branch_dir = segments_dir.join(&branch_hex);
        std::fs::create_dir_all(&branch_dir)?;

        let next_id = &self.next_segment_id;
        let start_id = next_id.load(Ordering::Relaxed);
        let bloom_bits = super::bloom_bits_for_level(1, self.bloom_bits_per_key());
        let compression = super::compression_for_level(1);
        let mut splitting_builder =
            crate::segment_builder::SplittingSegmentBuilder::new(self.target_file_size())
                .with_bloom_bits(bloom_bits)
                .with_compression(compression)
                .with_data_block_size(self.data_block_size());
        if let Some(ref l) = limiter {
            splitting_builder = splitting_builder.with_rate_limiter(Arc::clone(l));
        }
        let outputs = match splitting_builder.build_split(compaction_iter, |_split_idx| {
            let id = next_id.fetch_add(1, Ordering::Relaxed);
            branch_dir.join(format!("{}.sst", id))
        }) {
            Ok(outputs) => outputs,
            Err(e) => {
                let end_id = next_id.load(Ordering::Relaxed);
                cleanup_partial_compaction_outputs(&branch_dir, start_id, end_id);
                return Err(e);
            }
        };
        if let Err(e) = check_corruption_flags(&corruption_flags) {
            for (path, _) in &outputs {
                let _ = std::fs::remove_file(path);
            }
            return Err(e);
        }

        let output_entries: u64 = outputs.iter().map(|(_, m)| m.entry_count).sum();
        let output_file_size: u64 = outputs.iter().map(|(_, m)| m.file_size).sum();

        // Open all output segments
        let mut new_l1_segments: Vec<Arc<KVSegment>> = Vec::new();
        for (path, _meta) in &outputs {
            match KVSegment::open(path) {
                Ok(seg) => new_l1_segments.push(Arc::new(seg)),
                Err(e) => {
                    for (p, _) in &outputs {
                        let _ = std::fs::remove_file(p);
                    }
                    return Err(e);
                }
            }
        }

        // Atomic swap: L0 = only concurrently-flushed segments, L1 = non-overlapping + new
        {
            let mut branch = self
                .branches
                .entry(*branch_id)
                .or_insert_with(BranchState::new);
            let cur_ver = branch.version.load();

            // Keep only L0 segments that were added concurrently (not in our snapshot)
            let new_l0: Vec<Arc<KVSegment>> = cur_ver
                .l0_segments()
                .iter()
                .filter(|s| !l0_segs.iter().any(|old| Arc::ptr_eq(s, old)))
                .cloned()
                .collect();

            // Build new L1: non-overlapping preserved + new output segments, sorted
            let mut new_l1: Vec<Arc<KVSegment>> = non_overlapping_l1;
            new_l1.extend(new_l1_segments);
            // Sort L1 by key_range min ascending
            new_l1.sort_by(|a, b| a.key_range().0.cmp(b.key_range().0));

            let mut new_levels = cur_ver.levels.clone();
            new_levels[0] = new_l0;
            new_levels[1] = new_l1;
            branch
                .version
                .store(Arc::new(SegmentVersion { levels: new_levels }));
            refresh_level_targets(&mut branch, self.level_base_bytes());
        }

        // Persist manifest BEFORE deleting old files (crash safety).
        self.write_branch_manifest(branch_id);

        // Now safe to delete old files (refcount-guarded).
        for seg in &l0_segs {
            self.delete_segment_if_unreferenced(seg);
        }
        for seg in &overlapping_l1 {
            self.delete_segment_if_unreferenced(seg);
        }

        let entries_pruned = total_input_entries.saturating_sub(output_entries);

        Ok(Some(CompactionResult {
            segments_merged,
            output_entries,
            entries_pruned,
            output_file_size,
        }))
    }

    /// Compact one level into the next (L_n → L_{n+1}).
    ///
    /// Picks an input file from `levels[level]` using round-robin via
    /// `compact_pointers`, finds overlapping files in `levels[level+1]`,
    /// merges them via streaming compaction, and writes output files to
    /// `levels[level+1]`.
    ///
    /// Returns `Ok(None)` if there is nothing to compact (ephemeral mode,
    /// branch missing, empty level, or `level >= NUM_LEVELS - 1`).
    ///
    /// For L0, all overlapping L0 files are included as inputs (since L0
    /// files can overlap each other).
    pub fn compact_level(
        &self,
        branch_id: &BranchId,
        level: usize,
        prune_floor: u64,
    ) -> io::Result<Option<CompactionResult>> {
        if level >= NUM_LEVELS - 1 {
            return Ok(None); // can't compact the last level further
        }

        let segments_dir = match &self.segments_dir {
            Some(d) => d,
            None => return Ok(None),
        };

        // ── 1. Snapshot current version and pick input file(s) ──────────
        let (input_segs, overlap_segs, grandparent_segs, _non_overlap_next, is_bottommost) = {
            let branch = match self.branches.get(branch_id) {
                Some(b) => b,
                None => return Ok(None),
            };
            let ver = branch.version.load_full();

            if ver.levels.get(level).map_or(true, |l| l.is_empty()) {
                return Ok(None);
            }

            let level_segs = &ver.levels[level];

            // Pick input file via compact_pointer (round-robin)
            let pick_idx = if level == 0 {
                0 // for L0 we always start with first, then expand
            } else {
                let pointer = branch.compact_pointers.get(level).and_then(|p| p.as_ref());
                match pointer {
                    Some(ptr) => {
                        // Find first file whose max key > pointer
                        level_segs
                            .iter()
                            .position(|seg| {
                                let (_, max_ik) = seg.key_range();
                                let max_prefix = typed_key_prefix_of(max_ik);
                                max_prefix > ptr.as_slice()
                            })
                            .unwrap_or(0) // wrap around
                    }
                    None => 0,
                }
            };

            // Collect input segments
            let inputs = if level == 0 {
                // L0: always compact ALL L0 files (they can overlap arbitrarily)
                level_segs.to_vec()
            } else {
                vec![Arc::clone(&level_segs[pick_idx])]
            };

            // Compute key range of all inputs
            let (input_min, input_max) = compute_key_range(&inputs);

            // Find overlapping files in level+1
            let next_level = ver.levels.get(level + 1).cloned().unwrap_or_default();
            let (overlap, non_overlap) = partition_overlapping(&next_level, &input_min, &input_max);

            // Track grandparent files (level+2) for output splitting
            let grandparents = if level + 2 < ver.levels.len() {
                let gp_level = &ver.levels[level + 2];
                let (gp_overlap, _) = partition_overlapping(gp_level, &input_min, &input_max);
                gp_overlap
            } else {
                Vec::new()
            };

            // Output goes to level+1; bottommost if no segments exist below it.
            let bottom = (level + 2..ver.levels.len()).all(|l| ver.levels[l].is_empty());
            (inputs, overlap, grandparents, non_overlap, bottom)
        };

        // ── Compaction metrics logging ──────────────────────────────────
        let input_bytes: u64 = input_segs.iter().map(|s| s.file_size()).sum();
        let overlap_bytes: u64 = overlap_segs.iter().map(|s| s.file_size()).sum();
        let compact_start = std::time::Instant::now();
        if std::env::var("STRATA_LOG_COMPACT").is_ok() {
            eprintln!(
                "[compact] L{}→L{}: input_files={} overlap_files={} input_bytes={} overlap_bytes={} total_input_entries={}",
                level, level + 1,
                input_segs.len(),
                overlap_segs.len(),
                input_bytes,
                overlap_bytes,
                input_segs.iter().chain(overlap_segs.iter()).map(|s| s.entry_count()).sum::<u64>(),
            );
        }

        // ── 2. Check for trivial move ───────────────────────────────────
        if level > 0
            && input_segs.len() == 1
            && overlap_segs.is_empty()
            && grandparent_bytes_in_range(&grandparent_segs, &input_segs)
                <= self.target_file_size() * 10
        {
            // Metadata-only move: shift file to level+1 without I/O
            let moved_seg = Arc::clone(&input_segs[0]);
            let mut branch = self
                .branches
                .entry(*branch_id)
                .or_insert_with(BranchState::new);
            let cur_ver = branch.version.load();
            let mut new_levels = cur_ver.levels.clone();

            // Remove from current level
            new_levels[level].retain(|s| !Arc::ptr_eq(s, &moved_seg));

            // Add to next level, maintaining sort order
            new_levels[level + 1].push(moved_seg);
            new_levels[level + 1].sort_by(|a, b| a.key_range().0.cmp(b.key_range().0));

            // Update compact pointer
            let (_, input_max) = compute_key_range(&input_segs);
            branch.compact_pointers[level] = Some(input_max);

            branch
                .version
                .store(Arc::new(SegmentVersion { levels: new_levels }));
            refresh_level_targets(&mut branch, self.level_base_bytes());
            drop(branch);
            self.write_branch_manifest(branch_id);

            return Ok(Some(CompactionResult {
                segments_merged: 1,
                output_entries: input_segs[0].entry_count(),
                entries_pruned: 0,
                output_file_size: input_segs[0].file_size(),
            }));
        }

        // ── 3. Merge inputs via streaming compaction ────────────────────
        let segments_merged = input_segs.len() + overlap_segs.len();
        let total_input_entries: u64 = input_segs
            .iter()
            .chain(overlap_segs.iter())
            .map(|s| s.entry_count())
            .sum();

        let mut all_inputs: Vec<Arc<KVSegment>> = Vec::new();
        all_inputs.extend(input_segs.iter().cloned());
        all_inputs.extend(overlap_segs.iter().cloned());

        let limiter = self.compaction_rate_limiter.load_full();
        let (sources, corruption_flags) = streaming_sources(&all_inputs, &limiter);
        let merge = MergeIterator::new(sources);
        let max_versions = self.max_versions_per_key.load(Ordering::Relaxed);
        let snap_floor = self.snapshot_floor.load(Ordering::Relaxed);
        let compaction_iter = CompactionIterator::new(merge, prune_floor)
            .with_max_versions(max_versions)
            .with_snapshot_floor(snap_floor)
            .with_drop_expired(is_bottommost)
            .with_is_bottommost(is_bottommost);

        let branch_hex = hex_encode_branch(branch_id);
        let branch_dir = segments_dir.join(&branch_hex);
        std::fs::create_dir_all(&branch_dir)?;

        let next_id = &self.next_segment_id;
        let start_id = next_id.load(Ordering::Relaxed);
        let bloom_bits = super::bloom_bits_for_level(level + 1, self.bloom_bits_per_key());
        let compression = super::compression_for_level(level + 1);
        let mut splitting_builder =
            crate::segment_builder::SplittingSegmentBuilder::new(self.target_file_size())
                .with_bloom_bits(bloom_bits)
                .with_compression(compression)
                .with_data_block_size(self.data_block_size());
        if let Some(ref l) = limiter {
            splitting_builder = splitting_builder.with_rate_limiter(Arc::clone(l));
        }

        // Build grandparent-aware split predicate.
        //
        // Tracks cumulative overlap: each time the output key advances past a
        // grandparent file's max key, that grandparent's file size is added to
        // `gp_overlap_bytes`.  When the total exceeds the max grandparent overlap
        // threshold (target_file_size × 10), a split is forced and the counter
        // resets.  This limits how many grandparent files a single output segment
        // will overlap during the *next* compaction (L+1 → L+2).
        let max_gp_overlap = self.target_file_size() * 10;
        let gp_segs = grandparent_segs;
        let mut gp_idx: usize = 0;
        let mut gp_overlap_bytes: u64 = 0;
        let should_split = move |typed_key: &[u8]| -> bool {
            // Advance grandparent index past segments whose max < current key,
            // accumulating each crossed grandparent's file size.
            while gp_idx < gp_segs.len() {
                let (_, gp_max) = gp_segs[gp_idx].key_range();
                let gp_max_p = typed_key_prefix_of(gp_max);
                if gp_max_p < typed_key {
                    // Output key has passed this grandparent — count it
                    gp_overlap_bytes += gp_segs[gp_idx].file_size();
                    gp_idx += 1;
                } else {
                    break;
                }
            }
            if gp_overlap_bytes > max_gp_overlap {
                gp_overlap_bytes = 0;
                return true;
            }
            false
        };

        let outputs = match splitting_builder.build_split_with_predicate(
            compaction_iter,
            |_split_idx| {
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                branch_dir.join(format!("{}.sst", id))
            },
            should_split,
        ) {
            Ok(outputs) => outputs,
            Err(e) => {
                let end_id = next_id.load(Ordering::Relaxed);
                cleanup_partial_compaction_outputs(&branch_dir, start_id, end_id);
                return Err(e);
            }
        };
        if let Err(e) = check_corruption_flags(&corruption_flags) {
            for (path, _) in &outputs {
                let _ = std::fs::remove_file(path);
            }
            return Err(e);
        }

        let output_entries: u64 = outputs.iter().map(|(_, m)| m.entry_count).sum();
        let output_file_size: u64 = outputs.iter().map(|(_, m)| m.file_size).sum();

        // Open all output segments
        let mut new_output_segments: Vec<Arc<KVSegment>> = Vec::new();
        for (path, _meta) in &outputs {
            match KVSegment::open(path) {
                Ok(seg) => new_output_segments.push(Arc::new(seg)),
                Err(e) => {
                    for (p, _) in &outputs {
                        let _ = std::fs::remove_file(p);
                    }
                    return Err(e);
                }
            }
        }

        // ── 4. Atomic version swap ─────────────────────────────────────
        {
            let mut branch = self
                .branches
                .entry(*branch_id)
                .or_insert_with(BranchState::new);
            let cur_ver = branch.version.load();
            let mut new_levels = cur_ver.levels.clone();

            // Remove input files from levels[level]
            new_levels[level].retain(|s| !input_segs.iter().any(|old| Arc::ptr_eq(s, old)));

            // Remove overlapping files from levels[level+1]
            new_levels[level + 1].retain(|s| !overlap_segs.iter().any(|old| Arc::ptr_eq(s, old)));

            // Add output files to levels[level+1], sort by key range
            new_levels[level + 1].extend(new_output_segments);
            new_levels[level + 1].sort_by(|a, b| a.key_range().0.cmp(b.key_range().0));

            // Update compact pointer
            let (_, input_max) = compute_key_range(&input_segs);
            branch.compact_pointers[level] = Some(input_max);

            branch
                .version
                .store(Arc::new(SegmentVersion { levels: new_levels }));
            refresh_level_targets(&mut branch, self.level_base_bytes());
        }

        // ── 5. Cleanup ─────────────────────────────────────────────────

        // Persist manifest BEFORE deleting old files (crash safety).
        self.write_branch_manifest(branch_id);

        // Delete old segment files (refcount-guarded).
        for seg in &input_segs {
            self.delete_segment_if_unreferenced(seg);
        }
        for seg in &overlap_segs {
            self.delete_segment_if_unreferenced(seg);
        }

        let entries_pruned = total_input_entries.saturating_sub(output_entries);

        if std::env::var("STRATA_LOG_COMPACT").is_ok() {
            eprintln!(
                "[compact] L{}→L{}: done in {:.1}ms output_files={} output_bytes={} output_entries={} pruned={}",
                level, level + 1,
                compact_start.elapsed().as_secs_f64() * 1000.0,
                outputs.len(),
                output_file_size,
                output_entries,
                entries_pruned,
            );
        }

        Ok(Some(CompactionResult {
            segments_merged,
            output_entries,
            entries_pruned,
            output_file_size,
        }))
    }
}

type StreamingSources = (
    Vec<Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>>,
    Vec<Arc<AtomicBool>>,
);

fn streaming_sources(
    segments: &[Arc<KVSegment>],
    rate_limiter: &Option<Arc<crate::rate_limiter::RateLimiter>>,
) -> StreamingSources {
    let mut corruption_flags = Vec::with_capacity(segments.len());
    let sources = segments
        .iter()
        .map(|seg| {
            let flag = Arc::new(AtomicBool::new(false));
            corruption_flags.push(Arc::clone(&flag));
            let owned = crate::segment::OwnedSegmentIter::new(Arc::clone(seg))
                .with_raw_values()
                .with_corruption_flag(flag);
            if let Some(ref limiter) = rate_limiter {
                let throttled =
                    crate::segment::ThrottledSegmentIter::new(owned, Arc::clone(limiter));
                Box::new(throttled.map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se))))
                    as Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>
            } else {
                Box::new(owned.map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se))))
                    as Box<dyn Iterator<Item = (InternalKey, MemtableEntry)>>
            }
        })
        .collect();
    (sources, corruption_flags)
}

/// Remove partial compaction output files left by a failed build.
///
/// Cleans up both `.sst` (fully written then renamed) and `.tmp` (partially
/// written) files for segment IDs in `[start_id, end_id)`. Best-effort:
/// errors are ignored because we're already on an error path.
fn cleanup_partial_compaction_outputs(dir: &std::path::Path, start_id: u64, end_id: u64) {
    for id in start_id..end_id {
        let _ = std::fs::remove_file(dir.join(format!("{}.sst", id)));
        let _ = std::fs::remove_file(dir.join(format!("{}.tmp", id)));
    }
}

/// Check if any corruption flag was set during iteration.
/// If so, return an error to abort the compaction (#1677).
fn check_corruption_flags(flags: &[Arc<AtomicBool>]) -> io::Result<()> {
    if flags.iter().any(|f| f.load(Ordering::Relaxed)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "compaction aborted: source segment has corrupt data block (#1677)",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Multi-level compaction helpers
// ---------------------------------------------------------------------------

/// Extract the typed_key_prefix from an InternalKey byte slice.
///
/// InternalKey format: `typed_key_prefix || commit_id (8 bytes)`.
/// Returns the full slice if shorter than 8 bytes.
#[inline]
fn typed_key_prefix_of(ik_bytes: &[u8]) -> &[u8] {
    if ik_bytes.len() >= COMMIT_ID_SUFFIX_LEN {
        &ik_bytes[..ik_bytes.len() - COMMIT_ID_SUFFIX_LEN]
    } else {
        ik_bytes
    }
}

/// Compute the overall typed_key_prefix range across a set of segments.
///
/// Returns `(min_prefix, max_prefix)` as owned byte vectors.
fn compute_key_range(segments: &[Arc<KVSegment>]) -> (Vec<u8>, Vec<u8>) {
    let mut overall_min: Option<Vec<u8>> = None;
    let mut overall_max: Option<Vec<u8>> = None;
    for seg in segments {
        let (seg_min, seg_max) = seg.key_range();
        if seg_min.is_empty() && seg_max.is_empty() {
            continue;
        }
        let min_p = typed_key_prefix_of(seg_min).to_vec();
        let max_p = typed_key_prefix_of(seg_max).to_vec();
        overall_min = Some(match overall_min {
            None => min_p,
            Some(cur) => {
                if min_p < cur {
                    min_p
                } else {
                    cur
                }
            }
        });
        overall_max = Some(match overall_max {
            None => max_p,
            Some(cur) => {
                if max_p > cur {
                    max_p
                } else {
                    cur
                }
            }
        });
    }
    (
        overall_min.unwrap_or_default(),
        overall_max.unwrap_or_default(),
    )
}

/// Partition a sorted, non-overlapping level into segments that overlap
/// `[range_min, range_max]` and those that don't.
fn partition_overlapping(
    level_segs: &[Arc<KVSegment>],
    range_min: &[u8],
    range_max: &[u8],
) -> (Vec<Arc<KVSegment>>, Vec<Arc<KVSegment>>) {
    let mut overlapping = Vec::new();
    let mut non_overlapping = Vec::new();
    for seg in level_segs {
        let (seg_min, seg_max) = seg.key_range();
        if seg_min.is_empty() && seg_max.is_empty() {
            non_overlapping.push(Arc::clone(seg));
            continue;
        }
        let seg_min_p = typed_key_prefix_of(seg_min);
        let seg_max_p = typed_key_prefix_of(seg_max);
        if seg_min_p <= range_max && seg_max_p >= range_min {
            overlapping.push(Arc::clone(seg));
        } else {
            non_overlapping.push(Arc::clone(seg));
        }
    }
    (overlapping, non_overlapping)
}

/// Compute total file size of grandparent segments overlapping with a set of
/// input segments.
fn grandparent_bytes_in_range(grandparents: &[Arc<KVSegment>], inputs: &[Arc<KVSegment>]) -> u64 {
    if grandparents.is_empty() || inputs.is_empty() {
        return 0;
    }
    let (input_min, input_max) = compute_key_range(inputs);
    let (overlap, _) = partition_overlapping(grandparents, &input_min, &input_max);
    overlap.iter().map(|s| s.file_size()).sum()
}
