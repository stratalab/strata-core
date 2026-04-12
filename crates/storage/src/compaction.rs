//! Compaction iterator — version pruning for segment compaction.
//!
//! `CompactionIterator` wraps a sorted `(InternalKey, MemtableEntry)` stream
//! and prunes old versions below a `prune_floor` commit_id. Unlike
//! `MvccIterator` (which keeps only the newest version per key), compaction
//! keeps **all** versions with `commit_id >= prune_floor`, plus **one**
//! version below the floor per logical key (unless it's a tombstone —
//! dead keys are fully cleaned up).
//!
//! When `prune_floor == 0`, all versions pass through unchanged.

use crate::key_encoding::InternalKey;
use crate::memtable::MemtableEntry;
use strata_core::types::TypeTag;

/// Pruning iterator for segment compaction.
///
/// Consumes a sorted `(InternalKey, MemtableEntry)` stream (e.g., from
/// `MergeIterator`) and applies version pruning based on `prune_floor`.
///
/// For each logical key (identified by `typed_key_prefix`):
/// - **Above floor** (`commit_id >= prune_floor`): always emit
/// - **Below floor** (`commit_id < prune_floor`):
///   - Emit the newest version (first encountered), unless it's a tombstone
///   - Skip all remaining older versions
pub struct CompactionIterator<I: Iterator<Item = (InternalKey, MemtableEntry)>> {
    inner: I,
    prune_floor: u64,
    current_prefix: Option<Vec<u8>>,
    emitted_floor_entry: bool,
    /// Maximum versions to keep per logical key (0 = unlimited).
    max_versions: usize,
    /// Number of versions emitted for the current logical key.
    versions_emitted: usize,
    /// Whether to drop expired TTL entries (#1622).
    ///
    /// Should only be true for bottommost-level compactions, since
    /// non-bottommost compactions must preserve expired entries that
    /// shadow older versions in lower levels.
    drop_expired: bool,
    /// Whether this compaction is bottommost (no lower levels exist).
    ///
    /// When true, below-floor tombstones can be safely elided since
    /// there are no lower levels with older puts to shadow.
    /// When false, below-floor tombstones must be preserved (#1678).
    is_bottommost: bool,
    /// Snapshot-safe floor (#1697): versions with `commit_id >= snapshot_floor`
    /// are protected from `max_versions` pruning because an active snapshot
    /// might need them. When 0, no snapshot protection is applied.
    snapshot_floor: u64,
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> CompactionIterator<I> {
    /// Create a new compaction iterator with the given prune floor.
    ///
    /// Versions with `commit_id < prune_floor` are pruned (at most one
    /// surviving per logical key). When `prune_floor == 0`, nothing is pruned.
    pub fn new(inner: I, prune_floor: u64) -> Self {
        Self {
            inner,
            prune_floor,
            current_prefix: None,
            emitted_floor_entry: false,
            max_versions: 0,
            versions_emitted: 0,
            drop_expired: false,
            is_bottommost: true,
            snapshot_floor: 0,
        }
    }

    /// Set the maximum number of versions to keep per logical key.
    ///
    /// When `max > 0`, at most `max` versions are emitted per key
    /// (counting both above-floor and floor entries). When `max == 0`,
    /// version limiting is disabled and only `prune_floor` governs pruning.
    pub fn with_max_versions(mut self, max: usize) -> Self {
        self.max_versions = max;
        self
    }

    /// Enable dropping of expired TTL entries (#1622).
    ///
    /// When enabled, entries whose TTL has elapsed are silently dropped
    /// during compaction instead of being copied to the output segment.
    /// This should only be used for bottommost-level compactions.
    pub fn with_drop_expired(mut self, drop: bool) -> Self {
        self.drop_expired = drop;
        self
    }

    /// Set whether this is a bottommost-level compaction (#1678).
    ///
    /// When false, below-floor tombstones are preserved to shadow
    /// older puts in lower levels. Defaults to true (safe for callers
    /// that previously relied on unconditional tombstone elision).
    pub fn with_is_bottommost(mut self, is_bottommost: bool) -> Self {
        self.is_bottommost = is_bottommost;
        self
    }

    /// Set the snapshot-safe floor (#1697).
    ///
    /// When `snapshot_floor > 0`, versions with `commit_id >= snapshot_floor`
    /// are protected from `max_versions` pruning because an active snapshot
    /// may need them. When 0 (default), no snapshot protection is applied.
    pub fn with_snapshot_floor(mut self, floor: u64) -> Self {
        self.snapshot_floor = floor;
        self
    }
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> Iterator for CompactionIterator<I> {
    type Item = (InternalKey, MemtableEntry);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (ik, entry) = self.inner.next()?;

            // Event entries are exempt from all pruning (#1729).
            // EventLog relies on a SHA-256 hash chain for tamper evidence;
            // dropping any event breaks chain verification.
            if ik.type_tag_byte() == TypeTag::Event.as_byte() {
                return Some((ik, entry));
            }

            let prefix = ik.typed_key_prefix().to_vec();

            // New logical key — reset tracking
            if self.current_prefix.as_ref() != Some(&prefix) {
                self.current_prefix = Some(prefix);
                self.emitted_floor_entry = false;
                self.versions_emitted = 0;
            }

            let commit_id = ik.commit_id();

            // Drop expired TTL entries in bottommost compactions (#1622).
            // Non-bottommost compactions must keep them because they may
            // shadow older versions in lower levels.
            // #1727: Only drop if below prune_floor — entries above the floor
            // may be visible to active snapshots and must not be removed.
            if self.drop_expired && entry.is_expired() && commit_id < self.prune_floor {
                continue;
            }

            if commit_id >= self.prune_floor {
                // #1723: In non-bottommost compaction, above-floor tombstones
                // must always be emitted to shadow lower-level entries.
                // They do not count toward max_versions so they don't displace
                // value versions that active snapshots may need.
                if !self.is_bottommost && entry.is_tombstone {
                    return Some((ik, entry));
                }

                // Above floor: check max_versions limit.
                // #1697: versions at or above snapshot_floor are protected from
                // max_versions pruning because an active snapshot may need them.
                if self.max_versions > 0
                    && self.versions_emitted >= self.max_versions
                    && (self.snapshot_floor == 0 || commit_id < self.snapshot_floor)
                {
                    continue; // Safe to skip — no snapshot needs this version
                }
                self.versions_emitted += 1;
                return Some((ik, entry));
            }

            // Below floor
            if !self.emitted_floor_entry {
                self.emitted_floor_entry = true;
                if entry.is_tombstone {
                    if self.is_bottommost {
                        // Bottommost: safe to elide — no lower levels to shadow
                        continue;
                    }
                    // Non-bottommost: keep tombstone to shadow lower-level puts (#1678)
                    self.versions_emitted += 1;
                    return Some((ik, entry));
                }
                // Keep one floor entry, but respect max_versions (with snapshot safety #1697)
                if self.max_versions > 0
                    && self.versions_emitted >= self.max_versions
                    && (self.snapshot_floor == 0 || commit_id < self.snapshot_floor)
                {
                    continue;
                }
                self.versions_emitted += 1;
                return Some((ik, entry));
            }

            // Already emitted a floor entry for this key — prune
        }
    }
}

// ---------------------------------------------------------------------------
// Size-tiered compaction scheduler
// ---------------------------------------------------------------------------

/// Size-tiered compaction scheduler.
///
/// Groups segments into tiers by file size and identifies merge candidates.
/// Tier assignment: `tier = floor(log4(file_size / base_size))`.
pub struct CompactionScheduler {
    /// Minimum segments in a tier to trigger a merge.
    pub min_merge_width: usize,
    /// Base segment size in bytes for tier calculation.
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

/// A group of segments in the same size tier, eligible for merging.
#[derive(Debug, Clone)]
pub struct TierMergeCandidate {
    /// Tier number (0 = smallest segments).
    pub tier: u32,
    /// Indices into the branch's segment list for segments to merge.
    pub segment_indices: Vec<usize>,
}

impl CompactionScheduler {
    /// Assign a tier to a segment based on its file size.
    fn tier_for_size(&self, file_size: u64) -> u32 {
        if file_size == 0 || self.base_size == 0 {
            return 0;
        }
        let ratio = file_size as f64 / self.base_size as f64;
        if ratio <= 1.0 {
            0
        } else {
            ratio.log(4.0).floor() as u32
        }
    }

    /// Find merge candidates: tiers with segment count >= min_merge_width.
    ///
    /// Returns candidates sorted by tier ascending (merge smallest first).
    /// Each candidate contains the indices of segments to merge.
    pub fn pick_candidates(&self, segment_sizes: &[u64]) -> Vec<TierMergeCandidate> {
        use std::collections::BTreeMap;

        let mut tiers: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
        for (i, &size) in segment_sizes.iter().enumerate() {
            let tier = self.tier_for_size(size);
            tiers.entry(tier).or_default().push(i);
        }

        tiers
            .into_iter()
            .filter(|(_, indices)| indices.len() >= self.min_merge_width)
            .map(|(tier, segment_indices)| TierMergeCandidate {
                tier,
                segment_indices,
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_iter::MergeIterator;
    use std::sync::Arc;
    use strata_core::id::CommitVersion;
    use strata_core::types::{BranchId, Key, Namespace, TypeTag};
    use strata_core::value::Value;
    use strata_core::Timestamp;

    fn branch() -> BranchId {
        BranchId::from_bytes([1; 16])
    }

    fn key(user_key: &str) -> Key {
        let ns = Arc::new(Namespace::new(branch(), "default".to_string()));
        Key::new(ns, TypeTag::KV, user_key.as_bytes().to_vec())
    }

    fn entry(value: i64) -> MemtableEntry {
        MemtableEntry {
            value: Value::Int(value),
            is_tombstone: false,
            timestamp: Timestamp::now(),
            ttl_ms: 0,
            raw_value: None,
        }
    }

    fn tombstone() -> MemtableEntry {
        MemtableEntry {
            value: Value::Null,
            is_tombstone: true,
            timestamp: Timestamp::now(),
            ttl_ms: 0,
            raw_value: None,
        }
    }

    /// Helper: run compaction on a single sorted source.
    fn compact(
        items: Vec<(InternalKey, MemtableEntry)>,
        prune_floor: u64,
    ) -> Vec<(InternalKey, MemtableEntry)> {
        let merge = MergeIterator::new(vec![items.into_iter()]);
        CompactionIterator::new(merge, prune_floor).collect()
    }

    #[test]
    fn compaction_keeps_all_above_floor() {
        // versions 5, 3, 1 with floor=2 → keeps 5, 3 (above floor) + 1 (floor entry)
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(5)), entry(50)),
            (InternalKey::encode(&key("k"), CommitVersion(3)), entry(30)),
            (InternalKey::encode(&key("k"), CommitVersion(1)), entry(10)),
        ];
        let result = compact(items, 2);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0.commit_id(), 5);
        assert_eq!(result[1].0.commit_id(), 3);
        assert_eq!(result[2].0.commit_id(), 1);
    }

    #[test]
    fn compaction_keeps_one_below_floor() {
        // versions 5, 3, 1 with floor=4 → keeps 5 (above), 3 (newest below floor)
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(5)), entry(50)),
            (InternalKey::encode(&key("k"), CommitVersion(3)), entry(30)),
            (InternalKey::encode(&key("k"), CommitVersion(1)), entry(10)),
        ];
        let result = compact(items, 4);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0.commit_id(), 5);
        assert_eq!(result[1].0.commit_id(), 3);
    }

    #[test]
    fn compaction_prune_floor_zero_keeps_all() {
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(5)), entry(50)),
            (InternalKey::encode(&key("k"), CommitVersion(3)), entry(30)),
            (InternalKey::encode(&key("k"), CommitVersion(1)), entry(10)),
        ];
        let result = compact(items, 0);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn compaction_drops_dead_tombstone() {
        // tombstone at 3, value at 1, floor=5 → entire key dropped
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(3)),
                tombstone(),
            ),
            (InternalKey::encode(&key("k"), CommitVersion(1)), entry(10)),
        ];
        let result = compact(items, 5);
        assert!(result.is_empty());
    }

    #[test]
    fn compaction_keeps_tombstone_above_floor() {
        // tombstone at 3, value at 1, floor=2
        // → tombstone at 3 kept (above floor), value at 1 kept (newest below floor)
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(3)),
                tombstone(),
            ),
            (InternalKey::encode(&key("k"), CommitVersion(1)), entry(10)),
        ];
        let result = compact(items, 2);
        assert_eq!(result.len(), 2);
        assert!(result[0].1.is_tombstone);
        assert_eq!(result[0].0.commit_id(), 3);
        assert_eq!(result[1].0.commit_id(), 1);
    }

    #[test]
    fn compaction_multiple_keys() {
        // Two keys pruned independently
        let items = vec![
            (InternalKey::encode(&key("a"), CommitVersion(5)), entry(50)),
            (InternalKey::encode(&key("a"), CommitVersion(3)), entry(30)),
            (InternalKey::encode(&key("a"), CommitVersion(1)), entry(10)),
            (InternalKey::encode(&key("b"), CommitVersion(4)), entry(40)),
            (InternalKey::encode(&key("b"), CommitVersion(2)), entry(20)),
        ];
        let result = compact(items, 4);
        // key "a": 5 (above), 3 (newest below) → 2 entries
        // key "b": 4 (above), 2 (newest below) → 2 entries
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0.commit_id(), 5); // a@5
        assert_eq!(result[1].0.commit_id(), 3); // a@3
        assert_eq!(result[2].0.commit_id(), 4); // b@4
        assert_eq!(result[3].0.commit_id(), 2); // b@2
    }

    #[test]
    fn compaction_empty_input() {
        let result = compact(vec![], 10);
        assert!(result.is_empty());
    }

    #[test]
    fn compaction_only_below_floor_keeps_one() {
        // Key with only below-floor versions — newest survives as floor entry
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(3)), entry(30)),
            (InternalKey::encode(&key("k"), CommitVersion(1)), entry(10)),
        ];
        let result = compact(items, 10);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.commit_id(), 3);
        assert_eq!(result[0].1.value, Value::Int(30));
    }

    #[test]
    fn compaction_only_below_floor_tombstone_drops_all() {
        // Key with only a tombstone below floor — fully cleaned up
        let items = vec![(
            InternalKey::encode(&key("k"), CommitVersion(3)),
            tombstone(),
        )];
        let result = compact(items, 10);
        assert!(result.is_empty());
    }

    // -----------------------------------------------------------------------
    // max_versions tests
    // -----------------------------------------------------------------------

    #[test]
    fn compaction_max_versions_limits_above_floor() {
        // versions 10, 8, 6, 4, 2 with floor=0, max_versions=3
        // → keeps only 10, 8, 6 (first 3 above floor)
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(10)),
                entry(100),
            ),
            (InternalKey::encode(&key("k"), CommitVersion(8)), entry(80)),
            (InternalKey::encode(&key("k"), CommitVersion(6)), entry(60)),
            (InternalKey::encode(&key("k"), CommitVersion(4)), entry(40)),
            (InternalKey::encode(&key("k"), CommitVersion(2)), entry(20)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 0)
            .with_max_versions(3)
            .collect();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0.commit_id(), 10);
        assert_eq!(result[1].0.commit_id(), 8);
        assert_eq!(result[2].0.commit_id(), 6);
    }

    #[test]
    fn compaction_max_versions_zero_keeps_all() {
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(5)), entry(50)),
            (InternalKey::encode(&key("k"), CommitVersion(3)), entry(30)),
            (InternalKey::encode(&key("k"), CommitVersion(1)), entry(10)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 0)
            .with_max_versions(0)
            .collect();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn compaction_max_versions_with_floor() {
        // versions 10, 8, 6, 4, 2 with floor=5, max_versions=2
        // Above floor: 10, 8 (limited to 2)
        // Floor entry: 4 would be floor entry, but max_versions already reached → skip
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(10)),
                entry(100),
            ),
            (InternalKey::encode(&key("k"), CommitVersion(8)), entry(80)),
            (InternalKey::encode(&key("k"), CommitVersion(6)), entry(60)),
            (InternalKey::encode(&key("k"), CommitVersion(4)), entry(40)),
            (InternalKey::encode(&key("k"), CommitVersion(2)), entry(20)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 5)
            .with_max_versions(2)
            .collect();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0.commit_id(), 10);
        assert_eq!(result[1].0.commit_id(), 8);
    }

    #[test]
    fn compaction_max_versions_per_key_independent() {
        // Two keys with max_versions=2
        let items = vec![
            (InternalKey::encode(&key("a"), CommitVersion(5)), entry(50)),
            (InternalKey::encode(&key("a"), CommitVersion(3)), entry(30)),
            (InternalKey::encode(&key("a"), CommitVersion(1)), entry(10)),
            (InternalKey::encode(&key("b"), CommitVersion(4)), entry(40)),
            (InternalKey::encode(&key("b"), CommitVersion(2)), entry(20)),
            (InternalKey::encode(&key("b"), CommitVersion(1)), entry(10)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 0)
            .with_max_versions(2)
            .collect();
        // key "a": 5, 3 (2 versions)
        // key "b": 4, 2 (2 versions)
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].0.commit_id(), 5); // a@5
        assert_eq!(result[1].0.commit_id(), 3); // a@3
        assert_eq!(result[2].0.commit_id(), 4); // b@4
        assert_eq!(result[3].0.commit_id(), 2); // b@2
    }

    #[test]
    fn compaction_max_versions_one_keeps_only_newest() {
        // max_versions=1 should keep exactly one version per key
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(5)), entry(50)),
            (InternalKey::encode(&key("k"), CommitVersion(3)), entry(30)),
            (InternalKey::encode(&key("k"), CommitVersion(1)), entry(10)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 0)
            .with_max_versions(1)
            .collect();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.commit_id(), 5);
    }

    #[test]
    fn compaction_max_versions_tombstone_above_floor_counts() {
        // Tombstone above floor should count toward max_versions
        // versions: tomb@10, value@8, value@6 with floor=3, max_versions=2
        // → keeps tomb@10 (above, count=1), value@8 (above, count=2), skips rest
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(10)),
                tombstone(),
            ),
            (InternalKey::encode(&key("k"), CommitVersion(8)), entry(80)),
            (InternalKey::encode(&key("k"), CommitVersion(6)), entry(60)),
            (InternalKey::encode(&key("k"), CommitVersion(2)), entry(20)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 3)
            .with_max_versions(2)
            .collect();
        assert_eq!(result.len(), 2);
        assert!(result[0].1.is_tombstone);
        assert_eq!(result[0].0.commit_id(), 10);
        assert_eq!(result[1].0.commit_id(), 8);
    }

    #[test]
    fn compaction_max_versions_dead_tombstone_still_cleaned() {
        // Dead tombstone cleanup should work regardless of max_versions
        // All versions below floor, newest is tombstone → entire key dropped
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(3)),
                tombstone(),
            ),
            (InternalKey::encode(&key("k"), CommitVersion(1)), entry(10)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 5)
            .with_max_versions(10)
            .collect();
        assert!(result.is_empty());
    }

    // -----------------------------------------------------------------------
    // Issue #1678: Non-bottommost compaction must preserve below-floor tombstones
    // -----------------------------------------------------------------------

    #[test]
    fn test_issue_1678_non_bottommost_preserves_below_floor_tombstone() {
        // Scenario from the issue:
        // - k=v1 exists in a lower level (L2) at commit_id=1
        // - k=tombstone in L1 at commit_id=3, with prune_floor=5
        //   (so the tombstone is below floor)
        // - Compact L1 (non-bottommost: lower levels exist)
        //
        // Bug: CompactionIterator unconditionally drops below-floor tombstones,
        //      removing the shadow that hides v1 in L2 → data resurrection.
        //
        // The tombstone MUST be preserved when is_bottommost=false.
        let items = vec![(
            InternalKey::encode(&key("k"), CommitVersion(3)),
            tombstone(),
        )];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 5)
            .with_is_bottommost(false)
            .collect();
        // Non-bottommost: tombstone must survive to shadow lower-level puts
        assert_eq!(
            result.len(),
            1,
            "below-floor tombstone must be kept in non-bottommost compaction"
        );
        assert!(result[0].1.is_tombstone);
        assert_eq!(result[0].0.commit_id(), 3);
    }

    #[test]
    fn test_issue_1678_bottommost_drops_below_floor_tombstone() {
        // Same scenario but bottommost — safe to drop the tombstone since
        // no lower levels exist.
        let items = vec![(
            InternalKey::encode(&key("k"), CommitVersion(3)),
            tombstone(),
        )];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 5)
            .with_is_bottommost(true)
            .collect();
        // Bottommost: tombstone can be safely elided
        assert!(
            result.is_empty(),
            "below-floor tombstone should be dropped in bottommost compaction"
        );
    }

    #[test]
    fn test_issue_1678_non_bottommost_tombstone_with_older_versions() {
        // tombstone at 3, value at 1, floor=5, non-bottommost
        // Both below floor: tombstone is floor entry but must be kept.
        // The value at 1 should be pruned (tombstone is the floor entry).
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(3)),
                tombstone(),
            ),
            (InternalKey::encode(&key("k"), CommitVersion(1)), entry(10)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 5)
            .with_is_bottommost(false)
            .collect();
        // Tombstone kept as floor entry, older value pruned
        assert_eq!(result.len(), 1);
        assert!(result[0].1.is_tombstone);
        assert_eq!(result[0].0.commit_id(), 3);
    }

    #[test]
    fn test_issue_1678_non_bottommost_tombstone_preserved_despite_max_versions() {
        // Non-bottommost below-floor tombstone MUST be kept even when
        // max_versions is already exhausted — dropping it resurrects data.
        // versions: value@10, value@8 (above floor), tombstone@2 (below floor)
        // floor=5, max_versions=2, non-bottommost
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(10)),
                entry(100),
            ),
            (InternalKey::encode(&key("k"), CommitVersion(8)), entry(80)),
            (
                InternalKey::encode(&key("k"), CommitVersion(2)),
                tombstone(),
            ),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 5)
            .with_max_versions(2)
            .with_is_bottommost(false)
            .collect();
        // Must keep all 3: two above-floor values + the tombstone that shadows lower levels
        assert_eq!(
            result.len(),
            3,
            "tombstone must survive even when max_versions exhausted"
        );
        assert_eq!(result[0].0.commit_id(), 10);
        assert_eq!(result[1].0.commit_id(), 8);
        assert!(result[2].1.is_tombstone);
        assert_eq!(result[2].0.commit_id(), 2);
    }

    // -----------------------------------------------------------------------
    // above-floor tombstone vs max_versions tests (#1723)
    // -----------------------------------------------------------------------

    #[test]
    fn test_issue_1723_above_floor_tombstone_not_dropped_by_max_versions() {
        // Non-bottommost compaction, max_versions=1.
        // Key K: value@v7 (above floor), tombstone@v3 (above floor).
        // Lower level L3 has value@v1 for key K.
        //
        // Bug: max_versions=1 emits value@v7 (count=1), then SKIPS
        // tombstone@v3 because count >= max. After compaction, tombstone
        // is gone and value@v1 in L3 resurrects.
        //
        // Fix: above-floor tombstones in non-bottommost compaction must
        // always be emitted regardless of max_versions.
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(7)), entry(70)),
            (
                InternalKey::encode(&key("k"), CommitVersion(3)),
                tombstone(),
            ),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 2) // floor=2, both above
            .with_max_versions(1)
            .with_is_bottommost(false)
            .collect();
        assert_eq!(
            result.len(),
            2,
            "above-floor tombstone must survive max_versions cap in non-bottommost compaction"
        );
        assert_eq!(result[0].0.commit_id(), 7);
        assert!(result[1].1.is_tombstone);
        assert_eq!(result[1].0.commit_id(), 3);
    }

    #[test]
    fn test_issue_1723_tombstone_does_not_displace_values() {
        // Non-bottommost, max_versions=2.
        // Key K: value@v10, tombstone@v7, value@v5, all above floor=2.
        // The tombstone must be emitted AND must not count against
        // max_versions — otherwise value@v5 gets displaced.
        // A reader at max_version=6 should still see value@v5.
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(10)),
                entry(100),
            ),
            (
                InternalKey::encode(&key("k"), CommitVersion(7)),
                tombstone(),
            ),
            (InternalKey::encode(&key("k"), CommitVersion(5)), entry(50)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 2)
            .with_max_versions(2)
            .with_is_bottommost(false)
            .collect();
        assert_eq!(
            result.len(),
            3,
            "tombstone must not count against max_versions, displacing value@v5"
        );
        assert_eq!(result[0].0.commit_id(), 10);
        assert!(result[1].1.is_tombstone);
        assert_eq!(result[1].0.commit_id(), 7);
        assert_eq!(result[2].0.commit_id(), 5);
    }

    #[test]
    fn test_issue_1723_bottommost_tombstone_still_respects_max_versions() {
        // In bottommost compaction, above-floor tombstones CAN be dropped
        // by max_versions — there are no lower levels to shadow.
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(7)), entry(70)),
            (
                InternalKey::encode(&key("k"), CommitVersion(3)),
                tombstone(),
            ),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 2)
            .with_max_versions(1)
            .with_is_bottommost(true)
            .collect();
        assert_eq!(
            result.len(),
            1,
            "bottommost compaction can drop above-floor tombstone via max_versions"
        );
        assert_eq!(result[0].0.commit_id(), 7);
    }

    // -----------------------------------------------------------------------
    // snapshot_floor tests (#1697)
    // -----------------------------------------------------------------------

    #[test]
    fn test_issue_1697_max_versions_respects_snapshot_floor() {
        // max_versions=1, versions 6 and 5.
        // snapshot_floor=5 (gc_safe_point returned min_active_version=5).
        // Version 5 has commit_id >= snapshot_floor → protected from pruning.
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(6)), entry(60)),
            (InternalKey::encode(&key("k"), CommitVersion(5)), entry(50)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 0)
            .with_max_versions(1)
            .with_snapshot_floor(5)
            .collect();
        assert_eq!(
            result.len(),
            2,
            "version 5 must survive: active snapshot at version 5 needs it"
        );
        assert_eq!(result[0].0.commit_id(), 6);
        assert_eq!(result[1].0.commit_id(), 5);

        // snapshot_floor=6: version 5 (commit_id 5 < 6) is NOT protected.
        // max_versions=1 drops it normally.
        let items2 = vec![
            (InternalKey::encode(&key("k"), CommitVersion(6)), entry(60)),
            (InternalKey::encode(&key("k"), CommitVersion(5)), entry(50)),
        ];
        let merge2 = MergeIterator::new(vec![items2.into_iter()]);
        let result2: Vec<_> = CompactionIterator::new(merge2, 0)
            .with_max_versions(1)
            .with_snapshot_floor(6)
            .collect();
        assert_eq!(
            result2.len(),
            1,
            "version 5 is below snapshot_floor=6, not protected"
        );
        assert_eq!(result2[0].0.commit_id(), 6);
    }

    #[test]
    fn test_issue_1697_max_versions_drops_below_snapshot_floor() {
        // Versions 10, 8, 5, 3 with max_versions=2, snapshot_floor=6
        // Versions >= 6 are protected: 10, 8 (both above snapshot_floor)
        // Versions < 6: 5, 3 can be dropped by max_versions since they're
        // below the snapshot floor (no active snapshot needs them).
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(10)),
                entry(100),
            ),
            (InternalKey::encode(&key("k"), CommitVersion(8)), entry(80)),
            (InternalKey::encode(&key("k"), CommitVersion(5)), entry(50)),
            (InternalKey::encode(&key("k"), CommitVersion(3)), entry(30)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 0)
            .with_max_versions(2)
            .with_snapshot_floor(6)
            .collect();
        // max_versions=2, and we already emitted 2 (versions 10, 8).
        // Versions 5 and 3 are below snapshot_floor, so max_versions can drop them.
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0.commit_id(), 10);
        assert_eq!(result[1].0.commit_id(), 8);
    }

    #[test]
    fn test_issue_1697_snapshot_floor_zero_no_protection() {
        // snapshot_floor=0 means no active snapshots — max_versions applies normally.
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(6)), entry(60)),
            (InternalKey::encode(&key("k"), CommitVersion(5)), entry(50)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 0)
            .with_max_versions(1)
            .with_snapshot_floor(0)
            .collect();
        assert_eq!(
            result.len(),
            1,
            "no snapshot protection, max_versions=1 keeps only newest"
        );
        assert_eq!(result[0].0.commit_id(), 6);
    }

    #[test]
    fn test_issue_1697_snapshot_floor_with_prune_floor() {
        // prune_floor=4, max_versions=1, snapshot_floor=3.
        // Versions: 6 (above floor), 3 (below floor = floor entry).
        // Without snapshot_floor, max_versions=1 keeps version 6, drops floor entry 3.
        // With snapshot_floor=3, floor entry 3 has commit_id >= snapshot_floor → protected.
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(6)), entry(60)),
            (InternalKey::encode(&key("k"), CommitVersion(3)), entry(30)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 4)
            .with_max_versions(1)
            .with_snapshot_floor(3)
            .collect();
        assert_eq!(result.len(), 2, "floor entry protected by snapshot_floor");
        assert_eq!(result[0].0.commit_id(), 6);
        assert_eq!(result[1].0.commit_id(), 3);
    }

    #[test]
    fn test_issue_1697_snapshot_floor_protects_tombstone() {
        // max_versions=1, snapshot_floor=5. Tombstone at version 5 is the second
        // version for this key. Without protection, max_versions drops it.
        // With snapshot_floor=5, commit_id 5 >= 5 → protected.
        let items = vec![
            (InternalKey::encode(&key("k"), CommitVersion(8)), entry(80)),
            (
                InternalKey::encode(&key("k"), CommitVersion(5)),
                tombstone(),
            ),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 0)
            .with_max_versions(1)
            .with_snapshot_floor(5)
            .collect();
        assert_eq!(result.len(), 2, "tombstone at snapshot_floor must survive");
        assert_eq!(result[0].0.commit_id(), 8);
        assert!(result[1].1.is_tombstone);
        assert_eq!(result[1].0.commit_id(), 5);
    }

    // -----------------------------------------------------------------------
    // Issue #1727: TTL expiration must respect prune_floor (snapshot safety)
    // -----------------------------------------------------------------------

    /// Create an entry that is already expired (far-past timestamp + short TTL).
    fn expired_entry(value: i64) -> MemtableEntry {
        MemtableEntry {
            value: Value::Int(value),
            is_tombstone: false,
            // 1 second after epoch with 1ms TTL → always expired
            timestamp: Timestamp::from_micros(1_000_000),
            ttl_ms: 1,
            raw_value: None,
        }
    }

    #[test]
    fn test_issue_1727_drop_expired_respects_prune_floor() {
        // Scenario from the issue:
        // - Entry K@v50 has expired TTL, but commit_id 50 >= prune_floor 10
        // - An active transaction at snapshot version 50 should still see it
        // - Compaction with drop_expired must NOT drop it
        let items = vec![(
            InternalKey::encode(&key("k"), CommitVersion(50)),
            expired_entry(500),
        )];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 10)
            .with_drop_expired(true)
            .collect();
        // Entry is above prune_floor — must survive even though expired
        assert_eq!(
            result.len(),
            1,
            "expired entry above prune_floor must NOT be dropped (snapshot may need it)"
        );
        assert_eq!(result[0].0.commit_id(), 50);
    }

    #[test]
    fn test_issue_1727_drop_expired_below_prune_floor() {
        // Entry K@v5 is expired AND below prune_floor 10 → safe to drop
        let items = vec![(
            InternalKey::encode(&key("k"), CommitVersion(5)),
            expired_entry(50),
        )];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 10)
            .with_drop_expired(true)
            .collect();
        // Entry is below prune_floor and expired — should be dropped
        assert!(
            result.is_empty(),
            "expired entry below prune_floor should be dropped"
        );
    }

    #[test]
    fn test_issue_1727_mixed_expired_and_live_versions() {
        // Key "k" has versions: 50 (expired, above floor), 8 (expired, below floor), 3 (live, below floor)
        // prune_floor=10
        // Expected: keep v50 (above floor, even though expired),
        //           drop v8 (expired + below floor),
        //           v3 would be floor entry but v8 was the first below-floor seen
        //           (v8 is dropped by expired, so v3 becomes floor entry → kept)
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(50)),
                expired_entry(500),
            ),
            (
                InternalKey::encode(&key("k"), CommitVersion(8)),
                expired_entry(80),
            ),
            (InternalKey::encode(&key("k"), CommitVersion(3)), entry(30)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 10)
            .with_drop_expired(true)
            .collect();
        // v50 kept (above floor), v8 dropped (expired below floor), v3 kept (floor entry)
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0.commit_id(), 50);
        assert_eq!(result[1].0.commit_id(), 3);
    }

    #[test]
    fn test_issue_1727_prune_floor_zero_preserves_expired() {
        // prune_floor=0 means "don't prune". Even with drop_expired=true,
        // no entry should be dropped because commit_id < 0 is always false for u64.
        let items = vec![(
            InternalKey::encode(&key("k"), CommitVersion(5)),
            expired_entry(50),
        )];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 0)
            .with_drop_expired(true)
            .collect();
        assert_eq!(
            result.len(),
            1,
            "prune_floor=0 disables all pruning, including TTL expiration"
        );
    }

    #[test]
    fn test_issue_1727_all_below_floor_expired_drops_all() {
        // Above-floor entry + multiple expired below-floor entries.
        // All below-floor entries are expired → no floor entry survives.
        let items = vec![
            (
                InternalKey::encode(&key("k"), CommitVersion(50)),
                entry(500),
            ),
            (
                InternalKey::encode(&key("k"), CommitVersion(8)),
                expired_entry(80),
            ),
            (
                InternalKey::encode(&key("k"), CommitVersion(3)),
                expired_entry(30),
            ),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 10)
            .with_drop_expired(true)
            .collect();
        // Only v50 survives; both below-floor expired entries are dropped
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.commit_id(), 50);
    }

    // -----------------------------------------------------------------------
    // CompactionScheduler tests
    // -----------------------------------------------------------------------

    #[test]
    fn tier_assignment_sizes() {
        let sched = CompactionScheduler::default();
        // base_size = 64 MiB

        // Zero and sub-base sizes → tier 0
        assert_eq!(sched.tier_for_size(0), 0);
        assert_eq!(sched.tier_for_size(1), 0);
        assert_eq!(sched.tier_for_size(64 * 1024 * 1024), 0); // exactly base_size

        // 4x base_size → log4(4) = 1 → tier 1
        assert_eq!(sched.tier_for_size(4 * 64 * 1024 * 1024), 1);

        // 16x base_size → log4(16) = 2 → tier 2
        assert_eq!(sched.tier_for_size(16 * 64 * 1024 * 1024), 2);

        // 2x base_size → log4(2) ≈ 0.5 → floor → tier 0
        assert_eq!(sched.tier_for_size(2 * 64 * 1024 * 1024), 0);

        // 5x base_size → log4(5) ≈ 1.16 → floor → tier 1
        assert_eq!(sched.tier_for_size(5 * 64 * 1024 * 1024), 1);
    }

    #[test]
    fn pick_candidates_groups_by_tier() {
        let sched = CompactionScheduler {
            min_merge_width: 4,
            base_size: 64 * 1024 * 1024,
        };

        // 8 small segments (all tier 0)
        let sizes = vec![100u64; 8];
        let candidates = sched.pick_candidates(&sizes);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].tier, 0);
        assert_eq!(candidates[0].segment_indices.len(), 8);
        // Indices should be 0..8
        for i in 0..8 {
            assert_eq!(candidates[0].segment_indices[i], i);
        }
    }

    #[test]
    fn pick_candidates_respects_min_merge_width() {
        let sched = CompactionScheduler {
            min_merge_width: 4,
            base_size: 64 * 1024 * 1024,
        };

        // Only 3 segments — below min_merge_width of 4
        let sizes = vec![100u64; 3];
        let candidates = sched.pick_candidates(&sizes);
        assert!(candidates.is_empty());
    }

    #[test]
    fn pick_candidates_multiple_tiers() {
        let sched = CompactionScheduler {
            min_merge_width: 2,
            base_size: 1024, // 1 KiB base for easy testing
        };

        // Mix of sizes:
        // 100, 200, 300, 500 → all ≤ base_size → tier 0
        // 4096, 5000 → 4x-5x base → log4(4)=1, log4(~4.88)=1 → tier 1
        let sizes = vec![100, 200, 300, 500, 4096, 5000];
        let candidates = sched.pick_candidates(&sizes);

        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].tier, 0);
        assert_eq!(candidates[0].segment_indices.len(), 4);
        assert_eq!(candidates[1].tier, 1);
        assert_eq!(candidates[1].segment_indices.len(), 2);
    }

    // -----------------------------------------------------------------------
    // Issue #1729: EventLog entries must be exempt from compaction pruning
    // -----------------------------------------------------------------------

    /// Create a Key with TypeTag::Event (sequence-based event key).
    fn event_key(sequence: u64) -> Key {
        let ns = Arc::new(Namespace::new(branch(), "default".to_string()));
        Key::new(ns, TypeTag::Event, sequence.to_be_bytes().to_vec())
    }

    #[test]
    fn test_issue_1729_event_entries_exempt_from_version_pruning() {
        // Event entries at commit_ids 1, 2, 3 with prune_floor=10.
        // Normal KV entries below floor would be pruned to keep only the
        // newest floor entry. But Event entries must ALL survive because
        // dropping any event breaks the hash chain.
        let items = vec![
            (
                InternalKey::encode(&event_key(0), CommitVersion(1)),
                entry(100),
            ),
            (
                InternalKey::encode(&event_key(1), CommitVersion(2)),
                entry(200),
            ),
            (
                InternalKey::encode(&event_key(2), CommitVersion(3)),
                entry(300),
            ),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 10).collect();
        assert_eq!(
            result.len(),
            3,
            "all Event entries must survive compaction regardless of prune_floor"
        );
    }

    #[test]
    fn test_issue_1729_event_entries_exempt_from_max_versions() {
        // Event metadata key updated multiple times (same key, different versions).
        // max_versions=1 would normally keep only the newest. But Event entries
        // must be exempt from max_versions pruning.
        let meta_key = {
            let ns = Arc::new(Namespace::new(branch(), "default".to_string()));
            Key::new(ns, TypeTag::Event, b"__meta__".to_vec())
        };
        let items = vec![
            (
                InternalKey::encode(&meta_key, CommitVersion(10)),
                entry(100),
            ),
            (InternalKey::encode(&meta_key, CommitVersion(8)), entry(80)),
            (InternalKey::encode(&meta_key, CommitVersion(5)), entry(50)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 0)
            .with_max_versions(1)
            .collect();
        assert_eq!(
            result.len(),
            3,
            "Event entries must be exempt from max_versions pruning"
        );
    }

    #[test]
    fn test_issue_1729_event_entries_exempt_from_ttl_expiration() {
        // An expired Event entry below prune_floor — normally dropped by
        // drop_expired. But Event entries must never be dropped by TTL.
        let items = vec![(
            InternalKey::encode(&event_key(0), CommitVersion(5)),
            expired_entry(100),
        )];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 10)
            .with_drop_expired(true)
            .collect();
        assert_eq!(
            result.len(),
            1,
            "expired Event entry must NOT be dropped — hash chain integrity"
        );
    }

    #[test]
    fn test_issue_1729_kv_entries_still_pruned_normally() {
        // Verify that KV entries are still subject to normal pruning rules
        // when Event entries are exempt. Mix of KV and Event entries.
        let kv = key("user:alice");
        let ev = event_key(0);
        let items = vec![
            // KV entry below floor — should be kept as floor entry
            (InternalKey::encode(&kv, CommitVersion(3)), entry(30)),
            (InternalKey::encode(&kv, CommitVersion(1)), entry(10)),
            // Event entry below floor — must be kept (exempt)
            (InternalKey::encode(&ev, CommitVersion(2)), entry(200)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let result: Vec<_> = CompactionIterator::new(merge, 10).collect();
        // KV: keeps only floor entry (v3), prunes v1
        // Event: keeps v2 (exempt from pruning)
        assert_eq!(result.len(), 2, "KV pruned normally, Event exempt");
        // The KV floor entry
        assert_eq!(result[0].0.commit_id(), 3);
        // The Event entry
        assert_eq!(result[1].0.commit_id(), 2);
    }
}
