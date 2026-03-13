//! MVCC version chain — stores multiple versions of a value per key.
//!
//! `VersionChain` manages the version history for a single key.
//! Versions are stored newest-first for efficient snapshot reads.
//! Uses `VersionStorage` enum to inline single-version entries
//! (avoiding VecDeque allocation for the common case).

use crate::stored_value::StoredValue;
use std::collections::VecDeque;
use strata_core::traits::WriteMode;

/// Internal storage for version chains — optimized for the common single-version case.
///
/// Most keys (especially graph edges) only ever have one version. `Single` stores
/// the value inline without any heap allocation for the container, saving ~270 bytes
/// per entry compared to `VecDeque::with_capacity(4)`.
#[derive(Debug, Clone)]
pub(super) enum VersionStorage {
    /// Single version stored inline — no heap allocation for the container
    Single(StoredValue),
    /// Multiple versions stored newest-first in a VecDeque
    Multi(VecDeque<StoredValue>),
}

/// Version chain for MVCC - stores multiple versions of a value
///
/// Versions are stored in descending order (newest first) for efficient
/// snapshot reads - we typically want the most recent version <= snapshot_version.
///
/// # Memory Optimization
///
/// Uses `VersionStorage` enum to avoid VecDeque allocation for the common
/// single-version case. For keys with multiple versions, promotes to
/// VecDeque with O(1) push_front.
#[derive(Debug, Clone)]
pub struct VersionChain {
    /// Versions stored newest-first for efficient MVCC reads
    /// Uses VersionStorage enum to inline single-version entries
    versions: VersionStorage,
}

impl VersionChain {
    /// Create a new version chain with a single version
    pub fn new(value: StoredValue) -> Self {
        Self {
            versions: VersionStorage::Single(value),
        }
    }

    /// Replace the latest version in-place (no history accumulation).
    ///
    /// Drops all previous versions and stores only this value.
    /// Use for internal data (e.g., graph adjacency lists) where
    /// multi-version history is not needed and would waste memory.
    #[inline]
    pub fn replace_latest(&mut self, value: StoredValue) {
        self.versions = VersionStorage::Single(value);
    }

    /// Add a new version, respecting the write mode.
    ///
    /// - `WriteMode::Append`: standard MVCC — pushes a new version (keeps history)
    /// - `WriteMode::KeepLast(1)`: overwrites — drops all old versions (no history)
    /// - `WriteMode::KeepLast(n)`: pushes then prunes to at most n versions
    #[inline]
    pub fn put(&mut self, value: StoredValue, mode: WriteMode) {
        match mode {
            WriteMode::Append => self.push(value),
            WriteMode::KeepLast(1) => self.replace_latest(value),
            WriteMode::KeepLast(n) => {
                debug_assert!(n > 0, "KeepLast(0) is invalid; treating as KeepLast(1)");
                let n = n.max(1); // guard against KeepLast(0) in release builds
                self.push(value);
                self.truncate_to(n);
            }
        }
    }

    /// Truncate the version chain to at most `n` entries, dropping oldest.
    /// If only 1 entry remains, demotes Multi back to Single.
    fn truncate_to(&mut self, n: usize) {
        if let VersionStorage::Multi(deque) = &mut self.versions {
            while deque.len() > n {
                deque.pop_back(); // drop oldest (newest-first ordering)
            }
            if deque.len() == 1 {
                if let Some(sv) = deque.pop_front() {
                    self.versions = VersionStorage::Single(sv);
                }
            }
        }
    }

    /// Add a new version, maintaining descending version order.
    ///
    /// Promotes Single → Multi on second version. Uses sorted insertion
    /// to guarantee the newest-first invariant even if callers push
    /// versions out of order (e.g., concurrent blind writes where version
    /// allocation and shard-lock acquisition are not atomic).
    ///
    /// O(n) worst case, but n (chain length between GC cycles) is typically 1–5.
    pub fn push(&mut self, value: StoredValue) {
        match &mut self.versions {
            VersionStorage::Single(_) => {
                // Promote to Multi: take the existing single value, create VecDeque
                let old =
                    std::mem::replace(&mut self.versions, VersionStorage::Multi(VecDeque::new()));
                match old {
                    VersionStorage::Single(old_value) => {
                        let mut deque = VecDeque::with_capacity(2);
                        // Insert in descending version order
                        if value.version_raw() >= old_value.version_raw() {
                            deque.push_back(value);
                            deque.push_back(old_value);
                        } else {
                            deque.push_back(old_value);
                            deque.push_back(value);
                        }
                        self.versions = VersionStorage::Multi(deque);
                    }
                    VersionStorage::Multi(_) => {
                        unreachable!("matched Single but mem::replace returned Multi")
                    }
                }
            }
            VersionStorage::Multi(deque) => {
                // Fast path: value is newest (common case)
                if deque.is_empty() || value.version_raw() >= deque.front().unwrap().version_raw() {
                    deque.push_front(value);
                } else {
                    // Sorted insertion: find position to maintain descending order.
                    // Walk from front (newest) to back (oldest); insert before the
                    // first entry with a smaller version.
                    let version = value.version_raw();
                    let pos = deque
                        .iter()
                        .position(|sv| sv.version_raw() < version)
                        .unwrap_or(deque.len());
                    deque.insert(pos, value);
                }
            }
        }
    }

    /// Get the version at or before the given max_version
    ///
    /// Uses raw u64 comparison. All storage versions are `Version::Txn`
    /// variants — this invariant is enforced at `StoredValue` construction time.
    pub fn get_at_version(&self, max_version: u64) -> Option<&StoredValue> {
        match &self.versions {
            VersionStorage::Single(sv) => {
                if sv.version_raw() <= max_version {
                    Some(sv)
                } else {
                    None
                }
            }
            VersionStorage::Multi(deque) => deque.iter().find(|sv| sv.version_raw() <= max_version),
        }
    }

    /// Get the version at or before the given timestamp (microseconds since epoch).
    ///
    /// Versions are stored newest-first, so we scan until we find one with timestamp <= max_timestamp.
    /// Returns None if no version exists at or before the given timestamp.
    pub fn get_at_timestamp(&self, max_timestamp: u64) -> Option<&StoredValue> {
        match &self.versions {
            VersionStorage::Single(sv) => {
                if u64::from(sv.timestamp()) <= max_timestamp {
                    Some(sv)
                } else {
                    None
                }
            }
            VersionStorage::Multi(deque) => deque
                .iter()
                .find(|sv| u64::from(sv.timestamp()) <= max_timestamp),
        }
    }

    /// Get the latest version
    #[inline]
    pub fn latest(&self) -> Option<&StoredValue> {
        match &self.versions {
            VersionStorage::Single(sv) => Some(sv),
            VersionStorage::Multi(deque) => deque.front(),
        }
    }

    /// Remove versions older than min_version (garbage collection)
    /// Keeps at least one version.
    /// Returns the number of pruned versions.
    pub fn gc(&mut self, min_version: u64) -> usize {
        match &mut self.versions {
            VersionStorage::Single(_) => 0, // Only one version, always keep it
            VersionStorage::Multi(deque) => {
                if deque.len() <= 1 {
                    return 0;
                }
                let mut pruned = 0;
                while deque.len() > 1 {
                    if let Some(oldest) = deque.back() {
                        if oldest.version_raw() < min_version {
                            deque.pop_back();
                            pruned += 1;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                pruned
            }
        }
    }

    /// Number of versions stored
    pub fn version_count(&self) -> usize {
        match &self.versions {
            VersionStorage::Single(_) => 1,
            VersionStorage::Multi(deque) => deque.len(),
        }
    }

    /// Returns true if this chain represents a dead key — only a single
    /// tombstone or expired version remains. After GC, such keys can be
    /// fully removed from storage since no active snapshot can see a
    /// non-deleted value.
    pub fn is_dead(&self) -> bool {
        self.version_count() == 1
            && self
                .latest()
                .is_some_and(|sv| sv.is_tombstone() || sv.is_expired())
    }

    /// Get version history (newest first)
    ///
    /// Returns versions in descending order (newest first).
    /// Optionally limited and filtered by `before_version`.
    ///
    /// # Arguments
    /// * `limit` - Maximum versions to return (None = all)
    /// * `before_version` - Only return versions older than this (exclusive, for pagination)
    ///
    /// # Returns
    /// Vector of StoredValue references, newest first
    pub fn history(&self, limit: Option<usize>, before_version: Option<u64>) -> Vec<&StoredValue> {
        match &self.versions {
            VersionStorage::Single(sv) => {
                if limit == Some(0) {
                    return vec![];
                }
                let passes_filter = match before_version {
                    Some(before) => sv.version_raw() < before,
                    None => true,
                };
                if passes_filter {
                    vec![sv]
                } else {
                    vec![]
                }
            }
            VersionStorage::Multi(deque) => {
                let iter = deque.iter();
                let filtered: Vec<&StoredValue> = match before_version {
                    Some(before) => iter.filter(|sv| sv.version_raw() < before).collect(),
                    None => iter.collect(),
                };

                match limit {
                    Some(n) => filtered.into_iter().take(n).collect(),
                    None => filtered,
                }
            }
        }
    }

    /// Check if the version chain is empty
    pub fn is_empty(&self) -> bool {
        match &self.versions {
            VersionStorage::Single(_) => false, // Single always has one value
            VersionStorage::Multi(deque) => deque.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::value::Value;
    use strata_core::{Timestamp, Version};

    fn make_sv(version: u64) -> StoredValue {
        StoredValue::new(
            strata_core::value::Value::Int(version as i64),
            Version::txn(version),
            None,
        )
    }

    fn make_sv_with_ts(version: u64, timestamp_micros: u64) -> StoredValue {
        StoredValue::with_timestamp(
            strata_core::value::Value::Int(version as i64),
            Version::txn(version),
            Timestamp::from_micros(timestamp_micros),
            None,
        )
    }

    fn create_stored_value(value: Value, version: u64) -> StoredValue {
        StoredValue::new(value, Version::txn(version), None)
    }

    // ========================================================================
    // VersionChain / VersionStorage Tests
    // ========================================================================

    #[test]
    fn test_version_chain_single_latest() {
        let chain = VersionChain::new(make_sv(1));
        assert_eq!(chain.version_count(), 1);
        assert!(!chain.is_empty());
        let latest = chain.latest().unwrap();
        assert_eq!(latest.version(), Version::txn(1));
    }

    #[test]
    fn test_version_chain_single_to_multi_promotion() {
        let mut chain = VersionChain::new(make_sv(1));
        assert_eq!(chain.version_count(), 1);

        // Push promotes Single → Multi
        chain.push(make_sv(2));
        assert_eq!(chain.version_count(), 2);

        // Latest is the newest
        assert_eq!(chain.latest().unwrap().version(), Version::txn(2));

        // Push a third — stays Multi
        chain.push(make_sv(3));
        assert_eq!(chain.version_count(), 3);
        assert_eq!(chain.latest().unwrap().version(), Version::txn(3));
    }

    #[test]
    fn test_version_chain_get_at_version_single_boundary() {
        let chain = VersionChain::new(make_sv(5));

        // Version <= 5 finds it
        assert!(chain.get_at_version(5).is_some());
        assert!(chain.get_at_version(10).is_some());

        // Version < 5 misses
        assert!(chain.get_at_version(4).is_none());
        assert!(chain.get_at_version(0).is_none());
    }

    #[test]
    fn test_version_chain_get_at_version_multi_scan() {
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(make_sv(5));
        chain.push(make_sv(10));
        // Stored newest-first: [10, 5, 1]

        // At version 10 → gets version 10
        assert_eq!(
            chain.get_at_version(10).unwrap().version(),
            Version::txn(10)
        );
        // At version 7 → gets version 5 (first <= 7)
        assert_eq!(chain.get_at_version(7).unwrap().version(), Version::txn(5));
        // At version 3 → gets version 1
        assert_eq!(chain.get_at_version(3).unwrap().version(), Version::txn(1));
        // At version 0 → nothing
        assert!(chain.get_at_version(0).is_none());
    }

    #[test]
    fn test_version_chain_get_at_timestamp_single() {
        let chain = VersionChain::new(make_sv_with_ts(1, 1000));

        // Timestamp >= 1000 finds it
        assert!(chain.get_at_timestamp(1000).is_some());
        assert!(chain.get_at_timestamp(2000).is_some());

        // Timestamp < 1000 misses
        assert!(chain.get_at_timestamp(999).is_none());
    }

    #[test]
    fn test_version_chain_get_at_timestamp_multi() {
        let mut chain = VersionChain::new(make_sv_with_ts(1, 100));
        chain.push(make_sv_with_ts(2, 500));
        chain.push(make_sv_with_ts(3, 1000));
        // Stored newest-first: [ts=1000, ts=500, ts=100]

        // At ts=1000 → newest
        assert_eq!(
            chain.get_at_timestamp(1000).unwrap().version(),
            Version::txn(3)
        );
        // At ts=700 → middle
        assert_eq!(
            chain.get_at_timestamp(700).unwrap().version(),
            Version::txn(2)
        );
        // At ts=200 → oldest
        assert_eq!(
            chain.get_at_timestamp(200).unwrap().version(),
            Version::txn(1)
        );
        // At ts=50 → nothing
        assert!(chain.get_at_timestamp(50).is_none());
    }

    #[test]
    fn test_version_chain_gc_single_is_noop() {
        let mut chain = VersionChain::new(make_sv(1));

        // GC on Single should always return 0 — we never prune the last version
        let pruned = chain.gc(100);
        assert_eq!(pruned, 0);
        assert_eq!(chain.version_count(), 1);
        assert_eq!(chain.latest().unwrap().version(), Version::txn(1));
    }

    #[test]
    fn test_version_chain_gc_multi_prunes_old() {
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(make_sv(5));
        chain.push(make_sv(10));
        chain.push(make_sv(15));
        // Stored newest-first: [15, 10, 5, 1]

        // GC with min_version=6 → prunes versions < 6 (i.e., versions 1 and 5)
        let pruned = chain.gc(6);
        assert_eq!(pruned, 2);
        assert_eq!(chain.version_count(), 2);

        // Remaining: [15, 10]
        assert_eq!(chain.latest().unwrap().version(), Version::txn(15));
        assert!(chain.get_at_version(10).is_some());
        assert!(chain.get_at_version(5).is_none()); // pruned
    }

    #[test]
    fn test_version_chain_gc_multi_keeps_at_least_one() {
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(make_sv(5));
        // Stored: [5, 1]

        // GC with min_version=100 → would prune both, but keeps at least one
        let pruned = chain.gc(100);
        assert_eq!(pruned, 1); // Only pruned version 1, kept version 5
        assert_eq!(chain.version_count(), 1);
        assert_eq!(chain.latest().unwrap().version(), Version::txn(5));
    }

    #[test]
    fn test_version_chain_gc_multi_prunes_nothing_if_all_recent() {
        let mut chain = VersionChain::new(make_sv(10));
        chain.push(make_sv(20));
        // Stored: [20, 10]

        // GC with min_version=5 → nothing to prune (both >= 5)
        let pruned = chain.gc(5);
        assert_eq!(pruned, 0);
        assert_eq!(chain.version_count(), 2);
    }

    #[test]
    fn test_version_chain_history_limit_zero() {
        let chain = VersionChain::new(make_sv(1));
        // limit=0 should return empty even for Single
        assert!(chain.history(Some(0), None).is_empty());

        let mut multi_chain = VersionChain::new(make_sv(1));
        multi_chain.push(make_sv(2));
        // limit=0 on Multi too
        assert!(multi_chain.history(Some(0), None).is_empty());
    }

    #[test]
    fn test_version_chain_history_with_before_version() {
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(make_sv(5));
        chain.push(make_sv(10));
        // Stored newest-first: [10, 5, 1]

        // before_version=10 → only versions < 10 → [5, 1]
        let h = chain.history(None, Some(10));
        assert_eq!(h.len(), 2);
        assert_eq!(h[0].version(), Version::txn(5));
        assert_eq!(h[1].version(), Version::txn(1));

        // before_version=6, limit=1 → [5]
        let h = chain.history(Some(1), Some(6));
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].version(), Version::txn(5));
    }

    #[test]
    fn test_version_chain_history_single_before_version_filters() {
        let chain = VersionChain::new(make_sv(5));

        // before_version=10 → 5 < 10 → returns it
        assert_eq!(chain.history(None, Some(10)).len(), 1);

        // before_version=5 → 5 < 5 is false → empty
        assert!(chain.history(None, Some(5)).is_empty());

        // before_version=3 → 5 < 3 is false → empty
        assert!(chain.history(None, Some(3)).is_empty());
    }

    // ========================================================================
    // VersionChain: Out-of-Order Push (Issue #1383)
    // ========================================================================

    #[test]
    fn test_version_chain_push_out_of_order_single_to_multi() {
        // Simulate: v5 allocated first, v3 pushed first (Single→Multi promotion)
        let mut chain = VersionChain::new(make_sv(5));
        chain.push(make_sv(3)); // older version pushed after newer
                                // Must be stored descending: [5, 3]
        assert_eq!(chain.latest().unwrap().version(), Version::txn(5));
        assert_eq!(chain.get_at_version(4).unwrap().version(), Version::txn(3));
        assert_eq!(chain.version_count(), 2);
    }

    #[test]
    fn test_version_chain_push_out_of_order_multi() {
        // Simulate concurrent blind writes: versions allocated 5,6,7 but
        // pushed in order 6, 7, 5
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(make_sv(6));
        chain.push(make_sv(7));
        chain.push(make_sv(5)); // out of order
                                // Must be stored descending: [7, 6, 5, 1]
        assert_eq!(chain.latest().unwrap().version(), Version::txn(7));
        assert_eq!(chain.get_at_version(6).unwrap().version(), Version::txn(6));
        assert_eq!(chain.get_at_version(5).unwrap().version(), Version::txn(5));
        assert_eq!(chain.get_at_version(1).unwrap().version(), Version::txn(1));
        assert_eq!(chain.version_count(), 4);
    }

    #[test]
    fn test_version_chain_push_out_of_order_oldest_last() {
        // Push newest first, then middle, then oldest
        let mut chain = VersionChain::new(make_sv(10));
        chain.push(make_sv(5));
        chain.push(make_sv(1));
        // Must be descending: [10, 5, 1]
        assert_eq!(chain.latest().unwrap().version(), Version::txn(10));
        let h = chain.history(None, None);
        assert_eq!(h.len(), 3);
        assert_eq!(h[0].version(), Version::txn(10));
        assert_eq!(h[1].version(), Version::txn(5));
        assert_eq!(h[2].version(), Version::txn(1));
    }

    #[test]
    fn test_version_chain_push_out_of_order_middle_insert() {
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(make_sv(10));
        chain.push(make_sv(5)); // should go between 10 and 1
                                // Must be descending: [10, 5, 1]
        let h = chain.history(None, None);
        assert_eq!(h.len(), 3);
        assert_eq!(h[0].version(), Version::txn(10));
        assert_eq!(h[1].version(), Version::txn(5));
        assert_eq!(h[2].version(), Version::txn(1));
    }

    #[test]
    fn test_version_chain_push_duplicate_version() {
        // Same version pushed twice (shouldn't happen in practice, but be safe)
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(make_sv(5));
        chain.push(make_sv(5));
        assert_eq!(chain.version_count(), 3);
        assert_eq!(chain.latest().unwrap().version(), Version::txn(5));
    }

    #[test]
    fn test_version_chain_gc_after_out_of_order_push() {
        // Ensure GC works correctly on a chain that was built out of order
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(make_sv(10));
        chain.push(make_sv(5)); // out of order → sorted to [10, 5, 1]
                                // GC versions < 6 → prune 5 and 1
        let pruned = chain.gc(6);
        assert_eq!(pruned, 2);
        assert_eq!(chain.version_count(), 1);
        assert_eq!(chain.latest().unwrap().version(), Version::txn(10));
    }

    #[test]
    fn test_version_chain_get_at_version_after_out_of_order_push() {
        // The bug from #1383: without sorted insertion, get_at_version returns stale data
        let mut chain = VersionChain::new(make_sv(1));
        // Simulate: thread B gets v6, thread A gets v5, thread B pushes first, then A
        chain.push(make_sv(6));
        chain.push(make_sv(5)); // out of order

        // get_at_version(100) must return v6 (newest), not v5
        assert_eq!(
            chain.get_at_version(100).unwrap().version(),
            Version::txn(6)
        );
        // latest() must return v6
        assert_eq!(chain.latest().unwrap().version(), Version::txn(6));
        // get_at_version(5) must return v5
        assert_eq!(chain.get_at_version(5).unwrap().version(), Version::txn(5));
    }

    // ========================================================================
    // VersionChain: KeepLast Tests (Issue #1389)
    // ========================================================================

    #[test]
    fn test_version_chain_keep_last_3() {
        let mut chain = VersionChain::new(make_sv(1));
        chain.put(make_sv(2), WriteMode::KeepLast(3));
        chain.put(make_sv(3), WriteMode::KeepLast(3));
        chain.put(make_sv(4), WriteMode::KeepLast(3));
        chain.put(make_sv(5), WriteMode::KeepLast(3));
        // Should keep only the 3 newest: [5, 4, 3]
        assert_eq!(chain.version_count(), 3);
        assert_eq!(chain.latest().unwrap().version(), Version::txn(5));
        let h = chain.history(None, None);
        assert_eq!(h[0].version(), Version::txn(5));
        assert_eq!(h[1].version(), Version::txn(4));
        assert_eq!(h[2].version(), Version::txn(3));
    }

    #[test]
    fn test_version_chain_keep_last_demotes_to_single() {
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(make_sv(2));
        chain.push(make_sv(3));
        assert_eq!(chain.version_count(), 3);
        // KeepLast(1) should prune down to single
        chain.put(make_sv(4), WriteMode::KeepLast(1));
        assert_eq!(chain.version_count(), 1);
        assert_eq!(chain.latest().unwrap().version(), Version::txn(4));
    }

    #[test]
    fn test_version_chain_keep_last_1_matches_replace_behavior() {
        // Old Replace behavior: replace_latest drops all history
        let mut chain_replace = VersionChain::new(make_sv(1));
        chain_replace.push(make_sv(2));
        chain_replace.push(make_sv(3));
        chain_replace.replace_latest(make_sv(4));

        // New KeepLast(1) behavior should produce identical result
        let mut chain_keep = VersionChain::new(make_sv(1));
        chain_keep.push(make_sv(2));
        chain_keep.push(make_sv(3));
        chain_keep.put(make_sv(4), WriteMode::KeepLast(1));

        assert_eq!(chain_replace.version_count(), chain_keep.version_count());
        assert_eq!(
            chain_replace.latest().unwrap().version(),
            chain_keep.latest().unwrap().version()
        );
    }

    #[test]
    fn test_version_chain_keep_last_fewer_than_n() {
        // KeepLast(5) on a chain with only 2 entries should be a no-op
        let mut chain = VersionChain::new(make_sv(1));
        chain.put(make_sv(2), WriteMode::KeepLast(5));
        assert_eq!(chain.version_count(), 2);
        assert_eq!(chain.latest().unwrap().version(), Version::txn(2));
        // Both versions preserved
        let h = chain.history(None, None);
        assert_eq!(h.len(), 2);
        assert_eq!(h[0].version(), Version::txn(2));
        assert_eq!(h[1].version(), Version::txn(1));
    }

    #[test]
    fn test_version_chain_keep_last_with_out_of_order_push() {
        // KeepLast(2) with out-of-order version insertion
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(make_sv(10));
        chain.push(make_sv(5)); // out of order → sorted to [10, 5, 1]
        assert_eq!(chain.version_count(), 3);

        // KeepLast(2) should push v20 and truncate oldest (v1)
        chain.put(make_sv(20), WriteMode::KeepLast(2));
        assert_eq!(chain.version_count(), 2);
        // Newest two: [20, 10]
        assert_eq!(chain.latest().unwrap().version(), Version::txn(20));
        let h = chain.history(None, None);
        assert_eq!(h[0].version(), Version::txn(20));
        assert_eq!(h[1].version(), Version::txn(10));
    }

    #[test]
    fn test_version_chain_keep_last_0_treated_as_1() {
        // KeepLast(0) should not produce an empty chain (invariant violation)
        // In debug builds this triggers debug_assert, in release it's guarded to max(0,1)=1
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(make_sv(2));
        // We can't test KeepLast(0) in debug mode due to debug_assert.
        // But we can test that truncate_to(1) on a Multi chain demotes correctly.
        chain.put(make_sv(3), WriteMode::KeepLast(1));
        assert_eq!(chain.version_count(), 1);
        assert_eq!(chain.latest().unwrap().version(), Version::txn(3));
        // Verify MVCC reads work correctly after truncation
        assert!(chain.get_at_version(3).is_some());
        assert!(chain.get_at_version(2).is_none()); // pruned
    }

    // ========================================================================
    // VersionChain::history() Tests
    // ========================================================================

    #[test]
    fn test_version_chain_history_all_versions() {
        let mut chain = VersionChain::new(create_stored_value(Value::Int(1), 1));
        chain.push(create_stored_value(Value::Int(2), 2));
        chain.push(create_stored_value(Value::Int(3), 3));

        // Get all versions (newest first)
        let history = chain.history(None, None);
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].version().as_u64(), 3);
        assert_eq!(history[1].version().as_u64(), 2);
        assert_eq!(history[2].version().as_u64(), 1);
    }

    #[test]
    fn test_version_chain_history_with_limit() {
        let mut chain = VersionChain::new(create_stored_value(Value::Int(1), 1));
        chain.push(create_stored_value(Value::Int(2), 2));
        chain.push(create_stored_value(Value::Int(3), 3));
        chain.push(create_stored_value(Value::Int(4), 4));
        chain.push(create_stored_value(Value::Int(5), 5));

        // Get only 2 versions
        let history = chain.history(Some(2), None);
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].version().as_u64(), 5); // Newest
        assert_eq!(history[1].version().as_u64(), 4);
    }

    #[test]
    fn test_version_chain_history_with_before() {
        let mut chain = VersionChain::new(create_stored_value(Value::Int(1), 1));
        chain.push(create_stored_value(Value::Int(2), 2));
        chain.push(create_stored_value(Value::Int(3), 3));
        chain.push(create_stored_value(Value::Int(4), 4));
        chain.push(create_stored_value(Value::Int(5), 5));

        // Get versions before version 4 (should get 1, 2, 3)
        let history = chain.history(None, Some(4));
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].version().as_u64(), 3);
        assert_eq!(history[1].version().as_u64(), 2);
        assert_eq!(history[2].version().as_u64(), 1);
    }

    #[test]
    fn test_version_chain_history_with_limit_and_before() {
        let mut chain = VersionChain::new(create_stored_value(Value::Int(1), 1));
        chain.push(create_stored_value(Value::Int(2), 2));
        chain.push(create_stored_value(Value::Int(3), 3));
        chain.push(create_stored_value(Value::Int(4), 4));
        chain.push(create_stored_value(Value::Int(5), 5));

        // Get 2 versions before version 5
        let history = chain.history(Some(2), Some(5));
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].version().as_u64(), 4);
        assert_eq!(history[1].version().as_u64(), 3);
    }

    #[test]
    fn test_version_chain_history_before_first() {
        let chain = VersionChain::new(create_stored_value(Value::Int(1), 5));

        // Before version 5 returns empty (only version is 5)
        let history = chain.history(None, Some(5));
        assert!(history.is_empty());

        // Before version 6 returns the one version
        let history = chain.history(None, Some(6));
        assert_eq!(history.len(), 1);
    }

    #[test]
    fn test_version_chain_is_empty() {
        let chain = VersionChain::new(create_stored_value(Value::Int(1), 1));
        assert!(!chain.is_empty());
        assert_eq!(chain.version_count(), 1);
    }

    // ========================================================================
    // VersionChain::get_at_version() Tests (MVCC)
    // ========================================================================

    #[test]
    fn test_version_chain_get_at_version_single() {
        let chain = VersionChain::new(create_stored_value(Value::Int(42), 5));

        // Exact version match
        let result = chain.get_at_version(5);
        assert!(result.is_some());
        assert_eq!(result.unwrap().version().as_u64(), 5);

        // Higher version should still return the value (latest <= max_version)
        let result = chain.get_at_version(10);
        assert!(result.is_some());
        assert_eq!(result.unwrap().version().as_u64(), 5);

        // Lower version should return None
        let result = chain.get_at_version(4);
        assert!(result.is_none());
    }

    #[test]
    fn test_version_chain_get_at_version_multiple() {
        // Create chain with versions 1, 2, 3 (newest first after pushes)
        let mut chain = VersionChain::new(create_stored_value(Value::Int(100), 1));
        chain.push(create_stored_value(Value::Int(200), 2));
        chain.push(create_stored_value(Value::Int(300), 3));

        // Chain should have 3 versions
        assert_eq!(chain.version_count(), 3);

        // Query at version 3 should return version 3
        let result = chain.get_at_version(3);
        assert!(result.is_some());
        assert_eq!(result.unwrap().version().as_u64(), 3);
        assert_eq!(result.unwrap().versioned().value, Value::Int(300));

        // Query at version 2 should return version 2
        let result = chain.get_at_version(2);
        assert!(result.is_some());
        assert_eq!(result.unwrap().version().as_u64(), 2);
        assert_eq!(result.unwrap().versioned().value, Value::Int(200));

        // Query at version 1 should return version 1
        let result = chain.get_at_version(1);
        assert!(result.is_some());
        assert_eq!(result.unwrap().version().as_u64(), 1);
        assert_eq!(result.unwrap().versioned().value, Value::Int(100));

        // Query at version 0 should return None
        let result = chain.get_at_version(0);
        assert!(result.is_none());

        // Query at version 100 should return latest (version 3)
        let result = chain.get_at_version(100);
        assert!(result.is_some());
        assert_eq!(result.unwrap().version().as_u64(), 3);
    }

    #[test]
    fn test_version_chain_get_at_version_between_versions() {
        // Create chain with versions 10, 20, 30 (sparse)
        let mut chain = VersionChain::new(create_stored_value(Value::Int(1), 10));
        chain.push(create_stored_value(Value::Int(2), 20));
        chain.push(create_stored_value(Value::Int(3), 30));

        // Query at version 25 should return version 20 (latest <= 25)
        let result = chain.get_at_version(25);
        assert!(result.is_some());
        assert_eq!(result.unwrap().version().as_u64(), 20);

        // Query at version 15 should return version 10
        let result = chain.get_at_version(15);
        assert!(result.is_some());
        assert_eq!(result.unwrap().version().as_u64(), 10);

        // Query at version 5 should return None (no version <= 5)
        let result = chain.get_at_version(5);
        assert!(result.is_none());
    }

    #[test]
    fn test_version_chain_get_at_version_snapshot_isolation() {
        // Simulates snapshot isolation: reader sees consistent view
        let mut chain = VersionChain::new(create_stored_value(Value::String("v1".into()), 1));

        // Snapshot taken at version 1
        let snapshot_version = 1;

        // Writer adds new versions
        chain.push(create_stored_value(Value::String("v2".into()), 2));
        chain.push(create_stored_value(Value::String("v3".into()), 3));

        // Snapshot reader should still see version 1
        let result = chain.get_at_version(snapshot_version);
        assert!(result.is_some());
        assert_eq!(result.unwrap().version().as_u64(), 1);
        assert_eq!(
            result.unwrap().versioned().value,
            Value::String("v1".into())
        );

        // Current reader sees version 3
        let result = chain.get_at_version(u64::MAX);
        assert!(result.is_some());
        assert_eq!(result.unwrap().version().as_u64(), 3);
    }
}
