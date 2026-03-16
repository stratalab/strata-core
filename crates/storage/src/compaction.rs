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
        }
    }
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> Iterator for CompactionIterator<I> {
    type Item = (InternalKey, MemtableEntry);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (ik, entry) = self.inner.next()?;
            let prefix = ik.typed_key_prefix().to_vec();

            // New logical key — reset floor tracking
            if self.current_prefix.as_ref() != Some(&prefix) {
                self.current_prefix = Some(prefix);
                self.emitted_floor_entry = false;
            }

            let commit_id = ik.commit_id();

            if commit_id >= self.prune_floor {
                // Above floor: always keep
                return Some((ik, entry));
            }

            // Below floor
            if !self.emitted_floor_entry {
                self.emitted_floor_entry = true;
                if entry.is_tombstone {
                    // Dead key cleanup: skip floor tombstone
                    continue;
                }
                // Keep one version at floor for point-in-time reads
                return Some((ik, entry));
            }

            // Already emitted a floor entry for this key — prune
        }
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
        }
    }

    fn tombstone() -> MemtableEntry {
        MemtableEntry {
            value: Value::Null,
            is_tombstone: true,
            timestamp: Timestamp::now(),
            ttl_ms: 0,
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
            (InternalKey::encode(&key("k"), 5), entry(50)),
            (InternalKey::encode(&key("k"), 3), entry(30)),
            (InternalKey::encode(&key("k"), 1), entry(10)),
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
            (InternalKey::encode(&key("k"), 5), entry(50)),
            (InternalKey::encode(&key("k"), 3), entry(30)),
            (InternalKey::encode(&key("k"), 1), entry(10)),
        ];
        let result = compact(items, 4);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0.commit_id(), 5);
        assert_eq!(result[1].0.commit_id(), 3);
    }

    #[test]
    fn compaction_prune_floor_zero_keeps_all() {
        let items = vec![
            (InternalKey::encode(&key("k"), 5), entry(50)),
            (InternalKey::encode(&key("k"), 3), entry(30)),
            (InternalKey::encode(&key("k"), 1), entry(10)),
        ];
        let result = compact(items, 0);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn compaction_drops_dead_tombstone() {
        // tombstone at 3, value at 1, floor=5 → entire key dropped
        let items = vec![
            (InternalKey::encode(&key("k"), 3), tombstone()),
            (InternalKey::encode(&key("k"), 1), entry(10)),
        ];
        let result = compact(items, 5);
        assert!(result.is_empty());
    }

    #[test]
    fn compaction_keeps_tombstone_above_floor() {
        // tombstone at 3, value at 1, floor=2
        // → tombstone at 3 kept (above floor), value at 1 kept (newest below floor)
        let items = vec![
            (InternalKey::encode(&key("k"), 3), tombstone()),
            (InternalKey::encode(&key("k"), 1), entry(10)),
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
            (InternalKey::encode(&key("a"), 5), entry(50)),
            (InternalKey::encode(&key("a"), 3), entry(30)),
            (InternalKey::encode(&key("a"), 1), entry(10)),
            (InternalKey::encode(&key("b"), 4), entry(40)),
            (InternalKey::encode(&key("b"), 2), entry(20)),
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
            (InternalKey::encode(&key("k"), 3), entry(30)),
            (InternalKey::encode(&key("k"), 1), entry(10)),
        ];
        let result = compact(items, 10);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.commit_id(), 3);
        assert_eq!(result[0].1.value, Value::Int(30));
    }

    #[test]
    fn compaction_only_below_floor_tombstone_drops_all() {
        // Key with only a tombstone below floor — fully cleaned up
        let items = vec![(InternalKey::encode(&key("k"), 3), tombstone())];
        let result = compact(items, 10);
        assert!(result.is_empty());
    }
}
