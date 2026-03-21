//! Merge iterator and MVCC deduplication for the segmented read path.
//!
//! Two layers:
//!
//! - **`MergeIterator`**: K-way merge over N sorted sources yielding
//!   `(InternalKey, MemtableEntry)` in ascending InternalKey order.
//!   Ties are broken by source index (lower = newer).
//!
//! - **`MvccIterator`**: Wraps a `MergeIterator`, deduplicates by logical key
//!   (typed_key_prefix), and emits only the newest entry with
//!   `commit_id ≤ max_version` for each key.

use crate::key_encoding::InternalKey;
use crate::memtable::MemtableEntry;

use std::iter::Peekable;
use strata_core::types::BranchId;

// ---------------------------------------------------------------------------
// MergeIterator
// ---------------------------------------------------------------------------

/// K-way merge over sorted `(InternalKey, MemtableEntry)` iterators.
///
/// Sources are ordered by priority: index 0 is the *newest* source (active
/// memtable), higher indices are progressively older (frozen memtables, then
/// segments). When two sources yield the same InternalKey, the lower index
/// wins (newer data takes precedence).
///
/// With typically 1-3 sources, a linear scan for the minimum outperforms a
/// `BinaryHeap`.
pub struct MergeIterator<I: Iterator<Item = (InternalKey, MemtableEntry)>> {
    sources: Vec<Peekable<I>>,
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> MergeIterator<I> {
    /// Create a merge iterator over the given sources.
    ///
    /// Sources must be sorted in ascending `InternalKey` order.
    /// `sources[0]` is the newest; `sources[N-1]` is the oldest.
    pub fn new(sources: Vec<I>) -> Self {
        Self {
            sources: sources.into_iter().map(|s| s.peekable()).collect(),
        }
    }
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> Iterator for MergeIterator<I> {
    type Item = (InternalKey, MemtableEntry);

    fn next(&mut self) -> Option<Self::Item> {
        // Find source with the smallest key.  On ties, prefer lower index (newer).
        // Two-pass approach: first find the min key, then advance that source.
        let mut min_idx: Option<usize> = None;
        let mut min_key: Option<&InternalKey> = None;

        for (i, source) in self.sources.iter_mut().enumerate() {
            if let Some((ik, _)) = source.peek() {
                let is_smaller = match min_key {
                    None => true,
                    Some(current) => ik < current,
                };
                if is_smaller {
                    min_idx = Some(i);
                    min_key = Some(ik);
                }
                // On equal keys, keep the lower index (newer source)
            }
        }

        min_idx.and_then(|i| self.sources[i].next())
    }
}

// ---------------------------------------------------------------------------
// MvccIterator
// ---------------------------------------------------------------------------

/// MVCC deduplication iterator.
///
/// Wraps a `MergeIterator` and for each logical key (identified by
/// `typed_key_prefix`), emits only the first entry whose `commit_id ≤ max_version`.
/// Remaining versions of the same key are skipped.
pub struct MvccIterator<I: Iterator<Item = (InternalKey, MemtableEntry)>> {
    inner: MergeIterator<I>,
    max_version: u64,
    /// Typed key prefix of the last emitted key — used to skip duplicates.
    last_emitted_prefix: Option<Vec<u8>>,
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> MvccIterator<I> {
    /// Create a new MVCC iterator with the given snapshot version.
    pub fn new(inner: MergeIterator<I>, max_version: u64) -> Self {
        Self {
            inner,
            max_version,
            last_emitted_prefix: None,
        }
    }
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> Iterator for MvccIterator<I> {
    type Item = (InternalKey, MemtableEntry);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (ik, entry) = self.inner.next()?;
            let prefix = ik.typed_key_prefix().to_vec();

            // Skip remaining versions of a key we already emitted
            if let Some(ref last) = self.last_emitted_prefix {
                if *last == prefix {
                    continue;
                }
            }

            // MVCC visibility: only emit if commit_id ≤ max_version
            if ik.commit_id() <= self.max_version {
                self.last_emitted_prefix = Some(prefix);
                return Some((ik, entry));
            }
            // commit_id > max_version: skip this version, try the next
            // (which may be an older version of the same key)
        }
    }
}

// ---------------------------------------------------------------------------
// RewritingIterator (COW branching)
// ---------------------------------------------------------------------------

/// Iterator adapter that rewrites keys from a source branch's namespace into
/// the child branch's namespace, filtering out entries written after fork.
///
/// Two responsibilities:
/// 1. **Version gate:** skip entries with `commit_id > fork_version`
/// 2. **Branch_id rewrite:** source → child, so `MvccIterator` groups
///    own + inherited entries by the same `typed_key_prefix`
pub struct RewritingIterator<I> {
    inner: I,
    target_branch_id: BranchId,
    fork_version: u64,
}

impl<I> RewritingIterator<I> {
    /// Create a new rewriting iterator.
    ///
    /// - `inner`: iterator over source branch entries
    /// - `target_branch_id`: child branch's ID (rewrite TO)
    /// - `fork_version`: maximum visible commit_id from source
    pub fn new(inner: I, target_branch_id: BranchId, fork_version: u64) -> Self {
        Self {
            inner,
            target_branch_id,
            fork_version,
        }
    }
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> Iterator for RewritingIterator<I> {
    type Item = (InternalKey, MemtableEntry);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (ik, entry) = self.inner.next()?;
            if ik.commit_id() > self.fork_version {
                continue;
            }
            let rewritten = ik.with_rewritten_branch_id(&self.target_branch_id);
            return Some((rewritten, entry));
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
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

    // ===== MergeIterator =====

    #[test]
    fn merge_non_overlapping() {
        let s1 = vec![
            (InternalKey::encode(&key("a"), 1), entry(1)),
            (InternalKey::encode(&key("c"), 1), entry(3)),
        ];
        let s2 = vec![
            (InternalKey::encode(&key("b"), 1), entry(2)),
            (InternalKey::encode(&key("d"), 1), entry(4)),
        ];

        let merged: Vec<_> = MergeIterator::new(vec![s1.into_iter(), s2.into_iter()]).collect();
        assert_eq!(merged.len(), 4);
        // Should be in sorted order: a, b, c, d
        assert!(merged[0].0 < merged[1].0);
        assert!(merged[1].0 < merged[2].0);
        assert!(merged[2].0 < merged[3].0);
    }

    #[test]
    fn merge_overlapping_keys_same_commit() {
        // Same key+commit from two sources — source 0 (newer) wins on tie
        let ik = InternalKey::encode(&key("k"), 5);
        let s1 = vec![(ik.clone(), entry(100))];
        let s2 = vec![(ik.clone(), entry(200))];

        let merged: Vec<_> = MergeIterator::new(vec![s1.into_iter(), s2.into_iter()]).collect();
        // Both are emitted (merge doesn't dedup — that's MvccIterator's job)
        assert_eq!(merged.len(), 2);
        // Source 0 should come first (lower index wins on tie)
        assert_eq!(merged[0].1.value, Value::Int(100));
    }

    #[test]
    fn merge_empty_sources() {
        let s1: Vec<(InternalKey, MemtableEntry)> = vec![];
        let s2: Vec<(InternalKey, MemtableEntry)> = vec![];
        let merged: Vec<_> = MergeIterator::new(vec![s1.into_iter(), s2.into_iter()]).collect();
        assert!(merged.is_empty());
    }

    #[test]
    fn merge_single_source() {
        let s1 = vec![
            (InternalKey::encode(&key("a"), 1), entry(1)),
            (InternalKey::encode(&key("b"), 1), entry(2)),
        ];
        let merged: Vec<_> = MergeIterator::new(vec![s1.into_iter()]).collect();
        assert_eq!(merged.len(), 2);
    }

    // ===== MvccIterator =====

    #[test]
    fn mvcc_dedup_picks_newest_visible() {
        // key "k" has versions 3, 2, 1 — snapshot at 2 should pick version 2
        let items = vec![
            (InternalKey::encode(&key("k"), 3), entry(30)),
            (InternalKey::encode(&key("k"), 2), entry(20)),
            (InternalKey::encode(&key("k"), 1), entry(10)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let results: Vec<_> = MvccIterator::new(merge, 2).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.value, Value::Int(20));
    }

    #[test]
    fn mvcc_dedup_multiple_keys() {
        let items = vec![
            (InternalKey::encode(&key("a"), 2), entry(20)),
            (InternalKey::encode(&key("a"), 1), entry(10)),
            (InternalKey::encode(&key("b"), 3), entry(30)),
            (InternalKey::encode(&key("b"), 1), entry(10)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let results: Vec<_> = MvccIterator::new(merge, u64::MAX).collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1.value, Value::Int(20)); // a@2
        assert_eq!(results[1].1.value, Value::Int(30)); // b@3
    }

    #[test]
    fn mvcc_tombstone_is_emitted() {
        // Tombstone at version 2 should be emitted (caller decides filtering)
        let items = vec![
            (InternalKey::encode(&key("k"), 2), tombstone()),
            (InternalKey::encode(&key("k"), 1), entry(10)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let results: Vec<_> = MvccIterator::new(merge, u64::MAX).collect();
        assert_eq!(results.len(), 1);
        assert!(results[0].1.is_tombstone);
    }

    #[test]
    fn mvcc_all_versions_above_snapshot() {
        let items = vec![
            (InternalKey::encode(&key("k"), 5), entry(50)),
            (InternalKey::encode(&key("k"), 3), entry(30)),
        ];
        let merge = MergeIterator::new(vec![items.into_iter()]);
        let results: Vec<_> = MvccIterator::new(merge, 2).collect();
        assert!(results.is_empty());
    }
}
