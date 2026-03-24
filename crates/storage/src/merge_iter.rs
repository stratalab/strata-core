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

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::iter::Peekable;
use strata_core::types::BranchId;

/// Source count threshold: use linear scan at or below this, heap above.
const HEAP_THRESHOLD: usize = 4;

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
/// For ≤ 4 sources a linear scan is used (lower overhead). Above that
/// threshold a binary min-heap provides O(log k) per `next()` call.
pub struct MergeIterator<I: Iterator<Item = (InternalKey, MemtableEntry)>> {
    state: MergeState<I>,
}

enum MergeState<I: Iterator<Item = (InternalKey, MemtableEntry)>> {
    Linear {
        sources: Vec<Peekable<I>>,
    },
    Heap {
        sources: Vec<I>,
        heap: BinaryHeap<HeapItem>,
    },
}

/// Entry stored in the binary heap for the heap-based merge path.
struct HeapItem {
    key: InternalKey,
    entry: MemtableEntry,
    source_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_idx == other.source_idx
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reverse comparison for min-heap behavior.
        // Primary: ascending key order (reverse → other before self).
        // Tie-break: ascending source_idx (lower index = newer = wins).
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.source_idx.cmp(&self.source_idx))
    }
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> MergeIterator<I> {
    /// Create a merge iterator over the given sources.
    ///
    /// Sources must be sorted in ascending `InternalKey` order.
    /// `sources[0]` is the newest; `sources[N-1]` is the oldest.
    pub fn new(sources: Vec<I>) -> Self {
        if sources.len() > HEAP_THRESHOLD {
            let mut sources: Vec<I> = sources.into_iter().collect();
            let mut heap = BinaryHeap::with_capacity(sources.len());
            for (i, source) in sources.iter_mut().enumerate() {
                if let Some((key, entry)) = source.next() {
                    heap.push(HeapItem {
                        key,
                        entry,
                        source_idx: i,
                    });
                }
            }
            Self {
                state: MergeState::Heap { sources, heap },
            }
        } else {
            Self {
                state: MergeState::Linear {
                    sources: sources.into_iter().map(|s| s.peekable()).collect(),
                },
            }
        }
    }
}

impl<I: Iterator<Item = (InternalKey, MemtableEntry)>> Iterator for MergeIterator<I> {
    type Item = (InternalKey, MemtableEntry);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.state {
            MergeState::Linear { sources } => {
                // Find source with the smallest key. On ties, prefer lower index (newer).
                let mut min_idx: Option<usize> = None;
                let mut min_key: Option<&InternalKey> = None;

                for (i, source) in sources.iter_mut().enumerate() {
                    if let Some((ik, _)) = source.peek() {
                        let is_smaller = match min_key {
                            None => true,
                            Some(current) => ik < current,
                        };
                        if is_smaller {
                            min_idx = Some(i);
                            min_key = Some(ik);
                        }
                    }
                }

                min_idx.and_then(|i| sources[i].next())
            }
            MergeState::Heap { sources, heap } => {
                let item = heap.pop()?;
                if let Some((key, entry)) = sources[item.source_idx].next() {
                    heap.push(HeapItem {
                        key,
                        entry,
                        source_idx: item.source_idx,
                    });
                }
                Some((item.key, item.entry))
            }
        }
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

    /// Issue #1686: MergeIterator must produce correct results with many sources
    /// (heap-based path). Verifies ordering and tie-breaking with 8 sources.
    #[test]
    fn test_issue_1686_merge_iterator_heap_correctness() {
        // 8 sources — each has one unique key plus a shared key "m" at commit_id=10.
        // The shared key tests tie-breaking: source 0 (newest) must win.
        let shared_key = key("m");
        let shared_ik = InternalKey::encode(&shared_key, 10);

        let keys = ["a", "b", "c", "d", "e", "f", "g", "h"];
        let mut sources: Vec<Vec<(InternalKey, MemtableEntry)>> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            let unique_ik = InternalKey::encode(&key(k), 1);
            let mut items = vec![(unique_ik, entry(i as i64))];
            // All 8 sources contain the shared key "m"
            items.push((shared_ik.clone(), entry(100 + i as i64)));
            items.sort_by(|a, b| a.0.cmp(&b.0));
            sources.push(items);
        }

        let iters: Vec<_> = sources.into_iter().map(|s| s.into_iter()).collect();
        assert_eq!(iters.len(), 8); // above any reasonable linear threshold

        let merged: Vec<_> = MergeIterator::new(iters).collect();

        // Total items: 8 unique + 8 copies of shared key = 16
        assert_eq!(merged.len(), 16);

        // Verify ascending key order throughout
        for pair in merged.windows(2) {
            assert!(
                pair[0].0 <= pair[1].0,
                "merge output not in ascending order"
            );
        }

        // Find all entries for the shared key "m" — source 0 must come first (tie-break)
        let shared_entries: Vec<_> = merged
            .iter()
            .filter(|(ik, _)| ik.typed_key_prefix() == shared_ik.typed_key_prefix())
            .collect();
        assert_eq!(shared_entries.len(), 8);
        // Source i produced entry(100+i). Tie-breaking by source index means
        // they appear in source order: 100, 101, 102, ..., 107.
        for (i, (_, e)) in shared_entries.iter().enumerate() {
            assert_eq!(e.value, Value::Int(100 + i as i64));
        }
    }

    /// Issue #1686: MergeIterator with many sources through MvccIterator
    /// produces correct MVCC deduplication.
    #[test]
    fn test_issue_1686_merge_iterator_heap_with_mvcc() {
        // 6 sources, each with the same key "k" at different commit_ids.
        // Source 0 has commit_id=6 (newest), source 5 has commit_id=1 (oldest).
        // Snapshot at version 4 should pick commit_id=4 from source 2.
        let k = key("k");
        let mut sources: Vec<Vec<(InternalKey, MemtableEntry)>> = Vec::new();
        for i in 0..6 {
            let commit_id = 6 - i as u64; // source 0 → commit 6, source 5 → commit 1
            let ik = InternalKey::encode(&k, commit_id);
            sources.push(vec![(ik, entry(commit_id as i64 * 10))]);
        }

        let iters: Vec<_> = sources.into_iter().map(|s| s.into_iter()).collect();
        let merge = MergeIterator::new(iters);
        let results: Vec<_> = MvccIterator::new(merge, 4).collect();

        assert_eq!(results.len(), 1);
        // commit_id=4 → value=40
        assert_eq!(results[0].1.value, Value::Int(40));
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
