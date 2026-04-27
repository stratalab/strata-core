//! Seekable iterator stack — equivalent to RocksDB's `InternalIterator` hierarchy.
//!
//! Unlike `std::iter::Iterator`, [`SeekableIterator`] supports repositioning
//! via [`seek()`](SeekableIterator::seek) without destroying the iterator.
//! This enables persistent merge pipelines where children survive across seeks.
//!
//! The stack mirrors RocksDB's proven layering:
//!
//! | RocksDB | Strata |
//! |---------|--------|
//! | `InternalIterator` | [`SeekableIterator`] trait |
//! | `MergingIterator` | [`MergeSeekableIter`] |
//! | `DBIter` (MVCC) | [`MvccSeekableIter`] |
//! | Per-L0-file iterator | [`SegmentSeekableIter`] |
//! | `LevelIterator` | [`LevelSeekableIter`] |
//! | `MemtableIterator` | [`MemtableSeekableIter`] |
//! | `RewritingIterator` (COW) | [`RewritingSeekableIter`] |

use std::collections::BinaryHeap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use strata_core::id::CommitVersion;
use strata_core::BranchId;

use crate::key_encoding::{encode_typed_key_prefix, InternalKey};
use crate::memtable::{Memtable, MemtableEntry};
use crate::segment::{LevelSegmentIter, OwnedSegmentIter, SegmentEntry};
use crate::Key;

// ---------------------------------------------------------------------------
// SeekableIterator trait
// ---------------------------------------------------------------------------

/// Internal iterator trait — equivalent to RocksDB's `InternalIterator`.
///
/// All storage-layer iterators implement this. Unlike `std::iter::Iterator`,
/// supports repositioning via `seek()` without destroying the iterator.
/// Children persist across seeks, enabling RocksDB-style `MergingIterator`
/// that re-seeks children in place and re-heapifies.
pub trait SeekableIterator: Send {
    /// Position at the first entry whose encoded key >= `target`.
    ///
    /// `target` is encoded `InternalKey` bytes. Callers construct
    /// `InternalKey::encode(key, u64::MAX)` once and pass the bytes
    /// to all children — avoids redundant encoding.
    fn seek(&mut self, target: &[u8]);

    /// Advance to the next entry. Returns `false` when exhausted.
    fn advance(&mut self) -> bool;

    /// Whether the iterator is positioned at a valid entry.
    fn valid(&self) -> bool;

    /// Current key. Only valid when `valid()` returns true.
    fn current_key(&self) -> &InternalKey;

    /// Current entry. Only valid when `valid()` returns true.
    fn current_entry(&self) -> &MemtableEntry;

    /// Whether data corruption was detected during iteration.
    fn corruption_detected(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// MemtableSeekableIter
// ---------------------------------------------------------------------------

/// Seekable iterator over an `Arc<Memtable>`.
///
/// On each `seek()`, collects entries from the seek position into an owned
/// `Vec`. This is the correct Rust approach — crossbeam SkipMap's `range()`
/// returns a borrowing iterator that cannot be stored alongside the `Arc`.
/// Cursor-based memtable iterator. Fetches one entry at a time from the
/// skiplist instead of collecting the entire range into a Vec. This avoids
/// cloning thousands of 1KB values when only a few are needed (e.g., scan
/// with limit=10).
pub struct MemtableSeekableIter {
    memtable: Arc<Memtable>,
    prefix_bytes: Vec<u8>,
    current: Option<(InternalKey, MemtableEntry)>,
}

impl MemtableSeekableIter {
    /// Create a new memtable iterator. Call `seek()` before iterating.
    pub fn new(memtable: Arc<Memtable>, prefix: Key) -> Self {
        let prefix_bytes = encode_typed_key_prefix(&prefix);
        Self {
            memtable,
            prefix_bytes,
            current: None,
        }
    }

    /// Fetch the first entry >= `seek_ik_bytes` within the prefix range.
    fn fetch_first(&self, seek_ik_bytes: &[u8]) -> Option<(InternalKey, MemtableEntry)> {
        self.memtable
            .iter_range_raw(seek_ik_bytes, &self.prefix_bytes)
            .next()
    }
}

impl SeekableIterator for MemtableSeekableIter {
    fn seek(&mut self, target: &[u8]) {
        self.current = self.fetch_first(target);
    }

    fn advance(&mut self) -> bool {
        if let Some((ref ik, _)) = self.current {
            // Advance past the current entry by seeking to the byte after it.
            // InternalKey bytes are ordered so appending \0 gives the next position.
            let mut next = ik.as_bytes().to_vec();
            next.push(0);
            self.current = self.fetch_first(&next);
        }
        self.current.is_some()
    }

    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn current_key(&self) -> &InternalKey {
        &self.current.as_ref().unwrap().0
    }

    fn current_entry(&self) -> &MemtableEntry {
        &self.current.as_ref().unwrap().1
    }
}

// ---------------------------------------------------------------------------
// SegmentSeekableIter
// ---------------------------------------------------------------------------

/// Seekable iterator over a single segment (L0 file).
///
/// Wraps `OwnedSegmentIter` and converts `SegmentEntry` → `MemtableEntry`
/// at the boundary. On `seek()`, repositions via the in-memory index
/// (O(log B) binary search, no I/O unless the target block isn't cached).
pub struct SegmentSeekableIter {
    inner: OwnedSegmentIter,
    current: Option<(InternalKey, MemtableEntry)>,
    corruption_flag: Arc<AtomicBool>,
}

impl SegmentSeekableIter {
    /// Create from an `OwnedSegmentIter` with prefix filter and corruption flag.
    pub fn new(
        segment: Arc<crate::segment::KVSegment>,
        prefix_bytes: Vec<u8>,
        corruption_flag: Arc<AtomicBool>,
    ) -> Self {
        let inner = OwnedSegmentIter::new(segment)
            .with_prefix_filter(prefix_bytes)
            .with_corruption_flag(Arc::clone(&corruption_flag));
        Self {
            inner,
            current: None,
            corruption_flag,
        }
    }

    fn load_current(&mut self) {
        self.current = self
            .inner
            .next()
            .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)));
    }
}

impl SeekableIterator for SegmentSeekableIter {
    fn seek(&mut self, target: &[u8]) {
        if self.inner.segment().index_is_empty() {
            self.current = None;
            return;
        }
        let (partition_idx, block_within_partition) =
            self.inner.segment().index_seek_position(target);
        self.inner
            .reset_position(partition_idx, block_within_partition);
        self.load_current();
    }

    fn advance(&mut self) -> bool {
        self.load_current();
        self.current.is_some()
    }

    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn current_key(&self) -> &InternalKey {
        &self.current.as_ref().unwrap().0
    }

    fn current_entry(&self) -> &MemtableEntry {
        &self.current.as_ref().unwrap().1
    }

    fn corruption_detected(&self) -> bool {
        self.corruption_flag
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// LevelSeekableIter
// ---------------------------------------------------------------------------

/// Seekable iterator over a sorted, non-overlapping level (L1+).
///
/// Wraps `LevelSegmentIter` with RocksDB's same-file optimization:
/// before binary-searching the file list, checks if the target falls
/// within the current file's key range.
pub struct LevelSeekableIter {
    inner: LevelSegmentIter,
    current: Option<(InternalKey, MemtableEntry)>,
    corruption_flag: Arc<AtomicBool>,
}

impl LevelSeekableIter {
    /// Create a new level-seekable iterator.
    pub fn new(
        segments: Vec<Arc<crate::segment::KVSegment>>,
        prefix_bytes: Vec<u8>,
        corruption_flag: Arc<AtomicBool>,
    ) -> Self {
        let inner = LevelSegmentIter::new(segments, prefix_bytes, Arc::clone(&corruption_flag));
        Self {
            inner,
            current: None,
            corruption_flag,
        }
    }

    fn load_current(&mut self) {
        self.current = self
            .inner
            .next()
            .map(|(ik, se)| (ik, segment_entry_to_memtable_entry(se)));
    }
}

impl SeekableIterator for LevelSeekableIter {
    fn seek(&mut self, target: &[u8]) {
        self.inner.seek_bytes(target);
        self.load_current();
    }

    fn advance(&mut self) -> bool {
        self.load_current();
        self.current.is_some()
    }

    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn current_key(&self) -> &InternalKey {
        &self.current.as_ref().unwrap().0
    }

    fn current_entry(&self) -> &MemtableEntry {
        &self.current.as_ref().unwrap().1
    }

    fn corruption_detected(&self) -> bool {
        self.corruption_flag
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// MergeSeekableIter
// ---------------------------------------------------------------------------

/// K-way merge over seekable children — equivalent to RocksDB's
/// `MergingIterator`.
///
/// Owns `Vec<Box<dyn SeekableIterator>>`. On `seek()`: clears heap,
/// re-seeks each child in place, re-heapifies valid children.
/// Children persist across seeks — never destroyed and rebuilt.
pub struct MergeSeekableIter {
    children: Vec<Box<dyn SeekableIterator>>,
    heap: BinaryHeap<MergeHeapEntry>,
    /// Cached current entry (from the last popped heap item).
    current_key: Option<InternalKey>,
    current_entry: Option<MemtableEntry>,
}

/// Heap entry: stores child index and cached key for comparison.
struct MergeHeapEntry {
    key: InternalKey,
    source_idx: usize,
}

impl PartialEq for MergeHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_idx == other.source_idx
    }
}

impl Eq for MergeHeapEntry {}

impl PartialOrd for MergeHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is max-heap; reverse for min-heap.
        // Primary: ascending key. Tie-break: lower source_idx wins (newer).
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.source_idx.cmp(&self.source_idx))
    }
}

impl MergeSeekableIter {
    /// Create a merge iterator over the given seekable children.
    ///
    /// Children are ordered by priority: index 0 is newest (active memtable),
    /// higher indices are older. Call `seek()` before iterating.
    pub fn new(children: Vec<Box<dyn SeekableIterator>>) -> Self {
        let capacity = children.len();
        Self {
            children,
            heap: BinaryHeap::with_capacity(capacity),
            current_key: None,
            current_entry: None,
        }
    }

    /// Rebuild the heap from all currently-valid children.
    fn rebuild_heap(&mut self) {
        self.heap.clear();
        for (i, child) in self.children.iter().enumerate() {
            if child.valid() {
                self.heap.push(MergeHeapEntry {
                    key: child.current_key().clone(),
                    source_idx: i,
                });
            }
        }
    }

    /// Pop the minimum entry from the heap and advance that child.
    fn pop_and_advance(&mut self) -> bool {
        let entry = match self.heap.pop() {
            Some(e) => e,
            None => {
                self.current_key = None;
                self.current_entry = None;
                return false;
            }
        };

        let child = &mut self.children[entry.source_idx];
        // Cache the current key/entry before advancing
        self.current_key = Some(child.current_key().clone());
        self.current_entry = Some(child.current_entry().clone());

        // Advance the child and re-push if still valid
        child.advance();
        if child.valid() {
            self.heap.push(MergeHeapEntry {
                key: child.current_key().clone(),
                source_idx: entry.source_idx,
            });
        }
        true
    }
}

impl SeekableIterator for MergeSeekableIter {
    fn seek(&mut self, target: &[u8]) {
        // Re-seek each child in place (RocksDB MergingIterator::SeekImpl)
        for child in &mut self.children {
            child.seek(target);
        }
        self.rebuild_heap();
        // Position at first entry
        self.pop_and_advance();
    }

    fn advance(&mut self) -> bool {
        self.pop_and_advance()
    }

    fn valid(&self) -> bool {
        self.current_key.is_some()
    }

    fn current_key(&self) -> &InternalKey {
        self.current_key.as_ref().unwrap()
    }

    fn current_entry(&self) -> &MemtableEntry {
        self.current_entry.as_ref().unwrap()
    }

    fn corruption_detected(&self) -> bool {
        self.children.iter().any(|c| c.corruption_detected())
    }
}

// ---------------------------------------------------------------------------
// MvccSeekableIter
// ---------------------------------------------------------------------------

/// MVCC deduplication over a [`MergeSeekableIter`] — equivalent to
/// RocksDB's `DBIter::FindNextUserEntry`.
///
/// For each logical key (identified by `typed_key_prefix`), emits only
/// the first entry with `commit_id ≤ max_version`. Remaining versions
/// of the same key are skipped.
pub struct MvccSeekableIter {
    inner: MergeSeekableIter,
    max_version: CommitVersion,
    last_emitted_prefix: Option<Vec<u8>>,
    current_key: Option<InternalKey>,
    current_entry: Option<MemtableEntry>,
}

impl MvccSeekableIter {
    /// Create a new MVCC iterator with the given snapshot version.
    pub fn new(inner: MergeSeekableIter, max_version: CommitVersion) -> Self {
        Self {
            inner,
            max_version,
            last_emitted_prefix: None,
            current_key: None,
            current_entry: None,
        }
    }

    /// Advance inner until we find a visible, non-duplicate entry.
    fn find_next_visible(&mut self) -> bool {
        loop {
            if !self.inner.valid() {
                self.current_key = None;
                self.current_entry = None;
                return false;
            }

            let ik = self.inner.current_key();
            let prefix = ik.typed_key_prefix().to_vec();

            // Skip remaining versions of a key we already emitted
            if let Some(ref last) = self.last_emitted_prefix {
                if *last == prefix {
                    self.inner.advance();
                    continue;
                }
            }

            // MVCC visibility: only emit if commit_id ≤ max_version
            if ik.commit_id() <= self.max_version {
                self.last_emitted_prefix = Some(prefix);
                self.current_key = Some(ik.clone());
                self.current_entry = Some(self.inner.current_entry().clone());
                self.inner.advance();
                return true;
            }

            // commit_id > max_version: skip, try next (may be older version)
            self.inner.advance();
        }
    }
}

impl SeekableIterator for MvccSeekableIter {
    fn seek(&mut self, target: &[u8]) {
        self.inner.seek(target);
        self.last_emitted_prefix = None;
        self.find_next_visible();
    }

    fn advance(&mut self) -> bool {
        self.find_next_visible()
    }

    fn valid(&self) -> bool {
        self.current_key.is_some()
    }

    fn current_key(&self) -> &InternalKey {
        self.current_key.as_ref().unwrap()
    }

    fn current_entry(&self) -> &MemtableEntry {
        self.current_entry.as_ref().unwrap()
    }

    fn corruption_detected(&self) -> bool {
        self.inner.corruption_detected()
    }
}

// ---------------------------------------------------------------------------
// RewritingSeekableIter (COW branching)
// ---------------------------------------------------------------------------

/// Seekable adapter for COW inherited layers.
///
/// Wraps any `SeekableIterator` with two responsibilities:
/// 1. **Version gate:** skip entries with `commit_id > fork_version`
/// 2. **Branch_id rewrite:** source → child, so MVCC groups
///    own + inherited entries by the same `typed_key_prefix`
pub struct RewritingSeekableIter {
    inner: Box<dyn SeekableIterator>,
    target_branch_id: BranchId,
    source_branch_id: BranchId,
    fork_version: CommitVersion,
    current_key: Option<InternalKey>,
    current_entry: Option<MemtableEntry>,
}

impl RewritingSeekableIter {
    /// Create a new rewriting iterator.
    ///
    /// - `inner`: iterator over source branch segments
    /// - `target_branch_id`: child branch ID (rewrite output TO)
    /// - `source_branch_id`: source branch ID (rewrite seek target TO)
    /// - `fork_version`: maximum visible commit_id from source
    pub fn new(
        inner: Box<dyn SeekableIterator>,
        target_branch_id: BranchId,
        source_branch_id: BranchId,
        fork_version: CommitVersion,
    ) -> Self {
        Self {
            inner,
            target_branch_id,
            source_branch_id,
            fork_version,
            current_key: None,
            current_entry: None,
        }
    }

    fn find_next_visible(&mut self) -> bool {
        loop {
            if !self.inner.valid() {
                self.current_key = None;
                self.current_entry = None;
                return false;
            }

            let ik = self.inner.current_key();
            if ik.commit_id() > self.fork_version {
                self.inner.advance();
                continue;
            }

            let rewritten = ik.with_rewritten_branch_id(&self.target_branch_id);
            self.current_key = Some(rewritten);
            self.current_entry = Some(self.inner.current_entry().clone());
            self.inner.advance();
            return true;
        }
    }
}

impl SeekableIterator for RewritingSeekableIter {
    fn seek(&mut self, target: &[u8]) {
        // Rewrite the branch_id in the target from child → source namespace.
        // The first 16 bytes of InternalKey are the branch_id (UUID).
        if target.len() >= 16 {
            let mut rewritten = target.to_vec();
            rewritten[..16].copy_from_slice(self.source_branch_id.as_bytes());
            self.inner.seek(&rewritten);
        } else {
            self.inner.seek(target);
        }
        self.find_next_visible();
    }

    fn advance(&mut self) -> bool {
        self.find_next_visible()
    }

    fn valid(&self) -> bool {
        self.current_key.is_some()
    }

    fn current_key(&self) -> &InternalKey {
        self.current_key.as_ref().unwrap()
    }

    fn current_entry(&self) -> &MemtableEntry {
        self.current_entry.as_ref().unwrap()
    }

    fn corruption_detected(&self) -> bool {
        self.inner.corruption_detected()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a `SegmentEntry` to a `MemtableEntry` (same as
/// `segmented::segment_entry_to_memtable_entry` but local to avoid
/// circular dependency).
fn segment_entry_to_memtable_entry(se: SegmentEntry) -> MemtableEntry {
    MemtableEntry {
        value: se.value,
        is_tombstone: se.is_tombstone,
        timestamp: strata_core::Timestamp::from_micros(se.timestamp),
        ttl_ms: se.ttl_ms,
        raw_value: se.raw_value,
    }
}
