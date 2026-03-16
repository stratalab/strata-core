//! Concurrent skiplist memtable — bounded in-memory write buffer.
//!
//! The memtable replaces both `FxHashMap<Key, VersionChain>` and
//! `BTreeSet<Key>` from the old `Shard` struct. It stores entries ordered
//! by `InternalKey` (TypedKeyBytes ASC, commit_id DESC), which means:
//!
//! - **Point reads:** seek to `(key, +∞)`, return first entry with commit_id ≤ snapshot
//! - **Prefix scans:** iterate forward from `(prefix, +∞)` — natural merge-iterator input
//! - **Flush:** iterate in order — entries stream directly into segment blocks
//!
//! Uses `crossbeam_skiplist::SkipMap` for lock-free concurrent reads and
//! efficient ordered iteration.

use crate::key_encoding::{encode_typed_key, encode_typed_key_prefix, InternalKey};
use strata_core::types::Key;
use strata_core::value::Value;
use strata_core::{Timestamp, Version, VersionedValue};

use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

// ---------------------------------------------------------------------------
// MemtableEntry — what the skiplist stores as values
// ---------------------------------------------------------------------------

/// A single versioned entry in the memtable.
#[derive(Debug, Clone)]
pub struct MemtableEntry {
    /// The value (or tombstone marker).
    pub value: Value,
    /// Whether this entry is a deletion tombstone.
    pub is_tombstone: bool,
    /// When this entry was written (microseconds since epoch).
    pub timestamp: Timestamp,
    /// Time-to-live in milliseconds (0 = no TTL).
    pub ttl_ms: u64,
}

impl MemtableEntry {
    /// Check if this entry has expired based on its TTL.
    pub fn is_expired(&self) -> bool {
        if self.ttl_ms != 0 {
            let now = Timestamp::now();
            if let Some(age) = now.duration_since(self.timestamp) {
                return age >= Duration::from_millis(self.ttl_ms);
            }
        }
        false
    }

    /// Convert to a `VersionedValue` using the given commit_id.
    pub fn to_versioned(&self, commit_id: u64) -> VersionedValue {
        VersionedValue {
            value: self.value.clone(),
            version: Version::txn(commit_id),
            timestamp: self.timestamp,
        }
    }
}

// ---------------------------------------------------------------------------
// Memtable
// ---------------------------------------------------------------------------

/// Concurrent skiplist memtable with MVCC versioning.
///
/// Entries are ordered by `InternalKey` = `(TypedKeyBytes ASC, commit_id DESC)`.
/// Multiple versions of the same logical key coexist. Reads filter by snapshot
/// commit_id; the first matching entry is the newest visible version.
///
/// Thread safety:
/// - Reads (`get_versioned`, `iter_seek`) are lock-free.
/// - Writes (`put`) are serialized by the per-branch commit lock (external).
/// - `freeze()` prevents further writes.
pub struct Memtable {
    /// Unique identifier for this memtable instance.
    id: u64,
    /// The ordered store.
    map: SkipMap<InternalKey, MemtableEntry>,
    /// Approximate heap usage in bytes (updated on each put).
    approx_bytes: AtomicU64,
    /// Whether this memtable has been frozen (no more writes allowed).
    frozen: AtomicBool,
    /// Highest commit_id seen in any put.
    max_commit: AtomicU64,
    /// Lowest commit_id seen in any put.
    min_commit: AtomicU64,
}

impl Memtable {
    /// Create a new active memtable.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            map: SkipMap::new(),
            approx_bytes: AtomicU64::new(0),
            frozen: AtomicBool::new(false),
            max_commit: AtomicU64::new(0),
            min_commit: AtomicU64::new(u64::MAX),
        }
    }

    /// Memtable ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Insert a versioned entry.
    ///
    /// Convenience method that uses `Timestamp::now()` and no TTL.
    ///
    /// # Panics
    /// Panics if the memtable is frozen.
    pub fn put(&self, key: &Key, commit_id: u64, value: Value, is_tombstone: bool) {
        self.put_entry(
            key,
            commit_id,
            MemtableEntry {
                value,
                is_tombstone,
                timestamp: Timestamp::now(),
                ttl_ms: 0,
            },
        );
    }

    /// Insert a pre-built entry with full control over timestamp and TTL.
    ///
    /// # Panics
    /// Panics if the memtable is frozen.
    pub fn put_entry(&self, key: &Key, commit_id: u64, entry: MemtableEntry) {
        assert!(
            !self.frozen.load(Ordering::Relaxed),
            "cannot write to frozen memtable"
        );

        let ik = InternalKey::encode(key, commit_id);
        let entry_size = ik.as_bytes().len()
            + std::mem::size_of::<MemtableEntry>()
            + entry.value.approximate_size();
        self.map.insert(ik, entry);

        self.approx_bytes
            .fetch_add(entry_size as u64, Ordering::Relaxed);
        self.max_commit.fetch_max(commit_id, Ordering::Relaxed);
        self.min_commit.fetch_min(commit_id, Ordering::Relaxed);
    }

    /// Point read: get the newest visible version of a key at or before `snapshot_commit`.
    ///
    /// Returns `None` if the key doesn't exist or has no version ≤ `snapshot_commit`.
    pub fn get_versioned(&self, key: &Key, snapshot_commit: u64) -> Option<MemtableEntry> {
        // Seek to (key, u64::MAX) — the theoretical newest possible version
        let seek_key = InternalKey::encode(key, u64::MAX);
        let typed_prefix = encode_typed_key(key);

        // Iterate forward from the seek point
        for entry in self.map.range(seek_key..) {
            let ik = entry.key();
            // Stop if we've moved past this logical key
            if ik.typed_key_prefix() != typed_prefix.as_slice() {
                break;
            }
            // Check MVCC visibility
            if ik.commit_id() <= snapshot_commit {
                return Some(entry.value().clone());
            }
        }
        None
    }

    /// Point read returning both the commit_id and entry in a single traversal.
    ///
    /// Used by `SegmentedStore` to avoid a double-lookup race: if we called
    /// `get_versioned` and then `get_all_versions` separately, a concurrent
    /// write could insert between the two calls, making the commit_id stale.
    pub fn get_versioned_with_commit(
        &self,
        key: &Key,
        snapshot_commit: u64,
    ) -> Option<(u64, MemtableEntry)> {
        let seek_key = InternalKey::encode(key, u64::MAX);
        let typed_prefix = encode_typed_key(key);

        for entry in self.map.range(seek_key..) {
            let ik = entry.key();
            if ik.typed_key_prefix() != typed_prefix.as_slice() {
                break;
            }
            if ik.commit_id() <= snapshot_commit {
                return Some((ik.commit_id(), entry.value().clone()));
            }
        }
        None
    }

    /// Get only the latest commit_id for a key (no value clone).
    ///
    /// Used by OCC validation — needs only the version number, not the value.
    pub fn get_version_only(&self, key: &Key) -> Option<u64> {
        let seek_key = InternalKey::encode(key, u64::MAX);
        let typed_prefix = encode_typed_key(key);

        let entry = self.map.range(seek_key..).next()?;
        let ik = entry.key();
        if ik.typed_key_prefix() != typed_prefix.as_slice() {
            return None;
        }
        // The first entry IS the latest version (commit_id descending)
        Some(ik.commit_id())
    }

    /// Get all versions of a key, returned as `(commit_id, MemtableEntry)` pairs.
    ///
    /// Results are in descending commit_id order (newest first), matching the
    /// InternalKey sort order.
    pub fn get_all_versions(&self, key: &Key) -> Vec<(u64, MemtableEntry)> {
        let seek_key = InternalKey::encode(key, u64::MAX);
        let typed_prefix = encode_typed_key(key);

        let mut results = Vec::new();
        for entry in self.map.range(seek_key..) {
            let ik = entry.key();
            if ik.typed_key_prefix() != typed_prefix.as_slice() {
                break;
            }
            results.push((ik.commit_id(), entry.value().clone()));
        }
        results
    }

    /// Iterate entries starting from a prefix (for prefix scans).
    ///
    /// Returns raw entries in InternalKey order. The caller is responsible for
    /// MVCC filtering (dedup by logical key, commit_id ≤ snapshot).
    pub fn iter_prefix<'a>(
        &'a self,
        prefix: &Key,
    ) -> impl Iterator<Item = (InternalKey, MemtableEntry)> + 'a {
        let seek_key = InternalKey::encode(prefix, u64::MAX);
        // Use the prefix-match encoding (no terminator) for starts_with checks
        let match_prefix = encode_typed_key_prefix(prefix);

        self.map
            .range(seek_key..)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .take_while(move |(ik, _)| ik.typed_key_prefix().starts_with(&match_prefix))
    }

    /// Iterate ALL entries in sorted order (for flush to segment).
    pub fn iter_all(&self) -> impl Iterator<Item = (InternalKey, MemtableEntry)> + '_ {
        self.map
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
    }

    /// Freeze this memtable — no further writes allowed.
    pub fn freeze(&self) {
        self.frozen.store(true, Ordering::Release);
    }

    /// Whether this memtable is frozen.
    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Acquire)
    }

    /// Approximate heap usage in bytes.
    pub fn approx_bytes(&self) -> u64 {
        self.approx_bytes.load(Ordering::Relaxed)
    }

    /// Number of entries (all versions).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Highest commit_id seen.
    pub fn max_commit(&self) -> u64 {
        self.max_commit.load(Ordering::Relaxed)
    }

    /// Lowest commit_id seen.
    pub fn min_commit(&self) -> u64 {
        self.min_commit.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Value size estimation
// ---------------------------------------------------------------------------

pub(crate) trait ApproximateSize {
    fn approximate_size(&self) -> usize;
}

impl ApproximateSize for Value {
    fn approximate_size(&self) -> usize {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) => 8,
            Value::Float(_) => 8,
            Value::String(s) => s.len(),
            Value::Bytes(b) => b.len(),
            Value::Array(a) => a.iter().map(|v| v.approximate_size()).sum::<usize>() + 24,
            Value::Object(m) => {
                m.iter()
                    .map(|(k, v)| k.len() + v.approximate_size())
                    .sum::<usize>()
                    + 48
            }
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
    use strata_core::types::{BranchId, Namespace, TypeTag};

    fn branch() -> BranchId {
        BranchId::from_bytes([1; 16])
    }

    fn key(user_key: &str) -> Key {
        let ns = Arc::new(Namespace::new(branch(), "default".to_string()));
        Key::new(ns, TypeTag::KV, user_key.as_bytes().to_vec())
    }

    fn key_typed(type_tag: TypeTag, user_key: &str) -> Key {
        let ns = Arc::new(Namespace::new(branch(), "default".to_string()));
        Key::new(ns, type_tag, user_key.as_bytes().to_vec())
    }

    // ===== Basic CRUD =====

    #[test]
    fn put_then_get_returns_value() {
        let mt = Memtable::new(0);
        mt.put(&key("k1"), 1, Value::Int(42), false);
        let result = mt.get_versioned(&key("k1"), u64::MAX);
        assert_eq!(result.unwrap().value, Value::Int(42));
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let mt = Memtable::new(0);
        assert!(mt.get_versioned(&key("k1"), u64::MAX).is_none());
    }

    #[test]
    fn put_tombstone_then_get_returns_tombstone() {
        let mt = Memtable::new(0);
        mt.put(&key("k1"), 1, Value::Null, false);
        mt.put(&key("k1"), 2, Value::Null, true);
        let result = mt.get_versioned(&key("k1"), u64::MAX);
        assert!(result.unwrap().is_tombstone);
    }

    // ===== MVCC Versioning =====

    #[test]
    fn get_versioned_returns_correct_version() {
        let mt = Memtable::new(0);
        mt.put(&key("k1"), 1, Value::Int(10), false);
        mt.put(&key("k1"), 2, Value::Int(20), false);
        mt.put(&key("k1"), 3, Value::Int(30), false);

        assert_eq!(
            mt.get_versioned(&key("k1"), 3).unwrap().value,
            Value::Int(30)
        );
        assert_eq!(
            mt.get_versioned(&key("k1"), 2).unwrap().value,
            Value::Int(20)
        );
        assert_eq!(
            mt.get_versioned(&key("k1"), 1).unwrap().value,
            Value::Int(10)
        );
        assert!(mt.get_versioned(&key("k1"), 0).is_none());
    }

    #[test]
    fn get_version_only_returns_latest_commit() {
        let mt = Memtable::new(0);
        mt.put(&key("k1"), 5, Value::Int(50), false);
        mt.put(&key("k1"), 10, Value::Int(100), false);
        assert_eq!(mt.get_version_only(&key("k1")), Some(10));
    }

    #[test]
    fn get_version_only_nonexistent_returns_none() {
        let mt = Memtable::new(0);
        assert_eq!(mt.get_version_only(&key("k1")), None);
    }

    #[test]
    fn tombstone_at_snapshot_hides_older_versions() {
        let mt = Memtable::new(0);
        mt.put(&key("k1"), 1, Value::Int(10), false);
        mt.put(&key("k1"), 2, Value::Null, true);
        mt.put(&key("k1"), 3, Value::Int(30), false);

        assert!(mt.get_versioned(&key("k1"), 2).unwrap().is_tombstone);
        assert_eq!(
            mt.get_versioned(&key("k1"), 3).unwrap().value,
            Value::Int(30)
        );
        assert_eq!(
            mt.get_versioned(&key("k1"), 1).unwrap().value,
            Value::Int(10)
        );
    }

    // ===== Prefix Scan =====

    #[test]
    fn prefix_scan_returns_matching_keys() {
        let mt = Memtable::new(0);
        mt.put(&key("user:1"), 1, Value::Int(1), false);
        mt.put(&key("user:2"), 1, Value::Int(2), false);
        mt.put(&key("order:1"), 1, Value::Int(100), false);

        let prefix = key("user:");
        let results: Vec<_> = mt.iter_prefix(&prefix).collect();
        // Should only match user:1 and user:2
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn prefix_scan_includes_multiple_versions() {
        let mt = Memtable::new(0);
        mt.put(&key("k1"), 1, Value::Int(10), false);
        mt.put(&key("k1"), 3, Value::Int(30), false);
        mt.put(&key("k2"), 2, Value::Int(20), false);

        let prefix = key("k");
        let results: Vec<_> = mt.iter_prefix(&prefix).collect();
        // 2 versions of k1 + 1 version of k2 = 3 entries
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn prefix_scan_empty_prefix_returns_all_same_type() {
        let mt = Memtable::new(0);
        mt.put(&key("a"), 1, Value::Int(1), false);
        mt.put(&key("b"), 1, Value::Int(2), false);
        mt.put(&key("c"), 1, Value::Int(3), false);

        // Empty user_key prefix matches all KV keys in the same namespace
        let prefix = key("");
        let results: Vec<_> = mt.iter_prefix(&prefix).collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn prefix_scan_does_not_cross_type_tags() {
        let mt = Memtable::new(0);
        mt.put(&key_typed(TypeTag::KV, "k1"), 1, Value::Int(1), false);
        mt.put(&key_typed(TypeTag::Event, "k1"), 1, Value::Int(2), false);

        // Prefix scan for KV type should not return Event entries
        let prefix = key_typed(TypeTag::KV, "k");
        let results: Vec<_> = mt.iter_prefix(&prefix).collect();
        assert_eq!(results.len(), 1);
    }

    // ===== Freeze =====

    #[test]
    fn frozen_memtable_is_readable() {
        let mt = Memtable::new(0);
        mt.put(&key("k1"), 1, Value::Int(42), false);
        mt.freeze();
        assert!(mt.is_frozen());
        assert_eq!(
            mt.get_versioned(&key("k1"), u64::MAX).unwrap().value,
            Value::Int(42)
        );
    }

    #[test]
    #[should_panic(expected = "cannot write to frozen memtable")]
    fn frozen_memtable_rejects_writes() {
        let mt = Memtable::new(0);
        mt.freeze();
        mt.put(&key("k1"), 1, Value::Int(42), false);
    }

    #[test]
    fn frozen_memtable_iter_all_is_sorted() {
        let mt = Memtable::new(0);
        mt.put(&key("c"), 1, Value::Int(3), false);
        mt.put(&key("a"), 1, Value::Int(1), false);
        mt.put(&key("b"), 1, Value::Int(2), false);
        mt.freeze();

        let entries: Vec<_> = mt.iter_all().collect();
        assert_eq!(entries.len(), 3);
        // Must be in InternalKey order: a < b < c
        assert!(entries[0].0 < entries[1].0);
        assert!(entries[1].0 < entries[2].0);
    }

    // ===== Memory accounting =====

    #[test]
    fn approx_bytes_increases_on_put() {
        let mt = Memtable::new(0);
        assert_eq!(mt.approx_bytes(), 0);
        mt.put(&key("k1"), 1, Value::Int(42), false);
        assert!(mt.approx_bytes() > 0);
    }

    #[test]
    fn len_tracks_entry_count() {
        let mt = Memtable::new(0);
        assert_eq!(mt.len(), 0);
        assert!(mt.is_empty());

        mt.put(&key("k1"), 1, Value::Int(1), false);
        assert_eq!(mt.len(), 1);

        mt.put(&key("k1"), 2, Value::Int(2), false);
        assert_eq!(mt.len(), 2); // both versions stored

        mt.put(&key("k2"), 1, Value::Int(3), false);
        assert_eq!(mt.len(), 3);
    }

    // ===== Commit tracking =====

    #[test]
    fn commit_range_tracking() {
        let mt = Memtable::new(0);
        mt.put(&key("k1"), 5, Value::Int(1), false);
        mt.put(&key("k2"), 10, Value::Int(2), false);
        mt.put(&key("k3"), 3, Value::Int(3), false);
        assert_eq!(mt.max_commit(), 10);
        assert_eq!(mt.min_commit(), 3);
    }

    // ===== Concurrent reads =====

    #[test]
    fn concurrent_reads_are_safe() {
        use std::sync::Arc;
        let mt = Arc::new(Memtable::new(0));

        // Insert some data
        for i in 0..100u64 {
            mt.put(&key(&format!("k{}", i)), i, Value::Int(i as i64), false);
        }

        // Spawn readers
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let mt = Arc::clone(&mt);
                std::thread::spawn(move || {
                    for i in 0..100u64 {
                        let result = mt.get_versioned(&key(&format!("k{}", i)), u64::MAX);
                        assert!(result.is_some());
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
