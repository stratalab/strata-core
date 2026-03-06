//! Sharded storage performance
//!
//! Replaces RwLock + BTreeMap with DashMap + HashMap.
//! Lock-free reads, sharded writes, O(1) lookups.
//!
//! # Design
//!
//! - DashMap: 16-way sharded by default, lock-free reads
//! - FxHashMap: O(1) lookups, fast non-crypto hash
//! - Per-BranchId: Natural agent partitioning, no cross-branch contention
//!
//! # Performance Targets
//!
//! - get(): Lock-free via DashMap
//! - put(): Only locks target shard
//! - Snapshot acquisition: < 500ns
//! - Different branches: Never contend
//!
//! # Storage vs Contract Types
//!
//! - `StoredValue`: Internal storage type that includes TTL (storage concern)
//! - `VersionedValue`: Contract type returned to callers (no TTL)
//!
//! # Version Handling
//!
//! The storage layer uses raw `u64` for version comparisons because:
//! 1. All versions in storage are `Version::Txn` variants (transaction versions)
//! 2. Raw u64 comparison is correct for same-variant versions
//! 3. `Version` only supports same-variant comparison via `PartialOrd`
//!    (cross-variant returns `None`), but storage only uses same-variant versions
//! 4. Performance: Avoiding enum matching on every comparison

use dashmap::DashMap;
use rustc_hash::FxHashMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use strata_core::traits::WriteMode;
use strata_core::types::{BranchId, Key};
use strata_core::{StrataError, StrataResult, Timestamp, Version, VersionedValue};

use crate::stored_value::StoredValue;
use crate::ttl::TTLIndex;

/// Internal storage for version chains — optimized for the common single-version case.
///
/// Most keys (especially graph edges) only ever have one version. `Single` stores
/// the value inline without any heap allocation for the container, saving ~270 bytes
/// per entry compared to `VecDeque::with_capacity(4)`.
#[derive(Debug, Clone)]
enum VersionStorage {
    /// Single version stored inline — no heap allocation for the container
    Single(StoredValue),
    /// Multiple versions stored newest-first in a VecDeque
    Multi(VecDeque<StoredValue>),
}

/// Per-branch shard containing branch's data
///
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
    /// - `WriteMode::Replace`: overwrites — drops all old versions (no history)
    #[inline]
    pub fn put(&mut self, value: StoredValue, mode: WriteMode) {
        match mode {
            WriteMode::Append => self.push(value),
            WriteMode::Replace => self.replace_latest(value),
        }
    }

    /// Add a new version (must be newer than existing versions)
    ///
    /// Promotes Single → Multi on second version, then O(1) push_front.
    #[inline]
    pub fn push(&mut self, value: StoredValue) {
        match &mut self.versions {
            VersionStorage::Single(_) => {
                // Promote to Multi: take the existing single value, create VecDeque
                let old =
                    std::mem::replace(&mut self.versions, VersionStorage::Multi(VecDeque::new()));
                match old {
                    VersionStorage::Single(old_value) => {
                        let mut deque = VecDeque::with_capacity(2);
                        deque.push_back(old_value);
                        deque.push_front(value);
                        self.versions = VersionStorage::Multi(deque);
                    }
                    VersionStorage::Multi(_) => {
                        unreachable!("matched Single but mem::replace returned Multi")
                    }
                }
            }
            VersionStorage::Multi(deque) => {
                deque.push_front(value);
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

/// Each BranchId gets its own shard with an FxHashMap for O(1) lookups.
/// This ensures different branches never contend with each other.
///
/// Uses VersionChain for MVCC - multiple versions per key for snapshot isolation.
///
/// # Memory Optimization
///
/// Both `data` and `ordered_keys` store `Arc<Key>` so the BTreeSet shares
/// Key allocations with the HashMap instead of deep-cloning every Key.
#[derive(Debug)]
pub struct Shard {
    /// HashMap with FxHash for O(1) lookups, storing version chains
    pub(crate) data: FxHashMap<Arc<Key>, VersionChain>,
    /// Sorted index of all keys for O(log n + k) prefix scans.
    /// Built lazily on first scan request, then maintained incrementally.
    pub(crate) ordered_keys: Option<BTreeSet<Arc<Key>>>,
    /// TTL index for efficient expiration cleanup.
    /// Maps expiry timestamps to keys for O(expired) cleanup.
    pub(crate) ttl_index: TTLIndex,
}

impl Shard {
    /// Create a new empty shard
    pub fn new() -> Self {
        Self {
            data: FxHashMap::default(),
            ordered_keys: None,
            ttl_index: TTLIndex::new(),
        }
    }

    /// Create a shard with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: FxHashMap::with_capacity_and_hasher(capacity, Default::default()),
            ordered_keys: None,
            ttl_index: TTLIndex::new(),
        }
    }

    /// Rebuild `ordered_keys` from `data.keys()` if not yet built (None).
    /// Once built, the index is maintained incrementally by `put()` and
    /// `commit_batch()`, so this only triggers on the very first scan.
    pub(crate) fn ensure_ordered_keys(&mut self) {
        if self.ordered_keys.is_none() {
            self.ordered_keys = Some(
                self.data
                    .iter()
                    .filter(|(_, chain)| !chain.is_dead())
                    .map(|(k, _)| Arc::clone(k))
                    .collect(),
            );
        }
    }

    /// Get number of keys in this shard
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if shard is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Default for Shard {
    fn default() -> Self {
        Self::new()
    }
}

/// Memory usage statistics for the entire storage layer.
#[derive(Debug, Clone)]
pub struct StorageMemoryStats {
    /// Total number of branches
    pub total_branches: usize,
    /// Total number of entries across all branches
    pub total_entries: usize,
    /// Estimated total memory usage in bytes
    pub estimated_bytes: usize,
    /// Per-branch breakdown
    pub per_branch: Vec<BranchMemoryStats>,
}

/// Memory usage statistics for a single branch.
#[derive(Debug, Clone)]
pub struct BranchMemoryStats {
    /// Branch identifier
    pub branch_id: BranchId,
    /// Number of entries in this branch
    pub entry_count: usize,
    /// Whether the BTreeSet ordered-key index has been built
    pub has_btree_index: bool,
    /// Estimated memory usage in bytes
    pub estimated_bytes: usize,
}

/// Sharded storage - DashMap by BranchId, HashMap within
///
/// # Design
///
/// - DashMap: 16-way sharded by default, lock-free reads
/// - FxHashMap: O(1) lookups, fast non-crypto hash
/// - Per-BranchId: Natural agent partitioning, no cross-branch contention
///
/// # Thread Safety
///
/// All operations are thread-safe:
/// - get(): Lock-free read via DashMap
/// - put(): Only locks the target branch's shard
/// - Different branches never contend
///
/// # Example
///
/// ```text
/// use strata_storage::ShardedStore;
/// use std::sync::Arc;
///
/// let store = Arc::new(ShardedStore::new());
/// let snapshot = store.snapshot();
/// ```
pub struct ShardedStore {
    /// Per-branch shards using DashMap
    shards: DashMap<BranchId, Shard>,
    /// Global version for snapshots
    version: AtomicU64,
    /// Maximum number of branches allowed (0 = unlimited)
    max_branches: AtomicUsize,
}

impl ShardedStore {
    /// Create new sharded store
    pub fn new() -> Self {
        Self {
            shards: DashMap::new(),
            version: AtomicU64::new(0),
            max_branches: AtomicUsize::new(0),
        }
    }

    /// Create with expected number of branches
    pub fn with_capacity(num_branches: usize) -> Self {
        Self {
            shards: DashMap::with_capacity(num_branches),
            version: AtomicU64::new(0),
            max_branches: AtomicUsize::new(0),
        }
    }

    /// Create with a branch count limit
    pub fn with_limits(max_branches: usize) -> Self {
        Self {
            shards: DashMap::new(),
            version: AtomicU64::new(0),
            max_branches: AtomicUsize::new(max_branches),
        }
    }

    /// Set the maximum number of branches allowed (0 = unlimited).
    ///
    /// Callable after recovery without `&mut self`.
    pub fn set_max_branches(&self, max: usize) {
        self.max_branches.store(max, Ordering::Relaxed);
    }

    /// Get current version
    #[inline]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Increment version and return new value.
    ///
    /// Uses wrapping arithmetic intentionally — this is a storage-local
    /// counter (not the global MVCC version in `TransactionManager`).
    /// Wrapping at u64::MAX is safe here because the counter is only
    /// used for snapshot ordering within a single `ShardedStore` instance.
    /// In practice, overflow is extremely unlikely (~584 years at 1B versions/sec).
    #[inline]
    pub fn next_version(&self) -> u64 {
        // Use fetch_update with wrapping_add to prevent overflow panic in debug mode
        self.version
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                Some(v.wrapping_add(1))
            })
            .unwrap() // fetch_update with Some always succeeds
            .wrapping_add(1)
    }

    /// Set version (used during recovery)
    pub fn set_version(&self, version: u64) {
        self.version.store(version, Ordering::Release);
    }

    /// Get number of shards (branches)
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Return `(entry_count, has_ordered_keys)` for a branch shard.
    ///
    /// Used for profiling graph OOM (#1297) — confirms the FxHashMap has the
    /// expected entry count and whether the BTreeSet index has been built.
    pub fn shard_stats(&self, branch_id: &BranchId) -> Option<(usize, bool)> {
        self.shards
            .get(branch_id)
            .map(|s| (s.data.len(), s.ordered_keys.is_some()))
    }

    /// Get (entry_count, total_version_count, btree_built) for a branch.
    pub fn shard_stats_detailed(&self, branch_id: &BranchId) -> Option<(usize, usize, bool)> {
        self.shards.get(branch_id).map(|s| {
            let total_versions: usize = s.data.values().map(|c| c.version_count()).sum();
            (s.data.len(), total_versions, s.ordered_keys.is_some())
        })
    }

    /// Check if a branch exists
    pub fn has_branch(&self, branch_id: &BranchId) -> bool {
        self.shards.contains_key(branch_id)
    }

    /// Get total number of entries across all shards
    pub fn total_entries(&self) -> usize {
        self.shards.iter().map(|entry| entry.value().len()).sum()
    }

    /// Compute memory usage statistics across all branches.
    ///
    /// Estimates ~120 bytes per entry (56 StoredValue + ~64 Key/Arc overhead)
    /// plus ~64 bytes per BTreeSet entry when the ordered-key index is built.
    /// This is an O(branches) scan, not on any hot path.
    pub fn memory_stats(&self) -> StorageMemoryStats {
        // Per-entry estimate: StoredValue (56 bytes) + Key + Arc overhead (~64 bytes)
        const BYTES_PER_ENTRY: usize = 120;
        // BTreeSet node overhead per entry
        const BYTES_PER_BTREE_ENTRY: usize = 64;

        let mut total_entries = 0;
        let mut total_bytes = 0;
        let mut per_branch = Vec::with_capacity(self.shards.len());

        for entry in self.shards.iter() {
            let branch_id = *entry.key();
            let shard = entry.value();
            let count = shard.data.len();
            let has_btree = shard.ordered_keys.is_some();

            let mut branch_bytes = count * BYTES_PER_ENTRY;
            if has_btree {
                branch_bytes += count * BYTES_PER_BTREE_ENTRY;
            }

            total_entries += count;
            total_bytes += branch_bytes;

            per_branch.push(BranchMemoryStats {
                branch_id,
                entry_count: count,
                has_btree_index: has_btree,
                estimated_bytes: branch_bytes,
            });
        }

        StorageMemoryStats {
            total_branches: per_branch.len(),
            total_entries,
            estimated_bytes: total_bytes,
            per_branch,
        }
    }

    // ========================================================================
    // Get/Put/Delete Operations
    // ========================================================================

    // NOTE: `get()` is provided by the Storage trait implementation,
    // which includes proper TTL expiration checks.
    // Use `Storage::get()` trait method instead of an inherent method
    // to ensure consistent behavior across all callers.

    /// Check if adding a new branch would exceed the configured limit.
    ///
    /// Fast path: `max == 0` (unlimited) or branch already exists.
    #[inline]
    fn ensure_branch_limit(&self, branch_id: BranchId) -> StrataResult<()> {
        let max = self.max_branches.load(Ordering::Relaxed);
        if max == 0 || self.shards.contains_key(&branch_id) {
            return Ok(());
        }
        if self.shards.len() >= max {
            return Err(StrataError::capacity_exceeded(
                "branches",
                max,
                self.shards.len() + 1,
            ));
        }
        Ok(())
    }

    /// Put a value for a key (adds to version chain for MVCC)
    ///
    /// Sharded write - only locks this branch's shard.
    /// Other branches can read/write concurrently without contention.
    ///
    /// # Arguments
    ///
    /// * `key` - Key to store (contains BranchId)
    /// * `value` - StoredValue to store (includes TTL)
    ///
    /// # Performance
    ///
    /// - O(1) insert via FxHashMap
    /// - Only locks the target branch's shard
    #[inline]
    pub fn put(&self, key: Key, value: StoredValue, mode: WriteMode) -> StrataResult<()> {
        let branch_id = key.namespace.branch_id;
        let max = self.max_branches.load(Ordering::Relaxed);

        // Snapshot len() before entry() to avoid deadlock: DashMap::len()
        // acquires read locks on all shards, which would deadlock if we
        // already hold the write lock via entry().
        let current_len = if max > 0 { self.shards.len() } else { 0 };

        let mut shard = match self.shards.entry(branch_id) {
            dashmap::mapref::entry::Entry::Occupied(e) => e.into_ref(),
            dashmap::mapref::entry::Entry::Vacant(e) => {
                // Only check limit for NEW branches (rare path)
                if max > 0 && current_len >= max {
                    return Err(StrataError::capacity_exceeded(
                        "branches",
                        max,
                        current_len + 1,
                    ));
                }
                e.insert(Shard::default())
            }
        };

        let new_expiry = value.expiry_timestamp();

        // Split borrows on Shard to avoid a redundant hash lookup.
        // DerefMut into &mut Shard lets the borrow checker see data and
        // ttl_index as independent fields.
        let shard = &mut *shard;

        if let Some(chain) = shard.data.get_mut(&key) {
            // Remove old TTL entry if previous version had one
            let old_expiry = chain.latest().and_then(|sv| sv.expiry_timestamp());
            if let Some(exp) = old_expiry {
                shard.ttl_index.remove(exp, &key);
            }
            chain.put(value, mode);
            // Register new TTL entry
            if let Some(exp) = new_expiry {
                shard.ttl_index.insert(exp, key);
            }
        } else {
            // Clone key for TTL index only when there IS a TTL (rare path)
            let ttl_key = new_expiry.as_ref().map(|_| key.clone());
            // Create new chain — share Arc<Key> between HashMap and BTreeSet
            let arc_key = Arc::new(key);
            if let Some(ref mut btree) = shard.ordered_keys {
                btree.insert(Arc::clone(&arc_key));
            }
            shard.data.insert(arc_key, VersionChain::new(value));
            // Register new TTL entry
            if let Some(exp) = new_expiry {
                shard.ttl_index.insert(exp, ttl_key.unwrap());
            }
        }
        Ok(())
    }

    /// Delete a key by adding a tombstone
    ///
    /// MVCC-safe delete: adds a tombstone (Value::Null) at a new version
    /// instead of removing all versions. This preserves old versions for
    /// snapshots that may still need them.
    ///
    /// # Arguments
    ///
    /// * `key` - Key to delete (contains BranchId)
    ///
    /// # Returns
    /// The previous value if it existed and wasn't already deleted
    #[inline]
    pub fn delete(&self, key: &Key) -> StrataResult<Option<VersionedValue>> {
        let delete_version = self.next_version();

        // Get the previous value before adding tombstone
        let previous = self.shards.get(&key.namespace.branch_id).and_then(|shard| {
            shard.data.get(key).and_then(|chain| {
                chain.latest().and_then(|sv| {
                    if sv.is_tombstone() {
                        None
                    } else {
                        Some(sv.versioned())
                    }
                })
            })
        });

        let tombstone = StoredValue::tombstone(Version::txn(delete_version));
        self.put(key.clone(), tombstone, WriteMode::Append)?;

        Ok(previous)
    }

    /// Delete a key with a specific version (for batched deletes)
    ///
    /// Adds a tombstone at the specified version. Old versions are preserved
    /// for MVCC snapshot isolation. Does not return the previous value —
    /// all production callers discard it.
    #[inline]
    pub fn delete_with_version(&self, key: &Key, version: u64) -> StrataResult<()> {
        let tombstone = StoredValue::tombstone(Version::txn(version));
        self.put(key.clone(), tombstone, WriteMode::Append)?;
        Ok(())
    }

    /// Check if a key exists (excluding tombstones)
    ///
    /// Lock-free check via DashMap read guard.
    /// Returns false for deleted keys (tombstones).
    #[inline]
    pub fn contains(&self, key: &Key) -> bool {
        let branch_id = key.namespace.branch_id;
        self.shards
            .get(&branch_id)
            .map(|shard| {
                shard
                    .data
                    .get(key)
                    .is_some_and(|chain| chain.latest().is_some_and(|sv| !sv.is_tombstone()))
            })
            .unwrap_or(false)
    }

    /// Direct single-key read without transaction overhead.
    ///
    /// Returns the latest committed value, skipping snapshot creation,
    /// pool acquire/release, and coordinator bookkeeping.
    ///
    /// # Safety
    ///
    /// Returns the latest committed value (no snapshot isolation).
    /// Safe for single-key reads where multi-key consistency isn't needed.
    #[inline]
    pub fn get_direct(&self, key: &Key) -> Option<VersionedValue> {
        let branch_id = key.namespace.branch_id;
        self.shards.get(&branch_id).and_then(|shard| {
            shard.data.get(key).and_then(|chain| {
                chain.latest().and_then(|sv| {
                    if !sv.is_expired() && !sv.is_tombstone() {
                        Some(sv.versioned())
                    } else {
                        None
                    }
                })
            })
        })
    }

    /// Direct single-key read returning only the Value (no VersionedValue construction).
    ///
    /// Avoids constructing `Version` enum and `VersionedValue` struct when only
    /// the value is needed. Used by the `KVStore::get()` hot path.
    #[inline]
    pub fn get_value_direct(&self, key: &Key) -> Option<Value> {
        let branch_id = key.namespace.branch_id;
        self.shards.get(&branch_id).and_then(|shard| {
            shard.data.get(key).and_then(|chain| {
                chain.latest().and_then(|sv| {
                    if !sv.is_expired() && !sv.is_tombstone() {
                        Some(sv.value().clone())
                    } else {
                        None
                    }
                })
            })
        })
    }

    /// Apply a batch of writes and deletes atomically
    ///
    /// All operations in the batch are applied with the given version.
    /// Operations are grouped by branch and applied atomically **per branch**
    /// (the shard lock is held for the entire branch batch). However, there is
    /// no cross-branch rollback: if the batch spans branches A and B and A
    /// succeeds before B fails (e.g. branch limit exceeded), A's writes are
    /// already visible. Callers that need cross-branch atomicity should
    /// validate preconditions before calling this method.
    ///
    /// # Arguments
    ///
    /// * `writes` - Key-value pairs to write
    /// * `deletes` - Keys to delete
    /// * `version` - Version to assign to all writes
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All operations applied successfully
    ///
    /// # Performance
    ///
    /// Captures timestamp once per batch instead of per-write to avoid
    /// repeated syscalls. All writes in a transaction share the same timestamp.
    #[allow(clippy::type_complexity)]
    pub fn apply_batch(
        &self,
        writes: Vec<(Key, strata_core::value::Value)>,
        deletes: Vec<Key>,
        version: u64,
    ) -> strata_core::StrataResult<()> {
        use std::sync::atomic::Ordering;

        // Capture timestamp once for entire batch
        let timestamp = Timestamp::now();

        // Group writes and deletes by branch_id to apply atomically per branch.
        // This ensures concurrent readers never see partial transaction state
        // within a branch, since we hold the shard lock for the entire branch batch.
        let mut branch_ops: FxHashMap<BranchId, (Vec<(Key, StoredValue)>, Vec<Key>)> =
            FxHashMap::default();

        for (key, value) in writes {
            let stored = StoredValue::with_timestamp(value, Version::txn(version), timestamp, None);
            branch_ops
                .entry(key.namespace.branch_id)
                .or_insert_with(|| (Vec::new(), Vec::new()))
                .0
                .push((key, stored));
        }

        for key in deletes {
            branch_ops
                .entry(key.namespace.branch_id)
                .or_insert_with(|| (Vec::new(), Vec::new()))
                .1
                .push(key);
        }

        // Pre-check branch limits before applying any writes
        for branch_id in branch_ops.keys() {
            self.ensure_branch_limit(*branch_id)?;
        }

        // Apply atomically per branch (hold shard lock for entire branch batch)
        for (branch_id, (branch_writes, branch_deletes)) in branch_ops {
            let mut shard = self.shards.entry(branch_id).or_default();
            // Split borrows to avoid redundant hash lookups
            let shard = &mut *shard;

            for (key, stored) in branch_writes {
                if let Some(chain) = shard.data.get_mut(&key) {
                    // Clean up old TTL entry if overwriting a key that had TTL
                    let old_expiry = chain.latest().and_then(|sv| sv.expiry_timestamp());
                    if let Some(exp) = old_expiry {
                        shard.ttl_index.remove(exp, &key);
                    }
                    chain.push(stored);
                } else {
                    let arc_key = Arc::new(key);
                    if let Some(ref mut btree) = shard.ordered_keys {
                        btree.insert(Arc::clone(&arc_key));
                    }
                    shard.data.insert(arc_key, VersionChain::new(stored));
                }
            }

            for key in branch_deletes {
                let tombstone = StoredValue::tombstone(Version::txn(version));
                if let Some(chain) = shard.data.get_mut(&key) {
                    // Clean up old TTL entry if deleting a key that had TTL
                    let old_expiry = chain.latest().and_then(|sv| sv.expiry_timestamp());
                    if let Some(exp) = old_expiry {
                        shard.ttl_index.remove(exp, &key);
                    }
                    chain.push(tombstone);
                } else {
                    let arc_key = Arc::new(key);
                    if let Some(ref mut btree) = shard.ordered_keys {
                        btree.insert(Arc::clone(&arc_key));
                    }
                    shard.data.insert(arc_key, VersionChain::new(tombstone));
                }
            }
        }

        // Update global version to be at least this version
        // This ensures subsequent snapshots can see the committed data
        self.version.fetch_max(version, Ordering::AcqRel);

        Ok(())
    }

    /// Get count of entries for a specific branch
    pub fn branch_entry_count(&self, branch_id: &BranchId) -> usize {
        self.shards
            .get(branch_id)
            .map(|shard| shard.len())
            .unwrap_or(0)
    }

    /// Get value at or before the given timestamp.
    /// Returns None if key doesn't exist, has no version at that time, is expired, or is a tombstone.
    pub fn get_at_timestamp(
        &self,
        key: &Key,
        max_timestamp: u64,
    ) -> strata_core::StrataResult<Option<VersionedValue>> {
        let branch_id = key.namespace.branch_id;
        Ok(self.shards.get(&branch_id).and_then(|shard| {
            shard.data.get(key).and_then(|chain| {
                chain.get_at_timestamp(max_timestamp).and_then(|sv| {
                    if !sv.is_expired() && !sv.is_tombstone() {
                        Some(sv.versioned())
                    } else {
                        None
                    }
                })
            })
        }))
    }

    /// Scan keys matching a prefix, returning values at or before the given timestamp.
    pub fn scan_prefix_at_timestamp(
        &self,
        prefix: &Key,
        max_timestamp: u64,
    ) -> strata_core::StrataResult<Vec<(Key, VersionedValue)>> {
        let branch_id = prefix.namespace.branch_id;
        Ok(self
            .shards
            .get_mut(&branch_id)
            .map(|mut shard| {
                shard.ensure_ordered_keys();
                shard
                    .ordered_keys
                    .as_ref()
                    .unwrap()
                    .range::<Key, _>(prefix..)
                    .take_while(|k| k.starts_with(prefix))
                    .filter_map(|k| {
                        shard.data.get(k).and_then(|chain| {
                            chain.get_at_timestamp(max_timestamp).and_then(|sv| {
                                if !sv.is_expired() && !sv.is_tombstone() {
                                    Some(((**k).clone(), sv.versioned()))
                                } else {
                                    None
                                }
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Get the available time range for a branch.
    ///
    /// Scans all keys in the branch shard to find min/max timestamps.
    /// Returns `None` if the branch has no data or doesn't exist.
    ///
    /// Note: Entries with timestamp 0 are excluded. In normal operation,
    /// `Timestamp::now()` always returns values > 0 (microseconds since Unix
    /// epoch). A timestamp of 0 indicates uninitialized or legacy data that
    /// predates timestamp tracking, so it is not meaningful for time-range
    /// queries.
    pub fn time_range(&self, branch_id: BranchId) -> strata_core::StrataResult<Option<(u64, u64)>> {
        Ok(self.shards.get(&branch_id).and_then(|shard| {
            let mut min_ts = u64::MAX;
            let mut max_ts = 0u64;
            for chain in shard.data.values() {
                if let Some(sv) = chain.latest() {
                    let ts: u64 = sv.timestamp().into();
                    if ts > 0 && !sv.is_tombstone() {
                        min_ts = min_ts.min(ts);
                        max_ts = max_ts.max(ts);
                    }
                }
            }
            if max_ts == 0 {
                None
            } else {
                Some((min_ts, max_ts))
            }
        }))
    }

    /// Garbage-collect old versions from all entries for a given branch.
    ///
    /// Calls `VersionChain::gc(min_version)` on each entry in the branch's shard.
    /// Returns the total number of pruned versions.
    pub fn gc_branch(&self, branch_id: BranchId, min_version: u64) -> usize {
        let mut pruned = 0;
        if let Some(mut shard) = self.shards.get_mut(&branch_id) {
            // Collect dead keys during retain — keys whose only remaining
            // version is a tombstone or expired entry after GC.
            let mut dead_keys: Vec<Arc<Key>> = Vec::new();
            // Collect TTL entries to remove after retain releases the data borrow
            let mut ttl_removals: Vec<(Timestamp, Arc<Key>)> = Vec::new();

            shard.data.retain(|key, chain| {
                // Capture expiry before GC potentially changes the chain
                let had_expiry = chain.latest().and_then(|sv| sv.expiry_timestamp());

                pruned += chain.gc(min_version);
                if chain.is_dead() {
                    if let Some(exp) = had_expiry {
                        ttl_removals.push((exp, Arc::clone(key)));
                    }
                    dead_keys.push(Arc::clone(key));
                    return false; // Remove from HashMap
                }
                true
            });

            // Remove TTL entries for dead keys (after retain releases data borrow)
            for (exp, key) in &ttl_removals {
                shard.ttl_index.remove(*exp, key);
            }

            // Remove dead keys from BTreeSet (if it exists)
            if !dead_keys.is_empty() {
                if let Some(ref mut btree) = shard.ordered_keys {
                    for key in &dead_keys {
                        btree.remove(key);
                    }
                }
            }
        }
        pruned
    }

    /// Clean expired entries from TTL indexes across all branches.
    ///
    /// Removes stale timestamp→key mappings from TTL indexes. Does NOT
    /// add tombstones — expired keys are already detected by `is_expired()`
    /// and removed by `gc_branch()` via `is_dead()`.
    ///
    /// Returns the number of expired index entries removed.
    pub fn expire_ttl_keys(&self, now: Timestamp) -> usize {
        let mut total = 0;
        for branch_id in self.branch_ids() {
            if let Some(mut shard) = self.shards.get_mut(&branch_id) {
                total += shard.ttl_index.remove_expired(now);
            }
        }
        total
    }

    // ========================================================================
    // List Operations
    // ========================================================================

    /// List all entries for a branch
    ///
    /// Holds the shard read lock for the entire operation to provide a
    /// consistent snapshot of keys and values.
    ///
    /// # Arguments
    ///
    /// * `branch_id` - The branch to list entries for
    ///
    /// # Returns
    ///
    /// Vector of (Key, VersionedValue) pairs, sorted by key
    pub fn list_branch(&self, branch_id: &BranchId) -> Vec<(Key, VersionedValue)> {
        self.shards
            .get_mut(branch_id)
            .map(|mut shard| {
                shard.ensure_ordered_keys();
                shard
                    .ordered_keys
                    .as_ref()
                    .unwrap()
                    .iter()
                    .filter_map(|k| {
                        shard.data.get(k).and_then(|chain| {
                            chain.latest().and_then(|sv| {
                                if !sv.is_tombstone() {
                                    Some(((**k).clone(), sv.versioned()))
                                } else {
                                    None
                                }
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List entries matching a key prefix
    ///
    /// Returns all entries where `key.starts_with(prefix)`.
    /// The prefix key determines namespace, type_tag, and user_key prefix.
    ///
    /// NOTE: Requires filter + sort, O(n) where n = shard size.
    /// Use sparingly; not for hot path operations.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Prefix key to match (namespace + type_tag + user_key prefix)
    ///
    /// # Returns
    ///
    /// Vector of (Key, VersionedValue) pairs matching prefix, sorted by key
    pub fn list_by_prefix(&self, prefix: &Key) -> Vec<(Key, VersionedValue)> {
        let branch_id = prefix.namespace.branch_id;

        self.shards
            .get_mut(&branch_id)
            .map(|mut shard| {
                shard.ensure_ordered_keys();
                shard
                    .ordered_keys
                    .as_ref()
                    .unwrap()
                    .range::<Key, _>(prefix..)
                    .take_while(|k| k.starts_with(prefix))
                    .filter_map(|k| {
                        shard.data.get(k).and_then(|chain| {
                            chain.latest().and_then(|sv| {
                                if !sv.is_tombstone() {
                                    Some(((**k).clone(), sv.versioned()))
                                } else {
                                    None
                                }
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List entries of a specific type for a branch
    ///
    /// Filters by TypeTag within a branch's shard.
    ///
    /// # Arguments
    ///
    /// * `branch_id` - The branch to query
    /// * `type_tag` - The type to filter by (KV, Event, State, etc.)
    ///
    /// # Returns
    ///
    /// Vector of (Key, VersionedValue) pairs of the specified type, sorted
    pub fn list_by_type(
        &self,
        branch_id: &BranchId,
        type_tag: strata_core::types::TypeTag,
    ) -> Vec<(Key, VersionedValue)> {
        self.shards
            .get_mut(branch_id)
            .map(|mut shard| {
                shard.ensure_ordered_keys();
                shard
                    .ordered_keys
                    .as_ref()
                    .unwrap()
                    .iter()
                    .filter(|k| k.type_tag == type_tag)
                    .filter_map(|k| {
                        shard.data.get(k).and_then(|chain| {
                            chain.latest().and_then(|sv| {
                                if !sv.is_tombstone() {
                                    Some(((**k).clone(), sv.versioned()))
                                } else {
                                    None
                                }
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Count entries of a specific type for a branch (excludes tombstones)
    pub fn count_by_type(
        &self,
        branch_id: &BranchId,
        type_tag: strata_core::types::TypeTag,
    ) -> usize {
        self.shards
            .get_mut(branch_id)
            .map(|mut shard| {
                shard.ensure_ordered_keys();
                shard
                    .ordered_keys
                    .as_ref()
                    .unwrap()
                    .iter()
                    .filter(|k| {
                        k.type_tag == type_tag
                            && shard
                                .data
                                .get(*k)
                                .and_then(|chain| chain.latest())
                                .is_some_and(|sv| !sv.is_tombstone())
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    /// Iterate over all branches
    ///
    /// Returns an iterator over all BranchIds that have data
    pub fn branch_ids(&self) -> Vec<BranchId> {
        self.shards.iter().map(|entry| *entry.key()).collect()
    }

    /// Clear all data for a branch
    ///
    /// Removes the entire shard for the given branch.
    /// Returns true if the branch existed and was removed.
    pub fn clear_branch(&self, branch_id: &BranchId) -> bool {
        self.shards.remove(branch_id).is_some()
    }

    // ========================================================================
    // Snapshot Acquisition
    // ========================================================================

    /// Create a snapshot of the current store state
    ///
    /// FAST PATH: This is O(1) and < 500ns!
    ///
    /// Snapshot acquisition is:
    /// - Allocation-free (Arc reference count bump only)
    /// - Lock-free (atomic version load)
    /// - O(1) (no data structure scanning)
    ///
    /// The snapshot captures the current version and holds an Arc reference
    /// to the store, allowing reads at the captured version point.
    ///
    /// # Performance Contract
    ///
    /// - Must complete in < 500ns (RED FLAG if > 2µs)
    /// - Only operations: Arc::clone + atomic load
    ///
    /// # Example
    ///
    /// ```text
    /// use std::sync::Arc;
    /// use strata_storage::ShardedStore;
    ///
    /// let store = Arc::new(ShardedStore::new());
    /// let snapshot = store.snapshot();
    ///
    /// // Reads through snapshot see store state at snapshot time
    /// let value = snapshot.get(&key);
    /// ```
    #[inline]
    pub fn snapshot(self: &Arc<Self>) -> ShardedSnapshot {
        ShardedSnapshot {
            version: self.version.load(Ordering::Acquire),
            store: Arc::clone(self),
        }
    }

    /// Create a snapshot - API compatibility method
    ///
    /// This method provides API compatibility with `UnifiedStore::create_snapshot()`.
    /// It returns the same `ShardedSnapshot` as `snapshot()` but with a name that
    /// matches the legacy API.
    ///
    /// # Performance
    ///
    /// Same as `snapshot()` - O(1), < 500ns, allocation-free.
    ///
    /// # Example
    ///
    /// ```text
    /// use std::sync::Arc;
    /// use strata_storage::ShardedStore;
    ///
    /// let store = Arc::new(ShardedStore::new());
    /// let snapshot = store.create_snapshot();  // Same as store.snapshot()
    /// ```
    #[inline]
    pub fn create_snapshot(self: &Arc<Self>) -> ShardedSnapshot {
        self.snapshot()
    }
}

impl Default for ShardedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ShardedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedStore")
            .field("shard_count", &self.shard_count())
            .field("version", &self.version())
            .field("total_entries", &self.total_entries())
            .finish()
    }
}

// ============================================================================
// ShardedSnapshot
// ============================================================================

/// Lightweight snapshot view for MVCC reads
///
/// ShardedSnapshot is a thin wrapper that captures:
/// - The version number at snapshot time
/// - An Arc reference to the underlying store
///
/// All reads delegate to `store.get_versioned(key, version)`, which walks
/// the version chain to find the correct value. This provides:
/// - O(1) snapshot creation (just capture version + Arc clone)
/// - O(1) snapshot cloning (derive Clone)
/// - No unbounded memory growth (no per-snapshot cache)
/// - MVCC isolation via the store's version chain
///
/// # Performance
///
/// Snapshot acquisition is O(1) and < 500ns:
/// - Arc::clone: ~20-30ns (atomic increment)
/// - Atomic load: ~1-5ns
/// - Total: well under 500ns
///
/// # Thread Safety
///
/// ShardedSnapshot is Send + Sync since it only holds Arc<ShardedStore>.
/// Multiple snapshots can exist concurrently without blocking.
#[derive(Clone)]
pub struct ShardedSnapshot {
    /// Version captured at snapshot time
    version: u64,
    /// Reference to the underlying store
    store: Arc<ShardedStore>,
}

impl ShardedSnapshot {
    /// Get the snapshot version
    ///
    /// This is the version of the store at the time the snapshot was created.
    #[inline]
    pub fn version(&self) -> u64 {
        self.version
    }

    // NOTE: `get()` is provided by the SnapshotView trait implementation,
    // which includes proper MVCC version filtering and TTL expiration checks.
    // Use `SnapshotView::get()` directly instead of an inherent method.

    /// Check if a key exists at or before the snapshot version
    #[inline]
    pub fn contains(&self, key: &Key) -> bool {
        // Use the SnapshotView trait method for proper version filtering
        use strata_core::traits::SnapshotView;
        SnapshotView::get(self, key).ok().flatten().is_some()
    }

    /// List all entries for a branch at snapshot version
    ///
    /// Returns entries as they existed at the snapshot version,
    /// filtering out expired values and tombstones.
    /// Results are sorted by key (BTreeSet iteration order).
    pub fn list_branch(&self, branch_id: &BranchId) -> Vec<(Key, VersionedValue)> {
        self.store
            .shards
            .get_mut(branch_id)
            .map(|mut shard| {
                shard.ensure_ordered_keys();
                shard
                    .ordered_keys
                    .as_ref()
                    .unwrap()
                    .iter()
                    .filter_map(|k| {
                        shard.data.get(k).and_then(|chain| {
                            chain.get_at_version(self.version).and_then(|sv| {
                                if !sv.is_expired() && !sv.is_tombstone() {
                                    Some(((**k).clone(), sv.versioned()))
                                } else {
                                    None
                                }
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List entries matching a prefix at snapshot version
    ///
    /// Returns entries as they existed at the snapshot version,
    /// filtering out expired values and tombstones.
    /// Uses BTreeSet range scan for O(log n + k) performance.
    pub fn list_by_prefix(&self, prefix: &Key) -> Vec<(Key, VersionedValue)> {
        let branch_id = prefix.namespace.branch_id;
        self.store
            .shards
            .get_mut(&branch_id)
            .map(|mut shard| {
                shard.ensure_ordered_keys();
                shard
                    .ordered_keys
                    .as_ref()
                    .unwrap()
                    .range::<Key, _>(prefix..)
                    .take_while(|k| k.starts_with(prefix))
                    .filter_map(|k| {
                        shard.data.get(k).and_then(|chain| {
                            chain.get_at_version(self.version).and_then(|sv| {
                                if !sv.is_expired() && !sv.is_tombstone() {
                                    Some(((**k).clone(), sv.versioned()))
                                } else {
                                    None
                                }
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List entries of a specific type at snapshot version
    ///
    /// Returns entries as they existed at the snapshot version,
    /// filtering out expired values and tombstones.
    /// Results are sorted by key (BTreeSet iteration order).
    pub fn list_by_type(
        &self,
        branch_id: &BranchId,
        type_tag: strata_core::types::TypeTag,
    ) -> Vec<(Key, VersionedValue)> {
        self.store
            .shards
            .get_mut(branch_id)
            .map(|mut shard| {
                shard.ensure_ordered_keys();
                shard
                    .ordered_keys
                    .as_ref()
                    .unwrap()
                    .iter()
                    .filter(|k| k.type_tag == type_tag)
                    .filter_map(|k| {
                        shard.data.get(k).and_then(|chain| {
                            chain.get_at_version(self.version).and_then(|sv| {
                                if !sv.is_expired() && !sv.is_tombstone() {
                                    Some(((**k).clone(), sv.versioned()))
                                } else {
                                    None
                                }
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get count of entries for a branch at snapshot version
    ///
    /// Counts only entries that existed at the snapshot version
    /// (excludes tombstones and expired values).
    pub fn branch_entry_count(&self, branch_id: &BranchId) -> usize {
        self.store
            .shards
            .get(branch_id)
            .map(|shard| {
                shard
                    .data
                    .iter()
                    .filter(|(_, chain)| {
                        chain
                            .get_at_version(self.version)
                            .is_some_and(|sv| !sv.is_expired() && !sv.is_tombstone())
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    /// Get total entries across all branches at snapshot version
    ///
    /// Counts only entries that existed at the snapshot version
    /// (excludes tombstones and expired values).
    pub fn total_entries(&self) -> usize {
        self.store
            .shards
            .iter()
            .map(|entry| {
                entry
                    .value()
                    .data
                    .iter()
                    .filter(|(_, chain)| {
                        chain
                            .get_at_version(self.version)
                            .is_some_and(|sv| !sv.is_expired() && !sv.is_tombstone())
                    })
                    .count()
            })
            .sum()
    }

    /// Get number of branches (shards)
    pub fn shard_count(&self) -> usize {
        self.store.shard_count()
    }
}

impl std::fmt::Debug for ShardedSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedSnapshot")
            .field("version", &self.version)
            .field("shard_count", &self.store.shard_count())
            .field("total_entries", &self.store.total_entries())
            .finish()
    }
}

// ============================================================================
// Storage Trait Implementation
// ============================================================================

use std::time::Duration;
use strata_core::traits::Storage;
use strata_core::value::Value;

impl Storage for ShardedStore {
    /// Get current value for key (latest version)
    ///
    /// Returns None if key doesn't exist, is expired, or is a tombstone.
    fn get(&self, key: &Key) -> StrataResult<Option<VersionedValue>> {
        let branch_id = key.namespace.branch_id;
        Ok(self.shards.get(&branch_id).and_then(|shard| {
            shard.data.get(key).and_then(|chain| {
                chain.latest().and_then(|sv| {
                    // Filter out expired values and tombstones
                    if !sv.is_expired() && !sv.is_tombstone() {
                        Some(sv.versioned())
                    } else {
                        None
                    }
                })
            })
        }))
    }

    /// Get value at or before specified version (for snapshot isolation)
    ///
    /// Returns the value if version <= max_version, not expired, and not a tombstone.
    fn get_versioned(&self, key: &Key, max_version: u64) -> StrataResult<Option<VersionedValue>> {
        let branch_id = key.namespace.branch_id;
        Ok(self.shards.get(&branch_id).and_then(|shard| {
            shard.data.get(key).and_then(|chain| {
                chain.get_at_version(max_version).and_then(|sv| {
                    // Filter out expired values and tombstones
                    if !sv.is_expired() && !sv.is_tombstone() {
                        Some(sv.versioned())
                    } else {
                        None
                    }
                })
            })
        }))
    }

    /// Get version history for a key
    ///
    /// Returns historical versions newest first, filtered by limit and before_version.
    fn get_history(
        &self,
        key: &Key,
        limit: Option<usize>,
        before_version: Option<u64>,
    ) -> StrataResult<Vec<VersionedValue>> {
        let branch_id = key.namespace.branch_id;

        // Get the shard and extract history within the same scope to avoid lifetime issues
        let result = match self.shards.get(&branch_id) {
            Some(shard) => match shard.data.get(key) {
                Some(chain) => chain
                    .history(limit, before_version)
                    .into_iter()
                    .filter(|sv| !sv.is_expired())
                    .map(|sv| sv.versioned())
                    .collect(),
                None => Vec::new(),
            },
            None => Vec::new(),
        };

        Ok(result)
    }

    /// Put key-value pair with optional TTL
    ///
    /// Allocates a new version and returns it.
    fn put(&self, key: Key, value: Value, ttl: Option<Duration>) -> StrataResult<u64> {
        let version = self.next_version();
        let stored = StoredValue::new(value, Version::txn(version), ttl);

        // Use the inherent put method which handles version chain
        ShardedStore::put(self, key, stored, WriteMode::Append)?;

        Ok(version)
    }

    /// Delete key
    ///
    /// Returns the latest version's value if it existed.
    fn delete(&self, key: &Key) -> StrataResult<Option<VersionedValue>> {
        ShardedStore::delete(self, key)
    }

    /// Scan keys with given prefix at or before max_version
    ///
    /// Uses BTreeSet range scan for O(log n + k) performance.
    /// Results are sorted by key order (BTreeSet iteration is ordered).
    fn scan_prefix(
        &self,
        prefix: &Key,
        max_version: u64,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        let branch_id = prefix.namespace.branch_id;
        Ok(self
            .shards
            .get_mut(&branch_id)
            .map(|mut shard| {
                shard.ensure_ordered_keys();
                shard
                    .ordered_keys
                    .as_ref()
                    .unwrap()
                    .range::<Key, _>(prefix..)
                    .take_while(|k| k.starts_with(prefix))
                    .filter_map(|k| {
                        shard.data.get(k).and_then(|chain| {
                            chain.get_at_version(max_version).and_then(|sv| {
                                if !sv.is_expired() && !sv.is_tombstone() {
                                    Some(((**k).clone(), sv.versioned()))
                                } else {
                                    None
                                }
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Scan all keys for a given branch_id at or before max_version
    ///
    /// Returns all entries for the branch, filtered by version.
    fn scan_by_branch(
        &self,
        branch_id: BranchId,
        max_version: u64,
    ) -> StrataResult<Vec<(Key, VersionedValue)>> {
        Ok(self
            .shards
            .get(&branch_id)
            .map(|shard| {
                let mut results: Vec<_> = shard
                    .data
                    .iter()
                    .filter_map(|(k, chain)| {
                        chain.get_at_version(max_version).and_then(|sv| {
                            // Filter out expired values and tombstones
                            if !sv.is_expired() && !sv.is_tombstone() {
                                Some(((**k).clone(), sv.versioned()))
                            } else {
                                None
                            }
                        })
                    })
                    .collect();

                results.sort_by(|(a, _), (b, _)| a.cmp(b));
                results
            })
            .unwrap_or_default())
    }

    /// Get current global version
    fn current_version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Put a value with a specific version
    ///
    /// Used by transaction commit to apply writes with the commit version.
    fn put_with_version(
        &self,
        key: Key,
        value: Value,
        version: u64,
        ttl: Option<Duration>,
    ) -> StrataResult<()> {
        self.put_with_version_mode(key, value, version, ttl, WriteMode::Append)
    }

    fn put_with_version_mode(
        &self,
        key: Key,
        value: Value,
        version: u64,
        ttl: Option<Duration>,
        mode: WriteMode,
    ) -> StrataResult<()> {
        let stored = StoredValue::new(value, Version::txn(version), ttl);

        // Use the inherent put method which handles version chain + mode
        ShardedStore::put(self, key, stored, mode)?;

        // Update global version to be at least this version
        self.version.fetch_max(version, Ordering::AcqRel);

        Ok(())
    }

    /// Delete a key with a specific version (creates tombstone)
    ///
    /// Used by transaction commit to apply deletes.
    fn delete_with_version(&self, key: &Key, version: u64) -> StrataResult<()> {
        ShardedStore::delete_with_version(self, key, version)?;

        // Update global version to be at least this version
        self.version.fetch_max(version, Ordering::AcqRel);

        Ok(())
    }

    /// Get only the version number for a key (no Value clone)
    ///
    /// Skips `StoredValue::versioned()` entirely — no Value clone, no Version
    /// enum construction. Used by validation paths that only need version comparison.
    fn get_version_only(&self, key: &Key) -> StrataResult<Option<u64>> {
        let branch_id = key.namespace.branch_id;
        Ok(self.shards.get(&branch_id).and_then(|shard| {
            shard.data.get(key).and_then(|chain| {
                chain.latest().and_then(|sv| {
                    if !sv.is_expired() && !sv.is_tombstone() {
                        Some(sv.version_raw())
                    } else {
                        None
                    }
                })
            })
        }))
    }
}

// ============================================================================
// SnapshotView Trait Implementation
// ============================================================================

use strata_core::traits::SnapshotView;

impl SnapshotView for ShardedSnapshot {
    /// Get value from snapshot with MVCC version filtering
    ///
    /// Delegates to `store.get_versioned(key, version)` which walks the
    /// version chain to find the correct value at the snapshot version.
    fn get(&self, key: &Key) -> StrataResult<Option<VersionedValue>> {
        // Delegate to Storage::get_versioned for MVCC lookup
        Storage::get_versioned(&*self.store, key, self.version)
    }

    /// Scan keys with prefix from snapshot
    ///
    /// Uses BTreeSet range scan for O(log n + k) performance.
    /// Returns all matching keys at or before snapshot version.
    fn scan_prefix(&self, prefix: &Key) -> StrataResult<Vec<(Key, VersionedValue)>> {
        let branch_id = prefix.namespace.branch_id;
        Ok(self
            .store
            .shards
            .get_mut(&branch_id)
            .map(|mut shard| {
                shard.ensure_ordered_keys();
                shard
                    .ordered_keys
                    .as_ref()
                    .unwrap()
                    .range::<Key, _>(prefix..)
                    .take_while(|k| k.starts_with(prefix))
                    .filter_map(|k| {
                        shard.data.get(k).and_then(|chain| {
                            chain.get_at_version(self.version).and_then(|sv| {
                                if !sv.is_expired() && !sv.is_tombstone() {
                                    Some(((**k).clone(), sv.versioned()))
                                } else {
                                    None
                                }
                            })
                        })
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Get just the value and raw version, skipping VersionedValue construction.
    ///
    /// Avoids `Version::Txn` enum construction — returns raw `u64` directly
    /// from the `StoredValue`. Used by the transaction read path.
    fn get_value_and_version(&self, key: &Key) -> StrataResult<Option<(Value, u64)>> {
        let branch_id = key.namespace.branch_id;
        Ok(self.store.shards.get(&branch_id).and_then(|shard| {
            shard.data.get(key).and_then(|chain| {
                chain.get_at_version(self.version).and_then(|sv| {
                    if !sv.is_expired() && !sv.is_tombstone() {
                        Some((sv.value().clone(), sv.version_raw()))
                    } else {
                        None
                    }
                })
            })
        }))
    }

    /// Get snapshot version
    fn version(&self) -> u64 {
        self.version
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    // ========================================================================
    // VersionChain / VersionStorage Tests
    // ========================================================================

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
    // Arc<Key> Sharing Tests (Phase 3)
    // ========================================================================

    #[test]
    fn test_arc_key_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let branch_id = BranchId::new();
        let key1 = create_test_key(branch_id, "same_key");
        let key2 = create_test_key(branch_id, "same_key");

        // Two Arc<Key>s wrapping equal Keys must produce identical hashes
        let arc1 = Arc::new(key1);
        let arc2 = Arc::new(key2);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        arc1.hash(&mut h1);
        arc2.hash(&mut h2);

        assert_eq!(h1.finish(), h2.finish());
        assert_eq!(arc1, arc2); // PartialEq through Deref
    }

    #[test]
    fn test_arc_key_shared_between_hashmap_and_btreeset() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();

        // Insert several keys
        for i in 0..5 {
            let key = create_test_key(branch_id, &format!("shared_{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }

        // Force BTreeSet creation by doing a list operation
        let results = store.list_branch(&branch_id);
        assert_eq!(results.len(), 5);

        // Access the shard and verify Arc sharing
        let shard_ref = store.shards.get(&branch_id).unwrap();
        let shard = shard_ref.value();
        let btree = shard
            .ordered_keys
            .as_ref()
            .expect("BTreeSet should exist after list_branch");
        // Every key in the BTreeSet should be the same Arc as in the HashMap
        for arc_key in btree.iter() {
            // Verify it exists in the HashMap
            assert!(shard.data.contains_key(arc_key.as_ref()));
            // The Arc in the BTreeSet should point to the same allocation
            // as the one used as HashMap key (Arc::ptr_eq)
            let hashmap_arc = shard.data.keys().find(|k| *k == arc_key).unwrap();
            assert!(
                Arc::ptr_eq(arc_key, hashmap_arc),
                "BTreeSet and HashMap should share the same Arc<Key> allocation"
            );
        }
    }

    // ========================================================================
    // ShardedStore Tests
    // ========================================================================

    #[test]
    fn test_sharded_store_creation() {
        let store = ShardedStore::new();
        assert_eq!(store.shard_count(), 0);
        assert_eq!(store.version(), 0);
    }

    #[test]
    fn test_sharded_store_with_capacity() {
        let store = ShardedStore::with_capacity(100);
        assert_eq!(store.shard_count(), 0);
        assert_eq!(store.version(), 0);
    }

    #[test]
    fn test_version_increment() {
        let store = ShardedStore::new();
        assert_eq!(store.next_version(), 1);
        assert_eq!(store.next_version(), 2);
        assert_eq!(store.version(), 2);
    }

    #[test]
    fn test_set_version() {
        let store = ShardedStore::new();
        store.set_version(100);
        assert_eq!(store.version(), 100);
    }

    #[test]
    fn test_version_thread_safety() {
        use std::thread;
        let store = Arc::new(ShardedStore::new());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    for _ in 0..100 {
                        store.next_version();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(store.version(), 1000);
    }

    #[test]
    fn test_shard_creation() {
        let shard = Shard::new();
        assert!(shard.is_empty());
        assert_eq!(shard.len(), 0);
    }

    #[test]
    fn test_shard_with_capacity() {
        let shard = Shard::with_capacity(100);
        assert!(shard.is_empty());
    }

    #[test]
    fn test_debug_impl() {
        let store = ShardedStore::new();
        let debug_str = format!("{:?}", store);
        assert!(debug_str.contains("ShardedStore"));
        assert!(debug_str.contains("shard_count"));
    }

    // ========================================================================
    // Get/Put Operations Tests
    // ========================================================================

    fn create_test_key(branch_id: BranchId, name: &str) -> Key {
        use strata_core::types::Namespace;
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));
        Key::new_kv(ns, name)
    }

    fn create_stored_value(value: strata_core::value::Value, version: u64) -> StoredValue {
        StoredValue::new(value, Version::txn(version), None)
    }

    #[test]
    fn test_put_and_get() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "test_key");
        let value = create_stored_value(Value::Int(42), 1);

        // Put
        store.put(key.clone(), value, WriteMode::Append).unwrap();

        // Get (Storage trait returns Result<Option<...>>)
        let retrieved = store.get(&key).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().value, Value::Int(42));
    }

    #[test]
    fn test_get_nonexistent() {
        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "nonexistent");

        assert!(store.get(&key).unwrap().is_none());
    }

    #[test]
    fn test_delete() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "to_delete");
        let value = create_stored_value(Value::Int(42), 1);

        store.put(key.clone(), value, WriteMode::Append).unwrap();
        assert!(store.get(&key).unwrap().is_some());

        // Delete
        let deleted = store.delete(&key).unwrap();
        assert!(deleted.is_some());
        assert!(store.get(&key).unwrap().is_none());
    }

    #[test]
    fn test_delete_nonexistent() {
        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "nonexistent");

        assert!(store.delete(&key).unwrap().is_none());
    }

    #[test]
    fn test_contains() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "exists");
        let value = create_stored_value(Value::Int(42), 1);

        assert!(!store.contains(&key));
        store.put(key.clone(), value, WriteMode::Append).unwrap();
        assert!(store.contains(&key));
    }

    #[test]
    fn test_overwrite() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "overwrite");

        store
            .put(
                key.clone(),
                create_stored_value(Value::Int(1), 1),
                WriteMode::Append,
            )
            .unwrap();
        store
            .put(
                key.clone(),
                create_stored_value(Value::Int(2), 2),
                WriteMode::Append,
            )
            .unwrap();

        let retrieved = store.get(&key).unwrap().unwrap();
        assert_eq!(retrieved.value, Value::Int(2));
        assert_eq!(retrieved.version, Version::txn(2));
    }

    #[test]
    fn test_multiple_branches_isolated() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();

        let key1 = create_test_key(branch1, "key");
        let key2 = create_test_key(branch2, "key");

        store
            .put(
                key1.clone(),
                create_stored_value(Value::Int(1), 1),
                WriteMode::Append,
            )
            .unwrap();
        store
            .put(
                key2.clone(),
                create_stored_value(Value::Int(2), 1),
                WriteMode::Append,
            )
            .unwrap();

        // Different branches, same key name, different values
        assert_eq!(store.get(&key1).unwrap().unwrap().value, Value::Int(1));
        assert_eq!(store.get(&key2).unwrap().unwrap().value, Value::Int(2));
        assert_eq!(store.shard_count(), 2);
    }

    #[test]
    fn test_apply_batch() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();

        let key1 = create_test_key(branch_id, "batch1");
        let key2 = create_test_key(branch_id, "batch2");
        let key3 = create_test_key(branch_id, "batch3");

        // First, put key3 so we can delete it
        store
            .put(
                key3.clone(),
                create_stored_value(Value::Int(999), 1),
                WriteMode::Append,
            )
            .unwrap();

        // Apply batch
        let writes = vec![(key1.clone(), Value::Int(1)), (key2.clone(), Value::Int(2))];
        let deletes = vec![key3.clone()];

        store.apply_batch(writes, deletes, 2).unwrap();

        assert_eq!(store.get(&key1).unwrap().unwrap().value, Value::Int(1));
        assert_eq!(store.get(&key1).unwrap().unwrap().version, Version::txn(2));
        assert_eq!(store.get(&key2).unwrap().unwrap().value, Value::Int(2));
        assert!(store.get(&key3).unwrap().is_none());
    }

    #[test]
    fn test_branch_entry_count() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();

        assert_eq!(store.branch_entry_count(&branch_id), 0);

        for i in 0..5 {
            let key = create_test_key(branch_id, &format!("key{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }

        assert_eq!(store.branch_entry_count(&branch_id), 5);
        assert_eq!(store.total_entries(), 5);
    }

    #[test]
    fn test_concurrent_writes_different_branches() {
        use std::thread;
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());

        // 10 threads, each with its own branch, writing 100 keys
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    let branch_id = BranchId::new();
                    for i in 0..100 {
                        let key = create_test_key(branch_id, &format!("key{}", i));
                        store
                            .put(
                                key,
                                create_stored_value(Value::Int(i), 1),
                                WriteMode::Append,
                            )
                            .unwrap();
                    }
                    branch_id
                })
            })
            .collect();

        let branch_ids: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Verify each branch has 100 entries
        for branch_id in &branch_ids {
            assert_eq!(store.branch_entry_count(branch_id), 100);
        }

        assert_eq!(store.shard_count(), 10);
        assert_eq!(store.total_entries(), 1000);
    }

    // ========================================================================
    // List Operations Tests
    // ========================================================================

    #[test]
    fn test_list_branch_empty() {
        let store = ShardedStore::new();
        let branch_id = BranchId::new();

        let results = store.list_branch(&branch_id);
        assert!(results.is_empty());
    }

    #[test]
    fn test_list_branch() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();

        // Insert some keys
        for i in 0..5 {
            let key = create_test_key(branch_id, &format!("key{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }

        let results = store.list_branch(&branch_id);
        assert_eq!(results.len(), 5);

        // Verify sorted order
        for i in 0..results.len() - 1 {
            assert!(results[i].0 < results[i + 1].0);
        }
    }

    #[test]
    fn test_list_by_prefix() {
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Insert keys with different prefixes
        store.put(
            Key::new_kv(ns.clone(), "user:alice"),
            create_stored_value(Value::Int(1), 1),
            WriteMode::Append,
        );
        store.put(
            Key::new_kv(ns.clone(), "user:bob"),
            create_stored_value(Value::Int(2), 1),
            WriteMode::Append,
        );
        store.put(
            Key::new_kv(ns.clone(), "config:timeout"),
            create_stored_value(Value::Int(3), 1),
            WriteMode::Append,
        );

        // Query with "user:" prefix
        let prefix = Key::new_kv(ns.clone(), "user:");
        let results = store.list_by_prefix(&prefix);

        assert_eq!(results.len(), 2);
        // Should be alice, bob in sorted order
        assert!(results[0].0.user_key_string().unwrap().contains("alice"));
        assert!(results[1].0.user_key_string().unwrap().contains("bob"));
    }

    #[test]
    fn test_list_by_prefix_empty() {
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        store.put(
            Key::new_kv(ns.clone(), "data:foo"),
            create_stored_value(Value::Int(1), 1),
            WriteMode::Append,
        );

        // Query with non-matching prefix
        let prefix = Key::new_kv(ns.clone(), "user:");
        let results = store.list_by_prefix(&prefix);

        assert!(results.is_empty());
    }

    #[test]
    fn test_list_by_type() {
        use strata_core::types::{Namespace, TypeTag};
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Insert KV entries
        store.put(
            Key::new_kv(ns.clone(), "kv1"),
            create_stored_value(Value::Int(1), 1),
            WriteMode::Append,
        );
        store.put(
            Key::new_kv(ns.clone(), "kv2"),
            create_stored_value(Value::Int(2), 1),
            WriteMode::Append,
        );

        // Insert Event entries
        store.put(
            Key::new_event(ns.clone(), 1),
            create_stored_value(Value::Int(10), 1),
            WriteMode::Append,
        );

        // Insert State entries
        store.put(
            Key::new_state(ns.clone(), "state1"),
            create_stored_value(Value::Int(20), 1),
            WriteMode::Append,
        );

        // Query by type
        let kv_results = store.list_by_type(&branch_id, TypeTag::KV);
        assert_eq!(kv_results.len(), 2);

        let event_results = store.list_by_type(&branch_id, TypeTag::Event);
        assert_eq!(event_results.len(), 1);

        let state_results = store.list_by_type(&branch_id, TypeTag::State);
        assert_eq!(state_results.len(), 1);
    }

    #[test]
    fn test_count_by_type() {
        use strata_core::types::{Namespace, TypeTag};
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Insert mixed types
        for i in 0..5 {
            store.put(
                Key::new_kv(ns.clone(), format!("kv{}", i)),
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            );
        }
        for i in 0..3 {
            store.put(
                Key::new_event(ns.clone(), i as u64),
                create_stored_value(Value::Int(i), 1),
                WriteMode::Append,
            );
        }

        assert_eq!(store.count_by_type(&branch_id, TypeTag::KV), 5);
        assert_eq!(store.count_by_type(&branch_id, TypeTag::Event), 3);
        assert_eq!(store.count_by_type(&branch_id, TypeTag::State), 0);
    }

    #[test]
    fn test_branch_ids() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();
        let branch3 = BranchId::new();

        // Insert data for 3 branches
        store.put(
            create_test_key(branch1, "k1"),
            create_stored_value(Value::Int(1), 1),
            WriteMode::Append,
        );
        store.put(
            create_test_key(branch2, "k1"),
            create_stored_value(Value::Int(2), 1),
            WriteMode::Append,
        );
        store.put(
            create_test_key(branch3, "k1"),
            create_stored_value(Value::Int(3), 1),
            WriteMode::Append,
        );

        let branch_ids = store.branch_ids();
        assert_eq!(branch_ids.len(), 3);
        assert!(branch_ids.contains(&branch1));
        assert!(branch_ids.contains(&branch2));
        assert!(branch_ids.contains(&branch3));
    }

    #[test]
    fn test_clear_branch() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();

        // Insert some data
        for i in 0..5 {
            let key = create_test_key(branch_id, &format!("key{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }

        assert_eq!(store.branch_entry_count(&branch_id), 5);
        assert!(store.has_branch(&branch_id));

        // Clear the branch
        assert!(store.clear_branch(&branch_id));

        assert_eq!(store.branch_entry_count(&branch_id), 0);
        assert!(!store.has_branch(&branch_id));
    }

    #[test]
    fn test_clear_branch_nonexistent() {
        let store = ShardedStore::new();
        let branch_id = BranchId::new();

        // Clear non-existent branch returns false
        assert!(!store.clear_branch(&branch_id));
    }

    #[test]
    fn test_list_sorted_order() {
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::new(
            "tenant".to_string(),
            "app".to_string(),
            "agent".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Insert in random order
        let keys = vec!["zebra", "apple", "mango", "banana"];
        for k in &keys {
            store.put(
                Key::new_kv(ns.clone(), *k),
                create_stored_value(Value::String(k.to_string()), 1),
                WriteMode::Append,
            );
        }

        let results = store.list_branch(&branch_id);
        let result_keys: Vec<_> = results
            .iter()
            .map(|(k, _)| k.user_key_string().unwrap())
            .collect();

        // Should be sorted: apple, banana, mango, zebra
        assert_eq!(result_keys, vec!["apple", "banana", "mango", "zebra"]);
    }

    // ========================================================================
    // Snapshot Acquisition Tests
    // ========================================================================

    #[test]
    fn test_snapshot_creation() {
        let store = Arc::new(ShardedStore::new());
        let snapshot = store.snapshot();

        assert_eq!(snapshot.version(), 0);
        assert_eq!(snapshot.shard_count(), 0);
    }

    #[test]
    fn test_snapshot_captures_version() {
        let store = Arc::new(ShardedStore::new());

        // Increment version
        store.next_version();
        store.next_version();
        store.next_version();

        let snapshot = store.snapshot();
        assert_eq!(snapshot.version(), 3);

        // Further increments don't affect snapshot
        store.next_version();
        assert_eq!(snapshot.version(), 3);
        assert_eq!(store.version(), 4);
    }

    #[test]
    fn test_snapshot_read_operations() {
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();

        // Put some data (version=1)
        let key = create_test_key(branch_id, "test_key");
        store
            .put(
                key.clone(),
                create_stored_value(Value::Int(42), 1),
                WriteMode::Append,
            )
            .unwrap();
        // Update store version so snapshot can see data at version 1
        store.set_version(1);

        // Create snapshot (will capture version=1)
        let snapshot = store.snapshot();

        // Read through snapshot (SnapshotView returns Result<Option<...>>)
        let value = snapshot.get(&key).unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap().value, Value::Int(42));

        // contains works
        assert!(snapshot.contains(&key));
    }

    #[test]
    fn test_snapshot_list_operations() {
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();

        // Put some data at version 1
        for i in 0..5 {
            let key = create_test_key(branch_id, &format!("key{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }

        // Advance store version to 1 so snapshot will see the data
        store.next_version();
        let snapshot = store.snapshot();
        assert_eq!(snapshot.version(), 1);

        // list_branch works - sees data at version 1
        let results = snapshot.list_branch(&branch_id);
        assert_eq!(results.len(), 5);

        // branch_entry_count works
        assert_eq!(snapshot.branch_entry_count(&branch_id), 5);

        // total_entries works
        assert_eq!(snapshot.total_entries(), 5);
    }

    #[test]
    fn test_snapshot_multiple_concurrent() {
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();

        // Create first snapshot at version 0
        let snap1 = store.snapshot();
        assert_eq!(snap1.version(), 0);

        // Add data and increment version
        let key1 = create_test_key(branch_id, "key1");
        store
            .put(
                key1.clone(),
                create_stored_value(Value::Int(1), 1),
                WriteMode::Append,
            )
            .unwrap();
        store.next_version();

        // Create second snapshot at version 1
        let snap2 = store.snapshot();
        assert_eq!(snap2.version(), 1);

        // Add more data
        let key2 = create_test_key(branch_id, "key2");
        store
            .put(
                key2.clone(),
                create_stored_value(Value::Int(2), 2),
                WriteMode::Append,
            )
            .unwrap();
        store.next_version();

        // Create third snapshot at version 2
        let snap3 = store.snapshot();
        assert_eq!(snap3.version(), 2);

        // All snapshots retain their version
        assert_eq!(snap1.version(), 0);
        assert_eq!(snap2.version(), 1);
        assert_eq!(snap3.version(), 2);

        // Note: Current implementation doesn't do MVCC filtering,
        // so all snapshots see current data. This test verifies
        // version capture is working correctly.
    }

    #[test]
    fn test_snapshot_clone() {
        let store = Arc::new(ShardedStore::new());
        store.next_version();

        let snapshot = store.snapshot();
        let cloned = snapshot.clone();

        assert_eq!(snapshot.version(), cloned.version());
    }

    #[test]
    fn test_snapshot_debug() {
        let store = Arc::new(ShardedStore::new());
        let snapshot = store.snapshot();

        let debug_str = format!("{:?}", snapshot);
        assert!(debug_str.contains("ShardedSnapshot"));
        assert!(debug_str.contains("version"));
    }

    #[test]
    fn test_snapshot_thread_safety() {
        use std::thread;

        let store = Arc::new(ShardedStore::new());

        // Spawn threads that create snapshots concurrently
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    // Create snapshot
                    let snapshot = store.snapshot();

                    // Increment version
                    store.next_version();

                    // Create another snapshot
                    let snapshot2 = store.snapshot();

                    // Second snapshot should have higher or equal version
                    assert!(snapshot2.version() >= snapshot.version());

                    (snapshot.version(), snapshot2.version())
                })
            })
            .collect();

        for h in handles {
            let (v1, v2) = h.join().unwrap();
            assert!(v2 >= v1);
        }

        // Final version should be 10 (each thread incremented once)
        assert_eq!(store.version(), 10);
    }

    #[test]
    fn test_snapshot_fast_path() {
        use std::time::Instant;

        let store = Arc::new(ShardedStore::new());

        // Add some data to make it more realistic
        let branch_id = BranchId::new();
        for i in 0..1000 {
            let key = create_test_key(branch_id, &format!("key{}", i));
            store.put(
                key,
                create_stored_value(strata_core::value::Value::Int(i), 1),
                WriteMode::Append,
            );
        }

        // Measure snapshot creation time
        let iterations = 10000;
        let start = Instant::now();
        for _ in 0..iterations {
            let _snapshot = store.snapshot();
        }
        let elapsed = start.elapsed();
        let avg_ns = elapsed.as_nanos() / iterations as u128;

        // Should be well under 500ns
        // Note: In debug mode it might be slightly higher, but should still be fast
        println!("Snapshot acquisition avg: {}ns", avg_ns);
        assert!(
            avg_ns < 5000, // 5µs max in debug mode
            "Snapshot too slow: {}ns (target: <500ns in release)",
            avg_ns
        );
    }

    // ========================================================================
    // Storage Trait Implementation Tests
    // ========================================================================

    #[test]
    fn test_storage_trait_get_put() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns, "test_key");

        // Put via Storage trait
        let version = Storage::put(&store, key.clone(), Value::Int(42), None).unwrap();
        assert_eq!(version, 1);

        // Get via Storage trait
        let result = Storage::get(&store, &key).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Value::Int(42));
    }

    #[test]
    fn test_storage_trait_get_versioned() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns, "test_key");

        // Put with version 1
        Storage::put(&store, key.clone(), Value::Int(42), None).unwrap();

        // Get with max_version 1 - should return value
        let result = Storage::get_versioned(&store, &key, 1).unwrap();
        assert!(result.is_some());

        // Get with max_version 0 - should return None (version 1 > 0)
        let result = Storage::get_versioned(&store, &key, 0).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_storage_trait_delete() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns, "test_key");

        Storage::put(&store, key.clone(), Value::Int(42), None).unwrap();
        assert!(Storage::get(&store, &key).unwrap().is_some());

        // Delete via Storage trait
        let deleted = Storage::delete(&store, &key).unwrap();
        assert!(deleted.is_some());
        assert!(Storage::get(&store, &key).unwrap().is_none());
    }

    #[test]
    fn test_storage_trait_scan_prefix() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));

        // Insert keys with different prefixes
        Storage::put(
            &store,
            Key::new_kv(ns.clone(), "user:alice"),
            Value::Int(1),
            None,
        )
        .unwrap();
        Storage::put(
            &store,
            Key::new_kv(ns.clone(), "user:bob"),
            Value::Int(2),
            None,
        )
        .unwrap();
        Storage::put(
            &store,
            Key::new_kv(ns.clone(), "config:timeout"),
            Value::Int(3),
            None,
        )
        .unwrap();

        // Scan with "user:" prefix
        let prefix = Key::new_kv(ns.clone(), "user:");
        let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_storage_trait_scan_by_branch() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch1 = BranchId::new();
        let branch2 = BranchId::new();

        // Insert data for two branches
        let ns1 = Arc::new(Namespace::for_branch(branch1));
        let ns2 = Arc::new(Namespace::for_branch(branch2));

        Storage::put(
            &store,
            Key::new_kv(ns1.clone(), "key1"),
            Value::Int(1),
            None,
        )
        .unwrap();
        Storage::put(
            &store,
            Key::new_kv(ns1.clone(), "key2"),
            Value::Int(2),
            None,
        )
        .unwrap();
        Storage::put(
            &store,
            Key::new_kv(ns2.clone(), "key1"),
            Value::Int(3),
            None,
        )
        .unwrap();

        // Scan branch1
        let results = Storage::scan_by_branch(&store, branch1, u64::MAX).unwrap();
        assert_eq!(results.len(), 2);

        // Scan branch2
        let results = Storage::scan_by_branch(&store, branch2, u64::MAX).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_storage_trait_put_with_version() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns, "test_key");

        // Put with specific version 42
        Storage::put_with_version(&store, key.clone(), Value::Int(100), 42, None).unwrap();

        // Verify version is 42
        let result = Storage::get(&store, &key).unwrap().unwrap();
        assert_eq!(result.version, Version::txn(42));

        // current_version should be updated
        assert!(Storage::current_version(&store) >= 42);
    }

    #[test]
    fn test_storage_trait_delete_with_version() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns, "versioned_delete");

        // Put a value
        Storage::put(&*store, key.clone(), Value::Int(42), None).unwrap();
        assert!(Storage::get(&*store, &key).unwrap().is_some());

        // Delete with specific version
        Storage::delete_with_version(&*store, &key, 100).unwrap();

        // Key should no longer be visible
        assert!(Storage::get(&*store, &key).unwrap().is_none());

        // Global version should be updated
        assert!(Storage::current_version(&*store) >= 100);

        // Version 1 (before delete) should still be visible via MVCC
        let snap_at_1 = store.shards.get(&branch_id).and_then(|shard| {
            shard.data.get(&key).and_then(|chain| {
                chain.get_at_version(1).and_then(|sv| {
                    if !sv.is_tombstone() {
                        Some(sv.versioned())
                    } else {
                        None
                    }
                })
            })
        });
        assert!(
            snap_at_1.is_some(),
            "Version 1 should still be visible via MVCC"
        );
    }

    #[test]
    fn test_storage_trait_get_version_only_existing_key() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns, "version_check");

        // Put a value — version 1
        Storage::put(
            &store,
            key.clone(),
            Value::String("hello".to_string()),
            None,
        )
        .unwrap();

        // get_version_only should return the version
        let version = Storage::get_version_only(&store, &key).unwrap();
        assert_eq!(version, Some(1));

        // Should match what get() returns
        let full = Storage::get(&store, &key).unwrap().unwrap();
        assert_eq!(version.unwrap(), full.version.as_u64());
    }

    #[test]
    fn test_storage_trait_get_version_only_nonexistent() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns, "missing");

        assert_eq!(Storage::get_version_only(&store, &key).unwrap(), None);
    }

    #[test]
    fn test_storage_trait_get_version_only_tombstoned() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns, "to_delete");

        Storage::put(&store, key.clone(), Value::Int(1), None).unwrap();
        Storage::delete(&store, &key).unwrap();

        // get_version_only should return None for tombstoned key (same as get())
        assert_eq!(Storage::get_version_only(&store, &key).unwrap(), None);
        assert!(Storage::get(&store, &key).unwrap().is_none());
    }

    #[test]
    fn test_storage_trait_get_version_only_expired() {
        use std::time::Duration;
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns, "expiring");

        // Put with an already-expired TTL (use put_with_version + manual StoredValue)
        let sv = StoredValue::with_timestamp(
            Value::Int(1),
            Version::txn(1),
            strata_core::Timestamp::from_micros(0), // epoch = definitely expired
            Some(Duration::from_secs(1)),
        );
        ShardedStore::put(&store, key.clone(), sv, WriteMode::Append).unwrap();

        // get_version_only should return None for expired key (same as get())
        assert_eq!(Storage::get_version_only(&store, &key).unwrap(), None);
        assert!(Storage::get(&store, &key).unwrap().is_none());
    }

    #[test]
    fn test_storage_trait_get_version_only_matches_get() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));

        // Write multiple keys with different versions
        for i in 0..5 {
            let key = Key::new_kv(ns.clone(), &format!("key{}", i));
            Storage::put(&store, key, Value::Int(i), None).unwrap();
        }

        // Verify get_version_only matches get() for all keys
        for i in 0..5 {
            let key = Key::new_kv(ns.clone(), &format!("key{}", i));
            let version_only = Storage::get_version_only(&store, &key).unwrap();
            let full_get = Storage::get(&store, &key).unwrap();
            assert_eq!(
                version_only,
                full_get.map(|vv| vv.version.as_u64()),
                "get_version_only must match get() for key{}",
                i
            );
        }

        // Also check a nonexistent key
        let missing = Key::new_kv(ns, "nonexistent");
        assert_eq!(
            Storage::get_version_only(&store, &missing).unwrap(),
            Storage::get(&store, &missing)
                .unwrap()
                .map(|vv| vv.version.as_u64())
        );
    }

    #[test]
    fn test_snapshot_view_trait() {
        use strata_core::traits::{SnapshotView, Storage};
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));

        // Put at version 1
        let key1 = Key::new_kv(ns.clone(), "key1");
        Storage::put(&*store, key1.clone(), Value::Int(1), None).unwrap();

        // Create snapshot at version 1
        let snapshot = store.snapshot();
        assert_eq!(SnapshotView::version(&snapshot), 1);

        // Put at version 2
        let key2 = Key::new_kv(ns.clone(), "key2");
        Storage::put(&*store, key2.clone(), Value::Int(2), None).unwrap();

        // Snapshot should only see key1 (version 1) via MVCC filtering
        let snap_key1 = SnapshotView::get(&snapshot, &key1).unwrap();
        assert!(snap_key1.is_some());

        // key2 has version 2, but snapshot is at version 1
        // It will be visible since we're reading from shared storage
        // but won't pass version filter
        let snap_key2 = SnapshotView::get(&snapshot, &key2).unwrap();
        // Note: key2 has version 2, snapshot version is 1, so it should be None
        assert!(
            snap_key2.is_none(),
            "key2 should not be visible at version 1"
        );
    }

    #[test]
    fn test_snapshot_view_scan_prefix() {
        use strata_core::traits::{SnapshotView, Storage};
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));

        // Put two keys at version 1
        Storage::put(
            &*store,
            Key::new_kv(ns.clone(), "user:alice"),
            Value::Int(1),
            None,
        )
        .unwrap();
        Storage::put(
            &*store,
            Key::new_kv(ns.clone(), "user:bob"),
            Value::Int(2),
            None,
        )
        .unwrap();

        let snapshot = store.snapshot();

        // Put another key at version 3
        Storage::put(
            &*store,
            Key::new_kv(ns.clone(), "user:charlie"),
            Value::Int(3),
            None,
        )
        .unwrap();

        // Scan prefix via snapshot - should only see 2 keys at snapshot version
        let prefix = Key::new_kv(ns.clone(), "user:");
        let results = SnapshotView::scan_prefix(&snapshot, &prefix).unwrap();

        assert_eq!(
            results.len(),
            2,
            "Snapshot should only see keys at version <= 2"
        );
    }

    // ========================================================================
    // VersionChain::history() Tests
    // ========================================================================

    #[test]
    fn test_version_chain_history_all_versions() {
        use strata_core::value::Value;

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
        use strata_core::value::Value;

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
        use strata_core::value::Value;

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
        use strata_core::value::Value;

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
        use strata_core::value::Value;

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
        use strata_core::value::Value;

        let chain = VersionChain::new(create_stored_value(Value::Int(1), 1));
        assert!(!chain.is_empty());
        assert_eq!(chain.version_count(), 1);
    }

    // ========================================================================
    // VersionChain::get_at_version() Tests (MVCC)
    // ========================================================================

    #[test]
    fn test_version_chain_get_at_version_single() {
        use strata_core::value::Value;

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
        use strata_core::value::Value;

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
        use strata_core::value::Value;

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
        use strata_core::value::Value;

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

    // ========================================================================
    // Storage::get_history() Tests
    // ========================================================================

    #[test]
    fn test_storage_get_history() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns.clone(), "test-key");

        // Put multiple versions of the same key
        Storage::put_with_version(&store, key.clone(), Value::Int(1), 1, None).unwrap();
        Storage::put_with_version(&store, key.clone(), Value::Int(2), 2, None).unwrap();
        Storage::put_with_version(&store, key.clone(), Value::Int(3), 3, None).unwrap();

        // Get full history
        let history = Storage::get_history(&store, &key, None, None).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].version.as_u64(), 3);
        assert_eq!(history[1].version.as_u64(), 2);
        assert_eq!(history[2].version.as_u64(), 1);
    }

    #[test]
    fn test_storage_get_history_pagination() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns.clone(), "paginated-key");

        // Put 5 versions
        for i in 1..=5 {
            Storage::put_with_version(&store, key.clone(), Value::Int(i), i as u64, None).unwrap();
        }

        // Page 1: Get first 2
        let page1 = Storage::get_history(&store, &key, Some(2), None).unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].version.as_u64(), 5);
        assert_eq!(page1[1].version.as_u64(), 4);

        // Page 2: Get next 2 (before version 4)
        let page2 = Storage::get_history(&store, &key, Some(2), Some(4)).unwrap();
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].version.as_u64(), 3);
        assert_eq!(page2[1].version.as_u64(), 2);

        // Page 3: Get remaining
        let page3 = Storage::get_history(&store, &key, Some(2), Some(2)).unwrap();
        assert_eq!(page3.len(), 1);
        assert_eq!(page3[0].version.as_u64(), 1);
    }

    #[test]
    fn test_storage_get_history_nonexistent_key() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));
        let key = Key::new_kv(ns.clone(), "nonexistent");

        let history = Storage::get_history(&store, &key, None, None).unwrap();
        assert!(history.is_empty());
    }

    // ========================================================================
    // ADVERSARIAL TESTS - Bug Hunting
    // These tests probe edge cases and potential bugs.
    // ========================================================================

    /// MVCC delete with tombstones preserves snapshot isolation
    ///
    /// Delete now creates a tombstone at a new version instead of removing all versions.
    /// This ensures snapshots taken before the delete can still see the value.
    #[test]
    fn test_delete_uses_tombstone_preserving_mvcc() {
        use strata_core::traits::{SnapshotView, Storage};
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "mvcc_uncached");

        // Put a value at version 1
        Storage::put(&*store, key.clone(), Value::Int(100), None).unwrap();

        // Create a snapshot at version 1 - but DON'T read the key yet
        let snapshot = store.snapshot();
        assert_eq!(snapshot.version(), 1);

        // Delete the key BEFORE the snapshot reads it
        // This creates a tombstone at version 2
        Storage::delete(&*store, &key).unwrap();

        // Now read from snapshot - it should see version 1 (before the tombstone)
        let result = SnapshotView::get(&snapshot, &key).unwrap();

        // FIX VERIFIED: The snapshot sees the value at version 1 because
        // delete() creates a tombstone at version 2, leaving version 1 intact.
        assert!(
            result.is_some(),
            "Snapshot should see value at version 1 - tombstone is at version 2"
        );
        assert_eq!(
            result.unwrap().value,
            Value::Int(100),
            "Snapshot should see the original value"
        );
    }

    /// Verify snapshot cache provides isolation for pre-read keys
    ///
    /// If a snapshot reads a key BEFORE it's deleted, the cached value
    /// provides isolation and the snapshot continues to see the value.
    #[test]
    fn test_snapshot_cache_provides_isolation_for_cached_reads() {
        use strata_core::traits::{SnapshotView, Storage};
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "cached_test");

        // Put a value at version 1
        Storage::put(&*store, key.clone(), Value::Int(100), None).unwrap();

        // Create snapshot and READ the key (this caches it)
        let snapshot = store.snapshot();
        let before = SnapshotView::get(&snapshot, &key).unwrap();
        assert!(before.is_some());
        assert_eq!(before.unwrap().value, Value::Int(100));

        // Delete the key
        Storage::delete(&*store, &key).unwrap();

        // Snapshot should still see the cached value
        let after = SnapshotView::get(&snapshot, &key).unwrap();
        assert!(after.is_some(), "Cached value should survive delete");
        assert_eq!(after.unwrap().value, Value::Int(100));
    }

    /// Version counter wraps at u64::MAX boundary (fixed)
    ///
    /// The version counter now uses wrapping arithmetic to prevent panic.
    /// At ~584 years of continuous operation at 1 billion versions/second,
    /// overflow is extremely unlikely but now handled gracefully.
    #[test]
    fn test_version_counter_wraps_at_max_boundary() {
        let store = ShardedStore::new();

        // Set version close to MAX
        store.set_version(u64::MAX - 2);
        assert_eq!(store.version(), u64::MAX - 2);

        // Allocate versions up to MAX
        let v1 = store.next_version();
        assert_eq!(v1, u64::MAX - 1);

        let v2 = store.next_version();
        assert_eq!(v2, u64::MAX);

        // Cross the boundary - should wrap to 0 without panic
        let v3 = store.next_version();
        assert_eq!(v3, 0, "Version should wrap to 0 at u64::MAX");

        // Continue allocating
        let v4 = store.next_version();
        assert_eq!(v4, 1);

        // Note: After wrapping, versions are no longer monotonically increasing.
        // This is documented behavior for the extremely unlikely overflow case.
    }

    /// SAFETY: TTL expiration with clock going backward
    ///
    /// If system clock goes backward after storing a value,
    /// `duration_since()` returns `None` and `is_expired()` returns `false`.
    /// This is correct: the value persists rather than expiring prematurely.
    /// Clock-backward means we cannot reliably measure elapsed time, so the
    /// safe default is to treat the value as unexpired. Items will expire
    /// normally once the clock catches up past `timestamp + ttl`.
    #[test]
    fn test_ttl_expiration_with_old_timestamp() {
        use strata_core::value::Value;

        // Create a value with a timestamp in the "future" relative to epoch
        let future_ts = Timestamp::from_micros(u64::MAX / 2);
        let stored = StoredValue::with_timestamp(
            Value::Int(42),
            Version::txn(1),
            future_ts,
            Some(std::time::Duration::from_secs(1)), // 1 second TTL
        );

        // If current system time is before the timestamp,
        // duration_since returns None and is_expired returns false
        // This means the value appears NOT expired even though TTL passed
        let is_expired = stored.is_expired();

        // This test documents the edge case:
        // With a future timestamp, the value never expires
        // This is INTENDED behavior (safe default), but worth documenting
        assert!(
            !is_expired,
            "Value with future timestamp doesn't expire - clock backward protection"
        );
    }

    /// SAFETY: Concurrent snapshot reads during store modifications
    ///
    /// Snapshots hold an `Arc<ShardedStore>` and a captured version `u64`.
    /// Reads delegate to `get_versioned()`, which acquires a DashMap read
    /// lock and walks the version chain for entries <= snapshot version.
    /// New versions are only prepended (push_front) and old versions are
    /// never mutated in place, so concurrent writes cannot corrupt a
    /// snapshot read. DashMap's per-shard RwLock ensures structural safety.
    #[test]
    fn test_snapshot_isolation_under_concurrent_writes() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;
        use strata_core::traits::{SnapshotView, Storage};
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();

        // Put initial values
        for i in 0..10 {
            let key = create_test_key(branch_id, &format!("key{}", i));
            Storage::put(&*store, key, Value::Int(i), None).unwrap();
        }

        // Take snapshot
        let snapshot = store.snapshot();
        let snapshot_version = snapshot.version();

        // Flag to stop writers
        let stop = Arc::new(AtomicBool::new(false));

        // Spawn writers that continuously modify the store
        let writer_handles: Vec<_> = (0..4)
            .map(|t| {
                let store = Arc::clone(&store);
                let stop = Arc::clone(&stop);
                thread::spawn(move || {
                    let mut counter = 0;
                    while !stop.load(Ordering::Relaxed) {
                        for i in 0..10 {
                            let key = create_test_key(branch_id, &format!("key{}", i));
                            let _ =
                                Storage::put(&*store, key, Value::Int(t * 1000 + counter), None);
                        }
                        counter += 1;
                        if counter > 100 {
                            break;
                        }
                    }
                })
            })
            .collect();

        // Read from snapshot while writes are happening
        // Snapshot should see consistent values at snapshot_version
        for _ in 0..100 {
            for i in 0..10 {
                let key = create_test_key(branch_id, &format!("key{}", i));
                let result = SnapshotView::get(&snapshot, &key).unwrap();

                // Snapshot should see SOME value (the one at or before snapshot version)
                assert!(
                    result.is_some(),
                    "Snapshot lost visibility to key{} during concurrent writes",
                    i
                );

                // The version should be <= snapshot_version
                let version = result.as_ref().unwrap().version.as_u64();
                assert!(
                    version <= snapshot_version,
                    "Snapshot saw version {} but snapshot is at {} - MVCC violation",
                    version,
                    snapshot_version
                );
            }
        }

        // Stop writers
        stop.store(true, Ordering::Relaxed);
        for h in writer_handles {
            let _ = h.join();
        }
    }

    /// SAFETY: Snapshot cache race condition (not applicable)
    ///
    /// `ShardedSnapshot` has no cache. It stores only a version `u64` and
    /// an `Arc<ShardedStore>`, delegating every `get()` call directly to
    /// `Storage::get_versioned()`. There is no check-then-populate pattern,
    /// so no race condition is possible. Multiple threads calling `get()`
    /// concurrently each acquire independent DashMap read locks.
    #[test]
    fn test_snapshot_cache_concurrent_access() {
        use std::sync::Barrier;
        use std::thread;
        use strata_core::traits::{SnapshotView, Storage};
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "cached_key");

        // Put a value
        Storage::put(&*store, key.clone(), Value::Int(42), None).unwrap();

        // Create snapshot
        let snapshot = Arc::new(store.snapshot());

        // Use barrier to ensure all threads hit get() simultaneously
        let num_threads = 10;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let snapshot = Arc::clone(&snapshot);
                let key = key.clone();
                let barrier = Arc::clone(&barrier);

                thread::spawn(move || {
                    barrier.wait();
                    // All threads try to get() at the same time
                    let result = SnapshotView::get(&*snapshot, &key).unwrap();
                    result.map(|v| v.value.clone())
                })
            })
            .collect();

        // Collect results
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All threads should see the same value (cache is consistent)
        for result in &results {
            assert_eq!(
                *result,
                Some(Value::Int(42)),
                "Snapshot cache returned inconsistent value under concurrent access"
            );
        }
    }

    /// SAFETY: Version chain GC during concurrent reads
    ///
    /// `gc()` requires `&mut VersionChain`, which means the caller must hold
    /// a DashMap write lock on the shard (via `entry()` or `get_mut()`).
    /// `get_at_version()` takes `&self` and is reached through a DashMap read
    /// lock (via `get()`). DashMap's per-shard RwLock guarantees mutual
    /// exclusion between readers and writers, so GC cannot run while any
    /// read is in progress on the same shard, and vice versa.
    #[test]
    fn test_version_chain_gc_concurrent_reads() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "gc_test");

        // Build up a version chain with many versions
        for i in 1..=100 {
            Storage::put_with_version(&*store, key.clone(), Value::Int(i), i as u64, None).unwrap();
        }

        // Verify all versions exist
        let history = Storage::get_history(&*store, &key, None, None).unwrap();
        assert_eq!(history.len(), 100);

        // Run concurrent reads and simulated GC pressure
        let stop = Arc::new(AtomicBool::new(false));

        // Reader threads
        let reader_handles: Vec<_> = (0..4)
            .map(|_| {
                let store = Arc::clone(&store);
                let key = key.clone();
                let stop = Arc::clone(&stop);

                thread::spawn(move || {
                    let mut reads = 0;
                    while !stop.load(Ordering::Relaxed) && reads < 1000 {
                        // Read at various versions
                        for v in [1, 25, 50, 75, 100] {
                            let result = Storage::get_versioned(&*store, &key, v).unwrap();
                            if let Some(vv) = result {
                                assert!(
                                    vv.version.as_u64() <= v,
                                    "Read version {} but requested max {}",
                                    vv.version.as_u64(),
                                    v
                                );
                            }
                        }
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();

        // Writer thread that causes potential GC
        let writer_handle = {
            let store = Arc::clone(&store);
            let key = key.clone();
            let stop = Arc::clone(&stop);

            thread::spawn(move || {
                let mut version = 101;
                while !stop.load(Ordering::Relaxed) && version < 200 {
                    Storage::put_with_version(
                        &*store,
                        key.clone(),
                        Value::Int(version),
                        version as u64,
                        None,
                    )
                    .unwrap();
                    version += 1;
                }
                version
            })
        };

        // Let it run for a bit
        thread::sleep(std::time::Duration::from_millis(100));
        stop.store(true, Ordering::Relaxed);

        // Collect results - no panics means no data races detected
        for h in reader_handles {
            let reads = h.join().unwrap();
            assert!(reads > 0, "Reader thread did some work");
        }
        let final_version = writer_handle.join().unwrap();
        assert!(final_version > 101, "Writer thread did some work");
    }

    /// SAFETY: apply_batch atomicity under failure
    ///
    /// `apply_batch` groups operations by branch_id and holds the DashMap
    /// shard write lock for each branch's entire batch, making writes
    /// atomic within a single branch. Across branches, partial application
    /// is theoretically possible, but acceptable: branches are independent
    /// domains and each branch's batch is self-consistent. In practice the
    /// individual `insert`/`push` operations are infallible (no I/O, no
    /// allocation failure on these small structures), so mid-batch failure
    /// does not occur.
    #[test]
    fn test_apply_batch_partial_application() {
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();

        // Create batch with many keys
        let keys: Vec<_> = (0..10)
            .map(|i| create_test_key(branch_id, &format!("batch_key_{}", i)))
            .collect();

        let writes: Vec<_> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.clone(), Value::Int(i as i64)))
            .collect();

        // Apply batch
        store.apply_batch(writes, vec![], 1).unwrap();

        // All keys should be written with same version
        for (i, key) in keys.iter().enumerate() {
            let result = Storage::get(&store, key).unwrap();
            assert!(result.is_some(), "Key {} should exist after batch", i);
            let vv = result.unwrap();
            assert_eq!(vv.version.as_u64(), 1, "All keys should have version 1");
        }
    }

    /// SAFETY: fetch_max semantics for version updates
    ///
    /// `apply_batch` uses `AtomicU64::fetch_max(version, AcqRel)` to advance
    /// the global version counter. This is a single atomic operation that
    /// correctly handles out-of-order batch application: the version only
    /// moves forward, never backward, regardless of arrival order.
    #[test]
    fn test_version_fetch_max_out_of_order() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "out_of_order");

        // Apply batch at version 100 first
        store
            .apply_batch(vec![(key.clone(), Value::Int(100))], vec![], 100)
            .unwrap();
        assert_eq!(store.version(), 100);

        // Apply batch at version 50 (older) - version should stay at 100
        store
            .apply_batch(vec![(key.clone(), Value::Int(50))], vec![], 50)
            .unwrap();
        assert_eq!(
            store.version(),
            100,
            "Version should not decrease with fetch_max"
        );

        // Apply batch at version 150 - version should update
        store
            .apply_batch(vec![(key.clone(), Value::Int(150))], vec![], 150)
            .unwrap();
        assert_eq!(store.version(), 150);
    }

    /// SAFETY: Empty version chain after multiple deletes
    ///
    /// Deleting a nonexistent key is handled gracefully: `Storage::delete`
    /// returns `Ok(None)` when the key is not found. No version chain is
    /// created for a key that was never written, so there is no risk of
    /// an empty or corrupted chain.
    #[test]
    fn test_delete_nonexistent_key_no_panic() {
        use strata_core::traits::Storage;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "never_existed");

        // Delete should not panic
        let result = Storage::delete(&store, &key).unwrap();
        assert!(result.is_none(), "Delete of nonexistent key returns None");

        // Multiple deletes should be safe
        let result2 = Storage::delete(&store, &key).unwrap();
        assert!(result2.is_none());
    }

    /// SAFETY: Scan operations with expired TTL values
    ///
    /// Both `Storage::scan_prefix` and `SnapshotView::scan_prefix` explicitly
    /// check `!sv.is_expired() && !sv.is_tombstone()` before including a
    /// value in results. Expired TTL values are correctly filtered out.
    #[test]
    fn test_scan_filters_expired_ttl() {
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();

        // Create a mix of expired and non-expired values
        let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

        // Non-expired key
        let key1 = Key::new_kv(ns.clone(), "fresh");
        Storage::put(&store, key1.clone(), Value::Int(1), None).unwrap();

        // "Expired" key - we'll simulate by using internal API
        let key2 = Key::new_kv(ns.clone(), "expired");
        let old_ts = Timestamp::from_micros(0); // epoch
        let expired_value = StoredValue::with_timestamp(
            Value::Int(2),
            Version::txn(2),
            old_ts,
            Some(std::time::Duration::from_secs(1)), // expired long ago
        );
        store
            .put(key2.clone(), expired_value, WriteMode::Append)
            .unwrap();

        // Update store version
        store.set_version(2);

        // Scan should only return the fresh key
        let prefix = Key::new_kv(ns.clone(), "");
        let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

        assert_eq!(results.len(), 1, "Scan should filter out expired values");
        assert!(
            results[0].0.user_key_string().unwrap().contains("fresh"),
            "Only fresh key should be returned"
        );
    }

    // ========================================================================
    // BTreeSet Index Tests
    // ========================================================================

    #[test]
    fn test_ordered_keys_consistent_with_data() {
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();

        // Insert keys
        for i in 0..20 {
            let key = create_test_key(branch_id, &format!("key_{:03}", i));
            Storage::put(&store, key, Value::Int(i), None).unwrap();
        }

        // Overwrite some keys (should not duplicate in BTreeSet)
        for i in 0..10 {
            let key = create_test_key(branch_id, &format!("key_{:03}", i));
            Storage::put(&store, key, Value::Int(i + 100), None).unwrap();
        }

        // Delete some keys (tombstone — key stays in both structures)
        for i in 15..20 {
            let key = create_test_key(branch_id, &format!("key_{:03}", i));
            Storage::delete(&store, &key).unwrap();
        }

        // Ensure ordered_keys is rebuilt lazily, then verify consistency
        let mut shard = store.shards.get_mut(&branch_id).unwrap();
        shard.ensure_ordered_keys();
        let ordered = shard.ordered_keys.as_ref().unwrap();
        assert_eq!(
            ordered.len(),
            shard.data.len(),
            "ordered_keys and data must have the same number of keys"
        );

        // Every key in ordered_keys must exist in data
        for k in ordered.iter() {
            assert!(
                shard.data.contains_key(k),
                "ordered_keys has key not in data"
            );
        }
        // Every key in data must exist in ordered_keys
        for k in shard.data.keys() {
            assert!(ordered.contains(k), "data has key not in ordered_keys");
        }
    }

    #[test]
    fn test_prefix_scan_sorted_without_explicit_sort() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));

        // Insert keys in reverse order
        let names = vec![
            "user:zara",
            "user:mike",
            "user:alice",
            "user:bob",
            "user:charlie",
        ];
        for name in &names {
            Storage::put(&store, Key::new_kv(ns.clone(), name), Value::Int(1), None).unwrap();
        }

        let prefix = Key::new_kv(ns.clone(), "user:");
        let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

        assert_eq!(results.len(), 5);
        // Verify sorted order
        for i in 0..results.len() - 1 {
            assert!(
                results[i].0 < results[i + 1].0,
                "Results must be sorted: {:?} should be < {:?}",
                results[i].0.user_key_string(),
                results[i + 1].0.user_key_string(),
            );
        }
    }

    #[test]
    fn test_prefix_scan_with_tombstones() {
        use strata_core::traits::{SnapshotView, Storage};
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = Arc::new(ShardedStore::new());
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));

        // Insert 3 keys at version 1, 2, 3
        Storage::put(
            &*store,
            Key::new_kv(ns.clone(), "item:a"),
            Value::Int(1),
            None,
        )
        .unwrap();
        Storage::put(
            &*store,
            Key::new_kv(ns.clone(), "item:b"),
            Value::Int(2),
            None,
        )
        .unwrap();
        Storage::put(
            &*store,
            Key::new_kv(ns.clone(), "item:c"),
            Value::Int(3),
            None,
        )
        .unwrap();

        // Take snapshot before delete (version = 3)
        let snap_before = store.snapshot();

        // Delete item:b (tombstone at version 4)
        Storage::delete(&*store, &Key::new_kv(ns.clone(), "item:b")).unwrap();

        // Take snapshot after delete (version = 4)
        let snap_after = store.snapshot();

        let prefix = Key::new_kv(ns.clone(), "item:");

        // Snapshot before delete should see all 3
        let results_before = SnapshotView::scan_prefix(&snap_before, &prefix).unwrap();
        assert_eq!(
            results_before.len(),
            3,
            "Pre-delete snapshot sees all 3 items"
        );

        // Snapshot after delete should see 2 (item:b is tombstoned)
        let results_after = SnapshotView::scan_prefix(&snap_after, &prefix).unwrap();
        assert_eq!(results_after.len(), 2, "Post-delete snapshot sees 2 items");
        let keys_after: Vec<_> = results_after
            .iter()
            .map(|(k, _)| k.user_key_string().unwrap())
            .collect();
        assert!(keys_after.contains(&"item:a".to_string()));
        assert!(keys_after.contains(&"item:c".to_string()));
    }

    #[test]
    fn test_prefix_scan_scales_with_matches() {
        use strata_core::traits::Storage;
        use strata_core::types::Namespace;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(Namespace::for_branch(branch_id));

        // Insert 5000 keys with prefix "alpha:" and 5000 with prefix "beta:"
        for i in 0..5000 {
            Storage::put(
                &store,
                Key::new_kv(ns.clone(), format!("alpha:{:05}", i)),
                Value::Int(i),
                None,
            )
            .unwrap();
            Storage::put(
                &store,
                Key::new_kv(ns.clone(), format!("beta:{:05}", i)),
                Value::Int(i),
                None,
            )
            .unwrap();
        }

        // Scan "alpha:" should return exactly 5000
        let prefix_a = Key::new_kv(ns.clone(), "alpha:");
        let results_a = Storage::scan_prefix(&store, &prefix_a, u64::MAX).unwrap();
        assert_eq!(
            results_a.len(),
            5000,
            "alpha: prefix should match 5000 keys"
        );

        // Scan "beta:" should return exactly 5000
        let prefix_b = Key::new_kv(ns.clone(), "beta:");
        let results_b = Storage::scan_prefix(&store, &prefix_b, u64::MAX).unwrap();
        assert_eq!(results_b.len(), 5000, "beta: prefix should match 5000 keys");

        // Scan non-existent prefix should return 0
        let prefix_none = Key::new_kv(ns.clone(), "gamma:");
        let results_none = Storage::scan_prefix(&store, &prefix_none, u64::MAX).unwrap();
        assert_eq!(results_none.len(), 0, "gamma: prefix should match 0 keys");
    }

    // ========================================================================
    // Direct-path and Lazy BTreeSet Tests
    // ========================================================================

    #[test]
    fn test_get_direct_basic() {
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "direct_key");

        // Should return None for missing keys
        assert!(store.get_direct(&key).is_none());

        // Insert a value
        Storage::put(&store, key.clone(), Value::Int(42), None).unwrap();

        // Should return the latest value
        let result = store.get_direct(&key);
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Value::Int(42));
    }

    #[test]
    fn test_get_direct_filters_tombstones() {
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "tombstone_key");

        Storage::put(&store, key.clone(), Value::Int(1), None).unwrap();
        assert!(store.get_direct(&key).is_some());

        // Delete creates a tombstone
        Storage::delete(&store, &key).unwrap();
        assert!(
            store.get_direct(&key).is_none(),
            "get_direct must filter tombstones"
        );
    }

    #[test]
    fn test_get_direct_filters_expired_ttl() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "ttl_key");

        // Insert with TTL that's already expired
        let expired_value = StoredValue::with_timestamp(
            Value::Int(99),
            Version::txn(1),
            Timestamp::from_micros(1),               // ancient timestamp
            Some(std::time::Duration::from_secs(1)), // expired long ago
        );
        store
            .put(key.clone(), expired_value, WriteMode::Append)
            .unwrap();

        assert!(
            store.get_direct(&key).is_none(),
            "get_direct must filter expired values"
        );
    }

    #[test]
    fn test_get_direct_returns_latest_version() {
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "multi_version");

        // Insert multiple versions
        Storage::put(&store, key.clone(), Value::Int(1), None).unwrap();
        Storage::put(&store, key.clone(), Value::Int(2), None).unwrap();
        Storage::put(&store, key.clone(), Value::Int(3), None).unwrap();

        let result = store.get_direct(&key).unwrap();
        assert_eq!(
            result.value,
            Value::Int(3),
            "get_direct must return the latest version"
        );
    }

    #[test]
    fn test_lazy_ordered_keys_invalidation_and_rebuild() {
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

        // Insert keys — ordered_keys starts as None (never built yet)
        Storage::put(&store, Key::new_kv(ns.clone(), "c"), Value::Int(3), None).unwrap();
        Storage::put(&store, Key::new_kv(ns.clone(), "a"), Value::Int(1), None).unwrap();
        Storage::put(&store, Key::new_kv(ns.clone(), "b"), Value::Int(2), None).unwrap();

        // ordered_keys should be None (not yet built)
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ordered_keys.is_none(),
                "ordered_keys should be None after insertions"
            );
        }

        // scan_prefix forces a rebuild via ensure_ordered_keys
        let prefix = Key::new_kv(ns.clone(), "");
        let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
        assert_eq!(results.len(), 3);

        // Verify sorted order after lazy rebuild
        let keys: Vec<String> = results
            .iter()
            .filter_map(|(k, _)| k.user_key_string())
            .collect();
        assert_eq!(
            keys,
            vec!["a", "b", "c"],
            "Keys must be sorted after lazy rebuild"
        );

        // ordered_keys should now be Some (rebuilt by the scan)
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ordered_keys.is_some(),
                "ordered_keys should be Some after scan triggered rebuild"
            );
        }

        // Insert a NEW key — should be incrementally added to ordered_keys
        Storage::put(&store, Key::new_kv(ns.clone(), "aa"), Value::Int(4), None).unwrap();
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ordered_keys.is_some(),
                "ordered_keys should remain Some after incremental insert"
            );
        }

        // Scan again — should see all 4 keys in order
        let results2 = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
        let keys2: Vec<String> = results2
            .iter()
            .filter_map(|(k, _)| k.user_key_string())
            .collect();
        assert_eq!(keys2, vec!["a", "aa", "b", "c"]);
    }

    #[test]
    fn test_lazy_ordered_keys_update_existing_no_invalidation() {
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

        // Insert initial key
        Storage::put(&store, Key::new_kv(ns.clone(), "k"), Value::Int(1), None).unwrap();

        // Force a rebuild by scanning
        let prefix = Key::new_kv(ns.clone(), "");
        let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

        // ordered_keys should now be Some
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(shard.ordered_keys.is_some());
        }

        // Update EXISTING key — should NOT invalidate ordered_keys
        Storage::put(&store, Key::new_kv(ns.clone(), "k"), Value::Int(2), None).unwrap();
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ordered_keys.is_some(),
                "Updating an existing key must not invalidate the ordered index"
            );
        }
    }

    #[test]
    fn test_incremental_ordered_keys_interleaved_insert_scan() {
        // Reproduces the Workload E pattern: interleaved inserts and scans.
        // Before the fix, each insert invalidated ordered_keys (set to None),
        // forcing an O(N) rebuild on every subsequent scan.
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

        // Seed with initial keys
        for i in 0..10 {
            Storage::put(
                &store,
                Key::new_kv(ns.clone(), &format!("key_{:03}", i)),
                Value::Int(i),
                None,
            )
            .unwrap();
        }

        // First scan — builds ordered_keys from scratch
        let prefix = Key::new_kv(ns.clone(), "");
        let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
        assert_eq!(results.len(), 10);
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(shard.ordered_keys.is_some());
        }

        // Interleaved insert-scan pattern (simulates Workload E)
        for i in 10..20 {
            // Insert a new key
            Storage::put(
                &store,
                Key::new_kv(ns.clone(), &format!("key_{:03}", i)),
                Value::Int(i),
                None,
            )
            .unwrap();

            // ordered_keys must remain Some (incremental insert, no invalidation)
            {
                let shard = store.shards.get(&branch_id).unwrap();
                assert!(
                    shard.ordered_keys.is_some(),
                    "ordered_keys invalidated on insert {i} — should be incrementally maintained"
                );
            }

            // Scan must return all keys so far, in sorted order
            let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
            assert_eq!(
                results.len(),
                i as usize + 1,
                "Expected {} keys after insert {i}",
                i + 1
            );

            // Verify sorted order
            let keys: Vec<String> = results
                .iter()
                .filter_map(|(k, _)| k.user_key_string())
                .collect();
            let mut sorted = keys.clone();
            sorted.sort();
            assert_eq!(keys, sorted, "Keys not sorted after insert {i}");
        }
    }

    #[test]
    fn test_incremental_ordered_keys_btreeset_content_after_inserts() {
        // Verifies that the BTreeSet content is exactly correct after
        // incremental inserts — not just that it's Some.
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

        // Insert initial keys and build ordered_keys via scan
        Storage::put(&store, Key::new_kv(ns.clone(), "b"), Value::Int(1), None).unwrap();
        Storage::put(&store, Key::new_kv(ns.clone(), "d"), Value::Int(2), None).unwrap();
        let prefix = Key::new_kv(ns.clone(), "");
        let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

        // Incrementally insert new keys
        Storage::put(&store, Key::new_kv(ns.clone(), "a"), Value::Int(3), None).unwrap();
        Storage::put(&store, Key::new_kv(ns.clone(), "c"), Value::Int(4), None).unwrap();
        Storage::put(&store, Key::new_kv(ns.clone(), "e"), Value::Int(5), None).unwrap();

        // Verify BTreeSet matches data keys exactly
        {
            let shard = store.shards.get(&branch_id).unwrap();
            let btree = shard.ordered_keys.as_ref().expect("should be Some");
            assert_eq!(
                btree.len(),
                shard.data.len(),
                "BTreeSet length must match data length"
            );
            for k in shard.data.keys() {
                assert!(btree.contains(k), "BTreeSet missing key from data: {:?}", k);
            }
            for k in btree.iter() {
                assert!(
                    shard.data.contains_key(k),
                    "BTreeSet has key not in data: {:?}",
                    k
                );
            }

            // Verify BTreeSet iteration order
            let btree_keys: Vec<String> =
                btree.iter().filter_map(|k| k.user_key_string()).collect();
            assert_eq!(btree_keys, vec!["a", "b", "c", "d", "e"]);
        }
    }

    #[test]
    fn test_incremental_ordered_keys_via_apply_batch() {
        // Verifies that apply_batch (commit path) incrementally maintains
        // ordered_keys instead of invalidating it.
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

        // Insert initial keys and build ordered_keys via scan
        Storage::put(&store, Key::new_kv(ns.clone(), "a"), Value::Int(1), None).unwrap();
        Storage::put(&store, Key::new_kv(ns.clone(), "c"), Value::Int(2), None).unwrap();
        let prefix = Key::new_kv(ns.clone(), "");
        let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ordered_keys.is_some(),
                "precondition: ordered_keys built"
            );
        }

        // apply_batch with new keys
        let writes = vec![
            (Key::new_kv(ns.clone(), "b"), Value::Int(3)),
            (Key::new_kv(ns.clone(), "d"), Value::Int(4)),
        ];
        store.apply_batch(writes, vec![], 10).unwrap();

        // ordered_keys must remain Some
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ordered_keys.is_some(),
                "apply_batch should not invalidate ordered_keys"
            );
            let btree = shard.ordered_keys.as_ref().unwrap();
            assert_eq!(btree.len(), 4, "BTreeSet should have all 4 keys");
        }

        // Scan must return all keys in sorted order
        let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
        let keys: Vec<String> = results
            .iter()
            .filter_map(|(k, _)| k.user_key_string())
            .collect();
        assert_eq!(keys, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_incremental_ordered_keys_via_apply_batch_deletes() {
        // Verifies that apply_batch deletes of nonexistent keys (which create
        // tombstone entries) also incrementally maintain ordered_keys.
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

        // Insert initial key and build ordered_keys via scan
        Storage::put(&store, Key::new_kv(ns.clone(), "a"), Value::Int(1), None).unwrap();
        let prefix = Key::new_kv(ns.clone(), "");
        let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(shard.ordered_keys.is_some());
        }

        // apply_batch: delete a key that doesn't exist yet (creates tombstone entry)
        let deletes = vec![Key::new_kv(ns.clone(), "z")];
        store.apply_batch(vec![], deletes, 10).unwrap();

        // ordered_keys must remain Some and include the new tombstone key
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ordered_keys.is_some(),
                "apply_batch delete should not invalidate ordered_keys"
            );
            let btree = shard.ordered_keys.as_ref().unwrap();
            assert_eq!(
                btree.len(),
                shard.data.len(),
                "BTreeSet length must match data length after tombstone insert"
            );
        }
    }

    #[test]
    fn test_incremental_ordered_keys_delete_nonexistent_key() {
        // Deleting a nonexistent key goes through put() to create a tombstone.
        // This must incrementally update ordered_keys.
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

        // Insert initial key and build ordered_keys via scan
        Storage::put(&store, Key::new_kv(ns.clone(), "a"), Value::Int(1), None).unwrap();
        let prefix = Key::new_kv(ns.clone(), "");
        let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(shard.ordered_keys.is_some());
        }

        // Delete a key that doesn't exist — creates tombstone via put()
        let nonexistent = Key::new_kv(ns.clone(), "z");
        Storage::delete(&store, &nonexistent).unwrap();

        // ordered_keys must remain Some and include the tombstone key
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ordered_keys.is_some(),
                "delete of nonexistent key should not invalidate ordered_keys"
            );
            let btree = shard.ordered_keys.as_ref().unwrap();
            assert_eq!(
                btree.len(),
                shard.data.len(),
                "BTreeSet must include the tombstone key"
            );
        }
    }

    #[test]
    fn test_incremental_ordered_keys_apply_batch_mixed() {
        // apply_batch with a mix of: updates to existing keys, new key writes,
        // and new key deletes. Only new keys should be added to the BTreeSet.
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::for_branch(branch_id));

        // Insert initial keys and build ordered_keys
        Storage::put(&store, Key::new_kv(ns.clone(), "a"), Value::Int(1), None).unwrap();
        Storage::put(&store, Key::new_kv(ns.clone(), "c"), Value::Int(2), None).unwrap();
        Storage::put(&store, Key::new_kv(ns.clone(), "e"), Value::Int(3), None).unwrap();
        let prefix = Key::new_kv(ns.clone(), "");
        let _ = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();

        // apply_batch: update "a" (existing), insert "b" (new), delete "d" (new tombstone)
        let writes = vec![
            (Key::new_kv(ns.clone(), "a"), Value::Int(10)), // existing
            (Key::new_kv(ns.clone(), "b"), Value::Int(20)), // new
        ];
        let deletes = vec![
            Key::new_kv(ns.clone(), "d"), // new (tombstone)
        ];
        store.apply_batch(writes, deletes, 10).unwrap();

        // ordered_keys must remain Some
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ordered_keys.is_some(),
                "mixed apply_batch should not invalidate ordered_keys"
            );
            let btree = shard.ordered_keys.as_ref().unwrap();
            // data should have: a, b, c, d (tombstone), e = 5 keys
            assert_eq!(btree.len(), 5);
            assert_eq!(shard.data.len(), 5);
            assert_eq!(btree.len(), shard.data.len());
        }

        // Scan returns non-tombstoned keys in sorted order
        let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
        let keys: Vec<String> = results
            .iter()
            .filter_map(|(k, _)| k.user_key_string())
            .collect();
        assert_eq!(keys, vec!["a", "b", "c", "e"]);
    }

    // ========================================================================
    // Dead Key GC Tests (Issue #1304)
    // ========================================================================

    #[test]
    fn test_version_chain_is_dead() {
        // Single tombstone → dead
        let chain = VersionChain::new(StoredValue::tombstone(Version::txn(1)));
        assert!(chain.is_dead());

        // Single expired value → dead
        let expired = StoredValue::with_timestamp(
            strata_core::value::Value::Int(1),
            Version::txn(1),
            Timestamp::from_micros(0),
            Some(std::time::Duration::from_secs(1)),
        );
        let chain = VersionChain::new(expired);
        assert!(chain.is_dead());

        // Single live value → not dead
        let chain = VersionChain::new(make_sv(1));
        assert!(!chain.is_dead());

        // Multi with tombstone latest + live older → not dead
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(StoredValue::tombstone(Version::txn(10)));
        assert!(!chain.is_dead());

        // After GC reduces to single tombstone → dead
        let mut chain = VersionChain::new(make_sv(1));
        chain.push(StoredValue::tombstone(Version::txn(10)));
        assert_eq!(chain.version_count(), 2);
        chain.gc(5); // Prunes v1, leaves v10 tombstone
        assert_eq!(chain.version_count(), 1);
        assert!(chain.is_dead());
    }

    #[test]
    fn test_gc_branch_removes_dead_keys_from_hashmap() {
        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::new(
            "t".to_string(),
            "a".to_string(),
            "g".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Insert a key, then tombstone it
        let key = Key::new_kv(ns.clone(), "victim");
        store.put(
            key.clone(),
            StoredValue::new(strata_core::value::Value::Int(1), Version::txn(1), None),
            WriteMode::Append,
        );
        store
            .put(
                key.clone(),
                StoredValue::tombstone(Version::txn(10)),
                WriteMode::Append,
            )
            .unwrap();

        // BTreeSet not built yet — verify GC handles the None case
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ordered_keys.is_none(),
                "BTreeSet should not exist yet"
            );
        }

        // GC with min_version high enough to prune the live version
        let pruned = store.gc_branch(branch_id, 5);
        assert_eq!(pruned, 1, "should report 1 version pruned (v1 live value)");

        // Key should be removed from HashMap
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            !shard.data.contains_key(&key),
            "dead key should be removed from HashMap after GC"
        );
        assert!(
            shard.ordered_keys.is_none(),
            "BTreeSet should remain None (never built)"
        );
    }

    #[test]
    fn test_gc_branch_removes_single_tombstone_no_prior_value() {
        // Key inserted directly as tombstone (no prior live value) —
        // exercises the Single(tombstone) → gc() noop → is_dead() true path
        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::new(
            "t".to_string(),
            "a".to_string(),
            "g".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key = Key::new_kv(ns.clone(), "orphan_tombstone");
        store
            .put(
                key.clone(),
                StoredValue::tombstone(Version::txn(5)),
                WriteMode::Append,
            )
            .unwrap();

        // gc() on Single is a noop (returns 0), but is_dead() should still
        // detect the single tombstone and remove it
        let pruned = store.gc_branch(branch_id, 100);
        assert_eq!(pruned, 0, "gc on Single is always 0");

        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            !shard.data.contains_key(&key),
            "single tombstone with no prior value should be removed"
        );
    }

    #[test]
    fn test_gc_branch_removes_dead_keys_from_btreeset() {
        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::new(
            "t".to_string(),
            "a".to_string(),
            "g".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key = Key::new_kv(ns.clone(), "victim");
        store.put(
            key.clone(),
            StoredValue::new(strata_core::value::Value::Int(1), Version::txn(1), None),
            WriteMode::Append,
        );
        store
            .put(
                key.clone(),
                StoredValue::tombstone(Version::txn(10)),
                WriteMode::Append,
            )
            .unwrap();

        // Trigger BTreeSet build before GC
        {
            let mut shard = store.shards.get_mut(&branch_id).unwrap();
            shard.ensure_ordered_keys();
            assert!(shard.ordered_keys.as_ref().unwrap().contains(&key));
        }

        store.gc_branch(branch_id, 5);

        let shard = store.shards.get(&branch_id).unwrap();
        let btree = shard.ordered_keys.as_ref().unwrap();
        assert!(
            !btree.contains(&key),
            "dead key should be removed from BTreeSet after GC"
        );
    }

    #[test]
    fn test_gc_branch_keeps_alive_keys() {
        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::new(
            "t".to_string(),
            "a".to_string(),
            "g".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key_a = Key::new_kv(ns.clone(), "alive");
        let key_b = Key::new_kv(ns.clone(), "dead");

        // Key A: live value
        store.put(
            key_a.clone(),
            StoredValue::new(strata_core::value::Value::Int(1), Version::txn(1), None),
            WriteMode::Append,
        );

        // Key B: value then tombstone
        store.put(
            key_b.clone(),
            StoredValue::new(strata_core::value::Value::Int(2), Version::txn(2), None),
            WriteMode::Append,
        );
        store
            .put(
                key_b.clone(),
                StoredValue::tombstone(Version::txn(10)),
                WriteMode::Append,
            )
            .unwrap();

        // Build BTreeSet
        {
            let mut shard = store.shards.get_mut(&branch_id).unwrap();
            shard.ensure_ordered_keys();
        }

        let pruned = store.gc_branch(branch_id, 5);
        assert_eq!(pruned, 1, "only v2 of key_b should be pruned");

        let shard = store.shards.get(&branch_id).unwrap();
        // A remains in HashMap and BTreeSet with correct value
        assert!(shard.data.contains_key(&key_a));
        assert!(shard.ordered_keys.as_ref().unwrap().contains(&key_a));
        let chain_a = shard.data.get(&key_a).unwrap();
        assert_eq!(chain_a.version_count(), 1);
        assert!(!chain_a.is_dead());
        // B removed from both
        assert!(!shard.data.contains_key(&key_b));
        assert!(!shard.ordered_keys.as_ref().unwrap().contains(&key_b));
    }

    #[test]
    fn test_gc_branch_keeps_multi_reduced_to_live() {
        // Critical negative case: Multi chain reduced to a single LIVE value
        // by GC must NOT be treated as dead. Guards against is_dead() regressing
        // to just `version_count() == 1`.
        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::new(
            "t".to_string(),
            "a".to_string(),
            "g".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key = Key::new_kv(ns.clone(), "survivor");
        // v5: old value, v10: updated value (both live)
        store.put(
            key.clone(),
            StoredValue::new(strata_core::value::Value::Int(1), Version::txn(5), None),
            WriteMode::Append,
        );
        store.put(
            key.clone(),
            StoredValue::new(strata_core::value::Value::Int(2), Version::txn(10), None),
            WriteMode::Append,
        );

        // Build BTreeSet before GC
        {
            let mut shard = store.shards.get_mut(&branch_id).unwrap();
            shard.ensure_ordered_keys();
        }

        // GC with min_version=8 → prunes v5, leaves v10 (live)
        let pruned = store.gc_branch(branch_id, 8);
        assert_eq!(pruned, 1, "v5 should be pruned");

        let shard = store.shards.get(&branch_id).unwrap();
        // Key MUST survive — single live value is not dead
        assert!(
            shard.data.contains_key(&key),
            "key reduced to single LIVE value must NOT be removed"
        );
        assert!(shard.ordered_keys.as_ref().unwrap().contains(&key));
        let chain = shard.data.get(&key).unwrap();
        assert_eq!(chain.version_count(), 1);
        assert!(!chain.is_dead());
        assert_eq!(chain.latest().unwrap().version(), Version::txn(10));
    }

    #[test]
    fn test_gc_branch_keeps_key_with_multiple_versions() {
        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::new(
            "t".to_string(),
            "a".to_string(),
            "g".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key = Key::new_kv(ns.clone(), "multi");
        // v10: live value, v20: tombstone
        store.put(
            key.clone(),
            StoredValue::new(strata_core::value::Value::Int(1), Version::txn(10), None),
            WriteMode::Append,
        );
        store
            .put(
                key.clone(),
                StoredValue::tombstone(Version::txn(20)),
                WriteMode::Append,
            )
            .unwrap();

        // Build BTreeSet to verify it's maintained across progressive GC
        {
            let mut shard = store.shards.get_mut(&branch_id).unwrap();
            shard.ensure_ordered_keys();
        }

        // GC with min_version=5 → too low to prune v10, key stays
        let pruned = store.gc_branch(branch_id, 5);
        assert_eq!(
            pruned, 0,
            "nothing to prune when min_version < all versions"
        );
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.data.contains_key(&key),
                "key with multiple versions should survive low min_version GC"
            );
            assert!(
                shard.ordered_keys.as_ref().unwrap().contains(&key),
                "key should remain in BTreeSet after surviving GC"
            );
            assert_eq!(shard.data.get(&key).unwrap().version_count(), 2);
        }

        // GC with min_version=15 → prunes v10, leaves only v20 tombstone → dead
        let pruned = store.gc_branch(branch_id, 15);
        assert_eq!(pruned, 1, "v10 should be pruned");
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                !shard.data.contains_key(&key),
                "key reduced to single tombstone should be removed from HashMap"
            );
            assert!(
                !shard.ordered_keys.as_ref().unwrap().contains(&key),
                "key reduced to single tombstone should be removed from BTreeSet"
            );
        }
    }

    #[test]
    fn test_ensure_ordered_keys_skips_dead_keys() {
        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::new(
            "t".to_string(),
            "a".to_string(),
            "g".to_string(),
            branch_id,
            "default".to_string(),
        ));

        let key_live = Key::new_kv(ns.clone(), "live");
        let key_dead = Key::new_kv(ns.clone(), "dead");

        // Live key
        store.put(
            key_live.clone(),
            StoredValue::new(strata_core::value::Value::Int(1), Version::txn(1), None),
            WriteMode::Append,
        );

        // Dead key (tombstone only)
        store
            .put(
                key_dead.clone(),
                StoredValue::tombstone(Version::txn(2)),
                WriteMode::Append,
            )
            .unwrap();

        // Build BTreeSet — should skip the dead key
        {
            let mut shard = store.shards.get_mut(&branch_id).unwrap();
            shard.ensure_ordered_keys();
            let btree = shard.ordered_keys.as_ref().unwrap();
            assert!(btree.contains(&key_live));
            assert!(
                !btree.contains(&key_dead),
                "ensure_ordered_keys should skip dead (tombstone-only) keys"
            );
            // HashMap must still contain the dead key — filtering is BTreeSet-only
            assert!(
                shard.data.contains_key(&key_dead),
                "ensure_ordered_keys must NOT remove from HashMap"
            );
            assert_eq!(btree.len(), 1, "BTreeSet should only contain the live key");
            assert_eq!(shard.data.len(), 2, "HashMap should contain both keys");
        }
    }

    #[test]
    fn test_scan_prefix_after_gc_removes_dead_keys() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch_id = BranchId::new();
        let ns = Arc::new(strata_core::types::Namespace::new(
            "t".to_string(),
            "a".to_string(),
            "g".to_string(),
            branch_id,
            "default".to_string(),
        ));

        // Insert keys: alpha (live), beta (will be tombstoned), gamma (live)
        Storage::put(
            &store,
            Key::new_kv(ns.clone(), "alpha"),
            Value::Int(1),
            None,
        )
        .unwrap();
        Storage::put(&store, Key::new_kv(ns.clone(), "beta"), Value::Int(2), None).unwrap();
        Storage::put(
            &store,
            Key::new_kv(ns.clone(), "gamma"),
            Value::Int(3),
            None,
        )
        .unwrap();

        // Tombstone beta
        let beta_key = Key::new_kv(ns.clone(), "beta");
        store
            .put(
                beta_key.clone(),
                StoredValue::tombstone(Version::txn(100)),
                WriteMode::Append,
            )
            .unwrap();

        // Scan before GC — beta should not appear (tombstoned)
        let prefix = Key::new_kv(ns.clone(), "");
        let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
        let keys: Vec<String> = results
            .iter()
            .filter_map(|(k, _)| k.user_key_string())
            .collect();
        assert_eq!(keys, vec!["alpha", "gamma"]);

        // Verify beta is still in internal structures before GC
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.data.contains_key(&beta_key),
                "beta should still be in HashMap before GC"
            );
            assert_eq!(shard.data.len(), 3, "all 3 keys in HashMap before GC");
        }

        // Run GC to actually remove dead keys
        store.gc_branch(branch_id, 50);

        // Verify beta is gone from internal structures
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(!shard.data.contains_key(&beta_key));
            assert!(!shard.ordered_keys.as_ref().unwrap().contains(&beta_key));
            assert_eq!(shard.data.len(), 2, "only live keys remain after GC");
        } // Drop read guard before scan_prefix needs get_mut

        // Scan after GC — same result, live keys still present with correct values
        let results = Storage::scan_prefix(&store, &prefix, u64::MAX).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0.user_key_string().unwrap(), "alpha");
        assert_eq!(results[0].1.value, Value::Int(1));
        assert_eq!(results[1].0.user_key_string().unwrap(), "gamma");
        assert_eq!(results[1].1.value, Value::Int(3));
    }

    // ========================================================================
    // TTL Index Wiring Tests (S-4)
    // ========================================================================

    fn make_sv_with_ttl(version: u64, ttl_secs: u64) -> StoredValue {
        StoredValue::with_timestamp(
            strata_core::value::Value::Int(version as i64),
            Version::txn(version),
            Timestamp::from_micros(1_000_000), // fixed ts=1s for predictable expiry
            Some(std::time::Duration::from_secs(ttl_secs)),
        )
    }

    fn make_expired_sv(version: u64) -> StoredValue {
        // Timestamp at epoch + 1ms TTL → definitely expired
        StoredValue::with_timestamp(
            strata_core::value::Value::Int(version as i64),
            Version::txn(version),
            Timestamp::from_micros(0),
            Some(std::time::Duration::from_millis(1)),
        )
    }

    fn test_ns_and_key(suffix: &str) -> Key {
        use strata_core::types::Namespace;
        let branch_id = BranchId::from_bytes([1u8; 16]);
        let ns = Arc::new(Namespace::new(
            "t".to_string(),
            "a".to_string(),
            "g".to_string(),
            branch_id,
            "default".to_string(),
        ));
        Key::new_kv(ns, suffix)
    }

    #[test]
    fn test_put_with_ttl_updates_ttl_index() {
        let store = ShardedStore::new();
        let key = test_ns_and_key("ttl_key");
        let sv = make_sv_with_ttl(1, 60);
        let expected_expiry = sv.expiry_timestamp().unwrap();

        store.put(key.clone(), sv, WriteMode::Append).unwrap();

        // Verify TTL index has the entry
        let shard = store.shards.get(&key.namespace.branch_id).unwrap();
        assert_eq!(shard.ttl_index.len(), 1);
        let expired = shard.ttl_index.find_expired(expected_expiry);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0], key);
    }

    #[test]
    fn test_put_overwrite_removes_old_ttl() {
        let store = ShardedStore::new();
        let key = test_ns_and_key("overwrite_key");

        // First put with TTL=60s
        let sv1 = make_sv_with_ttl(1, 60);
        let old_expiry = sv1.expiry_timestamp().unwrap();
        store.put(key.clone(), sv1, WriteMode::Append).unwrap();

        // Overwrite with TTL=120s
        let sv2 = make_sv_with_ttl(2, 120);
        let new_expiry = sv2.expiry_timestamp().unwrap();
        store.put(key.clone(), sv2, WriteMode::Append).unwrap();

        let shard = store.shards.get(&key.namespace.branch_id).unwrap();
        // Should have exactly 1 TTL entry (the new one), not 2
        assert_eq!(shard.ttl_index.len(), 1);
        // Old expiry should find nothing
        let at_old = shard.ttl_index.find_expired(old_expiry);
        assert!(at_old.is_empty(), "Old TTL entry should be removed");
        // New expiry should find the key
        let at_new = shard.ttl_index.find_expired(new_expiry);
        assert_eq!(at_new.len(), 1);
    }

    #[test]
    fn test_put_overwrite_ttl_with_no_ttl_cleans_index() {
        let store = ShardedStore::new();
        let key = test_ns_and_key("ttl_to_none");

        // First put with TTL
        store
            .put(key.clone(), make_sv_with_ttl(1, 60), WriteMode::Append)
            .unwrap();
        {
            let shard = store.shards.get(&key.namespace.branch_id).unwrap();
            assert_eq!(shard.ttl_index.len(), 1);
        }

        // Overwrite without TTL
        store
            .put(key.clone(), make_sv(2), WriteMode::Append)
            .unwrap();
        {
            let shard = store.shards.get(&key.namespace.branch_id).unwrap();
            assert!(
                shard.ttl_index.is_empty(),
                "TTL index should be empty after overwrite with non-TTL value"
            );
        }
    }

    #[test]
    fn test_put_without_ttl_no_index_overhead() {
        let store = ShardedStore::new();
        let key = test_ns_and_key("no_ttl");

        store
            .put(key.clone(), make_sv(1), WriteMode::Append)
            .unwrap();

        let shard = store.shards.get(&key.namespace.branch_id).unwrap();
        assert!(
            shard.ttl_index.is_empty(),
            "Non-TTL put should not touch TTL index"
        );
    }

    #[test]
    fn test_delete_over_ttl_key_cleans_index() {
        let store = ShardedStore::new();
        let key = test_ns_and_key("delete_ttl");

        // Put with TTL
        store
            .put(key.clone(), make_sv_with_ttl(1, 60), WriteMode::Append)
            .unwrap();
        {
            let shard = store.shards.get(&key.namespace.branch_id).unwrap();
            assert_eq!(shard.ttl_index.len(), 1);
        }

        // Delete (tombstone) — should clean the TTL entry
        store
            .put(
                key.clone(),
                StoredValue::tombstone(Version::txn(2)),
                WriteMode::Append,
            )
            .unwrap();
        {
            let shard = store.shards.get(&key.namespace.branch_id).unwrap();
            assert!(
                shard.ttl_index.is_empty(),
                "Tombstone over TTL key should clean TTL index"
            );
        }
    }

    #[test]
    fn test_expire_ttl_keys_cleans_index() {
        let store = ShardedStore::new();
        let key1 = test_ns_and_key("exp1");
        let key2 = test_ns_and_key("exp2");

        // Insert two keys with TTLs that are already expired (epoch + 1ms)
        store
            .put(key1.clone(), make_expired_sv(1), WriteMode::Append)
            .unwrap();
        store
            .put(key2.clone(), make_expired_sv(2), WriteMode::Append)
            .unwrap();

        {
            let shard = store.shards.get(&key1.namespace.branch_id).unwrap();
            assert_eq!(shard.ttl_index.len(), 2);
        }

        // Expire — should clean both index entries
        let removed = store.expire_ttl_keys(Timestamp::now());
        assert_eq!(removed, 2);

        {
            let shard = store.shards.get(&key1.namespace.branch_id).unwrap();
            assert!(shard.ttl_index.is_empty());
        }
    }

    #[test]
    fn test_gc_branch_cleans_ttl_entries_for_dead_keys() {
        let store = ShardedStore::new();
        let key = test_ns_and_key("gc_ttl");
        let branch_id = key.namespace.branch_id;

        // Version 1: value with TTL (already expired)
        store
            .put(key.clone(), make_expired_sv(1), WriteMode::Append)
            .unwrap();
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert_eq!(shard.ttl_index.len(), 1);
        }

        // Version 2: tombstone (makes it dead after GC)
        store
            .put(
                key.clone(),
                StoredValue::tombstone(Version::txn(2)),
                WriteMode::Append,
            )
            .unwrap();

        // Tombstone should have cleaned the TTL entry from the put() path
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert!(
                shard.ttl_index.is_empty(),
                "TTL index should be clean after tombstone"
            );
        }

        // GC with min_version=3 should remove the dead key entirely
        let pruned = store.gc_branch(branch_id, 3);
        assert!(pruned > 0);

        // Key should be gone from data
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(shard.data.is_empty());
    }

    #[test]
    fn test_gc_branch_cleans_ttl_for_expired_only_key() {
        let store = ShardedStore::new();
        let key = test_ns_and_key("gc_exp_only");
        let branch_id = key.namespace.branch_id;

        // Single expired version (no tombstone, just expired TTL)
        store
            .put(key.clone(), make_expired_sv(1), WriteMode::Append)
            .unwrap();
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert_eq!(shard.ttl_index.len(), 1);
        }

        // GC with min_version=2 — the expired entry is dead, should be removed
        let pruned = store.gc_branch(branch_id, 2);
        // The chain is dead (expired), so key removed from data AND ttl index
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(shard.data.is_empty(), "Dead expired key should be removed");
        assert!(
            shard.ttl_index.is_empty(),
            "TTL entry for dead expired key should be cleaned"
        );
        let _ = pruned;
    }

    #[test]
    fn test_apply_batch_cleans_ttl_on_overwrite() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let key = test_ns_and_key("batch_ttl");
        let branch_id = key.namespace.branch_id;

        // First: put with TTL via the normal path
        store
            .put(key.clone(), make_sv_with_ttl(1, 60), WriteMode::Append)
            .unwrap();
        {
            let shard = store.shards.get(&branch_id).unwrap();
            assert_eq!(shard.ttl_index.len(), 1);
        }

        // Then: apply_batch overwrites the same key (no TTL in batch writes)
        store
            .apply_batch(vec![(key.clone(), Value::Int(99))], vec![], 2)
            .unwrap();

        // TTL entry should have been cleaned up
        let shard = store.shards.get(&branch_id).unwrap();
        assert!(
            shard.ttl_index.is_empty(),
            "apply_batch overwrite should clean stale TTL entry"
        );
    }

    // ========================================================================
    // Branch Limit Tests
    // ========================================================================

    #[test]
    fn test_branch_limit_allows_up_to_max() {
        use strata_core::value::Value;

        let store = ShardedStore::with_limits(3);
        for i in 0..3 {
            let branch_id = BranchId::new();
            let key = create_test_key(branch_id, &format!("k{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }
        assert_eq!(store.shard_count(), 3);
    }

    #[test]
    fn test_branch_limit_rejects_at_max() {
        use strata_core::value::Value;

        let store = ShardedStore::with_limits(2);
        for i in 0..2 {
            let branch_id = BranchId::new();
            let key = create_test_key(branch_id, &format!("k{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }
        // Third branch should fail
        let branch_id = BranchId::new();
        let key = create_test_key(branch_id, "overflow");
        let err = store
            .put(
                key,
                create_stored_value(Value::Int(99), 1),
                WriteMode::Append,
            )
            .unwrap_err();
        assert!(
            format!("{}", err).contains("branches"),
            "Error should mention branches: {}",
            err
        );
    }

    #[test]
    fn test_branch_limit_allows_write_to_existing() {
        use strata_core::value::Value;

        let store = ShardedStore::with_limits(1);
        let branch_id = BranchId::new();
        let key1 = create_test_key(branch_id, "k1");
        let key2 = create_test_key(branch_id, "k2");
        store
            .put(
                key1,
                create_stored_value(Value::Int(1), 1),
                WriteMode::Append,
            )
            .unwrap();
        // Same branch — should succeed even though limit is 1
        store
            .put(
                key2,
                create_stored_value(Value::Int(2), 2),
                WriteMode::Append,
            )
            .unwrap();
        assert_eq!(store.shard_count(), 1);
    }

    #[test]
    fn test_branch_limit_zero_unlimited() {
        use strata_core::value::Value;

        let store = ShardedStore::new(); // max_branches = 0 (unlimited)
        for i in 0..100 {
            let branch_id = BranchId::new();
            let key = create_test_key(branch_id, &format!("k{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }
        assert_eq!(store.shard_count(), 100);
    }

    #[test]
    fn test_branch_limit_apply_batch() {
        use strata_core::value::Value;

        let store = ShardedStore::with_limits(1);
        let b1 = BranchId::new();
        let key1 = create_test_key(b1, "k1");
        // First branch via apply_batch
        store
            .apply_batch(vec![(key1, Value::Int(1))], vec![], 1)
            .unwrap();

        // Second branch should fail
        let b2 = BranchId::new();
        let key2 = create_test_key(b2, "k2");
        let err = store
            .apply_batch(vec![(key2, Value::Int(2))], vec![], 2)
            .unwrap_err();
        assert!(format!("{}", err).contains("branches"));
    }

    // ========================================================================
    // Memory Stats Tests
    // ========================================================================

    #[test]
    fn test_memory_stats_empty() {
        let store = ShardedStore::new();
        let stats = store.memory_stats();
        assert_eq!(stats.total_branches, 0);
        assert_eq!(stats.total_entries, 0);
        assert_eq!(stats.estimated_bytes, 0);
        assert!(stats.per_branch.is_empty());
    }

    #[test]
    fn test_memory_stats_with_data() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let b1 = BranchId::new();
        let b2 = BranchId::new();

        // Insert 10 entries in branch 1
        for i in 0..10 {
            let key = create_test_key(b1, &format!("k{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }

        // Insert 5 entries in branch 2
        for i in 0..5 {
            let key = create_test_key(b2, &format!("k{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }

        let stats = store.memory_stats();
        assert_eq!(stats.total_branches, 2);
        assert_eq!(stats.total_entries, 15);
        assert!(stats.estimated_bytes > 0);
        assert_eq!(stats.per_branch.len(), 2);

        // Find branch stats
        let b1_stats = stats.per_branch.iter().find(|s| s.branch_id == b1).unwrap();
        assert_eq!(b1_stats.entry_count, 10);
        assert!(b1_stats.estimated_bytes > 0);
    }

    #[test]
    fn test_memory_stats_byte_formula() {
        use strata_core::value::Value;

        let store = ShardedStore::new();
        let branch = BranchId::new();

        // Insert exactly 10 entries
        for i in 0..10 {
            let key = create_test_key(branch, &format!("k{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }

        let stats = store.memory_stats();
        assert_eq!(stats.total_entries, 10);
        // 10 entries * 120 bytes/entry = 1200, no btree index by default
        assert_eq!(stats.estimated_bytes, 10 * 120);

        let branch_stats = &stats.per_branch[0];
        assert_eq!(branch_stats.estimated_bytes, 10 * 120);
        assert!(!branch_stats.has_btree_index);
    }

    #[test]
    fn test_set_max_branches_dynamic() {
        use strata_core::value::Value;

        let store = ShardedStore::new(); // unlimited
                                         // Create 5 branches
        let mut branches = Vec::new();
        for i in 0..5 {
            let branch = BranchId::new();
            branches.push(branch);
            let key = create_test_key(branch, &format!("k{}", i));
            store
                .put(
                    key,
                    create_stored_value(Value::Int(i), 1),
                    WriteMode::Append,
                )
                .unwrap();
        }
        assert_eq!(store.shard_count(), 5);

        // Now set a limit of 5 — existing branches still writable
        store.set_max_branches(5);
        let key = create_test_key(branches[0], "extra");
        store
            .put(
                key,
                create_stored_value(Value::Int(99), 2),
                WriteMode::Append,
            )
            .unwrap();

        // But a 6th branch should fail
        let new_branch = BranchId::new();
        let key = create_test_key(new_branch, "overflow");
        let err = store
            .put(
                key,
                create_stored_value(Value::Int(100), 3),
                WriteMode::Append,
            )
            .unwrap_err();
        assert!(format!("{}", err).contains("branches"));
    }

    #[test]
    fn test_branch_limit_delete_new_branch() {
        use strata_core::value::Value;

        let store = ShardedStore::with_limits(1);
        let b1 = BranchId::new();
        let key1 = create_test_key(b1, "k1");
        store
            .put(
                key1.clone(),
                create_stored_value(Value::Int(1), 1),
                WriteMode::Append,
            )
            .unwrap();

        // delete() on an existing branch is fine
        store.delete(&key1).unwrap();

        // delete() on a NEW branch should be rejected — delete inserts a
        // tombstone, which creates the shard if it doesn't exist.
        let b2 = BranchId::new();
        let key2 = create_test_key(b2, "phantom");
        let result = store.delete(&key2);
        // delete() calls put() internally, which checks the branch limit
        assert!(result.is_err());
    }

    #[test]
    fn test_branch_limit_via_storage_trait() {
        use strata_core::traits::Storage;
        use strata_core::value::Value;

        let store = ShardedStore::with_limits(1);
        let b1 = BranchId::new();
        let key1 = create_test_key(b1, "k1");
        // Storage::put via trait
        Storage::put(&store, key1, Value::Int(1), None).unwrap();

        // Second branch via trait should fail
        let b2 = BranchId::new();
        let key2 = create_test_key(b2, "k2");
        let err = Storage::put(&store, key2, Value::Int(2), None).unwrap_err();
        assert!(format!("{}", err).contains("branches"));
    }
}
