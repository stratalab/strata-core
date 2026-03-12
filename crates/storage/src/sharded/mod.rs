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

pub mod memory_stats;
pub mod shard;
pub mod version_chain;

#[cfg(test)]
mod tests;

pub use memory_stats::{BranchMemoryStats, StorageMemoryStats};
pub use shard::Shard;
pub use version_chain::VersionChain;

use dashmap::DashMap;
use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use strata_core::traits::{Storage, WriteMode};
use strata_core::types::{BranchId, Key};
use strata_core::value::Value;
use strata_core::{StrataError, StrataResult, Timestamp, Version, VersionedValue};

use crate::stored_value::StoredValue;

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
/// use strata_core::traits::Storage;
///
/// let store = ShardedStore::new();
/// let value = store.get_versioned(&key, store.version());
/// ```
pub struct ShardedStore {
    /// Per-branch shards using DashMap
    shards: DashMap<BranchId, Shard>,
    /// Global version for snapshots
    version: AtomicU64,
    /// Maximum number of branches allowed (0 = unlimited)
    max_branches: AtomicUsize,
    /// Maximum versions to retain per key (0 = unlimited).
    /// Applied to `WriteMode::Append` writes; explicit `KeepLast(n)` overrides.
    max_versions_per_key: AtomicUsize,
}

impl ShardedStore {
    /// Create new sharded store
    pub fn new() -> Self {
        Self {
            shards: DashMap::new(),
            version: AtomicU64::new(0),
            max_branches: AtomicUsize::new(0),
            max_versions_per_key: AtomicUsize::new(0),
        }
    }

    /// Create with expected number of branches
    pub fn with_capacity(num_branches: usize) -> Self {
        Self {
            shards: DashMap::with_capacity(num_branches),
            version: AtomicU64::new(0),
            max_branches: AtomicUsize::new(0),
            max_versions_per_key: AtomicUsize::new(0),
        }
    }

    /// Create with a branch count limit
    pub fn with_limits(max_branches: usize) -> Self {
        Self {
            shards: DashMap::new(),
            version: AtomicU64::new(0),
            max_branches: AtomicUsize::new(max_branches),
            max_versions_per_key: AtomicUsize::new(0),
        }
    }

    /// Set the maximum number of branches allowed (0 = unlimited).
    ///
    /// Callable after recovery without `&mut self`.
    pub fn set_max_branches(&self, max: usize) {
        self.max_branches.store(max, Ordering::Relaxed);
    }

    /// Set the maximum versions to retain per key (0 = unlimited).
    ///
    /// When set, `WriteMode::Append` writes are promoted to `KeepLast(max)`.
    /// Explicit `KeepLast(n)` overrides this global setting.
    pub fn set_max_versions_per_key(&self, max: usize) {
        self.max_versions_per_key.store(max, Ordering::Relaxed);
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

    // NOTE: `get_versioned()` is provided by the Storage trait implementation,
    // which includes proper TTL expiration checks.
    // Use `Storage::get_versioned()` trait method instead of an inherent method
    // to ensure consistent behavior across all callers.

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
        // Resolve effective mode: global default applies to Append only
        let effective_mode = match mode {
            WriteMode::Append => {
                let global_max = self.max_versions_per_key.load(Ordering::Relaxed);
                if global_max > 0 {
                    WriteMode::KeepLast(global_max)
                } else {
                    WriteMode::Append
                }
            }
            keep_last @ WriteMode::KeepLast(_) => keep_last, // per-write override
        };

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
            chain.put(value, effective_mode);
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

    /// Delete a key with a specific version (for batched deletes)
    ///
    /// MVCC-safe delete: adds a tombstone at the specified version instead
    /// of removing all versions. This preserves old versions for snapshots
    /// that may still need them.
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
    /// **Note:** This method always appends (ignores `max_versions_per_key`).
    /// It is designed for WAL recovery replay, where writes must be replayed
    /// exactly. Normal transaction commits go through `put_with_version_mode()`
    /// which respects the global version limit.
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

        // Branch limit — snapshot len() before entry() to avoid deadlock,
        // matching the same pattern used in put().
        let max = self.max_branches.load(Ordering::Relaxed);
        let current_len = if max > 0 { self.shards.len() } else { 0 };

        // Apply atomically per branch (hold shard lock for entire branch batch)
        for (branch_id, (branch_writes, branch_deletes)) in branch_ops {
            let mut shard = match self.shards.entry(branch_id) {
                dashmap::mapref::entry::Entry::Occupied(e) => e.into_ref(),
                dashmap::mapref::entry::Entry::Vacant(e) => {
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
// Storage Trait Implementation
// ============================================================================

impl Storage for ShardedStore {
    /// Get value at or before specified version (for snapshot isolation)
    ///
    /// Returns the value if version <= max_version, not expired, and not a tombstone.
    /// Pass `u64::MAX` to get the latest version unconditionally.
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

    /// Get current global version
    fn current_version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
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
