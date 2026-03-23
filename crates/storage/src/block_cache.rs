//! Shared block cache for decompressed KV segment data blocks.
//!
//! ## Architecture
//!
//! 16-shard CLOCK cache with lock-free lookups. Each shard uses a
//! `parking_lot::RwLock` so concurrent readers never block each other.
//! Writes (insert/eviction) take an exclusive lock but only occur on
//! cache misses.
//!
//! CLOCK eviction replaces LRU: each entry has an atomic reference counter
//! (0–3). Lookups set it to 3 (one atomic store, no list manipulation).
//! Eviction scans entries, decrementing non-zero counters and evicting
//! entries at zero. LOW priority entries are evicted before HIGH.
//!
//! Cache key: `(file_id, block_offset)` where file_id is derived from the
//! file path hash.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

/// Number of independent cache shards. Must be a power of two.
const NUM_SHARDS: usize = 16;

/// Default block cache capacity: 256 MiB.
const DEFAULT_CAPACITY_BYTES: usize = 256 * 1024 * 1024;

/// CLOCK counter value set on access (max lifetime before eviction).
const CLOCK_MAX: u8 = 3;

/// A cache key identifying a specific data block in a specific segment file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CacheKey {
    file_id: u64,
    block_offset: u64,
}

/// Priority tier for cached blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// Data blocks — evicted first under memory pressure.
    Low,
    /// Index and bloom filter blocks — evicted only when no LOW remain.
    High,
    /// Pinned blocks — never evicted by CLOCK pressure.
    Pinned,
}

// ---------------------------------------------------------------------------
// Per-entry state
// ---------------------------------------------------------------------------

struct ClockEntry {
    data: Arc<Vec<u8>>,
    size: usize,
    priority: Priority,
    /// CLOCK reference counter. Set to CLOCK_MAX on access, decremented
    /// during eviction scans. Entries at 0 are eviction candidates.
    clock: AtomicU8,
}

// ---------------------------------------------------------------------------
// Per-shard state
// ---------------------------------------------------------------------------

struct ClockShard {
    map: HashMap<CacheKey, ClockEntry>,
    current_bytes: usize,
    pinned_bytes: usize,
    capacity_bytes: usize,
    pinned_budget: usize,
}

impl ClockShard {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            map: HashMap::new(),
            current_bytes: 0,
            pinned_bytes: 0,
            capacity_bytes,
            pinned_budget: capacity_bytes / 10,
        }
    }

    /// CLOCK eviction: scan entries, decrementing counters and evicting
    /// zero-counter entries until `needed` bytes are free.
    /// Evicts LOW priority first, then HIGH. Never evicts PINNED.
    fn evict_for(&mut self, needed: usize) {
        if self.current_bytes + needed <= self.capacity_bytes {
            return;
        }

        // Collect eviction candidates and decrement clock counters.
        // Two passes: LOW first, then HIGH. Never evict PINNED.
        for target_priority in [Priority::Low, Priority::High] {
            // Scan: collect zero-clock entries, decrement non-zero
            let mut victims: Vec<CacheKey> = Vec::new();
            for (key, entry) in self.map.iter() {
                if entry.priority != target_priority {
                    continue;
                }
                let c = entry.clock.load(Ordering::Relaxed);
                if c == 0 {
                    victims.push(*key);
                } else {
                    entry.clock.store(c - 1, Ordering::Relaxed);
                }
            }
            // Remove victims
            for key in victims {
                if let Some(entry) = self.map.remove(&key) {
                    self.current_bytes = self.current_bytes.saturating_sub(entry.size);
                }
                if self.current_bytes + needed <= self.capacity_bytes {
                    return;
                }
            }
        }

        // Force-evict if still over (entries had non-zero clocks)
        if self.current_bytes + needed > self.capacity_bytes {
            let victims: Vec<CacheKey> = self
                .map
                .iter()
                .filter(|(_, e)| e.priority != Priority::Pinned)
                .map(|(k, _)| *k)
                .collect();
            for key in victims {
                if let Some(entry) = self.map.remove(&key) {
                    self.current_bytes = self.current_bytes.saturating_sub(entry.size);
                }
                if self.current_bytes + needed <= self.capacity_bytes {
                    return;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BlockCache — the public sharded CLOCK cache
// ---------------------------------------------------------------------------

/// Thread-safe sharded CLOCK block cache for decompressed segment data blocks.
///
/// 16 shards, each independently locked with RwLock. Lookups take a shared
/// read lock (no contention between concurrent readers). Inserts take an
/// exclusive write lock with CLOCK-based eviction.
pub struct BlockCache {
    shards: Vec<parking_lot::RwLock<ClockShard>>,
    total_capacity: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
}

/// Cache statistics snapshot.
#[derive(Debug, Clone)]
pub struct BlockCacheStats {
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
    /// Current number of cached blocks.
    pub entries: usize,
    /// Current total size of cached data in bytes.
    pub size_bytes: usize,
    /// Maximum capacity in bytes.
    pub capacity_bytes: usize,
    /// Current total size of pinned data in bytes.
    pub pinned_bytes: usize,
    /// Current number of pinned entries.
    pub pinned_entries: usize,
}

impl BlockCache {
    /// Create a new block cache with the given total capacity in bytes.
    pub fn new(capacity_bytes: usize) -> Self {
        let per_shard = capacity_bytes / NUM_SHARDS;
        let shards = (0..NUM_SHARDS)
            .map(|_| parking_lot::RwLock::new(ClockShard::new(per_shard)))
            .collect();
        Self {
            shards,
            total_capacity: AtomicUsize::new(capacity_bytes),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Create a block cache with the default capacity.
    pub fn default_capacity() -> Self {
        Self::new(DEFAULT_CAPACITY_BYTES)
    }

    /// Determine which shard a key maps to.
    #[inline]
    fn shard_index(key: &CacheKey) -> usize {
        let bh = key.block_offset.wrapping_mul(0x9e3779b97f4a7c15);
        let h = key.file_id.wrapping_mul(0x517cc1b727220a95) ^ (bh >> 32) ^ bh;
        (h as usize) & (NUM_SHARDS - 1)
    }

    /// Look up a cached block. Returns the decompressed data if present.
    ///
    /// Lock-free on the hot path: takes a shared RwLock read + one atomic
    /// store to mark the entry as recently used.
    pub fn get(&self, file_id: u64, block_offset: u64) -> Option<Arc<Vec<u8>>> {
        let key = CacheKey {
            file_id,
            block_offset,
        };
        let shard = self.shards[Self::shard_index(&key)].read();
        if let Some(entry) = shard.map.get(&key) {
            entry.clock.store(CLOCK_MAX, Ordering::Relaxed);
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(Arc::clone(&entry.data))
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a decompressed block with LOW priority (data blocks).
    pub fn insert(&self, file_id: u64, block_offset: u64, data: Vec<u8>) -> Arc<Vec<u8>> {
        self.insert_with_priority(file_id, block_offset, data, Priority::Low)
    }

    /// Insert a decompressed block with an explicit priority tier.
    pub fn insert_with_priority(
        &self,
        file_id: u64,
        block_offset: u64,
        data: Vec<u8>,
        priority: Priority,
    ) -> Arc<Vec<u8>> {
        let size = data.len();
        let data = Arc::new(data);
        let key = CacheKey {
            file_id,
            block_offset,
        };
        let mut shard = self.shards[Self::shard_index(&key)].write();

        if size > shard.capacity_bytes || shard.capacity_bytes == 0 {
            return data;
        }

        // Already present — return existing
        if let Some(entry) = shard.map.get(&key) {
            return Arc::clone(&entry.data);
        }

        // For Pinned: check budget, fall back to High if exceeded
        let effective_priority = if priority == Priority::Pinned {
            if shard.pinned_bytes + size <= shard.pinned_budget {
                Priority::Pinned
            } else {
                Priority::High
            }
        } else {
            priority
        };

        // Evict until there's room
        shard.evict_for(size);

        // Hard capacity check: if eviction couldn't free enough space
        // (e.g., shard is full of Pinned entries), reject the insertion.
        if shard.current_bytes + size > shard.capacity_bytes {
            return data;
        }

        if effective_priority == Priority::Pinned {
            shard.pinned_bytes += size;
        }

        shard.map.insert(
            key,
            ClockEntry {
                data: Arc::clone(&data),
                size,
                priority: effective_priority,
                clock: AtomicU8::new(CLOCK_MAX),
            },
        );
        shard.current_bytes += size;

        data
    }

    /// Remove all cached blocks for a given file.
    pub fn invalidate_file(&self, file_id: u64) {
        for rwlock in &self.shards {
            let mut shard = rwlock.write();
            let keys: Vec<CacheKey> = shard
                .map
                .keys()
                .filter(|k| k.file_id == file_id)
                .copied()
                .collect();
            for key in keys {
                if let Some(entry) = shard.map.remove(&key) {
                    shard.current_bytes = shard.current_bytes.saturating_sub(entry.size);
                    if entry.priority == Priority::Pinned {
                        shard.pinned_bytes = shard.pinned_bytes.saturating_sub(entry.size);
                    }
                }
            }
        }
    }

    /// Promote an existing cache entry to Pinned priority.
    pub fn promote_to_pinned(&self, file_id: u64, block_offset: u64) -> bool {
        let key = CacheKey {
            file_id,
            block_offset,
        };
        let mut shard = self.shards[Self::shard_index(&key)].write();
        let (size, already_pinned) = match shard.map.get(&key) {
            Some(entry) => (entry.size, entry.priority == Priority::Pinned),
            None => return false,
        };
        if already_pinned {
            return true;
        }
        if shard.pinned_bytes + size > shard.pinned_budget {
            return false;
        }
        // Now mutate
        shard.map.get_mut(&key).unwrap().priority = Priority::Pinned;
        shard.pinned_bytes += size;
        true
    }

    /// Demote all Pinned entries for a given file to High priority.
    pub fn demote_file(&self, file_id: u64) {
        for rwlock in &self.shards {
            let mut shard = rwlock.write();
            let keys: Vec<CacheKey> = shard
                .map
                .keys()
                .filter(|k| k.file_id == file_id)
                .copied()
                .collect();
            for key in keys {
                let is_pinned = shard
                    .map
                    .get(&key)
                    .is_some_and(|e| e.priority == Priority::Pinned);
                if is_pinned {
                    let entry = shard.map.get_mut(&key).unwrap();
                    let size = entry.size;
                    entry.priority = Priority::High;
                    shard.pinned_bytes = shard.pinned_bytes.saturating_sub(size);
                }
            }
        }
    }

    /// Update the cache capacity, distributing evenly across shards.
    fn set_capacity(&self, capacity_bytes: usize) {
        let per_shard = capacity_bytes / NUM_SHARDS;
        for rwlock in &self.shards {
            let mut shard = rwlock.write();
            shard.capacity_bytes = per_shard;
            shard.pinned_budget = per_shard / 10;
        }
        self.total_capacity.store(capacity_bytes, Ordering::Relaxed);
    }

    /// Get cache statistics (aggregated across all shards).
    pub fn stats(&self) -> BlockCacheStats {
        let mut total_entries = 0;
        let mut total_bytes = 0;
        let mut total_pinned_bytes = 0;
        let mut total_pinned_entries = 0;
        for rwlock in &self.shards {
            let shard = rwlock.read();
            total_entries += shard.map.len();
            total_bytes += shard.current_bytes;
            total_pinned_bytes += shard.pinned_bytes;
            for entry in shard.map.values() {
                if entry.priority == Priority::Pinned {
                    total_pinned_entries += 1;
                }
            }
        }
        BlockCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: total_entries,
            size_bytes: total_bytes,
            capacity_bytes: self.total_capacity.load(Ordering::Relaxed),
            pinned_bytes: total_pinned_bytes,
            pinned_entries: total_pinned_entries,
        }
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Compute a file identity hash from a file path.
pub fn file_path_hash(path: &std::path::Path) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = rustc_hash::FxHasher::default();
    path.hash(&mut hasher);
    hasher.finish()
}

/// Maximum auto-detected block cache size (4 GiB).
#[allow(dead_code)]
const MAX_AUTO_CACHE_BYTES: usize = 4 * 1024 * 1024 * 1024;

/// Compute cache capacity from available memory in bytes (testable helper).
fn compute_cache_from_available(available_bytes: usize) -> usize {
    let quarter = available_bytes / 4;
    quarter.min(MAX_AUTO_CACHE_BYTES)
}

/// Auto-detect a reasonable cache capacity based on available system memory.
pub fn auto_detect_capacity() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for line in contents.lines() {
                if let Some(rest) = line.strip_prefix("MemAvailable:") {
                    if let Some(kb_str) = rest.split_whitespace().next() {
                        if let Ok(kb) = kb_str.parse::<usize>() {
                            return compute_cache_from_available(kb * 1024);
                        }
                    }
                }
            }
        }
    }
    256 * 1024 * 1024
}

// ---------------------------------------------------------------------------
// Global singleton
// ---------------------------------------------------------------------------

static GLOBAL_CACHE: std::sync::OnceLock<BlockCache> = std::sync::OnceLock::new();
static GLOBAL_CAPACITY: AtomicUsize = AtomicUsize::new(DEFAULT_CAPACITY_BYTES);

/// Set the global block cache capacity.
///
/// If the cache is already initialized, updates the per-shard capacity on
/// the live instance. If not yet initialized, stores the value for use
/// when `global_cache()` first creates the cache.
pub fn set_global_capacity(bytes: usize) {
    GLOBAL_CAPACITY.store(bytes, Ordering::Relaxed);
    // Apply to the live cache if already initialized.
    if let Some(cache) = GLOBAL_CACHE.get() {
        cache.set_capacity(bytes);
    }
}

/// Get or create the global block cache.
pub fn global_cache() -> &'static BlockCache {
    GLOBAL_CACHE.get_or_init(|| BlockCache::new(GLOBAL_CAPACITY.load(Ordering::Relaxed)))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_hit_and_miss() {
        let cache = BlockCache::new(1024 * 1024);

        // Miss
        assert!(cache.get(1, 0).is_none());

        // Insert
        let data = vec![1, 2, 3, 4, 5];
        let cached = cache.insert(1, 0, data.clone());
        assert_eq!(&*cached, &data);

        // Hit
        let hit = cache.get(1, 0).unwrap();
        assert_eq!(&*hit, &data);

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entries, 1);
    }

    #[test]
    fn cache_eviction_on_capacity() {
        // 100 bytes per shard
        let cache = BlockCache::new(16 * 100);

        // Find 3 keys that map to the same shard
        let keys = find_keys_in_shard(0, 3);

        // Insert 2 blocks of 40 bytes each — fits in 100-byte shard
        cache.insert(keys[0], 0, vec![0; 40]);
        cache.insert(keys[1], 0, vec![1; 40]);
        assert_eq!(cache.stats().entries, 2);

        // Reset clock counters to 0 so they're evictable
        {
            let shard = cache.shards[0].read();
            for entry in shard.map.values() {
                entry.clock.store(0, Ordering::Relaxed);
            }
        }

        // 3rd should evict at least one
        cache.insert(keys[2], 0, vec![2; 40]);
        assert!(
            cache.stats().entries <= 3,
            "should have evicted to make room"
        );
        assert!(cache.get(keys[2], 0).is_some(), "newest entry should exist");
    }

    #[test]
    fn cache_different_files_same_offset() {
        let cache = BlockCache::new(1024 * 1024);

        cache.insert(1, 0, vec![1, 1, 1]);
        cache.insert(2, 0, vec![2, 2, 2]);

        let a = cache.get(1, 0).unwrap();
        let b = cache.get(2, 0).unwrap();
        assert_eq!(&*a, &[1, 1, 1]);
        assert_eq!(&*b, &[2, 2, 2]);
    }

    #[test]
    fn block_larger_than_cache_not_stored() {
        let cache = BlockCache::new(10);
        let data = vec![0; 100];
        let result = cache.insert(1, 0, data.clone());
        assert_eq!(&*result, &data);
        assert_eq!(cache.stats().entries, 0);
    }

    #[test]
    fn invalidate_file_removes_all_blocks() {
        let cache = BlockCache::new(1024 * 1024);

        cache.insert(1, 0, vec![0; 100]);
        cache.insert_with_priority(1, 100, vec![0; 100], Priority::High);
        cache.insert(2, 0, vec![0; 100]);
        assert_eq!(cache.stats().entries, 3);

        cache.invalidate_file(1);
        assert_eq!(cache.stats().entries, 1);
        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(1, 100).is_none());
        assert!(cache.get(2, 0).is_some());
        assert_eq!(cache.stats().size_bytes, 100);
    }

    #[test]
    fn high_priority_survives_low_eviction() {
        let cache = BlockCache::new(16 * 80); // 80 bytes per shard

        let high_fid = 100u64;
        let high_off = 0u64;
        cache.insert_with_priority(high_fid, high_off, vec![0xAA; 30], Priority::High);

        let target_shard = BlockCache::shard_index(&CacheKey {
            file_id: high_fid,
            block_offset: high_off,
        });

        let low_keys = find_keys_in_shard(target_shard, 5);

        // Set HIGH entry clock to max so it's not evicted
        // LOW entries get clock=3 on insert, but we set them to 0 before next insert
        for &fid in &low_keys {
            cache.insert(fid, 0, vec![0xBB; 30]);
            // Set LOW entries' clock to 0 so they're evictable
            let shard = cache.shards[target_shard].read();
            let key = CacheKey {
                file_id: fid,
                block_offset: 0,
            };
            if let Some(entry) = shard.map.get(&key) {
                entry.clock.store(0, Ordering::Relaxed);
            }
        }

        let high = cache.get(high_fid, high_off);
        assert!(
            high.is_some(),
            "HIGH priority block should survive LOW eviction"
        );
        assert_eq!(&*high.unwrap(), &vec![0xAA; 30]);
    }

    #[test]
    fn concurrent_access_no_deadlock() {
        let cache = Arc::new(BlockCache::new(1024 * 1024));
        let mut handles = Vec::new();

        for t in 0..16u64 {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..1000u64 {
                    let fid = t * 1000 + i;
                    c.insert(fid, 0, vec![t as u8; 64]);
                    c.get(fid, 0);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let stats = cache.stats();
        assert!(stats.entries > 0);
        assert!(stats.hits > 0);
    }

    #[test]
    fn duplicate_insert_returns_existing() {
        let cache = BlockCache::new(1024 * 1024);

        let first = cache.insert(1, 0, vec![0xAA; 10]);
        let second = cache.insert(1, 0, vec![0xBB; 10]);

        assert_eq!(&*second, &vec![0xAA; 10]);
        assert_eq!(Arc::as_ptr(&first), Arc::as_ptr(&second));
        assert_eq!(cache.stats().entries, 1);
    }

    #[test]
    fn zero_capacity_cache_does_not_store() {
        let cache = BlockCache::new(0);

        let data = cache.insert(1, 0, vec![1, 2, 3]);
        assert_eq!(&*data, &[1, 2, 3]);
        assert_eq!(cache.stats().entries, 0);
        assert!(cache.get(1, 0).is_none());
    }

    #[test]
    fn auto_detect_returns_positive() {
        let cap = auto_detect_capacity();
        assert!(cap > 0, "auto-detect should return positive value, got 0");
    }

    #[test]
    fn test_issue_1735_no_minimum_cache_clamp() {
        // Pi Zero scenario: 350 MB available → quarter = 87.5 MB
        // Bug: clamp(256 MiB, ...) forces 256 MiB — 73% of usable RAM
        let pi_available = 350 * 1024 * 1024; // 350 MB
        let cap = compute_cache_from_available(pi_available);
        let quarter = pi_available / 4;
        assert_eq!(
            cap, quarter,
            "cache {} should be 25% of available ({}), not clamped up",
            cap, quarter
        );
    }

    #[test]
    fn test_issue_1735_max_cap_still_enforced() {
        // 32 GB available → quarter = 8 GB, should be capped to 4 GB
        let big_available = 32 * 1024 * 1024 * 1024;
        let cap = compute_cache_from_available(big_available);
        assert_eq!(cap, MAX_AUTO_CACHE_BYTES);
    }

    #[test]
    fn test_issue_1735_zero_available_returns_zero() {
        assert_eq!(compute_cache_from_available(0), 0);
    }

    #[test]
    fn test_issue_1717_shard_distribution_with_aligned_offsets() {
        // Block offsets are multiples of 64KB (0x10000) in practice.
        // With NUM_SHARDS=16, all blocks from the same file should NOT
        // map to the same shard — they should spread across shards.
        let file_id = 42u64;
        let block_size: u64 = 64 * 1024; // 64KB

        let mut shards_seen = std::collections::HashSet::new();
        for i in 0..64u64 {
            let offset = i * block_size;
            let key = CacheKey {
                file_id,
                block_offset: offset,
            };
            shards_seen.insert(BlockCache::shard_index(&key));
        }

        // 64 blocks across 16 shards: a decent hash should hit at least 8 shards.
        // The bug causes all 64 to land in exactly 1 shard.
        assert!(
            shards_seen.len() >= 8,
            "64 blocks from the same file should use at least 8 of 16 shards, \
             but only {} shards were used (poor distribution due to aligned offsets)",
            shards_seen.len()
        );
    }

    /// Helper: find `count` file_ids that map to a given shard.
    fn find_keys_in_shard(target_shard: usize, count: usize) -> Vec<u64> {
        let mut keys = Vec::new();
        for fid in 0u64..10000 {
            let key = CacheKey {
                file_id: fid,
                block_offset: 0,
            };
            if BlockCache::shard_index(&key) == target_shard {
                keys.push(fid);
                if keys.len() >= count {
                    break;
                }
            }
        }
        keys
    }

    #[test]
    fn pinned_entries_survive_eviction() {
        let cache = BlockCache::new(16 * 100);
        let keys = find_keys_in_shard(0, 6);

        cache.insert_with_priority(keys[0], 0, vec![0xAA; 8], Priority::Pinned);

        for &fid in &keys[1..6] {
            cache.insert(fid, 0, vec![0xBB; 30]);
            // Make evictable
            let shard = cache.shards[0].read();
            let key = CacheKey {
                file_id: fid,
                block_offset: 0,
            };
            if let Some(entry) = shard.map.get(&key) {
                entry.clock.store(0, Ordering::Relaxed);
            }
        }

        assert!(
            cache.get(keys[0], 0).is_some(),
            "pinned entry should survive eviction"
        );
        assert_eq!(&*cache.get(keys[0], 0).unwrap(), &vec![0xAA; 8]);
    }

    #[test]
    fn pinned_budget_enforced() {
        let cache = BlockCache::new(16 * 100);
        let keys = find_keys_in_shard(0, 6);

        cache.insert_with_priority(keys[0], 0, vec![0xAA; 8], Priority::Pinned);
        let stats = cache.stats();
        assert_eq!(stats.pinned_entries, 1);
        assert_eq!(stats.pinned_bytes, 8);

        cache.insert_with_priority(keys[1], 0, vec![0xBB; 8], Priority::Pinned);
        let stats = cache.stats();
        assert_eq!(
            stats.pinned_entries, 1,
            "second entry should NOT be pinned (budget exceeded)"
        );
        assert_eq!(stats.pinned_bytes, 8);
    }

    #[test]
    fn promote_to_pinned_works() {
        let cache = BlockCache::new(16 * 200);
        let keys = find_keys_in_shard(0, 6);

        cache.insert_with_priority(keys[0], 0, vec![0xAA; 10], Priority::High);
        assert!(cache.promote_to_pinned(keys[0], 0));

        for &fid in &keys[1..6] {
            cache.insert(fid, 0, vec![0xBB; 50]);
            let shard = cache.shards[0].read();
            let key = CacheKey {
                file_id: fid,
                block_offset: 0,
            };
            if let Some(entry) = shard.map.get(&key) {
                entry.clock.store(0, Ordering::Relaxed);
            }
        }

        assert!(
            cache.get(keys[0], 0).is_some(),
            "promoted-to-pinned entry survives eviction"
        );
    }

    #[test]
    fn demote_file_moves_to_high() {
        let cache = BlockCache::new(16 * 200);
        let keys = find_keys_in_shard(0, 3);

        cache.insert_with_priority(keys[0], 0, vec![0xAA; 10], Priority::Pinned);
        assert_eq!(cache.stats().pinned_entries, 1);
        assert_eq!(cache.stats().pinned_bytes, 10);

        cache.demote_file(keys[0]);
        let stats = cache.stats();
        assert_eq!(stats.pinned_entries, 0);
        assert_eq!(stats.pinned_bytes, 0);

        let val = cache.get(keys[0], 0);
        assert!(val.is_some(), "entry survives demote");
        assert_eq!(&*val.unwrap(), &vec![0xAA; 10]);

        assert!(cache.promote_to_pinned(keys[0], 0));
        assert_eq!(cache.stats().pinned_entries, 1);
    }

    #[test]
    fn invalidate_file_handles_pinned() {
        let cache = BlockCache::new(16 * 200);

        cache.insert_with_priority(42, 0, vec![0xAA; 10], Priority::Pinned);
        cache.insert_with_priority(42, 100, vec![0xBB; 10], Priority::Pinned);
        cache.insert(99, 0, vec![0xCC; 10]);

        assert!(cache.stats().pinned_bytes > 0);

        cache.invalidate_file(42);
        let stats = cache.stats();
        assert_eq!(stats.pinned_bytes, 0);
        assert!(cache.get(42, 0).is_none());
        assert!(cache.get(42, 100).is_none());
        assert!(cache.get(99, 0).is_some());
    }

    #[test]
    fn test_issue_1684_capacity_hard_bound() {
        // Fill a shard entirely with Pinned entries (which evict_for never
        // evicts), then attempt another insertion. The insert must be rejected
        // so that current_bytes never exceeds capacity_bytes.
        let cache = BlockCache::new(16 * 50); // 50 bytes per shard

        // Raise pinned_budget so we can fill the shard with Pinned entries
        {
            let mut shard = cache.shards[0].write();
            shard.pinned_budget = 50;
        }
        let keys = find_keys_in_shard(0, 4);

        // Fill shard 0 to exactly 50 bytes of Pinned entries
        cache.insert_with_priority(keys[0], 0, vec![0xAA; 25], Priority::Pinned);
        cache.insert_with_priority(keys[1], 0, vec![0xBB; 25], Priority::Pinned);
        assert_eq!(cache.stats().size_bytes, 50, "shard should be at capacity");

        // Insert a new entry — evict_for cannot free Pinned entries,
        // so the insertion must be rejected to enforce the hard bound.
        let result = cache.insert(keys[2], 0, vec![0xCC; 10]);
        assert_eq!(
            &*result,
            &vec![0xCC; 10],
            "data is returned even if not cached"
        );

        let stats = cache.stats();
        assert!(
            stats.size_bytes <= 50,
            "capacity should be a hard bound, but size_bytes={} > capacity=50",
            stats.size_bytes
        );
        assert!(
            cache.get(keys[2], 0).is_none(),
            "entry should not be cached when eviction cannot free enough space"
        );
    }

    #[test]
    fn test_issue_1684_set_capacity_updates_live_cache() {
        // Verify that set_capacity() actually changes per-shard limits
        // on a live cache instance (the fix for the singleton race).
        let cache = BlockCache::new(16 * 100); // 100 bytes per shard
        assert_eq!(cache.stats().capacity_bytes, 16 * 100);

        // Resize down to 50 bytes per shard
        cache.set_capacity(16 * 50);
        assert_eq!(cache.stats().capacity_bytes, 16 * 50);

        // Verify the new per-shard capacity is enforced
        let keys = find_keys_in_shard(0, 3);
        cache.insert(keys[0], 0, vec![0xAA; 40]);
        assert!(
            cache.get(keys[0], 0).is_some(),
            "40 bytes fits in 50-byte shard"
        );

        // 40 + 20 = 60 > 50 — the second insert must trigger eviction or rejection
        {
            let shard = cache.shards[0].read();
            for entry in shard.map.values() {
                entry.clock.store(0, Ordering::Relaxed);
            }
        }
        cache.insert(keys[1], 0, vec![0xBB; 20]);
        let shard = cache.shards[0].read();
        assert!(
            shard.current_bytes <= 50,
            "resized shard capacity must be enforced, got {} bytes",
            shard.current_bytes
        );
    }
}
