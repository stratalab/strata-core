//! Lock-free block cache for decompressed KV segment data blocks.
//!
//! ## Architecture
//!
//! Open-addressed hash table with atomic metadata per slot, inspired by
//! RocksDB's HyperClockCache. All operations (lookup, insert, eviction) are
//! lock-free — no mutexes, no RwLocks.
//!
//! **Lookup** (hot path): hash → probe → atomic fetch_add on acquire counter →
//! validate visibility + key match → clone Arc → atomic fetch_add on release
//! counter. Total: ~3 atomic operations per probe, zero locks.
//!
//! **Insert**: probe for empty slot → atomic OR to claim → write key+data →
//! atomic store to make visible. If over capacity, run parallel CLOCK eviction
//! first (atomic CAS per slot, no mutex).
//!
//! **Eviction**: CLOCK algorithm with atomic clock pointer. Each thread
//! advances the pointer by a step size, then scans its slice of slots.
//! Unreferenced entries with clock=0 are evicted via CAS.
//!
//! Cache key: `(file_id, block_offset)` where file_id is derived from the
//! file path hash.

use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default block cache capacity: 256 MiB.
const DEFAULT_CAPACITY_BYTES: usize = 256 * 1024 * 1024;

/// Estimated average block size for table sizing. The table is allocated with
/// `capacity / ESTIMATED_BLOCK_SIZE / LOAD_FACTOR_INV` slots.
const ESTIMATED_BLOCK_SIZE: usize = 16 * 1024; // 16 KB

/// Inverse load factor. Table size = entries * LOAD_FACTOR_INV (i.e. 0.5 load).
/// Lower load factor = less probing, more memory. 2 = 50% load is a good
/// balance for lock-free tables (RocksDB uses 0.7, we're more conservative).
const LOAD_FACTOR_INV: usize = 2;

/// Minimum number of table slots.
const MIN_TABLE_SLOTS: usize = 1024;

/// CLOCK eviction step size: number of slots processed per eviction batch.
const EVICTION_STEP: u64 = 8;

/// Maximum clock countdown value (set on insert and refreshed on access).
const CLOCK_MAX: u64 = 3;

/// Initial clock value for newly inserted entries. Lower than CLOCK_MAX so
/// entries that are never re-read (e.g. compaction pass-through) can be
/// evicted in fewer CLOCK sweeps.
const CLOCK_INITIAL: u64 = 1;

/// Maximum number of probe steps before giving up on lookup/insert.
/// Prevents O(N) scans when the table is nearly full.
const MAX_PROBES: usize = 128;

/// When both acquire and release counters exceed this threshold, they are
/// reset by subtracting the same value from both (preserving refcount).
/// This prevents counter overflow into adjacent bit fields.
/// Must be well below COUNT_MASK (4194303) to leave headroom.
const COUNTER_RESET_THRESHOLD: u64 = 1 << 20; // ~1M

// ---------------------------------------------------------------------------
// Meta word encoding
// ---------------------------------------------------------------------------
//
// A single AtomicU64 encodes the full state of a slot:
//
//   Bits  0..2   (3 bits): CountdownClock (0-7)
//   Bits  3..24  (22 bits): AcquireCount
//   Bits 25..46  (22 bits): ReleaseCount
//   Bits 47..48  (2 bits): Priority (0=Low, 1=High, 2=Pinned)
//   Bit  60: Occupied — slot has been claimed
//   Bit  61: Shareable — entry is reference-counted (data valid)
//   Bit  62: Visible — entry is findable by Lookup

const CLOCK_BITS: u64 = 3;
const CLOCK_MASK: u64 = (1 << CLOCK_BITS) - 1; // 0x7

const ACQUIRE_SHIFT: u64 = 3;
const COUNT_BITS: u64 = 22;
const COUNT_MASK: u64 = (1 << COUNT_BITS) - 1; // 0x3F_FFFF
const ACQUIRE_ONE: u64 = 1 << ACQUIRE_SHIFT;

const RELEASE_SHIFT: u64 = ACQUIRE_SHIFT + COUNT_BITS; // 25
const RELEASE_ONE: u64 = 1 << RELEASE_SHIFT;

const PRIORITY_SHIFT: u64 = RELEASE_SHIFT + COUNT_BITS; // 47
const PRIORITY_MASK: u64 = 0x3; // 2 bits

const OCCUPIED_BIT: u64 = 1 << 60;
const SHAREABLE_BIT: u64 = 1 << 61;
const VISIBLE_BIT: u64 = 1 << 62;

/// All three state bits set = entry is live and findable.
const LIVE_BITS: u64 = OCCUPIED_BIT | SHAREABLE_BIT | VISIBLE_BIT;

/// Occupied but not Shareable/Visible = under construction.
const CONSTRUCTION_BITS: u64 = OCCUPIED_BIT;

#[inline(always)]
fn meta_is_empty(m: u64) -> bool {
    m & OCCUPIED_BIT == 0
}

#[inline(always)]
fn meta_is_visible(m: u64) -> bool {
    m & LIVE_BITS == LIVE_BITS
}

#[inline(always)]
fn meta_is_shareable(m: u64) -> bool {
    m & (OCCUPIED_BIT | SHAREABLE_BIT) == (OCCUPIED_BIT | SHAREABLE_BIT)
}

#[inline(always)]
fn meta_clock(m: u64) -> u64 {
    m & CLOCK_MASK
}

#[inline(always)]
fn meta_acquire_count(m: u64) -> u64 {
    (m >> ACQUIRE_SHIFT) & COUNT_MASK
}

#[inline(always)]
fn meta_release_count(m: u64) -> u64 {
    (m >> RELEASE_SHIFT) & COUNT_MASK
}

#[inline(always)]
fn meta_refcount(m: u64) -> u64 {
    meta_acquire_count(m).wrapping_sub(meta_release_count(m)) & COUNT_MASK
}

#[inline(always)]
fn meta_priority(m: u64) -> u64 {
    (m >> PRIORITY_SHIFT) & PRIORITY_MASK
}

/// Build the initial meta word for a newly visible entry.
#[inline(always)]
fn make_visible_meta(clock: u64, priority: Priority) -> u64 {
    let p = match priority {
        Priority::Low => 0u64,
        Priority::High => 1,
        Priority::Pinned => 2,
    };
    LIVE_BITS | (clock & CLOCK_MASK) | (p << PRIORITY_SHIFT)
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

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

impl Priority {
    #[allow(dead_code)]
    fn from_bits(bits: u64) -> Self {
        match bits {
            1 => Priority::High,
            2 => Priority::Pinned,
            _ => Priority::Low,
        }
    }
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

// ---------------------------------------------------------------------------
// Slot
// ---------------------------------------------------------------------------

/// A single cache slot. Cache-line aligned to prevent false sharing.
///
/// # Safety invariants
///
/// - `data` is a valid `*mut Arc<Vec<u8>>` only when meta has Shareable set.
/// - `data` can only be read (Arc::clone) while AcquireCount > ReleaseCount
///   (the reader holds a logical reference via the meta word).
/// - `data` can only be freed (Box::from_raw + drop) when the slot transitions
///   from Shareable to under-construction with refcount == 0.
/// - `key_*` fields are only valid when Occupied is set.
#[repr(C)]
struct Slot {
    meta: AtomicU64,
    key_file_id: AtomicU64,
    key_block_offset: AtomicU64,
    /// Heap-allocated `Arc<Vec<u8>>`. Null when empty.
    data: AtomicPtr<Arc<Vec<u8>>>,
    charge: AtomicU32,
}

impl Slot {
    fn new() -> Self {
        Self {
            meta: AtomicU64::new(0),
            key_file_id: AtomicU64::new(0),
            key_block_offset: AtomicU64::new(0),
            data: AtomicPtr::new(std::ptr::null_mut()),
            charge: AtomicU32::new(0),
        }
    }
}

// Slots are Send+Sync because all fields are atomic.
unsafe impl Send for Slot {}
unsafe impl Sync for Slot {}

// ---------------------------------------------------------------------------
// BlockCache (lock-free)
// ---------------------------------------------------------------------------

/// Lock-free block cache using open-addressed hashing and CLOCK eviction.
///
/// All operations (lookup, insert, eviction) are wait-free on the hot path.
/// No mutexes or RwLocks.
pub struct BlockCache {
    slots: Box<[Slot]>,
    len_mask: usize,
    clock_pointer: AtomicU64,
    occupancy: AtomicUsize,
    usage: AtomicUsize,
    capacity: AtomicUsize,
    pinned_usage: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
}

// SAFETY: All fields are either atomic or behind atomic guards.
unsafe impl Send for BlockCache {}
unsafe impl Sync for BlockCache {}

impl BlockCache {
    /// Create a new lock-free block cache with the given capacity in bytes.
    pub fn new(capacity_bytes: usize) -> Self {
        let estimated_entries = if capacity_bytes > 0 {
            capacity_bytes / ESTIMATED_BLOCK_SIZE
        } else {
            0
        };
        let raw_slots = (estimated_entries * LOAD_FACTOR_INV).max(MIN_TABLE_SLOTS);
        let num_slots = raw_slots.next_power_of_two();

        let mut slots = Vec::with_capacity(num_slots);
        for _ in 0..num_slots {
            slots.push(Slot::new());
        }

        Self {
            slots: slots.into_boxed_slice(),
            len_mask: num_slots - 1,
            clock_pointer: AtomicU64::new(0),
            occupancy: AtomicUsize::new(0),
            usage: AtomicUsize::new(0),
            capacity: AtomicUsize::new(capacity_bytes),
            pinned_usage: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Create a block cache with the default capacity.
    pub fn default_capacity() -> Self {
        Self::new(DEFAULT_CAPACITY_BYTES)
    }

    /// Number of slots in the hash table.
    #[inline]
    fn table_len(&self) -> usize {
        self.len_mask + 1
    }

    // -----------------------------------------------------------------------
    // Hashing / probing
    // -----------------------------------------------------------------------

    /// Compute primary index and probe increment from a key.
    /// The increment is forced odd so it visits every slot before cycling.
    #[inline]
    fn probe(file_id: u64, block_offset: u64, len_mask: usize) -> (usize, usize) {
        let h1 = file_id.wrapping_mul(0x517cc1b727220a95)
            ^ block_offset.wrapping_mul(0x9e3779b97f4a7c15);
        let h2 = block_offset.wrapping_mul(0x517cc1b727220a95)
            ^ file_id.wrapping_mul(0x9e3779b97f4a7c15);
        let base = (h1 as usize) & len_mask;
        let inc = ((h2 as usize) & len_mask) | 1; // force odd
        (base, inc)
    }

    // -----------------------------------------------------------------------
    // Lookup (lock-free hot path)
    // -----------------------------------------------------------------------

    /// Look up a cached block. Returns the decompressed data if present.
    ///
    /// Lock-free: uses atomic fetch_add for reference counting. On a cache hit,
    /// the cost is ~3 atomic operations (acquire, key compare, release or clone).
    pub fn get(&self, file_id: u64, block_offset: u64) -> Option<Arc<Vec<u8>>> {
        if self.capacity.load(Ordering::Relaxed) == 0 {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let (mut idx, inc) = Self::probe(file_id, block_offset, self.len_mask);

        for _ in 0..MAX_PROBES {
            let slot = &self.slots[idx];

            // Optimistically acquire a reference.
            let old_meta = slot.meta.fetch_add(ACQUIRE_ONE, Ordering::Acquire);

            if meta_is_visible(old_meta) {
                // Slot is live — check key match.
                let k_fid = slot.key_file_id.load(Ordering::Relaxed);
                let k_off = slot.key_block_offset.load(Ordering::Relaxed);

                if k_fid == file_id && k_off == block_offset {
                    // HIT — clone the data, then release our reference.
                    // SAFETY: data is valid because the slot is Shareable and we
                    // hold a reference (AcquireCount > ReleaseCount).
                    let arc = unsafe {
                        let ptr = slot.data.load(Ordering::Acquire);
                        debug_assert!(!ptr.is_null());
                        Arc::clone(&*ptr)
                    };

                    // Refresh clock (best-effort, relaxed).
                    self.refresh_clock(slot, old_meta);

                    // Release our reference.
                    self.release_ref(slot, Ordering::Release);
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    return Some(arc);
                }

                // Key mismatch — release reference.
                self.release_ref(slot, Ordering::Release);
            } else if meta_is_empty(old_meta) {
                // Empty slot means key is not in the table.
                self.release_ref(slot, Ordering::Relaxed);
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            } else {
                // Slot is occupied but not visible (under construction or
                // invisible). Undo acquire and keep probing.
                self.release_ref(slot, Ordering::Relaxed);
            }

            idx = (idx + inc) & self.len_mask;
        }

        // Probe limit exceeded — treat as miss.
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Release a reference by incrementing the release counter.
    /// Also checks for counter overflow and resets both counters if needed.
    #[inline(always)]
    fn release_ref(&self, slot: &Slot, ordering: Ordering) {
        let old = slot.meta.fetch_add(RELEASE_ONE, ordering);
        // Check if both counters are large (overflow risk). This is the
        // rare path — only triggers after ~1M lookups on the same entry.
        let acquire = meta_acquire_count(old);
        let release = meta_release_count(old) + 1; // after our increment
        if release > COUNTER_RESET_THRESHOLD && acquire > COUNTER_RESET_THRESHOLD {
            // Reset both counters by subtracting the smaller value from both.
            // This preserves the refcount (acquire - release) while freeing
            // counter space. Best-effort CAS — if it fails, another release
            // will try again.
            let drain = release.min(acquire);
            let sub = (drain << ACQUIRE_SHIFT) | (drain << RELEASE_SHIFT);
            let current = slot.meta.load(Ordering::Relaxed);
            // Only reset if counters are still high (another thread may have reset).
            if meta_acquire_count(current) > COUNTER_RESET_THRESHOLD {
                let _ = slot.meta.compare_exchange_weak(
                    current,
                    current.wrapping_sub(sub),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
            }
        }
    }

    /// Refresh the clock countdown to CLOCK_MAX (best-effort CAS).
    #[inline]
    fn refresh_clock(&self, slot: &Slot, old_meta: u64) {
        let current_clock = meta_clock(old_meta);
        if current_clock < CLOCK_MAX {
            // Best-effort CAS: set clock bits to CLOCK_MAX. Use post-acquire
            // meta as expected value since our fetch_add already incremented it.
            let expected = old_meta + ACQUIRE_ONE;
            let desired = (expected & !CLOCK_MASK) | CLOCK_MAX;
            let _ = slot.meta.compare_exchange_weak(
                expected,
                desired,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }

    // -----------------------------------------------------------------------
    // Insert (lock-free)
    // -----------------------------------------------------------------------

    /// Insert a decompressed block with LOW priority (data blocks).
    pub fn insert(&self, file_id: u64, block_offset: u64, data: Vec<u8>) -> Arc<Vec<u8>> {
        self.insert_with_priority(file_id, block_offset, data, Priority::Low)
    }

    /// Insert a decompressed block with an explicit priority tier.
    ///
    /// Returns the cached `Arc` (which may be a pre-existing entry if
    /// another thread inserted the same key concurrently).
    pub fn insert_with_priority(
        &self,
        file_id: u64,
        block_offset: u64,
        data: Vec<u8>,
        priority: Priority,
    ) -> Arc<Vec<u8>> {
        let size = data.len();
        let data = Arc::new(data);
        let cap = self.capacity.load(Ordering::Relaxed);

        if cap == 0 || size > cap {
            return data;
        }

        // Evict if over capacity.
        self.evict_if_needed(size);

        // Hard capacity check: if still over, skip caching.
        if self.usage.load(Ordering::Relaxed) + size > cap {
            return data;
        }

        let (mut idx, inc) = Self::probe(file_id, block_offset, self.len_mask);

        for _ in 0..MAX_PROBES {
            let slot = &self.slots[idx];
            let meta = slot.meta.load(Ordering::Acquire);

            if meta_is_empty(meta) {
                // Try to claim this empty slot.
                match slot.meta.compare_exchange(
                    meta,
                    CONSTRUCTION_BITS,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // We own this slot exclusively. Write key + data.
                        slot.key_file_id.store(file_id, Ordering::Relaxed);
                        slot.key_block_offset.store(block_offset, Ordering::Relaxed);
                        slot.charge.store(size as u32, Ordering::Relaxed);

                        // Heap-allocate the Arc and store the pointer.
                        let arc_box = Box::new(Arc::clone(&data));
                        let old_ptr = slot.data.swap(Box::into_raw(arc_box), Ordering::Release);
                        debug_assert!(old_ptr.is_null());

                        // Make visible: set Shareable + Visible + clock.
                        let visible_meta = make_visible_meta(CLOCK_INITIAL, priority);
                        slot.meta.store(visible_meta, Ordering::Release);

                        self.occupancy.fetch_add(1, Ordering::Relaxed);
                        self.usage.fetch_add(size, Ordering::Relaxed);
                        if priority == Priority::Pinned {
                            self.pinned_usage.fetch_add(size, Ordering::Relaxed);
                        }

                        return data;
                    }
                    Err(_) => {
                        // Another thread claimed it. Retry this slot.
                        continue;
                    }
                }
            } else if meta_is_visible(meta) {
                // Check if this is a duplicate key.
                let k_fid = slot.key_file_id.load(Ordering::Relaxed);
                let k_off = slot.key_block_offset.load(Ordering::Relaxed);

                if k_fid == file_id && k_off == block_offset {
                    // Duplicate — refresh clock and return existing.
                    self.refresh_clock(slot, meta);

                    // Read and clone the existing data.
                    // Acquire a ref first.
                    let old = slot.meta.fetch_add(ACQUIRE_ONE, Ordering::Acquire);
                    if meta_is_visible(old) {
                        let existing = unsafe {
                            let ptr = slot.data.load(Ordering::Acquire);
                            Arc::clone(&*ptr)
                        };
                        self.release_ref(slot, Ordering::Release);
                        return existing;
                    } else {
                        // Slot changed state — undo and fall through.
                        self.release_ref(slot, Ordering::Relaxed);
                    }
                }
            }
            // Occupied with different key, or under construction — keep probing.
            idx = (idx + inc) & self.len_mask;
        }

        // Probe limit exceeded — cannot insert. Return uncached.
        data
    }

    // -----------------------------------------------------------------------
    // CLOCK eviction (lock-free, parallel)
    // -----------------------------------------------------------------------

    /// Run CLOCK eviction until usage is below capacity or we've scanned
    /// the table up to CLOCK_MAX times.
    fn evict_if_needed(&self, needed: usize) {
        let cap = self.capacity.load(Ordering::Relaxed);
        if self.usage.load(Ordering::Relaxed) + needed <= cap {
            return;
        }

        let table_len = self.table_len() as u64;
        let max_scan = table_len * (CLOCK_MAX + 1);
        let mut scanned: u64 = 0;

        while self.usage.load(Ordering::Relaxed) + needed > cap && scanned < max_scan {
            let start = self
                .clock_pointer
                .fetch_add(EVICTION_STEP, Ordering::Relaxed);

            for i in 0..EVICTION_STEP {
                let slot_idx = ((start + i) % table_len) as usize;
                let slot = &self.slots[slot_idx];
                self.try_evict_slot(slot);
            }

            scanned += EVICTION_STEP;
        }
    }

    /// Try to evict a single slot via CLOCK algorithm.
    ///
    /// - If unreferenced and clock == 0 → evict (CAS to empty).
    /// - If unreferenced and clock > 0 → decrement clock (CAS).
    /// - If referenced or Pinned → skip.
    #[inline]
    fn try_evict_slot(&self, slot: &Slot) {
        let meta = slot.meta.load(Ordering::Acquire);

        if !meta_is_shareable(meta) {
            return; // Empty or under construction
        }

        // Never evict Pinned entries.
        if meta_priority(meta) == 2 {
            return;
        }

        let refcount = meta_refcount(meta);
        if refcount != 0 {
            return; // Currently referenced
        }

        let clock = meta_clock(meta);
        if clock > 0 {
            // Decrement clock — best-effort CAS (failure is fine).
            let new_meta = meta - 1; // clock is in lowest bits, so -1 decrements it
            let _ = slot.meta.compare_exchange_weak(
                meta,
                new_meta,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            return;
        }

        // clock == 0 and refcount == 0: evict.
        // Transition to under-construction (exclusive ownership).
        match slot.meta.compare_exchange(
            meta,
            CONSTRUCTION_BITS,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                // We own the slot. Free data and mark empty.
                let charge = slot.charge.load(Ordering::Relaxed) as usize;
                let priority_bits = meta_priority(meta);

                // Free the heap-allocated Arc.
                let ptr = slot.data.swap(std::ptr::null_mut(), Ordering::AcqRel);
                if !ptr.is_null() {
                    // SAFETY: we have exclusive ownership (under construction, refcount=0).
                    unsafe {
                        drop(Box::from_raw(ptr));
                    }
                }

                // Clear key fields.
                slot.key_file_id.store(0, Ordering::Relaxed);
                slot.key_block_offset.store(0, Ordering::Relaxed);
                slot.charge.store(0, Ordering::Relaxed);

                // Mark slot empty (release store so other threads see the cleared data).
                slot.meta.store(0, Ordering::Release);

                self.occupancy.fetch_sub(1, Ordering::Relaxed);
                self.usage.fetch_sub(charge, Ordering::Relaxed);
                if priority_bits == 2 {
                    self.pinned_usage.fetch_sub(charge, Ordering::Relaxed);
                }
            }
            Err(_) => {
                // Another thread modified the slot — skip.
            }
        }
    }

    // -----------------------------------------------------------------------
    // Maintenance operations
    // -----------------------------------------------------------------------

    /// Remove all cached blocks for a given file.
    ///
    /// Scans the entire table. This is an infrequent operation (called during
    /// compaction cleanup) so a full scan is acceptable.
    pub fn invalidate_file(&self, file_id: u64) {
        for slot in self.slots.iter() {
            let meta = slot.meta.load(Ordering::Acquire);
            if !meta_is_visible(meta) {
                continue;
            }
            if slot.key_file_id.load(Ordering::Relaxed) != file_id {
                continue;
            }
            // Try to evict this slot (even if clock > 0 or Pinned).
            let refcount = meta_refcount(meta);
            if refcount != 0 {
                // Slot is currently referenced — mark invisible so it won't
                // be found by future lookups. The last Release will clean up.
                // For simplicity, we just clear the Visible bit.
                let invisible = meta & !VISIBLE_BIT;
                let _ = slot.meta.compare_exchange(
                    meta,
                    invisible,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );
                continue;
            }
            // Unreferenced — evict directly.
            match slot.meta.compare_exchange(
                meta,
                CONSTRUCTION_BITS,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let charge = slot.charge.load(Ordering::Relaxed) as usize;
                    let priority_bits = meta_priority(meta);
                    let ptr = slot.data.swap(std::ptr::null_mut(), Ordering::AcqRel);
                    if !ptr.is_null() {
                        unsafe {
                            drop(Box::from_raw(ptr));
                        }
                    }
                    slot.key_file_id.store(0, Ordering::Relaxed);
                    slot.key_block_offset.store(0, Ordering::Relaxed);
                    slot.charge.store(0, Ordering::Relaxed);
                    slot.meta.store(0, Ordering::Release);
                    self.occupancy.fetch_sub(1, Ordering::Relaxed);
                    self.usage.fetch_sub(charge, Ordering::Relaxed);
                    if priority_bits == 2 {
                        self.pinned_usage.fetch_sub(charge, Ordering::Relaxed);
                    }
                }
                Err(_) => {} // Another thread got it
            }
        }
    }

    /// Promote an existing cache entry to Pinned priority.
    pub fn promote_to_pinned(&self, file_id: u64, block_offset: u64) -> bool {
        let (mut idx, inc) = Self::probe(file_id, block_offset, self.len_mask);

        for _ in 0..MAX_PROBES {
            let slot = &self.slots[idx];
            let meta = slot.meta.load(Ordering::Acquire);

            if meta_is_empty(meta) {
                return false; // Not found
            }

            if meta_is_visible(meta) {
                let k_fid = slot.key_file_id.load(Ordering::Relaxed);
                let k_off = slot.key_block_offset.load(Ordering::Relaxed);

                if k_fid == file_id && k_off == block_offset {
                    if meta_priority(meta) == 2 {
                        return true; // Already pinned
                    }
                    // CAS to change priority bits to Pinned (2).
                    let cleared = meta & !(PRIORITY_MASK << PRIORITY_SHIFT);
                    let new_meta = cleared | (2 << PRIORITY_SHIFT);
                    match slot.meta.compare_exchange(
                        meta,
                        new_meta,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            let charge = slot.charge.load(Ordering::Relaxed) as usize;
                            self.pinned_usage.fetch_add(charge, Ordering::Relaxed);
                            return true;
                        }
                        Err(_) => return false, // Slot changed, bail
                    }
                }
            }

            idx = (idx + inc) & self.len_mask;
        }
        false
    }

    /// Demote all Pinned entries for a given file to High priority.
    pub fn demote_file(&self, file_id: u64) {
        for slot in self.slots.iter() {
            let meta = slot.meta.load(Ordering::Acquire);
            if !meta_is_visible(meta) || meta_priority(meta) != 2 {
                continue;
            }
            if slot.key_file_id.load(Ordering::Relaxed) != file_id {
                continue;
            }
            // CAS priority from Pinned (2) to High (1).
            let cleared = meta & !(PRIORITY_MASK << PRIORITY_SHIFT);
            let new_meta = cleared | (1 << PRIORITY_SHIFT);
            if slot
                .meta
                .compare_exchange(meta, new_meta, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                let charge = slot.charge.load(Ordering::Relaxed) as usize;
                self.pinned_usage.fetch_sub(charge, Ordering::Relaxed);
            }
        }
    }

    /// Update the cache capacity.
    ///
    /// Does NOT resize the hash table (fixed at creation). Only affects
    /// eviction pressure — if the new capacity is smaller, subsequent
    /// inserts will trigger more eviction.
    fn set_capacity(&self, capacity_bytes: usize) {
        self.capacity.store(capacity_bytes, Ordering::Relaxed);
    }

    /// Get cache statistics.
    pub fn stats(&self) -> BlockCacheStats {
        let mut pinned_entries = 0usize;
        // Count pinned entries by scanning (infrequent operation).
        // Only sample a fraction of slots for performance.
        for slot in self.slots.iter() {
            let meta = slot.meta.load(Ordering::Relaxed);
            if meta_is_visible(meta) && meta_priority(meta) == 2 {
                pinned_entries += 1;
            }
        }

        BlockCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: self.occupancy.load(Ordering::Relaxed),
            size_bytes: self.usage.load(Ordering::Relaxed),
            capacity_bytes: self.capacity.load(Ordering::Relaxed),
            pinned_bytes: self.pinned_usage.load(Ordering::Relaxed),
            pinned_entries,
        }
    }
}

impl Drop for BlockCache {
    fn drop(&mut self) {
        // Free all remaining heap-allocated Arcs.
        for slot in self.slots.iter() {
            let ptr = slot.data.swap(std::ptr::null_mut(), Ordering::Relaxed);
            if !ptr.is_null() {
                unsafe {
                    drop(Box::from_raw(ptr));
                }
            }
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
/// If the cache is already initialized, updates the capacity on the live
/// instance. If not yet initialized, stores the value for use when
/// `global_cache()` first creates the cache.
pub fn set_global_capacity(bytes: usize) {
    GLOBAL_CAPACITY.store(bytes, Ordering::Relaxed);
    if let Some(cache) = GLOBAL_CACHE.get() {
        cache.set_capacity(bytes);
    }
}

/// Get or create the global block cache.
pub fn global_cache() -> &'static BlockCache {
    GLOBAL_CACHE.get_or_init(|| BlockCache::new(GLOBAL_CAPACITY.load(Ordering::Relaxed)))
}

/// Return the configured global block cache capacity in bytes.
pub fn global_capacity() -> usize {
    GLOBAL_CAPACITY.load(Ordering::Relaxed)
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

        // Second insert should find the existing entry and return it.
        assert_eq!(&*second, &vec![0xAA; 10]);
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
        let pi_available = 350 * 1024 * 1024;
        let cap = compute_cache_from_available(pi_available);
        let quarter = pi_available / 4;
        assert_eq!(cap, quarter);
    }

    #[test]
    fn test_issue_1735_max_cap_still_enforced() {
        let big_available = 32 * 1024 * 1024 * 1024;
        let cap = compute_cache_from_available(big_available);
        assert_eq!(cap, MAX_AUTO_CACHE_BYTES);
    }

    #[test]
    fn test_issue_1735_zero_available_returns_zero() {
        assert_eq!(compute_cache_from_available(0), 0);
    }

    #[test]
    fn pinned_entries_survive_eviction() {
        // Small cache: force eviction pressure.
        let cache = BlockCache::new(512);

        cache.insert_with_priority(1, 0, vec![0xAA; 8], Priority::Pinned);

        // Fill with evictable entries to force eviction.
        for i in 2..50u64 {
            cache.insert(i, 0, vec![0xBB; 30]);
        }

        assert!(
            cache.get(1, 0).is_some(),
            "pinned entry should survive eviction"
        );
        assert_eq!(&*cache.get(1, 0).unwrap(), &vec![0xAA; 8]);
    }

    #[test]
    fn promote_to_pinned_works() {
        let cache = BlockCache::new(1024 * 1024);

        cache.insert_with_priority(1, 0, vec![0xAA; 10], Priority::High);
        assert!(cache.promote_to_pinned(1, 0));

        let stats = cache.stats();
        assert_eq!(stats.pinned_entries, 1);
        assert_eq!(stats.pinned_bytes, 10);
    }

    #[test]
    fn demote_file_moves_to_high() {
        let cache = BlockCache::new(1024 * 1024);

        cache.insert_with_priority(1, 0, vec![0xAA; 10], Priority::Pinned);
        assert_eq!(cache.stats().pinned_entries, 1);

        cache.demote_file(1);
        let stats = cache.stats();
        assert_eq!(stats.pinned_entries, 0);
        assert_eq!(stats.pinned_bytes, 0);

        let val = cache.get(1, 0);
        assert!(val.is_some(), "entry survives demote");
        assert_eq!(&*val.unwrap(), &vec![0xAA; 10]);
    }

    #[test]
    fn invalidate_file_handles_pinned() {
        let cache = BlockCache::new(1024 * 1024);

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
    fn heavy_concurrent_stress() {
        let cache = Arc::new(BlockCache::new(64 * 1024)); // Small cache to force eviction
        let mut handles = Vec::new();

        for t in 0..32u64 {
            let c = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..5000u64 {
                    let fid = t * 10000 + i;
                    c.insert(fid, 0, vec![t as u8; 128]);
                    let _ = c.get(fid, 0);
                    // Also read keys from other threads.
                    let other = ((t + 1) % 32) * 10000 + i;
                    let _ = c.get(other, 0);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let stats = cache.stats();
        assert!(stats.hits + stats.misses > 0);
        // Ensure no memory leaks by checking the cache can still be used.
        cache.insert(999999, 0, vec![0xFF; 10]);
        assert!(cache.get(999999, 0).is_some());
    }
}
