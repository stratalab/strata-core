//! Table-local cache and read accelerators.

use super::{TableCacheConfig, TableRuntimeError, TableRuntimeResult};
use crate::observability::perf_trace;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, MutexGuard};

const MAX_TABLE_CACHE_ID_BYTES: usize = 512;
const MAX_BLOOM_BYTES: usize = 16 * 1024 * 1024;
const MAX_BLOOM_PROBES: u8 = 30;
const MAX_DEBUG_BYTES: usize = 16;
/// BS4.1: shard count is sized from CPU parallelism, clamped to this band (was capacity-derived,
/// capped at 16 — a low ceiling on many-core boxes). More shards = less lock contention on the read
/// hot path once BS4.4c routes lazy reads through the cache. The count is still capped by capacity so
/// no shard is smaller than one block (`TARGET_SHARD_BYTES`): a shard that can't hold a block would
/// oversized-reject every insert.
const MIN_SHARDS: usize = 4;
const MAX_SHARDS: usize = 64;
const TARGET_SHARD_BYTES: usize = 64 * 1024;

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TableCacheTableId {
    bytes: Vec<u8>,
}

impl TableCacheTableId {
    pub(crate) fn new(bytes: impl Into<Vec<u8>>) -> TableRuntimeResult<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(TableRuntimeError::InvalidConfig {
                field: "table_cache_id",
                reason: "must not be empty",
            });
        }
        if bytes.len() > MAX_TABLE_CACHE_ID_BYTES {
            return Err(TableRuntimeError::InvalidConfig {
                field: "table_cache_id",
                reason: "is too large",
            });
        }
        Ok(Self { bytes })
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for TableCacheTableId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TableCacheTableId(")?;
        fmt_bounded_bytes(formatter, self.as_slice())?;
        formatter.write_str(")")
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum TableBlockCacheKind {
    Data,
    Index,
    Properties,
    Accelerator,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TableBlockAddress {
    kind: TableBlockCacheKind,
    offset: u64,
    length: u32,
    ordinal: Option<u32>,
}

impl TableBlockAddress {
    pub(crate) fn new(
        kind: TableBlockCacheKind,
        offset: u64,
        length: u32,
        ordinal: Option<u32>,
    ) -> TableRuntimeResult<Self> {
        if length == 0 {
            return Err(TableRuntimeError::InvalidRange {
                field: "block_length",
            });
        }
        offset
            .checked_add(u64::from(length))
            .ok_or(TableRuntimeError::InvalidRange {
                field: "block_range",
            })?;
        Ok(Self {
            kind,
            offset,
            length,
            ordinal,
        })
    }

    pub(crate) const fn kind(&self) -> TableBlockCacheKind {
        self.kind
    }

    pub(crate) const fn offset(&self) -> u64 {
        self.offset
    }

    pub(crate) const fn length(&self) -> u32 {
        self.length
    }

    pub(crate) const fn ordinal(&self) -> Option<u32> {
        self.ordinal
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TableBlockCacheKey {
    table: TableCacheTableId,
    address: TableBlockAddress,
}

impl TableBlockCacheKey {
    pub(crate) fn new(table: TableCacheTableId, address: TableBlockAddress) -> Self {
        Self { table, address }
    }

    pub(crate) const fn table(&self) -> &TableCacheTableId {
        &self.table
    }

    pub(crate) const fn address(&self) -> &TableBlockAddress {
        &self.address
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TableBlockCacheStats {
    hits: u64,
    misses: u64,
    inserts: u64,
    duplicate_inserts: u64,
    evictions: u64,
    /// W2.4: no-evict warm inserts skipped because the shard was full.
    skipped_full: u64,
    removes: u64,
    table_invalidations: u64,
    clears: u64,
    skipped_oversized: u64,
    skipped_disabled: u64,
    entries: usize,
    bytes: usize,
    capacity_bytes: usize,
}

impl TableBlockCacheStats {
    pub(crate) const fn hits(self) -> u64 {
        self.hits
    }

    pub(crate) const fn misses(self) -> u64 {
        self.misses
    }

    pub(crate) const fn inserts(self) -> u64 {
        self.inserts
    }

    pub(crate) const fn duplicate_inserts(self) -> u64 {
        self.duplicate_inserts
    }

    pub(crate) const fn evictions(self) -> u64 {
        self.evictions
    }

    pub(crate) const fn skipped_full(self) -> u64 {
        self.skipped_full
    }

    pub(crate) const fn removes(self) -> u64 {
        self.removes
    }

    pub(crate) const fn table_invalidations(self) -> u64 {
        self.table_invalidations
    }

    pub(crate) const fn clears(self) -> u64 {
        self.clears
    }

    pub(crate) const fn skipped_oversized(self) -> u64 {
        self.skipped_oversized
    }

    pub(crate) const fn skipped_disabled(self) -> u64 {
        self.skipped_disabled
    }

    pub(crate) const fn entries(self) -> usize {
        self.entries
    }

    pub(crate) const fn bytes(self) -> usize {
        self.bytes
    }

    pub(crate) const fn capacity_bytes(self) -> usize {
        self.capacity_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CacheInsert {
    Inserted(Arc<[u8]>),
    DuplicateExisting(Arc<[u8]>),
    SkippedDisabled(Arc<[u8]>),
    SkippedOversized(Arc<[u8]>),
    /// W2.4: a no-evict insert found the shard full — the block was not
    /// admitted (publish-time warming never displaces demand-cached blocks).
    SkippedFull(Arc<[u8]>),
}

impl CacheInsert {
    pub(crate) fn bytes(&self) -> Arc<[u8]> {
        match self {
            Self::Inserted(bytes)
            | Self::DuplicateExisting(bytes)
            | Self::SkippedDisabled(bytes)
            | Self::SkippedOversized(bytes)
            | Self::SkippedFull(bytes) => Arc::clone(bytes),
        }
    }
}

#[derive(Debug)]
pub(crate) struct TableBlockCache {
    shards: Vec<CacheShard>,
}

#[derive(Debug)]
struct CacheShard {
    state: Mutex<CacheState>,
}

/// Null slot index for the intrusive LRU links and free list.
const LRU_NIL: u32 = u32::MAX;

/// One slot in the [`LruSlab`] slab. `entry` is `None` while the slot is on the free list — its key
/// and value are dropped then, so real memory tracks the byte charge, not the slab's high-water mark.
/// `next` doubles as the free-list link for a free slot.
#[derive(Debug)]
struct LruSlot {
    entry: Option<(TableBlockCacheKey, Arc<[u8]>)>,
    prev: u32,
    next: u32,
}

/// BS4.1: an O(1) intrusive slab-index LRU. Replaces the `BTreeMap` + `VecDeque` whose `touch_recency`
/// did an O(n) linear scan on every hit and insert. A `HashMap` gives O(1) lookup; a doubly-linked
/// list threaded through the slab by `u32` indices (pointer-free, all-safe) gives O(1) ops. `head`
/// is the LRU end (the eviction victim); `tail` is the MRU end (touch/insert target).
#[derive(Debug)]
struct LruSlab {
    slots: Vec<LruSlot>,
    index: HashMap<TableBlockCacheKey, u32>,
    head: u32,
    tail: u32,
    free: u32,
    bytes: usize,
}

impl LruSlab {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            index: HashMap::new(),
            head: LRU_NIL,
            tail: LRU_NIL,
            free: LRU_NIL,
            bytes: 0,
        }
    }

    fn len(&self) -> usize {
        self.index.len()
    }

    fn bytes(&self) -> usize {
        self.bytes
    }

    /// Presence check without recency movement or byte access (C2 preheat probe).
    fn contains(&self, key: &TableBlockCacheKey) -> bool {
        self.index.contains_key(key)
    }

    /// Look up `key`, moving it to the MRU end (touch-on-hit). Returns a clone of the cached bytes.
    fn get(&mut self, key: &TableBlockCacheKey) -> Option<Arc<[u8]>> {
        let idx = *self.index.get(key)?;
        self.unlink(idx);
        self.push_tail(idx);
        Some(Arc::clone(
            &self.slots[idx as usize]
                .entry
                .as_ref()
                .expect("occupied slot has an entry")
                .1,
        ))
    }

    /// Insert a fresh entry at the MRU end. The caller guarantees `key` is absent and that eviction
    /// has already made room; the byte charge is added here.
    fn insert(&mut self, key: TableBlockCacheKey, value: Arc<[u8]>) {
        self.bytes = self.bytes.saturating_add(value.len());
        let idx = self.alloc();
        self.slots[idx as usize].entry = Some((key.clone(), value));
        self.index.insert(key, idx);
        self.push_tail(idx);
    }

    /// Remove `key` if present, refunding its byte charge. Returns whether it was present.
    fn remove(&mut self, key: &TableBlockCacheKey) -> bool {
        let Some(idx) = self.index.remove(key) else {
            return false;
        };
        self.detach_slot(idx);
        true
    }

    /// Evict the LRU entry (the `head`). Returns whether one was evicted.
    fn evict_lru(&mut self) -> bool {
        if self.head == LRU_NIL {
            return false;
        }
        let idx = self.head;
        let key = self.detach_slot(idx);
        self.index.remove(&key);
        true
    }

    /// Remove every entry whose key belongs to `table`, refunding their byte charge. O(n) — used only
    /// for invalidation, never on the read hot path.
    /// Drop every entry whose table is NOT in `live`, refunding byte
    /// charges. O(n) — invalidation only, never the read hot path.
    fn retain_tables(&mut self, live: &std::collections::BTreeSet<TableCacheTableId>) -> usize {
        let victims: Vec<u32> = self
            .index
            .iter()
            .filter(|(key, _)| !live.contains(key.table()))
            .map(|(_, &idx)| idx)
            .collect();
        let removed = victims.len();
        for idx in victims {
            let key = self.detach_slot(idx);
            self.index.remove(&key);
        }
        removed
    }

    fn remove_table(&mut self, table: &TableCacheTableId) -> usize {
        let victims: Vec<u32> = self
            .index
            .iter()
            .filter(|(key, _)| key.table() == table)
            .map(|(_, &idx)| idx)
            .collect();
        let removed = victims.len();
        for idx in victims {
            let key = self.detach_slot(idx);
            self.index.remove(&key);
        }
        removed
    }

    fn clear(&mut self) {
        self.slots.clear();
        self.index.clear();
        self.head = LRU_NIL;
        self.tail = LRU_NIL;
        self.free = LRU_NIL;
        self.bytes = 0;
    }

    /// Unlink slot `idx`, take its entry (dropping the value's memory), refund the byte charge, and
    /// return it to the free list. Returns the entry's key so callers can drop it from the index
    /// without a second borrow of `slots`.
    fn detach_slot(&mut self, idx: u32) -> TableBlockCacheKey {
        self.unlink(idx);
        let (key, value) = self.slots[idx as usize]
            .entry
            .take()
            .expect("occupied slot has an entry");
        self.bytes = self.bytes.saturating_sub(value.len());
        self.free(idx);
        key
    }

    fn alloc(&mut self) -> u32 {
        if self.free == LRU_NIL {
            let idx = u32::try_from(self.slots.len()).expect("block cache slab fits in u32");
            self.slots.push(LruSlot {
                entry: None,
                prev: LRU_NIL,
                next: LRU_NIL,
            });
            idx
        } else {
            let idx = self.free;
            self.free = self.slots[idx as usize].next;
            idx
        }
    }

    fn free(&mut self, idx: u32) {
        self.slots[idx as usize].entry = None;
        self.slots[idx as usize].prev = LRU_NIL;
        self.slots[idx as usize].next = self.free;
        self.free = idx;
    }

    fn unlink(&mut self, idx: u32) {
        let prev = self.slots[idx as usize].prev;
        let next = self.slots[idx as usize].next;
        if prev == LRU_NIL {
            self.head = next;
        } else {
            self.slots[prev as usize].next = next;
        }
        if next == LRU_NIL {
            self.tail = prev;
        } else {
            self.slots[next as usize].prev = prev;
        }
    }

    fn push_tail(&mut self, idx: u32) {
        let old_tail = self.tail;
        self.slots[idx as usize].prev = old_tail;
        self.slots[idx as usize].next = LRU_NIL;
        if old_tail == LRU_NIL {
            self.head = idx;
        } else {
            self.slots[old_tail as usize].next = idx;
        }
        self.tail = idx;
    }

    /// Walk the list head→tail (LRU→MRU) — the observable recency order, for the property test.
    #[cfg(test)]
    fn lru_order(&self) -> Vec<TableBlockCacheKey> {
        let mut order = Vec::with_capacity(self.index.len());
        let mut cursor = self.head;
        while cursor != LRU_NIL {
            let slot = &self.slots[cursor as usize];
            order.push(
                slot.entry
                    .as_ref()
                    .expect("occupied slot has an entry")
                    .0
                    .clone(),
            );
            cursor = slot.next;
        }
        order
    }
}

#[derive(Debug)]
struct CacheState {
    enabled: bool,
    capacity_bytes: usize,
    lru: LruSlab,
    stats: TableBlockCacheStats,
}

impl TableBlockCache {
    pub(crate) fn new(config: TableCacheConfig) -> Self {
        let capacities = shard_capacities(config.capacity_bytes(), cache_shard_count(config));
        perf_trace::set_table_cache_capacity_gauge(capacities.iter().sum::<usize>() as u64);
        Self {
            shards: capacities
                .into_iter()
                .map(|capacity_bytes| CacheShard {
                    state: Mutex::new(CacheState {
                        enabled: config.enabled(),
                        capacity_bytes,
                        lru: LruSlab::new(),
                        stats: TableBlockCacheStats {
                            capacity_bytes,
                            ..TableBlockCacheStats::default()
                        },
                    }),
                })
                .collect(),
        }
    }

    pub(crate) fn disabled() -> Self {
        let config = TableCacheConfig::new(false, 0)
            .expect("disabled table cache with zero capacity is valid");
        Self::new(config)
    }

    pub(crate) fn enabled(&self) -> bool {
        self.shards
            .first()
            .expect("table block cache always has at least one shard")
            .lock_state()
            .enabled
    }

    /// Total bytes currently resident across all shards. Folded into the database-wide memory
    /// total so cached blocks count against the budget.
    pub(crate) fn current_bytes(&self) -> u64 {
        self.shards.iter().fold(0u64, |total, shard| {
            total.saturating_add(u64::try_from(shard.lock_state().lru.bytes()).unwrap_or(u64::MAX))
        })
    }

    pub(crate) fn get(&self, key: &TableBlockCacheKey) -> Option<Arc<[u8]>> {
        let mut state = self.shard_for_key(key).lock_state();
        if let Some(bytes) = state.lru.get(key) {
            state.stats.hits = state.stats.hits.saturating_add(1);
            perf_trace::record_table_cache_hit();
            Some(bytes)
        } else {
            state.stats.misses = state.stats.misses.saturating_add(1);
            perf_trace::record_table_cache_miss();
            None
        }
    }

    /// W2.3 (B3): a stats-quiet lookup for DERIVED artifacts (accelerator
    /// entries). Touches the LRU like `get` but records no hit/miss — the
    /// hit-rate metrics keep meaning "data-block demand", not doubled by the
    /// per-seek accelerator probe.
    pub(crate) fn get_quiet(&self, key: &TableBlockCacheKey) -> Option<Arc<[u8]>> {
        self.shard_for_key(key).lock_state().lru.get(key)
    }

    /// C2: a recency-neutral presence probe for the preheat sweep. Records no
    /// hit/miss and does NOT touch the LRU — a sweep that promoted every block
    /// it inspected would overwrite demand recency with scan order, so `get` /
    /// `get_quiet` are unusable here.
    pub(crate) fn contains_quiet(&self, key: &TableBlockCacheKey) -> bool {
        self.shard_for_key(key).lock_state().lru.contains(key)
    }

    pub(crate) fn insert(
        &self,
        key: TableBlockCacheKey,
        bytes: Arc<[u8]>,
    ) -> TableRuntimeResult<CacheInsert> {
        if bytes.is_empty() {
            return Err(TableRuntimeError::InvalidRange {
                field: "cache_charge",
            });
        }

        let mut state = self.shard_for_key(&key).lock_state();
        if !state.enabled {
            state.stats.skipped_disabled = state.stats.skipped_disabled.saturating_add(1);
            perf_trace::record_table_cache_skipped_insert();
            return Ok(CacheInsert::SkippedDisabled(bytes));
        }

        if let Some(existing) = state.lru.get(&key) {
            state.stats.duplicate_inserts = state.stats.duplicate_inserts.saturating_add(1);
            return Ok(CacheInsert::DuplicateExisting(existing));
        }

        if bytes.len() > state.capacity_bytes {
            state.stats.skipped_oversized = state.stats.skipped_oversized.saturating_add(1);
            perf_trace::record_table_cache_skipped_insert();
            return Ok(CacheInsert::SkippedOversized(bytes));
        }

        evict_to_fit(&mut state, bytes.len());
        if state.lru.bytes().saturating_add(bytes.len()) > state.capacity_bytes {
            state.stats.skipped_oversized = state.stats.skipped_oversized.saturating_add(1);
            perf_trace::record_table_cache_skipped_insert();
            return Ok(CacheInsert::SkippedOversized(bytes));
        }

        state.lru.insert(key, Arc::clone(&bytes));
        state.stats.inserts = state.stats.inserts.saturating_add(1);
        perf_trace::record_table_cache_insert();
        refresh_gauges(&mut state);
        Ok(CacheInsert::Inserted(bytes))
    }

    /// W2.4: insert WITHOUT evicting — publish-time warming admits freshly
    /// built blocks only into free capacity, so unproven blocks never displace
    /// demand-cached (proven-hot) ones. Duplicate/disabled/oversized behavior
    /// matches [`insert`](Self::insert).
    pub(crate) fn insert_if_free(
        &self,
        key: TableBlockCacheKey,
        bytes: Arc<[u8]>,
    ) -> TableRuntimeResult<CacheInsert> {
        if bytes.is_empty() {
            return Err(TableRuntimeError::InvalidRange {
                field: "cache_charge",
            });
        }
        let mut state = self.shard_for_key(&key).lock_state();
        if !state.enabled {
            state.stats.skipped_disabled = state.stats.skipped_disabled.saturating_add(1);
            perf_trace::record_table_cache_skipped_insert();
            return Ok(CacheInsert::SkippedDisabled(bytes));
        }
        if let Some(existing) = state.lru.get(&key) {
            state.stats.duplicate_inserts = state.stats.duplicate_inserts.saturating_add(1);
            return Ok(CacheInsert::DuplicateExisting(existing));
        }
        if bytes.len() > state.capacity_bytes {
            state.stats.skipped_oversized = state.stats.skipped_oversized.saturating_add(1);
            perf_trace::record_table_cache_skipped_insert();
            return Ok(CacheInsert::SkippedOversized(bytes));
        }
        if state.lru.bytes().saturating_add(bytes.len()) > state.capacity_bytes {
            state.stats.skipped_full = state.stats.skipped_full.saturating_add(1);
            perf_trace::record_table_cache_skipped_insert();
            return Ok(CacheInsert::SkippedFull(bytes));
        }
        state.lru.insert(key, Arc::clone(&bytes));
        state.stats.inserts = state.stats.inserts.saturating_add(1);
        perf_trace::record_table_cache_insert();
        refresh_gauges(&mut state);
        Ok(CacheInsert::Inserted(bytes))
    }

    pub(crate) fn remove(&self, key: &TableBlockCacheKey) -> bool {
        let mut state = self.shard_for_key(key).lock_state();
        if state.lru.remove(key) {
            state.stats.removes = state.stats.removes.saturating_add(1);
            refresh_gauges(&mut state);
            true
        } else {
            false
        }
    }

    /// C3a: drop every entry belonging to tables OUTSIDE the live set. The
    /// capped quarantine sweep leaves dead-table blocks resident for many
    /// cycles after compaction churn (measured: ~260K dead entries pinning
    /// the pool at capacity, whose eviction churn WAS the residual miss
    /// floor); a completed preheat pass knows the exact live set and cleans
    /// in one shot.
    pub(crate) fn retain_tables(
        &self,
        live: &std::collections::BTreeSet<TableCacheTableId>,
    ) -> usize {
        let mut removed = 0usize;
        for shard in &self.shards {
            let mut state = shard.lock_state();
            let dropped = state.lru.retain_tables(live);
            if dropped > 0 {
                state.stats.table_invalidations = state
                    .stats
                    .table_invalidations
                    .saturating_add(dropped as u64);
                refresh_gauges(&mut state);
                removed += dropped;
            }
        }
        removed
    }

    pub(crate) fn remove_table(&self, table: &TableCacheTableId) -> usize {
        let mut removed = 0usize;
        let mut states = self.lock_all_states();
        for state in &mut states {
            removed = removed.saturating_add(state.lru.remove_table(table));
            refresh_gauges(state);
        }
        if removed > 0 {
            let first_state = states
                .first_mut()
                .expect("table block cache always has at least one shard");
            first_state.stats.table_invalidations =
                first_state.stats.table_invalidations.saturating_add(1);
            refresh_gauges(first_state);
        }
        removed
    }

    pub(crate) fn clear(&self) {
        let mut states = self.lock_all_states();
        for state in &mut states {
            state.lru.clear();
            refresh_gauges(state);
        }
        let first_state = states
            .first_mut()
            .expect("table block cache always has at least one shard");
        first_state.stats.clears = first_state.stats.clears.saturating_add(1);
        refresh_gauges(first_state);
    }

    pub(crate) fn resize(&self, capacity_bytes: usize) {
        let capacities = shard_capacities(capacity_bytes, self.shards.len());
        perf_trace::set_table_cache_capacity_gauge(capacities.iter().sum::<usize>() as u64);
        let mut states = self.lock_all_states();
        for (state, capacity_bytes) in states.iter_mut().zip(capacities) {
            state.capacity_bytes = capacity_bytes;
            state.stats.capacity_bytes = capacity_bytes;
            evict_to_capacity(state);
            refresh_gauges(state);
        }
    }

    pub(crate) fn stats(&self) -> TableBlockCacheStats {
        let mut stats = TableBlockCacheStats::default();
        let shard_guards = self.lock_all_states();
        for cache_state in &shard_guards {
            let mut shard_stats = cache_state.stats;
            shard_stats.entries = cache_state.lru.len();
            shard_stats.bytes = cache_state.lru.bytes();
            shard_stats.capacity_bytes = cache_state.capacity_bytes;
            stats = merge_cache_stats(stats, shard_stats);
        }
        stats
    }

    #[cfg(test)]
    pub(crate) fn poison_for_test(&self) {
        for shard in &self.shards {
            let _ = std::panic::catch_unwind(|| {
                let _guard = shard
                    .state
                    .lock()
                    .expect("test lock should not already be poisoned");
                panic!("poison table cache mutex for recovery test");
            });
        }
    }

    #[cfg(test)]
    pub(crate) fn shard_count_for_test(&self) -> usize {
        self.shards.len()
    }

    #[cfg(test)]
    pub(crate) fn shard_index_for_test(&self, key: &TableBlockCacheKey) -> usize {
        shard_index(key, self.shards.len())
    }

    fn shard_for_key(&self, key: &TableBlockCacheKey) -> &CacheShard {
        &self.shards[shard_index(key, self.shards.len())]
    }

    fn lock_all_states(&self) -> Vec<MutexGuard<'_, CacheState>> {
        self.shards.iter().map(CacheShard::lock_state).collect()
    }
}

impl CacheShard {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableBloomProbe {
    DefinitelyAbsent,
    MaybePresent,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableBloomFilter {
    bits: Vec<u8>,
    bit_count: usize,
    probes: u8,
    key_count: usize,
}

impl TableBloomFilter {
    pub(crate) fn build<'a>(
        keys: impl IntoIterator<Item = &'a [u8]>,
        bits_per_key: usize,
    ) -> TableRuntimeResult<Self> {
        if bits_per_key == 0 {
            return Err(TableRuntimeError::InvalidConfig {
                field: "bits_per_key",
                reason: "must be nonzero",
            });
        }

        let keys = keys.into_iter().collect::<Vec<_>>();
        if keys.is_empty() {
            return Ok(Self {
                bits: Vec::new(),
                bit_count: 0,
                probes: 1,
                key_count: 0,
            });
        }

        let bit_count = keys
            .len()
            .checked_mul(bits_per_key)
            .ok_or(TableRuntimeError::InvalidRange {
                field: "bloom_bits",
            })?
            .max(8);
        let byte_count = bit_count.div_ceil(8);
        if byte_count > MAX_BLOOM_BYTES {
            return Err(TableRuntimeError::InvalidRange {
                field: "bloom_bytes",
            });
        }

        let probes = optimal_probe_count(bits_per_key);
        let mut filter = Self {
            bits: vec![0; byte_count],
            bit_count,
            probes,
            key_count: keys.len(),
        };
        for key in keys {
            filter.insert_key(key);
        }
        Ok(filter)
    }

    pub(crate) fn might_contain(&self, key: &[u8]) -> TableBloomProbe {
        if self.key_count == 0 {
            perf_trace::record_table_filter_negative_probe();
            return TableBloomProbe::DefinitelyAbsent;
        }
        if self.bits.is_empty() || self.bit_count == 0 || self.probes == 0 {
            perf_trace::record_table_filter_absent_probe();
            return TableBloomProbe::Unavailable;
        }
        for probe in 0..self.probes {
            let bit = bloom_bit(key, probe, self.bit_count);
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                perf_trace::record_table_filter_negative_probe();
                return TableBloomProbe::DefinitelyAbsent;
            }
        }
        perf_trace::record_table_filter_positive_probe();
        TableBloomProbe::MaybePresent
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.key_count == 0
    }

    pub(crate) fn approximate_size_bytes(&self) -> usize {
        self.bits.len()
    }

    pub(crate) const fn key_count(&self) -> usize {
        self.key_count
    }

    pub(crate) const fn probes(&self) -> u8 {
        self.probes
    }

    pub(crate) const fn bit_count(&self) -> usize {
        self.bit_count
    }

    pub(crate) fn bits(&self) -> &[u8] {
        &self.bits
    }

    /// BS4.2: reconstruct a filter from a persisted frame's raw parts (the inverse of the
    /// `probes`/`key_count`/`bit_count`/`bits` accessors). Re-validates the bounds so a direct caller
    /// is as safe as the frame decoder; a byte-exact round-trip of `{bits, bit_count, probes}`
    /// reproduces every `might_contain` result, and `key_count` drives the empty short-circuit.
    pub(crate) fn from_frame_parts(
        probes: u8,
        key_count: u64,
        bit_count: u64,
        bits: Vec<u8>,
    ) -> TableRuntimeResult<Self> {
        if probes > MAX_BLOOM_PROBES {
            return Err(TableRuntimeError::InvalidRange {
                field: "bloom_probes",
            });
        }
        if bits.len() > MAX_BLOOM_BYTES {
            return Err(TableRuntimeError::InvalidRange {
                field: "bloom_bytes",
            });
        }
        let bit_count =
            usize::try_from(bit_count).map_err(|_| TableRuntimeError::InvalidRange {
                field: "bloom_bits",
            })?;
        if bits.len() != bit_count.div_ceil(8) {
            return Err(TableRuntimeError::InvalidRange {
                field: "bloom_bits",
            });
        }
        let key_count =
            usize::try_from(key_count).map_err(|_| TableRuntimeError::InvalidRange {
                field: "bloom_key_count",
            })?;
        Ok(Self {
            bits,
            bit_count,
            probes,
            key_count,
        })
    }

    fn insert_key(&mut self, key: &[u8]) {
        for probe in 0..self.probes {
            let bit = bloom_bit(key, probe, self.bit_count);
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }
}

fn evict_to_fit(state: &mut CacheState, incoming_bytes: usize) {
    while state.lru.bytes().saturating_add(incoming_bytes) > state.capacity_bytes {
        if !evict_one(state) {
            break;
        }
    }
}

fn evict_to_capacity(state: &mut CacheState) {
    while state.lru.bytes() > state.capacity_bytes {
        if !evict_one(state) {
            break;
        }
    }
}

fn evict_one(state: &mut CacheState) -> bool {
    if state.lru.evict_lru() {
        state.stats.evictions = state.stats.evictions.saturating_add(1);
        perf_trace::record_table_cache_eviction();
        refresh_gauges(state);
        true
    } else {
        false
    }
}

fn refresh_gauges(state: &mut CacheState) {
    // C3a: occupancy gauges advance by two's-complement delta so the
    // process-wide atomics track absolute bytes/entries without a
    // full-shard sweep.
    perf_trace::adjust_table_cache_occupancy(
        (state.lru.bytes() as u64).wrapping_sub(state.stats.bytes as u64),
        (state.lru.len() as u64).wrapping_sub(state.stats.entries as u64),
    );
    state.stats.entries = state.lru.len();
    state.stats.bytes = state.lru.bytes();
    state.stats.capacity_bytes = state.capacity_bytes;
}

fn cache_shard_count(config: TableCacheConfig) -> usize {
    if !config.enabled() || config.capacity_bytes() == 0 {
        return 1;
    }
    // BS4.1: size shards from CPU parallelism (was capacity-derived, capped at 16 — a low ceiling on
    // many-core boxes). `available_parallelism` is a query, not a thread spawn — it errors on wasm,
    // where we fall back to the floor (C1: no new threads). Cap by capacity so no shard is smaller
    // than one block: a shard that can't hold a block would oversized-reject every insert.
    let by_cpu = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(MIN_SHARDS)
        .clamp(MIN_SHARDS, MAX_SHARDS);
    let by_capacity = (config.capacity_bytes() / TARGET_SHARD_BYTES).max(1);
    by_cpu.min(by_capacity)
}

fn shard_capacities(capacity_bytes: usize, shard_count: usize) -> Vec<usize> {
    let shard_count = shard_count.max(1);
    let base = capacity_bytes / shard_count;
    let remainder = capacity_bytes % shard_count;
    (0..shard_count)
        .map(|index| base + usize::from(index < remainder))
        .collect()
}

fn shard_index(key: &TableBlockCacheKey, shard_count: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let shard_count = u64::try_from(shard_count.max(1)).expect("shard count fits in u64");
    usize::try_from(hasher.finish() % shard_count).expect("shard index fits in usize")
}

fn merge_cache_stats(
    mut left: TableBlockCacheStats,
    right: TableBlockCacheStats,
) -> TableBlockCacheStats {
    left.hits = left.hits.saturating_add(right.hits);
    left.misses = left.misses.saturating_add(right.misses);
    left.inserts = left.inserts.saturating_add(right.inserts);
    left.duplicate_inserts = left
        .duplicate_inserts
        .saturating_add(right.duplicate_inserts);
    left.evictions = left.evictions.saturating_add(right.evictions);
    left.removes = left.removes.saturating_add(right.removes);
    left.table_invalidations = left
        .table_invalidations
        .saturating_add(right.table_invalidations);
    left.clears = left.clears.saturating_add(right.clears);
    left.skipped_oversized = left
        .skipped_oversized
        .saturating_add(right.skipped_oversized);
    left.skipped_disabled = left.skipped_disabled.saturating_add(right.skipped_disabled);
    left.skipped_full = left.skipped_full.saturating_add(right.skipped_full);
    left.entries = left.entries.saturating_add(right.entries);
    left.bytes = left.bytes.saturating_add(right.bytes);
    left.capacity_bytes = left.capacity_bytes.saturating_add(right.capacity_bytes);
    left
}

fn optimal_probe_count(bits_per_key: usize) -> u8 {
    let probes = bits_per_key.saturating_mul(693).saturating_add(999) / 1000;
    u8::try_from(probes.clamp(1, usize::from(MAX_BLOOM_PROBES)))
        .expect("clamped bloom probe count fits in u8")
}

fn bloom_bit(key: &[u8], probe: u8, bit_count: usize) -> usize {
    let mixed = fnv1a64(key, 0xcbf2_9ce4_8422_2325 ^ u64::from(probe))
        .wrapping_add(u64::from(probe).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let bit_count = u64::try_from(bit_count).expect("bloom bit count fits in u64");
    usize::try_from(mixed % bit_count).expect("bloom bit index fits in usize")
}

fn fnv1a64(bytes: &[u8], seed: u64) -> u64 {
    let mut hash = seed;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

fn fmt_bounded_bytes(formatter: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let shown = bytes.len().min(MAX_DEBUG_BYTES);
    for byte in &bytes[..shown] {
        formatter.write_str(char::from(HEX[usize::from(byte >> 4)]).encode_utf8(&mut [0; 4]))?;
        formatter.write_str(char::from(HEX[usize::from(byte & 0x0f)]).encode_utf8(&mut [0; 4]))?;
    }
    if bytes.len() > shown {
        write!(formatter, "...({} bytes)", bytes.len())?;
    }
    Ok(())
}

/// BS4.1: property test for the intrusive `LruSlab` — it must produce byte-for-byte the same recency
/// order, eviction victims, byte charge, and length as a naive reference LRU under every op sequence.
/// Lives in-file because `LruSlab` and its links are private; deterministic (no sharding), so it is
/// the home for eviction-victim exactness. `wasm32` is excluded like the other proptest suites.
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod lru_slab_property_tests {
    use super::{
        LruSlab, TableBlockAddress, TableBlockCacheKey, TableBlockCacheKind, TableCacheTableId,
    };
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::test_runner::{Config, FileFailurePersistence, TestRunner};
    use std::collections::HashMap;
    use std::sync::Arc;

    // A small key space so reuse, collisions, and free-slot recycling all occur under short sequences.
    const KEY_SPACE: u8 = 6;

    fn test_key(id: u8) -> TableBlockCacheKey {
        TableBlockCacheKey::new(
            TableCacheTableId::new([id]).expect("cache id"),
            TableBlockAddress::new(
                TableBlockCacheKind::Data,
                u64::from(id),
                4,
                Some(u32::from(id)),
            )
            .expect("block address"),
        )
    }

    fn payload(len: usize) -> Arc<[u8]> {
        Arc::<[u8]>::from(vec![0u8; len])
    }

    /// Naive reference LRU: `order` runs LRU (front) → MRU (back), mirroring `LruSlab` head → tail.
    #[derive(Default)]
    struct ReferenceLru {
        order: Vec<TableBlockCacheKey>,
        sizes: HashMap<TableBlockCacheKey, usize>,
        bytes: usize,
    }

    impl ReferenceLru {
        fn contains(&self, key: &TableBlockCacheKey) -> bool {
            self.sizes.contains_key(key)
        }

        fn insert(&mut self, key: TableBlockCacheKey, len: usize) {
            self.bytes += len;
            self.sizes.insert(key.clone(), len);
            self.order.push(key);
        }

        fn get(&mut self, key: &TableBlockCacheKey) -> bool {
            if self.sizes.contains_key(key) {
                self.order.retain(|candidate| candidate != key);
                self.order.push(key.clone());
                true
            } else {
                false
            }
        }

        fn remove(&mut self, key: &TableBlockCacheKey) -> bool {
            match self.sizes.remove(key) {
                Some(len) => {
                    self.bytes -= len;
                    self.order.retain(|candidate| candidate != key);
                    true
                }
                None => false,
            }
        }

        fn evict(&mut self) -> bool {
            if self.order.is_empty() {
                return false;
            }
            let victim = self.order.remove(0);
            let len = self.sizes.remove(&victim).expect("evicted key is sized");
            self.bytes -= len;
            true
        }
    }

    #[derive(Clone, Debug)]
    enum Op {
        Insert { id: u8, len: usize },
        Get { id: u8 },
        Remove { id: u8 },
        Evict,
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            (0u8..KEY_SPACE, 1usize..8usize).prop_map(|(id, len)| Op::Insert { id, len }),
            (0u8..KEY_SPACE).prop_map(|id| Op::Get { id }),
            (0u8..KEY_SPACE).prop_map(|id| Op::Remove { id }),
            Just(Op::Evict),
        ]
    }

    fn run_sequence(ops: &[Op]) -> Result<(), TestCaseError> {
        let mut slab = LruSlab::new();
        let mut model = ReferenceLru::default();
        for op in ops {
            match op {
                // `LruSlab::insert` requires an absent key (CacheState screens duplicates upstream);
                // mirror that contract.
                Op::Insert { id, len } => {
                    let key = test_key(*id);
                    if !model.contains(&key) {
                        slab.insert(key.clone(), payload(*len));
                        model.insert(key, *len);
                    }
                }
                Op::Get { id } => {
                    let key = test_key(*id);
                    prop_assert_eq!(slab.get(&key).is_some(), model.get(&key));
                }
                Op::Remove { id } => {
                    let key = test_key(*id);
                    prop_assert_eq!(slab.remove(&key), model.remove(&key));
                }
                Op::Evict => {
                    prop_assert_eq!(slab.evict_lru(), model.evict());
                }
            }
            // Identical recency order after every op ⇒ identical future eviction victims. Bytes and
            // length must also track exactly.
            prop_assert_eq!(slab.lru_order(), model.order.clone());
            prop_assert_eq!(slab.bytes(), model.bytes);
            prop_assert_eq!(slab.len(), model.order.len());
        }
        Ok(())
    }

    #[test]
    fn lru_slab_matches_reference_under_every_op_sequence() {
        let mut runner = TestRunner::new(Config {
            failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
                "proptest-regressions/table/cache_lru_slab.txt",
            ))),
            ..Config::default()
        });
        runner
            .run(&vec(op_strategy(), 1..=200), |ops| run_sequence(&ops))
            .expect("LruSlab must match the reference LRU under every op sequence");
    }
}
