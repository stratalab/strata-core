//! Vector Heap - Contiguous embedding storage
//!
//! VectorHeap stores embeddings in a contiguous Vec<f32> for cache-friendly
//! similarity computation. Uses BTreeMap for deterministic iteration order.
//!
//! # Critical Invariants
//!
//! - **S4**: VectorIds are NEVER reused, only storage slots are reused
//! - **S7**: id_to_offset is the SOLE source of truth for active vectors
//! - **T4**: next_id is monotonically increasing and MUST be persisted in snapshots
//! - **R3**: BTreeMap guarantees deterministic iteration order

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::primitives::vector::error::{VectorError, VectorResult};
use crate::primitives::vector::mmap::{self, MmapVectorData};
use crate::primitives::vector::types::{DistanceMetric, InlineMeta, VectorConfig, VectorId};

/// Backing storage for vector embeddings.
///
/// - `InMemory`: Heap-allocated `Vec<f32>` (default, mutable).
/// - `Mmap`: Memory-mapped file (read-only, OS-paged).
/// - `Tiered`: mmap base + small in-memory overlay for bounded-memory indexing.
pub(crate) enum VectorData {
    InMemory(Vec<f32>),
    Mmap(MmapVectorData),
    /// mmap base layer with a mutable overlay for new inserts.
    ///
    /// After a periodic flush, the overlay is merged into the base mmap and
    /// the overlay is reset. This keeps anonymous memory bounded to the
    /// overlay size (configurable, default ~768 MB for 500K × 384d).
    Tiered {
        base: MmapVectorData,
        overlay: Vec<f32>,
        /// VectorId → offset **within the overlay** Vec (in f32 elements).
        overlay_id_to_offset: BTreeMap<VectorId, usize>,
        /// Free slots within the overlay Vec.
        overlay_free_slots: Vec<usize>,
    },
}

/// Per-collection vector heap
///
/// Stores embeddings in a contiguous Vec<f32> for cache-friendly
/// similarity computation. Uses BTreeMap for deterministic iteration.
///
/// # Critical Invariants
///
/// - id_to_offset is the SOLE source of truth for active vectors (S7)
/// - VectorIds are NEVER reused, only storage slots are reused (S4)
/// - next_id is monotonically increasing and MUST be persisted in snapshots (T4)
/// - free_slots MUST be persisted in snapshots for correct recovery
pub struct VectorHeap {
    /// Collection configuration
    config: VectorConfig,

    /// Embedding storage — either heap-allocated or memory-mapped.
    ///
    /// Layout (InMemory): [v0_dim0, v0_dim1, ..., v0_dimN, v1_dim0, ...]
    /// Each vector occupies `config.dimension` consecutive f32 values.
    /// Mmap variant is read-only; mutating methods panic on it.
    data: VectorData,

    /// VectorId -> offset in data (in floats, not bytes)
    ///
    /// IMPORTANT: Use BTreeMap for deterministic iteration order.
    /// HashMap would cause nondeterministic search results.
    /// This is the SOLE source of truth for active vectors.
    ///
    /// For the Mmap variant this mirrors the mmap's id_to_offset.
    id_to_offset: BTreeMap<VectorId, usize>,

    /// Free list for deleted storage slots (enables slot reuse)
    ///
    /// When a vector is deleted, its storage slot offset is added here.
    /// New inserts can reuse these slots to avoid unbounded memory growth.
    ///
    /// NOTE: Storage slots are reused, but VectorId values are NEVER reused.
    /// This must be persisted in snapshots for correct recovery.
    free_slots: Vec<usize>,

    /// Next VectorId to allocate (monotonically increasing)
    ///
    /// This value is NEVER decremented, even after deletions.
    /// MUST be persisted in snapshots to maintain ID uniqueness across restarts.
    /// Without this, recovery could reuse IDs and break replay determinism.
    ///
    /// # Memory Ordering
    ///
    /// Uses Relaxed ordering for fetch_add because:
    /// 1. fetch_add is atomic and guarantees each caller gets a unique value
    /// 2. No other memory operations are synchronized by this counter
    /// 3. The uniqueness guarantee comes from the atomic operation, not ordering
    next_id: AtomicU64,

    /// Version counter for snapshot consistency
    version: AtomicU64,

    /// Inline metadata for O(1) search resolution (VectorId → key + source_ref).
    /// Populated during insert and recovery, queried during search.
    inline_meta: BTreeMap<VectorId, InlineMeta>,
}

impl VectorHeap {
    /// Create a new vector heap with the given configuration
    ///
    /// Note: next_id starts at 1, not 0, to match expected VectorId semantics
    /// where IDs are positive integers.
    pub fn new(config: VectorConfig) -> Self {
        VectorHeap {
            config,
            data: VectorData::InMemory(Vec::new()),
            id_to_offset: BTreeMap::new(),
            free_slots: Vec::new(),
            next_id: AtomicU64::new(1),
            version: AtomicU64::new(0),
            inline_meta: BTreeMap::new(),
        }
    }

    /// Create from snapshot data (for recovery)
    ///
    /// CRITICAL: next_id and free_slots MUST be restored from snapshot
    /// to maintain invariants T4 (VectorId monotonicity across crashes).
    pub fn from_snapshot(
        config: VectorConfig,
        data: Vec<f32>,
        id_to_offset: BTreeMap<VectorId, usize>,
        free_slots: Vec<usize>,
        next_id: u64,
    ) -> Self {
        VectorHeap {
            config,
            data: VectorData::InMemory(data),
            id_to_offset,
            free_slots,
            next_id: AtomicU64::new(next_id),
            version: AtomicU64::new(0),
            inline_meta: BTreeMap::new(),
        }
    }

    /// Open from an existing mmap file.
    ///
    /// The heap is read-only — mutating methods (`upsert`, `delete`, etc.) will panic.
    /// Use this for disk-backed databases on startup when the `.vec` cache file exists.
    pub fn from_mmap(path: &Path, config: VectorConfig) -> Result<Self, VectorError> {
        let mmap_data = MmapVectorData::open(path, config.dimension)?;
        let id_to_offset = mmap_data.id_to_offset(); // returns owned BTreeMap
        let free_slots = mmap_data.free_slots().to_vec();
        let next_id = mmap_data.next_id();
        Ok(VectorHeap {
            config,
            data: VectorData::Mmap(mmap_data),
            id_to_offset,
            free_slots,
            next_id: AtomicU64::new(next_id),
            version: AtomicU64::new(0),
            inline_meta: BTreeMap::new(),
        })
    }

    /// Get the dimension of vectors in this heap
    pub fn dimension(&self) -> usize {
        self.config.dimension
    }

    /// Get the distance metric
    pub fn metric(&self) -> DistanceMetric {
        self.config.metric
    }

    /// Get the config
    pub fn config(&self) -> &VectorConfig {
        &self.config
    }

    /// Get the number of active vectors
    pub fn len(&self) -> usize {
        self.id_to_offset.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.id_to_offset.is_empty()
    }

    /// Get current version (for snapshot consistency)
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Get next_id value (for snapshot persistence)
    pub fn next_id_value(&self) -> u64 {
        self.next_id.load(Ordering::Relaxed)
    }

    /// Get free_slots (for snapshot persistence)
    ///
    /// For `InMemory` and `Mmap` heaps, returns the tracked free slots.
    /// For `Tiered` heaps, returns an empty slice because `freeze_to_disk()`
    /// compacts the data and eliminates all free slots.
    pub fn free_slots(&self) -> &[usize] {
        match &self.data {
            VectorData::Tiered { .. } => &[],
            _ => &self.free_slots,
        }
    }

    /// Restore snapshot state (for recovery)
    ///
    /// Called after all vectors have been inserted with insert_with_id()
    /// to restore the exact next_id and free_slots from the snapshot.
    ///
    /// CRITICAL: This ensures VectorId uniqueness across restarts (T4).
    pub fn restore_snapshot_state(&mut self, next_id: u64, free_slots: Vec<usize>) {
        self.next_id.store(next_id, Ordering::Relaxed);
        self.free_slots = free_slots;
    }

    /// Allocate a new VectorId (monotonically increasing)
    ///
    /// This NEVER returns a previously used ID, even after deletions.
    /// This is the per-collection counter that ensures deterministic
    /// VectorId assignment across separate databases.
    pub fn allocate_id(&self) -> VectorId {
        VectorId::new(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    // ========================================================================
    // Insert/Upsert Operations
    // ========================================================================

    /// Insert or update a vector (upsert semantics)
    ///
    /// If the VectorId already exists, updates in place.
    /// If new, allocates a slot (reusing deleted slots if available).
    ///
    /// IMPORTANT: When reusing a slot, MUST copy embedding into that slot.
    pub fn upsert(&mut self, id: VectorId, embedding: &[f32]) -> VectorResult<()> {
        // Validate dimension
        let dim = self.config.dimension;
        if embedding.len() != dim {
            return Err(VectorError::DimensionMismatch {
                expected: dim,
                got: embedding.len(),
            });
        }

        match &mut self.data {
            VectorData::InMemory(vec) => {
                if let Some(&offset) = self.id_to_offset.get(&id) {
                    vec[offset..offset + dim].copy_from_slice(embedding);
                } else {
                    let offset = if let Some(slot) = self.free_slots.pop() {
                        vec[slot..slot + dim].copy_from_slice(embedding);
                        slot
                    } else {
                        let offset = vec.len();
                        vec.extend_from_slice(embedding);
                        offset
                    };
                    self.id_to_offset.insert(id, offset);
                }
            }
            VectorData::Mmap(_) => panic!("cannot mutate mmap-backed VectorHeap"),
            VectorData::Tiered {
                overlay,
                overlay_id_to_offset,
                overlay_free_slots,
                ..
            } => {
                // Check if already in overlay — update in place
                if let Some(&off) = overlay_id_to_offset.get(&id) {
                    overlay[off..off + dim].copy_from_slice(embedding);
                } else {
                    // New insert into overlay (even if it exists in base mmap,
                    // the overlay takes precedence on reads)
                    let offset = if let Some(slot) = overlay_free_slots.pop() {
                        overlay[slot..slot + dim].copy_from_slice(embedding);
                        slot
                    } else {
                        let offset = overlay.len();
                        overlay.extend_from_slice(embedding);
                        offset
                    };
                    overlay_id_to_offset.insert(id, offset);
                }
                // id_to_offset tracks presence for len()/contains()/iter().
                // The offset value is not meaningful for Tiered: get()/iter()
                // resolve embeddings via overlay_id_to_offset or base mmap.
                // freeze_to_disk() builds merged_offsets independently.
                self.id_to_offset.insert(id, 0);
            }
        }

        self.version.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Insert a new vector, allocating a new VectorId
    ///
    /// Returns the allocated VectorId.
    pub fn insert(&mut self, embedding: &[f32]) -> VectorResult<VectorId> {
        let id = self.allocate_id();
        self.upsert(id, embedding)?;
        Ok(id)
    }

    /// Insert with a specific VectorId (for WAL replay)
    ///
    /// Used during recovery to replay WAL entries with their original IDs.
    /// Updates next_id if necessary to maintain monotonicity.
    pub fn insert_with_id(&mut self, id: VectorId, embedding: &[f32]) -> VectorResult<()> {
        // Ensure next_id stays ahead of all assigned IDs
        let id_val = id.as_u64();
        loop {
            let current = self.next_id.load(Ordering::Relaxed);
            if current > id_val {
                break;
            }
            if self
                .next_id
                .compare_exchange(current, id_val + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        self.upsert(id, embedding)
    }

    // ========================================================================
    // Delete Operations
    // ========================================================================

    /// Delete a vector by ID
    ///
    /// Returns true if the vector existed and was deleted.
    /// The storage slot is added to free_slots for reuse.
    /// The VectorId is NEVER reused (Invariant S4).
    ///
    /// Security note: Data is zeroed to prevent information leakage.
    pub fn delete(&mut self, id: VectorId) -> bool {
        match &mut self.data {
            VectorData::InMemory(v) => {
                if let Some(offset) = self.id_to_offset.remove(&id) {
                    self.free_slots.push(offset);
                    let dim = self.config.dimension;
                    v[offset..offset + dim].fill(0.0);
                    self.version.fetch_add(1, Ordering::Release);
                    true
                } else {
                    false
                }
            }
            VectorData::Mmap(_) => {
                if self.id_to_offset.remove(&id).is_some() {
                    // mmap pages are read-only; skip zeroing.
                    // The mmap file will be re-frozen without the deleted vector.
                    self.version.fetch_add(1, Ordering::Release);
                    true
                } else {
                    false
                }
            }
            VectorData::Tiered {
                overlay,
                overlay_id_to_offset,
                overlay_free_slots,
                ..
            } => {
                if self.id_to_offset.remove(&id).is_none() {
                    return false;
                }
                // If in overlay, reclaim the slot
                if let Some(off) = overlay_id_to_offset.remove(&id) {
                    overlay_free_slots.push(off);
                    let dim = self.config.dimension;
                    overlay[off..off + dim].fill(0.0);
                }
                // If in base mmap only: nothing to zero (read-only pages).
                // The next flush will exclude it.
                self.version.fetch_add(1, Ordering::Release);
                true
            }
        }
    }

    /// Delete a vector by ID (for WAL replay)
    ///
    /// Same as delete(), but explicitly named for WAL replay context.
    pub fn delete_replay(&mut self, id: VectorId) -> bool {
        self.delete(id)
    }

    /// Clear all vectors (for testing or collection deletion)
    pub fn clear(&mut self) {
        self.data = VectorData::InMemory(Vec::new());
        self.id_to_offset.clear();
        self.free_slots.clear();
        self.inline_meta.clear();
        // Note: next_id is NOT reset — IDs are never reused
        self.version.fetch_add(1, Ordering::Release);
    }

    // ========================================================================
    // Inline Metadata (O(1) search resolution)
    // ========================================================================

    /// Set inline metadata for a VectorId (for O(1) search resolution).
    pub(crate) fn set_inline_meta(&mut self, id: VectorId, meta: InlineMeta) {
        self.inline_meta.insert(id, meta);
    }

    /// Get inline metadata for a VectorId.
    pub(crate) fn get_inline_meta(&self, id: VectorId) -> Option<&InlineMeta> {
        self.inline_meta.get(&id)
    }

    /// Remove inline metadata for a VectorId.
    pub(crate) fn remove_inline_meta(&mut self, id: VectorId) {
        self.inline_meta.remove(&id);
    }

    // ========================================================================
    // Read Operations
    // ========================================================================

    /// Get embedding by VectorId
    ///
    /// Returns None if the vector doesn't exist.
    /// Works for both InMemory and Mmap backing.
    pub fn get(&self, id: VectorId) -> Option<&[f32]> {
        match &self.data {
            VectorData::InMemory(vec) => {
                let offset = *self.id_to_offset.get(&id)?;
                let start = offset;
                let end = offset + self.config.dimension;
                Some(&vec[start..end])
            }
            VectorData::Mmap(mmap) => {
                // id_to_offset is the sole source of truth for active vectors (S7).
                // A vector may have been deleted at runtime (removed from
                // id_to_offset) while still present in the mmap file.
                if !self.id_to_offset.contains_key(&id) {
                    return None;
                }
                mmap.get(id)
            }
            VectorData::Tiered {
                base,
                overlay,
                overlay_id_to_offset,
                ..
            } => {
                // Check liveness first
                if !self.id_to_offset.contains_key(&id) {
                    return None;
                }
                // Overlay takes precedence over base
                if let Some(&off) = overlay_id_to_offset.get(&id) {
                    let dim = self.config.dimension;
                    return Some(&overlay[off..off + dim]);
                }
                base.get(id)
            }
        }
    }

    /// Check if a vector exists
    pub fn contains(&self, id: VectorId) -> bool {
        self.id_to_offset.contains_key(&id)
    }

    /// Iterate all vectors in deterministic order (sorted by VectorId)
    ///
    /// IMPORTANT: This uses BTreeMap iteration which guarantees sorted order.
    /// This is critical for deterministic brute-force search (Invariant R3).
    /// HashMap iteration would be nondeterministic.
    pub fn iter(&self) -> impl Iterator<Item = (VectorId, &[f32])> {
        let data = &self.data;
        let dim = self.config.dimension;
        // BTreeMap iterates in key order (VectorId ascending)
        self.id_to_offset.iter().map(move |(&id, &offset)| {
            let embedding = match data {
                VectorData::InMemory(vec) => &vec[offset..offset + dim],
                VectorData::Mmap(mmap) => mmap.get(id).expect("id_to_offset has stale entry"),
                VectorData::Tiered {
                    base,
                    overlay,
                    overlay_id_to_offset,
                    ..
                } => {
                    if let Some(&off) = overlay_id_to_offset.get(&id) {
                        &overlay[off..off + dim]
                    } else {
                        base.get(id)
                            .expect("id_to_offset has stale entry (tiered base)")
                    }
                }
            };
            (id, embedding)
        })
    }

    /// Get all VectorIds in deterministic order
    pub fn ids(&self) -> impl Iterator<Item = VectorId> + '_ {
        self.id_to_offset.keys().copied()
    }

    /// Get raw data slice (for snapshot serialization)
    ///
    /// Only available for InMemory heaps. Panics on Mmap/Tiered variants.
    pub fn raw_data(&self) -> &[f32] {
        match &self.data {
            VectorData::InMemory(vec) => vec,
            VectorData::Mmap(_) => panic!("raw_data() not available on mmap-backed heap"),
            VectorData::Tiered { .. } => panic!("raw_data() not available on tiered heap"),
        }
    }

    /// Get id_to_offset map (for snapshot serialization)
    pub fn id_to_offset_map(&self) -> &BTreeMap<VectorId, usize> {
        &self.id_to_offset
    }

    /// Check whether this heap is backed by a memory-mapped file.
    pub fn is_mmap(&self) -> bool {
        matches!(&self.data, VectorData::Mmap(_) | VectorData::Tiered { .. })
    }

    /// Write the current heap to a `.vec` mmap file.
    ///
    /// This creates a disk cache that can be opened with `from_mmap()` on
    /// subsequent startups, avoiding the cost of rebuilding from KV.
    ///
    /// - `InMemory`: writes all data.
    /// - `Mmap`: no-op (already on disk and unchanged).
    /// - `Tiered`: merges base + overlay into a new file.
    pub fn freeze_to_disk(&self, path: &Path) -> Result<(), VectorError> {
        match &self.data {
            VectorData::InMemory(vec) => mmap::write_mmap_file(
                path,
                self.config.dimension,
                self.next_id.load(Ordering::Relaxed),
                &self.id_to_offset,
                &self.free_slots,
                vec,
            ),
            VectorData::Mmap(mmap_data) => {
                // If vectors were deleted at runtime (id_to_offset shrank),
                // write a new mmap excluding deleted vectors. Otherwise no-op.
                if self.id_to_offset.len() == mmap_data.len() {
                    return Ok(()); // Unchanged, already on disk
                }
                // Build compacted data with only live vectors
                let dim = self.config.dimension;
                let live_count = self.id_to_offset.len();
                let mut compacted_data = Vec::with_capacity(live_count * dim);
                let mut compacted_offsets = BTreeMap::new();
                for &id in self.id_to_offset.keys() {
                    if let Some(emb) = mmap_data.get(id) {
                        let offset = compacted_data.len();
                        compacted_data.extend_from_slice(emb);
                        compacted_offsets.insert(id, offset);
                    }
                }
                mmap::write_mmap_file(
                    path,
                    dim,
                    self.next_id.load(Ordering::Relaxed),
                    &compacted_offsets,
                    &[], // compacted: no free slots
                    &compacted_data,
                )
            }
            VectorData::Tiered {
                base,
                overlay,
                overlay_id_to_offset,
                ..
            } => {
                // Merge base + overlay into a contiguous Vec for writing.
                // Only include vectors that are still live (in id_to_offset).
                let dim = self.config.dimension;
                let live_count = self.id_to_offset.len();
                let mut merged_data = Vec::with_capacity(live_count * dim);
                let mut merged_offsets = BTreeMap::new();
                let merged_free_slots = Vec::new();

                for &id in self.id_to_offset.keys() {
                    let offset = merged_data.len();
                    if let Some(&ov_off) = overlay_id_to_offset.get(&id) {
                        merged_data.extend_from_slice(&overlay[ov_off..ov_off + dim]);
                    } else if let Some(emb) = base.get(id) {
                        merged_data.extend_from_slice(emb);
                    } else {
                        // Should not happen — defensive skip
                        continue;
                    }
                    merged_offsets.insert(id, offset);
                }

                mmap::write_mmap_file(
                    path,
                    dim,
                    self.next_id.load(Ordering::Relaxed),
                    &merged_offsets,
                    &merged_free_slots,
                    &merged_data,
                )
            }
        }
    }

    /// Flush the overlay to disk and swap to a fresh Tiered state.
    ///
    /// Merges base + overlay into a new `.vec` file, mmaps it as the new base,
    /// and resets the overlay to empty. Returns the number of vectors flushed.
    ///
    /// Only meaningful on `Tiered` heaps. Returns `Ok(0)` for other variants.
    pub fn flush_overlay_to_disk(&mut self, path: &Path) -> Result<usize, VectorError> {
        let is_tiered = matches!(&self.data, VectorData::Tiered { .. });
        if !is_tiered {
            return Ok(0);
        }

        // Write merged state to disk
        self.freeze_to_disk(path)?;

        let count = self.id_to_offset.len();

        // Reopen as Tiered with the new mmap as base, empty overlay
        let mmap_data = MmapVectorData::open(path, self.config.dimension)?;
        // Rebuild id_to_offset from the fresh mmap (contiguous, no free slots)
        self.id_to_offset = mmap_data.id_to_offset(); // returns owned BTreeMap
        self.free_slots = mmap_data.free_slots().to_vec();

        self.data = VectorData::Tiered {
            base: mmap_data,
            overlay: Vec::new(),
            overlay_id_to_offset: BTreeMap::new(),
            overlay_free_slots: Vec::new(),
        };

        Ok(count)
    }

    /// Get the number of vectors currently in the overlay (Tiered only).
    /// Returns 0 for non-Tiered heaps.
    pub fn overlay_len(&self) -> usize {
        match &self.data {
            VectorData::Tiered {
                overlay_id_to_offset,
                ..
            } => overlay_id_to_offset.len(),
            _ => 0,
        }
    }

    /// Approximate anonymous (non-mmap) memory in bytes used by embedding data.
    ///
    /// - `InMemory`: full `Vec<f32>` allocation.
    /// - `Mmap`: 0 (OS-managed file-backed pages).
    /// - `Tiered`: overlay `Vec<f32>` only (base is mmap).
    pub fn anon_data_bytes(&self) -> usize {
        match &self.data {
            VectorData::InMemory(v) => v.len() * std::mem::size_of::<f32>(),
            VectorData::Mmap(_) => 0,
            VectorData::Tiered { overlay, .. } => overlay.len() * std::mem::size_of::<f32>(),
        }
    }

    /// Promote an `Mmap` heap to `Tiered` so that new inserts go to an overlay
    /// while reads fall through to the mmap base.
    ///
    /// No-op if already `InMemory` or `Tiered`.
    pub fn promote_to_tiered(&mut self) {
        let current = std::mem::replace(&mut self.data, VectorData::InMemory(Vec::new()));
        match current {
            VectorData::Mmap(mmap) => {
                self.data = VectorData::Tiered {
                    base: mmap,
                    overlay: Vec::new(),
                    overlay_id_to_offset: BTreeMap::new(),
                    overlay_free_slots: Vec::new(),
                };
            }
            other => {
                // Put it back — InMemory and Tiered stay as-is
                self.data = other;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_heap_basic_operations() {
        let config = VectorConfig::for_minilm(); // 384 dims
        let mut heap = VectorHeap::new(config);

        // Insert
        let embedding = vec![0.1; 384];
        let id = heap.insert(&embedding).unwrap();

        // Get
        let retrieved = heap.get(id).unwrap();
        assert_eq!(retrieved.len(), 384);
        assert!((retrieved[0] - 0.1).abs() < f32::EPSILON);

        // Update (upsert)
        let new_embedding = vec![0.2; 384];
        heap.upsert(id, &new_embedding).unwrap();
        let retrieved = heap.get(id).unwrap();
        assert!((retrieved[0] - 0.2).abs() < f32::EPSILON);

        // Delete
        assert!(heap.delete(id));
        assert!(heap.get(id).is_none());
    }

    #[test]
    fn test_vector_id_never_reused() {
        let config = VectorConfig::for_minilm();
        let mut heap = VectorHeap::new(config);

        let embedding = vec![0.1; 384];

        // Insert and delete several times
        let id1 = heap.insert(&embedding).unwrap();
        heap.delete(id1);

        let id2 = heap.insert(&embedding).unwrap();
        heap.delete(id2);

        let id3 = heap.insert(&embedding).unwrap();

        // IDs should be monotonically increasing
        assert!(id1.as_u64() < id2.as_u64());
        assert!(id2.as_u64() < id3.as_u64());
    }

    #[test]
    fn test_slot_reuse() {
        let config = VectorConfig::for_minilm();
        let mut heap = VectorHeap::new(config);

        let embedding = vec![0.1; 384];

        // Insert, then delete to create free slot
        let id1 = heap.insert(&embedding).unwrap();
        let initial_len = heap.raw_data().len();
        heap.delete(id1);

        // Insert again - should reuse slot, not grow data
        let new_embedding = vec![0.2; 384];
        let id2 = heap.insert(&new_embedding).unwrap();

        // Data length should not have grown
        assert_eq!(heap.raw_data().len(), initial_len);

        // New ID should be different
        assert_ne!(id1, id2);

        // New embedding should be in reused slot
        let retrieved = heap.get(id2).unwrap();
        assert!((retrieved[0] - 0.2).abs() < f32::EPSILON);
    }

    #[test]
    fn test_deterministic_iteration() {
        let config = VectorConfig::for_minilm();
        let mut heap = VectorHeap::new(config);

        // Insert multiple vectors
        let embedding = vec![0.1; 384];
        let _id1 = heap.insert(&embedding).unwrap();
        let _id2 = heap.insert(&embedding).unwrap();
        let _id3 = heap.insert(&embedding).unwrap();

        // Iteration should be in VectorId order
        let ids: Vec<_> = heap.ids().collect();
        for i in 1..ids.len() {
            assert!(ids[i - 1] < ids[i], "IDs should be in sorted order");
        }
    }

    #[test]
    fn test_dimension_validation() {
        let config = VectorConfig::for_minilm(); // 384 dims
        let mut heap = VectorHeap::new(config);

        // Wrong dimension should fail
        let wrong_embedding = vec![0.1; 256];
        let result = heap.insert(&wrong_embedding);
        assert!(matches!(result, Err(VectorError::DimensionMismatch { .. })));
    }

    #[test]
    fn test_snapshot_restore() {
        let config = VectorConfig::for_minilm();
        let mut heap = VectorHeap::new(config.clone());

        // Insert some vectors
        let e1 = vec![0.1; 384];
        let e2 = vec![0.2; 384];
        let id1 = heap.insert(&e1).unwrap();
        let id2 = heap.insert(&e2).unwrap();
        heap.delete(id1); // Create a free slot

        // Capture state for snapshot
        let data = heap.raw_data().to_vec();
        let id_to_offset = heap.id_to_offset_map().clone();
        let free_slots = heap.free_slots().to_vec();
        let next_id = heap.next_id_value();

        // Restore from snapshot
        let mut restored =
            VectorHeap::from_snapshot(config, data, id_to_offset, free_slots, next_id);

        // Verify state
        assert!(restored.get(id1).is_none()); // Deleted
        assert!(restored.get(id2).is_some()); // Exists
        assert_eq!(restored.free_slots().len(), 1); // One free slot

        // New insert should get higher ID
        let id3 = restored.insert(&vec![0.3; 384]).unwrap();
        assert!(
            id3.as_u64() >= next_id,
            "ID must be >= next_id from snapshot"
        );
    }

    #[test]
    fn test_insert_with_id_for_wal_replay() {
        let config = VectorConfig::for_minilm();
        let mut heap = VectorHeap::new(config);

        let embedding = vec![0.1; 384];

        // Insert with specific ID (simulating WAL replay)
        let replay_id = VectorId::new(100);
        heap.insert_with_id(replay_id, &embedding).unwrap();

        // Verify it exists
        assert!(heap.get(replay_id).is_some());

        // next_id should be updated to be > 100
        assert!(heap.next_id_value() > 100);

        // New insert should get ID > 100
        let new_id = heap.insert(&embedding).unwrap();
        assert!(new_id.as_u64() > 100);
    }

    #[test]
    fn test_clear_preserves_next_id() {
        let config = VectorConfig::for_minilm();
        let mut heap = VectorHeap::new(config);

        let embedding = vec![0.1; 384];
        let _id1 = heap.insert(&embedding).unwrap();
        let _id2 = heap.insert(&embedding).unwrap();
        let next_id_before = heap.next_id_value();

        heap.clear();

        // next_id should NOT be reset
        assert_eq!(heap.next_id_value(), next_id_before);
        assert!(heap.is_empty());

        // New insert should continue with higher ID
        let id3 = heap.insert(&embedding).unwrap();
        assert!(id3.as_u64() >= next_id_before);
    }

    #[test]
    fn test_contains() {
        let config = VectorConfig::for_minilm();
        let mut heap = VectorHeap::new(config);

        let embedding = vec![0.1; 384];
        let id = heap.insert(&embedding).unwrap();

        assert!(heap.contains(id));
        heap.delete(id);
        assert!(!heap.contains(id));
    }

    #[test]
    fn test_version_increments() {
        let config = VectorConfig::for_minilm();
        let mut heap = VectorHeap::new(config);

        let initial_version = heap.version();
        let embedding = vec![0.1; 384];

        let id = heap.insert(&embedding).unwrap();
        assert!(heap.version() > initial_version);

        let v1 = heap.version();
        heap.upsert(id, &embedding).unwrap();
        assert!(heap.version() > v1);

        let v2 = heap.version();
        heap.delete(id);
        assert!(heap.version() > v2);
    }

    #[test]
    fn test_accessors() {
        let config = VectorConfig::for_minilm();
        let heap = VectorHeap::new(config.clone());

        assert_eq!(heap.dimension(), 384);
        assert_eq!(heap.metric(), DistanceMetric::Cosine);
        assert_eq!(heap.len(), 0);
        assert!(heap.is_empty());
    }

    #[test]
    fn test_deleted_data_is_zeroed() {
        let config = VectorConfig::for_minilm();
        let mut heap = VectorHeap::new(config);

        let embedding = vec![0.5; 384];
        let id = heap.insert(&embedding).unwrap();

        // Get the offset before deletion
        let offset = *heap.id_to_offset_map().get(&id).unwrap();

        heap.delete(id);

        // Check that the data at that offset is zeroed
        let data = heap.raw_data();
        for i in offset..offset + 384 {
            assert_eq!(data[i], 0.0, "Data should be zeroed after deletion");
        }
    }

    // ====================================================================
    // mmap integration tests
    // ====================================================================

    #[test]
    fn test_freeze_and_reopen_mmap() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");

        let config = VectorConfig::for_minilm();
        let mut heap = VectorHeap::new(config.clone());

        // Insert a few vectors
        let e1 = vec![0.1_f32; 384];
        let e2 = vec![0.2_f32; 384];
        let e3 = vec![0.3_f32; 384];
        let id1 = heap.insert(&e1).unwrap();
        let id2 = heap.insert(&e2).unwrap();
        let id3 = heap.insert(&e3).unwrap();
        heap.delete(id2); // Create a free slot

        // Freeze to disk
        heap.freeze_to_disk(&vec_path).unwrap();

        // Reopen from mmap
        let mmap_heap = VectorHeap::from_mmap(&vec_path, config).unwrap();

        // Verify all active vectors match
        assert_eq!(mmap_heap.len(), 2); // id2 was deleted
        assert!(mmap_heap.is_mmap());

        let emb1 = mmap_heap.get(id1).unwrap();
        assert_eq!(emb1.len(), 384);
        assert!((emb1[0] - 0.1).abs() < f32::EPSILON);

        assert!(mmap_heap.get(id2).is_none()); // Deleted

        let emb3 = mmap_heap.get(id3).unwrap();
        assert!((emb3[0] - 0.3).abs() < f32::EPSILON);

        // Verify metadata
        assert_eq!(mmap_heap.next_id_value(), heap.next_id_value());
        assert_eq!(mmap_heap.free_slots().len(), 1);
        assert!(mmap_heap.contains(id1));
        assert!(!mmap_heap.contains(id2));
    }

    #[test]
    fn test_mmap_iter_matches_in_memory() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("iter.vec");

        let config = VectorConfig::for_minilm();
        let mut heap = VectorHeap::new(config.clone());

        let e1 = vec![1.0_f32; 384];
        let e2 = vec![2.0_f32; 384];
        let id1 = heap.insert(&e1).unwrap();
        let id2 = heap.insert(&e2).unwrap();

        // Collect in-memory iteration results
        let mem_entries: Vec<_> = heap.iter().collect();

        // Freeze and reopen
        heap.freeze_to_disk(&vec_path).unwrap();
        let mmap_heap = VectorHeap::from_mmap(&vec_path, config).unwrap();

        // Collect mmap iteration results
        let mmap_entries: Vec<_> = mmap_heap.iter().collect();

        assert_eq!(mem_entries.len(), mmap_entries.len());
        for ((mid, memb), (iid, iemb)) in mem_entries.iter().zip(mmap_entries.iter()) {
            assert_eq!(mid, iid);
            assert_eq!(*memb, *iemb);
        }
    }

    #[test]
    fn test_mmap_empty_heap() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("empty.vec");

        let config = VectorConfig::for_minilm();
        let heap = VectorHeap::new(config.clone());

        heap.freeze_to_disk(&vec_path).unwrap();
        let mmap_heap = VectorHeap::from_mmap(&vec_path, config).unwrap();

        assert_eq!(mmap_heap.len(), 0);
        assert!(mmap_heap.is_empty());
        assert!(mmap_heap.is_mmap());
    }

    #[test]
    fn test_is_mmap_flag() {
        let config = VectorConfig::for_minilm();
        let heap = VectorHeap::new(config);
        assert!(!heap.is_mmap());
    }

    #[test]
    fn test_delete_on_mmap_heap_does_not_panic() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config.clone());
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        heap.freeze_to_disk(&vec_path).unwrap();

        // Reopen as mmap
        let mut mmap_heap = VectorHeap::from_mmap(&vec_path, config).unwrap();
        assert!(mmap_heap.is_mmap());
        assert_eq!(mmap_heap.len(), 2);

        // Delete should succeed without panicking (skips zeroing on mmap)
        let deleted = mmap_heap.delete(VectorId::new(1));
        assert!(deleted);
        assert_eq!(mmap_heap.len(), 1);
        assert!(mmap_heap.get(VectorId::new(1)).is_none());
        assert!(mmap_heap.get(VectorId::new(2)).is_some());

        // Deleting nonexistent ID returns false
        assert!(!mmap_heap.delete(VectorId::new(99)));
    }

    // ====================================================================
    // Tiered variant tests
    // ====================================================================

    /// Helper: create a Tiered heap by writing InMemory data to mmap then promoting.
    fn make_tiered_heap(dim: usize) -> (VectorHeap, tempfile::TempDir) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");
        let config = VectorConfig::new(dim, DistanceMetric::Cosine).unwrap();

        let mut heap = VectorHeap::new(config.clone());
        // Insert base vectors
        heap.upsert(VectorId::new(1), &vec![1.0; dim]).unwrap();
        heap.upsert(VectorId::new(2), &vec![2.0; dim]).unwrap();
        heap.upsert(VectorId::new(3), &vec![3.0; dim]).unwrap();
        heap.freeze_to_disk(&vec_path).unwrap();

        // Reopen as mmap and promote to Tiered
        let mut tiered = VectorHeap::from_mmap(&vec_path, config).unwrap();
        tiered.promote_to_tiered();
        assert!(tiered.is_mmap()); // Tiered reports as mmap
        (tiered, temp_dir)
    }

    #[test]
    fn test_promote_to_tiered_from_mmap() {
        let (heap, _dir) = make_tiered_heap(3);
        assert!(heap.is_mmap());
        assert_eq!(heap.len(), 3);
        assert_eq!(heap.overlay_len(), 0);
        assert!(heap.get(VectorId::new(1)).is_some());
    }

    #[test]
    fn test_promote_to_tiered_noop_for_inmemory() {
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config);
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.promote_to_tiered(); // should be no-op
        assert!(!heap.is_mmap());
        assert_eq!(heap.get(VectorId::new(1)).unwrap(), &[1.0, 0.0, 0.0]);
    }

    #[test]
    fn test_tiered_insert_goes_to_overlay() {
        let (mut heap, _dir) = make_tiered_heap(3);

        // Insert new vector into overlay
        heap.upsert(VectorId::new(10), &[10.0, 10.0, 10.0]).unwrap();

        assert_eq!(heap.len(), 4);
        assert_eq!(heap.overlay_len(), 1);
        let emb = heap.get(VectorId::new(10)).unwrap();
        assert_eq!(emb, &[10.0, 10.0, 10.0]);
    }

    #[test]
    fn test_tiered_get_overlay_takes_precedence() {
        let (mut heap, _dir) = make_tiered_heap(3);

        // Base has VectorId(1) = [1.0, 1.0, 1.0]
        let base_emb = heap.get(VectorId::new(1)).unwrap();
        assert_eq!(base_emb, &[1.0, 1.0, 1.0]);

        // Upsert into overlay with different values
        heap.upsert(VectorId::new(1), &[9.0, 9.0, 9.0]).unwrap();

        // Overlay value should win
        let updated = heap.get(VectorId::new(1)).unwrap();
        assert_eq!(updated, &[9.0, 9.0, 9.0]);
        assert_eq!(heap.overlay_len(), 1);
        // len should NOT increase (same VectorId)
        assert_eq!(heap.len(), 3);
    }

    #[test]
    fn test_tiered_delete_from_overlay() {
        let (mut heap, _dir) = make_tiered_heap(3);

        // Insert into overlay then delete
        heap.upsert(VectorId::new(10), &[10.0, 10.0, 10.0]).unwrap();
        assert_eq!(heap.overlay_len(), 1);

        let deleted = heap.delete(VectorId::new(10));
        assert!(deleted);
        assert!(heap.get(VectorId::new(10)).is_none());
        assert_eq!(heap.len(), 3);
        assert_eq!(heap.overlay_len(), 0); // removed from overlay
    }

    #[test]
    fn test_tiered_delete_from_base_mmap() {
        let (mut heap, _dir) = make_tiered_heap(3);

        // Delete a vector that only exists in base mmap
        let deleted = heap.delete(VectorId::new(2));
        assert!(deleted);
        assert!(heap.get(VectorId::new(2)).is_none());
        assert_eq!(heap.len(), 2);
        // overlay_len should remain 0 (delete doesn't create overlay entries)
        assert_eq!(heap.overlay_len(), 0);
    }

    #[test]
    fn test_tiered_delete_nonexistent() {
        let (mut heap, _dir) = make_tiered_heap(3);
        assert!(!heap.delete(VectorId::new(999)));
        assert_eq!(heap.len(), 3);
    }

    #[test]
    fn test_tiered_iter_merges_overlay_and_base() {
        let (mut heap, _dir) = make_tiered_heap(3);

        // Add overlay entry and update a base entry
        heap.upsert(VectorId::new(10), &[10.0, 10.0, 10.0]).unwrap();
        heap.upsert(VectorId::new(2), &[20.0, 20.0, 20.0]).unwrap(); // override base

        let entries: Vec<(VectorId, &[f32])> = heap.iter().collect();
        assert_eq!(entries.len(), 4); // 1, 2, 3, 10

        // Check order: VectorId ascending
        let ids: Vec<u64> = entries.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(ids, vec![1, 2, 3, 10]);

        // Check that VectorId(2) returns overlay value
        let (_, emb2) = entries.iter().find(|(id, _)| id.as_u64() == 2).unwrap();
        assert_eq!(*emb2, &[20.0, 20.0, 20.0]);

        // Check that VectorId(1) returns base value
        let (_, emb1) = entries.iter().find(|(id, _)| id.as_u64() == 1).unwrap();
        assert_eq!(*emb1, &[1.0, 1.0, 1.0]);
    }

    #[test]
    fn test_tiered_iter_excludes_deleted() {
        let (mut heap, _dir) = make_tiered_heap(3);
        heap.delete(VectorId::new(2));

        let ids: Vec<u64> = heap.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn test_tiered_anon_data_bytes() {
        let (mut heap, _dir) = make_tiered_heap(3);

        // Initially overlay is empty — zero anonymous bytes
        assert_eq!(heap.anon_data_bytes(), 0);

        // Insert one vector of dim=3 → 3 * 4 = 12 bytes
        heap.upsert(VectorId::new(10), &[1.0, 2.0, 3.0]).unwrap();
        assert_eq!(heap.anon_data_bytes(), 3 * 4);
    }

    #[test]
    fn test_tiered_free_slots_returns_empty() {
        let (heap, _dir) = make_tiered_heap(3);
        // Tiered heaps always return empty free_slots (compacted on freeze)
        assert!(heap.free_slots().is_empty());
    }

    #[test]
    fn test_tiered_freeze_to_disk_merges_correctly() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");
        let out_path = temp_dir.path().join("merged.vec");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config.clone());
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        heap.freeze_to_disk(&vec_path).unwrap();

        // Promote to Tiered
        let mut tiered = VectorHeap::from_mmap(&vec_path, config.clone()).unwrap();
        tiered.promote_to_tiered();

        // Add overlay, update base, delete one
        tiered.upsert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
        tiered.upsert(VectorId::new(1), &[9.0, 9.0, 9.0]).unwrap(); // override
        tiered.delete(VectorId::new(2));

        // Freeze merged state
        tiered.freeze_to_disk(&out_path).unwrap();

        // Reopen and verify
        let reopened = VectorHeap::from_mmap(&out_path, config).unwrap();
        assert_eq!(reopened.len(), 2); // id=1 (updated) and id=3 (new)
        assert_eq!(reopened.get(VectorId::new(1)).unwrap(), &[9.0, 9.0, 9.0]);
        assert!(reopened.get(VectorId::new(2)).is_none());
        assert_eq!(reopened.get(VectorId::new(3)).unwrap(), &[0.0, 0.0, 1.0]);
    }

    // ====================================================================
    // flush_overlay_to_disk tests
    // ====================================================================

    #[test]
    fn test_flush_overlay_to_disk_round_trip() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config.clone());
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        heap.freeze_to_disk(&vec_path).unwrap();

        // Reopen as Tiered
        let mut tiered = VectorHeap::from_mmap(&vec_path, config.clone()).unwrap();
        tiered.promote_to_tiered();

        // Add overlay vectors
        tiered.upsert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
        tiered.upsert(VectorId::new(4), &[1.0, 1.0, 0.0]).unwrap();
        assert_eq!(tiered.overlay_len(), 2);

        // Flush overlay to disk
        let count = tiered.flush_overlay_to_disk(&vec_path).unwrap();
        assert_eq!(count, 4);

        // After flush: overlay is empty, all vectors accessible
        assert_eq!(tiered.overlay_len(), 0);
        assert_eq!(tiered.len(), 4);
        assert!(tiered.is_mmap()); // still Tiered
        assert_eq!(tiered.anon_data_bytes(), 0); // overlay empty

        // Verify all vectors readable
        assert_eq!(tiered.get(VectorId::new(1)).unwrap(), &[1.0, 0.0, 0.0]);
        assert_eq!(tiered.get(VectorId::new(2)).unwrap(), &[0.0, 1.0, 0.0]);
        assert_eq!(tiered.get(VectorId::new(3)).unwrap(), &[0.0, 0.0, 1.0]);
        assert_eq!(tiered.get(VectorId::new(4)).unwrap(), &[1.0, 1.0, 0.0]);
    }

    #[test]
    fn test_flush_overlay_multiple_cycles() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config.clone());
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.freeze_to_disk(&vec_path).unwrap();

        let mut tiered = VectorHeap::from_mmap(&vec_path, config).unwrap();
        tiered.promote_to_tiered();

        // Cycle 1: add, flush
        tiered.upsert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        tiered.flush_overlay_to_disk(&vec_path).unwrap();
        assert_eq!(tiered.len(), 2);

        // Cycle 2: add more, flush
        tiered.upsert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
        tiered.flush_overlay_to_disk(&vec_path).unwrap();
        assert_eq!(tiered.len(), 3);

        // Cycle 3: update + delete, flush
        tiered.upsert(VectorId::new(1), &[5.0, 5.0, 5.0]).unwrap();
        tiered.delete(VectorId::new(2));
        tiered.flush_overlay_to_disk(&vec_path).unwrap();
        assert_eq!(tiered.len(), 2);
        assert_eq!(tiered.get(VectorId::new(1)).unwrap(), &[5.0, 5.0, 5.0]);
        assert!(tiered.get(VectorId::new(2)).is_none());
        assert_eq!(tiered.get(VectorId::new(3)).unwrap(), &[0.0, 0.0, 1.0]);
    }

    #[test]
    fn test_flush_overlay_noop_for_inmemory() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("noop.vec");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config);
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();

        // flush_overlay_to_disk on InMemory should return Ok(0)
        let count = heap.flush_overlay_to_disk(&vec_path).unwrap();
        assert_eq!(count, 0);
    }

    // ====================================================================
    // Mmap freeze with runtime deletes
    // ====================================================================

    #[test]
    fn test_mmap_freeze_after_runtime_deletes() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");
        let refreeze_path = temp_dir.path().join("refrozen.vec");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config.clone());
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
        heap.freeze_to_disk(&vec_path).unwrap();

        // Reopen as mmap (NOT Tiered — stays Mmap variant)
        let mut mmap_heap = VectorHeap::from_mmap(&vec_path, config.clone()).unwrap();
        assert_eq!(mmap_heap.len(), 3);

        // Delete a vector at runtime
        mmap_heap.delete(VectorId::new(2));
        assert_eq!(mmap_heap.len(), 2);

        // Freeze should write a new file (not no-op) because deletes occurred
        mmap_heap.freeze_to_disk(&refreeze_path).unwrap();

        // Reopen and verify deleted vector is gone
        let reopened = VectorHeap::from_mmap(&refreeze_path, config).unwrap();
        assert_eq!(reopened.len(), 2);
        assert!(reopened.get(VectorId::new(1)).is_some());
        assert!(reopened.get(VectorId::new(2)).is_none());
        assert!(reopened.get(VectorId::new(3)).is_some());
    }

    #[test]
    fn test_mmap_freeze_noop_when_unchanged() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config.clone());
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.freeze_to_disk(&vec_path).unwrap();

        let mmap_heap = VectorHeap::from_mmap(&vec_path, config).unwrap();
        let stat_before = std::fs::metadata(&vec_path).unwrap().len();

        // Freeze without any changes — should be a no-op
        mmap_heap.freeze_to_disk(&vec_path).unwrap();
        let stat_after = std::fs::metadata(&vec_path).unwrap().len();
        assert_eq!(stat_before, stat_after);
    }

    // ====================================================================
    // Tiered overlay slot reuse
    // ====================================================================

    #[test]
    fn test_tiered_overlay_slot_reuse() {
        let (mut heap, _dir) = make_tiered_heap(3);

        // Insert and delete to create a free overlay slot
        heap.upsert(VectorId::new(10), &[10.0, 10.0, 10.0]).unwrap();
        let overlay_size_after_insert = heap.anon_data_bytes();

        heap.delete(VectorId::new(10));

        // Insert another — should reuse the freed slot
        heap.upsert(VectorId::new(20), &[20.0, 20.0, 20.0]).unwrap();
        let overlay_size_after_reuse = heap.anon_data_bytes();

        // Overlay should not have grown
        assert_eq!(overlay_size_after_insert, overlay_size_after_reuse);
        assert_eq!(heap.get(VectorId::new(20)).unwrap(), &[20.0, 20.0, 20.0]);
    }
}
