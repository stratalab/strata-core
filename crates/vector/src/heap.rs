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

use crate::distance::{compute_asymmetric_similarity, compute_similarity_cached};
use crate::error::{VectorError, VectorResult};
use crate::mmap::{self, MmapVectorData};
use crate::quantize::{self, QuantizationParams};
use crate::types::{DistanceMetric, InlineMeta, StorageDtype, VectorConfig, VectorId};

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
    /// Quantized u8 storage (SQ8). Each vector is `dimension` bytes.
    /// Offsets in id_to_offset are in u8 elements (= byte offsets within the Vec).
    InMemoryQuantized(Vec<u8>),
}

/// Sentinel value indicating absent entry in dense_offsets.
const DENSE_ABSENT: u32 = u32::MAX;

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

    /// Dense acceleration index: VectorId.0 → slot number (offset / dimension).
    /// DENSE_ABSENT means the ID is not present. Only valid for InMemory heaps.
    /// Memory: 1M vectors × 4 bytes = 4 MB (negligible vs embedding data).
    dense_offsets: Vec<u32>,

    /// Pre-computed L2 norms for each VectorId (indexed by VectorId.0).
    /// Used to accelerate cosine similarity: cos(a,b) = dot(a,b) / (norm_a * norm_b).
    norms: Vec<f32>,

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

    /// Quantization parameters (only used when config.storage_dtype == Int8).
    /// None for F32 heaps.
    quant_params: Option<QuantizationParams>,

    /// Number of calibration samples seen (Int8 only, pre-calibration).
    /// None for F32 heaps or after calibration completes.
    calibration_samples: Option<usize>,
}

impl VectorHeap {
    /// Create a new vector heap with the given configuration
    ///
    /// Note: next_id starts at 1, not 0, to match expected VectorId semantics
    /// where IDs are positive integers.
    pub fn new(config: VectorConfig) -> Self {
        let is_int8 = config.storage_dtype == StorageDtype::Int8;
        VectorHeap {
            quant_params: if is_int8 {
                Some(QuantizationParams::new(config.dimension))
            } else {
                None
            },
            calibration_samples: if is_int8 { Some(0) } else { None },
            config,
            data: VectorData::InMemory(Vec::new()),
            id_to_offset: BTreeMap::new(),
            dense_offsets: Vec::new(),
            norms: Vec::new(),
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
        let is_int8 = config.storage_dtype == StorageDtype::Int8;

        // For Int8: quantize the f32 snapshot data into u8 storage.
        // If the collection is empty, stay in calibration mode so new inserts work.
        let (vector_data, quant_params, cal_samples) = if is_int8 {
            let dim = config.dimension;
            if id_to_offset.is_empty() {
                // Empty collection — stay in f32 calibration mode
                (
                    VectorData::InMemory(data),
                    Some(QuantizationParams::new(dim)),
                    Some(0),
                )
            } else {
                let mut params = QuantizationParams::new(dim);
                for &offset in id_to_offset.values() {
                    if offset + dim <= data.len() {
                        params.update_calibration(&data[offset..offset + dim]);
                    }
                }
                params.finalize_calibration();
                let total_slots = data.len() / dim;
                let mut q_data = vec![0u8; total_slots * dim];
                for (&_id, &offset) in &id_to_offset {
                    if offset + dim <= data.len() {
                        // Offset is slot_number * dim — same for both f32 and u8 layouts
                        params.quantize_into(
                            &data[offset..offset + dim],
                            &mut q_data[offset..offset + dim],
                        );
                    }
                }
                (VectorData::InMemoryQuantized(q_data), Some(params), None)
            }
        } else {
            (VectorData::InMemory(data), None, None)
        };

        let mut heap = VectorHeap {
            config,
            data: vector_data,
            id_to_offset,
            dense_offsets: Vec::new(),
            norms: Vec::new(),
            free_slots,
            next_id: AtomicU64::new(next_id),
            version: AtomicU64::new(0),
            inline_meta: BTreeMap::new(),
            quant_params,
            calibration_samples: cal_samples,
        };
        heap.rebuild_dense_index();
        heap
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
        let quant_params = mmap_data.quant_params();
        let mut heap = VectorHeap {
            config,
            data: VectorData::Mmap(mmap_data),
            id_to_offset,
            dense_offsets: Vec::new(),
            norms: Vec::new(),
            free_slots,
            next_id: AtomicU64::new(next_id),
            version: AtomicU64::new(0),
            inline_meta: BTreeMap::new(),
            quant_params,
            calibration_samples: None,
        };
        heap.rebuild_dense_index();
        Ok(heap)
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
        self.rebuild_dense_index();
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
                    // Update norm for the modified embedding
                    let idx = id.0 as usize;
                    if idx < self.norms.len() {
                        self.norms[idx] = compute_l2_norm(embedding);
                    }
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
                    // Maintain dense_offsets
                    let idx = id.0 as usize;
                    let slot_number = (offset / dim) as u32;
                    if idx >= self.dense_offsets.len() {
                        self.dense_offsets.resize(idx + 1, DENSE_ABSENT);
                    }
                    self.dense_offsets[idx] = slot_number;
                    // Maintain norms
                    if idx >= self.norms.len() {
                        self.norms.resize(idx + 1, 0.0);
                    }
                    self.norms[idx] = compute_l2_norm(embedding);
                }
            }
            VectorData::Mmap(_) => {
                return Err(VectorError::Internal(
                    "cannot mutate mmap-backed VectorHeap".into(),
                ))
            }
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

                // Maintain norms (dense_offsets are not used for non-InMemory heaps,
                // but norms are needed for cosine similarity acceleration).
                let idx = id.0 as usize;
                if idx >= self.norms.len() {
                    self.norms.resize(idx + 1, 0.0);
                }
                self.norms[idx] = compute_l2_norm(embedding);
            }
            VectorData::InMemoryQuantized(vec) => {
                let params = self
                    .quant_params
                    .as_ref()
                    .expect("InMemoryQuantized requires quant_params");
                if let Some(&offset) = self.id_to_offset.get(&id) {
                    // Update in place
                    params.quantize_into(embedding, &mut vec[offset..offset + dim]);
                    let idx = id.0 as usize;
                    if idx < self.norms.len() {
                        self.norms[idx] = compute_l2_norm(embedding);
                    }
                } else {
                    let offset = if let Some(slot) = self.free_slots.pop() {
                        params.quantize_into(embedding, &mut vec[slot..slot + dim]);
                        slot
                    } else {
                        let offset = vec.len();
                        let mut buf = vec![0u8; dim];
                        params.quantize_into(embedding, &mut buf);
                        vec.extend_from_slice(&buf);
                        offset
                    };
                    self.id_to_offset.insert(id, offset);
                    let idx = id.0 as usize;
                    let slot_number = (offset / dim) as u32;
                    if idx >= self.dense_offsets.len() {
                        self.dense_offsets.resize(idx + 1, DENSE_ABSENT);
                    }
                    self.dense_offsets[idx] = slot_number;
                    if idx >= self.norms.len() {
                        self.norms.resize(idx + 1, 0.0);
                    }
                    self.norms[idx] = compute_l2_norm(embedding);
                }
            }
        }

        // Int8 calibration tracking: during calibration phase (data is still InMemory),
        // update calibration params and count. Auto-finalize when threshold reached.
        if self.config.storage_dtype == StorageDtype::Int8 {
            if let Some(ref mut count) = self.calibration_samples {
                if let Some(ref mut params) = self.quant_params {
                    if !params.calibrated {
                        params.update_calibration(embedding);
                        *count += 1;
                        if *count >= quantize::DEFAULT_CALIBRATION_THRESHOLD {
                            // Auto-finalize — this transitions data to InMemoryQuantized
                            self.version.fetch_add(1, Ordering::Release);
                            self.finalize_calibration_inner();
                            return Ok(());
                        }
                    }
                }
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
                    // Clear dense_offsets entry
                    let idx = id.0 as usize;
                    if idx < self.dense_offsets.len() {
                        self.dense_offsets[idx] = DENSE_ABSENT;
                    }
                    if idx < self.norms.len() {
                        self.norms[idx] = 0.0;
                    }
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
                    let idx = id.0 as usize;
                    if idx < self.dense_offsets.len() {
                        self.dense_offsets[idx] = DENSE_ABSENT;
                    }
                    if idx < self.norms.len() {
                        self.norms[idx] = 0.0;
                    }
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
                let idx = id.0 as usize;
                if idx < self.dense_offsets.len() {
                    self.dense_offsets[idx] = DENSE_ABSENT;
                }
                if idx < self.norms.len() {
                    self.norms[idx] = 0.0;
                }
                self.version.fetch_add(1, Ordering::Release);
                true
            }
            VectorData::InMemoryQuantized(v) => {
                if let Some(offset) = self.id_to_offset.remove(&id) {
                    self.free_slots.push(offset);
                    let dim = self.config.dimension;
                    v[offset..offset + dim].fill(0);
                    let idx = id.0 as usize;
                    if idx < self.dense_offsets.len() {
                        self.dense_offsets[idx] = DENSE_ABSENT;
                    }
                    if idx < self.norms.len() {
                        self.norms[idx] = 0.0;
                    }
                    self.version.fetch_add(1, Ordering::Release);
                    true
                } else {
                    false
                }
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
        self.dense_offsets.clear();
        self.norms.clear();
        self.free_slots.clear();
        self.inline_meta.clear();
        if self.config.storage_dtype == StorageDtype::Int8 {
            self.quant_params = Some(QuantizationParams::new(self.config.dimension));
            self.calibration_samples = Some(0);
        }
        // Note: next_id is NOT reset — IDs are never reused
        self.version.fetch_add(1, Ordering::Release);
    }

    // ========================================================================
    // Quantization Calibration
    // ========================================================================

    /// Finalize calibration: quantize all buffered vectors and transition
    /// from InMemory(f32) to InMemoryQuantized(u8).
    ///
    /// Called automatically when calibration sample count reaches the threshold,
    /// or explicitly (e.g., when sealing active buffer or on first search).
    pub fn finalize_calibration(&mut self) {
        if self.config.storage_dtype != StorageDtype::Int8 {
            return;
        }
        if self.quant_params.as_ref().is_none_or(|p| p.calibrated) {
            return; // Already calibrated or no params
        }
        self.finalize_calibration_inner();
    }

    fn finalize_calibration_inner(&mut self) {
        let params = self.quant_params.as_mut().expect("must have quant_params");
        params.finalize_calibration();

        // Quantize all existing vectors from the f32 data
        let dim = self.config.dimension;
        match &self.data {
            VectorData::InMemory(f32_data) => {
                let total_elements = f32_data.len();
                // f32 data has N slots of `dim` f32 each.
                // Quantized data has N slots of `dim` u8 each.
                let slot_count = total_elements / dim;
                let mut q_data = vec![0u8; slot_count * dim];
                let params_ref = self.quant_params.as_ref().unwrap();
                for (&_id, &offset) in &self.id_to_offset {
                    if offset + dim <= f32_data.len() {
                        params_ref.quantize_into(
                            &f32_data[offset..offset + dim],
                            &mut q_data[offset..offset + dim],
                        );
                    }
                }
                self.data = VectorData::InMemoryQuantized(q_data);
            }
            _ => {
                // Only InMemory → InMemoryQuantized transition is supported.
                // Mmap/Tiered heaps should not reach this path.
                debug_assert!(
                    false,
                    "finalize_calibration_inner called on non-InMemory heap"
                );
            }
        }

        self.calibration_samples = None;
        // Rebuild dense index + norms for the new quantized data
        self.rebuild_dense_index();
    }

    /// Whether calibration is in progress (Int8 only).
    pub fn is_calibrating(&self) -> bool {
        self.calibration_samples.is_some()
            && self.quant_params.as_ref().is_some_and(|p| !p.calibrated)
    }

    /// Get quantization parameters (if Int8).
    pub fn quant_params(&self) -> Option<&QuantizationParams> {
        self.quant_params.as_ref()
    }

    // ========================================================================
    // Distance Computation (dtype-aware)
    // ========================================================================

    /// Compute similarity between a query vector and a stored vector.
    ///
    /// For F32 heaps: uses cached norms for cosine.
    /// For Int8 heaps: uses asymmetric distance (f32 query vs u8 stored).
    ///
    /// Encapsulates storage format so callers don't need to know the dtype.
    #[inline]
    pub fn distance_to(
        &self,
        query: &[f32],
        id: VectorId,
        metric: DistanceMetric,
        norm_query: Option<f32>,
    ) -> Option<f32> {
        match &self.data {
            VectorData::InMemory(_) | VectorData::Mmap(_) | VectorData::Tiered { .. } => {
                let embedding = self.get(id)?;
                Some(compute_similarity_cached(
                    query,
                    embedding,
                    metric,
                    norm_query,
                    self.get_norm(id),
                ))
            }
            VectorData::InMemoryQuantized(vec) => {
                let params = self.quant_params.as_ref()?;
                let dim = self.config.dimension;
                let idx = id.0 as usize;
                let quantized = if idx < self.dense_offsets.len() {
                    let slot = self.dense_offsets[idx];
                    if slot == DENSE_ABSENT {
                        return None;
                    }
                    let offset = slot as usize * dim;
                    if offset + dim > vec.len() {
                        return None;
                    }
                    &vec[offset..offset + dim]
                } else {
                    let offset = *self.id_to_offset.get(&id)?;
                    if offset + dim > vec.len() {
                        return None;
                    }
                    &vec[offset..offset + dim]
                };
                Some(compute_asymmetric_similarity(
                    query,
                    quantized,
                    &params.mins,
                    &params.scales,
                    metric,
                    norm_query,
                    self.get_norm(id),
                ))
            }
        }
    }

    /// Distance computation using pre-resolved slot (for CompactHnswGraph).
    #[inline]
    pub fn distance_to_by_slot(
        &self,
        query: &[f32],
        slot: u32,
        metric: DistanceMetric,
        norm_query: Option<f32>,
        norm_stored: Option<f32>,
    ) -> Option<f32> {
        if slot == DENSE_ABSENT {
            return None;
        }
        let dim = self.config.dimension;
        let offset = slot as usize * dim;
        match &self.data {
            VectorData::InMemory(vec) => {
                if offset + dim > vec.len() {
                    return None;
                }
                Some(compute_similarity_cached(
                    query,
                    &vec[offset..offset + dim],
                    metric,
                    norm_query,
                    norm_stored,
                ))
            }
            VectorData::Mmap(mmap) => {
                let emb = mmap.get_by_offset(offset, dim)?;
                Some(compute_similarity_cached(
                    query,
                    emb,
                    metric,
                    norm_query,
                    norm_stored,
                ))
            }
            VectorData::InMemoryQuantized(vec) => {
                if offset + dim > vec.len() {
                    return None;
                }
                let params = self.quant_params.as_ref()?;
                Some(compute_asymmetric_similarity(
                    query,
                    &vec[offset..offset + dim],
                    &params.mins,
                    &params.scales,
                    metric,
                    norm_query,
                    norm_stored,
                ))
            }
            VectorData::Tiered { .. } => None, // Slot-based not supported for Tiered
        }
    }

    /// Get a dequantized f32 embedding (cold path, allocates).
    ///
    /// For F32 heaps: clones the slice.
    /// For Int8 heaps: dequantizes u8 → f32.
    pub fn get_f32_owned(&self, id: VectorId) -> Option<Vec<f32>> {
        match &self.data {
            VectorData::InMemory(_) | VectorData::Mmap(_) | VectorData::Tiered { .. } => {
                self.get(id).map(|s| s.to_vec())
            }
            VectorData::InMemoryQuantized(vec) => {
                let params = self.quant_params.as_ref()?;
                let dim = self.config.dimension;
                let offset = *self.id_to_offset.get(&id)?;
                if offset + dim > vec.len() {
                    return None;
                }
                Some(params.dequantize(&vec[offset..offset + dim]))
            }
        }
    }

    /// Get quantized u8 embedding slice (for Int8 heaps only).
    #[inline]
    pub fn get_quantized(&self, id: VectorId) -> Option<&[u8]> {
        match &self.data {
            VectorData::InMemoryQuantized(vec) => {
                let dim = self.config.dimension;
                let offset = *self.id_to_offset.get(&id)?;
                if offset + dim > vec.len() {
                    return None;
                }
                Some(&vec[offset..offset + dim])
            }
            _ => None,
        }
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
    // Dense Acceleration (O(1) lookups for hot paths)
    // ========================================================================

    /// Get the raw f32 offset for a VectorId (for CompactHnswGraph pre-resolution).
    ///
    /// Returns the slot number (offset / dim) if present, None otherwise.
    #[inline]
    pub fn get_offset(&self, id: VectorId) -> Option<u32> {
        let idx = id.0 as usize;
        if idx < self.dense_offsets.len() {
            let slot = self.dense_offsets[idx];
            if slot != DENSE_ABSENT {
                return Some(slot);
            }
        }
        // Fallback for Mmap/Tiered: derive from id_to_offset BTreeMap
        self.id_to_offset
            .get(&id)
            .map(|&off| (off / self.config.dimension) as u32)
    }

    /// O(1) embedding lookup by pre-resolved slot number.
    ///
    /// Used by CompactHnswGraph where embedding offsets are pre-resolved at seal time.
    #[inline]
    pub fn get_by_slot(&self, slot: u32) -> Option<&[f32]> {
        if slot == DENSE_ABSENT {
            return None;
        }
        let dim = self.config.dimension;
        let offset = slot as usize * dim;
        match &self.data {
            VectorData::InMemory(vec) => {
                if offset + dim <= vec.len() {
                    Some(&vec[offset..offset + dim])
                } else {
                    None
                }
            }
            VectorData::Mmap(mmap) => mmap.get_by_offset(offset, dim),
            VectorData::Tiered {
                base,
                overlay,
                overlay_id_to_offset,
                ..
            } => {
                // Slot-based access doesn't know which layer, try InMemory-like path
                // This is a fallback; callers on Tiered heaps should use get()
                let _ = (base, overlay, overlay_id_to_offset);
                None
            }
            VectorData::InMemoryQuantized(_) => {
                // For quantized heaps, use distance_to_by_slot() instead
                None
            }
        }
    }

    /// Get the pre-computed L2 norm for a VectorId.
    #[inline]
    pub fn get_norm(&self, id: VectorId) -> Option<f32> {
        let idx = id.0 as usize;
        if idx < self.norms.len() {
            let n = self.norms[idx];
            if n > 0.0 {
                return Some(n);
            }
        }
        None
    }

    /// Software prefetch the embedding data for a VectorId into L1 cache.
    ///
    /// Used in the HNSW inner loop to hide DRAM latency: prefetch the *next*
    /// neighbor's embedding while computing distance for the current one.
    /// Uses the O(1) dense_offsets path (same as `get()`).
    #[inline]
    pub fn prefetch_embedding(&self, id: VectorId) {
        let idx = id.0 as usize;
        if idx < self.dense_offsets.len() {
            let slot = self.dense_offsets[idx];
            if slot != DENSE_ABSENT {
                let offset = slot as usize * self.config.dimension;
                match &self.data {
                    VectorData::InMemory(vec) => {
                        if offset < vec.len() {
                            let ptr = vec[offset..].as_ptr() as *const u8;
                            let embedding_bytes = self.config.dimension * 4; // f32 = 4 bytes
                            crate::distance::prefetch_read(ptr);
                            if embedding_bytes > 64 {
                                unsafe {
                                    crate::distance::prefetch_read(ptr.add(64));
                                    if embedding_bytes > 128 {
                                        crate::distance::prefetch_read(ptr.add(128));
                                    }
                                    if embedding_bytes > 192 {
                                        crate::distance::prefetch_read(ptr.add(192));
                                    }
                                }
                            }
                        }
                    }
                    VectorData::InMemoryQuantized(vec) => {
                        // Quantized: dimension bytes (not dimension*4)
                        if offset < vec.len() {
                            let ptr = vec[offset..].as_ptr();
                            crate::distance::prefetch_read(ptr);
                            let embedding_bytes = self.config.dimension; // u8 = 1 byte
                            if embedding_bytes > 64 {
                                unsafe {
                                    crate::distance::prefetch_read(ptr.add(64));
                                    if embedding_bytes > 128 {
                                        crate::distance::prefetch_read(ptr.add(128));
                                    }
                                    if embedding_bytes > 192 {
                                        crate::distance::prefetch_read(ptr.add(192));
                                    }
                                }
                            }
                        }
                    }
                    _ => {} // Mmap/Tiered: no prefetch from dense path
                }
            }
        }
    }

    /// Rebuild the dense offset index and norms from `id_to_offset`.
    ///
    /// Called from `from_snapshot()`, `from_mmap()`, `restore_snapshot_state()`,
    /// `flush_overlay_to_disk()`, and `promote_to_tiered()`.
    pub fn rebuild_dense_index(&mut self) {
        self.dense_offsets.clear();
        self.norms.clear();
        let dim = self.config.dimension;

        // Collect (VectorId_idx, offset) first to avoid borrow conflicts
        let entries: Vec<(usize, usize)> = self
            .id_to_offset
            .iter()
            .map(|(&id, &offset)| (id.0 as usize, offset))
            .collect();

        if entries.is_empty() {
            return;
        }

        // Find max index to pre-allocate
        let max_idx = entries.iter().map(|e| e.0).max().unwrap();
        self.dense_offsets.resize(max_idx + 1, DENSE_ABSENT);
        self.norms.resize(max_idx + 1, 0.0);

        match &self.data {
            VectorData::InMemory(vec) => {
                for (idx, offset) in entries {
                    let slot = (offset / dim) as u32;
                    self.dense_offsets[idx] = slot;
                    if offset + dim <= vec.len() {
                        self.norms[idx] = compute_l2_norm(&vec[offset..offset + dim]);
                    }
                }
            }
            VectorData::Mmap(mmap) => {
                for (idx, offset) in entries {
                    let slot = (offset / dim) as u32;
                    self.dense_offsets[idx] = slot;
                    if let Some(emb) = mmap.get_by_offset(offset, dim) {
                        self.norms[idx] = compute_l2_norm(emb);
                    }
                }
            }
            VectorData::Tiered {
                base,
                overlay,
                overlay_id_to_offset,
                ..
            } => {
                // For Tiered heaps, id_to_offset stores dummy values (0).
                // dense_offsets are not usable (get() falls back to get_btree() anyway).
                // We only need to compute correct norms by resolving embeddings
                // through the overlay (precedence) or base mmap.
                for (idx, _offset) in entries {
                    let vid = VectorId::new(idx as u64);
                    let norm = if let Some(&ov_off) = overlay_id_to_offset.get(&vid) {
                        if ov_off + dim <= overlay.len() {
                            compute_l2_norm(&overlay[ov_off..ov_off + dim])
                        } else {
                            0.0
                        }
                    } else {
                        base.get(vid).map(compute_l2_norm).unwrap_or(0.0)
                    };
                    self.norms[idx] = norm;
                    // dense_offsets stay DENSE_ABSENT for Tiered (not used)
                }
            }
            VectorData::InMemoryQuantized(vec) => {
                let params = self.quant_params.as_ref();
                for (idx, offset) in entries {
                    let slot = (offset / dim) as u32;
                    self.dense_offsets[idx] = slot;
                    if let Some(p) = params {
                        if offset + dim <= vec.len() {
                            self.norms[idx] = p.quantized_l2_norm(&vec[offset..offset + dim]);
                        }
                    }
                }
            }
        }
    }

    // ========================================================================
    // Read Operations
    // ========================================================================

    /// Get embedding by VectorId
    ///
    /// Returns None if the vector doesn't exist.
    /// Uses O(1) dense offset lookup for InMemory heaps, falls back to
    /// BTreeMap for Mmap/Tiered.
    #[inline]
    pub fn get(&self, id: VectorId) -> Option<&[f32]> {
        let idx = id.0 as usize;
        if idx < self.dense_offsets.len() {
            let slot = self.dense_offsets[idx];
            if slot != DENSE_ABSENT {
                let dim = self.config.dimension;
                let offset = slot as usize * dim;
                return match &self.data {
                    VectorData::InMemory(vec) => Some(&vec[offset..offset + dim]),
                    _ => self.get_btree(id),
                };
            }
        }
        // Fallback: try BTreeMap path (handles Mmap/Tiered)
        self.get_btree(id)
    }

    /// BTreeMap-only embedding lookup (slow path).
    ///
    /// Used as fallback when the dense offset array doesn't cover this ID.
    fn get_btree(&self, id: VectorId) -> Option<&[f32]> {
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
            VectorData::InMemoryQuantized(_) => {
                // Quantized heaps don't have f32 data.
                // Use get_f32_owned() or get_quantized() instead.
                None
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
                // Invariant: id_to_offset is always consistent with the mmap data.
                // A mismatch here means the structure is corrupt.
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
                        // Invariant: id_to_offset is always consistent with the base mmap.
                        // A mismatch here means the structure is corrupt.
                        base.get(id)
                            .expect("id_to_offset has stale entry (tiered base)")
                    }
                }
                VectorData::InMemoryQuantized(_) => {
                    panic!("iter() not supported on quantized heap — use ids() + distance_to()")
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
    ///
    /// # Panics
    ///
    /// Panics if the heap is backed by mmap or tiered storage.
    pub fn raw_data(&self) -> &[f32] {
        match &self.data {
            VectorData::InMemory(vec) => vec,
            VectorData::Mmap(_) => panic!("raw_data() not available on mmap-backed heap"),
            VectorData::Tiered { .. } => panic!("raw_data() not available on tiered heap"),
            VectorData::InMemoryQuantized(_) => {
                panic!("raw_data() not available on quantized heap")
            }
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
            VectorData::InMemoryQuantized(q_vec) => {
                // Dequantize to f32 for mmap persistence — MmapVectorData::open()
                // only reads f32 format. On reload, from_mmap() will re-quantize.
                let params = self
                    .quant_params
                    .as_ref()
                    .expect("Int8 requires quant_params");
                let dim = self.config.dimension;
                let mut f32_data = vec![0.0f32; q_vec.len()];
                for (&_id, &offset) in &self.id_to_offset {
                    if offset + dim <= q_vec.len() {
                        params.dequantize_into(
                            &q_vec[offset..offset + dim],
                            &mut f32_data[offset..offset + dim],
                        );
                    }
                }
                mmap::write_mmap_file(
                    path,
                    dim,
                    self.next_id.load(Ordering::Relaxed),
                    &self.id_to_offset,
                    &self.free_slots,
                    &f32_data,
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

        self.rebuild_dense_index();

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
            VectorData::InMemoryQuantized(v) => v.len(), // 1 byte per element
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
                // Rebuild norms for the new Tiered layout (dense_offsets are
                // invalidated since the data is no longer InMemory).
                self.rebuild_dense_index();
            }
            other => {
                // Put it back — InMemory and Tiered stay as-is
                self.data = other;
            }
        }
    }
}

/// Compute L2 norm of a vector.
#[inline]
fn compute_l2_norm(v: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for &x in v {
        sum += x * x;
    }
    sum.sqrt()
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
        for item in &data[offset..offset + 384] {
            assert_eq!(*item, 0.0, "Data should be zeroed after deletion");
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
        let _id1 = heap.insert(&e1).unwrap();
        let _id2 = heap.insert(&e2).unwrap();

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

    #[test]
    fn test_upsert_on_mmap_heap_returns_error() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config.clone());
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.freeze_to_disk(&vec_path).unwrap();

        // Reopen as mmap
        let mut mmap_heap = VectorHeap::from_mmap(&vec_path, config).unwrap();
        assert!(mmap_heap.is_mmap());

        // Upsert on mmap heap should return an error, not panic
        let result = mmap_heap.upsert(VectorId::new(2), &[0.0, 1.0, 0.0]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("cannot mutate mmap-backed"),
            "Error message should mention mmap mutation: {}",
            err
        );

        // Existing data should be unaffected
        assert_eq!(mmap_heap.len(), 1);
        assert!(mmap_heap.get(VectorId::new(1)).is_some());
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

    // ====================================================================
    // Dense offsets + norms acceleration tests
    // ====================================================================

    #[test]
    fn test_get_uses_dense_path() {
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config);

        // Insert non-contiguous IDs to exercise dense_offsets with gaps
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(5), &[0.0, 1.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(10), &[0.0, 0.0, 1.0]).unwrap();

        // Verify get() returns correct embeddings (not just self-consistent)
        assert_eq!(heap.get(VectorId::new(1)).unwrap(), &[1.0, 0.0, 0.0]);
        assert_eq!(heap.get(VectorId::new(5)).unwrap(), &[0.0, 1.0, 0.0]);
        assert_eq!(heap.get(VectorId::new(10)).unwrap(), &[0.0, 0.0, 1.0]);

        // Both paths (dense and btree) must agree
        assert_eq!(heap.get(VectorId::new(1)), heap.get_btree(VectorId::new(1)),);
        assert_eq!(heap.get(VectorId::new(5)), heap.get_btree(VectorId::new(5)),);

        // Gap IDs should return None
        assert!(heap.get(VectorId::new(3)).is_none());
        assert!(heap.get(VectorId::new(7)).is_none());
        // Non-existent ID beyond range
        assert!(heap.get(VectorId::new(99)).is_none());
        // ID 0 (before any inserts)
        assert!(heap.get(VectorId::new(0)).is_none());
    }

    #[test]
    fn test_dense_offsets_after_delete() {
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config);

        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();

        assert!(heap.get(VectorId::new(1)).is_some());
        heap.delete(VectorId::new(1));
        assert!(heap.get(VectorId::new(1)).is_none());
        // Other entries unaffected
        assert!(heap.get(VectorId::new(2)).is_some());
    }

    #[test]
    fn test_norms_correctness() {
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config);

        let emb = [3.0, 4.0, 0.0]; // norm = 5.0
        heap.upsert(VectorId::new(1), &emb).unwrap();

        let norm = heap.get_norm(VectorId::new(1)).unwrap();
        assert!((norm - 5.0).abs() < 1e-6, "Expected norm 5.0, got {}", norm);

        // After delete, norm should be gone
        heap.delete(VectorId::new(1));
        assert!(heap.get_norm(VectorId::new(1)).is_none());
    }

    #[test]
    fn test_norms_updated_on_upsert() {
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config);

        heap.upsert(VectorId::new(1), &[3.0, 4.0, 0.0]).unwrap(); // norm = 5.0
        assert!((heap.get_norm(VectorId::new(1)).unwrap() - 5.0).abs() < 1e-6);

        // Update to different embedding
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap(); // norm = 1.0
        assert!((heap.get_norm(VectorId::new(1)).unwrap() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_norms_survive_snapshot_restore() {
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config.clone());

        heap.upsert(VectorId::new(1), &[3.0, 4.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(2), &[1.0, 0.0, 0.0]).unwrap();

        let data = heap.raw_data().to_vec();
        let id_to_offset = heap.id_to_offset_map().clone();
        let free_slots = heap.free_slots().to_vec();
        let next_id = heap.next_id_value();

        let restored = VectorHeap::from_snapshot(config, data, id_to_offset, free_slots, next_id);

        assert!((restored.get_norm(VectorId::new(1)).unwrap() - 5.0).abs() < 1e-6);
        assert!((restored.get_norm(VectorId::new(2)).unwrap() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_tiered_norms_maintained() {
        let (mut heap, _dir) = make_tiered_heap(3);

        // Base vectors should have norms from rebuild_dense_index
        let norm1 = heap.get_norm(VectorId::new(1)).unwrap();
        let expected = compute_l2_norm(&[1.0, 1.0, 1.0]);
        assert!((norm1 - expected).abs() < 1e-6);

        // New upsert into overlay should update norms
        heap.upsert(VectorId::new(10), &[3.0, 4.0, 0.0]).unwrap();
        assert!((heap.get_norm(VectorId::new(10)).unwrap() - 5.0).abs() < 1e-6);

        // Delete should clear norms
        heap.delete(VectorId::new(10));
        assert!(heap.get_norm(VectorId::new(10)).is_none());
    }

    #[test]
    fn test_mmap_delete_clears_dense_offsets() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config.clone());
        heap.upsert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        heap.upsert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        heap.freeze_to_disk(&vec_path).unwrap();

        let mut mmap_heap = VectorHeap::from_mmap(&vec_path, config).unwrap();
        assert!(mmap_heap.get_norm(VectorId::new(1)).is_some());

        mmap_heap.delete(VectorId::new(1));
        assert!(mmap_heap.get_norm(VectorId::new(1)).is_none());
        // Other entry unaffected
        assert!(mmap_heap.get_norm(VectorId::new(2)).is_some());
    }

    #[test]
    fn test_get_by_slot_correctness() {
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config);

        heap.upsert(VectorId::new(1), &[1.0, 2.0, 3.0]).unwrap();
        heap.upsert(VectorId::new(2), &[4.0, 5.0, 6.0]).unwrap();

        // Slot 0 should be VectorId(1)
        let emb = heap.get_by_slot(0).unwrap();
        assert_eq!(emb, &[1.0, 2.0, 3.0]);

        // Slot 1 should be VectorId(2)
        let emb = heap.get_by_slot(1).unwrap();
        assert_eq!(emb, &[4.0, 5.0, 6.0]);

        // DENSE_ABSENT should return None
        assert!(heap.get_by_slot(DENSE_ABSENT).is_none());

        // Out of range should return None
        assert!(heap.get_by_slot(999).is_none());
    }
}

#[cfg(test)]
mod profiling_tests {
    use super::*;
    use std::time::Instant;

    fn make_embedding(dim: usize, seed: usize) -> Vec<f32> {
        (0..dim)
            .map(|j| ((seed * dim + j) as f32 / 1000.0).sin())
            .collect()
    }

    /// Test 7: VectorHeap get() vs get_btree() (100K vectors, 1M lookups)
    ///
    /// Benchmarks the O(1) dense-offset path (get) against the
    /// BTreeMap-only path (get_btree) to quantify the acceleration ratio.
    #[test]
    #[ignore] // profiling test — run explicitly with `cargo test -- --ignored`
    fn profile_heap_get_vs_get_btree() {
        let dim = 128;
        let n = 100_000;
        let num_lookups = 1_000_000;

        let config = VectorConfig::new(dim, DistanceMetric::Cosine).unwrap();
        let mut heap = VectorHeap::new(config);

        // Insert n vectors (IDs 1..=n)
        for i in 1..=n {
            let emb = make_embedding(dim, i);
            heap.upsert(VectorId::new(i as u64), &emb).unwrap();
        }

        // Pre-generate random lookup IDs (deterministic via sin-based hash)
        let lookup_ids: Vec<VectorId> = (0..num_lookups)
            .map(|i| {
                let idx = ((i as f64 * 0.618033988749895).fract() * n as f64) as u64 + 1;
                VectorId::new(idx)
            })
            .collect();

        // --- get() (dense fast path) ---
        let start = Instant::now();
        let mut checksum_fast = 0.0f64;
        for &id in &lookup_ids {
            if let Some(emb) = heap.get(id) {
                checksum_fast += emb[0] as f64;
            }
        }
        let fast_elapsed = start.elapsed();
        let fast_ns = fast_elapsed.as_nanos() as f64 / num_lookups as f64;

        // --- get_btree() (BTreeMap-only path) ---
        let start = Instant::now();
        let mut checksum_get = 0.0f64;
        for &id in &lookup_ids {
            if let Some(emb) = heap.get_btree(id) {
                checksum_get += emb[0] as f64;
            }
        }
        let get_elapsed = start.elapsed();
        let get_ns = get_elapsed.as_nanos() as f64 / num_lookups as f64;

        let speedup = get_ns / fast_ns;

        println!("\n=== VectorHeap: get() vs get_btree() ({n} vectors, {num_lookups} lookups, dim={dim}) ===");
        println!("  get():          {fast_ns:.1} ns/call ({fast_elapsed:?} total)");
        println!("  get_btree():    {get_ns:.1} ns/call ({get_elapsed:?} total)");
        println!("  speedup: {speedup:.2}x");
        println!("  checksums: get={checksum_fast:.4}, get_btree={checksum_get:.4}");
        assert!(
            (checksum_fast - checksum_get).abs() < 1e-2,
            "Checksums diverged: get={checksum_fast}, get_btree={checksum_get}"
        );
    }
}
