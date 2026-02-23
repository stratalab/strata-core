//! Vector Index Backend trait
//!
//! Defines the interface for swappable vector index implementations.
//! BruteForceBackend (O(n) search)
//! HnswBackend (O(log n) search) - reserved

use crate::primitives::vector::types::InlineMeta;
use crate::primitives::vector::{DistanceMetric, VectorConfig, VectorError, VectorId};

/// Trait for swappable vector index implementations
///
/// BruteForceBackend (O(n) search)
/// HnswBackend (O(log n) search)
///
/// IMPORTANT: This trait is designed to work for BOTH brute-force and HNSW.
/// Do NOT add methods that assume brute-force semantics (like get_all_vectors).
/// See Evolution Warning A in architecture documentation.
pub trait VectorIndexBackend: Send + Sync {
    /// Allocate a new VectorId (monotonically increasing, per-collection)
    ///
    /// Each collection has its own ID counter. IDs are never reused.
    /// This counter is persisted in snapshots via `snapshot_state()`.
    ///
    /// CRITICAL: This is per-collection, not global. Two separate databases
    /// doing identical operations MUST get identical VectorIds.
    fn allocate_id(&mut self) -> VectorId;

    /// Insert a vector (upsert semantics)
    ///
    /// If the VectorId already exists, updates the embedding.
    /// The VectorId is assigned externally and passed in.
    fn insert(&mut self, id: VectorId, embedding: &[f32]) -> Result<(), VectorError>;

    /// Insert with specific VectorId (for WAL replay)
    ///
    /// Used during recovery to replay WAL entries with their original IDs.
    /// Updates next_id if necessary to maintain monotonicity (Invariant T4).
    ///
    /// IMPORTANT: This method MUST ensure that future ID allocations
    /// don't reuse IDs from replayed entries.
    fn insert_with_id(&mut self, id: VectorId, embedding: &[f32]) -> Result<(), VectorError>;

    /// Insert with timestamp (for temporal tracking)
    ///
    /// Backends that support temporal tracking override this to store the
    /// creation timestamp. Default: delegates to `insert()`, ignoring timestamp.
    fn insert_with_timestamp(
        &mut self,
        id: VectorId,
        embedding: &[f32],
        _created_at: u64,
    ) -> Result<(), VectorError> {
        self.insert(id, embedding)
    }

    /// Insert with specific ID and timestamp (for recovery with temporal data)
    ///
    /// Stores the timestamp for later use by `rebuild_index()`.
    /// Default: delegates to `insert_with_id()`, ignoring timestamp.
    fn insert_with_id_and_timestamp(
        &mut self,
        id: VectorId,
        embedding: &[f32],
        _created_at: u64,
    ) -> Result<(), VectorError> {
        self.insert_with_id(id, embedding)
    }

    /// Delete a vector
    ///
    /// Returns true if the vector existed and was deleted.
    fn delete(&mut self, id: VectorId) -> Result<bool, VectorError>;

    /// Delete with timestamp (for temporal tracking)
    ///
    /// Backends that support temporal tracking override this to record
    /// the deletion timestamp. Default: delegates to `delete()`, ignoring timestamp.
    fn delete_with_timestamp(
        &mut self,
        id: VectorId,
        _deleted_at: u64,
    ) -> Result<bool, VectorError> {
        self.delete(id)
    }

    /// Search for k nearest neighbors
    ///
    /// Returns (VectorId, score) pairs.
    /// Scores are normalized to "higher = more similar" (Invariant R2).
    /// Results are sorted by (score desc, VectorId asc) for determinism (Invariant R4).
    fn search(&self, query: &[f32], k: usize) -> Vec<(VectorId, f32)>;

    /// Search for k nearest neighbors as of a given timestamp.
    ///
    /// Backends that support temporal tracking override this. Default: delegates to
    /// regular search (ignoring timestamp), which is correct for backends without
    /// temporal data.
    fn search_at(&self, query: &[f32], k: usize, _as_of_ts: u64) -> Vec<(VectorId, f32)> {
        self.search(query, k)
    }

    /// Search for k nearest neighbors created within a time range.
    ///
    /// Backends that support temporal tracking override this. Default: delegates to
    /// regular search (ignoring time range), which is correct for backends without
    /// temporal data.
    fn search_in_range(
        &self,
        query: &[f32],
        k: usize,
        _start_ts: u64,
        _end_ts: u64,
    ) -> Vec<(VectorId, f32)> {
        self.search(query, k)
    }

    /// Get number of indexed vectors
    fn len(&self) -> usize;

    /// Check if empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get embedding dimension
    fn dimension(&self) -> usize;

    /// Get distance metric
    fn metric(&self) -> DistanceMetric;

    /// Get full collection config (Issue #452: for config validation during replay)
    fn config(&self) -> VectorConfig;

    /// Get a vector by ID (for metadata lookups after search)
    fn get(&self, id: VectorId) -> Option<&[f32]>;

    /// Check if a vector exists
    fn contains(&self, id: VectorId) -> bool;

    /// Return the index type name (e.g., "brute_force", "hnsw")
    fn index_type_name(&self) -> &'static str;

    /// Return approximate memory usage in bytes
    fn memory_usage(&self) -> usize;

    /// Rebuild derived index structures after recovery
    ///
    /// For BruteForce backend, this is a no-op.
    /// For HNSW backend, this rebuilds the graph from the heap.
    fn rebuild_index(&mut self) {
        // Default: no-op (BruteForce has no derived structures)
    }

    /// Seal any remaining active buffer entries into HNSW segments.
    ///
    /// Called after loading graphs from mmap cache to ensure no vectors
    /// remain in the brute-force active buffer. Default: no-op.
    fn seal_remaining_active(&mut self) {
        // Default: no-op (backends without active buffers ignore this)
    }

    /// Write the embedding heap to a `.vec` mmap cache file.
    ///
    /// Called after recovery to create a disk cache that speeds up subsequent
    /// starts. Default: no-op (backends that don't support mmap ignore this).
    fn freeze_heap_to_disk(&self, _path: &std::path::Path) -> Result<(), VectorError> {
        Ok(())
    }

    /// Replace the internal heap with a pre-loaded (mmap-backed) heap.
    ///
    /// Used when recovery detects a valid `.vec` cache file. The caller
    /// is responsible for populating graph structures via `register_mmap_vector()`
    /// and `rebuild_index()` afterward.
    fn replace_heap(&mut self, _heap: crate::primitives::vector::VectorHeap) {
        // Default: no-op (backends that don't support mmap ignore this)
    }

    /// Register a vector (ID + timestamps) without inserting its embedding.
    ///
    /// Called during mmap-based recovery: the heap already contains the embedding
    /// (loaded from mmap), so we only need to record the ID and timestamp for
    /// active-buffer / graph population before `rebuild_index()`.
    fn register_mmap_vector(&mut self, _id: VectorId, _created_at: u64) {
        // Default: no-op
    }

    /// Check if the internal heap is backed by a memory-mapped file.
    fn is_heap_mmap(&self) -> bool {
        false
    }

    /// Flush the embedding heap to disk if the in-memory overlay exceeds a
    /// size threshold. Returns `true` if a flush was actually performed.
    ///
    /// Called after segment sealing to keep anonymous memory bounded during
    /// long-running indexing. Default: no-op.
    fn flush_heap_to_disk_if_needed(
        &mut self,
        _path: &std::path::Path,
    ) -> Result<bool, VectorError> {
        Ok(false)
    }

    /// Write sealed segment graphs to disk for mmap-accelerated recovery.
    ///
    /// `dir` is the directory for graph files (e.g., `data_dir/vectors/{branch}/{collection}/`).
    /// Default: no-op (backends without sealed segments ignore this).
    fn freeze_graphs_to_disk(&self, _dir: &std::path::Path) -> Result<(), VectorError> {
        Ok(())
    }

    /// Load sealed segment graphs from mmap files, skipping `rebuild_index()`.
    ///
    /// Returns `true` if graphs were successfully loaded; `false` if files are
    /// missing/corrupt and the caller should fall back to `rebuild_index()`.
    /// Default: returns `false` (backends without sealed segments).
    fn load_graphs_from_disk(&mut self, _dir: &std::path::Path) -> Result<bool, VectorError> {
        Ok(false)
    }

    // ========================================================================
    // Inline Metadata (O(1) search resolution)
    // ========================================================================

    /// Store inline metadata for a VectorId (key + source_ref).
    /// Used to avoid O(n) KV prefix scans during search result resolution.
    fn set_inline_meta(&mut self, _id: VectorId, _meta: InlineMeta) {
        // Default: no-op (BruteForce ignores inline meta)
    }

    /// Get inline metadata for a VectorId.
    fn get_inline_meta(&self, _id: VectorId) -> Option<&InlineMeta> {
        None
    }

    /// Remove inline metadata for a VectorId.
    fn remove_inline_meta(&mut self, _id: VectorId) {
        // Default: no-op
    }

    // ========================================================================
    // Snapshot Methods
    // ========================================================================

    /// Get all VectorIds in deterministic order
    ///
    /// Returns VectorIds sorted ascending for deterministic snapshot serialization.
    fn vector_ids(&self) -> Vec<VectorId>;

    /// Get snapshot state for serialization
    ///
    /// Returns (next_id, free_slots) for snapshot header.
    /// CRITICAL: next_id and free_slots MUST be persisted to maintain
    /// VectorId uniqueness across restarts (Invariant T4).
    fn snapshot_state(&self) -> (u64, Vec<usize>);

    /// Restore snapshot state after deserialization
    ///
    /// Called after all vectors have been inserted with insert_with_id()
    /// to restore the exact next_id and free_slots from the snapshot.
    fn restore_snapshot_state(&mut self, next_id: u64, free_slots: Vec<usize>);
}

/// Factory for creating index backends
///
/// This abstraction allows switching between BruteForce and HNSW
/// without changing the VectorStore code.
#[derive(Clone)]
pub enum IndexBackendFactory {
    /// Brute-force O(n) search
    BruteForce,
    /// HNSW O(log n) approximate nearest neighbor search
    Hnsw(super::hnsw::HnswConfig),
    /// Segmented HNSW: O(1) inserts, multi-segment fan-out search
    SegmentedHnsw(super::segmented::SegmentedHnswConfig),
}

impl Default for IndexBackendFactory {
    fn default() -> Self {
        IndexBackendFactory::SegmentedHnsw(super::segmented::SegmentedHnswConfig::default())
    }
}

impl IndexBackendFactory {
    /// Create a new backend instance
    pub fn create(&self, config: &VectorConfig) -> Box<dyn VectorIndexBackend> {
        match self {
            IndexBackendFactory::BruteForce => {
                Box::new(super::brute_force::BruteForceBackend::new(config))
            }
            IndexBackendFactory::Hnsw(hnsw_config) => {
                Box::new(super::hnsw::HnswBackend::new(config, hnsw_config.clone()))
            }
            IndexBackendFactory::SegmentedHnsw(seg_config) => Box::new(
                super::segmented::SegmentedHnswBackend::new(config, seg_config.clone()),
            ),
        }
    }

    /// Get the index type name
    pub fn index_type_name(&self) -> &'static str {
        match self {
            IndexBackendFactory::BruteForce => "brute_force",
            IndexBackendFactory::Hnsw(_) => "hnsw",
            IndexBackendFactory::SegmentedHnsw(_) => "segmented_hnsw",
        }
    }
}
