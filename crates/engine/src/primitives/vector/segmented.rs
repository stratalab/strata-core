//! Segmented HNSW Backend
//!
//! Multi-segment architecture for vector indexing:
//! - O(1) inserts into an active buffer (brute-force searchable)
//! - Periodic sealing into small HNSW segments at a configurable threshold
//! - Fan-out search across all segments with merged results
//!
//! ## Single Heap Design
//!
//! A global `VectorHeap` is the authoritative source for embeddings, ID allocation,
//! and snapshot state. Each `SealedSegment` contains a `CompactHnswGraph` (graph-only,
//! no embedding ownership, flat `Vec<u64>` neighbor storage) which references the global
//! heap for distance computation. This eliminates both the dual-heap duplication and
//! the BTreeSet-per-layer overhead (~48→8 bytes per neighbor).
//!
//! ## Determinism
//!
//! - BTreeMap for timestamps, sorted VectorId within segments
//! - Segments ordered by segment_id
//! - All search merges use (score desc, VectorId asc) comparator
//! - Each sealed segment's HnswGraph uses seed=42, vectors inserted in VectorId order

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};

use once_cell::sync::Lazy;
use rayon::prelude::*;

use crate::primitives::vector::backend::VectorIndexBackend;
use crate::primitives::vector::distance::compute_similarity_cached;
use crate::primitives::vector::heap::VectorHeap;
use crate::primitives::vector::hnsw::{CompactHnswGraph, HnswConfig, HnswGraph};
use crate::primitives::vector::types::InlineMeta;
use crate::primitives::vector::{DistanceMetric, VectorConfig, VectorError, VectorId};

/// Scored entry for top-k selection in active buffer search.
///
/// Ordering matches the output invariant (R4): higher score is Greater,
/// lower VectorId is Greater for ties. Used in a min-heap
/// (`BinaryHeap<Reverse<ActiveScored>>`) so that `peek()` returns the
/// worst-quality result for O(1) eviction.
#[derive(Clone, PartialEq)]
struct ActiveScored {
    score: f32,
    id: VectorId,
}

impl Eq for ActiveScored {}

impl PartialOrd for ActiveScored {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ActiveScored {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.id.cmp(&self.id))
    }
}

/// Entry for k-way merge heap. Ordered by (score desc, VectorId asc) to
/// match the output ordering invariant (R4).
struct KWayEntry {
    score: f32,
    id: VectorId,
    set_idx: usize,
}

impl PartialEq for KWayEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.id == other.id
    }
}

impl Eq for KWayEntry {}

impl PartialOrd for KWayEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KWayEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher score = Greater (max-heap pops highest first)
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
            // Tie-break: lower VectorId = Greater (preferred)
            .then_with(|| other.id.cmp(&self.id))
    }
}

/// Dedicated thread pool for parallel vector search.
///
/// Isolated from rayon's global pool so that:
/// - Strata never hijacks the caller's rayon threads
/// - Thread count is capped at `MAX_SEARCH_THREADS` regardless of core count
/// - Stack size is reduced to 1 MB (search is not deeply recursive)
static SEARCH_POOL: Lazy<rayon::ThreadPool> = Lazy::new(|| {
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get().min(MAX_SEARCH_THREADS))
        .unwrap_or(2);
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .thread_name(|i| format!("strata-vsearch-{}", i))
        .stack_size(1024 * 1024) // 1 MB per thread (vs default 2 MB)
        .build()
        .unwrap_or_else(|e| panic!("failed to build vector search thread pool: {e}"))
});

/// Minimum number of sealed segments before using parallel search.
/// Below this threshold, sequential iteration avoids rayon thread pool overhead.
const PARALLEL_SEARCH_THRESHOLD: usize = 4;

/// Maximum number of threads in the dedicated vector search thread pool.
/// Capped to prevent an embedded database from consuming all CPU cores on
/// large machines. The actual count is `min(available_cores, MAX_SEARCH_THREADS)`.
const MAX_SEARCH_THREADS: usize = 4;

/// Segmented HNSW configuration
#[derive(Debug, Clone)]
pub struct SegmentedHnswConfig {
    /// HNSW config used for each sealed segment
    pub hnsw: HnswConfig,
    /// Number of vectors in the active buffer before sealing (default: 50_000)
    pub seal_threshold: usize,
    /// Number of overlay vectors before flushing heap to mmap (default: 500_000).
    /// Set to 0 to disable periodic flushing.
    pub heap_flush_threshold: usize,
    /// Maximum number of sealed segments before auto-compaction merges them
    /// into a single segment. Reduces fan-out penalty at high scale but trades
    /// recall for throughput — a compacted segment searched with ef_search=100
    /// explores far fewer candidates relative to its size than multiple smaller
    /// segments. Disabled by default (`usize::MAX`).
    /// A value of 4 fires compaction every `seal_threshold × 5` inserts.
    pub auto_compact_threshold: usize,
}

impl Default for SegmentedHnswConfig {
    fn default() -> Self {
        Self {
            hnsw: HnswConfig::default(),
            seal_threshold: 50_000,
            heap_flush_threshold: 500_000,
            auto_compact_threshold: usize::MAX,
        }
    }
}

/// Active buffer: brute-force searchable, O(1) insert
struct ActiveBuffer {
    /// VectorIds currently in the active buffer
    ids: Vec<VectorId>,
    /// Timestamps: VectorId → (created_at, deleted_at)
    timestamps: BTreeMap<VectorId, (u64, Option<u64>)>,
}

impl ActiveBuffer {
    fn new() -> Self {
        Self {
            ids: Vec::new(),
            timestamps: BTreeMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.ids.len()
    }

    fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    fn contains(&self, id: VectorId) -> bool {
        self.timestamps.contains_key(&id)
    }

    /// Insert a vector into the active buffer with timestamp
    fn insert(&mut self, id: VectorId, created_at: u64) {
        if !self.timestamps.contains_key(&id) {
            self.ids.push(id);
        }
        self.timestamps.insert(id, (created_at, None));
    }

    /// Soft-delete a vector in the active buffer.
    ///
    /// Removes from `ids` so that `len()` only reflects live entries,
    /// preventing premature seal triggers from deleted vectors.
    fn soft_delete(&mut self, id: VectorId, deleted_at: u64) -> bool {
        if let Some(entry) = self.timestamps.get_mut(&id) {
            if entry.1.is_none() {
                entry.1 = Some(deleted_at);
                self.ids.retain(|&i| i != id);
                return true;
            }
        }
        false
    }

    /// Remove a vector entirely (for updates — old entry replaced)
    fn remove(&mut self, id: VectorId) {
        self.timestamps.remove(&id);
        self.ids.retain(|&i| i != id);
    }

    /// Drain all entries, returning (ids, timestamps) sorted by VectorId
    #[allow(clippy::type_complexity)]
    fn drain_sorted(&mut self) -> (Vec<VectorId>, BTreeMap<VectorId, (u64, Option<u64>)>) {
        let mut ids = std::mem::take(&mut self.ids);
        ids.sort();
        ids.dedup();
        let timestamps = std::mem::take(&mut self.timestamps);
        (ids, timestamps)
    }
}

/// A sealed segment containing a compact HNSW graph
///
/// Uses `CompactHnswGraph` instead of `HnswBackend` to avoid duplicating embeddings
/// and to reduce per-neighbor overhead from ~48 bytes (BTreeSet) to 8 bytes (u64).
/// All distance computations use the global `VectorHeap` passed at search time.
struct SealedSegment {
    /// Monotonically increasing segment identifier
    #[allow(dead_code)]
    segment_id: u64,
    /// Compact graph index (sorted Vec<u64> neighbors, no BTreeSet overhead)
    graph: CompactHnswGraph,
    /// Count of live (non-deleted) vectors in this segment
    live_count: usize,
    /// Which branch produced this segment (Phase 2: merge awareness)
    #[allow(dead_code)]
    source_branch: Option<strata_core::BranchId>,
}

/// Segmented HNSW backend
///
/// Replaces monolithic HNSW with a multi-segment architecture:
/// - Active buffer for O(1) inserts
/// - Sealed segments for O(log n) HNSW search
/// - Fan-out search across all segments
pub struct SegmentedHnswBackend {
    config: SegmentedHnswConfig,
    vector_config: VectorConfig,
    /// Global source of truth for embeddings + ID allocation
    heap: VectorHeap,
    /// Active buffer (brute-force searchable)
    active: ActiveBuffer,
    /// Sealed HNSW segments, ordered by segment_id
    sealed: Vec<SealedSegment>,
    /// Next segment_id to assign
    next_segment_id: u64,
    /// Timestamps stored during recovery, consumed by rebuild_index()
    pending_timestamps: BTreeMap<VectorId, u64>,
    /// Path to the `.vec` mmap file (set when heap is loaded from mmap).
    /// Used for periodic overlay flushing.
    flush_path: Option<std::path::PathBuf>,
}

impl SegmentedHnswBackend {
    /// Create a new segmented HNSW backend
    pub fn new(vector_config: &VectorConfig, config: SegmentedHnswConfig) -> Self {
        Self {
            config,
            vector_config: vector_config.clone(),
            heap: VectorHeap::new(vector_config.clone()),
            active: ActiveBuffer::new(),
            sealed: Vec::new(),
            next_segment_id: 0,
            pending_timestamps: BTreeMap::new(),
            flush_path: None,
        }
    }

    /// Get read access to the global heap
    pub fn heap(&self) -> &VectorHeap {
        &self.heap
    }

    /// Get mutable access to the global heap (for recovery)
    pub fn heap_mut(&mut self) -> &mut VectorHeap {
        &mut self.heap
    }

    // ========================================================================
    // Seal Logic
    // ========================================================================

    /// Seal the active buffer into a new HNSW segment
    fn seal_active_buffer(&mut self) {
        if self.active.is_empty() {
            return;
        }

        let (ids, timestamps) = self.active.drain_sorted();

        // Create a graph-only HNSW structure (no embedding duplication)
        let mut graph = HnswGraph::new(&self.vector_config, self.config.hnsw.clone());
        let mut live_count = 0;

        // Insert each vector into the graph, using global heap for distance computation
        for &id in &ids {
            if let Some(embedding) = self.heap.get(id) {
                let embedding = embedding.to_vec();
                let created_at = timestamps.get(&id).map(|t| t.0).unwrap_or(0);
                graph.insert_into_graph(id, &embedding, created_at, &self.heap);
                // Apply soft-delete if marked
                if let Some(&(_, Some(deleted_at))) = timestamps.get(&id) {
                    graph.delete_with_timestamp(id, deleted_at);
                } else {
                    live_count += 1;
                }
            }
            // else: vector was deleted from global heap between insert and seal — skip
        }

        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;

        self.sealed.push(SealedSegment {
            segment_id,
            graph: CompactHnswGraph::from_graph(&graph),
            live_count,
            source_branch: None,
        });

        // Auto-compact: if too many sealed segments have accumulated, merge them
        // into one to reduce the fan-out penalty on search. At 1M vectors with
        // seal_threshold=50K, this prevents 20-segment fan-out.
        if self.sealed.len() > self.config.auto_compact_threshold {
            tracing::info!(
                target: "strata::vector",
                segments = self.sealed.len(),
                threshold = self.config.auto_compact_threshold,
                "Auto-compacting sealed segments"
            );
            self.compact();
        }
    }

    /// Flush the heap to mmap if it exceeds the configured threshold.
    ///
    /// After sealing a segment, this checks whether enough vectors have
    /// accumulated in anonymous memory to warrant flushing to disk.
    ///
    /// - **Tiered heap**: flushes the overlay into the mmap base.
    /// - **InMemory heap** (fresh indexing, never frozen): freezes all data to
    ///   a `.vec` file, reopens as mmap, and promotes to Tiered mode so that
    ///   subsequent inserts go to a small overlay instead of growing anon memory.
    fn flush_heap_if_needed(&mut self) {
        let threshold = self.config.heap_flush_threshold;
        if threshold == 0 {
            return; // Flushing disabled
        }
        let Some(path) = self.flush_path.clone() else {
            return; // No flush path configured (in-memory database)
        };

        if !self.heap.is_mmap() {
            // InMemory heap: check total vector count against threshold
            if self.heap.len() < threshold {
                return;
            }

            tracing::info!(
                target: "strata::vector",
                total_vectors = self.heap.len(),
                threshold,
                "Freezing InMemory heap to mmap (first flush)"
            );

            // Freeze InMemory → disk, reopen as mmap, promote to Tiered
            if let Err(e) = self.heap.freeze_to_disk(&path) {
                tracing::warn!(
                    target: "strata::vector",
                    error = %e,
                    "Failed to freeze InMemory heap to disk, continuing in-memory"
                );
                return;
            }

            let next_id = self.heap.next_id_value();
            let free_slots = self.heap.free_slots().to_vec();

            match VectorHeap::from_mmap(&path, self.vector_config.clone()) {
                Ok(mut new_heap) => {
                    new_heap.promote_to_tiered();
                    new_heap.restore_snapshot_state(next_id, free_slots);
                    self.heap = new_heap;
                    tracing::info!(
                        target: "strata::vector",
                        "InMemory heap promoted to Tiered (mmap-backed)"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        target: "strata::vector",
                        error = %e,
                        "Failed to reopen frozen heap as mmap, continuing in-memory"
                    );
                }
            }
        } else {
            // Tiered heap: check overlay count against threshold
            let overlay_count = self.heap.overlay_len();
            if overlay_count < threshold {
                return;
            }

            tracing::info!(
                target: "strata::vector",
                overlay_count,
                threshold,
                "Flushing heap overlay to mmap"
            );

            match self.heap.flush_overlay_to_disk(&path) {
                Ok(n) => {
                    tracing::info!(
                        target: "strata::vector",
                        flushed = n,
                        "Heap overlay flushed to mmap"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        target: "strata::vector",
                        error = %e,
                        "Failed to flush heap overlay to mmap, continuing with in-memory overlay"
                    );
                }
            }
        }
    }

    // ========================================================================
    // Active Buffer Search (brute-force)
    // ========================================================================

    /// Brute-force search the active buffer.
    ///
    /// Optimizations vs naive BTreeMap iteration + collect + sort:
    /// - Iterates `self.active.ids` (dense Vec, excludes deleted entries) for cache locality
    /// - Pre-computes query norm once for cosine similarity caching
    /// - Uses a top-k BinaryHeap: O(n log k) vs O(n log n) for full sort
    /// - Prefetches next embedding while processing current one
    fn search_active(&self, query: &[f32], k: usize) -> Vec<(VectorId, f32)> {
        if self.active.ids.is_empty() {
            return Vec::new();
        }
        let metric = self.vector_config.metric;

        // Pre-compute query norm once (same pattern as CompactHnswGraph::search_with_heap_ef)
        let q_norm = if metric == DistanceMetric::Cosine {
            Some(query.iter().map(|x| x * x).sum::<f32>().sqrt())
        } else {
            None
        };

        // Top-k min-heap: worst-quality result on top for O(1) eviction.
        // ActiveScored orders by (score desc, VectorId asc) — same as output invariant R4.
        // Wrapping in Reverse makes BinaryHeap a min-heap by quality.
        let mut top_k: BinaryHeap<Reverse<ActiveScored>> = BinaryHeap::with_capacity(k + 1);
        let ids = &self.active.ids; // Dense Vec, excludes deleted entries

        for (idx, &id) in ids.iter().enumerate() {
            // Prefetch next embedding while processing current one
            if idx + 1 < ids.len() {
                self.heap.prefetch_embedding(ids[idx + 1]);
            }
            if let Some(embedding) = self.heap.get(id) {
                let score = compute_similarity_cached(
                    query,
                    embedding,
                    metric,
                    q_norm,
                    self.heap.get_norm(id),
                );
                let entry = ActiveScored { score, id };
                if top_k.len() < k {
                    top_k.push(Reverse(entry));
                } else if let Some(worst) = top_k.peek() {
                    if entry > worst.0 {
                        top_k.pop();
                        top_k.push(Reverse(entry));
                    }
                }
            }
        }

        let mut results: Vec<(VectorId, f32)> = top_k
            .into_iter()
            .map(|Reverse(e)| (e.id, e.score))
            .collect();
        // Sort: score desc, VectorId asc (Invariant R4)
        results.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        results
    }

    /// Brute-force search the active buffer with temporal filter (as_of).
    ///
    /// Must use `timestamps` BTreeMap for temporal filtering (deleted entries
    /// need checked too), but uses norm cache and top-k heap.
    fn search_active_at(&self, query: &[f32], k: usize, as_of_ts: u64) -> Vec<(VectorId, f32)> {
        if self.active.timestamps.is_empty() {
            return Vec::new();
        }
        let metric = self.vector_config.metric;
        let q_norm = if metric == DistanceMetric::Cosine {
            Some(query.iter().map(|x| x * x).sum::<f32>().sqrt())
        } else {
            None
        };

        let mut top_k: BinaryHeap<Reverse<ActiveScored>> = BinaryHeap::with_capacity(k + 1);

        for (&id, &(created_at, deleted_at)) in &self.active.timestamps {
            // Must have been created at or before as_of_ts
            if created_at > as_of_ts {
                continue;
            }
            // Must not be deleted at or before as_of_ts
            if let Some(d) = deleted_at {
                if d <= as_of_ts {
                    continue;
                }
            }
            if let Some(embedding) = self.heap.get(id) {
                let score = compute_similarity_cached(
                    query,
                    embedding,
                    metric,
                    q_norm,
                    self.heap.get_norm(id),
                );
                let entry = ActiveScored { score, id };
                if top_k.len() < k {
                    top_k.push(Reverse(entry));
                } else if let Some(worst) = top_k.peek() {
                    if entry > worst.0 {
                        top_k.pop();
                        top_k.push(Reverse(entry));
                    }
                }
            }
        }

        let mut results: Vec<(VectorId, f32)> = top_k
            .into_iter()
            .map(|Reverse(e)| (e.id, e.score))
            .collect();
        results.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        results
    }

    /// Brute-force search the active buffer with time range filter.
    ///
    /// Must use `timestamps` BTreeMap for temporal filtering, but uses
    /// norm cache and top-k heap.
    fn search_active_in_range(
        &self,
        query: &[f32],
        k: usize,
        start_ts: u64,
        end_ts: u64,
    ) -> Vec<(VectorId, f32)> {
        if self.active.timestamps.is_empty() {
            return Vec::new();
        }
        let metric = self.vector_config.metric;
        let q_norm = if metric == DistanceMetric::Cosine {
            Some(query.iter().map(|x| x * x).sum::<f32>().sqrt())
        } else {
            None
        };

        let mut top_k: BinaryHeap<Reverse<ActiveScored>> = BinaryHeap::with_capacity(k + 1);

        for (&id, &(created_at, deleted_at)) in &self.active.timestamps {
            if created_at < start_ts || created_at > end_ts {
                continue;
            }
            if deleted_at.is_some() {
                continue;
            }
            if let Some(embedding) = self.heap.get(id) {
                let score = compute_similarity_cached(
                    query,
                    embedding,
                    metric,
                    q_norm,
                    self.heap.get_norm(id),
                );
                let entry = ActiveScored { score, id };
                if top_k.len() < k {
                    top_k.push(Reverse(entry));
                } else if let Some(worst) = top_k.peek() {
                    if entry > worst.0 {
                        top_k.pop();
                        top_k.push(Reverse(entry));
                    }
                }
            }
        }

        let mut results: Vec<(VectorId, f32)> = top_k
            .into_iter()
            .map(|Reverse(e)| (e.id, e.score))
            .collect();
        results.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        results
    }

    // ========================================================================
    // Segment Extraction / Adoption (Phase 2: Branch-Aware Merge)
    // ========================================================================

    /// Extract all sealed segments from this backend.
    ///
    /// Returns the sealed segments and resets the backend's segment list.
    /// The global heap is NOT drained — callers must handle embeddings
    /// separately (e.g., via KV-level merge which copies VectorRecords).
    ///
    /// Used during branch merge to transfer pre-built HNSW graphs from
    /// the source branch to the target branch without full rebuild.
    #[allow(dead_code)]
    pub(crate) fn extract_segments(
        &mut self,
    ) -> Vec<(u64, CompactHnswGraph, usize, Option<strata_core::BranchId>)> {
        let sealed = std::mem::take(&mut self.sealed);
        sealed
            .into_iter()
            .map(|s| (s.segment_id, s.graph, s.live_count, s.source_branch))
            .collect()
    }

    /// Adopt sealed segments from another backend (e.g., after branch merge).
    ///
    /// Each adopted segment is re-tagged with a new segment_id from this
    /// backend's counter to maintain ordering invariants, and optionally
    /// tagged with the source branch for provenance tracking.
    ///
    /// NOTE: The caller must ensure that the VectorIds in the adopted segments
    /// are valid in this backend's global heap. In the current implementation,
    /// post_merge_reload_vectors() rebuilds from KV which is simpler and
    /// always correct. This method is provided for future O(metadata)
    /// segment adoption with VectorId remapping.
    #[allow(dead_code)]
    pub(crate) fn adopt_segments(
        &mut self,
        segments: Vec<(CompactHnswGraph, usize, Option<strata_core::BranchId>)>,
    ) {
        for (graph, live_count, source_branch) in segments {
            let segment_id = self.next_segment_id;
            self.next_segment_id += 1;
            self.sealed.push(SealedSegment {
                segment_id,
                graph,
                live_count,
                source_branch,
            });
        }
    }

    /// Get the number of sealed segments (for diagnostics / testing)
    pub fn segment_count(&self) -> usize {
        self.sealed.len()
    }

    /// Get the number of vectors in the active buffer (for diagnostics / testing)
    pub fn active_buffer_len(&self) -> usize {
        self.active.len()
    }

    // ========================================================================
    // Merge Results
    // ========================================================================

    /// Merge multiple sorted result sets into one via k-way merge.
    ///
    /// Each input set is already sorted by (score desc, VectorId asc).
    /// Segments should have disjoint VectorId sets (each vector lives in exactly
    /// one sealed segment; updates soft-delete the old entry before re-inserting).
    /// Duplicates are skipped defensively via O(k) linear scan per entry.
    fn merge_results(sets: Vec<Vec<(VectorId, f32)>>, k: usize) -> Vec<(VectorId, f32)> {
        // Fast paths
        if sets.is_empty() {
            return Vec::new();
        }
        if sets.len() == 1 {
            let mut single = sets.into_iter().next().unwrap();
            single.truncate(k);
            return single;
        }

        // K-way merge of pre-sorted inputs using a max-heap.
        // Each entry tracks which iterator it came from so we can advance it.
        let mut iters: Vec<std::vec::IntoIter<(VectorId, f32)>> =
            sets.into_iter().map(|s| s.into_iter()).collect();

        let mut heap: BinaryHeap<KWayEntry> = BinaryHeap::with_capacity(iters.len());

        // Seed heap with first element from each iterator
        for (i, iter) in iters.iter_mut().enumerate() {
            if let Some((id, score)) = iter.next() {
                heap.push(KWayEntry {
                    score,
                    id,
                    set_idx: i,
                });
            }
        }

        let mut merged = Vec::with_capacity(k);
        while let Some(entry) = heap.pop() {
            // Safety: skip duplicates defensively. Segments should have disjoint
            // VectorId sets (updates soft-delete old entry before re-inserting),
            // but a bug in the delete path could cause duplicates. The linear
            // scan is O(k) per pop with k typically ≤ 100 — negligible.
            if merged.iter().any(|(id, _)| *id == entry.id) {
                // Advance the iterator this entry came from so the merge continues
                if let Some((id, score)) = iters[entry.set_idx].next() {
                    heap.push(KWayEntry {
                        score,
                        id,
                        set_idx: entry.set_idx,
                    });
                }
                continue;
            }
            merged.push((entry.id, entry.score));
            if merged.len() >= k {
                break;
            }
            // Advance the iterator this entry came from
            if let Some((id, score)) = iters[entry.set_idx].next() {
                heap.push(KWayEntry {
                    score,
                    id,
                    set_idx: entry.set_idx,
                });
            }
        }
        merged
    }
}

impl VectorIndexBackend for SegmentedHnswBackend {
    fn allocate_id(&mut self) -> VectorId {
        self.heap.allocate_id()
    }

    fn insert(&mut self, id: VectorId, embedding: &[f32]) -> Result<(), VectorError> {
        self.insert_with_timestamp(id, embedding, 0)
    }

    fn insert_with_timestamp(
        &mut self,
        id: VectorId,
        embedding: &[f32],
        created_at: u64,
    ) -> Result<(), VectorError> {
        let is_update = self.heap.contains(id);

        // Upsert into global heap (authoritative store)
        self.heap.upsert(id, embedding)?;

        if is_update {
            // Soft-delete from wherever the old entry lives
            if self.active.contains(id) {
                self.active.remove(id);
            } else {
                // Find in sealed segments and soft-delete
                for seg in &mut self.sealed {
                    if seg.graph.contains(id) {
                        if seg.graph.delete_with_timestamp(id, created_at) {
                            seg.live_count = seg.live_count.saturating_sub(1);
                        }
                        break;
                    }
                }
            }
        }

        // Append to active buffer
        self.active.insert(id, created_at);

        // Seal if threshold reached
        if self.active.len() >= self.config.seal_threshold {
            self.seal_active_buffer();
            self.flush_heap_if_needed();
        }

        Ok(())
    }

    fn insert_with_id(&mut self, id: VectorId, embedding: &[f32]) -> Result<(), VectorError> {
        // Recovery path: insert into global heap only, no graph building
        self.heap.insert_with_id(id, embedding)?;
        // Track in active buffer for later rebuild_index()
        self.active.insert(id, 0);
        Ok(())
    }

    fn insert_with_id_and_timestamp(
        &mut self,
        id: VectorId,
        embedding: &[f32],
        created_at: u64,
    ) -> Result<(), VectorError> {
        self.heap.insert_with_id(id, embedding)?;
        self.active.insert(id, created_at);
        self.pending_timestamps.insert(id, created_at);
        Ok(())
    }

    fn delete(&mut self, id: VectorId) -> Result<bool, VectorError> {
        self.delete_with_timestamp(id, 0)
    }

    fn delete_with_timestamp(
        &mut self,
        id: VectorId,
        deleted_at: u64,
    ) -> Result<bool, VectorError> {
        let existed = self.heap.delete(id);
        if existed {
            // Mark deleted in active buffer or sealed segments
            if !self.active.soft_delete(id, deleted_at) {
                for seg in &mut self.sealed {
                    if seg.graph.contains(id) {
                        if seg.graph.delete_with_timestamp(id, deleted_at) {
                            seg.live_count = seg.live_count.saturating_sub(1);
                        }
                        break;
                    }
                }
            }
        }
        Ok(existed)
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<(VectorId, f32)> {
        if k == 0 || self.heap.is_empty() {
            return Vec::new();
        }
        if query.len() != self.heap.dimension() {
            return Vec::new();
        }

        let mut result_sets = Vec::with_capacity(1 + self.sealed.len());

        // Search active buffer (brute-force)
        let active_results = self.search_active(query, k);
        if !active_results.is_empty() {
            result_sets.push(active_results);
        }

        // Search sealed segments (parallel when there are enough segments)
        if self.sealed.len() >= PARALLEL_SEARCH_THRESHOLD {
            let sealed_results: Vec<Vec<(VectorId, f32)>> = SEARCH_POOL.install(|| {
                self.sealed
                    .par_iter()
                    .filter(|seg| seg.live_count > 0)
                    .map(|seg| seg.graph.search_with_heap(query, k, &self.heap))
                    .filter(|r| !r.is_empty())
                    .collect()
            });
            result_sets.extend(sealed_results);
        } else {
            for seg in &self.sealed {
                if seg.live_count > 0 {
                    let seg_results = seg.graph.search_with_heap(query, k, &self.heap);
                    if !seg_results.is_empty() {
                        result_sets.push(seg_results);
                    }
                }
            }
        }

        Self::merge_results(result_sets, k)
    }

    fn search_at(&self, query: &[f32], k: usize, as_of_ts: u64) -> Vec<(VectorId, f32)> {
        if k == 0 || self.heap.is_empty() {
            return Vec::new();
        }
        if query.len() != self.heap.dimension() {
            return Vec::new();
        }

        let mut result_sets = Vec::with_capacity(1 + self.sealed.len());

        // Active buffer: temporal brute-force
        let active_results = self.search_active_at(query, k, as_of_ts);
        if !active_results.is_empty() {
            result_sets.push(active_results);
        }

        // Sealed segments: delegate temporal search (parallel when enough segments)
        // Skip fully-deleted segments (live_count == 0) to avoid unnecessary work.
        if self.sealed.len() >= PARALLEL_SEARCH_THRESHOLD {
            let sealed_results: Vec<Vec<(VectorId, f32)>> = SEARCH_POOL.install(|| {
                self.sealed
                    .par_iter()
                    .filter(|seg| seg.live_count > 0)
                    .map(|seg| {
                        seg.graph
                            .search_at_with_heap(query, k, as_of_ts, &self.heap)
                    })
                    .filter(|r| !r.is_empty())
                    .collect()
            });
            result_sets.extend(sealed_results);
        } else {
            for seg in &self.sealed {
                if seg.live_count > 0 {
                    let seg_results = seg
                        .graph
                        .search_at_with_heap(query, k, as_of_ts, &self.heap);
                    if !seg_results.is_empty() {
                        result_sets.push(seg_results);
                    }
                }
            }
        }

        Self::merge_results(result_sets, k)
    }

    fn search_in_range(
        &self,
        query: &[f32],
        k: usize,
        start_ts: u64,
        end_ts: u64,
    ) -> Vec<(VectorId, f32)> {
        if k == 0 || self.heap.is_empty() {
            return Vec::new();
        }
        if query.len() != self.heap.dimension() {
            return Vec::new();
        }

        let mut result_sets = Vec::with_capacity(1 + self.sealed.len());

        let active_results = self.search_active_in_range(query, k, start_ts, end_ts);
        if !active_results.is_empty() {
            result_sets.push(active_results);
        }

        // Sealed segments: delegate range search (parallel when enough segments)
        // Skip fully-deleted segments (live_count == 0) to avoid unnecessary work.
        if self.sealed.len() >= PARALLEL_SEARCH_THRESHOLD {
            let sealed_results: Vec<Vec<(VectorId, f32)>> = SEARCH_POOL.install(|| {
                self.sealed
                    .par_iter()
                    .filter(|seg| seg.live_count > 0)
                    .map(|seg| {
                        seg.graph
                            .search_in_range_with_heap(query, k, start_ts, end_ts, &self.heap)
                    })
                    .filter(|r| !r.is_empty())
                    .collect()
            });
            result_sets.extend(sealed_results);
        } else {
            for seg in &self.sealed {
                if seg.live_count > 0 {
                    let seg_results = seg
                        .graph
                        .search_in_range_with_heap(query, k, start_ts, end_ts, &self.heap);
                    if !seg_results.is_empty() {
                        result_sets.push(seg_results);
                    }
                }
            }
        }

        Self::merge_results(result_sets, k)
    }

    fn len(&self) -> usize {
        self.heap.len()
    }

    fn dimension(&self) -> usize {
        self.heap.dimension()
    }

    fn metric(&self) -> DistanceMetric {
        self.heap.metric()
    }

    fn config(&self) -> VectorConfig {
        self.heap.config().clone()
    }

    fn get(&self, id: VectorId) -> Option<&[f32]> {
        self.heap.get(id)
    }

    fn contains(&self, id: VectorId) -> bool {
        self.heap.contains(id)
    }

    fn index_type_name(&self) -> &'static str {
        "segmented_hnsw"
    }

    fn memory_usage(&self) -> usize {
        // Global heap: anonymous memory only (mmap pages are OS-managed)
        let heap_bytes = self.heap.anon_data_bytes();
        let heap_overhead =
            self.heap.len() * (std::mem::size_of::<VectorId>() + std::mem::size_of::<usize>() + 64);
        let free_slots_bytes = std::mem::size_of_val(self.heap.free_slots());

        // Active buffer
        let active_bytes = self.active.ids.capacity() * std::mem::size_of::<VectorId>()
            + self.active.timestamps.len() * (std::mem::size_of::<VectorId>() + 16 + 64);

        // Sealed segments (graph-only — no embedding duplication)
        let sealed_bytes: usize = self.sealed.iter().map(|seg| seg.graph.memory_usage()).sum();

        heap_bytes + heap_overhead + free_slots_bytes + active_bytes + sealed_bytes
    }

    fn rebuild_index(&mut self) {
        // Apply pending timestamps to active buffer entries
        for (&id, &ts) in &self.pending_timestamps {
            if let Some(entry) = self.active.timestamps.get_mut(&id) {
                entry.0 = ts;
            }
        }
        self.pending_timestamps.clear();

        // Clear any existing sealed segments (recovery rebuilds from scratch)
        self.sealed.clear();
        self.next_segment_id = 0;

        // Drain all entries from active buffer
        let (all_ids, all_timestamps) = self.active.drain_sorted();

        // Filter to only IDs that have live embeddings in the global heap.
        // During recovery, replay_delete removes embeddings from the heap,
        // so deleted vectors must be excluded before chunking to get accurate
        // segment sizes.
        let live_ids: Vec<VectorId> = all_ids
            .into_iter()
            .filter(|id| self.heap.contains(*id))
            .collect();

        if live_ids.len() >= self.config.seal_threshold {
            let chunks: Vec<&[VectorId]> = live_ids.chunks(self.config.seal_threshold).collect();

            for chunk in chunks {
                // Seal all chunks into HNSW segments (including partial last chunk).
                // Even small chunks benefit from O(log n) HNSW search vs O(n) brute-force.
                {
                    // Build sealed segment (graph-only, no embedding duplication)
                    let mut graph = HnswGraph::new(&self.vector_config, self.config.hnsw.clone());
                    let mut live_count = 0;

                    for &id in chunk {
                        if let Some(embedding) = self.heap.get(id) {
                            let embedding = embedding.to_vec();
                            let created_at = all_timestamps.get(&id).map(|t| t.0).unwrap_or(0);
                            graph.insert_into_graph(id, &embedding, created_at, &self.heap);
                            if let Some(&(_, Some(deleted_at))) = all_timestamps.get(&id) {
                                graph.delete_with_timestamp(id, deleted_at);
                            } else {
                                live_count += 1;
                            }
                        }
                    }

                    let segment_id = self.next_segment_id;
                    self.next_segment_id += 1;

                    self.sealed.push(SealedSegment {
                        segment_id,
                        graph: CompactHnswGraph::from_graph(&graph),
                        live_count,
                        source_branch: None,
                    });
                }
            }
        } else {
            // Below threshold: all live vectors stay in active buffer
            for &id in &live_ids {
                let ts = all_timestamps.get(&id).copied().unwrap_or((0, None));
                self.active.ids.push(id);
                self.active.timestamps.insert(id, ts);
            }
        }
    }

    fn seal_remaining_active(&mut self) {
        if self.active.is_empty() {
            return;
        }

        let (ids, timestamps) = self.active.drain_sorted();
        let live_ids: Vec<VectorId> = ids
            .into_iter()
            .filter(|id| self.heap.contains(*id))
            .collect();

        if live_ids.is_empty() {
            return;
        }

        let mut graph = HnswGraph::new(&self.vector_config, self.config.hnsw.clone());
        let mut live_count = 0;

        for &id in &live_ids {
            if let Some(embedding) = self.heap.get(id) {
                let embedding = embedding.to_vec();
                let created_at = timestamps.get(&id).map(|t| t.0).unwrap_or(0);
                graph.insert_into_graph(id, &embedding, created_at, &self.heap);
                if let Some(&(_, Some(deleted_at))) = timestamps.get(&id) {
                    graph.delete_with_timestamp(id, deleted_at);
                } else {
                    live_count += 1;
                }
            }
        }

        let segment_id = self.next_segment_id;
        self.next_segment_id += 1;

        self.sealed.push(SealedSegment {
            segment_id,
            graph: CompactHnswGraph::from_graph(&graph),
            live_count,
            source_branch: None,
        });
    }

    fn compact(&mut self) {
        // Seal any remaining active buffer first
        if !self.active.is_empty() {
            self.seal_active_buffer();
        }
        if self.sealed.len() <= 1 {
            return;
        }

        // Collect created_at timestamps from existing sealed segments
        // so temporal queries still work after compaction.
        let mut timestamps: BTreeMap<VectorId, u64> = BTreeMap::new();
        for seg in &self.sealed {
            for (id, node) in seg.graph.iter_nodes() {
                if node.deleted_at.is_none() && node.created_at != 0 {
                    timestamps.insert(id, node.created_at);
                }
            }
        }

        // Build a single monolithic HNSW graph from all live vectors
        let mut graph = HnswGraph::new(&self.vector_config, self.config.hnsw.clone());
        let mut live_count = 0;

        // Insert all live vectors from the global heap (sorted by VectorId for determinism)
        for id in self.heap.ids() {
            if let Some(emb) = self.heap.get(id) {
                let emb = emb.to_vec();
                let created_at = timestamps.get(&id).copied().unwrap_or(0);
                graph.insert_into_graph(id, &emb, created_at, &self.heap);
                live_count += 1;
            }
        }

        if live_count == 0 {
            // No live vectors — just clear segments, don't push an empty one
            self.sealed.clear();
            self.next_segment_id = 0;
            return;
        }

        // Replace all sealed segments with a single compacted segment
        self.sealed.clear();
        self.next_segment_id = 1;
        self.sealed.push(SealedSegment {
            segment_id: 0,
            graph: CompactHnswGraph::from_graph(&graph),
            live_count,
            source_branch: None,
        });
    }

    fn search_with_ef(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<(VectorId, f32)> {
        if k == 0 || self.heap.is_empty() {
            return Vec::new();
        }
        if query.len() != self.heap.dimension() {
            return Vec::new();
        }

        let mut result_sets = Vec::with_capacity(1 + self.sealed.len());

        // Search active buffer (brute-force — ef_search doesn't apply)
        let active_results = self.search_active(query, k);
        if !active_results.is_empty() {
            result_sets.push(active_results);
        }

        // Search sealed segments with custom ef_search
        if self.sealed.len() >= PARALLEL_SEARCH_THRESHOLD {
            let sealed_results: Vec<Vec<(VectorId, f32)>> = SEARCH_POOL.install(|| {
                self.sealed
                    .par_iter()
                    .filter(|seg| seg.live_count > 0)
                    .map(|seg| {
                        seg.graph
                            .search_with_heap_ef(query, k, ef_search, &self.heap)
                    })
                    .filter(|r| !r.is_empty())
                    .collect()
            });
            result_sets.extend(sealed_results);
        } else {
            for seg in &self.sealed {
                if seg.live_count > 0 {
                    let seg_results = seg
                        .graph
                        .search_with_heap_ef(query, k, ef_search, &self.heap);
                    if !seg_results.is_empty() {
                        result_sets.push(seg_results);
                    }
                }
            }
        }

        Self::merge_results(result_sets, k)
    }

    fn vector_ids(&self) -> Vec<VectorId> {
        self.heap.ids().collect()
    }

    fn snapshot_state(&self) -> (u64, Vec<usize>) {
        (self.heap.next_id_value(), self.heap.free_slots().to_vec())
    }

    fn restore_snapshot_state(&mut self, next_id: u64, free_slots: Vec<usize>) {
        self.heap.restore_snapshot_state(next_id, free_slots);
    }

    fn freeze_heap_to_disk(&self, path: &std::path::Path) -> Result<(), VectorError> {
        self.heap.freeze_to_disk(path)
    }

    fn flush_heap_to_disk_if_needed(
        &mut self,
        path: &std::path::Path,
    ) -> Result<bool, VectorError> {
        // Store the flush path for future periodic flushes
        self.flush_path = Some(path.to_path_buf());

        // Promote Mmap → Tiered if needed (first mutation after mmap-based recovery)
        self.heap.promote_to_tiered();

        let threshold = self.config.heap_flush_threshold;
        if threshold == 0 || self.heap.overlay_len() < threshold {
            return Ok(false);
        }

        self.heap.flush_overlay_to_disk(path)?;
        Ok(true)
    }

    fn replace_heap(&mut self, heap: crate::primitives::vector::VectorHeap) {
        self.heap = heap;
        // Promote Mmap → Tiered so that subsequent inserts go to the overlay
        // instead of panicking.
        self.heap.promote_to_tiered();
    }

    fn register_mmap_vector(&mut self, id: VectorId, created_at: u64) {
        self.active.insert(id, created_at);
        self.pending_timestamps.insert(id, created_at);
    }

    fn is_heap_mmap(&self) -> bool {
        self.heap.is_mmap()
    }

    fn set_inline_meta(&mut self, id: VectorId, meta: InlineMeta) {
        self.heap.set_inline_meta(id, meta);
    }

    fn get_inline_meta(&self, id: VectorId) -> Option<&InlineMeta> {
        self.heap.get_inline_meta(id)
    }

    fn remove_inline_meta(&mut self, id: VectorId) {
        self.heap.remove_inline_meta(id);
    }

    fn freeze_graphs_to_disk(&self, dir: &std::path::Path) -> Result<(), VectorError> {
        use crate::primitives::vector::mmap_graph;

        // Always write every segment — even previously mmap-backed ones may
        // have in-memory deletions (deleted_at updates) that must be persisted.
        for seg in &self.sealed {
            let path = dir.join(format!("seg_{}.hgr", seg.segment_id));
            mmap_graph::write_graph_file(&path, &seg.graph)?;
        }

        // Write a manifest recording the total vector count in the heap so
        // that load_graphs_from_disk() can detect staleness (e.g., vectors
        // deleted via WAL replay after graphs were frozen).
        let manifest_path = dir.join("segments.manifest");
        let heap_vector_count = self.heap.len() as u64;
        let mut manifest = Vec::with_capacity(8 + self.sealed.len() * 24);
        // First 8 bytes: heap vector count at freeze time (staleness check)
        manifest.extend_from_slice(&heap_vector_count.to_le_bytes());
        for seg in &self.sealed {
            manifest.extend_from_slice(&seg.segment_id.to_le_bytes());
            manifest.extend_from_slice(&(seg.live_count as u64).to_le_bytes());
            manifest.extend_from_slice(&0u64.to_le_bytes()); // reserved
        }
        if let Some(parent) = manifest_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| VectorError::Io(e.to_string()))?;
        }
        std::fs::write(&manifest_path, &manifest).map_err(|e| VectorError::Io(e.to_string()))?;

        Ok(())
    }

    fn load_graphs_from_disk(&mut self, dir: &std::path::Path) -> Result<bool, VectorError> {
        use crate::primitives::vector::mmap_graph;

        let manifest_path = dir.join("segments.manifest");
        if !manifest_path.exists() {
            return Ok(false);
        }

        let manifest_data =
            std::fs::read(&manifest_path).map_err(|e| VectorError::Io(e.to_string()))?;

        // Manifest format: [heap_vector_count: u64 LE (8 bytes)] + N * 24-byte entries
        if manifest_data.len() < 8 || (manifest_data.len() - 8) % 24 != 0 {
            tracing::warn!(
                target: "strata::vector",
                "Corrupt segment manifest, falling back to rebuild"
            );
            return Ok(false);
        }

        // Staleness check: if the heap vector count changed since the graphs
        // were frozen (e.g. vectors deleted via WAL replay), the graphs are
        // stale and must be rebuilt.
        let frozen_heap_count =
            u64::from_le_bytes(manifest_data[0..8].try_into().unwrap()) as usize;
        if frozen_heap_count != self.heap.len() {
            tracing::info!(
                target: "strata::vector",
                frozen = frozen_heap_count,
                current = self.heap.len(),
                "Graph mmap stale (heap size changed), rebuilding"
            );
            return Ok(false);
        }

        let segment_count = (manifest_data.len() - 8) / 24;
        let mut loaded_segments = Vec::with_capacity(segment_count);
        let mut max_segment_id = 0u64;

        for i in 0..segment_count {
            let offset = 8 + i * 24;
            let segment_id =
                u64::from_le_bytes(manifest_data[offset..offset + 8].try_into().unwrap());
            let live_count =
                u64::from_le_bytes(manifest_data[offset + 8..offset + 16].try_into().unwrap())
                    as usize;
            // offset+16..offset+24 reserved

            let graph_path = dir.join(format!("seg_{}.hgr", segment_id));
            if !graph_path.exists() {
                tracing::warn!(
                    target: "strata::vector",
                    segment_id,
                    "Missing graph file, falling back to rebuild"
                );
                return Ok(false);
            }

            match mmap_graph::open_graph_file(
                &graph_path,
                self.config.hnsw.clone(),
                self.vector_config.clone(),
            ) {
                Ok(graph) => {
                    loaded_segments.push(SealedSegment {
                        segment_id,
                        graph,
                        live_count,
                        source_branch: None,
                    });
                    if segment_id >= max_segment_id {
                        max_segment_id = segment_id + 1;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        target: "strata::vector",
                        segment_id,
                        error = %e,
                        "Failed to load graph from mmap, falling back to rebuild"
                    );
                    return Ok(false);
                }
            }
        }

        // Success: replace sealed segments and clear pending timestamps
        self.sealed = loaded_segments;
        self.next_segment_id = max_segment_id;
        self.pending_timestamps.clear();

        // Move any remaining active buffer entries that belong to loaded
        // segments out of the active buffer (they're already in sealed graphs).
        // On recovery, all vectors start in active buffer; after loading graphs,
        // only vectors NOT in any sealed segment should remain in active.
        let sealed_ids: std::collections::BTreeSet<VectorId> = self
            .sealed
            .iter()
            .flat_map(|seg| seg.graph.node_ids())
            .collect();
        self.active.ids.retain(|id| !sealed_ids.contains(id));
        self.active
            .timestamps
            .retain(|id, _| !sealed_ids.contains(id));

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_backend(dim: usize, metric: DistanceMetric) -> SegmentedHnswBackend {
        let config = VectorConfig::new(dim, metric).unwrap();
        SegmentedHnswBackend::new(&config, SegmentedHnswConfig::default())
    }

    fn make_backend_with_threshold(
        dim: usize,
        metric: DistanceMetric,
        seal_threshold: usize,
    ) -> SegmentedHnswBackend {
        let config = VectorConfig::new(dim, metric).unwrap();
        let seg_config = SegmentedHnswConfig {
            hnsw: HnswConfig::default(),
            seal_threshold,
            heap_flush_threshold: 0,            // Disable flushing in tests
            auto_compact_threshold: usize::MAX, // Disable auto-compact in tests
        };
        SegmentedHnswBackend::new(&config, seg_config)
    }

    #[test]
    fn test_basic_insert_search() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        backend.insert(VectorId::new(3), &[0.9, 0.1, 0.0]).unwrap();

        assert_eq!(backend.len(), 3);

        let results = backend.search(&[1.0, 0.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, VectorId::new(1));
        assert_eq!(results[1].0, VectorId::new(3));
    }

    #[test]
    fn test_delete() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        backend.insert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();

        let existed = backend.delete(VectorId::new(1)).unwrap();
        assert!(existed);
        assert_eq!(backend.len(), 2);

        let results = backend.search(&[1.0, 0.0, 0.0], 10);
        for (id, _) in &results {
            assert_ne!(*id, VectorId::new(1));
        }
    }

    #[test]
    fn test_seal_threshold() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 4);

        // Insert 4 vectors — should trigger seal
        for i in 1..=4 {
            backend
                .insert(VectorId::new(i), &[i as f32, 0.0, 0.0])
                .unwrap();
        }

        assert_eq!(backend.sealed.len(), 1);
        assert!(backend.active.is_empty());
        assert_eq!(backend.len(), 4);

        // Insert 2 more — active buffer only
        backend.insert(VectorId::new(5), &[5.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(6), &[6.0, 0.0, 0.0]).unwrap();

        assert_eq!(backend.sealed.len(), 1);
        assert_eq!(backend.active.len(), 2);
        assert_eq!(backend.len(), 6);

        // Search should find vectors across both active and sealed
        let results = backend.search(&[6.0, 0.0, 0.0], 6);
        assert_eq!(results.len(), 6);
    }

    #[test]
    fn test_search_across_segments_verifies_ranking() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        // Segment 1: vectors 1-3
        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.9, 0.1, 0.0]).unwrap();
        backend.insert(VectorId::new(3), &[0.0, 1.0, 0.0]).unwrap();
        assert_eq!(backend.sealed.len(), 1);

        // Segment 2: vectors 4-6
        backend
            .insert(VectorId::new(4), &[0.95, 0.05, 0.0])
            .unwrap();
        backend.insert(VectorId::new(5), &[0.0, 0.0, 1.0]).unwrap();
        backend.insert(VectorId::new(6), &[0.8, 0.2, 0.0]).unwrap();
        assert_eq!(backend.sealed.len(), 2);

        // Active buffer: vector 7
        backend
            .insert(VectorId::new(7), &[0.99, 0.01, 0.0])
            .unwrap();

        // Query [1,0,0]: exact match is id=1, closest is id=7 (0.99), then id=4 (0.95)
        let results = backend.search(&[1.0, 0.0, 0.0], 4);
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, VectorId::new(1)); // exact match (score ≈ 1.0)
        assert_eq!(results[1].0, VectorId::new(7)); // 0.99 cosine
        assert_eq!(results[2].0, VectorId::new(4)); // 0.95 cosine
        assert_eq!(results[3].0, VectorId::new(2)); // 0.9 cosine

        // Verify scores are descending
        for i in 0..results.len() - 1 {
            assert!(
                results[i].1 >= results[i + 1].1,
                "Scores not descending: {} >= {}",
                results[i].1,
                results[i + 1].1
            );
        }
    }

    #[test]
    fn test_update_vector_in_active_buffer() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();

        // Update: change direction completely
        backend
            .insert_with_timestamp(VectorId::new(1), &[0.0, 1.0, 0.0], 100)
            .unwrap();

        assert_eq!(backend.len(), 1);

        // Search should find the UPDATED embedding, not the old one
        let results = backend.search(&[0.0, 1.0, 0.0], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, VectorId::new(1));
        assert!((results[0].1 - 1.0).abs() < 1e-6); // perfect match

        // Old direction should have low similarity
        let results = backend.search(&[1.0, 0.0, 0.0], 1);
        assert_eq!(results[0].0, VectorId::new(1));
        assert!(results[0].1.abs() < 1e-6); // orthogonal now
    }

    #[test]
    fn test_update_vector_across_seal_boundary() {
        // Critical edge case: vector sealed into segment, then updated
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        // Insert 3 vectors, triggering seal
        backend
            .insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 10)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 20)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(3), &[0.0, 0.0, 1.0], 30)
            .unwrap();
        assert_eq!(backend.sealed.len(), 1);
        assert!(backend.active.is_empty());

        // Update id=1 which is now in a sealed segment: change from [1,0,0] to [0,0,1]
        backend
            .insert_with_timestamp(VectorId::new(1), &[0.0, 0.0, 1.0], 40)
            .unwrap();

        assert_eq!(backend.len(), 3); // still 3 vectors, one updated

        // Search for [0,0,1]: should find id=1 (updated) and id=3 (original)
        let results = backend.search(&[0.0, 0.0, 1.0], 3);
        assert_eq!(results.len(), 3);

        // The top two should be id=1 and id=3 (both have [0,0,1])
        let top_ids: Vec<VectorId> = results.iter().take(2).map(|r| r.0).collect();
        assert!(top_ids.contains(&VectorId::new(1)));
        assert!(top_ids.contains(&VectorId::new(3)));
        // Both should have score ≈ 1.0
        assert!((results[0].1 - 1.0).abs() < 1e-6);
        assert!((results[1].1 - 1.0).abs() < 1e-6);

        // Search for [1,0,0]: no vector matches well anymore
        // (id=1 was the only [1,0,0] and is now [0,0,1]; all vectors are orthogonal)
        let results = backend.search(&[1.0, 0.0, 0.0], 1);
        assert!(
            results[0].1.abs() < 1e-6,
            "Expected ~0 score, got {}",
            results[0].1
        );
    }

    #[test]
    fn test_rebuild_index_verifies_search_quality() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 4);

        // Recovery path: insert_with_id (no graph building)
        // Use orthogonal-ish vectors so search ranking is deterministic
        let vectors: Vec<(u64, [f32; 3])> = vec![
            (1, [1.0, 0.0, 0.0]),
            (2, [0.9, 0.1, 0.0]),
            (3, [0.0, 1.0, 0.0]),
            (4, [0.0, 0.0, 1.0]),
            (5, [0.8, 0.2, 0.0]),
            (6, [0.7, 0.3, 0.0]),
            (7, [0.1, 0.9, 0.0]),
            (8, [0.0, 0.1, 0.9]),
            (9, [0.5, 0.5, 0.0]),
            (10, [0.95, 0.05, 0.0]),
        ];
        for (id, emb) in &vectors {
            backend.insert_with_id(VectorId::new(*id), emb).unwrap();
        }

        assert_eq!(backend.len(), 10);
        assert_eq!(backend.sealed.len(), 0);

        // Rebuild
        backend.rebuild_index();

        // All chunks sealed (including partial last chunk)
        assert_eq!(backend.sealed.len(), 3); // 10 / 4 = 2 full + 1 partial
        assert_eq!(backend.active.len(), 0); // no remainder in active

        // Verify search returns the correct top result
        let results = backend.search(&[1.0, 0.0, 0.0], 3);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, VectorId::new(1)); // exact match
                                                    // Second should be id=10 (0.95) or id=2 (0.9)
        assert!(
            results[1].0 == VectorId::new(10) || results[1].0 == VectorId::new(2),
            "Expected id 10 or 2, got {:?}",
            results[1].0
        );
    }

    #[test]
    fn test_rebuild_index_with_deletions() {
        // Simulates WAL replay: insert 8 vectors, delete 3, rebuild
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 4);

        for i in 1..=8 {
            backend
                .insert_with_id_and_timestamp(VectorId::new(i), &[i as f32, 0.0, 0.0], i * 10)
                .unwrap();
        }

        // Simulate WAL replay deletions
        backend
            .delete_with_timestamp(VectorId::new(2), 100)
            .unwrap();
        backend
            .delete_with_timestamp(VectorId::new(5), 100)
            .unwrap();
        backend
            .delete_with_timestamp(VectorId::new(7), 100)
            .unwrap();

        assert_eq!(backend.len(), 5); // 8 - 3

        // Rebuild
        backend.rebuild_index();

        // 5 live vectors, threshold=4: all sealed (1 full + 1 partial)
        assert_eq!(backend.sealed.len(), 2);
        assert_eq!(backend.active.len(), 0);

        // Search should find only live vectors
        let results = backend.search(&[8.0, 0.0, 0.0], 10);
        assert_eq!(results.len(), 5);
        let ids: Vec<u64> = results.iter().map(|r| r.0.as_u64()).collect();
        assert!(!ids.contains(&2));
        assert!(!ids.contains(&5));
        assert!(!ids.contains(&7));
    }

    #[test]
    fn test_rebuild_index_with_timestamps_preserved() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        // Recovery path with timestamps
        backend
            .insert_with_id_and_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 100)
            .unwrap();
        backend
            .insert_with_id_and_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 200)
            .unwrap();
        backend
            .insert_with_id_and_timestamp(VectorId::new(3), &[0.0, 0.0, 1.0], 300)
            .unwrap();

        // Rebuild builds HNSW segments with timestamps
        backend.rebuild_index();
        assert_eq!(backend.sealed.len(), 1);

        // Temporal search: as of t=150, only vector 1 should be visible
        let results = backend.search_at(&[1.0, 0.0, 0.0], 10, 150);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, VectorId::new(1));

        // As of t=250: vectors 1 and 2
        let results = backend.search_at(&[1.0, 0.0, 0.0], 10, 250);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_snapshot_state_roundtrip() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        // Use allocate_id + insert to advance the ID counter
        let id1 = backend.allocate_id();
        backend.insert(id1, &[1.0, 0.0, 0.0]).unwrap();
        let id2 = backend.allocate_id();
        backend.insert(id2, &[0.0, 1.0, 0.0]).unwrap();

        let (next_id, free_slots) = backend.snapshot_state();
        assert!(next_id >= 3);
        assert!(free_slots.is_empty());

        // Delete one vector
        backend.delete(id1).unwrap();
        let (next_id2, free_slots2) = backend.snapshot_state();
        assert_eq!(next_id2, next_id); // next_id doesn't decrease
        assert_eq!(free_slots2.len(), 1); // one free slot

        // Restore into a new backend
        let mut backend2 = make_backend(3, DistanceMetric::Cosine);
        backend2.insert_with_id(id2, &[0.0, 1.0, 0.0]).unwrap();
        backend2.restore_snapshot_state(next_id2, free_slots2);

        let (restored_next, restored_free) = backend2.snapshot_state();
        assert_eq!(restored_next, next_id2);
        assert_eq!(restored_free.len(), 1);
    }

    #[test]
    fn test_empty_search() {
        let backend = make_backend(3, DistanceMetric::Cosine);
        let results = backend.search(&[1.0, 0.0, 0.0], 5);
        assert!(results.is_empty());
    }

    #[test]
    fn test_dimension_mismatch() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();

        // Wrong dimension query
        let results = backend.search(&[1.0, 0.0], 5);
        assert!(results.is_empty());
    }

    #[test]
    fn test_index_type_name() {
        let backend = make_backend(3, DistanceMetric::Cosine);
        assert_eq!(backend.index_type_name(), "segmented_hnsw");
    }

    #[test]
    fn test_delete_from_sealed_segment() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        // Insert enough to seal
        backend
            .insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 10)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 20)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(3), &[0.0, 0.0, 1.0], 30)
            .unwrap();

        assert_eq!(backend.sealed.len(), 1);
        assert_eq!(backend.sealed[0].live_count, 3);

        // Delete from sealed segment
        let existed = backend.delete_with_timestamp(VectorId::new(2), 40).unwrap();
        assert!(existed);
        assert_eq!(backend.len(), 2);
        assert_eq!(backend.sealed[0].live_count, 2);

        // Should not appear in search
        let results = backend.search(&[0.0, 1.0, 0.0], 10);
        for (id, _) in &results {
            assert_ne!(*id, VectorId::new(2));
        }
    }

    #[test]
    fn test_delete_nonexistent_vector() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();

        // Delete a vector that doesn't exist
        let existed = backend.delete(VectorId::new(999)).unwrap();
        assert!(!existed);
        assert_eq!(backend.len(), 1);

        // Original vector should still be searchable
        let results = backend.search(&[1.0, 0.0, 0.0], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, VectorId::new(1));
    }

    #[test]
    fn test_delete_all_vectors_search_returns_empty() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        // Seal some vectors
        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        backend.insert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
        assert_eq!(backend.sealed.len(), 1);

        // Add one to active buffer
        backend.insert(VectorId::new(4), &[0.5, 0.5, 0.0]).unwrap();

        // Delete everything
        backend.delete(VectorId::new(1)).unwrap();
        backend.delete(VectorId::new(2)).unwrap();
        backend.delete(VectorId::new(3)).unwrap();
        backend.delete(VectorId::new(4)).unwrap();

        assert_eq!(backend.len(), 0);
        let results = backend.search(&[1.0, 0.0, 0.0], 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_delete_then_reinsert_same_key_different_embedding() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        // Seal a segment containing id=1
        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        backend.insert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
        assert_eq!(backend.sealed.len(), 1);

        // Delete id=1 from the sealed segment
        backend.delete(VectorId::new(1)).unwrap();
        assert_eq!(backend.len(), 2);

        // Re-insert id=1 with a completely different embedding
        // (This goes through insert_with_timestamp with is_update=false since heap.contains is false)
        backend
            .insert_with_timestamp(VectorId::new(1), &[0.0, 1.0, 0.0], 50)
            .unwrap();
        assert_eq!(backend.len(), 3);

        // Search for [0,1,0]: should find id=1 (reinserted) and id=2
        let results = backend.search(&[0.0, 1.0, 0.0], 2);
        assert_eq!(results.len(), 2);
        let ids: Vec<VectorId> = results.iter().map(|r| r.0).collect();
        assert!(ids.contains(&VectorId::new(1)));
        assert!(ids.contains(&VectorId::new(2)));
        // Both should have high similarity
        assert!((results[0].1 - 1.0).abs() < 1e-6);
        assert!((results[1].1 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_soft_delete_does_not_inflate_active_buffer_len() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 4);

        // Insert 3 vectors
        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        backend.insert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
        assert_eq!(backend.active.len(), 3);

        // Delete one: active buffer len should decrease
        backend.delete(VectorId::new(2)).unwrap();
        assert_eq!(backend.active.len(), 2); // not 3

        // Insert one more (4th) should NOT trigger seal (threshold=4, len=3)
        backend.insert(VectorId::new(4), &[0.5, 0.5, 0.0]).unwrap();
        assert_eq!(backend.active.len(), 3);
        assert_eq!(backend.sealed.len(), 0); // no premature seal
    }

    #[test]
    fn test_temporal_search_in_active_buffer() {
        let mut backend = make_backend(3, DistanceMetric::Cosine);

        backend
            .insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 10)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 20)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(3), &[0.0, 0.0, 1.0], 30)
            .unwrap();

        // Search as of timestamp 15: only vector 1 should be visible
        let results = backend.search_at(&[1.0, 0.0, 0.0], 10, 15);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, VectorId::new(1));

        // Search in range [15, 25]: only vector 2
        let results = backend.search_in_range(&[0.0, 1.0, 0.0], 10, 15, 25);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, VectorId::new(2));
    }

    #[test]
    fn test_temporal_search_across_sealed_segments() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        // Segment 1: timestamps 10, 20, 30
        backend
            .insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 10)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 20)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(3), &[0.0, 0.0, 1.0], 30)
            .unwrap();
        assert_eq!(backend.sealed.len(), 1);

        // Segment 2: timestamps 40, 50, 60
        backend
            .insert_with_timestamp(VectorId::new(4), &[0.9, 0.1, 0.0], 40)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(5), &[0.1, 0.9, 0.0], 50)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(6), &[0.5, 0.5, 0.0], 60)
            .unwrap();
        assert_eq!(backend.sealed.len(), 2);

        // Active buffer: timestamp 70
        backend
            .insert_with_timestamp(VectorId::new(7), &[0.8, 0.2, 0.0], 70)
            .unwrap();

        // search_at(t=35): should see vectors 1,2,3 (sealed seg 1) but not 4,5,6,7
        let results = backend.search_at(&[1.0, 0.0, 0.0], 10, 35);
        assert_eq!(results.len(), 3);
        let ids: Vec<u64> = results.iter().map(|r| r.0.as_u64()).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));

        // search_at(t=55): should see vectors 1-5 but not 6,7
        let results = backend.search_at(&[1.0, 0.0, 0.0], 10, 55);
        assert_eq!(results.len(), 5);

        // search_in_range(35, 55): should see vectors 4,5 only
        let results = backend.search_in_range(&[1.0, 0.0, 0.0], 10, 35, 55);
        assert_eq!(results.len(), 2);
        let ids: Vec<u64> = results.iter().map(|r| r.0.as_u64()).collect();
        assert!(ids.contains(&4));
        assert!(ids.contains(&5));
    }

    #[test]
    fn test_deterministic_results() {
        // Same operations on two backends must produce identical search results
        let make = || {
            let mut b = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);
            b.insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 10)
                .unwrap();
            b.insert_with_timestamp(VectorId::new(2), &[0.9, 0.1, 0.0], 20)
                .unwrap();
            b.insert_with_timestamp(VectorId::new(3), &[0.0, 1.0, 0.0], 30)
                .unwrap(); // seals
            b.insert_with_timestamp(VectorId::new(4), &[0.5, 0.5, 0.0], 40)
                .unwrap();
            b.delete_with_timestamp(VectorId::new(2), 50).unwrap();
            b
        };

        let b1 = make();
        let b2 = make();

        let r1 = b1.search(&[1.0, 0.0, 0.0], 10);
        let r2 = b2.search(&[1.0, 0.0, 0.0], 10);

        assert_eq!(r1.len(), r2.len());
        for (a, b) in r1.iter().zip(r2.iter()) {
            assert_eq!(a.0, b.0, "VectorIds differ");
            assert!(
                (a.1 - b.1).abs() < 1e-10,
                "Scores differ: {} vs {}",
                a.1,
                b.1
            );
        }
    }

    #[test]
    fn test_multiple_seals_search_correctness() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 2);

        // Create 4 sealed segments (8 vectors / 2 per segment)
        let embeddings: Vec<[f32; 3]> = vec![
            [1.0, 0.0, 0.0],
            [0.9, 0.1, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.9, 0.1],
            [0.0, 0.0, 1.0],
            [0.1, 0.0, 0.9],
            [0.5, 0.5, 0.0],
            [0.3, 0.3, 0.4],
        ];
        for (i, emb) in embeddings.iter().enumerate() {
            backend.insert(VectorId::new((i + 1) as u64), emb).unwrap();
        }
        assert_eq!(backend.sealed.len(), 4);
        assert!(backend.active.is_empty());

        // Add one to active
        backend
            .insert(VectorId::new(9), &[0.95, 0.05, 0.0])
            .unwrap();

        // Search across all 4 sealed segments + active
        let results = backend.search(&[1.0, 0.0, 0.0], 3);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, VectorId::new(1)); // exact match
                                                    // id=9 (0.95) and id=2 (0.9) should be in top 3
        let top3_ids: Vec<u64> = results.iter().map(|r| r.0.as_u64()).collect();
        assert!(top3_ids.contains(&9));
        assert!(top3_ids.contains(&2));
    }

    // ========================================================================
    // Phase 2: extract_segments / adopt_segments
    // ========================================================================

    #[test]
    fn test_extract_segments_drains_sealed() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 2);

        // Insert enough to seal one segment
        backend
            .insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 100)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 200)
            .unwrap();
        assert_eq!(backend.segment_count(), 1);
        assert!(backend.active.is_empty());

        // Extract segments
        let extracted = backend.extract_segments();
        assert_eq!(extracted.len(), 1);
        assert_eq!(backend.segment_count(), 0, "Sealed list should be drained");

        // Extracted segment should have 2 live vectors
        let (_seg_id, ref graph, live_count, ref source_branch) = extracted[0];
        assert_eq!(live_count, 2);
        assert!(source_branch.is_none());
        assert_eq!(graph.len(), 2);
    }

    #[test]
    fn test_adopt_segments_appends_with_new_ids() {
        let mut source = make_backend_with_threshold(3, DistanceMetric::Cosine, 2);
        let mut target = make_backend_with_threshold(3, DistanceMetric::Cosine, 2);

        // Build segments on source
        source
            .insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 100)
            .unwrap();
        source
            .insert_with_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 200)
            .unwrap();
        assert_eq!(source.segment_count(), 1);

        // Build a segment on target too
        target
            .insert_with_timestamp(VectorId::new(10), &[0.0, 0.0, 1.0], 300)
            .unwrap();
        target
            .insert_with_timestamp(VectorId::new(11), &[0.5, 0.5, 0.0], 400)
            .unwrap();
        assert_eq!(target.segment_count(), 1);

        // Extract from source and adopt into target
        let extracted = source.extract_segments();
        let to_adopt: Vec<_> = extracted
            .into_iter()
            .map(|(_, graph, live, branch)| (graph, live, branch))
            .collect();
        target.adopt_segments(to_adopt);

        // Target should now have 2 segments
        assert_eq!(target.segment_count(), 2);
    }

    #[test]
    fn test_segment_count_and_active_buffer_len() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        assert_eq!(backend.segment_count(), 0);
        assert_eq!(backend.active_buffer_len(), 0);

        // Insert 2 — still in active buffer
        backend.insert(VectorId::new(1), &[1.0, 0.0, 0.0]).unwrap();
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        assert_eq!(backend.segment_count(), 0);
        assert_eq!(backend.active_buffer_len(), 2);

        // Insert 3rd — seals
        backend.insert(VectorId::new(3), &[0.0, 0.0, 1.0]).unwrap();
        assert_eq!(backend.segment_count(), 1);
        assert_eq!(backend.active_buffer_len(), 0);
    }

    #[test]
    fn test_freeze_and_load_graphs_roundtrip() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let dir = temp_dir.path().join("graphs");

        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        // Insert enough vectors to seal a segment (insert_with_timestamp triggers seal)
        backend
            .insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 10)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 20)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(3), &[0.0, 0.0, 1.0], 30)
            .unwrap();
        assert_eq!(backend.segment_count(), 1);

        // Search before freeze
        let results_before = backend.search(&[1.0, 0.0, 0.0], 3);

        // Freeze graphs to disk
        backend.freeze_graphs_to_disk(&dir).unwrap();

        // Create a new backend that simulates recovery: use insert_with_id_and_timestamp
        // (recovery path) which adds to active buffer without sealing, then load graphs.
        let mut backend2 = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);
        backend2
            .insert_with_id_and_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 10)
            .unwrap();
        backend2
            .insert_with_id_and_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 20)
            .unwrap();
        backend2
            .insert_with_id_and_timestamp(VectorId::new(3), &[0.0, 0.0, 1.0], 30)
            .unwrap();
        assert_eq!(backend2.segment_count(), 0); // Not yet sealed
        assert_eq!(backend2.len(), 3); // All in active buffer

        // Load graphs from disk — should replace active buffer with sealed segments
        let loaded = backend2.load_graphs_from_disk(&dir).unwrap();
        assert!(loaded);
        assert_eq!(backend2.segment_count(), 1);
        assert_eq!(backend2.active_buffer_len(), 0); // Vectors moved to sealed

        // Search after load should match
        let results_after = backend2.search(&[1.0, 0.0, 0.0], 3);
        assert_eq!(results_before.len(), results_after.len());
        for (a, b) in results_before.iter().zip(results_after.iter()) {
            assert_eq!(a.0, b.0, "VectorId mismatch");
        }
    }

    #[test]
    fn test_load_graphs_staleness_detection() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let dir = temp_dir.path().join("graphs");

        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        // Insert and seal (use insert_with_timestamp to trigger sealing)
        backend
            .insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 10)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 20)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(3), &[0.0, 0.0, 1.0], 30)
            .unwrap();

        // Freeze with 3 vectors in heap
        backend.freeze_graphs_to_disk(&dir).unwrap();

        // Create a new backend with DIFFERENT heap size (simulate a vector
        // being deleted via WAL replay before graph load)
        let mut backend2 = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);
        backend2
            .insert_with_id_and_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 10)
            .unwrap();
        backend2
            .insert_with_id_and_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 20)
            .unwrap();
        // Only 2 vectors in heap (not 3) — stale!

        let loaded = backend2.load_graphs_from_disk(&dir).unwrap();
        assert!(!loaded, "Should detect stale graphs and return false");
    }

    #[test]
    fn test_load_graphs_missing_manifest() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let dir = temp_dir.path().join("empty_dir");
        std::fs::create_dir_all(&dir).unwrap();

        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);
        let loaded = backend.load_graphs_from_disk(&dir).unwrap();
        assert!(!loaded, "Should return false when manifest is missing");
    }

    #[test]
    fn test_load_graphs_corrupt_manifest() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let dir = temp_dir.path().join("corrupt");
        std::fs::create_dir_all(&dir).unwrap();

        // Write a manifest with an invalid size (not 8 + N*24)
        let manifest_path = dir.join("segments.manifest");
        std::fs::write(&manifest_path, &[0u8; 13]).unwrap(); // 13 bytes — invalid

        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);
        let loaded = backend.load_graphs_from_disk(&dir).unwrap();
        assert!(!loaded, "Should return false on corrupt manifest");
    }

    // ====================================================================
    // flush_heap_if_needed / Tiered integration tests
    // ====================================================================

    #[test]
    fn test_flush_heap_if_needed_triggers_at_threshold() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");

        // Create backend with low flush threshold for testing
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let seg_config = SegmentedHnswConfig {
            hnsw: HnswConfig::default(),
            seal_threshold: 100,     // high seal threshold to avoid sealing
            heap_flush_threshold: 5, // flush after 5 overlay vectors
            auto_compact_threshold: usize::MAX,
        };
        let mut backend = SegmentedHnswBackend::new(&config, seg_config);

        // Write an initial mmap file and set up the flush path
        backend
            .heap
            .upsert(VectorId::new(100), &[1.0, 0.0, 0.0])
            .unwrap();
        backend.heap.freeze_to_disk(&vec_path).unwrap();

        // Reopen with mmap and configure
        let mmap_heap = VectorHeap::from_mmap(&vec_path, config.clone()).unwrap();
        backend.replace_heap(mmap_heap);
        backend.flush_path = Some(vec_path.clone());

        // Insert 4 vectors (below threshold)
        for i in 1..=4 {
            backend
                .heap
                .upsert(VectorId::new(i), &[i as f32, 0.0, 0.0])
                .unwrap();
        }
        backend.flush_heap_if_needed();
        assert!(
            backend.heap.overlay_len() > 0,
            "Should not flush below threshold"
        );

        // Insert 1 more (reaches threshold)
        backend
            .heap
            .upsert(VectorId::new(5), &[5.0, 0.0, 0.0])
            .unwrap();
        assert_eq!(backend.heap.overlay_len(), 5);
        backend.flush_heap_if_needed();
        assert_eq!(backend.heap.overlay_len(), 0, "Should flush at threshold");

        // All vectors should still be accessible
        assert_eq!(backend.heap.len(), 6); // 100 + 1..5
        assert!(backend.heap.get(VectorId::new(100)).is_some());
        assert!(backend.heap.get(VectorId::new(3)).is_some());
    }

    #[test]
    fn test_flush_heap_disabled_when_threshold_zero() {
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let seg_config = SegmentedHnswConfig {
            hnsw: HnswConfig::default(),
            seal_threshold: 100,
            heap_flush_threshold: 0, // disabled
            auto_compact_threshold: usize::MAX,
        };
        let mut backend = SegmentedHnswBackend::new(&config, seg_config);
        backend.flush_path = Some(std::path::PathBuf::from("/tmp/dummy.vec"));

        // Even with many overlay vectors, flush should not trigger
        for i in 1..=100 {
            backend
                .heap
                .upsert(VectorId::new(i), &[i as f32, 0.0, 0.0])
                .unwrap();
        }
        // No panic, no effect
        backend.flush_heap_if_needed();
    }

    #[test]
    fn test_flush_heap_no_path_configured() {
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let seg_config = SegmentedHnswConfig {
            hnsw: HnswConfig::default(),
            seal_threshold: 100,
            heap_flush_threshold: 1,
            auto_compact_threshold: usize::MAX,
        };
        let mut backend = SegmentedHnswBackend::new(&config, seg_config);
        // flush_path is None (in-memory database)

        backend
            .heap
            .upsert(VectorId::new(1), &[1.0, 0.0, 0.0])
            .unwrap();
        // Should not panic even though overlay exceeds threshold
        backend.flush_heap_if_needed();
    }

    #[test]
    fn test_vectors_searchable_after_mid_indexing_flush() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let seg_config = SegmentedHnswConfig {
            hnsw: HnswConfig::default(),
            seal_threshold: 3,
            heap_flush_threshold: 5,
            auto_compact_threshold: usize::MAX,
        };
        let mut backend = SegmentedHnswBackend::new(&config, seg_config);

        // Insert vectors that will trigger seal + flush
        // seal_threshold=3: seal after 3 inserts
        // heap_flush_threshold=5: flush after 5 overlay vectors
        for i in 1..=6 {
            backend
                .insert_with_timestamp(VectorId::new(i), &[i as f32, 0.0, 0.0], i as u64)
                .unwrap();
        }

        // Manually set up flush path and trigger flush
        let _ = backend.flush_heap_to_disk_if_needed(&vec_path);

        // Insert more after flush
        for i in 7..=9 {
            backend
                .insert_with_timestamp(VectorId::new(i), &[i as f32, 0.0, 0.0], i as u64)
                .unwrap();
        }

        // All vectors should be searchable
        assert_eq!(backend.len(), 9);
        let results = backend.search(&[5.0, 0.0, 0.0], 9);
        assert_eq!(results.len(), 9);
        // All [i, 0, 0] vectors point in the same direction → cosine=1.0.
        // Just verify we got all 9 back.
        let mut ids: Vec<u64> = results.iter().map(|(id, _)| id.as_u64()).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_replace_heap_promotes_to_tiered() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let vec_path = temp_dir.path().join("test.vec");

        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 100);

        // Create an mmap file
        backend
            .heap
            .upsert(VectorId::new(1), &[1.0, 0.0, 0.0])
            .unwrap();
        backend.heap.freeze_to_disk(&vec_path).unwrap();

        // Load mmap and replace heap
        let mmap_heap = VectorHeap::from_mmap(&vec_path, config).unwrap();
        backend.replace_heap(mmap_heap);

        // Should be promoted to Tiered (mmap flag true)
        assert!(backend.is_heap_mmap());

        // New inserts should go to overlay without panicking
        backend.insert(VectorId::new(2), &[0.0, 1.0, 0.0]).unwrap();
        assert_eq!(backend.len(), 2);
        assert!(backend.get(VectorId::new(1)).is_some());
        assert!(backend.get(VectorId::new(2)).is_some());
    }

    // ====================================================================
    // compact() tests
    // ====================================================================

    #[test]
    fn test_compact_merges_segments() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::DotProduct, 3);

        // Create 3 sealed segments (use DotProduct so higher magnitude = higher score)
        for i in 1..=9 {
            backend
                .insert(VectorId::new(i), &[i as f32, 0.0, 0.0])
                .unwrap();
        }
        assert_eq!(backend.segment_count(), 3);

        // Compact into a single segment
        backend.compact();
        assert_eq!(backend.segment_count(), 1);
        assert_eq!(backend.len(), 9);

        // Search should still work correctly
        let results = backend.search(&[1.0, 0.0, 0.0], 3);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, VectorId::new(9)); // highest dot product
    }

    #[test]
    fn test_compact_with_active_buffer() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        // Create 1 sealed segment + active buffer
        for i in 1..=5 {
            backend
                .insert(VectorId::new(i), &[i as f32, 0.0, 0.0])
                .unwrap();
        }
        assert_eq!(backend.segment_count(), 1);
        assert_eq!(backend.active_buffer_len(), 2);

        // Compact should seal active first, then merge all
        backend.compact();
        assert_eq!(backend.segment_count(), 1);
        assert!(backend.active.is_empty());
        assert_eq!(backend.len(), 5);
    }

    #[test]
    fn test_compact_single_segment_noop() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 10);

        for i in 1..=10 {
            backend
                .insert(VectorId::new(i), &[i as f32, 0.0, 0.0])
                .unwrap();
        }
        assert_eq!(backend.segment_count(), 1);

        // Compact with single segment is a no-op
        backend.compact();
        assert_eq!(backend.segment_count(), 1);
    }

    #[test]
    fn test_compact_preserves_timestamps() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        // Insert with timestamps
        backend
            .insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 100)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &[0.0, 1.0, 0.0], 200)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(3), &[0.0, 0.0, 1.0], 300)
            .unwrap();
        assert_eq!(backend.segment_count(), 1);

        // Create second segment
        backend
            .insert_with_timestamp(VectorId::new(4), &[1.0, 1.0, 0.0], 400)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(5), &[0.0, 1.0, 1.0], 500)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(6), &[1.0, 0.0, 1.0], 600)
            .unwrap();
        assert_eq!(backend.segment_count(), 2);

        // Compact
        backend.compact();
        assert_eq!(backend.segment_count(), 1);

        // Temporal search at ts=250: should find id=1 and id=2 (created at 100 and 200)
        let results = backend.search_at(&[1.0, 0.0, 0.0], 10, 250);
        let ids: Vec<u64> = results.iter().map(|(id, _)| id.as_u64()).collect();
        assert!(
            ids.contains(&1),
            "id=1 (created_at=100) should be visible at ts=250"
        );
        assert!(
            ids.contains(&2),
            "id=2 (created_at=200) should be visible at ts=250"
        );
        assert!(
            !ids.contains(&4),
            "id=4 (created_at=400) should NOT be visible at ts=250"
        );
    }

    #[test]
    fn test_compact_empty_after_deletes() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 3);

        for i in 1..=6 {
            backend
                .insert(VectorId::new(i), &[i as f32, 0.0, 0.0])
                .unwrap();
        }
        assert_eq!(backend.segment_count(), 2);

        // Delete all vectors
        for i in 1..=6 {
            backend.delete(VectorId::new(i)).unwrap();
        }

        // Compact with no live vectors should not push an empty segment
        backend.compact();
        assert_eq!(backend.segment_count(), 0);
        assert_eq!(backend.len(), 0);
    }

    // ====================================================================
    // search_with_ef() tests
    // ====================================================================

    #[test]
    fn test_search_with_ef_returns_results() {
        let mut backend = make_backend_with_threshold(32, DistanceMetric::Cosine, 50);

        // Insert enough vectors to have sealed segments
        for i in 1..=100 {
            let emb: Vec<f32> = (0..32)
                .map(|j| ((i * 32 + j) as f32 / 1000.0).sin())
                .collect();
            backend.insert(VectorId::new(i as u64), &emb).unwrap();
        }

        let query: Vec<f32> = (0..32).map(|i| (i as f32 / 100.0).cos()).collect();

        // Higher ef should give at least as good results
        let results_low = backend.search_with_ef(&query, 10, 50);
        let results_high = backend.search_with_ef(&query, 10, 200);

        assert_eq!(results_low.len(), 10);
        assert_eq!(results_high.len(), 10);

        // High ef result should have score >= low ef (or very close)
        // The best result from high ef should be at least as good
        assert!(
            results_high[0].1 >= results_low[0].1 - 1e-6,
            "Higher ef should find at least as good results"
        );
    }

    // ========================================================================
    // v7 optimization-specific tests
    // ========================================================================

    #[test]
    fn test_active_buffer_tie_breaking() {
        // Regression test: top-k heap must break ties by VectorId ascending.
        // All vectors have the same embedding → identical similarity scores.
        // The top-k selection must return the lowest VectorIds.
        let mut backend = make_backend_with_threshold(3, DistanceMetric::DotProduct, 100);

        let embedding = [1.0, 0.0, 0.0];
        // Insert in non-sorted order
        backend
            .insert_with_timestamp(VectorId::new(5), &embedding, 1)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &embedding, 2)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(8), &embedding, 3)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(1), &embedding, 4)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(7), &embedding, 5)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(3), &embedding, 6)
            .unwrap();

        // All 6 vectors have the same score. Ask for k=3 → should get {1, 2, 3}.
        let results = backend.search(&[1.0, 0.0, 0.0], 3);
        assert_eq!(results.len(), 3);
        let ids: Vec<u64> = results.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "Tie-breaking must prefer lowest VectorIds"
        );

        // Ask for all → should be sorted by VectorId ascending
        let results_all = backend.search(&[1.0, 0.0, 0.0], 10);
        let ids_all: Vec<u64> = results_all.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(ids_all, vec![1, 2, 3, 5, 7, 8]);
    }

    #[test]
    fn test_active_buffer_tie_breaking_temporal() {
        // Same as above but via search_at (temporal path uses BTreeMap iteration)
        let mut backend = make_backend_with_threshold(3, DistanceMetric::DotProduct, 100);

        let embedding = [1.0, 0.0, 0.0];
        backend
            .insert_with_timestamp(VectorId::new(5), &embedding, 10)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &embedding, 20)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(8), &embedding, 30)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(1), &embedding, 40)
            .unwrap();

        // search_at: all 4 exist at ts=50, ask for k=2
        let results = backend.search_at(&[1.0, 0.0, 0.0], 2, 50);
        assert_eq!(results.len(), 2);
        let ids: Vec<u64> = results.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(
            ids,
            vec![1, 2],
            "Temporal tie-breaking must prefer lowest VectorIds"
        );

        // search_in_range: created_at in [10, 30], so ids 5, 2, 8. Ask for k=2.
        let results_range = backend.search_in_range(&[1.0, 0.0, 0.0], 2, 10, 30);
        assert_eq!(results_range.len(), 2);
        let ids_range: Vec<u64> = results_range.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(ids_range, vec![2, 5]);
    }

    #[test]
    fn test_merge_results_kway() {
        // Directly test merge_results with multiple pre-sorted input sets
        let set1 = vec![
            (VectorId::new(1), 0.95),
            (VectorId::new(3), 0.80),
            (VectorId::new(5), 0.60),
        ];
        let set2 = vec![
            (VectorId::new(2), 0.90),
            (VectorId::new(4), 0.70),
            (VectorId::new(6), 0.50),
        ];
        let set3 = vec![(VectorId::new(7), 0.85), (VectorId::new(8), 0.40)];

        let merged = SegmentedHnswBackend::merge_results(vec![set1, set2, set3], 5);
        assert_eq!(merged.len(), 5);
        // Expected order: (1, 0.95), (2, 0.90), (7, 0.85), (3, 0.80), (4, 0.70)
        let ids: Vec<u64> = merged.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(ids, vec![1, 2, 7, 3, 4]);
    }

    #[test]
    fn test_merge_results_single_set_fast_path() {
        let set = vec![
            (VectorId::new(1), 0.95),
            (VectorId::new(2), 0.90),
            (VectorId::new(3), 0.80),
        ];
        let merged = SegmentedHnswBackend::merge_results(vec![set], 2);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].0, VectorId::new(1));
        assert_eq!(merged[1].0, VectorId::new(2));
    }

    #[test]
    fn test_merge_results_empty() {
        let merged = SegmentedHnswBackend::merge_results(vec![], 10);
        assert!(merged.is_empty());
    }

    #[test]
    fn test_merge_results_tie_breaking() {
        // Two sets with identical scores — merge must order by VectorId asc
        let set1 = vec![(VectorId::new(3), 0.80), (VectorId::new(5), 0.80)];
        let set2 = vec![(VectorId::new(1), 0.80), (VectorId::new(4), 0.80)];

        let merged = SegmentedHnswBackend::merge_results(vec![set1, set2], 4);
        let ids: Vec<u64> = merged.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(ids, vec![1, 3, 4, 5]);
    }

    #[test]
    fn test_auto_compact_reduces_segments() {
        // Use a very small seal_threshold and auto_compact_threshold
        // to verify that auto-compact fires and reduces segment count.
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let seg_config = SegmentedHnswConfig {
            hnsw: HnswConfig::default(),
            seal_threshold: 3,
            heap_flush_threshold: 0,
            auto_compact_threshold: 2, // compact after 3rd segment
        };
        let mut backend = SegmentedHnswBackend::new(&config, seg_config);

        // Insert 3 → seal to segment 1
        for i in 1..=3 {
            backend
                .insert_with_timestamp(VectorId::new(i), &[i as f32, 0.0, 0.0], i as u64)
                .unwrap();
        }
        assert_eq!(backend.segment_count(), 1);

        // Insert 3 more → seal to segment 2
        for i in 4..=6 {
            backend
                .insert_with_timestamp(VectorId::new(i), &[i as f32, 0.0, 0.0], i as u64)
                .unwrap();
        }
        assert_eq!(backend.segment_count(), 2);

        // Insert 3 more → seal to segment 3 → exceeds threshold of 2 → compact to 1
        for i in 7..=9 {
            backend
                .insert_with_timestamp(VectorId::new(i), &[i as f32, 0.0, 0.0], i as u64)
                .unwrap();
        }
        assert_eq!(
            backend.segment_count(),
            1,
            "Auto-compact should have merged 3 segments into 1"
        );

        // All 9 vectors should still be searchable
        let results = backend.search(&[5.0, 0.0, 0.0], 9);
        assert_eq!(results.len(), 9);

        // Verify temporal search still works after compaction
        let results_at = backend.search_at(&[5.0, 0.0, 0.0], 5, 6);
        // Only ids 1-6 existed at ts=6
        assert!(results_at.len() <= 6);
        for &(id, _score) in &results_at {
            assert!(id.as_u64() <= 6);
        }
    }

    #[test]
    fn test_auto_compact_disabled() {
        // Verify that usize::MAX threshold effectively disables auto-compact
        let config = VectorConfig::new(3, DistanceMetric::Cosine).unwrap();
        let seg_config = SegmentedHnswConfig {
            hnsw: HnswConfig::default(),
            seal_threshold: 3,
            heap_flush_threshold: 0,
            auto_compact_threshold: usize::MAX,
        };
        let mut backend = SegmentedHnswBackend::new(&config, seg_config);

        // Create 5 segments (15 vectors, seal every 3)
        for i in 1..=15 {
            backend
                .insert_with_timestamp(VectorId::new(i), &[i as f32, 0.0, 0.0], i as u64)
                .unwrap();
        }
        assert_eq!(
            backend.segment_count(),
            5,
            "With auto-compact disabled, all 5 segments should remain"
        );
    }

    #[test]
    fn test_deleted_vector_not_in_search() {
        // Verify that deleted vectors are excluded from search results
        let mut backend = make_backend_with_threshold(3, DistanceMetric::Cosine, 5);

        // Insert 5 vectors → seal
        for i in 1..=5 {
            backend
                .insert_with_timestamp(VectorId::new(i), &[i as f32, 0.0, 0.0], i as u64)
                .unwrap();
        }
        assert_eq!(backend.segment_count(), 1);

        // Delete vector 3 from the sealed segment
        backend
            .delete_with_timestamp(VectorId::new(3), 100)
            .unwrap();

        // Vector 3 should not appear in search results
        let results = backend.search(&[3.0, 0.0, 0.0], 5);
        for &(id, _score) in &results {
            assert_ne!(
                id.as_u64(),
                3,
                "Deleted vector should not appear in results"
            );
        }
    }

    #[test]
    fn test_active_buffer_varied_scores() {
        // Exercises the top-k heap eviction path with different scores.
        // DotProduct with [1,0,0] query: score = first component of embedding.
        let mut backend = make_backend_with_threshold(3, DistanceMetric::DotProduct, 100);

        // Insert 8 vectors with distinct first components → distinct scores
        let embeddings: Vec<(u64, f32)> = vec![
            (1, 0.3),
            (2, 0.9),
            (3, 0.1),
            (4, 0.7),
            (5, 0.5),
            (6, 0.8),
            (7, 0.2),
            (8, 0.6),
        ];
        for &(id, score_component) in &embeddings {
            backend
                .insert_with_timestamp(VectorId::new(id), &[score_component, 0.0, 0.0], id as u64)
                .unwrap();
        }

        // k=3: should return the 3 highest-scored vectors: ids 2(0.9), 6(0.8), 4(0.7)
        let results = backend.search(&[1.0, 0.0, 0.0], 3);
        assert_eq!(results.len(), 3);
        let ids: Vec<u64> = results.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(ids, vec![2, 6, 4], "Top-3 by score should be ids 2, 6, 4");

        // k=5: should return ids 2(0.9), 6(0.8), 4(0.7), 8(0.6), 5(0.5)
        let results5 = backend.search(&[1.0, 0.0, 0.0], 5);
        assert_eq!(results5.len(), 5);
        let ids5: Vec<u64> = results5.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(ids5, vec![2, 6, 4, 8, 5]);

        // Verify scores are monotonically decreasing
        for w in results5.windows(2) {
            assert!(
                w[0].1 >= w[1].1,
                "Scores must be non-increasing: {} >= {}",
                w[0].1,
                w[1].1
            );
        }
    }

    #[test]
    fn test_search_empty_active_buffer() {
        // Tests early return path when active buffer is empty
        let backend = make_backend_with_threshold(3, DistanceMetric::DotProduct, 100);

        let results = backend.search(&[1.0, 0.0, 0.0], 5);
        assert!(results.is_empty());

        let results_at = backend.search_at(&[1.0, 0.0, 0.0], 5, 100);
        assert!(results_at.is_empty());

        let results_range = backend.search_in_range(&[1.0, 0.0, 0.0], 5, 0, 100);
        assert!(results_range.is_empty());
    }

    #[test]
    fn test_deleted_vector_in_active_buffer() {
        // Tests that deleting a vector from the active buffer excludes it from search
        let mut backend = make_backend_with_threshold(3, DistanceMetric::DotProduct, 100);

        backend
            .insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 10)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &[0.8, 0.0, 0.0], 20)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(3), &[0.6, 0.0, 0.0], 30)
            .unwrap();

        // Delete id=2 from active buffer
        backend.delete_with_timestamp(VectorId::new(2), 50).unwrap();

        let results = backend.search(&[1.0, 0.0, 0.0], 5);
        assert_eq!(results.len(), 2);
        for &(id, _) in &results {
            assert_ne!(
                id.as_u64(),
                2,
                "Deleted vector in active buffer must not appear"
            );
        }
    }

    #[test]
    fn test_search_in_range_boundary_conditions() {
        let mut backend = make_backend_with_threshold(3, DistanceMetric::DotProduct, 100);

        backend
            .insert_with_timestamp(VectorId::new(1), &[1.0, 0.0, 0.0], 10)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(2), &[0.8, 0.0, 0.0], 20)
            .unwrap();
        backend
            .insert_with_timestamp(VectorId::new(3), &[0.6, 0.0, 0.0], 30)
            .unwrap();

        // Exact single timestamp: start_ts == end_ts == 20
        let r1 = backend.search_in_range(&[1.0, 0.0, 0.0], 5, 20, 20);
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0].0.as_u64(), 2);

        // Inverted range: start_ts > end_ts → no results
        let r2 = backend.search_in_range(&[1.0, 0.0, 0.0], 5, 30, 10);
        assert!(r2.is_empty(), "Inverted range should return no results");

        // Full range
        let r3 = backend.search_in_range(&[1.0, 0.0, 0.0], 5, 0, u64::MAX);
        assert_eq!(r3.len(), 3);
    }

    #[test]
    fn test_merge_results_two_sets() {
        // Most common real case: active buffer + 1 sealed segment
        let set1 = vec![(VectorId::new(1), 0.95), (VectorId::new(3), 0.80)];
        let set2 = vec![(VectorId::new(2), 0.90), (VectorId::new(4), 0.70)];
        let merged = SegmentedHnswBackend::merge_results(vec![set1, set2], 3);
        assert_eq!(merged.len(), 3);
        let ids: Vec<u64> = merged.iter().map(|(id, _)| id.as_u64()).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_merge_results_skips_duplicates() {
        // Verify production-safe dedup: if a VectorId appears in two sets
        // (shouldn't happen, but merge_results must handle it gracefully)
        let set1 = vec![(VectorId::new(1), 0.95), (VectorId::new(2), 0.80)];
        let set2 = vec![
            (VectorId::new(1), 0.90), // duplicate of id 1 from set1
            (VectorId::new(3), 0.70),
        ];
        let merged = SegmentedHnswBackend::merge_results(vec![set1, set2], 3);
        // Should contain exactly 3 unique IDs
        assert_eq!(merged.len(), 3);
        let ids: Vec<u64> = merged.iter().map(|(id, _)| id.as_u64()).collect();
        // id=1 appears at 0.95 (from set1), id=1 at 0.90 (from set2) is skipped
        assert_eq!(ids, vec![1, 2, 3]);
        // The first occurrence (0.95) should be kept
        assert!((merged[0].1 - 0.95).abs() < f32::EPSILON);
    }
}

#[cfg(test)]
mod profiling_tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::time::Instant;

    fn make_embedding(dim: usize, seed: usize) -> Vec<f32> {
        (0..dim)
            .map(|j| ((seed * dim + j) as f32 / 1000.0).sin())
            .collect()
    }

    /// Test 5: Segment Fan-Out vs Compact
    ///
    /// Builds a SegmentedHnswBackend with 100K vectors (seal_threshold=50K → 2 segments).
    /// Times 100 queries. Then compact() into 1 segment, times same queries.
    /// Reports QPS and recall@10 for both configurations.
    #[test]
    fn profile_segment_fanout_vs_compact() {
        use crate::primitives::vector::brute_force::BruteForceBackend;

        let n = 100_000;
        let dim = 128;
        let num_queries = 100;
        let k = 10;
        let seal_threshold = 50_000;

        let config = VectorConfig::new(dim, DistanceMetric::Cosine).unwrap();
        let seg_config = SegmentedHnswConfig {
            hnsw: HnswConfig::default(),
            seal_threshold,
            heap_flush_threshold: 0, // Disable mmap flushing in tests
            auto_compact_threshold: usize::MAX, // Disable auto-compact in tests
        };
        let mut backend = SegmentedHnswBackend::new(&config, seg_config);

        // Also build brute-force for recall measurement
        let mut brute = BruteForceBackend::new(&config);

        // Insert all vectors
        for i in 1..=n {
            let emb = make_embedding(dim, i);
            let id = VectorId::new(i as u64);
            backend.insert(id, &emb).unwrap();
            brute.insert(id, &emb).unwrap();
        }

        let num_segments_before = backend.sealed.len();

        // Pre-generate queries
        let queries: Vec<Vec<f32>> = (0..num_queries)
            .map(|i| make_embedding(dim, n + 1 + i))
            .collect();

        // --- Time multi-segment search ---
        let start = Instant::now();
        let mut multi_results: Vec<Vec<(VectorId, f32)>> = Vec::with_capacity(num_queries);
        for q in &queries {
            multi_results.push(backend.search(q, k));
        }
        let multi_elapsed = start.elapsed();
        let multi_qps = num_queries as f64 / multi_elapsed.as_secs_f64();

        // Compute recall@k for multi-segment
        let mut multi_recall_sum = 0.0;
        for (i, q) in queries.iter().enumerate() {
            let brute_results = brute.search(q, k);
            let brute_ids: BTreeSet<VectorId> = brute_results.iter().map(|(id, _)| *id).collect();
            let hnsw_ids: BTreeSet<VectorId> = multi_results[i].iter().map(|(id, _)| *id).collect();
            let overlap = brute_ids.intersection(&hnsw_ids).count();
            multi_recall_sum += overlap as f64 / k as f64;
        }
        let multi_recall = multi_recall_sum / num_queries as f64;

        // --- Compact ---
        backend.compact();
        let num_segments_after = backend.sealed.len();

        // --- Time single-segment search ---
        let start = Instant::now();
        let mut compact_results: Vec<Vec<(VectorId, f32)>> = Vec::with_capacity(num_queries);
        for q in &queries {
            compact_results.push(backend.search(q, k));
        }
        let compact_elapsed = start.elapsed();
        let compact_qps = num_queries as f64 / compact_elapsed.as_secs_f64();

        // Compute recall@k for compacted
        let mut compact_recall_sum = 0.0;
        for (i, q) in queries.iter().enumerate() {
            let brute_results = brute.search(q, k);
            let brute_ids: BTreeSet<VectorId> = brute_results.iter().map(|(id, _)| *id).collect();
            let hnsw_ids: BTreeSet<VectorId> =
                compact_results[i].iter().map(|(id, _)| *id).collect();
            let overlap = brute_ids.intersection(&hnsw_ids).count();
            compact_recall_sum += overlap as f64 / k as f64;
        }
        let compact_recall = compact_recall_sum / num_queries as f64;

        let speedup = compact_qps / multi_qps;

        println!("\n=== Segment Fan-Out: {num_segments_before} Segments vs {num_segments_after} Compact ({n} vectors, dim={dim}) ===");
        println!(
            "  {num_segments_before} segments: {multi_qps:.0} QPS, recall@{k} = {multi_recall:.3} ({multi_elapsed:?})"
        );
        println!(
            "  {num_segments_after} segment:  {compact_qps:.0} QPS, recall@{k} = {compact_recall:.3} ({compact_elapsed:?})"
        );
        println!("  compact speedup: {speedup:.2}x");
    }
}
