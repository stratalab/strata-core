//! Optional inverted index for fast keyword search
//!
//! This module provides:
//! - InvertedIndex with posting lists
//! - Enable/disable functionality
//! - Synchronous index updates on commit
//! - Version watermark for consistency
//! - Segmented architecture with sealed mmap-backed segments
//!
//! See `docs/architecture/M6_ARCHITECTURE.md` for authoritative specification.
//!
//! # Architectural Rules
//!
//! - Rule 1: No Data Movement - index stores integer doc IDs, not content
//! - Rule 5: Zero Overhead When Disabled - NOOP when disabled
//!
//! # Memory Efficiency
//!
//! PostingEntry uses a compact `u32` doc ID (12 bytes, Copy) instead of
//! cloning a full EntityRef (~87 bytes with heap allocation) per posting.
//! A single bidirectional `DocIdMap` holds one copy of each EntityRef,
//! reducing memory from O(terms × docs) to O(docs) for EntityRef storage.
//!
//! # Segmented Architecture
//!
//! The index uses a Lucene-inspired segmented design:
//! - **Active segment**: DashMap-based, mutable, absorbs writes
//! - **Sealed segments**: Immutable, mmap-backed `.sidx` files
//! - When active hits `seal_threshold` docs → seal → flush to disk
//! - `score_top_k()` merges results across all segments
//!
//! # Usage
//!
//! Indexing is OPTIONAL. Search works without it (via full scan).
//! When enabled, search uses the index for candidate lookup.

use super::manifest::{self, ManifestData, SegmentManifestEntry};
use super::segment::{self, SealedSegment};
use super::tokenizer::tokenize;
use super::types::EntityRef;
use dashmap::DashMap;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};
use strata_core::types::BranchId;

// ============================================================================
// PostingEntry
// ============================================================================

/// Entry in a posting list
///
/// Compact 12-byte, Copy struct. Uses an integer doc ID instead of a cloned
/// EntityRef to avoid 87 bytes of heap allocation per posting entry.
/// Resolve to EntityRef via `InvertedIndex::resolve_doc_id()`.
#[derive(Debug, Clone, Copy)]
pub struct PostingEntry {
    /// Integer document identifier (resolve via InvertedIndex::resolve_doc_id)
    pub doc_id: u32,
    /// Term frequency in this document
    pub tf: u32,
    /// Document length in tokens
    pub doc_len: u32,
}

impl PostingEntry {
    /// Create a new posting entry
    pub fn new(doc_id: u32, tf: u32, doc_len: u32) -> Self {
        PostingEntry {
            doc_id,
            tf,
            doc_len,
        }
    }
}

// ============================================================================
// ScoredDocId
// ============================================================================

/// Result of in-index BM25 scoring: doc_id + score.
///
/// Returned by `InvertedIndex::score_top_k()` to avoid exposing
/// posting list internals. Consumers resolve the `doc_id` back to
/// an `EntityRef` via `InvertedIndex::resolve_doc_id()`.
#[derive(Debug, Clone, Copy)]
pub struct ScoredDocId {
    /// Integer document identifier (resolve via InvertedIndex::resolve_doc_id)
    pub doc_id: u32,
    /// BM25 relevance score
    pub score: f32,
}

// ============================================================================
// DocIdMap
// ============================================================================

/// Bidirectional mapping between EntityRef and compact u32 doc IDs.
///
/// Stores exactly one copy of each EntityRef (not 60× per term).
/// Memory at 5.4M docs: ~918 MB (vs 28 GB with per-posting clones).
pub(crate) struct DocIdMap {
    /// doc_id -> EntityRef (append-only, indexed by doc_id)
    pub(crate) id_to_ref: RwLock<Vec<EntityRef>>,
    /// EntityRef -> doc_id (for O(1) lookup on index/remove)
    ref_to_id: DashMap<EntityRef, u32>,
}

impl DocIdMap {
    fn new() -> Self {
        Self {
            id_to_ref: RwLock::new(Vec::new()),
            ref_to_id: DashMap::new(),
        }
    }

    /// Get or assign a doc_id for the given EntityRef.
    fn get_or_insert(&self, doc_ref: &EntityRef) -> u32 {
        // Fast path: already assigned
        if let Some(id) = self.ref_to_id.get(doc_ref) {
            return *id;
        }

        // Slow path: assign new ID
        let mut vec = self.id_to_ref.write().unwrap();
        // Double-check after acquiring write lock
        if let Some(id) = self.ref_to_id.get(doc_ref) {
            return *id;
        }
        let id = vec.len() as u32;
        vec.push(doc_ref.clone());
        self.ref_to_id.insert(doc_ref.clone(), id);
        id
    }

    /// Look up a doc_id, returning None if the EntityRef is unknown.
    pub(crate) fn get(&self, doc_ref: &EntityRef) -> Option<u32> {
        self.ref_to_id.get(doc_ref).map(|r| *r)
    }

    /// Resolve a doc_id back to its EntityRef.
    fn resolve(&self, doc_id: u32) -> Option<EntityRef> {
        let vec = self.id_to_ref.read().unwrap();
        vec.get(doc_id as usize).cloned()
    }

    fn clear(&self) {
        self.id_to_ref.write().unwrap().clear();
        self.ref_to_id.clear();
    }

    /// Restore from a serialized Vec of EntityRefs.
    fn restore_from_vec(&self, entries: Vec<EntityRef>) {
        let mut vec = self.id_to_ref.write().unwrap();
        vec.clear();
        self.ref_to_id.clear();
        for (id, entity_ref) in entries.iter().enumerate() {
            self.ref_to_id.insert(entity_ref.clone(), id as u32);
        }
        *vec = entries;
    }

    /// Get the current number of entries.
    fn len(&self) -> usize {
        self.id_to_ref.read().unwrap().len()
    }
}

// ============================================================================
// PostingList
// ============================================================================

/// List of documents containing a term
#[derive(Debug, Clone, Default)]
pub struct PostingList {
    /// Document entries
    pub entries: Vec<PostingEntry>,
}

impl PostingList {
    /// Create a new empty posting list
    pub fn new() -> Self {
        PostingList { entries: vec![] }
    }

    /// Add an entry to the posting list
    pub fn add(&mut self, entry: PostingEntry) {
        self.entries.push(entry);
    }

    /// Remove entries matching a doc_id
    pub fn remove_by_id(&mut self, doc_id: u32) -> usize {
        let before = self.entries.len();
        self.entries.retain(|e| e.doc_id != doc_id);
        before - self.entries.len()
    }

    /// Number of documents containing this term
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if posting list is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ============================================================================
// Default seal threshold
// ============================================================================

/// Default number of documents in active segment before sealing
const DEFAULT_SEAL_THRESHOLD: usize = 100_000;

// ============================================================================
// InvertedIndex
// ============================================================================

/// Inverted index for fast keyword search
///
/// CRITICAL: This is OPTIONAL. Search works without it (via scan).
/// When disabled, all operations are NOOP (zero overhead).
///
/// # Thread Safety
///
/// Uses DashMap for concurrent access. Multiple readers/writers supported.
///
/// # Version Watermark
///
/// The version field tracks index state for consistency checking.
/// Incremented on every update operation.
pub struct InvertedIndex {
    // --- Active segment (DashMap-based, mutable) ---
    /// Term -> PostingList mapping (active segment)
    postings: DashMap<String, PostingList>,
    /// Term -> document frequency (active segment only)
    doc_freqs: DashMap<String, usize>,
    /// doc_id -> document length (indexed by u32 doc_id, active segment)
    doc_lengths: RwLock<Vec<Option<u32>>>,

    // --- Sealed segments (immutable, mmap-backed) ---
    /// Sealed segments ordered by segment_id
    sealed: RwLock<Vec<SealedSegment>>,

    // --- Shared state (spans all segments) ---
    /// Bidirectional EntityRef <-> u32 mapping (one copy per doc, not per term)
    doc_id_map: DocIdMap,
    /// Total documents indexed (across ALL segments)
    total_docs: AtomicUsize,
    /// Sum of all document lengths (across ALL segments)
    total_doc_len: AtomicUsize,
    /// Whether index is enabled
    enabled: AtomicBool,
    /// Version watermark for consistency
    version: AtomicU64,

    // --- Persistence ---
    /// Next segment ID to assign
    next_segment_id: AtomicU64,
    /// Number of active docs before sealing
    seal_threshold: usize,
    /// Data directory for persistence (None for ephemeral)
    data_dir: RwLock<Option<PathBuf>>,
    /// Number of docs currently in the active segment
    active_doc_count: AtomicUsize,
    /// Guard to prevent concurrent auto-seals
    sealing: AtomicBool,
    /// Set of branch IDs seen across all indexed documents.
    /// When only one branch exists, score_top_k can skip per-doc branch checks.
    branch_ids: RwLock<HashSet<BranchId>>,
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl InvertedIndex {
    /// Create a new disabled index
    pub fn new() -> Self {
        InvertedIndex {
            postings: DashMap::new(),
            doc_freqs: DashMap::new(),
            total_docs: AtomicUsize::new(0),
            enabled: AtomicBool::new(false),
            version: AtomicU64::new(0),
            total_doc_len: AtomicUsize::new(0),
            doc_lengths: RwLock::new(Vec::new()),
            doc_id_map: DocIdMap::new(),
            sealed: RwLock::new(Vec::new()),
            next_segment_id: AtomicU64::new(0),
            seal_threshold: DEFAULT_SEAL_THRESHOLD,
            data_dir: RwLock::new(None),
            active_doc_count: AtomicUsize::new(0),
            sealing: AtomicBool::new(false),
            branch_ids: RwLock::new(HashSet::new()),
        }
    }

    // ========================================================================
    // Enable/Disable
    // ========================================================================

    /// Check if index is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    /// Enable the index
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
    }

    /// Disable the index
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    /// Clear all index data
    ///
    /// Does NOT change enabled state.
    pub fn clear(&self) {
        self.postings.clear();
        self.doc_freqs.clear();
        self.doc_lengths.write().unwrap().clear();
        self.doc_id_map.clear();
        self.sealed.write().unwrap().clear();
        self.total_docs.store(0, Ordering::Relaxed);
        self.total_doc_len.store(0, Ordering::Relaxed);
        self.active_doc_count.store(0, Ordering::Relaxed);
        self.sealing.store(false, Ordering::Relaxed);
        self.branch_ids.write().unwrap().clear();
        self.version.fetch_add(1, Ordering::Release);
    }

    // ========================================================================
    // Configuration
    // ========================================================================

    /// Set the data directory for persistence.
    pub fn set_data_dir(&self, path: PathBuf) {
        *self.data_dir.write().unwrap() = Some(path);
    }

    /// Get the search directory path (data_dir/search/).
    fn search_dir(&self) -> Option<PathBuf> {
        self.data_dir
            .read()
            .unwrap()
            .as_ref()
            .map(|d| d.join("search"))
    }

    // ========================================================================
    // Version Watermark
    // ========================================================================

    /// Get current version
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Check if index is at least at given version
    pub fn is_at_version(&self, min_version: u64) -> bool {
        self.version.load(Ordering::Acquire) >= min_version
    }

    /// Wait for index to reach a version (with timeout)
    ///
    /// Returns true if version was reached, false on timeout.
    pub fn wait_for_version(&self, version: u64, timeout: Duration) -> bool {
        let start = Instant::now();
        loop {
            if self.version.load(Ordering::Acquire) >= version {
                return true;
            }
            if start.elapsed() >= timeout {
                return false;
            }
            std::thread::yield_now();
        }
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get total number of indexed documents (across all segments)
    ///
    /// Uses Acquire ordering to ensure visibility of updates from other threads.
    pub fn total_docs(&self) -> usize {
        self.total_docs.load(Ordering::Acquire)
    }

    /// Get document frequency for a term (across all segments)
    pub fn doc_freq(&self, term: &str) -> usize {
        // Active segment df
        let active_df = self.doc_freqs.get(term).map(|r| *r).unwrap_or(0);

        // Sum df from sealed segments
        let sealed = self.sealed.read().unwrap();
        let sealed_df: usize = sealed.iter().map(|seg| seg.doc_freq(term) as usize).sum();

        active_df + sealed_df
    }

    /// Get average document length
    ///
    /// Uses Acquire ordering to ensure consistent visibility of both counters.
    pub fn avg_doc_len(&self) -> f32 {
        let total = self.total_docs.load(Ordering::Acquire);
        if total == 0 {
            return 0.0;
        }
        self.total_doc_len.load(Ordering::Acquire) as f32 / total as f32
    }

    /// Compute IDF for a term
    ///
    /// Uses standard IDF formula with smoothing:
    /// IDF(t) = ln((N - df + 0.5) / (df + 0.5) + 1)
    ///
    /// Uses Acquire ordering to ensure visibility of document count updates.
    pub fn compute_idf(&self, term: &str) -> f32 {
        let n = self.total_docs.load(Ordering::Acquire) as f32;
        let df = self.doc_freq(term) as f32;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    // ========================================================================
    // Doc ID Resolution
    // ========================================================================

    /// Resolve a compact u32 doc_id back to its EntityRef.
    ///
    /// Used by search consumers (e.g., KVStore::search) to map posting
    /// entries back to the original document references.
    pub fn resolve_doc_id(&self, doc_id: u32) -> Option<EntityRef> {
        self.doc_id_map.resolve(doc_id)
    }

    // ========================================================================
    // In-Index BM25 Scoring
    // ========================================================================

    /// Score documents using BM25 entirely within the index.
    ///
    /// Searches across both the active segment and all sealed segments.
    /// Global stats (total_docs, avg_doc_len) span all segments.
    /// Per-term IDF is computed from the sum of df across all segments.
    ///
    /// Optimizations over naive approach:
    /// - **Cached find_term**: single binary search per (segment, term), reused for
    ///   both IDF computation and posting iteration.
    /// - **Tombstone AtomicBool**: skips RwLock read on segments with no tombstones.
    /// - **Single-branch detection**: skips per-doc branch_id check when only one
    ///   branch exists in the index.
    /// - **Dense Vec accumulator**: uses `Vec<f32>` indexed by doc_id instead of
    ///   HashMap for score accumulation.
    pub fn score_top_k(
        &self,
        query_terms: &[String],
        branch_id: &BranchId,
        k: usize,
        scorer_k1: f32,
        scorer_b: f32,
    ) -> Vec<ScoredDocId> {
        if !self.is_enabled() || query_terms.is_empty() || k == 0 {
            return Vec::new();
        }

        let total_docs = self.total_docs.load(Ordering::Acquire) as f32;
        let avg_doc_len = self.avg_doc_len().max(1.0);

        // Acquire sealed lock ONCE — used for both IDF and scoring
        let sealed = self.sealed.read().unwrap();
        let num_sealed = sealed.len();

        // [Opt 2] Combined IDF + posting location caching.
        // One find_term per (segment, term) — result cached for posting iteration.
        // Each entry: (term_str, idf, Vec<Option<(posting_offset, posting_byte_len)>>)
        #[allow(clippy::type_complexity)]
        let term_data: Vec<(&str, f32, Vec<Option<(u32, u32)>>)> = query_terms
            .iter()
            .map(|t| {
                let active_df = self.doc_freqs.get(t.as_str()).map(|r| *r).unwrap_or(0);
                let mut seg_locations = Vec::with_capacity(num_sealed);
                let mut sealed_df: usize = 0;
                for seg in sealed.iter() {
                    match seg.find_term_info(t) {
                        Some((df, offset, len)) => {
                            sealed_df += df as usize;
                            seg_locations.push(Some((offset, len)));
                        }
                        None => {
                            seg_locations.push(None);
                        }
                    }
                }
                let df = (active_df + sealed_df) as f32;
                let idf = ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln();
                (t.as_str(), idf, seg_locations)
            })
            .collect();

        // Acquire doc_id_map read lock ONCE for the entire search
        let id_to_ref = self.doc_id_map.id_to_ref.read().unwrap();
        let num_docs = id_to_ref.len();

        // [Opt 3] Single-branch detection: skip per-doc branch check
        let skip_branch_check = {
            let bids = self.branch_ids.read().unwrap();
            if bids.len() <= 1 {
                // If single branch and it doesn't match → empty result
                if bids.len() == 1 && !bids.contains(branch_id) {
                    return Vec::new();
                }
                true
            } else {
                false
            }
        };

        // [Opt 4] Dense Vec<f32> accumulator sized to id_to_ref.len()
        // Doc IDs are contiguous 0..N-1 by design.
        let mut scores = vec![0.0f32; num_docs];
        let mut seen = vec![false; num_docs];
        let mut touched: Vec<u32> = Vec::new();

        // Precompute BM25 constants
        let k1_times_b_over_avg = scorer_k1 * scorer_b / avg_doc_len;
        let k1_times_one_minus_b = scorer_k1 * (1.0 - scorer_b);
        let k1_plus_1 = scorer_k1 + 1.0;

        // --- Score active segment ---
        for (term, idf, _) in &term_data {
            if let Some(posting_list) = self.postings.get(*term) {
                for entry in &posting_list.entries {
                    if !skip_branch_check {
                        match id_to_ref.get(entry.doc_id as usize) {
                            Some(entity_ref) if entity_ref.branch_id() == *branch_id => {}
                            _ => continue,
                        }
                    }
                    let tf = entry.tf as f32;
                    let dl = entry.doc_len as f32;
                    let tf_component =
                        (tf * k1_plus_1) / (tf + k1_times_one_minus_b + k1_times_b_over_avg * dl);
                    let did = entry.doc_id as usize;
                    if !seen[did] {
                        seen[did] = true;
                        touched.push(entry.doc_id);
                    }
                    scores[did] += idf * tf_component;
                }
            }
        }

        // --- Score sealed segments ---
        for (seg_idx, seg) in sealed.iter().enumerate() {
            // [Opt 1] Skip tombstone lock if has_tombstones() == false
            let tombstone_guard = if seg.has_tombstones() {
                Some(seg.tombstone_guard())
            } else {
                None
            };

            for (_term, idf, seg_locations) in &term_data {
                // [Opt 2] Use cached offset — no second binary search
                let (offset, len) = match seg_locations[seg_idx] {
                    Some(v) => v,
                    None => continue,
                };
                let iter = match seg.posting_iter_from_offset(offset, len) {
                    Some(it) => it,
                    None => continue,
                };
                for entry in iter {
                    if let Some(ref ts) = tombstone_guard {
                        if ts.contains(&entry.doc_id) {
                            continue;
                        }
                    }
                    if !skip_branch_check {
                        match id_to_ref.get(entry.doc_id as usize) {
                            Some(entity_ref) if entity_ref.branch_id() == *branch_id => {}
                            _ => continue,
                        }
                    }
                    let tf = entry.tf as f32;
                    let dl = entry.doc_len as f32;
                    let tf_component =
                        (tf * k1_plus_1) / (tf + k1_times_one_minus_b + k1_times_b_over_avg * dl);
                    let did = entry.doc_id as usize;
                    if !seen[did] {
                        seen[did] = true;
                        touched.push(entry.doc_id);
                    }
                    scores[did] += idf * tf_component;
                }
            }
        }
        drop(sealed);
        drop(id_to_ref);

        if touched.is_empty() {
            return Vec::new();
        }

        // Collect top-k from touched (partial sort is O(n) vs O(n log n) full sort)
        let mut result: Vec<ScoredDocId> = touched
            .into_iter()
            .map(|doc_id| ScoredDocId {
                doc_id,
                score: scores[doc_id as usize],
            })
            .collect();

        let cmp = |a: &ScoredDocId, b: &ScoredDocId| -> std::cmp::Ordering {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        };

        if result.len() > k {
            // O(n) partition: puts top-k elements in [0..k] (unordered)
            result.select_nth_unstable_by(k - 1, cmp);
            result.truncate(k);
        }
        // Sort only the k elements (k is typically 10)
        result.sort_unstable_by(cmp);
        result
    }

    // ========================================================================
    // Index Updates
    // ========================================================================

    /// Index a document
    ///
    /// NOOP if index is disabled.
    /// If document is already indexed, removes old version first (fixes #609).
    pub fn index_document(&self, doc_ref: &EntityRef, text: &str, _ts_micros: Option<u64>) {
        if !self.is_enabled() {
            return; // Zero overhead when disabled
        }

        // Get or assign a compact doc_id
        let doc_id = self.doc_id_map.get_or_insert(doc_ref);

        // Track branch ID for single-branch fast path in score_top_k.
        // Fast path: skip write lock if already present.
        {
            let bid = doc_ref.branch_id();
            if !self.branch_ids.read().unwrap().contains(&bid) {
                self.branch_ids.write().unwrap().insert(bid);
            }
        }

        // Fix #609: Check if document already indexed, remove first to prevent double-counting
        {
            let lengths = self.doc_lengths.read().unwrap();
            if lengths.get(doc_id as usize).copied().flatten().is_some() {
                drop(lengths);
                self.remove_document(doc_ref);
            }
        }

        let tokens = tokenize(text);
        let doc_len = tokens.len() as u32;

        // Count term frequencies — consume tokens to avoid cloning
        let mut tf_map: HashMap<String, u32> = HashMap::with_capacity(tokens.len());
        for token in tokens {
            *tf_map.entry(token).or_insert(0) += 1;
        }

        // Update posting lists
        for (term, tf) in tf_map {
            let entry = PostingEntry::new(doc_id, tf, doc_len);

            self.postings.entry(term.clone()).or_default().add(entry);

            self.doc_freqs
                .entry(term)
                .and_modify(|c| *c += 1)
                .or_insert(1);
        }

        // Track document length for proper removal (fixes #608)
        {
            let mut lengths = self.doc_lengths.write().unwrap();
            let idx = doc_id as usize;
            if idx >= lengths.len() {
                lengths.resize(idx + 1, None);
            }
            lengths[idx] = Some(doc_len);
        }

        self.total_docs.fetch_add(1, Ordering::Relaxed);
        self.total_doc_len
            .fetch_add(doc_len as usize, Ordering::Relaxed);
        self.active_doc_count.fetch_add(1, Ordering::Relaxed);
        self.version.fetch_add(1, Ordering::Release);

        // Check if we should seal the active segment
        let active_count = self.active_doc_count.load(Ordering::Relaxed);
        if active_count >= self.seal_threshold {
            self.try_seal_active();
        }
    }

    /// Remove a document from the index
    ///
    /// NOOP if index is disabled.
    /// Handles both active and sealed segments:
    /// - Active: removes from DashMap directly
    /// - Sealed: adds to tombstone set
    /// - doc_lengths is a global map (persists across seals), so doc_len
    ///   is always available for accurate total_doc_len adjustment.
    pub fn remove_document(&self, doc_ref: &EntityRef) {
        if !self.is_enabled() {
            return;
        }

        // Resolve EntityRef -> doc_id
        let doc_id = match self.doc_id_map.get(doc_ref) {
            Some(id) => id,
            None => return, // Not indexed
        };

        // Retrieve doc_len from global tracking (persists across seals)
        let doc_len = {
            let mut lengths = self.doc_lengths.write().unwrap();
            let idx = doc_id as usize;
            if idx < lengths.len() {
                lengths[idx].take()
            } else {
                None
            }
        };

        // Try removing from active segment (no-op if doc not in active)
        let mut active_removed = false;
        for mut entry in self.postings.iter_mut() {
            let count = entry.remove_by_id(doc_id);
            if count > 0 {
                active_removed = true;
                let term = entry.key().clone();
                self.doc_freqs
                    .entry(term)
                    .and_modify(|c| *c = c.saturating_sub(count));
            }
        }

        // Add tombstone to all sealed segments (harmless if doc not in segment)
        {
            let sealed = self.sealed.read().unwrap();
            for seg in sealed.iter() {
                seg.add_tombstone(doc_id);
            }
        }

        // Update global stats
        if active_removed || doc_len.is_some() {
            self.total_docs.fetch_sub(1, Ordering::Relaxed);
            if let Some(len) = doc_len {
                self.total_doc_len
                    .fetch_sub(len as usize, Ordering::Relaxed);
            }
            if active_removed {
                self.active_doc_count.fetch_sub(1, Ordering::Relaxed);
            }
            self.version.fetch_add(1, Ordering::Release);
        }
    }

    // ========================================================================
    // Seal & Persistence
    // ========================================================================

    /// Try to seal the active segment if it has enough documents.
    ///
    /// Uses CAS on `sealing` to prevent concurrent auto-seals.
    fn try_seal_active(&self) {
        let active_count = self.active_doc_count.load(Ordering::Relaxed);
        if active_count < self.seal_threshold {
            return;
        }
        // Prevent concurrent auto-seals
        if self
            .sealing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return; // Another thread is already sealing
        }
        self.seal_active();
        self.sealing.store(false, Ordering::Release);
    }

    /// Seal the active segment into an immutable sealed segment.
    ///
    /// Drains the active DashMap → builds sorted term dict + delta-encodes
    /// postings → creates `SealedSegment`. Optionally flushes to disk.
    ///
    /// NOTE: `doc_lengths` is NOT cleared — it is a global map that persists
    /// across seals, enabling re-index detection and accurate `total_doc_len`
    /// adjustment when removing docs from sealed segments.
    pub fn seal_active(&self) {
        let active_count = self.active_doc_count.load(Ordering::Relaxed);
        if active_count == 0 {
            return;
        }

        // Drain active segment into sorted term map
        let mut term_postings: BTreeMap<String, Vec<PostingEntry>> = BTreeMap::new();

        // Collect and remove all entries from active postings
        let keys: Vec<String> = self.postings.iter().map(|r| r.key().clone()).collect();
        for key in keys {
            if let Some((term, posting_list)) = self.postings.remove(&key) {
                if !posting_list.entries.is_empty() {
                    term_postings.insert(term, posting_list.entries);
                }
            }
        }
        self.doc_freqs.clear();

        // Compute total doc len from unique doc_ids in the drained postings.
        // This is more accurate than iterating doc_lengths (which contains
        // entries from prior seals too).
        let mut seen_docs: HashSet<u32> = HashSet::new();
        let mut active_total_doc_len: u64 = 0;
        for entries in term_postings.values() {
            for entry in entries {
                if seen_docs.insert(entry.doc_id) {
                    active_total_doc_len += entry.doc_len as u64;
                }
            }
        }
        let actual_doc_count = seen_docs.len() as u32;

        let segment_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);

        // Build the sealed segment
        let seg = segment::build_sealed_segment(
            segment_id,
            term_postings,
            actual_doc_count,
            active_total_doc_len,
        );

        // Flush to disk if we have a data directory
        if let Some(search_dir) = self.search_dir() {
            let path = search_dir.join(format!("seg_{}.sidx", segment_id));
            if let Err(e) = seg.write_to_file(&path) {
                tracing::warn!(
                    target: "strata::search",
                    segment_id = segment_id,
                    error = %e,
                    "Failed to flush sealed segment to disk"
                );
            }
        }

        // Add to sealed list
        self.sealed.write().unwrap().push(seg);
        self.active_doc_count.store(0, Ordering::Relaxed);
    }

    /// Freeze the entire index to disk for recovery.
    ///
    /// 1. Seal active segment if non-empty
    /// 2. Flush any in-memory sealed segments to .sidx files
    /// 3. Write search.manifest with DocIdMap + stats
    pub fn freeze_to_disk(&self) -> std::io::Result<()> {
        let search_dir = match self.search_dir() {
            Some(d) => d,
            None => return Ok(()), // No data dir — ephemeral
        };

        // Seal active segment if it has data
        self.seal_active();

        // Ensure all segments are flushed to disk
        let sealed = self.sealed.read().unwrap();
        for seg in sealed.iter() {
            let path = search_dir.join(format!("seg_{}.sidx", seg.segment_id()));
            if !path.exists() {
                seg.write_to_file(&path)?;
            }
        }

        // Build manifest data
        let segment_entries: Vec<SegmentManifestEntry> = sealed
            .iter()
            .map(|seg| SegmentManifestEntry {
                segment_id: seg.segment_id(),
                doc_count: seg.doc_count(),
                total_doc_len: seg.total_doc_len(),
                tombstones: seg.tombstones(),
            })
            .collect();
        drop(sealed);

        let doc_id_map_vec = self.doc_id_map.id_to_ref.read().unwrap().clone();
        let doc_lengths_vec = self.doc_lengths.read().unwrap().clone();

        let manifest_data = ManifestData {
            version: 1,
            total_docs: self.total_docs.load(Ordering::Acquire) as u64,
            total_doc_len: self.total_doc_len.load(Ordering::Acquire) as u64,
            next_segment_id: self.next_segment_id.load(Ordering::Relaxed),
            segments: segment_entries,
            doc_id_map: doc_id_map_vec,
            doc_lengths: doc_lengths_vec,
        };

        let manifest_path = search_dir.join("search.manifest");
        manifest::write_manifest(&manifest_path, &manifest_data)?;

        tracing::info!(
            target: "strata::search",
            segments = manifest_data.segments.len(),
            total_docs = manifest_data.total_docs,
            "Search index frozen to disk"
        );

        Ok(())
    }

    /// Load index state from manifest and mmap'd segments.
    ///
    /// Returns true if successfully loaded, false if no manifest found.
    pub fn load_from_disk(&self) -> std::io::Result<bool> {
        let search_dir = match self.search_dir() {
            Some(d) => d,
            None => return Ok(false),
        };

        let manifest_path = search_dir.join("search.manifest");
        if !manifest_path.exists() {
            return Ok(false);
        }

        let data = manifest::load_manifest(&manifest_path)?;

        // Restore DocIdMap
        self.doc_id_map.restore_from_vec(data.doc_id_map);

        // Restore global stats
        self.total_docs
            .store(data.total_docs as usize, Ordering::Relaxed);
        self.total_doc_len
            .store(data.total_doc_len as usize, Ordering::Relaxed);
        self.next_segment_id
            .store(data.next_segment_id, Ordering::Relaxed);

        // Load sealed segments from mmap
        let mut sealed = self.sealed.write().unwrap();
        sealed.clear();

        for entry in &data.segments {
            let seg_path = search_dir.join(format!("seg_{}.sidx", entry.segment_id));
            match SealedSegment::from_mmap(&seg_path) {
                Ok(seg) => {
                    seg.set_tombstones(entry.tombstones.clone());
                    sealed.push(seg);
                }
                Err(e) => {
                    tracing::warn!(
                        target: "strata::search",
                        segment_id = entry.segment_id,
                        error = %e,
                        "Failed to load sealed segment from mmap"
                    );
                    return Err(e);
                }
            }
        }

        // Active segment starts empty
        self.postings.clear();
        self.doc_freqs.clear();
        self.active_doc_count.store(0, Ordering::Relaxed);

        // Restore global doc_lengths from manifest (persists across seals
        // for re-index detection and accurate total_doc_len on removal)
        *self.doc_lengths.write().unwrap() = data.doc_lengths;

        // Rebuild branch_ids from restored DocIdMap
        {
            let id_to_ref = self.doc_id_map.id_to_ref.read().unwrap();
            let mut bids = self.branch_ids.write().unwrap();
            bids.clear();
            for entity_ref in id_to_ref.iter() {
                bids.insert(entity_ref.branch_id());
            }
        }

        tracing::info!(
            target: "strata::search",
            segments = sealed.len(),
            total_docs = data.total_docs,
            doc_id_map_size = self.doc_id_map.len(),
            "Search index loaded from disk"
        );

        Ok(true)
    }

    // ========================================================================
    // Query
    // ========================================================================

    /// Lookup documents containing a term
    ///
    /// Returns None if term not found or index disabled.
    pub fn lookup(&self, term: &str) -> Option<PostingList> {
        if !self.is_enabled() {
            return None;
        }
        self.postings.get(term).map(|r| r.clone())
    }

    /// Get all terms in the index
    pub fn terms(&self) -> Vec<String> {
        self.postings.iter().map(|r| r.key().clone()).collect()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::types::BranchId;

    fn test_doc_ref(name: &str) -> EntityRef {
        let branch_id = BranchId::new();
        EntityRef::Kv {
            branch_id,
            key: name.to_string(),
        }
    }

    #[test]
    fn test_index_disabled_by_default() {
        let index = InvertedIndex::new();
        assert!(!index.is_enabled());
    }

    #[test]
    fn test_index_enable_disable() {
        let index = InvertedIndex::new();

        index.enable();
        assert!(index.is_enabled());

        index.disable();
        assert!(!index.is_enabled());
    }

    #[test]
    fn test_index_noop_when_disabled() {
        let index = InvertedIndex::new();
        let doc_ref = test_doc_ref("test");

        // Should be NOOP when disabled
        index.index_document(&doc_ref, "hello world", None);

        assert_eq!(index.total_docs(), 0);
        assert!(index.lookup("hello").is_none());
    }

    #[test]
    fn test_index_document_when_enabled() {
        let index = InvertedIndex::new();
        index.enable();

        let doc_ref = test_doc_ref("test");
        index.index_document(&doc_ref, "hello world test", None);

        assert_eq!(index.total_docs(), 1);
        assert_eq!(index.doc_freq("hello"), 1);
        assert_eq!(index.doc_freq("world"), 1);
        assert_eq!(index.doc_freq("test"), 1);

        let postings = index.lookup("hello").unwrap();
        assert_eq!(postings.len(), 1);
        assert_eq!(postings.entries[0].tf, 1);
        assert_eq!(postings.entries[0].doc_len, 3);
    }

    #[test]
    fn test_index_multiple_documents() {
        let index = InvertedIndex::new();
        index.enable();

        let doc1 = test_doc_ref("doc1");
        let doc2 = test_doc_ref("doc2");

        index.index_document(&doc1, "hello world", None);
        index.index_document(&doc2, "hello planet", None);

        assert_eq!(index.total_docs(), 2);
        assert_eq!(index.doc_freq("hello"), 2); // In both docs
        assert_eq!(index.doc_freq("world"), 1); // Only in doc1
        assert_eq!(index.doc_freq("planet"), 1); // Only in doc2

        let postings = index.lookup("hello").unwrap();
        assert_eq!(postings.len(), 2);
    }

    #[test]
    fn test_index_term_frequency() {
        let index = InvertedIndex::new();
        index.enable();

        let doc_ref = test_doc_ref("test");
        index.index_document(&doc_ref, "hello hello hello world", None);

        let postings = index.lookup("hello").unwrap();
        assert_eq!(postings.entries[0].tf, 3); // "hello" appears 3 times

        let postings = index.lookup("world").unwrap();
        assert_eq!(postings.entries[0].tf, 1);
    }

    #[test]
    fn test_remove_document() {
        let index = InvertedIndex::new();
        index.enable();

        let doc1 = test_doc_ref("doc1");
        let doc2 = test_doc_ref("doc2");

        index.index_document(&doc1, "hello world", None);
        index.index_document(&doc2, "hello there", None);

        assert_eq!(index.total_docs(), 2);

        index.remove_document(&doc1);

        assert_eq!(index.total_docs(), 1);
        assert_eq!(index.doc_freq("hello"), 1);
        assert_eq!(index.doc_freq("world"), 0);
    }

    #[test]
    fn test_clear() {
        let index = InvertedIndex::new();
        index.enable();

        let doc_ref = test_doc_ref("test");
        index.index_document(&doc_ref, "hello world", None);

        let v1 = index.version();
        index.clear();
        let v2 = index.version();

        assert_eq!(index.total_docs(), 0);
        assert!(index.lookup("hello").is_none());
        assert!(v2 > v1); // Version incremented
    }

    #[test]
    fn test_version_increment() {
        let index = InvertedIndex::new();
        index.enable();

        let v0 = index.version();

        let doc_ref = test_doc_ref("test");
        index.index_document(&doc_ref, "hello", None);
        let v1 = index.version();

        index.remove_document(&doc_ref);
        let v2 = index.version();

        assert!(v1 > v0);
        assert!(v2 > v1);
    }

    #[test]
    fn test_compute_idf() {
        let index = InvertedIndex::new();
        index.enable();

        // Add 10 documents, "common" in all, "rare" in 1
        for i in 0..10 {
            let doc_ref = test_doc_ref(&format!("doc{}", i));
            if i == 0 {
                index.index_document(&doc_ref, "common rare", None);
            } else {
                index.index_document(&doc_ref, "common", None);
            }
        }

        let idf_common = index.compute_idf("common");
        let idf_rare = index.compute_idf("rare");

        // Rare terms should have higher IDF
        assert!(idf_rare > idf_common);
    }

    #[test]
    fn test_avg_doc_len() {
        let index = InvertedIndex::new();
        index.enable();

        let doc1 = test_doc_ref("doc1");
        let doc2 = test_doc_ref("doc2");

        index.index_document(&doc1, "one two", None); // 2 tokens
        index.index_document(&doc2, "one two three four", None); // 4 tokens

        // Average: (2 + 4) / 2 = 3.0
        assert!((index.avg_doc_len() - 3.0).abs() < 0.01);
    }

    #[test]
    fn test_wait_for_version() {
        use std::thread;

        let index = std::sync::Arc::new(InvertedIndex::new());
        index.enable();

        let index_clone = index.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            let doc_ref = test_doc_ref("test");
            index_clone.index_document(&doc_ref, "hello", None);
        });

        // Wait for version to increment
        let result = index.wait_for_version(1, Duration::from_secs(1));
        handle.join().unwrap();

        assert!(result);
        assert!(index.version() >= 1);
    }

    #[test]
    fn test_wait_for_version_timeout() {
        let index = InvertedIndex::new();

        // Version is 0, waiting for 100 should timeout
        let result = index.wait_for_version(100, Duration::from_millis(10));
        assert!(!result);
    }

    #[test]
    fn test_posting_list() {
        let mut list = PostingList::new();
        assert!(list.is_empty());

        list.add(PostingEntry::new(42, 1, 10));

        assert_eq!(list.len(), 1);
        assert!(!list.is_empty());

        let removed = list.remove_by_id(42);
        assert_eq!(removed, 1);
        assert!(list.is_empty());
    }

    #[test]
    fn test_resolve_doc_id() {
        let index = InvertedIndex::new();
        index.enable();

        let doc_ref = test_doc_ref("resolve_test");
        index.index_document(&doc_ref, "hello world", None);

        // The first doc should get doc_id 0
        let postings = index.lookup("hello").unwrap();
        let doc_id = postings.entries[0].doc_id;

        let resolved = index.resolve_doc_id(doc_id).unwrap();
        assert_eq!(resolved, doc_ref);
    }

    #[test]
    fn test_reindex_same_document() {
        let index = InvertedIndex::new();
        index.enable();

        let doc_ref = test_doc_ref("reindex");
        index.index_document(&doc_ref, "hello world", None);
        assert_eq!(index.total_docs(), 1);

        // Re-index same doc with different content (use pre-stemmed tokens)
        index.index_document(&doc_ref, "planet world extra", None);
        assert_eq!(index.total_docs(), 1);
        assert_eq!(index.doc_freq("hello"), 0); // old term gone
        assert_eq!(index.doc_freq("planet"), 1); // new term present
        assert_eq!(index.doc_freq("world"), 1); // shared term still 1
    }

    // ====================================================================
    // score_top_k tests
    // ====================================================================

    /// Helper: create a KV EntityRef with a specific branch_id
    fn kv_ref(branch_id: BranchId, key: &str) -> EntityRef {
        EntityRef::Kv {
            branch_id,
            key: key.to_string(),
        }
    }

    #[test]
    fn test_score_top_k_disabled_index() {
        let index = InvertedIndex::new();
        // Index is disabled by default
        let branch_id = BranchId::new();
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
        assert!(result.is_empty());
    }

    #[test]
    fn test_score_top_k_empty_query() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();
        let doc = kv_ref(branch_id, "doc1");
        index.index_document(&doc, "hello world", None);

        let result = index.score_top_k(&[], &branch_id, 10, 0.9, 0.4);
        assert!(result.is_empty());
    }

    #[test]
    fn test_score_top_k_k_zero() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();
        let doc = kv_ref(branch_id, "doc1");
        index.index_document(&doc, "hello world", None);

        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 0, 0.9, 0.4);
        assert!(result.is_empty());
    }

    #[test]
    fn test_score_top_k_no_matching_term() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();
        let doc = kv_ref(branch_id, "doc1");
        index.index_document(&doc, "hello world", None);

        // Query with a term that doesn't exist in the index
        let terms = vec!["nonexistent".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
        assert!(result.is_empty());
    }

    #[test]
    fn test_score_top_k_basic_single_term() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc1 = kv_ref(branch_id, "doc1");
        let doc2 = kv_ref(branch_id, "doc2");
        index.index_document(&doc1, "hello world", None);
        index.index_document(&doc2, "hello planet", None);

        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);

        // Both docs contain "hello", so both should appear
        assert_eq!(result.len(), 2);
        // All scores should be positive
        assert!(result[0].score > 0.0);
        assert!(result[1].score > 0.0);
        // Results should be sorted descending by score
        assert!(result[0].score >= result[1].score);
    }

    #[test]
    fn test_score_top_k_branch_filtering() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();

        let doc_a = kv_ref(branch_a, "doc_a");
        let doc_b = kv_ref(branch_b, "doc_b");
        index.index_document(&doc_a, "hello world", None);
        index.index_document(&doc_b, "hello planet", None);

        // Search branch_a: should only find doc_a
        let terms = vec!["hello".to_string()];
        let result_a = index.score_top_k(&terms, &branch_a, 10, 0.9, 0.4);
        assert_eq!(result_a.len(), 1);
        let resolved = index.resolve_doc_id(result_a[0].doc_id).unwrap();
        assert_eq!(resolved, doc_a);

        // Search branch_b: should only find doc_b
        let result_b = index.score_top_k(&terms, &branch_b, 10, 0.9, 0.4);
        assert_eq!(result_b.len(), 1);
        let resolved = index.resolve_doc_id(result_b[0].doc_id).unwrap();
        assert_eq!(resolved, doc_b);
    }

    #[test]
    fn test_score_top_k_respects_k_limit() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Index 20 documents all containing "common"
        for i in 0..20 {
            let doc = kv_ref(branch_id, &format!("doc{}", i));
            index.index_document(&doc, &format!("common word{}", i), None);
        }

        let terms = vec!["common".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 5, 0.9, 0.4);
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_score_top_k_multi_term_accumulation() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // doc1 matches both query terms, doc2 matches only one
        let doc1 = kv_ref(branch_id, "doc1");
        let doc2 = kv_ref(branch_id, "doc2");
        index.index_document(&doc1, "hello world", None);
        index.index_document(&doc2, "hello planet", None);

        let terms = vec!["hello".to_string(), "world".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);

        assert_eq!(result.len(), 2);
        // doc1 matches both terms, so it should score higher
        let doc1_id = index.doc_id_map.get(&doc1).unwrap();
        let doc1_score = result.iter().find(|r| r.doc_id == doc1_id).unwrap().score;
        let doc2_id = index.doc_id_map.get(&doc2).unwrap();
        let doc2_score = result.iter().find(|r| r.doc_id == doc2_id).unwrap().score;
        assert!(
            doc1_score > doc2_score,
            "doc1 ({}) matches both terms, should score higher than doc2 ({})",
            doc1_score,
            doc2_score
        );
    }

    #[test]
    fn test_score_top_k_idf_weighting() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // "common" appears in all 10 docs, "rare" appears in only 1
        for i in 0..10 {
            let doc = kv_ref(branch_id, &format!("doc{}", i));
            if i == 0 {
                index.index_document(&doc, "common rare", None);
            } else {
                index.index_document(&doc, "common filler", None);
            }
        }

        // Search for "rare" — only doc0 should match
        let rare_terms = vec!["rare".to_string()];
        let rare_result = index.score_top_k(&rare_terms, &branch_id, 10, 0.9, 0.4);
        assert_eq!(rare_result.len(), 1);

        // Search for "common" — all 10 docs match
        let common_terms = vec!["common".to_string()];
        let common_result = index.score_top_k(&common_terms, &branch_id, 10, 0.9, 0.4);
        assert_eq!(common_result.len(), 10);

        // The "rare" term score for doc0 should be higher than "common" term
        // score for doc0, because rare terms have higher IDF
        let doc0_id = index.doc_id_map.get(&kv_ref(branch_id, "doc0")).unwrap();
        let rare_score = rare_result
            .iter()
            .find(|r| r.doc_id == doc0_id)
            .unwrap()
            .score;
        let common_score = common_result
            .iter()
            .find(|r| r.doc_id == doc0_id)
            .unwrap()
            .score;
        assert!(
            rare_score > common_score,
            "rare term score ({}) should be higher than common term score ({})",
            rare_score,
            common_score
        );
    }

    #[test]
    fn test_score_top_k_higher_tf_scores_higher() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // doc1 has "hello" once, doc2 has "hello" three times
        let doc1 = kv_ref(branch_id, "doc1");
        let doc2 = kv_ref(branch_id, "doc2");
        // Keep doc_len similar so TF is the main differentiator
        index.index_document(&doc1, "hello filler padding", None);
        index.index_document(&doc2, "hello hello hello", None);

        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);

        assert_eq!(result.len(), 2);
        let doc1_id = index.doc_id_map.get(&doc1).unwrap();
        let doc2_id = index.doc_id_map.get(&doc2).unwrap();
        let doc1_score = result.iter().find(|r| r.doc_id == doc1_id).unwrap().score;
        let doc2_score = result.iter().find(|r| r.doc_id == doc2_id).unwrap().score;
        assert!(
            doc2_score > doc1_score,
            "doc2 (tf=3, score={}) should beat doc1 (tf=1, score={})",
            doc2_score,
            doc1_score
        );
    }

    #[test]
    fn test_score_top_k_descending_order() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Index several docs with varying relevance
        for i in 0..10 {
            let doc = kv_ref(branch_id, &format!("doc{}", i));
            // Repeat "target" i+1 times to create varying TF
            let text = format!("{} filler", "target ".repeat(i + 1));
            index.index_document(&doc, &text, None);
        }

        let terms = vec!["target".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);

        // Verify strictly descending order
        for window in result.windows(2) {
            assert!(
                window[0].score >= window[1].score,
                "Results not sorted: {} should be >= {}",
                window[0].score,
                window[1].score
            );
        }
    }

    #[test]
    fn test_score_top_k_scores_match_bm25_formula() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Single doc, single term: verify exact BM25 calculation
        let doc = kv_ref(branch_id, "doc1");
        index.index_document(&doc, "hello hello world", None);

        let k1 = 1.2_f32;
        let b = 0.75_f32;
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, k1, b);
        assert_eq!(result.len(), 1);

        // Manual BM25 calculation:
        // total_docs = 1, df("hello") = 1
        // idf = ln((1 - 1 + 0.5) / (1 + 0.5) + 1) = ln(1.333...) ≈ 0.2877
        let total_docs = 1.0_f32;
        let df = 1.0_f32;
        let expected_idf = ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln();

        // tf = 2 (hello appears twice), doc_len = 3, avg_doc_len = 3
        let tf = 2.0_f32;
        let dl = 3.0_f32;
        let avg_dl = 3.0_f32;
        let tf_comp = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl / avg_dl));
        let expected_score = expected_idf * tf_comp;

        assert!(
            (result[0].score - expected_score).abs() < 1e-5,
            "Score {} should match expected BM25 {}",
            result[0].score,
            expected_score
        );
    }

    #[test]
    fn test_score_top_k_nonexistent_branch() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_a = BranchId::new();
        let branch_nonexistent = BranchId::new();

        let doc = kv_ref(branch_a, "doc1");
        index.index_document(&doc, "hello world", None);

        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_nonexistent, 10, 0.9, 0.4);
        assert!(
            result.is_empty(),
            "No docs should match a non-existent branch"
        );
    }

    #[test]
    fn test_score_top_k_k_larger_than_matches() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc = kv_ref(branch_id, "doc1");
        index.index_document(&doc, "hello world", None);

        // Request k=100 but only 1 doc matches
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 100, 0.9, 0.4);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_score_top_k_after_document_removal() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc1 = kv_ref(branch_id, "doc1");
        let doc2 = kv_ref(branch_id, "doc2");
        index.index_document(&doc1, "hello world", None);
        index.index_document(&doc2, "hello planet", None);

        // Remove doc1
        index.remove_document(&doc1);

        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);

        // Only doc2 should remain
        assert_eq!(result.len(), 1);
        let resolved = index.resolve_doc_id(result[0].doc_id).unwrap();
        assert_eq!(resolved, doc2);
    }

    #[test]
    fn test_score_top_k_with_non_kv_entity() {
        // Ensure non-KV entities are handled correctly (filtered by branch_id)
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Index a KV entity and an Event entity on the same branch
        let kv_doc = EntityRef::Kv {
            branch_id,
            key: "doc1".to_string(),
        };
        let event_doc = EntityRef::Event {
            branch_id,
            sequence: 42,
        };
        index.index_document(&kv_doc, "hello world", None);
        index.index_document(&event_doc, "hello planet", None);

        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);

        // Both should match since we filter by branch_id, not entity type
        assert_eq!(result.len(), 2);
    }

    // ====================================================================
    // Segmented index tests
    // ====================================================================

    #[test]
    fn test_seal_active_segment() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Index some documents
        for i in 0..10 {
            let doc = kv_ref(branch_id, &format!("doc{}", i));
            index.index_document(&doc, &format!("hello world extra{}", i), None);
        }

        assert_eq!(index.total_docs(), 10);
        assert_eq!(index.active_doc_count.load(Ordering::Relaxed), 10);

        // Seal the active segment
        index.seal_active();

        // Active segment should be empty now
        assert_eq!(index.active_doc_count.load(Ordering::Relaxed), 0);
        assert!(index.postings.is_empty());

        // But sealed segment should have the data
        let sealed = index.sealed.read().unwrap();
        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0].doc_count(), 10);

        // Total docs should be unchanged
        assert_eq!(index.total_docs(), 10);

        // doc_freq should still work (from sealed segment)
        assert_eq!(index.doc_freq("hello"), 10);

        // score_top_k should find results in sealed segment
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 20, 0.9, 0.4);
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn test_cross_segment_search() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Index 5 docs, seal, then index 5 more
        for i in 0..5 {
            let doc = kv_ref(branch_id, &format!("doc{}", i));
            index.index_document(&doc, "hello world", None);
        }
        index.seal_active();

        for i in 5..10 {
            let doc = kv_ref(branch_id, &format!("doc{}", i));
            index.index_document(&doc, "hello planet", None);
        }

        // Should find results from both segments
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 20, 0.9, 0.4);
        assert_eq!(result.len(), 10);

        // doc_freq should sum across segments
        assert_eq!(index.doc_freq("hello"), 10);
        assert_eq!(index.doc_freq("world"), 5);
        assert_eq!(index.doc_freq("planet"), 5);
    }

    #[test]
    fn test_reindex_after_seal() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Index a doc with initial content
        let doc = kv_ref(branch_id, "doc1");
        index.index_document(&doc, "hello world", None);
        assert_eq!(index.total_docs(), 1);

        // Seal the active segment
        index.seal_active();

        // Re-index same doc with different content (use stemming-stable words)
        index.index_document(&doc, "alpha planet", None);

        // total_docs should still be 1 (re-index, not new doc)
        assert_eq!(index.total_docs(), 1);

        // Old terms should not match (tombstoned in sealed segment)
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
        assert!(
            result.is_empty(),
            "Old term 'hello' should not match after re-index"
        );

        // New terms should match (in active segment)
        let terms = vec!["alpha".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
        assert_eq!(result.len(), 1, "New term 'alpha' should match");

        let resolved = index.resolve_doc_id(result[0].doc_id).unwrap();
        assert_eq!(resolved, doc);
    }

    #[test]
    fn test_remove_from_sealed_segment() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc1 = kv_ref(branch_id, "doc1");
        let doc2 = kv_ref(branch_id, "doc2");
        index.index_document(&doc1, "hello world", None);
        index.index_document(&doc2, "hello planet", None);
        assert_eq!(index.total_docs(), 2);

        // Seal
        index.seal_active();

        // Remove doc1 (from sealed segment)
        index.remove_document(&doc1);

        // total_docs should be 1
        assert_eq!(index.total_docs(), 1);

        // Search should only find doc2
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
        assert_eq!(result.len(), 1);
        let resolved = index.resolve_doc_id(result[0].doc_id).unwrap();
        assert_eq!(resolved, doc2);
    }

    #[test]
    fn test_total_doc_len_after_sealed_removal() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc1 = kv_ref(branch_id, "doc1");
        let doc2 = kv_ref(branch_id, "doc2");
        // "hello world" = 2 tokens, "goodbye planet earth" = 3 tokens
        index.index_document(&doc1, "hello world", None);
        index.index_document(&doc2, "goodbye planet earth", None);
        assert_eq!(index.total_doc_len.load(Ordering::Relaxed), 5); // 2 + 3

        // Seal
        index.seal_active();

        // Remove doc1 (2 tokens) — should decrement total_doc_len
        index.remove_document(&doc1);
        assert_eq!(index.total_doc_len.load(Ordering::Relaxed), 3); // 5 - 2
        assert_eq!(index.total_docs(), 1);
    }

    #[test]
    fn test_multiple_seals_with_interleaved_searches() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // First batch: 5 docs
        for i in 0..5 {
            let doc = kv_ref(branch_id, &format!("batch1_doc{}", i));
            index.index_document(&doc, "alpha beta", None);
        }
        index.seal_active();

        // Search after first seal
        let terms = vec!["alpha".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 20, 0.9, 0.4);
        assert_eq!(result.len(), 5);

        // Second batch: 5 more docs
        for i in 0..5 {
            let doc = kv_ref(branch_id, &format!("batch2_doc{}", i));
            index.index_document(&doc, "alpha gamma", None);
        }
        index.seal_active();

        // Search after second seal — should span both sealed segments
        let result = index.score_top_k(&terms, &branch_id, 20, 0.9, 0.4);
        assert_eq!(result.len(), 10);

        // Third batch: 3 more docs still in active
        for i in 0..3 {
            let doc = kv_ref(branch_id, &format!("batch3_doc{}", i));
            index.index_document(&doc, "alpha delta", None);
        }

        // Search across 2 sealed + 1 active
        let result = index.score_top_k(&terms, &branch_id, 20, 0.9, 0.4);
        assert_eq!(result.len(), 13);

        assert_eq!(index.total_docs(), 13);
        assert_eq!(index.doc_freq("alpha"), 13);
        assert_eq!(index.doc_freq("beta"), 5);
        assert_eq!(index.doc_freq("gamma"), 5);
        assert_eq!(index.doc_freq("delta"), 3);
    }

    #[test]
    fn test_tombstone_persistence_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        // Create index, add docs, seal, tombstone one, freeze
        {
            let index = InvertedIndex::new();
            index.enable();
            index.set_data_dir(tmp.path().to_path_buf());

            let doc1 = kv_ref(branch_id, "doc1");
            let doc2 = kv_ref(branch_id, "doc2");
            index.index_document(&doc1, "hello world", None);
            index.index_document(&doc2, "hello planet", None);

            index.seal_active();
            index.remove_document(&doc1);
            assert_eq!(index.total_docs(), 1);

            index.freeze_to_disk().unwrap();
        }

        // Load from disk and verify tombstones survived
        {
            let index = InvertedIndex::new();
            index.set_data_dir(tmp.path().to_path_buf());
            let loaded = index.load_from_disk().unwrap();
            assert!(loaded);
            index.enable();

            // total_docs should reflect the tombstone
            assert_eq!(index.total_docs(), 1);

            // Search should only find doc2
            let terms = vec!["hello".to_string()];
            let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
            assert_eq!(result.len(), 1);
            let resolved = index.resolve_doc_id(result[0].doc_id).unwrap();
            assert_eq!(resolved, kv_ref(branch_id, "doc2"));
        }
    }

    #[test]
    fn test_freeze_and_load_preserves_doc_id_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let doc1 = kv_ref(branch_id, "key_alpha");
        let doc2 = kv_ref(branch_id, "key_beta");

        {
            let index = InvertedIndex::new();
            index.enable();
            index.set_data_dir(tmp.path().to_path_buf());

            index.index_document(&doc1, "hello world", None);
            index.index_document(&doc2, "hello planet", None);

            index.freeze_to_disk().unwrap();
        }

        {
            let index = InvertedIndex::new();
            index.set_data_dir(tmp.path().to_path_buf());
            index.load_from_disk().unwrap();
            index.enable();

            let terms = vec!["hello".to_string()];
            let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
            assert_eq!(result.len(), 2);

            // Verify exact EntityRef identity (not just is_some)
            let resolved: Vec<EntityRef> = result
                .iter()
                .map(|r| index.resolve_doc_id(r.doc_id).unwrap())
                .collect();
            assert!(resolved.contains(&doc1));
            assert!(resolved.contains(&doc2));
        }
    }

    #[test]
    fn test_score_top_k_sealed_matches_bm25_formula() {
        // Verify the precomputed BM25 constants produce correct scores
        // through the sealed segment (posting_iter) path.
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc = kv_ref(branch_id, "doc1");
        index.index_document(&doc, "hello hello world", None);

        // Seal so scoring goes through posting_iter path
        index.seal_active();

        let k1 = 1.2_f32;
        let b = 0.75_f32;
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, k1, b);
        assert_eq!(result.len(), 1);

        // Manual BM25 calculation (same as active-segment test)
        let total_docs = 1.0_f32;
        let df = 1.0_f32;
        let expected_idf = ((total_docs - df + 0.5) / (df + 0.5) + 1.0).ln();
        let tf = 2.0_f32;
        let dl = 3.0_f32;
        let avg_dl = 3.0_f32;
        let tf_comp = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * dl / avg_dl));
        let expected_score = expected_idf * tf_comp;

        assert!(
            (result[0].score - expected_score).abs() < 1e-5,
            "Sealed score {} should match expected BM25 {}",
            result[0].score,
            expected_score
        );
    }

    #[test]
    fn test_score_top_k_sealed_tombstone_with_posting_iter() {
        // Ensure tombstone filtering works with posting_iter (not posting_entries)
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc1 = kv_ref(branch_id, "doc1");
        let doc2 = kv_ref(branch_id, "doc2");
        let doc3 = kv_ref(branch_id, "doc3");
        index.index_document(&doc1, "hello world", None);
        index.index_document(&doc2, "hello planet", None);
        index.index_document(&doc3, "hello galaxy", None);

        index.seal_active();

        // Tombstone the middle doc
        index.remove_document(&doc2);

        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
        assert_eq!(result.len(), 2);

        let resolved: Vec<EntityRef> = result
            .iter()
            .map(|r| index.resolve_doc_id(r.doc_id).unwrap())
            .collect();
        assert!(resolved.contains(&doc1));
        assert!(resolved.contains(&doc3));
        assert!(!resolved.contains(&doc2));
    }

    #[test]
    fn test_single_branch_detection() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        for i in 0..5 {
            let doc = kv_ref(branch_id, &format!("doc{}", i));
            index.index_document(&doc, "hello world", None);
        }

        // Only one branch — branch_ids should have exactly 1 entry
        assert_eq!(index.branch_ids.read().unwrap().len(), 1);
        assert!(index.branch_ids.read().unwrap().contains(&branch_id));

        // Search should still work correctly
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_multi_branch_detection() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();

        // Index docs on two branches
        for i in 0..3 {
            let doc = kv_ref(branch_a, &format!("a_doc{}", i));
            index.index_document(&doc, "hello world", None);
        }
        for i in 0..3 {
            let doc = kv_ref(branch_b, &format!("b_doc{}", i));
            index.index_document(&doc, "hello planet", None);
        }

        // Two branches — branch_ids should have exactly 2 entries
        assert_eq!(index.branch_ids.read().unwrap().len(), 2);

        // Search should filter correctly per branch
        let terms = vec!["hello".to_string()];
        let result_a = index.score_top_k(&terms, &branch_a, 10, 0.9, 0.4);
        assert_eq!(result_a.len(), 3);
        let result_b = index.score_top_k(&terms, &branch_b, 10, 0.9, 0.4);
        assert_eq!(result_b.len(), 3);
    }

    #[test]
    fn test_single_branch_wrong_branch_returns_empty() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();

        // Index only on branch_a
        let doc = kv_ref(branch_a, "doc1");
        index.index_document(&doc, "hello world", None);

        // Search for branch_b (not in index) — should return empty via early return
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_b, 10, 0.9, 0.4);
        assert!(result.is_empty());
    }

    #[test]
    fn test_freeze_and_load() {
        let tmp = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        // Create index, add docs, freeze
        {
            let index = InvertedIndex::new();
            index.enable();
            index.set_data_dir(tmp.path().to_path_buf());

            for i in 0..20 {
                let doc = kv_ref(branch_id, &format!("doc{}", i));
                index.index_document(&doc, &format!("hello world extra{}", i), None);
            }

            index.freeze_to_disk().unwrap();
        }

        // Create new index, load from disk
        {
            let index = InvertedIndex::new();
            index.set_data_dir(tmp.path().to_path_buf());
            let loaded = index.load_from_disk().unwrap();
            assert!(loaded);

            assert_eq!(index.total_docs(), 20);
            assert_eq!(index.doc_freq("hello"), 20);

            // Enable and search
            index.enable();
            let terms = vec!["hello".to_string()];
            let result = index.score_top_k(&terms, &branch_id, 30, 0.9, 0.4);
            assert_eq!(result.len(), 20);

            // Verify doc_id resolution works
            let resolved = index.resolve_doc_id(result[0].doc_id);
            assert!(resolved.is_some());
        }
    }

    // ====================================================================
    // Deeper tests for QPS optimizations
    // ====================================================================

    #[test]
    fn test_multi_term_sealed_bm25_formula() {
        // Verify multi-term BM25 through sealed segments produces correct
        // accumulated scores (tests Vec accumulator + cached find_term).
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc = kv_ref(branch_id, "doc1");
        index.index_document(&doc, "hello hello world", None);
        index.seal_active();

        let k1 = 1.2_f32;
        let b = 0.75_f32;
        let terms = vec!["hello".to_string(), "world".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, k1, b);
        assert_eq!(result.len(), 1);

        // Manual BM25: N=1, avg_dl=3
        let n = 1.0_f32;
        let avg_dl = 3.0_f32;
        let dl = 3.0_f32;

        // "hello": tf=2, df=1
        let idf_hello = ((n - 1.0 + 0.5) / (1.0 + 0.5) + 1.0).ln();
        let tf_hello = (2.0 * (k1 + 1.0)) / (2.0 + k1 * (1.0 - b + b * dl / avg_dl));

        // "world": tf=1, df=1
        let idf_world = ((n - 1.0 + 0.5) / (1.0 + 0.5) + 1.0).ln();
        let tf_world = (1.0 * (k1 + 1.0)) / (1.0 + k1 * (1.0 - b + b * dl / avg_dl));

        let expected = idf_hello * tf_hello + idf_world * tf_world;
        assert!(
            (result[0].score - expected).abs() < 1e-5,
            "Multi-term sealed score {} should match expected {}",
            result[0].score,
            expected
        );
    }

    #[test]
    fn test_mixed_tombstone_segments() {
        // Test scoring across segments where some have tombstones and some don't.
        // Note: remove_document() adds tombstones to ALL sealed segments (harmless
        // if doc not present in that segment's postings). The has_tombstones() fast
        // path helps when NO removals have happened at all.
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Segment 0: 2 docs, no tombstones
        let doc1 = kv_ref(branch_id, "doc1");
        let doc2 = kv_ref(branch_id, "doc2");
        index.index_document(&doc1, "hello world", None);
        index.index_document(&doc2, "hello planet", None);
        index.seal_active();

        // Segment 1: 2 docs, no tombstones
        let doc3 = kv_ref(branch_id, "doc3");
        let doc4 = kv_ref(branch_id, "doc4");
        index.index_document(&doc3, "hello earth", None);
        index.index_document(&doc4, "hello mars", None);
        index.seal_active();

        // Before any removals: both segments have no tombstones
        {
            let sealed = index.sealed.read().unwrap();
            assert!(
                !sealed[0].has_tombstones(),
                "Segment 0 should have no tombstones before removal"
            );
            assert!(
                !sealed[1].has_tombstones(),
                "Segment 1 should have no tombstones before removal"
            );
        }

        // All 4 docs found
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
        assert_eq!(result.len(), 4);

        // Now remove doc2 — sets has_tombstones on all segments
        index.remove_document(&doc2);

        {
            let sealed = index.sealed.read().unwrap();
            // remove_document adds tombstones to all segments
            assert!(sealed[0].has_tombstones());
            assert!(sealed[1].has_tombstones());
        }

        // Search should find 3 docs (doc2 tombstoned)
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
        assert_eq!(result.len(), 3);

        let resolved: Vec<EntityRef> = result
            .iter()
            .map(|r| index.resolve_doc_id(r.doc_id).unwrap())
            .collect();
        assert!(
            !resolved.contains(&doc2),
            "Tombstoned doc2 should not appear"
        );
        assert!(resolved.contains(&doc1));
        assert!(resolved.contains(&doc3));
        assert!(resolved.contains(&doc4));
    }

    #[test]
    fn test_single_branch_sealed_search() {
        // Verify single-branch optimization works through sealed segments.
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        for i in 0..10 {
            let doc = kv_ref(branch_id, &format!("doc{}", i));
            index.index_document(&doc, "hello world", None);
        }
        index.seal_active();

        // Single branch, all docs sealed
        assert_eq!(index.branch_ids.read().unwrap().len(), 1);

        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 20, 0.9, 0.4);
        assert_eq!(result.len(), 10);

        // Wrong branch should still return empty
        let other_branch = BranchId::new();
        let result = index.score_top_k(&terms, &other_branch, 20, 0.9, 0.4);
        assert!(result.is_empty());
    }

    #[test]
    fn test_branch_ids_after_load_from_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        {
            let index = InvertedIndex::new();
            index.enable();
            index.set_data_dir(tmp.path().to_path_buf());

            for i in 0..5 {
                let doc = kv_ref(branch_id, &format!("doc{}", i));
                index.index_document(&doc, "hello world", None);
            }
            index.freeze_to_disk().unwrap();
        }

        {
            let index = InvertedIndex::new();
            index.set_data_dir(tmp.path().to_path_buf());
            index.load_from_disk().unwrap();
            index.enable();

            // branch_ids should be rebuilt from DocIdMap
            assert_eq!(index.branch_ids.read().unwrap().len(), 1);
            assert!(index.branch_ids.read().unwrap().contains(&branch_id));

            // Single-branch optimization should work after load
            let terms = vec!["hello".to_string()];
            let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
            assert_eq!(result.len(), 5);

            let other_branch = BranchId::new();
            let result = index.score_top_k(&terms, &other_branch, 10, 0.9, 0.4);
            assert!(result.is_empty());
        }
    }

    #[test]
    fn test_has_tombstones_after_load_from_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        {
            let index = InvertedIndex::new();
            index.enable();
            index.set_data_dir(tmp.path().to_path_buf());

            let doc1 = kv_ref(branch_id, "doc1");
            let doc2 = kv_ref(branch_id, "doc2");
            index.index_document(&doc1, "hello world", None);
            index.index_document(&doc2, "hello planet", None);
            index.seal_active();
            index.remove_document(&doc1);
            index.freeze_to_disk().unwrap();
        }

        {
            let index = InvertedIndex::new();
            index.set_data_dir(tmp.path().to_path_buf());
            index.load_from_disk().unwrap();
            index.enable();

            // has_tombstones should be true after loading tombstoned segment
            let sealed = index.sealed.read().unwrap();
            assert!(sealed[0].has_tombstones());
            drop(sealed);

            // Search should still work correctly (only doc2)
            let terms = vec!["hello".to_string()];
            let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);
            assert_eq!(result.len(), 1);
            let resolved = index.resolve_doc_id(result[0].doc_id).unwrap();
            assert_eq!(resolved, kv_ref(branch_id, "doc2"));
        }
    }

    #[test]
    fn test_multi_branch_sealed_filtering() {
        // Verify branch filtering works through sealed segments
        // (exercises the !skip_branch_check path in sealed scoring).
        let index = InvertedIndex::new();
        index.enable();
        let branch_a = BranchId::new();
        let branch_b = BranchId::new();

        let doc_a1 = kv_ref(branch_a, "a1");
        let doc_a2 = kv_ref(branch_a, "a2");
        let doc_b1 = kv_ref(branch_b, "b1");
        index.index_document(&doc_a1, "hello world", None);
        index.index_document(&doc_a2, "hello planet", None);
        index.index_document(&doc_b1, "hello galaxy", None);
        index.seal_active();

        // multi-branch, so skip_branch_check = false
        assert_eq!(index.branch_ids.read().unwrap().len(), 2);

        let terms = vec!["hello".to_string()];
        let result_a = index.score_top_k(&terms, &branch_a, 10, 0.9, 0.4);
        assert_eq!(result_a.len(), 2);

        let result_b = index.score_top_k(&terms, &branch_b, 10, 0.9, 0.4);
        assert_eq!(result_b.len(), 1);
        let resolved = index.resolve_doc_id(result_b[0].doc_id).unwrap();
        assert_eq!(resolved, doc_b1);
    }

    #[test]
    fn test_cross_segment_multi_term_accumulation() {
        // A doc's terms are in one segment, but different query terms hit
        // different docs across sealed + active segments.
        // Verifies the dense Vec accumulator works across segment boundaries.
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Sealed segment: doc1 has "alpha beta", doc2 has "alpha"
        let doc1 = kv_ref(branch_id, "doc1");
        let doc2 = kv_ref(branch_id, "doc2");
        index.index_document(&doc1, "alpha beta", None);
        index.index_document(&doc2, "alpha gamma", None);
        index.seal_active();

        // Active segment: doc3 has "alpha beta gamma"
        let doc3 = kv_ref(branch_id, "doc3");
        index.index_document(&doc3, "alpha beta gamma", None);

        // Multi-term query: "alpha beta"
        let terms = vec!["alpha".to_string(), "beta".to_string()];
        let result = index.score_top_k(&terms, &branch_id, 10, 0.9, 0.4);

        // All 3 docs match "alpha", doc1 and doc3 also match "beta"
        assert_eq!(result.len(), 3);

        // doc1 and doc3 match both terms, doc2 only matches "alpha"
        let doc1_id = index.doc_id_map.get(&doc1).unwrap();
        let doc2_id = index.doc_id_map.get(&doc2).unwrap();
        let doc3_id = index.doc_id_map.get(&doc3).unwrap();

        let doc1_score = result.iter().find(|r| r.doc_id == doc1_id).unwrap().score;
        let doc2_score = result.iter().find(|r| r.doc_id == doc2_id).unwrap().score;
        let doc3_score = result.iter().find(|r| r.doc_id == doc3_id).unwrap().score;

        // Both-term docs should score higher than single-term doc
        assert!(
            doc1_score > doc2_score,
            "doc1 ({}) > doc2 ({})",
            doc1_score,
            doc2_score
        );
        assert!(
            doc3_score > doc2_score,
            "doc3 ({}) > doc2 ({})",
            doc3_score,
            doc2_score
        );
    }
}
