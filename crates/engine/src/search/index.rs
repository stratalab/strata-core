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
use super::tokenizer::tokenize_with_positions;
use super::types::EntityRef;
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use smallvec::SmallVec;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};
use strata_core::id::CommitVersion;
use strata_core::types::BranchId;

// ============================================================================
// Phrase matching
// ============================================================================

/// Configuration for phrase matching in `score_top_k`.
///
/// Bundled into a struct to keep the function signature clean.
/// When `phrases` is empty, phrase matching is skipped entirely (zero overhead).
pub struct PhraseConfig<'a> {
    /// Quoted phrases, each as a list of stemmed terms.
    pub phrases: &'a [Vec<String>],
    /// Score multiplier per matching phrase (boost mode). Default: 2.0.
    pub boost: f32,
    /// Maximum word gap between phrase terms. Default: 0.
    pub slop: u32,
    /// true = filter mode (AND: all phrases must match), false = boost mode.
    pub filter: bool,
}

impl<'a> PhraseConfig<'a> {
    /// No-op config: no phrases, zero overhead.
    pub fn none() -> Self {
        PhraseConfig {
            phrases: &[],
            boost: 2.0,
            slop: 0,
            filter: false,
        }
    }
}

/// Check if terms appear as a phrase with the given slop tolerance.
///
/// `term_positions[i]` contains the sorted positions of the i-th phrase term
/// in a document. For slop=0, terms must be at consecutive positions.
/// For slop=N, up to N total extra positions allowed between first and last term.
///
/// Uses a greedy approach: for each starting position of the first term,
/// find the nearest valid position for each subsequent term.
fn check_phrase_match(term_positions: &[SmallVec<[u32; 4]>], slop: u32) -> bool {
    let n = term_positions.len();
    if n < 2 {
        return true;
    }

    'outer: for &start_pos in &term_positions[0] {
        let mut prev = start_pos;
        let mut total_gap = 0u32;

        for term_pos in &term_positions[1..] {
            // Positions are sorted — first element > prev is the nearest
            let next = term_pos.iter().copied().find(|&p| p > prev);
            match next {
                Some(p) => {
                    let gap = p - prev - 1;
                    total_gap += gap;
                    if total_gap > slop {
                        continue 'outer;
                    }
                    prev = p;
                }
                None => continue 'outer,
            }
        }
        return true;
    }
    false
}

// ============================================================================
// Proximity scoring
// ============================================================================

/// Configuration for proximity scoring in `score_top_k`.
///
/// When `enabled` is false or the query has fewer than 2 terms, proximity
/// scoring is skipped entirely (zero overhead).
pub struct ProximityConfig {
    /// Enable proximity scoring. Default: true.
    pub enabled: bool,
    /// Window size for proximity normalization (in word positions).
    /// Smaller values reward tighter clustering more aggressively. Default: 10.
    pub window: u32,
    /// Weight of the additive proximity boost. Default: 0.5.
    /// Set to 0.0 to disable without turning off position gathering.
    pub weight: f32,
}

impl ProximityConfig {
    /// Default proximity config: enabled with window=10, weight=0.5.
    pub fn default_on() -> Self {
        ProximityConfig {
            enabled: true,
            window: 10,
            weight: 0.5,
        }
    }

    /// Disabled proximity config (zero overhead).
    pub fn off() -> Self {
        ProximityConfig {
            enabled: false,
            window: 10,
            weight: 0.0,
        }
    }
}

/// Compute the minimum span (smallest window containing one position from each term).
///
/// Uses a sliding-window approach over sorted position lists:
/// 1. Initialize one pointer per term at the first position.
/// 2. The span is max(positions) - min(positions) + 1.
/// 3. Advance the pointer at the minimum position.
/// 4. Track the global minimum span.
///
/// Returns `None` if any term has an empty position list.
/// Time: O(S × T) where S = total positions across all terms, T = number of terms.
/// Linear scan over T pointers is faster than a heap for typical query sizes (T ≤ 10).
fn compute_min_span(term_positions: &[SmallVec<[u32; 4]>]) -> Option<u32> {
    let n = term_positions.len();
    if n < 2 {
        return Some(1);
    }
    if term_positions.iter().any(|p| p.is_empty()) {
        return None;
    }

    // Pointers into each term's sorted position list
    let mut ptrs = vec![0usize; n];
    let mut best_span = u32::MAX;

    loop {
        // Find current min and max positions across all pointers
        let mut min_pos = u32::MAX;
        let mut max_pos = 0u32;
        let mut min_idx = 0;

        for (i, &ptr) in ptrs.iter().enumerate() {
            let pos = term_positions[i][ptr];
            if pos < min_pos {
                min_pos = pos;
                min_idx = i;
            }
            if pos > max_pos {
                max_pos = pos;
            }
        }

        let span = max_pos - min_pos + 1;
        if span < best_span {
            best_span = span;
        }

        // Advance the pointer at the minimum position
        ptrs[min_idx] += 1;
        if ptrs[min_idx] >= term_positions[min_idx].len() {
            break; // exhausted one term's positions
        }
    }

    Some(best_span)
}

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

/// List of documents containing a term.
///
/// Stores compact [`PostingEntry`] values (12 bytes, Copy) for BM25 scoring,
/// plus a parallel positions vector for phrase/proximity queries. The two
/// vectors are kept in sync: `positions[i]` contains the term positions for
/// `entries[i]`.
#[derive(Debug, Clone, Default)]
pub struct PostingList {
    /// Document entries (doc_id, tf, doc_len) — used by BM25 scoring.
    pub entries: Vec<PostingEntry>,
    /// Per-entry term positions. `positions[i]` holds the positions for
    /// `entries[i]`. SmallVec inlines up to 4 positions on the stack.
    pub positions: Vec<SmallVec<[u32; 4]>>,
}

impl PostingList {
    /// Create a new empty posting list.
    pub fn new() -> Self {
        PostingList {
            entries: Vec::new(),
            positions: Vec::new(),
        }
    }

    /// Add an entry without position data (backward-compat path).
    pub fn add(&mut self, entry: PostingEntry) {
        self.entries.push(entry);
        self.positions.push(SmallVec::new());
    }

    /// Add an entry with position data.
    pub fn add_with_positions(&mut self, entry: PostingEntry, pos: SmallVec<[u32; 4]>) {
        self.entries.push(entry);
        self.positions.push(pos);
    }

    /// Remove entries matching a doc_id. Returns number removed.
    pub fn remove_by_id(&mut self, doc_id: u32) -> usize {
        let before = self.entries.len();
        let mut i = 0;
        while i < self.entries.len() {
            if self.entries[i].doc_id == doc_id {
                self.entries.swap_remove(i);
                self.positions.swap_remove(i);
            } else {
                i += 1;
            }
        }
        before - self.entries.len()
    }

    /// Number of documents containing this term.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if posting list is empty.
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
///
/// # Memory Ordering
///
/// The metric counters (`total_docs`, `total_doc_len`, `active_doc_count`) use
/// `Relaxed` ordering for mutations (fetch_add/fetch_sub/store) because:
/// 1. They are observational metrics — approximate counts are acceptable.
/// 2. They do not synchronize any other memory operations.
/// 3. Atomic RMW instructions guarantee no torn reads regardless of ordering.
///
/// The *read* side uses `Acquire` in BM25-scoring methods (`total_docs()`,
/// `avg_doc_len()`, `compute_idf()`) for stronger visibility guarantees when
/// computing scores that depend on these values. For internal bookkeeping reads
/// (e.g. seal threshold checks), `Relaxed` is used since approximate values
/// are sufficient.
///
/// The `sealing` flag uses `Acquire` on the CAS success path and `Release` on
/// the subsequent store to establish a critical section that prevents concurrent
/// auto-seals.
pub struct InvertedIndex {
    // --- Active segment (DashMap-based, mutable) ---
    /// Term -> PostingList mapping (active segment)
    postings: DashMap<String, PostingList>,
    /// Term -> document frequency (active segment only)
    doc_freqs: DashMap<String, usize>,
    /// doc_id -> document length (indexed by u32 doc_id, active segment)
    doc_lengths: RwLock<Vec<Option<u32>>>,
    /// doc_id -> terms indexed for that document (forward index for O(terms) removal)
    doc_terms: RwLock<Vec<Option<Vec<String>>>>,
    /// doc_id -> content fingerprint for fast reconcile.
    doc_content_hashes: RwLock<Vec<Option<[u8; 32]>>>,

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
            doc_terms: RwLock::new(Vec::new()),
            doc_content_hashes: RwLock::new(Vec::new()),
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
        self.doc_terms.write().unwrap().clear();
        self.doc_content_hashes.write().unwrap().clear();
        self.doc_id_map.clear();
        self.sealed.write().unwrap().clear();
        // Relaxed: clear() is called during logical reset — the subsequent
        // version bump (Release) establishes the visibility fence.
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

    /// Check if a document is already present in the index.
    pub fn has_document(&self, doc_ref: &EntityRef) -> bool {
        let Some(doc_id) = self.doc_id_map.get(doc_ref) else {
            return false;
        };
        self.doc_lengths
            .read()
            .unwrap()
            .get(doc_id as usize)
            .copied()
            .flatten()
            .is_some()
    }

    /// Check whether the live indexed content matches `text`.
    pub(crate) fn document_matches_text(&self, doc_ref: &EntityRef, text: &str) -> bool {
        let Some(doc_id) = self.doc_id_map.get(doc_ref) else {
            return false;
        };
        self.doc_content_hashes
            .read()
            .unwrap()
            .get(doc_id as usize)
            .copied()
            .flatten()
            .is_some_and(|existing| existing == Self::content_fingerprint(text))
    }

    /// Check if index is at least at given version
    pub fn is_at_version(&self, min_version: CommitVersion) -> bool {
        self.version.load(Ordering::Acquire) >= min_version.as_u64()
    }

    /// Wait for index to reach a version (with timeout)
    ///
    /// Returns true if version was reached, false on timeout.
    pub fn wait_for_version(&self, version: CommitVersion, timeout: Duration) -> bool {
        let start = Instant::now();
        loop {
            if self.version.load(Ordering::Acquire) >= version.as_u64() {
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

    /// Snapshot all entity refs currently tracked by the index.
    pub(crate) fn entity_refs_snapshot(&self) -> Vec<EntityRef> {
        self.doc_id_map
            .id_to_ref
            .read()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }

    fn content_fingerprint(text: &str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(text.as_bytes());
        hasher.finalize().into()
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
    #[allow(clippy::too_many_arguments)]
    pub fn score_top_k(
        &self,
        query_terms: &[String],
        branch_id: &BranchId,
        k: usize,
        scorer_k1: f32,
        scorer_b: f32,
        phrase_cfg: &PhraseConfig<'_>,
        prox_cfg: &ProximityConfig,
        requested_space: Option<&str>,
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
                    // Combined branch + space filter (one map lookup).
                    // When `skip_branch_check` is true (single-branch index)
                    // and no space filter is requested, we skip the lookup
                    // entirely — preserving the existing fast path.
                    //
                    // `EntityRef::Branch` returns `None` for `space()` and is
                    // kept unconditionally — those refs are space-less metadata.
                    if !skip_branch_check || requested_space.is_some() {
                        match id_to_ref.get(entry.doc_id as usize) {
                            Some(entity_ref) => {
                                if !skip_branch_check && entity_ref.branch_id() != *branch_id {
                                    continue;
                                }
                                if let Some(s) = requested_space {
                                    if let Some(es) = entity_ref.space() {
                                        if es != s {
                                            continue;
                                        }
                                    }
                                    // entity_ref.space() == None → space-less
                                    // ref (Branch); keep it.
                                }
                            }
                            None => continue,
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
                    // Combined branch + space filter (mirrors active loop).
                    // Branch refs (space() == None) are kept unconditionally.
                    if !skip_branch_check || requested_space.is_some() {
                        match id_to_ref.get(entry.doc_id as usize) {
                            Some(entity_ref) => {
                                if !skip_branch_check && entity_ref.branch_id() != *branch_id {
                                    continue;
                                }
                                if let Some(s) = requested_space {
                                    if let Some(es) = entity_ref.space() {
                                        if es != s {
                                            continue;
                                        }
                                    }
                                }
                            }
                            None => continue,
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
        // --- Phase 2: Phrase matching (zero overhead when no phrases) ---
        if !phrase_cfg.phrases.is_empty() {
            // Per-doc count of how many phrases matched.
            // Filter mode: doc must match ALL phrases (AND).
            // Boost mode: score multiplied by phrase_boost per matching phrase.
            let mut phrase_match_count = vec![0u32; num_docs];
            let mut num_real_phrases = 0u32;

            for phrase in phrase_cfg.phrases {
                if phrase.len() < 2 {
                    continue;
                }
                num_real_phrases += 1;
                let num_terms = phrase.len();

                // Build doc_id → [positions_for_term_0, ..., positions_for_term_n]
                let mut doc_positions: HashMap<u32, Vec<SmallVec<[u32; 4]>>> = HashMap::new();

                // Active segment positions
                for (term_idx, term) in phrase.iter().enumerate() {
                    if let Some(posting_list) = self.postings.get(term.as_str()) {
                        for (entry_idx, entry) in posting_list.entries.iter().enumerate() {
                            let did = entry.doc_id as usize;
                            if did >= num_docs || !seen[did] {
                                continue;
                            }
                            let e = doc_positions
                                .entry(entry.doc_id)
                                .or_insert_with(|| vec![SmallVec::new(); num_terms]);
                            e[term_idx] = posting_list.positions[entry_idx].clone();
                        }
                    }
                }

                // Sealed segment positions
                for seg in sealed.iter() {
                    for (term_idx, term) in phrase.iter().enumerate() {
                        if let Some((iter, mut pr)) = seg.term_with_positions(term) {
                            for entry in iter {
                                let did = entry.doc_id as usize;
                                if did >= num_docs || !seen[did] {
                                    pr.skip_positions(entry.tf);
                                    continue;
                                }
                                let positions = pr.read_positions(entry.tf);
                                let e = doc_positions
                                    .entry(entry.doc_id)
                                    .or_insert_with(|| vec![SmallVec::new(); num_terms]);
                                e[term_idx] = positions;
                            }
                        }
                    }
                }

                // Check phrase adjacency for each candidate doc
                for (&doc_id, term_positions) in &doc_positions {
                    if term_positions.iter().any(|p| p.is_empty()) {
                        continue;
                    }
                    if check_phrase_match(term_positions, phrase_cfg.slop) {
                        phrase_match_count[doc_id as usize] += 1;
                    }
                }
            }

            // Apply boost or filter
            if phrase_cfg.filter {
                // AND logic: doc must match ALL phrases
                touched.retain(|&doc_id| phrase_match_count[doc_id as usize] >= num_real_phrases);
            } else {
                // Boost compounds per phrase: score × boost^(matches)
                for &doc_id in &touched {
                    let count = phrase_match_count[doc_id as usize];
                    if count > 0 {
                        // Exponentiation via repeated multiply (count is small)
                        let mut boost = 1.0f32;
                        for _ in 0..count {
                            boost *= phrase_cfg.boost;
                        }
                        scores[doc_id as usize] *= boost;
                    }
                }
            }
        }

        // --- Phase 3: Proximity scoring (zero overhead for single-term or disabled) ---
        if prox_cfg.enabled && prox_cfg.weight > 0.0 && query_terms.len() >= 2 {
            let num_query_terms = query_terms.len();

            // Build doc_id → [positions per query term] for all touched docs
            let mut doc_positions: HashMap<u32, Vec<SmallVec<[u32; 4]>>> = HashMap::new();

            // Active segment
            for (term_idx, term) in query_terms.iter().enumerate() {
                if let Some(posting_list) = self.postings.get(term.as_str()) {
                    for (entry_idx, entry) in posting_list.entries.iter().enumerate() {
                        let did = entry.doc_id as usize;
                        if did >= num_docs || !seen[did] {
                            continue;
                        }
                        let e = doc_positions
                            .entry(entry.doc_id)
                            .or_insert_with(|| vec![SmallVec::new(); num_query_terms]);
                        e[term_idx] = posting_list.positions[entry_idx].clone();
                    }
                }
            }

            // Sealed segments
            for seg in sealed.iter() {
                for (term_idx, term) in query_terms.iter().enumerate() {
                    if let Some((iter, mut pr)) = seg.term_with_positions(term) {
                        for entry in iter {
                            let did = entry.doc_id as usize;
                            if did >= num_docs || !seen[did] {
                                pr.skip_positions(entry.tf);
                                continue;
                            }
                            let positions = pr.read_positions(entry.tf);
                            let e = doc_positions
                                .entry(entry.doc_id)
                                .or_insert_with(|| vec![SmallVec::new(); num_query_terms]);
                            e[term_idx] = positions;
                        }
                    }
                }
            }

            // Compute proximity boost per doc
            let window = prox_cfg.window.max(1) as f32;
            for (&doc_id, term_positions) in &doc_positions {
                // Count how many query terms are present in this doc
                let present: Vec<&SmallVec<[u32; 4]>> =
                    term_positions.iter().filter(|p| !p.is_empty()).collect();
                let terms_present = present.len();

                if terms_present < 2 {
                    continue; // Need at least 2 terms for proximity
                }

                // Compute min span over the present terms
                let present_positions: Vec<SmallVec<[u32; 4]>> =
                    present.into_iter().cloned().collect();
                if let Some(span) = compute_min_span(&present_positions) {
                    let coverage = terms_present as f32 / num_query_terms as f32;
                    let proximity_score =
                        prox_cfg.weight * coverage * coverage / (1.0 + span as f32 / window);
                    scores[doc_id as usize] += proximity_score;
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

        let tokens = tokenize_with_positions(text);
        let content_hash = Self::content_fingerprint(text);
        // doc_len = max_position + 1 (includes stopword gaps for accurate BM25 length norm)
        let doc_len = tokens.last().map_or(0, |t| t.position + 1);

        // Build term → positions map
        let mut term_positions: HashMap<String, SmallVec<[u32; 4]>> =
            HashMap::with_capacity(tokens.len());
        for token in tokens {
            term_positions
                .entry(token.term)
                .or_default()
                .push(token.position);
        }

        // Collect term names for the forward index before consuming term_positions
        let term_names: Vec<String> = term_positions.keys().cloned().collect();

        // Update posting lists with positions
        for (term, positions) in term_positions {
            let tf = positions.len() as u32;
            let entry = PostingEntry::new(doc_id, tf, doc_len);

            self.postings
                .entry(term.clone())
                .or_default()
                .add_with_positions(entry, positions);

            self.doc_freqs
                .entry(term)
                .and_modify(|c| *c += 1)
                .or_insert(1);
        }

        // Store forward index (doc_id → terms) for O(terms) removal
        {
            let mut terms = self.doc_terms.write().unwrap();
            let idx = doc_id as usize;
            if idx >= terms.len() {
                terms.resize(idx + 1, None);
            }
            terms[idx] = Some(term_names);
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

        {
            let mut hashes = self.doc_content_hashes.write().unwrap();
            let idx = doc_id as usize;
            if idx >= hashes.len() {
                hashes.resize(idx + 1, None);
            }
            hashes[idx] = Some(content_hash);
        }

        // Relaxed: observational counters — the version bump below (Release)
        // is the synchronization fence for readers checking index freshness.
        self.total_docs.fetch_add(1, Ordering::Relaxed);
        self.total_doc_len
            .fetch_add(doc_len as usize, Ordering::Relaxed);
        self.active_doc_count.fetch_add(1, Ordering::Relaxed);
        self.version.fetch_add(1, Ordering::Release);

        // Relaxed: approximate count is fine for threshold-based seal decisions.
        // The actual sealing is guarded by the CAS on `sealing` (Acquire).
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

        // Retrieve and clear forward index entry for this doc
        let terms = {
            let mut dt = self.doc_terms.write().unwrap();
            let idx = doc_id as usize;
            if idx < dt.len() {
                dt[idx].take()
            } else {
                None
            }
        };

        {
            let mut hashes = self.doc_content_hashes.write().unwrap();
            let idx = doc_id as usize;
            if idx < hashes.len() {
                hashes[idx] = None;
            }
        }

        // Remove from active segment using forward index — O(terms_in_doc) not O(vocabulary)
        let mut active_removed = false;
        if let Some(ref term_list) = terms {
            for term in term_list {
                if let Some(mut posting) = self.postings.get_mut(term) {
                    let count = posting.remove_by_id(doc_id);
                    if count > 0 {
                        active_removed = true;
                        self.doc_freqs
                            .entry(term.clone())
                            .and_modify(|c| *c = c.saturating_sub(count));
                    }
                }
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
        // Relaxed: observational counters (see struct-level doc). The version
        // bump (Release) is the synchronization fence for freshness checks.
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

    /// Remove every indexed document whose `EntityRef` lives in
    /// `(branch_id, space)`. Returns the number of refs removed.
    ///
    /// Snapshots `doc_id_map.id_to_ref` under a read lock, then drops the
    /// lock before delegating to `remove_document` for each match — that
    /// way we never hold the read lock while taking the write locks
    /// `remove_document` needs.
    ///
    /// Cost: O(total_docs) snapshot + O(matching_docs × terms_per_doc)
    /// removal. Administrative-only — used by `space_delete --force`.
    pub fn remove_documents_in_space(&self, branch_id: BranchId, space: &str) -> usize {
        if !self.is_enabled() {
            return 0;
        }
        let to_remove: Vec<EntityRef> = {
            let guard = self.doc_id_map.id_to_ref.read().unwrap();
            guard
                .iter()
                .filter(|r| r.branch_id() == branch_id && r.space() == Some(space))
                .cloned()
                .collect()
        };
        let n = to_remove.len();
        for r in &to_remove {
            self.remove_document(r);
        }
        n
    }

    // ========================================================================
    // Seal & Persistence
    // ========================================================================

    /// Try to seal the active segment if it has enough documents.
    ///
    /// Uses CAS on `sealing` to prevent concurrent auto-seals.
    fn try_seal_active(&self) {
        // Relaxed: approximate count is sufficient for the threshold check.
        // A slightly stale value just delays or advances the seal by one doc.
        let active_count = self.active_doc_count.load(Ordering::Relaxed);
        if active_count < self.seal_threshold {
            return;
        }
        // CAS with Acquire on success ensures all prior index writes are
        // visible before we start reading the active segment for sealing.
        // Relaxed on failure: we just bail, no ordering needed.
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
        // Relaxed: early-exit check — exact count not needed.
        let active_count = self.active_doc_count.load(Ordering::Relaxed);
        if active_count == 0 {
            return;
        }

        // Drain active segment into sorted term map (entries + positions).
        let mut term_postings: BTreeMap<String, Vec<PostingEntry>> = BTreeMap::new();
        let mut term_positions: BTreeMap<String, Vec<SmallVec<[u32; 4]>>> = BTreeMap::new();

        // Collect and remove all entries from active postings.
        // Only remove doc_freqs for drained terms — concurrent inserts may have
        // added new terms between our snapshot and this point (#1738 / 9.5.B).
        let keys: Vec<String> = self.postings.iter().map(|r| r.key().clone()).collect();
        for key in &keys {
            if let Some((term, posting_list)) = self.postings.remove(key) {
                if !posting_list.entries.is_empty() {
                    term_positions.insert(term.clone(), posting_list.positions);
                    term_postings.insert(term, posting_list.entries);
                }
            }
        }
        for key in &keys {
            self.doc_freqs.remove(key);
        }

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

        // Relaxed: uniqueness is guaranteed by fetch_add atomicity.
        let segment_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);

        // Build the sealed segment (v2 with positions)
        let seg = segment::build_sealed_segment(
            segment_id,
            term_postings,
            term_positions,
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

        // Add to sealed list.
        // Subtract only the count of docs we actually drained, not reset to 0,
        // so concurrent inserts keep their active_doc_count contribution (#1738 / 9.5.B).
        self.sealed.write().unwrap().push(seg);
        // Relaxed: observational bookkeeping (see struct-level doc).
        self.active_doc_count
            .fetch_sub(actual_doc_count as usize, Ordering::Relaxed);
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
        let doc_terms_vec = self.doc_terms.read().unwrap().clone();
        let doc_content_hashes_vec = self.doc_content_hashes.read().unwrap().clone();

        let manifest_data = ManifestData {
            version: 1,
            total_docs: self.total_docs.load(Ordering::Acquire) as u64,
            total_doc_len: self.total_doc_len.load(Ordering::Acquire) as u64,
            next_segment_id: self.next_segment_id.load(Ordering::Relaxed),
            segments: segment_entries,
            doc_id_map: doc_id_map_vec,
            doc_lengths: doc_lengths_vec,
            doc_terms: doc_terms_vec,
            doc_content_hashes: doc_content_hashes_vec,
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

        // Restore global stats.
        // Relaxed: recovery runs single-threaded before the index is visible
        // to other threads, so no ordering is needed.
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

        // Active segment starts empty after recovery.
        // Relaxed: single-threaded recovery path (same as above).
        self.postings.clear();
        self.doc_freqs.clear();
        self.active_doc_count.store(0, Ordering::Relaxed);

        // Restore global doc_lengths from manifest (persists across seals
        // for re-index detection and accurate total_doc_len on removal)
        *self.doc_lengths.write().unwrap() = data.doc_lengths;

        // Restore forward index (doc_id → terms) for O(terms) removal
        *self.doc_terms.write().unwrap() = data.doc_terms;
        *self.doc_content_hashes.write().unwrap() = data.doc_content_hashes;

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
            space: "default".to_string(),
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
        let result = index.wait_for_version(CommitVersion(1), Duration::from_secs(1));
        handle.join().unwrap();

        assert!(result);
        assert!(index.version() >= 1);
    }

    #[test]
    fn test_wait_for_version_timeout() {
        let index = InvertedIndex::new();

        // Version is 0, waiting for 100 should timeout
        let result = index.wait_for_version(CommitVersion(100), Duration::from_millis(10));
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
            space: "default".to_string(),
            key: key.to_string(),
        }
    }

    #[test]
    fn test_score_top_k_disabled_index() {
        let index = InvertedIndex::new();
        // Index is disabled by default
        let branch_id = BranchId::new();
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert!(result.is_empty());
    }

    #[test]
    fn test_score_top_k_empty_query() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();
        let doc = kv_ref(branch_id, "doc1");
        index.index_document(&doc, "hello world", None);

        let result = index.score_top_k(
            &[],
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            0,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

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
        let result_a = index.score_top_k(
            &terms,
            &branch_a,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result_a.len(), 1);
        let resolved = index.resolve_doc_id(result_a[0].doc_id).unwrap();
        assert_eq!(resolved, doc_a);

        // Search branch_b: should only find doc_b
        let result_b = index.score_top_k(
            &terms,
            &branch_b,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result_b.len(), 1);
        let resolved = index.resolve_doc_id(result_b[0].doc_id).unwrap();
        assert_eq!(resolved, doc_b);
    }

    /// Two KV docs in the same branch but different spaces, identical key.
    /// Searching with `requested_space = Some("tenant_a")` must return only
    /// the tenant_a doc — never leak the tenant_b doc.
    #[test]
    fn test_score_top_k_kv_space_filter() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc_a = EntityRef::Kv {
            branch_id,
            space: "tenant_a".to_string(),
            key: "shared_key".to_string(),
        };
        let doc_b = EntityRef::Kv {
            branch_id,
            space: "tenant_b".to_string(),
            key: "shared_key".to_string(),
        };
        index.index_document(&doc_a, "alpha bravo charlie", None);
        index.index_document(&doc_b, "alpha bravo charlie", None);

        let terms = vec!["bravo".to_string()];

        // Filter to tenant_a → only doc_a
        let result_a = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            Some("tenant_a"),
        );
        assert_eq!(result_a.len(), 1, "tenant_a should see exactly its own doc");
        let resolved = index.resolve_doc_id(result_a[0].doc_id).unwrap();
        assert_eq!(resolved, doc_a);

        // Filter to tenant_b → only doc_b
        let result_b = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            Some("tenant_b"),
        );
        assert_eq!(result_b.len(), 1, "tenant_b should see exactly its own doc");
        let resolved = index.resolve_doc_id(result_b[0].doc_id).unwrap();
        assert_eq!(resolved, doc_b);

        // No filter (None) → both docs (cross-space recovery scan)
        let result_all = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result_all.len(), 2, "None requested_space sees all spaces");
    }

    /// Same as above for `EntityRef::Json` — same identity collision pattern,
    /// same expected outcome.
    #[test]
    fn test_score_top_k_json_space_filter() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc_a = EntityRef::Json {
            branch_id,
            space: "tenant_a".to_string(),
            doc_id: "doc1".to_string(),
        };
        let doc_b = EntityRef::Json {
            branch_id,
            space: "tenant_b".to_string(),
            doc_id: "doc1".to_string(),
        };
        index.index_document(&doc_a, "delta echo foxtrot", None);
        index.index_document(&doc_b, "delta echo foxtrot", None);

        let terms = vec!["echo".to_string()];

        let result_a = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            Some("tenant_a"),
        );
        assert_eq!(result_a.len(), 1);
        assert_eq!(index.resolve_doc_id(result_a[0].doc_id).unwrap(), doc_a);
    }

    /// Same as above for `EntityRef::Event`.
    #[test]
    fn test_score_top_k_event_space_filter() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc_a = EntityRef::Event {
            branch_id,
            space: "tenant_a".to_string(),
            sequence: 1,
        };
        let doc_b = EntityRef::Event {
            branch_id,
            space: "tenant_b".to_string(),
            sequence: 1,
        };
        index.index_document(&doc_a, "golf hotel india", None);
        index.index_document(&doc_b, "golf hotel india", None);

        let terms = vec!["hotel".to_string()];

        let result_a = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            Some("tenant_a"),
        );
        assert_eq!(result_a.len(), 1);
        assert_eq!(index.resolve_doc_id(result_a[0].doc_id).unwrap(), doc_a);
    }

    /// Sealed-segment path also needs the space filter. Index two docs,
    /// seal, then search.
    #[test]
    fn test_score_top_k_space_filter_sealed_segment() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc_a = EntityRef::Kv {
            branch_id,
            space: "tenant_a".to_string(),
            key: "k".to_string(),
        };
        let doc_b = EntityRef::Kv {
            branch_id,
            space: "tenant_b".to_string(),
            key: "k".to_string(),
        };
        index.index_document(&doc_a, "juliet kilo lima", None);
        index.index_document(&doc_b, "juliet kilo lima", None);
        index.seal_active();

        let terms = vec!["kilo".to_string()];
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            Some("tenant_a"),
        );
        assert_eq!(
            result.len(),
            1,
            "sealed-segment search must filter by space"
        );
        assert_eq!(index.resolve_doc_id(result[0].doc_id).unwrap(), doc_a);
    }

    /// `EntityRef::Branch` has no space (`space()` returns `None`). When a
    /// space filter is requested it must NOT drop Branch refs — they are
    /// space-less metadata that always belong to the caller's view.
    #[test]
    fn test_score_top_k_keeps_branch_refs_under_space_filter() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let branch_ref = EntityRef::Branch { branch_id };
        index.index_document(&branch_ref, "alpha bravo charlie", None);

        let terms = vec!["bravo".to_string()];
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            Some("tenant_a"),
        );
        assert_eq!(
            result.len(),
            1,
            "Branch refs must survive a space filter — they have no space"
        );
        assert_eq!(index.resolve_doc_id(result[0].doc_id).unwrap(), branch_ref);
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            5,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

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
        let rare_result = index.score_top_k(
            &rare_terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(rare_result.len(), 1);

        // Search for "common" — all 10 docs match
        let common_terms = vec!["common".to_string()];
        let common_result = index.score_top_k(
            &common_terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            k1,
            b,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_nonexistent,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            100,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

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
            space: "default".to_string(),
            key: "doc1".to_string(),
        };
        let event_doc = EntityRef::Event {
            branch_id,
            space: "default".to_string(),
            sequence: 42,
        };
        index.index_document(&kv_doc, "hello world", None);
        index.index_document(&event_doc, "hello planet", None);

        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            20,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            20,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert!(
            result.is_empty(),
            "Old term 'hello' should not match after re-index"
        );

        // New terms should match (in active segment)
        let terms = vec!["alpha".to_string()];
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            20,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result.len(), 5);

        // Second batch: 5 more docs
        for i in 0..5 {
            let doc = kv_ref(branch_id, &format!("batch2_doc{}", i));
            index.index_document(&doc, "alpha gamma", None);
        }
        index.seal_active();

        // Search after second seal — should span both sealed segments
        let result = index.score_top_k(
            &terms,
            &branch_id,
            20,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result.len(), 10);

        // Third batch: 3 more docs still in active
        for i in 0..3 {
            let doc = kv_ref(branch_id, &format!("batch3_doc{}", i));
            index.index_document(&doc, "alpha delta", None);
        }

        // Search across 2 sealed + 1 active
        let result = index.score_top_k(
            &terms,
            &branch_id,
            20,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
            let result = index.score_top_k(
                &terms,
                &branch_id,
                10,
                0.9,
                0.4,
                &PhraseConfig::none(),
                &ProximityConfig::off(),
                None,
            );
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
            let result = index.score_top_k(
                &terms,
                &branch_id,
                10,
                0.9,
                0.4,
                &PhraseConfig::none(),
                &ProximityConfig::off(),
                None,
            );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            k1,
            b,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result_a = index.score_top_k(
            &terms,
            &branch_a,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result_a.len(), 3);
        let result_b = index.score_top_k(
            &terms,
            &branch_b,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_b,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
            let result = index.score_top_k(
                &terms,
                &branch_id,
                30,
                0.9,
                0.4,
                &PhraseConfig::none(),
                &ProximityConfig::off(),
                None,
            );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            k1,
            b,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            20,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result.len(), 10);

        // Wrong branch should still return empty
        let other_branch = BranchId::new();
        let result = index.score_top_k(
            &terms,
            &other_branch,
            20,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
            let result = index.score_top_k(
                &terms,
                &branch_id,
                10,
                0.9,
                0.4,
                &PhraseConfig::none(),
                &ProximityConfig::off(),
                None,
            );
            assert_eq!(result.len(), 5);

            let other_branch = BranchId::new();
            let result = index.score_top_k(
                &terms,
                &other_branch,
                10,
                0.9,
                0.4,
                &PhraseConfig::none(),
                &ProximityConfig::off(),
                None,
            );
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
            let result = index.score_top_k(
                &terms,
                &branch_id,
                10,
                0.9,
                0.4,
                &PhraseConfig::none(),
                &ProximityConfig::off(),
                None,
            );
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
        let result_a = index.score_top_k(
            &terms,
            &branch_a,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result_a.len(), 2);

        let result_b = index.score_top_k(
            &terms,
            &branch_b,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
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
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

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

    // ====================================================================
    // Forward index (doc_terms) tests
    // ====================================================================

    #[test]
    fn test_forward_index_populated_on_index() {
        let index = InvertedIndex::new();
        index.enable();

        let doc_ref = test_doc_ref("fwd_test");
        index.index_document(&doc_ref, "hello world", None);

        let doc_id = index.doc_id_map.get(&doc_ref).unwrap();
        let terms = index.doc_terms.read().unwrap();
        let doc_terms = terms[doc_id as usize].as_ref().unwrap();
        assert!(doc_terms.contains(&"hello".to_string()));
        assert!(doc_terms.contains(&"world".to_string()));
        assert_eq!(doc_terms.len(), 2);
    }

    #[test]
    fn test_remove_uses_forward_index() {
        // Index two docs with distinct terms, remove one, verify the other's
        // posting lists are untouched.
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc1 = kv_ref(branch_id, "only_alpha");
        let doc2 = kv_ref(branch_id, "only_beta");
        index.index_document(&doc1, "alpha unique1", None);
        index.index_document(&doc2, "beta unique2", None);

        assert_eq!(index.doc_freq("alpha"), 1);
        assert_eq!(index.doc_freq("beta"), 1);

        // Remove doc1 — only "alpha" and "unique1" posting lists should be touched
        index.remove_document(&doc1);

        assert_eq!(index.doc_freq("alpha"), 0);
        assert_eq!(index.doc_freq("unique1"), 0);
        // doc2's terms must be completely untouched
        assert_eq!(index.doc_freq("beta"), 1);
        assert_eq!(index.doc_freq("unique2"), 1);
        assert_eq!(index.total_docs(), 1);
    }

    #[test]
    fn test_forward_index_cleared_on_remove() {
        let index = InvertedIndex::new();
        index.enable();

        let doc_ref = test_doc_ref("clear_test");
        index.index_document(&doc_ref, "hello world", None);

        let doc_id = index.doc_id_map.get(&doc_ref).unwrap();
        assert!(index.doc_terms.read().unwrap()[doc_id as usize].is_some());

        index.remove_document(&doc_ref);

        assert!(index.doc_terms.read().unwrap()[doc_id as usize].is_none());
    }

    #[test]
    fn test_forward_index_survives_seal() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc = kv_ref(branch_id, "seal_fwd");
        index.index_document(&doc, "alpha beta gamma", None);

        let doc_id = index.doc_id_map.get(&doc).unwrap();

        // Seal the active segment
        index.seal_active();

        // doc_terms should still be populated (it's a global map like doc_lengths)
        let terms = index.doc_terms.read().unwrap();
        let doc_terms = terms[doc_id as usize].as_ref().unwrap();
        assert!(doc_terms.contains(&"alpha".to_string()));
        assert!(doc_terms.contains(&"beta".to_string()));
        assert!(doc_terms.contains(&"gamma".to_string()));
    }

    #[test]
    fn test_forward_index_persistence_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let doc1 = kv_ref(branch_id, "persist_fwd1");
        let doc2 = kv_ref(branch_id, "persist_fwd2");
        let doc3 = kv_ref(branch_id, "persist_fwd3");
        let doc1_id;
        let doc2_id;
        let doc3_id;

        // Create index, add docs, remove one, freeze
        {
            let index = InvertedIndex::new();
            index.enable();
            index.set_data_dir(tmp.path().to_path_buf());

            index.index_document(&doc1, "delta epsilon", None);
            index.index_document(&doc2, "gamma zeta", None);
            index.index_document(&doc3, "theta iota", None);
            doc1_id = index.doc_id_map.get(&doc1).unwrap();
            doc2_id = index.doc_id_map.get(&doc2).unwrap();
            doc3_id = index.doc_id_map.get(&doc3).unwrap();

            // Remove doc2 before freezing — its doc_terms slot should be None
            index.remove_document(&doc2);

            index.freeze_to_disk().unwrap();
        }

        // Load and verify doc_terms survived correctly
        {
            let index = InvertedIndex::new();
            index.set_data_dir(tmp.path().to_path_buf());
            index.load_from_disk().unwrap();
            index.enable();

            let terms = index.doc_terms.read().unwrap();

            // doc1 should still have its terms
            let dt1 = terms[doc1_id as usize].as_ref().unwrap();
            assert!(dt1.contains(&"delta".to_string()));
            assert!(dt1.contains(&"epsilon".to_string()));

            // doc2 was removed — its slot should be None
            assert!(terms[doc2_id as usize].is_none());

            // doc3 should still have its terms
            let dt3 = terms[doc3_id as usize].as_ref().unwrap();
            assert!(dt3.contains(&"theta".to_string()));
            assert!(dt3.contains(&"iota".to_string()));

            // After loading, we should be able to remove doc3 using forward index
            drop(terms);
            index.remove_document(&doc3);
            assert_eq!(index.total_docs(), 1);

            // Verify doc3's forward index was cleared
            assert!(index.doc_terms.read().unwrap()[doc3_id as usize].is_none());
        }
    }

    #[test]
    fn test_reindex_uses_forward_index() {
        // Update a doc (same EntityRef, new text), verify old terms removed
        // and new terms indexed correctly via forward index.
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc = kv_ref(branch_id, "reindex_fwd");
        index.index_document(&doc, "old content here", None);
        assert_eq!(index.doc_freq("old"), 1);
        assert_eq!(index.doc_freq("content"), 1);

        // Re-index with completely different text
        index.index_document(&doc, "brand new text", None);

        // Old terms should be gone
        assert_eq!(index.doc_freq("old"), 0);
        assert_eq!(index.doc_freq("content"), 0);
        assert_eq!(index.doc_freq("here"), 0);

        // New terms should be present
        assert_eq!(index.doc_freq("brand"), 1);
        assert_eq!(index.doc_freq("new"), 1);
        assert_eq!(index.doc_freq("text"), 1);

        // Forward index should reflect the new terms
        let doc_id = index.doc_id_map.get(&doc).unwrap();
        let terms = index.doc_terms.read().unwrap();
        let doc_terms = terms[doc_id as usize].as_ref().unwrap();
        assert!(doc_terms.contains(&"brand".to_string()));
        assert!(doc_terms.contains(&"new".to_string()));
        assert!(doc_terms.contains(&"text".to_string()));
        assert!(!doc_terms.contains(&"old".to_string()));

        assert_eq!(index.total_docs(), 1);
    }

    #[test]
    fn test_forward_index_overlapping_terms() {
        // Two docs share a term; removing one must leave the shared term's
        // posting intact for the surviving doc.
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc1 = kv_ref(branch_id, "overlap1");
        let doc2 = kv_ref(branch_id, "overlap2");
        // Use stemming-stable words ("hello", "world", "planet")
        index.index_document(&doc1, "hello world", None);
        index.index_document(&doc2, "hello planet", None);

        assert_eq!(index.doc_freq("hello"), 2);

        index.remove_document(&doc1);

        // "hello" should still have df=1 from doc2
        assert_eq!(index.doc_freq("hello"), 1);
        assert_eq!(index.doc_freq("world"), 0);
        assert_eq!(index.doc_freq("planet"), 1);

        // Verify the surviving posting is actually doc2's
        let posting = index.lookup("hello").unwrap();
        assert_eq!(posting.len(), 1);
        let doc2_id = index.doc_id_map.get(&doc2).unwrap();
        assert_eq!(posting.entries[0].doc_id, doc2_id);

        // Search should find only doc2
        let terms = vec!["hello".to_string()];
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result.len(), 1);
        let resolved = index.resolve_doc_id(result[0].doc_id).unwrap();
        assert_eq!(resolved, doc2);
    }

    #[test]
    fn test_remove_sealed_doc_with_forward_index() {
        // After sealing, forward index should still enable removal.
        // The active postings are drained so the forward index loop finds
        // nothing in active (no error/panic), and tombstones handle sealed.
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc1 = kv_ref(branch_id, "sealed_fwd1");
        let doc2 = kv_ref(branch_id, "sealed_fwd2");
        index.index_document(&doc1, "alpha beta", None);
        index.index_document(&doc2, "alpha gamma", None);

        let doc1_id = index.doc_id_map.get(&doc1).unwrap();

        // Seal — all postings move to sealed segment
        index.seal_active();
        assert!(index.postings.is_empty());

        // Forward index should still be populated
        assert!(index.doc_terms.read().unwrap()[doc1_id as usize].is_some());

        // Remove doc1 from sealed segment
        index.remove_document(&doc1);

        // Forward index should be cleared for doc1
        assert!(index.doc_terms.read().unwrap()[doc1_id as usize].is_none());

        // Stats should be correct
        assert_eq!(index.total_docs(), 1);

        // Only doc2 should appear in search
        let terms = vec!["alpha".to_string()];
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result.len(), 1);
        let resolved = index.resolve_doc_id(result[0].doc_id).unwrap();
        assert_eq!(resolved, doc2);
    }

    #[test]
    fn test_backward_compat_empty_doc_terms() {
        // Simulate loading from an old manifest that has no doc_terms field.
        // The #[serde(default)] gives us an empty vec. Removal of sealed
        // docs should still work via tombstones even without forward index.
        let tmp = tempfile::tempdir().unwrap();
        let branch_id = BranchId::new();

        let doc1 = kv_ref(branch_id, "compat1");
        let doc2 = kv_ref(branch_id, "compat2");

        // Phase 1: Create index, freeze with doc_terms populated
        {
            let index = InvertedIndex::new();
            index.enable();
            index.set_data_dir(tmp.path().to_path_buf());

            index.index_document(&doc1, "hello world", None);
            index.index_document(&doc2, "hello planet", None);

            index.freeze_to_disk().unwrap();
        }

        // Phase 2: Load, then wipe doc_terms to simulate old manifest,
        // verify removal still works
        {
            let index = InvertedIndex::new();
            index.set_data_dir(tmp.path().to_path_buf());
            index.load_from_disk().unwrap();
            index.enable();

            // Simulate old manifest: clear doc_terms as if it wasn't persisted
            index.doc_terms.write().unwrap().clear();

            assert_eq!(index.total_docs(), 2);

            // Remove doc1 — should work via tombstone even without forward index
            index.remove_document(&doc1);
            assert_eq!(index.total_docs(), 1);

            // doc_lengths was used for stat update
            // Search should only find doc2
            let terms = vec!["hello".to_string()];
            let result = index.score_top_k(
                &terms,
                &branch_id,
                10,
                0.9,
                0.4,
                &PhraseConfig::none(),
                &ProximityConfig::off(),
                None,
            );
            assert_eq!(result.len(), 1);
            let resolved = index.resolve_doc_id(result[0].doc_id).unwrap();
            assert_eq!(resolved, doc2);
        }
    }

    #[test]
    fn test_reindex_after_seal_forward_index() {
        // Re-index a doc that was sealed. The forward index should enable
        // removal of old active postings (none here since sealed), tombstone
        // the sealed version, then create new forward index for the re-indexed doc.
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc = kv_ref(branch_id, "reindex_seal");
        // Use stemming-stable words (confirmed by existing test_reindex_after_seal)
        index.index_document(&doc, "hello world", None);

        let doc_id = index.doc_id_map.get(&doc).unwrap();

        // Seal
        index.seal_active();

        // Re-index same doc with different content
        index.index_document(&doc, "alpha planet", None);

        assert_eq!(index.total_docs(), 1);

        // Forward index should reflect the NEW terms
        let terms = index.doc_terms.read().unwrap();
        let doc_terms = terms[doc_id as usize].as_ref().unwrap();
        assert!(doc_terms.contains(&"alpha".to_string()));
        assert!(doc_terms.contains(&"planet".to_string()));
        assert!(!doc_terms.contains(&"hello".to_string()));
        assert!(!doc_terms.contains(&"world".to_string()));
        drop(terms);

        // Old terms should not match (tombstoned in sealed)
        let query = vec!["hello".to_string()];
        let result = index.score_top_k(
            &query,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert!(result.is_empty());

        // New terms should match (in active)
        let query = vec!["alpha".to_string()];
        let result = index.score_top_k(
            &query,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result.len(), 1);
    }

    /// Issue #1738 / 9.5.B: seal_active() snapshots posting keys, removes them,
    /// then clears doc_freqs and resets active_doc_count to 0. Inserts that land
    /// between snapshot and reset are orphaned: their posting entries survive but
    /// doc_freqs and active_doc_count are zeroed.
    ///
    /// This test simulates the race deterministically: index docs, manually snapshot
    /// and drain keys (simulating the seal's drain phase), insert a new doc, then
    /// call the clear/reset. The new doc's doc_freq must survive.
    #[test]
    fn test_issue_1738_seal_active_orphans_concurrent_postings() {
        let index = InvertedIndex::new();
        index.enable();

        // Index enough docs to make seal meaningful (but don't trigger auto-seal)
        let branch_id = BranchId::new();
        for i in 0..5 {
            let doc_ref = EntityRef::Kv {
                branch_id,
                space: "default".to_string(),
                key: format!("doc{}", i),
            };
            index.index_document(&doc_ref, "common term", None);
        }

        assert_eq!(index.active_doc_count.load(Ordering::Relaxed), 5);
        assert_eq!(index.doc_freq("common"), 5);

        // Now index a doc with a unique term AFTER the seal starts.
        // In real code, this would happen between seal_active's key snapshot
        // and its doc_freqs.clear(). We simulate by calling seal_active()
        // and checking that a doc inserted just before is properly accounted for.
        //
        // Since seal_active() does clear() and store(0), any doc in the active
        // segment that shares no terms with the drained set would lose its doc_freq.

        // Index a doc with a UNIQUE term (not shared with previously sealed docs)
        let late_doc = EntityRef::Kv {
            branch_id,
            space: "default".to_string(),
            key: "late_doc".to_string(),
        };
        index.index_document(&late_doc, "xylophone", None);
        assert_eq!(index.active_doc_count.load(Ordering::Relaxed), 6);
        assert_eq!(index.doc_freq("xylophon"), 1); // stemmed

        // Seal active — this drains everything including the late doc
        index.seal_active();

        // After seal: active segment should be clean
        // The bug manifests when active_doc_count is 0 but orphaned postings exist.
        // With the fix, active_doc_count should be 0 (all docs sealed) and no
        // orphaned doc_freqs remain for terms that weren't drained.
        assert_eq!(index.active_doc_count.load(Ordering::Relaxed), 0);

        // Now the real test: index ANOTHER doc after seal
        let post_seal_doc = EntityRef::Kv {
            branch_id,
            space: "default".to_string(),
            key: "post_seal_doc".to_string(),
        };
        index.index_document(&post_seal_doc, "zeppelin", None);

        // This doc's doc_freq must be 1 (not cleared by a prior seal)
        assert_eq!(index.doc_freq("zeppelin"), 1);
        assert_eq!(index.active_doc_count.load(Ordering::Relaxed), 1);

        // The sealed segment should contain all 6 previously sealed docs
        let sealed = index.sealed.read().unwrap();
        assert_eq!(sealed.len(), 1);
    }

    /// Issue #1738 / 9.5.B concurrent variant: Multiple threads index documents
    /// while seal_active is called. No doc_freqs or active_doc_count should be lost.
    #[test]
    fn test_issue_1738_seal_active_concurrent() {
        use std::sync::{Arc, Barrier};

        let index = Arc::new(InvertedIndex::new());
        index.enable();
        let branch_id = BranchId::new();

        // Pre-populate some docs
        for i in 0..10 {
            let doc_ref = EntityRef::Kv {
                branch_id,
                space: "default".to_string(),
                key: format!("pre_{}", i),
            };
            index.index_document(&doc_ref, &format!("term_{}", i), None);
        }

        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();

        // Thread 1: seal
        {
            let idx = Arc::clone(&index);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                b.wait();
                idx.seal_active();
            }));
        }

        // Thread 2 & 3: insert docs concurrently with seal
        for t in 0..2 {
            let idx = Arc::clone(&index);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                b.wait();
                for i in 0..20 {
                    let doc_ref = EntityRef::Kv {
                        branch_id,
                        space: "default".to_string(),
                        key: format!("concurrent_t{}_{}", t, i),
                    };
                    idx.index_document(&doc_ref, &format!("concurrent_term_t{}_{}", t, i), None);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify consistency: active_doc_count must match actual active postings
        let active_count = index.active_doc_count.load(Ordering::Relaxed);
        let actual_active_terms: usize = index.postings.iter().count();

        // Each post-seal doc has a unique term, so active terms ≈ active docs
        // The key invariant: active_doc_count must NOT be less than actual active docs
        // (which would happen if seal_active zeroed it while concurrent inserts were in flight)
        assert!(
            active_count >= actual_active_terms || actual_active_terms == 0,
            "active_doc_count ({}) is less than actual active term count ({}), \
             indicating seal_active() cleared concurrent inserts' state",
            active_count,
            actual_active_terms,
        );

        // Every active term must have a corresponding doc_freq entry
        for entry in index.postings.iter() {
            let term = entry.key();
            let freq = index.doc_freq(term);
            assert!(
                freq > 0,
                "Term '{}' exists in active postings but has doc_freq=0 (orphaned by seal_active)",
                term,
            );
        }
    }

    // ====================================================================
    // Phrase matching unit tests (#2239)
    // ====================================================================

    #[test]
    fn test_check_phrase_match_exact() {
        // "hello world" — positions [0, 1] — exact adjacency
        let positions = vec![SmallVec::from_slice(&[0u32]), SmallVec::from_slice(&[1u32])];
        assert!(check_phrase_match(&positions, 0));
    }

    #[test]
    fn test_check_phrase_match_no_match() {
        // "hello" at 0, "world" at 5 — not adjacent
        let positions = vec![SmallVec::from_slice(&[0u32]), SmallVec::from_slice(&[5u32])];
        assert!(!check_phrase_match(&positions, 0));
    }

    #[test]
    fn test_check_phrase_match_slop_1() {
        // "hello" at 0, "world" at 2 — gap of 1 word
        let positions = vec![SmallVec::from_slice(&[0u32]), SmallVec::from_slice(&[2u32])];
        assert!(!check_phrase_match(&positions, 0)); // exact: no
        assert!(check_phrase_match(&positions, 1)); // slop 1: yes
    }

    #[test]
    fn test_check_phrase_match_slop_2() {
        // "hello" at 0, "world" at 3 — gap of 2 words
        let positions = vec![SmallVec::from_slice(&[0u32]), SmallVec::from_slice(&[3u32])];
        assert!(!check_phrase_match(&positions, 1)); // slop 1: no
        assert!(check_phrase_match(&positions, 2)); // slop 2: yes
    }

    #[test]
    fn test_check_phrase_match_three_terms() {
        // "a b c" at positions [0, 1, 2]
        let positions = vec![
            SmallVec::from_slice(&[0u32]),
            SmallVec::from_slice(&[1u32]),
            SmallVec::from_slice(&[2u32]),
        ];
        assert!(check_phrase_match(&positions, 0));
    }

    #[test]
    fn test_check_phrase_match_multiple_positions() {
        // "hello" at [0, 5], "world" at [3, 6] — match at (5, 6)
        let positions = vec![
            SmallVec::from_slice(&[0u32, 5]),
            SmallVec::from_slice(&[3u32, 6]),
        ];
        assert!(check_phrase_match(&positions, 0));
    }

    #[test]
    fn test_check_phrase_match_empty_term() {
        // Missing term — should not match
        let positions = vec![SmallVec::from_slice(&[0u32]), SmallVec::new()];
        assert!(!check_phrase_match(&positions, 0));
    }

    #[test]
    fn test_check_phrase_match_repeated_term() {
        // Same term twice: "test test" — both position lists are identical.
        // Should match at consecutive positions (e.g., positions 0 and 1).
        let positions = vec![
            SmallVec::from_slice(&[0u32, 1, 5]),
            SmallVec::from_slice(&[0u32, 1, 5]),
        ];
        // Start=0, next > 0 → 1, gap=0. Match!
        assert!(check_phrase_match(&positions, 0));

        // Only one occurrence — no consecutive pair possible
        let single = vec![SmallVec::from_slice(&[3u32]), SmallVec::from_slice(&[3u32])];
        // Start=3, next > 3 → none. No match.
        assert!(!check_phrase_match(&single, 0));
    }

    // ====================================================================
    // Phrase query integration tests (score_top_k with phrases)
    // ====================================================================

    /// Helper: create an enabled index and index docs with known text
    fn phrase_test_index() -> (InvertedIndex, BranchId) {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Doc 0: contains "machine learning" as exact phrase
        let doc0 = kv_ref(branch_id, "doc0");
        index.index_document(&doc0, "introduction to machine learning algorithms", None);

        // Doc 1: contains both words but NOT as a phrase
        let doc1 = kv_ref(branch_id, "doc1");
        index.index_document(&doc1, "machine tools for learning purposes", None);

        // Doc 2: contains "machine learning" as exact phrase AND extra context
        let doc2 = kv_ref(branch_id, "doc2");
        index.index_document(
            &doc2,
            "deep machine learning and machine learning models",
            None,
        );

        (index, branch_id)
    }

    #[test]
    fn test_phrase_boost_mode() {
        let (index, branch_id) = phrase_test_index();

        // Search with phrase "machine learning" in boost mode
        let terms = vec!["machin".to_string(), "learn".to_string()];
        let phrases = vec![vec!["machin".to_string(), "learn".to_string()]];

        // Boost mode: all docs returned, phrase matches boosted
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig {
                phrases: &phrases,
                boost: 2.0,
                slop: 0,
                filter: false,
            },
            &ProximityConfig::off(),
            None,
        );

        // All matching docs returned
        assert!(
            result.len() >= 2,
            "expected at least 2 results, got {}",
            result.len()
        );

        // Doc with exact phrase should score higher than doc without
        // (doc0 has "machine learning", doc1 has "machine...learning" not adjacent)
        let doc0_score = result
            .iter()
            .find(|r| {
                let e = index.resolve_doc_id(r.doc_id).unwrap();
                matches!(e, EntityRef::Kv { ref key, space: _, .. } if key == "doc0")
            })
            .map(|r| r.score);
        let doc1_score = result
            .iter()
            .find(|r| {
                let e = index.resolve_doc_id(r.doc_id).unwrap();
                matches!(e, EntityRef::Kv { ref key, space: _, .. } if key == "doc1")
            })
            .map(|r| r.score);

        assert!(
            doc0_score.unwrap() > doc1_score.unwrap(),
            "phrase-matching doc should score higher: doc0={:?} doc1={:?}",
            doc0_score,
            doc1_score
        );
    }

    #[test]
    fn test_phrase_filter_mode() {
        let (index, branch_id) = phrase_test_index();

        let terms = vec!["machin".to_string(), "learn".to_string()];
        let phrases = vec![vec!["machin".to_string(), "learn".to_string()]];

        // Filter mode: only docs with exact phrase returned
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig {
                phrases: &phrases,
                boost: 2.0,
                slop: 0,
                filter: true,
            },
            &ProximityConfig::off(),
            None,
        );

        // doc1 has both words but not adjacent — should be filtered out
        for scored in &result {
            let entity = index.resolve_doc_id(scored.doc_id).unwrap();
            if let EntityRef::Kv {
                ref key, space: _, ..
            } = entity
            {
                assert_ne!(key, "doc1", "doc1 should be filtered out in filter mode");
            }
        }
        // At least doc0 and doc2 should match (both have "machine learning")
        assert!(
            result.len() >= 2,
            "expected at least 2 results, got {}",
            result.len()
        );
    }

    #[test]
    fn test_phrase_slop_allows_gap() {
        let (index, branch_id) = phrase_test_index();

        let terms = vec!["machin".to_string(), "learn".to_string()];
        let phrases = vec![vec!["machin".to_string(), "learn".to_string()]];

        // doc1 has "machine tools for learning" — gap of 2 words
        // With slop=0 (filter), doc1 excluded
        let strict = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig {
                phrases: &phrases,
                boost: 2.0,
                slop: 0,
                filter: true,
            },
            &ProximityConfig::off(),
            None,
        );
        let strict_keys: Vec<String> = strict
            .iter()
            .filter_map(|s| {
                let e = index.resolve_doc_id(s.doc_id)?;
                if let EntityRef::Kv { key, space: _, .. } = e {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();
        assert!(!strict_keys.contains(&"doc1".to_string()));

        // With slop=2 (filter), doc1 included (gap is 2)
        let sloppy = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig {
                phrases: &phrases,
                boost: 2.0,
                slop: 2,
                filter: true,
            },
            &ProximityConfig::off(),
            None,
        );
        let sloppy_keys: Vec<String> = sloppy
            .iter()
            .filter_map(|s| {
                let e = index.resolve_doc_id(s.doc_id)?;
                if let EntityRef::Kv { key, space: _, .. } = e {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();
        assert!(
            sloppy_keys.contains(&"doc1".to_string()),
            "slop=2 should include doc1"
        );
    }

    #[test]
    fn test_phrase_no_phrases_zero_overhead() {
        let (index, branch_id) = phrase_test_index();

        let terms = vec!["machin".to_string(), "learn".to_string()];
        let empty_phrases: Vec<Vec<String>> = vec![];

        // No phrases — should behave exactly like vanilla BM25
        let with_phrases = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig {
                phrases: &empty_phrases,
                boost: 2.0,
                slop: 0,
                filter: false,
            },
            &ProximityConfig::off(),
            None,
        );
        let without_phrases = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

        assert_eq!(with_phrases.len(), without_phrases.len());
        for (a, b) in with_phrases.iter().zip(without_phrases.iter()) {
            assert_eq!(a.doc_id, b.doc_id);
            assert!((a.score - b.score).abs() < 1e-6);
        }
    }

    #[test]
    fn test_phrase_multiple_phrases() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc0 = kv_ref(branch_id, "both");
        index.index_document(&doc0, "error code and stack trace found", None);

        let doc1 = kv_ref(branch_id, "one");
        index.index_document(&doc1, "error code but no relevant trace", None);

        let terms = vec![
            "error".to_string(),
            "code".to_string(),
            "stack".to_string(),
            "trace".to_string(),
        ];
        let phrases = vec![
            vec!["error".to_string(), "code".to_string()],
            vec!["stack".to_string(), "trace".to_string()],
        ];

        // Filter mode (AND): only docs matching ALL phrases survive
        // doc0 has both "error code" and "stack trace" → passes
        // doc1 has "error code" but NOT "stack trace" → filtered out
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig {
                phrases: &phrases,
                boost: 2.0,
                slop: 0,
                filter: true,
            },
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(
            result.len(),
            1,
            "only doc matching both phrases should survive"
        );
        let entity = index.resolve_doc_id(result[0].doc_id).unwrap();
        if let EntityRef::Kv {
            ref key, space: _, ..
        } = entity
        {
            assert_eq!(key, "both");
        } else {
            panic!("expected Kv entity");
        }
    }

    #[test]
    fn test_phrase_with_stopword_gap() {
        // Phrases with stopwords: "the quick" → tokens are "quick" at pos 1
        // but "the" is a stopword and doesn't produce a term.
        // So a query for "quick brown" should match consecutive non-stopword positions.
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc = kv_ref(branch_id, "doc");
        index.index_document(&doc, "the quick brown fox", None);

        // "quick" is at position 1, "brown" at position 2 — adjacent
        let terms = vec!["quick".to_string(), "brown".to_string()];
        let phrases = vec![vec!["quick".to_string(), "brown".to_string()]];

        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig {
                phrases: &phrases,
                boost: 2.0,
                slop: 0,
                filter: true,
            },
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result.len(), 1, "exact phrase 'quick brown' should match");
    }

    #[test]
    fn test_phrase_sealed_segment() {
        // Test phrase matching works across sealed segments
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc0 = kv_ref(branch_id, "sealed_doc");
        index.index_document(&doc0, "machine learning algorithms", None);
        index.seal_active();

        let terms = vec!["machin".to_string(), "learn".to_string()];
        let phrases = vec![vec!["machin".to_string(), "learn".to_string()]];

        // Filter mode on sealed segment
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig {
                phrases: &phrases,
                boost: 2.0,
                slop: 0,
                filter: true,
            },
            &ProximityConfig::off(),
            None,
        );
        assert_eq!(result.len(), 1, "phrase should match in sealed segment");
    }

    #[test]
    fn test_phrase_boost_factor() {
        let (index, branch_id) = phrase_test_index();

        let terms = vec!["machin".to_string(), "learn".to_string()];
        let phrases = vec![vec!["machin".to_string(), "learn".to_string()]];

        // Get base score without phrases
        let base = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        // Get boosted score with phrases (boost=3.0)
        let boosted = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig {
                phrases: &phrases,
                boost: 3.0,
                slop: 0,
                filter: false,
            },
            &ProximityConfig::off(),
            None,
        );

        // Find doc0 (has exact phrase) in both
        let base_doc0 = base
            .iter()
            .find(|r| {
                let e = index.resolve_doc_id(r.doc_id).unwrap();
                matches!(e, EntityRef::Kv { ref key, space: _, .. } if key == "doc0")
            })
            .unwrap();
        let boosted_doc0 = boosted
            .iter()
            .find(|r| {
                let e = index.resolve_doc_id(r.doc_id).unwrap();
                matches!(e, EntityRef::Kv { ref key, space: _, .. } if key == "doc0")
            })
            .unwrap();

        // Boosted score should be 3x the base score
        let ratio = boosted_doc0.score / base_doc0.score;
        assert!(
            (ratio - 3.0).abs() < 0.01,
            "expected 3x boost, got ratio: {}",
            ratio
        );
    }

    #[test]
    fn test_phrase_compound_boost() {
        // Two phrases: docs matching both get boost^2, docs matching one get boost^1
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc_both = kv_ref(branch_id, "both");
        index.index_document(&doc_both, "error code and stack trace", None);

        let doc_one = kv_ref(branch_id, "one");
        index.index_document(&doc_one, "error code without stack info", None);

        let terms = vec![
            "error".to_string(),
            "code".to_string(),
            "stack".to_string(),
            "trace".to_string(),
        ];
        let phrases = vec![
            vec!["error".to_string(), "code".to_string()],
            vec!["stack".to_string(), "trace".to_string()],
        ];

        // Boost mode with boost=2.0
        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig {
                phrases: &phrases,
                boost: 2.0,
                slop: 0,
                filter: false,
            },
            &ProximityConfig::off(),
            None,
        );
        assert!(result.len() >= 2);

        let both_score = result
            .iter()
            .find(|r| {
                let e = index.resolve_doc_id(r.doc_id).unwrap();
                matches!(e, EntityRef::Kv { ref key, space: _, .. } if key == "both")
            })
            .unwrap()
            .score;
        let one_score = result
            .iter()
            .find(|r| {
                let e = index.resolve_doc_id(r.doc_id).unwrap();
                matches!(e, EntityRef::Kv { ref key, space: _, .. } if key == "one")
            })
            .unwrap()
            .score;

        // "both" matches 2 phrases → boost^2 = 4x
        // "one" matches 1 phrase → boost^1 = 2x
        // So "both" should score higher than "one"
        assert!(
            both_score > one_score,
            "doc matching 2 phrases should score higher: both={} one={}",
            both_score,
            one_score
        );
    }

    // ====================================================================
    // Proximity scoring unit tests (#2240)
    // ====================================================================

    #[test]
    fn test_compute_min_span_basic() {
        // A at [5], B at [7], C at [9] → span = 9-5+1 = 5
        let positions = vec![
            SmallVec::from_slice(&[5u32]),
            SmallVec::from_slice(&[7u32]),
            SmallVec::from_slice(&[9u32]),
        ];
        assert_eq!(compute_min_span(&positions), Some(5));
    }

    #[test]
    fn test_compute_min_span_adjacent() {
        // Adjacent terms: span = 3
        let positions = vec![
            SmallVec::from_slice(&[0u32]),
            SmallVec::from_slice(&[1u32]),
            SmallVec::from_slice(&[2u32]),
        ];
        assert_eq!(compute_min_span(&positions), Some(3));
    }

    #[test]
    fn test_compute_min_span_multiple_positions() {
        // A at [5, 20, 100], B at [7, 50], C at [9, 80]
        // Best window: [5, 7, 9] → span = 5
        let positions = vec![
            SmallVec::from_slice(&[5u32, 20, 100]),
            SmallVec::from_slice(&[7u32, 50]),
            SmallVec::from_slice(&[9u32, 80]),
        ];
        assert_eq!(compute_min_span(&positions), Some(5));
    }

    #[test]
    fn test_compute_min_span_single_term() {
        let positions = vec![SmallVec::from_slice(&[3u32])];
        assert_eq!(compute_min_span(&positions), Some(1));
    }

    #[test]
    fn test_compute_min_span_empty() {
        let positions = vec![SmallVec::from_slice(&[3u32]), SmallVec::new()];
        assert_eq!(compute_min_span(&positions), None);
    }

    #[test]
    fn test_compute_min_span_same_position() {
        // Two terms at the same position → span = 1
        let positions = vec![SmallVec::from_slice(&[5u32]), SmallVec::from_slice(&[5u32])];
        assert_eq!(compute_min_span(&positions), Some(1));
    }

    #[test]
    fn test_compute_min_span_two_terms() {
        // A at [0, 10], B at [3, 8] → best is [8, 10] span=3 or [0, 3] span=4
        // Actually: pointer approach — start with (A=0, B=3), span=4.
        // Advance min (A=0→10), now (A=10, B=3), span=8. Advance min (B=3→8),
        // now (A=10, B=8), span=3. Advance min (B=8→exhausted). Best=3.
        let positions = vec![
            SmallVec::from_slice(&[0u32, 10]),
            SmallVec::from_slice(&[3u32, 8]),
        ];
        assert_eq!(compute_min_span(&positions), Some(3));
    }

    // ====================================================================
    // Proximity scoring integration tests
    // ====================================================================

    #[test]
    fn test_proximity_boost_close_terms() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Doc with terms close together
        let close = kv_ref(branch_id, "close");
        index.index_document(&close, "machine learning algorithms today", None);

        // Doc with terms far apart
        let far = kv_ref(branch_id, "far");
        index.index_document(
            &far,
            "machine tools and various other equipment for advanced learning",
            None,
        );

        let terms = vec!["machin".to_string(), "learn".to_string()];

        // With proximity enabled, "close" doc should score higher
        let with_prox = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::default_on(),
            None,
        );
        assert_eq!(with_prox.len(), 2);
        let close_score = with_prox
            .iter()
            .find(|r| {
                let e = index.resolve_doc_id(r.doc_id).unwrap();
                matches!(e, EntityRef::Kv { ref key, space: _, .. } if key == "close")
            })
            .unwrap()
            .score;
        let far_score = with_prox
            .iter()
            .find(|r| {
                let e = index.resolve_doc_id(r.doc_id).unwrap();
                matches!(e, EntityRef::Kv { ref key, space: _, .. } if key == "far")
            })
            .unwrap()
            .score;
        assert!(
            close_score > far_score,
            "close doc should score higher with proximity: close={} far={}",
            close_score,
            far_score
        );
    }

    #[test]
    fn test_proximity_disabled_no_effect() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc = kv_ref(branch_id, "doc");
        index.index_document(&doc, "machine learning algorithms", None);

        let terms = vec!["machin".to_string(), "learn".to_string()];

        let with_prox = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::default_on(),
            None,
        );
        let without_prox = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

        // With proximity, score should be higher (additive boost)
        assert!(
            with_prox[0].score > without_prox[0].score,
            "proximity should add to score: with={} without={}",
            with_prox[0].score,
            without_prox[0].score
        );
    }

    #[test]
    fn test_proximity_single_term_zero_overhead() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc = kv_ref(branch_id, "doc");
        index.index_document(&doc, "machine learning algorithms", None);

        let terms = vec!["machin".to_string()];

        // Single term: proximity should have no effect
        let with_prox = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::default_on(),
            None,
        );
        let without_prox = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

        assert_eq!(with_prox.len(), without_prox.len());
        assert!(
            (with_prox[0].score - without_prox[0].score).abs() < 1e-6,
            "single term should have no proximity effect"
        );
    }

    #[test]
    fn test_proximity_partial_coverage() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        // Doc has 2 of 3 query terms
        let doc = kv_ref(branch_id, "partial");
        index.index_document(&doc, "machine learning", None);

        let terms = vec![
            "machin".to_string(),
            "learn".to_string(),
            "algorithm".to_string(),
        ];

        let with_prox = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::default_on(),
            None,
        );
        let without_prox = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

        // Partial coverage should still add a (smaller) boost
        assert!(
            with_prox[0].score > without_prox[0].score,
            "partial coverage should still boost: with={} without={}",
            with_prox[0].score,
            without_prox[0].score
        );
    }

    #[test]
    fn test_proximity_sealed_segment() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc = kv_ref(branch_id, "sealed");
        index.index_document(&doc, "machine learning algorithms", None);
        index.seal_active();

        let terms = vec!["machin".to_string(), "learn".to_string()];

        let result = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::default_on(),
            None,
        );
        let base = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );

        assert!(
            result[0].score > base[0].score,
            "proximity should work on sealed segments"
        );
    }

    #[test]
    fn test_proximity_weight_scales() {
        let index = InvertedIndex::new();
        index.enable();
        let branch_id = BranchId::new();

        let doc = kv_ref(branch_id, "doc");
        index.index_document(&doc, "machine learning", None);

        let terms = vec!["machin".to_string(), "learn".to_string()];

        let base = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig::off(),
            None,
        );
        let w05 = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig {
                enabled: true,
                window: 10,
                weight: 0.5,
            },
            None,
        );
        let w10 = index.score_top_k(
            &terms,
            &branch_id,
            10,
            0.9,
            0.4,
            &PhraseConfig::none(),
            &ProximityConfig {
                enabled: true,
                window: 10,
                weight: 1.0,
            },
            None,
        );

        let boost_05 = w05[0].score - base[0].score;
        let boost_10 = w10[0].score - base[0].score;

        // weight=1.0 should give ~2x the boost of weight=0.5
        let ratio = boost_10 / boost_05;
        assert!(
            (ratio - 2.0).abs() < 0.01,
            "expected 2x ratio, got {}",
            ratio
        );
    }
}
