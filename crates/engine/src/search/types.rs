//! Core search types for Retrieval Surfaces
//!
//! This module defines the foundational search types used throughout the system:
//! - SearchRequest: Universal request type for all search APIs
//! - SearchBudget: Time and candidate limits for search execution
//! - SearchResponse: Results from any search operation
//! - SearchHit: Individual search result with score and rank
//! - SearchStats: Execution statistics for debugging/monitoring
//!
//!
//! These types define the interface contracts for search operations.
//! See `the architecture documentation` for authoritative specification.

use std::collections::HashMap;
use strata_core::types::BranchId;

// Re-export contract types
pub use strata_core::contract::EntityRef;
pub use strata_core::contract::PrimitiveType;

// ============================================================================
// SearchBudget
// ============================================================================

/// Limits on search execution
///
/// Search operations respect these limits and return truncated
/// results rather than timing out or erroring. This provides
/// predictable latency and graceful degradation.
///
/// # Default Values
///
/// - max_wall_time_micros: 100,000 (100ms)
/// - max_candidates: 10,000
/// - max_candidates_per_primitive: 2,000
#[derive(Debug, Clone, Copy)]
pub struct SearchBudget {
    /// Hard stop on wall time (microseconds)
    pub max_wall_time_micros: u64,

    /// Maximum total candidates to consider
    pub max_candidates: usize,

    /// Maximum candidates per primitive (for composite search)
    pub max_candidates_per_primitive: usize,
}

impl Default for SearchBudget {
    fn default() -> Self {
        SearchBudget {
            max_wall_time_micros: 100_000, // 100ms
            max_candidates: 10_000,
            max_candidates_per_primitive: 2_000,
        }
    }
}

impl SearchBudget {
    /// Create a new SearchBudget with custom limits
    pub fn new(max_time_micros: u64, max_candidates: usize) -> Self {
        SearchBudget {
            max_wall_time_micros: max_time_micros,
            max_candidates,
            max_candidates_per_primitive: max_candidates / 6, // Split across 6 primitives
        }
    }

    /// Builder: set max wall time
    pub fn with_time(mut self, micros: u64) -> Self {
        self.max_wall_time_micros = micros;
        self
    }

    /// Builder: set max candidates
    pub fn with_candidates(mut self, max: usize) -> Self {
        self.max_candidates = max;
        self
    }

    /// Builder: set max candidates per primitive
    pub fn with_per_primitive(mut self, max: usize) -> Self {
        self.max_candidates_per_primitive = max;
        self
    }
}

// ============================================================================
// SearchMode
// ============================================================================

/// Search mode - determines the search strategy
///
/// Currently implements Keyword mode. Vector and Hybrid are reserved
/// for future enhancements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchMode {
    /// Keyword-based search using BM25-lite (default)
    #[default]
    Keyword,
    /// Reserved for future vector search
    Vector,
    /// Reserved for future hybrid search (keyword + vector)
    Hybrid,
}

// ============================================================================
// SearchRequest
// ============================================================================

/// Request for search across primitives
///
/// This is the universal search request type used by both
/// primitive-level search (e.g., `kv.search()`) and composite
/// search (e.g., `db.hybrid().search()`).
///
/// # Invariant
///
/// The same SearchRequest type is used for all search operations.
/// This invariant must not change.
///
/// # Examples
///
/// ```
/// use strata_engine::{SearchRequest, SearchBudget};
/// use strata_core::types::BranchId;
///
/// let branch_id = BranchId::new();
/// let req = SearchRequest::new(branch_id, "authentication error")
///     .with_k(20)
///     .with_budget(SearchBudget::default().with_time(50_000));
///
/// assert_eq!(req.query, "authentication error");
/// assert_eq!(req.k, 20);
/// ```
#[derive(Debug, Clone)]
pub struct SearchRequest {
    /// Run to search within
    pub branch_id: BranchId,

    /// Query string (interpreted by scorer)
    pub query: String,

    /// Maximum results to return (top-k)
    pub k: usize,

    /// Time and work limits
    pub budget: SearchBudget,

    /// Search mode (Keyword, Vector, Hybrid)
    pub mode: SearchMode,

    /// Optional: limit to specific primitives (for composite search)
    pub primitive_filter: Option<Vec<PrimitiveType>>,

    /// Optional: time range filter (microseconds since epoch)
    pub time_range: Option<(u64, u64)>,

    /// Optional: tag filter (match any)
    pub tags_any: Vec<String>,

    /// Optional: precomputed query embedding (skip embedder call in hybrid search).
    /// Used by `search_expanded()` to batch-embed all expansion texts upfront.
    pub precomputed_embedding: Option<Vec<f32>>,
}

impl SearchRequest {
    /// Create a new SearchRequest with defaults
    ///
    /// Default values:
    /// - k: 10
    /// - budget: SearchBudget::default()
    /// - mode: SearchMode::Keyword
    /// - primitive_filter: None (search all primitives)
    /// - time_range: None
    /// - tags_any: empty
    /// - precomputed_embedding: None
    pub fn new(branch_id: BranchId, query: impl Into<String>) -> Self {
        SearchRequest {
            branch_id,
            query: query.into(),
            k: 10,
            budget: SearchBudget::default(),
            mode: SearchMode::default(),
            primitive_filter: None,
            time_range: None,
            tags_any: vec![],
            precomputed_embedding: None,
        }
    }

    /// Builder: set top-k results count
    pub fn with_k(mut self, k: usize) -> Self {
        self.k = k;
        self
    }

    /// Builder: set search budget
    pub fn with_budget(mut self, budget: SearchBudget) -> Self {
        self.budget = budget;
        self
    }

    /// Builder: set search mode
    pub fn with_mode(mut self, mode: SearchMode) -> Self {
        self.mode = mode;
        self
    }

    /// Builder: set primitive filter
    pub fn with_primitive_filter(mut self, filter: Vec<PrimitiveType>) -> Self {
        self.primitive_filter = Some(filter);
        self
    }

    /// Builder: set time range filter
    pub fn with_time_range(mut self, start: u64, end: u64) -> Self {
        self.time_range = Some((start, end));
        self
    }

    /// Builder: set tags filter
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags_any = tags;
        self
    }

    /// Builder: set precomputed query embedding (avoids re-embedding in hybrid search)
    pub fn with_precomputed_embedding(mut self, embedding: Vec<f32>) -> Self {
        self.precomputed_embedding = Some(embedding);
        self
    }

    /// Check if a primitive is included in this request
    pub fn includes_primitive(&self, kind: PrimitiveType) -> bool {
        match &self.primitive_filter {
            Some(filter) => filter.contains(&kind),
            None => true, // No filter means include all
        }
    }
}

// ============================================================================
// SearchHit
// ============================================================================

/// A single search result
///
/// Contains a back-pointer to the source record (EntityRef),
/// the score from the scorer, and the rank in the result set.
#[derive(Debug, Clone)]
pub struct SearchHit {
    /// Back-pointer to source record
    pub doc_ref: EntityRef,

    /// Score from scorer (higher = more relevant)
    pub score: f32,

    /// Rank in result set (1-indexed)
    pub rank: u32,

    /// Optional snippet for display
    pub snippet: Option<String>,
}

impl SearchHit {
    /// Create a new SearchHit
    pub fn new(doc_ref: EntityRef, score: f32, rank: u32) -> Self {
        SearchHit {
            doc_ref,
            score,
            rank,
            snippet: None,
        }
    }

    /// Builder: set snippet
    pub fn with_snippet(mut self, snippet: String) -> Self {
        self.snippet = Some(snippet);
        self
    }
}

// ============================================================================
// SearchStats
// ============================================================================

/// Execution statistics for a search
///
/// Provides metadata about how the search was executed,
/// useful for debugging and monitoring.
#[derive(Debug, Clone, Default)]
pub struct SearchStats {
    /// Time spent in search (microseconds)
    pub elapsed_micros: u64,

    /// Total candidates considered
    pub candidates_considered: usize,

    /// Candidates per primitive (for composite search)
    pub candidates_by_primitive: HashMap<PrimitiveType, usize>,

    /// Whether an index was used (vs. full scan)
    pub index_used: bool,
}

impl SearchStats {
    /// Create new SearchStats
    pub fn new(elapsed_micros: u64, candidates: usize) -> Self {
        SearchStats {
            elapsed_micros,
            candidates_considered: candidates,
            candidates_by_primitive: HashMap::new(),
            index_used: false,
        }
    }

    /// Builder: set index_used flag
    pub fn with_index_used(mut self, used: bool) -> Self {
        self.index_used = used;
        self
    }

    /// Add candidates count for a primitive
    pub fn add_primitive_candidates(&mut self, kind: PrimitiveType, count: usize) {
        self.candidates_by_primitive.insert(kind, count);
        self.candidates_considered += count;
    }
}

// ============================================================================
// SearchResponse
// ============================================================================

/// Search results
///
/// Returned by both primitive-level and composite search.
/// Contains ranked hits plus execution metadata.
///
/// # Invariant
///
/// All search methods return SearchResponse. No primitive-specific
/// result types. This invariant must not change.
#[derive(Debug, Clone)]
pub struct SearchResponse {
    /// Ranked hits (highest score first)
    pub hits: Vec<SearchHit>,

    /// True if budget caused early termination
    pub truncated: bool,

    /// Execution statistics
    pub stats: SearchStats,
}

impl SearchResponse {
    /// Create an empty response
    pub fn empty() -> Self {
        SearchResponse {
            hits: vec![],
            truncated: false,
            stats: SearchStats::default(),
        }
    }

    /// Create a new response
    pub fn new(hits: Vec<SearchHit>, truncated: bool, stats: SearchStats) -> Self {
        SearchResponse {
            hits,
            truncated,
            stats,
        }
    }

    /// Check if response has no hits
    pub fn is_empty(&self) -> bool {
        self.hits.is_empty()
    }

    /// Get number of hits
    pub fn len(&self) -> usize {
        self.hits.len()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // PrimitiveType and EntityRef tests are now in contract module

    // ========================================
    // SearchBudget Tests
    // ========================================

    #[test]
    fn test_search_budget_defaults() {
        let budget = SearchBudget::default();

        assert_eq!(budget.max_wall_time_micros, 100_000);
        assert_eq!(budget.max_candidates, 10_000);
        assert_eq!(budget.max_candidates_per_primitive, 2_000);
    }

    #[test]
    fn test_search_budget_builder() {
        let budget = SearchBudget::default()
            .with_time(50_000)
            .with_candidates(5_000)
            .with_per_primitive(1_000);

        assert_eq!(budget.max_wall_time_micros, 50_000);
        assert_eq!(budget.max_candidates, 5_000);
        assert_eq!(budget.max_candidates_per_primitive, 1_000);
    }

    #[test]
    fn test_search_budget_new() {
        let budget = SearchBudget::new(200_000, 20_000);

        assert_eq!(budget.max_wall_time_micros, 200_000);
        assert_eq!(budget.max_candidates, 20_000);
        // 20_000 / 6 = 3_333
        assert_eq!(budget.max_candidates_per_primitive, 3_333);
    }

    // ========================================
    // SearchMode Tests
    // ========================================

    #[test]
    fn test_search_mode_default() {
        let mode = SearchMode::default();
        assert_eq!(mode, SearchMode::Keyword);
    }

    #[test]
    fn test_search_mode_variants() {
        let _keyword = SearchMode::Keyword;
        let _vector = SearchMode::Vector;
        let _hybrid = SearchMode::Hybrid;
    }

    // ========================================
    // SearchRequest Tests
    // ========================================

    #[test]
    fn test_search_request_new() {
        let branch_id = BranchId::new();
        let req = SearchRequest::new(branch_id, "test query");

        assert_eq!(req.branch_id, branch_id);
        assert_eq!(req.query, "test query");
        assert_eq!(req.k, 10);
        assert_eq!(req.mode, SearchMode::Keyword);
        assert!(req.primitive_filter.is_none());
        assert!(req.time_range.is_none());
        assert!(req.tags_any.is_empty());
    }

    #[test]
    fn test_search_request_builder() {
        let branch_id = BranchId::new();
        let req = SearchRequest::new(branch_id, "test query")
            .with_k(20)
            .with_budget(SearchBudget::default().with_time(50_000))
            .with_mode(SearchMode::Keyword)
            .with_primitive_filter(vec![PrimitiveType::Kv, PrimitiveType::Json])
            .with_time_range(1000, 2000)
            .with_tags(vec!["important".to_string()]);

        assert_eq!(req.k, 20);
        assert_eq!(req.budget.max_wall_time_micros, 50_000);
        assert_eq!(
            req.primitive_filter,
            Some(vec![PrimitiveType::Kv, PrimitiveType::Json])
        );
        assert_eq!(req.time_range, Some((1000, 2000)));
        assert_eq!(req.tags_any, vec!["important".to_string()]);
    }

    #[test]
    fn test_search_request_includes_primitive() {
        let branch_id = BranchId::new();

        // No filter - includes all
        let req1 = SearchRequest::new(branch_id, "test");
        assert!(req1.includes_primitive(PrimitiveType::Kv));
        assert!(req1.includes_primitive(PrimitiveType::Json));
        assert!(req1.includes_primitive(PrimitiveType::Event));

        // With filter - only includes specified
        let req2 = SearchRequest::new(branch_id, "test")
            .with_primitive_filter(vec![PrimitiveType::Kv, PrimitiveType::Json]);
        assert!(req2.includes_primitive(PrimitiveType::Kv));
        assert!(req2.includes_primitive(PrimitiveType::Json));
        assert!(!req2.includes_primitive(PrimitiveType::Event));
        assert!(!req2.includes_primitive(PrimitiveType::State));
    }

    // ========================================
    // SearchHit Tests
    // ========================================

    #[test]
    fn test_search_hit_new() {
        let branch_id = BranchId::new();
        let doc_ref = EntityRef::branch(branch_id);

        let hit = SearchHit::new(doc_ref.clone(), 0.95, 1);

        assert_eq!(hit.doc_ref, doc_ref);
        assert!((hit.score - 0.95).abs() < f32::EPSILON);
        assert_eq!(hit.rank, 1);
        assert!(hit.snippet.is_none());
    }

    #[test]
    fn test_search_hit_with_snippet() {
        let branch_id = BranchId::new();
        let doc_ref = EntityRef::branch(branch_id);

        let hit = SearchHit::new(doc_ref, 0.95, 1).with_snippet("matched text here".to_string());

        assert_eq!(hit.snippet, Some("matched text here".to_string()));
    }

    // ========================================
    // SearchStats Tests
    // ========================================

    #[test]
    fn test_search_stats_default() {
        let stats = SearchStats::default();

        assert_eq!(stats.elapsed_micros, 0);
        assert_eq!(stats.candidates_considered, 0);
        assert!(stats.candidates_by_primitive.is_empty());
        assert!(!stats.index_used);
    }

    #[test]
    fn test_search_stats_new() {
        let stats = SearchStats::new(1000, 500);

        assert_eq!(stats.elapsed_micros, 1000);
        assert_eq!(stats.candidates_considered, 500);
    }

    #[test]
    fn test_search_stats_builder() {
        let stats = SearchStats::new(1000, 500).with_index_used(true);

        assert!(stats.index_used);
    }

    #[test]
    fn test_search_stats_add_primitive_candidates() {
        let mut stats = SearchStats::default();

        stats.add_primitive_candidates(PrimitiveType::Kv, 100);
        stats.add_primitive_candidates(PrimitiveType::Json, 200);

        assert_eq!(stats.candidates_considered, 300);
        assert_eq!(
            stats.candidates_by_primitive.get(&PrimitiveType::Kv),
            Some(&100)
        );
        assert_eq!(
            stats.candidates_by_primitive.get(&PrimitiveType::Json),
            Some(&200)
        );
    }

    // ========================================
    // SearchResponse Tests
    // ========================================

    #[test]
    fn test_search_response_empty() {
        let response = SearchResponse::empty();

        assert!(response.is_empty());
        assert_eq!(response.len(), 0);
        assert!(!response.truncated);
    }

    #[test]
    fn test_search_response_new() {
        let branch_id = BranchId::new();
        let hits = vec![
            SearchHit::new(EntityRef::branch(branch_id), 0.9, 1),
            SearchHit::new(EntityRef::branch(branch_id), 0.8, 2),
        ];
        let stats = SearchStats::new(500, 100);

        let response = SearchResponse::new(hits, true, stats);

        assert_eq!(response.len(), 2);
        assert!(!response.is_empty());
        assert!(response.truncated);
        assert_eq!(response.stats.elapsed_micros, 500);
    }
}
