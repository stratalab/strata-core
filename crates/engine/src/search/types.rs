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
use strata_core::id::CommitVersion;
use strata_core::types::BranchId;

use crate::JsonValue;

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
/// search (e.g., `substrate::retrieve()`).
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

    /// Space to search within (defaults to "default").
    pub space: String,

    /// Optional: MVCC snapshot version for cross-primitive consistency (#1921).
    /// When set, all primitive searches in a retrieval substrate query share the
    /// same snapshot. Primitives that support versioned reads use this bound; the
    /// inverted index scoring is not yet version-bounded (future work).
    pub snapshot_version: Option<CommitVersion>,

    /// Optional: field filter for secondary index queries on JsonStore.
    /// When present, only documents matching the filter are returned.
    pub field_filter: Option<FieldFilter>,

    /// Optional: sort results by an indexed field instead of by score.
    /// The field must have a secondary index. When set, results are returned
    /// in index order rather than sorted by doc_id or score.
    pub sort_by: Option<SortSpec>,

    /// BM25 k1 parameter (term frequency saturation). Default: 0.9.
    /// Set by the retrieval substrate from the recipe's BM25Config.
    pub bm25_k1: f32,

    /// BM25 b parameter (document length normalization). Default: 0.4.
    /// Set by the retrieval substrate from the recipe's BM25Config.
    pub bm25_b: f32,

    /// Phrase boost factor. Default: 2.0.
    pub phrase_boost: f32,

    /// Phrase slop (max word gap between phrase terms). Default: 0.
    pub phrase_slop: u32,

    /// Phrase mode: true = filter (exclude non-matching), false = boost (default).
    pub phrase_filter: bool,

    /// Enable proximity scoring. Default: true.
    pub proximity: bool,

    /// Proximity window size (in word positions). Default: 10.
    pub proximity_window: u32,

    /// Proximity boost weight. Default: 0.5.
    pub proximity_weight: f32,
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
            space: "default".to_string(),
            snapshot_version: None,
            field_filter: None,
            bm25_k1: 0.9,
            bm25_b: 0.4,
            phrase_boost: 2.0,
            phrase_slop: 0,
            phrase_filter: false,
            proximity: true,
            proximity_window: 10,
            proximity_weight: 0.5,
            sort_by: None,
        }
    }

    /// Builder: set field filter for secondary index queries
    pub fn with_field_filter(mut self, filter: FieldFilter) -> Self {
        self.field_filter = Some(filter);
        self
    }

    /// Builder: sort results by an indexed field
    pub fn with_sort_by(mut self, field: impl Into<String>, direction: SortDirection) -> Self {
        self.sort_by = Some(SortSpec {
            field: field.into(),
            direction,
        });
        self
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

    /// Builder: set space to search within
    pub fn with_space(mut self, space: impl Into<String>) -> Self {
        self.space = space.into();
        self
    }

    /// Builder: set BM25 scoring parameters from recipe
    pub fn with_bm25_params(mut self, k1: f32, b: f32) -> Self {
        self.bm25_k1 = k1;
        self.bm25_b = b;
        self
    }

    /// Builder: set phrase matching parameters from recipe
    pub fn with_phrase_params(mut self, boost: f32, slop: u32, filter: bool) -> Self {
        self.phrase_boost = boost;
        self.phrase_slop = slop;
        self.phrase_filter = filter;
        self
    }

    /// Builder: set proximity scoring parameters from recipe
    pub fn with_proximity_params(mut self, enabled: bool, window: u32, weight: f32) -> Self {
        self.proximity = enabled;
        self.proximity_window = window;
        self.proximity_weight = weight;
        self
    }

    /// Builder: set MVCC snapshot version for consistent cross-primitive reads
    pub fn with_snapshot_version(mut self, version: CommitVersion) -> Self {
        self.snapshot_version = Some(version);
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
// Field Filter Types (for secondary index queries)
// ============================================================================

/// A predicate on an indexed JSON field.
///
/// Each predicate maps to a scan on a secondary index.
/// The `field` is the `field_path` from the index definition (e.g., "$.price").
#[derive(Debug, Clone)]
pub enum FieldPredicate {
    /// Exact match: `@status:{active}`
    Eq {
        /// Field path (e.g., "$.status")
        field: String,
        /// Value to match
        value: JsonValue,
    },

    /// Range query: `@price:[100 200]`
    Range {
        /// Field path (e.g., "$.price")
        field: String,
        /// Lower bound (None = unbounded)
        lower: Option<JsonValue>,
        /// Upper bound (None = unbounded)
        upper: Option<JsonValue>,
        /// Whether lower bound is inclusive
        lower_inclusive: bool,
        /// Whether upper bound is inclusive
        upper_inclusive: bool,
    },

    /// Prefix match on text: `@name:wire*`
    Prefix {
        /// Field path (e.g., "$.name")
        field: String,
        /// Prefix to match
        prefix: String,
    },
}

/// Compound filter with boolean logic over field predicates.
///
/// Used to narrow the candidate set before text scoring.
#[derive(Debug, Clone)]
pub enum FieldFilter {
    /// A single predicate
    Predicate(FieldPredicate),
    /// All sub-filters must match (intersection)
    And(Vec<FieldFilter>),
    /// Any sub-filter must match (union)
    Or(Vec<FieldFilter>),
}

// ============================================================================
// Sort Specification
// ============================================================================

/// Sort direction for index-backed ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending (smallest first)
    Asc,
    /// Descending (largest first)
    Desc,
}

/// Specification for sorting search results by an indexed field.
#[derive(Debug, Clone)]
pub struct SortSpec {
    /// Field path to sort by (must have a secondary index)
    pub field: String,
    /// Sort direction
    pub direction: SortDirection,
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
    }

    #[test]
    fn test_issue_1921_snapshot_version_in_request() {
        let req = SearchRequest::new(BranchId::new(), "test query")
            .with_snapshot_version(CommitVersion(42));
        assert_eq!(req.snapshot_version, Some(CommitVersion(42)));

        // Default should be None
        let req2 = SearchRequest::new(BranchId::new(), "test query");
        assert_eq!(req2.snapshot_version, None);
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
