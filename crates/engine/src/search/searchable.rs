//! Searchable trait for primitives that support search
//!
//! This module defines the `Searchable` trait and search support structures
//! used by all primitives for retrieval operations.
//!
//! Scoring infrastructure:
//! - `SearchDoc`: ephemeral document representation for scoring
//! - `ScorerContext`: corpus-level statistics for BM25
//! - `Scorer` trait: pluggable scoring interface
//! - `BM25LiteScorer`: default BM25-inspired scorer

use super::index::InvertedIndex;
use super::tokenizer::tokenize;
use super::types::{EntityRef, SearchHit, SearchRequest, SearchResponse, SearchStats};
use crate::semantics::value::extractable_text;
use crate::StrataResult;
use std::collections::HashMap;
use strata_core::PrimitiveType;
use strata_core::Value;

/// Extract indexable text from a value for keyword search.
///
/// String values index directly. Numeric and structured values are serialized
/// through JSON. Nulls, booleans, and raw bytes do not contribute searchable
/// text.
pub fn extract_search_text(value: &Value) -> Option<String> {
    extractable_text(value)
}

/// Trait for primitives that support search
///
/// Each primitive implements this trait to provide its own search functionality
/// with primitive-specific text extraction.
///
/// # Invariant
///
/// All search methods return SearchResponse. No primitive-specific result types.
/// This invariant must not change.
pub trait Searchable {
    /// Search within this primitive
    ///
    /// Returns results matching the query within budget constraints.
    /// Uses a snapshot for consistency.
    fn search(&self, req: &SearchRequest) -> StrataResult<SearchResponse>;

    /// Get the primitive type
    fn primitive_kind(&self) -> PrimitiveType;
}

/// Internal candidate for scoring
///
/// Represents a document that matches the search criteria before final scoring.
#[derive(Debug, Clone)]
pub struct SearchCandidate {
    /// Back-pointer to source record
    pub doc_ref: EntityRef,
    /// Extracted text for scoring
    pub text: String,
    /// Timestamp for time-based filtering/ordering
    pub timestamp: Option<u64>,
    /// Pre-computed term frequencies and doc length from the inverted index.
    /// When present, BM25 scoring uses these directly instead of re-tokenizing
    /// the document text (which requires stemming every token).
    /// Format: (term -> frequency, document_length_in_tokens)
    pub precomputed_tf: Option<(HashMap<String, u32>, u32)>,
}

impl SearchCandidate {
    /// Create a new search candidate
    pub fn new(doc_ref: EntityRef, text: String, timestamp: Option<u64>) -> Self {
        SearchCandidate {
            doc_ref,
            text,
            timestamp,
            precomputed_tf: None,
        }
    }

    /// Create a search candidate with pre-computed term frequencies from the
    /// inverted index. This allows BM25 scoring to skip re-tokenizing the
    /// document body.
    pub fn with_precomputed_tf(mut self, term_freqs: HashMap<String, u32>, doc_len: u32) -> Self {
        self.precomputed_tf = Some((term_freqs, doc_len));
        self
    }
}

// ============================================================================
// SearchDoc
// ============================================================================

/// Internal representation of a document for scoring
///
/// This is an ephemeral view created during search, not stored.
/// Contains all information a scorer might need.
#[derive(Debug, Clone)]
pub struct SearchDoc {
    /// Primary searchable text
    pub body: String,

    /// Optional title (e.g., key name)
    pub title: Option<String>,

    /// Tags for filtering
    pub tags: Vec<String>,

    /// Timestamp in microseconds
    pub ts_micros: Option<u64>,

    /// Document size in bytes
    pub byte_size: Option<u32>,
}

impl SearchDoc {
    /// Create a new SearchDoc with body text
    pub fn new(body: String) -> Self {
        SearchDoc {
            body,
            title: None,
            tags: vec![],
            ts_micros: None,
            byte_size: None,
        }
    }

    /// Builder: set title
    pub fn with_title(mut self, title: String) -> Self {
        self.title = Some(title);
        self
    }

    /// Builder: set timestamp
    pub fn with_timestamp(mut self, ts: u64) -> Self {
        self.ts_micros = Some(ts);
        self
    }

    /// Builder: set tags
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Builder: set byte size
    pub fn with_byte_size(mut self, size: u32) -> Self {
        self.byte_size = Some(size);
        self
    }
}

// ============================================================================
// ScorerContext
// ============================================================================

/// Context for scoring operations
///
/// Contains corpus-level statistics needed for algorithms like BM25.
/// Built during candidate enumeration.
#[derive(Debug, Clone)]
pub struct ScorerContext {
    /// Total documents in corpus (for IDF calculation)
    pub total_docs: usize,

    /// Document frequency per term (for IDF calculation)
    pub doc_freqs: HashMap<String, usize>,

    /// Average document length in tokens (for length normalization)
    pub avg_doc_len: f32,

    /// Current timestamp for recency calculations (microseconds)
    pub now_micros: u64,

    /// Extension point for future scoring signals
    pub extensions: HashMap<String, serde_json::Value>,
}

impl ScorerContext {
    /// Create a new ScorerContext
    pub fn new(total_docs: usize) -> Self {
        ScorerContext {
            total_docs,
            doc_freqs: HashMap::new(),
            avg_doc_len: 0.0,
            now_micros: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            extensions: HashMap::new(),
        }
    }

    /// Compute IDF for a term
    ///
    /// Uses standard IDF formula with smoothing:
    /// IDF(t) = ln((N - df + 0.5) / (df + 0.5) + 1)
    pub fn idf(&self, term: &str) -> f32 {
        let df = self.doc_freqs.get(term).copied().unwrap_or(0) as f32;
        let n = self.total_docs as f32;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    /// Add document frequency for a term
    pub fn add_doc_freq(&mut self, term: &str, count: usize) {
        self.doc_freqs.insert(term.to_string(), count);
    }

    /// Set average document length
    pub fn with_avg_doc_len(mut self, len: f32) -> Self {
        self.avg_doc_len = len;
        self
    }
}

impl Default for ScorerContext {
    fn default() -> Self {
        Self::new(0)
    }
}

// ============================================================================
// Scorer Trait
// ============================================================================

/// Pluggable scoring interface
///
/// Scorers take a document and query and return a relevance score.
/// Higher scores indicate more relevant documents.
///
/// # Thread Safety
///
/// Scorers must be Send + Sync for concurrent search operations.
pub trait Scorer: Send + Sync {
    /// Score a document against a query
    ///
    /// Returns a score where higher = more relevant.
    fn score(&self, doc: &SearchDoc, query: &str, ctx: &ScorerContext) -> f32;

    /// Name for debugging and logging
    fn name(&self) -> &str;
}

// ============================================================================
// BM25LiteScorer
// ============================================================================

/// BM25-Lite: BM25-inspired scorer with recency and title boost
///
/// # BM25 Formula
///
/// For each query term t:
/// score += IDF(t) * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl/avgdl))
///
/// Where:
/// - tf = term frequency in document
/// - dl = document length
/// - avgdl = average document length
/// - k1 = term saturation parameter (default 0.9)
/// - b = length normalization parameter (default 0.4)
///
/// Defaults follow Anserini/Pyserini (k1=0.9, b=0.4), which outperform
/// Lucene's raw defaults (1.2/0.75) on BEIR benchmarks.
#[derive(Debug, Clone)]
pub struct BM25LiteScorer {
    /// k1 parameter: term frequency saturation (default 0.9)
    pub k1: f32,
    /// b parameter: length normalization (default 0.4)
    pub b: f32,
    /// Optional recency boost factor (0.0 = disabled)
    pub recency_boost: f32,
}

impl Default for BM25LiteScorer {
    fn default() -> Self {
        BM25LiteScorer {
            k1: 0.9,
            b: 0.4,
            recency_boost: 0.1,
        }
    }
}

impl BM25LiteScorer {
    /// Create a new BM25LiteScorer with custom parameters
    pub fn new(k1: f32, b: f32) -> Self {
        BM25LiteScorer {
            k1,
            b,
            recency_boost: 0.0,
        }
    }

    /// Builder: set recency boost factor
    pub fn with_recency_boost(mut self, factor: f32) -> Self {
        self.recency_boost = factor;
        self
    }

    /// Score using pre-computed term frequencies from the inverted index.
    ///
    /// This avoids re-tokenizing and re-stemming the entire document body,
    /// which is the dominant cost in BM25 scoring (~100ms on 5K docs).
    /// The term frequencies and doc length were already computed at index time.
    pub fn score_precomputed(
        &self,
        query_terms: &[String],
        doc_tf: &HashMap<String, u32>,
        doc_len: f32,
        ctx: &ScorerContext,
    ) -> f32 {
        if query_terms.is_empty() || doc_len == 0.0 {
            return 0.0;
        }

        let avg_len = ctx.avg_doc_len.max(1.0);
        let mut score = 0.0f32;

        for query_term in query_terms {
            let tf = doc_tf.get(query_term.as_str()).copied().unwrap_or(0) as f32;
            if tf == 0.0 {
                continue;
            }

            let idf = ctx.idf(query_term);
            let tf_component = (tf * (self.k1 + 1.0))
                / (tf + self.k1 * (1.0 - self.b + self.b * doc_len / avg_len));

            score += idf * tf_component;
        }

        score
    }
}

impl Scorer for BM25LiteScorer {
    fn score(&self, doc: &SearchDoc, query: &str, ctx: &ScorerContext) -> f32 {
        let query_terms = tokenize(query);
        let doc_terms = tokenize(&doc.body);
        let doc_len = doc_terms.len() as f32;

        if query_terms.is_empty() || doc_terms.is_empty() {
            return 0.0;
        }

        let mut score = 0.0;

        // Count term frequencies in document
        let mut doc_term_counts: HashMap<&str, usize> = HashMap::new();
        for term in &doc_terms {
            *doc_term_counts.entry(term.as_str()).or_insert(0) += 1;
        }

        // BM25 scoring
        for query_term in &query_terms {
            let tf = doc_term_counts
                .get(query_term.as_str())
                .copied()
                .unwrap_or(0) as f32;
            if tf == 0.0 {
                continue;
            }

            let idf = ctx.idf(query_term);

            // BM25 term score
            let avg_len = ctx.avg_doc_len.max(1.0);
            let tf_component = (tf * (self.k1 + 1.0))
                / (tf + self.k1 * (1.0 - self.b + self.b * doc_len / avg_len));

            score += idf * tf_component;
        }

        // Optional recency boost
        if self.recency_boost > 0.0 {
            if let Some(ts) = doc.ts_micros {
                let age_hours = (ctx.now_micros.saturating_sub(ts)) as f32 / 3_600_000_000.0;
                let recency_factor = 1.0 / (1.0 + age_hours / 24.0);
                score *= 1.0 + self.recency_boost * recency_factor;
            }
        }

        // Title match boost
        if let Some(title) = &doc.title {
            let title_terms = tokenize(title);
            for query_term in &query_terms {
                if title_terms.contains(query_term) {
                    score *= 1.2; // 20% boost
                    break;
                }
            }
        }

        score
    }

    fn name(&self) -> &str {
        "bm25-lite"
    }
}

// ============================================================================
// SimpleScorer (kept for backward compatibility)
// ============================================================================

/// Simple keyword matcher/scorer for search
///
/// BM25-lite implementation: scores based on term frequency and document length.
/// Kept for backward compatibility; new code should prefer `BM25LiteScorer`.
pub struct SimpleScorer;

impl SimpleScorer {
    /// Score a candidate against a query
    ///
    /// Returns a score in [0.0, 1.0] based on token overlap.
    pub fn score(query: &str, text: &str) -> f32 {
        if query.is_empty() || text.is_empty() {
            return 0.0;
        }

        let query_lower = query.to_lowercase();
        let text_lower = text.to_lowercase();

        let query_tokens: Vec<String> = query_lower
            .split_whitespace()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        let text_token_count = text_lower.split_whitespace().count();

        if query_tokens.is_empty() || text_token_count == 0 {
            return 0.0;
        }

        // Count matching tokens
        let mut matches = 0;
        for qt in &query_tokens {
            if text_lower.contains(qt.as_str()) {
                matches += 1;
            }
        }

        if matches == 0 {
            return 0.0;
        }

        // BM25-lite: TF * IDF approximation
        let tf = matches as f32 / query_tokens.len() as f32;
        let length_norm = 1.0 + (text_token_count as f32 / 100.0);
        let length_factor = 1.0 / length_norm;

        (tf * length_factor).clamp(0.01, 1.0)
    }

    /// Score candidates and return top-k hits
    pub fn score_and_rank(
        candidates: Vec<SearchCandidate>,
        query: &str,
        k: usize,
    ) -> Vec<SearchHit> {
        let mut scored: Vec<(SearchCandidate, f32)> = candidates
            .into_iter()
            .map(|c| {
                let score = Self::score(query, &c.text);
                (c, score)
            })
            .filter(|(_, score)| *score > 0.0)
            .collect();

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        scored
            .into_iter()
            .take(k)
            .enumerate()
            .map(|(i, (candidate, score))| SearchHit {
                doc_ref: candidate.doc_ref,
                score,
                rank: (i + 1) as u32,
                snippet: Some(truncate_text(&candidate.text, 100)),
            })
            .collect()
    }
}

/// Truncate text to max byte length, adding "..." if truncated.
/// Rounds down to the nearest char boundary to avoid splitting multi-byte UTF-8.
pub fn truncate_text(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        text.to_string()
    } else {
        let mut end = max_len.saturating_sub(3);
        // Walk back to a char boundary (at most 3 bytes for any UTF-8 sequence).
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &text[..end])
    }
}

/// Helper to build SearchResponse from scored candidates
///
/// Accepts an optional `&InvertedIndex` parameter. When the index is
/// enabled, uses corpus statistics (IDF, avg_doc_len) for proper BM25
/// scoring. When disabled or None, falls back to SimpleScorer (current
/// behavior).
pub fn build_search_response(
    candidates: Vec<SearchCandidate>,
    query: &str,
    k: usize,
    truncated: bool,
    elapsed_micros: u64,
) -> SearchResponse {
    build_search_response_with_index(candidates, query, k, truncated, elapsed_micros, None)
}

/// Build SearchResponse with optional index for BM25 corpus stats
pub fn build_search_response_with_index(
    candidates: Vec<SearchCandidate>,
    query: &str,
    k: usize,
    truncated: bool,
    elapsed_micros: u64,
    index: Option<&InvertedIndex>,
) -> SearchResponse {
    build_search_response_with_scorer(candidates, query, k, truncated, elapsed_micros, index, None)
}

/// Build SearchResponse with optional index and optional custom scorer.
///
/// When `scorer` is `None`, uses `BM25LiteScorer::default()`.
pub fn build_search_response_with_scorer(
    candidates: Vec<SearchCandidate>,
    query: &str,
    k: usize,
    truncated: bool,
    elapsed_micros: u64,
    index: Option<&InvertedIndex>,
    scorer: Option<&BM25LiteScorer>,
) -> SearchResponse {
    let candidates_count = candidates.len();

    // If index is enabled, use BM25LiteScorer with corpus stats
    let hits = if let Some(idx) = index {
        if idx.is_enabled() && idx.total_docs() > 0 {
            let mut ctx = ScorerContext::new(idx.total_docs());
            ctx.avg_doc_len = idx.avg_doc_len();

            // Tokenize query once
            let query_terms = tokenize(query);
            for term in &query_terms {
                ctx.add_doc_freq(term, idx.doc_freq(term));
            }

            let default_scorer = BM25LiteScorer::default();
            let scorer = scorer.unwrap_or(&default_scorer);

            let mut scored: Vec<(SearchCandidate, f32)> = candidates
                .into_iter()
                .map(|c| {
                    let score = if let Some((ref tf_map, doc_len)) = c.precomputed_tf {
                        // Fast path: use pre-computed term frequencies from the
                        // inverted index. Skips re-tokenizing the document body.
                        scorer.score_precomputed(&query_terms, tf_map, doc_len as f32, &ctx)
                    } else {
                        // Slow path: re-tokenize the document text for scoring.
                        let doc =
                            SearchDoc::new(c.text.clone()).with_timestamp(c.timestamp.unwrap_or(0));
                        scorer.score(&doc, query, &ctx)
                    };
                    (c, score)
                })
                .filter(|(_, score)| *score > 0.0)
                .collect();

            scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

            scored
                .into_iter()
                .take(k)
                .enumerate()
                .map(|(i, (candidate, score))| SearchHit {
                    doc_ref: candidate.doc_ref,
                    score,
                    rank: (i + 1) as u32,
                    snippet: Some(truncate_text(&candidate.text, 100)),
                })
                .collect()
        } else {
            // Index disabled or empty — fallback to SimpleScorer
            SimpleScorer::score_and_rank(candidates, query, k)
        }
    } else {
        // No index provided — use SimpleScorer
        SimpleScorer::score_and_rank(candidates, query, k)
    };

    let mut stats = SearchStats::new(elapsed_micros, candidates_count);
    if let Some(idx) = index {
        if idx.is_enabled() {
            stats = stats.with_index_used(true);
        }
    }

    SearchResponse {
        hits,
        truncated,
        stats,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_core::BranchId;

    #[test]
    fn test_simple_scorer_basic() {
        let score = SimpleScorer::score("hello world", "hello world this is a test");
        assert!(score > 0.0);
    }

    #[test]
    fn test_simple_scorer_no_match() {
        let score = SimpleScorer::score("xyz", "hello world");
        assert!(score == 0.0);
    }

    #[test]
    fn test_simple_scorer_partial_match() {
        let score = SimpleScorer::score("hello test", "hello world");
        assert!(score > 0.0);
        assert!(score < 1.0);
    }

    #[test]
    fn test_simple_scorer_case_insensitive() {
        let score1 = SimpleScorer::score("Hello", "hello world");
        let score2 = SimpleScorer::score("hello", "HELLO WORLD");
        assert!(score1 > 0.0);
        assert!(score2 > 0.0);
    }

    #[test]
    fn test_score_and_rank() {
        let branch_id = BranchId::new();
        let candidates = vec![
            SearchCandidate::new(
                EntityRef::Branch { branch_id },
                "hello world".to_string(),
                None,
            ),
            SearchCandidate::new(
                EntityRef::Branch { branch_id },
                "hello hello hello".to_string(),
                None,
            ),
            SearchCandidate::new(
                EntityRef::Branch { branch_id },
                "goodbye world".to_string(),
                None,
            ),
        ];

        let hits = SimpleScorer::score_and_rank(candidates, "hello", 10);
        assert!(!hits.is_empty());
        assert_eq!(hits[0].rank, 1);
        assert!(hits[0].score >= hits.last().map(|h| h.score).unwrap_or(0.0));
    }

    #[test]
    fn test_score_and_rank_respects_k() {
        let branch_id = BranchId::new();
        let candidates: Vec<_> = (0..100)
            .map(|i| {
                SearchCandidate::new(
                    EntityRef::Branch { branch_id },
                    format!("hello document {}", i),
                    None,
                )
            })
            .collect();

        let hits = SimpleScorer::score_and_rank(candidates, "hello", 5);
        assert_eq!(hits.len(), 5);
    }

    #[test]
    fn test_truncate_text() {
        assert_eq!(truncate_text("short", 10), "short");
        assert_eq!(truncate_text("this is a longer string", 10), "this is...");
    }

    #[test]
    fn test_truncate_text_multibyte() {
        // "′" is 3 bytes (U+2032). A cut at byte 7 would land inside it.
        let text = "hello ′world";
        let result = truncate_text(text, 10);
        // Should not panic, and should round down to a char boundary.
        assert!(result.ends_with("..."));
        assert!(result.len() <= 10);
    }

    // BM25LiteScorer tests

    #[test]
    fn test_bm25_basic_scoring() {
        let scorer = BM25LiteScorer::default();
        let doc = SearchDoc::new("the quick brown fox jumps over the lazy dog".into());
        let mut ctx = ScorerContext::new(100);
        ctx.add_doc_freq("quick", 10);
        ctx.add_doc_freq("fox", 5);
        ctx.avg_doc_len = 10.0;

        let score = scorer.score(&doc, "quick fox", &ctx);
        assert!(score > 0.0);
    }

    #[test]
    fn test_bm25_no_match() {
        let scorer = BM25LiteScorer::default();
        let doc = SearchDoc::new("hello world".into());
        let ctx = ScorerContext::default();

        let score = scorer.score(&doc, "banana", &ctx);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_bm25_empty_query() {
        let scorer = BM25LiteScorer::default();
        let doc = SearchDoc::new("hello world".into());
        let ctx = ScorerContext::default();

        let score = scorer.score(&doc, "", &ctx);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn test_bm25_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BM25LiteScorer>();
    }

    #[test]
    fn test_build_search_response_with_index_enabled() {
        let branch_id = BranchId::new();
        let index = InvertedIndex::new();
        index.enable();

        // Index some documents
        let ref1 = EntityRef::Kv {
            branch_id,
            space: "default".to_string(),
            key: "doc1".to_string(),
        };
        let ref2 = EntityRef::Kv {
            branch_id,
            space: "default".to_string(),
            key: "doc2".to_string(),
        };
        index.index_document(&ref1, "hello world test", None);
        index.index_document(&ref2, "hello there", None);

        let candidates = vec![
            SearchCandidate::new(ref1, "hello world test".to_string(), None),
            SearchCandidate::new(ref2, "hello there".to_string(), None),
        ];

        let response =
            build_search_response_with_index(candidates, "hello", 10, false, 100, Some(&index));

        assert!(!response.hits.is_empty());
        assert!(response.stats.index_used);
    }

    #[test]
    fn test_build_search_response_without_index() {
        let branch_id = BranchId::new();
        let candidates = vec![SearchCandidate::new(
            EntityRef::Branch { branch_id },
            "hello world".to_string(),
            None,
        )];

        let response = build_search_response(candidates, "hello", 10, false, 100);
        assert!(!response.hits.is_empty());
        assert!(!response.stats.index_used);
    }
}
