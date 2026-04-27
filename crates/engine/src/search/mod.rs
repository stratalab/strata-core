//! Search module for keyword and retrieval operations
//!
//! This module contains:
//! - `types`: Core search types (SearchRequest, SearchResponse, SearchHit, etc.)
//! - `searchable`: Searchable trait and scoring infrastructure
//! - `index`: Optional inverted index for fast keyword search
//! - `segment`: Sealed segment file format (.sidx) for persistence
//! - `manifest`: Search manifest for persisting segmented index state
//! - `recovery`: SearchSubsystem for restoring the inverted index on startup
//! - `tokenizer`: Basic text tokenization

mod index;
pub(crate) mod manifest;
pub mod recipe;
pub(crate) mod recovery;
mod searchable;
pub(crate) mod segment;
pub mod stemmer;
pub mod tokenizer;
mod types;

pub use index::{
    InvertedIndex, PhraseConfig, PostingEntry, PostingList, ProximityConfig, ScoredDocId,
};
pub use recipe::Recipe;
pub use recovery::{extract_indexable_text, SearchSubsystem};
pub use searchable::{
    build_search_response, build_search_response_with_index, build_search_response_with_scorer,
    extract_search_text, truncate_text, BM25LiteScorer, Scorer, ScorerContext, SearchCandidate,
    SearchDoc, Searchable, SimpleScorer,
};
pub use tokenizer::{
    parse_query, tokenize, tokenize_unique, tokenize_with_positions, ParsedQuery, Token,
};
pub use types::{
    EntityRef, FieldFilter, FieldPredicate, PrimitiveType, SearchBudget, SearchHit, SearchMode,
    SearchRequest, SearchResponse, SearchStats, SortDirection, SortSpec,
};
