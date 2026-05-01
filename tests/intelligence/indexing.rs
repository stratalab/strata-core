//! Tier 7: Index Consistency
//!
//! Tests for inverted index behavior and consistency.

use strata_core::{BranchId, EntityRef as DocRef};
use strata_engine::search::InvertedIndex;

const DEFAULT_SPACE: &str = "default";

fn kv_doc_ref(branch_id: &BranchId, key: impl Into<String>) -> DocRef {
    DocRef::kv(branch_id.clone(), DEFAULT_SPACE, key.into())
}

// ============================================================================
// Index Enable/Disable Tests
// ============================================================================

/// Index is disabled by default
#[test]
fn test_tier7_index_disabled_by_default() {
    let index = InvertedIndex::new();
    assert!(!index.is_enabled());
}

/// Index can be enabled
#[test]
fn test_tier7_index_enable() {
    let index = InvertedIndex::new();
    index.enable();
    assert!(index.is_enabled());
}

/// Index can be disabled
#[test]
fn test_tier7_index_disable() {
    let index = InvertedIndex::new();
    index.enable();
    index.disable();
    assert!(!index.is_enabled());
}

/// Adding documents when disabled is a no-op
#[test]
fn test_tier7_add_when_disabled_noop() {
    let index = InvertedIndex::new();
    let branch_id = BranchId::new();
    let doc_ref = kv_doc_ref(&branch_id, "test");

    // Add without enabling
    index.index_document(&doc_ref, "test content", None);

    // Should return None (disabled)
    assert!(index.lookup("test").is_none());
}

/// Adding documents when enabled works
#[test]
fn test_tier7_add_when_enabled_works() {
    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();
    let doc_ref = kv_doc_ref(&branch_id, "test");

    index.index_document(&doc_ref, "hello world", None);

    let results = index.lookup("hello");
    assert!(results.is_some());
    assert_eq!(results.unwrap().len(), 1);
}

// ============================================================================
// Index Lookup Tests
// ============================================================================

/// Index returns matching documents
#[test]
fn test_tier7_index_returns_matches() {
    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();

    let ref1 = kv_doc_ref(&branch_id, "doc1");
    let ref2 = kv_doc_ref(&branch_id, "doc2");
    let ref3 = kv_doc_ref(&branch_id, "doc3");

    index.index_document(&ref1, "hello world", None);
    index.index_document(&ref2, "hello there", None);
    index.index_document(&ref3, "goodbye world", None);

    let results = index.lookup("hello");
    assert!(results.is_some());
    assert_eq!(results.unwrap().len(), 2);
}

/// Index lookup is case-insensitive (via tokenizer)
#[test]
fn test_tier7_index_case_insensitive() {
    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();
    let doc_ref = kv_doc_ref(&branch_id, "doc");

    index.index_document(&doc_ref, "Hello World", None);

    // Should match (tokenizer lowercases)
    assert!(index.lookup("hello").is_some());
}

/// Index lookup returns None for no matches
#[test]
fn test_tier7_index_no_matches_empty() {
    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();
    let doc_ref = kv_doc_ref(&branch_id, "doc");

    index.index_document(&doc_ref, "hello world", None);

    let results = index.lookup("banana");
    assert!(results.is_none());
}

// ============================================================================
// Index Document Management Tests
// ============================================================================

/// Index can remove documents
#[test]
fn test_tier7_index_remove_document() {
    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();
    let doc_ref = kv_doc_ref(&branch_id, "doc");

    index.index_document(&doc_ref, "hello world", None);
    assert_eq!(index.lookup("hello").unwrap().len(), 1);

    index.remove_document(&doc_ref);
    let results = index.lookup("hello");
    assert!(results.is_none() || results.unwrap().is_empty());
}

/// Index can be cleared
#[test]
fn test_tier7_index_clear() {
    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();

    for i in 0..10 {
        let doc_ref = kv_doc_ref(&branch_id, format!("doc{}", i));
        index.index_document(&doc_ref, "hello world", None);
    }

    assert_eq!(index.lookup("hello").unwrap().len(), 10);

    index.clear();
    let results = index.lookup("hello");
    assert!(results.is_none() || results.unwrap().is_empty());
}

// ============================================================================
// Index Statistics Tests
// ============================================================================

/// Index tracks total documents
#[test]
fn test_tier7_index_total_docs() {
    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();

    assert_eq!(index.total_docs(), 0);

    for i in 0..5 {
        let doc_ref = kv_doc_ref(&branch_id, format!("doc{}", i));
        index.index_document(&doc_ref, "hello world", None);
    }

    assert_eq!(index.total_docs(), 5);
}

/// Index tracks average document length
#[test]
fn test_tier7_index_avg_doc_len() {
    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();

    let ref1 = kv_doc_ref(&branch_id, "doc1");
    let ref2 = kv_doc_ref(&branch_id, "doc2");

    index.index_document(&ref1, "hello world", None); // 2 tokens
    index.index_document(&ref2, "this is test", None); // 3 tokens

    let avg = index.avg_doc_len();
    assert!(avg > 0.0);
}

/// Index computes IDF correctly
#[test]
fn test_tier7_index_idf() {
    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();

    // Add documents where "hello" appears in 2 and "rare" in 1
    for i in 0..10 {
        let doc_ref = kv_doc_ref(&branch_id, format!("doc{}", i));
        if i < 2 {
            index.index_document(&doc_ref, "hello world", None);
        } else if i == 5 {
            index.index_document(&doc_ref, "rare word", None);
        } else {
            index.index_document(&doc_ref, "other content", None);
        }
    }

    let idf_hello = index.compute_idf("hello");
    let idf_rare = index.compute_idf("rare");

    // Rare term should have higher IDF
    assert!(idf_rare > idf_hello, "Rare terms should have higher IDF");
}

/// Index tracks document frequency
#[test]
fn test_tier7_index_doc_freq() {
    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();

    for i in 0..5 {
        let doc_ref = kv_doc_ref(&branch_id, format!("doc{}", i));
        index.index_document(&doc_ref, "hello world", None);
    }

    assert_eq!(index.doc_freq("hello"), 5);
    assert_eq!(index.doc_freq("world"), 5);
    assert_eq!(index.doc_freq("nonexistent"), 0);
}

// ============================================================================
// Index Version Tests
// ============================================================================

/// Index version increments on changes
#[test]
fn test_tier7_index_version_increments() {
    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();
    let doc_ref = kv_doc_ref(&branch_id, "doc");

    let v1 = index.version();
    index.index_document(&doc_ref, "hello", None);
    let v2 = index.version();

    assert!(v2 > v1, "Version should increment after add");
}

/// Can wait for specific version
#[test]
fn test_tier7_index_wait_for_version() {
    use std::time::Duration;

    let index = InvertedIndex::new();
    index.enable();

    let branch_id = BranchId::new();
    let doc_ref = kv_doc_ref(&branch_id, "doc");

    index.index_document(&doc_ref, "hello", None);
    let current = index.version();

    // Waiting for current version should succeed immediately
    let result = index.wait_for_version(current, Duration::from_millis(100));
    assert!(result, "Should succeed when version already reached");
}

/// Wait for version times out correctly
#[test]
fn test_tier7_index_wait_for_version_timeout() {
    use std::time::Duration;

    let index = InvertedIndex::new();
    index.enable();

    let current = index.version();

    // Waiting for future version should timeout
    let result = index.wait_for_version(current + 100, Duration::from_millis(10));
    assert!(!result, "Should timeout when version not reached");
}
