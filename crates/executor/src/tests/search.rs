//! Search command tests: verify executor Search command works end-to-end.
//!
//! Note: Search is handled by the intelligence layer (strata-intelligence).
//! The primitive-level Searchable implementations return empty results.
//! These tests verify the Search command infrastructure works correctly,
//! even when primitives return empty results.

use crate::types::SearchQuery;
use crate::Value;
use crate::{Command, Executor, Output};
use strata_engine::Database;

fn create_executor() -> Executor {
    let db = Database::cache().unwrap();
    Executor::new(db)
}

#[test]
fn test_search_empty_database() {
    let executor = create_executor();

    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "nonexistent".to_string(),
            k: None,
            primitives: None,
            time_range: None,
            mode: None,
            expand: None,
            rerank: None,
            precomputed_embedding: None,
        },
    });

    match result {
        Ok(Output::SearchResults(hits)) => {
            assert!(hits.is_empty(), "Empty database should return no results");
        }
        other => panic!("Expected SearchResults, got {:?}", other),
    }
}

#[test]
fn test_search_returns_empty_for_kv_primitive() {
    let executor = create_executor();

    // Insert some data
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "greeting".to_string(),
            value: Value::String("hello world".into()),
        })
        .unwrap();

    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "farewell".to_string(),
            value: Value::String("goodbye world".into()),
        })
        .unwrap();

    // Search for "hello" — BM25 index is enabled, KV search returns results
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "hello".to_string(),
            k: Some(10),
            primitives: Some(vec!["kv".to_string()]),
            time_range: None,
            mode: None,
            expand: None,
            rerank: None,
            precomputed_embedding: None,
        },
    });

    match result {
        Ok(Output::SearchResults(hits)) => {
            assert_eq!(hits.len(), 1, "Should find the doc containing 'hello'");
            assert!(hits[0].score > 0.0);
        }
        other => panic!("Expected SearchResults, got {:?}", other),
    }
}

#[test]
fn test_search_with_primitive_filter() {
    let executor = create_executor();

    // Insert KV data
    executor
        .execute(Command::KvPut {
            branch: None,
            space: None,
            key: "test_key".to_string(),
            value: Value::String("searchable data".into()),
        })
        .unwrap();

    // Search only in event primitive
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "searchable".to_string(),
            k: Some(10),
            primitives: Some(vec!["event".to_string()]),
            time_range: None,
            mode: None,
            expand: None,
            rerank: None,
            precomputed_embedding: None,
        },
    });

    match result {
        Ok(Output::SearchResults(hits)) => {
            // Should not find any data from event primitive
            assert!(hits.is_empty(), "Should not find data in event primitive");
        }
        other => panic!("Expected SearchResults, got {:?}", other),
    }
}

#[test]
fn test_search_command_infrastructure_works() {
    let executor = create_executor();

    // Test that the Search command executes without error
    // even when no results are found
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "test query".to_string(),
            k: Some(5),
            primitives: None,
            time_range: None,
            mode: None,
            expand: None,
            rerank: None,
            precomputed_embedding: None,
        },
    });

    // Verify the command infrastructure works
    match result {
        Ok(Output::SearchResults(_)) => {
            // Command executed successfully
        }
        other => panic!("Expected SearchResults output type, got {:?}", other),
    }
}

#[test]
fn test_search_with_mode_override() {
    let executor = create_executor();

    // Keyword mode
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "test".to_string(),
            k: None,
            primitives: None,
            time_range: None,
            mode: Some("keyword".to_string()),
            expand: None,
            rerank: None,
            precomputed_embedding: None,
        },
    });
    assert!(result.is_ok());

    // Hybrid mode
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "test".to_string(),
            k: None,
            primitives: None,
            time_range: None,
            mode: Some("hybrid".to_string()),
            expand: None,
            rerank: None,
            precomputed_embedding: None,
        },
    });
    assert!(result.is_ok());
}

#[test]
fn test_search_with_expand_rerank_disabled() {
    let executor = create_executor();

    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "test".to_string(),
            k: None,
            primitives: None,
            time_range: None,
            mode: None,
            expand: Some(false),
            rerank: Some(false),
            precomputed_embedding: None,
        },
    });
    assert!(result.is_ok());
}
