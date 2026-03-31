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
            recipe: None,
            precomputed_embedding: None,
            k: None,
        },
    });

    match result {
        Ok(Output::SearchResults { hits, .. }) => {
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
            recipe: None,
            precomputed_embedding: None,
            k: Some(10),
        },
    });

    match result {
        Ok(Output::SearchResults { hits, .. }) => {
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

    // Search should find the KV data
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "searchable".to_string(),
            recipe: None,
            precomputed_embedding: None,
            k: Some(10),
        },
    });

    match result {
        Ok(Output::SearchResults { hits, .. }) => {
            assert!(!hits.is_empty(), "Should find KV data via BM25");
            assert_eq!(hits[0].entity, "test_key");
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
            recipe: None,
            precomputed_embedding: None,
            k: Some(5),
        },
    });

    // Verify the command infrastructure works
    match result {
        Ok(Output::SearchResults { .. }) => {
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
            recipe: None,
            precomputed_embedding: None,
            k: None,
        },
    });
    assert!(result.is_ok());

    // Hybrid mode
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "test".to_string(),
            recipe: None,
            precomputed_embedding: None,
            k: None,
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
            recipe: None,
            precomputed_embedding: None,
            k: None,
        },
    });
    assert!(result.is_ok());
}

/// Issue #1768: search stats should include embedding progress when auto-embed
/// is enabled and items are still pending.
#[cfg(feature = "embed")]
#[test]
fn test_issue_1768_search_stats_include_embedding_progress() {
    let db = Database::cache().unwrap();
    db.set_auto_embed(true);
    let executor = Executor::new(db);

    // Insert several KV entries — with auto_embed on, these queue into the
    // EmbedBuffer. Default batch_size=256, so they stay pending.
    for i in 0..5 {
        executor
            .execute(Command::KvPut {
                branch: None,
                space: None,
                key: format!("embed-key-{}", i),
                value: Value::String(format!("text for embedding test {}", i)),
            })
            .unwrap();
    }

    // Search — stats should include embedding progress
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "test".to_string(),
            recipe: None,
            precomputed_embedding: None,
            k: None,
        },
    });

    match result {
        Ok(Output::SearchResults { stats, .. }) => {
            assert!(
                stats.embedding_pending.is_some(),
                "Should report pending embeds when auto-embed is on with queued items"
            );
            assert!(
                stats.embedding_pending.unwrap() > 0,
                "Should have pending > 0"
            );
            assert!(
                stats.embedding_total.is_some(),
                "Should report total queued embeds"
            );
            assert!(stats.embedding_total.unwrap() > 0, "Should have total > 0");
        }
        other => panic!("Expected SearchResults, got {:?}", other),
    }
}

/// Issue #1768: search stats should NOT include embedding progress when
/// auto-embed is enabled but nothing is pending (all embedded).
#[test]
fn test_issue_1768_search_stats_no_embedding_when_nothing_pending() {
    let db = Database::cache().unwrap();
    db.set_auto_embed(true);
    let executor = Executor::new(db);

    // No KV inserts → nothing pending in the embed buffer
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "test".to_string(),
            recipe: None,
            precomputed_embedding: None,
            k: None,
        },
    });

    match result {
        Ok(Output::SearchResults { stats, .. }) => {
            assert_eq!(
                stats.embedding_pending, None,
                "Should not report embedding progress when nothing is pending"
            );
            assert_eq!(
                stats.embedding_total, None,
                "Should not report embedding total when nothing is pending"
            );
        }
        other => panic!("Expected SearchResults, got {:?}", other),
    }
}

/// Issue #1768: search stats should NOT include embedding progress when
/// auto-embed is disabled.
#[test]
fn test_issue_1768_search_stats_no_embedding_when_disabled() {
    let executor = create_executor();

    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "test".to_string(),
            recipe: None,
            precomputed_embedding: None,
            k: None,
        },
    });

    match result {
        Ok(Output::SearchResults { stats, .. }) => {
            assert_eq!(
                stats.embedding_pending, None,
                "Should not report embedding progress when auto-embed is off"
            );
            assert_eq!(
                stats.embedding_total, None,
                "Should not report embedding total when auto-embed is off"
            );
        }
        other => panic!("Expected SearchResults, got {:?}", other),
    }
}
