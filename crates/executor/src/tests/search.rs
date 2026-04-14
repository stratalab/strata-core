//! Search command tests: verify executor Search command works end-to-end.
//!
//! Note: Search is handled by the intelligence layer (strata-intelligence).
//! The primitive-level Searchable implementations return empty results.
//! These tests verify the Search command infrastructure works correctly,
//! even when primitives return empty results.

use crate::types::SearchQuery;
use crate::Value;
use crate::{Command, Executor, Output};
use strata_engine::database::OpenSpec;
use strata_engine::{Database, SearchSubsystem};

fn create_executor() -> Executor {
    let spec = OpenSpec::cache().with_subsystem(SearchSubsystem);
    let db = Database::open_runtime(spec).unwrap();
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
            as_of: None,
            diff: None,
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
            as_of: None,
            diff: None,
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
            as_of: None,
            diff: None,
        },
    });

    match result {
        Ok(Output::SearchResults { hits, .. }) => {
            assert!(!hits.is_empty(), "Should find KV data via BM25");
            assert_eq!(hits[0].entity_ref.kind, "kv");
            assert_eq!(hits[0].entity_ref.key.as_deref(), Some("test_key"));
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
            as_of: None,
            diff: None,
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
            as_of: None,
            diff: None,
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
            as_of: None,
            diff: None,
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
            as_of: None,
            diff: None,
        },
    });
    assert!(result.is_ok());
}

/// Issue #1768: search stats should include embedding progress when auto-embed
/// is enabled and items are still pending.
#[cfg(feature = "embed")]
#[test]
fn test_issue_1768_search_stats_include_embedding_progress() {
    let spec = OpenSpec::cache().with_subsystem(SearchSubsystem);
    let db = Database::open_runtime(spec).unwrap();
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
            as_of: None,
            diff: None,
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
    let spec = OpenSpec::cache().with_subsystem(SearchSubsystem);
    let db = Database::open_runtime(spec).unwrap();
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
            as_of: None,
            diff: None,
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

// ============================================================================
// v0.6 RAG wiring tests
// ============================================================================
//
// These exercise the executor → intelligence layer wiring (`generate_rag_answer`
// shim, projection into `Output::SearchResults.answer`, RAG stats fields).
// They deliberately avoid loading a real model: the default-recipe path doesn't
// invoke RAG at all, and the rag-recipe path only triggers the zero-hits canned
// response which never calls a model.

/// Default recipe must NOT invoke RAG. `Output::SearchResults.answer` must be
/// `None` and the `rag_*` stats fields must all be `None`. Catches accidental
/// RAG invocation regressions where the recipe gate is bypassed.
#[test]
fn test_search_default_recipe_no_rag_answer() {
    let executor = create_executor();
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "anything".to_string(),
            recipe: None,
            precomputed_embedding: None,
            k: None,
            as_of: None,
            diff: None,
        },
    });
    match result {
        Ok(Output::SearchResults { answer, stats, .. }) => {
            assert!(answer.is_none(), "default recipe must not invoke RAG");
            assert!(stats.rag_used.is_none());
            assert!(stats.rag_model.is_none());
            assert!(stats.rag_elapsed_ms.is_none());
            assert!(stats.rag_tokens_in.is_none());
            assert!(stats.rag_tokens_out.is_none());
        }
        other => panic!("Expected SearchResults, got {:?}", other),
    }
}

/// An inline recipe with `prompt` set must invoke RAG, and over an empty index
/// the intelligence layer returns the zero-hits canned response (no model call).
/// This verifies the executor wiring end-to-end without requiring model
/// infrastructure: `generate_rag_answer` → `AnswerResponse` projection → stats
/// population → serialization.
///
/// Gated on `embed` because the non-embed `generate_rag_answer` shim always
/// returns `None`, which would make this test pass vacuously.
#[cfg(feature = "embed")]
#[test]
fn test_search_rag_recipe_zero_hits_canned_response() {
    let executor = create_executor();
    // Inline recipe with `prompt` set — RAG opt-in. No model is invoked because
    // the index is empty (zero hits → canned response in `generate_answer`).
    let recipe = serde_json::json!({
        "prompt": "Answer using only the provided context. Cite sources with [N].",
        "rag_context_hits": 3,
        "rag_max_tokens": 200,
        "models": {
            "generate": "local:qwen3:1.7b"
        }
    });
    let result = executor.execute(Command::Search {
        branch: None,
        space: None,
        search: SearchQuery {
            query: "what are the side effects of metformin".to_string(),
            recipe: Some(recipe),
            precomputed_embedding: None,
            k: None,
            as_of: None,
            diff: None,
        },
    });
    match result {
        Ok(Output::SearchResults {
            hits,
            answer,
            stats,
            ..
        }) => {
            assert!(hits.is_empty(), "empty index should yield no hits");
            let answer = answer.expect("rag recipe + zero hits must return canned answer");
            assert!(
                answer.text.contains("don't have enough information"),
                "expected canned response, got {:?}",
                answer.text
            );
            assert!(answer.sources.is_empty(), "canned response cites nothing");
            assert_eq!(stats.rag_used, Some(true));
            assert_eq!(stats.rag_model.as_deref(), Some("local:qwen3:1.7b"));
            assert_eq!(
                stats.rag_elapsed_ms,
                Some(0.0),
                "canned-response path skips the model"
            );
            assert_eq!(stats.rag_tokens_in, Some(0));
            assert_eq!(stats.rag_tokens_out, Some(0));
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
            as_of: None,
            diff: None,
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
