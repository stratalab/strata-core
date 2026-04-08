//! Integration tests for the extract → embed pipeline.
//!
//! Tests cross-module behaviors through the public API:
//! - extract_text produces strings the embedding engine can consume
//! - EmbedModelState lifecycle integrates with extract + embed
//! - Edge cases at module boundaries (empty text, large text, nested values)

#![cfg(feature = "embed")]

use strata_core::Value;
use strata_intelligence::embed::extract::extract_text;
use strata_intelligence::embed::EmbedModelState;

use std::collections::HashMap;
use std::sync::Arc;

// =========================================================================
// extract_text integration (cross-module, not already in extract.rs unit tests)
// =========================================================================

#[test]
fn test_extract_returns_none_for_non_embeddable() {
    assert!(extract_text(&Value::Null).is_none());
    assert!(extract_text(&Value::Bytes(vec![1, 2, 3])).is_none());
    assert!(extract_text(&Value::String("".into())).is_none());
    assert!(extract_text(&Value::array(vec![Value::Null, Value::Null])).is_none());
}

#[test]
fn test_extract_positive_cases_produce_nonempty_text() {
    // Every embeddable type should produce a non-empty string.
    // Use 2.5 instead of 3.14 to avoid clippy::approx_constant (PI).
    // The point of the test is round-trip, not the specific value.
    let cases: Vec<Value> = vec![
        Value::String("hello world".into()),
        Value::Int(42),
        Value::Float(2.5),
        Value::Bool(true),
    ];
    for value in &cases {
        let text = extract_text(value);
        assert!(text.is_some(), "expected Some for {:?}, got None", value);
        assert!(
            !text.as_ref().unwrap().is_empty(),
            "expected non-empty text for {:?}",
            value
        );
    }
}

#[test]
fn test_extract_complex_value() {
    let mut map = HashMap::new();
    map.insert("name".to_string(), Value::String("Alice".into()));
    map.insert(
        "scores".to_string(),
        Value::array(vec![Value::Int(10), Value::Int(20)]),
    );
    let nested = Value::object(map);

    let text = extract_text(&nested).unwrap();
    assert!(text.contains("name: Alice"));
    assert!(text.contains("scores:"));
    assert!(text.contains("10"));
    assert!(text.contains("20"));
}

#[test]
fn test_extract_mixed_array_filters_nulls() {
    let arr = Value::array(vec![
        Value::String("keep".into()),
        Value::Null,
        Value::Int(7),
        Value::Bytes(vec![0xFF]),
        Value::String("also keep".into()),
    ]);
    let text = extract_text(&arr).unwrap();
    assert!(text.contains("keep"));
    assert!(text.contains("7"));
    assert!(text.contains("also keep"));
    // Null and Bytes should be filtered out — no stray tokens.
    assert!(!text.contains("null"), "null should be filtered: {}", text);
}

#[test]
fn test_extract_preserves_key_order_in_nested_objects() {
    let mut inner = HashMap::new();
    inner.insert("z_field".to_string(), Value::String("last".into()));
    inner.insert("a_field".to_string(), Value::String("first".into()));

    let mut outer = HashMap::new();
    outer.insert("data".to_string(), Value::object(inner));

    let text = extract_text(&Value::object(outer)).unwrap();
    let a_pos = text.find("a_field").expect("a_field missing");
    let z_pos = text.find("z_field").expect("z_field missing");
    assert!(
        a_pos < z_pos,
        "keys should be sorted alphabetically: {}",
        text
    );
}

// =========================================================================
// EmbedModelState integration (tests that don't require real model files)
// =========================================================================

#[test]
fn test_embed_model_state_default_then_load() {
    let state = EmbedModelState::default();
    assert!(
        state.embedding_dim().is_none(),
        "dim should be None before load"
    );

    // Trigger a load. On CI without model files this returns Err, which is fine.
    let result = state.get_or_load(std::path::Path::new("/unused"), "miniLM", None);

    // After load attempt, embedding_dim should be consistent with the result.
    match &result {
        Ok(_) => {
            let dim = state
                .embedding_dim()
                .expect("dim should be Some after successful load");
            assert!(dim > 0, "dimension should be positive, got {}", dim);
        }
        Err(_) => {
            assert!(
                state.embedding_dim().is_none(),
                "dim should remain None after failed load"
            );
        }
    }
}

#[test]
fn test_embed_model_state_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<EmbedModelState>();
}

#[test]
fn test_embed_model_state_concurrent_get_or_load() {
    // Multiple threads calling get_or_load concurrently should all get the
    // same result without panicking.
    let state = Arc::new(EmbedModelState::default());
    let mut handles = Vec::new();

    for _ in 0..4 {
        let s = Arc::clone(&state);
        handles.push(std::thread::spawn(move || {
            s.get_or_load(std::path::Path::new("/unused"), "miniLM", None)
        }));
    }

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All results must have the same Ok/Err variant: either every thread sees
    // the model load succeed (real model files present), or every thread sees
    // it fail. A mix would mean the lifecycle is corrupting itself under
    // contention.
    let first_is_ok = results[0].is_ok();
    for (i, r) in results.iter().enumerate() {
        assert_eq!(
            r.is_ok(),
            first_is_ok,
            "thread {} got different Ok/Err than thread 0",
            i
        );
    }

    // Note: the current `EmbedModelState::get_or_load` does NOT single-flight
    // the underlying load — multiple concurrent callers can each successfully
    // load the model and only one wins the race to store the Arc on the
    // state. The contract is "every caller gets a working engine", not "every
    // caller gets the *same* engine instance". So we don't assert Arc::ptr_eq
    // across the concurrent batch. Subsequent serial calls (covered by the
    // lifecycle test's `test_embed_model_state_caches_across_calls`) DO get
    // the cached Arc via the fast path.
    if first_is_ok {
        // Sanity-check: every Ok result is a valid Arc with a usable engine.
        for (i, r) in results.iter().enumerate() {
            let arc = r.as_ref().unwrap();
            let healthy = arc.lock().unwrap_or_else(|e| e.into_inner()).is_healthy();
            assert!(healthy, "thread {} returned an unhealthy engine", i);
        }
    }
}

// =========================================================================
// Extract → embed roundtrip (requires real model, #[ignore])
// =========================================================================

#[test]
#[ignore]
fn test_extract_then_embed_roundtrip() {
    let text = extract_text(&Value::String("hello world".into())).unwrap();
    let engine = strata_intelligence::EmbeddingEngine::from_registry("miniLM")
        .expect("failed to load miniLM");
    let embedding = engine.embed(&text).expect("embed failed");
    assert_eq!(embedding.len(), engine.embedding_dim());
    // Should be L2-normalized.
    let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!(
        (norm - 1.0).abs() < 1e-4,
        "L2 norm = {}, expected 1.0",
        norm
    );
}

#[test]
#[ignore]
fn test_extract_object_then_embed_produces_valid_vector() {
    let mut map = HashMap::new();
    map.insert(
        "title".to_string(),
        Value::String("Rust programming".into()),
    );
    map.insert("year".to_string(), Value::Int(2024));
    let obj = Value::object(map);

    let text = extract_text(&obj).expect("extraction should succeed");
    let engine = strata_intelligence::EmbeddingEngine::from_registry("miniLM")
        .expect("failed to load miniLM");
    let embedding = engine.embed(&text).expect("embed failed");
    assert_eq!(embedding.len(), engine.embedding_dim());

    // Sanity: vector should not be all zeros.
    let max_abs = embedding.iter().map(|x| x.abs()).fold(0.0f32, f32::max);
    assert!(
        max_abs > 1e-6,
        "embedding should not be a zero vector, max abs = {}",
        max_abs
    );
}

#[test]
#[ignore]
fn test_embed_model_state_produces_same_result_as_direct_engine() {
    let state = EmbedModelState::default();
    let engine_via_state = state
        .get_or_load(std::path::Path::new("/unused"), "miniLM", None)
        .expect("load via state failed");

    let engine_direct =
        strata_intelligence::EmbeddingEngine::from_registry("miniLM").expect("direct load failed");

    let text = "consistency check";
    // engine_via_state is Arc<Mutex<Box<dyn InferenceEngine>>>; lock to call
    // the trait method. engine_direct is a concrete EmbeddingEngine and
    // exposes embed() directly.
    let v1 = engine_via_state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .embed(text)
        .expect("embed via state failed");
    let v2 = engine_direct.embed(text).expect("embed direct failed");

    assert_eq!(v1.len(), v2.len());
    // Both engines use the same model, so results should be identical.
    for (i, (a, b)) in v1.iter().zip(v2.iter()).enumerate() {
        assert!(
            (a - b).abs() < 1e-6,
            "dimension {} differs: state={}, direct={}",
            i,
            a,
            b
        );
    }
}
