//! Integration tests for the embedding engine lifecycle.
//!
//! All tests require real model files and are `#[ignore]` by default.
//! Run with: cargo test -p strata-intelligence --features embed -- --include-ignored

#![cfg(feature = "embed")]

use strata_intelligence::embed::EmbedModelState;
use strata_intelligence::EmbeddingEngine;

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "vectors must have same dimension");
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a > 0.0 && norm_b > 0.0 {
        dot / (norm_a * norm_b)
    } else {
        0.0
    }
}

#[test]
#[ignore]
fn test_engine_load_and_embed() {
    let engine =
        EmbeddingEngine::from_registry("miniLM").expect("failed to load miniLM from registry");
    let embedding = engine.embed("hello world").expect("embed failed");

    // Dimension matches what the engine reports.
    assert_eq!(
        embedding.len(),
        engine.embedding_dim(),
        "vector length should match engine.embedding_dim()"
    );

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
fn test_embed_same_text_is_deterministic() {
    let engine =
        EmbeddingEngine::from_registry("miniLM").expect("failed to load miniLM from registry");
    let v1 = engine.embed("deterministic test").expect("embed failed");
    let v2 = engine.embed("deterministic test").expect("embed failed");

    assert_eq!(v1.len(), v2.len());
    for (i, (a, b)) in v1.iter().zip(v2.iter()).enumerate() {
        assert!(
            (a - b).abs() < 1e-7,
            "dimension {} differs between identical inputs: {} vs {}",
            i,
            a,
            b
        );
    }
}

#[test]
#[ignore]
fn test_embed_batch_matches_individual() {
    let engine =
        EmbeddingEngine::from_registry("miniLM").expect("failed to load miniLM from registry");
    let texts = ["the cat sat on the mat", "quantum physics is fascinating"];

    let batch = engine.embed_batch(&texts).expect("batch embed failed");
    assert_eq!(batch.len(), texts.len());

    for (i, text) in texts.iter().enumerate() {
        let individual = engine.embed(text).expect("individual embed failed");
        assert_eq!(batch[i].len(), individual.len());
        for (j, (a, b)) in batch[i].iter().zip(individual.iter()).enumerate() {
            assert!(
                (a - b).abs() < 1e-6,
                "text[{}] dim[{}] differs: batch={}, individual={}",
                i,
                j,
                a,
                b
            );
        }
    }
}

#[test]
#[ignore]
fn test_similar_texts_have_similar_embeddings() {
    let engine =
        EmbeddingEngine::from_registry("miniLM").expect("failed to load miniLM from registry");
    let a = engine
        .embed("the cat sat on the mat")
        .expect("embed failed");
    let b = engine
        .embed("a cat rested on the mat")
        .expect("embed failed");
    let sim = cosine_similarity(&a, &b);
    assert!(
        sim > 0.8,
        "similar texts should have cosine similarity > 0.8, got {}",
        sim
    );
}

#[test]
#[ignore]
fn test_dissimilar_texts_have_low_similarity() {
    let engine =
        EmbeddingEngine::from_registry("miniLM").expect("failed to load miniLM from registry");
    let a = engine.embed("quantum physics").expect("embed failed");
    let b = engine.embed("chocolate cake recipe").expect("embed failed");
    let sim = cosine_similarity(&a, &b);
    assert!(
        sim < 0.5,
        "dissimilar texts should have cosine similarity < 0.5, got {}",
        sim
    );
}

#[test]
#[ignore]
fn test_embed_model_state_caches_across_calls() {
    let state = EmbedModelState::default();
    let dir = std::path::Path::new("/unused");

    let arc1 = state.get_or_load(dir).expect("first load");
    let arc2 = state.get_or_load(dir).expect("second load");

    // Same Arc (pointer equality) — engine was only loaded once.
    assert!(
        std::sync::Arc::ptr_eq(&arc1, &arc2),
        "get_or_load should return the same Arc on second call"
    );
}

#[test]
#[ignore]
fn test_embed_model_state_dim_after_load() {
    let state = EmbedModelState::default();
    // Before load, dim is None.
    assert!(state.embedding_dim().is_none());

    let engine = state
        .get_or_load(std::path::Path::new("/unused"))
        .expect("load failed");

    // After load, dim should match what the engine reports.
    let dim = state
        .embedding_dim()
        .expect("dim should be Some after load");
    assert_eq!(dim, engine.embedding_dim());
    assert!(dim > 0, "dimension should be positive");
}

#[test]
#[ignore]
fn test_embedding_dim_accessor() {
    let engine =
        EmbeddingEngine::from_registry("miniLM").expect("failed to load miniLM from registry");
    let dim = engine.embedding_dim();
    assert!(dim > 0, "dimension should be positive");

    // Verify it matches actual output.
    let embedding = engine.embed("test").expect("embed failed");
    assert_eq!(
        embedding.len(),
        dim,
        "embedding_dim() should match actual vector length"
    );
}
