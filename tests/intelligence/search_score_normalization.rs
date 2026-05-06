//! R2: Score Normalization Tests
//!
//! Invariant R2: All metrics return "higher is better" scores.

use crate::common::*;
use strata_engine::DistanceMetric;

/// Test cosine similarity: higher score = more similar
#[test]
fn test_r2_cosine_higher_is_more_similar() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::Cosine))
        .unwrap();

    let query = vec![1.0, 0.0, 0.0];
    let identical = vec![1.0, 0.0, 0.0]; // Same direction
    let orthogonal = vec![0.0, 1.0, 0.0]; // Perpendicular
    let opposite = vec![-1.0, 0.0, 0.0]; // Opposite direction

    vector
        .insert(test_db.branch_id, "embeddings", "identical", &identical, None)
        .unwrap();
    vector
        .insert(test_db.branch_id, "embeddings", "orthogonal", &orthogonal, None)
        .unwrap();
    vector
        .insert(test_db.branch_id, "embeddings", "opposite", &opposite, None)
        .unwrap();

    let results = vector
        .search(test_db.branch_id, "embeddings", &query, 3, None)
        .unwrap();

    // Identical should have highest score
    assert_eq!(results[0].key, "identical", "R2 VIOLATED: Identical vector not first");
    assert!(
        results[0].score > results[1].score,
        "R2 VIOLATED: Higher similarity should have higher score"
    );

    // Opposite should have lowest score
    assert_eq!(results[2].key, "opposite", "R2 VIOLATED: Opposite vector not last");
}

/// Test Euclidean: higher score = closer distance
#[test]
fn test_r2_euclidean_higher_is_closer() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::Euclidean))
        .unwrap();

    let query = vec![0.0, 0.0, 0.0];
    let close = vec![0.1, 0.0, 0.0]; // Distance ~0.1
    let medium = vec![1.0, 0.0, 0.0]; // Distance 1.0
    let far = vec![10.0, 0.0, 0.0]; // Distance 10.0

    vector
        .insert(test_db.branch_id, "embeddings", "close", &close, None)
        .unwrap();
    vector
        .insert(test_db.branch_id, "embeddings", "medium", &medium, None)
        .unwrap();
    vector
        .insert(test_db.branch_id, "embeddings", "far", &far, None)
        .unwrap();

    let results = vector
        .search(test_db.branch_id, "embeddings", &query, 3, None)
        .unwrap();

    // Close should have highest score (1 / (1 + 0.1) ≈ 0.91)
    assert_eq!(results[0].key, "close", "R2 VIOLATED: Closest vector not first");
    assert!(
        results[0].score > results[1].score,
        "R2 VIOLATED: Closer vector should have higher score"
    );

    // Far should have lowest score (1 / (1 + 10) ≈ 0.09)
    assert_eq!(results[2].key, "far", "R2 VIOLATED: Farthest vector not last");
}

/// Test dot product: higher score = more similar
#[test]
fn test_r2_dotproduct_higher_is_more_similar() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::DotProduct))
        .unwrap();

    let query = vec![1.0, 1.0, 1.0];
    let aligned = vec![1.0, 1.0, 1.0]; // Dot = 3
    let partial = vec![1.0, 0.0, 0.0]; // Dot = 1
    let negative = vec![-1.0, -1.0, -1.0]; // Dot = -3

    vector
        .insert(test_db.branch_id, "embeddings", "aligned", &aligned, None)
        .unwrap();
    vector
        .insert(test_db.branch_id, "embeddings", "partial", &partial, None)
        .unwrap();
    vector
        .insert(test_db.branch_id, "embeddings", "negative", &negative, None)
        .unwrap();

    let results = vector
        .search(test_db.branch_id, "embeddings", &query, 3, None)
        .unwrap();

    assert_eq!(results[0].key, "aligned", "R2 VIOLATED: Aligned vector not first");
    assert_eq!(results[2].key, "negative", "R2 VIOLATED: Negative vector not last");
}

/// Test score normalization consistency
#[test]
fn test_r2_score_normalization_consistency() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::Cosine))
        .unwrap();

    // Insert 10 vectors at various angles
    for i in 0..10 {
        let angle = (i as f32) * 0.1 * std::f32::consts::PI; // 0 to π
        let v = vec![angle.cos(), angle.sin(), 0.0];
        vector
            .insert(test_db.branch_id, "embeddings", &format!("v_{}", i), &v, None)
            .unwrap();
    }

    let query = vec![1.0, 0.0, 0.0];
    let results = vector
        .search(test_db.branch_id, "embeddings", &query, 10, None)
        .unwrap();

    // Scores should be monotonically decreasing (for vectors at increasing angles from query)
    for i in 1..results.len() {
        assert!(
            results[i - 1].score >= results[i].score,
            "R2 VIOLATED: Scores not monotonically decreasing at position {} ({} < {})",
            i,
            results[i - 1].score,
            results[i].score
        );
    }
}

/// Test that all scores are finite
#[test]
fn test_r2_scores_always_finite() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_minilm())
        .unwrap();

    // Insert various vectors
    for i in 0..20 {
        vector
            .insert(
                test_db.branch_id,
                "embeddings",
                &format!("key_{}", i),
                &random_vector(384),
                None,
            )
            .unwrap();
    }

    let results = vector
        .search(test_db.branch_id, "embeddings", &random_vector(384), 20, None)
        .unwrap();

    for result in &results {
        assert!(
            result.score.is_finite(),
            "R2 VIOLATED: Non-finite score {} for key {}",
            result.score,
            result.key
        );
    }
}
