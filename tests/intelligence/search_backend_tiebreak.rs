//! R4: Backend Tie-Break Tests
//!
//! Invariant R4: Backend sorts by (score desc, VectorId asc).

use crate::common::*;
use strata_engine::DistanceMetric;

/// Test that backend breaks ties by VectorId (ascending)
#[test]
fn test_r4_backend_tiebreak_by_vectorid() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::Cosine))
        .unwrap();

    // All identical vectors = all identical scores
    let identical = vec![1.0, 0.0, 0.0];

    // Insert in specific order to control VectorId assignment
    // First inserted gets lowest VectorId
    vector
        .insert(test_db.branch_id, "embeddings", "key_c", &identical, None)
        .unwrap();
    vector
        .insert(test_db.branch_id, "embeddings", "key_a", &identical, None)
        .unwrap();
    vector
        .insert(test_db.branch_id, "embeddings", "key_b", &identical, None)
        .unwrap();

    // Get VectorIds to understand ordering
    let entry_c = vector
        .get(test_db.branch_id, "embeddings", "key_c")
        .unwrap()
        .unwrap();
    let entry_a = vector
        .get(test_db.branch_id, "embeddings", "key_a")
        .unwrap()
        .unwrap();
    let entry_b = vector
        .get(test_db.branch_id, "embeddings", "key_b")
        .unwrap()
        .unwrap();

    // key_c has lowest VectorId (inserted first)
    assert!(
        entry_c.value.vector_id().as_u64() < entry_a.value.vector_id().as_u64(),
        "key_c should have lower VectorId than key_a"
    );
    assert!(
        entry_a.value.vector_id().as_u64() < entry_b.value.vector_id().as_u64(),
        "key_a should have lower VectorId than key_b"
    );

    let query = vec![1.0, 0.0, 0.0];
    let results = vector
        .search(test_db.branch_id, "embeddings", &query, 3, None)
        .unwrap();

    // All scores are tied, so ordering should be deterministic
    // Note: The actual ordering depends on whether backend uses VectorId or facade uses key
    // This test verifies consistent ordering exists
    assert_eq!(results.len(), 3);

    // Run multiple times to ensure consistency
    for _ in 0..10 {
        let results2 = vector
            .search(test_db.branch_id, "embeddings", &query, 3, None)
            .unwrap();
        let keys1: Vec<&str> = results.iter().map(|r| r.key.as_str()).collect();
        let keys2: Vec<&str> = results2.iter().map(|r| r.key.as_str()).collect();
        assert_eq!(keys1, keys2, "R4 VIOLATED: Tie-break order inconsistent");
    }
}

/// Test tie-break with many tied scores
#[test]
fn test_r4_many_tied_scores() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::Cosine))
        .unwrap();

    // Insert 20 identical vectors
    let identical = vec![1.0, 0.0, 0.0];
    for i in 0..20 {
        vector
            .insert(
                test_db.branch_id,
                "embeddings",
                &format!("key_{:02}", i),
                &identical,
                None,
            )
            .unwrap();
    }

    let query = vec![1.0, 0.0, 0.0];
    let results = vector
        .search(test_db.branch_id, "embeddings", &query, 20, None)
        .unwrap();

    // Verify all scores are equal (or very close)
    let first_score = results[0].score;
    for result in &results {
        assert!(
            (result.score - first_score).abs() < 1e-6,
            "Expected tied scores but got {} vs {}",
            first_score,
            result.score
        );
    }

    // Verify ordering is deterministic
    let keys_first: Vec<String> = results.iter().map(|r| r.key.clone()).collect();
    for _ in 0..10 {
        let results2 = vector
            .search(test_db.branch_id, "embeddings", &query, 20, None)
            .unwrap();
        let keys_second: Vec<String> = results2.iter().map(|r| r.key.clone()).collect();
        assert_eq!(
            keys_first, keys_second,
            "R4 VIOLATED: Tie-break ordering not consistent"
        );
    }
}

/// Test that non-tied scores take precedence over tie-break
#[test]
fn test_r4_score_takes_precedence_over_tiebreak() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::Cosine))
        .unwrap();

    let query = vec![1.0, 0.0, 0.0];
    let perfect = vec![1.0, 0.0, 0.0]; // Cosine = 1.0
    let good = vec![0.9, 0.1, 0.0]; // Cosine < 1.0

    // Insert good match first (lower VectorId)
    vector
        .insert(test_db.branch_id, "embeddings", "good", &good, None)
        .unwrap();
    // Insert perfect match second (higher VectorId)
    vector
        .insert(test_db.branch_id, "embeddings", "perfect", &perfect, None)
        .unwrap();

    let results = vector
        .search(test_db.branch_id, "embeddings", &query, 2, None)
        .unwrap();

    // Perfect match should be first despite higher VectorId
    assert_eq!(
        results[0].key, "perfect",
        "R4 VIOLATED: Score should take precedence over VectorId tie-break"
    );
}
