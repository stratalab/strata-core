//! R1: Dimension Match Tests
//!
//! Invariant R1: Query dimension must match collection dimension.

use crate::common::*;
use strata_vector::VectorError;

/// Test query with correct dimension works
#[test]
fn test_r1_query_correct_dimension_works() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_minilm())
        .unwrap();

    vector
        .insert(test_db.branch_id, "embeddings", "key1", &random_vector(384), None)
        .unwrap();

    // Search with correct dimension works
    let result = vector.search(test_db.branch_id, "embeddings", &random_vector(384), 10, None);
    assert!(result.is_ok(), "Search with correct dimension should work");
}

/// Test query with wrong dimension fails
#[test]
fn test_r1_query_wrong_dimension_fails() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_minilm())
        .unwrap();

    vector
        .insert(test_db.branch_id, "embeddings", "key1", &random_vector(384), None)
        .unwrap();

    // Search with wrong dimension fails
    let result = vector.search(test_db.branch_id, "embeddings", &random_vector(768), 10, None);
    assert!(
        matches!(result, Err(VectorError::DimensionMismatch { expected: 384, got: 768 })),
        "R1 VIOLATED: Search with wrong dimension should fail, got {:?}",
        result
    );
}

/// Test dimension check with various collection sizes
#[test]
fn test_r1_dimension_check_various_sizes() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    // Test with small dimension
    vector
        .create_collection(test_db.branch_id, "small", config_small())
        .unwrap();
    vector
        .insert(test_db.branch_id, "small", "key1", &vec![1.0, 2.0, 3.0], None)
        .unwrap();

    // Correct
    assert!(vector.search(test_db.branch_id, "small", &vec![1.0, 2.0, 3.0], 10, None).is_ok());

    // Wrong
    assert!(matches!(
        vector.search(test_db.branch_id, "small", &vec![1.0, 2.0], 10, None),
        Err(VectorError::DimensionMismatch { expected: 3, got: 2 })
    ));

    // Test with large dimension
    vector
        .create_collection(test_db.branch_id, "large", config_openai_ada())
        .unwrap();
    vector
        .insert(test_db.branch_id, "large", "key1", &random_vector(1536), None)
        .unwrap();

    // Correct
    assert!(vector.search(test_db.branch_id, "large", &random_vector(1536), 10, None).is_ok());

    // Wrong
    assert!(matches!(
        vector.search(test_db.branch_id, "large", &random_vector(384), 10, None),
        Err(VectorError::DimensionMismatch { expected: 1536, got: 384 })
    ));
}

/// Test dimension check on empty collection
#[test]
fn test_r1_dimension_check_empty_collection() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_minilm())
        .unwrap();

    // Even with no vectors, dimension should be checked
    let result = vector.search(test_db.branch_id, "embeddings", &random_vector(768), 10, None);
    assert!(
        matches!(result, Err(VectorError::DimensionMismatch { .. })),
        "R1 VIOLATED: Dimension check should apply to empty collection"
    );

    // Correct dimension on empty collection returns empty results
    let result = vector.search(test_db.branch_id, "embeddings", &random_vector(384), 10, None);
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

/// Test dimension check consistency across restarts
#[test]
fn test_r1_dimension_check_survives_restart() {
    let mut test_db = TestDb::new();
    let branch_id = test_db.branch_id;

    {
        let vector = test_db.vector();
        vector
            .create_collection(branch_id, "embeddings", config_minilm())
            .unwrap();
        vector
            .insert(branch_id, "embeddings", "key1", &random_vector(384), None)
            .unwrap();
    }

    test_db.reopen();

    let vector = test_db.vector();

    // Dimension check should still work after restart
    let result = vector.search(branch_id, "embeddings", &random_vector(768), 10, None);
    assert!(
        matches!(result, Err(VectorError::DimensionMismatch { .. })),
        "R1 VIOLATED: Dimension check should persist across restart"
    );
}
