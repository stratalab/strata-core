//! R5: Facade Tie-Break Tests
//!
//! Invariant R5: Facade sorts by (score desc, key asc).

use crate::common::*;
use strata_vector::DistanceMetric;

/// Test that facade breaks ties by key (ascending)
#[test]
fn test_r5_facade_tiebreak_by_key() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::Cosine))
        .unwrap();

    // All identical vectors = all identical scores
    let identical = vec![1.0, 0.0, 0.0];

    // Insert with keys that will be sorted alphabetically
    vector
        .insert(test_db.branch_id, "embeddings", "charlie", &identical, None)
        .unwrap();
    vector
        .insert(test_db.branch_id, "embeddings", "alice", &identical, None)
        .unwrap();
    vector
        .insert(test_db.branch_id, "embeddings", "bob", &identical, None)
        .unwrap();

    let query = vec![1.0, 0.0, 0.0];
    let results = vector
        .search(test_db.branch_id, "embeddings", &query, 3, None)
        .unwrap();

    // All scores tied, facade sorts by key asc
    assert_eq!(results[0].key, "alice", "R5 VIOLATED: First should be alice");
    assert_eq!(results[1].key, "bob", "R5 VIOLATED: Second should be bob");
    assert_eq!(results[2].key, "charlie", "R5 VIOLATED: Third should be charlie");
}

/// Test tie-break with numeric keys
#[test]
fn test_r5_facade_tiebreak_numeric_keys() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::Cosine))
        .unwrap();

    let identical = vec![1.0, 0.0, 0.0];

    // Insert with numeric keys (note: string sorting, not numeric)
    for key in ["10", "2", "1", "20", "3"] {
        vector
            .insert(test_db.branch_id, "embeddings", key, &identical, None)
            .unwrap();
    }

    let query = vec![1.0, 0.0, 0.0];
    let results = vector
        .search(test_db.branch_id, "embeddings", &query, 5, None)
        .unwrap();

    // String sort: "1", "10", "2", "20", "3"
    let keys: Vec<&str> = results.iter().map(|r| r.key.as_str()).collect();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();

    assert_eq!(keys, sorted_keys, "R5 VIOLATED: Keys not in lexicographic order");
}

/// Test tie-break with mixed case keys
#[test]
fn test_r5_facade_tiebreak_mixed_case() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::Cosine))
        .unwrap();

    let identical = vec![1.0, 0.0, 0.0];

    for key in ["Zebra", "apple", "Apple", "zebra"] {
        vector
            .insert(test_db.branch_id, "embeddings", key, &identical, None)
            .unwrap();
    }

    let query = vec![1.0, 0.0, 0.0];
    let results = vector
        .search(test_db.branch_id, "embeddings", &query, 4, None)
        .unwrap();

    // ASCII sort order: uppercase before lowercase
    let keys: Vec<&str> = results.iter().map(|r| r.key.as_str()).collect();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();

    assert_eq!(keys, sorted_keys, "R5 VIOLATED: Keys not in ASCII sort order");
}

/// Test that score still takes precedence over key tie-break
#[test]
fn test_r5_score_precedence_over_key() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::Cosine))
        .unwrap();

    let query = vec![1.0, 0.0, 0.0];
    let perfect = vec![1.0, 0.0, 0.0];
    let good = vec![0.9, 0.1, 0.0];

    // Insert with key that would come first alphabetically but has worse score
    vector
        .insert(test_db.branch_id, "embeddings", "aaa_worse", &good, None)
        .unwrap();
    vector
        .insert(test_db.branch_id, "embeddings", "zzz_better", &perfect, None)
        .unwrap();

    let results = vector
        .search(test_db.branch_id, "embeddings", &query, 2, None)
        .unwrap();

    // Better score should win despite alphabetically later key
    assert_eq!(
        results[0].key, "zzz_better",
        "R5 VIOLATED: Score should take precedence over key tie-break"
    );
}

/// Test tie-break consistency
#[test]
fn test_r5_tiebreak_consistency() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_custom(3, DistanceMetric::Cosine))
        .unwrap();

    let identical = vec![1.0, 0.0, 0.0];

    // Insert 20 keys
    let keys: Vec<String> = (0..20).map(|i| format!("key_{:02}", i)).collect();
    for key in &keys {
        vector
            .insert(test_db.branch_id, "embeddings", key, &identical, None)
            .unwrap();
    }

    let query = vec![1.0, 0.0, 0.0];

    // Run multiple searches
    let first_results = vector
        .search(test_db.branch_id, "embeddings", &query, 20, None)
        .unwrap();

    for _ in 0..20 {
        let results = vector
            .search(test_db.branch_id, "embeddings", &query, 20, None)
            .unwrap();
        let result_keys: Vec<&str> = results.iter().map(|r| r.key.as_str()).collect();
        let first_keys: Vec<&str> = first_results.iter().map(|r| r.key.as_str()).collect();
        assert_eq!(
            first_keys, result_keys,
            "R5 VIOLATED: Tie-break ordering not consistent"
        );
    }
}
