//! VectorStore Primitive Tests
//!
//! Tests for vector storage with similarity search.

use crate::common::*;

/// Helper to check if a collection exists using list_collections
fn collection_exists(vector: &VectorStore, branch_id: strata_core::BranchId, name: &str) -> bool {
    vector
        .list_collections(branch_id, "default")
        .unwrap()
        .iter()
        .any(|c| c.name == name)
}

// ============================================================================
// Collection Management
// ============================================================================

#[test]
fn create_collection() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "test_collection", config)
        .unwrap();

    assert!(collection_exists(
        &vector,
        test_db.branch_id,
        "test_collection"
    ));
}

#[test]
fn create_collection_duplicate_fails() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(
            test_db.branch_id,
            "default",
            "test_collection",
            config.clone(),
        )
        .unwrap();

    let result = vector.create_collection(test_db.branch_id, "default", "test_collection", config);
    assert!(result.is_err());
}

#[test]
fn list_collections() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll_a", config.clone())
        .unwrap();
    vector
        .create_collection(test_db.branch_id, "default", "coll_b", config.clone())
        .unwrap();

    let collections = vector
        .list_collections(test_db.branch_id, "default")
        .unwrap();
    assert_eq!(collections.len(), 2);

    let names: Vec<_> = collections.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"coll_a"));
    assert!(names.contains(&"coll_b"));
}

#[test]
fn get_collection_info() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_custom(128, DistanceMetric::Euclidean);
    vector
        .create_collection(test_db.branch_id, "default", "test_coll", config)
        .unwrap();

    // Verify via list_collections since get_collection is pub(crate)
    let collections = vector
        .list_collections(test_db.branch_id, "default")
        .unwrap();
    let info = collections.iter().find(|c| c.name == "test_coll").unwrap();
    assert_eq!(info.name, "test_coll");
    assert_eq!(info.config.dimension, 128);
    assert_eq!(info.config.metric, DistanceMetric::Euclidean);
}

#[test]
fn delete_collection() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "to_delete", config)
        .unwrap();
    assert!(collection_exists(&vector, test_db.branch_id, "to_delete"));

    vector
        .delete_collection(test_db.branch_id, "default", "to_delete")
        .unwrap();
    assert!(!collection_exists(&vector, test_db.branch_id, "to_delete"));
}

#[test]
fn delete_collection_removes_vectors() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    // Insert some vectors
    let v1 = [1.0f32, 0.0, 0.0];
    vector
        .insert(test_db.branch_id, "default", "coll", "key1", &v1, None)
        .unwrap();

    // Delete collection
    vector
        .delete_collection(test_db.branch_id, "default", "coll")
        .unwrap();

    // Recreate collection
    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    // Vector should not exist
    let result = vector
        .get(test_db.branch_id, "default", "coll", "key1")
        .unwrap();
    assert!(result.is_none());
}

// ============================================================================
// Vector CRUD
// ============================================================================

#[test]
fn insert_and_get() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    let v = [1.0f32, 2.0, 3.0];
    vector
        .insert(test_db.branch_id, "default", "coll", "vec1", &v, None)
        .unwrap();

    let result = vector
        .get(test_db.branch_id, "default", "coll", "vec1")
        .unwrap();
    assert!(result.is_some());

    let entry = result.unwrap();
    assert_eq!(entry.value.embedding, v.to_vec());
}

#[test]
fn insert_with_metadata() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    let v = [1.0f32, 2.0, 3.0];
    let metadata = serde_json::json!({"category": "test", "score": 42});
    vector
        .insert(
            test_db.branch_id,
            "default",
            "coll",
            "vec1",
            &v,
            Some(metadata.clone()),
        )
        .unwrap();

    let result = vector
        .get(test_db.branch_id, "default", "coll", "vec1")
        .unwrap()
        .unwrap();
    assert_eq!(result.value.metadata, Some(metadata));
}

#[test]
fn insert_dimension_mismatch_fails() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small(); // 3 dimensions
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    let wrong_dim = [1.0f32, 2.0]; // Only 2 dimensions
    let result = vector.insert(
        test_db.branch_id,
        "default",
        "coll",
        "vec1",
        &wrong_dim,
        None,
    );
    assert!(result.is_err());
}

#[test]
fn insert_to_nonexistent_collection_fails() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let v = [1.0f32, 2.0, 3.0];
    let result = vector.insert(
        test_db.branch_id,
        "default",
        "nonexistent",
        "vec1",
        &v,
        None,
    );
    assert!(result.is_err());
}

#[test]
fn delete_vector() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    let v = [1.0f32, 2.0, 3.0];
    vector
        .insert(test_db.branch_id, "default", "coll", "vec1", &v, None)
        .unwrap();

    let deleted = vector
        .delete(test_db.branch_id, "default", "coll", "vec1")
        .unwrap();
    assert!(deleted);

    let result = vector
        .get(test_db.branch_id, "default", "coll", "vec1")
        .unwrap();
    assert!(result.is_none());
}

#[test]
fn delete_nonexistent_returns_false() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    let deleted = vector
        .delete(test_db.branch_id, "default", "coll", "nonexistent")
        .unwrap();
    assert!(!deleted);
}

#[test]
fn count_vectors() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    // count rewritten using list_collections to check collection count field
    let get_count = || -> usize {
        vector
            .list_collections(test_db.branch_id, "default")
            .unwrap()
            .iter()
            .find(|c| c.name == "coll")
            .map(|c| c.count)
            .unwrap_or(0)
    };

    assert_eq!(get_count(), 0);

    vector
        .insert(
            test_db.branch_id,
            "default",
            "coll",
            "v1",
            &[1.0f32, 0.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            test_db.branch_id,
            "default",
            "coll",
            "v2",
            &[0.0f32, 1.0, 0.0],
            None,
        )
        .unwrap();

    assert_eq!(get_count(), 2);
}

// ============================================================================
// Search
// ============================================================================

#[test]
fn search_returns_similar_vectors() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    // Insert vectors
    vector
        .insert(
            test_db.branch_id,
            "default",
            "coll",
            "x_axis",
            &[1.0f32, 0.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            test_db.branch_id,
            "default",
            "coll",
            "y_axis",
            &[0.0f32, 1.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            test_db.branch_id,
            "default",
            "coll",
            "z_axis",
            &[0.0f32, 0.0, 1.0],
            None,
        )
        .unwrap();

    // Search for vector similar to x_axis
    let query = [0.9f32, 0.1, 0.0];
    let results = vector
        .search(test_db.branch_id, "default", "coll", &query, 2, None)
        .unwrap();

    assert_eq!(results.len(), 2);
    // x_axis should be most similar
    assert_eq!(results[0].key, "x_axis");
}

#[test]
fn search_respects_k_limit() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    // Insert 10 vectors
    for i in 0..10 {
        let v = [i as f32, 0.0f32, 0.0];
        vector
            .insert(
                test_db.branch_id,
                "default",
                "coll",
                &format!("v{}", i),
                &v,
                None,
            )
            .unwrap();
    }

    // Search with k=3
    let query = [5.0f32, 0.0, 0.0];
    let results = vector
        .search(test_db.branch_id, "default", "coll", &query, 3, None)
        .unwrap();

    assert_eq!(results.len(), 3);
}

#[test]
fn search_empty_collection() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    let query = [1.0f32, 0.0, 0.0];
    let results = vector
        .search(test_db.branch_id, "default", "coll", &query, 5, None)
        .unwrap();

    assert!(results.is_empty());
}

// ============================================================================
// Distance Metrics
// ============================================================================

#[test]
fn euclidean_distance() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_custom(3, DistanceMetric::Euclidean);
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    vector
        .insert(
            test_db.branch_id,
            "default",
            "coll",
            "origin",
            &[0.0f32, 0.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            test_db.branch_id,
            "default",
            "coll",
            "unit",
            &[1.0f32, 0.0, 0.0],
            None,
        )
        .unwrap();

    let query = [2.0f32, 0.0, 0.0];
    let results = vector
        .search(test_db.branch_id, "default", "coll", &query, 2, None)
        .unwrap();

    // unit (distance 1) should be closer than origin (distance 2)
    assert_eq!(results[0].key, "unit");
    assert_eq!(results[1].key, "origin");
}

#[test]
fn cosine_distance() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_custom(3, DistanceMetric::Cosine);
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    // Same direction but different magnitude should be similar in cosine
    vector
        .insert(
            test_db.branch_id,
            "default",
            "coll",
            "unit",
            &[1.0f32, 0.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            test_db.branch_id,
            "default",
            "coll",
            "scaled",
            &[10.0f32, 0.0, 0.0],
            None,
        )
        .unwrap();
    vector
        .insert(
            test_db.branch_id,
            "default",
            "coll",
            "perpendicular",
            &[0.0f32, 1.0, 0.0],
            None,
        )
        .unwrap();

    let query = [5.0f32, 0.0, 0.0];
    let results = vector
        .search(test_db.branch_id, "default", "coll", &query, 3, None)
        .unwrap();

    // Both unit and scaled should be top 2 (same direction)
    let top_two: Vec<_> = results[0..2].iter().map(|r| r.key.as_str()).collect();
    assert!(top_two.contains(&"unit"));
    assert!(top_two.contains(&"scaled"));
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn empty_collection_name() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    // Empty name might be allowed or rejected depending on implementation
    let result = vector.create_collection(test_db.branch_id, "default", "", config);
    // Just ensure it doesn't panic - either works or returns error
    let _ = result;
}

#[test]
fn special_characters_in_key() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_small();
    vector
        .create_collection(test_db.branch_id, "default", "coll", config)
        .unwrap();

    let v = [1.0f32, 2.0, 3.0];
    let key = "key/with:special@chars";
    vector
        .insert(test_db.branch_id, "default", "coll", key, &v, None)
        .unwrap();

    let result = vector
        .get(test_db.branch_id, "default", "coll", key)
        .unwrap();
    assert_eq!(result.unwrap().value.embedding, vec![1.0f32, 2.0, 3.0]);
}

// ============================================================================
// Int8 Scalar Quantization (SQ8)
// ============================================================================

/// Helper: create a VectorConfig with Int8 storage
fn config_int8(dimension: usize) -> VectorConfig {
    VectorConfig {
        dimension,
        metric: DistanceMetric::Cosine,
        storage_dtype: StorageDtype::Int8,
    }
}

#[test]
fn int8_create_collection_and_insert() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_int8(384);
    vector
        .create_collection(test_db.branch_id, "default", "int8_coll", config)
        .unwrap();

    // Insert enough vectors to trigger calibration (default threshold = 256)
    for i in 0..300 {
        let key = format!("vec_{}", i);
        let emb = seeded_vector(384, i as u64);
        vector
            .insert(test_db.branch_id, "default", "int8_coll", &key, &emb, None)
            .unwrap();
    }

    // Verify count
    let info = vector
        .list_collections(test_db.branch_id, "default")
        .unwrap();
    let coll = info.iter().find(|c| c.name == "int8_coll").unwrap();
    assert_eq!(coll.count, 300);
}

#[test]
fn int8_search_returns_results() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_int8(384);
    vector
        .create_collection(test_db.branch_id, "default", "int8_search", config)
        .unwrap();

    // Insert 300 vectors (exceeds calibration threshold)
    for i in 0..300 {
        let key = format!("vec_{}", i);
        let emb = seeded_vector(384, i as u64);
        vector
            .insert(
                test_db.branch_id,
                "default",
                "int8_search",
                &key,
                &emb,
                None,
            )
            .unwrap();
    }

    // Search
    let query = seeded_vector(384, 42); // query matches vec_42 exactly
    let results = vector
        .search(test_db.branch_id, "default", "int8_search", &query, 5, None)
        .unwrap();

    assert_eq!(results.len(), 5);
    // The exact match should be the top result (highest cosine similarity)
    assert_eq!(results[0].key, "vec_42");
    // Score should be very close to 1.0 (exact match quantized)
    assert!(
        results[0].score > 0.95,
        "expected score > 0.95, got {}",
        results[0].score
    );
}

#[test]
fn int8_search_accuracy_vs_f32() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    // Create both F32 and Int8 collections
    let config_f = VectorConfig {
        dimension: 384,
        metric: DistanceMetric::Cosine,
        storage_dtype: StorageDtype::F32,
    };
    let config_i = config_int8(384);

    vector
        .create_collection(test_db.branch_id, "default", "f32_coll", config_f)
        .unwrap();
    vector
        .create_collection(test_db.branch_id, "default", "int8_coll", config_i)
        .unwrap();

    // Insert identical vectors into both
    for i in 0..300 {
        let key = format!("vec_{}", i);
        let emb = seeded_vector(384, i as u64);
        vector
            .insert(test_db.branch_id, "default", "f32_coll", &key, &emb, None)
            .unwrap();
        vector
            .insert(test_db.branch_id, "default", "int8_coll", &key, &emb, None)
            .unwrap();
    }

    // Search both with same query
    let query = seeded_vector(384, 999);
    let f32_results = vector
        .search(test_db.branch_id, "default", "f32_coll", &query, 10, None)
        .unwrap();
    let int8_results = vector
        .search(test_db.branch_id, "default", "int8_coll", &query, 10, None)
        .unwrap();

    assert_eq!(f32_results.len(), 10);
    assert_eq!(int8_results.len(), 10);

    // Top result should match (recall check)
    assert_eq!(
        f32_results[0].key, int8_results[0].key,
        "Top-1 result should match between F32 and Int8"
    );

    // Count how many of the top-10 from F32 appear in Int8's top-10
    let f32_keys: std::collections::HashSet<_> =
        f32_results.iter().map(|r| r.key.clone()).collect();
    let int8_keys: std::collections::HashSet<_> =
        int8_results.iter().map(|r| r.key.clone()).collect();
    let overlap = f32_keys.intersection(&int8_keys).count();
    assert!(
        overlap >= 8,
        "Expected at least 8/10 overlap in top-10, got {}/10",
        overlap
    );
}

#[test]
fn int8_delete_works() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_int8(384);
    vector
        .create_collection(test_db.branch_id, "default", "int8_del", config)
        .unwrap();

    // Insert 300 vectors
    for i in 0..300 {
        let key = format!("vec_{}", i);
        let emb = seeded_vector(384, i as u64);
        vector
            .insert(test_db.branch_id, "default", "int8_del", &key, &emb, None)
            .unwrap();
    }

    // Delete one
    vector
        .delete(test_db.branch_id, "default", "int8_del", "vec_50")
        .unwrap();

    // Verify it's gone from search
    let query = seeded_vector(384, 50);
    let results = vector
        .search(test_db.branch_id, "default", "int8_del", &query, 5, None)
        .unwrap();

    assert!(
        !results.iter().any(|r| r.key == "vec_50"),
        "Deleted vector should not appear in search results"
    );
}

#[test]
fn int8_small_collection_before_calibration() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    let config = config_int8(8);
    vector
        .create_collection(test_db.branch_id, "default", "int8_small", config)
        .unwrap();

    // Insert only 5 vectors (below calibration threshold of 256)
    // This exercises the pre-calibration search path (f32 fallback)
    for i in 0..5 {
        let key = format!("vec_{}", i);
        let emb = seeded_vector(8, i as u64);
        vector
            .insert(test_db.branch_id, "default", "int8_small", &key, &emb, None)
            .unwrap();
    }

    let query = seeded_vector(8, 2);
    let results = vector
        .search(test_db.branch_id, "default", "int8_small", &query, 3, None)
        .unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].key, "vec_2");
}

#[test]
fn int8_memory_savings() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    // Create F32 and Int8 collections
    let config_f = VectorConfig {
        dimension: 384,
        metric: DistanceMetric::Cosine,
        storage_dtype: StorageDtype::F32,
    };
    let config_i = config_int8(384);

    vector
        .create_collection(test_db.branch_id, "default", "f32_mem", config_f)
        .unwrap();
    vector
        .create_collection(test_db.branch_id, "default", "int8_mem", config_i)
        .unwrap();

    // Insert 300 vectors into both
    for i in 0..300 {
        let key = format!("vec_{}", i);
        let emb = seeded_vector(384, i as u64);
        vector
            .insert(test_db.branch_id, "default", "f32_mem", &key, &emb, None)
            .unwrap();
        vector
            .insert(test_db.branch_id, "default", "int8_mem", &key, &emb, None)
            .unwrap();
    }

    let f32_info = vector
        .list_collections(test_db.branch_id, "default")
        .unwrap();
    let f32_coll = f32_info.iter().find(|c| c.name == "f32_mem").unwrap();
    let int8_coll = f32_info.iter().find(|c| c.name == "int8_mem").unwrap();

    // Both should have the same count
    assert_eq!(f32_coll.count, 300);
    assert_eq!(int8_coll.count, 300);
    // Note: actual memory_usage comparison requires backend access;
    // here we just verify the collections work correctly with Int8
}
