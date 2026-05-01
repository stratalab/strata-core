//! Tier 7: M6 Hybrid Search Tests

use crate::common::*;
use strata_core::Value;

#[test]
fn test_vector_with_kv_coexistence() {
    let test_db = TestDb::new();
    let vector = test_db.vector();
    let kv = test_db.kv();

    vector.create_collection(test_db.branch_id, "embeddings", config_minilm()).unwrap();

    // Insert document content in KV and embedding in vector
    for i in 0..20 {
        let key = format!("doc_{}", i);
        kv.put(&test_db.branch_id, &key, Value::String(format!("content about topic {}", i))).unwrap();
        vector.insert(test_db.branch_id, "embeddings", &key, &seeded_random_vector(384, i as u64), None).unwrap();
    }

    // Both KV and Vector should be searchable
    let kv_result = kv.get(&test_db.branch_id, "doc_5").unwrap();
    assert!(kv_result.is_some());

    let query = seeded_random_vector(384, 999);
    let vector_results = vector.search(test_db.branch_id, "embeddings", &query, 10, None).unwrap();
    assert!(!vector_results.is_empty());
}

#[test]
fn test_same_keys_in_kv_and_vector() {
    let test_db = TestDb::new();
    let vector = test_db.vector();
    let kv = test_db.kv();

    vector.create_collection(test_db.branch_id, "embeddings", config_minilm()).unwrap();

    // Use same key for both primitives
    kv.put(&test_db.branch_id, "document", Value::String("text content".into())).unwrap();
    vector.insert(test_db.branch_id, "embeddings", "document", &random_vector(384), None).unwrap();

    // Both should be retrievable
    assert!(kv.get(&test_db.branch_id, "document").unwrap().is_some());
    assert!(vector.get(test_db.branch_id, "embeddings", "document").unwrap().is_some());
}

#[test]
fn test_independent_deletion() {
    let test_db = TestDb::new();
    let vector = test_db.vector();
    let kv = test_db.kv();

    vector.create_collection(test_db.branch_id, "embeddings", config_minilm()).unwrap();

    kv.put(&test_db.branch_id, "doc", Value::String("content".into())).unwrap();
    vector.insert(test_db.branch_id, "embeddings", "doc", &random_vector(384), None).unwrap();

    // Delete from KV only
    kv.delete(&test_db.branch_id, "doc").unwrap();

    // Vector should still exist
    assert!(kv.get(&test_db.branch_id, "doc").unwrap().is_none());
    assert!(vector.get(test_db.branch_id, "embeddings", "doc").unwrap().is_some());
}
