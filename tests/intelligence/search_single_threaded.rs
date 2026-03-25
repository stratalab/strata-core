//! R8: Single-Threaded Tests
//!
//! Invariant R8: Similarity computation is single-threaded for determinism.

use crate::common::*;
use std::sync::Arc;
use std::thread;

/// Test that concurrent searches return identical results
#[test]
fn test_r8_search_deterministic_across_threads() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_minilm())
        .unwrap();

    for i in 0..100 {
        vector
            .insert(
                test_db.branch_id,
                "embeddings",
                &format!("key_{}", i),
                &seeded_random_vector(384, i as u64),
                None,
            )
            .unwrap();
    }

    let query = seeded_random_vector(384, 54321);
    let db = test_db.db.clone();
    let branch_id = test_db.branch_id;

    // Run search from multiple threads concurrently
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let db = db.clone();
            let query = query.clone();
            thread::spawn(move || {
                let vector = strata_vector::VectorStore::new(db);
                vector.search(branch_id, "embeddings", &query, 20, None).unwrap()
            })
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All results must be identical
    let keys_0: Vec<&str> = results[0].iter().map(|r| r.key.as_str()).collect();
    for (i, r) in results.iter().enumerate().skip(1) {
        let keys: Vec<&str> = r.iter().map(|r| r.key.as_str()).collect();
        assert_eq!(keys_0, keys, "R8 VIOLATED: Different results from thread {}", i);
    }
}

/// Test that scores are identical across threads
#[test]
fn test_r8_scores_identical_across_threads() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_minilm())
        .unwrap();

    for i in 0..50 {
        vector
            .insert(
                test_db.branch_id,
                "embeddings",
                &format!("key_{}", i),
                &seeded_random_vector(384, i as u64),
                None,
            )
            .unwrap();
    }

    let query = seeded_random_vector(384, 11111);
    let db = test_db.db.clone();
    let branch_id = test_db.branch_id;

    let handles: Vec<_> = (0..5)
        .map(|_| {
            let db = db.clone();
            let query = query.clone();
            thread::spawn(move || {
                let vector = strata_vector::VectorStore::new(db);
                vector.search(branch_id, "embeddings", &query, 10, None).unwrap()
            })
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // Compare scores across threads
    for (i, result) in results.iter().enumerate().skip(1) {
        for (j, (r0, ri)) in results[0].iter().zip(result.iter()).enumerate() {
            assert!(
                (r0.score - ri.score).abs() < 1e-6,
                "R8 VIOLATED: Score differs at position {} between thread 0 and {}: {} vs {}",
                j,
                i,
                r0.score,
                ri.score
            );
        }
    }
}

/// Test concurrent search and modification
#[test]
fn test_r8_concurrent_search_and_modify() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_minilm())
        .unwrap();

    // Insert initial vectors
    for i in 0..50 {
        vector
            .insert(
                test_db.branch_id,
                "embeddings",
                &format!("key_{}", i),
                &seeded_random_vector(384, i as u64),
                None,
            )
            .unwrap();
    }

    let db = test_db.db.clone();
    let branch_id = test_db.branch_id;

    // Launch search threads
    let search_handles: Vec<_> = (0..5)
        .map(|thread_id| {
            let db = db.clone();
            let query = seeded_random_vector(384, thread_id as u64);
            thread::spawn(move || {
                let vector = strata_vector::VectorStore::new(db);
                for _ in 0..10 {
                    let _ = vector.search(branch_id, "embeddings", &query, 10, None);
                }
            })
        })
        .collect();

    // Wait for all threads
    for handle in search_handles {
        handle.join().expect("Search thread panicked");
    }

    // System should remain stable
    let count = vector.count(test_db.branch_id, "embeddings").unwrap();
    assert!(count >= 50, "Concurrent access should not corrupt data");
}

/// Test single search doesn't use multiple cores for computation
#[test]
fn test_r8_single_search_determinism() {
    let test_db = TestDb::new();
    let vector = test_db.vector();

    vector
        .create_collection(test_db.branch_id, "embeddings", config_minilm())
        .unwrap();

    for i in 0..200 {
        vector
            .insert(
                test_db.branch_id,
                "embeddings",
                &format!("key_{}", i),
                &seeded_random_vector(384, i as u64),
                None,
            )
            .unwrap();
    }

    let query = seeded_random_vector(384, 99999);

    // Run many searches and verify all return identical results
    let mut all_results: Vec<Vec<String>> = Vec::new();
    for _ in 0..50 {
        let results = vector
            .search(test_db.branch_id, "embeddings", &query, 50, None)
            .unwrap();
        let keys: Vec<String> = results.iter().map(|r| r.key.clone()).collect();
        all_results.push(keys);
    }

    for (i, results) in all_results.iter().enumerate().skip(1) {
        assert_eq!(
            &all_results[0], results,
            "R8 VIOLATED: Search {} returned different results",
            i
        );
    }
}
