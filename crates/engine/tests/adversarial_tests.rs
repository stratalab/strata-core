// Tests temporarily commented out during engine re-architecture.
// These tests use internal engine methods (wal, flush, transaction_with_version,
// transaction_with_retry) that are now pub(crate). Uncomment once the new API
// surface exposes equivalent functionality.

/*
//! Adversarial Tests for strata-primitives
//!
//! Tests targeting edge cases, race conditions, and error paths that
//! could expose bugs in primitives:
//!
//! 1. KVStore: Concurrent contention, large values, special keys
//! 2. EventLog: Concurrent appends, hash chain integrity, payload validation
//! 3. StateCell: CAS contention, version monotonicity under stress
//! 4. VectorStore: Numerical precision, concurrent operations
//! 5. BranchIndex: Concurrent branch operations, cascade delete
//!
//! These tests follow the TESTING_METHODOLOGY.md principles:
//! - Test behavior, not implementation
//! - One failure mode per test
//! - Verify values, not just is_ok()

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use strata_core::contract::Version;
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_engine::Database;
use strata_engine::{EventLog, KVStore};
use strata_core::primitives::vector::{DistanceMetric, VectorConfig};
// VectorStore is in strata-vector; see root integration tests
use tempfile::TempDir;

// ============================================================================
// Test Helpers
// ============================================================================

fn setup() -> (Arc<Database>, TempDir, BranchId) {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();
    let branch_id = BranchId::new();
    (db, temp_dir, branch_id)
}

fn empty_payload() -> Value {
    Value::object(HashMap::new())
}

fn int_payload(v: i64) -> Value {
    Value::object(HashMap::from([("value".to_string(), Value::Int(v))]))
}

// ============================================================================
// Module 1: KVStore Adversarial Tests
// ============================================================================

/// Test concurrent puts to same key - last write wins, no data corruption
#[test]
fn test_kv_concurrent_puts_same_key_no_corruption() {
    let (db, _temp, branch_id) = setup();
    let kv = KVStore::new(db.clone());

    let num_threads = 8;
    let writes_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));
    let success_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let kv = kv.clone();
            let branch_id = branch_id;
            let barrier = Arc::clone(&barrier);
            let success_count = Arc::clone(&success_count);

            thread::spawn(move || {
                barrier.wait();
                for i in 0..writes_per_thread {
                    let value = Value::Int((thread_id * 1000 + i) as i64);
                    if kv.put(&branch_id, "contested_key", value).is_ok() {
                        success_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // All writes should succeed (OCC handles conflicts)
    let total_writes = success_count.load(Ordering::Relaxed);
    assert_eq!(
        total_writes,
        (num_threads * writes_per_thread) as u64,
        "All writes should succeed with OCC retry"
    );

    // Final value should be valid (from some thread)
    let final_value = kv.get(&branch_id, "contested_key").unwrap().unwrap();
    match final_value.value {
        Value::Int(v) => assert!(v >= 0, "Value should be a valid integer"),
        _ => panic!("Value should be Int"),
    }
}

/// Test concurrent puts to different keys - all should succeed
#[test]
fn test_kv_concurrent_puts_different_keys_all_succeed() {
    let (db, _temp, branch_id) = setup();
    let kv = KVStore::new(db.clone());

    let num_threads = 8;
    let keys_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let kv = kv.clone();
            let branch_id = branch_id;
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();
                for i in 0..keys_per_thread {
                    let key = format!("thread_{}_key_{}", thread_id, i);
                    let value = Value::Int((thread_id * 1000 + i) as i64);
                    kv.put(&branch_id, &key, value).unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all keys exist with correct values
    for thread_id in 0..num_threads {
        for i in 0..keys_per_thread {
            let key = format!("thread_{}_key_{}", thread_id, i);
            let expected = (thread_id * 1000 + i) as i64;
            let actual = kv.get(&branch_id, &key).unwrap().unwrap();
            assert_eq!(
                actual.value,
                Value::Int(expected),
                "Key {} should have correct value",
                key
            );
        }
    }
}

/// Test rapid put-delete cycles don't cause inconsistency
#[test]
fn test_kv_rapid_put_delete_cycles() {
    let (db, _temp, branch_id) = setup();
    let kv = KVStore::new(db.clone());

    let cycles = 100;

    for i in 0..cycles {
        let key = format!("cycle_key_{}", i % 10); // Reuse 10 keys

        // Put
        kv.put(&branch_id, &key, Value::Int(i as i64)).unwrap();

        // Verify exists
        assert!(kv.exists(&branch_id, &key).unwrap(), "Key should exist after put");

        // Delete
        kv.delete(&branch_id, &key).unwrap();

        // Verify deleted (uses MVCC tombstone)
        assert!(
            !kv.exists(&branch_id, &key).unwrap(),
            "Key should not exist after delete"
        );
    }
}

/// Test special characters in keys
#[test]
fn test_kv_special_characters_in_keys() {
    let (db, _temp, branch_id) = setup();
    let kv = KVStore::new(db.clone());

    let long_key = "x".repeat(1000);
    let special_keys = vec![
        "key with spaces",
        "key/with/slashes",
        "key:with:colons",
        "key\twith\ttabs",
        "key\nwith\nnewlines",
        "key🎉with🎉emoji",
        "key_with_underscores",
        "key-with-dashes",
        "key.with.dots",
        "",  // Empty key
        "a", // Single char
        long_key.as_str(), // Very long key
    ];

    for (i, key) in special_keys.iter().enumerate() {
        let value = Value::Int(i as i64);
        kv.put(&branch_id, key, value.clone()).unwrap();

        let retrieved = kv.get(&branch_id, key).unwrap().unwrap();
        assert_eq!(
            retrieved.value, value,
            "Key {:?} should round-trip correctly",
            key
        );
    }
}

/// Test version monotonicity under concurrent updates
#[test]
fn test_kv_version_monotonicity_under_contention() {
    let (db, _temp, branch_id) = setup();
    let kv = KVStore::new(db.clone());

    // Initial put
    let v1 = kv.put(&branch_id, "versioned_key", Value::Int(0)).unwrap();

    let num_threads = 4;
    let updates_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));
    let versions = Arc::new(parking_lot::Mutex::new(vec![v1.as_u64()]));

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let kv = kv.clone();
            let branch_id = branch_id;
            let barrier = Arc::clone(&barrier);
            let versions = Arc::clone(&versions);

            thread::spawn(move || {
                barrier.wait();
                for i in 0..updates_per_thread {
                    let result = kv.put(&branch_id, "versioned_key", Value::Int(i as i64));
                    if let Ok(version) = result {
                        versions.lock().push(version.as_u64());
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // All versions collected should be monotonically increasing for each thread
    // But versions from different threads may interleave
    let all_versions = versions.lock();
    let mut sorted_versions = all_versions.clone();
    sorted_versions.sort();
    sorted_versions.dedup();

    // Should have many unique versions (some may collide due to concurrent writes)
    assert!(
        sorted_versions.len() > num_threads,
        "Should have multiple unique versions"
    );

    // Final read should have highest version
    let final_read = kv.get(&branch_id, "versioned_key").unwrap().unwrap();
    assert!(
        final_read.version.as_u64() >= *sorted_versions.last().unwrap(),
        "Final version should be at least max recorded"
    );
}

// ============================================================================
// Module 2: EventLog Adversarial Tests
// ============================================================================

/// Test concurrent appends maintain total order
#[test]
fn test_eventlog_concurrent_appends_total_order() {
    let (db, _temp, branch_id) = setup();
    let event_log = EventLog::new(db.clone());

    let num_threads = 4;
    let events_per_thread = 25;
    let barrier = Arc::new(Barrier::new(num_threads));
    let sequences = Arc::new(parking_lot::Mutex::new(Vec::<u64>::new()));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let event_log = event_log.clone();
            let branch_id = branch_id;
            let barrier = Arc::clone(&barrier);
            let sequences = Arc::clone(&sequences);

            thread::spawn(move || {
                barrier.wait();
                for i in 0..events_per_thread {
                    let event_type = format!("thread_{}_event", thread_id);
                    let payload = int_payload((thread_id * 1000 + i) as i64);
                    if let Ok(seq) = event_log.append(&branch_id, &event_type, payload) {
                        sequences.lock().push(seq.as_u64());
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify total order: all sequences should be unique and form contiguous range
    let mut all_seqs = sequences.lock().clone();
    all_seqs.sort();
    all_seqs.dedup();

    let expected_count = (num_threads * events_per_thread) as usize;
    assert_eq!(
        all_seqs.len(),
        expected_count,
        "All events should have unique sequences"
    );

    // Sequences should be 0..expected_count
    for (i, seq) in all_seqs.iter().enumerate() {
        assert_eq!(
            *seq, i as u64,
            "Sequences should be contiguous starting from 0"
        );
    }
}

/// Test hash chain integrity after concurrent appends
#[test]
fn test_eventlog_hash_chain_integrity_under_concurrency() {
    let (db, _temp, branch_id) = setup();
    let event_log = EventLog::new(db.clone());

    let num_threads = 4;
    let events_per_thread = 20;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let event_log = event_log.clone();
            let branch_id = branch_id;
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();
                for i in 0..events_per_thread {
                    let event_type = format!("event_{}", thread_id);
                    let payload = int_payload(i as i64);
                    let _ = event_log.append(&branch_id, &event_type, payload);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify chain integrity
    let verification = event_log.verify_chain(&branch_id).unwrap();
    assert!(
        verification.is_valid,
        "Hash chain should be valid after concurrent appends"
    );
    assert_eq!(
        verification.length,
        (num_threads * events_per_thread) as u64,
        "All events should be verified"
    );
}

/// Test event type validation edge cases
#[test]
fn test_eventlog_event_type_validation() {
    let (db, _temp, branch_id) = setup();
    let event_log = EventLog::new(db.clone());

    // Empty event type should fail
    let result = event_log.append(&branch_id, "", empty_payload());
    assert!(result.is_err(), "Empty event type should be rejected");

    // Very long event type should fail (max 256 chars)
    let long_type = "x".repeat(300);
    let result = event_log.append(&branch_id, &long_type, empty_payload());
    assert!(result.is_err(), "Event type > 256 chars should be rejected");

    // Max length should succeed
    let max_type = "x".repeat(256);
    let result = event_log.append(&branch_id, &max_type, empty_payload());
    assert!(result.is_ok(), "Event type of exactly 256 chars should succeed");

    // Special characters should work
    let special_types = vec![
        "event/with/slashes",
        "event:with:colons",
        "event.with.dots",
        "event_with_underscores",
        "event-with-dashes",
        "event with spaces",
        "event🎉emoji",
    ];

    for event_type in special_types {
        let result = event_log.append(&branch_id, event_type, empty_payload());
        assert!(
            result.is_ok(),
            "Event type {:?} should be accepted",
            event_type
        );
    }
}

/// Test payload validation - only objects allowed
#[test]
fn test_eventlog_payload_validation() {
    let (db, _temp, branch_id) = setup();
    let event_log = EventLog::new(db.clone());

    // Object payload should succeed
    let result = event_log.append(&branch_id, "test", empty_payload());
    assert!(result.is_ok(), "Object payload should succeed");

    // Primitive payloads should fail
    let primitives = vec![
        Value::Int(42),
        Value::Float(3.14),
        Value::String("hello".into()),
        Value::Bool(true),
        Value::Null,
    ];

    for payload in primitives {
        let result = event_log.append(&branch_id, "test", payload.clone());
        assert!(
            result.is_err(),
            "Primitive payload {:?} should be rejected",
            payload
        );
    }

    // Array payload should fail
    let array_payload = Value::array(vec![Value::Int(1), Value::Int(2)]);
    let result = event_log.append(&branch_id, "test", array_payload);
    assert!(result.is_err(), "Array payload should be rejected");
}

/// Test stream metadata accuracy under concurrent appends
#[test]
fn test_eventlog_stream_metadata_accuracy() {
    let (db, _temp, branch_id) = setup();
    let event_log = EventLog::new(db.clone());

    let num_threads = 4;
    let events_per_thread = 20;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let event_log = event_log.clone();
            let branch_id = branch_id;
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();
                let event_type = format!("stream_{}", thread_id);
                for i in 0..events_per_thread {
                    let payload = int_payload(i as i64);
                    let _ = event_log.append(&branch_id, &event_type, payload);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify total count
    let total = event_log.len(&branch_id).unwrap();
    assert_eq!(
        total,
        (num_threads * events_per_thread) as u64,
        "Total event count should match"
    );

    // Verify per-stream counts
    for thread_id in 0..num_threads {
        let stream = format!("stream_{}", thread_id);
        let stream_info = event_log.stream_info(&branch_id, &stream).unwrap();
        let count = stream_info.map(|m| m.count).unwrap_or(0);
        assert_eq!(
            count, events_per_thread as u64,
            "Stream {} should have correct count",
            stream
        );
    }
}

// ============================================================================
// Module 4: VectorStore Adversarial Tests
// ============================================================================

/// Test vector search with edge case floats (zeros, very small, very large)
#[test]
fn test_vector_search_edge_case_floats() {
    let (db, _temp, branch_id) = setup();
    let store = VectorStore::new(db.clone());

    let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
    store.create_collection(branch_id, "floats", config).unwrap();

    // Insert vectors with edge case values
    let vectors = vec![
        ("zeros", vec![0.0, 0.0, 0.0, 0.0]),
        ("ones", vec![1.0, 1.0, 1.0, 1.0]),
        ("negative", vec![-1.0, -1.0, -1.0, -1.0]),
        ("small", vec![1e-10, 1e-10, 1e-10, 1e-10]),
        ("large", vec![1e10, 1e10, 1e10, 1e10]),
        ("mixed", vec![-1e10, 0.0, 1e-10, 1e10]),
    ];

    for (key, embedding) in &vectors {
        store
            .insert(branch_id, "floats", key, embedding, None)
            .unwrap();
    }

    // Search with normal query
    let query = vec![1.0, 1.0, 1.0, 1.0];
    let results = store.search(branch_id, "floats", &query, 6, None).unwrap();

    // Should return all vectors (6 results)
    assert_eq!(results.len(), 6, "Should return all 6 vectors");

    // Scores should be valid (not NaN or Inf)
    for result in &results {
        assert!(
            result.score.is_finite(),
            "Score for {} should be finite, got {}",
            result.key,
            result.score
        );
    }

    // "ones" should have cosine similarity of 1.0 (perfect match)
    let ones_result = results.iter().find(|r| r.key == "ones").unwrap();
    assert!(
        (ones_result.score - 1.0).abs() < 0.001,
        "ones should have cosine similarity ~1.0, got {}",
        ones_result.score
    );

    // "negative" should have cosine similarity of -1.0 (opposite direction)
    let neg_result = results.iter().find(|r| r.key == "negative").unwrap();
    assert!(
        (neg_result.score + 1.0).abs() < 0.001,
        "negative should have cosine similarity ~-1.0, got {}",
        neg_result.score
    );
}

/// Test vector dimension validation
#[test]
fn test_vector_dimension_validation() {
    let (db, _temp, branch_id) = setup();
    let store = VectorStore::new(db.clone());

    let config = VectorConfig::new(3, DistanceMetric::Euclidean).unwrap();
    store.create_collection(branch_id, "dim3", config).unwrap();

    // Correct dimension should work
    store
        .insert(branch_id, "dim3", "correct", &[1.0, 2.0, 3.0], None)
        .unwrap();

    // Wrong dimension should fail
    let result = store.insert(branch_id, "dim3", "wrong_small", &[1.0, 2.0], None);
    assert!(
        result.is_err(),
        "Vector with wrong dimension (2) should be rejected"
    );

    let result = store.insert(
        branch_id,
        "dim3",
        "wrong_large",
        &[1.0, 2.0, 3.0, 4.0],
        None,
    );
    assert!(
        result.is_err(),
        "Vector with wrong dimension (4) should be rejected"
    );
}

/// Test collection isolation - different collections don't interfere
#[test]
fn test_vector_collection_isolation() {
    let (db, _temp, branch_id) = setup();
    let store = VectorStore::new(db.clone());

    let config = VectorConfig::new(2, DistanceMetric::Cosine).unwrap();
    store.create_collection(branch_id, "coll_a", config.clone()).unwrap();
    store.create_collection(branch_id, "coll_b", config).unwrap();

    // Insert into collection A
    store
        .insert(branch_id, "coll_a", "key1", &[1.0, 0.0], None)
        .unwrap();

    // Insert into collection B
    store
        .insert(branch_id, "coll_b", "key1", &[0.0, 1.0], None)
        .unwrap();

    // Search in A should find A's vector
    let results_a = store
        .search(branch_id, "coll_a", &[1.0, 0.0], 10, None)
        .unwrap();
    assert_eq!(results_a.len(), 1);
    assert_eq!(results_a[0].key, "key1");

    // Search in B should find B's vector
    let results_b = store
        .search(branch_id, "coll_b", &[0.0, 1.0], 10, None)
        .unwrap();
    assert_eq!(results_b.len(), 1);
    assert_eq!(results_b[0].key, "key1");

    // Delete from A shouldn't affect B
    store.delete(branch_id, "coll_a", "key1").unwrap();

    let results_a_after = store
        .search(branch_id, "coll_a", &[1.0, 0.0], 10, None)
        .unwrap();
    assert_eq!(results_a_after.len(), 0, "A should be empty after delete");

    let results_b_after = store
        .search(branch_id, "coll_b", &[0.0, 1.0], 10, None)
        .unwrap();
    assert_eq!(results_b_after.len(), 1, "B should still have the vector");
}

/// Test upsert overwrites existing vector
#[test]
fn test_vector_upsert_overwrites() {
    let (db, _temp, branch_id) = setup();
    let store = VectorStore::new(db.clone());

    let config = VectorConfig::new(2, DistanceMetric::DotProduct).unwrap();
    store.create_collection(branch_id, "overwrite", config).unwrap();

    // Initial insert
    store
        .insert(branch_id, "overwrite", "key", &[1.0, 0.0], None)
        .unwrap();

    // Search should find it
    let results = store
        .search(branch_id, "overwrite", &[1.0, 0.0], 10, None)
        .unwrap();
    assert_eq!(results.len(), 1);
    assert!((results[0].score - 1.0).abs() < 0.001, "Score should be ~1.0");

    // Overwrite with different vector
    store
        .insert(branch_id, "overwrite", "key", &[0.0, 1.0], None)
        .unwrap();

    // Search with old query should have lower score
    let results_after = store
        .search(branch_id, "overwrite", &[1.0, 0.0], 10, None)
        .unwrap();
    assert_eq!(results_after.len(), 1);
    assert!(
        results_after[0].score.abs() < 0.001,
        "Score should be ~0.0 (orthogonal)"
    );

    // Search with new query should have high score
    let results_new = store
        .search(branch_id, "overwrite", &[0.0, 1.0], 10, None)
        .unwrap();
    assert!(
        (results_new[0].score - 1.0).abs() < 0.001,
        "Score should be ~1.0 for new vector"
    );
}

/// Test distance metric correctness
#[test]
fn test_vector_distance_metrics_correctness() {
    let (db, _temp, branch_id) = setup();
    let store = VectorStore::new(db.clone());

    // Test each distance metric
    let test_cases = vec![
        (
            "cosine",
            DistanceMetric::Cosine,
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            0.0, // Orthogonal vectors have 0 cosine similarity
        ),
        (
            "euclidean",
            DistanceMetric::Euclidean,
            vec![0.0, 0.0],
            vec![3.0, 4.0],
            5.0, // sqrt(9 + 16) = 5
        ),
        (
            "dot",
            DistanceMetric::DotProduct,
            vec![2.0, 3.0],
            vec![4.0, 5.0],
            23.0, // 2*4 + 3*5 = 23
        ),
    ];

    for (name, metric, v1, v2, expected) in test_cases {
        let config = VectorConfig::new(2, metric).unwrap();
        store
            .create_collection(branch_id, name, config)
            .unwrap();

        store
            .insert(branch_id, name, "target", &v1, None)
            .unwrap();

        let results = store.search(branch_id, name, &v2, 1, None).unwrap();
        assert_eq!(results.len(), 1);

        let score = results[0].score;
        let tolerance = 0.001;

        match metric {
            DistanceMetric::Euclidean => {
                // For Euclidean, lower is better, score might be distance or 1/distance
                // Just verify it's finite
                assert!(score.is_finite(), "{} score should be finite", name);
            }
            _ => {
                assert!(
                    (score - expected).abs() < tolerance,
                    "{}: expected {}, got {}",
                    name,
                    expected,
                    score
                );
            }
        }
    }
}

/// Test concurrent upserts to same collection
#[test]
fn test_vector_concurrent_upserts_same_collection() {
    let (db, _temp, branch_id) = setup();
    let store = VectorStore::new(db.clone());

    let config = VectorConfig::new(4, DistanceMetric::Cosine).unwrap();
    store.create_collection(branch_id, "concurrent", config).unwrap();

    let num_threads = 4;
    let vectors_per_thread = 25;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();
                for i in 0..vectors_per_thread {
                    let key = format!("thread_{}_vec_{}", thread_id, i);
                    let embedding = [
                        thread_id as f32,
                        i as f32,
                        (thread_id + i) as f32,
                        1.0,
                    ];
                    store
                        .insert(branch_id, "concurrent", &key, &embedding, None)
                        .unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all vectors exist
    let expected_count = num_threads * vectors_per_thread;
    let results = store
        .search(branch_id, "concurrent", &[1.0, 1.0, 1.0, 1.0], expected_count, None)
        .unwrap();

    assert_eq!(
        results.len(),
        expected_count,
        "All {} vectors should be searchable",
        expected_count
    );
}

// ============================================================================
// Module 5: Cross-Primitive Adversarial Tests
// ============================================================================

/// Test branch isolation - operations in one branch don't affect another
#[test]
fn test_branch_isolation_comprehensive() {
    let (db, _temp, _) = setup();
    let branch_a = BranchId::new();
    let branch_b = BranchId::new();

    let kv = KVStore::new(db.clone());
    let event_log = EventLog::new(db.clone());
    let state_cell = StateCell::new(db.clone());

    // Write to branch A
    kv.put(&branch_a, "key", Value::Int(1)).unwrap();
    event_log.append(&branch_a, "event", empty_payload()).unwrap();
    state_cell.init(&branch_a, "default", "cell", Value::Int(100)).unwrap();

    // Write to branch B
    kv.put(&branch_b, "key", Value::Int(2)).unwrap();
    event_log.append(&branch_b, "event", empty_payload()).unwrap();
    state_cell.init(&branch_b, "default", "cell", Value::Int(200)).unwrap();

    // Verify isolation
    assert_eq!(
        kv.get(&branch_a, "key").unwrap().unwrap(),
        Value::Int(1),
        "Branch A should see its own KV value"
    );
    assert_eq!(
        kv.get(&branch_b, "key").unwrap().unwrap(),
        Value::Int(2),
        "Branch B should see its own KV value"
    );

    assert_eq!(
        event_log.len(&branch_a).unwrap(),
        1,
        "Branch A should have 1 event"
    );
    assert_eq!(
        event_log.len(&branch_b).unwrap(),
        1,
        "Branch B should have 1 event"
    );

    assert_eq!(
        state_cell.get(&branch_a, "default", "cell").unwrap().unwrap().value,
        Value::Int(100),
        "Branch A should see its own state"
    );
    assert_eq!(
        state_cell.get(&branch_b, "default", "cell").unwrap().unwrap().value,
        Value::Int(200),
        "Branch B should see its own state"
    );

    // Delete from branch A shouldn't affect branch B
    kv.delete(&branch_a, "key").unwrap();
    assert!(
        kv.get(&branch_b, "key").unwrap().is_some(),
        "Branch B key should still exist after deleting from branch A"
    );
}

/// Test database reopen preserves all primitive state
#[test]
fn test_persistence_across_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();

    // Write data
    {
        let db = Database::open(temp_dir.path()).unwrap();
        let kv = KVStore::new(db.clone());
        let event_log = EventLog::new(db.clone());
        let state_cell = StateCell::new(db.clone());

        kv.put(&branch_id, "persistent_key", Value::String("value".into()))
            .unwrap();
        event_log
            .append(&branch_id, "persistent_event", int_payload(42))
            .unwrap();
        state_cell
            .init(&branch_id, "default", "persistent_cell", Value::Int(999))
            .unwrap();

        db.flush().unwrap();
    }

    // Reopen and verify
    {
        let db = Database::open(temp_dir.path()).unwrap();
        let kv = KVStore::new(db.clone());
        let event_log = EventLog::new(db.clone());
        let state_cell = StateCell::new(db.clone());

        // KV should persist
        let kv_value = kv.get(&branch_id, "persistent_key").unwrap().unwrap();
        assert_eq!(
            kv_value.value,
            Value::String("value".into()),
            "KV value should persist across reopen"
        );

        // EventLog should persist
        let event_count = event_log.len(&branch_id).unwrap();
        assert_eq!(event_count, 1, "Event should persist across reopen");

        // StateCell should persist
        let state = state_cell.get(&branch_id, "default", "persistent_cell").unwrap().unwrap();
        assert_eq!(
            state.value.value,
            Value::Int(999),
            "StateCell value should persist across reopen"
        );
    }
}

*/
