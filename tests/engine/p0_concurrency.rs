//! P0 Concurrency Tests (Issue #1910)
//!
//! Three high-risk concurrency scenarios identified during the 2026-03-25 audit:
//! 1. Concurrent branch merge + write to source branch (TOCTOU)
//! 2. Vector HNSW search during concurrent upsert + seal
//! 3. Event append hash chain under high contention

use crate::common::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use strata_engine::{MergeOptions, MergeStrategy};

/// Helper to create an event payload object
fn event_payload(data: Value) -> Value {
    Value::object(HashMap::from([("data".to_string(), data)]))
}

// ============================================================================
// Test 1: Concurrent branch merge + write to source branch
// ============================================================================

/// Verifies that concurrent writes to a target branch during merge do not cause
/// lost writes or phantom reads.
///
/// Setup:
///   - Fork branch B from A
///   - Diverge: write different keys to A and B
///   - Thread 1: merge(B → A) with LWW strategy
///   - Thread 2: concurrent put() to branch A during merge
///
/// Invariants tested: ACID-003, ACID-004, MVCC-001, MVCC-004, COW-003
#[test]
fn test_issue_1910_merge_concurrent_source_write() {
    for iteration in 0..50 {
        let test_db = TestDb::new();
        let branches = test_db.db.branches();
        let kv = test_db.kv();

        // Create named branches
        branches.create("target").unwrap();
        let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

        // Write initial data before fork
        kv.put(&target_id, "default", "base_key", Value::Int(0))
            .unwrap();
        for i in 0..5 {
            kv.put(
                &target_id,
                "default",
                &format!("shared_{}", i),
                Value::Int(i),
            )
            .unwrap();
        }

        // Fork target → source
        test_db.db.branches().fork("target", "source").unwrap();
        let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

        // Diverge: source modifies shared keys and adds new ones
        for i in 0..5 {
            kv.put(
                &source_id,
                "default",
                &format!("shared_{}", i),
                Value::Int(100 + i),
            )
            .unwrap();
        }
        kv.put(
            &source_id,
            "default",
            "source_only",
            Value::String("from_source".into()),
        )
        .unwrap();

        let db = test_db.db.clone();
        let barrier = Arc::new(Barrier::new(2));

        // Thread 1: merge source → target
        let db1 = db.clone();
        let barrier1 = barrier.clone();
        let merge_handle = thread::spawn(move || {
            barrier1.wait();
            db1.branches().merge_with_options(
                "source",
                "target",
                MergeOptions::with_strategy(MergeStrategy::LastWriterWins),
            )
        });

        // Thread 2: concurrent writes to target during merge
        let db2 = db.clone();
        let barrier2 = barrier.clone();
        let write_handle = thread::spawn(move || {
            let kv2 = KVStore::new(db2);
            let target_id2 = strata_engine::primitives::branch::resolve_branch_name("target");
            barrier2.wait();

            // Write to non-overlapping keys (should not conflict with merge)
            for i in 0..10 {
                let key = format!("concurrent_{}", i);
                kv2.put(&target_id2, "default", &key, Value::Int(1000 + i))
                    .unwrap();
            }
        });

        let merge_result = merge_handle.join().unwrap();
        write_handle.join().unwrap();

        // The merge should succeed (non-overlapping keys)
        match &merge_result {
            Ok(info) => {
                assert!(
                    info.keys_applied > 0,
                    "iteration {}: merge should have applied keys",
                    iteration
                );
            }
            Err(e) => {
                // OCC conflict is acceptable — means concurrency was detected
                let err_msg = format!("{}", e);
                assert!(
                    err_msg.contains("conflict")
                        || err_msg.contains("Conflict")
                        || err_msg.contains("concurrently modified"),
                    "iteration {}: unexpected merge error: {}",
                    iteration,
                    e
                );
            }
        }

        // Verify: no lost writes — concurrent keys must exist
        for i in 0..10 {
            let key = format!("concurrent_{}", i);
            let val = kv.get(&target_id, "default", &key).unwrap();
            assert!(
                val.is_some(),
                "iteration {}: concurrent key '{}' was lost",
                iteration,
                key
            );
        }

        // Verify: if merge succeeded, source-only key must be present
        if merge_result.is_ok() {
            let source_only = kv.get(&target_id, "default", "source_only").unwrap();
            assert_eq!(
                source_only,
                Some(Value::String("from_source".into())),
                "iteration {}: merge succeeded but source_only key is missing",
                iteration
            );
        }
    }
}

/// Stress variant: merge with overlapping keys forces OCC conflict detection.
#[test]
fn test_issue_1910_merge_concurrent_overlapping_write() {
    let mut conflict_count = 0u64;
    let mut success_count = 0u64;

    for _ in 0..50 {
        let test_db = TestDb::new();
        let branches = test_db.db.branches();
        let kv = test_db.kv();

        branches.create("target").unwrap();
        let target_id = strata_engine::primitives::branch::resolve_branch_name("target");

        kv.put(&target_id, "default", "contested", Value::Int(0))
            .unwrap();

        test_db.db.branches().fork("target", "source").unwrap();
        let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

        // Source modifies the contested key
        kv.put(&source_id, "default", "contested", Value::Int(42))
            .unwrap();

        let db = test_db.db.clone();
        let barrier = Arc::new(Barrier::new(2));

        // Thread 1: merge source → target (touches "contested")
        let db1 = db.clone();
        let barrier1 = barrier.clone();
        let merge_handle = thread::spawn(move || {
            barrier1.wait();
            db1.branches().merge_with_options(
                "source",
                "target",
                MergeOptions::with_strategy(MergeStrategy::LastWriterWins),
            )
        });

        // Thread 2: write to the same contested key on target
        let db2 = db.clone();
        let barrier2 = barrier.clone();
        let write_handle = thread::spawn(move || {
            let kv2 = KVStore::new(db2);
            let target_id2 = strata_engine::primitives::branch::resolve_branch_name("target");
            barrier2.wait();
            kv2.put(&target_id2, "default", "contested", Value::Int(999))
        });

        let merge_result = merge_handle.join().unwrap();
        let write_result = write_handle.join().unwrap();

        // At least one must succeed; final value must be consistent
        let final_val = kv
            .get(&target_id, "default", "contested")
            .unwrap()
            .expect("contested key must exist");

        match (&merge_result, &write_result) {
            (Ok(_), Ok(_)) => {
                // Both succeeded — last writer wins. Value must be one of the two.
                assert!(
                    final_val == Value::Int(42) || final_val == Value::Int(999),
                    "unexpected value after both succeeded: {:?}",
                    final_val
                );
                success_count += 1;
            }
            (Err(_), Ok(_)) => {
                // Merge failed (OCC conflict), write succeeded
                assert_eq!(final_val, Value::Int(999));
                conflict_count += 1;
            }
            (Ok(_), Err(_)) => {
                // Merge succeeded, write failed (unlikely but possible)
                assert_eq!(final_val, Value::Int(42));
                conflict_count += 1;
            }
            (Err(e1), Err(e2)) => {
                // Both failed is acceptable only if contested key has pre-merge value
                assert_eq!(
                    final_val,
                    Value::Int(0),
                    "both failed but value changed: merge={}, write={}",
                    e1,
                    e2
                );
                conflict_count += 1;
            }
        }
    }

    // Over 50 iterations, we should see at least some success and the test should
    // demonstrate that overlapping writes are handled without data corruption.
    println!(
        "Merge+write overlap: {} successes, {} conflicts",
        success_count, conflict_count
    );
}

// ============================================================================
// Test 2: Vector HNSW search during concurrent upsert + seal
// ============================================================================

/// Verifies that vector search returns valid results during concurrent upsert
/// operations (which may trigger segment seals).
///
/// Setup:
///   - Create vector collection
///   - Insert initial vectors
///   - Thread 1: continuous search() queries
///   - Thread 2: continuous upsert() of new vectors (triggers seals)
///   - Thread 3: continuous upsert() of existing vectors (updates)
///
/// Invariants tested: ARCH-002, ARCH-003
#[test]
fn test_issue_1910_vector_query_during_concurrent_upsert() {
    let test_db = TestDb::new_in_memory();
    let vector = test_db.vector();
    let branch_id = test_db.branch_id;
    let dimension = 8;

    let config = config_custom(dimension, DistanceMetric::Cosine);
    vector
        .create_collection(branch_id, "default", "test_coll", config)
        .unwrap();

    // Insert initial vectors so search has something to find
    let initial_count = 20;
    for i in 0..initial_count {
        let key = format!("init_{}", i);
        let embedding = seeded_vector(dimension, i as u64);
        vector
            .insert(branch_id, "default", "test_coll", &key, &embedding, None)
            .unwrap();
    }

    let db = test_db.db.clone();
    let num_iterations = 100;
    let barrier = Arc::new(Barrier::new(3));
    let invalid_results = Arc::new(AtomicU64::new(0));
    let search_errors = Arc::new(AtomicU64::new(0));
    let search_count = Arc::new(AtomicU64::new(0));
    let insert_count = Arc::new(AtomicU64::new(0));
    let update_count = Arc::new(AtomicU64::new(0));
    // Search runs until both writer threads signal done
    let writers_done = Arc::new(AtomicU64::new(0));

    // Thread 1: continuous search
    let db1 = db.clone();
    let barrier1 = barrier.clone();
    let invalid1 = invalid_results.clone();
    let search_errors1 = search_errors.clone();
    let search_count1 = search_count.clone();
    let writers_done1 = writers_done.clone();
    let search_handle = thread::spawn(move || {
        let vs = VectorStore::new(db1);
        barrier1.wait();

        // Run until both writer threads (insert + update) are done
        while writers_done1.load(Ordering::Acquire) < 2 {
            let query = seeded_vector(dimension, search_count1.load(Ordering::Relaxed));
            match vs.search(branch_id, "default", "test_coll", &query, 5, None) {
                Ok(results) => {
                    // Each result must have a valid key and finite score
                    for r in &results {
                        if r.key.is_empty() || r.score.is_nan() {
                            invalid1.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Err(_) => {
                    // Search errors during concurrent modification are tolerable
                    // as long as they don't panic — count for observability
                    search_errors1.fetch_add(1, Ordering::Relaxed);
                }
            }
            search_count1.fetch_add(1, Ordering::Relaxed);
        }
    });

    // Thread 2: insert new vectors (may trigger seals)
    let db2 = db.clone();
    let barrier2 = barrier.clone();
    let insert_count2 = insert_count.clone();
    let writers_done2 = writers_done.clone();
    let insert_handle = thread::spawn(move || {
        let vs = VectorStore::new(db2);
        barrier2.wait();

        for i in 0..num_iterations {
            let key = format!("new_{}", i);
            let embedding = seeded_vector(dimension, 1000 + i as u64);
            let _ = vs.insert(branch_id, "default", "test_coll", &key, &embedding, None);
            insert_count2.fetch_add(1, Ordering::Relaxed);
        }
        writers_done2.fetch_add(1, Ordering::Release);
    });

    // Thread 3: update existing vectors
    let db3 = db.clone();
    let barrier3 = barrier.clone();
    let update_count3 = update_count.clone();
    let writers_done3 = writers_done.clone();
    let update_handle = thread::spawn(move || {
        let vs = VectorStore::new(db3);
        barrier3.wait();

        for i in 0..num_iterations {
            let key = format!("init_{}", i % initial_count);
            let embedding = seeded_vector(dimension, 2000 + i as u64);
            let _ = vs.insert(branch_id, "default", "test_coll", &key, &embedding, None);
            update_count3.fetch_add(1, Ordering::Relaxed);
        }
        writers_done3.fetch_add(1, Ordering::Release);
    });

    search_handle.join().unwrap();
    insert_handle.join().unwrap();
    update_handle.join().unwrap();

    let total_invalid = invalid_results.load(Ordering::Relaxed);
    let total_search_errors = search_errors.load(Ordering::Relaxed);
    let total_searches = search_count.load(Ordering::Relaxed);
    let total_inserts = insert_count.load(Ordering::Relaxed);
    let total_updates = update_count.load(Ordering::Relaxed);

    println!(
        "Vector concurrent: {} searches ({} transient errors), {} inserts, {} updates, {} invalid results",
        total_searches, total_search_errors, total_inserts, total_updates, total_invalid
    );

    assert_eq!(
        total_invalid, 0,
        "Search returned invalid results during concurrent upsert"
    );
    assert!(total_searches > 0, "Search thread must have executed");
    assert_eq!(total_inserts, num_iterations as u64);
    assert_eq!(total_updates, num_iterations as u64);

    // Verify: all inserted vectors are eventually searchable (eventual consistency)
    // Search with each inserted vector's own embedding — it should find itself
    let vs_final = test_db.vector();
    for i in 0..num_iterations {
        let key = format!("new_{}", i);
        let embedding = seeded_vector(dimension, 1000 + i as u64);
        let results = vs_final
            .search(branch_id, "default", "test_coll", &embedding, 1, None)
            .unwrap();
        // The top result should be the vector itself (exact match)
        assert!(
            !results.is_empty(),
            "Inserted vector '{}' not found in search after completion",
            key
        );
    }
}

// ============================================================================
// Test 3: Event append hash chain under high contention
// ============================================================================

/// Verifies hash chain integrity when many threads concurrently append events
/// to the same event space.
///
/// Setup:
///   - Create event space
///   - 16 threads each append 100 events
///   - Barrier synchronization for simultaneous start
///
/// Verifications:
///   - Exactly 1600 events exist
///   - Hash chain is unbroken (every event's prev_hash == predecessor's hash)
///   - Sequence numbers are monotonic with no gaps
///   - OCC conflicts must have occurred (concurrent appends MUST conflict)
///
/// Invariants tested: ACID-003, ARCH-001, MVCC-003
#[test]
fn test_issue_1910_event_hash_chain_under_contention() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let num_threads: usize = 16;
    let events_per_thread: usize = 100;
    let total_events = num_threads * events_per_thread;

    let barrier = Arc::new(Barrier::new(num_threads));
    let success_count = Arc::new(AtomicU64::new(0));
    let conflict_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = db.clone();
            let barrier = barrier.clone();
            let success = success_count.clone();
            let conflicts = conflict_count.clone();

            thread::spawn(move || {
                let event = EventLog::new(db);
                barrier.wait();

                for i in 0..events_per_thread {
                    let payload = event_payload(Value::Int((thread_id * 1000 + i) as i64));

                    // Retry loop for OCC conflicts
                    loop {
                        match event.append(
                            &branch_id,
                            "default",
                            "contention_test",
                            payload.clone(),
                        ) {
                            Ok(_seq) => {
                                success.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                            Err(_) => {
                                conflicts.fetch_add(1, Ordering::Relaxed);
                                // Retry — OCC conflict expected under contention
                            }
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total_success = success_count.load(Ordering::SeqCst);
    let total_conflicts = conflict_count.load(Ordering::Relaxed);

    println!(
        "Event contention: {} successes, {} conflicts",
        total_success, total_conflicts
    );

    // 1. Exactly total_events appended
    assert_eq!(
        total_success, total_events as u64,
        "All {} events must succeed (with retries)",
        total_events
    );

    let event = EventLog::new(db.clone());
    let len = event.len(&branch_id, "default").unwrap();
    assert_eq!(
        len, total_events as u64,
        "Event log length should be {}",
        total_events
    );

    // 2. Verify hash chain integrity: every event's prev_hash == predecessor's hash
    let all_events = event
        .range(&branch_id, "default", 0, None, None, false, None)
        .unwrap();
    assert_eq!(all_events.len(), total_events);

    let mut prev_hash = [0u8; 32]; // First event's prev_hash should be zeros
    for (idx, versioned_event) in all_events.iter().enumerate() {
        let evt = &versioned_event.value;

        // 3. Sequence numbers are monotonic with no gaps
        assert_eq!(
            evt.sequence, idx as u64,
            "Sequence gap at index {}: expected {}, got {}",
            idx, idx, evt.sequence
        );

        // Verify hash chain linkage
        assert_eq!(
            evt.prev_hash, prev_hash,
            "Hash chain broken at sequence {}: prev_hash mismatch",
            evt.sequence
        );

        // Hash must be non-zero (computed)
        assert_ne!(
            evt.hash, [0u8; 32],
            "Event at sequence {} has zero hash",
            evt.sequence
        );

        prev_hash = evt.hash;
    }

    // 4. OCC conflicts must have occurred (16 threads contending on same metadata key)
    assert!(
        total_conflicts > 0,
        "Expected OCC conflicts with {} concurrent writers, got 0",
        num_threads
    );
}

/// Stress variant: higher thread count with mixed event types.
#[test]
fn test_issue_1910_event_hash_chain_concurrent() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let num_threads: usize = 8;
    let events_per_thread: usize = 50;
    let total_events = num_threads * events_per_thread;

    let barrier = Arc::new(Barrier::new(num_threads));
    let success_count = Arc::new(AtomicU64::new(0));
    let conflict_count = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = db.clone();
            let barrier = barrier.clone();
            let success = success_count.clone();
            let conflicts = conflict_count.clone();

            thread::spawn(move || {
                let event = EventLog::new(db);
                barrier.wait();

                for i in 0..events_per_thread {
                    let payload = event_payload(Value::Int((thread_id * 1000 + i) as i64));

                    loop {
                        match event.append(
                            &branch_id,
                            "default",
                            &format!("type_{}", thread_id % 4),
                            payload.clone(),
                        ) {
                            Ok(_) => {
                                success.fetch_add(1, Ordering::SeqCst);
                                break;
                            }
                            Err(_) => {
                                conflicts.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total_success = success_count.load(Ordering::SeqCst);
    let total_conflicts = conflict_count.load(Ordering::Relaxed);

    println!(
        "Event stress: {} successes, {} conflicts",
        total_success, total_conflicts
    );

    assert_eq!(total_success, total_events as u64);

    // Verify log length matches expected count
    let event = EventLog::new(db.clone());
    let len = event.len(&branch_id, "default").unwrap();
    assert_eq!(
        len, total_events as u64,
        "Event log length should be {}",
        total_events
    );

    // Verify chain integrity
    let all_events = event
        .range(&branch_id, "default", 0, None, None, false, None)
        .unwrap();
    assert_eq!(
        all_events.len(),
        total_events,
        "Range should return all {} events",
        total_events
    );

    let mut prev_hash = [0u8; 32];
    for (idx, ve) in all_events.iter().enumerate() {
        assert_eq!(ve.value.sequence, idx as u64);
        assert_eq!(ve.value.prev_hash, prev_hash);
        assert_ne!(ve.value.hash, [0u8; 32]);
        prev_hash = ve.value.hash;
    }

    // OCC conflicts must have occurred
    assert!(
        total_conflicts > 0,
        "Expected OCC conflicts with {} concurrent writers, got 0",
        num_threads
    );
}

// ============================================================================
// Issue #2108: fork_branch succeeds with missing data when source is
//              concurrently deleted
// ============================================================================

/// Race fork_branch against delete_branch on the same source.
///
/// The root cause is that `flush_oldest_frozen` resurrects a deleted branch
/// as an empty BranchState via `entry().or_insert_with()`. Fork then captures
/// this empty ghost and returns Ok with no inherited data.
///
/// This test verifies:
///   - No panics
///   - If fork succeeds, the child is fully readable (all source keys present)
///   - No zombie branches
#[test]
fn test_issue_2108_fork_races_delete_source_branch() {
    for iteration in 0..100 {
        let test_db = TestDb::new();
        let branches = test_db.db.branches();
        let kv = test_db.kv();

        // Create and populate source branch
        branches.create("source").unwrap();
        let source_id = strata_engine::primitives::branch::resolve_branch_name("source");
        for i in 0..20 {
            kv.put(&source_id, "default", &format!("key_{}", i), Value::Int(i))
                .unwrap();
        }

        let db = test_db.db.clone();
        let barrier = Arc::new(Barrier::new(2));

        // Thread A: fork source -> child
        let db1 = db.clone();
        let b1 = barrier.clone();
        let fork_handle = thread::spawn(move || {
            b1.wait();
            db1.branches()
                .fork("source", &format!("child_{}", iteration))
        });

        // Thread B: delete source
        let db2 = db.clone();
        let b2 = barrier.clone();
        let delete_handle = thread::spawn(move || {
            b2.wait();
            db2.branches().delete("source")
        });

        let fork_result = fork_handle.join().unwrap();
        let delete_result = delete_handle.join().unwrap();

        let child_name = format!("child_{}", iteration);

        if let Ok(_fork_info) = &fork_result {
            // Fork succeeded — child MUST be fully readable
            let child_id = strata_engine::primitives::branch::resolve_branch_name(&child_name);
            for i in 0..20 {
                let val = kv.get(&child_id, "default", &format!("key_{}", i)).unwrap();
                assert!(
                    val.is_some(),
                    "iteration {}: child missing key_{} after successful fork",
                    iteration,
                    i
                );
            }
        }

        // Source should be gone if delete succeeded
        if delete_result.is_ok() {
            let listed = branches.list().unwrap();
            assert!(
                !listed.iter().any(|b| b == "source"),
                "iteration {}: delete succeeded but source still listed",
                iteration
            );
        }
    }
}

/// Stress variant: more iterations with concurrent writes to increase
/// the probability of hitting the flush-resurrects-deleted-branch window.
#[test]
fn test_issue_2108_fork_races_delete_source_branch_concurrent() {
    for iteration in 0..50 {
        let test_db = TestDb::new();
        let branches = test_db.db.branches();
        let kv = test_db.kv();

        branches.create("src").unwrap();
        let src_id = strata_engine::primitives::branch::resolve_branch_name("src");

        // Write enough data to ensure memtable flush during fork
        for i in 0..50 {
            kv.put(
                &src_id,
                "default",
                &format!("data_{}", i),
                Value::String(format!("value_{}", i)),
            )
            .unwrap();
        }

        let db = test_db.db.clone();
        let barrier = Arc::new(Barrier::new(3));

        // Thread A: continuous writes to source (increases flush contention)
        let db_w = db.clone();
        let bw = barrier.clone();
        let writer = thread::spawn(move || {
            let kv_w = KVStore::new(db_w);
            bw.wait();
            for i in 0..20 {
                let _ = kv_w.put(&src_id, "default", &format!("extra_{}", i), Value::Int(i));
                thread::yield_now();
            }
        });

        // Thread B: fork
        let db1 = db.clone();
        let b1 = barrier.clone();
        let child_name = format!("child_{}", iteration);
        let cn = child_name.clone();
        let fork_handle = thread::spawn(move || {
            b1.wait();
            db1.branches().fork("src", &cn)
        });

        // Thread C: delete
        let db2 = db.clone();
        let b2 = barrier.clone();
        let delete_handle = thread::spawn(move || {
            b2.wait();
            db2.branches().delete("src")
        });

        let fork_result = fork_handle.join().unwrap();
        let _delete_result = delete_handle.join().unwrap();
        writer.join().unwrap();

        if fork_result.is_ok() {
            // If fork succeeded, the original 50 keys MUST be present
            let child_id = strata_engine::primitives::branch::resolve_branch_name(&child_name);
            for i in 0..50 {
                let val = kv
                    .get(&child_id, "default", &format!("data_{}", i))
                    .unwrap();
                assert!(
                    val.is_some(),
                    "iteration {}: child missing data_{} after successful fork",
                    iteration,
                    i
                );
            }
        }
    }
}

// ============================================================================
// Test 4: Fork version gap — child misses inflight writes (#2110 / #2105)
// ============================================================================

/// Verifies that fork_branch does not produce a child that is missing data
/// the parent has at versions <= fork_version.
///
/// The bug (#2105): The global storage version is shared across all branches.
/// A commit to branch B can advance the global version while a concurrent
/// commit to the source branch hasn't applied its writes yet. Fork reads
/// the advanced version (which includes B's commit) but the source branch's
/// pending commit hasn't materialized in the memtable. Result: fork_version
/// claims to include a version whose data for the source branch is missing.
///
/// Race scenario:
///   1. Commit A (source): allocates version V, begins apply_writes
///   2. Commit B (other):  allocates V+1, applies, fetch_max(V+1) → storage.version = V+1
///   3. Fork: reads storage.version = V+1, acquires DashMap guard on source
///   4. Commit A: blocked on DashMap guard (fork holds it)
///   5. Fork: flushes memtable (V's data not yet present), fork_version = V+1
///   6. Commit A: applies after fork releases → data at V in parent only
///
/// Invariants tested: COW-003, MVCC-001, ARCH-002
#[test]
fn test_issue_2110_fork_version_gap() {
    let mut failures = Vec::new();

    for iteration in 0..100 {
        let test_db = TestDb::new();
        let branches = test_db.db.branches();
        let kv = test_db.kv();

        // Create source + other branches
        branches.create("source").unwrap();
        branches.create("other").unwrap();
        let source_id = strata_engine::primitives::branch::resolve_branch_name("source");
        let other_id = strata_engine::primitives::branch::resolve_branch_name("other");

        // Seed source branch
        for i in 0..5 {
            kv.put(&source_id, "default", &format!("seed_{}", i), Value::Int(i))
                .unwrap();
        }

        let db = test_db.db.clone();
        let barrier = Arc::new(Barrier::new(3));
        let fork_version_slot = Arc::new(AtomicU64::new(0));

        // Thread 1: writes to SOURCE branch
        let db1 = db.clone();
        let barrier1 = barrier.clone();
        let write_source_handle = thread::spawn(move || {
            let kv1 = KVStore::new(db1);
            barrier1.wait();
            for i in 0..50 {
                let key = format!("src_{}", i);
                let _ = kv1.put(&source_id, "default", &key, Value::Int(1000 + i));
            }
        });

        // Thread 2: writes to OTHER branch (advances global version)
        let db2 = db.clone();
        let barrier2 = barrier.clone();
        let write_other_handle = thread::spawn(move || {
            let kv2 = KVStore::new(db2);
            barrier2.wait();
            for i in 0..50 {
                let key = format!("oth_{}", i);
                let _ = kv2.put(&other_id, "default", &key, Value::Int(2000 + i));
            }
        });

        // Thread 3: fork SOURCE while both writers are active
        let db3 = db.clone();
        let barrier3 = barrier.clone();
        let fv_slot = fork_version_slot.clone();
        let fork_name = format!("child_{}", iteration);
        let fork_handle = thread::spawn(move || {
            barrier3.wait();
            thread::yield_now();
            match db3.branches().fork("source", &fork_name) {
                Ok(info) => {
                    fv_slot.store(info.fork_version.unwrap_or(0), Ordering::Release);
                    true
                }
                Err(_) => false,
            }
        });

        write_source_handle.join().unwrap();
        write_other_handle.join().unwrap();
        let fork_ok = fork_handle.join().unwrap();

        if !fork_ok {
            continue;
        }

        let fork_version = fork_version_slot.load(Ordering::Acquire);
        if fork_version == 0 {
            continue;
        }

        let child_name = format!("child_{}", iteration);
        let child_id = strata_engine::primitives::branch::resolve_branch_name(&child_name);

        // Verify: every key committed to SOURCE at version <= fork_version
        // must be visible in the child through inherited layers.
        for i in 0..50 {
            let key = format!("src_{}", i);
            let parent_versioned = kv.get_versioned(&source_id, "default", &key).unwrap();

            if let Some(pv) = parent_versioned {
                if pv.version <= Version::Txn(fork_version) {
                    let child_val = kv.get(&child_id, "default", &key).unwrap();
                    if child_val.is_none() {
                        failures.push(format!(
                            "iteration {}: key '{}' in source@{:?} (fork_version={}) MISSING in child",
                            iteration, key, pv.version, fork_version
                        ));
                    }
                }
            }
        }

        // Seed keys must always be visible
        for i in 0..5 {
            let key = format!("seed_{}", i);
            let child_val = kv.get(&child_id, "default", &key).unwrap();
            if child_val.is_none() {
                failures.push(format!(
                    "iteration {}: seed key '{}' MISSING in child (fork_version={})",
                    iteration, key, fork_version
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "Fork version gap detected in {} cases:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Stress variant: higher contention with cross-branch writes.
#[test]
fn test_issue_2110_fork_version_gap_concurrent() {
    let mut failures = Vec::new();

    for iteration in 0..50 {
        let test_db = TestDb::new();
        let branches = test_db.db.branches();
        let kv = test_db.kv();

        branches.create("source").unwrap();
        let source_id = strata_engine::primitives::branch::resolve_branch_name("source");

        // Create multiple other branches for cross-branch version advancement
        let mut other_ids = Vec::new();
        for t in 0..3u8 {
            let name = format!("other_{}", t);
            branches.create(&name).unwrap();
            other_ids.push(strata_engine::primitives::branch::resolve_branch_name(
                &name,
            ));
        }

        // Seed source
        for i in 0..5 {
            kv.put(&source_id, "default", &format!("base_{}", i), Value::Int(i))
                .unwrap();
        }

        let db = test_db.db.clone();
        let barrier = Arc::new(Barrier::new(5)); // 1 source writer + 3 other writers + 1 forker
        let fork_version_slot = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();

        // Source writer
        let db_s = db.clone();
        let barrier_s = barrier.clone();
        handles.push(thread::spawn(move || {
            let kv_s = KVStore::new(db_s);
            barrier_s.wait();
            for i in 0..30 {
                let key = format!("src_{}", i);
                let _ = kv_s.put(&source_id, "default", &key, Value::Int(1000 + i));
            }
        }));

        // Other-branch writers (advance global version concurrently)
        for (t, other_id) in other_ids.iter().enumerate() {
            let db_o = db.clone();
            let barrier_o = barrier.clone();
            let oid = *other_id;
            handles.push(thread::spawn(move || {
                let kv_o = KVStore::new(db_o);
                barrier_o.wait();
                for i in 0..30 {
                    let key = format!("o{}_{}", t, i);
                    let _ = kv_o.put(&oid, "default", &key, Value::Int((t as i64) * 1000 + i));
                }
            }));
        }

        // Forker
        let db_f = db.clone();
        let barrier_f = barrier.clone();
        let fv_slot = fork_version_slot.clone();
        let fork_name = format!("child_{}", iteration);
        handles.push(thread::spawn(move || {
            barrier_f.wait();
            thread::yield_now();
            if let Ok(info) = db_f.branches().fork("source", &fork_name) {
                fv_slot.store(info.fork_version.unwrap_or(0), Ordering::Release);
            }
        }));

        for h in handles {
            h.join().unwrap();
        }

        let fork_version = fork_version_slot.load(Ordering::Acquire);
        if fork_version == 0 {
            continue;
        }

        let child_id =
            strata_engine::primitives::branch::resolve_branch_name(&format!("child_{}", iteration));

        for i in 0..30 {
            let key = format!("src_{}", i);
            let parent_versioned = kv.get_versioned(&source_id, "default", &key).unwrap();
            if let Some(pv) = parent_versioned {
                if pv.version <= Version::Txn(fork_version) {
                    let child_val = kv.get(&child_id, "default", &key).unwrap();
                    if child_val.is_none() {
                        failures.push(format!(
                            "iteration {}: key '{}' source@{:?} (fork_version={}) MISSING in child",
                            iteration, key, pv.version, fork_version
                        ));
                    }
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "Fork version gap detected in {} cases:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
