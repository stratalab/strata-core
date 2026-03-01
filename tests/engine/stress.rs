//! Stress Tests
//!
//! Heavy workload tests for the engine. All marked #[ignore] for opt-in execution.
//! Run with: cargo test --test engine stress -- --ignored

use crate::common::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use strata_engine::{EventLogExt, KVStoreExt};

/// Helper to create an event payload object
fn event_payload(data: Value) -> Value {
    Value::object(HashMap::from([("data".to_string(), data)]))
}

/// High concurrency KV workload
#[test]
#[ignore]
fn stress_concurrent_kv_operations() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let barrier = Arc::new(Barrier::new(8));
    let ops = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..8)
        .map(|thread_id| {
            let db = db.clone();
            let barrier = barrier.clone();
            let ops = ops.clone();

            thread::spawn(move || {
                let kv = KVStore::new(db);
                barrier.wait();

                for i in 0..1000 {
                    let key = format!("thread_{}_key_{}", thread_id, i);
                    kv.put(&branch_id, "default", &key, Value::Int(i)).unwrap();
                    let _ = kv.get(&branch_id, "default", &key);
                    ops.fetch_add(2, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total = ops.load(Ordering::Relaxed);
    println!("Total KV operations: {}", total);
    assert_eq!(total, 8 * 1000 * 2); // 8 threads * 1000 iterations * 2 ops
}

/// Sustained transaction throughput
#[test]
#[ignore]
fn stress_transaction_throughput() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();

    kv.put(&branch_id, "default", "counter", Value::Int(0))
        .unwrap();

    let duration = Duration::from_secs(5);
    let start = Instant::now();
    let mut commits = 0u64;

    while start.elapsed() < duration {
        let result = test_db.db.transaction(branch_id, |txn| {
            let val = txn.kv_get("counter")?.unwrap();
            if let Value::Int(n) = val {
                txn.kv_put("counter", Value::Int(n + 1))?;
            }
            Ok(())
        });

        if result.is_ok() {
            commits += 1;
        }
    }

    let elapsed = start.elapsed();
    let tps = commits as f64 / elapsed.as_secs_f64();

    println!("Commits: {}, TPS: {:.0}", commits, tps);
    assert!(commits > 0);
}

/// Large batch operations
#[test]
#[ignore]
fn stress_large_batch_kv() {
    let test_db = TestDb::new();
    let branch_id = test_db.branch_id;
    let kv = test_db.kv();

    let start = Instant::now();

    // Write 10K keys
    for i in 0..10_000 {
        kv.put(&branch_id, "default", &format!("key_{}", i), Value::Int(i))
            .unwrap();
    }

    let write_time = start.elapsed();

    // Read all keys
    let read_start = Instant::now();
    for i in 0..10_000 {
        let _ = kv.get(&branch_id, "default", &format!("key_{}", i));
    }
    let read_time = read_start.elapsed();

    println!("10K writes: {:?}, 10K reads: {:?}", write_time, read_time);

    // List all keys
    let list_start = Instant::now();
    let keys = kv.list(&branch_id, "default", None).unwrap();
    let list_time = list_start.elapsed();

    println!("List {} keys: {:?}", keys.len(), list_time);
    assert_eq!(keys.len(), 10_000);
}

/// Many concurrent branches
#[test]
#[ignore]
fn stress_many_concurrent_branches() {
    let test_db = TestDb::new_in_memory();
    let db = test_db.db.clone();

    let barrier = Arc::new(Barrier::new(50));
    let success = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..50)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            let success = success.clone();

            thread::spawn(move || {
                let branch_id = BranchId::new(); // Each thread has its own branch
                let kv = KVStore::new(db.clone());

                barrier.wait();

                // Each branch does independent work
                for i in 0..100 {
                    kv.put(&branch_id, "default", &format!("key_{}", i), Value::Int(i))
                        .unwrap();
                }

                // Verify
                for i in 0..100 {
                    let val = kv
                        .get(&branch_id, "default", &format!("key_{}", i))
                        .unwrap();
                    if val.is_some() && val.unwrap() == Value::Int(i) {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total = success.load(Ordering::Relaxed);
    println!("Successful verifications: {}", total);
    assert_eq!(total, 50 * 100); // All should succeed (no cross-branch interference)
}

/// Cross-primitive stress
#[test]
#[ignore]
fn stress_cross_primitive_transactions() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    let barrier = Arc::new(Barrier::new(4));
    let commits = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let db = db.clone();
            let barrier = barrier.clone();
            let commits = commits.clone();

            thread::spawn(move || {
                barrier.wait();

                for i in 0..100 {
                    // Retry with exponential backoff to recover from contention
                    let mut committed = false;
                    for attempt in 0..10 {
                        let result = db.transaction(branch_id, |txn| {
                            let key = format!("thread_{}_iter_{}", thread_id, i);

                            // KV + Event in same transaction
                            txn.kv_put(&key, Value::Int(i))?;
                            txn.event_append("stress", event_payload(Value::String(key)))?;

                            Ok(())
                        });

                        if result.is_ok() {
                            committed = true;
                            break;
                        }
                        // Exponential backoff: 1ms, 2ms, 4ms, ...
                        std::thread::sleep(std::time::Duration::from_millis(1 << attempt));
                    }
                    if committed {
                        commits.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let total = commits.load(Ordering::Relaxed);
    println!("Cross-primitive commits: {}", total);
    // All 4 threads write to the shared "stress" event stream, causing contention.
    // With 10 retries and exponential backoff, most transactions should eventually commit.
    assert!(total > 200, "At least 50% should commit");
}

/// Vector search stress
#[test]
#[ignore]
fn stress_vector_search() {
    let test_db = TestDb::new_in_memory();
    let vector = test_db.vector();
    let branch_id = test_db.branch_id;

    let config = config_standard(); // 384 dimensions
    vector
        .create_collection(branch_id, "default", "stress_coll", config)
        .unwrap();

    // Insert 1000 vectors
    let insert_start = Instant::now();
    for i in 0..1000 {
        let v = seeded_vector(384, i as u64);
        vector
            .insert(
                branch_id,
                "default",
                "stress_coll",
                &format!("vec_{}", i),
                &v,
                None,
            )
            .unwrap();
    }
    let insert_time = insert_start.elapsed();

    println!("Inserted 1000 384-dim vectors in {:?}", insert_time);

    // Perform 100 searches
    let search_start = Instant::now();
    for i in 0..100 {
        let query = seeded_vector(384, i * 10);
        let _ = vector
            .search(branch_id, "default", "stress_coll", &query, 10, None)
            .unwrap();
    }
    let search_time = search_start.elapsed();

    println!(
        "100 searches in {:?} ({:.2} ms/search)",
        search_time,
        search_time.as_millis() as f64 / 100.0
    );
}

/// EventLog append stress
#[test]
#[ignore]
fn stress_eventlog_append() {
    let test_db = TestDb::new_in_memory();
    let event = test_db.event();
    let branch_id = test_db.branch_id;

    let start = Instant::now();

    // Append 10K events
    for i in 0..10_000 {
        event
            .append(
                &branch_id,
                "default",
                "stress_event",
                event_payload(Value::Int(i)),
            )
            .unwrap();
    }

    let elapsed = start.elapsed();

    println!(
        "10K event appends in {:?} ({:.0} events/sec)",
        elapsed,
        10_000.0 / elapsed.as_secs_f64()
    );

    assert_eq!(event.len(&branch_id, "default").unwrap(), 10_000);

    // Verify all events are readable
    let verify_start = Instant::now();
    for i in 0..10_000u64 {
        let ev = event.get(&branch_id, "default", i).unwrap();
        assert!(ev.is_some(), "Event at sequence {} should exist", i);
    }
    let verify_time = verify_start.elapsed();

    println!("Event read verification in {:?}", verify_time);
}
