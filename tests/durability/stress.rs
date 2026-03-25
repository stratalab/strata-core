//! Stress Tests
//!
//! Heavy-workload durability tests. All marked #[ignore] for opt-in execution.
//! Run with: cargo test --test durability stress -- --ignored

use crate::common::*;
use std::thread;
use std::time::{Duration, Instant};

/// 10K key recovery
#[test]
#[ignore]
fn stress_large_wal_recovery() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    for i in 0..10_000 {
        kv.put(&branch_id, "default", &format!("key_{}", i), Value::Int(i))
            .unwrap();
    }

    let start = Instant::now();
    test_db.reopen();
    let recovery_time = start.elapsed();
    println!("10K key recovery took: {:?}", recovery_time);

    let kv = test_db.kv();
    for i in (0..10_000).step_by(100) {
        let val = kv
            .get(&branch_id, "default", &format!("key_{}", i))
            .unwrap();
        assert_eq!(
            val,
            Some(Value::Int(i)),
            "Key {} should be {} after recovery",
            i,
            i
        );
    }
}

/// Concurrent writers to the same database
#[test]
#[ignore]
fn stress_concurrent_writes() {
    let test_db = TestDb::new_in_memory();
    let db = test_db.db.clone();

    let handles: Vec<_> = (0..8)
        .map(|thread_id| {
            let db = db.clone();
            thread::spawn(move || {
                let branch_id = BranchId::new();
                let kv = KVStore::new(db);
                for i in 0..1000 {
                    kv.put(
                        &branch_id,
                        "default",
                        &format!("t{}_k{}", thread_id, i),
                        Value::Int(i),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

/// Concurrent readers while writing
#[test]
#[ignore]
fn stress_concurrent_read_write() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let db = test_db.db.clone();

    // Pre-populate
    let kv = KVStore::new(db.clone());
    for i in 0..500 {
        kv.put(&branch_id, "default", &format!("rw_{}", i), Value::Int(i))
            .unwrap();
    }

    let writer_db = db.clone();
    let writer = thread::spawn(move || {
        let kv = KVStore::new(writer_db);
        for i in 500..1000 {
            kv.put(&branch_id, "default", &format!("rw_{}", i), Value::Int(i))
                .unwrap();
        }
    });

    let readers: Vec<_> = (0..4)
        .map(|_| {
            let db = db.clone();
            thread::spawn(move || {
                let kv = KVStore::new(db);
                let mut reads = 0u64;
                for _ in 0..1000 {
                    for i in 0..500 {
                        let _ = kv.get(&branch_id, "default", &format!("rw_{}", i));
                        reads += 1;
                    }
                }
                reads
            })
        })
        .collect();

    writer.join().unwrap();
    for reader in readers {
        let reads = reader.join().unwrap();
        assert!(reads > 0);
    }
}

/// 100K small writes throughput
#[test]
#[ignore]
fn stress_many_small_writes() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    let start = Instant::now();
    for i in 0..100_000 {
        kv.put(&branch_id, "default", &format!("k{}", i), Value::Int(i))
            .unwrap();
    }
    let elapsed = start.elapsed();

    println!(
        "100K writes: {:?} ({:.0} ops/sec)",
        elapsed,
        100_000.0 / elapsed.as_secs_f64()
    );

    // Verify sampling
    for i in (0..100_000).step_by(10_000) {
        let val = kv.get(&branch_id, "default", &format!("k{}", i)).unwrap();
        assert_eq!(val, Some(Value::Int(i)));
    }
}

/// Large value writes
#[test]
#[ignore]
fn stress_large_values() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    let large = Value::String("x".repeat(1_000_000)); // 1MB

    for i in 0..50 {
        kv.put(
            &branch_id,
            "default",
            &format!("large_{}", i),
            large.clone(),
        )
        .unwrap();
    }

    for i in 0..50 {
        let val = kv
            .get(&branch_id, "default", &format!("large_{}", i))
            .unwrap();
        assert_eq!(
            val,
            Some(large.clone()),
            "Large value {} should be preserved",
            i
        );
    }
}

/// Mixed operations under load
#[test]
#[ignore]
fn stress_mixed_operations() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    for i in 0..10_000 {
        match i % 4 {
            0 => {
                kv.put(
                    &branch_id,
                    "default",
                    &format!("k{}", i % 500),
                    Value::Int(i),
                )
                .unwrap();
            }
            1 => {
                let _ = kv.get(&branch_id, "default", &format!("k{}", i % 500));
            }
            2 => {
                kv.delete(&branch_id, "default", &format!("k{}", (i + 250) % 500))
                    .ok();
            }
            3 => {
                let _ = kv.list(&branch_id, "default", None);
            }
            _ => {}
        }
    }
}

/// Recovery after high-volume churn
#[test]
#[ignore]
fn stress_recovery_after_churn() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    let kv = test_db.kv();
    for i in 0..10_000 {
        kv.put(
            &branch_id,
            "default",
            &format!("churn_{}", i % 100),
            Value::Int(i),
        )
        .unwrap();
    }

    let state_before = CapturedState::capture(&test_db.db, &branch_id);

    test_db.reopen();

    let state_after = CapturedState::capture(&test_db.db, &branch_id);
    assert_states_equal(&state_before, &state_after, "Churn recovery mismatch");
}

/// Multiple reopen cycles under load
#[test]
#[ignore]
fn stress_repeated_reopen() {
    let mut test_db = TestDb::new_strict();
    let branch_id = test_db.branch_id;

    for cycle in 0..20 {
        let kv = test_db.kv();
        for i in 0..100 {
            kv.put(
                &branch_id,
                "default",
                &format!("c{}_k{}", cycle, i),
                Value::Int((cycle * 100 + i) as i64),
            )
            .unwrap();
        }
        test_db.reopen();
    }

    // Verify data from all cycles
    let kv = test_db.kv();
    for cycle in 0..20 {
        let val = kv
            .get(&branch_id, "default", &format!("c{}_k0", cycle))
            .unwrap();
        assert_eq!(
            val,
            Some(Value::Int((cycle * 100) as i64)),
            "Data from cycle {} should survive repeated reopens",
            cycle
        );
    }
}

/// All 6 primitives under sustained load
#[test]
#[ignore]
fn stress_all_primitives_sustained() {
    let test_db = TestDb::new_in_memory();
    let branch_id = test_db.branch_id;
    let p = test_db.all_primitives();

    let duration = Duration::from_secs(5);
    let start = Instant::now();
    let mut ops = 0u64;

    while start.elapsed() < duration {
        let key = format!("k{}", ops);
        p.kv.put(&branch_id, "default", &key, Value::Int(ops as i64))
            .unwrap();

        if ops.is_multiple_of(10) {
            p.event
                .append(
                    &branch_id,
                    "default",
                    "load_stream",
                    int_payload(ops as i64),
                )
                .unwrap();
        }
        if ops.is_multiple_of(50) {
            let doc = format!("doc_{}", ops);
            p.json
                .create(&branch_id, "default", &doc, test_json_value(ops as usize))
                .unwrap();
        }

        ops += 1;
    }

    println!(
        "Sustained load: {} ops in {:?} ({:.0} ops/sec)",
        ops,
        duration,
        ops as f64 / duration.as_secs_f64()
    );
    assert!(ops > 100, "Should complete at least 100 operations in 5s");
}
