//! Transaction Performance Benchmarks
//!
//! Measures transaction throughput for validation.
//! Targets:
//! - Single-threaded: >5K txns/sec
//! - Multi-threaded (no conflict): >10K txns/sec
//! - Multi-threaded (with conflict): >2K txns/sec

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use std::thread;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_engine::Database;
use tempfile::TempDir;

fn create_ns(branch_id: BranchId) -> Arc<Namespace> {
    Arc::new(Namespace::new(branch_id, "default".to_string()))
}

/// Benchmark: Single-threaded transactions (no contention)
fn bench_single_threaded_transactions(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    let mut group = c.benchmark_group("single_threaded");
    group.throughput(Throughput::Elements(1));

    group.bench_function("transaction_put", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Key::new_kv(ns.clone(), format!("key_{}", i));
            let result = db.transaction(branch_id, |txn| {
                txn.put(key.clone(), Value::Int(i as i64))?;
                Ok(())
            });
            black_box(result.unwrap());
            i += 1;
        });
    });

    group.bench_function("transaction_get_put", |b| {
        let key = Key::new_kv(ns.clone(), "get_put_key");
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(0))?;
            Ok(())
        })
        .unwrap();

        b.iter(|| {
            let result = db.transaction(branch_id, |txn| {
                let val = txn.get(&key)?;
                let new_val = match val {
                    Some(Value::Int(n)) => n + 1,
                    _ => 1,
                };
                txn.put(key.clone(), Value::Int(new_val))?;
                Ok(())
            });
            black_box(result.unwrap());
        });
    });

    group.finish();
}

/// Benchmark: Multi-threaded transactions (no conflicts - different keys)
fn bench_multi_threaded_no_conflict(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_threaded_no_conflict");

    for num_threads in [2, 4, 8] {
        group.throughput(Throughput::Elements(num_threads as u64));

        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let temp_dir = TempDir::new().unwrap();
                    let db = Database::open(temp_dir.path().join("db")).unwrap();
                    let branch_id = BranchId::new();

                    let start = std::time::Instant::now();

                    let handles: Vec<_> = (0..num_threads)
                        .map(|thread_id| {
                            let db = Arc::clone(&db);
                            let ns = create_ns(branch_id);

                            thread::spawn(move || {
                                for i in 0..iters {
                                    // Each thread writes to its own key prefix
                                    let key =
                                        Key::new_kv(ns.clone(), format!("t{}_{}", thread_id, i));
                                    db.transaction(branch_id, |txn| {
                                        txn.put(key.clone(), Value::Int(i as i64))?;
                                        Ok(())
                                    })
                                    .unwrap();
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }

                    start.elapsed()
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Multi-threaded transactions (with conflicts - same keys)
fn bench_multi_threaded_with_conflict(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_threaded_with_conflict");

    for num_threads in [2, 4] {
        group.throughput(Throughput::Elements(num_threads as u64));

        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            &num_threads,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let temp_dir = TempDir::new().unwrap();
                    let db = Database::open(temp_dir.path().join("db")).unwrap();
                    let branch_id = BranchId::new();
                    let ns = create_ns(branch_id);
                    let key = Key::new_kv(ns, "contested_key");

                    // Pre-populate
                    db.transaction(branch_id, |txn| {
                        txn.put(key.clone(), Value::Int(0))?;
                        Ok(())
                    })
                    .unwrap();

                    let start = std::time::Instant::now();

                    let handles: Vec<_> = (0..num_threads)
                        .map(|_| {
                            let db = Arc::clone(&db);
                            let key = key.clone();

                            thread::spawn(move || {
                                for _ in 0..iters {
                                    // All threads contend on same key
                                    let _ = db.transaction(branch_id, |txn| {
                                        let val = txn.get(&key)?;
                                        let new_val = match val {
                                            Some(Value::Int(n)) => n + 1,
                                            _ => 1,
                                        };
                                        txn.put(key.clone(), Value::Int(new_val))?;
                                        Ok(())
                                    });
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }

                    start.elapsed()
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Read-only transactions
fn bench_read_only_transactions(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    // Pre-populate
    for i in 0..1000 {
        let key = Key::new_kv(ns.clone(), format!("key_{}", i));
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(i as i64))?;
            Ok(())
        })
        .unwrap();
    }

    let mut group = c.benchmark_group("read_only");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_read", |b| {
        let key = Key::new_kv(ns.clone(), "key_500");
        b.iter(|| {
            let result = db.transaction(branch_id, |txn| txn.get(&key));
            black_box(result.unwrap());
        });
    });

    group.bench_function("multi_read_10", |b| {
        let keys: Vec<_> = (0..10)
            .map(|i| Key::new_kv(ns.clone(), format!("key_{}", i * 100)))
            .collect();

        b.iter(|| {
            let result = db.transaction(branch_id, |txn| {
                for key in &keys {
                    txn.get(key)?;
                }
                Ok(())
            });
            black_box(result.unwrap());
        });
    });

    group.finish();
}

/// Benchmark: Direct put/get (Legacy API)
fn bench_direct_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_ns(branch_id);

    let mut group = c.benchmark_group("direct_operations");
    group.throughput(Throughput::Elements(1));

    group.bench_function("direct_put", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = Key::new_kv(ns.clone(), format!("direct_key_{}", i));
            let result = db.transaction(branch_id, |txn| {
                txn.put(key.clone(), Value::Int(i as i64))?;
                Ok(())
            });
            black_box(result.unwrap());
            i += 1;
        });
    });

    // Pre-populate for get benchmark
    let get_key = Key::new_kv(ns.clone(), "get_key");
    db.transaction(branch_id, |txn| {
        txn.put(get_key.clone(), Value::Int(42))?;
        Ok(())
    })
    .unwrap();

    group.bench_function("direct_get", |b| {
        b.iter(|| {
            let result = db.transaction(branch_id, |txn| txn.get(&get_key));
            black_box(result.unwrap());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_threaded_transactions,
    bench_multi_threaded_no_conflict,
    bench_multi_threaded_with_conflict,
    bench_read_only_transactions,
    bench_direct_operations,
);

criterion_main!(benches);
