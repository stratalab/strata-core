//! Primitive Performance Benchmarks
//!
//! Performance targets from architecture documentation:
//! - KV put: >10K ops/sec
//! - KV get: >20K ops/sec
//! - EventLog append: >5K ops/sec
//! - Cross-primitive txn: >1K ops/sec

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use strata_core::types::BranchId;
use strata_core::value::Value;
use strata_engine::Database;
use strata_engine::{EventLog, EventLogExt, KVStore, KVStoreExt};
use tempfile::TempDir;

fn setup_db() -> (Arc<Database>, TempDir, BranchId) {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();
    let branch_id = BranchId::new();
    (db, temp_dir, branch_id)
}

/// Benchmark KV put operations
/// Target: >10K ops/sec
fn bench_kv_put(c: &mut Criterion) {
    let (db, _temp, branch_id) = setup_db();
    let kv = KVStore::new(db.clone());

    let mut group = c.benchmark_group("kv");
    group.throughput(Throughput::Elements(1));

    let counter = AtomicU64::new(0);
    group.bench_function("put", |b| {
        b.iter(|| {
            let i = counter.fetch_add(1, Ordering::SeqCst);
            kv.put(
                &branch_id,
                "default",
                &format!("key{}", i),
                Value::Int(i as i64),
            )
            .unwrap()
        })
    });
    group.finish();
}

/// Benchmark KV get operations
/// Target: >20K ops/sec
fn bench_kv_get(c: &mut Criterion) {
    let (db, _temp, branch_id) = setup_db();
    let kv = KVStore::new(db.clone());

    // Pre-populate 1000 keys
    for i in 0..1000 {
        kv.put(
            &branch_id,
            "default",
            &format!("key{}", i),
            Value::Int(i as i64),
        )
        .unwrap();
    }

    let mut group = c.benchmark_group("kv");
    group.throughput(Throughput::Elements(1));

    let counter = AtomicU64::new(0);
    group.bench_function("get", |b| {
        b.iter(|| {
            let i = counter.fetch_add(1, Ordering::SeqCst) % 1000;
            kv.get(&branch_id, "default", &format!("key{}", i)).unwrap()
        })
    });
    group.finish();
}

/// Benchmark EventLog append operations
/// Target: >5K ops/sec
fn bench_event_append(c: &mut Criterion) {
    let (db, _temp, branch_id) = setup_db();
    let event_log = EventLog::new(db.clone());

    let mut group = c.benchmark_group("event_log");
    group.throughput(Throughput::Elements(1));

    group.bench_function("append", |b| {
        b.iter(|| {
            event_log
                .append(&branch_id, "default", "benchmark_event", Value::Int(42))
                .unwrap()
        })
    });
    group.finish();
}

/// Benchmark cross-primitive transactions
/// Target: >1K ops/sec
fn bench_cross_primitive_transaction(c: &mut Criterion) {
    let (db, _temp, branch_id) = setup_db();

    let mut group = c.benchmark_group("cross_primitive");
    group.throughput(Throughput::Elements(1));

    let counter = AtomicU64::new(0);
    group.bench_function("2_primitive_txn", |b| {
        b.iter(|| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            db.transaction(branch_id, |txn| {
                txn.kv_put(&format!("txn_key{}", n), Value::Int(n as i64))?;
                txn.event_append("txn_event", Value::Int(n as i64))?;
                Ok(())
            })
            .unwrap()
        })
    });
    group.finish();
}

/// Benchmark EventLog read operations
fn bench_event_get(c: &mut Criterion) {
    let (db, _temp, branch_id) = setup_db();
    let event_log = EventLog::new(db.clone());

    // Pre-populate 1000 events
    for i in 0..1000 {
        event_log
            .append(&branch_id, "default", "numbered", Value::Int(i as i64))
            .unwrap();
    }

    let mut group = c.benchmark_group("event_log");
    group.throughput(Throughput::Elements(1));

    let counter = AtomicU64::new(0);
    group.bench_function("read", |b| {
        b.iter(|| {
            let i = counter.fetch_add(1, Ordering::SeqCst) % 1000;
            event_log.get(&branch_id, "default", i).unwrap()
        })
    });
    group.finish();
}

/// Benchmark KV list operations
fn bench_kv_list(c: &mut Criterion) {
    let (db, _temp, branch_id) = setup_db();
    let kv = KVStore::new(db.clone());

    // Pre-populate keys with prefix
    for i in 0..100 {
        kv.put(
            &branch_id,
            "default",
            &format!("prefix/key{}", i),
            Value::Int(i as i64),
        )
        .unwrap();
    }
    for i in 0..100 {
        kv.put(
            &branch_id,
            "default",
            &format!("other/key{}", i),
            Value::Int(i as i64),
        )
        .unwrap();
    }

    let mut group = c.benchmark_group("kv");
    group.throughput(Throughput::Elements(1));

    group.bench_function("list", |b| {
        b.iter(|| kv.list(&branch_id, "default", Some("prefix/")).unwrap())
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_kv_put,
    bench_kv_get,
    bench_kv_list,
    bench_event_append,
    bench_event_get,
    bench_cross_primitive_transaction,
);
criterion_main!(benches);
