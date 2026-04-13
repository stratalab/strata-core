//! Stress Tests
//!
//! Heavy-workload tests for the storage layer. All marked #[ignore] for opt-in execution.
//! Run with: cargo test --test storage stress -- --ignored

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use strata_core::id::CommitVersion;
use strata_core::traits::{Storage, WriteMode};
use strata_core::types::{Key, Namespace};
use strata_core::value::Value;
use strata_core::BranchId;
use strata_storage::SegmentedStore;

fn create_test_key(branch_id: BranchId, name: &str) -> Key {
    let ns = Arc::new(Namespace::for_branch(branch_id));
    Key::new_kv(ns, name)
}

/// High-concurrency writers and readers
#[test]
#[ignore]
fn stress_concurrent_writers_readers() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(AtomicU64::new(0));

    // Pre-populate
    for i in 0..100 {
        let key = create_test_key(branch_id, &format!("key_{}", i));
        let version = CommitVersion(store.next_version());
        store
            .put_with_version_mode(key, Value::Int(i), version, None, WriteMode::Append)
            .unwrap();
    }

    // Spawn 4 writers
    let writer_handles: Vec<_> = (0..4)
        .map(|t| {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            let writes = Arc::clone(&writes);
            thread::spawn(move || {
                let mut counter = 0i64;
                while !stop.load(Ordering::Relaxed) {
                    for i in 0..100 {
                        let key = create_test_key(branch_id, &format!("key_{}", i));
                        let version = CommitVersion(store.next_version());
                        let _ = store.put_with_version_mode(
                            key,
                            Value::Int(t * 10000 + counter),
                            version,
                            None,
                            WriteMode::Append,
                        );
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                    counter += 1;
                    if counter > 500 {
                        break;
                    }
                }
            })
        })
        .collect();

    // Spawn 8 readers
    let reader_handles: Vec<_> = (0..8)
        .map(|_| {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            let reads = Arc::clone(&reads);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for i in 0..100 {
                        let key = create_test_key(branch_id, &format!("key_{}", i));
                        let _ = store.get_versioned(&key, CommitVersion::MAX);
                        reads.fetch_add(1, Ordering::Relaxed);
                    }
                    if reads.load(Ordering::Relaxed) > 500_000 {
                        break;
                    }
                }
            })
        })
        .collect();

    // Wait for writers
    for h in writer_handles {
        h.join().unwrap();
    }

    stop.store(true, Ordering::Relaxed);

    // Wait for readers
    for h in reader_handles {
        h.join().unwrap();
    }

    println!(
        "Writes: {}, Reads: {}",
        writes.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed)
    );
}

/// Rapid snapshot creation
#[test]
#[ignore]
fn stress_rapid_snapshot_creation() {
    let store = Arc::new(SegmentedStore::new());
    let branch_id = BranchId::new();

    // Populate
    for i in 0..1000 {
        let key = create_test_key(branch_id, &format!("key_{}", i));
        let version = CommitVersion(store.next_version());
        store
            .put_with_version_mode(key, Value::Int(i), version, None, WriteMode::Append)
            .unwrap();
    }

    let start = Instant::now();
    let mut versions = Vec::new();

    for _ in 0..100_000 {
        versions.push(store.version());
    }

    let elapsed = start.elapsed();
    println!(
        "100K version captures in {:?} ({:.0} captures/sec)",
        elapsed,
        100_000.0 / elapsed.as_secs_f64()
    );

    // Verify versioned reads work
    for version in versions.iter().take(100) {
        let key = create_test_key(branch_id, "key_0");
        let result = store.get_versioned(&key, CommitVersion(*version)).unwrap();
        assert_eq!(result.unwrap().value, Value::Int(0));
    }
}

/// Deep version chain growth
#[test]
#[ignore]
fn stress_version_chain_growth() {
    let store = SegmentedStore::new();
    let branch_id = BranchId::new();
    let key = create_test_key(branch_id, "deep_chain");

    let start = Instant::now();

    // Create 100K versions of the same key
    for i in 0..100_000i64 {
        store
            .put_with_version_mode(
                key.clone(),
                Value::Int(i),
                CommitVersion((i + 1) as u64),
                None,
                WriteMode::Append,
            )
            .unwrap();
    }

    let write_elapsed = start.elapsed();

    // Read latest
    let read_start = Instant::now();
    for _ in 0..10_000 {
        let _ = store.get_versioned(&key, CommitVersion::MAX);
    }
    let read_elapsed = read_start.elapsed();

    // Get history
    let history = Storage::get_history(&store, &key, Some(100), None).unwrap();
    assert_eq!(history.len(), 100);

    println!(
        "100K writes: {:?}, 10K reads: {:?}",
        write_elapsed, read_elapsed
    );
}

/// TTL expiration under load
#[test]
#[ignore]
fn stress_ttl_expiration_cleanup() {
    let store = SegmentedStore::new();
    let branch_id = BranchId::new();

    // Insert 10K keys with short TTL
    let ttl = Some(Duration::from_millis(100));
    for i in 0..10_000 {
        let key = create_test_key(branch_id, &format!("ttl_key_{}", i));
        store
            .put_with_version_mode(
                key,
                Value::Int(i),
                CommitVersion((i + 1) as u64),
                ttl,
                WriteMode::Append,
            )
            .unwrap();
    }

    // Wait for expiration
    thread::sleep(Duration::from_millis(200));

    // All should be expired
    let mut expired_count = 0;
    for i in 0..10_000 {
        let key = create_test_key(branch_id, &format!("ttl_key_{}", i));
        if store
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .is_none()
        {
            expired_count += 1;
        }
    }

    println!("Expired: {} / 10000", expired_count);
    assert_eq!(expired_count, 10_000, "All keys should be expired");
}

/// Many branches concurrent access
#[test]
#[ignore]
fn stress_many_branches_concurrent() {
    let store = Arc::new(SegmentedStore::new());
    let num_branches = 100;
    let keys_per_branch = 100;

    let start = Instant::now();

    let handles: Vec<_> = (0..num_branches)
        .map(|_| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let branch_id = BranchId::new();
                for i in 0..keys_per_branch {
                    let key = create_test_key(branch_id, &format!("key_{}", i));
                    let version = CommitVersion(store.next_version());
                    store
                        .put_with_version_mode(key, Value::Int(i), version, None, WriteMode::Append)
                        .unwrap();
                }
                branch_id
            })
        })
        .collect();

    let branch_ids: Vec<BranchId> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    let write_elapsed = start.elapsed();

    // Verify all data
    let verify_start = Instant::now();
    for branch_id in branch_ids {
        for i in 0..keys_per_branch {
            let key = create_test_key(branch_id, &format!("key_{}", i));
            let val = store.get_versioned(&key, CommitVersion::MAX).unwrap();
            assert_eq!(val.unwrap().value, Value::Int(i));
        }
    }
    let verify_elapsed = verify_start.elapsed();

    println!(
        "{} branches x {} keys: write {:?}, verify {:?}",
        num_branches, keys_per_branch, write_elapsed, verify_elapsed
    );
}

/// Sustained throughput measurement
#[test]
#[ignore]
fn stress_sustained_throughput() {
    let store = SegmentedStore::new();
    let branch_id = BranchId::new();

    let duration = Duration::from_secs(5);
    let start = Instant::now();
    let mut ops = 0u64;
    let mut version = CommitVersion(1);

    while start.elapsed() < duration {
        let key = create_test_key(branch_id, &format!("key_{}", ops % 1000));
        store
            .put_with_version_mode(
                key.clone(),
                Value::Int(ops as i64),
                version,
                None,
                WriteMode::Append,
            )
            .unwrap();
        version = CommitVersion(version.0 + 1);
        let _ = store.get_versioned(&key, CommitVersion::MAX);
        ops += 2; // put + get
    }

    let elapsed = start.elapsed();
    println!(
        "Sustained: {} ops in {:?} ({:.0} ops/sec)",
        ops,
        elapsed,
        ops as f64 / elapsed.as_secs_f64()
    );
}
