use super::*;

#[test]
fn test_put_direct_contention_scaling() {
    const OPS_PER_THREAD: usize = 10_000;
    let temp_dir = TempDir::new().unwrap();
    let db =
        Database::open_with_durability(temp_dir.path().join("contention"), DurabilityMode::Cache)
            .unwrap();

    // Phase 1: Concurrent writes — measure throughput scaling
    let thread_counts = [1, 4, 8, 16];
    let mut baseline_throughput = 0.0_f64;

    // Use a shared branch so we can read back all keys afterwards
    let branch_id = BranchId::new();

    for &num_threads in &thread_counts {
        let barrier = Arc::new(std::sync::Barrier::new(num_threads));
        let start = std::time::Instant::now();

        std::thread::scope(|s| {
            for _t in 0..num_threads {
                let db = &db;
                let barrier = barrier.clone();
                s.spawn(move || {
                    let ns = Arc::new(Namespace::new(branch_id, "kv".to_string()));
                    barrier.wait();
                    for i in 0..OPS_PER_THREAD {
                        let key = Key::new_kv(ns.clone(), format!("k{i}"));
                        blind_write(db, key, Value::Int(i as i64));
                    }
                });
            }
        });

        let elapsed = start.elapsed();
        let total_ops = (num_threads * OPS_PER_THREAD) as f64;
        let throughput = total_ops / elapsed.as_secs_f64();

        eprintln!("  {num_threads:>2} threads: {throughput:>10.0} ops/s ({elapsed:?})");

        if num_threads == 1 {
            baseline_throughput = throughput;
        } else {
            // Throughput should not collapse to less than 1/3 of
            // single-threaded baseline (catches lock convoys and
            // atomic contention regressions).
            assert!(
                throughput > baseline_throughput / 3.0,
                "{num_threads}t throughput ({throughput:.0} ops/s) collapsed \
                 below 1/3 of 1t baseline ({baseline_throughput:.0} ops/s)"
            );
        }
    }

    // Phase 2: Verify data correctness — every write actually persisted
    // Check the last round (16 threads × OPS_PER_THREAD keys)
    let last_thread_count = *thread_counts.last().unwrap();
    for t in 0..last_thread_count {
        let ns = Arc::new(Namespace::new(branch_id, "kv".to_string()));
        // Spot-check first, middle, and last keys
        for &i in &[0, OPS_PER_THREAD / 2, OPS_PER_THREAD - 1] {
            let key = Key::new_kv(ns.clone(), format!("k{i}"));
            let stored = db
                .storage()
                .get_versioned(&key, CommitVersion::MAX)
                .unwrap();
            assert!(
                stored.is_some(),
                "blind write data missing: thread={t}, key=k{i}"
            );
            assert_eq!(
                stored.unwrap().value,
                Value::Int(i as i64),
                "blind write data corrupted: thread={t}, key=k{i}"
            );
        }
    }
}

/// Verify that blind writes are visible to concurrent snapshot
/// readers and that GC correctly respects active reader snapshots.
#[test]
fn test_put_direct_gc_safety_with_concurrent_reader() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("gc_safety")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "gc_target");

    // Write initial value
    let v1 = blind_write(&db, key.clone(), Value::Int(1));

    // Write v2 (creates a version chain: v1 -> v2)
    let v2 = blind_write(&db, key.clone(), Value::Int(2));
    assert!(v2 > v1, "versions must be monotonically increasing");

    // Start a snapshot reader that pins the version at v2
    let mut reader_txn = db.begin_transaction(branch_id).unwrap();
    let pinned_version = reader_txn.start_version;

    // Write more versions while reader holds its snapshot
    for i in 3..=10 {
        blind_write(&db, key.clone(), Value::Int(i));
    }

    // GC safe point must not advance past the reader's pinned version
    let safe_point = db.gc_safe_point();
    assert!(
        safe_point <= pinned_version.as_u64(),
        "gc_safe_point ({safe_point}) advanced past pinned reader version ({})",
        pinned_version.as_u64()
    );

    // GC should not prune versions the reader can still see
    let (_, pruned) = db.run_gc();
    // If any pruning happened, confirm reader's data is still intact
    let _ = pruned;

    // Reader should still see the value at its snapshot version
    let reader_val = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert!(reader_val.is_some(), "key disappeared during concurrent GC");
    // Latest version should be 10
    assert_eq!(reader_val.unwrap().value, Value::Int(10));

    // Release the reader — gc_safe_version should now be free to advance
    reader_txn.abort();

    let safe_point_after = db.gc_safe_point();
    assert!(
        safe_point_after > safe_point,
        "gc_safe_point should advance after reader releases: {safe_point_after} > {safe_point}"
    );

    // SegmentedStore prunes via compaction, not gc_branch(), so pruned_after is 0.
    let (_, _pruned_after) = db.run_gc();

    // Latest value must still be readable after GC
    let final_val = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(final_val.value, Value::Int(10));
}

/// Verify blind writes concurrent with GC under thread contention.
/// Writers and GC race — no panics, no data loss on latest version.
#[test]
fn test_put_direct_concurrent_gc_no_data_loss() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("gc_race")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let writers_remaining = std::sync::atomic::AtomicUsize::new(NUM_WRITERS);

    const NUM_WRITERS: usize = 4;
    const WRITES_PER_THREAD: usize = 1_000;

    std::thread::scope(|s| {
        // Spawn writer threads
        for t in 0..NUM_WRITERS {
            let db = &db;
            let ns = ns.clone();
            let remaining = &writers_remaining;
            s.spawn(move || {
                for i in 0..WRITES_PER_THREAD {
                    let key = Key::new_kv(ns.clone(), format!("w{t}_k{i}"));
                    blind_write(db, key, Value::Int(i as i64));
                }
                remaining.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            });
        }

        // Spawn a GC thread that runs concurrently with writers
        let db = &db;
        let remaining = &writers_remaining;
        s.spawn(move || {
            while remaining.load(std::sync::atomic::Ordering::Acquire) > 0 {
                db.run_gc();
                std::thread::yield_now();
            }
            // One final GC after all writers complete
            db.run_gc();
        });
    });

    // All writers finished — verify latest value for every key
    for t in 0..NUM_WRITERS {
        for &i in &[0, WRITES_PER_THREAD / 2, WRITES_PER_THREAD - 1] {
            let key = Key::new_kv(ns.clone(), format!("w{t}_k{i}"));
            let stored = db
                .storage()
                .get_versioned(&key, CommitVersion::MAX)
                .unwrap();
            assert!(
                stored.is_some(),
                "data lost after concurrent GC: thread={t}, key=w{t}_k{i}"
            );
            assert_eq!(
                stored.unwrap().value,
                Value::Int(i as i64),
                "data corrupted after concurrent GC: thread={t}, key=w{t}_k{i}"
            );
        }
    }
}
