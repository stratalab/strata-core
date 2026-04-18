use super::*;

#[test]
fn test_is_open_initially_true() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    // Database should be open initially
    assert!(db.is_open());
}

#[test]
fn test_shutdown_sets_not_open() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    assert!(db.is_open());

    // Shutdown should succeed
    assert!(db.shutdown().is_ok());

    // Database should no longer be open
    assert!(!db.is_open());
}

#[test]
fn test_shutdown_rejects_new_transactions() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "after_shutdown");

    // Shutdown the database
    db.shutdown().unwrap();

    // New transactions should be rejected
    let result = db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(42))?;
        Ok(())
    });

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, StrataError::InvalidInput { .. }));
}

#[test]
fn test_shutdown_idempotent() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    // Multiple shutdowns should be safe
    assert!(db.shutdown().is_ok());
    assert!(db.shutdown().is_ok());
    assert!(db.shutdown().is_ok());

    // Should remain not open
    assert!(!db.is_open());
}
#[test]
fn test_flush_thread_performs_final_sync() {
    // Use a 2-second flush interval so the periodic sync will NOT have
    // run by the time we call shutdown() (the write + shutdown completes
    // well within the first sleep cycle). In Standard mode, commit only
    // writes to BufWriter without fsync. The data can only reach disk via:
    //   (a) the flush thread's final sync (E-5), or
    //   (b) shutdown's explicit self.flush()
    //
    // This test verifies the full shutdown path (a + b) persists data.
    // The E-5 final sync provides defense-in-depth for crash scenarios
    // where the process dies between thread join and the explicit flush.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("final_sync_db");

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "final_sync_key");

    {
        // 2s interval: long enough that no periodic sync runs before we
        // call shutdown, short enough that the test doesn't take forever
        // (the flush thread sleeps for one interval before checking the flag).
        let mode = DurabilityMode::Standard {
            interval_ms: 2_000,
            batch_size: 1_000_000,
        };
        let db = Database::open_with_durability(&db_path, mode).unwrap();

        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Bytes(b"final_sync_value".to_vec()))?;
            Ok(())
        })
        .unwrap();

        // At this point data is in the WAL BufWriter but NOT fsynced.
        // Neither the periodic sync (2s away) nor the inline safety-net
        // (3×2s = 6s away) could have run.
        db.shutdown().unwrap();
    }

    // Re-open and verify data was persisted
    let db = Database::open(&db_path).unwrap();
    let val = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert!(
        val.is_some(),
        "Data should be recoverable after shutdown (flush thread final sync + explicit flush)"
    );
    assert_eq!(
        val.unwrap().value,
        Value::Bytes(b"final_sync_value".to_vec())
    );
}

#[test]
fn test_shutdown_blocks_until_in_flight_transactions_drain() {
    // Verify that shutdown() actually BLOCKS while a transaction is
    // in-flight, rather than returning immediately.
    //
    // Sequence:
    //   1. Background thread: begin_transaction (active_count=1)
    //   2. Background thread: signal "txn started"
    //   3. Main thread: call shutdown() → enters drain loop (blocked)
    //   4. Background thread: sleep 200ms → commit → active_count=0
    //   5. Main thread: drain loop exits → shutdown continues
    //
    // We verify shutdown took at least 150ms (i.e., it actually waited
    // for the transaction, not just returned immediately).
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drain_db");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "drain_key");

    let (started_tx, started_rx) = std::sync::mpsc::channel();

    let db2 = Arc::clone(&db);
    let key2 = key.clone();
    let handle = std::thread::spawn(move || {
        let mut txn = db2.begin_transaction(branch_id).unwrap();
        // Signal that the transaction is in-flight
        started_tx.send(()).unwrap();
        // Hold the transaction open for 200ms before committing
        std::thread::sleep(std::time::Duration::from_millis(200));
        txn.put(key2, Value::Bytes(b"drain_value".to_vec()))
            .unwrap();
        db2.commit_transaction(&mut txn).unwrap();
        db2.end_transaction(txn);
    });

    // Wait until the background thread has started the transaction
    started_rx.recv().unwrap();
    assert_eq!(
        db.coordinator.active_count(),
        1,
        "Should have 1 active transaction before shutdown"
    );

    // shutdown() should block until the transaction commits (~200ms)
    let start = std::time::Instant::now();
    db.shutdown().unwrap();
    let elapsed = start.elapsed();

    handle.join().unwrap();

    // Verify shutdown actually waited for the transaction
    assert!(
        elapsed >= std::time::Duration::from_millis(150),
        "Shutdown returned in {:?}, should have blocked waiting for in-flight transaction",
        elapsed
    );

    // Re-open and verify committed data was persisted
    drop(db);
    let db = Database::open(&db_path).unwrap();
    let val = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert!(
        val.is_some(),
        "Transaction committed during shutdown drain should be persisted"
    );
    assert_eq!(val.unwrap().value, Value::Bytes(b"drain_value".to_vec()));
}

#[test]
fn test_shutdown_proceeds_after_draining_with_no_transactions() {
    // Verify that shutdown completes quickly when no transactions are
    // active, and that data committed before shutdown is persisted.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("fast_shutdown_db");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "pre_shutdown_key");

    // Commit data, then verify shutdown is fast and data persists
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Bytes(b"pre_shutdown".to_vec()))?;
        Ok(())
    })
    .unwrap();

    assert_eq!(
        db.coordinator.active_count(),
        0,
        "No active transactions after commit"
    );

    let start = std::time::Instant::now();
    db.shutdown().unwrap();
    let elapsed = start.elapsed();

    // Drain loop should skip immediately when active_count == 0
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "Shutdown took {:?}, expected fast completion with no active transactions",
        elapsed
    );
    assert!(!db.is_open());

    // Re-open and verify persistence
    drop(db);
    let db = Database::open(&db_path).unwrap();
    let val = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert!(val.is_some(), "Data committed before shutdown must persist");
}

#[test]
fn test_shutdown_multiple_in_flight_transactions() {
    // Verify shutdown waits for ALL in-flight transactions, not just one.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("multi_txn_db");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key1 = Key::new_kv(ns.clone(), "txn1_key");
    let key2 = Key::new_kv(ns.clone(), "txn2_key");

    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();

    // Spawn two transactions that hold open for different durations
    let db2 = Arc::clone(&db);
    let k1 = key1.clone();
    let started_tx1 = started_tx.clone();
    let h1 = std::thread::spawn(move || {
        let mut txn = db2.begin_transaction(branch_id).unwrap();
        started_tx1.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        txn.put(k1, Value::Bytes(b"value1".to_vec())).unwrap();
        db2.commit_transaction(&mut txn).unwrap();
        db2.end_transaction(txn);
    });

    let db3 = Arc::clone(&db);
    let k2 = key2.clone();
    let h2 = std::thread::spawn(move || {
        let mut txn = db3.begin_transaction(branch_id).unwrap();
        started_tx.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(250));
        txn.put(k2, Value::Bytes(b"value2".to_vec())).unwrap();
        db3.commit_transaction(&mut txn).unwrap();
        db3.end_transaction(txn);
    });

    // Wait for both transactions to start
    started_rx.recv().unwrap();
    started_rx.recv().unwrap();
    assert!(
        db.coordinator.active_count() >= 2,
        "Should have 2 active transactions"
    );

    // Shutdown must wait for BOTH transactions
    let start = std::time::Instant::now();
    db.shutdown().unwrap();
    let elapsed = start.elapsed();

    h1.join().unwrap();
    h2.join().unwrap();

    // Should have waited for the slower transaction (~250ms)
    assert!(
        elapsed >= std::time::Duration::from_millis(200),
        "Shutdown returned in {:?}, should have waited for both transactions",
        elapsed
    );

    // Both transactions' data should be persisted
    drop(db);
    let db = Database::open(&db_path).unwrap();
    assert!(
        db.storage()
            .get_versioned(&key1, CommitVersion::MAX)
            .unwrap()
            .is_some(),
        "txn1 data lost"
    );
    assert!(
        db.storage()
            .get_versioned(&key2, CommitVersion::MAX)
            .unwrap()
            .is_some(),
        "txn2 data lost"
    );
}
// ========================================================================
// Shutdown lifecycle coverage (ENG-DEBT-009)
// ========================================================================

/// Verify the full shutdown lifecycle: open → write → shutdown → verify.
///
/// Covers: transaction draining, WAL flush, flush thread join,
/// freeze hooks, and clean re-open after shutdown.
#[test]
fn test_shutdown_full_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("lifecycle_db");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "lifecycle_key");

    // Write data
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Bytes(b"lifecycle_value".to_vec()))?;
        Ok(())
    })
    .unwrap();

    // Shutdown
    db.shutdown().unwrap();

    // Verify shutdown state
    assert!(!db.is_open());
    assert!(
        !db.accepting_transactions
            .load(std::sync::atomic::Ordering::Relaxed),
        "accepting_transactions must be false after shutdown"
    );
    assert!(
        db.shutdown_complete
            .load(std::sync::atomic::Ordering::Relaxed),
        "shutdown_complete must be true after shutdown"
    );

    // Verify flush thread was joined (handle consumed)
    assert!(
        db.flush_handle.lock().is_none(),
        "flush thread handle must be consumed (joined) after shutdown"
    );

    // Re-open and verify data persisted through shutdown
    drop(db);
    let db = Database::open(&db_path).unwrap();
    let val = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert!(
        val.is_some(),
        "Data committed before shutdown must survive re-open"
    );
    assert_eq!(
        val.unwrap().value,
        Value::Bytes(b"lifecycle_value".to_vec())
    );
}

/// Verify cache (ephemeral) database shutdown is clean.
#[test]
fn test_shutdown_cache_database() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "cache_key");

    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(99))?;
        Ok(())
    })
    .unwrap();

    // Shutdown should succeed on ephemeral databases
    db.shutdown().unwrap();
    assert!(!db.is_open());

    // Double shutdown is safe
    db.shutdown().unwrap();
}

/// Verify the WAL flush thread terminates during shutdown.
///
/// Opens with Standard durability (which spawns a flush thread),
/// shuts down, and verifies the thread handle was consumed.
#[test]
fn test_shutdown_flush_thread_termination() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("flush_thread_db");

    // Standard mode spawns a background flush thread
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();

    // The flush thread handle should exist before shutdown
    assert!(
        db.flush_handle.lock().is_some(),
        "Standard mode should have a flush thread handle"
    );

    // Signal + join happens inside shutdown
    db.shutdown().unwrap();

    // After shutdown, the flush_shutdown flag should be set
    assert!(
        db.flush_shutdown.load(std::sync::atomic::Ordering::Relaxed),
        "flush_shutdown flag must be set after shutdown"
    );

    // And the handle must be consumed
    assert!(
        db.flush_handle.lock().is_none(),
        "flush thread handle must be joined (consumed) during shutdown"
    );
}

#[test]
fn test_set_durability_mode_restarts_flush_thread_for_standard_reconfiguration() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("durability_switch_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();

    let original_thread_id = db
        .flush_handle
        .lock()
        .as_ref()
        .expect("standard mode should start a flush thread")
        .thread()
        .id();

    db.set_durability_mode(DurabilityMode::Standard {
        interval_ms: 1,
        batch_size: 64,
    })
    .unwrap();

    let restarted_thread_id = db
        .flush_handle
        .lock()
        .as_ref()
        .expect("reconfigured standard mode should have a flush thread")
        .thread()
        .id();

    assert_ne!(
        original_thread_id, restarted_thread_id,
        "standard-mode reconfiguration must restart the shared flush thread"
    );
}

#[test]
fn test_set_durability_mode_updates_database_state() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("durability_state_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();

    db.set_durability_mode(DurabilityMode::Always).unwrap();

    assert_eq!(
        *db.durability_mode.read(),
        DurabilityMode::Always,
        "database state must reflect the runtime durability mode"
    );
}

#[test]
#[serial(open_databases)]
fn test_set_durability_mode_updates_runtime_signature_for_reuse_checks() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("runtime_signature_durability");
    let db = Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_subsystem(TestRuntimeSubsystem::named("runtime-subsystem")),
    )
    .unwrap();

    db.set_durability_mode(DurabilityMode::Always).unwrap();
    assert_eq!(
        db.runtime_signature().unwrap().durability_mode,
        DurabilityMode::Always,
        "runtime signature must track live durability mode"
    );

    // Second opener that *explicitly* requests the old Standard mode must
    // be rejected on signature mismatch. (T3-E7 review: `set_durability_mode`
    // now also persists "always" to strata.toml, so a second opener that
    // reads the default config no longer has a stale-config problem —
    // hence the explicit request here.)
    let stale_cfg = crate::StrataConfig {
        durability: "standard".to_string(),
        ..crate::StrataConfig::default()
    };
    let err = match Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_config(stale_cfg)
            .with_subsystem(TestRuntimeSubsystem::named("runtime-subsystem")),
    ) {
        Ok(_) => panic!("explicit request for Standard must not reuse a switched-to-Always db"),
        Err(err) => err,
    };
    assert!(
        matches!(err, StrataError::IncompatibleReuse { .. }),
        "expected IncompatibleReuse, got {:?}",
        err
    );

    // A second opener whose requested config matches the post-switch state
    // (either because they read the now-persisted strata.toml or because
    // they explicitly asked for Always) must succeed and reuse the instance.
    let ok_reuse = Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_subsystem(TestRuntimeSubsystem::named("runtime-subsystem")),
    )
    .expect("reopen with no-config reads the now-persisted 'always' and reuses");
    assert!(
        Arc::ptr_eq(&db, &ok_reuse),
        "matching signature must return the same Arc, not a fresh instance"
    );

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

#[test]
#[serial]
fn test_set_durability_mode_cache_error_preserves_flush_thread() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("durability_cache_reject_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();

    let original_thread_id = db
        .flush_handle
        .lock()
        .as_ref()
        .expect("standard mode should start a flush thread")
        .thread()
        .id();

    assert!(db.set_durability_mode(DurabilityMode::Cache).is_err());
    assert_eq!(
        *db.durability_mode.read(),
        DurabilityMode::standard_default(),
        "rejected runtime switch must leave database durability state unchanged"
    );
    assert_eq!(
        db.runtime_signature().unwrap().durability_mode,
        DurabilityMode::standard_default(),
        "rejected runtime switch must not mutate registry compatibility state"
    );

    {
        let guard = db.flush_handle.lock();
        let handle = guard
            .as_ref()
            .expect("rejected runtime switch must keep the existing flush thread");
        assert_eq!(
            handle.thread().id(),
            original_thread_id,
            "rejected runtime switch should not tear down the existing flush thread"
        );
        assert!(
            !handle.is_finished(),
            "rejected runtime switch must leave the existing flush thread running"
        );
    }

    db.shutdown().unwrap();
}

#[test]
#[serial]
fn test_set_durability_mode_spawn_failure_rolls_back_state() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("durability_spawn_failure_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();

    super::test_hooks::clear_flush_thread_spawn_failure(&db_path);
    super::test_hooks::inject_flush_thread_spawn_failure(&db_path);

    let err = db
        .set_durability_mode(DurabilityMode::Standard {
            interval_ms: 1,
            batch_size: 64,
        })
        .expect_err("injected spawn failure should bubble up");

    super::test_hooks::clear_flush_thread_spawn_failure(&db_path);

    assert!(
        matches!(err, StrataError::Internal { .. }),
        "expected internal error from injected spawn failure, got {:?}",
        err
    );
    assert_eq!(
        *db.durability_mode.read(),
        DurabilityMode::standard_default(),
        "spawn failure must roll database durability state back"
    );
    assert_eq!(
        db.wal_writer.as_ref().unwrap().lock().durability_mode(),
        DurabilityMode::standard_default(),
        "spawn failure must roll WAL writer durability mode back"
    );
    assert_eq!(
        db.runtime_signature().unwrap().durability_mode,
        DurabilityMode::standard_default(),
        "spawn failure must roll registry compatibility state back"
    );
    assert!(
        db.flush_handle
            .lock()
            .as_ref()
            .is_some_and(|handle| !handle.is_finished()),
        "spawn failure must restore the prior standard-mode flush thread"
    );

    db.shutdown().unwrap();
}

/// T3-E2: Background sync failure halts the writer and rejects both new and
/// already-open manual commits until explicit resume succeeds.
#[test]
#[serial]
fn test_background_sync_failure_halts_writer_and_rejects_manual_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("bg_sync_halt_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();
    db.set_durability_mode(DurabilityMode::Standard {
        interval_ms: 10,
        batch_size: 64,
    })
    .unwrap();

    let branch_id = BranchId::new();
    let trigger_key = Key::new_kv(create_test_namespace(branch_id), "bg_sync_key");
    let pending_key = Key::new_kv(create_test_namespace(branch_id), "pending_key");

    super::test_hooks::clear_sync_failure(&db_path);
    let mut manual_txn = db.begin_transaction(branch_id).unwrap();
    manual_txn.put(pending_key.clone(), Value::Int(6)).unwrap();

    // Commit a separate transaction to create unsynced WAL data.
    super::test_hooks::inject_sync_failure(&db_path, std::io::ErrorKind::Other);
    db.transaction(branch_id, |txn| {
        txn.put(trigger_key.clone(), Value::Int(7))?;
        Ok(())
    })
    .unwrap();

    // Wait for background sync to fail and halt the writer
    wait_until(Duration::from_secs(2), || {
        matches!(
            db.wal_writer_health(),
            super::WalWriterHealth::Halted { .. }
        )
    });

    // Verify health state shows halted
    let health = db.wal_writer_health();
    match health {
        super::WalWriterHealth::Halted {
            failed_sync_count, ..
        } => {
            assert!(failed_sync_count >= 1, "expected at least one failed sync");
        }
        super::WalWriterHealth::Healthy => {
            panic!("expected writer to be halted after sync failure");
        }
    }

    let err = manual_txn
        .commit()
        .expect_err("manual transaction should be rejected once the writer halts");
    assert!(
        matches!(err, StrataError::WriterHalted { .. }),
        "expected WriterHalted from manual commit, got: {:?}",
        err
    );

    // Verify new transactions are rejected with WriterHalted error
    let err = db
        .transaction(branch_id, |txn| {
            txn.put(trigger_key.clone(), Value::Int(8))?;
            Ok(())
        })
        .expect_err("transaction should be rejected when writer is halted");

    assert!(
        matches!(err, StrataError::WriterHalted { .. }),
        "expected WriterHalted error, got: {:?}",
        err
    );

    // Clearing the fault should NOT auto-resume; the flush thread stays alive
    // but passive until explicit operator resume.
    super::test_hooks::clear_sync_failure(&db_path);
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        matches!(
            db.wal_writer_health(),
            super::WalWriterHealth::Halted { .. }
        ),
        "writer must remain halted until explicit resume"
    );

    db.resume_wal_writer("test cleared injected failure")
        .unwrap();

    // Verify health is restored
    assert!(
        matches!(db.wal_writer_health(), super::WalWriterHealth::Healthy),
        "expected writer to be healthy after resume"
    );

    // Verify transactions work again
    db.transaction(branch_id, |txn| {
        txn.put(trigger_key.clone(), Value::Int(9))?;
        Ok(())
    })
    .unwrap();

    db.shutdown().unwrap();
}

/// T3-E2: Failed explicit resumes preserve the halt and increment the failure streak.
#[test]
#[serial]
fn test_resume_while_still_failing_increments_failed_sync_count() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("repeated_sync_fail_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();
    db.set_durability_mode(DurabilityMode::Standard {
        interval_ms: 10,
        batch_size: 64,
    })
    .unwrap();

    let branch_id = BranchId::new();
    let key = Key::new_kv(create_test_namespace(branch_id), "repeat_fail_key");

    super::test_hooks::clear_sync_failure(&db_path);
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();

    // Inject sync failure
    super::test_hooks::inject_sync_failure(&db_path, std::io::ErrorKind::Other);

    // Wait for writer to halt
    wait_until(Duration::from_secs(2), || {
        matches!(
            db.wal_writer_health(),
            super::WalWriterHealth::Halted { .. }
        )
    });

    let (first_reason, first_seen, first_count) = match db.wal_writer_health() {
        super::WalWriterHealth::Halted {
            reason,
            first_observed_at,
            failed_sync_count,
        } => (reason, first_observed_at, failed_sync_count),
        super::WalWriterHealth::Healthy => {
            panic!("expected writer to be halted");
        }
    };
    assert!(first_count >= 1, "expected at least 1 failed sync");

    super::test_hooks::inject_sync_failure(&db_path, std::io::ErrorKind::Other);
    let err = db
        .resume_wal_writer("fault still present")
        .expect_err("resume should fail while sync is still failing");
    assert!(
        matches!(err, StrataError::WriterHalted { .. }),
        "expected WriterHalted, got: {:?}",
        err
    );

    match db.wal_writer_health() {
        super::WalWriterHealth::Halted {
            reason,
            first_observed_at,
            failed_sync_count,
        } => {
            assert_eq!(reason, first_reason, "halt reason should stay stable");
            assert_eq!(
                first_observed_at, first_seen,
                "first_observed_at should not reset across failed resumes"
            );
            assert_eq!(
                failed_sync_count,
                first_count + 1,
                "failed resume must increment failed_sync_count"
            );
        }
        super::WalWriterHealth::Healthy => panic!("writer must remain halted"),
    }

    super::test_hooks::clear_sync_failure(&db_path);
    db.shutdown().unwrap();
}

/// D1: A failure in `begin_background_sync` (flush-setup phase) latches
/// `WalWriterHealth::Halted` and flips `accepting_transactions = false`,
/// matching the existing `sync_all`-failure contract. Closes DG-003.
#[test]
#[serial]
fn test_begin_sync_failure_halts_writer_and_rejects_manual_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("begin_sync_halt_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();
    db.set_durability_mode(DurabilityMode::Standard {
        interval_ms: 10,
        batch_size: 64,
    })
    .unwrap();

    let branch_id = BranchId::new();
    let trigger_key = Key::new_kv(create_test_namespace(branch_id), "begin_sync_key");
    let pending_key = Key::new_kv(create_test_namespace(branch_id), "pending_key");

    super::test_hooks::clear_begin_sync_failure(&db_path);
    let mut manual_txn = db.begin_transaction(branch_id).unwrap();
    manual_txn.put(pending_key.clone(), Value::Int(6)).unwrap();

    // Commit a separate transaction to create unsynced WAL data so the flush
    // thread will actually call begin_background_sync (which requires
    // has_unsynced_data == true).
    super::test_hooks::inject_begin_sync_failure(&db_path, std::io::ErrorKind::Other);
    db.transaction(branch_id, |txn| {
        txn.put(trigger_key.clone(), Value::Int(7))?;
        Ok(())
    })
    .unwrap();

    // Wait for the flush thread to hit the injected begin failure and halt.
    wait_until(Duration::from_secs(2), || {
        matches!(
            db.wal_writer_health(),
            super::WalWriterHealth::Halted { .. }
        )
    });

    let health = db.wal_writer_health();
    match health {
        super::WalWriterHealth::Halted {
            failed_sync_count, ..
        } => {
            assert!(failed_sync_count >= 1, "expected at least one failed sync");
        }
        super::WalWriterHealth::Healthy => {
            panic!("expected writer to be halted after begin-sync failure");
        }
    }

    // Manual commit held open before the halt must now be rejected.
    let err = manual_txn
        .commit()
        .expect_err("manual transaction should be rejected once the writer halts");
    assert!(
        matches!(err, StrataError::WriterHalted { .. }),
        "expected WriterHalted from manual commit, got: {:?}",
        err
    );

    // New transactions after halt must be rejected too.
    let err = db
        .transaction(branch_id, |txn| {
            txn.put(trigger_key.clone(), Value::Int(8))?;
            Ok(())
        })
        .expect_err("transaction should be rejected when writer is halted");
    assert!(
        matches!(err, StrataError::WriterHalted { .. }),
        "expected WriterHalted error, got: {:?}",
        err
    );

    // Clearing the injection must NOT auto-resume; only explicit resume does.
    super::test_hooks::clear_begin_sync_failure(&db_path);
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        matches!(
            db.wal_writer_health(),
            super::WalWriterHealth::Halted { .. }
        ),
        "writer must remain halted until explicit resume"
    );

    db.resume_wal_writer("test cleared injected begin failure")
        .unwrap();
    assert!(
        matches!(db.wal_writer_health(), super::WalWriterHealth::Healthy),
        "expected writer to be healthy after resume"
    );

    // Post-resume transactions work again.
    db.transaction(branch_id, |txn| {
        txn.put(trigger_key.clone(), Value::Int(9))?;
        Ok(())
    })
    .unwrap();

    db.shutdown().unwrap();
}

/// D1: A failure in `commit_background_sync` (bookkeeping phase — post-fsync,
/// so data is durable on disk but the writer's in-memory counters have
/// diverged) latches `WalWriterHealth::Halted` and flips
/// `accepting_transactions = false`. Closes DG-004.
#[test]
#[serial]
fn test_commit_sync_failure_halts_writer_and_rejects_manual_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("commit_sync_halt_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();
    db.set_durability_mode(DurabilityMode::Standard {
        interval_ms: 10,
        batch_size: 64,
    })
    .unwrap();

    let branch_id = BranchId::new();
    let trigger_key = Key::new_kv(create_test_namespace(branch_id), "commit_sync_key");
    let pending_key = Key::new_kv(create_test_namespace(branch_id), "pending_key");

    super::test_hooks::clear_commit_sync_failure(&db_path);
    let mut manual_txn = db.begin_transaction(branch_id).unwrap();
    manual_txn.put(pending_key.clone(), Value::Int(6)).unwrap();

    super::test_hooks::inject_commit_sync_failure(&db_path, std::io::ErrorKind::Other);
    db.transaction(branch_id, |txn| {
        txn.put(trigger_key.clone(), Value::Int(7))?;
        Ok(())
    })
    .unwrap();

    wait_until(Duration::from_secs(2), || {
        matches!(
            db.wal_writer_health(),
            super::WalWriterHealth::Halted { .. }
        )
    });

    let health = db.wal_writer_health();
    match health {
        super::WalWriterHealth::Halted {
            failed_sync_count, ..
        } => {
            assert!(failed_sync_count >= 1, "expected at least one failed sync");
        }
        super::WalWriterHealth::Healthy => {
            panic!("expected writer to be halted after commit-sync failure");
        }
    }

    let err = manual_txn
        .commit()
        .expect_err("manual transaction should be rejected once the writer halts");
    assert!(
        matches!(err, StrataError::WriterHalted { .. }),
        "expected WriterHalted from manual commit, got: {:?}",
        err
    );

    let err = db
        .transaction(branch_id, |txn| {
            txn.put(trigger_key.clone(), Value::Int(8))?;
            Ok(())
        })
        .expect_err("transaction should be rejected when writer is halted");
    assert!(
        matches!(err, StrataError::WriterHalted { .. }),
        "expected WriterHalted error, got: {:?}",
        err
    );

    super::test_hooks::clear_commit_sync_failure(&db_path);
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        matches!(
            db.wal_writer_health(),
            super::WalWriterHealth::Halted { .. }
        ),
        "writer must remain halted until explicit resume"
    );

    db.resume_wal_writer("test cleared injected commit failure")
        .unwrap();
    assert!(
        matches!(db.wal_writer_health(), super::WalWriterHealth::Healthy),
        "expected writer to be healthy after resume"
    );

    // Post-resume transactions work again. The fsync before the bookkeeping
    // halt succeeded, so the pre-halt trigger_key is already durable; resume
    // only clears the halt state.
    db.transaction(branch_id, |txn| {
        txn.put(trigger_key.clone(), Value::Int(9))?;
        Ok(())
    })
    .unwrap();

    db.shutdown().unwrap();
}

/// D1 regression: resume must serialize with an in-flight halt publication so
/// the flush thread cannot restore `accepting_transactions = false` after
/// `resume_wal_writer()` has already published `Healthy`.
#[test]
#[serial]
fn test_resume_waits_for_inflight_halt_publication_before_restoring_accepting() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("resume_vs_halt_publish_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();
    db.set_durability_mode(DurabilityMode::Standard {
        interval_ms: 10,
        batch_size: 64,
    })
    .unwrap();

    let branch_id = BranchId::new();
    let trigger_key = Key::new_kv(create_test_namespace(branch_id), "resume_halt_trigger");
    let resumed_key = Key::new_kv(create_test_namespace(branch_id), "resume_after_halt");

    super::test_hooks::clear_begin_sync_failure(&db_path);
    super::test_hooks::clear_halt_publish_pause(&db_path);
    super::test_hooks::install_halt_publish_pause(&db_path);
    super::test_hooks::inject_begin_sync_failure(&db_path, std::io::ErrorKind::Other);

    db.transaction(branch_id, |txn| {
        txn.put(trigger_key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();

    assert!(
        super::test_hooks::wait_for_halt_publish_pause(&db_path, Duration::from_secs(2)),
        "flush thread should reach the halt-publication pause"
    );
    assert!(
        matches!(
            db.wal_writer_health(),
            super::WalWriterHealth::Halted { .. }
        ),
        "writer health should already be halted while publication is paused"
    );

    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let db_resume = Arc::clone(&db);
    let resume_handle = std::thread::spawn(move || {
        let result = db_resume.resume_wal_writer("test serialize with in-flight halt");
        resume_tx.send(result).unwrap();
    });

    assert!(
        resume_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "resume must block until the in-flight halt publication finishes"
    );

    super::test_hooks::release_halt_publish_pause(&db_path);
    let resume_result = resume_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("resume should finish once the halt publication is released");
    resume_handle.join().unwrap();
    super::test_hooks::clear_halt_publish_pause(&db_path);
    super::test_hooks::clear_begin_sync_failure(&db_path);

    resume_result.unwrap();
    assert!(
        matches!(db.wal_writer_health(), super::WalWriterHealth::Healthy),
        "writer should be healthy after a serialized resume"
    );

    db.transaction(branch_id, |txn| {
        txn.put(resumed_key.clone(), Value::Int(2))?;
        Ok(())
    })
    .unwrap();

    db.shutdown().unwrap();
}

/// T3-E2: Resume on ephemeral database returns error.
#[test]
fn test_resume_ephemeral_database_returns_error() {
    let db = Database::cache().unwrap();

    // Ephemeral databases have no WAL writer, so resume should fail
    let err = db
        .resume_wal_writer("test")
        .expect_err("resume on ephemeral should fail");

    assert!(
        matches!(err, StrataError::Internal { .. }),
        "expected Internal error for ephemeral resume, got: {:?}",
        err
    );
}

/// T3-E2: Resume when already healthy is a no-op.
#[test]
fn test_resume_when_already_healthy_is_noop() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("resume_healthy_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();

    // Verify initially healthy
    assert!(
        matches!(db.wal_writer_health(), super::WalWriterHealth::Healthy),
        "expected writer to be healthy initially"
    );

    // Resume when already healthy should succeed (no-op)
    db.resume_wal_writer("no-op test").unwrap();

    // Still healthy
    assert!(
        matches!(db.wal_writer_health(), super::WalWriterHealth::Healthy),
        "expected writer to remain healthy"
    );

    // Transactions should still work
    let branch_id = BranchId::new();
    let key = Key::new_kv(create_test_namespace(branch_id), "noop_key");
    db.transaction(branch_id, |txn| {
        txn.put(key, Value::Int(42))?;
        Ok(())
    })
    .unwrap();

    db.shutdown().unwrap();
}

/// T3-E2: Resume must not reopen a database after shutdown has started.
#[test]
fn test_resume_after_shutdown_returns_error_and_does_not_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("resume_after_shutdown_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();

    db.shutdown().unwrap();
    assert!(!db.is_open(), "shutdown must close the database");

    let err = db
        .resume_wal_writer("should not reopen closed database")
        .expect_err("resume after shutdown must fail");
    assert!(
        matches!(err, StrataError::InvalidInput { .. }),
        "expected InvalidInput after shutdown, got: {:?}",
        err
    );
    assert!(!db.is_open(), "resume must not reopen a shut-down database");
}

/// T3-E2: Engine callers see DurableButNotVisible and the durable write becomes
/// visible after reopen recovery.
#[test]
#[serial(open_databases)]
fn test_durable_but_not_visible_is_surfaced_and_recovers_on_reopen() {
    concurrency_test_hooks::clear_apply_failure_injection();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("durable_not_visible_db");
    let branch_id = BranchId::new();
    let key = Key::new_kv(create_test_namespace(branch_id), "durable_not_visible_key");

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

        concurrency_test_hooks::inject_apply_failure_once(
            "injected apply_writes failure after WAL commit",
        );
        let err = db
            .transaction(branch_id, |txn| {
                txn.put(key.clone(), Value::Int(77))?;
                Ok(())
            })
            .expect_err("commit should surface durable-but-not-visible");

        match err {
            StrataError::DurableButNotVisible {
                txn_id,
                commit_version,
            } => {
                assert!(txn_id > 0, "txn_id must be preserved");
                assert!(commit_version > 0, "commit_version must be preserved");
            }
            other => panic!(
                "expected DurableButNotVisible from engine commit path, got {:?}",
                other
            ),
        }

        let visible_now: Option<Value> = db.transaction(branch_id, |txn| txn.get(&key)).unwrap();
        assert!(
            visible_now.is_none(),
            "write must remain invisible in the current process"
        );
    }

    concurrency_test_hooks::clear_apply_failure_injection();
    OPEN_DATABASES.lock().clear();

    let reopened = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
    let recovered: Option<Value> = reopened
        .transaction(branch_id, |txn| txn.get(&key))
        .unwrap();
    assert_eq!(
        recovered,
        Some(Value::Int(77)),
        "reopen recovery must make the durable write visible"
    );

    reopened.shutdown().unwrap();
}

#[test]
fn test_explicit_flush_waits_for_inflight_background_sync() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("flush_waits_for_sync_db");
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();
    db.set_durability_mode(DurabilityMode::Standard {
        interval_ms: 1,
        batch_size: 64,
    })
    .unwrap();
    db.stop_flush_thread();

    let branch_id = BranchId::new();
    let key = Key::new_kv(create_test_namespace(branch_id), "flush_wait_key");
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(11))?;
        Ok(())
    })
    .unwrap();

    std::thread::sleep(Duration::from_millis(10));

    let handle = {
        let mut wal = db.wal_writer.as_ref().unwrap().lock();
        wal.begin_background_sync()
            .unwrap()
            .expect("expected an in-flight background sync")
    };

    let (tx, rx) = std::sync::mpsc::channel();
    let db_for_flush = Arc::clone(&db);
    let flush_thread = std::thread::spawn(move || {
        let result = db_for_flush.flush();
        tx.send(result).unwrap();
    });

    assert!(
        rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "explicit flush should wait for the in-flight background sync to resolve"
    );

    handle.fd().sync_all().unwrap();
    {
        let mut wal = db.wal_writer.as_ref().unwrap().lock();
        wal.commit_background_sync(handle).unwrap();
    }

    rx.recv_timeout(Duration::from_secs(1))
        .expect("flush should complete after background sync commit")
        .unwrap();
    flush_thread.join().unwrap();

    db.shutdown().unwrap();
}

#[test]
fn test_source_contains_single_flush_thread_implementation() {
    // Paths are relative to this test file (database/tests/shutdown.rs);
    // `../` reaches the production source files (database/open.rs,
    // database/mod.rs), not the sibling test submodules.
    let open_source = include_str!("../open.rs");
    let mod_source = include_str!("../mod.rs");
    let loop_marker = ["name(\"", "strata-wal-flush", "\""].concat();
    let open_count = open_source.matches(&loop_marker).count();
    let mod_count = mod_source.matches(&loop_marker).count();

    assert_eq!(
        open_count + mod_count,
        1,
        "engine database sources must contain exactly one production flush loop"
    );
}
#[test]
#[serial(open_databases)]
fn test_open_runtime_lifecycle_order_and_reuse() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("runtime_lifecycle");
    let events = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let primary_spec = OpenSpec::primary(&db_path)
        .with_subsystem(TestRuntimeSubsystem::recording(
            "runtime-subsystem",
            events.clone(),
        ))
        .with_default_branch("main");
    let primary = Database::open_runtime(primary_spec).unwrap();
    assert_eq!(
        events.lock().as_slice(),
        ["recover", "initialize", "bootstrap"],
        "primary open_runtime must run recover -> initialize -> bootstrap"
    );

    let primary_reuse = Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_subsystem(TestRuntimeSubsystem::recording(
                "runtime-subsystem",
                events.clone(),
            ))
            .with_default_branch("main"),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&primary, &primary_reuse));
    assert_eq!(
        events.lock().as_slice(),
        ["recover", "initialize", "bootstrap"],
        "reusing an initialized primary must not rerun lifecycle hooks"
    );

    drop(primary_reuse);
    drop(primary);
    OPEN_DATABASES.lock().clear();

    events.lock().clear();
    let follower = Database::open_runtime(OpenSpec::follower(&db_path).with_subsystem(
        TestRuntimeSubsystem::recording("runtime-subsystem", events.clone()),
    ))
    .unwrap();
    assert_eq!(
        events.lock().as_slice(),
        ["recover", "initialize"],
        "follower open_runtime must skip bootstrap"
    );

    // Followers are NOT deduplicated — each open_follower call returns an
    // independent instance with its own refresh state. This differs from
    // primaries which have singleton semantics per path.
    events.lock().clear();
    let follower2 = Database::open_runtime(OpenSpec::follower(&db_path).with_subsystem(
        TestRuntimeSubsystem::recording("runtime-subsystem", events.clone()),
    ))
    .unwrap();
    assert!(
        !Arc::ptr_eq(&follower, &follower2),
        "follower opens must return independent instances"
    );
    assert_eq!(
        events.lock().as_slice(),
        ["recover", "initialize"],
        "each follower open runs its own lifecycle"
    );

    drop(follower2);
    drop(follower);
}

#[test]
#[serial(open_databases)]
fn test_open_runtime_rejects_incompatible_reuse() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("runtime_signature");

    let db = Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_subsystem(TestRuntimeSubsystem::named("runtime-subsystem"))
            .with_default_branch("main"),
    )
    .unwrap();

    let err = match Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_subsystem(TestRuntimeSubsystem::named("runtime-subsystem"))
            .with_default_branch("develop"),
    ) {
        Ok(_) => panic!("expected incompatible reuse error"),
        Err(err) => err,
    };
    assert!(
        matches!(err, StrataError::IncompatibleReuse { .. }),
        "expected IncompatibleReuse, got {:?}",
        err
    );

    drop(db);
    OPEN_DATABASES.lock().clear();
}

#[test]
#[serial(open_databases)]
fn test_open_runtime_failed_open_can_retry() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("runtime_retry");
    let fail_once = Arc::new(AtomicBool::new(true));

    let err = match Database::open_runtime(OpenSpec::primary(&db_path).with_subsystem(
        TestRuntimeSubsystem::fail_initialize_once("runtime-subsystem", fail_once.clone()),
    )) {
        Ok(_) => panic!("expected first open to fail"),
        Err(err) => err,
    };
    assert!(
        matches!(err, StrataError::Internal { .. }),
        "unexpected error for failed first open: {:?}",
        err
    );

    let canonical_path = db_path.canonicalize().unwrap();
    assert!(
        !OPEN_DATABASES.lock().contains_key(&canonical_path),
        "failed lifecycle open must not leave a registry entry behind"
    );

    let db = Database::open_runtime(OpenSpec::primary(&db_path).with_subsystem(
        TestRuntimeSubsystem::fail_initialize_once("runtime-subsystem", fail_once),
    ))
    .unwrap();
    assert!(db.is_lifecycle_complete());

    drop(db);
    OPEN_DATABASES.lock().clear();
}

#[test]
#[serial(open_databases)]
fn test_open_runtime_failed_config_write_can_retry() {
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("runtime_config_write");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(db_path.join(crate::database::config::CONFIG_FILE_NAME)).unwrap();

    let err = match Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_config(StrataConfig::default())
            .with_subsystem(TestRuntimeSubsystem::named("runtime-subsystem")),
    ) {
        Ok(_) => panic!("config write should fail while strata.toml is a directory"),
        Err(err) => err,
    };

    let canonical_path = db_path.canonicalize().unwrap();
    assert!(
        !OPEN_DATABASES.lock().contains_key(&canonical_path),
        "failed config persistence must not leave a registry entry behind"
    );
    assert!(
        !matches!(err, StrataError::IncompatibleReuse { .. }),
        "expected a real open failure, got incompatible reuse: {:?}",
        err
    );

    std::fs::remove_dir_all(db_path.join(crate::database::config::CONFIG_FILE_NAME)).unwrap();

    let db = Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_subsystem(TestRuntimeSubsystem::named("runtime-subsystem")),
    )
    .unwrap();
    assert!(db.is_lifecycle_complete());

    drop(db);
    OPEN_DATABASES.lock().clear();
}
// ============================================================================
// T3-E6: Shutdown Hardening — acceptance tests
// ============================================================================

/// Test subsystem that counts how many times `freeze()` is invoked. Used to
/// prove that a timed-out shutdown does NOT run subsystem freeze hooks.
struct FreezeCountingSubsystem {
    name: &'static str,
    freeze_calls: Arc<AtomicUsize>,
}

impl FreezeCountingSubsystem {
    fn new(name: &'static str) -> (Self, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        (
            Self {
                name,
                freeze_calls: counter.clone(),
            },
            counter,
        )
    }
}

impl Subsystem for FreezeCountingSubsystem {
    fn name(&self) -> &'static str {
        self.name
    }
    fn recover(&self, _db: &Arc<Database>) -> StrataResult<()> {
        Ok(())
    }
    fn freeze(&self, _db: &Database) -> StrataResult<()> {
        self.freeze_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
#[serial(open_databases)]
fn shutdown_twice_is_idempotent() {
    OPEN_DATABASES.lock().clear();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("idempotent_shutdown");
    let db = Database::open(&db_path).unwrap();

    db.shutdown().expect("first shutdown must succeed");
    db.shutdown().expect("second shutdown must be a no-op");
}

#[test]
#[serial(open_databases)]
fn drop_without_shutdown_still_cleans_up() {
    OPEN_DATABASES.lock().clear();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drop_fallback");

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "drop_key");

    {
        let db = Database::open(&db_path).unwrap();
        let mut txn = db.begin_transaction(branch_id).unwrap();
        txn.put(key.clone(), Value::Bytes(b"drop_value".to_vec()))
            .unwrap();
        db.commit_transaction(&mut txn).unwrap();
        db.end_transaction(txn);
        // Intentionally no shutdown() — rely on Drop flush+freeze fallback.
    }
    OPEN_DATABASES.lock().clear();

    let db = Database::open(&db_path).unwrap();
    let val = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert_eq!(val.unwrap().value, Value::Bytes(b"drop_value".to_vec()));
}

#[test]
#[serial(open_databases)]
fn shutdown_timeout_returns_error_and_skips_freeze() {
    OPEN_DATABASES.lock().clear();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("shutdown_timeout");

    let (freeze_sub, freeze_count) = FreezeCountingSubsystem::new("freeze-counter");
    let spec = OpenSpec::primary(&db_path)
        .with_subsystem(crate::search::SearchSubsystem)
        .with_subsystem(freeze_sub);
    let db = Database::open_runtime(spec).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "stuck_key");

    // Hold a transaction open past the deadline; release after shutdown returns.
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let db2 = Arc::clone(&db);
    let key2 = key.clone();
    let handle = std::thread::spawn(move || {
        let mut txn = db2.begin_transaction(branch_id).unwrap();
        started_tx.send(()).unwrap();
        // Block until main thread releases us.
        release_rx.recv().unwrap();
        txn.put(key2, Value::Bytes(b"late".to_vec())).unwrap();
        db2.commit_transaction(&mut txn).unwrap();
        db2.end_transaction(txn);
    });
    started_rx.recv().unwrap();

    let result = db.shutdown_with_deadline(Duration::from_millis(100));
    match result {
        Err(StrataError::ShutdownTimeout { active_txn_count }) => {
            assert!(
                active_txn_count >= 1,
                "expected at least 1 active txn, got {}",
                active_txn_count
            );
        }
        other => panic!("expected ShutdownTimeout, got {:?}", other),
    }

    // Freeze hooks must NOT have run on the timeout path.
    assert_eq!(
        freeze_count.load(Ordering::SeqCst),
        0,
        "freeze hooks must not run after a shutdown timeout"
    );

    // Releasing the blocked txn and retrying succeeds; freeze then runs once.
    release_tx.send(()).unwrap();
    handle.join().unwrap();
    db.shutdown_with_deadline(Duration::from_secs(5))
        .expect("retry after txn completes must succeed");
    assert_eq!(
        freeze_count.load(Ordering::SeqCst),
        1,
        "successful retry must run freeze hooks exactly once"
    );
}

#[test]
#[serial(open_databases)]
fn shutdown_releases_registry_slot_and_allows_fresh_reopen() {
    // T3-E6 contract: a successful `shutdown()` must release BOTH the
    // in-process `OPEN_DATABASES` slot AND the on-disk `.lock` flock so a
    // fresh `Database::open` on the same path succeeds immediately — without
    // waiting for `Drop` on the old `Arc<Database>`. Anything less means
    // reopen is gated on arbitrary Arc-holder cleanup timing.
    OPEN_DATABASES.lock().clear();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("registry_release");

    let db = Database::open(&db_path).unwrap();
    let canonical_key: std::path::PathBuf = db.data_dir().to_path_buf();
    assert!(
        OPEN_DATABASES.lock().contains_key(&canonical_key),
        "registry must have an entry while db is open"
    );

    db.shutdown().unwrap();

    // The in-process registry slot must be released before Drop runs.
    assert!(
        !OPEN_DATABASES.lock().contains_key(&canonical_key),
        "successful shutdown must release the registry slot without waiting for Drop"
    );

    // The OS-level `.lock` file must also be released: a fresh open on the
    // same path must succeed even though we still hold the old
    // `Arc<Database>`. Before the file-lock-release fix this failed with
    // "database is already in use by another process".
    let db_reopen = Database::open(&db_path)
        .expect("fresh Database::open must succeed after shutdown without waiting for Drop");
    assert!(!Arc::ptr_eq(&db, &db_reopen));
    drop(db_reopen);
    drop(db);
}

#[test]
#[serial(open_databases)]
fn drop_releases_registry_slot_if_shutdown_skipped() {
    // Drop's removal from OPEN_DATABASES is a `try_lock` best-effort because
    // cascading drops during recovery failure must not self-deadlock against
    // `acquire_primary_db`'s guard. The semantic guarantee is that a
    // subsequent `Database::open` for the same path succeeds (either the
    // entry was released, or it's stale-Weak and gets overwritten on insert).
    // Verify that guarantee end-to-end.
    OPEN_DATABASES.lock().clear();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drop_registry_release");

    {
        let db = Database::open(&db_path).unwrap();
        let mut txn = db.begin_transaction(BranchId::new()).unwrap();
        db.commit_transaction(&mut txn).unwrap();
        db.end_transaction(txn);
        // No shutdown() — rely on Drop.
    }

    // Re-open must succeed without error. This exercises either the
    // clean-release path or the stale-Weak-overwrite fallback path.
    let db2 = Database::open(&db_path).expect(
        "reopen after Drop-only cleanup must succeed via registry release or stale overwrite",
    );
    drop(db2);
}

#[test]
#[serial(open_databases)]
fn shutdown_fsyncs_manifest() {
    OPEN_DATABASES.lock().clear();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("shutdown_manifest");
    let db = Database::open(&db_path).unwrap();

    db.shutdown().unwrap();

    let manifest_path = db_path.join("MANIFEST");
    assert!(
        strata_durability::ManifestManager::exists(&manifest_path),
        "shutdown must leave the MANIFEST present on disk"
    );
    strata_durability::ManifestManager::load(manifest_path)
        .expect("persisted MANIFEST must parse after shutdown fsync");
}

/// Subsystem whose `freeze()` fails while `fail_once` is `true` and then flips
/// the flag off so the next invocation succeeds. Used to exercise the retry
/// path after a freeze error.
struct FlakyFreezeSubsystem {
    name: &'static str,
    fail_once: Arc<AtomicBool>,
    freeze_calls: Arc<AtomicUsize>,
}

impl FlakyFreezeSubsystem {
    fn new(name: &'static str) -> (Self, Arc<AtomicBool>, Arc<AtomicUsize>) {
        let fail_once = Arc::new(AtomicBool::new(true));
        let counter = Arc::new(AtomicUsize::new(0));
        (
            Self {
                name,
                fail_once: fail_once.clone(),
                freeze_calls: counter.clone(),
            },
            fail_once,
            counter,
        )
    }
}

impl Subsystem for FlakyFreezeSubsystem {
    fn name(&self) -> &'static str {
        self.name
    }
    fn recover(&self, _db: &Arc<Database>) -> StrataResult<()> {
        Ok(())
    }
    fn freeze(&self, _db: &Database) -> StrataResult<()> {
        self.freeze_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_once.swap(false, Ordering::SeqCst) {
            Err(StrataError::internal("injected freeze failure".to_string()))
        } else {
            Ok(())
        }
    }
}

#[test]
#[serial(open_databases)]
fn shutdown_freeze_failure_is_not_marked_complete() {
    // A subsystem freeze error must not be misclassified as a successful
    // shutdown: the registry slot must stay claimed, subsequent shutdown()
    // calls must re-run (not short-circuit), and the Drop fallback must still
    // get a chance to finish the work. Retry must succeed once the underlying
    // fault clears because every step of the shutdown path is idempotent.
    OPEN_DATABASES.lock().clear();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("flaky_freeze");

    let (flaky, _fail_once, counter) = FlakyFreezeSubsystem::new("flaky-freeze");
    let spec = OpenSpec::primary(&db_path)
        .with_subsystem(crate::search::SearchSubsystem)
        .with_subsystem(flaky);
    let db = Database::open_runtime(spec).unwrap();
    let canonical_key = db.data_dir().to_path_buf();

    // First shutdown: freeze fails → error is surfaced, state is NOT completed.
    let err = db
        .shutdown()
        .expect_err("first shutdown must surface the freeze failure");
    assert!(
        matches!(err, StrataError::Internal { .. }),
        "unexpected error kind: {:?}",
        err
    );
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "freeze must have run once"
    );
    assert!(
        OPEN_DATABASES.lock().contains_key(&canonical_key),
        "registry slot must stay claimed when shutdown did not complete"
    );

    // Second shutdown: idempotency short-circuit must NOT fire; the injected
    // fault has cleared, so this attempt succeeds and finishes the close.
    db.shutdown()
        .expect("retry after freeze failure clears must succeed");
    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "retry must re-run freeze (no false idempotency)"
    );
    assert!(
        !OPEN_DATABASES.lock().contains_key(&canonical_key),
        "successful retry must release the registry slot"
    );

    drop(db);
}

#[test]
#[serial(open_databases)]
fn follower_shutdown_does_not_evict_primary_registry_entry() {
    // Followers are not deduplicated via OPEN_DATABASES; only the primary owns
    // the path's slot. Follower shutdown must therefore never call
    // `release_registry_slot`, otherwise a live primary at the same path loses
    // its entry and subsequent `Database::open` no longer reuses it.
    OPEN_DATABASES.lock().clear();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("follower_vs_primary");

    let primary = Database::open(&db_path).unwrap();
    let canonical_key = primary.data_dir().to_path_buf();
    let follower = Database::open_runtime(
        OpenSpec::follower(&db_path).with_subsystem(crate::search::SearchSubsystem),
    )
    .expect("follower must open alongside the primary");
    assert!(follower.is_follower());

    // Hold the `OPEN_DATABASES` lock across the follower shutdown. With the
    // follower-path fix in `shutdown_with_deadline`, follower shutdown never
    // touches the registry, so this cannot deadlock — and it makes the
    // assertion immune to unrelated tests that call
    // `OPEN_DATABASES.lock().clear()` for their own isolation.
    let registry = OPEN_DATABASES.lock();
    assert!(
        registry.contains_key(&canonical_key),
        "primary must be registered before follower shutdown"
    );
    follower.shutdown().expect("follower shutdown must succeed");
    assert!(
        registry.contains_key(&canonical_key),
        "follower shutdown must not evict the primary's registry entry"
    );
    drop(registry);

    drop(follower);
    primary.shutdown().unwrap();
    drop(primary);
}

#[test]
#[serial(open_databases)]
fn shutdown_timeout_leaves_database_usable_for_new_transactions() {
    // The scope requires a timed-out shutdown to leave the database usable so
    // the caller can finish outstanding work and retry. In particular, new
    // `begin_transaction` calls must succeed after the timeout error.
    OPEN_DATABASES.lock().clear();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("timeout_usable");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let blocker_key = Key::new_kv(ns.clone(), "blocker");
    let post_key = Key::new_kv(ns, "post_timeout");

    // Hold a long-lived txn open to force the wait_for_idle timeout.
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let db_bg = Arc::clone(&db);
    let blocker_key_bg = blocker_key.clone();
    let handle = std::thread::spawn(move || {
        let mut txn = db_bg.begin_transaction(branch_id).unwrap();
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        txn.put(blocker_key_bg, Value::Bytes(b"done".to_vec()))
            .unwrap();
        db_bg.commit_transaction(&mut txn).unwrap();
        db_bg.end_transaction(txn);
    });
    started_rx.recv().unwrap();

    let err = db
        .shutdown_with_deadline(Duration::from_millis(100))
        .expect_err("shutdown must time out while the blocker txn is held");
    assert!(
        matches!(err, StrataError::ShutdownTimeout { .. }),
        "unexpected error: {:?}",
        err
    );

    // After the timeout the caller must be able to start NEW transactions,
    // not only let pre-existing ones commit. This is the "leave the database
    // usable" requirement from the T3-E6 scope.
    let mut new_txn = db
        .begin_transaction(branch_id)
        .expect("new transactions must be allowed after a shutdown timeout");
    new_txn
        .put(post_key.clone(), Value::Bytes(b"after_timeout".to_vec()))
        .unwrap();
    db.commit_transaction(&mut new_txn).unwrap();
    db.end_transaction(new_txn);

    // Release the blocker so the retry below can drain cleanly.
    release_tx.send(()).unwrap();
    handle.join().unwrap();

    // Verify the post-timeout write is visible in live storage before
    // shutdown — this fully proves the database was usable after the timeout.
    let post_value = db
        .storage()
        .get_versioned(&post_key, CommitVersion::MAX)
        .unwrap();
    assert_eq!(
        post_value.unwrap().value,
        Value::Bytes(b"after_timeout".to_vec()),
        "transaction started after shutdown timeout must be visible"
    );
    let pre_value = db
        .storage()
        .get_versioned(&blocker_key, CommitVersion::MAX)
        .unwrap();
    assert_eq!(pre_value.unwrap().value, Value::Bytes(b"done".to_vec()));

    db.shutdown_with_deadline(Duration::from_secs(5))
        .expect("retry must succeed once every txn has drained");
    drop(db);
}

#[test]
#[serial(open_databases)]
fn shutdown_timeout_preserves_writer_halt_signal() {
    // The WAL flush thread uses `accepting_transactions = false` as a
    // published signal that the writer has halted after a sync failure
    // (see `open.rs:756`). If a sync failure halts the writer while
    // `shutdown_with_deadline` is blocked on `wait_for_idle`, the timeout
    // path must NOT restore `accepting_transactions = true` — doing so
    // would erase the halt signal and let new `begin_transaction` calls
    // past `check_accepting`, failing later at commit. The halt must keep
    // winning.
    OPEN_DATABASES.lock().clear();

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("shutdown_timeout_vs_halt");
    super::test_hooks::clear_sync_failure(&db_path);
    let db = Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let blocker_key = Key::new_kv(ns.clone(), "blocker");

    // Hold a long-lived transaction open to force the wait_for_idle timeout.
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let db_bg = Arc::clone(&db);
    let blocker_key_bg = blocker_key.clone();
    let handle = std::thread::spawn(move || {
        let mut txn = db_bg.begin_transaction(branch_id).unwrap();
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        txn.put(blocker_key_bg, Value::Bytes(b"done".to_vec()))
            .unwrap();
        // Commit is allowed to fail — the writer may have halted by then.
        let _ = db_bg.commit_transaction(&mut txn);
        db_bg.end_transaction(txn);
    });
    started_rx.recv().unwrap();

    // Inject a sync failure and commit a txn that forces a background sync,
    // so the flush thread halts the writer while our blocker is still open.
    super::test_hooks::inject_sync_failure(&db_path, std::io::ErrorKind::Other);
    db.transaction(branch_id, |txn| {
        txn.put(Key::new_kv(ns.clone(), "halt_trigger"), Value::Int(1))?;
        Ok(())
    })
    .unwrap();
    wait_until(Duration::from_secs(2), || {
        matches!(db.wal_writer_health(), WalWriterHealth::Halted { .. })
    });
    assert!(
        matches!(db.wal_writer_health(), WalWriterHealth::Halted { .. }),
        "writer must be halted before we run the timed-out shutdown"
    );

    // Run shutdown: the blocker txn is still open, so we'll time out.
    let err = db
        .shutdown_with_deadline(Duration::from_millis(100))
        .expect_err("shutdown must time out while blocker txn is held");
    assert!(
        matches!(err, StrataError::ShutdownTimeout { .. }),
        "unexpected error: {:?}",
        err
    );

    // The halt signal must still be observable on new transactions — the
    // timeout restore path must not have overwritten it with `true`.
    assert!(
        matches!(db.wal_writer_health(), WalWriterHealth::Halted { .. }),
        "writer health must still show Halted after shutdown timeout"
    );
    match db.begin_transaction(branch_id) {
        Ok(_) => panic!("new transaction must be rejected while writer is halted"),
        Err(StrataError::WriterHalted { .. }) => {}
        Err(other) => panic!(
            "expected WriterHalted (halt signal preserved); got {:?}",
            other
        ),
    }

    // Clean up: clear the fault, release the blocker, drop the DB. We don't
    // attempt a successful shutdown retry here because the WAL is halted
    // and that recovery path (`resume_wal_writer`) is out of scope for this
    // test — the Drop fallback will handle teardown.
    super::test_hooks::clear_sync_failure(&db_path);
    release_tx.send(()).unwrap();
    handle.join().unwrap();
    drop(db);
    OPEN_DATABASES.lock().clear();
}

#[test]
// Serialize against the `open_databases` tests, which mutate the shared
// `OPEN_DATABASES` registry. The sync-failure hook itself is path-scoped,
// so it no longer requires global test serialization.
#[serial(open_databases)]
#[serial]
fn shutdown_timeout_halt_interleaving_preserves_invariant() {
    // Stress regression for a TOCTOU race between the shutdown-timeout
    // cleanup and the WAL flush thread's halt publishing (two-phase: set
    // `WalWriterHealth::Halted` under health-lock, drop lock, then
    // `accepting_transactions.store(false)`). A naive check-then-restore
    // can read `Healthy`, then the flush thread completes its full halt
    // sequence, and our subsequent `store(true)` erases the halt.
    //
    // The fix holds `wal_writer_health.lock()` across the observation and
    // the restore so the publisher cannot advance its first phase during
    // our critical section, and a halt that lands after our release runs
    // its full two-phase sequence — its final `store(false)` wins over our
    // `store(true)` because it is ordered after our lock release.
    //
    // This test schedules the sync-failure injection concurrently with a
    // short-deadline shutdown across many iterations, attempting to land
    // the halt within or just before the timeout-cleanup window. On every
    // iteration the invariant is: if `wal_writer_health` is `Halted` after
    // shutdown returns, `begin_transaction` must return `WriterHalted`
    // (never `Ok`).
    const ITERATIONS: usize = 24;
    let mut halt_observed = 0usize;

    for i in 0..ITERATIONS {
        OPEN_DATABASES.lock().clear();

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join(format!("halt_race_{}", i));
        super::test_hooks::clear_sync_failure(&db_path);
        let db =
            Database::open_with_durability(&db_path, DurabilityMode::standard_default()).unwrap();
        // Tight sync interval so the background flush thread has time to
        // observe the sync failure and publish the halt within the
        // shutdown deadline.
        db.set_durability_mode(DurabilityMode::Standard {
            interval_ms: 5,
            batch_size: 64,
        })
        .unwrap();

        let branch_id = BranchId::new();
        let ns = create_test_namespace(branch_id);
        let blocker_key = Key::new_kv(ns.clone(), "blocker");
        let trigger_key = Key::new_kv(ns.clone(), "halt_trigger");

        // Stage the sync failure BEFORE shutdown. The trigger commit
        // queues dirty WAL bytes, but the actual `sync_all` happens on the
        // next flush-thread tick (every 5ms). By varying how long after
        // shutdown starts we set things up, the halt publishes at
        // different points relative to the timeout-cleanup window.
        super::test_hooks::inject_sync_failure(&db_path, std::io::ErrorKind::Other);
        db.transaction(branch_id, |txn| {
            txn.put(trigger_key.clone(), Value::Int(i as i64))?;
            Ok(())
        })
        .unwrap();

        // Blocker txn — keeps `wait_for_idle` busy so shutdown must time out.
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let db_blocker = Arc::clone(&db);
        let blocker_key_bg = blocker_key.clone();
        let blocker_handle = std::thread::spawn(move || {
            let mut txn = db_blocker.begin_transaction(branch_id).unwrap();
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            txn.put(blocker_key_bg, Value::Bytes(b"done".to_vec()))
                .unwrap();
            let _ = db_blocker.commit_transaction(&mut txn);
            db_blocker.end_transaction(txn);
        });
        started_rx.recv().unwrap();

        // Per-iteration jitter before shutdown positions the halt so it
        // has to publish *during* or *just after* the timeout-cleanup
        // runs. 0..=7 ms covers a range around the 5 ms flush interval so
        // at least some iterations will land the halt inside the critical
        // window where the race would manifest.
        std::thread::sleep(Duration::from_millis(i as u64 % 8));

        // Timed-out shutdown — whether or not the halt lands within the
        // cleanup window, the post-return invariant must hold. Very short
        // deadline so cleanup runs close to the halt publication.
        let err = db
            .shutdown_with_deadline(Duration::from_millis(2))
            .expect_err("shutdown must time out while blocker is held");
        assert!(
            matches!(err, StrataError::ShutdownTimeout { .. }),
            "iteration {}: unexpected error {:?}",
            i,
            err
        );

        // Give the flush thread a small chance to publish the halt if it
        // was pending when shutdown returned. We measure state *after* the
        // timeout cleanup has run, which is the window where the race
        // would manifest (health = Halted, accepting = true).
        std::thread::sleep(Duration::from_millis(30));

        // Invariant: if the writer halted, `begin_transaction` must reject
        // new admissions with `WriterHalted`. The race would allow a stray
        // `Ok` here — that is the bug.
        if matches!(db.wal_writer_health(), WalWriterHealth::Halted { .. }) {
            halt_observed += 1;
            match db.begin_transaction(branch_id) {
                Ok(_) => panic!(
                    "iteration {}: begin_transaction returned Ok despite Halted writer — \
                     shutdown-timeout cleanup clobbered the halt signal",
                    i
                ),
                Err(StrataError::WriterHalted { .. }) => {}
                Err(other) => panic!("iteration {}: expected WriterHalted, got {:?}", i, other),
            }
        }

        // Clean up.
        super::test_hooks::clear_sync_failure(&db_path);
        release_tx.send(()).unwrap();
        blocker_handle.join().unwrap();
        drop(db);
    }
    // `halt_handle` no longer exists — trigger is committed inline.

    OPEN_DATABASES.lock().clear();
    assert!(
        halt_observed > 0,
        "stress test must land at least one halt across {} iterations to be meaningful",
        ITERATIONS
    );
}

#[test]
#[serial(open_databases)]
fn shutdown_closes_mutating_maintenance_apis() {
    // After a successful `shutdown()`, the registry slot and the on-disk
    // `.lock` file are both released, so a fresh `Database::open` on the
    // same path can start writing. The old `Arc<Database>` must therefore
    // reject every mutating maintenance API — otherwise it could race the
    // fresh instance and corrupt its WAL / MANIFEST / storage state.
    // The transaction gate already rejects `begin_transaction`; this test
    // covers the non-transactional maintenance surface.
    OPEN_DATABASES.lock().clear();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("maintenance_after_shutdown");
    let db = Database::open(&db_path).unwrap();

    db.shutdown().unwrap();

    // Result-returning APIs must return a clear "closed" error rather than
    // silently succeeding on the stale handle.
    fn assert_closed(res: StrataResult<()>, api: &str) {
        match res {
            Ok(()) => panic!("{} must be rejected after shutdown; got Ok(())", api),
            Err(StrataError::InvalidInput { message })
                if message.contains("Database has been shut down") => {}
            Err(other) => panic!("{}: unexpected error {:?}", api, other),
        }
    }
    assert_closed(db.flush(), "flush");
    assert_closed(db.checkpoint(), "checkpoint");
    assert_closed(db.compact(), "compact");
    assert_closed(
        db.set_durability_mode(DurabilityMode::Always),
        "set_durability_mode",
    );
    assert_closed(db.freeze_vector_heaps(), "freeze_vector_heaps");
    assert_closed(
        db.update_config(|cfg| {
            cfg.auto_embed = !cfg.auto_embed;
        }),
        "update_config",
    );

    // Tuple-returning APIs must early-return zeros rather than exercising
    // the underlying storage on the closed handle.
    assert_eq!(db.run_gc(), (0, 0), "run_gc must no-op on a closed handle");
    assert_eq!(
        db.run_maintenance(),
        (0, 0, 0),
        "run_maintenance must no-op on a closed handle"
    );

    // `set_auto_embed` inherits the `update_config` guard but discards the
    // error by design (its signature is infallible). Verify that means it
    // leaves the on-handle view of the flag unchanged after a closed
    // shutdown rather than silently reporting success with stale state.
    let before = db.auto_embed_enabled();
    db.set_auto_embed(!before);
    assert_eq!(
        db.auto_embed_enabled(),
        before,
        "set_auto_embed must silently no-op on a closed handle — on-handle flag stays unchanged"
    );

    drop(db);
}

#[test]
#[serial(open_databases)]
fn shutdown_in_progress_rejects_maintenance_apis() {
    // The authoritative ordered close barrier must reject non-transactional
    // maintenance APIs AS SOON AS `shutdown_started = true`, not only after
    // `shutdown_complete = true`. Otherwise `flush`, `checkpoint`,
    // `compact`, `update_config`, `set_durability_mode`, `run_gc`, and
    // `run_maintenance` can run concurrently with shutdown's own final
    // flush / MANIFEST fsync / freeze loop. The sharpest case is
    // `set_durability_mode`, which can stop and restart the flush thread
    // while shutdown is also manipulating it.
    //
    // Force shutdown into a held-open state by blocking `wait_for_idle` on
    // a long-lived transaction in another thread, call `shutdown()` with a
    // long deadline in a second thread (it will park on the idle wait),
    // observe `shutdown_started = true` from the main thread, then assert
    // every maintenance API is rejected.
    OPEN_DATABASES.lock().clear();
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("shutdown_in_progress_guards");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let blocker_key = Key::new_kv(ns, "blocker");

    // Hold a transaction open to keep `wait_for_idle` busy.
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let db_blocker = Arc::clone(&db);
    let blocker_key_bg = blocker_key.clone();
    let blocker_handle = std::thread::spawn(move || {
        let mut txn = db_blocker.begin_transaction(branch_id).unwrap();
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        txn.put(blocker_key_bg, Value::Bytes(b"done".to_vec()))
            .unwrap();
        db_blocker.commit_transaction(&mut txn).unwrap();
        db_blocker.end_transaction(txn);
    });
    started_rx.recv().unwrap();

    // Call shutdown in another thread. A long deadline keeps shutdown
    // parked inside `wait_for_idle` with `shutdown_started = true` and
    // `shutdown_complete = false` — exactly the window under test.
    let db_shutter = Arc::clone(&db);
    let shutdown_handle = std::thread::spawn(move || {
        db_shutter
            .shutdown_with_deadline(Duration::from_secs(10))
            .expect("shutdown must succeed once the blocker releases")
    });

    // Wait for shutdown_started to be observable. `accepting_transactions`
    // flips at the same time — use it as a proxy to synchronize without a
    // test-only hook.
    wait_until(Duration::from_secs(2), || !db.is_open());
    assert!(
        !db.is_open(),
        "shutdown must have started and flipped the accepting flag"
    );

    // Every mutating maintenance API must be rejected while shutdown is
    // parked on `wait_for_idle` — before it ever reaches its own
    // flush/freeze/registry-release phase.
    fn assert_in_progress(res: StrataResult<()>, api: &str) {
        match res {
            Ok(()) => panic!("{} must be rejected while shutdown is in progress", api),
            Err(StrataError::InvalidInput { message })
                if message.contains("shutting down") || message.contains("shut down") => {}
            Err(other) => panic!("{}: unexpected error {:?}", api, other),
        }
    }
    assert_in_progress(db.flush(), "flush");
    assert_in_progress(db.checkpoint(), "checkpoint");
    assert_in_progress(db.compact(), "compact");
    assert_in_progress(
        db.set_durability_mode(DurabilityMode::Always),
        "set_durability_mode",
    );
    assert_in_progress(
        db.update_config(|cfg| {
            cfg.auto_embed = !cfg.auto_embed;
        }),
        "update_config",
    );
    assert_eq!(
        db.run_gc(),
        (0, 0),
        "run_gc must no-op while shutdown is in progress"
    );
    assert_eq!(
        db.run_maintenance(),
        (0, 0, 0),
        "run_maintenance must no-op while shutdown is in progress"
    );

    // Let shutdown drain and complete.
    release_tx.send(()).unwrap();
    blocker_handle.join().unwrap();
    shutdown_handle.join().unwrap();

    // After successful shutdown the same guards still reject (keying on
    // `shutdown_complete` now). Sanity-check one API.
    assert_in_progress(db.flush(), "flush (post-completion)");

    drop(db);
}
