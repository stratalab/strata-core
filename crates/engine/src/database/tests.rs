use super::*;
use crate::recovery::Subsystem;
use serial_test::serial;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use strata_concurrency::TransactionPayload;
use strata_concurrency::__internal as concurrency_test_hooks;
use strata_core::id::{CommitVersion, TxnId};
use strata_core::types::{Key, Namespace, TypeTag};
use strata_core::value::Value;
use strata_core::{Storage, StrataError, Timestamp, WriteMode};
use strata_durability::__internal::WalWriterEngineExt;
use strata_durability::codec::IdentityCodec;
use strata_durability::format::WalRecord;
use strata_durability::now_micros;
use strata_durability::wal::WalConfig;
use strata_durability::KvSnapshotEntry;
use tempfile::TempDir;

impl Database {
    /// Test-only helper: open with a specific durability mode.
    fn open_with_durability<P: AsRef<Path>>(
        path: P,
        durability_mode: DurabilityMode,
    ) -> StrataResult<Arc<Self>> {
        let dur_str = match durability_mode {
            DurabilityMode::Always => "always",
            DurabilityMode::Cache => "cache",
            _ => "standard",
        };
        let cfg = StrataConfig {
            durability: dur_str.to_string(),
            ..StrataConfig::default()
        };
        let spec = super::spec::OpenSpec::primary(path)
            .with_config(cfg)
            .with_subsystem(crate::search::SearchSubsystem);
        Self::open_runtime(spec)
    }
}

/// Helper: write a committed transaction to the segmented WAL
fn write_wal_txn(
    wal_dir: &std::path::Path,
    _txn_id: u64,
    branch_id: BranchId,
    puts: Vec<(Key, Value)>,
    deletes: Vec<Key>,
    version: u64,
) {
    let mut wal = WalWriter::new(
        wal_dir.to_path_buf(),
        [0u8; 16],
        DurabilityMode::Always,
        WalConfig::for_testing(),
        Box::new(IdentityCodec),
    )
    .unwrap();

    let payload = TransactionPayload {
        version,
        puts,
        deletes,
        put_ttls: vec![],
    };
    // Use version (commit_version) as WAL record ordering key (#1696)
    let record = WalRecord::new(
        TxnId(version),
        *branch_id.as_bytes(),
        now_micros(),
        payload.to_bytes(),
    );
    wal.append(&record).unwrap();
    wal.flush().unwrap();
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if predicate() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("condition not satisfied within {:?}", timeout);
}

#[test]
fn test_open_creates_directory() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("new_db");

    assert!(!db_path.exists());
    let _db = Database::open(&db_path).unwrap();
    assert!(db_path.exists());
}

#[test]
fn test_ephemeral_no_files() {
    let db = Database::cache().unwrap();

    // Should work for operations
    assert!(db.is_cache());
}

#[test]
fn test_wal_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    // Write directly to segmented WAL (simulating a crash recovery scenario)
    {
        let wal_dir = db_path.join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        write_wal_txn(
            &wal_dir,
            1,
            branch_id,
            vec![(
                Key::new_kv(ns.clone(), "key1"),
                Value::Bytes(b"value1".to_vec()),
            )],
            vec![],
            1,
        );
    }

    // Open database (should replay WAL)
    let db = Database::open(&db_path).unwrap();

    // Storage should have data from WAL
    let key1 = Key::new_kv(ns, "key1");
    let val = db
        .storage()
        .get_versioned(&key1, CommitVersion::MAX)
        .unwrap()
        .unwrap();

    if let Value::Bytes(bytes) = val.value {
        assert_eq!(bytes, b"value1");
    } else {
        panic!("Wrong value type");
    }
}

#[test]
fn test_open_close_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    // Open database, write via transaction, close
    {
        let db = Database::open(&db_path).unwrap();

        db.transaction(branch_id, |txn| {
            txn.put(
                Key::new_kv(ns.clone(), "persistent"),
                Value::Bytes(b"data".to_vec()),
            )?;
            Ok(())
        })
        .unwrap();

        db.flush().unwrap();
    }

    // Reopen database
    {
        let db = Database::open(&db_path).unwrap();

        // Data should be restored from WAL
        let key = Key::new_kv(ns, "persistent");
        let val = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .unwrap();

        if let Value::Bytes(bytes) = val.value {
            assert_eq!(bytes, b"data");
        } else {
            panic!("Wrong value type");
        }
    }
}

#[test]
fn test_partial_record_discarded() {
    // With the segmented WAL, partial records (crash mid-write) are
    // automatically discarded by the reader. There are no "incomplete
    // transactions" since each WalRecord = one committed transaction.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    // Write one valid record, then append garbage to simulate crash
    {
        let wal_dir = db_path.join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        write_wal_txn(
            &wal_dir,
            1,
            branch_id,
            vec![(Key::new_kv(ns.clone(), "valid"), Value::Int(42))],
            vec![],
            1,
        );

        // Append garbage to simulate crash mid-write of second record
        let segment_path = strata_durability::format::WalSegment::segment_path(&wal_dir, 1);
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&segment_path)
            .unwrap();
        file.write_all(&[0xFF; 20]).unwrap();
    }

    // Open database (should recover valid record, skip garbage)
    let db = Database::open(&db_path).unwrap();

    // Valid transaction should be present
    let key = Key::new_kv(ns.clone(), "valid");
    let val = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(val.value, Value::Int(42));
}

#[test]
fn test_corrupted_wal_handled_gracefully() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    // Create WAL directory but with no valid segment files
    // (the segmented WAL will find no segments and return empty)
    {
        std::fs::create_dir_all(db_path.join("wal")).unwrap();
    }

    // Open should succeed with empty storage (no segments found)
    let result = Database::open(&db_path);
    assert!(result.is_ok());

    let db = result.unwrap();
    // Storage has only system branch init transactions (no user data recovered)
    // init_system_branch writes 3 transactions: branch, graph, node
    assert!(db.storage().current_version() <= CommitVersion(3));
}

#[test]
fn test_open_with_different_durability_modes() {
    let temp_dir = TempDir::new().unwrap();

    // Always mode
    {
        let db =
            Database::open_with_durability(temp_dir.path().join("strict"), DurabilityMode::Always)
                .unwrap();
        assert!(!db.is_cache());
    }

    // Standard mode
    {
        let db = Database::open_with_durability(
            temp_dir.path().join("batched"),
            DurabilityMode::Standard {
                interval_ms: 100,
                batch_size: 1000,
            },
        )
        .unwrap();
        assert!(!db.is_cache());
    }

    // Cache mode
    {
        let db =
            Database::open_with_durability(temp_dir.path().join("none"), DurabilityMode::Cache)
                .unwrap();
        assert!(!db.is_cache());
    }
}

#[test]
fn test_flush() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let db = Database::open(&db_path).unwrap();

    // Flush should succeed
    assert!(db.flush().is_ok());
}

// ========================================================================
// Transaction API Tests
// ========================================================================

fn create_test_namespace(branch_id: BranchId) -> Arc<Namespace> {
    Arc::new(Namespace::new(branch_id, "default".to_string()))
}

#[test]
fn test_transaction_closure_api() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "test_key");

    // Execute transaction
    let result = db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(42))?;
        Ok(())
    });

    assert!(result.is_ok());

    // Verify data was committed
    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(stored.value, Value::Int(42));
}

#[test]
fn test_transaction_returns_closure_value() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "test_key");

    // Pre-populate using transaction
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(100))?;
        Ok(())
    })
    .unwrap();

    // Transaction returns a value
    let result: StrataResult<i64> = db.transaction(branch_id, |txn| {
        let val = txn.get(&key)?.unwrap();
        if let Value::Int(n) = val {
            Ok(n)
        } else {
            Err(StrataError::invalid_input("wrong type".to_string()))
        }
    });

    assert_eq!(result.unwrap(), 100);
}

#[test]
fn test_transaction_read_your_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "ryw_key");

    // Per spec Section 2.1: "Its own uncommitted writes - always visible"
    let result: StrataResult<Value> = db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::String("written".to_string()))?;

        // Should see our own write
        let val = txn.get(&key)?.unwrap();
        Ok(val)
    });

    assert_eq!(result.unwrap(), Value::String("written".to_string()));
}

#[test]
fn test_transaction_aborts_on_closure_error() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "abort_key");

    // Transaction that errors
    let result: StrataResult<()> = db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(999))?;
        Err(StrataError::invalid_input("intentional error".to_string()))
    });

    assert!(result.is_err());

    // Data should NOT be committed
    assert!(db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .is_none());
}

#[test]
fn test_begin_and_commit_manual() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "manual_key");

    // Manual transaction control
    let mut txn = db.begin_transaction(branch_id).unwrap();
    txn.put(key.clone(), Value::Int(123)).unwrap();

    // Commit manually
    db.commit_transaction(&mut txn).unwrap();

    // Verify committed
    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(stored.value, Value::Int(123));
}

#[test]
fn test_owned_transaction_commit_emits_one_commit_observer_event() {
    struct CountingCommitObserver {
        count: AtomicUsize,
    }

    impl super::CommitObserver for CountingCommitObserver {
        fn name(&self) -> &'static str {
            "counting-commit"
        }

        fn on_commit(&self, _info: &super::CommitInfo) -> Result<(), super::ObserverError> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let observer = Arc::new(CountingCommitObserver {
        count: AtomicUsize::new(0),
    });
    db.commit_observers().register(observer.clone());

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "observer_key");

    let mut txn = db.begin_transaction(branch_id).unwrap();
    txn.put(key.clone(), Value::Int(1)).unwrap();
    let version = txn.commit().unwrap();

    assert!(version > 0);
    assert_eq!(
        observer.count.load(Ordering::SeqCst),
        1,
        "successful manual commits should emit exactly one observer event"
    );
}

// ========================================================================
// Graceful Shutdown Tests
// ========================================================================

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

// ========================================================================
// Singleton Registry Tests
// ========================================================================

#[test]
fn test_open_same_path_returns_same_instance() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("singleton_test");

    // Open database twice with same path
    let db1 = Database::open(&db_path).unwrap();
    let db2 = Database::open(&db_path).unwrap();

    // Both should be the same Arc (same pointer)
    assert!(Arc::ptr_eq(&db1, &db2));
}

#[test]
fn test_open_different_paths_returns_different_instances() {
    let temp_dir = TempDir::new().unwrap();
    let path1 = temp_dir.path().join("db1");
    let path2 = temp_dir.path().join("db2");

    let db1 = Database::open(&path1).unwrap();
    let db2 = Database::open(&path2).unwrap();

    // Should be different instances
    assert!(!Arc::ptr_eq(&db1, &db2));
}

#[test]
fn test_ephemeral_not_registered() {
    // Create two cache databases
    let db1 = Database::cache().unwrap();
    let db2 = Database::cache().unwrap();

    // They should be different instances (not shared via registry)
    assert!(!Arc::ptr_eq(&db1, &db2));
}

/// Test that opening the same path twice returns the same Arc via registry.
///
/// NOTE: This test is temporarily ignored because the singleton registry has a
/// race condition when the same path is opened concurrently. The registry lookup
/// and insert are not atomic, so two threads can both pass the "not in registry"
/// check and then both try to open the database, with one failing on the lock.
/// Issue: stratalab/strata-core#TBD
#[test]
#[ignore = "singleton registry race condition - needs fix"]
fn test_open_uses_registry() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let temp_dir = TempDir::new().unwrap();
    // Use a unique subdir name to avoid registry collisions in parallel tests
    let unique_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let db_path = temp_dir
        .path()
        .join(format!("singleton_via_open_{}", unique_id));

    // Open via Database::open twice
    let db1 = Database::open(&db_path).unwrap();
    let db2 = Database::open(&db_path).unwrap();

    // Should be same instance
    assert!(Arc::ptr_eq(&db1, &db2));
}

#[test]
fn test_open_creates_config_file() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("config_test");

    let db = Database::open(&db_path).unwrap();

    assert!(!db.is_cache());
    assert!(db_path.exists());
    // Config file should have been created
    assert!(db_path.join("strata.toml").exists());
}

#[test]
fn test_open_reads_always_config() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("always_config");

    // Create directory and write always config before opening
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::write(db_path.join("strata.toml"), "durability = \"always\"\n").unwrap();

    let db = Database::open(&db_path).unwrap();
    assert!(!db.is_cache());
}

#[test]
fn test_open_rejects_invalid_config() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("bad_config");

    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::write(db_path.join("strata.toml"), "durability = \"turbo\"\n").unwrap();

    let result = Database::open(&db_path);
    assert!(result.is_err());
}

// ========================================================================
// Checkpoint & Compaction Tests
// ========================================================================

#[test]
fn test_checkpoint_ephemeral_noop() {
    let db = Database::cache().unwrap();
    // checkpoint on ephemeral database is a no-op
    assert!(db.checkpoint().is_ok());
}

#[test]
fn test_compact_ephemeral_noop() {
    let db = Database::cache().unwrap();
    // compact on ephemeral database is a no-op
    assert!(db.compact().is_ok());
}

#[test]
fn test_compact_without_checkpoint_fails() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    // Compact before any checkpoint should fail (no snapshot watermark)
    let result = db.compact();
    assert!(result.is_err());
}

#[test]
fn test_checkpoint_creates_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "checkpoint_test");

    // Write some data
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::String("hello".to_string()))?;
        Ok(())
    })
    .unwrap();

    // Checkpoint should succeed
    assert!(db.checkpoint().is_ok());

    // Snapshots directory should exist with files
    let snapshots_dir = db_path.canonicalize().unwrap().join("snapshots");
    assert!(snapshots_dir.exists());

    // MANIFEST should exist
    let manifest_path = db_path.canonicalize().unwrap().join("MANIFEST");
    assert!(manifest_path.exists());
}

#[test]
fn test_checkpoint_then_compact_without_flush_succeeds() {
    // Inverted in the T3-E5 follow-up (retention-complete DTOs + install).
    //
    // Pre-retention, compact() refused on snapshot-only coverage because
    // recovery was either WAL-only or lacked tombstone/TTL/Branch fidelity.
    // Now that snapshot install is retention-complete, the snapshot
    // watermark is an authoritative recovery input and compact() succeeds
    // without a flush watermark.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "compact_test");

    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(42))?;
        Ok(())
    })
    .unwrap();

    db.checkpoint()
        .expect("checkpoint must succeed to populate the snapshot watermark");

    // Compact now succeeds on snapshot-only coverage.
    db.compact()
        .expect("compact() must succeed once recovery consumes retention-complete snapshots");
}

#[test]
fn test_gc_safe_point_no_active_txns() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // After open, system branch init may have advanced the version.
    // gc_safe_point = max(0, current_version - 1)
    let initial_sp = db.gc_safe_point();
    assert!(
        initial_sp <= 3,
        "gc_safe_point should be small after init, got {}",
        initial_sp
    );

    // Commit two transactions to advance version past 1
    let key = Key::new_kv(ns.clone(), "gc_key");
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(2))?;
        Ok(())
    })
    .unwrap();

    // Now current_version >= 2, no active txns → safe_point = current - 1
    let sp = db.gc_safe_point();
    assert!(sp >= 1, "safe point should be at least 1, got {}", sp);
}

#[test]
fn test_gc_safe_point_with_active_txn() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Commit a few transactions to advance version well past 1
    for i in 0..5 {
        let key = Key::new_kv(ns.clone(), format!("key_{}", i));
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(i))?;
            Ok(())
        })
        .unwrap();
    }

    // Start a transaction but don't commit it yet — pins version
    let mut txn = db.begin_transaction(branch_id).unwrap();
    let txn_start_version = txn.start_version;

    // Commit two more transactions to advance version further
    for i in 5..7 {
        let key = Key::new_kv(ns.clone(), format!("key_{}", i));
        db.transaction(branch_id, |t| {
            t.put(key.clone(), Value::Int(i))?;
            Ok(())
        })
        .unwrap();
    }

    // GC safe point should be min(current - 1, min_active_version)
    // The active txn pins the safe point at its start version
    let sp = db.gc_safe_point();
    assert!(
        sp <= txn_start_version.as_u64(),
        "safe point {} should be <= active txn start version {}",
        sp,
        txn_start_version.as_u64()
    );

    // Abort the active transaction
    txn.abort();

    // Now safe point should advance to current - 1 since no active txns
    let sp_after = db.gc_safe_point();
    assert!(
        sp_after > sp,
        "safe point should advance after abort: {} > {}",
        sp_after,
        sp
    );
}

#[test]
fn test_run_gc_prunes_old_versions() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "gc_prune");

    // Write v1
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();

    // Overwrite with v2
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(2))?;
        Ok(())
    })
    .unwrap();

    // Overwrite with v3
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(3))?;
        Ok(())
    })
    .unwrap();

    // Run GC — SegmentedStore prunes via compaction, not gc_branch(),
    // so pruned count is 0.  Verify the API doesn't error.
    let (safe_point, _pruned) = db.run_gc();
    assert!(safe_point > 0, "safe point should be non-zero");

    // Latest value should still be readable
    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(stored.value, Value::Int(3));
}

#[test]
fn test_run_gc_no_prune_at_version_zero() {
    let db = Database::cache().unwrap();

    // After init_system_branch, version has advanced; no user data to prune
    let (safe_point, pruned) = db.run_gc();
    // safe_point may be > 0 due to system branch init transactions
    assert!(
        safe_point <= 4,
        "safe_point should be small, got {}",
        safe_point
    );
    assert_eq!(pruned, 0);
}

#[test]
fn test_run_maintenance_end_to_end() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Write and overwrite a key to create GC-able versions
    let key = Key::new_kv(ns, "maint_key");
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();

    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(2))?;
        Ok(())
    })
    .unwrap();

    // Run full maintenance cycle
    let (safe_point, pruned, expired) = db.run_maintenance();
    assert!(safe_point > 0, "safe point should be non-zero");
    // pruned may or may not be > 0 depending on version chain gc logic
    // expired should be 0 since we didn't use TTL
    assert_eq!(expired, 0, "no TTL entries to expire");

    // Data should still be readable
    let stored = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(stored.value, Value::Int(2));

    let _ = pruned; // suppress unused warning
}

// ========================================================================
// E-1: Recovery Failure Tests
// ========================================================================

/// Helper: create a corrupted WAL segment file in the given db path.
/// Returns the WAL directory path.
fn corrupt_wal_segment(db_path: &std::path::Path) -> PathBuf {
    let wal_dir = db_path.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let segment_path = wal_dir.join("wal-000001.seg");
    std::fs::write(&segment_path, b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER").unwrap();
    wal_dir
}

#[test]
fn test_open_corrupted_wal_fails_by_default() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    corrupt_wal_segment(&db_path);

    // Default behavior: refuse to open
    let result = Database::open(&db_path);
    match result {
        Err(ref e) => {
            // Verify it's a Corruption variant, not just any error
            assert!(
                matches!(e, StrataError::Corruption { .. }),
                "Expected Corruption error variant, got: {}",
                e
            );
            let err_msg = format!("{}", e);
            assert!(
                err_msg.contains("WAL recovery failed"),
                "Error should mention WAL recovery failure, got: {}",
                err_msg
            );
            assert!(
                err_msg.contains("allow_lossy_recovery"),
                "Error should hint at the escape hatch, got: {}",
                err_msg
            );
        }
        Ok(_) => panic!("Database should refuse to open with corrupted WAL"),
    }
}

#[test]
fn test_open_corrupted_wal_succeeds_with_lossy_flag() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    corrupt_wal_segment(&db_path);

    // With allow_lossy_recovery=true, should open with empty state
    let cfg = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let result = Database::open_with_config(&db_path, cfg);
    assert!(
        result.is_ok(),
        "Database should open with allow_lossy_recovery=true, got: {:?}",
        result.err()
    );

    let db = result.unwrap();
    // Only system branch init transactions present (no user data recovered)
    assert!(
        db.storage().current_version() <= CommitVersion(3),
        "Should have at most system init transactions, got {}",
        db.storage().current_version()
    );
}

#[test]
fn test_lossy_recovery_discards_valid_data_before_corruption() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let wal_dir = db_path.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    // Write a VALID WAL record first
    write_wal_txn(
        &wal_dir,
        1,
        branch_id,
        vec![(
            Key::new_kv(ns.clone(), "important_data"),
            Value::Bytes(b"precious".to_vec()),
        )],
        vec![],
        1,
    );

    // Now corrupt by adding a second segment file with invalid header.
    // The WAL reader iterates segments in order and the corrupted segment
    // will cause read_all to error.
    let corrupt_segment = wal_dir.join("wal-000002.seg");
    std::fs::write(&corrupt_segment, b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER").unwrap();

    // Without lossy: should refuse to open
    let result = Database::open(&db_path);
    assert!(result.is_err(), "Should fail without lossy flag");

    // With lossy: opens but data is LOST (recovery falls back to empty)
    let cfg = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let db = Database::open_with_config(&db_path, cfg).unwrap();
    let key = Key::new_kv(ns, "important_data");
    assert!(
        db.storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .is_none(),
        "Valid data before corruption should be lost in lossy mode"
    );
    // Only system branch init transactions present (user data was discarded)
    assert!(db.storage().current_version() <= CommitVersion(3));
}

// ========================================================================
// E-5 + E-2: Shutdown coordination tests
// ========================================================================

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

/// Helper: blind-write a single key via transaction_with_version.
fn blind_write(db: &Database, key: Key, value: Value) -> u64 {
    let branch_id = key.namespace.branch_id;
    let ((), version) = db
        .transaction_with_version(branch_id, |txn| txn.put(key, value))
        .expect("blind write failed");
    version
}

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

/// Issue #1697: background compaction with max_versions_per_key must not
/// prune versions that active snapshots need.
#[test]
fn test_issue_1697_compaction_preserves_snapshot_versions() {
    let temp_dir = TempDir::new().unwrap();
    let mut cfg = StrataConfig::default();
    cfg.storage.max_versions_per_key = 1;
    let db = Database::open_with_config(temp_dir.path().join("db"), cfg).unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "snap_key");

    // Write version 1
    blind_write(&db, key.clone(), Value::Int(1));

    // Start a reader that pins the current snapshot
    let mut reader = db.begin_read_only_transaction(branch_id).unwrap();
    let pinned_version = reader.start_version;

    // Write version 2 — now key has 2 versions, max_versions_per_key=1
    blind_write(&db, key.clone(), Value::Int(2));

    // Force data from memtable → segments so compaction can process it.
    // Rotate twice to create 2 frozen memtables → flush both → compact.
    db.storage().rotate_memtable(&branch_id);
    db.storage().flush_oldest_frozen(&branch_id).unwrap();
    // Need a second segment to trigger compaction (min 2 segments).
    // Write a dummy key so the second memtable isn't empty.
    blind_write(&db, Key::new_kv(ns.clone(), "dummy"), Value::Int(999));
    db.storage().rotate_memtable(&branch_id);
    db.storage().flush_oldest_frozen(&branch_id).unwrap();

    // Now compact — this is where max_versions would drop the old version.
    let compacted = db
        .storage()
        .compact_branch(&branch_id, CommitVersion::ZERO)
        .unwrap();
    assert!(compacted.is_some(), "compaction should have run");

    // The reader's snapshot must still be able to find version 1
    // via the storage layer at the pinned version.
    let result = db.storage().get_versioned(&key, pinned_version).unwrap();
    assert!(
        result.is_some(),
        "version at snapshot {} was pruned by compaction despite active reader",
        pinned_version
    );
    assert_eq!(
        result.unwrap().value,
        Value::Int(1),
        "snapshot read returned wrong value after compaction"
    );

    // Latest version must also be intact
    let latest = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert!(latest.is_some(), "latest version missing after compaction");
    assert_eq!(latest.unwrap().value, Value::Int(2));

    // Clean up reader
    reader.abort();
}

#[test]
#[serial(open_databases)]
fn test_issue_1730_checkpoint_compact_recovery_data_loss() {
    // Inverted in the T3-E5 follow-up.
    //
    // Pre-retention, recovery was either WAL-only or lacked tombstone/TTL
    // fidelity, so `compact()` had to refuse WAL deletion on snapshot-only
    // coverage (`#1730`). Now that snapshot install is retention-complete
    // (tombstones, TTL, branch metadata round-trip), compact() succeeds,
    // WAL is reclaimed, and the committed data survives reopen through
    // snapshot install alone.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "critical_data");

        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::String("must_survive".to_string()))?;
            Ok(())
        })
        .unwrap();

        db.checkpoint().unwrap();

        // compact() now succeeds on snapshot-only coverage.
        db.compact()
            .expect("compact() must succeed once snapshot coverage is trusted");
    }

    OPEN_DATABASES.lock().clear();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "critical_data");

        let result = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("data must survive checkpoint+compact+recovery via snapshot install");
        assert_eq!(result.value, Value::String("must_survive".to_string()));
    }
}

#[test]
#[serial(open_databases)]
fn test_issue_1730_standard_durability() {
    // Inverted twin for Standard durability mode. Same contract: compact
    // succeeds on snapshot-only coverage, data round-trips via snapshot
    // install.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let durability = DurabilityMode::Standard {
        interval_ms: 100,
        batch_size: 1,
    };

    {
        let db = Database::open_with_durability(&db_path, durability).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "standard_data");

        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::String("durable".to_string()))?;
            Ok(())
        })
        .unwrap();

        // Flush ensures the record is on disk before checkpoint so the
        // snapshot + MANIFEST watermark reflect the transaction.
        db.flush().unwrap();
        db.checkpoint().unwrap();

        db.compact()
            .expect("compact() must succeed on snapshot-only watermark under Standard durability");
    }

    OPEN_DATABASES.lock().clear();

    // Reopen and verify data survived
    {
        let db = Database::open_with_durability(&db_path, durability).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "standard_data");

        let result = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .expect("data must survive checkpoint+compact+recovery under Standard durability");
        assert_eq!(result.value, Value::String("durable".to_string()));
    }
}

/// Issue #1699: Follower refresh must use the WAL record's commit timestamp,
/// not Timestamp::now(), so that all entries from the same transaction share
/// a single timestamp for correct time-travel queries.
///
/// Without the fix, each put_with_version_mode / delete_with_version call
/// creates its own Timestamp::now(), splitting a transaction across multiple
/// timestamps and causing time-travel queries to see partial state.
#[test]
fn test_issue_1699_refresh_preserves_wal_timestamp() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::default();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    // 1. Open primary with Always durability (syncs every commit to WAL)
    let primary = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

    // 2. Pre-populate key B
    let key_b = Key::new_kv(ns.clone(), "b");
    primary
        .transaction(branch_id, |txn| {
            txn.put(key_b.clone(), Value::String("old".into()))?;
            Ok(())
        })
        .unwrap();

    // 3. Open follower — initial recovery replays B via RecoveryCoordinator (correct path)
    // Followers don't use the registry, so they can coexist with a primary in the same process.
    let follower = Database::open_follower(&db_path).unwrap();

    // 4. Primary commits: put A + delete B in one transaction
    let key_a = Key::new_kv(ns.clone(), "a");
    primary
        .transaction(branch_id, |txn| {
            txn.put(key_a.clone(), Value::String("new".into()))?;
            txn.delete(key_b.clone())?;
            Ok(())
        })
        .unwrap();

    // Record upper bound on the WAL record's timestamp
    let commit_ts_upper = Timestamp::now().as_micros();

    // 5. Sleep to create a clear gap between commit time and refresh time.
    //    100ms = 100_000 microseconds — far larger than any clock jitter.
    std::thread::sleep(Duration::from_millis(100));

    // 6. Follower refresh — applies the new WAL records via Database::refresh
    let outcome = follower.refresh();
    assert!(outcome.is_caught_up(), "follower should catch up");
    assert!(
        outcome.applied_count() > 0,
        "follower should apply the new transaction"
    );

    // 7. Check: A's entry timestamp should come from the WAL record (near
    //    commit time), NOT from Timestamp::now() during refresh.
    let a_entry = follower
        .storage()
        .get_versioned(&key_a, CommitVersion::MAX)
        .unwrap()
        .expect("key A should exist after refresh");
    let a_ts = a_entry.timestamp.as_micros();

    assert!(
        a_ts <= commit_ts_upper,
        "Entry timestamp ({}) should be from WAL record (≤ {}), \
         not from Timestamp::now() during refresh",
        a_ts,
        commit_ts_upper,
    );

    // 8. Time-travel at A's timestamp: B should be deleted (same transaction).
    //    With split timestamps, B's tombstone has a later timestamp than A's put,
    //    so querying at A's timestamp would incorrectly show B as still alive.
    let b_at_a_ts = follower.storage().get_at_timestamp(&key_b, a_ts).unwrap();
    assert!(
        b_at_a_ts.is_none(),
        "Key B should be deleted at A's timestamp (same transaction). \
         Got {:?}, indicating split timestamps between put and delete.",
        b_at_a_ts.map(|v| v.value),
    );
}

/// Issue #1736: Compaction must run on the background scheduler, not inline
/// on the writer thread. We verify by checking that the background scheduler
/// completed compaction tasks — before the fix, compaction ran synchronously
/// and the scheduler had zero completed tasks.
#[test]
fn test_issue_1736_compaction_runs_in_background() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    // Tiny write buffer: forces a memtable rotation (and flush) after ~1-2 writes.
    let cfg = StrataConfig {
        durability: "always".to_string(),
        storage: StorageConfig {
            write_buffer_size: 256,
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };

    let db = Database::open_with_config(&db_path, cfg).unwrap();
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    let tasks_before = db.scheduler.stats().tasks_completed;

    // Write enough entries to create 6+ L0 segments, triggering compaction.
    // Each write ~100 bytes; write_buffer_size=256 → rotation every ~2-3 writes.
    for i in 0..20 {
        let key = Key::new_kv(ns.clone(), format!("key_{:04}", i));
        db.transaction(
            branch_id,
            |txn: &mut strata_concurrency::TransactionContext| {
                txn.put(key.clone(), Value::String(format!("value_{}", i)))?;
                Ok(())
            },
        )
        .unwrap();
    }

    // Drain the scheduler to ensure all background tasks complete.
    db.scheduler.drain();

    let tasks_after = db.scheduler.stats().tasks_completed;
    let bg_tasks = tasks_after - tasks_before;

    // With background compaction (fix): scheduler ran compaction tasks → bg_tasks > 0.
    // With synchronous compaction (bug): scheduler was never used → bg_tasks == 0.
    assert!(
        bg_tasks > 0,
        "Background scheduler should have completed compaction tasks. \
         tasks_completed before={}, after={}, delta={}. \
         If delta is 0, compaction ran synchronously on the writer thread.",
        tasks_before,
        tasks_after,
        bg_tasks,
    );

    // Verify data integrity: all entries should be readable after compaction.
    for i in 0..20 {
        let key = Key::new_kv(ns.clone(), format!("key_{:04}", i));
        let entry = db
            .storage
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("key_{:04} should exist after compaction", i));
        assert_eq!(entry.value, Value::String(format!("value_{}", i)),);
    }
}

#[test]
fn test_issue_1732_checkpoint_data_preserves_timestamps() {
    // Issue #1732: collect_checkpoint_data() rewrites all timestamps to `now`,
    // losing the original commit timestamps.
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "ts_test");

    // Write data
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::String("hello".to_string()))?;
        Ok(())
    })
    .unwrap();

    // Get the timestamp from list_by_type (same source as collect_checkpoint_data)
    let listed = db.storage().list_by_type(&branch_id, TypeTag::KV);
    let entry_for_key = listed
        .iter()
        .find(|(k, _)| k.user_key_string().unwrap_or_default() == "ts_test")
        .expect("should find ts_test entry");
    let storage_ts = entry_for_key.1.timestamp.as_micros();
    assert!(storage_ts > 0, "Storage timestamp should be non-zero");

    // Sleep so `now` would differ from the storage timestamp
    std::thread::sleep(Duration::from_millis(50));
    let now_after_sleep = strata_durability::now_micros();

    let data = db.collect_checkpoint_data();
    let kv_entries = data.kv.expect("should have KV entries");

    let entry = kv_entries
        .iter()
        .find(|e| e.user_key == b"ts_test".to_vec())
        .expect("should find ts_test in checkpoint");

    // The checkpoint timestamp must match the storage timestamp, not `now`
    assert_eq!(
        entry.timestamp, storage_ts,
        "Checkpoint timestamp should match storage timestamp, not now",
    );
    // Sanity: now is significantly later than storage_ts
    assert!(
        now_after_sleep > storage_ts + 40_000,
        "now ({}) should be at least 40ms after storage_ts ({})",
        now_after_sleep,
        storage_ts,
    );
}

#[test]
fn test_issue_1732_checkpoint_data_preserves_branch_names() {
    // Issue #1732: collect_checkpoint_data() sets branch name to String::new()
    // instead of extracting it from the serialized BranchMetadata.
    use crate::primitives::branch::{BranchIndex, BranchMetadata};

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_index = BranchIndex::new(db.clone());
    branch_index.create_branch("my-test-branch").unwrap();

    let data = db.collect_checkpoint_data();
    let branch_entries = data.branches.expect("should have branch entries");

    let found = branch_entries.iter().find(|b| b.key == "my-test-branch");
    assert!(
        found.is_some(),
        "Branch key 'my-test-branch' should be preserved in checkpoint. \
         Got keys: {:?}",
        branch_entries.iter().map(|b| &b.key).collect::<Vec<_>>(),
    );
    let branch_entry = found.unwrap();

    let stored_value: Value =
        serde_json::from_slice(&branch_entry.value).expect("branch snapshot value should decode");
    let stored_json = match stored_value {
        Value::String(json) => json,
        other => panic!("branch snapshot should store Value::String, got {other:?}"),
    };
    let metadata: BranchMetadata =
        serde_json::from_str(&stored_json).expect("branch metadata should decode");

    assert_eq!(metadata.name, "my-test-branch");
    assert!(
        metadata.created_at.as_micros() > 0,
        "Branch created_at should be non-zero in serialized metadata",
    );
    assert!(
        branch_entry.version > 0,
        "Branch snapshot must preserve MVCC version"
    );
    assert!(
        branch_entry.timestamp > 0,
        "Branch snapshot must preserve MVCC timestamp",
    );
}

#[test]
fn test_checkpoint_data_preserves_branch_space_and_type_for_kv_entries() {
    let db = Database::cache().unwrap();
    let branch_a = BranchId::from_bytes([1; 16]);
    let branch_b = BranchId::from_bytes([2; 16]);
    let timestamp_micros = 1_234_567u64;
    let version = CommitVersion(7);
    let value = Value::Int(42);

    let ns_a = Arc::new(Namespace::new(branch_a, "default".to_string()));
    let ns_b = Arc::new(Namespace::new(branch_b, "tenant_a".to_string()));

    db.storage
        .put_recovery_entry(
            Key::new_kv(ns_a, "ambiguous"),
            value.clone(),
            version,
            timestamp_micros,
            0,
        )
        .unwrap();
    db.storage
        .put_recovery_entry(
            Key::new_kv(ns_b, "ambiguous"),
            value.clone(),
            version,
            timestamp_micros,
            0,
        )
        .unwrap();

    let data = db.collect_checkpoint_data();
    let kv_entries = data.kv.expect("should have KV entries");
    assert!(kv_entries.iter().any(|entry| {
        entry.branch_id == *branch_a.as_bytes()
            && entry.space == "default"
            && entry.type_tag == TypeTag::KV.as_byte()
            && entry.user_key == b"ambiguous".to_vec()
            && entry.value == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
    assert!(kv_entries.iter().any(|entry| {
        entry.branch_id == *branch_b.as_bytes()
            && entry.space == "tenant_a"
            && entry.type_tag == TypeTag::KV.as_byte()
            && entry.user_key == b"ambiguous".to_vec()
            && entry.value == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
}

#[test]
fn test_checkpoint_data_preserves_binary_kv_keys() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([9; 16]);
    let namespace = Arc::new(Namespace::new(branch_id, "binary".to_string()));
    let user_key = vec![0x00, 0xFF, 0x80, 0x01];
    let key = Key::new(namespace, TypeTag::KV, user_key.clone());
    let value = Value::Int(7);
    let version = CommitVersion(13);
    let timestamp_micros = 9_999_999u64;

    db.storage
        .put_recovery_entry(key, value.clone(), version, timestamp_micros, 0)
        .unwrap();

    let kv_entries = db
        .collect_checkpoint_data()
        .kv
        .expect("should have KV entries");
    assert!(kv_entries.iter().any(|entry| {
        entry.branch_id == *branch_id.as_bytes()
            && entry.space == "binary"
            && entry.type_tag == TypeTag::KV.as_byte()
            && entry.user_key == user_key
            && entry.value == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
}

#[test]
fn test_checkpoint_data_preserves_graph_type_and_space_metadata() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([3; 16]);
    let namespace = Arc::new(Namespace::new(branch_id, "tenant_graph".to_string()));
    let timestamp_micros = 2_345_678u64;
    let version = CommitVersion(9);
    let value = Value::String("graph-payload".to_string());

    db.storage
        .put_recovery_entry(
            Key::new_graph(namespace, "shared"),
            value.clone(),
            version,
            timestamp_micros,
            0,
        )
        .unwrap();

    let data = db.collect_checkpoint_data();
    let kv_entries = data.kv.expect("should have KV entries");
    assert!(kv_entries.iter().any(|entry| {
        entry.branch_id == *branch_id.as_bytes()
            && entry.space == "tenant_graph"
            && entry.type_tag == TypeTag::Graph.as_byte()
            && entry.user_key == b"shared".to_vec()
            && entry.value == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
}

#[test]
fn test_checkpoint_data_preserves_event_space_metadata() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([6; 16]);
    let namespace = Arc::new(Namespace::new(branch_id, "tenant_events".to_string()));
    let key = Key::new_event(namespace, 7);
    let value = Value::String("event-payload".to_string());
    let timestamp_micros = 7_654_321u64;
    let version = CommitVersion(3);

    db.storage
        .put_recovery_entry(key, value.clone(), version, timestamp_micros, 0)
        .unwrap();

    let events = db
        .collect_checkpoint_data()
        .events
        .expect("should have event entries");
    assert!(events.iter().any(|entry| {
        entry.branch_id == *branch_id.as_bytes()
            && entry.space == "tenant_events"
            && entry.sequence == 7
            && entry.payload == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
}

#[test]
fn test_checkpoint_data_preserves_json_space_metadata() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([7; 16]);
    let namespace = Arc::new(Namespace::new(branch_id, "products".to_string()));
    let key = Key::new_json(namespace, "doc-1");
    let value = Value::String("{\"sku\":\"123\"}".to_string());
    let timestamp_micros = 8_765_432u64;
    let version = CommitVersion(4);

    db.storage
        .put_recovery_entry(key, value.clone(), version, timestamp_micros, 0)
        .unwrap();

    let json_entries = db
        .collect_checkpoint_data()
        .json
        .expect("should have json entries");
    assert!(json_entries.iter().any(|entry| {
        entry.branch_id == *branch_id.as_bytes()
            && entry.space == "products"
            && entry.doc_id == "doc-1"
            && entry.content == serde_json::to_vec(&value).unwrap()
            && entry.version == version.as_u64()
            && entry.timestamp == timestamp_micros
    }));
}

#[test]
fn test_checkpoint_data_preserves_vector_space_metadata() {
    #[derive(serde::Serialize)]
    struct TestVectorRecord {
        vector_id: u64,
        #[serde(default)]
        embedding: Vec<f32>,
        metadata: Option<serde_json::Value>,
        version: u64,
        created_at: u64,
        updated_at: u64,
        #[serde(default)]
        source_ref: Option<strata_core::EntityRef>,
    }

    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([8; 16]);
    let namespace = Arc::new(Namespace::new(branch_id, "semantic".to_string()));
    let config_key = Key::new_vector_config(namespace.clone(), "embeddings");
    let vector_key = Key::new_vector(namespace, "embeddings", "vec-1");
    let config_version = CommitVersion(1);
    let vector_version = CommitVersion(2);
    let config_timestamp = 1_000u64;
    let vector_timestamp = 2_000u64;
    let record = TestVectorRecord {
        vector_id: 99,
        embedding: vec![0.1, 0.2, 0.3],
        metadata: Some(serde_json::json!({ "label": "test" })),
        version: 1,
        created_at: 1_000,
        updated_at: 1_000,
        source_ref: None,
    };
    let raw_record = rmp_serde::to_vec(&record).unwrap();

    db.storage
        .put_recovery_entry(
            config_key,
            Value::Bytes(b"{\"dimensions\":3}".to_vec()),
            config_version,
            config_timestamp,
            0,
        )
        .unwrap();
    db.storage
        .put_recovery_entry(
            vector_key,
            Value::Bytes(raw_record.clone()),
            vector_version,
            vector_timestamp,
            0,
        )
        .unwrap();

    let vector_entries = db
        .collect_checkpoint_data()
        .vectors
        .expect("should have vector entries");
    assert!(vector_entries.iter().any(|entry| {
        entry.branch_id == *branch_id.as_bytes()
            && entry.space == "semantic"
            && entry.name == "embeddings"
            && entry.config == b"{\"dimensions\":3}".to_vec()
            && entry.config_version == config_version.as_u64()
            && entry.config_timestamp == config_timestamp
            && entry.vectors.iter().any(|vector| {
                vector.key == "vec-1"
                    && vector.raw_value == raw_record
                    && vector.version == vector_version.as_u64()
                    && vector.timestamp == vector_timestamp
            })
    }));
}

#[test]
fn test_checkpoint_data_includes_tombstones() {
    // Inverted from the pre-retention `test_checkpoint_data_omits_tombstones_*`.
    // The collector now uses `list_by_type_at_version(…, MAX)` which preserves
    // tombstones, so a deleted key must appear in the checkpoint payload as
    // an `is_tombstone = true` entry.
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([4; 16]);
    let key = Key::new_kv(create_test_namespace(branch_id), "deleted");

    db.storage
        .put_recovery_entry(key.clone(), Value::Int(1), CommitVersion(1), 1_000, 0)
        .unwrap();
    db.storage
        .delete_recovery_entry(&key, CommitVersion(2), 2_000)
        .unwrap();

    let kv_entries = db.collect_checkpoint_data().kv.unwrap_or_default();
    let entry = kv_entries
        .iter()
        .find(|e| e.user_key == b"deleted".to_vec())
        .expect("deleted key must appear in checkpoint as a tombstone entry");
    assert!(
        entry.is_tombstone,
        "deleted key must surface with is_tombstone=true, got: {:?}",
        entry
    );
    assert_eq!(
        entry.version, 2,
        "tombstone must carry the delete's commit version, not the prior put's"
    );
}

#[test]
fn test_checkpoint_data_includes_ttl_metadata() {
    // Inverted from the pre-retention `test_checkpoint_data_omits_ttl_metadata`.
    // The KV snapshot DTO now carries `ttl_ms`, so the collected entry
    // reflects the per-key TTL exactly.
    let db = Database::cache().unwrap();
    let branch_id = BranchId::from_bytes([5; 16]);
    let key = Key::new_kv(create_test_namespace(branch_id), "ttl_key");
    let version = CommitVersion(11);
    let ttl_ms = 60_000u64;
    let value = Value::String("ttl".to_string());

    db.storage
        .put_with_version_mode(
            key.clone(),
            value.clone(),
            version,
            Some(Duration::from_millis(ttl_ms)),
            WriteMode::Append,
        )
        .unwrap();

    let kv_entries = db
        .collect_checkpoint_data()
        .kv
        .expect("should have KV entries");
    let snapshot_entry = kv_entries
        .iter()
        .find(|entry| entry.user_key == b"ttl_key".to_vec())
        .expect("should find ttl_key in checkpoint");

    assert_eq!(
        snapshot_entry.ttl_ms, ttl_ms,
        "snapshot entry must carry the original TTL value"
    );
    assert!(
        !snapshot_entry.is_tombstone,
        "live entry must not be tombstoned"
    );
    assert_eq!(snapshot_entry.version, version.as_u64());
}

/// Issue #1738 / 9.2.A: begin_transaction() does not check accepting_transactions.
/// After shutdown(), manual callers can still start transactions.
#[test]
fn test_issue_1738_begin_transaction_bypasses_shutdown_gate() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::new();
    db.shutdown().unwrap();

    // transaction() correctly rejects after shutdown
    let closure_result = db.transaction(branch_id, |_txn| Ok(()));
    assert!(
        closure_result.is_err(),
        "transaction() should reject after shutdown"
    );

    // begin_transaction() SHOULD also reject after shutdown
    let manual_result = db.begin_transaction(branch_id);
    assert!(
        manual_result.is_err(),
        "begin_transaction() must reject after shutdown, but it succeeded"
    );
}

/// Issue #1738 / 9.2.B: shutdown() polls active_count() with Relaxed ordering
/// instead of using wait_for_idle() with SeqCst. This test verifies that the
/// shutdown path uses the correct synchronization by checking that it calls
/// wait_for_idle (which properly synchronizes with record_start/record_commit).
///
/// We verify this indirectly: start a transaction, then shutdown. If shutdown
/// uses proper ordering, it will always see the active transaction and wait.
#[test]
fn test_issue_1738_shutdown_waits_for_active_transactions() {
    let db = Database::cache().unwrap();
    let branch_id = BranchId::new();

    // Start a transaction manually (increments active_count)
    let txn = db.begin_transaction(branch_id).unwrap();

    // Spawn shutdown in background — it should wait for the active transaction
    let db2 = Arc::clone(&db);
    let shutdown_handle = std::thread::spawn(move || {
        db2.shutdown().unwrap();
    });

    // Give shutdown a moment to start waiting
    std::thread::sleep(Duration::from_millis(50));

    // End the transaction (decrements active_count)
    db.end_transaction(txn);

    // Shutdown should complete within a reasonable time
    shutdown_handle.join().unwrap();
}

#[test]
#[serial(open_databases)]
fn test_issue_1733_history_no_duplicates_after_recovery() {
    // Issue #1733: WAL replay replays ALL records (including already-flushed
    // ones) into memtables. After segment recovery loads the same data from
    // disk, get_history() returns duplicate versions.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();

    // Phase 1: Write data and flush to segments
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "history_key");

        // Write 3 versions
        blind_write(&db, key.clone(), Value::Int(1));
        blind_write(&db, key.clone(), Value::Int(2));
        blind_write(&db, key.clone(), Value::Int(3));

        // Flush memtable to segment — data is now in BOTH WAL and segment
        db.storage().rotate_memtable(&branch_id);
        db.storage().flush_oldest_frozen(&branch_id).unwrap();

        // Verify history has 3 versions before close
        let history = db.get_history(&key, None, None).unwrap();
        assert_eq!(history.len(), 3, "pre-close history should have 3 versions");
    }

    // Clear registry so reopen creates a fresh instance
    OPEN_DATABASES.lock().clear();

    // Phase 2: Reopen — WAL replays all records, then segments are loaded
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "history_key");

        let history = db.get_history(&key, None, None).unwrap();

        // BUG: Without dedup, this returns 6 versions (3 from memtable + 3 from segment)
        assert_eq!(
            history.len(),
            3,
            "get_history() returned {} versions after recovery (expected 3 — duplicates detected)",
            history.len()
        );

        // Verify correct values (newest first)
        assert_eq!(history[0].value, Value::Int(3));
        assert_eq!(history[1].value, Value::Int(2));
        assert_eq!(history[2].value, Value::Int(1));

        // Also verify point read returns the correct latest value (no regression)
        let latest = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap();
        assert!(latest.is_some(), "point read should find the key");
        assert_eq!(latest.unwrap().value, Value::Int(3));
    }
}

#[test]
#[serial(open_databases)]
fn test_issue_1733_partial_flush_no_duplicates() {
    // Variant: only some versions are flushed. After recovery, flushed
    // versions appear in both memtable and segment; unflushed versions
    // appear only in memtable. History must still have no duplicates.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "partial_key");

        // Write v1, v2 then flush
        blind_write(&db, key.clone(), Value::Int(1));
        blind_write(&db, key.clone(), Value::Int(2));
        db.storage().rotate_memtable(&branch_id);
        db.storage().flush_oldest_frozen(&branch_id).unwrap();

        // Write v3 (not flushed — only in WAL)
        blind_write(&db, key.clone(), Value::Int(3));
    }

    OPEN_DATABASES.lock().clear();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "partial_key");

        let history = db.get_history(&key, None, None).unwrap();
        assert_eq!(
            history.len(),
            3,
            "partial flush: expected 3 versions, got {}",
            history.len()
        );
        assert_eq!(history[0].value, Value::Int(3));
        assert_eq!(history[1].value, Value::Int(2));
        assert_eq!(history[2].value, Value::Int(1));
    }
}

#[test]
#[serial(open_databases)]
fn test_issue_1733_tombstone_no_duplicate_after_recovery() {
    // Variant: a delete (tombstone) was flushed. After recovery, history
    // must show the tombstone exactly once, not twice.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns.clone(), "tomb_key");

        blind_write(&db, key.clone(), Value::Int(1));

        // Delete the key (creates tombstone)
        db.transaction(branch_id, |txn| {
            txn.delete(key.clone())?;
            Ok(())
        })
        .unwrap();

        // Flush both versions to segment
        db.storage().rotate_memtable(&branch_id);
        db.storage().flush_oldest_frozen(&branch_id).unwrap();
    }

    OPEN_DATABASES.lock().clear();

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "tomb_key");

        let history = db.get_history(&key, None, None).unwrap();
        // History includes tombstones: should be [tombstone@v2, put@v1]
        assert_eq!(
            history.len(),
            2,
            "tombstone dedup: expected 2 versions (tombstone + put), got {}",
            history.len()
        );

        // Point read should return None (key is deleted)
        let latest = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap();
        assert!(
            latest.is_none(),
            "deleted key should not be found via point read"
        );
    }
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

    let err = match Database::open_runtime(
        OpenSpec::primary(&db_path)
            .with_subsystem(TestRuntimeSubsystem::named("runtime-subsystem")),
    ) {
        Ok(_) => panic!("reopen with stale config should not reuse a live-switched database"),
        Err(err) => err,
    };
    assert!(
        matches!(err, StrataError::IncompatibleReuse { .. }),
        "expected IncompatibleReuse, got {:?}",
        err
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

    super::test_hooks::clear_flush_thread_spawn_failure();
    super::test_hooks::inject_flush_thread_spawn_failure();

    let err = db
        .set_durability_mode(DurabilityMode::Standard {
            interval_ms: 1,
            batch_size: 64,
        })
        .expect_err("injected spawn failure should bubble up");

    super::test_hooks::clear_flush_thread_spawn_failure();

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

    super::test_hooks::clear_sync_failure();
    let mut manual_txn = db.begin_transaction(branch_id).unwrap();
    manual_txn.put(pending_key.clone(), Value::Int(6)).unwrap();

    // Commit a separate transaction to create unsynced WAL data.
    super::test_hooks::inject_sync_failure(std::io::ErrorKind::Other);
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
    super::test_hooks::clear_sync_failure();
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

    super::test_hooks::clear_sync_failure();
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();

    // Inject sync failure
    super::test_hooks::inject_sync_failure(std::io::ErrorKind::Other);

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

    super::test_hooks::inject_sync_failure(std::io::ErrorKind::Other);
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

    super::test_hooks::clear_sync_failure();
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
#[serial]
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
    let open_source = include_str!("open.rs");
    let mod_source = include_str!("mod.rs");
    let loop_marker = ["name(\"", "strata-wal-flush", "\""].concat();
    let open_count = open_source.matches(&loop_marker).count();
    let mod_count = mod_source.matches(&loop_marker).count();

    assert_eq!(
        open_count + mod_count,
        1,
        "engine database sources must contain exactly one production flush loop"
    );
}

struct TestRuntimeSubsystem {
    name: &'static str,
    events: Option<Arc<parking_lot::Mutex<Vec<&'static str>>>>,
    fail_initialize_once: Option<Arc<AtomicBool>>,
}

impl TestRuntimeSubsystem {
    fn named(name: &'static str) -> Self {
        Self {
            name,
            events: None,
            fail_initialize_once: None,
        }
    }

    fn recording(name: &'static str, events: Arc<parking_lot::Mutex<Vec<&'static str>>>) -> Self {
        Self {
            name,
            events: Some(events),
            fail_initialize_once: None,
        }
    }

    fn fail_initialize_once(name: &'static str, fail_flag: Arc<AtomicBool>) -> Self {
        Self {
            name,
            events: None,
            fail_initialize_once: Some(fail_flag),
        }
    }

    fn record(&self, event: &'static str) {
        if let Some(events) = &self.events {
            events.lock().push(event);
        }
    }
}

impl Subsystem for TestRuntimeSubsystem {
    fn name(&self) -> &'static str {
        self.name
    }

    fn recover(&self, _db: &Arc<Database>) -> StrataResult<()> {
        self.record("recover");
        Ok(())
    }

    fn initialize(&self, _db: &Arc<Database>) -> StrataResult<()> {
        self.record("initialize");
        if self
            .fail_initialize_once
            .as_ref()
            .is_some_and(|flag| flag.swap(false, Ordering::SeqCst))
        {
            return Err(StrataError::internal("initialize failed"));
        }
        Ok(())
    }

    fn bootstrap(&self, _db: &Arc<Database>) -> StrataResult<()> {
        self.record("bootstrap");
        Ok(())
    }
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

// ========================================================================
// Issue #1914: Executor event append bypasses OCC — hash chain corruption
// ========================================================================

/// Two concurrent transactions both appending events to the same branch
/// must conflict at commit time. Before the fix, the meta_key was never
/// read, so OCC validation had no read-set entry to detect the conflict.
#[test]
fn test_issue_1914_concurrent_event_append_occ_conflict() {
    use crate::transaction_ops::TransactionOps;

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();

    let ns = Arc::new(Namespace::for_branch(branch_id));

    // Begin two transactions on the same branch at the same snapshot
    let mut txn1 = db.begin_transaction(branch_id).unwrap();
    let mut txn2 = db.begin_transaction(branch_id).unwrap();

    // Both append an event using the Transaction wrapper (executor path)
    {
        let mut t1 = crate::transaction::context::Transaction::new(&mut txn1, ns.clone());
        t1.event_append("test_event", Value::String("from_t1".into()))
            .unwrap();
    }
    {
        let mut t2 = crate::transaction::context::Transaction::new(&mut txn2, ns.clone());
        t2.event_append("test_event", Value::String("from_t2".into()))
            .unwrap();
    }

    // Commit T1 — should succeed
    db.commit_transaction(&mut txn1).unwrap();
    db.end_transaction(txn1);

    // Commit T2 — MUST fail with a conflict.
    // Both wrote to meta_key (event log metadata). If meta_key is in both
    // read_sets, OCC detects that T1 modified it after T2's snapshot.
    // Before the fix: both commit succeeds → duplicate sequence 0,
    // corrupted hash chain.
    let result = db.commit_transaction(&mut txn2);
    db.end_transaction(txn2);

    assert!(
        result.is_err(),
        "T2 must conflict: both transactions appended to the same event log \
         but OCC did not detect the conflict because meta_key was never read"
    );
}

/// Verify that after the fix, sequential event appends produce correct
/// sequence numbers sourced from persisted metadata, not ephemeral defaults.
#[test]
fn test_issue_1914_sequence_from_persisted_meta() {
    use crate::transaction_ops::TransactionOps;

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();

    let ns = Arc::new(Namespace::for_branch(branch_id));

    // First transaction: append event at sequence 0
    let mut txn1 = db.begin_transaction(branch_id).unwrap();
    {
        let mut t = crate::transaction::context::Transaction::new(&mut txn1, ns.clone());
        let v = t
            .event_append("evt", Value::String("first".into()))
            .unwrap();
        assert_eq!(v, strata_core::Version::seq(0));
    }
    db.commit_transaction(&mut txn1).unwrap();
    db.end_transaction(txn1);

    // Second transaction: should continue at sequence 1 (from persisted meta)
    let mut txn2 = db.begin_transaction(branch_id).unwrap();
    {
        let mut t = crate::transaction::context::Transaction::new(&mut txn2, ns.clone());
        let v = t
            .event_append("evt", Value::String("second".into()))
            .unwrap();
        // Before fix: base_sequence comes from ctx.event_sequence_count()
        // which defaults to 0 → duplicate sequence 0.
        // After fix: reads persisted meta.next_sequence = 1.
        assert_eq!(
            v,
            strata_core::Version::seq(1),
            "Second append must use sequence 1 from persisted meta, not 0"
        );
    }
    db.commit_transaction(&mut txn2).unwrap();
    db.end_transaction(txn2);

    // Verify the hash chain: event 1's prev_hash must equal event 0's hash
    let mut txn3 = db.begin_transaction(branch_id).unwrap();
    {
        let mut t = crate::transaction::context::Transaction::new(&mut txn3, ns.clone());
        let e0 = t.event_get(0).unwrap().expect("event 0 must exist");
        let e1 = t.event_get(1).unwrap().expect("event 1 must exist");
        assert_eq!(
            e1.value.prev_hash, e0.value.hash,
            "event 1's prev_hash must chain from event 0's hash"
        );
    }
    db.end_transaction(txn3);
}

#[test]
fn test_issue_1551_database_uuid_is_nonzero() {
    // Verify that a disk-backed database gets a real UUID, not all-zeros.
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();
    let uuid = db.database_uuid();
    assert_ne!(
        uuid, [0u8; 16],
        "disk-backed database must have a non-zero UUID"
    );
}

#[test]
fn test_issue_1551_database_uuid_persists_across_reopen() {
    // Verify that the UUID is stable across close/reopen.
    let temp_dir = TempDir::new().unwrap();

    let uuid_first = {
        let db = Database::open(temp_dir.path()).unwrap();
        db.database_uuid()
    };
    assert_ne!(uuid_first, [0u8; 16]);

    // Reopen — should get the same UUID from MANIFEST
    let uuid_second = {
        let db = Database::open(temp_dir.path()).unwrap();
        db.database_uuid()
    };
    assert_eq!(uuid_first, uuid_second, "UUID must persist across reopen");
}

#[test]
fn test_issue_1551_cache_database_uuid_is_zero() {
    // Ephemeral databases have no persistence, so UUID is all-zeros.
    let db = Database::cache().unwrap();
    assert_eq!(db.database_uuid(), [0u8; 16]);
}

/// Issue #1924: Write stall timeout must be surfaced to the caller.
///
/// When L0 segment count exceeds `l0_stop_writes_trigger` and compaction
/// cannot drain L0 within `write_stall_timeout_ms`, write transactions
/// must return `Err(WriteStallTimeout)` instead of silently succeeding
/// with the timeout error discarded.
#[test]
fn test_issue_1924_write_stall_timeout_surfaced_to_caller() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    // Configure: tiny write buffer to force frequent L0 creation,
    // stop trigger of 1 (any L0 segment triggers stall), and 1ms timeout
    // so the stall expires before compaction can drain L0.
    let cfg = StrataConfig {
        durability: "cache".to_string(),
        storage: StorageConfig {
            write_buffer_size: 128,        // tiny: forces memtable rotation
            l0_stop_writes_trigger: 1,     // stall when ANY L0 segment exists
            l0_slowdown_writes_trigger: 0, // disabled
            write_stall_timeout_ms: 1,     // 1ms: too short for compaction to drain
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };

    let spec = super::spec::OpenSpec::primary(&db_path)
        .with_config(cfg)
        .with_subsystem(crate::search::SearchSubsystem);
    let db = Database::open_runtime(spec).unwrap();

    // Write large values to force memtable rotation and L0 creation.
    // With write_buffer_size=128, a 512-byte value exceeds the threshold.
    // After each commit, schedule_flush_if_needed flushes frozen memtables
    // into L0 segments.
    let big_value = Value::Bytes(vec![0u8; 512]);

    // Rapidly write transactions. Some may succeed (if compaction drains L0
    // between writes), some may timeout. We care that at least one fails.
    let mut saw_timeout = false;
    for i in 0..20 {
        let key_name = format!("key_{}", i);
        let ns_clone = ns.clone();
        let val = big_value.clone();
        let result = db.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns_clone, &key_name), val)?;
            Ok(())
        });
        if matches!(&result, Err(StrataError::WriteStallTimeout { .. })) {
            saw_timeout = true;
            break;
        }
    }

    assert!(
        saw_timeout,
        "Expected at least one WriteStallTimeout error among 20 writes, \
         but all succeeded. L0 count: {}",
        db.storage().max_l0_segment_count()
    );
}

/// Issue #1924 (manual API): Same backpressure fix for begin/commit_transaction path.
#[test]
fn test_issue_1924_write_stall_timeout_manual_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    let cfg = StrataConfig {
        durability: "cache".to_string(),
        storage: StorageConfig {
            write_buffer_size: 128,
            l0_stop_writes_trigger: 1,
            l0_slowdown_writes_trigger: 0,
            write_stall_timeout_ms: 1,
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };

    let spec = super::spec::OpenSpec::primary(&db_path)
        .with_config(cfg)
        .with_subsystem(crate::search::SearchSubsystem);
    let db = Database::open_runtime(spec).unwrap();
    let big_value = Value::Bytes(vec![0u8; 512]);

    let mut saw_timeout = false;
    for i in 0..20 {
        let mut txn = db.begin_transaction(branch_id).unwrap();
        let key = Key::new_kv(ns.clone(), format!("key_{}", i));
        txn.put(key, big_value.clone()).unwrap();
        let result = db.commit_transaction(&mut txn);
        db.end_transaction(txn);
        if matches!(&result, Err(StrataError::WriteStallTimeout { .. })) {
            saw_timeout = true;
            break;
        }
    }

    assert!(
        saw_timeout,
        "Expected WriteStallTimeout via manual commit_transaction path, \
         but all succeeded. L0 count: {}",
        db.storage().max_l0_segment_count()
    );
}

#[test]
fn test_issue_1380_default_codec_is_identity() {
    // Verify that the default codec is "identity" and the MANIFEST records it.
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();
    drop(db);

    // Verify MANIFEST stores "identity" codec
    let manifest_path = temp_dir.path().join("MANIFEST");
    let m = strata_durability::ManifestManager::load(manifest_path).unwrap();
    assert_eq!(m.manifest().codec_id, "identity");
}

/// Serializes tests that mutate `STRATA_ENCRYPTION_KEY`, which is a
/// process-global env var. Without this mutex, parallel tests
/// (e.g. `test_issue_1380_codec_mismatch_rejected` and
/// `test_issue_1380_encryption_rejected_with_wal`) can interleave their
/// set/remove calls — one test's `remove_var` lands between another
/// test's `set_var` and the internal `open_runtime` read, producing a
/// "STRATA_ENCRYPTION_KEY environment variable not set" error instead
/// of the codec-mismatch / WAL-unsupported error the test expects.
///
/// Using `std::sync::Mutex` + `Lazy` rather than pulling in
/// `serial_test` keeps the fix local and dependency-free.
static ENV_VAR_TEST_LOCK: once_cell::sync::Lazy<std::sync::Mutex<()>> =
    once_cell::sync::Lazy::new(|| std::sync::Mutex::new(()));

#[test]
fn test_issue_1380_codec_mismatch_rejected() {
    // A database created with "identity" must reject reopen with a different codec.
    // Use Cache mode to bypass the WAL-not-supported guard (encryption works in
    // Cache mode, WAL codec support is pending).
    let _env_guard = ENV_VAR_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let temp_dir = TempDir::new().unwrap();

    // First open with Cache mode: creates MANIFEST with "identity"
    {
        let cfg = StrataConfig {
            durability: "cache".to_string(),
            ..StrataConfig::default()
        };
        let spec = super::spec::OpenSpec::primary(temp_dir.path())
            .with_config(cfg)
            .with_subsystem(crate::search::SearchSubsystem);
        let db = Database::open_runtime(spec).unwrap();
        drop(db);
    }

    // Second open with mismatched codec in Cache mode: must fail with codec mismatch
    // (We need the env var set so get_codec validation passes before hitting the
    // MANIFEST mismatch check)
    std::env::set_var(
        "STRATA_ENCRYPTION_KEY",
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    );
    let cfg = StrataConfig {
        durability: "cache".to_string(),
        storage: StorageConfig {
            codec: "aes-gcm-256".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };
    let spec = super::spec::OpenSpec::primary(temp_dir.path())
        .with_config(cfg)
        .with_subsystem(crate::search::SearchSubsystem);
    let result = Database::open_runtime(spec);
    std::env::remove_var("STRATA_ENCRYPTION_KEY");

    match result {
        Err(e) => {
            let err = e.to_string();
            assert!(
                err.contains("codec mismatch"),
                "error should mention codec mismatch: {}",
                err
            );
        }
        Ok(_) => panic!("should reject codec mismatch"),
    }
}

#[test]
fn test_issue_1380_encryption_rejected_with_wal() {
    // Non-identity codecs must be rejected when WAL recovery is required,
    // because the WalReader does not yet support codec decoding.
    let _env_guard = ENV_VAR_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let temp_dir = TempDir::new().unwrap();
    std::env::set_var(
        "STRATA_ENCRYPTION_KEY",
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    );
    let cfg = StrataConfig {
        storage: StorageConfig {
            codec: "aes-gcm-256".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };
    // Note: Using config with standard durability mode
    let mut cfg_with_durability = cfg;
    cfg_with_durability.durability = "standard".to_string();
    let spec = super::spec::OpenSpec::primary(temp_dir.path())
        .with_config(cfg_with_durability)
        .with_subsystem(crate::search::SearchSubsystem);
    let result = Database::open_runtime(spec);
    std::env::remove_var("STRATA_ENCRYPTION_KEY");

    match result {
        Err(e) => {
            let err = e.to_string();
            assert!(
                err.contains("not yet supported with WAL"),
                "error should mention WAL limitation: {}",
                err
            );
        }
        Ok(_) => panic!("should reject encryption with WAL-based durability"),
    }
}

// ============================================================================
// T3-E5 Chunk 2: checkpoint-only restart via snapshot load + install
// ============================================================================

/// Write KV entries, checkpoint, drop the database, manually delete the WAL
/// segments (simulating what post-Chunk-4 `compact()` will do), reopen, and
/// verify all data is recovered from the snapshot alone.
///
/// This is the core Epic 5 correctness gate: checkpoints must be a complete
/// recovery artifact, not just a future optimization. Until Chunk 2 wired
/// snapshot loading into the coordinator and engine install decoder, this
/// scenario was impossible — checkpoints were written but never consumed.
///
/// Tranche 4 branch-aware retention will depend on this path being correct
/// for tombstone and TTL state as well; those cases are validated in
/// Chunk 3 once the checkpoint payload carries that metadata.
#[test]
#[serial(open_databases)]
fn test_checkpoint_only_restart_recovers_kv_from_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Step 1: write several KV entries on the user branch.
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        for i in 0u64..10 {
            let key = Key::new_kv(ns.clone(), format!("kv-{}", i));
            let value = Value::String(format!("payload-{}", i));
            db.transaction(branch_id, |txn| {
                txn.put(key, value.clone())?;
                Ok(())
            })
            .unwrap();
        }

        // Step 2: checkpoint. This flushes the WAL, serializes the KV
        // section to a `snap-NNNNNN.chk` file, and updates the MANIFEST
        // with the new snapshot id and watermark.
        db.checkpoint().unwrap();
    }
    // Step 3: drop DB. Clear registry so reopen produces a fresh instance.
    OPEN_DATABASES.lock().clear();

    // Step 4: simulate post-Chunk-4 compact() by manually deleting every WAL
    // segment. The MANIFEST still points at the snapshot; recovery must fall
    // back to snapshot load and surface all 10 entries without WAL replay.
    let wal_dir = db_path.join("wal");
    for entry in std::fs::read_dir(&wal_dir).unwrap().flatten() {
        let p = entry.path();
        if p.is_file() {
            std::fs::remove_file(&p).unwrap();
        }
    }
    // Confirm the WAL dir is now empty so the test is actually exercising
    // snapshot-only recovery rather than accidentally still hitting WAL.
    let remaining: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.path().is_file())
        .collect();
    assert!(
        remaining.is_empty(),
        "WAL dir should be empty before reopen"
    );

    // Step 5: reopen. Recovery must install the snapshot.
    let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

    // Step 6: every committed key must be visible with its original value.
    for i in 0u64..10 {
        let key = Key::new_kv(ns.clone(), format!("kv-{}", i));
        let stored = db
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("kv-{} must survive snapshot-only restart", i));
        assert_eq!(stored.value, Value::String(format!("payload-{}", i)));
    }
}

/// Checkpoint + delta-WAL replay: keys written before the checkpoint arrive
/// through snapshot install; keys written after the checkpoint arrive through
/// WAL replay filtered by the snapshot watermark. Both groups must be visible
/// after reopen, and the snapshot-covered records must not be double-applied.
#[test]
#[serial(open_databases)]
fn test_checkpoint_plus_delta_wal_replay_merges_sources() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Phase 1: write 5 "pre-checkpoint" keys, checkpoint, then write 5
    // "post-checkpoint" keys that only live in the WAL tail.
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        for i in 0u64..5 {
            let key = Key::new_kv(ns.clone(), format!("pre-{}", i));
            db.transaction(branch_id, |txn| {
                txn.put(key, Value::String(format!("v{}", i)))?;
                Ok(())
            })
            .unwrap();
        }
        db.checkpoint().unwrap();

        for i in 0u64..5 {
            let key = Key::new_kv(ns.clone(), format!("post-{}", i));
            db.transaction(branch_id, |txn| {
                txn.put(key, Value::String(format!("w{}", i)))?;
                Ok(())
            })
            .unwrap();
        }
    }
    OPEN_DATABASES.lock().clear();

    // Phase 2: reopen. Both the snapshot-installed pre-* keys and the
    // delta-replayed post-* keys must be visible.
    let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

    for i in 0u64..5 {
        let pre_key = Key::new_kv(ns.clone(), format!("pre-{}", i));
        let pre = db
            .storage()
            .get_versioned(&pre_key, CommitVersion::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("pre-{} must survive via snapshot install", i));
        assert_eq!(pre.value, Value::String(format!("v{}", i)));

        let post_key = Key::new_kv(ns.clone(), format!("post-{}", i));
        let post = db
            .storage()
            .get_versioned(&post_key, CommitVersion::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("post-{} must survive via delta-WAL replay", i));
        assert_eq!(post.value, Value::String(format!("w{}", i)));
    }

    // The post-reopen database must be writable at a commit version above
    // both sources — exercise it to surface monotonicity regressions.
    let followup_key = Key::new_kv(ns.clone(), "after-reopen");
    db.transaction(branch_id, |txn| {
        txn.put(followup_key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();
    let followup = db
        .storage()
        .get_versioned(&followup_key, CommitVersion::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(followup.value, Value::Int(1));
}

/// Codec mismatch at reopen must fail loud regardless of the
/// `allow_lossy_recovery` flag. Lossy recovery is for tolerating WAL
/// corruption, not for silently rewriting the database under a different
/// codec id. The check belongs ahead of the lossy fallback so an operator
/// cannot accidentally discard a database by misconfiguring the codec.
///
/// Because there is only one WAL-compatible codec in the workspace today
/// (`identity`), the test simulates codec drift by rewriting the MANIFEST
/// directly with a foreign codec id, then reopens with `identity`.
#[test]
#[serial(open_databases)]
fn test_codec_mismatch_on_reopen_fails_even_with_lossy_flag() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Create the DB with the default identity codec and write a key so there
    // is real state for lossy mode to potentially discard.
    let database_uuid;
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        db.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), "k"), Value::Int(1))?;
            Ok(())
        })
        .unwrap();
        database_uuid = db.database_uuid();
    }
    OPEN_DATABASES.lock().clear();

    // Rewrite the MANIFEST to claim a different codec id without changing
    // any other on-disk state. This is the smallest faithful simulation of
    // codec drift we can produce with the currently-shipping codec set.
    let manifest_path = db_path.join("MANIFEST");
    std::fs::remove_file(&manifest_path).unwrap();
    strata_durability::ManifestManager::create(
        manifest_path,
        database_uuid,
        "some-other-codec".to_string(),
    )
    .unwrap();

    // Reopen with the default (identity) codec AND allow_lossy_recovery.
    // The lossy flag must not suppress the codec-mismatch error.
    let cfg = StrataConfig {
        allow_lossy_recovery: true,
        ..Default::default()
    };
    let result = Database::open_with_config(&db_path, cfg);
    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("codec mismatch"),
                "reopen with different codec must fail with codec mismatch, got: {}",
                msg
            );
        }
        Ok(_) => panic!("reopen with wrong codec + lossy flag must still fail"),
    }
}

/// Follower open must fail loud when the MANIFEST cannot be parsed.
/// Pre-fix, this case silently degraded to WAL-only recovery, which is
/// unsafe once snapshot-aware compaction reclaims pre-snapshot WAL: the
/// follower would serve stale/empty state without operator visibility.
#[test]
#[serial(open_databases)]
fn test_follower_open_fails_hard_on_corrupt_manifest() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        db.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), "k"), Value::Int(1))?;
            Ok(())
        })
        .unwrap();
    }
    OPEN_DATABASES.lock().clear();

    // Corrupt the MANIFEST on disk so ManifestManager::load fails.
    let manifest_path = db_path.join("MANIFEST");
    std::fs::write(&manifest_path, b"NOT-A-VALID-MANIFEST").unwrap();

    let err = match Database::open_follower(&db_path) {
        Ok(_) => panic!("follower must fail on corrupt MANIFEST"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("manifest"),
        "error should mention MANIFEST, got: {}",
        msg
    );
}

/// Follower open: after the primary checkpoints, a fresh follower must see
/// every committed value — including the ones that now live only in the
/// snapshot on disk — without going through the primary's in-process state.
/// This exercises the Chunk 3 follower migration to the direct callback API
/// plus snapshot install.
#[test]
fn test_follower_open_installs_checkpoint_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Primary writes then checkpoints then exits. The data that is now
    // covered by the snapshot is the follower's only path to see those
    // values after the WAL is manually truncated.
    {
        let primary = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        for i in 0u64..6 {
            let key = Key::new_kv(ns.clone(), format!("k{}", i));
            primary
                .transaction(branch_id, |txn| {
                    txn.put(key, Value::String(format!("v{}", i)))?;
                    Ok(())
                })
                .unwrap();
        }
        primary.checkpoint().unwrap();
    }
    OPEN_DATABASES.lock().clear();

    // Delete WAL so the follower can only see data through the snapshot.
    let wal_dir = db_path.join("wal");
    for entry in std::fs::read_dir(&wal_dir).unwrap().flatten() {
        let p = entry.path();
        if p.is_file() {
            std::fs::remove_file(&p).unwrap();
        }
    }

    // Open as follower and verify all pre-checkpoint data is visible.
    let follower = Database::open_follower(&db_path).unwrap();
    for i in 0u64..6 {
        let key = Key::new_kv(ns.clone(), format!("k{}", i));
        let stored = follower
            .storage()
            .get_versioned(&key, CommitVersion::MAX)
            .unwrap()
            .unwrap_or_else(|| panic!("follower must see k{} via snapshot install", i));
        assert_eq!(stored.value, Value::String(format!("v{}", i)));
    }
}

// ============================================================================
// T3-E5 follow-up: retention-complete checkpoint + compact + reopen
// ============================================================================

/// Tombstones survive checkpoint + compact + reopen.
///
/// Write K, delete K, checkpoint, compact (reclaims WAL), reopen — the
/// key must stay hidden from reads because the snapshot install
/// reconstructs the tombstone barrier. Before the follow-up, this was
/// the corruption mode: the WAL delete record vanished with the WAL
/// and the snapshot didn't carry tombstones, so K would reappear.
#[test]
#[serial(open_databases)]
fn test_checkpoint_compact_preserves_tombstones() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "deleted");

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(1))?;
            Ok(())
        })
        .unwrap();
        db.transaction(branch_id, |txn| {
            txn.delete(key.clone())?;
            Ok(())
        })
        .unwrap();
        db.checkpoint().unwrap();
        db.compact()
            .expect("compact must succeed on snapshot-only coverage");
    }
    OPEN_DATABASES.lock().clear();

    let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
    let observed = db
        .storage()
        .get_versioned(&key, CommitVersion::MAX)
        .unwrap();
    assert!(
        observed.is_none(),
        "deleted key must remain hidden after checkpoint+compact+reopen (tombstone survived)"
    );
}

/// TTL-bound entries still expire at the original deadline after
/// checkpoint + compact + reopen.
///
/// Today's engine has no public TTL-aware transaction API, so the TTL
/// key is injected directly via the recovery write path and the
/// coordinator version is advanced via a separate normal transaction
/// (so `checkpoint()` writes a non-zero watermark and `compact()` can
/// run). The critical invariants — `ttl_ms` round-trips through the
/// snapshot and `get_at_timestamp` still honors the original deadline —
/// are preserved regardless of which write path set the TTL.
#[test]
#[serial(open_databases)]
fn test_checkpoint_compact_preserves_ttl() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let ttl_key = Key::new_kv(ns.clone(), "expires");
    let ttl = Duration::from_millis(60_000);
    let version_for_ttl = CommitVersion(1_000);

    let commit_ts_micros: u64;
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

        // Bump coordinator via a normal transaction so checkpoint writes a
        // real watermark (quiesced_version > 0).
        let warmup_key = Key::new_kv(ns.clone(), "warmup");
        db.transaction(branch_id, |txn| {
            txn.put(warmup_key.clone(), Value::Int(1))?;
            Ok(())
        })
        .unwrap();

        // Inject the TTL-bound entry at a known commit version above the
        // coordinator's current floor.
        db.storage
            .put_with_version_mode(
                ttl_key.clone(),
                Value::String("temporary".into()),
                version_for_ttl,
                Some(ttl),
                WriteMode::Append,
            )
            .unwrap();
        commit_ts_micros = db
            .storage()
            .get_versioned(&ttl_key, CommitVersion::MAX)
            .unwrap()
            .unwrap()
            .timestamp
            .as_micros();

        db.checkpoint().unwrap();
        db.compact()
            .expect("compact must succeed on snapshot-only coverage");
    }
    OPEN_DATABASES.lock().clear();

    let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

    // Before expiry: visible.
    let pre = db
        .storage()
        .get_at_timestamp(&ttl_key, commit_ts_micros + ttl.as_micros() as u64 - 1)
        .unwrap();
    assert!(pre.is_some(), "entry must be visible before TTL expiry");

    // After expiry: gone.
    let post = db
        .storage()
        .get_at_timestamp(&ttl_key, commit_ts_micros + ttl.as_micros() as u64 + 1)
        .unwrap();
    assert!(
        post.is_none(),
        "TTL barrier must survive checkpoint+compact+reopen"
    );
}

/// Branch metadata survives checkpoint + compact + reopen.
///
/// Create a user branch, checkpoint, compact, reopen — `branches.exists`
/// must still return true. The Branch section install decoder routes
/// branch-metadata entries back into the global branch index under the
/// nil-UUID sentinel so the in-memory branch registry repopulates on
/// reopen from the snapshot alone.
#[test]
#[serial(open_databases)]
fn test_checkpoint_compact_preserves_branch_metadata() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let branch_name = "retention-ctx";

    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        db.branches().create(branch_name).unwrap();
        assert!(db.branches().exists(branch_name).unwrap());
        db.checkpoint().unwrap();
        db.compact()
            .expect("compact must succeed on snapshot-only coverage");
    }
    OPEN_DATABASES.lock().clear();

    let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
    assert!(
        db.branches().exists(branch_name).unwrap(),
        "branch metadata must survive checkpoint+compact+reopen via snapshot install"
    );
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
fn shutdown_releases_registry_slot() {
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

    // The entry must be removed even though we still hold the `Arc<Database>`
    // (i.e. before Drop runs). This is the deterministic-release property.
    assert!(
        !OPEN_DATABASES.lock().contains_key(&canonical_key),
        "successful shutdown must release the registry slot without waiting for Drop"
    );

    // Drop the remaining handle so the on-disk file lock releases before the
    // `temp_dir` cleans up.
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
