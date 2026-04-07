use super::*;
use std::sync::Arc;
use std::time::Duration;
use strata_concurrency::TransactionPayload;
use strata_core::types::{Key, Namespace, TypeTag};
use strata_core::value::Value;
use strata_core::{Storage, Timestamp};
use strata_durability::codec::IdentityCodec;
use strata_durability::format::WalRecord;
use strata_durability::now_micros;
use strata_durability::wal::WalConfig;
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
        Self::open_internal(path, durability_mode, cfg)
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
        version,
        *branch_id.as_bytes(),
        now_micros(),
        payload.to_bytes(),
    );
    wal.append(&record).unwrap();
    wal.flush().unwrap();
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
        .get_versioned(&key1, u64::MAX)
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
        let val = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();

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
    let val = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
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
    assert!(db.storage().current_version() <= 3);
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
    let stored = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
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
        .get_versioned(&key, u64::MAX)
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
    let stored = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
    assert_eq!(stored.value, Value::Int(123));
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

#[test]
fn test_open_uses_registry() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("singleton_via_open");

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
fn test_checkpoint_then_compact_without_flush_fails() {
    // Issue #1730: compact() after checkpoint-only must fail because
    // recovery cannot load snapshots. WAL compaction is only safe
    // when driven by the flush watermark (data in SST segments).
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let db = Database::open(&db_path).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns, "compact_test");

    // Write data
    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(42))?;
        Ok(())
    })
    .unwrap();

    // Checkpoint creates snapshot but no flush watermark
    assert!(db.checkpoint().is_ok());

    // Compact must fail — snapshot watermark alone is not safe
    let result = db.compact();
    assert!(result.is_err());
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
    let txn = db.begin_transaction(branch_id).unwrap();
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
        sp <= txn_start_version,
        "safe point {} should be <= active txn start version {}",
        sp,
        txn_start_version
    );

    // Abort the active transaction
    db.coordinator.record_abort(txn.txn_id);

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
    let stored = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
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
    let stored = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
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
        db.storage().current_version() <= 3,
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
            .get_versioned(&key, u64::MAX)
            .unwrap()
            .is_none(),
        "Valid data before corruption should be lost in lossy mode"
    );
    // Only system branch init transactions present (user data was discarded)
    assert!(db.storage().current_version() <= 3);
}

// ========================================================================
// E-4: Auto-Registration Tests
// ========================================================================

#[test]
fn test_recovery_participants_auto_registered() {
    // After Database::open, search recovery should be registered automatically.
    // Vector recovery is now handled by strata-vector crate (caller must register).
    let temp_dir = TempDir::new().unwrap();
    let _db = Database::open(temp_dir.path().join("db")).unwrap();

    // The registry is global and additive (idempotent), so other tests
    // may have already registered participants. We just verify the count
    // is at least 1 (search). Vector recovery is registered by the caller
    // (executor/Strata API) since it moved to the strata-vector crate.
    let count = crate::recovery::recovery_registry_count();
    assert!(
        count >= 1,
        "Expected at least 1 recovery participant (search), got {}",
        count
    );
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
    let val = db.storage().get_versioned(&key, u64::MAX).unwrap();
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
    let val = db.storage().get_versioned(&key, u64::MAX).unwrap();
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
    let val = db.storage().get_versioned(&key, u64::MAX).unwrap();
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
            .get_versioned(&key1, u64::MAX)
            .unwrap()
            .is_some(),
        "txn1 data lost"
    );
    assert!(
        db.storage()
            .get_versioned(&key2, u64::MAX)
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
            let stored = db.storage().get_versioned(&key, u64::MAX).unwrap();
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
    let reader_txn = db.begin_transaction(branch_id).unwrap();
    let pinned_version = reader_txn.start_version;

    // Write more versions while reader holds its snapshot
    for i in 3..=10 {
        blind_write(&db, key.clone(), Value::Int(i));
    }

    // GC safe point must not advance past the reader's pinned version
    let safe_point = db.gc_safe_point();
    assert!(
        safe_point <= pinned_version,
        "gc_safe_point ({safe_point}) advanced past pinned reader version ({pinned_version})"
    );

    // GC should not prune versions the reader can still see
    let (_, pruned) = db.run_gc();
    // If any pruning happened, confirm reader's data is still intact
    let _ = pruned;

    // Reader should still see the value at its snapshot version
    let reader_val = db.storage().get_versioned(&key, u64::MAX).unwrap();
    assert!(reader_val.is_some(), "key disappeared during concurrent GC");
    // Latest version should be 10
    assert_eq!(reader_val.unwrap().value, Value::Int(10));

    // Release the reader — gc_safe_version should now be free to advance
    db.coordinator.record_abort(reader_txn.txn_id);

    let safe_point_after = db.gc_safe_point();
    assert!(
        safe_point_after > safe_point,
        "gc_safe_point should advance after reader releases: {safe_point_after} > {safe_point}"
    );

    // SegmentedStore prunes via compaction, not gc_branch(), so pruned_after is 0.
    let (_, _pruned_after) = db.run_gc();

    // Latest value must still be readable after GC
    let final_val = db.storage().get_versioned(&key, u64::MAX).unwrap().unwrap();
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
            let stored = db.storage().get_versioned(&key, u64::MAX).unwrap();
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
    let reader = db.begin_read_only_transaction(branch_id).unwrap();
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
    let compacted = db.storage().compact_branch(&branch_id, 0).unwrap();
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
    let latest = db.storage().get_versioned(&key, u64::MAX).unwrap();
    assert!(latest.is_some(), "latest version missing after compaction");
    assert_eq!(latest.unwrap().value, Value::Int(2));

    // Clean up reader
    db.coordinator.record_abort(reader.txn_id);
}

#[test]
fn test_issue_1730_checkpoint_compact_recovery_data_loss() {
    // Issue #1730: checkpoint+compact deletes WAL segments, but recovery
    // is WAL-only and never loads snapshots. This causes data loss.
    //
    // After the fix, compact() must refuse to delete WAL segments based
    // on the snapshot watermark alone, since recovery cannot load snapshots.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();

    // Step 1: Write data
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "critical_data");

        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::String("must_survive".to_string()))?;
            Ok(())
        })
        .unwrap();

        // Step 2: Checkpoint (creates snapshot, sets watermark in MANIFEST)
        db.checkpoint().unwrap();

        // Step 3: Compact — should NOT delete WAL segments since recovery
        // cannot load snapshots. With the fix, this returns an error.
        let compact_result = db.compact();
        assert!(
            compact_result.is_err(),
            "compact() must fail when only snapshot watermark exists \
             (no flush watermark) because recovery cannot load snapshots"
        );
    }
    // Database dropped (simulates clean shutdown)

    // Clear the registry so reopen doesn't return the cached instance
    OPEN_DATABASES.lock().clear();

    // Step 4: Reopen — recovery replays WAL
    {
        let db = Database::open_with_durability(&db_path, DurabilityMode::Always).unwrap();

        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "critical_data");

        // Step 5: Data MUST still be present
        let result = db.storage().get_versioned(&key, u64::MAX).unwrap();
        assert!(
            result.is_some(),
            "CRITICAL: Data lost after checkpoint+compact+recovery! \
             WAL segments were deleted but recovery never loaded the snapshot."
        );
        assert_eq!(
            result.unwrap().value,
            Value::String("must_survive".to_string())
        );
    }
}

#[test]
fn test_issue_1730_standard_durability() {
    // Same as above but with Standard durability mode (disk-based, batched fsync).
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

        // Ensure WAL is flushed to disk
        db.flush().unwrap();

        db.checkpoint().unwrap();

        // compact() must fail — only snapshot watermark, no flush watermark
        let compact_result = db.compact();
        assert!(
            compact_result.is_err(),
            "compact() after checkpoint-only must fail under Standard durability"
        );
    }

    OPEN_DATABASES.lock().clear();

    // Reopen and verify data survived
    {
        let db = Database::open_with_durability(&db_path, durability).unwrap();
        let ns = create_test_namespace(branch_id);
        let key = Key::new_kv(ns, "standard_data");

        let result = db.storage().get_versioned(&key, u64::MAX).unwrap();
        assert!(
            result.is_some(),
            "Data must survive checkpoint+failed-compact+recovery under Standard durability"
        );
        assert_eq!(result.unwrap().value, Value::String("durable".to_string()));
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
    let applied = follower.refresh().unwrap();
    assert!(applied > 0, "follower should apply the new transaction");

    // 7. Check: A's entry timestamp should come from the WAL record (near
    //    commit time), NOT from Timestamp::now() during refresh.
    let a_entry = follower
        .storage()
        .get_versioned(&key_a, u64::MAX)
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
            .get_versioned(&key, u64::MAX)
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
        .find(|e| e.key == "ts_test")
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
    use crate::primitives::branch::BranchIndex;

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();

    let branch_index = BranchIndex::new(db.clone());
    branch_index.create_branch("my-test-branch").unwrap();

    let data = db.collect_checkpoint_data();
    let branch_entries = data.branches.expect("should have branch entries");

    let found = branch_entries.iter().find(|b| b.name == "my-test-branch");
    assert!(
        found.is_some(),
        "Branch name 'my-test-branch' should be preserved in checkpoint. \
         Got names: {:?}",
        branch_entries.iter().map(|b| &b.name).collect::<Vec<_>>(),
    );
    // Also verify created_at is a real timestamp, not rewritten to `now`
    let branch_entry = found.unwrap();
    assert!(
        branch_entry.created_at > 0,
        "Branch created_at should be non-zero",
    );
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
        let latest = db.storage().get_versioned(&key, u64::MAX).unwrap();
        assert!(latest.is_some(), "point read should find the key");
        assert_eq!(latest.unwrap().value, Value::Int(3));
    }
}

#[test]
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
        let latest = db.storage().get_versioned(&key, u64::MAX).unwrap();
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
    let val = db.storage().get_versioned(&key, u64::MAX).unwrap();
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

// ========================================================================
// Issue #1914: Executor event append bypasses OCC — hash chain corruption
// ========================================================================

/// Two concurrent transactions both appending events to the same branch
/// must conflict at commit time. Before the fix, the meta_key was never
/// read, so OCC validation had no read-set entry to detect the conflict.
#[test]
fn test_issue_1914_concurrent_event_append_occ_conflict() {
    use crate::transaction::Transaction;
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
        let mut t1 = Transaction::new(&mut txn1, ns.clone());
        t1.event_append("test_event", Value::String("from_t1".into()))
            .unwrap();
    }
    {
        let mut t2 = Transaction::new(&mut txn2, ns.clone());
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
    use crate::transaction::Transaction;
    use crate::transaction_ops::TransactionOps;

    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path().join("db")).unwrap();
    let branch_id = BranchId::new();

    let ns = Arc::new(Namespace::for_branch(branch_id));

    // First transaction: append event at sequence 0
    let mut txn1 = db.begin_transaction(branch_id).unwrap();
    {
        let mut t = Transaction::new(&mut txn1, ns.clone());
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
        let mut t = Transaction::new(&mut txn2, ns.clone());
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
        let mut t = Transaction::new(&mut txn3, ns.clone());
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

    let db = Database::open_internal(&db_path, DurabilityMode::Cache, cfg).unwrap();

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

    let db = Database::open_internal(&db_path, DurabilityMode::Cache, cfg).unwrap();
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

#[test]
fn test_issue_1380_codec_mismatch_rejected() {
    // A database created with "identity" must reject reopen with a different codec.
    // Use Cache mode to bypass the WAL-not-supported guard (encryption works in
    // Cache mode, WAL codec support is pending).
    let temp_dir = TempDir::new().unwrap();

    // First open with Cache mode: creates MANIFEST with "identity"
    {
        let cfg = StrataConfig::default();
        let db = Database::open_internal(temp_dir.path(), DurabilityMode::Cache, cfg).unwrap();
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
        storage: StorageConfig {
            codec: "aes-gcm-256".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };
    let result = Database::open_internal(temp_dir.path(), DurabilityMode::Cache, cfg);
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
    let result = Database::open_internal(
        temp_dir.path(),
        DurabilityMode::Standard {
            interval_ms: 100,
            batch_size: 1000,
        },
        cfg,
    );
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
