use super::*;

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
