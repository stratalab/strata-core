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
        let segment_path =
            strata_storage::durability::format::WalSegment::segment_path(&wal_dir, 1);
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

    // Ephemeral cache is an open mode, not a disk-backed durability.
    let cache = Database::cache().unwrap();
    assert!(cache.is_cache());
}

#[test]
fn test_primary_open_rejects_cache_durability_string() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("cache_is_not_disk_durability");

    let result = Database::open_runtime(
        super::spec::OpenSpec::primary(&db_path)
            .with_config(StrataConfig {
                durability: "cache".to_string(),
                ..StrataConfig::default()
            })
            .with_subsystem(crate::search::SearchSubsystem),
    );

    let error = match result {
        Ok(_) => panic!("primary open must reject durability = cache"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("Database::cache()"),
        "error must point callers at cache open mode, got: {error}"
    );
}

#[test]
fn test_cache_open_rejects_invalid_durability_string() {
    let result = Database::open_runtime(
        super::spec::OpenSpec::cache()
            .with_config(StrataConfig {
                durability: "turbo".to_string(),
                ..StrataConfig::default()
            })
            .with_subsystem(crate::search::SearchSubsystem),
    );

    let error = match result {
        Ok(_) => panic!("cache open must still validate durability config strings"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("Invalid durability mode 'turbo'"),
        "cache open must reject invalid durability strings, got: {error}"
    );
}

#[test]
fn test_cache_open_rejects_cache_durability_string() {
    let result = Database::open_runtime(
        super::spec::OpenSpec::cache()
            .with_config(StrataConfig {
                durability: "cache".to_string(),
                ..StrataConfig::default()
            })
            .with_subsystem(crate::search::SearchSubsystem),
    );

    let error = match result {
        Ok(_) => panic!("cache open mode must not accept durability = cache"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("Database::cache()"),
        "error should explain cache is selected by open mode, got: {error}"
    );
}

fn observable_storage_runtime_config() -> StorageConfig {
    StorageConfig {
        max_branches: 17,
        max_versions_per_key: 3,
        write_buffer_size: 256 * 1024,
        max_immutable_memtables: 2,
        target_file_size: 3 * 1024 * 1024,
        level_base_bytes: 11 * 1024 * 1024,
        data_block_size: 8 * 1024,
        bloom_bits_per_key: 13,
        compaction_rate_limit: 7 * 1024 * 1024,
        ..StorageConfig::default()
    }
}

struct BlockCacheCapacityGuard {
    previous_capacity: usize,
}

impl BlockCacheCapacityGuard {
    fn capture() -> Self {
        Self {
            previous_capacity: strata_storage::block_cache::global_capacity(),
        }
    }
}

impl Drop for BlockCacheCapacityGuard {
    fn drop(&mut self) {
        strata_storage::block_cache::set_global_capacity(self.previous_capacity);
    }
}

fn assert_observable_storage_runtime_config(db: &Database, cfg: &StorageConfig) {
    let storage = db.storage();
    let runtime_config = crate::database::config::storage_runtime_config_from(cfg);
    assert_eq!(
        storage.write_buffer_size_for_test(),
        runtime_config.write_buffer_size
    );
    assert_eq!(storage.max_branches_for_test(), cfg.max_branches);
    assert_eq!(
        storage.max_versions_per_key_for_test(),
        cfg.max_versions_per_key
    );
    assert_eq!(
        storage.max_immutable_memtables_for_test(),
        runtime_config.max_immutable_memtables
    );
    assert_eq!(storage.target_file_size(), cfg.target_file_size);
    assert_eq!(storage.level_base_bytes(), cfg.level_base_bytes);
    assert_eq!(storage.data_block_size(), cfg.data_block_size);
    assert_eq!(storage.bloom_bits_per_key(), cfg.bloom_bits_per_key);
    assert_eq!(
        storage.compaction_rate_limit_for_test(),
        cfg.compaction_rate_limit
    );
}

#[test]
#[serial(open_databases)]
fn persistent_open_applies_observable_storage_runtime_knobs() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("persistent_runtime_config");
    let storage = observable_storage_runtime_config();
    let cfg = StrataConfig {
        storage: storage.clone(),
        ..StrataConfig::default()
    };

    let db = Database::open_runtime(super::spec::OpenSpec::primary(&db_path).with_config(cfg))
        .expect("primary open should apply storage runtime config");

    assert!(!db.is_cache());
    assert_observable_storage_runtime_config(&db, &storage);

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

#[test]
#[serial(open_databases)]
fn cache_open_applies_observable_storage_runtime_knobs() {
    let storage = observable_storage_runtime_config();
    let cfg = StrataConfig {
        storage: storage.clone(),
        ..StrataConfig::default()
    };

    let db = Database::open_runtime(super::spec::OpenSpec::cache().with_config(cfg))
        .expect("cache open should apply storage runtime config");

    assert!(db.is_cache());
    assert_observable_storage_runtime_config(&db, &storage);
}

#[test]
#[serial(open_databases)]
fn update_config_reapplies_observable_storage_runtime_knobs() {
    let initial_storage = observable_storage_runtime_config();
    let initial_cfg = StrataConfig {
        storage: initial_storage.clone(),
        ..StrataConfig::default()
    };
    let db = Database::open_runtime(super::spec::OpenSpec::cache().with_config(initial_cfg))
        .expect("cache open should succeed");
    assert_observable_storage_runtime_config(&db, &initial_storage);

    let updated_storage = StorageConfig {
        write_buffer_size: 384 * 1024,
        max_branches: 23,
        max_versions_per_key: 5,
        max_immutable_memtables: 4,
        target_file_size: 5 * 1024 * 1024,
        level_base_bytes: 19 * 1024 * 1024,
        data_block_size: 16 * 1024,
        bloom_bits_per_key: 15,
        compaction_rate_limit: 3 * 1024 * 1024,
        ..initial_storage
    };

    db.update_config(|cfg| {
        cfg.storage = updated_storage.clone();
    })
    .expect("storage runtime config update should succeed");

    assert_observable_storage_runtime_config(&db, &updated_storage);
}

#[test]
#[serial(open_databases)]
fn persistent_open_applies_memory_budget_runtime_derivations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("persistent_memory_budget");
    let storage = StorageConfig {
        memory_budget: 32 << 20,
        write_buffer_size: 512 * 1024,
        max_immutable_memtables: 7,
        ..observable_storage_runtime_config()
    };
    let cfg = StrataConfig {
        storage: storage.clone(),
        ..StrataConfig::default()
    };

    let db = Database::open_runtime(super::spec::OpenSpec::primary(&db_path).with_config(cfg))
        .expect("primary open should apply memory-budget-derived storage config");

    assert_observable_storage_runtime_config(&db, &storage);

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

#[test]
#[serial(open_databases)]
fn cache_open_applies_memory_budget_runtime_derivations() {
    let storage = StorageConfig {
        memory_budget: 32 << 20,
        write_buffer_size: 512 * 1024,
        max_immutable_memtables: 7,
        ..observable_storage_runtime_config()
    };
    let cfg = StrataConfig {
        storage: storage.clone(),
        ..StrataConfig::default()
    };

    let db = Database::open_runtime(super::spec::OpenSpec::cache().with_config(cfg))
        .expect("cache open should apply memory-budget-derived storage config");

    assert_observable_storage_runtime_config(&db, &storage);
}

#[test]
#[serial(open_databases)]
fn primary_open_characterizes_tiny_memory_budget() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("primary_tiny_memory_budget");
    let _capacity_guard = BlockCacheCapacityGuard::capture();
    let storage = StorageConfig {
        memory_budget: 1,
        block_cache_size: 64 << 20,
        write_buffer_size: 128 << 20,
        max_immutable_memtables: 7,
        ..observable_storage_runtime_config()
    };
    let cfg = StrataConfig {
        storage,
        ..StrataConfig::default()
    };

    strata_storage::block_cache::set_global_capacity(8 << 20);

    let db = Database::open_runtime(super::spec::OpenSpec::primary(&db_path).with_config(cfg))
        .expect("primary open should accept and apply tiny memory budgets");

    assert!(!db.is_cache());
    assert_eq!(
        strata_storage::block_cache::global_capacity(),
        0,
        "memory_budget=1 derives an explicit zero-byte block cache, not auto"
    );
    assert_eq!(
        db.storage().write_buffer_size_for_test(),
        0,
        "memory_budget=1 derives a zero-byte active write buffer"
    );
    assert_eq!(
        db.storage().max_immutable_memtables_for_test(),
        1,
        "memory_budget derivation still retains one immutable memtable"
    );

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

#[test]
#[serial(open_databases)]
fn cache_open_applies_default_block_cache_runtime_config() {
    let _capacity_guard = BlockCacheCapacityGuard::capture();
    let mut cfg = StrataConfig::default();
    crate::database::profile::apply_hardware_profile_if_defaults(&mut cfg);
    let runtime_config = crate::database::config::storage_runtime_config_from(&cfg.storage);

    strata_storage::block_cache::set_global_capacity(1);

    let db =
        Database::open_runtime(super::spec::OpenSpec::cache().with_config(StrataConfig::default()))
            .expect("cache open should apply default block-cache runtime config");

    assert!(db.is_cache());
    let applied_capacity = strata_storage::block_cache::global_capacity();
    match runtime_config.block_cache {
        strata_storage::runtime_config::StorageBlockCacheConfig::Bytes(bytes) => {
            assert_eq!(applied_capacity, bytes);
        }
        strata_storage::runtime_config::StorageBlockCacheConfig::Auto => {
            assert_ne!(applied_capacity, 1);
            assert!(applied_capacity > 0);
        }
        _ => unreachable!("unknown storage block-cache config variant"),
    }
}

#[test]
#[serial(open_databases)]
fn cache_open_rejects_invalid_codec() {
    let cfg = StrataConfig {
        storage: StorageConfig {
            codec: "missing-codec".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };

    let error = match Database::open_runtime(super::spec::OpenSpec::cache().with_config(cfg)) {
        Ok(_) => panic!("cache open should reject an unknown storage codec"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("cache database could not initialize codec 'missing-codec'"),
        "error should name the rejected cache codec, got: {error}"
    );
}

#[test]
#[serial(open_databases)]
fn primary_open_rejects_invalid_codec() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("primary_invalid_codec");
    let cfg = StrataConfig {
        storage: StorageConfig {
            codec: "missing-codec".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };

    let error =
        match Database::open_runtime(super::spec::OpenSpec::primary(&db_path).with_config(cfg)) {
            Ok(_) => panic!("primary open should reject an unknown storage codec"),
            Err(error) => error,
        };

    assert!(
        error.to_string().contains("missing-codec"),
        "error should name the rejected primary codec, got: {error}"
    );
}

#[test]
#[serial(open_databases)]
fn follower_open_rejects_invalid_codec() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("follower_invalid_codec");
    std::fs::create_dir_all(&db_path).unwrap();
    let cfg = StrataConfig {
        storage: StorageConfig {
            codec: "missing-codec".to_string(),
            ..StorageConfig::default()
        },
        ..StrataConfig::default()
    };

    let error =
        match Database::open_runtime(super::spec::OpenSpec::follower(&db_path).with_config(cfg)) {
            Ok(_) => panic!("follower open should reject an unknown storage codec"),
            Err(error) => error,
        };

    assert!(
        error.to_string().contains("missing-codec"),
        "error should name the rejected follower codec, got: {error}"
    );
}

#[test]
#[serial(open_databases)]
fn follower_open_applies_block_cache_runtime_config() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("follower_block_cache");
    let _capacity_guard = BlockCacheCapacityGuard::capture();
    let storage = StorageConfig {
        block_cache_size: 7 << 20,
        ..StorageConfig::default()
    };
    let cfg = StrataConfig {
        storage,
        ..StrataConfig::default()
    };

    let primary =
        Database::open_runtime(super::spec::OpenSpec::primary(&db_path).with_config(cfg.clone()))
            .expect("primary open should create recoverable database state");
    primary.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();

    strata_storage::block_cache::set_global_capacity(1);

    let follower =
        Database::open_runtime(super::spec::OpenSpec::follower(&db_path).with_config(cfg))
            .expect("follower open should apply storage runtime config");

    assert_eq!(strata_storage::block_cache::global_capacity(), 7 << 20);

    follower.shutdown().unwrap();
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
#[serial(open_databases)]
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
#[serial(open_databases)]
fn test_open_rejects_missing_segments_root_after_manifest_exists() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("missing_segments_root");
    let branch_id = BranchId::new();
    let ns = Arc::new(Namespace::new(branch_id, "default".to_string()));

    {
        let db = Database::open(&db_path).unwrap();
        db.transaction(branch_id, |txn| {
            txn.put(
                Key::new_kv(ns.clone(), "persistent"),
                Value::Bytes(b"segment-backed".to_vec()),
            )?;
            Ok(())
        })
        .unwrap();
        db.flush().unwrap();
        db.shutdown().unwrap();
    }
    OPEN_DATABASES.lock().clear();

    let segments_dir = db_path.join("segments");
    assert!(
        segments_dir.exists(),
        "test setup must create the authoritative storage root before deleting it"
    );
    std::fs::remove_dir_all(&segments_dir).unwrap();

    let err = match Database::open(&db_path) {
        Ok(_) => panic!("strict reopen must refuse a database whose segments root vanished"),
        Err(err) => err,
    };
    assert!(
        matches!(err, StrataError::Corruption { .. }),
        "missing authoritative segments root must surface as corruption, got {err:?}"
    );
    assert!(
        err.to_string().contains("segments directory"),
        "error should name the missing storage root, got: {err}"
    );
}

#[test]
#[serial(open_databases)]
fn test_open_with_config_rejects_registry_reuse_when_requested_config_drifted() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("reuse_requested_config_drift");

    let db = Database::open(&db_path).unwrap();

    let mut drifted = StrataConfig::default();
    drifted.snapshot_retention.retain_count = 1;

    let err = match Database::open_with_config(&db_path, drifted) {
        Ok(_) => panic!("drifted explicit config must not silently reuse the running database"),
        Err(err) => err,
    };
    assert!(
        matches!(err, StrataError::IncompatibleReuse { .. }),
        "expected IncompatibleReuse for explicit config drift, got {:?}",
        err
    );
    assert!(
        err.to_string().contains("programmatic config")
            || err.to_string().contains("explicit configuration"),
        "reuse rejection should name the explicit-config surface, got: {}",
        err
    );

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
}

#[test]
#[serial(open_databases)]
fn test_open_rejects_registry_reuse_when_strata_toml_drifted() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("reuse_strata_toml_drift");

    let db = Database::open(&db_path).unwrap();

    let mut drifted = db.config();
    drifted.snapshot_retention.retain_count = 1;
    drifted.write_to_file(&db_path.join("strata.toml")).unwrap();

    let err = match Database::open(&db_path) {
        Ok(_) => panic!("stale on-disk strata.toml must not silently reuse the running database"),
        Err(err) => err,
    };
    assert!(
        matches!(err, StrataError::IncompatibleReuse { .. }),
        "expected IncompatibleReuse for strata.toml drift, got {:?}",
        err
    );
    assert!(
        err.to_string().contains("strata.toml"),
        "reuse rejection should name the control artifact, got: {}",
        err
    );

    db.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
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
#[serial(open_databases)]
fn test_concurrent_open_same_path_returns_same_instance() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("singleton_via_open_concurrent");
    let barrier = Arc::new(Barrier::new(3));

    let spawn_open = |barrier: Arc<Barrier>, path: std::path::PathBuf| {
        thread::spawn(move || {
            barrier.wait();
            Database::open(&path).unwrap()
        })
    };

    let h1 = spawn_open(Arc::clone(&barrier), db_path.clone());
    let h2 = spawn_open(Arc::clone(&barrier), db_path.clone());
    barrier.wait();

    let db1 = h1.join().expect("first opener thread panicked");
    let db2 = h2.join().expect("second opener thread panicked");

    assert!(Arc::ptr_eq(&db1, &db2));

    db1.shutdown().unwrap();
    OPEN_DATABASES.lock().clear();
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

fn run_primary_recovery(
    db_path: &std::path::Path,
    cfg: StrataConfig,
) -> Result<crate::database::recovery::RecoveryOutcome, crate::database::RecoveryError> {
    let layout = strata_storage::durability::layout::DatabaseLayout::from_root(db_path);
    Database::run_recovery(
        db_path,
        &layout,
        crate::database::recovery::RecoveryRuntimeConfig::from_strata_config(&cfg),
        crate::database::recovery::RecoveryMode::Primary,
    )
}

fn seed_snapshot_fixture(db_path: &std::path::Path, snapshot_id: u64, watermark: u64) -> PathBuf {
    const TEST_UUID: [u8; 16] = [0xAA; 16];

    let snapshots_dir = db_path.join("snapshots");
    std::fs::create_dir_all(&snapshots_dir).unwrap();
    let writer = strata_storage::durability::SnapshotWriter::new(
        snapshots_dir.clone(),
        Box::new(strata_storage::durability::codec::IdentityCodec),
        TEST_UUID,
    )
    .unwrap();
    let sections = vec![strata_storage::durability::SnapshotSection::new(
        strata_storage::durability::format::primitive_tags::KV,
        vec![0, 0, 0, 0],
    )];
    let info = writer
        .create_snapshot(snapshot_id, watermark, sections)
        .unwrap();

    let mut mgr = strata_storage::durability::ManifestManager::create(
        db_path.join("MANIFEST"),
        TEST_UUID,
        "identity".to_string(),
    )
    .unwrap();
    mgr.set_snapshot_watermark(snapshot_id, TxnId(watermark))
        .unwrap();

    info.path
}

fn write_invalid_payload_wal(db_path: &std::path::Path) {
    let wal_dir = db_path.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let mut wal = strata_storage::durability::wal::WalWriter::new(
        wal_dir,
        [0u8; 16],
        strata_storage::durability::wal::DurabilityMode::Always,
        WalConfig::for_testing(),
        Box::new(strata_storage::durability::codec::IdentityCodec),
    )
    .unwrap();
    let record = WalRecord::new(TxnId(1), [7u8; 16], now_micros(), vec![0xFF]);
    wal.append(&record).unwrap();
    wal.flush().unwrap();
}

fn seed_outer_len_crc_mismatch(db_path: &std::path::Path) {
    let wal_dir = db_path.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    write_wal_txn(
        &wal_dir,
        1,
        branch_id,
        vec![(Key::new_kv(ns, "crc"), Value::Bytes(b"value".to_vec()))],
        vec![],
        1,
    );

    let segment_path = wal_dir.join("wal-000001.seg");
    let mut bytes = std::fs::read(&segment_path).unwrap();
    bytes[strata_storage::durability::SEGMENT_HEADER_SIZE_V2 + 5] ^= 0xFF;
    std::fs::write(&segment_path, &bytes).unwrap();
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

    // T3-E10: lossy fallback must be observable. The report must carry the
    // count of records applied before the coordinator errored and the
    // storage version reached, so operators can distinguish "empty because
    // lossy wipe" from "empty because genuinely empty".
    let report = db
        .last_lossy_recovery_report()
        .expect("lossy fallback must populate LossyRecoveryReport");
    assert!(
        report.records_applied_before_failure >= 1,
        "at least one valid record was applied before the corrupt segment; \
         got records_applied_before_failure = {}",
        report.records_applied_before_failure
    );
    assert!(
        report.discarded_on_wipe,
        "whole-database wipe is the current pinned contract"
    );
    assert!(
        !report.error.is_empty(),
        "report.error must render the coordinator error, got empty string"
    );
    assert!(
        report.version_reached_before_failure > CommitVersion::ZERO,
        "storage version should have advanced past zero before the wipe, got {}",
        report.version_reached_before_failure.as_u64()
    );
    // WAL-level corruption currently surfaces as `StrataError::Storage`
    // (the coordinator wraps WAL read failures with
    // `StrataError::storage(...)`), so `LossyErrorKind` classifies this
    // scenario under `Storage`. Lock this in so a future refactor that
    // regresses the mapping — or that reclassifies WAL corruption
    // upstream — trips the test and forces a conscious doc/enum update.
    use crate::LossyErrorKind;
    assert_eq!(
        report.error_kind,
        LossyErrorKind::Storage,
        "WAL read failure must classify as Storage per current upstream \
         wrapping, got {:?}",
        report.error_kind
    );
}

#[test]
fn test_non_lossy_recovery_has_no_report() {
    // A healthy, strict open leaves the lossy-report slot at `None`.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let wal_dir = db_path.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    write_wal_txn(
        &wal_dir,
        1,
        branch_id,
        vec![(Key::new_kv(ns.clone(), "k"), Value::Bytes(b"v".to_vec()))],
        vec![],
        1,
    );

    let db = Database::open(&db_path).unwrap();
    assert!(
        db.last_lossy_recovery_report().is_none(),
        "strict open must not populate a lossy-recovery report"
    );

    // Cache databases never perform recovery — also `None`.
    let cache = Database::cache().unwrap();
    assert!(cache.last_lossy_recovery_report().is_none());
}

#[test]
fn test_lossy_recovery_report_on_immediate_failure() {
    // When recovery errors before any record is applied, the report must
    // still populate with zero counters — differentiating "lossy fired but
    // nothing was in-flight" from "lossy did not fire".
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");

    // Write a single corrupt segment with no preceding valid records. The
    // coordinator will error on the first read attempt; `on_record` never
    // fires, so records_applied_before_failure must be zero.
    let wal_dir = db_path.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::write(
        wal_dir.join("wal-000001.seg"),
        b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER",
    )
    .unwrap();

    let cfg = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let db = Database::open_with_config(&db_path, cfg).unwrap();

    let report = db
        .last_lossy_recovery_report()
        .expect("lossy fallback must populate LossyRecoveryReport even on immediate failure");
    assert_eq!(
        report.records_applied_before_failure, 0,
        "no records should have been applied before the immediate failure"
    );
    assert_eq!(
        report.version_reached_before_failure,
        CommitVersion::ZERO,
        "storage version must stay at zero when no records applied"
    );
    assert!(report.discarded_on_wipe);
    assert!(!report.error.is_empty());
    // See note in the sibling `_discards_valid_data_before_corruption` test:
    // WAL-level garbage surfaces as `StrataError::Storage`.
    use crate::LossyErrorKind;
    assert_eq!(report.error_kind, LossyErrorKind::Storage);
}

#[test]
fn test_lossy_error_kind_mapping_covers_relevant_variants() {
    // Direct unit test of `LossyErrorKind::from_strata_error` — the
    // integration tests above only exercise the Corruption path because
    // real lossy failures come from WAL corruption. This test locks in
    // the mapping for Storage, CodecDecode, LegacyFormat, and Other so a
    // future refactor that regresses `from_strata_error` fails without
    // needing a new failure fixture.
    use crate::LossyErrorKind;
    let corruption = StrataError::corruption("bad CRC".to_string());
    assert_eq!(
        LossyErrorKind::from_strata_error(&corruption),
        LossyErrorKind::Corruption
    );
    let storage = StrataError::storage("disk full".to_string());
    assert_eq!(
        LossyErrorKind::from_strata_error(&storage),
        LossyErrorKind::Storage
    );
    // T3-E12 Phase 1: codec-decode failures classify as their own
    // category (key-rotation / key-recovery dispatch) rather than
    // `Storage`.
    let codec_decode = StrataError::codec_decode("AES-GCM auth tag mismatch");
    assert_eq!(
        LossyErrorKind::from_strata_error(&codec_decode),
        LossyErrorKind::CodecDecode
    );
    // T3-E12 §D6: `LegacyFormat` is a hard-fail error that never
    // reaches the lossy-report slot at runtime. The mapping falls
    // through to `Other` as a safe default in case the open.rs guard
    // is ever misordered — the intent is that this arm is unreachable
    // in normal operation, not that LegacyFormat is a lossy-recoverable
    // class.
    let legacy = StrataError::legacy_format(
        2,
        "this build requires version 3. Delete the `wal/` subdirectory and reopen.",
    );
    assert_eq!(
        LossyErrorKind::from_strata_error(&legacy),
        LossyErrorKind::Other
    );
    // Anything that isn't one of the above falls through to Other.
    let internal = StrataError::internal("unexpected".to_string());
    assert_eq!(
        LossyErrorKind::from_strata_error(&internal),
        LossyErrorKind::Other
    );
}

#[test]
fn test_lossy_error_kind_display_covers_all_variants() {
    // The `Display` impl feeds the `error_kind` field of the
    // `strata::recovery::lossy` tracing event. A missing arm would
    // compile-fail in-crate (the match is exhaustive), but a wrong
    // string would silently regress tracing output. Pin all four
    // rendered strings so a future rename trips CI.
    use crate::LossyErrorKind;
    assert_eq!(format!("{}", LossyErrorKind::Corruption), "corruption");
    assert_eq!(format!("{}", LossyErrorKind::Storage), "storage");
    assert_eq!(format!("{}", LossyErrorKind::CodecDecode), "codec_decode");
    assert_eq!(format!("{}", LossyErrorKind::Other), "other");
}

/// T3-E12 §D6: a pre-v3 WAL segment on disk triggers
/// `StrataError::LegacyFormat` and MUST NOT route through the lossy
/// wipe, even when `allow_lossy_recovery=true`.
///
/// Rationale: the lossy branch only recreates the in-memory
/// `SegmentedStore` and leaves `wal/` bytes on disk untouched. Without
/// the hard-fail guard at `open.rs:1000` / `:540`, a pre-v3 segment
/// would re-poison every subsequent open in an infinite loop.
/// Operator remediation is manual — delete `wal/` and reopen.
#[test]
fn test_legacy_format_under_lossy_flag_still_hard_fails() {
    use crc32fast::Hasher;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let wal_dir = db_path.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    // Craft a 36-byte v2 segment header directly — matches the pre-
    // T3-E12 on-disk layout (magic + version=2 + segment_number +
    // database_uuid + header_crc over the first 32 bytes). The v3
    // reader rejects this with `SegmentHeaderError::LegacyFormat`
    // before reading any records.
    let mut header = [0u8; 36];
    header[0..4].copy_from_slice(b"STRA"); // SEGMENT_MAGIC
    header[4..8].copy_from_slice(&2u32.to_le_bytes()); // format_version = 2 (legacy)
    header[8..16].copy_from_slice(&1u64.to_le_bytes()); // segment_number = 1
    header[16..32].copy_from_slice(&[0xAA; 16]); // database_uuid
    let header_crc = {
        let mut h = Hasher::new();
        h.update(&header[0..32]);
        h.finalize()
    };
    header[32..36].copy_from_slice(&header_crc.to_le_bytes());

    let segment_path = wal_dir.join("wal-000001.seg");
    std::fs::write(&segment_path, header).unwrap();

    // Snapshot the on-disk bytes BEFORE the open attempt so we can
    // prove no wipe occurred.
    let bytes_before = std::fs::read(&segment_path).unwrap();

    // Open with allow_lossy_recovery=true. The D6 hard-fail guard
    // must re-raise the `LegacyFormat` error WITHOUT wiping.
    let cfg = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let result = Database::open_with_config(&db_path, cfg);

    match result {
        Err(StrataError::LegacyFormat {
            found_version,
            hint,
        }) => {
            assert_eq!(found_version, 2);
            assert!(
                hint.contains("requires segment format version"),
                "hint must name required version, got: {hint}"
            );
            assert!(
                hint.contains("wal/"),
                "hint must name the wal/ directory for remediation, got: {hint}"
            );
        }
        Ok(_) => panic!(
            "legacy-format open must hard-fail even under allow_lossy_recovery=true; got Ok(Database)",
        ),
        Err(other) => panic!(
            "legacy-format open must produce StrataError::LegacyFormat, got: {other:?}",
        ),
    }

    // Filesystem observable: the pre-v3 segment bytes on disk must be
    // unchanged after the failed open. A wipe would have replaced or
    // cleared the segment. Re-reading identical bytes proves no
    // `storage = SegmentedStore::with_dir(...)` branch executed and
    // no lossy report was populated on the (failed) open attempt.
    let bytes_after = std::fs::read(&segment_path).unwrap();
    assert_eq!(
        bytes_before, bytes_after,
        "legacy-format open must NOT mutate the pre-v3 segment; the \
         wipe branch should be skipped entirely",
    );

    // Reproducible: a second open attempt returns the same typed error
    // — the failure is deterministic, not a transient recovery artifact.
    let cfg2 = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let result2 = Database::open_with_config(&db_path, cfg2);
    assert!(
        matches!(result2, Err(StrataError::LegacyFormat { .. })),
        "second open must reproduce the same typed LegacyFormat error",
    );
}

/// D5: a pre-v2 MANIFEST file surfaces `StrataError::LegacyFormat` even
/// under `allow_lossy_recovery=true`. Parity with the WAL legacy-format
/// hard-fail contract — otherwise the lossy branch would swallow a
/// legacy MANIFEST and silently reconstruct invalid metadata.
#[test]
fn test_legacy_manifest_under_lossy_flag_still_hard_fails() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    std::fs::create_dir_all(&db_path).unwrap();

    // Craft a v1 MANIFEST: magic + version=1 + uuid + codec + wal_seg +
    // watermark + snap_id + crc. No `flushed_through_commit_id` field
    // — that was added in v2. The D5 cutover rejects this at the parser.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"STRM"); // MANIFEST_MAGIC
    bytes.extend_from_slice(&1u32.to_le_bytes()); // format_version = 1 (legacy)
    bytes.extend_from_slice(&[0xAAu8; 16]); // database_uuid
    let codec = b"identity";
    bytes.extend_from_slice(&(codec.len() as u32).to_le_bytes());
    bytes.extend_from_slice(codec);
    bytes.extend_from_slice(&1u64.to_le_bytes()); // active_wal_segment
    bytes.extend_from_slice(&0u64.to_le_bytes()); // snapshot_watermark
    bytes.extend_from_slice(&0u64.to_le_bytes()); // snapshot_id
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());

    let manifest_path = db_path.join("MANIFEST");
    std::fs::write(&manifest_path, &bytes).unwrap();

    let cfg = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let result = Database::open_with_config(&db_path, cfg);

    match result {
        Err(StrataError::LegacyFormat {
            found_version,
            hint,
        }) => {
            assert_eq!(found_version, 1);
            assert!(
                hint.contains("MANIFEST"),
                "hint must name the MANIFEST surface, got: {hint}"
            );
            assert!(
                hint.contains("requires"),
                "hint must describe the required floor, got: {hint}"
            );
        }
        Ok(_) => panic!(
            "legacy MANIFEST open must hard-fail even under allow_lossy_recovery=true; \
             got Ok(Database)",
        ),
        Err(other) => {
            panic!("legacy MANIFEST open must produce StrataError::LegacyFormat, got: {other:?}",)
        }
    }

    // Reproducible: a second open attempt returns the same typed error.
    let cfg2 = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let result2 = Database::open_with_config(&db_path, cfg2);
    assert!(
        matches!(result2, Err(StrataError::LegacyFormat { .. })),
        "second open must reproduce the same typed LegacyFormat error",
    );
}

/// Follower MANIFEST failures must preserve the on-disk path in the surfaced
/// diagnostic so operators can identify which database root is broken.
#[test]
fn test_open_follower_corrupt_manifest_names_manifest_path() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    std::fs::create_dir_all(&db_path).unwrap();

    let manifest_path = db_path.join("MANIFEST");
    std::fs::write(&manifest_path, b"not-a-manifest").unwrap();

    let err = match Database::open_follower(&db_path) {
        Err(err) => err,
        Ok(_) => panic!("corrupt MANIFEST must fail follower"),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("follower could not load MANIFEST"),
        "error should name the follower MANIFEST load, got: {}",
        msg
    );
    assert!(
        msg.contains(&manifest_path.display().to_string()),
        "error should include the MANIFEST path, got: {}",
        msg
    );
}

/// D5: a pre-v2 snapshot file surfaces `StrataError::LegacyFormat` even under
/// `allow_lossy_recovery=true`. Parity with WAL and MANIFEST: the lossy branch
/// must not wipe around a legacy snapshot artifact it cannot heal.
#[test]
fn test_legacy_snapshot_under_lossy_flag_still_hard_fails() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let snapshots_dir = db_path.join("snapshots");
    std::fs::create_dir_all(&snapshots_dir).unwrap();

    let mut mgr = strata_storage::durability::ManifestManager::create(
        db_path.join("MANIFEST"),
        [0xAAu8; 16],
        "identity".to_string(),
    )
    .unwrap();
    mgr.set_snapshot_watermark(1, TxnId(100)).unwrap();

    let snapshot_path = strata_storage::durability::format::snapshot_path(&snapshots_dir, 1);
    let mut header = [0u8; strata_storage::durability::SNAPSHOT_HEADER_SIZE];
    header[0..4].copy_from_slice(&strata_storage::durability::SNAPSHOT_MAGIC);
    header[4..8].copy_from_slice(&1u32.to_le_bytes()); // format_version = 1 (legacy)
    header[8..16].copy_from_slice(&1u64.to_le_bytes()); // snapshot_id
    header[16..24].copy_from_slice(&100u64.to_le_bytes()); // watermark_txn
    header[24..32].copy_from_slice(&0u64.to_le_bytes()); // created_at
    header[32..48].copy_from_slice(&[0xAAu8; 16]); // database_uuid
    header[48] = 0; // codec_id_len = 0

    // Pad beyond the reader's minimum-size guard. LegacyFormat short-circuits
    // before codec/body parsing, so the filler bytes are arbitrary.
    let mut bytes = header.to_vec();
    bytes.extend_from_slice(&[0u8; 8]);
    std::fs::write(&snapshot_path, &bytes).unwrap();
    let bytes_before = std::fs::read(&snapshot_path).unwrap();

    let cfg = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let result = Database::open_with_config(&db_path, cfg);

    match result {
        Err(StrataError::LegacyFormat {
            found_version,
            hint,
        }) => {
            assert_eq!(found_version, 1);
            assert!(
                hint.contains("snapshot format version"),
                "hint must name the required snapshot floor, got: {hint}"
            );
            assert!(
                hint.contains("snap-"),
                "hint must name the snapshot file shape, got: {hint}"
            );
        }
        Ok(_) => panic!(
            "legacy snapshot open must hard-fail even under allow_lossy_recovery=true; \
             got Ok(Database)",
        ),
        Err(other) => {
            panic!("legacy snapshot open must produce StrataError::LegacyFormat, got: {other:?}",)
        }
    }

    let bytes_after = std::fs::read(&snapshot_path).unwrap();
    assert_eq!(
        bytes_before, bytes_after,
        "legacy snapshot open must NOT mutate the pre-v2 snapshot file",
    );

    let cfg2 = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let result2 = Database::open_with_config(&db_path, cfg2);
    assert!(
        matches!(result2, Err(StrataError::LegacyFormat { .. })),
        "second open must reproduce the same typed LegacyFormat error",
    );
}

#[test]
fn test_run_recovery_reports_snapshot_missing_typed() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let snapshot_path = seed_snapshot_fixture(&db_path, 7, 100);
    std::fs::remove_file(&snapshot_path).unwrap();

    let err = match run_primary_recovery(&db_path, StrataConfig::default()) {
        Ok(_) => panic!("missing snapshot must fail recovery"),
        Err(err) => err,
    };

    match err {
        crate::database::RecoveryError::SnapshotMissing {
            role: crate::database::ErrorRole::Primary,
            snapshot_id,
            path,
        } => {
            assert_eq!(snapshot_id, 7);
            assert_eq!(path, snapshot_path);
        }
        other => panic!("expected typed SnapshotMissing, got: {other:?}"),
    }
}

#[test]
fn test_run_recovery_reports_snapshot_crc_typed() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    let snapshot_path = seed_snapshot_fixture(&db_path, 9, 101);

    let mut bytes = std::fs::read(&snapshot_path).unwrap();
    let corrupt_idx = bytes.len() - 7;
    bytes[corrupt_idx] ^= 0xFF;
    std::fs::write(&snapshot_path, &bytes).unwrap();

    let err = match run_primary_recovery(&db_path, StrataConfig::default()) {
        Ok(_) => panic!("corrupt snapshot must fail recovery"),
        Err(err) => err,
    };

    match err {
        crate::database::RecoveryError::SnapshotRead {
            role: crate::database::ErrorRole::Primary,
            snapshot_id,
            path,
            inner: strata_storage::durability::SnapshotReadError::CrcMismatch { .. },
        } => {
            assert_eq!(snapshot_id, 9);
            assert_eq!(path, snapshot_path);
        }
        other => panic!("expected typed SnapshotRead::CrcMismatch, got: {other:?}"),
    }
}

#[test]
fn test_run_recovery_reports_wal_checksum_typed() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    seed_outer_len_crc_mismatch(&db_path);

    let err = match run_primary_recovery(&db_path, StrataConfig::default()) {
        Ok(_) => panic!("outer envelope CRC mismatch must fail recovery"),
        Err(err) => err,
    };

    match err {
        crate::database::RecoveryError::WalChecksum {
            role: crate::database::ErrorRole::Primary,
            records_before,
            ..
        } => {
            assert_eq!(records_before, 0);
        }
        other => panic!("expected typed WalChecksum, got: {other:?}"),
    }
}

#[test]
fn test_run_recovery_reports_payload_decode_typed() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("db");
    write_invalid_payload_wal(&db_path);

    let err = match run_primary_recovery(&db_path, StrataConfig::default()) {
        Ok(_) => panic!("invalid transaction payload must fail recovery"),
        Err(err) => err,
    };

    match err {
        crate::database::RecoveryError::PayloadDecode {
            role: crate::database::ErrorRole::Primary,
            txn_id,
            ..
        } => {
            assert_eq!(txn_id, TxnId(1));
        }
        other => panic!("expected typed PayloadDecode, got: {other:?}"),
    }
}
