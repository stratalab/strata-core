//! Integration tests for multi-process WAL coordination (Phases 3–5).
//!
//! These tests verify:
//! - Two Database instances can open the same directory concurrently
//! - WAL refresh brings one instance up-to-date with another's writes
//! - Coordinated commit detects conflicts across instances
//! - Crash recovery preserves both instances' committed data

use std::sync::{Arc, Once};
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_engine::database::config::StrataConfig;
use strata_engine::search::InvertedIndex;
use strata_engine::{register_search_recovery, Database};
use tempfile::TempDir;

static INIT_RECOVERY: Once = Once::new();

fn ensure_recovery_registered() {
    INIT_RECOVERY.call_once(|| {
        register_search_recovery();
    });
}

fn create_test_namespace(branch_id: BranchId) -> Arc<Namespace> {
    Arc::new(Namespace::new(
        "tenant".to_string(),
        "app".to_string(),
        "agent".to_string(),
        branch_id,
        "default".to_string(),
    ))
}

/// Read a key value through a read-only transaction.
fn read_key(db: &Database, branch_id: BranchId, key: &Key) -> Option<Value> {
    db.transaction(branch_id, |txn| Ok(txn.get(key)?)).unwrap()
}

// ============================================================================
// Phase 3: Multi-Instance Database Open
// ============================================================================

#[test]
fn test_multi_process_two_instances_same_path() {
    let dir = TempDir::new().unwrap();
    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();

    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // Distinct instances (not the same Arc)
    assert!(!Arc::ptr_eq(&db1, &db2));
    assert!(db1.is_open());
    assert!(db2.is_open());
}

#[test]
fn test_multi_process_both_read_after_recovery() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "shared_key");

    // Write data with instance 1
    {
        let cfg = StrataConfig::default();
        let mode = cfg.durability_mode().unwrap();
        let db = Database::open_multi_process(dir.path(), mode, cfg).unwrap();
        db.transaction(branch_id, |txn| {
            txn.put(key.clone(), Value::Int(42))?;
            Ok(())
        })
        .unwrap();
        db.flush().unwrap();
    }

    // Both new instances should see the data after recovery
    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    let val1 = read_key(&db1, branch_id, &key);
    assert_eq!(val1, Some(Value::Int(42)));

    let val2 = read_key(&db2, branch_id, &key);
    assert_eq!(val2, Some(Value::Int(42)));
}

#[test]
fn test_default_mode_still_exclusive_lock() {
    let dir = TempDir::new().unwrap();

    let db1 = Database::open(dir.path()).unwrap();
    let db2 = Database::open(dir.path()).unwrap();

    // Same Arc from registry
    assert!(Arc::ptr_eq(&db1, &db2));
}

// ============================================================================
// Phase 4: WAL Refresh
// ============================================================================

#[test]
fn test_refresh_sees_other_instance_writes() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "from_db1");

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // Write via db1
    db1.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(100))?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    // After refresh, db2 should see it
    let applied = db2.refresh().unwrap();
    assert!(applied > 0, "Should have applied new records");

    let val = read_key(&db2, branch_id, &key);
    assert_eq!(val, Some(Value::Int(100)));
}

#[test]
fn test_refresh_updates_version_counter() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    let v_before = db2.current_version();

    db1.transaction(branch_id, |txn| {
        txn.put(Key::new_kv(ns.clone(), "key"), Value::Int(1))?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    db2.refresh().unwrap();

    let v_after = db2.current_version();
    assert!(
        v_after > v_before,
        "Version should advance after refresh: {} -> {}",
        v_before,
        v_after
    );
}

#[test]
fn test_refresh_idempotent_no_new_records() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    db1.transaction(branch_id, |txn| {
        txn.put(Key::new_kv(ns.clone(), "key"), Value::Int(1))?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    let applied1 = db2.refresh().unwrap();
    assert!(applied1 > 0);

    let applied2 = db2.refresh().unwrap();
    assert_eq!(applied2, 0, "Second refresh should find no new records");
}

#[test]
fn test_refresh_multiple_records() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    for i in 1..=5i64 {
        db1.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), format!("key{}", i)), Value::Int(i))?;
            Ok(())
        })
        .unwrap();
    }
    db1.flush().unwrap();

    let applied = db2.refresh().unwrap();
    assert_eq!(applied, 5, "Should apply all 5 records");

    for i in 1..=5i64 {
        let key = Key::new_kv(ns.clone(), format!("key{}", i));
        let val = read_key(&db2, branch_id, &key);
        assert_eq!(val, Some(Value::Int(i)));
    }
}

// ============================================================================
// Phase 5: Coordinated Commit
// ============================================================================

#[test]
fn test_coordinated_commit_basic() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "key");

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    db.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(42))?;
        Ok(())
    })
    .unwrap();

    let val = read_key(&db, branch_id, &key);
    assert_eq!(val, Some(Value::Int(42)));
}

#[test]
fn test_coordinated_commit_conflict_detected() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "conflict_key");

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // Write initial value via db1
    db1.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(1))?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    // db2 refreshes to see the initial value
    db2.refresh().unwrap();

    // db1 updates the key (read-modify-write)
    db1.transaction(branch_id, |txn| {
        let _ = txn.get(&key)?;
        txn.put(key.clone(), Value::Int(2))?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    // db2 tries read-modify-write on same key.
    // The coordinated commit refreshes WAL and re-validates,
    // detecting that the key was modified since db2's snapshot.
    let result = db2.transaction(branch_id, |txn| {
        let _ = txn.get(&key)?;
        txn.put(key.clone(), Value::Int(3))?;
        Ok(())
    });

    assert!(
        result.is_err(),
        "Should detect conflict: db1 updated key between db2's read and commit"
    );
}

#[test]
fn test_coordinated_commit_blind_writes_no_conflict() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // Both write different keys blindly (no reads) — no conflict
    db1.transaction(branch_id, |txn| {
        txn.put(Key::new_kv(ns.clone(), "from_db1"), Value::Int(1))?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    db2.transaction(branch_id, |txn| {
        txn.put(Key::new_kv(ns.clone(), "from_db2"), Value::Int(2))?;
        Ok(())
    })
    .unwrap();
    db2.flush().unwrap();

    // Both keys should be present after refresh
    db1.refresh().unwrap();
    db2.refresh().unwrap();

    let k1 = Key::new_kv(ns.clone(), "from_db1");
    let k2 = Key::new_kv(ns.clone(), "from_db2");

    assert_eq!(read_key(&db1, branch_id, &k1), Some(Value::Int(1)));
    assert_eq!(read_key(&db1, branch_id, &k2), Some(Value::Int(2)));
    assert_eq!(read_key(&db2, branch_id, &k1), Some(Value::Int(1)));
    assert_eq!(read_key(&db2, branch_id, &k2), Some(Value::Int(2)));
}

#[test]
fn test_coordinated_commit_versions_monotonic_across_instances() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // Alternate commits between instances and track versions
    let v0 = db1.current_version();

    db1.transaction(branch_id, |txn| {
        txn.put(Key::new_kv(ns.clone(), "a"), Value::Int(1))?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();
    let v1 = db1.current_version();

    db2.transaction(branch_id, |txn| {
        txn.put(Key::new_kv(ns.clone(), "b"), Value::Int(2))?;
        Ok(())
    })
    .unwrap();
    db2.flush().unwrap();
    let v2 = db2.current_version();

    db1.transaction(branch_id, |txn| {
        txn.put(Key::new_kv(ns.clone(), "c"), Value::Int(3))?;
        Ok(())
    })
    .unwrap();
    let v3 = db1.current_version();

    assert!(v0 < v1, "v0={} should be < v1={}", v0, v1);
    assert!(v1 < v2, "v1={} should be < v2={}", v1, v2);
    assert!(v2 < v3, "v2={} should be < v3={}", v2, v3);
}

#[test]
fn test_two_instances_interleaved_commits() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    for i in 0..5i64 {
        let db = if i % 2 == 0 { &db1 } else { &db2 };
        db.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), format!("key{}", i)), Value::Int(i))?;
            Ok(())
        })
        .unwrap();
        db.flush().unwrap();
    }

    db1.refresh().unwrap();
    db2.refresh().unwrap();

    for i in 0..5i64 {
        let key = Key::new_kv(ns.clone(), format!("key{}", i));
        assert_eq!(
            read_key(&db1, branch_id, &key),
            Some(Value::Int(i)),
            "db1 should see key{}",
            i
        );
        assert_eq!(
            read_key(&db2, branch_id, &key),
            Some(Value::Int(i)),
            "db2 should see key{}",
            i
        );
    }
}

#[test]
fn test_coordinated_commit_crash_recovery_preserves_both() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    {
        let cfg = StrataConfig::default();
        let mode = cfg.durability_mode().unwrap();
        let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
        let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

        db1.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), "from_db1"), Value::Int(100))?;
            Ok(())
        })
        .unwrap();
        db1.flush().unwrap();

        db2.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), "from_db2"), Value::Int(200))?;
            Ok(())
        })
        .unwrap();
        db2.flush().unwrap();
    }

    // Reopen — all data should be recovered
    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    let k1 = Key::new_kv(ns.clone(), "from_db1");
    let k2 = Key::new_kv(ns.clone(), "from_db2");

    assert_eq!(read_key(&db, branch_id, &k1), Some(Value::Int(100)));
    assert_eq!(read_key(&db, branch_id, &k2), Some(Value::Int(200)));
}

#[test]
fn test_coordinated_commit_different_branches_both_succeed() {
    let dir = TempDir::new().unwrap();
    let branch_a = BranchId::new();
    let branch_b = BranchId::new();
    let ns_a = create_test_namespace(branch_a);
    let ns_b = create_test_namespace(branch_b);

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    db1.transaction(branch_a, |txn| {
        txn.put(Key::new_kv(ns_a.clone(), "key"), Value::Int(1))?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    db2.transaction(branch_b, |txn| {
        txn.put(Key::new_kv(ns_b.clone(), "key"), Value::Int(2))?;
        Ok(())
    })
    .unwrap();
    db2.flush().unwrap();

    db1.refresh().unwrap();

    let ka = Key::new_kv(ns_a, "key");
    let kb = Key::new_kv(ns_b, "key");

    assert_eq!(read_key(&db1, branch_a, &ka), Some(Value::Int(1)));
    assert_eq!(read_key(&db1, branch_b, &kb), Some(Value::Int(2)));
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Verifies that opening a database in multi-process mode after prior single-process
/// usage correctly initializes the counter file from recovery, preventing version
/// collisions.
#[test]
fn test_single_to_multi_process_conversion() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key_old = Key::new_kv(ns.clone(), "old_data");
    let key_new = Key::new_kv(ns.clone(), "new_data");

    // Phase 1: Write data in single-process mode
    let version_after_single;
    {
        let db = Database::open(dir.path()).unwrap();
        for i in 1..=10i64 {
            db.transaction(branch_id, |txn| {
                txn.put(Key::new_kv(ns.clone(), format!("key{}", i)), Value::Int(i))?;
                Ok(())
            })
            .unwrap();
        }
        db.transaction(branch_id, |txn| {
            txn.put(key_old.clone(), Value::Int(999))?;
            Ok(())
        })
        .unwrap();
        version_after_single = db.current_version();
        db.flush().unwrap();
    }

    // Phase 2: Reopen in multi-process mode and write new data
    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // Old data must be recovered
    assert_eq!(read_key(&db, branch_id, &key_old), Some(Value::Int(999)));

    // New commit must get a version HIGHER than any single-process version
    db.transaction(branch_id, |txn| {
        txn.put(key_new.clone(), Value::Int(123))?;
        Ok(())
    })
    .unwrap();

    let version_after_multi = db.current_version();
    assert!(
        version_after_multi > version_after_single,
        "Multi-process version {} must exceed single-process version {}",
        version_after_multi,
        version_after_single
    );

    // Both old and new data must be readable
    assert_eq!(read_key(&db, branch_id, &key_old), Some(Value::Int(999)));
    assert_eq!(read_key(&db, branch_id, &key_new), Some(Value::Int(123)));
}

/// Simulates the counter file being stale (e.g., crash between WAL write and
/// counter update) and verifies that the next commit still allocates unique
/// version/txn_id values by cross-referencing with local coordinator state.
#[test]
fn test_stale_counter_file_recovery() {
    use strata_durability::coordination::CounterFile;

    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Open and write several transactions to establish versions
    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    {
        let db = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();

        for i in 1..=5i64 {
            db.transaction(branch_id, |txn| {
                txn.put(Key::new_kv(ns.clone(), format!("key{}", i)), Value::Int(i))?;
                Ok(())
            })
            .unwrap();
        }
        db.flush().unwrap();
    }

    // Corrupt the counter file to simulate a crash gap:
    // Reset it to (0, 0), as if the last counter update never happened.
    let wal_dir = dir.path().join("wal");
    let counter_file = CounterFile::new(&wal_dir);
    counter_file.write(0, 0).unwrap();

    // Reopen — recovery should fix the counter file
    let db = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // The counter file should have been reseeded from recovery
    let (cf_ver, cf_txn) = counter_file.read().unwrap();
    assert!(
        cf_ver >= 5,
        "Counter file version should be >= 5, got {}",
        cf_ver
    );
    assert!(
        cf_txn >= 5,
        "Counter file txn_id should be >= 5, got {}",
        cf_txn
    );

    // A new commit should get a version higher than anything before
    let key_new = Key::new_kv(ns.clone(), "after_recovery");
    db.transaction(branch_id, |txn| {
        txn.put(key_new.clone(), Value::Int(42))?;
        Ok(())
    })
    .unwrap();

    let new_version = db.current_version();
    assert!(
        new_version > 5,
        "New version {} must be > 5 (pre-crash version)",
        new_version
    );

    // All old data must still be readable
    for i in 1..=5i64 {
        let key = Key::new_kv(ns.clone(), format!("key{}", i));
        assert_eq!(read_key(&db, branch_id, &key), Some(Value::Int(i)));
    }
    assert_eq!(read_key(&db, branch_id, &key_new), Some(Value::Int(42)));
}

/// Verifies that deletes propagate correctly through WAL refresh.
#[test]
fn test_refresh_propagates_deletes() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "to_delete");

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // db1 writes a key
    db1.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(42))?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    // db2 refreshes and sees it
    db2.refresh().unwrap();
    assert_eq!(read_key(&db2, branch_id, &key), Some(Value::Int(42)));

    // db1 deletes the key
    db1.transaction(branch_id, |txn| {
        txn.delete(key.clone())?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    // db2 refreshes and should see the deletion
    db2.refresh().unwrap();
    assert_eq!(
        read_key(&db2, branch_id, &key),
        None,
        "Deleted key should be None after refresh"
    );
}

/// Both instances blindly overwrite the same key. Both commits should succeed
/// (no read set → no conflict), and the final value should be the later writer's.
#[test]
fn test_same_key_blind_overwrite_both_succeed() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "shared");

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // db1 blindly writes
    db1.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(100))?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    // db2 blindly writes the same key (no read → no conflict)
    db2.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(200))?;
        Ok(())
    })
    .unwrap();
    db2.flush().unwrap();

    // After both refresh, the later write (db2's 200) should win
    // because it has a higher version number
    db1.refresh().unwrap();
    db2.refresh().unwrap();

    assert_eq!(read_key(&db1, branch_id, &key), Some(Value::Int(200)));
    assert_eq!(read_key(&db2, branch_id, &key), Some(Value::Int(200)));
}

/// Read-only transactions in multi-process mode should not acquire
/// the WAL file lock and should complete without coordination overhead.
#[test]
fn test_read_only_transaction_skips_coordination() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "data");

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // Write some data
    db1.transaction(branch_id, |txn| {
        txn.put(key.clone(), Value::Int(42))?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    db2.refresh().unwrap();

    // A read-only transaction on db2 should work even without coordination
    let val: Option<Value> = db2
        .transaction(branch_id, |txn| Ok(txn.get(&key)?))
        .unwrap();
    assert_eq!(val, Some(Value::Int(42)));

    // A purely read-only transaction should also work on db1
    let val: Option<Value> = db1
        .transaction(branch_id, |txn| Ok(txn.get(&key)?))
        .unwrap();
    assert_eq!(val, Some(Value::Int(42)));
}

/// After crash recovery, both instances' data should be present and the
/// counter file should be correctly seeded so new commits get unique versions.
#[test]
fn test_crash_recovery_counter_file_consistency() {
    use strata_durability::coordination::CounterFile;

    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    // Two instances write data and close (simulating crash)
    {
        let cfg = StrataConfig::default();
        let mode = cfg.durability_mode().unwrap();
        let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
        let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

        db1.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), "from_1"), Value::Int(1))?;
            Ok(())
        })
        .unwrap();
        db1.flush().unwrap();

        db2.transaction(branch_id, |txn| {
            txn.put(Key::new_kv(ns.clone(), "from_2"), Value::Int(2))?;
            Ok(())
        })
        .unwrap();
        db2.flush().unwrap();
    }

    // Check counter file state before reopen
    let wal_dir = dir.path().join("wal");
    let cf = CounterFile::new(&wal_dir);
    let (ver_before, txn_before) = cf.read().unwrap();

    // Reopen single instance — counter file should be consistent
    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    let (ver_after, txn_after) = cf.read().unwrap();
    assert!(
        ver_after >= ver_before,
        "Counter version should not go backward: {} -> {}",
        ver_before,
        ver_after
    );
    assert!(
        txn_after >= txn_before,
        "Counter txn_id should not go backward: {} -> {}",
        txn_before,
        txn_after
    );

    // New commit should get version higher than anything before
    db.transaction(branch_id, |txn| {
        txn.put(Key::new_kv(ns.clone(), "after_reopen"), Value::Int(3))?;
        Ok(())
    })
    .unwrap();

    let new_version = db.current_version();
    assert!(
        new_version > ver_before,
        "New version must exceed pre-crash version"
    );

    // All data readable
    assert_eq!(
        read_key(&db, branch_id, &Key::new_kv(ns.clone(), "from_1")),
        Some(Value::Int(1))
    );
    assert_eq!(
        read_key(&db, branch_id, &Key::new_kv(ns.clone(), "from_2")),
        Some(Value::Int(2))
    );
    assert_eq!(
        read_key(&db, branch_id, &Key::new_kv(ns.clone(), "after_reopen")),
        Some(Value::Int(3))
    );
}

// ============================================================================
// Incremental Index Refresh Tests
// ============================================================================

/// After refresh, the BM25 search index should contain documents from the other instance.
#[test]
fn test_refresh_updates_search_index() {
    ensure_recovery_registered();

    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // db1 writes KV entries with string values
    db1.transaction(branch_id, |txn| {
        txn.put(
            Key::new_kv(ns.clone(), "doc1"),
            Value::String("the quick brown fox".to_string()),
        )?;
        txn.put(
            Key::new_kv(ns.clone(), "doc2"),
            Value::String("jumped over the lazy dog".to_string()),
        )?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    // db2 refreshes — should update its local search index
    db2.refresh().unwrap();

    // Verify search index on db2 contains the documents
    let index = db2.extension::<InvertedIndex>().unwrap();
    assert!(index.is_enabled(), "Search index should be enabled");

    let terms = strata_engine::search::tokenize("fox");
    let results = index.score_top_k(&terms, &branch_id, 10, 1.2, 0.75);
    assert!(
        !results.is_empty(),
        "Search should find 'fox' after refresh"
    );

    let terms = strata_engine::search::tokenize("lazy");
    let results = index.score_top_k(&terms, &branch_id, 10, 1.2, 0.75);
    assert!(
        !results.is_empty(),
        "Search should find 'lazy' after refresh"
    );
}

/// After a delete is refreshed, the search index should no longer contain the deleted document.
#[test]
fn test_refresh_updates_search_index_on_delete() {
    ensure_recovery_registered();

    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);
    let key = Key::new_kv(ns.clone(), "to_remove");

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // db1 writes a searchable entry
    db1.transaction(branch_id, |txn| {
        txn.put(
            key.clone(),
            Value::String("unique_searchterm_xyzzy".to_string()),
        )?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    // db2 refreshes and verifies it's searchable
    db2.refresh().unwrap();
    let index = db2.extension::<InvertedIndex>().unwrap();
    let terms = strata_engine::search::tokenize("unique_searchterm_xyzzy");
    let results = index.score_top_k(&terms, &branch_id, 10, 1.2, 0.75);
    assert!(
        !results.is_empty(),
        "Should find the document after first refresh"
    );

    // db1 deletes the entry
    db1.transaction(branch_id, |txn| {
        txn.delete(key.clone())?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    // db2 refreshes again — document should be removed from search
    db2.refresh().unwrap();
    let results = index.score_top_k(&terms, &branch_id, 10, 1.2, 0.75);
    assert!(
        results.is_empty(),
        "Deleted document should not appear in search after refresh"
    );
}

/// Refresh with a disabled search index should not error.
#[test]
fn test_refresh_search_index_disabled_no_overhead() {
    let dir = TempDir::new().unwrap();
    let branch_id = BranchId::new();
    let ns = create_test_namespace(branch_id);

    let cfg = StrataConfig::default();
    let mode = cfg.durability_mode().unwrap();
    let db1 = Database::open_multi_process(dir.path(), mode, cfg.clone()).unwrap();
    let db2 = Database::open_multi_process(dir.path(), mode, cfg).unwrap();

    // Disable search index on db2
    let index = db2.extension::<InvertedIndex>().unwrap();
    index.disable();

    // db1 writes data
    db1.transaction(branch_id, |txn| {
        txn.put(
            Key::new_kv(ns.clone(), "key"),
            Value::String("hello world".to_string()),
        )?;
        Ok(())
    })
    .unwrap();
    db1.flush().unwrap();

    // Refresh should succeed without errors even with disabled index
    let applied = db2.refresh().unwrap();
    assert!(applied > 0);

    // Data should still be applied to storage
    let val = read_key(&db2, branch_id, &Key::new_kv(ns.clone(), "key"));
    assert_eq!(val, Some(Value::String("hello world".to_string())));
}
