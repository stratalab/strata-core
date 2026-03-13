//! Integration tests for read-only follower mode.
//!
//! Verifies that a follower can open a database that is exclusively locked by
//! a primary, replay the WAL, read data, refresh to see new data, and that
//! writes are rejected.

use std::sync::Arc;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_engine::Database;
use tempfile::tempdir;

fn ns(branch: BranchId) -> Arc<Namespace> {
    Arc::new(Namespace::new(branch, "default".to_string()))
}

/// Helper: write a key-value pair through a transaction on the primary.
fn primary_put(db: &Database, branch: BranchId, key: &str, value: &str) {
    let k = Key::new_kv(ns(branch), key);
    db.transaction(branch, |txn| {
        txn.put(k.clone(), Value::String(value.to_string()))?;
        Ok(())
    })
    .expect("primary put should succeed");
}

/// Helper: delete a key through a transaction on the primary.
fn primary_del(db: &Database, branch: BranchId, key: &str) {
    let k = Key::new_kv(ns(branch), key);
    db.transaction(branch, |txn| {
        txn.delete(k.clone())?;
        Ok(())
    })
    .expect("primary delete should succeed");
}

/// Helper: read a key-value pair via a read-only transaction.
fn read_kv(db: &Database, branch: BranchId, key: &str) -> Option<String> {
    let k = Key::new_kv(ns(branch), key);
    let mut txn = db
        .begin_read_only_transaction(branch)
        .expect("begin_read_only_transaction");
    match txn.get(&k) {
        Ok(Some(Value::String(s))) => Some(s),
        Ok(Some(other)) => Some(format!("{:?}", other)),
        Ok(None) => None,
        Err(e) => panic!("read_kv failed unexpectedly: {}", e),
    }
}

// ============================================================================
// Core functionality
// ============================================================================

#[test]
fn test_follower_opens_without_lock() {
    let dir = tempdir().unwrap();

    // Primary holds exclusive lock
    let _primary = Database::open(dir.path()).unwrap();

    // Follower should open successfully without any lock
    let follower = Database::open_follower(dir.path());
    assert!(
        follower.is_ok(),
        "Follower should open while primary holds exclusive lock: {:?}",
        follower.err()
    );
    assert!(follower.unwrap().is_follower());
}

#[test]
fn test_follower_sees_primary_data() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    // Primary writes data and flushes
    {
        let primary = Database::open(dir.path()).unwrap();
        primary_put(&primary, branch, "greeting", "hello");
        primary_put(&primary, branch, "count", "42");
        primary.flush().unwrap();
    }
    // Primary dropped, lock released

    // Follower opens and should see ALL the data via WAL recovery
    let follower = Database::open_follower(dir.path()).unwrap();
    assert_eq!(
        read_kv(&follower, branch, "greeting").as_deref(),
        Some("hello")
    );
    assert_eq!(read_kv(&follower, branch, "count").as_deref(), Some("42"));
    // Non-existent key should return None
    assert_eq!(read_kv(&follower, branch, "missing"), None);
}

#[test]
fn test_follower_refresh_sees_new_data() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    // Primary writes initial data
    let primary = Database::open(dir.path()).unwrap();
    primary_put(&primary, branch, "k1", "v1");
    primary.flush().unwrap();

    // Open follower — sees initial data
    let follower = Database::open_follower(dir.path()).unwrap();
    assert_eq!(read_kv(&follower, branch, "k1").as_deref(), Some("v1"));

    // Primary writes more data
    primary_put(&primary, branch, "k2", "v2");
    primary.flush().unwrap();

    // Follower doesn't see new data yet
    assert_eq!(read_kv(&follower, branch, "k2"), None);

    // After refresh, follower sees the new data
    let applied = follower.refresh().unwrap();
    assert!(applied > 0, "refresh should apply new records");
    assert_eq!(read_kv(&follower, branch, "k2").as_deref(), Some("v2"));

    // Original data still intact after refresh
    assert_eq!(read_kv(&follower, branch, "k1").as_deref(), Some("v1"));

    // Second refresh with no new data returns 0
    let applied2 = follower.refresh().unwrap();
    assert_eq!(
        applied2, 0,
        "refresh with no new WAL records should return 0"
    );
}

#[test]
fn test_follower_refresh_sees_updates_to_existing_keys() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let primary = Database::open(dir.path()).unwrap();
    primary_put(&primary, branch, "key", "original");
    primary.flush().unwrap();

    let follower = Database::open_follower(dir.path()).unwrap();
    assert_eq!(
        read_kv(&follower, branch, "key").as_deref(),
        Some("original")
    );

    // Primary UPDATES the same key
    primary_put(&primary, branch, "key", "updated");
    primary.flush().unwrap();

    follower.refresh().unwrap();
    assert_eq!(
        read_kv(&follower, branch, "key").as_deref(),
        Some("updated"),
        "Follower should see updated value after refresh"
    );
}

#[test]
fn test_follower_refresh_sees_deletes() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let primary = Database::open(dir.path()).unwrap();
    primary_put(&primary, branch, "ephemeral", "here_now");
    primary.flush().unwrap();

    let follower = Database::open_follower(dir.path()).unwrap();
    assert_eq!(
        read_kv(&follower, branch, "ephemeral").as_deref(),
        Some("here_now")
    );

    // Primary DELETES the key
    primary_del(&primary, branch, "ephemeral");
    primary.flush().unwrap();

    follower.refresh().unwrap();
    assert_eq!(
        read_kv(&follower, branch, "ephemeral"),
        None,
        "Follower should see deletion after refresh"
    );
}

#[test]
fn test_follower_multiple_refreshes() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let primary = Database::open(dir.path()).unwrap();
    primary.flush().unwrap();

    let follower = Database::open_follower(dir.path()).unwrap();

    // Write and refresh in rounds
    for i in 0..5 {
        let key = format!("round_{}", i);
        let val = format!("value_{}", i);
        primary_put(&primary, branch, &key, &val);
        primary.flush().unwrap();

        let applied = follower.refresh().unwrap();
        assert!(applied > 0, "round {} should apply records", i);
        assert_eq!(
            read_kv(&follower, branch, &key).as_deref(),
            Some(val.as_str()),
            "round {} value mismatch",
            i
        );
    }

    // All 5 keys still present
    for i in 0..5 {
        let key = format!("round_{}", i);
        let val = format!("value_{}", i);
        assert_eq!(
            read_kv(&follower, branch, &key).as_deref(),
            Some(val.as_str())
        );
    }
}

// ============================================================================
// Write rejection
// ============================================================================

#[test]
fn test_follower_rejects_writes() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    // Create primary, write something, then drop it
    {
        let primary = Database::open(dir.path()).unwrap();
        primary_put(&primary, branch, "k", "v");
        primary.flush().unwrap();
    }

    // Open follower
    let follower = Database::open_follower(dir.path()).unwrap();

    // Attempt to write should fail at commit
    let k = Key::new_kv(ns(branch), "new_key");
    let result = follower.transaction(branch, |txn| {
        txn.put(k.clone(), Value::String("new_value".to_string()))?;
        Ok(())
    });
    assert!(
        result.is_err(),
        "Writes should be rejected in follower mode"
    );

    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("follower") || err_msg.contains("read-only"),
        "Error should mention follower or read-only mode, got: {}",
        err_msg
    );
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn test_follower_with_empty_wal() {
    let dir = tempdir().unwrap();

    // Create the database directory structure but no WAL entries
    std::fs::create_dir_all(dir.path().join("wal")).unwrap();

    // Follower should open with empty state
    let follower = Database::open_follower(dir.path()).unwrap();
    assert!(follower.is_follower());

    // Reads return nothing
    let branch = BranchId::default();
    assert_eq!(read_kv(&follower, branch, "anything"), None);

    // Refresh on empty WAL is a no-op
    let applied = follower.refresh().unwrap();
    assert_eq!(applied, 0);
}

#[test]
fn test_follower_on_nonexistent_path() {
    let dir = tempdir().unwrap();
    let nonexistent = dir.path().join("does_not_exist");

    let result = Database::open_follower(&nonexistent);
    assert!(result.is_err(), "Follower should fail on nonexistent path");
}

#[test]
fn test_follower_multiple_followers() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    // Primary writes data
    let primary = Database::open(dir.path()).unwrap();
    primary_put(&primary, branch, "shared_key", "shared_value");
    primary.flush().unwrap();

    // Open 3 followers simultaneously
    let f1 = Database::open_follower(dir.path()).unwrap();
    let f2 = Database::open_follower(dir.path()).unwrap();
    let f3 = Database::open_follower(dir.path()).unwrap();

    // All should see the data
    for (i, f) in [&f1, &f2, &f3].iter().enumerate() {
        assert_eq!(
            read_kv(f, branch, "shared_key").as_deref(),
            Some("shared_value"),
            "follower {} should see data",
            i
        );
    }

    // Each follower refreshes independently
    primary_put(&primary, branch, "new_key", "new_value");
    primary.flush().unwrap();

    // Only f1 refreshes
    f1.refresh().unwrap();
    assert_eq!(
        read_kv(&f1, branch, "new_key").as_deref(),
        Some("new_value")
    );
    // f2 and f3 don't see it yet
    assert_eq!(read_kv(&f2, branch, "new_key"), None);
    assert_eq!(read_kv(&f3, branch, "new_key"), None);

    // Now f2 refreshes
    f2.refresh().unwrap();
    assert_eq!(
        read_kv(&f2, branch, "new_key").as_deref(),
        Some("new_value")
    );
    assert_eq!(read_kv(&f3, branch, "new_key"), None);
}

#[test]
fn test_follower_survives_primary_crash() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    // Simulate: primary writes data then "crashes" (drop without clean shutdown)
    {
        let primary = Database::open(dir.path()).unwrap();
        primary_put(&primary, branch, "crash_key", "crash_value");
        primary.flush().unwrap();
        // Drop without explicit close — simulates crash
    }

    // Follower should recover and see committed data
    let follower = Database::open_follower(dir.path()).unwrap();
    assert_eq!(
        read_kv(&follower, branch, "crash_key").as_deref(),
        Some("crash_value")
    );
}

// ============================================================================
// Accessor tests
// ============================================================================

#[test]
fn test_follower_is_follower_accessor() {
    let dir = tempdir().unwrap();

    // Primary is not a follower
    let primary = Database::open(dir.path()).unwrap();
    assert!(!primary.is_follower());

    // Follower is a follower
    let follower = Database::open_follower(dir.path()).unwrap();
    assert!(follower.is_follower());

    // Cache is not a follower
    let cache = Database::cache().unwrap();
    assert!(!cache.is_follower());
}

#[test]
fn test_refresh_on_non_follower_returns_zero() {
    let dir = tempdir().unwrap();

    let primary = Database::open(dir.path()).unwrap();
    let result = primary.refresh().unwrap();
    assert_eq!(result, 0, "refresh on non-follower should be a no-op");

    let cache = Database::cache().unwrap();
    let result = cache.refresh().unwrap();
    assert_eq!(result, 0, "refresh on cache should be a no-op");
}
