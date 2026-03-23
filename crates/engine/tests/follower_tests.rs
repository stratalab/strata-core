//! Integration tests for read-only follower mode.
//!
//! Verifies that a follower can open a database that is exclusively locked by
//! a primary, replay the WAL, read data, refresh to see new data, and that
//! writes are rejected.

use std::sync::atomic::{AtomicBool, Ordering};
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

// ============================================================================
// Atomicity tests (Issue #1707)
// ============================================================================

/// Verify that refresh() applies each WAL transaction atomically.
///
/// A concurrent reader during refresh must never observe partial transaction
/// state: if it sees ANY write from a committed transaction, it must see ALL
/// writes from that transaction.
///
/// The bug: refresh() applied puts one-by-one via put_with_version_mode(),
/// each of which advances the global storage version independently. Between
/// the first put and the last delete, a concurrent reader could snapshot at
/// the transaction's version and observe partial state.
#[test]
fn test_issue_1707_refresh_atomic_visibility() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let primary = Database::open(dir.path()).unwrap();

    // Set up: commit many keys that will be deleted in the multi-key transaction.
    // Use a large number of keys to widen the race window.
    let n = ns(branch);
    for i in 0..50 {
        let k = Key::new_kv(n.clone(), &format!("old_{}", i));
        primary
            .transaction(branch, |txn| {
                txn.put(k.clone(), Value::String(format!("old_val_{}", i)))?;
                Ok(())
            })
            .unwrap();
    }
    primary.flush().unwrap();

    // Open follower BEFORE the multi-key transaction — it sees only old keys.
    let follower = Arc::new(Database::open_follower(dir.path()).unwrap());

    // Now commit ONE transaction that puts 50 new keys AND deletes 50 old keys.
    // All 100 mutations must become visible atomically during follower refresh.
    primary
        .transaction(branch, |txn| {
            for i in 0..50 {
                txn.put(
                    Key::new_kv(n.clone(), &format!("new_{}", i)),
                    Value::String(format!("new_val_{}", i)),
                )?;
                txn.delete(Key::new_kv(n.clone(), &format!("old_{}", i)))?;
            }
            Ok(())
        })
        .unwrap();
    primary.flush().unwrap();

    // Verify pre-condition: old keys visible, new keys not
    assert!(read_kv(&follower, branch, "old_0").is_some());
    assert!(read_kv(&follower, branch, "new_0").is_none());

    // Spawn reader threads that continuously check for partial state
    let found_partial = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let f = follower.clone();
            let found = found_partial.clone();
            let stop_flag = stop.clone();
            std::thread::spawn(move || {
                let n = ns(branch);
                while !stop_flag.load(Ordering::Relaxed) {
                    // Take a snapshot read
                    let mut txn = match f.begin_read_only_transaction(branch) {
                        Ok(t) => t,
                        Err(_) => continue,
                    };

                    // Count how many "new_" keys are visible
                    let mut new_visible = 0u32;
                    let mut old_visible = 0u32;
                    for i in 0..50 {
                        let nk = Key::new_kv(n.clone(), &format!("new_{}", i));
                        if let Ok(Some(_)) = txn.get(&nk) {
                            new_visible += 1;
                        }
                        let ok = Key::new_kv(n.clone(), &format!("old_{}", i));
                        if let Ok(Some(_)) = txn.get(&ok) {
                            old_visible += 1;
                        }
                    }

                    // Atomicity invariant: either we see the pre-transaction state
                    // (new_visible == 0 && old_visible == 50) or the post-transaction
                    // state (new_visible == 50 && old_visible == 0). Anything else
                    // means partial visibility.
                    if new_visible > 0 && new_visible < 50 {
                        found.store(true, Ordering::Relaxed);
                    }
                    if new_visible > 0 && old_visible > 0 {
                        found.store(true, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    // Give readers a moment to start, then refresh
    std::thread::sleep(std::time::Duration::from_millis(5));
    follower.refresh().unwrap();

    // Let readers run a bit after refresh to capture any lagging partial state
    std::thread::sleep(std::time::Duration::from_millis(10));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    // After refresh, verify final state is correct
    for i in 0..50 {
        assert_eq!(
            read_kv(&follower, branch, &format!("new_{}", i)).as_deref(),
            Some(format!("new_val_{}", i).as_str()),
            "new_{} should be visible after refresh",
            i
        );
        assert_eq!(
            read_kv(&follower, branch, &format!("old_{}", i)),
            None,
            "old_{} should be deleted after refresh",
            i
        );
    }

    assert!(
        !found_partial.load(Ordering::Relaxed),
        "Concurrent reader observed partial transaction state during refresh — \
         atomicity violation (ARCH-002)"
    );
}

/// Stress test variant: multiple large transactions replayed concurrently with readers.
#[test]
fn test_issue_1707_refresh_atomic_concurrent() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let primary = Database::open(dir.path()).unwrap();
    primary.flush().unwrap();

    // Open follower BEFORE the batch transactions.
    let follower = Arc::new(Database::open_follower(dir.path()).unwrap());

    // Commit 10 transactions, each writing 20 keys with the same value tag.
    // Within each txn, all keys get value "txn_T" where T is the txn index.
    let n = ns(branch);
    for t in 0..10u32 {
        primary
            .transaction(branch, |txn| {
                for k in 0..20u32 {
                    txn.put(
                        Key::new_kv(n.clone(), &format!("batch_{}", k)),
                        Value::String(format!("txn_{}", t)),
                    )?;
                }
                Ok(())
            })
            .unwrap();
    }
    primary.flush().unwrap();

    let found_partial = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let f = follower.clone();
            let found = found_partial.clone();
            let stop_flag = stop.clone();
            std::thread::spawn(move || {
                let n = ns(branch);
                while !stop_flag.load(Ordering::Relaxed) {
                    let mut txn = match f.begin_read_only_transaction(branch) {
                        Ok(t) => t,
                        Err(_) => continue,
                    };

                    // All 20 keys must have the SAME value (from the same txn).
                    // If any key has a different value, we saw a mix of two transactions.
                    let mut first_val: Option<String> = None;
                    let mut all_none = true;
                    let mut mismatch = false;

                    for k in 0..20u32 {
                        let key = Key::new_kv(n.clone(), &format!("batch_{}", k));
                        match txn.get(&key) {
                            Ok(Some(Value::String(s))) => {
                                all_none = false;
                                if let Some(ref expected) = first_val {
                                    if *expected != s {
                                        mismatch = true;
                                        break;
                                    }
                                } else {
                                    first_val = Some(s);
                                }
                            }
                            Ok(None) => {
                                // Key not yet visible — fine if ALL are none
                                if first_val.is_some() {
                                    mismatch = true;
                                    break;
                                }
                            }
                            _ => continue,
                        }
                    }

                    if mismatch && !all_none {
                        found.store(true, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    std::thread::sleep(std::time::Duration::from_millis(5));
    follower.refresh().unwrap();

    std::thread::sleep(std::time::Duration::from_millis(10));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    // All keys should have the final transaction's value
    for k in 0..20u32 {
        assert_eq!(
            read_kv(&follower, branch, &format!("batch_{}", k)).as_deref(),
            Some("txn_9"),
            "batch_{} should have final txn value",
            k
        );
    }

    assert!(
        !found_partial.load(Ordering::Relaxed),
        "Concurrent reader observed mixed transaction state during refresh — \
         atomicity violation (ARCH-002)"
    );
}
