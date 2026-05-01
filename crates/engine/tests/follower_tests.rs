//! Integration tests for read-only follower mode.
//!
//! Verifies that a follower can open a database that is exclusively locked by
//! a primary, replay the WAL, read data, refresh to see new data, and that
//! writes are rejected.

use serial_test::serial;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use strata_core::BranchId;
use strata_core::Value;
use strata_engine::database::OpenSpec;
use strata_engine::search::Searchable;
use strata_engine::{
    Database, JsonStore, JsonValue, NoopPreparedRefresh, PreparedRefresh, RefreshHook,
    RefreshHookError, RefreshHooks, SearchRequest, SearchSubsystem, Subsystem,
};
use strata_storage::{Key, Namespace};
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

#[derive(Clone)]
struct FailOnceRefreshSubsystem {
    fail_once: Arc<AtomicBool>,
}

impl FailOnceRefreshSubsystem {
    fn new(fail_once: Arc<AtomicBool>) -> Self {
        Self { fail_once }
    }
}

struct FailOnceRefreshHook {
    fail_once: Arc<AtomicBool>,
}

impl RefreshHook for FailOnceRefreshHook {
    fn name(&self) -> &'static str {
        "test-fail-once"
    }

    fn pre_delete_read(&self, _db: &Database, _deletes: &[Key]) -> Vec<(Key, Vec<u8>)> {
        Vec::new()
    }

    fn apply_refresh(
        &self,
        puts: &[(Key, Value)],
        _pre_read_deletes: &[(Key, Vec<u8>)],
    ) -> Result<Box<dyn PreparedRefresh>, RefreshHookError> {
        if !puts.is_empty() && self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(RefreshHookError::new(
                "test-fail-once",
                "injected refresh hook failure",
            ));
        }
        Ok(Box::new(NoopPreparedRefresh))
    }

    fn freeze_to_disk(&self, _db: &Database) -> strata_engine::StrataResult<()> {
        Ok(())
    }
}

impl Subsystem for FailOnceRefreshSubsystem {
    fn name(&self) -> &'static str {
        "fail-once-refresh"
    }

    fn recover(&self, _db: &Arc<Database>) -> strata_engine::StrataResult<()> {
        Ok(())
    }

    fn initialize(&self, db: &Arc<Database>) -> strata_engine::StrataResult<()> {
        let hooks = db.extension::<RefreshHooks>()?;
        hooks.register(Arc::new(FailOnceRefreshHook {
            fail_once: self.fail_once.clone(),
        }));
        Ok(())
    }
}

// ============================================================================
// Core functionality
// ============================================================================

#[test]
fn test_follower_opens_without_lock() {
    let dir = tempdir().unwrap();

    // Primary holds exclusive lock
    let _primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    // Follower should open successfully without any lock
    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem));
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
        let primary =
            Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
                .unwrap();
        primary_put(&primary, branch, "greeting", "hello");
        primary_put(&primary, branch, "count", "42");
        primary.flush().unwrap();
    }
    // Primary dropped, lock released

    // Follower opens and should see ALL the data via WAL recovery
    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
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
    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    primary_put(&primary, branch, "k1", "v1");
    primary.flush().unwrap();

    // Open follower — sees initial data
    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    assert_eq!(read_kv(&follower, branch, "k1").as_deref(), Some("v1"));

    // Primary writes more data
    primary_put(&primary, branch, "k2", "v2");
    primary.flush().unwrap();

    // Follower doesn't see new data yet
    assert_eq!(read_kv(&follower, branch, "k2"), None);

    // After refresh, follower sees the new data
    let outcome = follower.refresh();
    assert!(outcome.is_caught_up(), "refresh should catch up");
    assert!(
        outcome.applied_count() > 0,
        "refresh should apply new records"
    );
    assert_eq!(read_kv(&follower, branch, "k2").as_deref(), Some("v2"));

    // Original data still intact after refresh
    assert_eq!(read_kv(&follower, branch, "k1").as_deref(), Some("v1"));

    // Second refresh with no new data returns 0
    let outcome2 = follower.refresh();
    assert!(outcome2.is_caught_up());
    assert_eq!(
        outcome2.applied_count(),
        0,
        "refresh with no new WAL records should return 0"
    );
}

#[test]
fn test_follower_refresh_updates_json_search_index() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    let primary_json = JsonStore::new(primary.clone());
    let follower_json = JsonStore::new(follower.clone());
    let req = SearchRequest::new(branch, "supremacy").with_space("tenant_a");

    primary_json
        .create(
            &branch,
            "tenant_a",
            "doc1",
            JsonValue::from_value(serde_json::json!({
                "title": "breakthrough quantum supremacy result"
            })),
        )
        .unwrap();
    primary.flush().unwrap();

    let before = follower_json.search(&req).unwrap();
    assert!(
        before.hits.is_empty(),
        "follower search should lag until refresh applies the WAL"
    );

    let first_refresh = follower.refresh();
    assert!(first_refresh.is_caught_up(), "refresh should catch up");
    assert!(
        first_refresh.applied_count() > 0,
        "refresh should replay the JSON create transaction"
    );

    let after_create = follower_json.search(&req).unwrap();
    assert_eq!(
        after_create.hits.len(),
        1,
        "follower refresh should index JSON docs via the search refresh hook"
    );

    primary_json.destroy(&branch, "tenant_a", "doc1").unwrap();
    primary.flush().unwrap();

    let second_refresh = follower.refresh();
    assert!(second_refresh.is_caught_up(), "refresh should catch up");
    assert!(
        second_refresh.applied_count() > 0,
        "refresh should replay the JSON delete transaction"
    );

    let after_delete = follower_json.search(&req).unwrap();
    assert!(
        after_delete.hits.is_empty(),
        "follower refresh should remove deleted JSON docs from the search index"
    );
}

#[test]
fn test_follower_refresh_sees_updates_to_existing_keys() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    primary_put(&primary, branch, "key", "original");
    primary.flush().unwrap();

    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    assert_eq!(
        read_kv(&follower, branch, "key").as_deref(),
        Some("original")
    );

    // Primary UPDATES the same key
    primary_put(&primary, branch, "key", "updated");
    primary.flush().unwrap();

    let outcome = follower.refresh();
    assert!(outcome.is_caught_up());
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

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    primary_put(&primary, branch, "ephemeral", "here_now");
    primary.flush().unwrap();

    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    assert_eq!(
        read_kv(&follower, branch, "ephemeral").as_deref(),
        Some("here_now")
    );

    // Primary DELETES the key
    primary_del(&primary, branch, "ephemeral");
    primary.flush().unwrap();

    let outcome = follower.refresh();
    assert!(outcome.is_caught_up());
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

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    primary.flush().unwrap();

    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    // Write and refresh in rounds
    for i in 0..5 {
        let key = format!("round_{}", i);
        let val = format!("value_{}", i);
        primary_put(&primary, branch, &key, &val);
        primary.flush().unwrap();

        let outcome = follower.refresh();
        assert!(outcome.is_caught_up(), "round {} should catch up", i);
        assert!(
            outcome.applied_count() > 0,
            "round {} should apply records",
            i
        );
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
        let primary =
            Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
                .unwrap();
        primary_put(&primary, branch, "k", "v");
        primary.flush().unwrap();
    }

    // Open follower
    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

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
    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    assert!(follower.is_follower());

    // Reads return nothing
    let branch = BranchId::default();
    assert_eq!(read_kv(&follower, branch, "anything"), None);

    // Refresh on empty WAL is a no-op
    let outcome = follower.refresh();
    assert!(outcome.is_caught_up());
    assert_eq!(outcome.applied_count(), 0);
}

#[test]
fn test_follower_on_nonexistent_path() {
    let dir = tempdir().unwrap();
    let nonexistent = dir.path().join("does_not_exist");

    let result =
        Database::open_runtime(OpenSpec::follower(&nonexistent).with_subsystem(SearchSubsystem));
    assert!(result.is_err(), "Follower should fail on nonexistent path");
}

#[test]
fn test_follower_multiple_followers() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    // Primary writes data
    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    primary_put(&primary, branch, "shared_key", "shared_value");
    primary.flush().unwrap();

    // Open 3 followers simultaneously
    let f1 = Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
        .unwrap();
    let f2 = Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
        .unwrap();
    let f3 = Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
        .unwrap();

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
    let outcome = f1.refresh();
    assert!(outcome.is_caught_up());
    assert_eq!(
        read_kv(&f1, branch, "new_key").as_deref(),
        Some("new_value")
    );
    // f2 and f3 don't see it yet
    assert_eq!(read_kv(&f2, branch, "new_key"), None);
    assert_eq!(read_kv(&f3, branch, "new_key"), None);

    // Now f2 refreshes
    let outcome = f2.refresh();
    assert!(outcome.is_caught_up());
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
        let primary =
            Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
                .unwrap();
        primary_put(&primary, branch, "crash_key", "crash_value");
        primary.flush().unwrap();
        // Drop without explicit close — simulates crash
    }

    // Follower should recover and see committed data
    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
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
    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    assert!(!primary.is_follower());

    // Follower is a follower
    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    assert!(follower.is_follower());

    // Cache is not a follower
    let cache = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
    assert!(!cache.is_follower());
}

#[test]
fn test_refresh_on_non_follower_returns_zero() {
    let dir = tempdir().unwrap();

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    let outcome = primary.refresh();
    assert!(outcome.is_caught_up());
    assert_eq!(
        outcome.applied_count(),
        0,
        "refresh on non-follower should be a no-op"
    );

    let cache = Database::open_runtime(OpenSpec::cache().with_subsystem(SearchSubsystem)).unwrap();
    let outcome = cache.refresh();
    assert!(outcome.is_caught_up());
    assert_eq!(
        outcome.applied_count(),
        0,
        "refresh on cache should be a no-op"
    );
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

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    // Set up: commit many keys that will be deleted in the multi-key transaction.
    // Use a large number of keys to widen the race window.
    let n = ns(branch);
    for i in 0..50 {
        let k = Key::new_kv(n.clone(), format!("old_{}", i));
        primary
            .transaction(branch, |txn| {
                txn.put(k.clone(), Value::String(format!("old_val_{}", i)))?;
                Ok(())
            })
            .unwrap();
    }
    primary.flush().unwrap();

    // Open follower BEFORE the multi-key transaction — it sees only old keys.
    let follower = Arc::new(
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap(),
    );

    // Now commit ONE transaction that puts 50 new keys AND deletes 50 old keys.
    // All 100 mutations must become visible atomically during follower refresh.
    primary
        .transaction(branch, |txn| {
            for i in 0..50 {
                txn.put(
                    Key::new_kv(n.clone(), format!("new_{}", i)),
                    Value::String(format!("new_val_{}", i)),
                )?;
                txn.delete(Key::new_kv(n.clone(), format!("old_{}", i)))?;
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
                        let nk = Key::new_kv(n.clone(), format!("new_{}", i));
                        if let Ok(Some(_)) = txn.get(&nk) {
                            new_visible += 1;
                        }
                        let ok = Key::new_kv(n.clone(), format!("old_{}", i));
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
    let outcome = follower.refresh();
    assert!(outcome.is_caught_up());

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

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    primary.flush().unwrap();

    // Open follower BEFORE the batch transactions.
    let follower = Arc::new(
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap(),
    );

    // Commit 10 transactions, each writing 20 keys with the same value tag.
    // Within each txn, all keys get value "txn_T" where T is the txn index.
    let n = ns(branch);
    for t in 0..10u32 {
        primary
            .transaction(branch, |txn| {
                for k in 0..20u32 {
                    txn.put(
                        Key::new_kv(n.clone(), format!("batch_{}", k)),
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
                        let key = Key::new_kv(n.clone(), format!("batch_{}", k));
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
    let outcome = follower.refresh();
    assert!(outcome.is_caught_up());

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

// ============================================================================
// Follower status and admin skip tests (T3-E3)
// ============================================================================

#[test]
fn test_follower_status_basic() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    // Primary writes initial data
    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    primary_put(&primary, branch, "k1", "v1");
    primary.flush().unwrap();

    // Open follower
    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    // Initial status: caught up, not blocked
    let status = follower.follower_status();
    assert!(
        !status.is_blocked(),
        "follower should not be blocked initially"
    );
    assert!(!status.refresh_in_progress, "no refresh in progress");
    // applied and received should be equal after recovery
    assert_eq!(
        status.applied_watermark, status.received_watermark,
        "watermarks should match after recovery"
    );

    // Primary writes more data
    primary_put(&primary, branch, "k2", "v2");
    primary.flush().unwrap();

    // Before refresh: status reflects not caught up (once we check WAL)
    let outcome = follower.refresh();
    assert!(outcome.is_caught_up());

    // After refresh: status shows new watermarks
    let status2 = follower.follower_status();
    assert!(!status2.is_blocked());
    assert!(
        status2.applied_watermark >= status.applied_watermark,
        "applied watermark should advance after refresh"
    );
}

#[test]
fn test_follower_status_on_non_follower() {
    let dir = tempdir().unwrap();

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    // Primary should still be able to get follower_status (returns zero watermarks)
    let status = primary.follower_status();
    // We expect TxnId::ZERO or the recovered max_txn_id; either way, should not panic
    assert!(!status.is_blocked());
    assert!(!status.refresh_in_progress);
}

#[test]
fn test_admin_skip_rejects_non_follower() {
    let dir = tempdir().unwrap();

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    // admin_skip should fail on non-follower
    use strata_core::id::TxnId;
    use strata_engine::UnblockError;

    let result = primary.admin_skip_blocked_record(TxnId(1), "test skip");
    assert!(matches!(result, Err(UnblockError::NotFollower)));
}

#[test]
fn test_admin_skip_rejects_when_not_blocked() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    // Primary writes data
    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    primary_put(&primary, branch, "k1", "v1");
    primary.flush().unwrap();

    // Open follower - should catch up without issues
    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    // Verify follower is not blocked
    assert!(!follower.follower_status().is_blocked());

    // admin_skip should fail when not blocked
    use strata_core::id::TxnId;
    use strata_engine::UnblockError;

    let result = follower.admin_skip_blocked_record(TxnId(1), "test skip");
    assert!(matches!(result, Err(UnblockError::NotBlocked)));
}

#[test]
fn test_admin_skip_after_hook_failure_makes_record_visible() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();
    let fail_once = Arc::new(AtomicBool::new(true));

    let primary = Database::open_runtime(
        OpenSpec::primary(dir.path())
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();
    primary_put(&primary, branch, "base", "v1");
    primary.flush().unwrap();

    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path()).with_subsystem(FailOnceRefreshSubsystem::new(fail_once)),
    )
    .unwrap();

    primary_put(&primary, branch, "blocked", "v2");
    primary.flush().unwrap();

    let outcome = follower.refresh();
    let blocked_txn = match outcome {
        strata_engine::RefreshOutcome::Stuck {
            applied,
            applied_through,
            blocked_at,
        } => {
            assert_eq!(applied, 0, "hook failure should block before advancement");
            assert_eq!(
                blocked_at.reason.to_string(),
                "test-fail-once hook failed: injected refresh hook failure"
            );
            assert_eq!(
                applied_through,
                follower.follower_status().applied_watermark,
                "stuck outcome must report the last contiguously applied txn"
            );
            blocked_at.txn_id
        }
        other => panic!("expected refresh to get stuck, got {:?}", other),
    };

    let blocked_status = follower.follower_status();
    assert!(
        blocked_status.is_blocked(),
        "follower should report blocked"
    );
    assert!(
        blocked_status.received_watermark > blocked_status.applied_watermark,
        "received watermark should move ahead of applied on hook failure"
    );
    assert_eq!(
        read_kv(&follower, branch, "blocked"),
        None,
        "storage apply must remain invisible until the blocked record is resolved"
    );
    assert!(
        dir.path().join("follower_state.json").exists(),
        "blocked follower state must be persisted for restart recovery"
    );

    follower
        .admin_skip_blocked_record(blocked_txn, "operator acknowledged hook failure")
        .unwrap();

    assert_eq!(
        read_kv(&follower, branch, "blocked").as_deref(),
        Some("v2"),
        "admin skip must make a post-storage blocked record visible"
    );
    let recovered = follower.follower_status();
    assert!(!recovered.is_blocked(), "skip should clear blocked state");
    assert_eq!(
        recovered.applied_watermark, recovered.received_watermark,
        "skip should reconcile the watermarks for the skipped txn"
    );
    assert!(
        !dir.path().join("follower_state.json").exists(),
        "skip should clear the persisted blocked follower state"
    );

    let audit_log = std::fs::read_to_string(dir.path().join("follower_audit.log")).unwrap();
    assert!(
        audit_log.contains(&format!("txn_id={}", blocked_txn.as_u64())),
        "durable audit log must include the skipped txn id"
    );
}

#[test]
fn test_blocked_follower_state_survives_restart() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();
    let fail_once = Arc::new(AtomicBool::new(true));

    let primary = Database::open_runtime(
        OpenSpec::primary(dir.path())
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();
    primary_put(&primary, branch, "base", "v1");
    primary.flush().unwrap();

    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();

    primary_put(&primary, branch, "blocked_after_restart", "v2");
    primary.flush().unwrap();

    let blocked_txn = match follower.refresh() {
        strata_engine::RefreshOutcome::Stuck { blocked_at, .. } => blocked_at.txn_id,
        other => panic!("expected blocked refresh before restart, got {:?}", other),
    };
    assert_eq!(
        read_kv(&follower, branch, "blocked_after_restart"),
        None,
        "blocked record must remain invisible before restart"
    );

    drop(follower);

    let reopened = Database::open_runtime(
        OpenSpec::follower(dir.path()).with_subsystem(FailOnceRefreshSubsystem::new(fail_once)),
    )
    .unwrap();

    let status = reopened.follower_status();
    assert!(status.is_blocked(), "blocked follower state must persist");
    assert_eq!(
        status.blocked_at.as_ref().map(|blocked| blocked.txn_id),
        Some(blocked_txn),
        "reopened follower must report the same blocked txn"
    );
    assert_eq!(
        read_kv(&reopened, branch, "blocked_after_restart"),
        None,
        "reopened blocked follower must keep the record invisible"
    );
    assert!(
        matches!(
            reopened.refresh(),
            strata_engine::RefreshOutcome::NoProgress { .. }
        ),
        "blocked follower must stay blocked until operator action"
    );

    reopened
        .admin_skip_blocked_record(blocked_txn, "verify blocked follower restart durability")
        .unwrap();
    assert_eq!(
        read_kv(&reopened, branch, "blocked_after_restart").as_deref(),
        Some("v2"),
        "admin skip after restart must restore visibility"
    );
}

#[test]
fn test_blocked_follower_restart_keeps_search_state_clamped() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();
    let fail_once = Arc::new(AtomicBool::new(true));

    let primary = Database::open_runtime(
        OpenSpec::primary(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();
    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();

    let primary_json = JsonStore::new(primary.clone());
    let follower_json = JsonStore::new(follower.clone());
    let req = SearchRequest::new(branch, "restartblocked").with_space("tenant_a");

    primary_json
        .create(
            &branch,
            "tenant_a",
            "doc1",
            JsonValue::from_value(serde_json::json!({
                "title": "restartblocked must stay invisible across restart"
            })),
        )
        .unwrap();
    primary.flush().unwrap();

    assert!(
        matches!(
            follower.refresh(),
            strata_engine::RefreshOutcome::Stuck { .. }
        ),
        "refresh should block before making the document visible"
    );
    assert!(
        follower_json.search(&req).unwrap().hits.is_empty(),
        "blocked search updates must remain invisible before restart"
    );

    drop(follower);
    primary.shutdown().unwrap();
    drop(primary);

    let reopened = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_subsystem(FailOnceRefreshSubsystem::new(Arc::new(AtomicBool::new(
                false,
            )))),
    )
    .unwrap();
    let reopened_json = JsonStore::new(reopened);

    assert!(
        reopened_json.search(&req).unwrap().hits.is_empty(),
        "follower reopen must not rebuild blocked search docs from newer primary caches"
    );
}

#[test]
fn test_search_refresh_does_not_leak_before_visibility_advance() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();
    let fail_once = Arc::new(AtomicBool::new(true));

    let primary = Database::open_runtime(
        OpenSpec::primary(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();
    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once)),
    )
    .unwrap();

    let primary_json = JsonStore::new(primary.clone());
    let follower_json = JsonStore::new(follower.clone());
    let req = SearchRequest::new(branch, "blockedterm").with_space("tenant_a");

    primary_json
        .create(
            &branch,
            "tenant_a",
            "doc1",
            JsonValue::from_value(serde_json::json!({
                "title": "blockedterm should stay invisible while refresh is stuck"
            })),
        )
        .unwrap();
    primary.flush().unwrap();

    match follower.refresh() {
        strata_engine::RefreshOutcome::Stuck { .. } => {}
        other => panic!(
            "expected refresh to get stuck after search staging, got {:?}",
            other
        ),
    }

    assert!(
        follower_json.search(&req).unwrap().hits.is_empty(),
        "staged BM25 updates must not leak before visibility advance"
    );
}

#[test]
fn test_concurrent_refresh_serialized() {
    use std::thread;

    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    // Primary writes data
    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    for i in 0..100 {
        primary_put(&primary, branch, &format!("k{}", i), &format!("v{}", i));
    }
    primary.flush().unwrap();

    // Open follower
    let follower = Arc::new(
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap(),
    );

    // Write more data that follower hasn't seen
    for i in 100..200 {
        primary_put(&primary, branch, &format!("k{}", i), &format!("v{}", i));
    }
    primary.flush().unwrap();

    // Spawn multiple threads trying to refresh concurrently
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let f = follower.clone();
            thread::spawn(move || f.refresh())
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // Single-flight refresh means exactly one caller should apply the unseen
    // WAL records. The rest should wait, then observe that the follower is
    // already caught up.
    let total_applied: usize = results.iter().map(|r| r.applied_count()).sum();
    let non_zero_count = results.iter().filter(|r| r.applied_count() > 0).count();

    // All should report caught up
    assert!(
        results.iter().all(|r| r.is_caught_up()),
        "all refreshes should report caught up"
    );

    assert!(
        non_zero_count <= 1,
        "single-flight refresh should have only one caller apply records, saw {}",
        non_zero_count,
    );
    assert_eq!(
        total_applied, 100,
        "concurrent refreshes should apply each unseen record exactly once"
    );

    // Verify all data is visible
    for i in 0..200 {
        assert_eq!(
            read_kv(&follower, branch, &format!("k{}", i)).as_deref(),
            Some(format!("v{}", i).as_str()),
            "key k{} should be visible after refresh",
            i
        );
    }
}

/// T3-E9: a follower that has been shut down no longer accepts `refresh()` —
/// the follower-side shutdown barrier now short-circuits the refresh path
/// rather than mutating WAL/watermark state through a stale handle.
#[test]
fn test_follower_refresh_is_noop_after_shutdown() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    primary_put(&primary, branch, "k1", "v1");
    primary.flush().unwrap();

    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    let applied_before = follower.follower_status().applied_watermark;

    // Explicit shutdown on the follower. Per T3-E6, this quiesces the
    // follower and returns Ok; per T3-E9, a follower refresh after shutdown
    // must not mutate watermarks.
    follower.shutdown().unwrap();

    let outcome = follower.refresh();
    // Post-shutdown refresh reports CaughtUp at the last-applied watermark
    // (matching the non-follower / ephemeral branch semantics) — no new
    // records are processed.
    use strata_engine::RefreshOutcome;
    match outcome {
        RefreshOutcome::CaughtUp {
            applied,
            applied_through,
        } => {
            assert_eq!(applied, 0, "post-shutdown refresh applies zero records");
            assert_eq!(
                applied_through, applied_before,
                "applied watermark must not move after shutdown"
            );
        }
        other => panic!("expected CaughtUp after shutdown, got {:?}", other),
    }
    // Watermark is stable: the stale handle has not advanced state.
    assert_eq!(
        follower.follower_status().applied_watermark,
        applied_before,
        "follower_status must show no post-shutdown drift"
    );
}

/// T3-E9: admin_skip on a shut-down follower returns `UnblockError::DatabaseClosed`
/// rather than mutating watermarks / writing audit logs through a stale handle.
#[test]
fn test_admin_skip_rejects_after_shutdown() {
    use strata_core::id::TxnId;
    use strata_engine::UnblockError;

    let dir = tempdir().unwrap();

    let _primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    let status_before = follower.follower_status();

    follower.shutdown().unwrap();

    let result = follower.admin_skip_blocked_record(TxnId(1), "post-shutdown attempt");
    assert!(
        matches!(result, Err(UnblockError::DatabaseClosed)),
        "expected DatabaseClosed after shutdown, got {:?}",
        result
    );

    // Defence-in-depth: the guard must reject *before* any state mutates. If
    // a future refactor inadvertently reorders guards past the audit / watermark
    // writes, this assertion catches it.
    let status_after = follower.follower_status();
    assert_eq!(
        status_after.received_watermark, status_before.received_watermark,
        "received_watermark must not drift after a rejected admin_skip"
    );
    assert_eq!(
        status_after.applied_watermark, status_before.applied_watermark,
        "applied_watermark must not drift after a rejected admin_skip"
    );
    assert!(
        status_after.blocked_at.is_none(),
        "admin_skip on a non-blocked follower must not introduce a blocked_at state"
    );
}

/// T3-E10: a follower opening a corrupt WAL with `allow_lossy_recovery=true`
/// falls back to an empty database and populates the same
/// `LossyRecoveryReport` surface as the primary path.
///
/// Primary-path coverage lives in `crates/engine/src/database/tests/open.rs`.
/// The follower lossy branch is a distinct code path in `open.rs` with its
/// own `on_record` wrapper and lossy report construction; a regression that
/// breaks one without the other would slip past the primary-only test.
#[test]
fn test_follower_lossy_recovery_populates_report() {
    use strata_engine::StrataConfig;

    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    // Primary writes one valid record + flushes so the WAL has persisted
    // data for the follower to discover.
    {
        let primary =
            Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
                .unwrap();
        primary_put(&primary, branch, "k1", "v1");
        primary.flush().unwrap();
        primary.shutdown().unwrap(); // release WAL lock + registry slot
    }

    // Append garbage to the WAL as a fresh segment so the reader fails
    // mid-recovery. Using a new segment file avoids racing with any
    // partial-tail truncation on the existing active segment.
    let wal_dir = dir.path().join("wal");
    let corrupt_segment = wal_dir.join("wal-000099.seg");
    std::fs::write(&corrupt_segment, b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER").unwrap();

    // Strict follower open must refuse.
    let strict =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem));
    assert!(
        strict.is_err(),
        "strict follower open must refuse a corrupt WAL, got {:?}",
        strict.as_ref().map(|_| "Ok(Database)")
    );

    // Lossy follower open succeeds with an empty state + populated report.
    let cfg = StrataConfig {
        allow_lossy_recovery: true,
        ..StrataConfig::default()
    };
    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_config(cfg),
    )
    .expect("lossy follower open must succeed");

    let report = follower
        .last_lossy_recovery_report()
        .expect("follower lossy fallback must populate LossyRecoveryReport");
    assert!(
        report.discarded_on_wipe,
        "whole-database wipe is the pinned contract on the follower path too"
    );
    assert!(
        !report.error.is_empty(),
        "report.error must render the coordinator error"
    );
    // The typed classification must be populated on the follower path just
    // like the primary. WAL read failures surface as `StrataError::Storage`,
    // which maps to `LossyErrorKind::Storage` (see the mod.rs doc on
    // `LossyErrorKind::Storage` for why WAL-level corruption currently lands
    // in this bucket).
    use strata_engine::LossyErrorKind;
    assert_eq!(report.error_kind, LossyErrorKind::Storage);
    // records_applied_before_failure may be 0 or positive depending on where
    // in the replay the corrupt segment lands; assert only on the invariants
    // the follower path shares with the primary (Some vs None; wipe flag; error
    // non-empty; typed kind). Exact counts are covered by the primary-path test.
}

/// T3-E12 Phase 2 §D3 Site 2: follower refresh must decode records
/// through the cached `wal_codec`.
///
/// Pre-part-4, `Database::refresh` at `lifecycle.rs:339` built a raw
/// `WalReader::new()` with no codec — encrypted followers recovered
/// cleanly at open but then failed every `refresh()` with a
/// codec-decode error on the first new record. This test pins the
/// contract that open + refresh + read all share a single codec all
/// the way through. The review on PR 2427 flagged the absence of
/// this test as the gap that let four codec-threading sites slip.
#[test]
#[serial(open_databases)]
fn test_follower_refresh_with_non_identity_codec() {
    use strata_engine::StrataConfig;

    // `STRATA_ENCRYPTION_KEY` is a process-wide env var and the
    // `aes-gcm-256` codec reads it at construction. Use a stable value
    // for the whole test so primary and follower both see the same
    // key. Guard it with a local RAII so the var is released on drop.
    const TEST_AES_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    struct KeyGuard;
    impl Drop for KeyGuard {
        fn drop(&mut self) {
            std::env::remove_var("STRATA_ENCRYPTION_KEY");
        }
    }
    std::env::set_var("STRATA_ENCRYPTION_KEY", TEST_AES_KEY);
    let _key_guard = KeyGuard;

    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let aes_cfg = StrataConfig {
        durability: "standard".to_string(),
        storage: strata_engine::StorageConfig {
            codec: "aes-gcm-256".to_string(),
            ..strata_engine::StorageConfig::default()
        },
        ..StrataConfig::default()
    };

    // Primary writes an initial record through the encrypted WAL.
    let primary = Database::open_runtime(
        OpenSpec::primary(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_config(aes_cfg.clone()),
    )
    .expect("primary must open with aes-gcm-256 + standard durability");
    primary_put(&primary, branch, "pre", "before-follower");
    primary.flush().unwrap();

    // Follower opens through the MANIFEST-resolved codec (D7) and
    // recovers the existing record.
    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_config(aes_cfg.clone()),
    )
    .expect("follower must open cleanly against an encrypted WAL");
    assert_eq!(
        read_kv(&follower, branch, "pre").as_deref(),
        Some("before-follower"),
        "follower recovery must decode existing encrypted records at open",
    );

    // Primary writes NEW records post-follower-open. Without the
    // codec threading at `lifecycle.rs:339`, the follower's
    // `refresh()` would build a codec-less `WalReader` and fail to
    // decode the new ciphertext records. This is exactly the gap the
    // reviewer caught on PR 2427 pre-part-4.
    primary_put(&primary, branch, "post1", "after-refresh-1");
    primary_put(&primary, branch, "post2", "after-refresh-2");
    primary.flush().unwrap();

    let outcome = follower.refresh();
    assert!(
        outcome.is_caught_up(),
        "refresh under aes-gcm-256 must complete without blocking; got {outcome:?}",
    );
    assert!(
        outcome.applied_count() >= 2,
        "refresh must apply both new records; got applied_count = {}",
        outcome.applied_count(),
    );
    assert_eq!(
        read_kv(&follower, branch, "post1").as_deref(),
        Some("after-refresh-1"),
        "first post-open record must be visible after refresh",
    );
    assert_eq!(
        read_kv(&follower, branch, "post2").as_deref(),
        Some("after-refresh-2"),
        "second post-open record must be visible after refresh",
    );
    assert_eq!(
        read_kv(&follower, branch, "pre").as_deref(),
        Some("before-follower"),
        "pre-refresh record must still be visible (no wipe during healthy refresh)",
    );

    primary.shutdown().unwrap();
    follower.shutdown().unwrap();
}

/// D2 regression: follower refresh must degrade into persisted blocked state
/// on residual WAL-read failures instead of panicking the process.
#[test]
#[serial(open_databases)]
fn test_follower_refresh_corrupt_wal_returns_stuck_instead_of_panicking() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    primary_put(&primary, branch, "base", "v1");
    primary.flush().unwrap();

    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    assert_eq!(read_kv(&follower, branch, "base").as_deref(), Some("v1"));

    let corrupt_segment = dir.path().join("wal").join("wal-000099.seg");
    std::fs::write(&corrupt_segment, b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER").unwrap();

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| follower.refresh()))
        .expect("refresh must return blocked state instead of panicking on malformed WAL");
    let blocked_at = match outcome {
        strata_engine::RefreshOutcome::Stuck {
            applied,
            blocked_at,
            ..
        } => {
            assert_eq!(
                applied, 0,
                "malformed WAL should block before new records apply"
            );
            blocked_at
        }
        other => panic!(
            "malformed WAL during refresh must return Stuck, not {:?}",
            other
        ),
    };

    assert_eq!(
        blocked_at.txn_id.as_u64(),
        2,
        "residual WAL read failure should pin the next expected txn"
    );
    assert!(
        blocked_at.reason.to_string().contains("WAL read failed"),
        "blocked reason must preserve the reader failure detail: {:?}",
        blocked_at
    );

    let status = follower.follower_status();
    assert!(status.is_blocked(), "follower must surface blocked status");
    assert!(
        dir.path().join("follower_state.json").exists(),
        "blocked refresh must persist follower_state.json"
    );
    assert_eq!(
        read_kv(&follower, branch, "base").as_deref(),
        Some("v1"),
        "existing visible state must remain readable after blocked refresh"
    );

    primary.shutdown().unwrap();
    follower.shutdown().unwrap();
}

/// T3-E12 §D7: follower-without-MANIFEST falls back to
/// `get_codec(&cfg.storage.codec)` so encrypted WAL-only recovery
/// works even when no MANIFEST has been persisted yet.
///
/// Pre-part-4, the absent-MANIFEST branch at `open.rs:441` left
/// `follower_codec = None` and the recovery-coordinator wire was
/// conditional on `Some(_)`. An encrypted follower opening against a
/// directory without a MANIFEST would then read raw ciphertext and
/// fail.
///
/// This test constructs a bare `wal/` directory (no MANIFEST) with an
/// encrypted segment pre-written by a primary that cleanly shut down
/// its MANIFEST alongside. We then delete the MANIFEST to simulate
/// the "MANIFEST absent" state, and open a follower with the same
/// aes-gcm-256 config. The open must succeed; WAL records must be
/// readable through the config-derived codec fallback.
#[test]
#[serial(open_databases)]
fn test_follower_without_manifest_uses_config_codec() {
    use strata_engine::StrataConfig;

    const TEST_AES_KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    struct KeyGuard;
    impl Drop for KeyGuard {
        fn drop(&mut self) {
            std::env::remove_var("STRATA_ENCRYPTION_KEY");
        }
    }
    std::env::set_var("STRATA_ENCRYPTION_KEY", TEST_AES_KEY);
    let _key_guard = KeyGuard;

    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let aes_cfg = StrataConfig {
        durability: "standard".to_string(),
        storage: strata_engine::StorageConfig {
            codec: "aes-gcm-256".to_string(),
            ..strata_engine::StorageConfig::default()
        },
        ..StrataConfig::default()
    };

    // Primary writes an encrypted record then cleanly shuts down.
    {
        let primary = Database::open_runtime(
            OpenSpec::primary(dir.path())
                .with_subsystem(SearchSubsystem)
                .with_config(aes_cfg.clone()),
        )
        .unwrap();
        primary_put(&primary, branch, "k", "v-from-primary");
        primary.flush().unwrap();
        primary.shutdown().unwrap();
    }

    // Simulate "MANIFEST absent": remove the persisted MANIFEST but
    // leave the encrypted WAL segments. This is the D7 fallback case
    // the reviewer flagged (PR 2427 finding #4) — pre-part-4, the
    // follower would resolve `follower_codec = None` and silently read
    // raw ciphertext through a codec-less reader.
    let manifest_path = dir.path().join("MANIFEST");
    if manifest_path.exists() {
        std::fs::remove_file(&manifest_path).unwrap();
    }

    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_config(aes_cfg),
    )
    .expect(
        "follower must fall back to cfg.storage.codec when MANIFEST is \
         absent so encrypted WAL-only recovery works (T3-E12 §D7)",
    );

    assert_eq!(
        read_kv(&follower, branch, "k").as_deref(),
        Some("v-from-primary"),
        "follower without MANIFEST must decode the encrypted WAL via \
         config-codec fallback and expose the primary's record",
    );

    follower.shutdown().unwrap();
}

/// SE2 follow-up: a MANIFEST-absent follower open is a WAL-only bootstrap
/// case, so it must not fail just because `segments/` is absent.
#[test]
#[serial(open_databases)]
fn test_follower_without_manifest_tolerates_missing_segments_root() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    {
        let primary =
            Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
                .unwrap();
        primary_put(&primary, branch, "k", "v-from-primary");
        primary.shutdown().unwrap();
    }

    let manifest_path = dir.path().join("MANIFEST");
    if manifest_path.exists() {
        std::fs::remove_file(&manifest_path).unwrap();
    }
    let segments_dir = dir.path().join("segments");
    if segments_dir.exists() {
        std::fs::remove_dir_all(&segments_dir).unwrap();
    }

    let follower =
        Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem))
            .expect(
                "MANIFEST-absent follower open must stay WAL-only even when the \
         segments root is absent",
            );

    assert_eq!(
        read_kv(&follower, branch, "k").as_deref(),
        Some("v-from-primary"),
        "follower should recover from WAL without requiring a preexisting segments root",
    );

    follower.shutdown().unwrap();
}

/// T3-E11 / DR-011 regression guard for the follower open/refresh policy matrix.
///
/// The mixed policy is documented in
/// `docs/design/architecture-cleanup/durability-recovery-scope.md` §"Follower
/// open/refresh policy matrix":
///
/// * follower *open* honors `allow_lossy_recovery` (whole-DB wipe-and-reopen,
///   surfaced via `LossyRecoveryReport`)
/// * follower *refresh* is strict-only (`RefreshOutcome::Stuck` on apply
///   failure, operator-gated unblock via `admin_skip_blocked_record`)
///
/// Both behaviors already ship — the refresh-side strictness came from Phase
/// DR-3 (`ContiguousWatermark`) and the T3-E9 `AdvanceError::NonContiguous`
/// guard; the open-side lossy path came from T3-E10. DR-011's "consistent
/// documented policy matrix" acceptance clause was the one piece still
/// missing. This test locks the matrix in code so a future change that
/// widens `allow_lossy_recovery` to the refresh loop fails CI.
#[test]
fn follower_lossy_open_and_strict_refresh_coexist() {
    use strata_engine::StrataConfig;

    // -- Row 1: follower open honors allow_lossy_recovery ------------------
    // Mirrors the T3-E10 lossy-open regression (`test_follower_lossy_recovery_populates_report`)
    // so this test does not regress if the T3-E10 test is renamed or gated.
    {
        let dir = tempdir().unwrap();
        let branch = BranchId::default();

        {
            let primary = Database::open_runtime(
                OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem),
            )
            .unwrap();
            primary_put(&primary, branch, "pre-corruption", "v1");
            primary.flush().unwrap();
            primary.shutdown().unwrap();
        }

        let corrupt_segment = dir.path().join("wal").join("wal-000099.seg");
        std::fs::write(&corrupt_segment, b"GARBAGE_NOT_A_VALID_SEGMENT_HEADER").unwrap();

        // The strict path (no allow_lossy_recovery) must refuse — this is the
        // negative side of the open row; if it passed, the distinction between
        // strict and lossy open would be vacuous.
        let strict =
            Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(SearchSubsystem));
        assert!(
            strict.is_err(),
            "strict follower open must refuse a corrupt WAL"
        );

        let lossy_cfg = StrataConfig {
            allow_lossy_recovery: true,
            ..StrataConfig::default()
        };
        let lossy_follower = Database::open_runtime(
            OpenSpec::follower(dir.path())
                .with_subsystem(SearchSubsystem)
                .with_config(lossy_cfg),
        )
        .expect("lossy follower open must succeed against a corrupt WAL");

        let report = lossy_follower.last_lossy_recovery_report().expect(
            "follower open must populate LossyRecoveryReport when allow_lossy_recovery=true",
        );
        assert!(
            report.discarded_on_wipe,
            "whole-DB wipe-and-reopen is the pinned contract"
        );
        assert!(!report.error.is_empty());
    }

    // -- Row 2: follower refresh is strict even with allow_lossy_recovery=true
    //
    // If the refresh path incorrectly widened `allow_lossy_recovery` to mean
    // "auto-skip apply failures," the assertions below would fail: refresh
    // would silently advance the applied watermark and the `"blocked"` record
    // would become visible without operator action.
    {
        let dir = tempdir().unwrap();
        let branch = BranchId::default();
        let fail_once = Arc::new(AtomicBool::new(true));

        let primary = Database::open_runtime(
            OpenSpec::primary(dir.path())
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
        )
        .unwrap();
        primary_put(&primary, branch, "base", "v1");
        primary.flush().unwrap();

        let lossy_cfg = StrataConfig {
            allow_lossy_recovery: true,
            ..StrataConfig::default()
        };
        let follower = Database::open_runtime(
            OpenSpec::follower(dir.path())
                .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone()))
                .with_config(lossy_cfg),
        )
        .unwrap();
        // The open-row fallback did NOT fire on this follower — its WAL is
        // clean. Assert the slot is None so the cross-row isolation is pinned:
        // a lossy config alone must not produce a report.
        assert!(
            follower.last_lossy_recovery_report().is_none(),
            "a clean lossy follower open must not populate LossyRecoveryReport"
        );

        primary_put(&primary, branch, "blocked", "v2");
        primary.flush().unwrap();

        let outcome = follower.refresh();
        let blocked_txn = match outcome {
            strata_engine::RefreshOutcome::Stuck { blocked_at, .. } => blocked_at.txn_id,
            other => panic!(
                "follower refresh must be strict (return Stuck) regardless of \
                 allow_lossy_recovery; a silent skip here means the policy \
                 matrix has regressed. Got {:?}",
                other
            ),
        };

        let blocked = follower.follower_status();
        assert!(
            blocked.is_blocked(),
            "refresh must leave the follower blocked, not silently advanced"
        );
        assert_eq!(
            read_kv(&follower, branch, "blocked"),
            None,
            "the failed record must remain invisible — refresh does not skip on allow_lossy_recovery"
        );

        // `admin_skip_blocked_record` is the only documented unblock path.
        // Exercising it proves the refresh row's "operator-gated unblock"
        // mechanism is the one the matrix names; a future change that adds a
        // second path would violate CLAUDE.md §2 "one canonical path".
        follower
            .admin_skip_blocked_record(blocked_txn, "T3-E11 policy matrix: operator unblocks")
            .expect("admin_skip_blocked_record is the matrix-documented unblock");

        assert_eq!(
            read_kv(&follower, branch, "blocked").as_deref(),
            Some("v2"),
            "post-skip record becomes visible, confirming refresh honored the \
             skip rather than auto-advancing earlier"
        );
    }
}

// ============================================================================
// D6 — Follower Completion (DG-009, DG-010)
// ============================================================================

use std::sync::atomic::AtomicUsize;

/// Counting subsystem that records how many times `freeze()` runs. Used to
/// prove that follower shutdown and Drop fallback both invoke freeze hooks
/// (DG-010), and that retry-after-failure works.
struct FollowerFreezeCountingSubsystem {
    freeze_calls: Arc<AtomicUsize>,
    fail_first: Arc<AtomicBool>,
}

impl FollowerFreezeCountingSubsystem {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        let sub = Self {
            freeze_calls: counter.clone(),
            fail_first: Arc::new(AtomicBool::new(false)),
        };
        (sub, counter)
    }

    fn with_first_freeze_failure(self) -> Self {
        self.fail_first.store(true, Ordering::SeqCst);
        self
    }
}

impl Subsystem for FollowerFreezeCountingSubsystem {
    fn name(&self) -> &'static str {
        "follower-freeze-counter"
    }

    fn recover(&self, _db: &Arc<Database>) -> strata_engine::StrataResult<()> {
        Ok(())
    }

    fn freeze(&self, _db: &Database) -> strata_engine::StrataResult<()> {
        self.freeze_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_first.swap(false, Ordering::SeqCst) {
            Err(strata_engine::StrataError::internal(
                "injected follower freeze failure",
            ))
        } else {
            Ok(())
        }
    }
}

/// DG-009: a tampered `follower_state.json` whose persisted `BlockedTxnState`
/// violates a semantic invariant must be rejected on reopen. The follower
/// falls back to a fresh watermark at the recovered max txn id, does not
/// crash, and does not pin itself behind a record it already applied.
#[test]
fn test_follower_rejects_tampered_blocked_state_on_reopen() {
    use serde_json::Value as JsonValue;

    let dir = tempdir().unwrap();
    let branch = BranchId::default();
    let fail_once = Arc::new(AtomicBool::new(true));

    // Natural flow: create a valid follower_state.json with a blocked txn.
    let primary = Database::open_runtime(
        OpenSpec::primary(dir.path())
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();
    primary_put(&primary, branch, "base", "v1");
    primary.flush().unwrap();

    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();
    primary_put(&primary, branch, "next", "v2");
    primary.flush().unwrap();
    match follower.refresh() {
        strata_engine::RefreshOutcome::Stuck { .. } => {}
        other => panic!("expected blocked refresh to produce follower_state.json, got {other:?}"),
    }
    drop(follower);

    // Tamper: set `blocked.txn_id = applied_watermark`, which violates
    // BlockedNotAheadOfApplied. Persisted state still parses as valid JSON,
    // but the helper must reject it.
    let state_path = dir.path().join("follower_state.json");
    let bytes = std::fs::read(&state_path).expect("state file should exist after blocked refresh");
    let mut v: JsonValue = serde_json::from_slice(&bytes).unwrap();
    let applied = v["applied_watermark"].as_u64().expect("applied field");
    v["blocked"]["blocked"]["txn_id"] = JsonValue::from(applied);
    std::fs::write(&state_path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();

    // Reopen: must succeed. Follower must NOT be blocked (state was rejected).
    let reopened = Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(
        FailOnceRefreshSubsystem::new(Arc::new(AtomicBool::new(false))),
    ))
    .expect("follower must reopen even with a tampered state file");
    let status = reopened.follower_status();
    assert!(
        !status.is_blocked(),
        "tampered state must be rejected; reopen must not carry the bogus blocked txn"
    );
    assert!(
        !state_path.exists(),
        "reopen should clear the rejected follower_state.json instead of leaving stale state behind"
    );
}

/// D6: a persisted hook-failure blocked state must keep the post-apply
/// visibility floor it needs for truthful operator recovery. Removing that
/// floor from `follower_state.json` must cause reopen to drop the state
/// rather than restore a stale `SecondaryIndex` block that can no longer
/// repair visibility on admin skip.
#[test]
fn test_follower_rejects_tampered_hook_block_without_visibility_version() {
    use serde_json::Value as JsonValue;

    let dir = tempdir().unwrap();
    let branch = BranchId::default();
    let fail_once = Arc::new(AtomicBool::new(true));

    let primary = Database::open_runtime(
        OpenSpec::primary(dir.path())
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();
    primary_put(&primary, branch, "base", "v1");
    primary.flush().unwrap();

    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();
    primary_put(&primary, branch, "next", "v2");
    primary.flush().unwrap();
    match follower.refresh() {
        strata_engine::RefreshOutcome::Stuck { .. } => {}
        other => panic!("expected blocked refresh to produce follower_state.json, got {other:?}"),
    }
    drop(follower);

    let state_path = dir.path().join("follower_state.json");
    let bytes = std::fs::read(&state_path).expect("state file should exist after blocked refresh");
    let mut v: JsonValue = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        v["blocked"]["blocked"]["reason"]["SecondaryIndex"]["hook_name"]
            .as_str()
            .expect("secondary-index hook name"),
        "test-fail-once"
    );
    v["blocked"]["visibility_version"] = JsonValue::Null;
    std::fs::write(&state_path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();

    let reopened = Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(
        FailOnceRefreshSubsystem::new(Arc::new(AtomicBool::new(false))),
    ))
    .expect("follower must reopen even with a tampered state file");
    let status = reopened.follower_status();
    assert!(
        !status.is_blocked(),
        "hook block missing visibility_version must be rejected on reopen"
    );
}

/// D6 follow-up: post-apply invariant failures are intentionally
/// non-skippable. If a persisted blocked state is tampered from a hook
/// failure into `PostApplyInvariant` while leaving `skip_allowed=true`,
/// follower reopen must reject it rather than restore an invalid operator
/// bypass.
#[test]
fn test_follower_rejects_tampered_post_apply_block_with_skip_allowed() {
    use serde_json::json;
    use serde_json::Value as JsonValue;

    let dir = tempdir().unwrap();
    let branch = BranchId::default();
    let fail_once = Arc::new(AtomicBool::new(true));

    let primary = Database::open_runtime(
        OpenSpec::primary(dir.path())
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();
    primary_put(&primary, branch, "base", "v1");
    primary.flush().unwrap();

    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(FailOnceRefreshSubsystem::new(fail_once.clone())),
    )
    .unwrap();
    primary_put(&primary, branch, "next", "v2");
    primary.flush().unwrap();
    match follower.refresh() {
        strata_engine::RefreshOutcome::Stuck { .. } => {}
        other => panic!("expected blocked refresh to produce follower_state.json, got {other:?}"),
    }
    drop(follower);

    let state_path = dir.path().join("follower_state.json");
    let bytes = std::fs::read(&state_path).expect("state file should exist after blocked refresh");
    let mut v: JsonValue = serde_json::from_slice(&bytes).unwrap();
    v["blocked"]["blocked"]["reason"] = json!({
        "PostApplyInvariant": {
            "message": "tampered post-apply invariant"
        }
    });
    v["blocked"]["skip_allowed"] = JsonValue::from(true);
    std::fs::write(&state_path, serde_json::to_vec_pretty(&v).unwrap()).unwrap();

    let reopened = Database::open_runtime(OpenSpec::follower(dir.path()).with_subsystem(
        FailOnceRefreshSubsystem::new(Arc::new(AtomicBool::new(false))),
    ))
    .expect("follower must reopen even with a tampered state file");
    let status = reopened.follower_status();
    assert!(
        !status.is_blocked(),
        "post-apply block with skip_allowed=true must be rejected on reopen"
    );
}

/// DG-010: follower shutdown must run installed subsystem freeze hooks.
#[test]
fn test_follower_shutdown_runs_freeze_hooks() {
    let dir = tempdir().unwrap();

    let _primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    let (freeze_sub, freeze_count) = FollowerFreezeCountingSubsystem::new();
    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_subsystem(freeze_sub),
    )
    .unwrap();

    assert_eq!(freeze_count.load(Ordering::SeqCst), 0);
    follower.shutdown().expect("follower shutdown must succeed");
    assert_eq!(
        freeze_count.load(Ordering::SeqCst),
        1,
        "follower shutdown must invoke freeze() exactly once"
    );
}

/// DG-010: a follower shutdown whose freeze hook errors leaves the database
/// retriable — `shutdown_complete` stays unset, a subsequent `shutdown()`
/// re-runs freeze, and on success sets `shutdown_complete`.
#[test]
fn test_follower_shutdown_freeze_failure_is_retriable() {
    let dir = tempdir().unwrap();

    let _primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    let (freeze_sub, freeze_count) = FollowerFreezeCountingSubsystem::new();
    let freeze_sub = freeze_sub.with_first_freeze_failure();
    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_subsystem(freeze_sub),
    )
    .unwrap();

    assert!(
        follower.shutdown().is_err(),
        "first shutdown must bubble freeze failure"
    );
    assert_eq!(
        freeze_count.load(Ordering::SeqCst),
        1,
        "freeze ran exactly once on the failing call"
    );

    follower
        .shutdown()
        .expect("retry must succeed once freeze stops failing");
    assert_eq!(
        freeze_count.load(Ordering::SeqCst),
        2,
        "retry must invoke freeze a second time"
    );
}

/// DG-010: Drop fallback must run freeze hooks when `shutdown()` is skipped
/// on a follower. Prior to D6 the Drop path gated freeze on `!self.follower`,
/// so a follower that was dropped without explicit shutdown lost its
/// search/vector in-memory state.
#[test]
fn test_follower_drop_runs_freeze_when_shutdown_skipped() {
    let dir = tempdir().unwrap();

    let _primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();

    let (freeze_sub, freeze_count) = FollowerFreezeCountingSubsystem::new();
    {
        let follower = Database::open_runtime(
            OpenSpec::follower(dir.path())
                .with_subsystem(SearchSubsystem)
                .with_subsystem(freeze_sub),
        )
        .unwrap();
        // Intentionally no shutdown — rely on Drop fallback.
        drop(follower);
    }

    assert_eq!(
        freeze_count.load(Ordering::SeqCst),
        1,
        "Drop fallback must run freeze hooks on a follower whose shutdown() was skipped"
    );
}

/// DG-010: concurrent `refresh()` must not race with follower shutdown's
/// freeze hooks. D6 acquires `RefreshGuard` in the shutdown path so an
/// in-flight refresh finishes first; the re-check of `shutdown_complete`
/// under the gate short-circuits any refresh queued behind shutdown.
/// Without these, freeze and refresh would mutate search/vector state
/// concurrently.
#[test]
fn test_follower_shutdown_serializes_with_concurrent_refresh() {
    let dir = tempdir().unwrap();
    let branch = BranchId::default();

    let primary =
        Database::open_runtime(OpenSpec::primary(dir.path()).with_subsystem(SearchSubsystem))
            .unwrap();
    primary_put(&primary, branch, "k1", "v1");
    primary.flush().unwrap();

    let (freeze_sub, freeze_count) = FollowerFreezeCountingSubsystem::new();
    let follower = Database::open_runtime(
        OpenSpec::follower(dir.path())
            .with_subsystem(SearchSubsystem)
            .with_subsystem(freeze_sub),
    )
    .unwrap();

    // Fire refresh() repeatedly from a background thread while the main
    // thread calls shutdown(). Any refresh that started before shutdown
    // must finish cleanly; any refresh that queues behind shutdown's gate
    // must see shutdown_complete under the gate and short-circuit. Freeze
    // must have been observed exactly once, no panic, no deadlock.
    let refresh_handle = {
        let follower = Arc::clone(&follower);
        std::thread::spawn(move || {
            for _ in 0..50 {
                let _ = follower.refresh();
            }
        })
    };

    follower.shutdown().expect("follower shutdown must succeed");
    refresh_handle
        .join()
        .expect("refresh thread must not panic");

    assert_eq!(
        freeze_count.load(Ordering::SeqCst),
        1,
        "freeze must have run exactly once despite concurrent refresh"
    );

    // Post-shutdown refresh is a stable no-op.
    let outcome = follower.refresh();
    assert!(outcome.is_caught_up());
    assert_eq!(outcome.applied_count(), 0);
}
