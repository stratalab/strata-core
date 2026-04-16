//! Integration tests for read-only follower mode.
//!
//! Verifies that a follower can open a database that is exclusively locked by
//! a primary, replay the WAL, read data, refresh to see new data, and that
//! writes are rejected.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use strata_core::types::{BranchId, Key, Namespace};
use strata_core::value::Value;
use strata_core::JsonValue;
use strata_engine::database::OpenSpec;
use strata_engine::search::Searchable;
use strata_engine::{
    Database, JsonStore, NoopPreparedRefresh, PreparedRefresh, RefreshHook, RefreshHookError,
    RefreshHooks, SearchRequest, SearchSubsystem, Subsystem,
};
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

    fn freeze_to_disk(&self, _db: &Database) -> strata_core::StrataResult<()> {
        Ok(())
    }
}

impl Subsystem for FailOnceRefreshSubsystem {
    fn name(&self) -> &'static str {
        "fail-once-refresh"
    }

    fn recover(&self, _db: &Arc<Database>) -> strata_core::StrataResult<()> {
        Ok(())
    }

    fn initialize(&self, db: &Arc<Database>) -> strata_core::StrataResult<()> {
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
